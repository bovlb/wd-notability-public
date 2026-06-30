from __future__ import annotations

import asyncio
import calendar
import json
import os
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
import logging

from wd_notability.evaluation_cache import CACHE
from wd_notability.creations import CREATIONS, CreationMetadata
from wd_notability.file_lock import acquire_file_lock
from wd_notability.replica_connection import connect_replica

logger = logging.getLogger(__name__)

RECENT_CHANGES_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "recent_changes_worker"
RECENT_CHANGES_WORKER_POLL_SECONDS = 10.0  # Sleep between worker cycles.
RECENT_CHANGES_WORKER_REWIND_SECONDS = 86400.0  # Bootstrap from 24h ago when there is no saved cursor.
RECENT_CHANGES_WORKER_OVERLAP_SECONDS = 5.0  # Keep a small tail overlap when advancing the cursor.
RECENT_CHANGES_CREATION_BACKFILL_LIMIT = 500  # Cap creation metadata backfill work per cycle.
RECENT_CHANGES_REPLICA_QUERY_LIMIT = 1000  # Upper bound for a single replica read batch.
RECENT_CHANGES_LOOKUP_STATE_KEY = "recent_changes_worker_cursor"
RECENT_CHANGES_OBSERVABILITY_SAMPLE_SECONDS = 60.0
RECENT_CHANGES_THROUGHPUT_LOCK = asyncio.Lock()
RECENT_CHANGES_THROUGHPUT_TOTAL_PROCESSED = 0
RECENT_CHANGES_THROUGHPUT_STARTED_AT: float | None = None
RECENT_CHANGES_OBSERVABILITY_LOCK = asyncio.Lock()
RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED = 0.0


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True, frozen=True)
class ReplicaConfig:
    enabled: bool
    host: str
    port: int
    database: str
    defaults_file: Path

    @classmethod
    def from_env(cls) -> "ReplicaConfig":
        defaults_file = Path(
            os.getenv(
                "WD_NOTABILITY_RECENT_CHANGES_REPLICA_DEFAULTS_FILE",
                os.path.expanduser("~/replica.my.cnf"),
            )
        )
        return cls(
            enabled=_env_flag(
                "WD_NOTABILITY_RECENT_CHANGES_REPLICA_ENABLED",
                default=defaults_file.exists(),
            ),
            host=os.getenv(
                "WD_NOTABILITY_REPLICA_HOST",
                os.getenv("WD_NOTABILITY_RECENT_CHANGES_REPLICA_HOST", "wikidatawiki.analytics.db.svc.wikimedia.cloud"),
            ),
            port=int(os.getenv("WD_NOTABILITY_RECENT_CHANGES_REPLICA_PORT", "3306")),
            database=os.getenv("WD_NOTABILITY_RECENT_CHANGES_REPLICA_DATABASE", "wikidatawiki_p"),
            defaults_file=defaults_file,
        )


def _normalize_qid(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    value = value.strip().upper()
    return value if value.startswith("Q") and value[1:].isdigit() else None


def _parse_replica_timestamp(timestamp: object) -> float | None:
    if isinstance(timestamp, bytes):
        try:
            timestamp = timestamp.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(timestamp, str):
        return None
    text = timestamp.strip()
    if len(text) < 14 or not text[:14].isdigit():
        return None
    try:
        return float(calendar.timegm(time.strptime(text[:14], "%Y%m%d%H%M%S")))
    except (ValueError, OverflowError):
        return None


def _format_replica_timestamp(epoch_seconds: float) -> str:
    return time.strftime("%Y%m%d%H%M%S", time.gmtime(epoch_seconds))


def _connect_recent_changes_replica():
    return _RECENT_CHANGES_REPLICA._connect_replica()


async def _load_recent_changes_state() -> tuple[float | None, int | None, float | None]:
    payload = await CACHE.get_lookup_state(RECENT_CHANGES_LOOKUP_STATE_KEY)
    if not payload:
        return None, None, None
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        return None, None, None
    if not isinstance(data, dict):
        return None, None, None
    cursor_ts = _parse_replica_timestamp(data.get("rc_timestamp"))
    cursor_id = data.get("rc_id")
    try:
        cursor_id_num = int(cursor_id) if cursor_id is not None else None
    except (TypeError, ValueError):
        cursor_id_num = None
    creation_ts = _parse_replica_timestamp(data.get("creation_timestamp"))
    return cursor_ts, cursor_id_num, creation_ts


async def _save_recent_changes_state(
    cursor_timestamp: float | None,
    cursor_id: int | None,
    creation_timestamp: float | None,
) -> None:
    payload = json.dumps(
        {
            "rc_timestamp": None if cursor_timestamp is None else _format_replica_timestamp(cursor_timestamp),
            "rc_id": cursor_id,
            "creation_timestamp": None if creation_timestamp is None else _format_replica_timestamp(creation_timestamp),
        }
    )
    await CACHE.set_lookup_state(RECENT_CHANGES_LOOKUP_STATE_KEY, payload)


class _RecentChangesReplicaSource:
    def __init__(self) -> None:
        self._config = ReplicaConfig.from_env()

    @staticmethod
    def _pymysql_module():
        try:
            import pymysql  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError(
                "Recent changes replica access requires the optional 'pymysql' Python package."
            ) from exc
        return pymysql

    def _connect_replica(self):
        pymysql = self._pymysql_module()
        return connect_replica(
            pymysql,
            defaults_file=self._config.defaults_file,
            host=self._config.host,
            port=self._config.port,
            database=self._config.database,
        )

    def fetch_recent_changes(
        self,
        *,
        start_epoch: float,
        start_rc_id: int = 0,
        limit: int = RECENT_CHANGES_REPLICA_QUERY_LIMIT,
    ) -> tuple[list[dict[str, object]], tuple[float | None, int | None]]:
        if not self._config.enabled:
            raise RuntimeError("Recent changes replica access is disabled or unavailable")

        start_timestamp = _format_replica_timestamp(start_epoch)
        query = """
            SELECT
                rc_id,
                rc_timestamp,
                rc_title,
                rc_actor,
                rc_this_oldid,
                rc_last_oldid,
                rc_source,
                rc_log_type
            FROM recentchanges
            WHERE rc_namespace = 0
              AND (rc_log_type IS NULL OR rc_log_type <> 'delete')
              AND (
                    rc_timestamp > %s
                 OR (rc_timestamp = %s AND rc_id > %s)
              )
            ORDER BY rc_timestamp ASC, rc_id ASC
            LIMIT %s
        """

        changes: list[dict[str, object]] = []
        last_cursor: tuple[float | None, int | None] = (None, None)
        with closing(self._connect_replica()) as db:
            cursor = db.cursor()
            cursor.execute(query, (start_timestamp, start_timestamp, start_rc_id, limit))
            for rc_id, rc_timestamp, title, rc_actor, this_oldid, old_revid, rc_source, rc_log_type in cursor.fetchall():
                timestamp_epoch = _parse_replica_timestamp(rc_timestamp)
                normalized_qid = _normalize_qid(title)
                if timestamp_epoch is None or normalized_qid is None:
                    logger.warning("Skipping recent change with invalid timestamp or QID: %s, %s", rc_timestamp, title)
                    continue
                try:
                    creator_actor_id_num = int(rc_actor)
                except (TypeError, ValueError):
                    logger.warning("Recent change with invalid actor ID: %s", rc_actor)
                    creator_actor_id_num = None
                try:
                    old_revid_num = int(old_revid)
                except (TypeError, ValueError):
                    logger.warning("Recent change with invalid old_revid: %s", old_revid)
                    old_revid_num = None
                changes.append(
                    {
                        "title": normalized_qid,
                        "creator_actor_id": creator_actor_id_num,
                        "this_oldid": int(this_oldid) if isinstance(this_oldid, int) else None,
                        "revid": int(this_oldid) if isinstance(this_oldid, int) else None,
                        "old_revid": old_revid_num,
                        "rc_source": rc_source.decode("utf-8") if isinstance(rc_source, bytes) else rc_source,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp_epoch)),
                    }
                )
                try:
                    last_cursor = (timestamp_epoch, int(rc_id))
                except (TypeError, ValueError):
                    logger.warning("Recent change with invalid rc_id: %s", rc_id)
                    last_cursor = (timestamp_epoch, None)

        return changes, last_cursor


def _parse_rc_timestamp(timestamp: object) -> float | None:
    if not isinstance(timestamp, str):
        return None
    try:
        return float(calendar.timegm(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")))
    except (ValueError, OverflowError):
        return None


def _format_lag_seconds(lag_seconds: float | None) -> str:
    if lag_seconds is None:
        return "unknown"
    return f"{max(0.0, lag_seconds):.1f}s"


def _format_iso8601_epoch(epoch_seconds: int | float | None) -> str:
    if epoch_seconds is None:
        return "unknown"
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(epoch_seconds)))
    except (TypeError, ValueError, OverflowError):
        return "unknown"


async def count_recent_changes_backlog() -> int | None:
    if not _RECENT_CHANGES_REPLICA._config.enabled:
        return None

    saved_cursor_ts, saved_cursor_id, _saved_creation_ts = await _load_recent_changes_state()
    if saved_cursor_ts is None:
        start_epoch = max(0.0, time.time() - max(0.0, RECENT_CHANGES_WORKER_REWIND_SECONDS))
        start_rc_id = 0
    else:
        start_epoch = max(0.0, saved_cursor_ts)
        start_rc_id = max(0, saved_cursor_id or 0)

    start_timestamp = _format_replica_timestamp(start_epoch)
    query = """
        SELECT COUNT(*)
        FROM recentchanges
        WHERE rc_namespace = 0
          AND (
                rc_timestamp > %s
             OR (rc_timestamp = %s AND rc_id > %s)
          )
    """
    with closing(_connect_recent_changes_replica()) as db:
        cursor = db.cursor()
        cursor.execute(query, (start_timestamp, start_timestamp, start_rc_id))
        row = cursor.fetchone()

    if not row or row[0] is None:
        return 0
    try:
        return max(0, int(row[0]))
    except (TypeError, ValueError):
        return None


async def queue_stats() -> dict[str, int | None]:
    rc_backlog = await count_recent_changes_backlog()
    creation_backfill = await CACHE.count_missing_creation_qids()
    total = None if rc_backlog is None else rc_backlog + creation_backfill
    return {
        "recent_changes": rc_backlog,
        "creation_backfill": creation_backfill,
        "total": total,
    }


async def _record_recent_changes_throughput(processed_count: int) -> None:
    if processed_count <= 0:
        return

    global RECENT_CHANGES_THROUGHPUT_TOTAL_PROCESSED
    global RECENT_CHANGES_THROUGHPUT_STARTED_AT

    now = asyncio.get_running_loop().time()
    async with RECENT_CHANGES_THROUGHPUT_LOCK:
        if RECENT_CHANGES_THROUGHPUT_STARTED_AT is None:
            RECENT_CHANGES_THROUGHPUT_STARTED_AT = now
        RECENT_CHANGES_THROUGHPUT_TOTAL_PROCESSED += processed_count


async def _recent_changes_throughput_snapshot() -> dict[str, float | int | None]:
    async with RECENT_CHANGES_THROUGHPUT_LOCK:
        started_at = RECENT_CHANGES_THROUGHPUT_STARTED_AT
        total_processed = RECENT_CHANGES_THROUGHPUT_TOTAL_PROCESSED

    now = asyncio.get_running_loop().time()
    elapsed = max(0.0, now - started_at) if started_at is not None else 0.0
    rate = total_processed / elapsed if elapsed > 0 else 0.0
    return {
        "total_processed": total_processed,
        "started_at": started_at,
        "elapsed_seconds": elapsed,
        "rate_per_second": rate,
    }


async def _emit_recent_changes_observability() -> None:
    global RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED

    async with RECENT_CHANGES_OBSERVABILITY_LOCK:
        now = time.monotonic()
        if now - RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED < RECENT_CHANGES_OBSERVABILITY_SAMPLE_SECONDS:
            return
        RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED = now

    try:
        await CACHE.observability.record_worker_snapshot(
            worker_name="recent_changes",
            data={
                "queue": await queue_stats(),
                "throughput": await _recent_changes_throughput_snapshot(),
            },
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes observability emit failed: {exc}")


async def _upsert_creation_metadata(qids: list[str]) -> tuple[int, str | None]:
    if not qids:
        return 0, None

    metadata_rows = await asyncio.to_thread(CREATIONS.fetch_creation_metadata_many, qids)
    if not metadata_rows:
        return 0, None

    updated = await CACHE.upsert_creation_metadata_many(metadata_rows)
    creation_range = None
    if metadata_rows:
        creation_range = f"{_format_iso8601_epoch(metadata_rows[0].creation_time)}..{_format_iso8601_epoch(metadata_rows[-1].creation_time)}"
    return updated, creation_range


async def _run_creation_backfill() -> tuple[int, str | None]:
    creation_qids: set[str] = set()
    try:
        creation_qids.update(await CACHE.list_missing_creation_qids(limit=RECENT_CHANGES_CREATION_BACKFILL_LIMIT))
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes worker creation backfill candidate lookup failed: {exc}")
        return 0, None

    if not creation_qids:
        return 0, None

    try:
        return await _upsert_creation_metadata(sorted(creation_qids))
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes worker creation backfill failed: {exc}")
        return 0, None


_RECENT_CHANGES_REPLICA = _RecentChangesReplicaSource()


async def _work_recent_changes_pass(
    start_epoch: float,
    start_rc_id: int = 0,
) -> tuple[int, int, tuple[float | None, int | None, float | None, float]]:
    latest_seen = start_epoch
    latest_creation_seen: float | None = None
    qid_to_revid: dict[str, int] = {}
    creation_rows: dict[str, CreationMetadata] = {}
    cursor_timestamp = start_epoch
    cursor_id = start_rc_id
    total_fetched = 0
    oldest_timestamp = None
    newest_timestamp = None

    while True:
        changes, last_cursor = await asyncio.to_thread(
            _RECENT_CHANGES_REPLICA.fetch_recent_changes,
            start_epoch=cursor_timestamp,
            start_rc_id=cursor_id,
            limit=RECENT_CHANGES_REPLICA_QUERY_LIMIT,
        )
        total_fetched += len(changes)
        if last_cursor[0] is not None:
            cursor_timestamp, cursor_id = last_cursor[0], last_cursor[1] or 0
        for change in changes:
            if not isinstance(change, dict):
                logger.warning("Skipping invalid recent change record: %s", change)
                continue
            oldest_timestamp = min(oldest_timestamp, change.get("timestamp")) if oldest_timestamp is not None else change.get("timestamp")
            newest_timestamp = max(newest_timestamp, change.get("timestamp")) if newest_timestamp is not None else change.get("timestamp")
            title = change.get("title")
            qid = title if isinstance(title, str) and title.startswith("Q") and title[1:].isdigit() else None
            revid = change.get("revid")
            rc_source = change.get("rc_source")
            timestamp = _parse_rc_timestamp(change.get("timestamp"))
            creator_actor_id = change.get("creator_actor_id")
            creator_actor_id_num = creator_actor_id if isinstance(creator_actor_id, int) else None
            if qid is None or not isinstance(revid, int):
                logger.warning("Skipping invalid recent change record: %s", change)
                continue
            if timestamp is not None:
                latest_seen = max(latest_seen, timestamp)
            previous = qid_to_revid.get(qid)
            if previous is None or revid > previous:
                qid_to_revid[qid] = revid
            if (
                timestamp is not None
                and creator_actor_id_num is not None
                and isinstance(rc_source, str)
                and rc_source == "mw.new"
                and qid not in creation_rows
            ):
                creation_rows[qid] = CreationMetadata(
                    qid=qid,
                    creator_actor_id=creator_actor_id_num,
                    creation_time=int(timestamp),
                )
                latest_creation_seen = max(latest_creation_seen, timestamp) if latest_creation_seen is not None else timestamp

        if len(changes) < RECENT_CHANGES_REPLICA_QUERY_LIMIT or last_cursor[0] is None:
            break

    updated = 0
    creation_updated = 0
    if qid_to_revid:
        # Deduped by QID here: each target is written once, with the highest revid seen.
        updated = await CACHE.update_recent_changes_last_revids(qid_to_revid)
    if creation_rows:
        creation_updated = await CACHE.upsert_creation_metadata_many(list(creation_rows.values()))

    logging.info(f"Recent changes worker pass: entries read {total_fetched} {oldest_timestamp}..{newest_timestamp}, updates prepapred {len(qid_to_revid)}, updates made {updated}, creations prepared {len(creation_rows)}, creations upserted {creation_updated}")

    return updated, creation_updated, (latest_seen, cursor_id, latest_creation_seen, start_epoch)


async def recent_changes_worker_loop(
    *,
    poll_seconds: float = RECENT_CHANGES_WORKER_POLL_SECONDS,
    rewind_seconds: float = RECENT_CHANGES_WORKER_REWIND_SECONDS,
) -> None:
    with acquire_file_lock(RECENT_CHANGES_WORKER_LOCK_TARGET):
        saved_cursor_ts, _saved_cursor_id, saved_creation_ts = await _load_recent_changes_state()
        base_start_epoch = time.time() - max(0.0, rewind_seconds)
        if saved_cursor_ts is None:
            start_epoch = max(0.0, base_start_epoch)
        else:
            start_epoch = max(
                0.0,
                max(base_start_epoch, saved_cursor_ts - RECENT_CHANGES_WORKER_OVERLAP_SECONDS),
            )
        start_rc_id = 0
        while True:
            run_started = time.monotonic()
            try:
                rc_pass_started = time.monotonic()
                updated, rc_creation_updated, cursor = await _work_recent_changes_pass(start_epoch, start_rc_id)
                await _record_recent_changes_throughput(updated + rc_creation_updated)
                latest_seen, latest_rc_id, latest_creation_in_pass, scan_start_epoch = cursor
                rc_lag_seconds = None if latest_seen is None else time.time() - latest_seen
                if latest_seen is not None:
                    # Persist the live RC checkpoint before any slower backfill work runs.
                    await _save_recent_changes_state(
                        latest_seen,
                        latest_rc_id,
                        latest_creation_in_pass if latest_creation_in_pass is not None else saved_creation_ts,
                    )
                rc_pass_seconds = time.monotonic() - rc_pass_started

                backfill_started = time.monotonic()
                backfill_creation_updated, backfill_creation_range = await _run_creation_backfill()
                backfill_seconds = time.monotonic() - backfill_started
                latest_creation_seen = (
                    latest_creation_in_pass
                    if latest_creation_in_pass is not None
                    else saved_creation_ts
                )
                backfill_range_text = f", backfill_range={backfill_creation_range}" if backfill_creation_range else ""
                scan_range_text = (
                    f"{_format_iso8601_epoch(scan_start_epoch)}..{_format_iso8601_epoch(latest_seen)}"
                    if latest_seen is not None
                    else f"{_format_iso8601_epoch(scan_start_epoch)}..unknown"
                )
                print(
                    "Recent changes worker updated "
                    f"{updated} RC revid(s); "
                    f"live_creation={rc_creation_updated} row(s), "
                    f"backfill_creation={backfill_creation_updated} row(s)"
                    f"{backfill_range_text}; "
                    f"scan_range={scan_range_text}; "
                    f"lag={_format_lag_seconds(rc_lag_seconds)}; "
                    f"rc_pass={rc_pass_seconds:.1f}s, "
                    f"backfill={backfill_seconds:.1f}s"
                )
                if latest_seen is not None:
                    await _save_recent_changes_state(latest_seen, latest_rc_id, latest_creation_seen)
                    next_start = latest_seen - RECENT_CHANGES_WORKER_OVERLAP_SECONDS
                    start_epoch = max(
                        0.0,
                        max(time.time() - max(0.0, rewind_seconds), next_start),
                    )
                    start_rc_id = 0
                    saved_creation_ts = latest_creation_seen
                await _emit_recent_changes_observability()
            except Exception as exc:  # noqa: BLE001
                print(f"Recent changes worker failed: {exc}")

            sleep_for = max(0.0, poll_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)

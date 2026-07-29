from __future__ import annotations

import asyncio
import calendar
import json
import time
from contextlib import closing
from datetime import UTC, datetime

from wd_notability.evaluation_cache import CACHE
from wd_notability.models import QID
from wd_notability.content.fetcher import CONTENT_SOURCE
from wd_notability import cache_state

# Batch size for reading deletion/restore log entries from the replica.
CONTENT_DELETION_LOG_BATCH_SIZE = 200
# Lookup-state key that stores the last processed deletion log cursor.
CONTENT_DELETION_LOG_STATE_KEY = "content_deletion_log_cursor"


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
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(epoch_seconds))


async def _load_deletion_log_cursor() -> int:
    payload = await cache_state.get_lookup_state(CACHE, CONTENT_DELETION_LOG_STATE_KEY)
    if not payload:
        return await _bootstrap_deletion_log_cursor()
    try:
        data = json.loads(payload)
    except (TypeError, ValueError):
        return await _bootstrap_deletion_log_cursor()
    if not isinstance(data, dict):
        return await _bootstrap_deletion_log_cursor()
    cursor_log_id = data.get("log_id")
    try:
        log_id_num = max(0, int(cursor_log_id))
        if log_id_num == 0:
            return await _bootstrap_deletion_log_cursor()
        return log_id_num
    except (TypeError, ValueError):
        return await _bootstrap_deletion_log_cursor()


async def _bootstrap_deletion_log_cursor() -> int:
    if not CONTENT_SOURCE._replica_config.enabled:
        return 0

    cutoff_timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime(time.time() - 86400))
    query = """
        SELECT COALESCE(MAX(log_id), 0)
        FROM logging
        WHERE log_namespace = 0
          AND log_type = 'delete'
          AND log_action IN ('delete', 'restore')
          AND log_timestamp < %s
    """

    with closing(CONTENT_SOURCE._connect_replica()) as db:
        with db.cursor() as cursor:
            cursor.execute(query, (cutoff_timestamp,))
            row = cursor.fetchone()

    cursor_log_id = 0
    if row:
        try:
            cursor_log_id = max(0, int(row[0]))
        except (TypeError, ValueError):
            cursor_log_id = 0

    await _save_deletion_log_cursor(cursor_log_id)
    return cursor_log_id


async def _save_deletion_log_cursor(log_id: int) -> None:
    await cache_state.set_lookup_state(CACHE, CONTENT_DELETION_LOG_STATE_KEY, json.dumps({"log_id": int(log_id)}))


async def _fetch_deletion_log_candidates(limit: int) -> tuple[list[tuple[QID, int, str, int]], int | None, float | None, float | None]:
    if limit < 1:
        return [], None, None, None
    if not CONTENT_SOURCE._replica_config.enabled:
        return [], None, None, None

    cursor_log_id = await _load_deletion_log_cursor()
    query = """
        SELECT log_id, log_timestamp, log_title, log_action
        FROM logging
        WHERE log_namespace = 0
          AND log_type = 'delete'
          AND log_action IN ('delete', 'restore')
          AND log_id > %s
        ORDER BY log_id ASC
        LIMIT %s
    """

    events: list[tuple[QID, int, str, int]] = []
    last_log_id: int | None = None
    first_timestamp: float | None = None
    last_timestamp: float | None = None
    with closing(CONTENT_SOURCE._connect_replica()) as db:
        with db.cursor() as cursor:
            cursor.execute(query, (cursor_log_id, limit))
            for log_id, log_timestamp, log_title, log_action in cursor.fetchall():
                try:
                    last_log_id = int(log_id)
                except (TypeError, ValueError):
                    pass
                parsed_timestamp = _parse_replica_timestamp(log_timestamp)
                if parsed_timestamp is not None:
                    if first_timestamp is None:
                        first_timestamp = parsed_timestamp
                    last_timestamp = parsed_timestamp
                if isinstance(log_title, bytes):
                    try:
                        log_title = log_title.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                if isinstance(log_action, bytes):
                    try:
                        log_action = log_action.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                action = str(log_action).strip().lower() if isinstance(log_action, str) else ""
                event_type = "undelete" if action == "restore" else "delete" if action == "delete" else None
                if event_type is None:
                    continue
                qid = str(log_title).strip().upper() if isinstance(log_title, str) else None
                if qid is None or not qid.startswith("Q") or not qid[1:].isdigit():
                    continue
                if parsed_timestamp is None:
                    continue
                event_timestamp = datetime.fromtimestamp(parsed_timestamp, tz=UTC)
                events.append((qid, int(log_id), event_type, event_timestamp))
    return events, last_log_id, first_timestamp, last_timestamp


async def count_content_deletion_log_candidates() -> int | None:
    if not CONTENT_SOURCE._replica_config.enabled:
        return None

    cursor_log_id = await _load_deletion_log_cursor()
    query = """
        SELECT COUNT(*)
        FROM logging
        WHERE log_namespace = 0
          AND log_type = 'delete'
          AND log_action IN ('delete', 'restore')
          AND log_id > %s
    """

    with closing(CONTENT_SOURCE._connect_replica()) as db:
        with db.cursor() as cursor:
            cursor.execute(query, (cursor_log_id,))
            row = cursor.fetchone()

    if not row or row[0] is None:
        return 0
    try:
        return max(0, int(row[0]))
    except (TypeError, ValueError):
        return None


async def queue_stats() -> dict[str, int | None]:
    deletion_monitor = await count_content_deletion_log_candidates()
    return {
        "deletion_monitor": deletion_monitor,
        "total": deletion_monitor,
    }


async def work_content_deletion_monitor_batch(
    batch_size: int = CONTENT_DELETION_LOG_BATCH_SIZE,
) -> tuple[list[QID], str]:
    batch_started = time.perf_counter()
    events, deletion_cursor_id, first_timestamp, last_timestamp = await _fetch_deletion_log_candidates(batch_size)
    if not events:
        if deletion_cursor_id is not None:
            await _save_deletion_log_cursor(deletion_cursor_id)
        return [], "deletion log"

    recorded = await CACHE.upsert_content_deletion_events(events)
    if recorded != len(events):
        print(
            "Content deletion monitor recorded fewer events than fetched: "
            f"fetched={len(events)} recorded={recorded}"
        )
    if deletion_cursor_id is not None:
        await _save_deletion_log_cursor(deletion_cursor_id)

    qids = sorted({qid for qid, _log_id, _event_type, _event_timestamp in events})

    if first_timestamp is not None and last_timestamp is not None:
        if first_timestamp == last_timestamp:
            source_label = f"deletion log {_format_replica_timestamp(first_timestamp)}"
        else:
            source_label = (
                "deletion log "
                f"{_format_replica_timestamp(first_timestamp)} to {_format_replica_timestamp(last_timestamp)}"
            )
    else:
        source_label = "deletion log"

    elapsed = max(0.0, time.perf_counter() - batch_started)
    print(
        f"Content deletion monitor processed {len(qids)} qid(s) from {source_label} in {elapsed:.2f} seconds"
    )
    return qids, source_label


async def deletion_monitor_loop(poll_seconds: float = 60.0, batch_size: int = CONTENT_DELETION_LOG_BATCH_SIZE) -> None:
    while True:
        try:
            batch, _source_label = await work_content_deletion_monitor_batch(batch_size=batch_size)
        except Exception as exc:  # noqa: BLE001
            print(f"Content deletion monitor failed: {exc}")
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        if not batch:
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        await asyncio.sleep(max(0.1, poll_seconds))


__all__ = [
    "CONTENT_DELETION_LOG_BATCH_SIZE",
    "count_content_deletion_log_candidates",
    "deletion_monitor_loop",
    "queue_stats",
    "work_content_deletion_monitor_batch",
]

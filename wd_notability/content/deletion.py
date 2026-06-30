from __future__ import annotations

import asyncio
import calendar
import json
import time

from wd_notability.api_backoff import get_retry_after_remaining
from wd_notability.evaluation_cache import CACHE
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.models import QID
from wd_notability.content.fetcher import ENTITY_DATA_SOURCE

ENTITYDATA_DELETION_LOG_BATCH_SIZE = 200
ENTITYDATA_DELETION_LOG_STATE_KEY = "entitydata_deletion_log_cursor"


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
    payload = await CACHE.get_lookup_state(ENTITYDATA_DELETION_LOG_STATE_KEY)
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
    if not ENTITY_DATA_SOURCE._replica_config.enabled:
        return 0

    cutoff_timestamp = time.strftime("%Y%m%d%H%M%S", time.gmtime(time.time() - 86400))
    query = """
        SELECT COALESCE(MAX(log_id), 0)
        FROM logging
        WHERE log_namespace = 0
          AND log_type = 'delete'
          AND log_timestamp < %s
    """

    db = ENTITY_DATA_SOURCE._get_replica_connection()
    try:
        with db.cursor() as cursor:
            cursor.execute(query, (cutoff_timestamp,))
            row = cursor.fetchone()
    except Exception:
        ENTITY_DATA_SOURCE._reset_replica_connection()
        raise

    cursor_log_id = 0
    if row:
        try:
            cursor_log_id = max(0, int(row[0]))
        except (TypeError, ValueError):
            cursor_log_id = 0

    await _save_deletion_log_cursor(cursor_log_id)
    return cursor_log_id


async def _save_deletion_log_cursor(log_id: int) -> None:
    await CACHE.set_lookup_state(
        ENTITYDATA_DELETION_LOG_STATE_KEY,
        json.dumps({"log_id": int(log_id)}),
    )


async def _fetch_deletion_log_candidates(limit: int) -> tuple[list[QID], int | None, float | None, float | None]:
    if limit < 1:
        return [], None, None, None
    if not ENTITY_DATA_SOURCE._replica_config.enabled:
        return [], None, None, None

    cursor_log_id = await _load_deletion_log_cursor()
    query = """
        SELECT log_id, log_timestamp, log_title
        FROM logging
        WHERE log_namespace = 0
          AND log_type = 'delete'
          AND log_id > %s
        ORDER BY log_id ASC
        LIMIT %s
    """

    qids: list[QID] = []
    seen: set[QID] = set()
    last_log_id: int | None = None
    first_timestamp: float | None = None
    last_timestamp: float | None = None
    db = ENTITY_DATA_SOURCE._get_replica_connection()
    try:
        with db.cursor() as cursor:
            cursor.execute(query, (cursor_log_id, limit))
            for log_id, log_timestamp, log_title in cursor.fetchall():
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
                qid = str(log_title).strip().upper() if isinstance(log_title, str) else None
                if qid is None or not qid.startswith("Q") or not qid[1:].isdigit() or qid in seen:
                    continue
                seen.add(qid)
                qids.append(qid)
    except Exception:
        ENTITY_DATA_SOURCE._reset_replica_connection()
        raise
    return qids, last_log_id, first_timestamp, last_timestamp


async def count_entitydata_deletion_log_candidates() -> int | None:
    if not ENTITY_DATA_SOURCE._replica_config.enabled:
        return None

    cursor_log_id = await _load_deletion_log_cursor()
    query = """
        SELECT COUNT(DISTINCT log_title)
        FROM logging
        WHERE log_namespace = 0
          AND log_type = 'delete'
          AND log_id > %s
    """

    db = ENTITY_DATA_SOURCE._get_replica_connection()
    try:
        with db.cursor() as cursor:
            cursor.execute(query, (cursor_log_id,))
            row = cursor.fetchone()
    except Exception:
        ENTITY_DATA_SOURCE._reset_replica_connection()
        raise

    if not row or row[0] is None:
        return 0
    try:
        return max(0, int(row[0]))
    except (TypeError, ValueError):
        return None


async def queue_stats() -> dict[str, int | None]:
    deletion_monitor = await count_entitydata_deletion_log_candidates()
    return {
        "deletion_monitor": deletion_monitor,
        "total": deletion_monitor,
    }


async def work_entitydata_deletion_monitor_batch(
    batch_size: int = ENTITYDATA_DELETION_LOG_BATCH_SIZE,
) -> tuple[list[QID], str]:
    batch_started = time.perf_counter()
    if (retry_remaining := get_retry_after_remaining()) > 0:
        print(
            "EntityData deletion monitor abandoned batch selection due to shared Wikidata backoff "
            f"({retry_remaining} seconds remaining)"
        )
        return [], "shared Wikidata backoff"

    qids, deletion_cursor_id, first_timestamp, last_timestamp = await _fetch_deletion_log_candidates(batch_size)
    if not qids:
        return [], "deletion log"

    if deletion_cursor_id is not None:
        await _save_deletion_log_cursor(deletion_cursor_id)

    cleared = await CACHE.clear_entitydata_last_revids(qids)
    if cleared != len(qids):
        print(
            "EntityData deletion monitor cleared fewer rows than fetched: "
            f"fetched={len(qids)} cleared={cleared}"
        )

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
        f"EntityData deletion monitor processed {len(qids)} qid(s) from {source_label} in {elapsed:.2f} seconds"
    )
    return qids, source_label


async def deletion_monitor_loop(poll_seconds: float = 60.0, batch_size: int = ENTITYDATA_DELETION_LOG_BATCH_SIZE) -> None:
    while True:
        loop = asyncio.get_running_loop()
        try:
            wait_started = loop.time()
            await wait_for_foreground_evaluations()
            wait_elapsed = max(0.0, loop.time() - wait_started)
            print(f"EntityData deletion monitor waited {wait_elapsed:.2f} seconds for foreground work")
            batch, _source_label = await work_entitydata_deletion_monitor_batch(batch_size=batch_size)
        except Exception as exc:  # noqa: BLE001
            print(f"EntityData deletion monitor failed: {exc}")
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        if not batch:
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        await asyncio.sleep(max(0.1, poll_seconds))


__all__ = [
    "ENTITYDATA_DELETION_LOG_BATCH_SIZE",
    "count_entitydata_deletion_log_candidates",
    "deletion_monitor_loop",
    "queue_stats",
    "work_entitydata_deletion_monitor_batch",
]

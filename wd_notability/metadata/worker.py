from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from wd_notability.evaluation_cache import CACHE
from wd_notability.file_lock import acquire_file_lock
from wd_notability import user_history
from wd_notability.metadata import read as rc_read
from wd_notability.metadata import write as rc_write

logger = logging.getLogger(__name__)

RECENT_CHANGES_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "recent_changes_worker"
RECENT_CHANGES_WORKER_POLL_SECONDS = 2.0
RECENT_CHANGES_WORKER_REWIND_SECONDS = rc_read.RECENT_CHANGES_WORKER_REWIND_SECONDS
RECENT_CHANGES_WORKER_OVERLAP_SECONDS = 5.0
RECENT_CHANGES_CREATION_BACKFILL_LIMIT = 500
RECENT_CHANGES_USER_HISTORY_BACKFILL_LIMIT = 1
RECENT_CHANGES_REPLICA_QUERY_LIMIT = rc_read.RECENT_CHANGES_REPLICA_QUERY_LIMIT
RECENT_CHANGES_LOOKUP_STATE_KEY = rc_read.RECENT_CHANGES_LOOKUP_STATE_KEY
RECENT_CHANGES_OBSERVABILITY_SAMPLE_SECONDS = 60.0
RECENT_CHANGES_SCAN_THROUGHPUT_LOCK = asyncio.Lock()
RECENT_CHANGES_SCAN_THROUGHPUT_TOTAL_PROCESSED = 0
RECENT_CHANGES_SCAN_THROUGHPUT_STARTED_AT: float | None = None
RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_LOCK = asyncio.Lock()
RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_TOTAL_PROCESSED = 0
RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_STARTED_AT: float | None = None
RECENT_CHANGES_USER_CREATION_THROUGHPUT_LOCK = asyncio.Lock()
RECENT_CHANGES_USER_CREATION_THROUGHPUT_TOTAL_PROCESSED = 0
RECENT_CHANGES_USER_CREATION_THROUGHPUT_STARTED_AT: float | None = None
RECENT_CHANGES_OBSERVABILITY_LOCK = asyncio.Lock()
RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED = 0.0

_RECENT_CHANGES_REPLICA = rc_read.RECENT_CHANGES_REPLICA


def _sync_modules() -> None:
    rc_read.CACHE = CACHE
    rc_write.CACHE = CACHE
    rc_read.RECENT_CHANGES_REPLICA = _RECENT_CHANGES_REPLICA


def _format_iso8601_epoch(epoch_seconds: int | float | None) -> str:
    return rc_write._format_iso8601_epoch(epoch_seconds)


def _parse_rc_timestamp(timestamp: object) -> float | None:
    if not isinstance(timestamp, str):
        return None
    try:
        return float(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")))
    except (ValueError, OverflowError):
        return None


def _format_lag_seconds(lag_seconds: float | None) -> str:
    if lag_seconds is None:
        return "unknown"
    return f"{max(0.0, lag_seconds):.1f}s"


async def _load_recent_changes_state() -> tuple[float | None, int | None, float | None]:
    _sync_modules()
    return await rc_read.load_recent_changes_state()


async def _save_recent_changes_state(
    cursor_timestamp: float | None,
    cursor_id: int | None,
    creation_timestamp: float | None,
) -> None:
    _sync_modules()
    await rc_read.save_recent_changes_state(cursor_timestamp, cursor_id, creation_timestamp)


async def count_recent_changes_backlog() -> int | None:
    _sync_modules()
    return await rc_read.count_recent_changes_backlog()


async def _scan_queue_stats() -> dict[str, int | None]:
    rc_backlog = await count_recent_changes_backlog()
    return {"recent_changes": rc_backlog, "total": rc_backlog}


async def _creation_interest_queue_stats() -> dict[str, int | None]:
    creation_backfill = await CACHE.interest.count_interest_creation_targets()
    return {"creation_interest_backfill": creation_backfill, "total": creation_backfill}


async def _user_creation_queue_stats() -> dict[str, int | None]:
    user_history_backfill = await user_history.count_user_history_requests(CACHE)
    return {"user_creation_backfill": user_history_backfill, "total": user_history_backfill}


async def queue_stats() -> dict[str, int | None]:
    rc_queue = await _scan_queue_stats()
    creation_queue = await _creation_interest_queue_stats()
    user_queue = await _user_creation_queue_stats()
    rc_backlog = rc_queue["recent_changes"]
    creation_backfill = creation_queue["creation_interest_backfill"]
    user_history_backfill = user_queue["user_creation_backfill"]
    total = None if rc_backlog is None else rc_backlog + creation_backfill + user_history_backfill
    return {
        "recent_changes": rc_backlog,
        "creation_interest_backfill": creation_backfill,
        "user_creation_backfill": user_history_backfill,
        "total": total,
    }


async def _record_scan_throughput(processed_count: int) -> None:
    global RECENT_CHANGES_SCAN_THROUGHPUT_TOTAL_PROCESSED
    global RECENT_CHANGES_SCAN_THROUGHPUT_STARTED_AT
    if processed_count <= 0:
        return
    now = asyncio.get_running_loop().time()
    async with RECENT_CHANGES_SCAN_THROUGHPUT_LOCK:
        if RECENT_CHANGES_SCAN_THROUGHPUT_STARTED_AT is None:
            RECENT_CHANGES_SCAN_THROUGHPUT_STARTED_AT = now
        RECENT_CHANGES_SCAN_THROUGHPUT_TOTAL_PROCESSED += processed_count


async def _record_creation_interest_throughput(processed_count: int) -> None:
    global RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_TOTAL_PROCESSED
    global RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_STARTED_AT
    if processed_count <= 0:
        return
    now = asyncio.get_running_loop().time()
    async with RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_LOCK:
        if RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_STARTED_AT is None:
            RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_STARTED_AT = now
        RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_TOTAL_PROCESSED += processed_count


async def _record_user_creation_throughput(processed_count: int) -> None:
    global RECENT_CHANGES_USER_CREATION_THROUGHPUT_TOTAL_PROCESSED
    global RECENT_CHANGES_USER_CREATION_THROUGHPUT_STARTED_AT
    if processed_count <= 0:
        return
    now = asyncio.get_running_loop().time()
    async with RECENT_CHANGES_USER_CREATION_THROUGHPUT_LOCK:
        if RECENT_CHANGES_USER_CREATION_THROUGHPUT_STARTED_AT is None:
            RECENT_CHANGES_USER_CREATION_THROUGHPUT_STARTED_AT = now
        RECENT_CHANGES_USER_CREATION_THROUGHPUT_TOTAL_PROCESSED += processed_count


async def _throughput_snapshot(
    *,
    lock: asyncio.Lock,
    started_at_ref: str,
    total_processed_ref: str,
) -> dict[str, float | int | None]:
    async with lock:
        started_at = globals()[started_at_ref]
        total_processed = globals()[total_processed_ref]
    now = asyncio.get_running_loop().time()
    elapsed = max(0.0, now - started_at) if started_at is not None else 0.0
    rate = total_processed / elapsed if elapsed > 0 else 0.0
    return {"total_processed": total_processed, "started_at": started_at, "elapsed_seconds": elapsed, "rate_per_second": rate}


async def _scan_throughput_snapshot() -> dict[str, float | int | None]:
    return await _throughput_snapshot(
        lock=RECENT_CHANGES_SCAN_THROUGHPUT_LOCK,
        started_at_ref="RECENT_CHANGES_SCAN_THROUGHPUT_STARTED_AT",
        total_processed_ref="RECENT_CHANGES_SCAN_THROUGHPUT_TOTAL_PROCESSED",
    )


async def _creation_interest_throughput_snapshot() -> dict[str, float | int | None]:
    return await _throughput_snapshot(
        lock=RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_LOCK,
        started_at_ref="RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_STARTED_AT",
        total_processed_ref="RECENT_CHANGES_CREATION_INTEREST_THROUGHPUT_TOTAL_PROCESSED",
    )


async def _user_creation_throughput_snapshot() -> dict[str, float | int | None]:
    return await _throughput_snapshot(
        lock=RECENT_CHANGES_USER_CREATION_THROUGHPUT_LOCK,
        started_at_ref="RECENT_CHANGES_USER_CREATION_THROUGHPUT_STARTED_AT",
        total_processed_ref="RECENT_CHANGES_USER_CREATION_THROUGHPUT_TOTAL_PROCESSED",
    )


async def _emit_recent_changes_observability(
    *,
    worker_name: str,
    queue: dict[str, int | None],
    throughput: dict[str, float | int | None],
) -> None:
    global RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED
    async with RECENT_CHANGES_OBSERVABILITY_LOCK:
        now = time.monotonic()
        if now - RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED < RECENT_CHANGES_OBSERVABILITY_SAMPLE_SECONDS:
            return
        RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED = now
    try:
        await CACHE.observability.record_worker_snapshot(worker_name=worker_name, data={"queue": queue, "throughput": throughput})
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes observability emit failed for {worker_name}: {exc}")


async def _run_creation_interest_backfill() -> tuple[int, str | None]:
    _sync_modules()
    updated, created_range = await rc_write.run_creation_interest_backfill(CACHE, RECENT_CHANGES_CREATION_BACKFILL_LIMIT)
    return updated, created_range


async def _run_user_creation_backfill() -> tuple[int, str | None]:
    _sync_modules()
    return await rc_write.run_user_creation_backfill(CACHE, RECENT_CHANGES_USER_HISTORY_BACKFILL_LIMIT)


async def _run_recent_changes_scan_pass(
    start_epoch: float,
    start_rc_id: int = 0,
) -> tuple[int, int, tuple[float | None, int | None, float | None, float]]:
    _sync_modules()
    return await rc_write.run_recent_changes_scan_pass(CACHE, start_epoch, start_rc_id)


async def _recent_changes_scan_loop(*, poll_seconds: float = RECENT_CHANGES_WORKER_POLL_SECONDS, rewind_seconds: float = RECENT_CHANGES_WORKER_REWIND_SECONDS) -> None:
    saved_cursor_ts, _saved_cursor_id, saved_creation_ts = await _load_recent_changes_state()
    base_start_epoch = time.time() - max(0.0, rewind_seconds)
    start_epoch = max(0.0, base_start_epoch) if saved_cursor_ts is None else max(0.0, max(base_start_epoch, saved_cursor_ts - RECENT_CHANGES_WORKER_OVERLAP_SECONDS))
    start_rc_id = 0
    while True:
        run_started = time.monotonic()
        try:
            rc_pass_started = time.monotonic()
            updated, rc_creation_updated, cursor = await _run_recent_changes_scan_pass(start_epoch, start_rc_id)
            latest_seen, latest_rc_id, latest_creation_in_pass, scan_start_epoch = cursor
            rc_lag_seconds = None if latest_seen is None else time.time() - latest_seen
            if latest_seen is not None:
                await _save_recent_changes_state(latest_seen, latest_rc_id, latest_creation_in_pass if latest_creation_in_pass is not None else saved_creation_ts)
            await _record_scan_throughput(updated + rc_creation_updated)
            rc_pass_seconds = time.monotonic() - rc_pass_started
            latest_creation_seen = latest_creation_in_pass if latest_creation_in_pass is not None else saved_creation_ts
            scan_range_text = f"{_format_iso8601_epoch(scan_start_epoch)}..{_format_iso8601_epoch(latest_seen)}" if latest_seen is not None else f"{_format_iso8601_epoch(scan_start_epoch)}..unknown"
            print(
                "Recent changes monitor scanned "
                f"{updated} RC revid(s); "
                f"live_creation={rc_creation_updated} row(s); "
                f"scan_range={scan_range_text}; "
                f"lag={_format_lag_seconds(rc_lag_seconds)}; "
                f"rc_pass={rc_pass_seconds:.1f}s"
            )
            if latest_seen is not None:
                next_start = latest_seen - RECENT_CHANGES_WORKER_OVERLAP_SECONDS
                start_epoch = max(0.0, max(time.time() - max(0.0, rewind_seconds), next_start))
                start_rc_id = 0
                saved_creation_ts = latest_creation_seen
            await _emit_recent_changes_observability(worker_name="recent_changes_scan", queue=await _scan_queue_stats(), throughput=await _scan_throughput_snapshot())
        except Exception as exc:  # noqa: BLE001
            print(f"Recent changes monitor scan loop failed: {exc}")
        await asyncio.sleep(max(0.0, poll_seconds - (time.monotonic() - run_started)))


async def _creation_interest_backfill_loop(*, poll_seconds: float = RECENT_CHANGES_WORKER_POLL_SECONDS) -> None:
    while True:
        run_started = time.monotonic()
        try:
            backfill_started = time.monotonic()
            backfill_creation_updated, backfill_creation_range = await _run_creation_interest_backfill()
            backfill_seconds = time.monotonic() - backfill_started
            backfill_range_text = f", creation_interest_range={backfill_creation_range}" if backfill_creation_range else ""
            print(
                "Recent changes monitor creation-interest backfill "
                f"updated {backfill_creation_updated} row(s)"
                f"{backfill_range_text}; "
                f"backfill={backfill_seconds:.1f}s"
            )
            await _record_creation_interest_throughput(backfill_creation_updated)
            await _emit_recent_changes_observability(worker_name="recent_changes_creation_interest", queue=await _creation_interest_queue_stats(), throughput=await _creation_interest_throughput_snapshot())
        except Exception as exc:  # noqa: BLE001
            print(f"Recent changes monitor creation-interest loop failed: {exc}")
        await asyncio.sleep(max(0.0, poll_seconds - (time.monotonic() - run_started)))


async def _user_creation_backfill_loop(*, poll_seconds: float = RECENT_CHANGES_WORKER_POLL_SECONDS) -> None:
    while True:
        run_started = time.monotonic()
        try:
            user_history_started = time.monotonic()
            user_history_updated, user_history_summary = await _run_user_creation_backfill()
            user_history_seconds = time.monotonic() - user_history_started
            user_history_range_text = f", user_creation={user_history_summary}" if user_history_summary else ""
            print(
                "Recent changes monitor user-creation backfill "
                f"updated {user_history_updated} row(s)"
                f"{user_history_range_text}; "
                f"user_creation={user_history_seconds:.1f}s"
            )
            await _record_user_creation_throughput(user_history_updated)
            await _emit_recent_changes_observability(worker_name="recent_changes_user_creation", queue=await _user_creation_queue_stats(), throughput=await _user_creation_throughput_snapshot())
        except Exception as exc:  # noqa: BLE001
            print(f"Recent changes monitor user-creation loop failed: {exc}")
        await asyncio.sleep(max(0.0, poll_seconds - (time.monotonic() - run_started)))


async def recent_changes_worker_loop(*, poll_seconds: float = RECENT_CHANGES_WORKER_POLL_SECONDS, rewind_seconds: float = RECENT_CHANGES_WORKER_REWIND_SECONDS) -> None:
    _sync_modules()
    with acquire_file_lock(RECENT_CHANGES_WORKER_LOCK_TARGET):
        await asyncio.gather(
            _recent_changes_scan_loop(poll_seconds=poll_seconds, rewind_seconds=rewind_seconds),
            _creation_interest_backfill_loop(poll_seconds=poll_seconds),
            _user_creation_backfill_loop(poll_seconds=poll_seconds),
        )

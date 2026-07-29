from __future__ import annotations

import asyncio
import contextvars
import logging
import math
import os
from dataclasses import dataclass
from collections import deque
from collections.abc import Collection, Sequence
from pathlib import Path
import time
from uuid import uuid4

from wd_notability.evaluation_cache import CACHE
from wd_notability.file_lock import acquire_file_lock
from wd_notability.item_trace import ITEM_TRACE_ENABLED
from wd_notability.inlinks.cache import upsert_inlinks_strong_many
from wd_notability.item_trace import ItemTraceRecord
from wd_notability.models import EvaluationResult, NotabilityLevel, QID
from wd_notability.content.fetcher import CONTENT_SOURCE
from wd_notability.wikidata import EntityDeletedError

logger = logging.getLogger(__name__)

# Default batch size shared by the worker loop and ad hoc helpers.
DEFAULT_BATCH_SIZE = 50
# Queue capacity used by the dispatcher.
CONTENT_QUEUE_MAXSIZE = 5
# Number of worker coroutines in the content pool.
CONTENT_WORKER_COUNT = 5
# Idle dispatcher wake timeout used to discover externally published interest.
CONTENT_DISPATCH_WAKE_SECONDS = 1.0
# Chunk size used when evaluating a single worker batch.
CONTENT_EVALUATION_CHUNK_SIZE = DEFAULT_BATCH_SIZE
# QIDs currently claimed by content workers.
CONTENT_INFLIGHT_QIDS: set[QID] = set()
# Serializes access to the in-flight claim set.
CONTENT_INFLIGHT_LOCK = asyncio.Lock()
# File lock that prevents more than one content worker pool from running.
WORKER_POOL_LOCK_TARGET = Path(
    __file__).resolve().parents[2] / "data" / "worker_pool"
# Number of recent batches used to smooth the throughput rate.
CONTENT_THROUGHPUT_SAMPLE_WINDOW = 10
# Trailing wall-clock window used for trace-based throughput estimates.
CONTENT_THROUGHPUT_TRACE_WINDOW_SECONDS = float(
    os.getenv("WD_NOTABILITY_CONTENT_THROUGHPUT_TRACE_WINDOW_SECONDS", "30.0")
)
# Serializes throughput counters.
CONTENT_THROUGHPUT_LOCK = asyncio.Lock()
# Total number of QIDs processed since startup.
CONTENT_THROUGHPUT_TOTAL_PROCESSED = 0
# Monotonic timestamp when throughput tracking began.
CONTENT_THROUGHPUT_STARTED_AT: float | None = None
# Recent throughput samples used to compute a rolling rate.
CONTENT_THROUGHPUT_RECENT_BATCHES: deque[tuple[float, int]] = deque(
    maxlen=CONTENT_THROUGHPUT_SAMPLE_WINDOW)
# Minimum spacing between observability snapshots.
CONTENT_OBSERVABILITY_SAMPLE_SECONDS = 60.0
# Serializes observability emission timestamps.
CONTENT_OBSERVABILITY_LOCK = asyncio.Lock()
# Last observability emission time.
CONTENT_OBSERVABILITY_LAST_EMITTED = 0.0
# Serializes failure counter updates.
CONTENT_FAILURE_LOCK = asyncio.Lock()
# Aggregated failure counters for worker diagnostics.
CONTENT_FAILURE_TOTALS = {
    "context_errors": 0,
    "missing_lastrevid": 0,
    "unknown_live_result": 0,
    "validation_rejected": 0,
    "worker_exceptions": 0,
}
# Serializes accumulated timing totals.
CONTENT_TIMING_LOCK = asyncio.Lock()
# Aggregated wall-clock time by worker phase.
CONTENT_TIMING_TOTALS = {
    "selection": 0.0,
    "fetch_contexts": 0.0,
    "detector_sitelinks": 0.0,
    "detector_identifiers": 0.0,
    "detector_sources": 0.0,
    "evaluate": 0.0,
    "upsert": 0.0,
    "verify": 0.0,
    "release": 0.0,
    "other": 0.0,
}
# Per-call fetch timing, used to separate API latency from detector time.
CONTENT_EVALUATE_FETCH_SECONDS = contextvars.ContextVar(
    "CONTENT_EVALUATE_FETCH_SECONDS", default=0.0)
# Per-call detector timing broken down by detector family.
CONTENT_EVALUATE_DETECTOR_SECONDS = contextvars.ContextVar(
    "CONTENT_EVALUATE_DETECTOR_SECONDS",
    default={
        "sitelinks": 0.0,
        "identifiers": 0.0,
        "sources": 0.0,
    },
)


def _content_recent_throughput_rate() -> float:
    if len(CONTENT_THROUGHPUT_RECENT_BATCHES) < 2:
        return 0.0

    first_timestamp = CONTENT_THROUGHPUT_RECENT_BATCHES[0][0]
    last_timestamp = CONTENT_THROUGHPUT_RECENT_BATCHES[-1][0]
    elapsed = last_timestamp - first_timestamp
    if elapsed <= 0:
        return 0.0

    processed = sum(batch_size for _timestamp,
                    batch_size in CONTENT_THROUGHPUT_RECENT_BATCHES)
    return processed / elapsed


async def _content_trace_throughput_rate() -> float | None:
    if not ITEM_TRACE_ENABLED:
        return None
    trace_window_seconds = max(1.0, CONTENT_THROUGHPUT_TRACE_WINDOW_SECONDS)
    trace_since = max(0, int(time.time() - trace_window_seconds))
    trace_count = await CACHE.item_trace.count_events(
        worker_names=["content"],
        event_types=["results_written"],
        since=trace_since,
    )
    return trace_count / trace_window_seconds


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Optional debug check that rereads completed batches after write.
CONTENT_VERIFY_COMPLETED_BATCHES = _env_flag(
    "WD_NOTABILITY_CONTENT_VERIFY_COMPLETED_BATCHES",
    default=False,
)


@dataclass(frozen=True)
class ContentUpdate:
    qid: QID
    is_redirect: bool
    has_claims_count: int
    has_sitelinks_count: int
    is_deleted: bool
    n1: NotabilityLevel
    n2a: NotabilityLevel
    n2b: NotabilityLevel
    content_last_revid: int | None
    redirect_target: int | None = None


@dataclass(slots=True)
class ContentWorkBatch:
    qids: list[QID]
    batch_id: str
    source_label: str
    batch_timings: dict[str, float]


def _parse_redirect_target(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.startswith("Q") and value[1:].isdigit():
        return int(value[1:])
    return None


def _content_update_from_result(result: EvaluationResult, *, redirect_target: int | None = None) -> ContentUpdate:
    return ContentUpdate(
        qid=result.qid,
        is_redirect=result.is_redirect,
        has_claims_count=result.has_claims_count,
        has_sitelinks_count=result.has_sitelinks_count,
        is_deleted=result.is_deleted,
        n1=result.n1,
        n2a=result.n2a,
        n2b=result.n2b,
        content_last_revid=result.content_last_revid,
        redirect_target=redirect_target,
    )


def _format_content_problem(result: EvaluationResult, context: object) -> str:
    context_type = type(context).__name__
    errors = {
        key: value
        for key, value in result.errors.items()
        if value
    }
    return (
        f"deleted={result.is_deleted}, "
        f"revid={result.content_last_revid}, "
        f"claims={result.has_claims_count}, sitelinks={result.has_sitelinks_count}, "
        f"n1={result.n1}, n2a={result.n2a}, n2b={result.n2b}, "
        f"context_type={context_type}, errors={errors}"
    )


def _format_content_update_problem(update: ContentUpdate) -> str:
    return (
        f"deleted={update.is_deleted}, "
        f"revid={update.content_last_revid}, "
        f"claims={update.has_claims_count}, sitelinks={update.has_sitelinks_count}, "
        f"n1={update.n1}, n2a={update.n2a}, n2b={update.n2b}"
    )


def _content_writeback_details(update: ContentUpdate) -> dict[str, object]:
    return {
        "changed_rows": 1,
        "content_last_revid": update.content_last_revid,
        "redirect_target": update.redirect_target,
        "has_sitelinks_count": update.has_sitelinks_count,
        "has_claims_count": update.has_claims_count,
        "is_deleted": update.is_deleted,
        "n1": int(update.n1),
        "n2a": int(update.n2a),
        "n2b": int(update.n2b),
    }


def _empty_content_timings() -> dict[str, float]:
    return {
        "selection": 0.0,
        "fetch_contexts": 0.0,
        "detector_sitelinks": 0.0,
        "detector_identifiers": 0.0,
        "detector_sources": 0.0,
        "evaluate": 0.0,
        "upsert": 0.0,
        "verify": 0.0,
        "release": 0.0,
        "other": 0.0,
    }


async def _record_content_timings(timings: dict[str, float]) -> None:
    async with CONTENT_TIMING_LOCK:
        for key in CONTENT_TIMING_TOTALS:
            CONTENT_TIMING_TOTALS[key] += float(timings.get(key, 0.0))


async def _content_timing_snapshot(timings: dict[str, float]) -> str:
    total_seconds = sum(timings.values())
    if total_seconds <= 0:
        return "batch content timings: no accumulated time"

    return (
        f"batch content timings: total={total_seconds:.2f}s, "
        f"selection={timings['selection']:.2f}s, "
        f"fetch_contexts={timings['fetch_contexts']:.2f}s, "
        f"detector_sitelinks={timings['detector_sitelinks']:.2f}s, "
        f"detector_identifiers={timings['detector_identifiers']:.2f}s, "
        f"detector_sources={timings['detector_sources']:.2f}s, "
        f"evaluate={timings['evaluate']:.2f}s, "
        f"upsert={timings['upsert']:.2f}s, "
        f"verify={timings['verify']:.2f}s, "
        f"release={timings['release']:.2f}s, "
        f"other={timings['other']:.2f}s"
    )


async def _record_content_failure(kind: str, count: int = 1) -> None:
    if count <= 0:
        return
    async with CONTENT_FAILURE_LOCK:
        if kind not in CONTENT_FAILURE_TOTALS:
            CONTENT_FAILURE_TOTALS[kind] = 0
        CONTENT_FAILURE_TOTALS[kind] += count


async def _content_failure_snapshot() -> dict[str, int]:
    async with CONTENT_FAILURE_LOCK:
        return dict(CONTENT_FAILURE_TOTALS)


def _format_content_failure_delta(
    before: dict[str, int],
    after: dict[str, int],
) -> str:
    parts: list[str] = []
    for key in CONTENT_FAILURE_TOTALS:
        delta = int(after.get(key, 0)) - int(before.get(key, 0))
        if delta > 0:
            parts.append(f"{key}={delta}")
    return ", ".join(parts) if parts else "none"


async def _record_content_batch_events(
    event_type: str,
    qids: Sequence[QID],
    *,
    batch_id: str,
    details: dict[str, object] | None = None,
) -> None:
    trace = getattr(CACHE, "item_trace", None)
    if not qids or not getattr(trace, "enabled", True):
        return

    payload = dict(details or {})
    try:
        await CACHE.item_trace.record_events(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type=event_type,
                    worker_name="content",
                    batch_id=batch_id,
                    details=payload,
                )
                for qid in qids
            ]
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Content worker trace {event_type} emit failed: {exc}")


async def find_content_qids(
    batch_size: int,
    *,
    exclude_qids: Collection[QID] | None = None,
) -> list[QID]:
    """Return the next interest-driven content candidates.

    The dispatcher owns queue discovery and reservation.
    """
    if batch_size < 1:
        return []

    return await CACHE.interest.list_interest_content_candidates(
        limit=batch_size,
        exclude_qids=exclude_qids,
    )


async def queue_stats() -> dict[str, Any]:
    by_staleness = await CACHE.interest.count_interest_content_candidates_by_staleness()
    async with CONTENT_INFLIGHT_LOCK:
        inflight_count = len(CONTENT_INFLIGHT_QIDS)
    return {
        "stale": by_staleness["total"],
        "by_staleness": by_staleness,
        "in_flight": inflight_count,
    }


def _format_staleness_breakdown(by_staleness: dict[str, Any] | None) -> str:
    if not isinstance(by_staleness, dict):
        return "batch_staleness=unknown"

    ordered_keys = (
        "never_evaluated",
        "recent_changes_missing",
        "recent_changes",
        "redirect_target",
        "deletion_events",
        "content_policy",
    )
    total = int(by_staleness.get("total", 0) or 0)
    parts = [
        f"{key}={int(by_staleness.get(key, 0) or 0)}"
        for key in ordered_keys
    ]
    return f"batch_staleness(total={total}, {', '.join(parts)})"


def _split_content_qids(qids: Sequence[QID], batch_count: int) -> list[list[QID]]:
    if batch_count < 1 or not qids:
        return []

    batch_count = min(batch_count, len(qids))
    base_size, remainder = divmod(len(qids), batch_count)
    batches: list[list[QID]] = []
    start = 0
    for index in range(batch_count):
        batch_size = base_size + (1 if index < remainder else 0)
        end = start + batch_size
        batch = list(qids[start:end])
        if batch:
            batches.append(batch)
        start = end
    return batches


async def _reserve_content_qids(qids: Sequence[QID]) -> None:
    if not qids:
        return
    async with CONTENT_INFLIGHT_LOCK:
        CONTENT_INFLIGHT_QIDS.update(qids)


async def _release_content_qids(qids: Sequence[QID]) -> None:
    if not qids:
        return
    async with CONTENT_INFLIGHT_LOCK:
        CONTENT_INFLIGHT_QIDS.difference_update(qids)


async def _select_content_batches(
    *,
    free_slots: int,
    exclude_qids: Collection[QID] | None = None,
) -> tuple[list[list[QID]], float]:
    if free_slots < 1:
        return [], 0.0

    selection_started = time.perf_counter()
    qids = await find_content_qids(
        free_slots * DEFAULT_BATCH_SIZE,
        exclude_qids=exclude_qids,
    )
    selection_elapsed = max(0.0, time.perf_counter() - selection_started)
    await _record_content_timings({"selection": selection_elapsed})
    if not qids:
        return [], selection_elapsed

    batch_count = min(
        free_slots,
        math.ceil(len(qids) / DEFAULT_BATCH_SIZE),
    )
    return _split_content_qids(qids, batch_count), selection_elapsed


async def _process_content_batch(
    batch: ContentWorkBatch,
    *,
    add_new_cache_entries: bool = False,
) -> tuple[list[ContentUpdate], str, dict[str, int] | None, dict[str, float]]:
    batch_started = time.perf_counter()
    batch_started_epoch = int(time.time())
    updates: list[ContentUpdate] = []
    source_labels = [batch.source_label] if batch.source_label else []
    batch_outlinks: set[QID] = set()
    batch_timings = batch.batch_timings
    batch_staleness: dict[str, int] | None = None

    try:
        for start in range(0, len(batch.qids), CONTENT_EVALUATION_CHUNK_SIZE):
            chunk = batch.qids[start: start + CONTENT_EVALUATION_CHUNK_SIZE]
            failure_before = await _content_failure_snapshot()
            evaluate_started = time.perf_counter()
            chunk_updates, chunk_outlinks = await evaluate_content_many(chunk)

            evaluate_elapsed = max(
                0.0, time.perf_counter() - evaluate_started)
            fetch_elapsed = max(0.0, CONTENT_EVALUATE_FETCH_SECONDS.get())
            detector_timings = CONTENT_EVALUATE_DETECTOR_SECONDS.get()
            batch_timings["fetch_contexts"] += fetch_elapsed
            detector_elapsed = 0.0
            for key in ("sitelinks", "identifiers", "sources"):
                detector_value = max(0.0, float(
                    detector_timings.get(key, 0.0)))
                batch_timings[f"detector_{key}"] += detector_value
                detector_elapsed += detector_value
            batch_timings["evaluate"] += max(
                0.0, evaluate_elapsed - fetch_elapsed - detector_elapsed)

            if not chunk_updates:
                failure_after = await _content_failure_snapshot()
                print(
                    f"Content worker found no updates for chunk of {len(chunk)} qids "
                    f"(failure_delta={_format_content_failure_delta(failure_before, failure_after)})"
                )
                await _record_content_batch_events(
                    "work_abandoned",
                    chunk,
                    batch_id=batch.batch_id,
                    details={
                        "abandon_reason": "no_valid_updates",
                        "source": "pubsub",
                    },
                )
                continue

            changed = await _persist_content_chunk(
                chunk_updates,
                batch_timings,
                batch_id=batch.batch_id,
            )
            updates.extend(chunk_updates)
            batch_outlinks.update(chunk_outlinks)
    finally:
        release_started = time.perf_counter()
        if batch_outlinks:
            await upsert_inlinks_strong_many(
                CACHE,
                sorted(batch_outlinks),
                inlinks_last_evaluated=batch_started_epoch,
                create_missing=add_new_cache_entries,
            )
        try:
            await CACHE.item_trace.flush()
        except Exception as exc:  # noqa: BLE001
            print(f"Content worker trace flush failed: {exc}")
        batch_timings["release"] += max(0.0,
                                        time.perf_counter() - release_started)

    batch_elapsed = max(0.0, time.perf_counter() - batch_started)
    named_total = sum(batch_timings.values())
    batch_timings["other"] += max(0.0, batch_elapsed - named_total)
    await _record_content_timings(batch_timings)

    return updates, " and ".join(dict.fromkeys(source_labels)) or "unknown", batch_staleness, batch_timings


async def _run_selected_content_batch(
    qids: Sequence[QID],
    *,
    add_new_cache_entries: bool = False,
) -> tuple[list[ContentUpdate], str, dict[str, int] | None, dict[str, float]]:
    batch = ContentWorkBatch(
        qids=list(qids),
        batch_id=str(uuid4()),
        source_label="pubsub",
        batch_timings=_empty_content_timings(),
    )
    await _reserve_content_qids(batch.qids)
    await _record_content_batch_events(
        "batch_added",
        batch.qids,
        batch_id=batch.batch_id,
        details={
            "batch_size": len(batch.qids),
            "source": batch.source_label,
        },
    )
    try:
        return await _process_content_batch(
            batch,
            add_new_cache_entries=add_new_cache_entries,
        )
    finally:
        await _release_content_qids(batch.qids)


async def evaluate_content_many(qids: Collection[QID]) -> tuple[list[ContentUpdate], set[QID]]:
    qid_list = sorted(str(qid) for qid in qids)
    if not qid_list:
        print("evaluate_content_many called with no qids")
        return [], set()

    fetch_started = time.perf_counter()
    contexts = await CONTENT_SOURCE.get_contexts(qid_list)
    CONTENT_EVALUATE_FETCH_SECONDS.set(
        max(0.0, time.perf_counter() - fetch_started))
    detector_totals = {
        "sitelinks": 0.0,
        "identifiers": 0.0,
        "sources": 0.0,
    }
    CONTENT_EVALUATE_DETECTOR_SECONDS.set(detector_totals)
    updates: list[ContentUpdate] = []
    outlinks: set[QID] = set()

    for qid in qid_list:
        context = contexts.get(qid)
        if context is None:
            context = KeyError(
                f"Source {CONTENT_SOURCE.name} did not return context for {qid}")

        if isinstance(context, EntityDeletedError):
            result = EvaluationResult(qid=qid, is_deleted=True)
        elif isinstance(context, Exception):
            await _record_content_failure("context_errors")
            result = EvaluationResult(qid=qid)
            for detector in CONTENT_SOURCE.detectors:
                result.add_error(detector, context)
        else:
            result = await CONTENT_SOURCE._run_context_core(qid, context)
            source_timings = result.source_timings
            for key in detector_totals:
                detector_totals[key] += max(0.0,
                                            float(source_timings.get(f"detector_{key}", 0.0)))
        redirect_target = _parse_redirect_target(context.get(
            "redirect_target")) if isinstance(context, dict) else None
        if result.content_last_revid is None and not result.is_deleted:
            await _record_content_failure("missing_lastrevid")
            continue

        if not result.is_deleted and (
            result.n1 == NotabilityLevel.UNKNOWN
            or result.n2a == NotabilityLevel.UNKNOWN
            or result.n2b == NotabilityLevel.UNKNOWN
        ):
            await _record_content_failure("unknown_live_result")
            print(
                f"Content worker produced unknown live result for {qid}; "
                f"{_format_content_problem(result, context)}"
            )
            continue

        updates.append(_content_update_from_result(
            result, redirect_target=redirect_target))
        if result.n12 == NotabilityLevel.STRONG and isinstance(context, dict):
            raw_outlinks = context.get("outlinks", [])
            if isinstance(raw_outlinks, list):
                outlinks.update(
                    outlink for outlink in raw_outlinks if isinstance(outlink, str))

    # print(f"evaluate_content_many: qids={len(qid_list)}, contexts={len(contexts)}, updates={len(updates)}")
    CONTENT_EVALUATE_DETECTOR_SECONDS.set(detector_totals)
    return updates, outlinks


async def upsert_content_updates(updates: Sequence[ContentUpdate]) -> list[tuple[QID, int]]:
    if not updates:
        return []

    invalid_qids: list[str] = []
    for update in updates:
        if update.content_last_revid is None and not update.is_deleted:
            invalid_qids.append(str(update.qid))
            continue
        if not update.is_deleted and (
            update.n1 == NotabilityLevel.UNKNOWN
            or update.n2a == NotabilityLevel.UNKNOWN
            or update.n2b == NotabilityLevel.UNKNOWN
        ):
            invalid_qids.append(str(update.qid))

    if invalid_qids:
        await _record_content_failure("validation_rejected", len(invalid_qids))
        invalid_details = [
            f"{update.qid}: {_format_content_update_problem(update)}"
            for update in updates
            if update.qid in {qid for qid in invalid_qids}
        ]
        print(
            f"Content worker refusing to upsert incomplete batch: {', '.join(invalid_details)}")
        return []

    changed = await CACHE.upsert_content_many(updates)
    return changed


async def _debug_verify_completed_content_batch(qids: Sequence[QID]) -> None:
    if not CONTENT_VERIFY_COMPLETED_BATCHES or not qids:
        return

    rows = await CACHE.get_many(list(qids))
    stale_qids: list[str] = []
    for qid in qids:
        row = rows.get(str(qid))
        if row is None:
            stale_qids.append(str(qid))
            continue
        content_last_revid = row.content_last_revid
        recent_changes_last_revid = row.recent_changes_last_revid
        if content_last_revid is None or recent_changes_last_revid is None or content_last_revid < recent_changes_last_revid:
            stale_qids.append(str(qid))

    if stale_qids:
        print(
            f"Content debug verification found {len(stale_qids)} stale qids after upsert: "
            f"{stale_qids}"
        )
    else:
        print(f"Content debug verification passed for {len(qids)} qid(s)")


async def _persist_content_chunk(
    chunk_updates: Sequence[ContentUpdate],
    batch_timings: dict[str, float],
    *,
    batch_id: str | None = None,
) -> list[tuple[QID, int]]:
    if not chunk_updates:
        return []

    upsert_started = time.perf_counter()
    changed = await upsert_content_updates(chunk_updates)
    batch_timings["upsert"] += max(0.0, time.perf_counter() - upsert_started)

    if changed:
        try:
            trace = getattr(CACHE, "item_trace", None)
            if getattr(trace, "enabled", True):
                event_batch_id = batch_id or str(uuid4())
                updates_by_qid = {
                    str(update.qid): update for update in chunk_updates}
                await trace.record_events(
                    [
                        ItemTraceRecord(
                            qid=qid,
                            event_type="results_written",
                            worker_name="content",
                            batch_id=event_batch_id,
                            details=_content_writeback_details(update),
                        )
                        for qid, _rowcount in changed
                        if (update := updates_by_qid.get(str(qid))) is not None
                    ]
                )
        except Exception as exc:  # noqa: BLE001
            print(f"Content worker trace writeback emit failed: {exc}")

    await _debug_verify_completed_content_batch([update.qid for update in chunk_updates])
    return changed


async def work_content_pubsub_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    *,
    add_new_cache_entries: bool = False,
) -> tuple[list[ContentUpdate], str, dict[str, int] | None, dict[str, float]]:
    qids = await find_content_qids(batch_size)
    if not qids:
        return [], "unknown", {}, _empty_content_timings()
    return await _run_selected_content_batch(
        qids,
        add_new_cache_entries=add_new_cache_entries,
    )


async def work_content_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    *,
    add_new_cache_entries: bool = False,
) -> tuple[list[ContentUpdate], str, dict[str, int] | None, dict[str, float]]:
    return await work_content_pubsub_batch(
        batch_size=batch_size,
        add_new_cache_entries=add_new_cache_entries,
    )


async def work_queued_items(
    *,
    limit: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
    add_new_cache_entries: bool = False,
) -> int:
    processed = 0
    while limit <= 0 or processed < limit:
        current_batch_size = batch_size if limit <= 0 else min(
            batch_size, limit - processed)
        qids = await find_content_qids(current_batch_size)
        if not qids:
            break
        batch, _source_label, _batch_staleness, _batch_timings = await _run_selected_content_batch(
            qids,
            add_new_cache_entries=add_new_cache_entries,
        )
        processed += len(batch)
    return processed


async def _record_content_throughput(batch_size: int) -> str:
    global CONTENT_THROUGHPUT_TOTAL_PROCESSED
    global CONTENT_THROUGHPUT_STARTED_AT

    now = asyncio.get_running_loop().time()
    async with CONTENT_THROUGHPUT_LOCK:
        if CONTENT_THROUGHPUT_STARTED_AT is None:
            CONTENT_THROUGHPUT_STARTED_AT = now

        CONTENT_THROUGHPUT_TOTAL_PROCESSED += batch_size
        CONTENT_THROUGHPUT_RECENT_BATCHES.append((now, batch_size))
    total_rate = _content_recent_throughput_rate()
    try:
        trace_rate = await _content_trace_throughput_rate()
    except Exception as exc:  # noqa: BLE001
        print(f"Content worker trace throughput lookup failed: {exc}")
        trace_rate = None
    if trace_rate is not None and trace_rate > 0:
        return f"throughput={trace_rate:.2f} qid/s"
    if total_rate > 0:
        return f"throughput={total_rate:.2f} qid/s"
    return "throughput=unknown"


async def _content_throughput_snapshot() -> dict[str, float | int | None]:
    async with CONTENT_THROUGHPUT_LOCK:
        started_at = CONTENT_THROUGHPUT_STARTED_AT
        total_processed = CONTENT_THROUGHPUT_TOTAL_PROCESSED
        recent_rate = _content_recent_throughput_rate()

    now = asyncio.get_running_loop().time()
    elapsed = max(0.0, now - started_at) if started_at is not None else 0.0
    try:
        trace_rate = await _content_trace_throughput_rate()
    except Exception as exc:  # noqa: BLE001
        print(f"Content worker trace throughput snapshot failed: {exc}")
        trace_rate = None
    return {
        "total_processed": total_processed,
        "started_at": started_at,
        "elapsed_seconds": elapsed,
        "rate_per_second": trace_rate if trace_rate is not None and trace_rate > 0 else recent_rate,
    }


async def _content_timing_totals_snapshot() -> dict[str, float]:
    async with CONTENT_TIMING_LOCK:
        return dict(CONTENT_TIMING_TOTALS)


async def _emit_content_observability(
    worker_id: int,
    *,
    poll_seconds: float,
    queue_depth: int = 0,
) -> None:
    global CONTENT_OBSERVABILITY_LAST_EMITTED

    async with CONTENT_OBSERVABILITY_LOCK:
        now = asyncio.get_running_loop().time()
        if now - CONTENT_OBSERVABILITY_LAST_EMITTED < CONTENT_OBSERVABILITY_SAMPLE_SECONDS:
            return
        CONTENT_OBSERVABILITY_LAST_EMITTED = now

    snapshot = {
        "worker_id": worker_id,
        "pid": os.getpid(),
        "poll_seconds": poll_seconds,
        "queue_depth": queue_depth,
        "queue": await queue_stats(),
        "throughput": await _content_throughput_snapshot(),
        "failures": await _content_failure_snapshot(),
        "timings": await _content_timing_totals_snapshot(),
    }
    try:
        await CACHE.observability.record_worker_snapshot(
            worker_name="content",
            data=snapshot,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"Content observability emit failed for worker {worker_id}: {exc}")


async def worker_loop(
    worker_id: int,
    queue: asyncio.Queue[ContentWorkBatch],
    dispatch_event: asyncio.Event,
    *,
    poll_seconds: float = CONTENT_DISPATCH_WAKE_SECONDS,
    add_new_cache_entries: bool = False,
) -> None:
    while True:
        loop = asyncio.get_running_loop()
        start = loop.time()
        batch = await queue.get()
        dispatch_event.set()
        updates: list[ContentUpdate] = []
        try:
            await _record_content_batch_events(
                "batch_accepted",
                batch.qids,
                batch_id=batch.batch_id,
                details={
                    "batch_size": len(batch.qids),
                    "source": batch.source_label,
                },
            )
            updates, source_label, batch_staleness, batch_timings = await _process_content_batch(
                batch,
                add_new_cache_entries=add_new_cache_entries,
            )
        except Exception as exc:  # noqa: BLE001
            await _record_content_failure("worker_exceptions")
            print(f"Worker {worker_id} failed: {exc}")
            await asyncio.sleep(max(0.1, poll_seconds))
            continue
        finally:
            await _release_content_qids(batch.qids)

        if not updates:
            await _emit_content_observability(
                worker_id,
                poll_seconds=poll_seconds,
                queue_depth=queue.qsize(),
            )
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        elapsed = loop.time() - start
        throughput_text = await _record_content_throughput(len(updates))
        timing_text = await _content_timing_snapshot(batch_timings)
        staleness_text = _format_staleness_breakdown(batch_staleness)
        await _emit_content_observability(
            worker_id,
            poll_seconds=poll_seconds,
            queue_depth=queue.qsize(),
        )

        print(
            f"Worker {worker_id} processed {len(updates)} content qid(s) {batch.qids if len(batch.qids) <= 10 else ''} from {source_label} in {elapsed:.2f} seconds "
            f"({throughput_text}; {timing_text}; {staleness_text})"
        )


async def _dispatcher_loop(
    queue: asyncio.Queue[ContentWorkBatch],
    dispatch_event: asyncio.Event,
    *,
    poll_seconds: float = CONTENT_DISPATCH_WAKE_SECONDS,
) -> None:
    while True:
        wake_reason = "event"
        try:
            await asyncio.wait_for(dispatch_event.wait(), timeout=max(0.1, poll_seconds))
        except asyncio.TimeoutError:
            wake_reason = "timeout"
        dispatch_event.clear()

        queue_depth = queue.qsize()
        free_slots = queue.maxsize - queue_depth
        async with CONTENT_INFLIGHT_LOCK:
            inflight_count = len(CONTENT_INFLIGHT_QIDS)

        logger.info(
            "Content dispatcher wake: reason=%s queue_depth=%d/%d free_slots=%d inflight=%d",
            wake_reason,
            queue_depth,
            queue.maxsize,
            free_slots,
            inflight_count,
        )

        if free_slots <= 0:
            logger.info(
                "Content dispatcher idle: queue full queue_depth=%d/%d inflight=%d",
                queue_depth,
                queue.maxsize,
                inflight_count,
            )
            continue

        async with CONTENT_INFLIGHT_LOCK:
            inflight_qids = set(CONTENT_INFLIGHT_QIDS)

        batches, selection_elapsed = await _select_content_batches(
            free_slots=free_slots,
            exclude_qids=inflight_qids,
        )
        if not batches:
            logger.info(
                "Content dispatcher found no work: queue_depth=%d/%d free_slots=%d inflight=%d selection=%.2fs",
                queue_depth,
                queue.maxsize,
                free_slots,
                len(inflight_qids),
                selection_elapsed,
            )
            continue

        batch_qid_count = sum(len(batch_qids) for batch_qids in batches)
        logger.info(
            "Content dispatcher selected %d qid(s) into %d batch(es): queue_depth=%d/%d free_slots=%d inflight=%d selection=%.2fs",
            batch_qid_count,
            len(batches),
            queue_depth,
            queue.maxsize,
            free_slots,
            len(inflight_qids),
            selection_elapsed,
        )

        try:
            for batch_qids in batches:
                await _reserve_content_qids(batch_qids)
                batch = ContentWorkBatch(
                    qids=batch_qids,
                    batch_id=str(uuid4()),
                    source_label="pubsub",
                    batch_timings=_empty_content_timings(),
                )
                queue.put_nowait(batch)
                logger.info(
                    "Content dispatcher enqueued batch %s: size=%d queue_depth=%d/%d source=%s",
                    batch.batch_id,
                    len(batch.qids),
                    queue.qsize(),
                    queue.maxsize,
                    batch.source_label,
                )
                await _record_content_batch_events(
                    "batch_added",
                    batch.qids,
                    batch_id=batch.batch_id,
                    details={
                        "batch_size": len(batch.qids),
                        "source": batch.source_label,
                    },
                )
        except asyncio.QueueFull:
            await _release_content_qids([qid for batch_qids in batches for qid in batch_qids])
            raise


async def run_worker_pool(
    worker_count: int = CONTENT_WORKER_COUNT,
    poll_seconds: float = CONTENT_DISPATCH_WAKE_SECONDS,
    *,
    add_new_cache_entries: bool = False,
) -> None:
    if worker_count < 1:
        raise ValueError("worker_count must be at least 1")

    logger.info(
        "Content worker pool starting: worker_count=%d poll_seconds=%.2f queue_maxsize=%d",
        worker_count,
        poll_seconds,
        CONTENT_QUEUE_MAXSIZE,
    )
    with acquire_file_lock(WORKER_POOL_LOCK_TARGET):
        queue: asyncio.Queue[ContentWorkBatch] = asyncio.Queue(
            maxsize=CONTENT_QUEUE_MAXSIZE)
        dispatch_event = asyncio.Event()
        await asyncio.gather(
            _dispatcher_loop(
                queue,
                dispatch_event,
                poll_seconds=poll_seconds,
            ),
            *(
                worker_loop(
                    worker_id=index + 1,
                    queue=queue,
                    dispatch_event=dispatch_event,
                    poll_seconds=poll_seconds,
                    add_new_cache_entries=add_new_cache_entries,
                )
                for index in range(worker_count)
            )
        )

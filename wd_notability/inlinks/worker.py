from __future__ import annotations

import asyncio
import math
import sqlite3
import time
from collections import Counter
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from wd_notability.evaluation_cache import CACHE
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.file_lock import acquire_file_lock
from wd_notability.inlinks.source import INLINKS_SOURCE
from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel, QID

INLINKS_VISIBLE_LIMIT = 100
INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE = 200
INLINKS_LOW_PRIORITY_CONSIDER_LIMIT = 100
INLINKS_LOW_PRIORITY_MAX_IN_FLIGHT = 10
INLINKS_WORKER_RUN_INTERVAL_SECONDS = 30.0
INLINKS_WORKER_SESSION_TTL_SECONDS = 60
INLINKS_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "inlinks_worker"

INLINKS_BATCH_TARGET_SECONDS_LOW = 10.0
INLINKS_BATCH_TARGET_SECONDS_HIGH = 20.0
INLINKS_BATCH_SIZE_MIN = 50
INLINKS_BATCH_SIZE_MAX = 25_000
INLINKS_INTEREST_SESSION_ID = "inlinks"
INLINKS_INTEREST_TTL_MIN_SECONDS = 10
INLINKS_INTEREST_TTL_MAX_SECONDS = 60
INLINKS_INTEREST_EMIT_LIMIT = 1_000
INLINKS_YEAR_MICROSECONDS = 365 * 24 * 60 * 60 * 1_000_000
INLINKS_HOUR_MICROSECONDS = 60 * 60 * 1_000_000
INLINKS_OBSERVABILITY_SAMPLE_SECONDS = 60.0

INLINKS_THROUGHPUT_LOCK = asyncio.Lock()
INLINKS_THROUGHPUT_TOTAL_PROCESSED = 0
INLINKS_THROUGHPUT_STARTED_AT: float | None = None
INLINKS_OBSERVABILITY_LOCK = asyncio.Lock()
INLINKS_OBSERVABILITY_LAST_EMITTED = 0.0
INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT: dict[str, Any] | None = None
INLINKS_LAST_BATCH_TIMINGS: dict[str, float] | None = None
INLINKS_BATCH_SIZE_CURRENT = INLINKS_VISIBLE_LIMIT
INLINKS_INTEREST_TTL_SECONDS = INLINKS_WORKER_SESSION_TTL_SECONDS


@dataclass(frozen=True)
class InlinksUpdate:
    qid: QID
    n3_inlinks: NotabilityLevel


@dataclass(frozen=True)
class InlinksBatchCandidate:
    qid: QID
    creation_time_seconds: int | None
    inlinks_last_evaluated_seconds: int | None
    active_priority: int
    is_unknown: bool
    item_age_seconds: float
    age_at_last_refresh_seconds: float
    age_since_last_refresh_seconds: float
    score: float
    priority_bucket: str


def _chunked(values: list[QID], size: int) -> list[list[QID]]:
    if size < 1:
        raise ValueError("size must be at least 1")
    return [values[index : index + size] for index in range(0, len(values), size)]


def _normalize_qid_list(values: Collection[object]) -> list[QID]:
    qids: list[QID] = []
    seen: set[QID] = set()
    for value in values:
        if not isinstance(value, str) or not value.startswith("Q") or not value[1:].isdigit():
            continue
        if value in seen:
            continue
        seen.add(value)
        qids.append(value)
    return qids


def _creation_time_to_microseconds(creation_time: object) -> int | None:
    try:
        if isinstance(creation_time, bytes):
            creation_time = creation_time.decode("utf-8")
        if isinstance(creation_time, (int, float)):
            return int(float(creation_time) * 1_000_000)
        if not isinstance(creation_time, str):
            return None
        text = creation_time.strip()
        if not text:
            return None
        if text.isdigit():
            return int(text) * 1_000_000
        return int(
            datetime.strptime(text, "%Y%m%d%H%M%S")
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1_000_000
        )
    except (TypeError, ValueError, UnicodeDecodeError):
        return None


def _inlinks_refresh_score(now_microseconds: int, creation_time_seconds: int, inlinks_last_evaluated_seconds: int) -> float | None:
    creation_microseconds = creation_time_seconds * 1_000_000
    evaluation_microseconds = inlinks_last_evaluated_seconds * 1_000_000
    item_age = max(0, now_microseconds - creation_microseconds)
    age_at_last_refresh = max(0, evaluation_microseconds - creation_microseconds)
    age_since_last_refresh = max(0, now_microseconds - evaluation_microseconds)
    evaluation_age = max(INLINKS_HOUR_MICROSECONDS, min(INLINKS_YEAR_MICROSECONDS, age_at_last_refresh))
    if evaluation_age <= 0:
        return None
    _ = age_since_last_refresh
    return item_age / evaluation_age


def _priority_bucket(is_unknown: bool, active_priority: int) -> str:
    return ("unknown" if is_unknown else "refresh") + ("_active" if active_priority > 0 else "_idle")


def _priority_rank(bucket: str) -> int:
    return {
        "unknown_active": 0,
        "unknown_idle": 1,
        "refresh_active": 2,
        "refresh_idle": 3,
    }.get(bucket, 99)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def _batch_priority_snapshot_template() -> dict[str, dict[str, float | int]]:
    return {
        bucket: {
            "selected": 0,
            "processed": 0,
            "finalized": 0,
            "deferred": 0,
            "interests_emitted": 0,
            "queue_depth": 0,
            "avg_age_seconds": 0.0,
            "p95_age_seconds": 0.0,
        }
        for bucket in ("unknown_active", "unknown_idle", "refresh_active", "refresh_idle")
    }


def _build_candidate_snapshot(
    candidate_rows: list[tuple[str, int | None, int | None, int, bool]],
) -> list[InlinksBatchCandidate]:
    now_microseconds = int(time.time() * 1_000_000)
    candidates: list[InlinksBatchCandidate] = []
    for qid, creation_time_seconds, inlinks_last_evaluated_seconds, active_priority, is_unknown in candidate_rows:
        if creation_time_seconds is None:
            item_age_seconds = 0.0
            age_at_last_refresh_seconds = 0.0
            age_since_last_refresh_seconds = (
                max(0, now_microseconds - inlinks_last_evaluated_seconds * 1_000_000) / 1_000_000
                if inlinks_last_evaluated_seconds is not None
                else 0.0
            )
            score = 0.0
        else:
            evaluation_seconds = inlinks_last_evaluated_seconds
            if evaluation_seconds is None:
                evaluation_seconds = creation_time_seconds
            item_age_seconds = max(0, now_microseconds - creation_time_seconds * 1_000_000) / 1_000_000
            age_at_last_refresh_seconds = max(0, evaluation_seconds * 1_000_000 - creation_time_seconds * 1_000_000) / 1_000_000
            age_since_last_refresh_seconds = max(0, now_microseconds - evaluation_seconds * 1_000_000) / 1_000_000
            score = _inlinks_refresh_score(now_microseconds, creation_time_seconds, evaluation_seconds)
        if score is None:
            score = 0.0
        bucket = _priority_bucket(is_unknown, int(active_priority))
        candidates.append(
            InlinksBatchCandidate(
                qid=qid,
                creation_time_seconds=creation_time_seconds,
                inlinks_last_evaluated_seconds=inlinks_last_evaluated_seconds,
                active_priority=int(active_priority),
                is_unknown=bool(is_unknown),
                item_age_seconds=item_age_seconds,
                age_at_last_refresh_seconds=age_at_last_refresh_seconds,
                age_since_last_refresh_seconds=age_since_last_refresh_seconds,
                score=score,
                priority_bucket=bucket,
            )
        )
    candidates.sort(
        key=lambda candidate: (
            _priority_rank(candidate.priority_bucket),
            -candidate.score,
            -candidate.active_priority,
            -candidate.item_age_seconds,
            candidate.qid,
        )
    )
    return candidates


def _summarize_candidates(candidates: list[InlinksBatchCandidate]) -> dict[str, Any]:
    snapshot = _batch_priority_snapshot_template()
    queue_depths = Counter(candidate.priority_bucket for candidate in candidates)
    age_samples: dict[str, list[float]] = {bucket: [] for bucket in snapshot}
    for candidate in candidates:
        bucket_snapshot = snapshot[candidate.priority_bucket]
        bucket_snapshot["selected"] += 1
        bucket_snapshot["processed"] += 1
        age_samples[candidate.priority_bucket].append(candidate.item_age_seconds)

    for bucket, depth in queue_depths.items():
        snapshot[bucket]["queue_depth"] = int(depth)
        ages = age_samples[bucket]
        if ages:
            snapshot[bucket]["avg_age_seconds"] = sum(ages) / len(ages)
            p95 = _percentile(ages, 0.95)
            snapshot[bucket]["p95_age_seconds"] = 0.0 if p95 is None else p95

    return snapshot


async def _record_inlinks_throughput(processed_count: int) -> None:
    if processed_count <= 0:
        return

    global INLINKS_THROUGHPUT_TOTAL_PROCESSED
    global INLINKS_THROUGHPUT_STARTED_AT

    now = asyncio.get_running_loop().time()
    async with INLINKS_THROUGHPUT_LOCK:
        if INLINKS_THROUGHPUT_STARTED_AT is None:
            INLINKS_THROUGHPUT_STARTED_AT = now
        INLINKS_THROUGHPUT_TOTAL_PROCESSED += processed_count


async def _inlinks_throughput_snapshot() -> dict[str, float | int | None]:
    async with INLINKS_THROUGHPUT_LOCK:
        started_at = INLINKS_THROUGHPUT_STARTED_AT
        total_processed = INLINKS_THROUGHPUT_TOTAL_PROCESSED

    now = asyncio.get_running_loop().time()
    elapsed = max(0.0, now - started_at) if started_at is not None else 0.0
    rate = total_processed / elapsed if elapsed > 0 else 0.0
    return {
        "total_processed": total_processed,
        "started_at": started_at,
        "elapsed_seconds": elapsed,
        "rate_per_second": rate,
    }


async def queue_stats() -> dict[str, Any]:
    queue = await CACHE.count_inlinks_work_candidates()
    return {
        "total": queue["total"],
        "by_priority": {
            bucket: {"depth": queue[bucket]}
            for bucket in ("unknown_active", "unknown_idle", "refresh_active", "refresh_idle")
        },
    }


async def _set_last_batch_observability_snapshot(snapshot: dict[str, Any] | None) -> None:
    global INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT

    async with INLINKS_OBSERVABILITY_LOCK:
        INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT = snapshot


async def _set_last_batch_timings_snapshot(snapshot: dict[str, float] | None) -> None:
    global INLINKS_LAST_BATCH_TIMINGS

    async with INLINKS_OBSERVABILITY_LOCK:
        INLINKS_LAST_BATCH_TIMINGS = snapshot


async def _last_batch_timings_snapshot() -> dict[str, float] | None:
    async with INLINKS_OBSERVABILITY_LOCK:
        if INLINKS_LAST_BATCH_TIMINGS is None:
            return None
        return dict(INLINKS_LAST_BATCH_TIMINGS)


async def _last_batch_observability_snapshot() -> dict[str, Any] | None:
    async with INLINKS_OBSERVABILITY_LOCK:
        if INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT is None:
            return None
        return dict(INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT)


def _format_timing_breakdown(timings: dict[str, float]) -> str:
    order = (
        ("wait_foreground", "wait_foreground"),
        ("work_pass", "work_pass"),
        ("find_work", "find_work"),
        ("get_inlinks", "get_inlinks"),
        ("get_context_replica_connect", "replica_connect"),
        ("get_context_replica_query", "replica_query"),
        ("get_context_replica_fetch", "replica_fetch"),
        ("get_context_replica_normalize", "replica_normalize"),
        ("check_cache", "check_cache"),
        ("process", "process"),
        ("finalize", "finalize"),
        ("register_interest", "register_interest"),
    )
    parts = []
    for key, label in order:
        value = timings.get(key)
        if value is None:
            continue
        parts.append(f"{label}={value:.2f}s")
    return ", ".join(parts)


def _aggregate_context_timings(contexts: dict[QID, dict]) -> dict[str, float]:
    totals: dict[str, float] = {}
    seen_timings: set[int] = set()
    for context in contexts.values():
        if not isinstance(context, dict):
            continue
        timings = context.get("_timings")
        if not isinstance(timings, dict):
            continue
        timings_id = id(timings)
        if timings_id in seen_timings:
            continue
        seen_timings.add(timings_id)
        for key in (
            "get_context_query",
            "get_context_replica_connect",
            "get_context_replica_query",
            "get_context_replica_fetch",
            "get_context_replica_normalize",
            "get_context_limiter_wait",
            "get_context_retry_wait",
        ):
            value = timings.get(key)
            if isinstance(value, (int, float)):
                totals[key] = totals.get(key, 0.0) + float(value)
    return totals


async def _emit_inlinks_observability() -> None:
    global INLINKS_OBSERVABILITY_LAST_EMITTED

    async with INLINKS_OBSERVABILITY_LOCK:
        now = time.monotonic()
        if now - INLINKS_OBSERVABILITY_LAST_EMITTED < INLINKS_OBSERVABILITY_SAMPLE_SECONDS:
            return
        INLINKS_OBSERVABILITY_LAST_EMITTED = now
        batch_snapshot = INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT

    snapshot: dict[str, Any] = {
        "queue": await queue_stats(),
        "throughput": await _inlinks_throughput_snapshot(),
    }
    if batch_snapshot is not None:
        snapshot["batch"] = batch_snapshot
    try:
        await CACHE.observability.record_worker_snapshot(
            worker_name="inlinks",
            data=snapshot,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Inlinks observability emit failed: {exc}")


def _snapshot_from_context(qid: QID, context: dict[str, object]) -> tuple[list[QID], bool]:
    raw_inlinks = context.get("inlinks", [])
    if not isinstance(raw_inlinks, list):
        raw_inlinks = []

    visible_inlinks: list[QID] = []
    seen: set[QID] = set()
    for inlink in raw_inlinks:
        if not isinstance(inlink, str) or not inlink.startswith("Q") or not inlink[1:].isdigit():
            continue
        if inlink == qid or inlink in seen:
            continue
        seen.add(inlink)
        visible_inlinks.append(inlink)

    return visible_inlinks, bool(context.get("truncated", False))


def _evaluate_snapshot(
    visible_inlinks: list[QID],
    truncated: bool,
    cached_rows: dict[QID, tuple[int, int | None, int | None]],
) -> tuple[NotabilityLevel | None, list[QID]]:
    if not visible_inlinks:
        return (NotabilityLevel.NONE if not truncated else None, [])

    best_level = NotabilityLevel.NONE
    unresolved: list[QID] = []
    for inlink in visible_inlinks:
        cached_row = cached_rows.get(inlink)
        if cached_row is None:
            unresolved.append(inlink)
            continue

        cached_result = EvaluationResult.from_summary(qid=inlink, summary=cached_row[0])
        level = cached_result.n12
        if level == NotabilityLevel.UNKNOWN:
            unresolved.append(inlink)
            continue

        best_level = max(best_level, level)
        if level == NotabilityLevel.STRONG:
            return NotabilityLevel.STRONG, unresolved

    if unresolved:
        return None, unresolved

    if truncated:
        return None, []

    return best_level, []


async def _emit_dependency_interest(
    interest_qids: list[QID],
    *,
    ttl_seconds: int,
) -> int:
    if not interest_qids:
        try:
            await CACHE.pubsub.delete_pubsub_session(owner_id="inlinks", session_id=INLINKS_INTEREST_SESSION_ID)
        except Exception:
            pass
        return 0

    emitted = await CACHE.pubsub.create_pubsub_session(
        owner_id="inlinks",
        session_id=INLINKS_INTEREST_SESSION_ID,
        ttl_seconds=ttl_seconds,
        priority=1,
        wants_entitydata=True,
        wants_inlinks=False,
        wants_sync=False,
        qids=interest_qids,
    )
    return int(emitted)


async def _select_inlinks_batch(batch_size: int, *, limit: int | None = None) -> list[InlinksBatchCandidate]:
    target_size = batch_size if limit is None else min(batch_size, limit)
    if target_size < 1:
        return []

    oversample_limit = max(target_size * 4, target_size + 100)
    candidates = await CACHE.list_inlinks_work_candidates(limit=oversample_limit)
    if not candidates:
        return []

    ranked_candidates = _build_candidate_snapshot(candidates)
    return ranked_candidates[:target_size]


def _tune_batch_size(batch_size: int, elapsed_seconds: float) -> int:
    if elapsed_seconds < INLINKS_BATCH_TARGET_SECONDS_LOW:
        return min(INLINKS_BATCH_SIZE_MAX, max(batch_size + 1, int(batch_size * 1.25)))
    if elapsed_seconds > INLINKS_BATCH_TARGET_SECONDS_HIGH:
        return max(INLINKS_BATCH_SIZE_MIN, int(batch_size * 0.8))
    return batch_size


def _tune_interest_ttl(elapsed_seconds: float) -> int:
    ttl = int(max(INLINKS_INTEREST_TTL_MIN_SECONDS, min(INLINKS_INTEREST_TTL_MAX_SECONDS, elapsed_seconds * 4.0)))
    return max(INLINKS_INTEREST_TTL_MIN_SECONDS, ttl)


async def _work_inlinks_batch(
    qids: list[QID],
    *,
    cache_only: bool = False,
) -> tuple[int, int]:
    if not qids:
        await _set_last_batch_observability_snapshot(None)
        return 0, 0

    batch_started_epoch = int(time.time())
    batch_candidates = _build_candidate_snapshot([(qid, batch_started_epoch, batch_started_epoch, 0, True) for qid in qids])
    candidate_snapshot = _summarize_candidates(batch_candidates)

    contexts = await INLINKS_SOURCE.get_contexts(qids)
    visible_union: list[QID] = []
    visible_by_qid: dict[QID, list[QID]] = {}
    truncated_by_qid: dict[QID, bool] = {}
    for qid in qids:
        context = contexts.get(qid)
        if not isinstance(context, dict):
            continue
        visible_inlinks, truncated = _snapshot_from_context(qid, context)
        visible_by_qid[qid] = visible_inlinks
        truncated_by_qid[qid] = truncated
        visible_union.extend(visible_inlinks)

    cached_rows = await CACHE.get_many(_normalize_qid_list(visible_union))

    final_updates: list[InlinksUpdate] = []
    finalized_targets = 0
    deferred_targets: list[QID] = []
    interest_candidates: dict[QID, tuple[int, float, str]] = {}
    processed_targets = 0
    batch_metrics = _batch_priority_snapshot_template()

    for qid in qids:
        context = contexts.get(qid)
        candidate_bucket = "unknown_idle"
        if isinstance(context, dict):
            visible_inlinks = visible_by_qid.get(qid, [])
            truncated = truncated_by_qid.get(qid, False)
            processed_targets += 1
            candidate_bucket = "unknown_idle"
            if not visible_inlinks and not truncated:
                final_updates.append(InlinksUpdate(qid=qid, n3_inlinks=NotabilityLevel.NONE))
                batch_metrics[candidate_bucket]["finalized"] += 1
                continue
            final_level, unresolved = _evaluate_snapshot(visible_inlinks, truncated, cached_rows)
            if final_level is not None:
                final_updates.append(InlinksUpdate(qid=qid, n3_inlinks=final_level))
                batch_metrics[candidate_bucket]["finalized"] += 1
                continue

            if cache_only:
                batch_metrics[candidate_bucket]["deferred"] += 1
                continue

            deferred_targets.append(qid)
            batch_metrics[candidate_bucket]["deferred"] += 1
            for inlink_qid in unresolved:
                if inlink_qid in cached_rows:
                    continue
                current = interest_candidates.get(inlink_qid)
                target_score = 0.0
                if current is None or target_score > current[1]:
                    interest_candidates[inlink_qid] = (_priority_rank(candidate_bucket), target_score)

    if final_updates:
        changed = await CACHE.upsert_inlinks_many(final_updates)
        finalized_targets += len(changed)

    deferred_refresh_targets = deferred_targets if deferred_targets else []
    if deferred_refresh_targets:
        await CACHE.touch_inlinks_last_evaluated_many(
            deferred_refresh_targets,
            inlinks_last_evaluated=batch_started_epoch,
        )

    interest_qids = [
        qid
        for qid, _score in sorted(
            interest_candidates.items(),
            key=lambda item: (item[1][0], item[0]),
        )[:INLINKS_INTEREST_EMIT_LIMIT]
    ]
    emitted_interest_rows = await _emit_dependency_interest(
        interest_qids,
        ttl_seconds=INLINKS_INTEREST_TTL_SECONDS,
    )

    if batch_candidates:
        observed = candidate_snapshot
        observed["selected"] = processed_targets
        observed["processed"] = processed_targets
        observed["finalized"] = finalized_targets
        observed["deferred"] = len(deferred_targets)
        observed["interests_emitted"] = emitted_interest_rows
        await _set_last_batch_observability_snapshot(
            {
                "selected": processed_targets,
                "processed": processed_targets,
                "finalized": finalized_targets,
                "deferred": len(deferred_targets),
                "interests_emitted": emitted_interest_rows,
                "by_priority": observed,
            }
        )
    else:
        await _set_last_batch_observability_snapshot(None)

    return processed_targets, finalized_targets


async def work_inlinks_pass(batch_size: int = INLINKS_VISIBLE_LIMIT, limit: int | None = None) -> int:
    global INLINKS_INTEREST_TTL_SECONDS

    effective_batch_size = batch_size if limit is None else min(batch_size, limit)
    if effective_batch_size < 1:
        await _set_last_batch_observability_snapshot(None)
        await _set_last_batch_timings_snapshot(
            {
                "wait_foreground": 0.0,
                "find_work": 0.0,
                "get_inlinks": 0.0,
                "check_cache": 0.0,
                "process": 0.0,
                "finalize": 0.0,
                "register_interest": 0.0,
                "work_pass": 0.0,
            }
        )
        return 0

    pass_started = time.perf_counter()
    find_work_started = time.perf_counter()
    selected_candidates = await _select_inlinks_batch(effective_batch_size, limit=limit)
    find_work_elapsed = max(0.0, time.perf_counter() - find_work_started)
    if not selected_candidates:
        await _set_last_batch_observability_snapshot(
            {
                "selected": 0,
                "processed": 0,
                "finalized": 0,
                "deferred": 0,
                "interests_emitted": 0,
                "by_priority": _batch_priority_snapshot_template(),
            }
        )
        await _set_last_batch_timings_snapshot(
            {
                "wait_foreground": 0.0,
                "find_work": find_work_elapsed,
                "get_inlinks": 0.0,
                "check_cache": 0.0,
                "process": 0.0,
                "finalize": 0.0,
                "register_interest": 0.0,
                "work_pass": max(0.0, time.perf_counter() - pass_started),
            }
        )
        return 0

    qids = [candidate.qid for candidate in selected_candidates]
    batch_candidate_snapshot = _summarize_candidates(selected_candidates)
    get_inlinks_started = time.perf_counter()
    contexts = await INLINKS_SOURCE.get_contexts(qids)
    get_inlinks_elapsed = max(0.0, time.perf_counter() - get_inlinks_started)
    source_timings = _aggregate_context_timings(contexts)

    visible_union: list[QID] = []
    visible_by_qid: dict[QID, list[QID]] = {}
    truncated_by_qid: dict[QID, bool] = {}
    for candidate in selected_candidates:
        context = contexts.get(candidate.qid)
        if not isinstance(context, dict):
            continue
        visible_inlinks, truncated = _snapshot_from_context(candidate.qid, context)
        visible_by_qid[candidate.qid] = visible_inlinks
        truncated_by_qid[candidate.qid] = truncated
        visible_union.extend(visible_inlinks)
    distinct_inlinks_found = len(set(visible_union))
    truncated_targets = sum(1 for value in truncated_by_qid.values() if value)

    check_cache_started = time.perf_counter()
    cached_rows = await CACHE.get_many(_normalize_qid_list(visible_union))
    check_cache_elapsed = max(0.0, time.perf_counter() - check_cache_started)

    final_updates: list[InlinksUpdate] = []
    deferred_targets: list[QID] = []
    interest_candidates: dict[QID, tuple[int, float]] = {}
    batch_metrics = _batch_priority_snapshot_template()
    processed_targets = 0
    finalized_targets = 0
    interest_rows_emitted = 0

    process_started = time.perf_counter()
    for candidate in selected_candidates:
        bucket = candidate.priority_bucket
        batch_metrics[bucket]["selected"] += 1
        context = contexts.get(candidate.qid)
        if not isinstance(context, dict):
            continue

        processed_targets += 1
        batch_metrics[bucket]["processed"] += 1

        visible_inlinks = visible_by_qid.get(candidate.qid, [])
        truncated = truncated_by_qid.get(candidate.qid, False)
        if not visible_inlinks and not truncated:
            final_updates.append(InlinksUpdate(qid=candidate.qid, n3_inlinks=NotabilityLevel.NONE))
            batch_metrics[bucket]["finalized"] += 1
            finalized_targets += 1
            continue

        final_level, unresolved = _evaluate_snapshot(visible_inlinks, truncated, cached_rows)
        if final_level is not None:
            final_updates.append(InlinksUpdate(qid=candidate.qid, n3_inlinks=final_level))
            batch_metrics[bucket]["finalized"] += 1
            finalized_targets += 1
            continue

        deferred_targets.append(candidate.qid)
        batch_metrics[bucket]["deferred"] += 1
        for inlink_qid in unresolved:
            current = interest_candidates.get(inlink_qid)
            candidate_key = (_priority_rank(bucket), -candidate.score, bucket)
            if current is None or candidate_key < current:
                interest_candidates[inlink_qid] = candidate_key

        if candidate.item_age_seconds > 0:
            batch_metrics[bucket]["avg_age_seconds"] += candidate.item_age_seconds
    process_elapsed = max(0.0, time.perf_counter() - process_started)
    distinct_unknown_inlinks = len(interest_candidates)

    finalize_started = time.perf_counter()
    if final_updates:
        changed = await CACHE.upsert_inlinks_many(final_updates)
        finalized_targets = len(changed)

    if deferred_targets:
        await CACHE.touch_inlinks_last_evaluated_many(
            deferred_targets,
            inlinks_last_evaluated=int(time.time()),
        )
    finalize_elapsed = max(0.0, time.perf_counter() - finalize_started)

    register_interest_started = time.perf_counter()
    interest_qids = [
        qid
        for qid, _score in sorted(
            interest_candidates.items(),
            key=lambda item: (item[1][0], item[1][1], item[0]),
        )[:INLINKS_INTEREST_EMIT_LIMIT]
    ]
    distinct_interest_qids = len(interest_qids)
    interest_rows_emitted = await _emit_dependency_interest(
        interest_qids,
        ttl_seconds=INLINKS_INTEREST_TTL_SECONDS,
    )
    register_interest_elapsed = max(0.0, time.perf_counter() - register_interest_started)

    for qid in interest_qids:
        source = interest_candidates.get(qid)
        if source is None:
            continue
        batch_metrics[source[2]]["interests_emitted"] += 1

    for bucket in batch_candidate_snapshot:
        ages = [candidate.item_age_seconds for candidate in selected_candidates if candidate.priority_bucket == bucket]
        if ages:
            batch_candidate_snapshot[bucket]["avg_age_seconds"] = sum(ages) / len(ages)
            p95 = _percentile(ages, 0.95)
            batch_candidate_snapshot[bucket]["p95_age_seconds"] = 0.0 if p95 is None else p95

    batch_metrics_snapshot = {
        "selected": len(selected_candidates),
        "processed": processed_targets,
        "finalized": finalized_targets,
        "deferred": len(deferred_targets),
        "distinct_inlinks_found": distinct_inlinks_found,
        "truncated_targets": truncated_targets,
        "distinct_unknown_inlinks": distinct_unknown_inlinks,
        "distinct_interest_qids": distinct_interest_qids,
        "interests_emitted": interest_rows_emitted,
        "by_priority": {
            bucket: {
                **batch_candidate_snapshot[bucket],
                **batch_metrics[bucket],
            }
            for bucket in batch_candidate_snapshot
        },
    }
    await _set_last_batch_observability_snapshot(batch_metrics_snapshot)

    batch_elapsed = max(0.0, time.perf_counter() - pass_started)
    INLINKS_INTEREST_TTL_SECONDS = _tune_interest_ttl(batch_elapsed)
    await _set_last_batch_timings_snapshot(
        {
            "wait_foreground": 0.0,
            "find_work": find_work_elapsed,
            "get_inlinks": get_inlinks_elapsed,
            "check_cache": check_cache_elapsed,
            "process": process_elapsed,
            "finalize": finalize_elapsed,
            "register_interest": register_interest_elapsed,
            "work_pass": batch_elapsed,
            **source_timings,
        }
    )

    return processed_targets


async def inlinks_worker_loop(
    *,
    batch_size: int = INLINKS_VISIBLE_LIMIT,
    run_interval_seconds: float = INLINKS_WORKER_RUN_INTERVAL_SECONDS,
) -> None:
    global INLINKS_BATCH_SIZE_CURRENT

    print("Inlinks worker starting; acquiring worker lock...")
    with acquire_file_lock(INLINKS_WORKER_LOCK_TARGET):
        print("Inlinks worker acquired lock; waiting for foreground evaluations...")
        INLINKS_BATCH_SIZE_CURRENT = max(1, batch_size)
        while True:
            run_started = time.monotonic()
            wait_started = time.perf_counter()
            try:
                await wait_for_foreground_evaluations()
                wait_elapsed = max(0.0, time.perf_counter() - wait_started)
                print("Inlinks worker foreground wait complete; running pass...")
                processed = await work_inlinks_pass(batch_size=INLINKS_BATCH_SIZE_CURRENT)
                elapsed = time.monotonic() - run_started
                await _record_inlinks_throughput(processed)
                INLINKS_BATCH_SIZE_CURRENT = _tune_batch_size(INLINKS_BATCH_SIZE_CURRENT, elapsed)
                batch_snapshot = await _last_batch_observability_snapshot()
                finalized = 0
                distinct_inlinks_found = 0
                truncated_targets = 0
                distinct_unknown_inlinks = 0
                distinct_interest_qids = 0
                if isinstance(batch_snapshot, dict):
                    finalized_value = batch_snapshot.get("finalized")
                    if isinstance(finalized_value, int):
                        finalized = finalized_value
                    inlinks_value = batch_snapshot.get("distinct_inlinks_found")
                    if isinstance(inlinks_value, int):
                        distinct_inlinks_found = inlinks_value
                    truncated_value = batch_snapshot.get("truncated_targets")
                    if isinstance(truncated_value, int):
                        truncated_targets = truncated_value
                    unknown_value = batch_snapshot.get("distinct_unknown_inlinks")
                    if isinstance(unknown_value, int):
                        distinct_unknown_inlinks = unknown_value
                    interest_value = batch_snapshot.get("distinct_interest_qids")
                    if isinstance(interest_value, int):
                        distinct_interest_qids = interest_value
                timings = await _last_batch_timings_snapshot()
                timing_text = ""
                if timings is not None:
                    combined_timings = dict(timings)
                    combined_timings["wait_foreground"] = wait_elapsed
                    timing_text = f", {_format_timing_breakdown(combined_timings)}"
                print(
                    f"Inlinks worker processed {processed} candidate qid(s), finalized {finalized}, "
                    f"distinct_inlinks={distinct_inlinks_found}, truncated_targets={truncated_targets}, "
                    f"distinct_unknown_inlinks={distinct_unknown_inlinks}, "
                    f"interest_qids={distinct_interest_qids}, in {elapsed:.2f}s "
                    f"(batch_size={INLINKS_BATCH_SIZE_CURRENT}, ttl={INLINKS_INTEREST_TTL_SECONDS}s{timing_text})"
                )
                await _emit_inlinks_observability()
            except Exception as exc:  # noqa: BLE001
                print(f"Inlinks worker failed: {exc}")

            sleep_for = max(0.0, run_interval_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)


async def evaluate_inlinks_many(qids: Collection[QID], *, cache_only: bool = True) -> tuple[int, int]:
    qid_list = _normalize_qid_list(qids)
    if not qid_list:
        return 0, 0
    return await _work_inlinks_batch(qid_list, cache_only=cache_only)

from __future__ import annotations

import asyncio
import contextvars
import os
from dataclasses import dataclass
from collections import deque
from collections.abc import Collection, Sequence
from pathlib import Path
import time

from wd_notability.evaluation_cache import CACHE
from wd_notability.api_backoff import get_retry_after_remaining
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.file_lock import acquire_file_lock
from wd_notability.inlinks.cache import upsert_inlinks_strong_many
from wd_notability.models import EvaluationResult, NotabilityLevel, QID
from wd_notability.content.fetcher import ENTITY_DATA_SOURCE
from wd_notability.wikidata import EntityDeletedError
from wd_notability.wikidata_api import WikidataBackoffActiveError, WikidataRetryAfterError

DEFAULT_BATCH_SIZE = 500
ENTITYDATA_EVALUATION_CHUNK_SIZE = DEFAULT_BATCH_SIZE
ENTITYDATA_INFLIGHT_QIDS: set[QID] = set()
ENTITYDATA_INFLIGHT_LOCK = asyncio.Lock()
WORKER_POOL_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "worker_pool"
ENTITYDATA_THROUGHPUT_SAMPLE_WINDOW = 10
ENTITYDATA_THROUGHPUT_LOCK = asyncio.Lock()
ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED = 0
ENTITYDATA_THROUGHPUT_STARTED_AT: float | None = None
ENTITYDATA_THROUGHPUT_RECENT_BATCHES: deque[tuple[float, int]] = deque(maxlen=ENTITYDATA_THROUGHPUT_SAMPLE_WINDOW)
ENTITYDATA_OBSERVABILITY_SAMPLE_SECONDS = 60.0
ENTITYDATA_OBSERVABILITY_LOCK = asyncio.Lock()
ENTITYDATA_OBSERVABILITY_LAST_EMITTED: dict[int, float] = {}
ENTITYDATA_FAILURE_LOCK = asyncio.Lock()
ENTITYDATA_FAILURE_TOTALS = {
    "context_errors": 0,
    "missing_lastrevid": 0,
    "unknown_live_result": 0,
    "validation_rejected": 0,
    "worker_exceptions": 0,
}
ENTITYDATA_TIMING_LOCK = asyncio.Lock()
ENTITYDATA_TIMING_TOTALS = {
    "selection": 0.0,
    "fetch_contexts": 0.0,
    "detector_sitelinks": 0.0,
    "detector_identifiers": 0.0,
    "detector_sources": 0.0,
    "evaluate": 0.0,
    "upsert": 0.0,
    "verify": 0.0,
    "wait_foreground": 0.0,
    "release": 0.0,
    "other": 0.0,
}
ENTITYDATA_EVALUATE_FETCH_SECONDS = contextvars.ContextVar("ENTITYDATA_EVALUATE_FETCH_SECONDS", default=0.0)
ENTITYDATA_EVALUATE_DETECTOR_SECONDS = contextvars.ContextVar(
    "ENTITYDATA_EVALUATE_DETECTOR_SECONDS",
    default={
        "sitelinks": 0.0,
        "identifiers": 0.0,
        "sources": 0.0,
    },
)


def _entitydata_recent_throughput_rate() -> float:
    if len(ENTITYDATA_THROUGHPUT_RECENT_BATCHES) < 2:
        return 0.0

    first_timestamp = ENTITYDATA_THROUGHPUT_RECENT_BATCHES[0][0]
    last_timestamp = ENTITYDATA_THROUGHPUT_RECENT_BATCHES[-1][0]
    elapsed = last_timestamp - first_timestamp
    if elapsed <= 0:
        return 0.0

    processed = sum(batch_size for _timestamp, batch_size in ENTITYDATA_THROUGHPUT_RECENT_BATCHES)
    return processed / elapsed


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


ENTITYDATA_VERIFY_COMPLETED_BATCHES = _env_flag(
    "WD_NOTABILITY_ENTITYDATA_VERIFY_COMPLETED_BATCHES",
    default=False,
)


@dataclass(frozen=True)
class EntityDataUpdate:
    qid: QID
    is_redirect: bool
    has_claims: bool
    has_sitelinks: bool
    is_deleted: bool
    n1: NotabilityLevel
    n2a: NotabilityLevel
    n2b: NotabilityLevel
    entitydata_last_revid: int | None


def _entitydata_update_from_result(result: EvaluationResult) -> EntityDataUpdate:
    return EntityDataUpdate(
        qid=result.qid,
        is_redirect=result.is_redirect,
        has_claims=result.has_claims,
        has_sitelinks=result.has_sitelinks,
        is_deleted=result.is_deleted,
        n1=result.n1,
        n2a=result.n2a,
        n2b=result.n2b,
        entitydata_last_revid=result.entitydata_last_revid,
    )


def _format_entitydata_problem(result: EvaluationResult, context: object) -> str:
    context_type = type(context).__name__
    errors = {
        key: value
        for key, value in result.errors.items()
        if value
    }
    return (
        f"deleted={result.is_deleted}, "
        f"revid={result.entitydata_last_revid}, "
        f"n1={result.n1}, n2a={result.n2a}, n2b={result.n2b}, "
        f"context_type={context_type}, errors={errors}"
    )


def _format_entitydata_update_problem(update: EntityDataUpdate) -> str:
    return (
        f"deleted={update.is_deleted}, "
        f"revid={update.entitydata_last_revid}, "
        f"n1={update.n1}, n2a={update.n2a}, n2b={update.n2b}"
    )


def _empty_entitydata_timings() -> dict[str, float]:
    return {
        "selection": 0.0,
        "fetch_contexts": 0.0,
        "detector_sitelinks": 0.0,
        "detector_identifiers": 0.0,
        "detector_sources": 0.0,
        "evaluate": 0.0,
        "upsert": 0.0,
        "verify": 0.0,
        "wait_foreground": 0.0,
        "release": 0.0,
        "other": 0.0,
    }


async def _record_entitydata_timings(timings: dict[str, float]) -> None:
    async with ENTITYDATA_TIMING_LOCK:
        for key in ENTITYDATA_TIMING_TOTALS:
            ENTITYDATA_TIMING_TOTALS[key] += float(timings.get(key, 0.0))


async def _entitydata_timing_snapshot() -> str:
    async with ENTITYDATA_TIMING_LOCK:
        totals = dict(ENTITYDATA_TIMING_TOTALS)

    total_seconds = sum(totals.values())
    if total_seconds <= 0:
        return "global content timings: no accumulated time"

    return (
        f"global content timings: total={total_seconds:.2f}s, "
        f"selection={totals['selection']:.2f}s, "
        f"fetch_contexts={totals['fetch_contexts']:.2f}s, "
        f"detector_sitelinks={totals['detector_sitelinks']:.2f}s, "
        f"detector_identifiers={totals['detector_identifiers']:.2f}s, "
        f"detector_sources={totals['detector_sources']:.2f}s, "
        f"evaluate={totals['evaluate']:.2f}s, "
        f"upsert={totals['upsert']:.2f}s, "
        f"verify={totals['verify']:.2f}s, "
        f"wait_foreground={totals['wait_foreground']:.2f}s, "
        f"release={totals['release']:.2f}s, "
        f"other={totals['other']:.2f}s"
    )


async def _record_entitydata_failure(kind: str, count: int = 1) -> None:
    if count <= 0:
        return
    async with ENTITYDATA_FAILURE_LOCK:
        if kind not in ENTITYDATA_FAILURE_TOTALS:
            ENTITYDATA_FAILURE_TOTALS[kind] = 0
        ENTITYDATA_FAILURE_TOTALS[kind] += count


async def _entitydata_failure_snapshot() -> dict[str, int]:
    async with ENTITYDATA_FAILURE_LOCK:
        return dict(ENTITYDATA_FAILURE_TOTALS)


async def find_entitydata_qids(batch_size: int, *, allow_uninterested: bool = False) -> set[QID]:
    if batch_size < 1:
        return set()

    if (retry_remaining := get_retry_after_remaining()) > 0:
        print(f"EntityData worker abandoned queue selection due to shared Wikidata backoff ({retry_remaining} seconds remaining)")
        return set()

    async with ENTITYDATA_INFLIGHT_LOCK:
        inflight_count = len(ENTITYDATA_INFLIGHT_QIDS)

    pubsub_candidates = await CACHE.pubsub.list_pubsub_entitydata_candidates(
        limit=batch_size + inflight_count,
        allow_uninterested=allow_uninterested,
    )
    candidates = list(dict.fromkeys(pubsub_candidates))
    if not candidates:
        return set()

    claimed: list[QID] = []
    async with ENTITYDATA_INFLIGHT_LOCK:
        for qid in candidates:
            if qid in ENTITYDATA_INFLIGHT_QIDS:
                continue
            ENTITYDATA_INFLIGHT_QIDS.add(qid)
            claimed.append(qid)
            if len(claimed) >= batch_size:
                break
    return set(claimed)


async def _claim_entitydata_qids(candidates: list[QID], batch_size: int) -> list[QID]:
    if batch_size < 1 or not candidates:
        return []

    claimed: list[QID] = []
    async with ENTITYDATA_INFLIGHT_LOCK:
        for qid in candidates:
            if qid in ENTITYDATA_INFLIGHT_QIDS:
                continue
            ENTITYDATA_INFLIGHT_QIDS.add(qid)
            claimed.append(qid)
            if len(claimed) >= batch_size:
                break
    return claimed


async def queue_stats() -> dict[str, int | None]:
    pubsub_candidates = await CACHE.pubsub.count_pubsub_entitydata_candidates()
    async with ENTITYDATA_INFLIGHT_LOCK:
        inflight_count = len(ENTITYDATA_INFLIGHT_QIDS)
    return {
        "pubsub": pubsub_candidates,
        "total": pubsub_candidates,
        "in_flight": inflight_count,
    }


async def evaluate_entitydata_many(qids: Collection[QID]) -> tuple[list[EntityDataUpdate], set[QID]]:
    qid_list = sorted(str(qid) for qid in qids)
    if not qid_list:
        print("evaluate_entitydata_many called with no qids")
        return [], set()

    fetch_started = time.perf_counter()
    contexts = await ENTITY_DATA_SOURCE.get_contexts(qid_list)
    ENTITYDATA_EVALUATE_FETCH_SECONDS.set(max(0.0, time.perf_counter() - fetch_started))
    detector_totals = {
        "sitelinks": 0.0,
        "identifiers": 0.0,
        "sources": 0.0,
    }
    ENTITYDATA_EVALUATE_DETECTOR_SECONDS.set(detector_totals)
    updates: list[EntityDataUpdate] = []
    outlinks: set[QID] = set()

    for qid in qid_list:
        context = contexts.get(qid)
        if context is None:
            context = KeyError(f"Source {ENTITY_DATA_SOURCE.name} did not return context for {qid}")

        if isinstance(context, EntityDeletedError):
            result = EvaluationResult(qid=qid, is_deleted=True)
        elif isinstance(context, Exception):
            await _record_entitydata_failure("context_errors")
            result = EvaluationResult(qid=qid)
            for detector in ENTITY_DATA_SOURCE.detectors:
                result.add_error(detector, context)
        else:
            result = await ENTITY_DATA_SOURCE._run_context_core(qid, context)
            source_timings = result.source_timings
            for key in detector_totals:
                detector_totals[key] += max(0.0, float(source_timings.get(f"detector_{key}", 0.0)))
        if result.entitydata_last_revid is None and not result.is_deleted:
            await _record_entitydata_failure("missing_lastrevid")
            print(
                f"EntityData worker missing lastrevid for {qid}; "
                f"{_format_entitydata_problem(result, context)}"
            )
            continue

        if not result.is_deleted and (
            result.n1 == NotabilityLevel.UNKNOWN
            or result.n2a == NotabilityLevel.UNKNOWN
            or result.n2b == NotabilityLevel.UNKNOWN
        ):
            await _record_entitydata_failure("unknown_live_result")
            print(
                f"EntityData worker produced unknown live result for {qid}; "
                f"{_format_entitydata_problem(result, context)}"
            )
            continue

        updates.append(_entitydata_update_from_result(result))
        if result.n12 == NotabilityLevel.STRONG and isinstance(context, dict):
            raw_outlinks = context.get("outlinks", [])
            if isinstance(raw_outlinks, list):
                outlinks.update(outlink for outlink in raw_outlinks if isinstance(outlink, str))

    # print(f"evaluate_entitydata_many: qids={len(qid_list)}, contexts={len(contexts)}, updates={len(updates)}")
    ENTITYDATA_EVALUATE_DETECTOR_SECONDS.set(detector_totals)
    return updates, outlinks


async def upsert_entitydata_updates(updates: Sequence[EntityDataUpdate]) -> list[tuple[QID, int]]:
    if not updates:
        return []

    invalid_qids: list[str] = []
    for update in updates:
        if update.entitydata_last_revid is None and not update.is_deleted:
            invalid_qids.append(str(update.qid))
            continue
        if not update.is_deleted and (
            update.n1 == NotabilityLevel.UNKNOWN
            or update.n2a == NotabilityLevel.UNKNOWN
            or update.n2b == NotabilityLevel.UNKNOWN
        ):
            invalid_qids.append(str(update.qid))

    if invalid_qids:
        await _record_entitydata_failure("validation_rejected", len(invalid_qids))
        invalid_details = [
            f"{update.qid}: {_format_entitydata_update_problem(update)}"
            for update in updates
            if update.qid in {qid for qid in invalid_qids}
        ]
        print(f"EntityData worker refusing to upsert incomplete batch: {', '.join(invalid_details)}")
        return []

    changed = await CACHE.upsert_entitydata_many(updates)
    return changed


async def _debug_verify_completed_entitydata_batch(qids: Sequence[QID]) -> None:
    if not ENTITYDATA_VERIFY_COMPLETED_BATCHES or not qids:
        return

    rows = await CACHE.get_many(list(qids))
    stale_qids: list[str] = []
    for qid in qids:
        row = rows.get(str(qid))
        if row is None:
            stale_qids.append(str(qid))
            continue
        _summary, entitydata_last_revid, recent_changes_last_revid = row
        if entitydata_last_revid is None or recent_changes_last_revid is None or entitydata_last_revid < recent_changes_last_revid:
            stale_qids.append(str(qid))

    if stale_qids:
        print(
            f"EntityData debug verification found {len(stale_qids)} stale qids after upsert: "
            f"{stale_qids}"
        )
    else:
        print(f"EntityData debug verification passed for {len(qids)} qid(s)")


async def _persist_entitydata_chunk(
    chunk_updates: Sequence[EntityDataUpdate],
    batch_timings: dict[str, float],
) -> list[tuple[QID, int]]:
    if not chunk_updates:
        return []

    upsert_started = time.perf_counter()
    changed = await upsert_entitydata_updates(chunk_updates)
    batch_timings["upsert"] += max(0.0, time.perf_counter() - upsert_started)

    await _debug_verify_completed_entitydata_batch([update.qid for update in chunk_updates])
    return changed


async def work_entitydata_pubsub_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    *,
    allow_uninterested: bool = False,
) -> tuple[list[EntityDataUpdate], str]:
    batch_started = time.perf_counter()
    batch_started_epoch = int(time.time())
    updates: list[EntityDataUpdate] = []
    claimed_qids: list[QID] = []
    source_labels: list[str] = []
    batch_outlinks: set[QID] = set()
    batch_timings = _empty_entitydata_timings()
    if (retry_remaining := get_retry_after_remaining()) > 0:
        print(
            "EntityData worker abandoned batch selection due to shared Wikidata backoff "
            f"({retry_remaining} seconds remaining)"
        )
        return [], "shared Wikidata backoff"

    selection_started = time.perf_counter()
    qids = await find_entitydata_qids(batch_size, allow_uninterested=allow_uninterested)
    batch_timings["selection"] += max(0.0, time.perf_counter() - selection_started)
    if not qids:
        batch_elapsed = max(0.0, time.perf_counter() - batch_started)
        batch_timings["other"] += batch_elapsed
        await _record_entitydata_timings(batch_timings)
        return updates, " and ".join(dict.fromkeys(source_labels)) or "unknown"

    qid_list = sorted(qids)
    claimed_qids.extend(qid_list)
    if qid_list:
        source_labels.append("pubsub")
    try:
        for start in range(0, len(qid_list), ENTITYDATA_EVALUATION_CHUNK_SIZE):
            chunk = qid_list[start : start + ENTITYDATA_EVALUATION_CHUNK_SIZE]
            if (retry_remaining := get_retry_after_remaining()) > 0:
                print(
                    "EntityData worker paused before starting a new chunk due to "
                    f"shared Wikidata backoff ({retry_remaining} seconds remaining)"
                )
                return updates
            try:
                evaluate_started = time.perf_counter()
                chunk_updates, chunk_outlinks = await evaluate_entitydata_many(chunk)
                evaluate_elapsed = max(0.0, time.perf_counter() - evaluate_started)
                fetch_elapsed = max(0.0, ENTITYDATA_EVALUATE_FETCH_SECONDS.get())
                detector_timings = ENTITYDATA_EVALUATE_DETECTOR_SECONDS.get()
                batch_timings["fetch_contexts"] += fetch_elapsed
                detector_elapsed = 0.0
                for key in ("sitelinks", "identifiers", "sources"):
                    detector_value = max(0.0, float(detector_timings.get(key, 0.0)))
                    batch_timings[f"detector_{key}"] += detector_value
                    detector_elapsed += detector_value
                batch_timings["evaluate"] += max(0.0, evaluate_elapsed - fetch_elapsed - detector_elapsed)
            except (WikidataBackoffActiveError, WikidataRetryAfterError):
                print(f"EntityData worker paused remaining qids after Wikidata backoff in chunk {chunk}")
                return updates

            if not chunk_updates:
                print(f"EntityData worker found no updates for chunk of {len(chunk)} qids")
                continue

            changed = await _persist_entitydata_chunk(chunk_updates, batch_timings)
            updates.extend(chunk_updates)
            batch_outlinks.update(chunk_outlinks)
    finally:
        release_started = time.perf_counter()
        await _release_entitydata_batch(claimed_qids)
        batch_timings["release"] += max(0.0, time.perf_counter() - release_started)
        if batch_outlinks:
            await upsert_inlinks_strong_many(
                CACHE,
                sorted(batch_outlinks),
                inlinks_last_evaluated=batch_started_epoch,
            )

    batch_elapsed = max(0.0, time.perf_counter() - batch_started)
    named_total = sum(batch_timings.values())
    batch_timings["other"] += max(0.0, batch_elapsed - named_total)
    await _record_entitydata_timings(batch_timings)

    return updates, " and ".join(dict.fromkeys(source_labels)) or "unknown"


async def work_entitydata_batch(
    batch_size: int = DEFAULT_BATCH_SIZE,
    *,
    allow_uninterested: bool = False,
) -> tuple[list[EntityDataUpdate], str]:
    return await work_entitydata_pubsub_batch(batch_size=batch_size, allow_uninterested=allow_uninterested)


async def work_queued_items(
    *,
    limit: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
    allow_uninterested: bool = False,
) -> int:
    processed = 0
    while limit <= 0 or processed < limit:
        current_batch_size = batch_size if limit <= 0 else min(batch_size, limit - processed)
        batch, _source_label = await work_entitydata_batch(
            batch_size=current_batch_size,
            allow_uninterested=allow_uninterested,
        )
        if not batch:
            break
        processed += len(batch)
    return processed


async def _release_entitydata_batch(qids: list[QID]) -> None:
    async with ENTITYDATA_INFLIGHT_LOCK:
        for qid in qids:
            ENTITYDATA_INFLIGHT_QIDS.discard(qid)


async def _record_entitydata_throughput(batch_size: int) -> str:
    global ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED
    global ENTITYDATA_THROUGHPUT_STARTED_AT

    now = asyncio.get_running_loop().time()
    async with ENTITYDATA_THROUGHPUT_LOCK:
        if ENTITYDATA_THROUGHPUT_STARTED_AT is None:
            ENTITYDATA_THROUGHPUT_STARTED_AT = now

        ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED += batch_size
        ENTITYDATA_THROUGHPUT_RECENT_BATCHES.append((now, batch_size))
        total_rate = _entitydata_recent_throughput_rate()
    if total_rate > 0:
        return f"throughput={total_rate:.2f} qid/s"
    return "throughput=unknown"


async def _entitydata_throughput_snapshot() -> dict[str, float | int | None]:
    async with ENTITYDATA_THROUGHPUT_LOCK:
        started_at = ENTITYDATA_THROUGHPUT_STARTED_AT
        total_processed = ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED
        recent_rate = _entitydata_recent_throughput_rate()

    now = asyncio.get_running_loop().time()
    elapsed = max(0.0, now - started_at) if started_at is not None else 0.0
    return {
        "total_processed": total_processed,
        "started_at": started_at,
        "elapsed_seconds": elapsed,
        "rate_per_second": recent_rate,
    }


async def _entitydata_timing_totals_snapshot() -> dict[str, float]:
    async with ENTITYDATA_TIMING_LOCK:
        return dict(ENTITYDATA_TIMING_TOTALS)


async def _emit_entitydata_observability(worker_id: int, *, poll_seconds: float) -> None:
    async with ENTITYDATA_OBSERVABILITY_LOCK:
        last_emitted = ENTITYDATA_OBSERVABILITY_LAST_EMITTED.get(worker_id, 0.0)
        now = asyncio.get_running_loop().time()
        if now - last_emitted < ENTITYDATA_OBSERVABILITY_SAMPLE_SECONDS:
            return
        ENTITYDATA_OBSERVABILITY_LAST_EMITTED[worker_id] = now

    snapshot = {
        "worker_id": worker_id,
        "pid": os.getpid(),
        "poll_seconds": poll_seconds,
        "queue": await queue_stats(),
        "throughput": await _entitydata_throughput_snapshot(),
        "failures": await _entitydata_failure_snapshot(),
        "timings": await _entitydata_timing_totals_snapshot(),
    }
    try:
        await CACHE.observability.record_worker_snapshot(
            worker_name="content",
            data=snapshot,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"EntityData observability emit failed for worker {worker_id}: {exc}")


async def worker_loop(worker_id: int, poll_seconds: float = 5.0, *, allow_uninterested: bool = False) -> None:
    while True:
        loop = asyncio.get_running_loop()
        start = loop.time()
        try:
            wait_started = loop.time()
            await wait_for_foreground_evaluations()
            wait_elapsed = max(0.0, loop.time() - wait_started)
            await _record_entitydata_timings({"wait_foreground": wait_elapsed})
            batch, source_label = await work_entitydata_pubsub_batch(
                batch_size=DEFAULT_BATCH_SIZE,
                allow_uninterested=allow_uninterested,
            )
        except Exception as exc:  # noqa: BLE001
            await _record_entitydata_failure("worker_exceptions")
            print(f"Worker {worker_id} failed: {exc}")
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        if not batch:
            await _emit_entitydata_observability(worker_id, poll_seconds=poll_seconds)
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        elapsed = loop.time() - start
        throughput_text = await _record_entitydata_throughput(len(batch))
        timing_text = await _entitydata_timing_snapshot()
        await _emit_entitydata_observability(worker_id, poll_seconds=poll_seconds)

        print(
            f"Worker {worker_id} processed {len(batch)} content qid(s) from {source_label} in {elapsed:.2f} seconds "
            f"({throughput_text}; {timing_text})"
        )


async def run_worker_pool(
    worker_count: int = 1,
    poll_seconds: float = 5.0,
    *,
    allow_uninterested: bool = False,
) -> None:
    if worker_count < 1:
        raise ValueError("worker_count must be at least 1")

    with acquire_file_lock(WORKER_POOL_LOCK_TARGET):
        await asyncio.gather(
            *(
                worker_loop(
                    worker_id=index + 1,
                    poll_seconds=poll_seconds,
                    allow_uninterested=allow_uninterested,
                )
                for index in range(worker_count)
            )
        )

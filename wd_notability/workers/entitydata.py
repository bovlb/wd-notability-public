from __future__ import annotations

import asyncio
from dataclasses import dataclass
from collections.abc import Collection, Sequence
from pathlib import Path

from wd_notability.evaluation_cache import CACHE
from wd_notability.api_backoff import get_retry_after_remaining
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.file_lock import acquire_file_lock
from wd_notability.models import EvaluationResult, NotabilityLevel, QID
from wd_notability.sources import ENTITY_DATA_SOURCE
from wd_notability.wikidata import EntityDeletedError
from wd_notability.wikidata_api import WikidataBackoffActiveError, WikidataRetryAfterError

DEFAULT_BATCH_SIZE = 20
ENTITYDATA_EVALUATION_CHUNK_SIZE = 10
ENTITYDATA_INFLIGHT_QIDS: set[QID] = set()
ENTITYDATA_INFLIGHT_LOCK = asyncio.Lock()
WORKER_POOL_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "worker_pool"
ENTITYDATA_EVENT_TYPE = "entity_data"


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


async def find_entitydata_qids(batch_size: int) -> set[QID]:
    if batch_size < 1:
        return set()

    if (retry_remaining := get_retry_after_remaining()) > 0:
        print(f"EntityData worker abandoned queue selection due to shared Wikidata backoff ({retry_remaining} seconds remaining)")
        return set()

    async with ENTITYDATA_INFLIGHT_LOCK:
        inflight_count = len(ENTITYDATA_INFLIGHT_QIDS)

    candidates = await CACHE.pubsub.list_pubsub_entitydata_candidates(limit=batch_size + inflight_count)
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


async def _release_entitydata_batch(qids: list[QID]) -> None:
    async with ENTITYDATA_INFLIGHT_LOCK:
        for qid in qids:
            ENTITYDATA_INFLIGHT_QIDS.discard(qid)


async def evaluate_entitydata_many(qids: Collection[QID]) -> list[EntityDataUpdate]:
    qid_list = sorted(qid for qid in qids if isinstance(qid, str))
    if not qid_list:
        return []

    contexts = await ENTITY_DATA_SOURCE.get_contexts(qid_list)
    updates: list[EntityDataUpdate] = []

    for qid in qid_list:
        context = contexts.get(qid)
        if context is None:
            context = KeyError(f"Source {ENTITY_DATA_SOURCE.name} did not return context for {qid}")

        if isinstance(context, EntityDeletedError):
            result = EvaluationResult(qid=qid, is_deleted=True)
        elif isinstance(context, Exception):
            result = EvaluationResult(qid=qid)
            for detector in ENTITY_DATA_SOURCE.detectors:
                result.add_error(detector, context)
        else:
            result = await ENTITY_DATA_SOURCE._run_context_core(qid, context)

        updates.append(_entitydata_update_from_result(result))

    return updates


async def upsert_entitydata_updates(updates: Sequence[EntityDataUpdate]) -> list[tuple[QID, int]]:
    if not updates:
        return []

    return await CACHE.upsert_entitydata_many(updates)


async def work_entitydata_batch(batch_size: int = DEFAULT_BATCH_SIZE) -> list[EntityDataUpdate]:
    qids = await find_entitydata_qids(batch_size)
    if not qids:
        return []

    updates: list[EntityDataUpdate] = []
    qid_list = sorted(qids)
    try:
        for start in range(0, len(qid_list), ENTITYDATA_EVALUATION_CHUNK_SIZE):
            chunk = qid_list[start : start + ENTITYDATA_EVALUATION_CHUNK_SIZE]
            try:
                chunk_updates = await evaluate_entitydata_many(chunk)
            except (WikidataBackoffActiveError, WikidataRetryAfterError):
                print(f"EntityData worker abandoned chunk {chunk} due to Wikidata backoff")
                break

            if not chunk_updates:
                continue

            changed = await upsert_entitydata_updates(chunk_updates)
            if changed:
                async with CACHE._write_guard():
                    async with CACHE._connect() as db:
                        await db.execute("BEGIN IMMEDIATE")
                        await CACHE.events.append_summary_updates_in_txn(
                            db,
                            event_type=ENTITYDATA_EVENT_TYPE,
                            summary_updates=changed,
                            mask=CACHE._entitydata_mask(),
                        )
                        await db.commit()
            updates.extend(chunk_updates)
    finally:
        await _release_entitydata_batch(qid_list)

    return updates


async def work_queued_items(limit: int | None = None) -> int:
    with acquire_file_lock(WORKER_POOL_LOCK_TARGET):
        processed = 0
        while limit is None or processed < limit:
            batch_limit = DEFAULT_BATCH_SIZE if limit is None else min(DEFAULT_BATCH_SIZE, limit - processed)
            batch = await work_entitydata_batch(batch_size=batch_limit)
            if not batch:
                break
            processed += len(batch)
        return processed


async def worker_loop(worker_id: int, poll_seconds: float = 5.0) -> None:
    while True:
        start = asyncio.get_event_loop().time()
        try:
            await wait_for_foreground_evaluations()
            batch = await work_entitydata_batch(batch_size=DEFAULT_BATCH_SIZE)
        except Exception as exc:  # noqa: BLE001
            print(f"Worker {worker_id} failed: {exc}")
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        if not batch:
            await asyncio.sleep(max(0.1, poll_seconds))
            continue

        elapsed = asyncio.get_event_loop().time() - start
        print(f"Worker {worker_id} processed {len(batch)} entitydata qid(s) in {elapsed:.2f} seconds")


async def run_worker_pool(worker_count: int = 1, poll_seconds: float = 5.0) -> None:
    if worker_count < 1:
        raise ValueError("worker_count must be at least 1")

    with acquire_file_lock(WORKER_POOL_LOCK_TARGET):
        await asyncio.gather(
            *(worker_loop(worker_id=index + 1, poll_seconds=poll_seconds) for index in range(worker_count))
        )

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path

from wd_notability.evaluation_cache import CACHE
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.file_lock import acquire_file_lock
from wd_notability.models import NotabilityLevel, QID

CACHE_SYNC_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "cache_sync_worker"
CACHE_SYNC_WORKER_BATCH_SIZE = 100
CACHE_SYNC_WORKER_RUN_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class CacheSyncUpdate:
    qid: QID
    n3_osm: NotabilityLevel
    n3_wikisub: NotabilityLevel
    n3_sdc: NotabilityLevel


def _chunked(values: list[QID], size: int) -> list[list[QID]]:
    if size < 1:
        raise ValueError("size must be at least 1")
    return [values[index : index + size] for index in range(0, len(values), size)]


async def _build_cache_sync_updates(qids: list[QID]) -> list[CacheSyncUpdate]:
    if not qids:
        return []

    from wd_notability.lookup_cache import lookup_cache

    qid_list = [qid if isinstance(qid, str) else f"Q{qid}" for qid in qids]
    osm_usage = lookup_cache.get_osm_usage_for(qid_list)
    sdc_usage = lookup_cache.get_sdc_usage_for(qid_list)
    wikisub_qids = lookup_cache.get_wiki_subscribers_for(qid_list)

    osm_set = {qid if isinstance(qid, str) else f"Q{qid}" for qid in osm_usage}
    sdc_set = {qid if isinstance(qid, str) else f"Q{qid}" for qid in sdc_usage}
    wikisub_set = {qid if isinstance(qid, str) else f"Q{qid}" for qid in wikisub_qids}

    updates: list[CacheSyncUpdate] = []
    seen: set[QID] = set()
    for qid in qids:
        if qid in seen:
            continue
        seen.add(qid)
        n3_osm = NotabilityLevel.WEAK if qid in osm_set else NotabilityLevel.NONE
        n3_sdc = NotabilityLevel.STRONG if qid in sdc_set else NotabilityLevel.NONE
        n3_wikisub = NotabilityLevel.STRONG if qid in wikisub_set else NotabilityLevel.NONE
        updates.append(
            CacheSyncUpdate(
                qid=qid,
                n3_osm=n3_osm,
                n3_wikisub=n3_wikisub,
                n3_sdc=n3_sdc,
            )
        )
    return updates


async def work_cache_sync_pass(batch_size: int = CACHE_SYNC_WORKER_BATCH_SIZE, limit: int | None = None) -> int:
    candidates = await CACHE.pubsub.list_pubsub_sync_qids(limit=limit)
    if not candidates:
        return 0

    processed = 0
    for batch in _chunked(candidates, batch_size):
        updates = await _build_cache_sync_updates(batch)
        if not updates:
            continue
        changed = await CACHE.upsert_cache_sync_many(updates)
        processed += len(changed)
    return processed


async def cache_sync_worker_loop(
    *,
    batch_size: int = CACHE_SYNC_WORKER_BATCH_SIZE,
    run_interval_seconds: float = CACHE_SYNC_WORKER_RUN_INTERVAL_SECONDS,
) -> None:
    with acquire_file_lock(CACHE_SYNC_WORKER_LOCK_TARGET):
        while True:
            run_started = time.monotonic()
            try:
                await wait_for_foreground_evaluations()
                processed = await work_cache_sync_pass(batch_size=batch_size)
                print(f"Cache sync worker processed {processed} qid(s)")
            except Exception as exc:  # noqa: BLE001
                print(f"Cache sync worker failed: {exc}")

            sleep_for = max(0.0, run_interval_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)

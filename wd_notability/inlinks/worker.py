from __future__ import annotations

from pathlib import Path
from typing import Any, Collection

from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.file_lock import acquire_file_lock
from wd_notability.inlinks.pipeline import (
    run_inlinks_pass as _run_inlinks_pass,
    run_inlinks_pipeline as _run_inlinks_pipeline,
)
from wd_notability.models import QID

# Default batch size exposed to the worker loop and ad hoc evaluation helpers.
INLINKS_VISIBLE_LIMIT = 100
# Sleep interval between worker passes so the inlinks worker stays responsive without spinning.
INLINKS_WORKER_RUN_INTERVAL_SECONDS = 5.0
# File lock that prevents multiple inlinks worker loops from running at once.
INLINKS_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "inlinks_worker"

cache = EvaluationCache()


async def queue_stats() -> dict[str, Any]:
    total = await cache.interest.count_interest_inlinks_targets()
    return {"total": total}


async def work_inlinks_pass(batch_size: int = INLINKS_VISIBLE_LIMIT, limit: int | None = None) -> int:
    effective_limit = limit if limit is not None else batch_size
    return await _run_inlinks_pass(limit=effective_limit)


async def inlinks_worker_loop(
    *,
    batch_size: int = INLINKS_VISIBLE_LIMIT,
    run_interval_seconds: float = INLINKS_WORKER_RUN_INTERVAL_SECONDS,
) -> None:
    print(
        f"Inlinks worker workflow starting: batch_size={batch_size} "
        f"run_interval_seconds={run_interval_seconds}"
    )
    with acquire_file_lock(INLINKS_WORKER_LOCK_TARGET):
        del batch_size, run_interval_seconds
        await _run_inlinks_pipeline()


async def evaluate_inlinks_many(qids: Collection[QID], *, cache_only: bool = True) -> tuple[int, int]:
    del cache_only
    qid_list = [qid for qid in qids if isinstance(qid, str) and qid.startswith("Q") and qid[1:].isdigit()]
    if not qid_list:
        return 0, 0
    processed = await _run_inlinks_pass(limit=len(qid_list))
    return processed, 0


__all__ = [
    "INLINKS_VISIBLE_LIMIT",
    "INLINKS_WORKER_LOCK_TARGET",
    "INLINKS_WORKER_RUN_INTERVAL_SECONDS",
    "evaluate_inlinks_many",
    "inlinks_worker_loop",
    "queue_stats",
    "work_inlinks_pass",
]

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

from wd_notability.models import NotabilityCriterion, NotabilityLevel

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


async def list_unknown_inlinks_qids(cache: "EvaluationCache", limit: int | None = None) -> list[str]:
    return await cache.list_unknown_inlinks_qids(limit=limit)


async def count_unknown_inlinks_qids(cache: "EvaluationCache") -> int:
    return await cache.count_unknown_inlinks_qids()


async def list_known_inlinks_refresh_candidates(
    cache: "EvaluationCache",
    limit: int | None = None,
) -> list[tuple[str, str, int]]:
    return await cache.list_known_inlinks_refresh_candidates(limit=limit)


async def count_known_inlinks_refresh_candidates(cache: "EvaluationCache") -> int:
    return await cache.count_known_inlinks_refresh_candidates()


async def upsert_inlinks_strong_many(
    cache: "EvaluationCache",
    qids: list[str | int],
    *,
    inlinks_last_evaluated: int,
    create_missing: bool = True,
) -> list[tuple[str, int]]:
    await cache.initialize()

    normalized: list[object] = []
    seen: set[str] = set()
    for qid in qids:
        qid_text = cache._normalize_qid(qid)
        if qid_text in seen:
            continue
        seen.add(qid_text)
        normalized.append(
            SimpleNamespace(
                qid=qid_text,
                n3_inlinks=NotabilityLevel.STRONG,
                inlinks_count=1,
                inlinks_last_evaluated=inlinks_last_evaluated,
            )
        )

    if not normalized:
        return []

    if not create_missing:
        existing = await cache.get_many([getattr(item, "qid") for item in normalized])
        normalized = [item for item in normalized if getattr(item, "qid") in existing]
        if not normalized:
            return []

    return await cache.upsert_inlinks_many(normalized)

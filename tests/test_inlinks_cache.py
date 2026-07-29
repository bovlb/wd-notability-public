from __future__ import annotations

import pytest

from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.inlinks.cache import upsert_inlinks_strong_many
from wd_notability.models import NotabilityLevel


@pytest.mark.asyncio
async def test_upsert_inlinks_strong_many_sets_timestamp_and_dedupes(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    changed = await upsert_inlinks_strong_many(
        cache,
        ["Q2", "Q1", "Q2"],
        inlinks_last_evaluated=123456,
    )

    assert [qid for qid, _ in changed] == ["Q2", "Q1"]
    assert all(summary == changed[0][1] for _, summary in changed)

    rows = await cache.get_many(["Q1", "Q2"])
    assert set(rows) == {"Q1", "Q2"}
    for result in rows.values():
        assert result.n3_inlinks == NotabilityLevel.STRONG
        assert result.inlinks_count == 1

    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT qid, inlinks_count, inlinks_last_evaluated FROM inlinks_cache ORDER BY qid ASC"
        )
        db_rows = await cursor.fetchall()

    assert db_rows == [(1, 1, 123456), (2, 1, 123456)]


@pytest.mark.asyncio
async def test_upsert_inlinks_strong_many_can_skip_missing_rows(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    changed = await upsert_inlinks_strong_many(
        cache,
        ["Q2", "Q1"],
        inlinks_last_evaluated=123456,
        create_missing=False,
    )

    assert changed == []

    rows = await cache.get_many(["Q1", "Q2"])
    assert rows == {}

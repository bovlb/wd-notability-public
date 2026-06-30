from __future__ import annotations

import pytest

from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.inlinks.cache import upsert_inlinks_strong_many
from wd_notability.models import EvaluationResult, NotabilityLevel


@pytest.mark.asyncio
async def test_upsert_inlinks_strong_many_sets_timestamp_and_dedupes(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    changed = await upsert_inlinks_strong_many(
        cache,
        ["Q2", "Q1", "Q2"],
        inlinks_last_evaluated=123456,
    )

    assert [qid for qid, _ in changed] == ["Q2", "Q1"]
    assert all(summary == changed[0][1] for _, summary in changed)

    rows = await cache.get_many(["Q1", "Q2"])
    assert set(rows) == {"Q1", "Q2"}
    for qid, (summary, _entitydata_last_revid, _recent_changes_last_revid) in rows.items():
        result = EvaluationResult.from_summary(qid=qid, summary=summary)
        assert result.n3_inlinks == NotabilityLevel.STRONG

    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT qid, inlinks_last_evaluated FROM evaluation_cache ORDER BY qid ASC"
        )
        db_rows = await cursor.fetchall()

    assert db_rows == [(1, 123456), (2, 123456)]

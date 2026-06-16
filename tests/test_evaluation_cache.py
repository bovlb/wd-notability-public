import sqlite3

import pytest

from wd_notability import evaluation_cache as evaluation_cache_module
from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


@pytest.mark.asyncio
async def test_cache_creates_schema_and_upserts(tmp_path):
    db_path = tmp_path / "evaluation_cache.sqlite3"
    cache = EvaluationCache(db_path=db_path)

    await cache.upsert("Q42", 7, entitydata_last_revid=123)
    await cache.upsert("Q42", 9, entitydata_last_revid=456)

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT qid, summary, entitydata_last_revid FROM evaluation_cache WHERE qid = 42"
        ).fetchone()
    finally:
        conn.close()

    assert row == (42, 9, 456)

    conn = sqlite3.connect(db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    finally:
        conn.close()

    assert {"evaluation_cache", "pubsub_sessions", "pubsub_events"}.issubset(tables)
    assert not any(table.startswith("source_") for table in tables)


@pytest.mark.asyncio
async def test_cache_rejects_invalid_qid(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    with pytest.raises(ValueError, match="qid must look like Q42"):
        await cache.upsert("X42", 1, entitydata_last_revid=123)


@pytest.mark.asyncio
async def test_cache_stats(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)

    empty_stats = await cache.stats()
    assert empty_stats["evaluations"]["entries"] == 0
    assert empty_stats["evaluations"]["oldest_entitydata_last_revid"] is None
    assert empty_stats["evaluations"]["newest_entitydata_last_revid"] is None
    assert empty_stats["evaluations"]["oldest_recent_changes_last_revid"] is None
    assert empty_stats["evaluations"]["newest_recent_changes_last_revid"] is None
    assert empty_stats["evaluations"]["wikisub_entries"] == 0
    assert "timing" in empty_stats
    assert empty_stats["db_path"].endswith("evaluation_cache.sqlite3")

    await cache.upsert("Q1", 3, entitydata_last_revid=100)
    await cache.upsert("Q2", 4, entitydata_last_revid=250)
    wikisub = EvaluationResult(qid="Q6")
    wikisub.set(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.STRONG)
    await cache.upsert("Q6", wikisub.summary, entitydata_last_revid=275)

    stats = await cache.stats()
    assert stats["evaluations"]["entries"] == 3
    assert stats["evaluations"]["oldest_entitydata_last_revid"] == 100
    assert stats["evaluations"]["newest_entitydata_last_revid"] == 275
    assert stats["evaluations"]["wikisub_entries"] == 1


@pytest.mark.asyncio
async def test_cache_get_returns_cached_entry(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    result = EvaluationResult(qid="Q42")
    result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    result.set(NotabilityCriterion.N2a, NotabilityLevel.STRONG)
    result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    await cache.upsert("Q42", result.summary, entitydata_last_revid=777)
    row, entitydata_last_revid, recent_changes_last_revid = await cache.get("Q42")

    assert row is not None
    assert row.qid == "Q42"
    assert row.n1 == NotabilityLevel.WEAK
    assert row.n2a == NotabilityLevel.STRONG
    assert row.n2b == NotabilityLevel.WEAK
    assert row.n12 == NotabilityLevel.WEAK
    assert entitydata_last_revid == 777
    assert recent_changes_last_revid is None


@pytest.mark.asyncio
async def test_cache_entitydata_last_revid_changes_only_when_summary_changes(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    original = EvaluationResult(qid="Q42")
    original.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    changed = EvaluationResult(qid="Q42")
    changed.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)

    await cache.upsert("Q42", original.summary, entitydata_last_revid=100)
    await cache.upsert("Q42", original.summary, entitydata_last_revid=200)
    row, entitydata_last_revid, _ = await cache.get("Q42")

    assert row is not None
    assert row.summary == original.summary
    assert entitydata_last_revid == 100

    await cache.upsert("Q42", changed.summary, entitydata_last_revid=300)
    row, entitydata_last_revid, _ = await cache.get("Q42")

    assert row is not None
    assert row.summary == changed.summary
    assert entitydata_last_revid == 300


@pytest.mark.asyncio
async def test_cache_get_returns_none_for_missing_entry(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    row, entitydata_last_revid, recent_changes_last_revid = await cache.get("Q999")
    assert row is None
    assert entitydata_last_revid is None
    assert recent_changes_last_revid is None


@pytest.mark.asyncio
async def test_cache_clear_removes_evaluation(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    result = EvaluationResult(qid="Q42")
    result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    await cache.upsert("Q42", result.summary, entitydata_last_revid=123)

    await cache.clear()

    assert await cache.get("Q42") == (None, None, None)
    stats = await cache.stats()
    assert stats["evaluations"]["entries"] == 0


@pytest.mark.asyncio
async def test_cache_elevate_updates_existing_and_missing_rows(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    existing = EvaluationResult(qid="Q1")
    existing.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    existing.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
    existing.set(NotabilityCriterion.N2b, NotabilityLevel.STRONG)
    await cache.upsert("Q1", existing.summary, entitydata_last_revid=100)

    updated = await cache.elevate(NotabilityCriterion.N3_INLINKS, NotabilityLevel.STRONG, {"Q1", "Q2"})

    row1, entitydata_last_revid1, _ = await cache.get("Q1")
    row2, entitydata_last_revid2, _ = await cache.get("Q2")

    assert updated == 2
    assert entitydata_last_revid1 is not None
    assert entitydata_last_revid2 is not None
    assert row1 is not None
    assert row1.n1 == NotabilityLevel.WEAK
    assert row1.n2a == NotabilityLevel.WEAK
    assert row1.n2b == NotabilityLevel.STRONG
    assert row1.n3 == NotabilityLevel.STRONG
    assert row2 is not None
    assert row2.n1 == NotabilityLevel.UNKNOWN
    assert row2.n2a == NotabilityLevel.UNKNOWN
    assert row2.n2b == NotabilityLevel.UNKNOWN
    assert row2.n3 == NotabilityLevel.STRONG

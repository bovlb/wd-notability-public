import sqlite3
import asyncio
from types import SimpleNamespace

import pytest

from wd_notability import summary as summary_bits
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

    assert {"evaluation_cache", "pubsub_sessions"}.issubset(tables)
    assert not any(table.startswith("source_") for table in tables)


def test_cache_auto_detects_toolforge(monkeypatch):
    monkeypatch.delenv("WD_NOTABILITY_DB_BACKEND", raising=False)
    monkeypatch.delenv("WD_NOTABILITY_CACHE_BACKEND", raising=False)
    monkeypatch.setattr("wd_notability.evaluation_cache.toolforge_defaults_file_exists", lambda: True)
    monkeypatch.setattr("wd_notability.evaluation_cache.toolforge_database_name", lambda **kwargs: "tool-wd-notability")

    cache = EvaluationCache(db_path=Path("/tmp/ignored.sqlite3"))

    assert cache._backend_name == "mariadb"
    assert cache.database == "tool-wd-notability"


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
async def test_cache_breakdown_splits_detected_and_deduced_criteria(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    first = EvaluationResult(qid="Q1")
    first.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    first.set(NotabilityCriterion.N2a, NotabilityLevel.STRONG)
    first.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    first.set(NotabilityCriterion.N3_INLINKS, NotabilityLevel.NONE)
    first.set(NotabilityCriterion.N3_OSM, NotabilityLevel.STRONG)
    first.set(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.NONE)
    first.set(NotabilityCriterion.N3_SDC, NotabilityLevel.NONE)
    await cache.upsert("Q1", first.summary)

    second = EvaluationResult(qid="Q2")
    await cache.upsert("Q2", second.summary)

    breakdown = await cache.breakdown()

    assert breakdown["entries"] == 2
    assert breakdown["criteria_detected"]["N1"] == {"unknown": 0, "none": 1, "weak": 1, "strong": 0}
    assert breakdown["criteria_detected"]["N3_osm"] == {"unknown": 0, "none": 1, "weak": 0, "strong": 1}
    assert breakdown["criteria_deduced"]["N3"] == {"unknown": 0, "none": 1, "weak": 0, "strong": 1}
    assert breakdown["criteria_deduced"]["N12"] == {"unknown": 0, "none": 1, "weak": 1, "strong": 0}


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
async def test_pubsub_events_for_session_reads_last_updated(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    await cache.initialize()
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_entitydata=True,
        wants_inlinks=True,
        wants_sync=True,
        qids=["Q42", "Q99"],
    )

    await cache.upsert("Q42", 1)
    await asyncio.sleep(0.01)
    await cache.upsert("Q42", 2)
    await asyncio.sleep(0.01)
    await cache.upsert("Q99", 3)

    rows = await cache.pubsub.list_pubsub_events_for_session(
        owner_id="gadget",
        session_id="sub",
        after_event_id=0,
    )

    assert [row["qid"] for row in rows] == [42, 99]
    assert [row["summary"] for row in rows] == [2, 3]
    assert [row["event_type"] for row in rows] == ["summary_change", "summary_change"]
    assert [row["event_id"] for row in rows] == [rows[0]["timestamp"], rows[1]["timestamp"]]


@pytest.mark.asyncio
async def test_pubsub_interest_items_are_aggregated_by_qid_and_owner(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    await cache.initialize()
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub-1",
        ttl_seconds=3600,
        priority=5,
        wants_entitydata=True,
        wants_inlinks=True,
        wants_sync=False,
        qids=["Q42"],
    )
    await cache.pubsub.create_pubsub_session(
        owner_id="report",
        session_id="sub-2",
        ttl_seconds=3600,
        priority=7,
        wants_entitydata=False,
        wants_inlinks=True,
        wants_sync=True,
        qids=["Q42"],
    )
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub-3",
        ttl_seconds=3600,
        priority=11,
        wants_entitydata=True,
        wants_inlinks=False,
        wants_sync=True,
        qids=["Q99"],
    )

    items = await cache.pubsub.list_pubsub_interest_items()

    assert [item["qid"] for item in items] == ["Q42", "Q99"]
    assert items[0]["session_rows"] == 2
    assert items[0]["total_priority"] == 12
    assert items[0]["owner_count"] == 2
    assert items[0]["wants_entitydata"] is True
    assert items[0]["wants_inlinks"] is True
    assert items[0]["wants_sync"] is True
    assert {worker["owner_id"] for worker in items[0]["workers"]} == {"gadget", "report"}
    assert items[1]["session_rows"] == 1
    assert items[1]["total_priority"] == 11
    assert items[1]["owner_count"] == 1
    assert items[1]["workers"][0]["owner_id"] == "gadget"


@pytest.mark.asyncio
async def test_pubsub_sync_qids_prioritize_interest_and_sweep_fallback(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    await cache.initialize()

    interested = EvaluationResult(qid="Q1")
    interested.set(NotabilityCriterion.N3_OSM, NotabilityLevel.UNKNOWN)
    await cache.upsert("Q1", interested.summary)

    uninterested = EvaluationResult(qid="Q2")
    uninterested.set(NotabilityCriterion.N3_OSM, NotabilityLevel.UNKNOWN)
    await cache.upsert("Q2", uninterested.summary)

    fallback_only = EvaluationResult(qid="Q3")
    fallback_only.set(NotabilityCriterion.N3_OSM, NotabilityLevel.UNKNOWN)
    await cache.upsert("Q3", fallback_only.summary)

    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub-1",
        ttl_seconds=3600,
        priority=9,
        wants_entitydata=False,
        wants_inlinks=False,
        wants_sync=True,
        qids=["Q1"],
    )
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub-2",
        ttl_seconds=3600,
        priority=5,
        wants_entitydata=False,
        wants_inlinks=False,
        wants_sync=False,
        qids=["Q2"],
    )

    interested_only = await cache.pubsub.list_pubsub_sync_qids()
    with_fallback = await cache.pubsub.list_pubsub_sync_qids(allow_uninterested=True)
    interested_count = await cache.pubsub.count_pubsub_sync_qids()
    fallback_count = await cache.pubsub.count_pubsub_sync_qids(allow_uninterested=True)

    assert interested_only == ["Q1"]
    assert with_fallback == ["Q1", "Q2", "Q3"]
    assert interested_count == 1
    assert fallback_count == 3


@pytest.mark.asyncio
async def test_cache_last_updated_only_changes_on_summary_change(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    await cache.upsert("Q42", 1, entitydata_last_revid=100)
    async with cache._connect() as db:
        cursor = await db.execute("SELECT last_updated FROM evaluation_cache WHERE qid = 42")
        before_same_summary = await cursor.fetchone()
    assert before_same_summary is not None

    await asyncio.sleep(0.01)
    await cache.upsert("Q42", 1, entitydata_last_revid=200)
    row2, entitydata_last_revid, _ = await cache.get("Q42")
    assert row2 is not None
    assert row2.summary == 1
    assert entitydata_last_revid == 200

    async with cache._connect() as db:
        cursor = await db.execute("SELECT last_updated FROM evaluation_cache WHERE qid = 42")
        after_same_summary = await cursor.fetchone()

    await asyncio.sleep(0.01)
    await cache.upsert("Q42", 3, entitydata_last_revid=300)
    async with cache._connect() as db:
        cursor = await db.execute("SELECT last_updated FROM evaluation_cache WHERE qid = 42")
        after_changed_summary = await cursor.fetchone()

    assert after_same_summary is not None
    assert after_changed_summary is not None
    assert after_same_summary[0] == before_same_summary[0]
    assert after_changed_summary[0] > after_same_summary[0]


@pytest.mark.asyncio
async def test_cache_upsert_refreshes_entitydata_last_revid_even_without_summary_change(tmp_path):
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
    assert entitydata_last_revid == 200

    await cache.upsert("Q42", changed.summary, entitydata_last_revid=300)
    row, entitydata_last_revid, _ = await cache.get("Q42")

    assert row is not None
    assert row.summary == changed.summary
    assert entitydata_last_revid == 300


@pytest.mark.asyncio
async def test_cache_upsert_entitydata_many_refreshes_revid_without_summary_change(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    original = EvaluationResult(qid="Q42")
    original.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    await cache.upsert("Q42", original.summary, entitydata_last_revid=100)

    update = SimpleNamespace(
        qid="Q42",
        is_redirect=False,
        has_claims=False,
        has_sitelinks=False,
        is_deleted=False,
        n1=NotabilityLevel.WEAK,
        n2a=NotabilityLevel.NONE,
        n2b=NotabilityLevel.NONE,
        entitydata_last_revid=200,
    )

    changed = await cache.upsert_entitydata_many([update])
    row, entitydata_last_revid, _ = await cache.get("Q42")

    assert changed == [("Q42", original.summary)]
    assert row is not None
    assert row.summary == original.summary
    assert entitydata_last_revid == 200

    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT recent_changes_last_revid FROM evaluation_cache WHERE qid = 42"
        )
        recent_changes_last_revid = await cursor.fetchone()

    assert recent_changes_last_revid is not None
    assert recent_changes_last_revid[0] == 200


@pytest.mark.asyncio
async def test_pubsub_entitydata_candidates_include_null_recent_changes_revid(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    await cache.upsert("Q42", 1, entitydata_last_revid=100)
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_entitydata=True,
        wants_inlinks=False,
        wants_sync=False,
        qids=["Q42"],
    )

    candidates = await cache.pubsub.list_pubsub_entitydata_candidates()

    assert candidates == ["Q42"]


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

    updated = await cache.elevate(NotabilityCriterion.N1, NotabilityLevel.STRONG, {"Q1", "Q2"})

    row1, entitydata_last_revid1, _ = await cache.get("Q1")
    row2, entitydata_last_revid2, _ = await cache.get("Q2")

    assert updated == 2
    assert entitydata_last_revid1 is not None
    assert entitydata_last_revid2 is None
    assert row1 is not None
    assert row1.n1 == NotabilityLevel.STRONG
    assert row1.n2a == NotabilityLevel.WEAK
    assert row1.n2b == NotabilityLevel.STRONG
    assert row1.n3 == NotabilityLevel.NONE
    assert row2 is not None
    assert row2.n1 == NotabilityLevel.STRONG
    assert row2.n2a == NotabilityLevel.UNKNOWN
    assert row2.n2b == NotabilityLevel.UNKNOWN
    assert row2.n3 == NotabilityLevel.UNKNOWN


@pytest.mark.asyncio
async def test_cache_rejects_inlinks_criterion_from_generic_writers(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    with pytest.raises(ValueError, match="N3_inlinks may only be set by the inlinks worker"):
        await cache.elevate(NotabilityCriterion.N3_INLINKS, NotabilityLevel.STRONG, {"Q1"})

    with pytest.raises(ValueError, match="N3_inlinks may only be set by the inlinks worker"):
        await cache.set_criterion(
            NotabilityCriterion.N3_INLINKS,
            NotabilityLevel.NONE,
            {"Q1"},
        )


@pytest.mark.asyncio
async def test_cache_set_criterion_syncs_missing_rows(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    q1 = EvaluationResult(qid="Q1")
    q1.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    q2 = EvaluationResult(qid="Q2")
    q2.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
    await cache.upsert("Q1", q1.summary, entitydata_last_revid=100)
    await cache.upsert("Q2", q2.summary, entitydata_last_revid=200)

    changed = await cache.set_criterion(
        NotabilityCriterion.N1,
        NotabilityLevel.STRONG,
        {"Q1"},
        clear_missing=True,
    )

    row1, _, _ = await cache.get("Q1")
    row2, _, _ = await cache.get("Q2")

    assert changed == 2
    assert row1 is not None
    assert row1.n1 == NotabilityLevel.STRONG
    assert row2 is not None
    assert row2.n1 == NotabilityLevel.NONE


@pytest.mark.asyncio
async def test_cache_update_summary_bits_updates_rows_without_pre_read(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    await cache.upsert("Q42", 0, entitydata_last_revid=123)

    changed = await cache.update_summary_bits({"Q42"}, set_bits=summary_bits.REDIRECT)
    row, _, _ = await cache.get("Q42")

    assert changed == 1
    assert row is not None
    assert row.summary & summary_bits.REDIRECT


@pytest.mark.asyncio
async def test_cache_upsert_inlinks_many_refreshes_last_evaluated_without_summary_change(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")

    item = SimpleNamespace(qid="Q42", n3_inlinks=NotabilityLevel.STRONG)

    await cache.upsert_inlinks_many([item])
    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT summary, inlinks_last_evaluated FROM evaluation_cache WHERE qid = 42"
        )
        before = await cursor.fetchone()
    assert before is not None

    await asyncio.sleep(0.01)
    await cache.upsert_inlinks_many([item])
    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT summary, inlinks_last_evaluated FROM evaluation_cache WHERE qid = 42"
        )
        after = await cursor.fetchone()

    assert after is not None
    assert after[0] == before[0]
    assert after[1] > before[1]


@pytest.mark.asyncio
async def test_list_inlinks_work_candidates_orders_by_refresh_ratio(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    await cache.initialize()
    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)

    first = EvaluationResult(qid="Q1")
    first.set(NotabilityCriterion.N3_INLINKS, NotabilityLevel.NONE)
    second = EvaluationResult(qid="Q2")
    second.set(NotabilityCriterion.N3_INLINKS, NotabilityLevel.NONE)
    await cache.upsert("Q1", first.summary)
    await cache.upsert("Q2", second.summary)

    async with cache._connect() as db:
        await db.execute(
            "UPDATE evaluation_cache SET creation_time = ?, inlinks_last_evaluated = ? WHERE qid = ?",
            (0, 50, 1),
        )
        await db.execute(
            "UPDATE evaluation_cache SET creation_time = ?, inlinks_last_evaluated = ? WHERE qid = ?",
            (900, 950, 2),
        )
        await db.commit()

    rows = await cache.list_inlinks_work_candidates(limit=2)

    assert [row[0] for row in rows] == ["Q1", "Q2"]


@pytest.mark.asyncio
async def test_list_inlinks_work_candidates_includes_unknown_without_creation_time(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "evaluation_cache.sqlite3")
    await cache.initialize()
    async with cache._connect() as db:
        await db.execute(
            "INSERT INTO evaluation_cache (qid, summary) VALUES (?, ?)",
            (1, 0),
        )
        await db.commit()

    rows = await cache.list_inlinks_work_candidates(limit=1)

    assert [row[0] for row in rows] == ["Q1"]

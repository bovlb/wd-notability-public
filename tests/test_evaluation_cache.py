import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from wd_notability import evaluation_cache as evaluation_cache_module
from wd_notability import cache_state
from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


def _content_item(
    qid: str,
    *,
    n1: NotabilityLevel = NotabilityLevel.NONE,
    n2a: NotabilityLevel = NotabilityLevel.NONE,
    n2b: NotabilityLevel = NotabilityLevel.NONE,
    content_last_revid: int | None = None,
    redirect_target: int | None = None,
    has_sitelinks_count: int = 0,
    has_claims_count: int = 0,
    deleted: bool = False,
    recent_changes_last_revid: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        qid=qid,
        n1=n1,
        n2a=n2a,
        n2b=n2b,
        content_last_revid=content_last_revid,
        redirect_target=redirect_target,
        has_sitelinks_count=has_sitelinks_count,
        has_claims_count=has_claims_count,
        is_deleted=deleted,
        recent_changes_last_revid=recent_changes_last_revid,
    )


async def _upsert_content(cache: EvaluationCache, **kwargs) -> None:
    last_updated = kwargs.pop("last_updated", None)
    original_timestamp_sql = None
    if last_updated is not None:
        original_timestamp_sql = cache._summary_update_timestamp_sql
        cache._summary_update_timestamp_sql = lambda: str(last_updated)
    try:
        await cache.upsert_content_many([_content_item(**kwargs)])
    finally:
        if original_timestamp_sql is not None:
            cache._summary_update_timestamp_sql = original_timestamp_sql


@pytest.mark.asyncio
async def test_cache_creates_schema_and_upserts_content_rows(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    await _upsert_content(
        cache,
        qid="Q42",
        n1=NotabilityLevel.WEAK,
        redirect_target=99,
        content_last_revid=123,
    )
    await _upsert_content(
        cache,
        qid="Q42",
        n1=NotabilityLevel.STRONG,
        redirect_target=99,
        content_last_revid=456,
    )

    row, content_last_revid, redirect_target = await cache.get("Q42")

    assert row is not None
    assert row.qid == "Q42"
    assert row.n1 == NotabilityLevel.STRONG
    assert row.is_redirect is True
    assert content_last_revid == 456
    assert redirect_target == 99

    async with cache._connect() as db:
        cursor = await db.execute("SHOW TABLES LIKE 'user_history'")
        assert await cursor.fetchone() is not None

    stats = await cache.stats()
    assert stats["evaluations"]["entries"] == 1
    assert stats["db_path"] == cache.database


def test_cache_prefers_toolsdb_environment(monkeypatch):
    monkeypatch.setenv("WD_NOTABILITY_DB_BACKEND", "mariadb")
    monkeypatch.setenv("TOOLSDB_HOST", "127.0.0.1")
    monkeypatch.setenv("TOOLSDB_PORT", "3306")
    monkeypatch.setenv("TOOLSDB_DATABASE", "tool-wd-notability")
    monkeypatch.setenv("TOOLSDB_USER", "tool")
    monkeypatch.setenv("TOOLSDB_PASSWORD", "secret")

    cache = EvaluationCache(db_path=Path("/tmp/ignored"))

    assert cache._backend_name == "mariadb"
    assert cache.host == "127.0.0.1"
    assert cache.database == "tool-wd-notability"


def test_cache_requires_toolsdb_env_for_mariadb(monkeypatch):
    monkeypatch.setenv("WD_NOTABILITY_DB_BACKEND", "mariadb")
    for name in ("TOOLSDB_HOST", "TOOLSDB_PORT", "TOOLSDB_DATABASE", "TOOLSDB_USER", "TOOLSDB_PASSWORD"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError, match="TOOLSDB_HOST"):
        EvaluationCache(db_path=Path("/tmp/ignored"))


@pytest.mark.asyncio
async def test_cache_open_connection_uses_autocommit(monkeypatch):
    cache = EvaluationCache(db_path=Path("/tmp/ignored"))

    captured: dict[str, object] = {}

    class FakeConnection:
        pass

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return FakeConnection()

    monkeypatch.setattr(
        evaluation_cache_module,
        "credentials_from_env",
        lambda *_args: SimpleNamespace(user="tool", password="secret"),
    )
    monkeypatch.setattr(
        evaluation_cache_module,
        "require_env_value",
        lambda name: {
            "TOOLSDB_PORT": "3306",
        }[name],
    )
    monkeypatch.setattr(evaluation_cache_module.pymysql, "connect", fake_connect)

    connection = await cache._open_connection()

    assert captured["autocommit"] is True


@pytest.mark.asyncio
async def test_cache_initialize_bootstraps_user_history_schema(monkeypatch):
    cache = EvaluationCache(db_path=Path("/tmp/ignored"))

    executed_sql: list[str] = []
    user_history_calls = 0

    class FakeCursor:
        def __init__(self, sql: str):
            self.sql = sql
            self.rowcount = 0

        async def fetchone(self):
            return None

    class FakeDB:
        async def execute(self, sql: str, params=None):
            executed_sql.append(sql)
            return FakeCursor(sql)

        async def commit(self):
            return None

    class FakeConnect:
        async def __aenter__(self):
            return FakeDB()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    original_ensure_schema = evaluation_cache_module.user_history.ensure_schema

    async def wrapped_ensure_schema(db):
        nonlocal user_history_calls
        user_history_calls += 1
        await original_ensure_schema(db)

    monkeypatch.setattr(evaluation_cache_module.user_history, "ensure_schema", wrapped_ensure_schema)
    monkeypatch.setattr(cache, "_connect", lambda: FakeConnect())

    await cache.initialize()

    assert user_history_calls == 1
    assert any("CREATE TABLE IF NOT EXISTS user_history" in sql for sql in executed_sql)


@pytest.mark.asyncio
async def test_cache_connect_opens_fresh_connection_each_time(monkeypatch):
    cache = EvaluationCache(db_path=Path("/tmp/ignored"))

    opened: list[int] = []
    closed: list[int] = []

    class FakeConnection:
        def __init__(self, index: int):
            self.index = index

        def close(self):
            closed.append(self.index)

    async def fake_open_connection():
        index = len(opened) + 1
        opened.append(index)
        return FakeConnection(index)

    monkeypatch.setattr(cache, "_open_connection", fake_open_connection)

    async with cache._connect() as first:
        assert first.index == 1
    async with cache._connect() as second:
        assert second.index == 2

    assert opened == [1, 2]
    assert closed == [1, 2]


@pytest.mark.asyncio
async def test_cache_rejects_invalid_qid(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    with pytest.raises(ValueError, match="qid must look like Q42"):
        await cache.upsert_content_many([_content_item("X42")])


@pytest.mark.asyncio
async def test_cache_stats(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)
    monkeypatch.setattr(
        evaluation_cache_module.lookup_cache,
        "get_external_usage",
        lambda qids: {
            qid: {
                "osm": None,
                "sdc": None,
                "wikisub": qid == "Q6",
            }
            for qid in qids
        },
    )

    empty_stats = await cache.stats()
    assert empty_stats["evaluations"]["entries"] == 0
    assert empty_stats["evaluations"]["oldest_content_last_revid"] is None
    assert empty_stats["evaluations"]["newest_content_last_revid"] is None
    assert empty_stats["evaluations"]["oldest_recent_changes_last_revid"] is None
    assert empty_stats["evaluations"]["newest_recent_changes_last_revid"] is None
    assert empty_stats["evaluations"]["wikisub_entries"] == 0

    await _upsert_content(cache, qid="Q1", content_last_revid=100)
    await _upsert_content(cache, qid="Q2", content_last_revid=250)
    await _upsert_content(cache, qid="Q6", content_last_revid=275)

    stats = await cache.stats()
    assert stats["evaluations"]["entries"] == 3
    assert stats["evaluations"]["oldest_content_last_revid"] == 100
    assert stats["evaluations"]["newest_content_last_revid"] == 275
    assert stats["evaluations"]["wikisub_entries"] == 1


@pytest.mark.asyncio
async def test_cache_breakdown_splits_detected_and_deduced_criteria(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    monkeypatch.setattr(
        evaluation_cache_module.lookup_cache,
        "get_external_usage",
        lambda qids: {
            qid: {
                "osm": {"count_all": 1} if qid == "Q1" else None,
                "sdc": None,
                "wikisub": False,
            }
            for qid in qids
        },
    )

    await _upsert_content(
        cache,
        qid="Q1",
        n1=NotabilityLevel.WEAK,
        n2a=NotabilityLevel.STRONG,
        n2b=NotabilityLevel.WEAK,
        content_last_revid=100,
    )
    await cache.upsert_inlinks_many([
        SimpleNamespace(
            qid="Q1",
            n3_inlinks=NotabilityLevel.NONE,
            inlinks_count=1,
            inlinks_last_evaluated=1000,
        )
    ])
    await _upsert_content(cache, qid="Q2", content_last_revid=200)

    breakdown = await cache.breakdown()

    assert breakdown["entries"] == 2
    assert breakdown["criteria_detected"]["N1"] == {
        "unknown": 0,
        "none": 1,
        "partial-weak": 0,
        "partial-strong": 0,
        "weak": 1,
        "strong": 0,
    }
    assert breakdown["criteria_detected"]["N3_osm"] == {
        "unknown": 0,
        "none": 1,
        "partial-weak": 0,
        "partial-strong": 0,
        "weak": 1,
        "strong": 0,
    }
    assert breakdown["criteria_deduced"]["N3"] == {
        "unknown": 1,
        "none": 0,
        "partial-weak": 0,
        "partial-strong": 0,
        "weak": 1,
        "strong": 0,
    }


@pytest.mark.asyncio
async def test_cache_breakdown_marks_flags_unknown_without_content_revid(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    await _upsert_content(cache, qid="Q1", redirect_target=1, content_last_revid=101)
    await _upsert_content(cache, qid="Q2", content_last_revid=202)
    await _upsert_content(cache, qid="Q3")

    breakdown = await cache.breakdown()

    assert breakdown["flags"]["redirect"] == {"unknown": 1, "no": 1, "yes": 1}


@pytest.mark.asyncio
async def test_pubsub_content_staleness_breakdown_counts_reason_buckets(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    await cache.clear()
    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)

    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_content=True,
        wants_inlinks=False,
        qids=["Q1", "Q2"],
    )

    await _upsert_content(
        cache,
        qid="Q2",
        content_last_revid=10,
        recent_changes_last_revid=20,
    )
    breakdown = await cache.pubsub.count_pubsub_content_candidates_by_staleness()

    assert breakdown == {
        "total": 2,
        "never_evaluated": 1,
        "recent_changes_missing": 0,
        "recent_changes": 1,
        "redirect_target": 0,
        "deletion_events": 0,
        "content_policy": 0,
    }

    batch_breakdown = await cache.pubsub.count_pubsub_content_candidate_staleness_for_qids(["Q2"])

    assert batch_breakdown == {
        "total": 1,
        "never_evaluated": 0,
        "recent_changes_missing": 0,
        "recent_changes": 1,
        "redirect_target": 0,
        "deletion_events": 0,
        "content_policy": 0,
    }

    reasons = await cache.pubsub.list_pubsub_content_candidate_reasons(["Q2"])

    assert reasons == {
        "Q2": "recent_changes",
    }


@pytest.mark.asyncio
async def test_pubsub_redirect_staleness_uses_target_last_updated(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    source_qid = "Q4294967001"
    target_qid = "Q4294967000"
    try:
        await cache.pubsub.create_pubsub_session(
            owner_id="gadget",
            session_id="sub",
            ttl_seconds=3600,
            priority=10,
            wants_content=True,
            wants_inlinks=False,
            qids=[source_qid],
        )

        await _upsert_content(
            cache,
            qid=source_qid,
            content_last_revid=500,
            redirect_target=int(target_qid[1:]),
            last_updated=1000,
        )
        await _upsert_content(
            cache,
            qid=target_qid,
            content_last_revid=200,
            last_updated=2000,
        )
        await cache_state.set_content_policy_updated_at(cache, 0)

        assert await cache.pubsub.count_pubsub_content_candidate_staleness_for_qids([source_qid]) == {
            "total": 1,
            "never_evaluated": 0,
            "recent_changes_missing": 0,
            "recent_changes": 0,
            "redirect_target": 1,
            "deletion_events": 0,
            "content_policy": 0,
        }
    finally:
        await cache.pubsub.delete_pubsub_lease(owner_id="gadget", lease_id="sub")


@pytest.mark.asyncio
async def test_cache_get_returns_cached_entry(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    monkeypatch.setattr(evaluation_cache_module.lookup_cache, "get_osm_usage_for", lambda qids: {"Q42"})
    monkeypatch.setattr(evaluation_cache_module.lookup_cache, "get_sdc_usage_for", lambda qids: set())
    monkeypatch.setattr(evaluation_cache_module.lookup_cache, "get_wiki_subscribers_for", lambda qids: {"Q42"})

    await _upsert_content(
        cache,
        qid="Q42",
        n1=NotabilityLevel.WEAK,
        n2a=NotabilityLevel.STRONG,
        n2b=NotabilityLevel.WEAK,
        content_last_revid=777,
        recent_changes_last_revid=456,
    )
    await cache.upsert_inlinks_many([
        SimpleNamespace(
            qid="Q42",
            n3_inlinks=NotabilityLevel.STRONG,
            inlinks_count=5,
            inlinks_last_evaluated=123,
        )
    ])

    row, content_last_revid, recent_changes_last_revid = await cache.get("Q42")

    assert row is not None
    assert row.qid == "Q42"
    assert row.n1 == NotabilityLevel.WEAK
    assert row.n2a == NotabilityLevel.STRONG
    assert row.n2b == NotabilityLevel.WEAK
    assert row.n12 == NotabilityLevel.WEAK
    assert row.n3_inlinks == NotabilityLevel.STRONG
    assert row.n3_osm == NotabilityLevel.WEAK
    assert row.n3_wikisub == NotabilityLevel.WEAK
    assert content_last_revid == 777
    assert recent_changes_last_revid == 456


@pytest.mark.asyncio
async def test_cache_get_marks_direct_levels_unknown_without_content_revid(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    monkeypatch.setattr(evaluation_cache_module.lookup_cache, "get_osm_usage_for", lambda qids: set())
    monkeypatch.setattr(evaluation_cache_module.lookup_cache, "get_sdc_usage_for", lambda qids: set())
    monkeypatch.setattr(evaluation_cache_module.lookup_cache, "get_wiki_subscribers_for", lambda qids: set())

    await _upsert_content(
        cache,
        qid="Q43",
        n1=NotabilityLevel.NONE,
        n2a=NotabilityLevel.NONE,
        n2b=NotabilityLevel.NONE,
        content_last_revid=None,
        recent_changes_last_revid=123,
    )

    row, content_last_revid, recent_changes_last_revid = await cache.get("Q43")

    assert row is not None
    assert row.n1 == NotabilityLevel.UNKNOWN
    assert row.n2a == NotabilityLevel.UNKNOWN
    assert row.n2b == NotabilityLevel.UNKNOWN
    assert row.n == NotabilityLevel.UNKNOWN
    assert content_last_revid is None
    assert recent_changes_last_revid == 123


@pytest.mark.asyncio
async def test_pubsub_events_for_session_reads_last_updated(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_content=True,
        wants_inlinks=True,
        qids=["Q42", "Q99"],
    )

    await _upsert_content(cache, qid="Q42", n1=NotabilityLevel.WEAK, content_last_revid=100)
    await asyncio.sleep(0.01)
    await _upsert_content(cache, qid="Q42", n1=NotabilityLevel.STRONG, content_last_revid=200)
    await _upsert_content(cache, qid="Q99", n1=NotabilityLevel.NONE, content_last_revid=300)

    rows = await cache.pubsub.list_pubsub_events_for_session(
        owner_id="gadget",
        session_id="sub",
        after_event_id=0,
    )

    assert [row["qid"] for row in rows] == [42, 99]
    assert [row["event_type"] for row in rows] == ["summary_change", "summary_change"]
    assert rows[0]["content_last_revid"] == 200
    assert rows[1]["content_last_revid"] == 300
    assert rows[0]["n1"] == 3
    assert rows[1]["n1"] == 0


@pytest.mark.asyncio
async def test_content_policy_cutoff_marks_old_content_stale(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_content=True,
        wants_inlinks=False,
        qids=["Q42"],
    )

    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)
    await _upsert_content(
        cache,
        qid="Q42",
        n1=NotabilityLevel.NONE,
        content_last_revid=500,
        recent_changes_last_revid=500,
        last_updated=1000,
    )

    await cache_state.set_content_policy_updated_at(cache, 1500)

    assert await cache_state.get_content_policy_updated_at(cache) == datetime.fromtimestamp(1500, tz=UTC)
    assert await cache.is_stale_content_qid("Q42") is True
    assert (await cache.get_content_staleness_for_qids(["Q42"]))["Q42"] is True
    assert (await cache.get_content_staleness_for_qids(["Q999999"]))["Q999999"] is True
    assert "Q42" in await cache.list_stale_content_qids()
    assert "Q42" in await cache.pubsub.list_pubsub_content_candidates()
    assert await cache.pubsub.count_pubsub_content_candidates() >= 1


@pytest.mark.asyncio
async def test_reset_main_cache_can_set_content_policy_cutoff_without_clearing(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)

    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)
    await _upsert_content(
        cache,
        qid="Q42",
        n1=NotabilityLevel.STRONG,
        content_last_revid=500,
        recent_changes_last_revid=500,
    )

    await evaluation_cache_module.reset_main_cache(
        tmp_path,
        content_policy_updated_at=1500,
    )

    fresh_cache = EvaluationCache(db_path=tmp_path)
    row, content_last_revid, recent_changes_last_revid = await fresh_cache.get("Q42")
    assert row is not None
    assert content_last_revid == 500
    assert recent_changes_last_revid == 500
    assert await cache_state.get_content_policy_updated_at(fresh_cache) == datetime.fromtimestamp(1500, tz=UTC)


@pytest.mark.asyncio
async def test_cache_clear_interest_batches_until_empty(monkeypatch):
    cache = EvaluationCache(db_path=Path("/tmp/ignored"))

    monkeypatch.setattr(cache, "initialize", lambda: asyncio.sleep(0))

    rowcounts = [5000, 123, 0]
    executed_sql: list[str] = []
    commits = 0

    class FakeCursor:
        def __init__(self, rowcount: int):
            self.rowcount = rowcount

    class FakeDB:
        async def execute(self, sql: str, params=None):
            nonlocal rowcounts
            executed_sql.append(sql)
            assert sql == "DELETE FROM interest LIMIT 5000"
            return FakeCursor(rowcounts.pop(0))

        async def commit(self):
            nonlocal commits
            commits += 1

    class FakeConnect:
        async def __aenter__(self):
            return FakeDB()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "_connect", lambda: FakeConnect())

    deleted = await cache.clear_interest()

    assert deleted == 5123
    assert executed_sql == [
        "DELETE FROM interest LIMIT 5000",
        "DELETE FROM interest LIMIT 5000",
        "DELETE FROM interest LIMIT 5000",
    ]
    assert commits == 2


@pytest.mark.asyncio
async def test_reset_sources_can_clear_interest_only(monkeypatch):
    cache = EvaluationCache(db_path=Path("/tmp/ignored"))

    monkeypatch.setattr(cache, "initialize", lambda: asyncio.sleep(0))
    monkeypatch.setattr(cache, "clear_interest", lambda: asyncio.sleep(0, result=9))

    called = False

    class FakeConnect:
        async def __aenter__(self):
            nonlocal called
            called = True
            raise AssertionError("reset_sources should not open a transaction for interest only")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "_connect", lambda: FakeConnect())

    updated = await cache.reset_sources(["interest"])

    assert updated == 9
    assert called is False


@pytest.mark.asyncio
async def test_pubsub_events_for_session_reflects_external_usage(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_content=True,
        wants_inlinks=True,
        qids=["Q42"],
    )

    await _upsert_content(cache, qid="Q42", n1=NotabilityLevel.NONE, content_last_revid=100)

    async with cache._connect() as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute(
            """
            INSERT INTO osm_usage (qid, count_all, count_nodes, count_ways, count_relations)
            VALUES (42, 1, 0, 0, 0)
            ON DUPLICATE KEY UPDATE
                count_all = VALUES(count_all),
                count_nodes = VALUES(count_nodes),
                count_ways = VALUES(count_ways),
                count_relations = VALUES(count_relations)
            """
        )
        await db.execute(
            """
            INSERT INTO sdc_usage (qid, usage_count)
            VALUES (42, 1)
            ON DUPLICATE KEY UPDATE usage_count = VALUES(usage_count)
            """
        )
        await db.execute(
            """
            INSERT INTO wiki_subscribers (qid)
            VALUES (42)
            ON DUPLICATE KEY UPDATE qid = VALUES(qid)
            """
        )
        await db.commit()

    rows = await cache.pubsub.list_pubsub_events_for_session(
        owner_id="gadget",
        session_id="sub",
        after_event_id=0,
    )

    assert len(rows) == 1
    assert rows[0]["qid"] == 42
    assert rows[0]["n3_osm"] == 1
    assert rows[0]["n3_sdc"] == 3
    assert rows[0]["n3_wikisub"] == 3


@pytest.mark.asyncio
async def test_pubsub_interest_items_accept_datetime_aggregates(monkeypatch, tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    store = cache.pubsub

    oldest = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
    newest = datetime(2026, 7, 20, 12, 30, tzinfo=UTC)
    rows = [
        (
            42,
            "gadget",
            2,
            12,
            oldest,
            newest,
            1,
            2,
            0,
            1,
            1,
            0,
        )
    ]

    class FakeCursor:
        async def fetchall(self):
            return rows

    class FakeDB:
        async def execute(self, sql, params=None):
            return FakeCursor()

    class FakeConnect:
        async def __aenter__(self):
            return FakeDB()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "initialize", lambda: asyncio.sleep(0))
    monkeypatch.setattr(cache, "_connect", lambda: FakeConnect())

    items = await store.list_pubsub_interest_items()

    assert items == [
        {
            "qid": "Q42",
            "session_rows": 2,
            "lease_rows": 2,
            "total_priority": 12,
            "oldest_expires_at": int(oldest.timestamp()),
            "newest_expires_at": int(newest.timestamp()),
            "owner_count": 1,
            "owner_ids": ["gadget"],
            "wants_creation": True,
            "wants_content": True,
            "wants_inlinks": False,
            "workers": [
                {
                    "owner_id": "gadget",
                    "session_rows": 2,
                    "lease_rows": 2,
                    "total_priority": 12,
                    "oldest_expires_at": int(oldest.timestamp()),
                    "newest_expires_at": int(newest.timestamp()),
                    "wants_creation": True,
                    "wants_creation_rows": 1,
                    "wants_content": True,
                    "wants_content_rows": 2,
                    "wants_inlinks": False,
                    "wants_inlinks_rows": 0,
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_pubsub_stats_accepts_datetime_thresholds(monkeypatch, tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    store = cache.pubsub

    class FakeCursor:
        def __init__(self, row=None, rows=None):
            self._row = row
            self._rows = rows or []

        async def fetchone(self):
            return self._row

        async def fetchall(self):
            return self._rows

    class FakeDB:
        async def execute(self, sql, params=None):
            if "COUNT(*)," in sql:
                return FakeCursor((3, 2, 1, 100, 200, 0, 0, 0, 0))
            if "SELECT owner_id, COUNT(*)" in sql:
                return FakeCursor(rows=[("gadget", 1), ("report", 1)])
            return FakeCursor(rows=[(0, 0), (1, 2)])

    class FakeConnect:
        async def __aenter__(self):
            return FakeDB()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cache, "initialize", lambda: asyncio.sleep(0))
    monkeypatch.setattr(cache, "_connect", lambda: FakeConnect())

    stats = await store.pubsub_stats()

    assert stats["entries"] == 3
    assert stats["distinct_sessions"] == 2
    assert stats["oldest_expires_at"] == 100
    assert stats["newest_expires_at"] == 200
    assert stats["by_owner"] == {"gadget": 1, "report": 1}


@pytest.mark.asyncio
async def test_cache_clear_removes_rows(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    await _upsert_content(cache, qid="Q42", n1=NotabilityLevel.WEAK, content_last_revid=123)
    await cache.upsert_inlinks_many([
        SimpleNamespace(
            qid="Q42",
            n3_inlinks=NotabilityLevel.NONE,
            inlinks_count=0,
            inlinks_last_evaluated=456,
        )
    ])

    await cache.clear()

    assert await cache.get("Q42") == (None, None, None)
    stats = await cache.stats()
    assert stats["evaluations"]["entries"] == 0


@pytest.mark.asyncio
async def test_cache_clear_content_last_revids_keeps_inlinks_and_recent_changes(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    await _upsert_content(
        cache,
        qid="Q42",
        n1=NotabilityLevel.WEAK,
        content_last_revid=123,
        recent_changes_last_revid=456,
    )
    await cache.upsert_inlinks_many([
        SimpleNamespace(
            qid="Q42",
            n3_inlinks=NotabilityLevel.NONE,
            inlinks_count=0,
            inlinks_last_evaluated=999,
        )
    ])

    updated = await cache.clear_content_last_revids(["Q42"])
    row, content_last_revid, recent_changes_last_revid = await cache.get("Q42")

    assert updated == 1
    assert row is not None
    assert row.n1 == NotabilityLevel.WEAK
    assert content_last_revid is None
    assert recent_changes_last_revid == 456


@pytest.mark.asyncio
async def test_cache_upsert_inlinks_many_refreshes_last_evaluated(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)

    item = SimpleNamespace(qid="Q42", n3_inlinks=NotabilityLevel.STRONG, inlinks_count=1)

    await cache.upsert_inlinks_many([item])
    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT n3_inlinks, inlinks_last_evaluated FROM inlinks_cache WHERE qid = 42"
        )
        before = await cursor.fetchone()
    assert before is not None

    await asyncio.sleep(0.01)
    await cache.upsert_inlinks_many([item])
    async with cache._connect() as db:
        cursor = await db.execute(
            "SELECT n3_inlinks, inlinks_last_evaluated FROM inlinks_cache WHERE qid = 42"
        )
        after = await cursor.fetchone()

    assert after is not None
    assert after[0] == before[0]
    assert after[1] > before[1]


@pytest.mark.asyncio
async def test_list_inlinks_work_candidates_orders_by_last_evaluated(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    await cache.clear()
    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)

    await _upsert_content(cache, qid="Q1")
    await _upsert_content(cache, qid="Q2")
    await cache.upsert_inlinks_many([
        SimpleNamespace(qid="Q1", n3_inlinks=NotabilityLevel.NONE, inlinks_count=0, inlinks_last_evaluated=50),
        SimpleNamespace(qid="Q2", n3_inlinks=NotabilityLevel.NONE, inlinks_count=0, inlinks_last_evaluated=950),
    ])
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_creation=False,
        wants_content=False,
        wants_inlinks=True,
        qids=["Q1", "Q2"],
    )

    rows = await cache.list_inlinks_work_candidates(limit=2)

    assert [row[0] for row in rows] == ["Q1", "Q2"]


@pytest.mark.asyncio
async def test_list_inlinks_work_candidates_requires_active_interest(tmp_path, monkeypatch):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()
    await cache.clear()
    monkeypatch.setattr(evaluation_cache_module.time, "time", lambda: 1000)

    await _upsert_content(cache, qid="Q1", deleted=True)
    await _upsert_content(cache, qid="Q2")
    await cache.pubsub.create_pubsub_session(
        owner_id="gadget",
        session_id="sub",
        ttl_seconds=3600,
        priority=10,
        wants_creation=False,
        wants_content=False,
        wants_inlinks=True,
        qids=["Q1"],
    )

    rows = await cache.list_inlinks_work_candidates(limit=10)

    assert [row[0] for row in rows] == ["Q1"]
    assert all(row[2] > 0 for row in rows)
    assert rows[0][1] is None


@pytest.mark.asyncio
async def test_interest_manager_purges_owner_interest_on_start(monkeypatch):
    from wd_notability.interest import InterestManager

    calls: list[tuple[str, str]] = []

    class FakePubsub:
        async def delete_interest_for_owner(self, *, owner_id: str) -> int:
            calls.append(("delete_interest_for_owner", owner_id))
            return 7

        async def delete_pubsub_interest_for_worker(self, *, worker_id: str) -> int:
            calls.append(("delete_pubsub_interest_for_worker", worker_id))
            return 3

    manager = InterestManager(
        FakePubsub(),
        worker_id="gadget:sub",
        priority=10,
        wants_content=True,
    )

    await manager.start()
    await manager.close()

    assert calls[0] == ("delete_interest_for_owner", "gadget")
    assert calls[-1] == ("delete_pubsub_interest_for_worker", "gadget:sub")


@pytest.mark.asyncio
async def test_interest_manager_flush_records_interest_started(monkeypatch):
    from wd_notability.interest import InterestManager

    started: list[dict[str, object]] = []
    expired: list[dict[str, object]] = []

    class FakeTrace:
        async def record_interest_started_many(self, **kwargs):
            started.append(dict(kwargs))
            return len(kwargs["qids"])

        async def record_interest_expired_many(self, **kwargs):
            expired.append(dict(kwargs))
            return len(kwargs["qids"])

    class FakePubsub:
        cache = SimpleNamespace(item_trace=FakeTrace())

        async def upsert_interest_rows(self, **kwargs):
            return len(kwargs["qids"])

        async def delete_interest_rows(self, **kwargs):
            return len(kwargs["qids"])

    manager = InterestManager(
        FakePubsub(),
        worker_id="web",
        priority=10,
        wants_creation=True,
        wants_content=True,
    )
    manager.current.update({42, 99})
    manager.persisted.update({7})

    await manager._flush()

    assert started == [
        {
            "worker_name": "web",
            "qids": (42, 99),
            "interest_type": "web",
            "details": {
                "worker_id": "web",
                "priority": 10,
                "wants_creation": True,
                "wants_content": True,
                "wants_inlinks": False,
            },
        }
    ]
    assert expired == [
        {
            "worker_name": "web",
            "qids": (7,),
            "interest_type": "web",
            "details": {
                "worker_id": "web",
                "priority": 10,
                "wants_creation": True,
                "wants_content": True,
                "wants_inlinks": False,
            },
        }
    ]


@pytest.mark.asyncio
async def test_interest_manager_close_records_interest_expired(monkeypatch):
    from wd_notability.interest import InterestManager

    expired: list[dict[str, object]] = []

    class FakeTrace:
        async def record_interest_expired_many(self, **kwargs):
            expired.append(dict(kwargs))
            return len(kwargs["qids"])

    class FakePubsub:
        cache = SimpleNamespace(item_trace=FakeTrace())

        async def delete_pubsub_interest_for_worker(self, *, worker_id: str):
            return 2

    manager = InterestManager(
        FakePubsub(),
        worker_id="web",
        priority=10,
        wants_creation=True,
        wants_content=True,
    )
    manager.persisted.update({42, 99})

    await manager.close()

    assert expired == [
        {
            "worker_name": "web",
            "qids": (42, 99),
            "interest_type": "web",
            "details": {
                "worker_id": "web",
                "priority": 10,
                "wants_creation": True,
                "wants_content": True,
                "wants_inlinks": False,
            },
        }
    ]


@pytest.mark.asyncio
async def test_creation_metadata_uses_recent_changes_without_content_rows(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    await cache.initialize()

    future_start = "2030-01-01T00:00:00Z"
    future_end = "2030-01-02T00:00:00Z"

    await cache.upsert_creation_metadata_many([
        SimpleNamespace(qid="Q999999991", creation_time=1893456000, creator_actor_id=77),
        SimpleNamespace(qid="Q999999992", creation_time=1893456600, creator_actor_id=88),
    ])

    rows = await cache.list_creation_metadata(start=future_start, end=future_end)
    by_qid = {row.qid: row for row in rows}

    assert set(by_qid) == {"Q999999991", "Q999999992"}
    assert by_qid["Q999999991"].creator_actor_id == 77
    assert by_qid["Q999999991"].creation_time == 1893456000

    many = await cache.get_creation_metadata_many(["Q999999991", "Q999999992"])
    assert many["Q999999991"].creator_actor_id == 77
    assert many["Q999999992"].creation_time == 1893456600

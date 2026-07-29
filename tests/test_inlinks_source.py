from __future__ import annotations

from pathlib import Path

import pytest

from wd_notability.inlinks.source import InlinksSource, ReplicaConfig


@pytest.mark.asyncio
async def test_inlinks_source_splits_small_and_huge_graphs(monkeypatch):
    from wd_notability.inlinks import source as source_module

    calls = {"counts": 0, "batch": [], "huge": []}

    monkeypatch.setattr(
        "wd_notability.inlinks.source.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    source = InlinksSource(name="inlinks", detectors=set())

    class FakeDB:
        def close(self):
            pass

    monkeypatch.setattr(source, "_connect_replica", lambda: FakeDB())

    def fake_count_query(db, qids):
        calls["counts"] += 1
        return (
            {
                "Q1": source_module.INLINKS_CONTEXT_LIMIT + 1,
                "Q2": 2,
            },
            {
                "count_inlinks": 0.1,
                "count_inlinks_query": 0.05,
                "count_inlinks_fetch": 0.02,
            },
        )

    def fake_batch_query(db, qids):
        calls["batch"].append(list(qids))
        return (
            {"Q2": ["Q4", "Q5"]},
            {"Q2": False},
            {
                "get_context_query": 0.2,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
                "get_context_replica_query": 0.1,
                "get_context_replica_fetch": 0.05,
                "get_context_replica_normalize": 0.03,
            },
        )

    def fake_huge_query(db, qid, *, limit):
        calls["huge"].append((qid, limit))
        return (
            ["Q2", "Q3"],
            True,
            {
                "get_context_query": 0.3,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
                "get_context_replica_query": 0.15,
                "get_context_replica_fetch": 0.08,
                "get_context_replica_normalize": 0.0,
            },
        )

    monkeypatch.setattr(source, "_query_replica_inlink_counts_on_connection", fake_count_query)
    monkeypatch.setattr(source, "_query_replica_inlinks_many_on_connection", fake_batch_query)
    monkeypatch.setattr(source, "_query_replica_inlinks_on_connection", fake_huge_query)

    contexts = await source.get_contexts(["Q1", "Q2"])

    assert calls["counts"] == 1
    assert calls["batch"] == [["Q2"]]
    assert calls["huge"] == [("Q1", source_module.INLINKS_CONTEXT_LIMIT)]
    assert contexts["Q1"]["inlinks"] == ["Q2", "Q3"]
    assert contexts["Q2"]["inlinks"] == ["Q4", "Q5"]
    assert contexts["Q1"]["truncated"] is True
    assert contexts["Q2"]["truncated"] is False
    assert contexts["Q1"]["_timings"]["get_context_query"] == 0.5


@pytest.mark.asyncio
async def test_inlinks_source_normalizes_bytes_from_replica(monkeypatch):
    from wd_notability.inlinks import source as source_module

    monkeypatch.setattr(
        "wd_notability.inlinks.source.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    source = InlinksSource(name="inlinks", detectors=set())

    class FakeDB:
        def close(self):
            pass

    monkeypatch.setattr(source, "_connect_replica", lambda: FakeDB())

    def fake_count_query(db, qids):
        return (
            {"Q140157373": 0},
            {
                "count_inlinks": 0.1,
                "count_inlinks_query": 0.05,
                "count_inlinks_fetch": 0.02,
            },
        )

    def fake_query(db, qids):
        return (
            {"Q140157373": [b"Q2", b"Q3"]},
            {"Q140157373": False},
            {
                "get_context_query": 0.1,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
                "get_context_replica_connect": 0.01,
                "get_context_replica_query": 0.05,
                "get_context_replica_fetch": 0.02,
                "get_context_replica_normalize": 0.01,
            },
        )

    monkeypatch.setattr(source, "_query_replica_inlink_counts_on_connection", fake_count_query)
    monkeypatch.setattr(source, "_query_replica_inlinks_many_on_connection", fake_query)

    contexts = await source.get_contexts(["Q140157373"])

    assert contexts["Q140157373"]["inlinks"] == ["Q2", "Q3"]


@pytest.mark.asyncio
async def test_inlinks_source_avoids_partition_window_query(monkeypatch):
    from wd_notability.inlinks import source as source_module

    monkeypatch.setattr(
        "wd_notability.inlinks.source.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    class FakeCursor:
        def __init__(self):
            self.calls = []
            self.query = ""
            self.params = ()

        def execute(self, query, params):
            self.calls.append((query, params))
            self.query = query
            self.params = params

        def fetchall(self):
            if "COUNT(*) AS inlink_count" in self.query:
                return [("Q1", source_module.INLINKS_CONTEXT_LIMIT + 1), ("Q2", 2)]
            if "lt.lt_title IN" in self.query:
                return [("Q2", "Q4"), ("Q2", "Q5")]
            if "LIMIT %s" in self.query:
                return [("Q2",), ("Q3",)]
            return []

    class FakeDB:
        def __init__(self):
            self.cursor_obj = FakeCursor()
            self.closed = False

        def cursor(self):
            return self.cursor_obj

        def close(self):
            self.closed = True

    fake_db = FakeDB()
    source = InlinksSource(name="inlinks", detectors=set())
    monkeypatch.setattr(source, "_connect_replica", lambda: fake_db)

    contexts = await source.get_contexts(["Q1", "Q2"])

    assert len(fake_db.cursor_obj.calls) == 3
    first_query, first_params = fake_db.cursor_obj.calls[0]
    second_query, second_params = fake_db.cursor_obj.calls[1]
    third_query, third_params = fake_db.cursor_obj.calls[2]
    assert "WITH qids AS" not in first_query
    assert "ROW_NUMBER() OVER" not in first_query
    assert "COUNT(*) AS inlink_count" in first_query
    assert "src.page_is_redirect = 0" in first_query
    assert first_params == ("Q1", "Q2")
    assert "lt.lt_title IN" in second_query
    assert "src.page_is_redirect = 0" in second_query
    assert second_params == ("Q2",)
    assert "LIMIT %s" in third_query
    assert "src.page_is_redirect = 0" in third_query
    assert third_params == ("Q1", source_module.INLINKS_CONTEXT_LIMIT + 1)
    assert contexts["Q1"]["inlinks"] == ["Q2", "Q3"]
    assert contexts["Q2"]["inlinks"] == ["Q4", "Q5"]


@pytest.mark.asyncio
async def test_inlinks_source_keeps_all_inlinks_for_large_targets(monkeypatch):
    from wd_notability.inlinks import source as source_module

    monkeypatch.setattr(
        "wd_notability.inlinks.source.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    class FakeCursor:
        def __init__(self):
            self.calls = []
            self.query = ""

        def execute(self, query, params):
            self.calls.append((query, params))
            self.query = query

        def fetchall(self):
            if "COUNT(*) AS inlink_count" in self.query:
                return [("Q1", 250)]
            return [("Q1", f"Q{index + 2}") for index in range(250)]

    class FakeDB:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

        def close(self):
            pass

    source = InlinksSource(name="inlinks", detectors=set())
    monkeypatch.setattr(source, "_connect_replica", lambda: FakeDB())

    contexts = await source.get_contexts(["Q1"])

    assert len(contexts["Q1"]["inlinks"]) == 250
    assert contexts["Q1"]["truncated"] is False


@pytest.mark.asyncio
async def test_inlinks_source_marks_large_targets_truncated(monkeypatch):
    from wd_notability.inlinks import source as source_module

    monkeypatch.setattr(
        "wd_notability.inlinks.source.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    class FakeCursor:
        def __init__(self):
            self.query = ""
            self.params = ()

        def execute(self, query, params):
            self.query = query
            self.params = params

        def fetchall(self):
            limit = source_module.INLINKS_CONTEXT_LIMIT
            if "COUNT(*) AS inlink_count" in self.query:
                return [("Q1", limit + 1)]
            return [(f"Q{index + 2}",) for index in range(limit + 1)]

    class FakeDB:
        def __init__(self):
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

        def close(self):
            pass

    source = InlinksSource(name="inlinks", detectors=set())
    monkeypatch.setattr(source, "_connect_replica", lambda: FakeDB())

    contexts = await source.get_contexts(["Q1"])

    assert len(contexts["Q1"]["inlinks"]) == source_module.INLINKS_CONTEXT_LIMIT
    assert contexts["Q1"]["truncated"] is True


@pytest.mark.asyncio
async def test_inlinks_source_reuses_replica_connection(monkeypatch):
    monkeypatch.setattr(
        "wd_notability.inlinks.source.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    connect_calls = []

    class FakeDB:
        def close(self):
            pass

    fake_db = FakeDB()
    source = InlinksSource(name="inlinks", detectors=set())

    def fake_connect_replica():
        connect_calls.append(1)
        return fake_db

    monkeypatch.setattr(source, "_connect_replica", fake_connect_replica)
    monkeypatch.setattr(
        source,
        "_query_replica_inlink_counts_on_connection",
        lambda db, qids: ({qid: 0 for qid in qids}, {"count_inlinks": 0.0, "count_inlinks_query": 0.0, "count_inlinks_fetch": 0.0}),
    )
    monkeypatch.setattr(
        source,
        "_query_replica_inlinks_many_on_connection",
        lambda db, qids: ({qid: [f"Q{int(qid[1:]) + 1}"] for qid in qids}, {qid: False for qid in qids}, {"get_context_query": 0.0, "get_context_limiter_wait": 0.0, "get_context_retry_wait": 0.0, "get_context_replica_query": 0.0, "get_context_replica_fetch": 0.0, "get_context_replica_normalize": 0.0}),
    )
    monkeypatch.setattr(
        source,
        "_query_replica_inlinks_on_connection",
        lambda db, qid, *, limit: ([f"Q{int(qid[1:]) + 1}"], False, {"get_context_query": 0.0, "get_context_limiter_wait": 0.0, "get_context_retry_wait": 0.0, "get_context_replica_query": 0.0, "get_context_replica_fetch": 0.0, "get_context_replica_normalize": 0.0}),
    )

    first = source._query_replica_inlinks(["Q1"])
    second = source._query_replica_inlinks(["Q2"])

    assert connect_calls == [1, 1]
    assert first[0]["Q1"] == ["Q2"]
    assert second[0]["Q2"] == ["Q3"]

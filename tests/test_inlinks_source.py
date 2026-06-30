from __future__ import annotations

from pathlib import Path

import pytest

from wd_notability.inlinks.source import InlinksSource, ReplicaConfig


@pytest.mark.asyncio
async def test_inlinks_source_uses_replica_batch(monkeypatch):
    calls = []

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

    def fake_query(db, qids):
        calls.append(list(qids))
        return (
            {
                "Q1": ["Q2", "Q3"],
                "Q2": [],
            },
            {
                "Q1": False,
                "Q2": False,
            },
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

    monkeypatch.setattr(source, "_query_replica_inlinks_on_connection", fake_query)

    contexts = await source.get_contexts(["Q1", "Q2"])

    assert calls == [["Q1", "Q2"]]
    assert contexts["Q1"]["inlinks"] == ["Q2", "Q3"]
    assert contexts["Q2"]["inlinks"] == []
    assert contexts["Q1"]["_timings"]["get_context_query"] == 0.1


@pytest.mark.asyncio
async def test_inlinks_source_normalizes_bytes_from_replica(monkeypatch):
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

    def fake_query(db, qids):
        return (
            {
                "Q140157373": [b"Q2", b"Q3"],
            },
            {
                "Q140157373": False,
            },
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

    monkeypatch.setattr(source, "_query_replica_inlinks_on_connection", fake_query)

    contexts = await source.get_contexts(["Q140157373"])

    assert contexts["Q140157373"]["inlinks"] == ["Q2", "Q3"]


@pytest.mark.asyncio
async def test_inlinks_source_batches_replica_query_per_chunk(monkeypatch):
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
            self.current_qids = []

        def execute(self, query, params):
            self.calls.append((query, params))
            self.current_qids = list(params[:-1])

        def fetchall(self):
            rows = []
            for qid in self.current_qids:
                if qid == "Q1":
                    rows.extend([(qid, "Q2"), (qid, "Q3")])
                elif qid == "Q2":
                    rows.append((qid, "Q4"))
            return rows

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

    contexts = await source.get_contexts(["Q1", "Q2", *[f"Q{i}" for i in range(3, 5005)]])

    assert len(fake_db.cursor_obj.calls) == 2
    first_query, first_params = fake_db.cursor_obj.calls[0]
    second_query, second_params = fake_db.cursor_obj.calls[1]
    assert "IN (" in first_query
    assert first_params[-1] == source.MAX_INLINKS_PER_TARGET + 1
    assert second_params[-1] == source.MAX_INLINKS_PER_TARGET + 1
    assert contexts["Q1"]["inlinks"] == ["Q2", "Q3"]
    assert contexts["Q2"]["inlinks"] == ["Q4"]


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

    class FakeCursor:
        def __init__(self, qids):
            self.qids = qids

        def execute(self, query, params):
            self.qids[:] = list(params[:-1])

        def fetchall(self):
            return [(qid, f"Q{int(qid[1:]) + 1}") for qid in self.qids]

    class FakeDB:
        def __init__(self):
            self.current_qids: list[str] = []

        def cursor(self):
            return FakeCursor(self.current_qids)

        def close(self):
            pass

    fake_db = FakeDB()
    source = InlinksSource(name="inlinks", detectors=set())

    def fake_connect_replica():
        connect_calls.append(1)
        return fake_db

    monkeypatch.setattr(source, "_connect_replica", fake_connect_replica)

    first = source._query_replica_inlinks(["Q1"])
    second = source._query_replica_inlinks(["Q2"])

    assert connect_calls == [1]
    assert first[0]["Q1"] == ["Q2"]
    assert second[0]["Q2"] == ["Q3"]

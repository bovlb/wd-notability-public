from __future__ import annotations

from pathlib import Path

import pytest

from wd_notability.sources.inlinks import InlinksSource, ReplicaConfig


@pytest.mark.asyncio
async def test_inlinks_source_uses_replica_batch(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "wd_notability.sources.inlinks.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    source = InlinksSource(name="inlinks", detectors=set())

    def fake_query(qids):
        calls.append(list(qids))
        return (
            {
                "Q1": ["Q2", "Q3"],
                "Q2": [],
            },
            {
                "get_context_query": 0.1,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
            },
        )

    monkeypatch.setattr(source, "_query_replica_inlinks", fake_query)

    contexts = await source.get_contexts(["Q1", "Q2"])

    assert calls == [["Q1", "Q2"]]
    assert contexts["Q1"]["inlinks"] == ["Q2", "Q3"]
    assert contexts["Q2"]["inlinks"] == []
    assert contexts["Q1"]["_timings"]["get_context_query"] == 0.1


@pytest.mark.asyncio
async def test_inlinks_source_normalizes_bytes_from_replica(monkeypatch):
    monkeypatch.setattr(
        "wd_notability.sources.inlinks.ReplicaConfig.from_env",
        classmethod(lambda cls: ReplicaConfig(
            enabled=True,
            host="localhost",
            port=3306,
            database="wikidatawiki_p",
            defaults_file=Path("/tmp/replica.my.cnf"),
        )),
    )

    source = InlinksSource(name="inlinks", detectors=set())

    def fake_query(qids):
        return (
            {
                "Q140157373": [b"Q2", b"Q3"],
            },
            {
                "get_context_query": 0.1,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
            },
        )

    monkeypatch.setattr(source, "_query_replica_inlinks", fake_query)

    contexts = await source.get_contexts(["Q140157373"])

    assert contexts["Q140157373"]["inlinks"] == ["Q2", "Q3"]

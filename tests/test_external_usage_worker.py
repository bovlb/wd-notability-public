from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_cache_sync_worker_sweeps_uninterested_by_default(monkeypatch):
    from wd_notability.external_usage import worker as cache_sync_worker

    recorded = {}

    class FakePubSub:
        async def list_pubsub_sync_qids(self, limit=None, *, allow_uninterested=False):
            recorded["limit"] = limit
            recorded["allow_uninterested"] = allow_uninterested
            return ["Q1", "Q2"]

    class FakeCache:
        pubsub = FakePubSub()

        async def upsert_cache_sync_many(self, updates):
            recorded["updates"] = [update.qid for update in updates]
            return [(update.qid, 1) for update in updates]

    async def fake_build_cache_sync_updates(qids):
        return [SimpleNamespace(qid=qid) for qid in qids]

    monkeypatch.setattr(cache_sync_worker, "CACHE", FakeCache())
    monkeypatch.setattr(cache_sync_worker, "_build_cache_sync_updates", fake_build_cache_sync_updates)

    processed = await cache_sync_worker.work_cache_sync_pass(batch_size=10, limit=5)

    assert processed == 2
    assert recorded["limit"] == 5
    assert recorded["allow_uninterested"] is True
    assert recorded["updates"] == ["Q1", "Q2"]

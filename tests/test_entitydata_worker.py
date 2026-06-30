from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from wd_notability.models import EvaluationResult
from wd_notability.models import NotabilityCriterion, NotabilityLevel
from wd_notability.wikidata import EntityDeletedError


@pytest.mark.asyncio
async def test_entitydata_worker_keeps_deleted_items_without_lastrevid(monkeypatch):
    from wd_notability.content import worker as entitydata_worker

    deleted_result = EvaluationResult(qid="Q1", is_deleted=True)
    live_result = EvaluationResult(qid="Q2")
    live_result.entitydata_last_revid = None

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": EntityDeletedError("Q1"),
                "Q2": {"qid": "Q2"},
            }

        async def _run_context_core(self, qid, context):
            if qid == "Q2":
                return live_result
            return deleted_result

        detectors = ()

    recorded_failures: list[tuple[str, int]] = []

    async def fake_record_failure(kind, count=1):
        recorded_failures.append((kind, count))

    monkeypatch.setattr(entitydata_worker, "ENTITY_DATA_SOURCE", FakeSource())
    monkeypatch.setattr(entitydata_worker, "_record_entitydata_failure", fake_record_failure)

    updates, outlinks = await entitydata_worker.evaluate_entitydata_many(["Q1", "Q2"])

    assert [update.qid for update in updates] == ["Q1"]
    assert updates[0].is_deleted is True
    assert updates[0].entitydata_last_revid is None
    assert outlinks == set()
    assert recorded_failures == [("missing_lastrevid", 1)]


@pytest.mark.asyncio
async def test_entitydata_worker_batches_strong_outlinks_into_inlinks_upsert(monkeypatch):
    from wd_notability.content import worker as entitydata_worker

    strong_result = EvaluationResult(qid="Q1")
    strong_result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
    strong_result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
    strong_result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    strong_result.entitydata_last_revid = 123

    weak_result = EvaluationResult(qid="Q2")
    weak_result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    weak_result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
    weak_result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    weak_result.entitydata_last_revid = 456

    class FakeSource:
        name = "entity_data"
        detectors = ()

        async def get_contexts(self, qids):
            return {
                "Q1": {"qid": "Q1", "entity": {"id": "Q1"}, "outlinks": ["Q10", "Q11"]},
                "Q2": {"qid": "Q2", "entity": {"id": "Q2"}, "outlinks": ["Q11", "Q12"]},
            }

        async def _run_context_core(self, qid, context):
            return strong_result if qid == "Q1" else weak_result

    async def fake_find_entitydata_qids(batch_size, *, allow_uninterested=False):
        assert batch_size == 2
        return {"Q1", "Q2"}

    async def fake_persist_entitydata_chunk(chunk_updates, batch_timings):
        return [(update.qid, update.entitydata_last_revid or 0) for update in chunk_updates]

    recorded_outlinks: list[list[str]] = []
    recorded_timestamps: list[int] = []

    async def fake_upsert_inlinks_strong_many(cache, qids, *, inlinks_last_evaluated):
        recorded_outlinks.append(list(qids))
        recorded_timestamps.append(inlinks_last_evaluated)
        return [(qid, 3) for qid in qids]

    async def fake_release_entitydata_batch(qids):
        return None

    monkeypatch.setattr(entitydata_worker, "ENTITY_DATA_SOURCE", FakeSource())
    monkeypatch.setattr(entitydata_worker, "find_entitydata_qids", fake_find_entitydata_qids)
    monkeypatch.setattr(entitydata_worker, "_persist_entitydata_chunk", fake_persist_entitydata_chunk)
    monkeypatch.setattr(entitydata_worker, "upsert_inlinks_strong_many", fake_upsert_inlinks_strong_many)
    monkeypatch.setattr(entitydata_worker, "_release_entitydata_batch", fake_release_entitydata_batch)
    monkeypatch.setattr(entitydata_worker, "get_retry_after_remaining", lambda: 0)
    monkeypatch.setattr(entitydata_worker.time, "time", lambda: 1000.0)

    batch, source_label = await entitydata_worker.work_entitydata_pubsub_batch(batch_size=2)

    assert [item.qid for item in batch] == ["Q1", "Q2"]
    assert source_label == "pubsub"
    assert recorded_outlinks == [["Q10", "Q11"]]
    assert recorded_timestamps == [1000]


@pytest.mark.asyncio
async def test_entitydata_fetcher_overlaps_chunk_requests(monkeypatch):
    from wd_notability.content import fetcher as content_fetcher

    source = content_fetcher._ContentFetcher(name="entity_data", detectors=set())
    source._replica_config = SimpleNamespace(enabled=False)

    first_chunk_started = asyncio.Event()
    second_chunk_started = asyncio.Event()
    release_first_chunk = asyncio.Event()
    started_chunks: list[list[str]] = []

    async def fake_get_with_timings(url, *, limiter, params=None, data=None, max_attempts=5):
        ids = str((params or {}).get("ids", "")).split("|")
        started_chunks.append(ids)
        if len(started_chunks) == 1:
            first_chunk_started.set()
            await release_first_chunk.wait()
        else:
            second_chunk_started.set()
        response = httpx.Response(
            200,
            json={"entities": {qid: {"id": qid} for qid in ids}},
            request=httpx.Request("GET", url),
        )
        return response, SimpleNamespace(as_dict=lambda prefix: {f"{prefix}_query": 0.0, f"{prefix}_limiter_wait": 0.0, f"{prefix}_retry_wait": 0.0})

    async def fake_timing_snapshot() -> str:
        return "global wikidata timings: test"

    monkeypatch.setattr(content_fetcher.wikidata_session, "get_with_timings", fake_get_with_timings)
    monkeypatch.setattr(content_fetcher.wikidata_session, "timing_snapshot", fake_timing_snapshot)

    qids = [f"Q{i}" for i in range(1, 61)]
    task = asyncio.create_task(source.get_contexts(qids))

    await asyncio.wait_for(first_chunk_started.wait(), 1)
    await asyncio.wait_for(second_chunk_started.wait(), 1)
    release_first_chunk.set()

    contexts = await asyncio.wait_for(task, 1)

    assert len(started_chunks) == 2
    assert started_chunks[0] == [f"Q{i}" for i in range(1, 51)]
    assert started_chunks[1] == [f"Q{i}" for i in range(51, 61)]
    assert set(contexts) == set(qids)

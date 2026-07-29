from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from wd_notability.models import EvaluationResult
from wd_notability.models import NotabilityCriterion, NotabilityLevel
from wd_notability.wikidata import EntityDeletedError


@pytest.mark.asyncio
async def test_content_worker_does_not_warn_for_deleted_entities(capsys, monkeypatch):
    from types import SimpleNamespace

    from wd_notability.content import fetcher as content_fetcher

    shared_payload = {
        "entities": {
            "Q1": {"id": "Q1", "missing": ""},
            "Q2": {"id": "Q2", "lastrevid": 1, "claims": {}, "sitelinks": {}},
        }
    }

    class FakeResponse:
        request = SimpleNamespace(
            url="https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q1|Q2"
        )

        def raise_for_status(self):
            return None

        def json(self):
            return shared_payload

    async def fake_get_with_timings(*_args, **_kwargs):
        class FakeTimings:
            def as_dict(self, _label):
                return {}

        return FakeResponse(), FakeTimings()

    async def fake_timing_snapshot():
        return {}

    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "get_with_timings",
        fake_get_with_timings,
    )
    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "timing_snapshot",
        fake_timing_snapshot,
    )
    object.__setattr__(
        content_fetcher.CONTENT_SOURCE,
        "_replica_config",
        SimpleNamespace(enabled=False),
    )

    contexts = await content_fetcher.CONTENT_SOURCE._get_context_chunk(["Q1", "Q2"])

    stdout = capsys.readouterr().out
    assert "Content worker API fetch had problems" not in stdout
    assert isinstance(contexts["Q1"], EntityDeletedError)
    assert contexts["Q2"]["entity"]["id"] == "Q2"


@pytest.mark.asyncio
async def test_content_worker_keeps_deleted_items_without_lastrevid(monkeypatch):
    from wd_notability.content import worker as content_worker

    deleted_result = EvaluationResult(qid="Q1", is_deleted=True)
    live_result = EvaluationResult(qid="Q2")
    live_result.content_last_revid = None

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

    monkeypatch.setattr(content_worker, "CONTENT_SOURCE", FakeSource())
    monkeypatch.setattr(
        content_worker, "_record_content_failure", fake_record_failure)

    updates, outlinks = await content_worker.evaluate_content_many(["Q1", "Q2"])

    assert [update.qid for update in updates] == ["Q1"]
    assert updates[0].is_deleted is True
    assert updates[0].content_last_revid is None
    assert outlinks == set()
    assert recorded_failures == [("missing_lastrevid", 1)]


@pytest.mark.asyncio
async def test_content_queue_stats_reflects_content_state_only(monkeypatch):
    from wd_notability.content import worker as content_worker

    class FakeCache:
        class pubsub:
            @staticmethod
            async def count_pubsub_content_candidates_by_staleness():
                return {"total": 4, "never_evaluated": 2}

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    async with content_worker.CONTENT_INFLIGHT_LOCK:
        content_worker.CONTENT_INFLIGHT_QIDS.clear()
        content_worker.CONTENT_INFLIGHT_QIDS.update({"Q1", "Q2"})

    try:
        stats = await content_worker.queue_stats()
    finally:
        async with content_worker.CONTENT_INFLIGHT_LOCK:
            content_worker.CONTENT_INFLIGHT_QIDS.clear()

    assert stats == {
        "stale": 4,
        "by_staleness": {"total": 4, "never_evaluated": 2},
        "in_flight": 2,
    }


@pytest.mark.asyncio
async def test_content_worker_dumps_api_payload_once_per_fetch(capsys, monkeypatch):
    from types import SimpleNamespace

    from wd_notability.content import fetcher as content_fetcher
    shared_payload = {
        "entities": {
            "Q1": {"id": "Q1", "lastrevid": None, "claims": {}, "sitelinks": {}},
            "Q2": {"id": "Q2", "lastrevid": None, "claims": {}, "sitelinks": {}},
        }
    }

    class FakeResponse:
        request = SimpleNamespace(
            url="https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q1|Q2"
        )

        def raise_for_status(self):
            return None

        def json(self):
            return shared_payload

    async def fake_get_with_timings(*_args, **_kwargs):
        class FakeTimings:
            def as_dict(self, _label):
                return {}

        return FakeResponse(), FakeTimings()

    async def fake_timing_snapshot():
        return {}

    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "get_with_timings",
        fake_get_with_timings,
    )
    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "timing_snapshot",
        fake_timing_snapshot,
    )
    object.__setattr__(
        content_fetcher.CONTENT_SOURCE,
        "_replica_config",
        SimpleNamespace(enabled=False),
    )

    await content_fetcher.CONTENT_SOURCE._get_context_chunk(["Q1", "Q2"])

    stdout = capsys.readouterr().out
    assert stdout.count("API URL:") == 1
    assert "Q1" in stdout
    assert "Q2" in stdout


@pytest.mark.asyncio
async def test_content_worker_dumps_api_payload_for_context_errors_once_per_fetch(capsys, monkeypatch):
    from types import SimpleNamespace

    from wd_notability.content import fetcher as content_fetcher

    shared_payload = {"entities": {}}

    class FakeResponse:
        request = SimpleNamespace(
            url="https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q1|Q2"
        )

        def raise_for_status(self):
            return None

        def json(self):
            return shared_payload

    async def fake_get_with_timings(*_args, **_kwargs):
        class FakeTimings:
            def as_dict(self, _label):
                return {}

        return FakeResponse(), FakeTimings()

    async def fake_timing_snapshot():
        return {}

    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "get_with_timings",
        fake_get_with_timings,
    )
    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "timing_snapshot",
        fake_timing_snapshot,
    )
    object.__setattr__(
        content_fetcher.CONTENT_SOURCE,
        "_replica_config",
        SimpleNamespace(enabled=False),
    )

    await content_fetcher.CONTENT_SOURCE._get_context_chunk(["Q1", "Q2"])

    stdout = capsys.readouterr().out
    assert stdout.count("API URL:") == 1
    assert "Content worker API fetch had problems for Q1, Q2" in stdout


@pytest.mark.asyncio
async def test_content_worker_rejects_singleton_mismatch_and_truncates_payload(capsys, monkeypatch):
    from types import SimpleNamespace

    from wd_notability.content import fetcher as content_fetcher

    shared_payload = {
        "entities": {
            "Q999": {
                "id": "Q999",
                "lastrevid": 1,
                "claims": {},
                "sitelinks": {},
                "notes": "x" * 10000,
            }
        }
    }

    class FakeResponse:
        request = SimpleNamespace(
            url="https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q1|Q2"
        )

        def raise_for_status(self):
            return None

        def json(self):
            return shared_payload

    async def fake_get_with_timings(*_args, **_kwargs):
        class FakeTimings:
            def as_dict(self, _label):
                return {}

        return FakeResponse(), FakeTimings()

    async def fake_timing_snapshot():
        return {}

    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "get_with_timings",
        fake_get_with_timings,
    )
    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "timing_snapshot",
        fake_timing_snapshot,
    )
    object.__setattr__(
        content_fetcher.CONTENT_SOURCE,
        "_replica_config",
        SimpleNamespace(enabled=False),
    )

    contexts = await content_fetcher.CONTENT_SOURCE._get_context_chunk(["Q1", "Q2"])

    stdout = capsys.readouterr().out
    assert isinstance(contexts["Q1"], ValueError)
    assert isinstance(contexts["Q2"], ValueError)
    assert "Content worker API fetch had problems for Q1, Q2" in stdout
    assert "API URL:" in stdout


@pytest.mark.asyncio
async def test_content_worker_recovers_good_qids_when_chunk_includes_missing_entity(monkeypatch):
    from types import SimpleNamespace

    from wd_notability.content import fetcher as content_fetcher

    missing_qid = "Q42"
    requested_ids: list[str] = []

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    async def fake_get_with_timings(*_args, **kwargs):
        params = kwargs["params"]
        ids = params["ids"]
        requested_ids.append(ids)
        requested_qids = ids.split("|")
        class FakeTimings:
            def as_dict(self, _label):
                return {}

        if missing_qid in requested_qids and len(requested_qids) > 1:
            return FakeResponse(
                {
                    "error": {
                        "code": "no-such-entity",
                        "id": missing_qid,
                        "info": f'Could not find an entity with the ID "{missing_qid}".',
                    }
                }
            ), FakeTimings()

        entities = {
            qid: {"id": qid, "lastrevid": 1, "claims": {}, "sitelinks": {}}
            for qid in requested_qids
        }
        return FakeResponse({"entities": entities}), FakeTimings()

    async def fake_timing_snapshot():
        return {}

    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "get_with_timings",
        fake_get_with_timings,
    )
    monkeypatch.setattr(
        content_fetcher.wikidata_session,
        "timing_snapshot",
        fake_timing_snapshot,
    )
    object.__setattr__(
        content_fetcher.CONTENT_SOURCE,
        "_replica_config",
        SimpleNamespace(enabled=False),
    )

    contexts = await content_fetcher.CONTENT_SOURCE._get_context_chunk(
        ["Q1", missing_qid, "Q2"]
    )

    assert requested_ids == [f"Q1|{missing_qid}|Q2", "Q1|Q2"]
    assert contexts["Q1"]["entity"]["id"] == "Q1"
    assert contexts["Q2"]["entity"]["id"] == "Q2"
    assert isinstance(contexts[missing_qid], EntityDeletedError)


@pytest.mark.asyncio
async def test_content_worker_reports_failure_delta_when_chunk_has_no_updates(capsys, monkeypatch):
    from wd_notability.content import worker as content_worker

    class FakeTrace:
        async def flush(self):
            return 0

    class FakePubsub:
        async def count_pubsub_content_candidate_staleness_for_qids(self, qids):
            assert list(qids) == ["Q1", "Q2"]
            return {
                "total": 0,
                "never_evaluated": 0,
                "recent_changes_missing": 0,
                "recent_changes": 0,
                "redirect_target": 0,
                "deletion_events": 0,
                "content_policy": 0,
            }

    class FakeCache:
        item_trace = FakeTrace()
        pubsub = FakePubsub()

    snapshots = iter(
        [
            {
                "context_errors": 0,
                "missing_lastrevid": 0,
                "unknown_live_result": 0,
                "validation_rejected": 0,
                "worker_exceptions": 0,
            },
            {
                "context_errors": 0,
                "missing_lastrevid": 2,
                "unknown_live_result": 0,
                "validation_rejected": 0,
                "worker_exceptions": 0,
            },
        ]
    )

    async def fake_find_content_qids(batch_size):
        assert batch_size == 2
        return ["Q1", "Q2"]

    async def fake_evaluate_content_many(qids):
        assert list(qids) == ["Q1", "Q2"]
        return [], set()

    async def fake_snapshot():
        return next(snapshots)

    async def fake_record_events(*args, **kwargs):
        return 0

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "find_content_qids", fake_find_content_qids)
    monkeypatch.setattr(content_worker, "evaluate_content_many", fake_evaluate_content_many)
    monkeypatch.setattr(content_worker, "_content_failure_snapshot", fake_snapshot)
    monkeypatch.setattr(content_worker, "_record_content_batch_events", fake_record_events)
    monkeypatch.setattr(content_worker.time, "time", lambda: 1000.0)

    batch, source_label, batch_staleness, _batch_timings = await content_worker.work_content_pubsub_batch(
        batch_size=2
    )

    stdout = capsys.readouterr().out
    assert batch == []
    assert source_label == "pubsub"
    assert batch_staleness is None
    assert "Content worker found no updates for chunk of 2 qids" in stdout
    assert "failure_delta=missing_lastrevid=2" in stdout


@pytest.mark.asyncio
async def test_content_worker_forwards_in_flight_exclusions(monkeypatch):
    from wd_notability.content import worker as content_worker

    captured: dict[str, object] = {}

    class FakeInterest:
        async def list_interest_content_candidates(self, *, limit=None, exclude_qids=None):
            captured["limit"] = limit
            captured["exclude_qids"] = list(exclude_qids or [])
            return ["Q2", "Q3"]

    class FakeCache:
        interest = FakeInterest()

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())

    qids = await content_worker.find_content_qids(100, exclude_qids={"Q1"})

    assert qids == ["Q2", "Q3"]
    assert captured == {"limit": 100, "exclude_qids": ["Q1"]}


@pytest.mark.asyncio
async def test_content_worker_batches_strong_outlinks_into_inlinks_upsert(monkeypatch):
    from wd_notability.content import worker as content_worker

    strong_result = EvaluationResult(qid="Q1")
    strong_result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
    strong_result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
    strong_result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    strong_result.content_last_revid = 123

    weak_result = EvaluationResult(qid="Q2")
    weak_result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    weak_result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
    weak_result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
    weak_result.content_last_revid = 456

    class FakeSource:
        name = "content"
        detectors = ()

        async def get_contexts(self, qids):
            return {
                "Q1": {"qid": "Q1", "entity": {"id": "Q1"}, "outlinks": ["Q10", "Q11"]},
                "Q2": {"qid": "Q2", "entity": {"id": "Q2"}, "outlinks": ["Q11", "Q12"]},
            }

        async def _run_context_core(self, qid, context):
            return strong_result if qid == "Q1" else weak_result

    async def fake_find_content_qids(batch_size):
        assert batch_size == 2
        return ["Q1", "Q2"]

    async def fake_persist_content_chunk(chunk_updates, batch_timings, *, batch_id):
        return [(update.qid, update.content_last_revid or 0) for update in chunk_updates]

    recorded_outlinks: list[list[str]] = []
    recorded_timestamps: list[int] = []

    async def fake_upsert_inlinks_strong_many(cache, qids, *, inlinks_last_evaluated, create_missing=True):
        assert create_missing is False
        recorded_outlinks.append(list(qids))
        recorded_timestamps.append(inlinks_last_evaluated)
        return [(qid, 3) for qid in qids]

    class FakeTrace:
        async def record_events(self, records):
            return len(records)

        async def flush(self):
            return 0

    class FakePubsub:
        async def count_pubsub_content_candidate_staleness_for_qids(self, qids):
            assert list(qids) == ["Q1", "Q2"]
            return {
                "total": 0,
                "never_evaluated": 0,
                "recent_changes_missing": 0,
                "recent_changes": 0,
                "redirect_target": 0,
                "deletion_events": 0,
                "content_policy": 0,
            }

    class FakeCache:
        item_trace = FakeTrace()
        pubsub = FakePubsub()

        async def upsert_content_many(self, updates):
            return [(update.qid, 1) for update in updates]

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "CONTENT_SOURCE", FakeSource())
    monkeypatch.setattr(content_worker, "find_content_qids",
                        fake_find_content_qids)
    monkeypatch.setattr(
        content_worker, "_persist_content_chunk", fake_persist_content_chunk)
    monkeypatch.setattr(
        content_worker, "upsert_inlinks_strong_many", fake_upsert_inlinks_strong_many)
    monkeypatch.setattr(content_worker.time, "time", lambda: 1000.0)
    async def fake_evaluate_content_many(qids):
        return [
            content_worker.ContentUpdate(
                qid="Q1",
                is_redirect=False,
                has_claims_count=1,
                has_sitelinks_count=1,
                is_deleted=False,
                n1=NotabilityLevel.NONE,
                n2a=NotabilityLevel.NONE,
                n2b=NotabilityLevel.NONE,
                content_last_revid=123,
            ),
            content_worker.ContentUpdate(
                qid="Q2",
                is_redirect=False,
                has_claims_count=1,
                has_sitelinks_count=1,
                is_deleted=False,
                n1=NotabilityLevel.NONE,
                n2a=NotabilityLevel.NONE,
                n2b=NotabilityLevel.NONE,
                content_last_revid=456,
            ),
        ], {"Q10", "Q11"}

    monkeypatch.setattr(
        content_worker, "evaluate_content_many", fake_evaluate_content_many)

    batch, source_label, batch_staleness, batch_timings = await content_worker.work_content_pubsub_batch(batch_size=2)

    assert [item.qid for item in batch] == ["Q1", "Q2"]
    assert source_label == "pubsub"
    assert batch_staleness is None
    assert batch_timings["selection"] >= 0.0
    assert recorded_outlinks == [["Q10", "Q11"]]
    assert recorded_timestamps == [1000]


@pytest.mark.asyncio
async def test_content_worker_batch_added_trace_stays_item_scoped(monkeypatch):
    from wd_notability.content import worker as content_worker

    recorded_events: list[dict[str, object]] = []

    class FakeTrace:
        async def record_events(
            self,
            records,
        ):
            for record in records:
                recorded_events.append(
                    {
                        "qid": str(record.qid),
                        "event_type": record.event_type,
                        "worker_name": record.worker_name,
                        "batch_id": record.batch_id,
                        "details": dict(record.details or {}),
                    }
                )
            return len(records)

        async def flush(self):
            return 0

    class FakePubsub:
        async def count_pubsub_content_candidate_staleness_for_qids(self, qids):
            assert list(qids) == ["Q1", "Q2"]
            return {
                "total": 0,
                "never_evaluated": 0,
                "recent_changes_missing": 0,
                "recent_changes": 0,
                "redirect_target": 2,
                "deletion_events": 0,
                "content_policy": 0,
            }

    class FakeCache:
        item_trace = FakeTrace()
        pubsub = FakePubsub()
        async def upsert_content_many(self, updates):
            return [(update.qid, 1) for update in updates]

    async def fake_find_content_qids(batch_size):
        assert batch_size == 2
        return ["Q1", "Q2"]

    async def fake_evaluate_content_many(qids):
        return [
            content_worker.ContentUpdate(
                qid="Q1",
                is_redirect=False,
                has_claims_count=1,
                has_sitelinks_count=1,
                is_deleted=False,
                n1=NotabilityLevel.NONE,
                n2a=NotabilityLevel.NONE,
                n2b=NotabilityLevel.NONE,
                content_last_revid=123,
            ),
            content_worker.ContentUpdate(
                qid="Q2",
                is_redirect=False,
                has_claims_count=1,
                has_sitelinks_count=1,
                is_deleted=False,
                n1=NotabilityLevel.NONE,
                n2a=NotabilityLevel.NONE,
                n2b=NotabilityLevel.NONE,
                content_last_revid=456,
            ),
        ], set()

    async def fake_persist_content_chunk(chunk_updates, batch_timings, *, batch_id):
        return []

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "find_content_qids", fake_find_content_qids)
    monkeypatch.setattr(content_worker, "evaluate_content_many", fake_evaluate_content_many)
    monkeypatch.setattr(content_worker, "_persist_content_chunk", fake_persist_content_chunk)

    batch, source_label, batch_staleness, batch_timings = await content_worker.work_content_pubsub_batch(batch_size=2)

    assert [item.qid for item in batch] == ["Q1", "Q2"]
    assert source_label == "pubsub"
    assert batch_staleness is None
    assert batch_timings["selection"] >= 0.0
    assert [event["event_type"] for event in recorded_events] == ["batch_added", "batch_added"]
    assert [event["qid"] for event in recorded_events] == ["Q1", "Q2"]
    assert all(event["worker_name"] == "content" for event in recorded_events)
    assert all(event["details"] == {"batch_size": 2, "source": "pubsub"} for event in recorded_events)


@pytest.mark.asyncio
async def test_content_worker_batch_accepted_trace_and_inflight_release(monkeypatch):
    from wd_notability.content import worker as content_worker

    recorded_events: list[dict[str, object]] = []

    class FakeTrace:
        async def record_events(self, records):
            for record in records:
                recorded_events.append(
                    {
                        "qid": str(record.qid),
                        "event_type": record.event_type,
                        "worker_name": record.worker_name,
                        "batch_id": record.batch_id,
                        "details": dict(record.details or {}),
                    }
                )
            return len(records)

    class FakeCache:
        item_trace = FakeTrace()

    class FakeQueue:
        def __init__(self, batch):
            self._batch = batch
            self._count = 0
            self.maxsize = 2

        async def get(self):
            self._count += 1
            if self._count == 1:
                return self._batch
            raise asyncio.CancelledError

        def qsize(self):
            return 0

    batch = content_worker.ContentWorkBatch(
        qids=["Q1", "Q2"],
        batch_id="batch-1",
        source_label="pubsub",
        batch_timings=content_worker._empty_content_timings(),
    )

    async def fake_process_content_batch(batch_arg, *, add_new_cache_entries=False):
        assert batch_arg.qids == ["Q1", "Q2"]
        return [
            content_worker.ContentUpdate(
                qid="Q1",
                is_redirect=False,
                has_claims_count=1,
                has_sitelinks_count=1,
                is_deleted=False,
                n1=NotabilityLevel.NONE,
                n2a=NotabilityLevel.NONE,
                n2b=NotabilityLevel.NONE,
                content_last_revid=123,
            )
        ], "pubsub", {"total": 2}, content_worker._empty_content_timings()

    async def fake_record_throughput(batch_size):
        return f"throughput={batch_size:.2f} qid/s"

    async def fake_timing_snapshot(batch_timings):
        return "batch content timings: test"

    async def fake_emit_observability(*args, **kwargs):
        return None

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "_process_content_batch", fake_process_content_batch)
    monkeypatch.setattr(content_worker, "_record_content_throughput", fake_record_throughput)
    monkeypatch.setattr(content_worker, "_content_timing_snapshot", fake_timing_snapshot)
    monkeypatch.setattr(content_worker, "_emit_content_observability", fake_emit_observability)

    async with content_worker.CONTENT_INFLIGHT_LOCK:
        content_worker.CONTENT_INFLIGHT_QIDS.clear()
        content_worker.CONTENT_INFLIGHT_QIDS.update({"Q1", "Q2"})

    try:
        task = asyncio.create_task(
            content_worker.worker_loop(
                1,
                FakeQueue(batch),
                asyncio.Event(),
                poll_seconds=0.1,
                add_new_cache_entries=False,
            )
        )
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        async with content_worker.CONTENT_INFLIGHT_LOCK:
            assert content_worker.CONTENT_INFLIGHT_QIDS == set()

    assert [event["event_type"] for event in recorded_events] == ["batch_accepted", "batch_accepted"]
    assert [event["qid"] for event in recorded_events] == ["Q1", "Q2"]
    assert all(event["details"] == {"batch_size": 2, "source": "pubsub"} for event in recorded_events)


@pytest.mark.asyncio
async def test_content_worker_trace_throughput_uses_item_trace_window(monkeypatch):
    from wd_notability.content import worker as content_worker

    class FakeTrace:
        async def count_events(self, **kwargs):
            assert kwargs["worker_names"] == ["content"]
            assert kwargs["event_types"] == ["results_written"]
            assert kwargs["since"] == 70
            return 60

    class FakeCache:
        item_trace = FakeTrace()

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "ITEM_TRACE_ENABLED", True)
    monkeypatch.setattr(content_worker.time, "time", lambda: 100.0)
    monkeypatch.setattr(content_worker, "CONTENT_THROUGHPUT_TRACE_WINDOW_SECONDS", 30.0)

    async with content_worker.CONTENT_THROUGHPUT_LOCK:
        content_worker.CONTENT_THROUGHPUT_STARTED_AT = None
        content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 0
        content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.clear()

    try:
        throughput_text = await content_worker._record_content_throughput(5)
        assert throughput_text == "throughput=2.00 qid/s"
    finally:
        async with content_worker.CONTENT_THROUGHPUT_LOCK:
            content_worker.CONTENT_THROUGHPUT_STARTED_AT = None
            content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 0
            content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.clear()


@pytest.mark.asyncio
async def test_content_dispatcher_logs_lifecycle(monkeypatch, caplog):
    from wd_notability.content import worker as content_worker

    caplog.set_level("INFO", logger=content_worker.__name__)

    selected_batches = [["Q1", "Q2"]]
    call_count = 0
    recorded_batches: list[content_worker.ContentWorkBatch] = []

    class FakeQueue:
        maxsize = 2

        def qsize(self):
            return len(recorded_batches)

        def put_nowait(self, batch):
            recorded_batches.append(batch)

    class FakeEvent:
        async def wait(self):
            return None

        def clear(self):
            return None

    async def fake_wait_for(awaitable, timeout):
        del timeout
        return await awaitable

    async def fake_select(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return selected_batches, 0.25
        raise asyncio.CancelledError

    async def fake_noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(content_worker.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setattr(content_worker, "_select_content_batches", fake_select)
    monkeypatch.setattr(content_worker, "_reserve_content_qids", fake_noop)
    monkeypatch.setattr(content_worker, "_release_content_qids", fake_noop)
    monkeypatch.setattr(content_worker, "_record_content_batch_events", fake_noop)

    with pytest.raises(asyncio.CancelledError):
        await content_worker._dispatcher_loop(FakeQueue(), FakeEvent(), poll_seconds=0.1)

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("Content dispatcher wake: reason=event") for message in messages)
    assert any(
        "Content dispatcher selected 2 qid(s) into 1 batch(es)" in message
        for message in messages
    )
    assert any(
        message.startswith("Content dispatcher enqueued batch ")
        for message in messages
    )


@pytest.mark.asyncio
async def test_content_deletion_monitor_records_delete_and_restore_events(monkeypatch):
    from wd_notability.content import deletion as deletion_worker

    rows = [
        (100, b"19700101000001", "Q1", "delete"),
        (101, b"19700101000002", "Q1", "restore"),
    ]
    recorded_events: list[tuple[str, int, str, int]] = []
    saved_cursor: list[int] = []

    class FakeCursor:
        def __init__(self, fetch_rows):
            self._fetch_rows = fetch_rows

        def execute(self, query, params):
            return None

        def fetchall(self):
            return list(self._fetch_rows)

        def fetchone(self):
            return (0,)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor(rows)

        def close(self):
            return None

    class FakeSource:
        _replica_config = SimpleNamespace(enabled=True)

        def _connect_replica(self):
            return FakeConnection()

    async def fake_load_cursor():
        return 0

    async def fake_save_cursor(log_id):
        saved_cursor.append(log_id)

    class FakeCache:
        async def upsert_content_deletion_events(self, events):
            recorded_events.extend(events)
            return len(events)

    monkeypatch.setattr(deletion_worker, "CONTENT_SOURCE", FakeSource())
    monkeypatch.setattr(
        deletion_worker, "_load_deletion_log_cursor", fake_load_cursor)
    monkeypatch.setattr(
        deletion_worker, "_save_deletion_log_cursor", fake_save_cursor)
    monkeypatch.setattr(deletion_worker, "CACHE", FakeCache())

    batch, source_label = await deletion_worker.work_content_deletion_monitor_batch(batch_size=10)

    assert batch == ["Q1"]
    assert source_label == "deletion log 1970-01-01 00:00:01 UTC to 1970-01-01 00:00:02 UTC"
    assert recorded_events == [
        ("Q1", 100, "delete", 1_000_000),
        ("Q1", 101, "undelete", 2_000_000),
    ]
    assert saved_cursor == [101]


@pytest.mark.asyncio
async def test_content_fetcher_overlaps_chunk_requests(monkeypatch):
    from wd_notability.content import fetcher as content_fetcher

    source = content_fetcher._ContentFetcher(name="content", detectors=set())
    source._replica_config = SimpleNamespace(enabled=False)

    first_chunk_started = asyncio.Event()
    second_chunk_started = asyncio.Event()
    release_first_chunk = asyncio.Event()
    started_chunks: list[list[str]] = []

    async def fake_get_with_timings(url, *, params=None, data=None, max_attempts=5):
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

    monkeypatch.setattr(content_fetcher.wikidata_session,
                        "get_with_timings", fake_get_with_timings)
    monkeypatch.setattr(content_fetcher.wikidata_session,
                        "timing_snapshot", fake_timing_snapshot)

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


def test_content_fetcher_replica_connection_is_fresh_and_reset_safe(monkeypatch):
    from wd_notability.content import fetcher as content_fetcher

    created: list[object] = []

    class FakeConnection:
        def close(self):
            return None

    def fake_connect_replica(*args, **kwargs):
        conn = FakeConnection()
        created.append(conn)
        return conn

    monkeypatch.setattr(content_fetcher, "connect_replica",
                        fake_connect_replica)

    source = content_fetcher._ContentFetcher(name="content", detectors=set())
    source._replica_config = SimpleNamespace(
        enabled=True,
        defaults_file=SimpleNamespace(exists=lambda: True),
        host="localhost",
        port=3306,
        database="wikidatawiki_p",
    )
    monkeypatch.setattr(source, "_pymysql_module", lambda: object())

    first = source._get_replica_connection()
    second = source._get_replica_connection()

    assert first is not second
    assert len(created) == 2

    source._reset_replica_connection()

    third = source._get_replica_connection()

    assert third is not first
    assert third is not second
    assert len(created) == 3

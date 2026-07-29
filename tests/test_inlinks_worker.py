import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_inlinks_worker_queue_stats_returns_priority_depths(monkeypatch):
    from wd_notability.inlinks import worker as inlinks_module

    class FakeCache:
        class interest:
            @staticmethod
            async def count_interest_inlinks_targets():
                return 11

    monkeypatch.setattr(inlinks_module, "cache", FakeCache())

    stats = await inlinks_module.queue_stats()

    assert stats == {"total": 11}


@pytest.mark.asyncio
async def test_inlinks_worker_work_pass_forwards_limit(monkeypatch):
    from wd_notability.inlinks import worker as inlinks_module

    recorded = {}

    async def fake_run_inlinks_pass(*, limit=None):
        recorded["limit"] = limit
        return 7

    monkeypatch.setattr(inlinks_module, "_run_inlinks_pass", fake_run_inlinks_pass)

    processed = await inlinks_module.work_inlinks_pass(batch_size=25, limit=9)

    assert processed == 7
    assert recorded["limit"] == 9


@pytest.mark.asyncio
async def test_inlinks_worker_evaluate_many_delegates_to_pass(monkeypatch):
    from wd_notability.inlinks import worker as inlinks_module

    recorded = {}

    async def fake_run_inlinks_pass(*, limit=None):
        recorded["limit"] = limit
        return 3

    monkeypatch.setattr(inlinks_module, "_run_inlinks_pass", fake_run_inlinks_pass)

    processed, finalized = await inlinks_module.evaluate_inlinks_many(["Q1", "bad", "Q2"])

    assert processed == 3
    assert finalized == 0
    assert recorded["limit"] == 2


def test_inlinks_worker_loop_delegates_to_pipeline(monkeypatch):
    import datetime

    if not hasattr(datetime, "UTC"):
        datetime.UTC = datetime.timezone.utc

    from wd_notability.inlinks import worker as inlinks_module

    called = {}

    class DummyLock:
        def __enter__(self):
            called["locked"] = True
            return self

        def __exit__(self, exc_type, exc, tb):
            called["unlocked"] = True
            return False

    async def fake_run_inlinks_pipeline():
        called["pipeline"] = True

    monkeypatch.setattr(
        inlinks_module,
        "acquire_file_lock",
        lambda *_args, **_kwargs: DummyLock(),
    )
    monkeypatch.setattr(inlinks_module, "_run_inlinks_pipeline", fake_run_inlinks_pipeline)

    asyncio.run(inlinks_module.inlinks_worker_loop(batch_size=3, run_interval_seconds=7.0))

    assert called["locked"] is True
    assert called["pipeline"] is True
    assert called["unlocked"] is True


def test_inlinks_blackboard_skips_graph_fetch_for_zero_counts():
    from wd_notability.inlinks.blackboard import InlinksBlackboard

    board = InlinksBlackboard()
    board.apply_interest(["Q1"], observed_at=100)
    board.record_count("Q1", 0, observed_at=101)

    assert board.graph_candidates(stale_after_seconds=0) == []
    assert [entry.qid for entry in board.evaluation_candidates(stale_after_seconds=0)] == ["Q1"]


def test_inlinks_blackboard_evaluation_candidates_prioritize_never_then_unknown_then_resolved(monkeypatch):
    from wd_notability.inlinks import blackboard as blackboard_module
    from wd_notability.inlinks.blackboard import InlinksBlackboard
    from wd_notability.models import NotabilityLevel

    monkeypatch.setattr(blackboard_module, "time", lambda: 1000.0)

    board = InlinksBlackboard()
    board.apply_interest(["Q1", "Q2", "Q3"], observed_at=100)
    board.record_count("Q1", 3, observed_at=101)
    board.record_graph("Q1", ["Q10"], observed_at=102)

    board.record_count("Q2", 3, observed_at=103)
    board.record_graph("Q2", ["Q11"], observed_at=104)
    board._entries["Q2"] = replace(
        board._entries["Q2"],
        evaluated_at=105,
        inlinks_count=3,
    )
    board._entries["Q2"] = replace(
        board._entries["Q2"],
        n3_inlinks=NotabilityLevel.UNKNOWN,
    )

    board.record_count("Q3", 3, observed_at=106)
    board.record_graph("Q3", ["Q12"], observed_at=107)
    board._entries["Q3"] = replace(
        board._entries["Q3"],
        evaluated_at=108,
        inlinks_count=3,
    )

    assert [entry.qid for entry in board.evaluation_candidates(stale_after_seconds=2)] == [
        "Q1",
        "Q2",
        "Q3",
    ]


@pytest.mark.asyncio
async def test_inlinks_interest_fetcher_traces_new_and_leaving_qids(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.inlinks.blackboard import InlinksBoardEntry

    captured: list[list[object]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeCache:
        item_trace = FakeTrace()

    pipeline = inlinks_pipeline.InlinksPipeline()
    pipeline.blackboard._entries["Q1"] = InlinksBoardEntry(qid="Q1", interest_seen_at=90)

    async def fake_list_interest_inlinks_rows():
        return [("Q2", 0, None, None, None)]

    async def fake_sleep_or_stop(seconds):
        pipeline._stop.set()
        return 0

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "_list_interest_inlinks_rows", fake_list_interest_inlinks_rows)
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 100.0)
    monkeypatch.setattr(pipeline, "_sleep_or_stop", fake_sleep_or_stop)

    await pipeline.interest_fetcher()

    assert [[record.event_type for record in batch] for batch in captured] == [
        ["interest_started"],
        ["interest_expired"],
    ]
    assert captured[0][0].qid == "Q2"
    assert captured[1][0].qid == "Q1"


@pytest.mark.asyncio
async def test_inlinks_pipeline_tight_n12_helper_uses_only_content_and_inlinks_tables(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    class _FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return list(self._rows)

    class _FakeDB:
        def __init__(self):
            self.executed = []

        async def execute(self, sql, params=None):
            self.executed.append((sql, tuple(params or ())))
            return _FakeCursor([
                (1, 1, 1, 0, 0),
                (2, None, None, None, None),
            ])

    class _FakeCache:
        def __init__(self, db):
            self._db = db

        def _parse_qid(self, qid):
            return int(qid[1:])

        @asynccontextmanager
        async def _connect(self):
            yield self._db

    db = _FakeDB()
    monkeypatch.setattr(inlinks_pipeline, "cache", _FakeCache(db))

    levels = await inlinks_pipeline._get_inlinks_n12_many(["Q1", "Q2"])

    assert levels == {
        "Q1": NotabilityLevel.WEAK,
        "Q2": NotabilityLevel.UNKNOWN,
    }
    sql, params = db.executed[0]
    assert "LEFT JOIN content_evaluation ce" in sql
    assert "recent_changes_cache" not in sql
    assert "osm_usage" not in sql
    assert "sdc_usage" not in sql
    assert "wiki_subscribers" not in sql
    assert params == (1, 2)


def test_inlinks_pipeline_records_results_written_trace(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    captured_records: list[list[object]] = []
    flushed: list[int] = []

    class FakeTrace:
        async def record_events(self, records):
            captured_records.append(list(records))
            return len(records)

        async def flush(self):
            flushed.append(1)
            return 0

    class FakeCache:
        item_trace = FakeTrace()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, 1) for update in updates]

        async def delete_inlinks_many(self, qids):
            return None

    async def fake_wait_or_tick(self, event, seconds):
        self._stop.set()
        return False

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)
    monkeypatch.setattr(inlinks_pipeline.InlinksPipeline, "_wait_or_tick", fake_wait_or_tick)

    async def fake_get_inlinks_n12_many(qids):
        return {"Q1": NotabilityLevel.UNKNOWN}

    monkeypatch.setattr(inlinks_pipeline, "_get_inlinks_n12_many", fake_get_inlinks_n12_many)

    pipeline = inlinks_pipeline.InlinksPipeline()
    pipeline.blackboard.evaluation_candidates = lambda **_kwargs: [
        SimpleNamespace(
            qid="Q42",
            inlinks=("Q1",),
            inlinks_count=3,
            evaluated_at=None,
        )
    ]

    stats = asyncio.run(pipeline.evaluator())

    assert stats is None
    assert [batch[0].event_type for batch in captured_records] == [
        "evaluation_attempted",
        "results_written",
    ]
    record = captured_records[1][0]
    assert record.worker_name == "inlinks"
    assert record.event_type == "results_written"
    assert record.qid == "Q42"
    assert record.details is not None
    assert record.details["n3_inlinks_label"] == "unknown"
    assert record.details["inlinks_count"] == 3
    assert record.details["best_level_label"] == "none"
    assert record.details["unknown_inlinks"] == ["Q1"]
    assert len(flushed) == 2


@pytest.mark.asyncio
async def test_inlinks_pass_traces_stage_work_and_unknown_evaluation(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    captured_records: list[list[object]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured_records.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return list(self._rows)

    class FakeDB:
        async def execute(self, sql, params=None):
            return FakeCursor([(42, 0, 0, 0, None)])

    class FakeCache:
        item_trace = FakeTrace()

        async def initialize(self):
            return None

        @asynccontextmanager
        async def _connect(self):
            yield FakeDB()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, 1) for update in updates]

    class FakeSource:
        async def count_inlinks(self, qids):
            return {"Q42": 3}, {}

        async def get_contexts(self, qids):
            return {"Q42": {"inlinks": ["Q1"], "truncated": False}}

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "INLINKS_SOURCE", FakeSource())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)

    async def fake_get_inlinks_n12_many(qids):
        return {"Q1": NotabilityLevel.UNKNOWN}

    monkeypatch.setattr(inlinks_pipeline, "_get_inlinks_n12_many", fake_get_inlinks_n12_many)

    processed = await inlinks_pipeline.run_inlinks_pass()

    assert processed == 1
    assert [batch[0].event_type for batch in captured_records] == [
        "interest_started",
        "work_claimed",
        "count_fetched",
        "work_claimed",
        "graph_fetched",
        "evaluation_attempted",
        "results_written",
    ]
    start_record = captured_records[0][0]
    assert start_record.details == {
        "interest_type": "inlinks",
        "source": "interest",
        "prior_inlinks_count": 0,
        "prior_n3_inlinks": 0,
        "prior_n3_inlinks_label": "none",
    }
    assert captured_records[2][0].details is not None
    assert captured_records[2][0].details["count"] == 3
    assert captured_records[5][0].details is not None
    assert captured_records[5][0].details["loaded_inlinks_count"] == 1
    unknown_record = captured_records[-1][0]
    assert unknown_record.details is not None
    assert unknown_record.details["best_level_label"] == "none"


@pytest.mark.asyncio
async def test_inlinks_pass_marks_truncated_graph_unknown(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    captured_records: list[list[object]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured_records.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return list(self._rows)

    class FakeDB:
        async def execute(self, sql, params=None):
            return FakeCursor([(42, 1, None, None, None)])

    class FakeCache:
        item_trace = FakeTrace()

        async def initialize(self):
            return None

        @asynccontextmanager
        async def _connect(self):
            yield FakeDB()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, 1) for update in updates]

    class FakeSource:
        async def count_inlinks(self, qids):
            return {"Q42": 1}, {}

        async def get_contexts(self, qids):
            return {"Q42": {"inlinks": ["Q1"], "truncated": True}}

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "INLINKS_SOURCE", FakeSource())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)

    async def fake_get_inlinks_n12_many(qids):
        return {"Q1": NotabilityLevel.WEAK}

    monkeypatch.setattr(inlinks_pipeline, "_get_inlinks_n12_many", fake_get_inlinks_n12_many)

    processed = await inlinks_pipeline.run_inlinks_pass()

    assert processed == 1
    result_record = captured_records[-1][0]
    assert result_record.details is not None
    assert result_record.details["n3_inlinks_label"] == "unknown"
    assert result_record.details["best_level_label"] == "weak"


@pytest.mark.asyncio
async def test_inlinks_pass_uses_unified_graph_queue_order(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    context_calls: list[list[str]] = []

    class FakeTrace:
        async def record_events(self, records):
            return len(records)

        async def flush(self):
            return 0

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return list(self._rows)

    class FakeDB:
        async def execute(self, sql, params=None):
            return FakeCursor([(2000, 0, None, None, None), (3, 0, None, None, None)])

    class FakeCache:
        item_trace = FakeTrace()

        async def initialize(self):
            return None

        @asynccontextmanager
        async def _connect(self):
            yield FakeDB()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, 1) for update in updates]

    class FakeSource:
        async def count_inlinks(self, qids):
            return {"Q2000": 1201, "Q3": 3}, {}

        async def get_contexts(self, qids):
            context_calls.append(list(qids))
            return {qid: {"inlinks": ["Q1"], "truncated": False} for qid in qids}

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "INLINKS_SOURCE", FakeSource())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)

    async def fake_get_inlinks_n12_many(qids):
        return {"Q1": NotabilityLevel.UNKNOWN}

    monkeypatch.setattr(inlinks_pipeline, "_get_inlinks_n12_many", fake_get_inlinks_n12_many)

    processed = await inlinks_pipeline.run_inlinks_pass()

    assert processed == 2
    assert context_calls == [["Q3", "Q2000"]]


@pytest.mark.asyncio
async def test_inlinks_graph_fetcher_updates_counts_from_graph(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    captured_records: list[list[object]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured_records.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": {"inlinks": ["Q9", "Q8"]},
                "Q2": {"inlinks": []},
            }

    class FakeCache:
        item_trace = FakeTrace()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, int(update.n3_inlinks)) for update in updates]

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "INLINKS_SOURCE", FakeSource())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)

    pipeline = inlinks_pipeline.InlinksPipeline()
    pipeline.blackboard.apply_interest(["Q1", "Q2"], observed_at=100)
    pipeline.blackboard.record_count("Q1", 1, observed_at=101)
    pipeline.blackboard.record_count("Q2", 5, observed_at=101)

    changed, refreshed, calls, empty_qids = await inlinks_pipeline._fetch_graph_candidates(
        pipeline,
        ["Q1", "Q2"],
    )

    assert changed is True
    assert refreshed == 2
    assert calls == 1
    assert empty_qids == ["Q2"]
    assert pipeline.blackboard.snapshot()["Q1"].inlinks_count == 2
    assert pipeline.blackboard.snapshot()["Q1"].graph_fetched_at == 1000
    assert pipeline.blackboard.snapshot()["Q2"].inlinks_count == 0
    assert pipeline.blackboard.snapshot()["Q2"].n3_inlinks == NotabilityLevel.NONE
    assert [batch[0].event_type for batch in captured_records] == [
        "work_claimed",
        "results_written",
        "graph_fetched",
        "graph_fetched",
    ]
    assert captured_records[2][0].details == {
        "graph_size": 2,
        "inlinks": ["Q9", "Q8"],
    }
    assert captured_records[3][0].details == {
        "graph_size": 0,
        "empty_graph": True,
    }


@pytest.mark.asyncio
async def test_inlinks_graph_fetcher_preserves_count_when_truncated(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline
    from wd_notability.models import NotabilityLevel

    captured_records: list[list[object]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured_records.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": {"inlinks": [f"Q{i}" for i in range(1000)], "truncated": True},
            }

    class FakeCache:
        item_trace = FakeTrace()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, int(update.n3_inlinks)) for update in updates]

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "INLINKS_SOURCE", FakeSource())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)

    pipeline = inlinks_pipeline.InlinksPipeline()
    pipeline.blackboard.apply_interest(["Q1"], observed_at=100)
    pipeline.blackboard.record_count("Q1", 3803986, observed_at=101)

    changed, refreshed, calls, empty_qids = await inlinks_pipeline._fetch_graph_candidates(
        pipeline,
        ["Q1"],
    )

    assert changed is True
    assert refreshed == 1
    assert calls == 1
    assert empty_qids == []
    snapshot = pipeline.blackboard.snapshot()["Q1"]
    assert snapshot.inlinks_count == 3803986
    assert snapshot.graph_fetched_at == 1000
    assert snapshot.graph_truncated is True
    assert snapshot.n3_inlinks is None
    assert [batch[0].event_type for batch in captured_records] == [
        "work_claimed",
        "graph_fetched",
    ]
    assert captured_records[1][0].details == {
        "graph_size": 1000,
        "inlinks": [f"Q{i}" for i in range(1000)],
    }


@pytest.mark.asyncio
async def test_inlinks_pass_emits_results_written_for_zero_count(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline

    captured_records: list[list[object]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured_records.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return list(self._rows)

    class FakeDB:
        async def execute(self, sql, params=None):
            return FakeCursor([(42, 0, None, None, None)])

    class FakeCache:
        item_trace = FakeTrace()

        async def initialize(self):
            return None

        @asynccontextmanager
        async def _connect(self):
            yield FakeDB()

        async def upsert_inlinks_many(self, updates):
            return [(update.qid, 1) for update in updates]

    class FakeSource:
        async def count_inlinks(self, qids):
            return {"Q42": 0}, {}

        async def get_contexts(self, qids):
            raise AssertionError("zero-count items should not fetch inlinks")

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(inlinks_pipeline, "INLINKS_SOURCE", FakeSource())
    monkeypatch.setattr(inlinks_pipeline, "time", lambda: 1000.0)

    async def fake_get_inlinks_n12_many(qids):
        return {}

    monkeypatch.setattr(inlinks_pipeline, "_get_inlinks_n12_many", fake_get_inlinks_n12_many)

    processed = await inlinks_pipeline.run_inlinks_pass()

    assert processed == 1
    assert [batch[0].event_type for batch in captured_records] == [
        "interest_started",
        "work_claimed",
        "results_written",
    ]
    result_record = captured_records[-1][0]
    assert result_record.details is not None
    assert result_record.details["n3_inlinks_label"] == "none"
    assert result_record.details["inlinks_count"] == 0


@pytest.mark.asyncio
async def test_inlinks_interest_publisher_marks_inlinks_interest(monkeypatch):
    from wd_notability.inlinks import pipeline as inlinks_pipeline

    recorded: list[dict[str, object]] = []
    trace_records: list[list[object]] = []

    class FakeSession:
        async def replace(self, qids):
            recorded.append({"replace": list(qids)})

        async def clear(self):
            recorded.append({"clear": True})

        async def close(self):
            recorded.append({"session_closed": True})

    class FakeManager:
        def create_session(self):
            return FakeSession()

        async def close(self):
            recorded.append({"manager_closed": True})

    class FakeInterest:
        async def create_interest_manager(self, **kwargs):
            recorded.append(dict(kwargs))
            return FakeManager()

    class FakeCache:
        class FakeTrace:
            async def record_events(self, records):
                trace_records.append(list(records))
                return len(records)

            async def flush(self):
                return 0

        interest = FakeInterest()
        item_trace = FakeTrace()

    pipeline = inlinks_pipeline.InlinksPipeline()
    pipeline.blackboard.interest_candidates = lambda **_kwargs: ["Q1", "Q2"]  # type: ignore[assignment]
    pipeline.blackboard.apply_interest(["Q1", "Q2"], observed_at=100)
    pipeline.blackboard.record_graph("Q1", ["Q2"], observed_at=101)

    async def fake_sleep_or_stop(seconds):
        pipeline._stop.set()

    monkeypatch.setattr(inlinks_pipeline, "cache", FakeCache())
    monkeypatch.setattr(pipeline, "_sleep_or_stop", fake_sleep_or_stop)

    await pipeline.interest_publisher()

    assert recorded == [
        {
            "worker_id": "inlinks",
            "priority": 1,
            "wants_content": True,
            "wants_inlinks": False,
        },
        {"replace": ["Q2"]},
        {"session_closed": True},
        {"manager_closed": True},
    ]
    assert len(trace_records) == 1
    assert [record.event_type for record in trace_records[0]] == [
        "interest_published",
    ]
    assert [record.qid for record in trace_records[0]] == ["Q1"]
    assert trace_records[0][0].details == {
        "published_qid_count": 1,
        "published_qids": ["Q2"],
    }

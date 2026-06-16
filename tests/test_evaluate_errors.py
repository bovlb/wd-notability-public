import pytest
import asyncio

from wd_notability.models import Detector, EvaluationResult, NotabilityCriterion, NotabilityLevel, Source
from wd_notability.sources import EntityDataSource
from wd_notability.wikidata import EntityDeletedError


class GoodDetector(Detector):
    def __init__(self) -> None:
        super().__init__("good", NotabilityCriterion.N1)

    async def detect(self, entity: dict):
        yield self.make_signal(
            level=NotabilityLevel.WEAK,
            key="ok",
            properties={"entity": entity.get("id")},
        )


class FailingDetector(Detector):
    def __init__(self) -> None:
        super().__init__("failing", NotabilityCriterion.N2a)

    async def detect(self, entity: dict):
        raise RuntimeError("boom")
        yield entity


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeTimings:
    def as_dict(self, prefix):
        return {prefix: 0.0}


def make_fake_get_with_timings(*, entities=None, deleted_qids=()):
    entities = entities or {}
    deleted_qids = set(deleted_qids)

    async def fake_get_with_timings(url, limiter=None, params=None):
        qids = (params or {}).get("ids", "").split("|")
        payload_entities = {}
        for qid in qids:
            if not qid:
                continue
            if qid in deleted_qids:
                raise EntityDeletedError(qid)
            payload_entities[qid] = entities.get(
                qid,
                {
                    "id": qid,
                    "claims": {},
                    "sitelinks": {},
                },
            )
        return FakeResponse({"entities": payload_entities}), FakeTimings()

    return fake_get_with_timings


@pytest.mark.asyncio
async def test_evaluate_many_collects_detector_errors(monkeypatch):
    from wd_notability import evaluate as evaluate_module

    class BatchSource(Source):
        async def get_contexts(self, qids):
            return {qid: {"id": qid, "claims": {}, "sitelinks": {}} for qid in qids}

    result = (
        await evaluate_module.evaluate_many(
            ["Q42"],
            sources=[BatchSource(name="batch", detectors={GoodDetector(), FailingDetector()})],
            stop_on_strong=False,
            update_cache=False,
        )
    )["Q42"]

    assert result.n1 == NotabilityLevel.WEAK
    assert len(result.signals) == 1
    assert result.errors["N2a"] == ["failing: boom"]
    assert result.errors["N1"] == []


@pytest.mark.asyncio
async def test_evaluate_handles_redirect_targets(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(
            entities={
                "Q42": {
                    "id": "Q7",
                    "claims": {"P31": [{}]},
                    "sitelinks": {"enwiki": {}},
                    "redirect": {"to": "Q7"},
                }
            }
        ),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [EntityDataSource(name="entity_data", detectors={GoodDetector()})],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = (
        await evaluate_module.evaluate_many(
            ["Q42"],
            sources=evaluate_module.SOURCES,
            stop_on_strong=True,
            update_cache=False,
        )
    )["Q42"]

    assert result.qid == "Q42"
    assert result.is_redirect is True
    assert result.has_claims is True
    assert result.has_sitelinks is True
    assert result.n1 == NotabilityLevel.WEAK


@pytest.mark.asyncio
async def test_evaluate_runs_sources_in_order(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    calls = []

    class FirstDetector(Detector):
        def __init__(self) -> None:
            super().__init__("first", NotabilityCriterion.N1)

        async def detect(self, entity: dict):
            calls.append("first")
            yield self.make_signal(level=NotabilityLevel.WEAK, key="first")

    class SecondDetector(Detector):
        def __init__(self) -> None:
            super().__init__("second", NotabilityCriterion.N3_INLINKS)

        async def detect(self, entity: dict):
            calls.append("second")
            yield self.make_signal(level=NotabilityLevel.WEAK, key="second")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [
            EntityDataSource(name="entity_data", detectors={FirstDetector()}),
            Source(name="second_source", detectors={SecondDetector()}),
        ],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    await evaluate_module.evaluate_many(
        ["Q42"],
        sources=evaluate_module.SOURCES,
        stop_on_strong=True,
        update_cache=False,
    )

    assert calls == ["first", "second"]


@pytest.mark.asyncio
async def test_evaluate_stops_sources_after_strong_result(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    calls = []

    class StrongDetector(Detector):
        def __init__(self) -> None:
            super().__init__("strong", NotabilityCriterion.N1)

        async def detect(self, entity: dict):
            calls.append("strong")
            yield self.make_signal(level=NotabilityLevel.STRONG, key="strong")

    class LaterDetector(Detector):
        def __init__(self) -> None:
            super().__init__("later", NotabilityCriterion.N3_INLINKS)

        async def detect(self, entity: dict):
            calls.append("later")
            yield self.make_signal(level=NotabilityLevel.WEAK, key="later")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [
            EntityDataSource(name="entity_data", detectors={StrongDetector()}),
            Source(name="later_source", detectors={LaterDetector()}),
        ],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = (
        await evaluate_module.evaluate_many(
            ["Q42"],
            sources=evaluate_module.SOURCES,
            stop_on_strong=True,
            update_cache=False,
        )
    )["Q42"]

    assert result.n == NotabilityLevel.STRONG
    assert result.n3 == NotabilityLevel.UNKNOWN
    assert calls == ["strong"]


@pytest.mark.asyncio
async def test_evaluate_marks_skipped_non_strong_criteria_unknown(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    calls = []

    class StrongAndWeakDetector(Detector):
        def __init__(self) -> None:
            super().__init__("strong_and_weak", NotabilityCriterion.N1)

        async def detect(self, entity: dict):
            calls.append("strong_and_weak")
            yield self.make_signal(level=NotabilityLevel.STRONG, key="strong")

    class WeakDetector(Detector):
        def __init__(self) -> None:
            super().__init__("weak", NotabilityCriterion.N2a)

        async def detect(self, context):
            calls.append("weak")
            yield self.make_signal(level=NotabilityLevel.WEAK, key="weak")

    class SkippedSameCriterionDetector(Detector):
        def __init__(self) -> None:
            super().__init__("skipped_same", NotabilityCriterion.N2a)

        async def detect(self, context):
            calls.append("skipped_same")
            yield self.make_signal(level=NotabilityLevel.STRONG, key="skipped_same")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [
            EntityDataSource(
                name="entity_data",
                detectors={StrongAndWeakDetector(), WeakDetector()},
            ),
            Source(name="later_source", detectors={SkippedSameCriterionDetector()}),
        ],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = (
        await evaluate_module.evaluate_many(
            ["Q42"],
            sources=evaluate_module.SOURCES,
            stop_on_strong=True,
            update_cache=False,
        )
    )["Q42"]

    assert result.n == NotabilityLevel.STRONG
    assert result.n1 == NotabilityLevel.STRONG
    assert result.n2a == NotabilityLevel.UNKNOWN
    assert set(calls) == {"strong_and_weak", "weak"}


@pytest.mark.asyncio
async def test_evaluate_full_runs_sources_after_strong_result(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    calls = []

    class StrongDetector(Detector):
        def __init__(self) -> None:
            super().__init__("strong", NotabilityCriterion.N1)

        async def detect(self, entity: dict):
            calls.append("strong")
            yield self.make_signal(level=NotabilityLevel.STRONG, key="strong")

    class LaterDetector(Detector):
        def __init__(self) -> None:
            super().__init__("later", NotabilityCriterion.N3_INLINKS)

        async def detect(self, entity: dict):
            calls.append("later")
            yield self.make_signal(level=NotabilityLevel.WEAK, key="later")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [
            EntityDataSource(name="entity_data", detectors={StrongDetector()}),
            Source(name="later_source", detectors={LaterDetector()}),
        ],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = await evaluate_module.evaluate_full("Q42")

    assert result.n == NotabilityLevel.STRONG
    assert result.n3 == NotabilityLevel.WEAK
    assert calls == ["strong", "later"]


@pytest.mark.asyncio
async def test_evaluate_updates_cache_after_entity_data_criteria(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    upserts = []

    class N1Detector(Detector):
        def __init__(self) -> None:
            super().__init__("n1", NotabilityCriterion.N1)

        async def detect(self, entity: dict):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="n1")

    class N2aDetector(Detector):
        def __init__(self) -> None:
            super().__init__("n2a", NotabilityCriterion.N2a)

        async def detect(self, entity: dict):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="n2a")

    class N2bDetector(Detector):
        def __init__(self) -> None:
            super().__init__("n2b", NotabilityCriterion.N2b)

        async def detect(self, entity: dict):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="n2b")

    class N3Detector(Detector):
        def __init__(self) -> None:
            super().__init__("n3", NotabilityCriterion.N3_INLINKS)

        async def detect(self, context):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="n3")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        upserts.append(EvaluationResult.from_summary(qid, summary))

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [
            EntityDataSource(
                name="entity_data",
                detectors={N1Detector(), N2aDetector(), N2bDetector()},
            ),
            Source(name="later_source", detectors={N3Detector()}),
        ],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = await evaluate_module.evaluate_full("Q42")

    assert len(upserts) == 2
    assert upserts[0].n1 == NotabilityLevel.WEAK
    assert upserts[0].n2a == NotabilityLevel.WEAK
    assert upserts[0].n2b == NotabilityLevel.WEAK
    assert upserts[0].n3 == NotabilityLevel.UNKNOWN
    assert upserts[1].n3 == NotabilityLevel.WEAK
    assert result.n3 == NotabilityLevel.WEAK


@pytest.mark.asyncio
async def test_evaluate_sources_iter_yields_source_progress(monkeypatch):
    from wd_notability import evaluate as evaluate_module

    class FirstDetector(Detector):
        def __init__(self) -> None:
            super().__init__("first", NotabilityCriterion.N1)

        async def detect(self, context):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="first")

    class SecondDetector(Detector):
        def __init__(self) -> None:
            super().__init__("second", NotabilityCriterion.N3_INLINKS)

        async def detect(self, context):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="second")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    events = [
        event async for event in evaluate_module.evaluate_sources_iter(
            "Q42",
            sources=[
                Source(name="first_source", detectors={FirstDetector()}),
                Source(name="second_source", detectors={SecondDetector()}),
            ],
            stop_on_strong=False,
            update_cache=True,
        )
    ]

    assert [event["event"] for event in events] == [
        "source_started",
        "cache_updated",
        "source_completed",
        "source_started",
        "cache_updated",
        "source_completed",
        "completed",
    ]
    assert events[0]["source"] == "first_source"
    assert events[0]["criteria"] == ["N1"]
    assert events[3]["source"] == "second_source"
    assert events[-1]["result"].n3 == NotabilityLevel.WEAK


@pytest.mark.asyncio
async def test_evaluate_sources_iter_runs_sources_in_parallel(monkeypatch):
    from wd_notability import evaluate as evaluate_module

    first_can_finish = asyncio.Event()

    class FirstDetector(Detector):
        def __init__(self) -> None:
            super().__init__("first", NotabilityCriterion.N1)

        async def detect(self, context):
            await first_can_finish.wait()
            yield self.make_signal(level=NotabilityLevel.WEAK, key="first")

    class SecondDetector(Detector):
        def __init__(self) -> None:
            super().__init__("second", NotabilityCriterion.N3_INLINKS)

        async def detect(self, context):
            yield self.make_signal(level=NotabilityLevel.WEAK, key="second")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    events = []
    async for event in evaluate_module.evaluate_sources_iter(
        "Q42",
        sources=[
            Source(name="first_source", detectors={FirstDetector()}),
            Source(name="second_source", detectors={SecondDetector()}),
        ],
        stop_on_strong=False,
        update_cache=True,
        parallel=True,
    ):
        events.append(event)
        if event["event"] == "source_completed" and event["source"] == "second_source":
            first_can_finish.set()

    assert [event["event"] for event in events[:2]] == ["source_started", "source_started"]
    assert [event["source"] for event in events[:2]] == ["first_source", "second_source"]
    assert [event["source"] for event in events if event["event"] == "source_completed"] == [
        "second_source",
        "first_source",
    ]
    assert events[-1]["result"].n3 == NotabilityLevel.WEAK


@pytest.mark.asyncio
async def test_source_report_urls_are_included_in_results(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(),
    )
    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [EntityDataSource(name="entity_data", detectors={GoodDetector()})],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = await evaluate_module.evaluate_full("Q42")

    assert result.source_urls == [
        {
            "source": "entity_data",
            "api_url": "https://www.wikidata.org/wiki/Special:EntityData/Q42.json",
            "ui_url": "https://www.wikidata.org/wiki/Q42",
        }
    ]


@pytest.mark.asyncio
async def test_evaluate_many_can_skip_n3_sources(monkeypatch):
    from wd_notability import evaluate as evaluate_module

    calls = []

    class N1Detector(Detector):
        def __init__(self) -> None:
            super().__init__("n1", NotabilityCriterion.N1)

        async def detect(self, context):
            calls.append("n1")
            yield self.make_signal(level=NotabilityLevel.WEAK, key="n1")

    class N3Detector(Detector):
        def __init__(self) -> None:
            super().__init__("n3", NotabilityCriterion.N3_INLINKS)

        async def detect(self, context):
            calls.append("n3")
            yield self.make_signal(level=NotabilityLevel.STRONG, key="n3")

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        return None

    monkeypatch.setattr(
        evaluate_module,
        "SOURCES",
        [
            Source(name="n3_source", detectors={N3Detector()}),
            Source(name="n1_source", detectors={N1Detector()}),
        ],
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = (
        await evaluate_module.evaluate_many(
            ["Q42"],
            sources=[evaluate_module.SOURCES[1]],
            possible_sources=evaluate_module.SOURCES,
            stop_on_strong=True,
            update_cache=True,
        )
    )["Q42"]

    assert calls == ["n1"]
    assert result.n1 == NotabilityLevel.WEAK
    assert result.n3 == NotabilityLevel.UNKNOWN


@pytest.mark.asyncio
async def test_evaluate_caches_deleted_entity(monkeypatch):
    from wd_notability import evaluate as evaluate_module
    from wd_notability.sources import entity_data as entity_data_module

    upserts = []

    async def fake_upsert(qid: str, summary: int, last_updated=None):
        upserts.append((qid, summary))

    monkeypatch.setattr(
        entity_data_module.wikidata_session,
        "get_with_timings",
        make_fake_get_with_timings(deleted_qids={"Q404"}),
    )
    monkeypatch.setattr(evaluate_module.CACHE, "upsert", fake_upsert)

    result = (
        await evaluate_module.evaluate_many(
            ["Q404"],
            sources=evaluate_module.SOURCES,
            stop_on_strong=True,
            update_cache=False,
        )
    )["Q404"]

    assert result.qid == "Q404"
    assert result.is_deleted is True
    assert result.n == NotabilityLevel.NONE
    assert upserts == [("Q404", result.summary)]




@pytest.mark.asyncio
async def test_foreground_evaluation_blocks_worker_waits():
    from wd_notability import evaluate as evaluate_module

    passed_gate = asyncio.Event()

    async def wait_for_gate():
        await evaluate_module.wait_for_foreground_evaluations()
        passed_gate.set()

    async with evaluate_module.foreground_evaluation():
        waiter = asyncio.create_task(wait_for_gate())
        await asyncio.sleep(0)
        assert passed_gate.is_set() is False

    await waiter
    assert passed_gate.is_set() is True

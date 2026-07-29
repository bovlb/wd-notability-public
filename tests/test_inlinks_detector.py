import pytest

from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


async def collect_signals(detector, entity):
    return [signal async for signal in detector.detect(entity)]


@pytest.mark.asyncio
async def test_inlinks_detector_emits_cached_inlinks_in_input_order(monkeypatch):
    from wd_notability.inlinks import detector as inlinks_module

    detector = inlinks_module.InlinksDetector()

    class CacheWithTwoInlinks:
        async def get_many(self, qids):
            rows = {}
            if "Q2" in qids:
                result = EvaluationResult(qid="Q2")
                result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
                rows["Q2"] = result
            if "Q3" in qids:
                result = EvaluationResult(qid="Q3")
                result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
                rows["Q3"] = result
            return rows

    monkeypatch.setattr(inlinks_module, "CACHE", CacheWithTwoInlinks())

    signals = await collect_signals(detector, {"id": "Q1", "inlinks": ["Q3", "Q9", "Q2"]})

    assert [signal.properties.get("qid") for signal in signals] == ["Q3", "Q9", "Q2"]
    assert [signal.level for signal in signals] == [NotabilityLevel.WEAK, NotabilityLevel.UNKNOWN, NotabilityLevel.WEAK]
    assert [signal.key for signal in signals] == ["inlinks", "inlinks_unknown", "inlinks"]
    assert all(signal.criterion == NotabilityCriterion.N3_INLINKS for signal in signals)


@pytest.mark.asyncio
async def test_inlinks_detector_emits_unknown_when_inlinks_are_unresolved(monkeypatch):
    from wd_notability.inlinks import detector as inlinks_module

    detector = inlinks_module.InlinksDetector()

    class EmptyCache:
        async def get_many(self, qids):
            return {}

    monkeypatch.setattr(inlinks_module, "CACHE", EmptyCache())

    signals = await collect_signals(detector, {"id": "Q1", "inlinks": ["Q9", "Q8"]})

    assert len(signals) == 2
    assert [signal.level for signal in signals] == [NotabilityLevel.UNKNOWN, NotabilityLevel.UNKNOWN]
    assert [signal.key for signal in signals] == ["inlinks_unknown", "inlinks_unknown"]


@pytest.mark.asyncio
async def test_inlinks_detector_marks_truncated_weak_inlinks_unknown(monkeypatch):
    from wd_notability.inlinks import detector as inlinks_module

    detector = inlinks_module.InlinksDetector()

    class CacheWithWeakInlink:
        async def get_many(self, qids):
            result = EvaluationResult(qid="Q2")
            result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
            result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
            result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
            return {"Q2": result}

    monkeypatch.setattr(inlinks_module, "CACHE", CacheWithWeakInlink())

    signals = await collect_signals(detector, {"id": "Q1", "inlinks": ["Q2"], "truncated": True})

    assert [signal.level for signal in signals] == [NotabilityLevel.WEAK, NotabilityLevel.UNKNOWN]
    assert [signal.key for signal in signals] == ["inlinks", "truncated_so_unknown"]


@pytest.mark.asyncio
async def test_inlinks_detector_keeps_strong_even_when_truncated(monkeypatch):
    from wd_notability.inlinks import detector as inlinks_module

    detector = inlinks_module.InlinksDetector()

    class CacheWithStrongInlink:
        async def get_many(self, qids):
            result = EvaluationResult(qid="Q2")
            result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
            result.set(NotabilityCriterion.N2a, NotabilityLevel.STRONG)
            result.set(NotabilityCriterion.N2b, NotabilityLevel.STRONG)
            return {"Q2": result}

    monkeypatch.setattr(inlinks_module, "CACHE", CacheWithStrongInlink())

    signals = await collect_signals(detector, {"id": "Q1", "inlinks": ["Q2"], "truncated": True})

    assert [signal.level for signal in signals] == [NotabilityLevel.STRONG, NotabilityLevel.STRONG]
    assert [signal.key for signal in signals] == ["inlinks", "truncated_but_strong"]


@pytest.mark.asyncio
async def test_inlinks_source_updates_inlinks_count():
    from wd_notability.inlinks.source import InlinksSource

    source = InlinksSource(name="inlinks", detectors=set())
    result = EvaluationResult(qid="Q1")

    await source.update_result(result, {"id": "Q1", "inlinks": ["Q2", "Q3", "Q4"]})

    assert result.inlinks_count == 3

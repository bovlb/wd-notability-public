import pytest

from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


async def collect_signals(detector, entity):
    return [signal async for signal in detector.detect(entity)]


@pytest.mark.asyncio
async def test_inlinks_detector_emits_cached_inlinks_in_input_order(monkeypatch):
    from wd_notability.detectors import inlinks as inlinks_module

    detector = inlinks_module.InlinksDetector()

    class CacheWithTwoInlinks:
        async def get_many(self, qids):
            rows = {}
            if "Q2" in qids:
                result = EvaluationResult(qid="Q2")
                result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
                rows["Q2"] = (result.summary, 123)
            if "Q3" in qids:
                result = EvaluationResult(qid="Q3")
                result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
                result.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
                result.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
                rows["Q3"] = (result.summary, 123)
            return rows

    monkeypatch.setattr(inlinks_module, "CACHE", CacheWithTwoInlinks())

    signals = await collect_signals(detector, {"id": "Q1", "inlinks": ["Q3", "Q9", "Q2"]})

    assert [signal.properties["qid"] for signal in signals] == ["Q3", "Q2"]
    assert [signal.level for signal in signals] == [NotabilityLevel.STRONG, NotabilityLevel.WEAK]
    assert all(signal.criterion == NotabilityCriterion.N3_INLINKS for signal in signals)


@pytest.mark.asyncio
async def test_inlinks_detector_emits_none_when_no_cached_inlinks(monkeypatch):
    from wd_notability.detectors import inlinks as inlinks_module

    detector = inlinks_module.InlinksDetector()

    class EmptyCache:
        async def get_many(self, qids):
            return {}

    monkeypatch.setattr(inlinks_module, "CACHE", EmptyCache())

    signals = await collect_signals(detector, {"id": "Q1", "inlinks": ["Q9", "Q8"]})

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.NONE
    assert signals[0].key == "inlinks_none"

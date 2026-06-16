import pytest

from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


@pytest.mark.asyncio
async def test_inlinks_worker_finalizes_when_strong_inlink_is_cached(monkeypatch):
    from wd_notability.workers import inlinks as inlinks_module

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": {"id": "Q1", "inlinks": ["Q10", "Q11"]},
                "Q2": {"id": "Q2", "inlinks": ["Q10", "Q11"]},
            }

    class FakeCache:
        def __init__(self):
            self.summary_updates = []

        async def get_many(self, qids):
            rows = {}
            strong = EvaluationResult(qid="Q10")
            strong.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
            strong.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
            strong.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
            weak = EvaluationResult(qid="Q11")
            weak.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
            weak.set(NotabilityCriterion.N2a, NotabilityLevel.WEAK)
            weak.set(NotabilityCriterion.N2b, NotabilityLevel.WEAK)
            for qid in qids:
                if qid == "Q10":
                    rows[qid] = (strong.summary, 123)
                elif qid == "Q11":
                    rows[qid] = (weak.summary, 123)
            return rows

        async def upsert_inlinks_many(self, items):
            self.summary_updates.extend(items)
            return [(item.qid, item.n3_inlinks) for item in items]

    fake_cache = FakeCache()
    monkeypatch.setattr(inlinks_module, "CACHE", fake_cache)
    monkeypatch.setattr(inlinks_module, "INLINKS_SOURCE", FakeSource())

    processed, finalized = await inlinks_module._work_inlinks_batch(["Q1", "Q2"])

    assert processed == 2
    assert finalized == 2
    assert [item.qid for item in fake_cache.summary_updates] == ["Q1", "Q2"]
    assert all(item.n3_inlinks == NotabilityLevel.STRONG for item in fake_cache.summary_updates)


@pytest.mark.asyncio
async def test_inlinks_worker_finalizes_empty_inlinks_as_none(monkeypatch):
    from wd_notability.workers import inlinks as inlinks_module

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": {"id": "Q1", "inlinks": []},
            }

    class FakeCache:
        def __init__(self):
            self.summary_updates = []

        async def get_many(self, qids):
            return {}

        async def upsert_inlinks_many(self, items):
            self.summary_updates.extend(items)
            return [(item.qid, item.n3_inlinks) for item in items]

    fake_cache = FakeCache()
    monkeypatch.setattr(inlinks_module, "CACHE", fake_cache)
    monkeypatch.setattr(inlinks_module, "INLINKS_SOURCE", FakeSource())

    processed, finalized = await inlinks_module._work_inlinks_batch(["Q1"])

    assert processed == 1
    assert finalized == 1
    assert [item.qid for item in fake_cache.summary_updates] == ["Q1"]
    assert fake_cache.summary_updates[0].n3_inlinks == NotabilityLevel.NONE

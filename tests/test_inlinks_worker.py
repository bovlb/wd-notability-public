from datetime import datetime, timezone, timedelta

import pytest

from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


@pytest.mark.asyncio
async def test_inlinks_worker_finalizes_when_strong_inlink_is_cached(monkeypatch):
    from wd_notability.inlinks import worker as inlinks_module

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": {"id": "Q1", "inlinks": ["Q10", "Q11"]},
            }

    class FakeCache:
        def __init__(self):
            self.summary_updates = []
            self.touches = []

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
                    rows[qid] = (strong.summary, 123, None)
                elif qid == "Q11":
                    rows[qid] = (weak.summary, 123, None)
            return rows

        async def upsert_inlinks_many(self, items):
            self.summary_updates.extend(items)
            return [(item.qid, item.n3_inlinks) for item in items]

        async def touch_inlinks_last_evaluated_many(self, qids, *, inlinks_last_evaluated):
            self.touches.append((list(qids), inlinks_last_evaluated))
            return len(qids)

        class pubsub:
            @staticmethod
            async def create_pubsub_session(**_kwargs):
                return 0

            @staticmethod
            async def delete_pubsub_session(**_kwargs):
                return 0

    fake_cache = FakeCache()
    monkeypatch.setattr(inlinks_module, "CACHE", fake_cache)
    monkeypatch.setattr(inlinks_module, "INLINKS_SOURCE", FakeSource())

    processed, finalized = await inlinks_module._work_inlinks_batch(["Q1"])

    assert processed == 1
    assert finalized == 1
    assert [item.qid for item in fake_cache.summary_updates] == ["Q1"]
    assert all(item.n3_inlinks == NotabilityLevel.STRONG for item in fake_cache.summary_updates)
    assert fake_cache.touches == []


@pytest.mark.asyncio
async def test_inlinks_worker_emits_dependency_interest_for_uncached_inlinks(monkeypatch):
    from wd_notability.inlinks import worker as inlinks_module

    class FakeSource:
        async def get_contexts(self, qids):
            return {
                "Q1": {"id": "Q1", "inlinks": ["Q10", "Q11"]},
            }

    class FakeCache:
        def __init__(self):
            self.summary_updates = []
            self.touches = []
            self.sessions = []

        async def get_many(self, qids):
            return {}

        async def upsert_inlinks_many(self, items):
            self.summary_updates.extend(items)
            return [(item.qid, item.n3_inlinks) for item in items]

        async def touch_inlinks_last_evaluated_many(self, qids, *, inlinks_last_evaluated):
            self.touches.append((list(qids), inlinks_last_evaluated))
            return len(qids)

        class pubsub:
            sessions = []

            @classmethod
            async def create_pubsub_session(cls, **kwargs):
                cls.sessions.append(kwargs)
                return len(kwargs.get("qids", []))

            @staticmethod
            async def delete_pubsub_session(**_kwargs):
                return 0

    fake_cache = FakeCache()
    monkeypatch.setattr(inlinks_module, "CACHE", fake_cache)
    monkeypatch.setattr(inlinks_module, "INLINKS_SOURCE", FakeSource())

    processed, finalized = await inlinks_module._work_inlinks_batch(["Q1"], cache_only=False)

    assert processed == 1
    assert finalized == 0
    assert fake_cache.summary_updates == []
    assert fake_cache.touches == [(["Q1"], pytest.approx(fake_cache.touches[0][1]))]
    assert fake_cache.pubsub.sessions[0]["owner_id"] == "inlinks"
    assert fake_cache.pubsub.sessions[0]["session_id"] == inlinks_module.INLINKS_INTEREST_SESSION_ID
    assert fake_cache.pubsub.sessions[0]["qids"] == ["Q10", "Q11"]
    assert fake_cache.pubsub.sessions[0]["wants_entitydata"] is True
    assert fake_cache.pubsub.sessions[0]["wants_inlinks"] is False


@pytest.mark.asyncio
async def test_inlinks_worker_queue_stats_returns_priority_depths(monkeypatch):
    from wd_notability.inlinks import worker as inlinks_module

    class FakeCache:
        async def count_inlinks_work_candidates(self):
            return {
                "unknown_active": 5,
                "unknown_idle": 3,
                "refresh_active": 2,
                "refresh_idle": 1,
                "total": 11,
            }

    monkeypatch.setattr(inlinks_module, "CACHE", FakeCache())

    stats = await inlinks_module.queue_stats()

    assert stats == {
        "total": 11,
        "by_priority": {
            "unknown_active": {"depth": 5},
            "unknown_idle": {"depth": 3},
            "refresh_active": {"depth": 2},
            "refresh_idle": {"depth": 1},
        },
    }

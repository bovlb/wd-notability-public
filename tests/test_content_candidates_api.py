from __future__ import annotations

import pytest

import server.app as app_module
import server.routes_api as routes_api


@pytest.mark.asyncio
async def test_api_content_candidates_uses_default_limit(monkeypatch):
    captured: dict[str, object] = {}

    class FakePubsub:
        async def list_pubsub_content_candidate_reasons(self, qids):
            assert qids == ["Q2", "Q4", "Q8"]
            return {
                "Q2": "never_evaluated",
                "Q4": "redirect_target",
                "Q8": "content_policy",
            }

    async def fake_find_content_qids(limit, *, exclude_qids=None):
        captured["limit"] = limit
        captured["exclude_qids"] = exclude_qids
        return ["Q2", "Q4", "Q8"]

    class FakeCache:
        pubsub = FakePubsub()

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(routes_api, "find_content_qids", fake_find_content_qids)

    payload = await routes_api.api_content_candidates()

    assert payload == {
        "limit": 1000,
        "count": 3,
        "items": [
            {"row_number": 1, "qid": "Q2", "content_reason": "never_evaluated"},
            {"row_number": 2, "qid": "Q4", "content_reason": "redirect_target"},
            {"row_number": 3, "qid": "Q8", "content_reason": "content_policy"},
        ],
    }
    assert captured["limit"] == 1000
    assert captured["exclude_qids"] is None


@pytest.mark.asyncio
async def test_api_content_candidates_honors_explicit_limit(monkeypatch):
    captured: dict[str, object] = {}

    class FakePubsub:
        async def list_pubsub_content_candidate_reasons(self, qids):
            assert qids == ["Q1"]
            return {"Q1": "recent_changes"}

    async def fake_find_content_qids(limit, *, exclude_qids=None):
        captured["limit"] = limit
        captured["exclude_qids"] = exclude_qids
        return ["Q1"]

    class FakeCache:
        pubsub = FakePubsub()

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(routes_api, "find_content_qids", fake_find_content_qids)

    payload = await routes_api.api_content_candidates(limit=17)

    assert payload == {
        "limit": 17,
        "count": 1,
        "items": [{"row_number": 1, "qid": "Q1", "content_reason": "recent_changes"}],
    }
    assert captured["limit"] == 17
    assert captured["exclude_qids"] is None

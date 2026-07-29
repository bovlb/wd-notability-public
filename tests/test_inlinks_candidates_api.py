from __future__ import annotations

import pytest

import server.app as app_module
import server.routes_api as routes_api


@pytest.mark.asyncio
async def test_api_inlinks_candidates_uses_default_limit(monkeypatch):
    captured: dict[str, object] = {}

    class FakeCache:
        async def list_inlinks_work_candidates(self, *, limit=None):
            captured["limit"] = limit
            return [
                ("Q1", None, 5, True),
                ("Q2", 30, 0, False),
            ]

    monkeypatch.setattr(app_module, "CACHE", FakeCache())

    payload = await routes_api.api_inlinks_candidates()

    assert payload == {
        "limit": 1000,
        "count": 2,
        "items": [
            {
                "row_number": 1,
                "qid": "Q1",
                "inlinks_last_evaluated": None,
                "active_priority": 5,
                "is_unknown": True,
            },
            {
                "row_number": 2,
                "qid": "Q2",
                "inlinks_last_evaluated": 30,
                "active_priority": 0,
                "is_unknown": False,
            },
        ],
    }
    assert captured["limit"] == 1000


@pytest.mark.asyncio
async def test_api_inlinks_candidates_honors_explicit_limit(monkeypatch):
    captured: dict[str, object] = {}

    class FakeCache:
        async def list_inlinks_work_candidates(self, *, limit=None):
            captured["limit"] = limit
            return [("Q3", 40, 7, True)]

    monkeypatch.setattr(app_module, "CACHE", FakeCache())

    payload = await routes_api.api_inlinks_candidates(limit=17)

    assert payload == {
        "limit": 17,
        "count": 1,
        "items": [
            {
                "row_number": 1,
                "qid": "Q3",
                "inlinks_last_evaluated": 40,
                "active_priority": 7,
                "is_unknown": True,
            }
        ],
    }
    assert captured["limit"] == 17

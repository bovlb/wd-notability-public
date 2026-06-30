from __future__ import annotations

import pytest

from server import app as app_module


@pytest.mark.asyncio
async def test_cache_stats_endpoint_includes_lookup_counts(monkeypatch):
    class FakeEvaluationCache:
        async def stats(self):
            return {"evaluations": {"entries": 1}}

    class FakeLookupCache:
        def stats(self):
            return {"namespace_sites": 2, "property_qids": 5}

    monkeypatch.setattr(app_module, "CACHE", FakeEvaluationCache())
    monkeypatch.setattr(app_module, "lookup_cache", FakeLookupCache())

    stats = await app_module.api_cache_stats()

    assert stats["evaluations"]["entries"] == 1
    assert stats["lookup_cache"]["namespace_sites"] == 2
    assert stats["lookup_cache"]["property_qids"] == 5

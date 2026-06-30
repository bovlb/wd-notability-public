from __future__ import annotations

import pytest

from wd_notability.lookup_cache import lookup_cache
from wd_notability.external_usage.wiki_subscribers.source import WikiUsageSource


@pytest.mark.asyncio
async def test_wiki_usage_source_uses_cache_batch(monkeypatch):
    calls = []

    def fake_get(qids):
        qid_list = list(qids)
        calls.append(qid_list)
        return {"Q1"}

    monkeypatch.setattr(lookup_cache, "get_wiki_subscribers_for", fake_get)

    source = WikiUsageSource(name="wiki_usage", detectors=set())
    contexts = await source.get_contexts(["Q1", "Q2"])

    assert calls == [["Q1", "Q2"]]
    assert contexts["Q1"]["is_subscribed"] is True
    assert contexts["Q2"]["is_subscribed"] is False
    assert contexts["Q1"]["_timings"]["get_context"] >= 0.0

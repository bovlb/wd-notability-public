import pytest

from wd_notability.lookup_cache import lookup_cache
from wd_notability.external_usage.sdc.source import SdcSource


@pytest.mark.asyncio
async def test_sdc_source_uses_cached_usage(monkeypatch):
    calls = []

    monkeypatch.setattr(
        lookup_cache,
        "get_sdc_usage_for",
        lambda qids: calls.append(list(qids)) or {"Q42": 9},
    )

    source = SdcSource(name="sdc", detectors=set())
    contexts = await source.get_contexts(["Q42", "Q43"])

    assert contexts["Q42"]["usage_count"] == 9
    assert contexts["Q42"]["search_query"].startswith("haswbstatement:")
    assert calls == [["Q42", "Q43"]]

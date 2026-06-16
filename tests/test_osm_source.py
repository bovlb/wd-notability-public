import pytest

from wd_notability.lookup_cache import lookup_cache
from wd_notability.sources.osm import OsmSource


@pytest.mark.asyncio
async def test_osm_source_uses_cached_usage(monkeypatch):
    calls = []

    monkeypatch.setattr(
        lookup_cache,
        "get_osm_usage_for",
        lambda qids: calls.append(list(qids)) or {
            "Q42": {
                "count_all": 12,
                "count_nodes": 5,
                "count_ways": 4,
                "count_relations": 3,
            }
        },
    )

    source = OsmSource(name="osm", detectors=set())
    contexts = await source.get_contexts(["Q42", "Q43"])

    assert contexts["Q42"]["row"]["count_all"] == 12
    assert contexts["Q42"]["object_explorer_url"].startswith("https://overpass-turbo.eu/?Q=")
    assert calls == [["Q42", "Q43"]]

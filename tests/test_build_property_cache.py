from __future__ import annotations

from pathlib import Path

import pytest

import wd_notability.external_usage.property.builder as build_property_cache_module
from wd_notability.lookup_cache import LookupCache


@pytest.mark.asyncio
async def test_refresh_property_cache_from_json(tmp_path):
    output = tmp_path / "lookup_cache.db"
    properties_json = Path("wd_notability/data/property_instances_by_qid.json")

    build_property_cache_module.refresh_property_cache_from_json(
        output,
        properties_json,
    )

    cache = LookupCache(output)
    assert "P10009" in await cache.property_instances("Q62589316")
    assert "P10000" in await cache.property_instances("Q18614948")
    assert "P10152" in await cache.property_instances("Q105388954")

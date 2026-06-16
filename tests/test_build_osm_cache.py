from __future__ import annotations

from pathlib import Path

import pytest

import scripts.build_osm_cache as build_osm_cache_module


@pytest.mark.asyncio
async def test_build_osm_cache_replaces_lookup_rows(monkeypatch, tmp_path):
    rows = [
        {"value": "Q1", "count_all": 3, "count_nodes": 1, "count_ways": 1, "count_relations": 1},
        {"value": "Q2", "count": 4},
    ]

    async def fake_fetch_page(client, page, page_size):
        return rows if page == 1 else []

    class FakeCache:
        def __init__(self, output: Path):
            self.output = output
            self.replace_called = None

        def replace_osm_usage(self, osm_usage_by_qid):
            self.replace_called = osm_usage_by_qid

    refresh_calls = []

    async def fake_refresh_cache(self, cache, usage_by_qid):
        refresh_calls.append((cache, dict(usage_by_qid)))
        return len(usage_by_qid)

    fake_cache = FakeCache(tmp_path / "lookup_cache.db")
    monkeypatch.setattr(build_osm_cache_module, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(build_osm_cache_module, "LookupCache", lambda output: fake_cache)
    monkeypatch.setattr(type(build_osm_cache_module.OSM_SOURCE), "refresh_cache", fake_refresh_cache)

    await build_osm_cache_module.build_osm_cache(tmp_path / "lookup_cache.db", page_size=999)

    assert fake_cache.replace_called == {
        "Q1": {"count_all": 3, "count_nodes": 1, "count_ways": 1, "count_relations": 1},
        "Q2": {"count_all": 4, "count_nodes": 0, "count_ways": 0, "count_relations": 0},
    }
    assert refresh_calls == [
        (fake_cache, {
            "Q1": {"count_all": 3, "count_nodes": 1, "count_ways": 1, "count_relations": 1},
            "Q2": {"count_all": 4, "count_nodes": 0, "count_ways": 0, "count_relations": 0},
        }),
    ]

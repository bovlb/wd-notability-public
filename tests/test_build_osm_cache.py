from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import wd_notability.external_usage.osm.builder as build_osm_cache_module
from wd_notability import summary as summary_bits
from wd_notability.models import NotabilityCriterion, NotabilityLevel


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
            self.initialize_called = False

        def replace_osm_usage(self, osm_usage_by_qid):
            self.replace_called = osm_usage_by_qid

        def initialize(self):
            self.initialize_called = True


    class FakeMainCache:
        def __init__(self):
            self.initialize_called = False
            self.chunk_calls = 0
            self.update_calls = []

        async def initialize(self):
            self.initialize_called = True

        async def close(self):
            self.closed = True

        async def iter_qid_summary_chunks(self, chunk_size=5000):
            self.chunk_calls += 1
            yield [
                (
                    "Q1",
                    summary_bits.value(NotabilityCriterion.N3_OSM, NotabilityLevel.NONE),
                ),
                (
                    "Q3",
                    summary_bits.value(NotabilityCriterion.N3_OSM, NotabilityLevel.WEAK),
                ),
                (
                    "Q4",
                    summary_bits.value(NotabilityCriterion.N3_OSM, NotabilityLevel.STRONG),
                ),
            ]

        async def update_summary_bits(self, qids, *, set_bits=0, clear_bits=0):
            self.update_calls.append((set(qids), set_bits, clear_bits))
            return len(qids)

    fake_cache = FakeCache(tmp_path / "lookup_cache.db")
    fake_main_cache = FakeMainCache()
    monkeypatch.setattr(build_osm_cache_module, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(build_osm_cache_module, "LookupCache", lambda output: fake_cache)
    monkeypatch.setattr(build_osm_cache_module, "EvaluationCache", lambda: fake_main_cache)

    await build_osm_cache_module.build_osm_cache(tmp_path / "lookup_cache.db", page_size=999)

    assert fake_cache.replace_called == {
        "Q1": {"count_all": 3, "count_nodes": 1, "count_ways": 1, "count_relations": 1},
        "Q2": {"count_all": 4, "count_nodes": 0, "count_ways": 0, "count_relations": 0},
    }
    assert fake_cache.initialize_called is True
    assert fake_main_cache.initialize_called is True
    assert fake_main_cache.chunk_calls == 1
    assert fake_main_cache.update_calls == [
        (
            {"Q1"},
            summary_bits.value(NotabilityCriterion.N3_OSM, NotabilityLevel.WEAK),
            summary_bits.mask(NotabilityCriterion.N3_OSM),
        ),
        (
            {"Q3", "Q4"},
            summary_bits.value(NotabilityCriterion.N3_OSM, NotabilityLevel.NONE),
            summary_bits.mask(NotabilityCriterion.N3_OSM),
        ),
    ]


@pytest.mark.asyncio
async def test_build_osm_cache_times_out_on_slow_close(monkeypatch, tmp_path):
    rows = [{"value": "Q1", "count_all": 1}]

    async def fake_fetch_page(client, page, page_size):
        return rows if page == 1 else []

    class FakeCache:
        def __init__(self, output: Path):
            self.output = output

        def replace_osm_usage(self, osm_usage_by_qid):
            self.replace_called = osm_usage_by_qid

        def initialize(self):
            self.initialize_called = True

    class FakeMainCache:
        def __init__(self):
            self.initialize_called = False
            self.close_called = False

        async def initialize(self):
            self.initialize_called = True

        async def close(self):
            self.close_called = True
            await asyncio.sleep(0.05)

        async def iter_qid_summary_chunks(self, chunk_size=5000):
            if False:
                yield []

        async def update_summary_bits(self, qids, *, set_bits=0, clear_bits=0):
            return len(qids)

    fake_cache = FakeCache(tmp_path / "lookup_cache.db")
    fake_main_cache = FakeMainCache()
    monkeypatch.setattr(build_osm_cache_module, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(build_osm_cache_module, "LookupCache", lambda output: fake_cache)
    monkeypatch.setattr(build_osm_cache_module, "EvaluationCache", lambda: fake_main_cache)
    monkeypatch.setattr(build_osm_cache_module, "MAIN_CACHE_CLOSE_TIMEOUT_SECONDS", 0.01)

    await build_osm_cache_module.build_osm_cache(tmp_path / "lookup_cache.db", page_size=999)

    assert fake_main_cache.initialize_called is True
    assert fake_main_cache.close_called is True

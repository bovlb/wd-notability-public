import pytest

import server.app as app_module


@pytest.mark.asyncio
async def test_startup_event_refuses_without_lookup_cache(tmp_path, monkeypatch):
    class FakeLookupCache:
        def assert_ready(self, required_property_qids=()):
            raise RuntimeError("Lookup cache database is missing")

        def get_osm_usage(self):
            return {"Q42": {"count_all": 1, "count_nodes": 1, "count_ways": 0, "count_relations": 0}}

        def get_sdc_usage(self):
            return {"Q42": 1}

    monkeypatch.setattr(app_module, "lookup_cache", FakeLookupCache())

    with pytest.raises(RuntimeError, match="Lookup cache database is missing"):
        await app_module.startup_event()

import pytest

from wd_notability.localdb_paths import LOOKUP_CACHE_PATH
from wd_notability.lookup_backend import create_lookup_backend
from wd_notability.lookup_cache import LookupCache


@pytest.mark.asyncio
async def test_lookup_cache_round_trip(tmp_path):
    db_path = tmp_path / "lookup_cache.db"
    cache = LookupCache(db_path)

    cache.replace_namespace_data(
        namespaces_by_site={
            "enwiki": {"Main": 0, "Talk": 1},
            "frwiki_p": {"User": 2},
        },
        site_api_urls={
            "enwiki": "https://en.wikipedia.org/w/api.php",
            "frwiki": "https://fr.wikipedia.org/w/api.php",
        },
    )
    cache.replace_property_instances(
        {
            "Q62589316": ["P1", "P2"],
            "Q18614948": ["P3"],
        }
    )
    cache.replace_osm_usage(
        {
            "Q42": {"count_all": 10, "count_nodes": 4, "count_ways": 3, "count_relations": 3},
        }
    )
    cache.replace_sdc_usage(
        {
            "Q42": 7,
        }
    )
    cache.replace_wiki_subscribers({"Q42", "Q99"})

    namespace_mapping = cache.get_prefix_to_id("enwiki")
    assert namespace_mapping == {"main": 0, "talk": 1}
    assert cache.get_prefix_to_id("frwiki_p") == {"user": 2}
    assert cache.get_site_api_urls()["enwiki"] == "https://en.wikipedia.org/w/api.php"

    assert await cache.property_instances("Q62589316") == {"P1", "P2"}
    assert await cache.property_instances("Q18614948") == {"P3"}
    assert await cache.property_instances_for(["Q62589316", "Q18614948"]) == {
        "Q62589316": {"P1", "P2"},
        "Q18614948": {"P3"},
    }
    assert cache.get_osm_usage()["Q42"]["count_all"] == 10
    assert cache.get_sdc_usage()["Q42"] == 7
    assert cache.get_wiki_subscribers() == {"Q42", "Q99"}
    assert cache.get_wiki_subscribers_for(["Q42", "Q100"]) == {"Q42"}

    reloaded = LookupCache(db_path)
    assert reloaded.get_prefix_to_id("enwiki") == {"main": 0, "talk": 1}
    assert await reloaded.property_instances("Q62589316") == {"P1", "P2"}
    assert await reloaded.property_instances_for(["Q62589316", "Q18614948"]) == {
        "Q62589316": {"P1", "P2"},
        "Q18614948": {"P3"},
    }
    assert reloaded.get_osm_usage()["Q42"]["count_nodes"] == 4
    assert reloaded.get_sdc_usage()["Q42"] == 7
    assert reloaded.get_wiki_subscribers() == {"Q42", "Q99"}

    with pytest.raises(KeyError):
        await reloaded.property_instances("Q000000")


def test_lookup_cache_assert_ready_rejects_missing_db(tmp_path):
    cache = LookupCache(tmp_path / "missing.db")

    with pytest.raises(RuntimeError, match="missing"):
        cache.assert_ready(required_property_qids=("Q62589316",))


def test_lookup_cache_stats_counts_rows(tmp_path):
    db_path = tmp_path / "lookup_cache.db"
    cache = LookupCache(db_path)

    cache.replace_namespace_data(
        namespaces_by_site={
            "enwiki": {"Main": 0, "Talk": 1},
            "frwiki": {"Main": 0},
        },
        site_api_urls={
            "enwiki": "https://en.wikipedia.org/w/api.php",
            "frwiki": "https://fr.wikipedia.org/w/api.php",
        },
    )
    cache.replace_property_instances(
        {
            "Q62589316": ["P1", "P2"],
            "Q18614948": ["P3"],
        }
    )
    cache.replace_osm_usage(
        {
            "Q42": {"count_all": 10, "count_nodes": 4, "count_ways": 3, "count_relations": 3},
            "Q43": {"count_all": 1, "count_nodes": 1, "count_ways": 0, "count_relations": 0},
        }
    )
    cache.replace_sdc_usage(
        {
            "Q42": 7,
            "Q43": 2,
        }
    )
    cache.replace_wiki_subscribers({"Q42", "Q43", "Q44"})

    stats = cache.stats()

    assert stats["namespace_sites"] == 2
    assert stats["namespace_prefixes"] == 3
    assert stats["site_api_urls"] == 2
    assert stats["property_qids"] == 2
    assert stats["property_instances"] == 3
    assert stats["lookup_loaded"] == 1


def test_lookup_backend_defaults_to_localdb_lookup_cache(monkeypatch):
    monkeypatch.setenv("WD_NOTABILITY_DB_BACKEND", "sqlite")
    backend = create_lookup_backend()

    assert backend.db_path == LOOKUP_CACHE_PATH


def test_lookup_backend_auto_detects_toolforge(monkeypatch):
    monkeypatch.delenv("WD_NOTABILITY_DB_BACKEND", raising=False)
    monkeypatch.delenv("WD_NOTABILITY_LOOKUP_BACKEND", raising=False)
    monkeypatch.setattr("wd_notability.lookup_backend.toolforge_defaults_file_exists", lambda: True)
    monkeypatch.setattr("wd_notability.lookup_backend.toolforge_database_name", lambda **kwargs: "tool-wd-notability")

    backend = create_lookup_backend()

    assert backend.database == "tool-wd-notability"
    assert backend.host == "tools.db.svc.wikimedia.cloud"

from __future__ import annotations

from collections.abc import Iterable

import pytest

from wd_notability.lookup_backend import MariaDBLookupBackend, LookupSnapshot, create_lookup_backend
from wd_notability.lookup_cache import LookupCache


class _FakeLookupBackend:
    def __init__(self):
        self.database = "tool-wd-notability"
        self._state_token = object()
        self._sites: dict[str, dict[str, int]] = {}
        self._site_api_urls: dict[str, str] = {}
        self._properties_by_qid: dict[str, set[str]] = {}
        self._osm_usage: dict[str, dict[str, int]] = {}
        self._sdc_usage: dict[str, int] = {}
        self._wiki_subscribers: set[str] = set()
        self._lookup_state: dict[str, str] = {}
        self.external_usage_calls: int = 0
        self.external_usage_qids: list[str] = []

    def ensure_schema(self) -> None:
        return None

    def state_token(self) -> object | None:
        return self._state_token

    def load_snapshot(self) -> LookupSnapshot:
        return LookupSnapshot(
            namespaces_by_site={site: {prefix.lower(): ns_id for prefix, ns_id in prefixes.items()} for site, prefixes in self._sites.items()},
            site_api_urls=dict(self._site_api_urls),
            property_instances_by_qid={qid: set(values) for qid, values in self._properties_by_qid.items()},
        )

    def get_osm_usage(self, qids: Iterable[str] | None = None) -> dict[str, dict[str, int]]:
        if qids is None:
            return {qid: dict(values) for qid, values in self._osm_usage.items()}
        return {qid: dict(self._osm_usage[qid]) for qid in qids if qid in self._osm_usage}

    def get_sdc_usage(self, qids: Iterable[str] | None = None) -> dict[str, int]:
        if qids is None:
            return dict(self._sdc_usage)
        return {qid: self._sdc_usage[qid] for qid in qids if qid in self._sdc_usage}

    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
        if qids is None:
            return set(self._wiki_subscribers)
        return {qid for qid in qids if qid in self._wiki_subscribers}

    def get_external_usage(self, qids: Iterable[str]) -> dict[str, dict[str, object]]:
        self.external_usage_calls += 1
        self.external_usage_qids = list(qids)
        result: dict[str, dict[str, object]] = {}
        for qid in self.external_usage_qids:
            result[qid] = {
                "osm": self._osm_usage.get(qid),
                "sdc": self._sdc_usage.get(qid),
                "wikisub": qid in self._wiki_subscribers,
            }
        return result

    def replace_namespace_data(self, *, namespaces_by_site, site_api_urls) -> None:
        self._sites = {site: dict(prefixes) for site, prefixes in namespaces_by_site.items()}
        self._site_api_urls = dict(site_api_urls)
        self._state_token = object()

    def replace_property_instances(self, property_instances_by_qid) -> None:
        self._properties_by_qid = {qid: set(values) for qid, values in property_instances_by_qid.items()}
        self._state_token = object()

    def replace_osm_usage(self, osm_usage_by_qid) -> None:
        self._osm_usage = {qid: dict(values) for qid, values in osm_usage_by_qid.items()}

    def replace_sdc_usage(self, sdc_usage_by_qid) -> None:
        self._sdc_usage = dict(sdc_usage_by_qid)

    def replace_wiki_subscribers(self, wiki_subscribers) -> None:
        self._wiki_subscribers = set(wiki_subscribers)

    def upsert_wiki_subscribers(self, wiki_subscribers) -> int:
        before = len(self._wiki_subscribers)
        self._wiki_subscribers.update(wiki_subscribers)
        return len(self._wiki_subscribers) - before

    def get_lookup_state(self, key: str) -> str | None:
        return self._lookup_state.get(key)

    def set_lookup_state(self, key: str, value: str) -> None:
        self._lookup_state[key] = value

    def assert_ready(self, required_property_qids: Iterable[str] = ()) -> None:
        return None


@pytest.mark.asyncio
async def test_lookup_cache_round_trip():
    backend = _FakeLookupBackend()
    cache = LookupCache(backend=backend)

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
    cache.replace_sdc_usage({"Q42": 7})
    cache.replace_wiki_subscribers({"Q42", "Q99"})

    assert cache.get_prefix_to_id("enwiki") == {"main": 0, "talk": 1}
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

    external_usage = cache.get_external_usage(["Q42", "Q99"])

    assert backend.external_usage_calls == 1
    assert backend.external_usage_qids == ["Q42", "Q99"]
    assert external_usage["Q42"] == {
        "osm": {"count_all": 10, "count_nodes": 4, "count_ways": 3, "count_relations": 3},
        "sdc": 7,
        "wikisub": True,
    }
    assert external_usage["Q99"] == {
        "osm": None,
        "sdc": None,
        "wikisub": True,
    }


def test_lookup_cache_assert_ready_delegates():
    backend = _FakeLookupBackend()
    cache = LookupCache(backend=backend)

    cache.assert_ready(required_property_qids=("Q62589316",))


def test_lookup_cache_stats_counts_rows():
    backend = _FakeLookupBackend()
    cache = LookupCache(backend=backend)

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
    cache.replace_sdc_usage({"Q42": 7, "Q43": 2})
    cache.replace_wiki_subscribers({"Q42", "Q43", "Q44"})

    stats = cache.stats()

    assert stats["namespace_sites"] == 2
    assert stats["namespace_prefixes"] == 3
    assert stats["site_api_urls"] == 2
    assert stats["property_qids"] == 2
    assert stats["property_instances"] == 3
    assert stats["lookup_loaded"] == 1
    assert stats["db_path"] == "tool-wd-notability"


def test_lookup_backend_defaults_to_mariadb(monkeypatch):
    monkeypatch.setenv("TOOLSDB_HOST", "127.0.0.1")
    monkeypatch.setenv("TOOLSDB_PORT", "3306")
    monkeypatch.setenv("TOOLSDB_DATABASE", "tool-wd-notability")
    monkeypatch.setenv("TOOLSDB_USER", "tool")
    monkeypatch.setenv("TOOLSDB_PASSWORD", "secret")

    backend = create_lookup_backend()

    assert isinstance(backend, MariaDBLookupBackend)
    assert backend.database == "tool-wd-notability"
    assert backend.host == "127.0.0.1"


def test_lookup_backend_requires_toolsdb_env_for_mariadb(monkeypatch):
    for name in ("TOOLSDB_HOST", "TOOLSDB_PORT", "TOOLSDB_DATABASE", "TOOLSDB_USER", "TOOLSDB_PASSWORD"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError, match="TOOLSDB_DATABASE"):
        create_lookup_backend()


def test_mariadb_namespace_schema_uses_binary_collation(monkeypatch):
    monkeypatch.setenv("TOOLSDB_HOST", "127.0.0.1")
    monkeypatch.setenv("TOOLSDB_PORT", "3306")

    statements: list[str] = []

    class FakeCursor:
        def execute(self, sql, params=None):
            statements.append(sql)

        def fetchone(self):
            return None

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    backend = MariaDBLookupBackend(database="tool-wd-notability")
    monkeypatch.setattr(backend, "_connect", lambda: FakeConnection())

    backend.ensure_schema()

    assert any("COLLATE utf8mb4_bin" in sql and "namespace_prefixes" in sql for sql in statements)
    assert any("COLLATE utf8mb4_bin" in sql and "site_api_urls" in sql for sql in statements)


def test_mariadb_namespace_staging_tables_use_binary_collation(monkeypatch):
    monkeypatch.setenv("TOOLSDB_HOST", "127.0.0.1")
    monkeypatch.setenv("TOOLSDB_PORT", "3306")

    statements: list[str] = []

    class FakeCursor:
        def execute(self, sql, params=None):
            statements.append(sql)

        def executemany(self, sql, params):
            statements.append(sql)

        def fetchone(self):
            return None

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    backend = MariaDBLookupBackend(database="tool-wd-notability")
    monkeypatch.setattr(backend, "_connect", lambda: FakeConnection())

    backend.replace_namespace_data(
        namespaces_by_site={"banwiki": {"media": -2, "média": -2}},
        site_api_urls={"banwiki": "https://ban.wikipedia.org/w/api.php"},
    )

    assert any("CREATE TEMPORARY TABLE temp_namespace_prefixes" in sql and "COLLATE utf8mb4_bin" in sql for sql in statements)
    assert any("CREATE TEMPORARY TABLE temp_site_api_urls" in sql and "COLLATE utf8mb4_bin" in sql for sql in statements)

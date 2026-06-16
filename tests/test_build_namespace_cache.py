from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from scripts import build_namespace_cache as build_namespace_cache_module
from wd_notability.lookup_cache import LookupCache


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get(self, url: str) -> httpx.Response:
        self.calls.append(url)
        return httpx.Response(200, json={"query": {"namespaces": {}}}, request=httpx.Request("GET", url))


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    async def get_with_timings(self, url: str, *, limiter):
        self.calls.append((url, limiter))
        response = httpx.Response(200, json={"sitematrix": {}}, request=httpx.Request("GET", url))
        return response, object()


@pytest.mark.asyncio
async def test_fetch_json_uses_wikidata_session_for_wikidata_urls(monkeypatch):
    fake_session = FakeSession()
    fake_client = FakeClient()
    monkeypatch.setattr(build_namespace_cache_module, "wikidata_session", fake_session)

    payload = await build_namespace_cache_module._fetch_json(
        fake_client,
        "https://www.wikidata.org/w/api.php?action=sitematrix&format=json",
    )

    assert payload == {"sitematrix": {}}
    assert fake_session.calls
    assert fake_client.calls == []


@pytest.mark.asyncio
async def test_fetch_json_uses_client_for_non_wikidata_urls(monkeypatch):
    fake_session = FakeSession()
    fake_client = FakeClient()
    monkeypatch.setattr(build_namespace_cache_module, "wikidata_session", fake_session)

    payload = await build_namespace_cache_module._fetch_json(
        fake_client,
        "https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json",
    )

    assert payload == {"query": {"namespaces": {}}}
    assert fake_session.calls == []
    assert fake_client.calls == [
        "https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json"
    ]


def test_refresh_namespace_cache_from_json(tmp_path):
    output_dir = tmp_path / "data"
    namespaces_json = Path("wd_notability/data/namespaces_by_site.json")
    site_api_urls_json = Path("wd_notability/data/site_api_urls.json")

    build_namespace_cache_module.refresh_namespace_cache_from_json(
        output_dir,
        namespaces_json,
        site_api_urls_json,
    )

    cache = LookupCache(output_dir / "lookup_cache.db")
    prefix_to_id = cache.get_prefix_to_id("enwiki")
    assert prefix_to_id is not None
    assert prefix_to_id["wikipedia"] == 4
    assert prefix_to_id["talk"] == 1
    assert prefix_to_id["user"] == 2
    assert cache.get_site_api_urls()["enwiki"] == "https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json"

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from threading import Lock
from typing import Any

from wd_notability.localdb_paths import LOOKUP_CACHE_PATH
from wd_notability.lookup_backend import LookupBackend, LookupSnapshot, create_lookup_backend

DEFAULT_LOOKUP_CACHE_PATH = LOOKUP_CACHE_PATH


class LookupCache:
    def __init__(
        self,
        db_path: str | Path | None = None,
        backend: LookupBackend | None = None,
    ):
        self._backend = backend if backend is not None else create_lookup_backend(db_path)
        self._loaded = False
        self._state_token: object | None = None
        self._sites: dict[str, dict[str, int]] = {}
        self._site_api_urls: dict[str, str] = {}
        self._properties_by_qid: dict[str, set[str]] = {}
        self._lock = Lock()

    def initialize(self) -> None:
        self._backend.ensure_schema()

    def assert_ready(self, required_property_qids: Iterable[str] = ()) -> None:
        self._backend.assert_ready(required_property_qids)

    def _load_snapshot(self, force: bool = False) -> None:
        current_token = self._backend.state_token()
        if self._loaded and not force and self._state_token == current_token:
            return

        snapshot: LookupSnapshot = self._backend.load_snapshot()
        self._sites = snapshot.namespaces_by_site
        self._site_api_urls = snapshot.site_api_urls
        self._properties_by_qid = snapshot.property_instances_by_qid
        self._loaded = True
        self._state_token = current_token

    @staticmethod
    def _candidate_site_keys(site_key: str) -> list[str]:
        key = site_key.strip().lower()
        candidates = [key]
        if key.endswith("_p"):
            candidates.append(key[:-2])
        return candidates

    def replace_namespace_data(
        self,
        *,
        namespaces_by_site: dict[str, dict[str, int]],
        site_api_urls: dict[str, str],
    ) -> None:
        self._backend.replace_namespace_data(
            namespaces_by_site=namespaces_by_site,
            site_api_urls=site_api_urls,
        )
        self._loaded = False
        self._state_token = None

    def replace_property_instances(self, property_instances_by_qid: dict[str, list[str] | set[str]]) -> None:
        self._backend.replace_property_instances(property_instances_by_qid)
        self._loaded = False
        self._state_token = None

    def replace_osm_usage(self, osm_usage_by_qid: dict[str, dict[str, int]]) -> None:
        self._backend.replace_osm_usage(osm_usage_by_qid)

    def replace_sdc_usage(self, sdc_usage_by_qid: dict[str, int]) -> None:
        self._backend.replace_sdc_usage(sdc_usage_by_qid)

    def replace_wiki_subscribers(self, wiki_subscribers: set[str] | list[str] | tuple[str, ...]) -> None:
        self._backend.replace_wiki_subscribers(wiki_subscribers)

    def upsert_wiki_subscribers(self, wiki_subscribers: set[str] | list[str] | tuple[str, ...]) -> int:
        count = self._backend.upsert_wiki_subscribers(wiki_subscribers)
        return count

    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
        return self._backend.get_wiki_subscribers(qids)

    def get_wiki_subscribers_for(self, qids: Iterable[str]) -> set[str]:
        return self._backend.get_wiki_subscribers(qids)

    def get_lookup_state(self, key: str) -> str | None:
        return self._backend.get_lookup_state(key)

    def set_lookup_state(self, key: str, value: str) -> None:
        self._backend.set_lookup_state(key, value)
        self._loaded = False
        self._state_token = None

    def get_prefix_to_id(self, site_key: str) -> dict[str, int] | None:
        with self._lock:
            self._load_snapshot()
            for candidate in self._candidate_site_keys(site_key):
                mapping = self._sites.get(candidate)
                if mapping is not None:
                    return mapping

            self._load_snapshot(force=True)
            for candidate in self._candidate_site_keys(site_key):
                mapping = self._sites.get(candidate)
                if mapping is not None:
                    return mapping

            return None

    def get_site_api_urls(self) -> dict[str, str]:
        with self._lock:
            self._load_snapshot()
            return dict(self._site_api_urls)

    async def property_instances(self, qid: str) -> set[str]:
        rows = await self.property_instances_for([qid])
        properties = rows.get(qid)
        if properties is None:
            raise KeyError(
                f"QID {qid} is not present in the lookup cache. "
                "Regenerate the property-instance lookup data and include this QID."
            )
        return set(properties)

    async def property_instances_for(self, qids: Iterable[str]) -> dict[str, set[str]]:
        with self._lock:
            self._load_snapshot()
            result: dict[str, set[str]] = {}
            for qid in qids:
                properties = self._properties_by_qid.get(qid)
                if properties is not None:
                    result[qid] = set(properties)
            return result

    def get_osm_usage(self) -> dict[str, dict[str, int]]:
        return self._backend.get_osm_usage()

    def get_osm_usage_for(self, qids: Iterable[str]) -> dict[str, dict[str, int]]:
        return self._backend.get_osm_usage(qids)

    def get_sdc_usage(self) -> dict[str, int]:
        return self._backend.get_sdc_usage()

    def get_sdc_usage_for(self, qids: Iterable[str]) -> dict[str, int]:
        return self._backend.get_sdc_usage(qids)

    def stats(self) -> dict[str, int | str]:
        with self._lock:
            self._load_snapshot()
            namespace_prefixes = sum(len(prefixes) for prefixes in self._sites.values())
            property_instances = sum(len(properties) for properties in self._properties_by_qid.values())
            return {
                "db_path": str(getattr(self._backend, "db_path", "")),
                "namespace_sites": len(self._sites),
                "namespace_prefixes": namespace_prefixes,
                "site_api_urls": len(self._site_api_urls),
                "property_qids": len(self._properties_by_qid),
                "property_instances": property_instances,
                "lookup_loaded": int(self._loaded),
                "lookup_state_token": "set" if self._state_token is not None else "unset",
            }


lookup_cache = LookupCache()

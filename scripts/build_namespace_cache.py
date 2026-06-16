#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from wd_notability.async_limiters import WIKIDATA_ACTION_API_LIMITER, WIKIMEDIA_SITEINFO_LIMITER
from wd_notability.file_lock import acquire_file_lock
from wd_notability.http_client import current_http_priority
from wd_notability.lookup_cache import LookupCache
from wd_notability.wikidata_api import wikidata_session

USER_AGENT = "wd-notability/1.0 (contact:User:Bovlb)"
SITEMATRIX_URL = "https://www.wikidata.org/w/api.php?action=sitematrix&format=json&smlangprop=code|site&smlimit=max"
NAMESPACE_QUERY = "action=query&meta=siteinfo&siprop=namespaces&format=json"


def _is_rate_limited(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response is not None
        and exc.response.status_code == 429
    )


def _retry_after_seconds(exc: BaseException) -> float | None:
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response is None:
        return None

    header = exc.response.headers.get("Retry-After")
    if not header:
        return None

    value = header.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)

    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _wait_for_rate_limit(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    if exc is not None:
        retry_after = _retry_after_seconds(exc)
        if retry_after is not None:
            return min(retry_after, 300.0)

    # Fallback exponential backoff with an upper bound.
    return float(min(30, max(1, 2 ** (retry_state.attempt_number - 1))))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build namespace and API URL lookup cache from Wikimedia sitematrix."
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data"),
        help="Directory where the lookup cache database is written",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum concurrent namespace requests",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of sites to fetch (0 = all)",
    )
    parser.add_argument(
        "--from-json",
        action="store_true",
        help="Refresh the cache from the checked-in JSON snapshot instead of live siteinfo requests",
    )
    parser.add_argument(
        "--namespaces-json",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "namespaces_by_site.json"),
        help="Path to the old namespaces JSON cache",
    )
    parser.add_argument(
        "--site-api-urls-json",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "site_api_urls.json"),
        help="Path to the old site API URLs JSON cache",
    )
    return parser.parse_args()


def _extract_sites(sitematrix: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}

    for key, value in sitematrix.items():
        if key == "count":
            continue

        if key == "specials" and isinstance(value, list):
            iterable = value
        elif isinstance(value, dict):
            iterable = value.get("site", [])
        else:
            iterable = []

        for site in iterable:
            if not isinstance(site, dict):
                continue
            dbname = site.get("dbname")
            url = site.get("url")
            if not isinstance(dbname, str) or not isinstance(url, str):
                continue
            if dbname.endswith("_p"):
                dbname = dbname[:-2]
            result[dbname] = url.rstrip("/")

    return result


def _api_url(site_url: str) -> str:
    return f"{site_url}/w/api.php?{NAMESPACE_QUERY}"


def _parse_namespaces(payload: dict[str, Any]) -> dict[str, int]:
    query = payload.get("query", {})
    namespaces = query.get("namespaces", {})
    prefix_to_id: dict[str, int] = {}

    for ns in namespaces.values():
        if not isinstance(ns, dict):
            continue
        ns_id = ns.get("id")
        if not isinstance(ns_id, int):
            continue

        canonical = ns.get("canonical")
        display = ns.get("*")
        for prefix in (canonical, display):
            if isinstance(prefix, str) and prefix:
                prefix_to_id[prefix.lower()] = ns_id

    return prefix_to_id


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected JSON structure in {path}")
    return payload


def _normalize_site_api_urls(payload: dict[str, Any]) -> dict[str, str]:
    urls = payload.get("site_api_urls", {})
    if not isinstance(urls, dict):
        raise RuntimeError("Unexpected site API JSON structure: missing 'site_api_urls'")

    return {
        site_key: api_url
        for site_key, api_url in urls.items()
        if isinstance(site_key, str) and isinstance(api_url, str)
    }


def _normalize_namespaces(payload: dict[str, Any]) -> dict[str, dict[str, int]]:
    sites = payload.get("sites", {})
    if not isinstance(sites, dict):
        raise RuntimeError("Unexpected namespaces JSON structure: missing 'sites'")

    namespaces_by_site: dict[str, dict[str, int]] = {}
    for site_key, mapping in sites.items():
        if not isinstance(site_key, str) or not isinstance(mapping, dict):
            continue
        namespaces_by_site[site_key] = {
            prefix.lower(): int(ns_id)
            for prefix, ns_id in mapping.items()
            if isinstance(prefix, str) and isinstance(ns_id, int)
        }
    return namespaces_by_site


def refresh_namespace_cache_from_json(
    output_dir: Path,
    namespaces_json: Path,
    site_api_urls_json: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "lookup_cache.db"
    with acquire_file_lock(db_path, "namespace"):
        namespaces_payload = _load_json(namespaces_json)
        site_api_urls_payload = _load_json(site_api_urls_json)

        namespaces_by_site = _normalize_namespaces(namespaces_payload)
        site_api_urls = _normalize_site_api_urls(site_api_urls_payload)

        cache = LookupCache(db_path)
        cache.replace_namespace_data(
            namespaces_by_site=dict(sorted(namespaces_by_site.items())),
            site_api_urls=dict(sorted(site_api_urls.items())),
        )

        print(f"Wrote: {db_path}")
        print(
            f"Sites: {len(namespaces_by_site)} total, "
            f"{len(site_api_urls)} API URLs"
        )


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    limiter = WIKIDATA_ACTION_API_LIMITER if "wikidata.org/w/api.php" in url else WIKIMEDIA_SITEINFO_LIMITER
    if "wikidata.org/w/api.php" in url:
        response, _timings = await wikidata_session.get_with_timings(url, limiter=limiter)
        response.raise_for_status()
        return response.json()
    return await _fetch_json_with_retry(client, url, limiter)


@retry(
    retry=retry_if_exception(_is_rate_limited),
    wait=_wait_for_rate_limit,
    stop=stop_after_attempt(8),
    reraise=True,
)
async def _fetch_json_with_retry(client: httpx.AsyncClient, url: str, limiter) -> dict[str, Any]:
    print(f"Fetching with retry: {url}")
    if hasattr(limiter, "limit"):
        limit_context = limiter.limit(priority=current_http_priority())
    else:
        limit_context = limiter

    async with limit_context:
        response = await client.get(url)
    response.raise_for_status()
    return response.json()


async def build_namespace_cache(
    output_dir: Path,
    concurrency: int,
    limit: int,
    *,
    from_json: bool = False,
    namespaces_json: Path | None = None,
    site_api_urls_json: Path | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "lookup_cache.db"
    with acquire_file_lock(db_path, "namespace"):
        if from_json:
            if namespaces_json is None or site_api_urls_json is None:
                raise ValueError("from_json requires namespaces_json and site_api_urls_json")
            refresh_namespace_cache_from_json(output_dir, namespaces_json, site_api_urls_json)
            return

        headers = {"User-Agent": USER_AGENT}
        async with httpx.AsyncClient(timeout=60, headers=headers) as client:
            sitematrix_payload = await _fetch_json(client, SITEMATRIX_URL)
            sitematrix = sitematrix_payload.get("sitematrix")
            if not isinstance(sitematrix, dict):
                raise RuntimeError("Unexpected sitematrix response: missing 'sitematrix' object")

            site_urls = _extract_sites(sitematrix)
            site_keys = sorted(site_urls.keys())
            if limit > 0:
                site_keys = site_keys[:limit]

            print(f"Found {len(site_urls)} sites in sitematrix, fetching namespaces for {len(site_keys)} sites with concurrency={concurrency}...")

            semaphore = asyncio.Semaphore(concurrency)
            namespaces_by_site: dict[str, dict[str, int]] = {}
            failures: dict[str, str] = {}

            async def fetch_one(site_key: str) -> None:
                api_url = _api_url(site_urls[site_key])
                async with semaphore:
                    try:
                        print(f"Fetching: {api_url}")
                        payload = await _fetch_json(client, api_url)
                        namespaces_by_site[site_key] = _parse_namespaces(payload)
                    except Exception as exc:  # noqa: BLE001
                        failures[site_key] = str(exc)

            await asyncio.gather(*(fetch_one(site_key) for site_key in site_keys))

        used_site_urls = {site_key: _api_url(site_urls[site_key]) for site_key in site_keys}

        if failures:
            sample = ", ".join(f"{site}: {msg}" for site, msg in list(sorted(failures.items()))[:5])
            raise RuntimeError(
                "Namespace cache build failed: "
                f"{len(failures)} site fetches failed out of {len(site_keys)}. "
                "No output files were written. "
                f"Sample failures: {sample}"
            )

        cache = LookupCache(db_path)
        cache.replace_namespace_data(
            namespaces_by_site=dict(sorted(namespaces_by_site.items())),
            site_api_urls=dict(sorted(used_site_urls.items())),
        )

        print(f"Wrote: {db_path}")
        print(
            f"Sites: {len(site_keys)} total, {len(namespaces_by_site)} successes, {len(failures)} failures"
        )


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_namespace_cache(
            output_dir=Path(args.output_dir),
            concurrency=max(1, args.concurrency),
            limit=max(0, args.limit),
            from_json=bool(args.from_json),
            namespaces_json=Path(args.namespaces_json),
            site_api_urls_json=Path(args.site_api_urls_json),
        )
    )


if __name__ == "__main__":
    main()

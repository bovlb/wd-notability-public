#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from wd_notability.async_limiters import TAGINFO_API_LIMITER
from wd_notability.file_lock import acquire_file_lock
from wd_notability.http_client import current_http_priority
from wd_notability.lookup_cache import LookupCache
from wd_notability.sources.osm import OSM_SOURCE

TAGINFO_URL = "https://taginfo.openstreetmap.org/api/4/key/values"
USER_AGENT = "wd-notability/1.0 (contact:User:Bovlb)"


def _is_rate_limited(exc: BaseException) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response is not None
        and exc.response.status_code == 429
    )


def _is_retryable_failure(exc: BaseException) -> bool:
    return isinstance(exc, httpx.TransportError) or _is_rate_limited(exc)


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

    return float(min(30, max(1, 2 ** (retry_state.attempt_number - 1))))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an OSM Taginfo QID usage table in the lookup cache."
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "lookup_cache.db"),
        help="Output lookup cache database path",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=999,
        help="Taginfo rows per page",
    )
    parser.add_argument(
        "--sync-main-cache-only",
        action="store_true",
        help="Skip Taginfo fetching and only resync N3_osm from the existing lookup cache",
    )
    return parser.parse_args()


async def _fetch_page(client: httpx.AsyncClient, page: int, page_size: int) -> list[dict]:
    return await _fetch_page_with_retry(client, page, page_size)


@retry(
    retry=retry_if_exception(_is_retryable_failure),
    wait=_wait_for_rate_limit,
    stop=stop_after_attempt(6),
    reraise=True,
)
async def _fetch_page_with_retry(client: httpx.AsyncClient, page: int, page_size: int) -> list[dict]:
    params = {
        "key": "wikidata",
        "page": page,
        "rp": page_size,
        "sortname": "value",
        "sortorder": "asc",
    }
    if hasattr(TAGINFO_API_LIMITER, "limit"):
        limit_context = TAGINFO_API_LIMITER.limit(priority=current_http_priority())
    else:
        limit_context = TAGINFO_API_LIMITER

    async with limit_context:
        response = await client.get(TAGINFO_URL, params=params)
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return data if isinstance(data, list) else []


async def build_osm_cache(output: Path, page_size: int, *, sync_main_cache_only: bool = False) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    with acquire_file_lock(output, "osm"):
        cache = LookupCache(output)
        cache.initialize()

        if sync_main_cache_only:
            osm_usage_by_qid = cache.get_osm_usage()
            if not osm_usage_by_qid:
                raise RuntimeError(
                    "Lookup cache has no OSM usage rows. Run scripts/build_osm_cache.py first."
                )
            await OSM_SOURCE.refresh_cache(cache, osm_usage_by_qid)
            print(f"Resynced {len(osm_usage_by_qid)} OSM QID rows from {output}")
            return

        headers = {"User-Agent": USER_AGENT}
        osm_usage_by_qid: dict[str, dict[str, int]] = {}

        async with httpx.AsyncClient(timeout=60, headers=headers) as client:
            page = 1
            while True:
                rows = await _fetch_page(client, page, page_size)
                if not rows:
                    break

                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    value = row.get("value")
                    if not (isinstance(value, str) and value.startswith("Q") and value[1:].isdigit()):
                        continue

                    qid = value
                    osm_usage_by_qid[qid] = {
                        "count_all": int(row.get("count_all") or row.get("count") or 1),
                        "count_nodes": int(row.get("count_nodes") or 0),
                        "count_ways": int(row.get("count_ways") or 0),
                        "count_relations": int(row.get("count_relations") or 0),
                    }

                page += 1

        cache.replace_osm_usage(osm_usage_by_qid)
        await OSM_SOURCE.refresh_cache(cache, osm_usage_by_qid)
        print(f"Wrote {len(osm_usage_by_qid)} OSM QID rows to {output}")


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_osm_cache(
            output=Path(args.output),
            page_size=max(1, args.page_size),
            sync_main_cache_only=bool(args.sync_main_cache_only),
        )
    )


if __name__ == "__main__":
    main()

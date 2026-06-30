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
from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.file_lock import acquire_file_lock
from wd_notability.localdb_paths import LOOKUP_CACHE_PATH
from wd_notability.http_client import current_http_priority
from wd_notability.lookup_cache import LookupCache
from wd_notability import summary as summary_bits
from wd_notability.models import NotabilityCriterion, NotabilityLevel

TAGINFO_URL = "https://taginfo.openstreetmap.org/api/4/key/values"
USER_AGENT = "wd-notability/1.0 (contact:User:Bovlb)"
MAIN_CACHE_CLOSE_TIMEOUT_SECONDS = 10.0


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
        default=str(LOOKUP_CACHE_PATH),
        help="Output lookup cache database path",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=999,
        help="Taginfo rows per page",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of accepted QID rows to process (0 = all)",
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


async def _sync_osm_usage(
    main_cache: EvaluationCache,
    osm_qids: set[str],
    *,
    clear_missing: bool,
    chunk_size: int = 5000,
) -> None:
    criterion = NotabilityCriterion.N3_OSM
    weak_value = summary_bits.value(criterion, NotabilityLevel.WEAK)
    none_value = summary_bits.value(criterion, NotabilityLevel.NONE)
    criterion_mask = summary_bits.mask(criterion)

    remaining_osm_qids = osm_qids
    scanned_rows = 0
    set_rows = 0
    cleared_rows = 0
    chunk_index = 0

    print(
        f"Scanning {len(osm_qids)} OSM QID row(s) against the evaluation cache in {chunk_size}-row chunks..."
    )

    async for chunk in main_cache.iter_qid_summary_chunks(chunk_size=chunk_size):
        chunk_index += 1
        scanned_rows += len(chunk)
        to_set: set[str] = set()
        to_clear: set[str] = set()

        for qid, summary in chunk:
            current = summary_bits.get(summary, criterion)
            if qid in remaining_osm_qids:
                remaining_osm_qids.discard(qid)
                if current in {NotabilityLevel.UNKNOWN, NotabilityLevel.NONE}:
                    to_set.add(qid)
            elif clear_missing and current in {NotabilityLevel.WEAK, NotabilityLevel.STRONG}:
                to_clear.add(qid)

        if to_set:
            set_rows += await main_cache.update_summary_bits(
                to_set,
                set_bits=weak_value,
                clear_bits=criterion_mask,
            )

        if to_clear:
            cleared_rows += await main_cache.update_summary_bits(
                to_clear,
                set_bits=none_value,
                clear_bits=criterion_mask,
            )

        print(
            f"Processed evaluation cache chunk {chunk_index}: "
            f"scanned {len(chunk)} row(s), set {len(to_set)} to WEAK, cleared {len(to_clear)} to NONE"
        )

    if remaining_osm_qids:
        print(
            f"Skipping {len(remaining_osm_qids)} OSM QID row(s) that were not present in the evaluation cache."
        )

    print(
        "OSM sync complete: "
        f"scanned {scanned_rows} evaluation-cache row(s), "
        f"set {set_rows} row(s) to WEAK, "
        f"cleared {cleared_rows} row(s) to NONE"
    )


async def build_osm_cache(
    output: Path,
    page_size: int,
    *,
    limit: int = 0,
    sync_main_cache_only: bool = False,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    with acquire_file_lock(output, "osm"):
        cache = LookupCache(output)
        cache.initialize()
        main_cache = EvaluationCache()
        await main_cache.initialize()
        try:
            if sync_main_cache_only:
                print(f"Loading existing OSM usage rows from {output}...")
                osm_usage_by_qid = cache.get_osm_usage()
                if not osm_usage_by_qid:
                    raise RuntimeError(
                        "Lookup cache has no OSM usage rows. Run `main.py build-osm-cache` first."
                    )
                if limit > 0:
                    osm_usage_by_qid = dict(list(osm_usage_by_qid.items())[:limit])
                print(f"Syncing {len(osm_usage_by_qid)} OSM QID rows into the main cache...")
                await _sync_osm_usage(
                    main_cache,
                    set(osm_usage_by_qid),
                    clear_missing=limit <= 0,
                )
                if limit > 0:
                    print("Partial sync mode: skipped clearing missing OSM rows in the main cache.")
                print(f"Resynced {len(osm_usage_by_qid)} OSM QID rows from {output}")
                return

            headers = {"User-Agent": USER_AGENT}
            osm_usage_by_qid: dict[str, dict[str, int]] = {}
            total_rows = 0
            page_count = 0
            stop_fetching = False

            async with httpx.AsyncClient(timeout=60, headers=headers) as client:
                page = 1
                print(f"Fetching OSM Taginfo data from {TAGINFO_URL} with page_size={page_size}...")
                while not stop_fetching:
                    rows = await _fetch_page(client, page, page_size)
                    if not rows:
                        print(f"Taginfo returned no rows on page {page}; stopping fetch.")
                        break

                    page_count += 1
                    page_rows = 0
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
                        page_rows += 1
                        total_rows += 1
                        if limit > 0 and total_rows >= limit:
                            stop_fetching = True
                            break

                    print(
                        f"Fetched Taginfo page {page} with {len(rows)} row(s); "
                        f"accepted {page_rows} QID row(s), total accepted {total_rows}"
                    )

                    page += 1

            print(f"Writing {len(osm_usage_by_qid)} unique OSM QID rows to {output}...")
            cache.replace_osm_usage(osm_usage_by_qid)
            print(f"Syncing {len(osm_usage_by_qid)} OSM QID rows into the main cache...")
            await _sync_osm_usage(
                main_cache,
                set(osm_usage_by_qid),
                clear_missing=limit <= 0,
            )
            if limit > 0:
                print("Partial sync mode: skipped clearing missing OSM rows in the main cache.")
            print(
                f"Wrote {len(osm_usage_by_qid)} unique OSM QID rows to {output} "
                f"from {total_rows} accepted Taginfo row(s) across {page_count} page(s)"
            )
        finally:
            try:
                await asyncio.wait_for(main_cache.close(), timeout=MAIN_CACHE_CLOSE_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                print(
                    "[build-osm-cache] Timed out while closing the evaluation cache; "
                    "continuing shutdown."
                )


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_osm_cache(
            output=Path(args.output),
            page_size=max(1, args.page_size),
            limit=max(0, args.limit),
            sync_main_cache_only=bool(args.sync_main_cache_only),
        )
    )


if __name__ == "__main__":
    main()


OSM_BUILDER = build_osm_cache
OsmBuilder = build_osm_cache

__all__ = [
    "OSM_BUILDER",
    "OsmBuilder",
    "build_osm_cache",
]

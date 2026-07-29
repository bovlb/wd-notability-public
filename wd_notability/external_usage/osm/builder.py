#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from wd_notability.file_lock import acquire_file_lock
from wd_notability.lookup_cache import LookupCache
from wd_notability.lookup_backend import create_lookup_backend

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
        default=None,
        help="Directory or file path used only for the cache lock",
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
    response = await client.get(TAGINFO_URL, params=params)
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return data if isinstance(data, list) else []


def _normalize_osm_row(row: dict) -> tuple[int, int, int, int, int] | None:
    qid = row.get("value")
    if not isinstance(qid, str) or len(qid) < 2 or qid[0] != "Q" or not qid[1:].isdigit():
        return None
    return (
        int(qid[1:]),
        int(row.get("count", row.get("count_all", 0)) or 0),
        int(row.get("count_nodes", 0) or 0),
        int(row.get("count_ways", 0) or 0),
        int(row.get("count_relations", 0) or 0),
    )


async def build_osm_cache(output: Path | None, page_size: int, *, limit: int = 0) -> None:
    lock_target = output if output is not None else Path(
        "/tmp/wd-notability/osm-cache")

    with acquire_file_lock(lock_target, "osm"):
        backend = create_lookup_backend()
        cache = LookupCache(backend=backend)
        cache.initialize()
        with backend._connect() as db:  # noqa: SLF001
            cursor = db.cursor()
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_osm_usage")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_osm_usage (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    count_all BIGINT NOT NULL DEFAULT 0,
                    count_nodes BIGINT NOT NULL DEFAULT 0,
                    count_ways BIGINT NOT NULL DEFAULT 0,
                    count_relations BIGINT NOT NULL DEFAULT 0
                )
                """
            )

            async with httpx.AsyncClient(timeout=None, headers={"User-Agent": USER_AGENT}) as client:
                page = 1
                accepted_rows = 0
                staged_rows = 0
                fetched_pages = 0
                print(
                    f"Starting OSM cache build with page_size={page_size}, "
                    f"limit={limit or 'unbounded'}"
                )
                while True:
                    rows = await _fetch_page(client, page, page_size)
                    fetched_pages += 1
                    print(f"Fetched OSM Taginfo page {page} with {len(rows)} row(s)")
                    if not rows:
                        break

                    page_rows: list[tuple[int, int, int, int, int]] = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        normalized_row = _normalize_osm_row(row)
                        if normalized_row is None:
                            continue
                        page_rows.append(normalized_row)
                        accepted_rows += 1
                        if limit > 0 and accepted_rows >= limit:
                            print(
                                f"Reached limit={limit} after accepting {accepted_rows} QID row(s)"
                            )
                            break

                    if page_rows:
                        cursor.executemany(
                            """
                            INSERT INTO temp_osm_usage
                                (qid, count_all, count_nodes, count_ways, count_relations)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            page_rows,
                        )
                        db.commit()
                        staged_rows += len(page_rows)

                    print(
                        f"Accepted {len(page_rows)} QID row(s) from OSM Taginfo page {page}; "
                        f"accepted_total={accepted_rows}; staged_total={staged_rows}"
                    )

                    if limit > 0 and accepted_rows >= limit:
                        print(f"Stopping OSM cache build after reaching limit={limit}")
                        break
                    page += 1

            print(
                f"Swapping {staged_rows} staged OSM QID row(s) into ToolsDB "
                f"from {fetched_pages} fetched page(s)"
            )
            cursor.execute("START TRANSACTION")
            try:
                cursor.execute("DELETE FROM osm_usage")
                cursor.execute(
                    """
                    INSERT INTO osm_usage (qid, count_all, count_nodes, count_ways, count_relations)
                    SELECT qid, count_all, count_nodes, count_ways, count_relations
                    FROM temp_osm_usage
                    ORDER BY qid
                    """
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_osm_usage")
            print(
                f"Wrote {staged_rows} unique OSM QID rows to ToolsDB "
                f"from {fetched_pages} fetched page(s)"
            )


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_osm_cache(
            Path(args.output) if args.output is not None else None,
            args.page_size,
            limit=max(0, args.limit),
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

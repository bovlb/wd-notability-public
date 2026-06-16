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

from wd_notability.async_limiters import WIKIDATA_SPARQL_LIMITER
from wd_notability.file_lock import acquire_file_lock
from wd_notability.http_client import current_http_priority
from wd_notability.lookup_cache import LookupCache

USER_AGENT = "wd-notability/1.0 (contact:User:Bovlb)"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
DEFAULT_QIDS = ["Q105388954", "Q18614948", "Q62589316"]


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
        description="Build property-instance lookup cache from Wikidata SPARQL."
    )
    parser.add_argument(
        "--output",
        default=str(
            Path(__file__).resolve().parents[1]
            / "wd_notability"
            / "data"
            / "lookup_cache.db"
        ),
        help="Output lookup cache database path",
    )
    parser.add_argument(
        "--qid",
        action="append",
        dest="qids",
        help="QID to include (repeatable). Defaults are used if omitted.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=1.0,
        help="Delay between SPARQL requests to avoid rate limits",
    )
    parser.add_argument(
        "--from-json",
        action="store_true",
        help="Refresh the cache from the checked-in JSON snapshot instead of live SPARQL requests",
    )
    parser.add_argument(
        "--properties-json",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "property_instances_by_qid.json"),
        help="Path to the old property-instance JSON cache",
    )
    return parser.parse_args()


def _query_for_qid(qid: str) -> str:
    return f'''
    SELECT DISTINCT ?prop WHERE {{
      ?prop wdt:P31/wdt:P279* wd:{qid} .
      FILTER(STRSTARTS(STR(?prop), "http://www.wikidata.org/entity/P"))
    }}
    '''


async def _fetch_props(client: httpx.AsyncClient, qid: str) -> list[str]:
    return await _fetch_props_with_retry(client, qid)


@retry(
    retry=retry_if_exception(_is_rate_limited),
    wait=_wait_for_rate_limit,
    stop=stop_after_attempt(6),
    reraise=True,
)
async def _fetch_props_with_retry(client: httpx.AsyncClient, qid: str) -> list[str]:
    if hasattr(WIKIDATA_SPARQL_LIMITER, "limit"):
        limit_context = WIKIDATA_SPARQL_LIMITER.limit(priority=current_http_priority())
    else:
        limit_context = WIKIDATA_SPARQL_LIMITER

    async with limit_context:
        response = await client.post(
            SPARQL_ENDPOINT,
            data={"query": _query_for_qid(qid)},
        )
    response.raise_for_status()
    data: Any = response.json()

    props: set[str] = set()
    for row in data.get("results", {}).get("bindings", []):
        uri = row.get("prop", {}).get("value")
        if isinstance(uri, str) and uri.startswith("http://www.wikidata.org/entity/P"):
            props.add(uri.rsplit("/", 1)[-1])

    return sorted(props)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected JSON structure in {path}")
    return payload


def refresh_property_cache_from_json(output: Path, properties_json: Path) -> None:
    payload = _load_json(properties_json)
    property_instances = payload.get("property_instances_by_qid", {})
    if not isinstance(property_instances, dict):
        raise RuntimeError("Unexpected property JSON structure: missing 'property_instances_by_qid'")

    properties_by_qid: dict[str, list[str]] = {}
    for qid, props in property_instances.items():
        if not isinstance(qid, str) or not isinstance(props, list):
            continue
        cleaned = sorted({prop for prop in props if isinstance(prop, str) and prop.startswith("P")})
        properties_by_qid[qid] = cleaned

    output.parent.mkdir(parents=True, exist_ok=True)
    with acquire_file_lock(output, "property"):
        cache = LookupCache(output)
        cache.replace_property_instances(properties_by_qid)
        print(f"Wrote: {output}")
        print(f"QIDs: {len(properties_by_qid)} total")


async def build_property_cache(
    output: Path,
    qids: list[str],
    delay_seconds: float,
    *,
    from_json: bool = False,
    properties_json: Path | None = None,
) -> None:
    if from_json:
        if properties_json is None:
            raise ValueError("from_json requires properties_json")
        refresh_property_cache_from_json(output, properties_json)
        return

    output.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    }

    properties_by_qid: dict[str, list[str]] = {}
    failures: dict[str, str] = {}

    async with httpx.AsyncClient(timeout=90, headers=headers) as client:
        for qid in qids:
            try:
                properties_by_qid[qid] = await _fetch_props(client, qid)
            except Exception as exc:  # noqa: BLE001
                failures[qid] = str(exc)
            await asyncio.sleep(max(0.0, delay_seconds))

    if failures:
        sample = ", ".join(f"{qid}: {msg}" for qid, msg in list(sorted(failures.items()))[:5])
        raise RuntimeError(
            "Property cache build failed: "
            f"{len(failures)} QID fetches failed out of {len(qids)}. "
            "No output file was written. "
            f"Sample failures: {sample}"
        )

    with acquire_file_lock(output, "property"):
        cache = LookupCache(output)
        cache.replace_property_instances(properties_by_qid)
        print(f"Wrote: {output}")
        print(f"QIDs: {len(qids)} total, {len(properties_by_qid)} successes, {len(failures)} failures")


def main() -> None:
    args = parse_args()
    qids = args.qids if args.qids else DEFAULT_QIDS
    qids = sorted(set(qids))
    asyncio.run(
        build_property_cache(
            output=Path(args.output),
            qids=qids,
            delay_seconds=args.delay_seconds,
            from_json=bool(args.from_json),
            properties_json=Path(args.properties_json),
        )
    )


if __name__ == "__main__":
    main()

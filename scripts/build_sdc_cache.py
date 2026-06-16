#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import bz2
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

from wd_notability.lookup_cache import LookupCache
from wd_notability.file_lock import acquire_file_lock
from wd_notability.sources.sdc import SDC_SOURCE

DUMP_URL = "https://dumps.wikimedia.org/commonswiki/entities/latest-mediainfo.ttl.bz2"
USER_AGENT = "wd-notability/1.0 (contact:User:Bovlb)"
QID_PATTERN = re.compile(r"wd:(Q[1-9][0-9]*)\b")
LOOKUP_STATE_KEY = "sdc_dump_last_modified"


def _is_retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response is None:
        return False
    return exc.response.status_code == 429 or exc.response.status_code >= 500


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
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an SDC usage cache from the Commons mediainfo dump."
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "lookup_cache.db"),
        help="Output lookup cache database path",
    )
    parser.add_argument(
        "--dump-url",
        default=DUMP_URL,
        help="Commons mediainfo TTL dump URL",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even if the remote dump timestamp has not changed",
    )
    parser.add_argument(
        "--sync-main-cache-only",
        action="store_true",
        help="Skip dump fetching and only resync N3_sdc from the existing lookup cache",
    )
    return parser.parse_args()


def _remote_last_modified(response: httpx.Response) -> str | None:
    header = response.headers.get("Last-Modified")
    if not header:
        return None
    try:
        parsed = parsedate_to_datetime(header.strip())
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.isoformat()


async def build_sdc_cache(
    output: Path,
    dump_url: str,
    *,
    force: bool = False,
    sync_main_cache_only: bool = False,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    with acquire_file_lock(output, "sdc"):
        headers = {"User-Agent": USER_AGENT}
        async with httpx.AsyncClient(timeout=None, headers=headers) as client:
            cache = LookupCache(output)
            cache.initialize()

            if sync_main_cache_only:
                sdc_usage_by_qid = cache.get_sdc_usage()
                if not sdc_usage_by_qid:
                    raise RuntimeError(
                        "Lookup cache has no SDC usage rows. Run scripts/build_sdc_cache.py first."
                    )
                await SDC_SOURCE.refresh_cache(cache, sdc_usage_by_qid)
                print(f"Resynced {len(sdc_usage_by_qid)} SDC QID rows from {output}")
                return

            meta_response = await client.head(dump_url, follow_redirects=True)
            meta_response.raise_for_status()
            remote_last_modified = _remote_last_modified(meta_response)

            cache_last_modified = cache.get_lookup_state(LOOKUP_STATE_KEY)
            if not force and remote_last_modified is not None and cache_last_modified == remote_last_modified:
                print(f"SDC dump unchanged since {remote_last_modified}; skipping rebuild")
                return

            sdc_usage_by_qid: dict[str, int] = {}
            for attempt_index in range(6):
                try:
                    decompressor = bz2.BZ2Decompressor()
                    text_buffer = ""
                    sdc_usage_by_qid.clear()

                    async with client.stream("GET", dump_url) as response:
                        response.raise_for_status()
                        async for chunk in response.aiter_bytes():
                            if not chunk:
                                continue
                            text_buffer += decompressor.decompress(chunk).decode("utf-8", errors="ignore")
                            *lines, text_buffer = text_buffer.split("\n")
                            for line in lines:
                                for match in QID_PATTERN.finditer(line):
                                    qid = match.group(1)
                                    sdc_usage_by_qid[qid] = sdc_usage_by_qid.get(qid, 0) + 1

                    if text_buffer:
                        for match in QID_PATTERN.finditer(text_buffer):
                            qid = match.group(1)
                            sdc_usage_by_qid[qid] = sdc_usage_by_qid.get(qid, 0) + 1
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt_index == 5 or not _is_retryable_http_error(exc):
                        raise
                    delay = _retry_after_seconds(exc)
                    if delay is None:
                        delay = float(min(30, max(1, 2 ** attempt_index)))
                    await asyncio.sleep(delay)

            cache.replace_sdc_usage(sdc_usage_by_qid)
            if remote_last_modified is not None:
                cache.set_lookup_state(LOOKUP_STATE_KEY, remote_last_modified)
            await SDC_SOURCE.refresh_cache(cache, sdc_usage_by_qid)
            print(f"Wrote {len(sdc_usage_by_qid)} SDC QID rows to {output}")


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_sdc_cache(
            output=Path(args.output),
            dump_url=args.dump_url,
            force=bool(args.force),
            sync_main_cache_only=bool(args.sync_main_cache_only),
        )
    )


if __name__ == "__main__":
    main()

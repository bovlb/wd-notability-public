#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import bz2
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx

from wd_notability.file_lock import acquire_file_lock
from wd_notability.lookup_cache import LookupCache
from wd_notability.lookup_backend import create_lookup_backend

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
        default=None,
        help="Directory or file path used only for the cache lock",
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
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show a tqdm progress bar while downloading the SDC dump",
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


def _make_progress_bar(total_bytes: int | None) -> Any | None:
    try:
        from tqdm import tqdm
    except ImportError:
        print("tqdm is not installed; continuing without a progress bar")
        return None

    return tqdm(
        total=total_bytes,
        desc="SDC dump download",
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        dynamic_ncols=True,
        leave=False,
    )


async def build_sdc_cache(
    output: Path | None,
    dump_url: str,
    *,
    force: bool = False,
    progress: bool = True,
) -> None:
    lock_target = output if output is not None else Path("/tmp/wd-notability/sdc-cache")

    with acquire_file_lock(lock_target, "sdc"):
        headers = {"User-Agent": USER_AGENT}
        async with httpx.AsyncClient(timeout=None, headers=headers) as client:
            backend = create_lookup_backend()
            cache = LookupCache(backend=backend)
            cache.initialize()

            print(f"Checking remote SDC dump timestamp for {dump_url}...")
            meta_response = await client.head(dump_url, follow_redirects=True)
            meta_response.raise_for_status()
            remote_last_modified = _remote_last_modified(meta_response)

            cache_last_modified = backend.get_lookup_state(LOOKUP_STATE_KEY)
            if not force and remote_last_modified is not None and cache_last_modified == remote_last_modified:
                print(f"SDC dump unchanged since {remote_last_modified}; skipping rebuild")
                return

            with backend._connect() as db:  # noqa: SLF001
                cursor = db.cursor()
                cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_sdc_usage")
                cursor.execute(
                    """
                    CREATE TEMPORARY TABLE temp_sdc_usage (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        usage_count BIGINT NOT NULL DEFAULT 0
                    )
                    """
                )

                print(f"Downloading SDC dump from {dump_url}...")
                flushed_rows = 0
                for attempt_index in range(6):
                    try:
                        decompressor = bz2.BZ2Decompressor()
                        text_buffer = ""
                        chunk_counts: dict[str, int] = {}

                        async with client.stream("GET", dump_url) as response:
                            response.raise_for_status()
                            total_bytes = None
                            if progress:
                                content_length = response.headers.get("Content-Length")
                                if content_length is not None:
                                    try:
                                        total_bytes = max(0, int(content_length))
                                    except ValueError:
                                        total_bytes = None
                            progress_bar = _make_progress_bar(total_bytes) if progress else None
                            downloaded_bytes = 0
                            try:
                                async for chunk in response.aiter_bytes():
                                    if not chunk:
                                        continue
                                    downloaded_bytes += len(chunk)
                                    if progress_bar is not None:
                                        progress_bar.update(len(chunk))
                                    text_buffer += decompressor.decompress(chunk).decode("utf-8", errors="ignore")
                                    *lines, text_buffer = text_buffer.split("\n")
                                    for line in lines:
                                        for match in QID_PATTERN.finditer(line):
                                            qid = match.group(1)
                                            chunk_counts[qid] = chunk_counts.get(qid, 0) + 1
                                    if len(chunk_counts) >= 5_000:
                                        rows = [
                                            (int(qid[1:]), usage_count)
                                            for qid, usage_count in chunk_counts.items()
                                        ]
                                        cursor.executemany(
                                            """
                                            INSERT INTO temp_sdc_usage (qid, usage_count)
                                            VALUES (%s, %s)
                                            ON DUPLICATE KEY UPDATE
                                                usage_count = usage_count + VALUES(usage_count)
                                            """,
                                            rows,
                                        )
                                        db.commit()
                                        flushed_rows += len(rows)
                                        print(
                                            f"Staged {len(rows)} SDC QID row(s) so far; "
                                            f"downloaded={downloaded_bytes} byte(s); "
                                            f"flushed_total={flushed_rows}"
                                        )
                                        chunk_counts.clear()
                            finally:
                                if progress_bar is not None:
                                    progress_bar.close()

                        if text_buffer:
                            for match in QID_PATTERN.finditer(text_buffer):
                                qid = match.group(1)
                                chunk_counts[qid] = chunk_counts.get(qid, 0) + 1

                        if chunk_counts:
                            rows = [
                                (int(qid[1:]), usage_count)
                                for qid, usage_count in chunk_counts.items()
                            ]
                            cursor.executemany(
                                """
                                INSERT INTO temp_sdc_usage (qid, usage_count)
                                VALUES (%s, %s)
                                ON DUPLICATE KEY UPDATE
                                    usage_count = usage_count + VALUES(usage_count)
                                """,
                                rows,
                            )
                            db.commit()
                            flushed_rows += len(rows)

                        cursor.execute("SELECT COUNT(*) FROM temp_sdc_usage")
                        row = cursor.fetchone()
                        staged_rows = int(row[0]) if row and row[0] is not None else 0
                        print(
                            f"Parsed and staged {staged_rows} unique SDC QID rows from "
                            f"{downloaded_bytes} downloaded byte(s)"
                        )
                        break
                    except Exception as exc:  # noqa: BLE001
                        db.rollback()
                        if attempt_index == 5 or not _is_retryable_http_error(exc):
                            raise
                        delay = _retry_after_seconds(exc)
                        if delay is None:
                            delay = float(min(30, max(1, 2 ** attempt_index)))
                        await asyncio.sleep(delay)

                print(
                    f"Swapping staged SDC rows into ToolsDB from {dump_url}..."
                )
                cursor.execute("START TRANSACTION")
                try:
                    cursor.execute("DELETE FROM sdc_usage")
                    cursor.execute(
                        """
                        INSERT INTO sdc_usage (qid, usage_count)
                        SELECT qid, usage_count
                        FROM temp_sdc_usage
                        ORDER BY qid
                        """
                    )
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
                finally:
                    cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_sdc_usage")

                if remote_last_modified is not None:
                    backend.set_lookup_state(LOOKUP_STATE_KEY, remote_last_modified)
                print(f"Wrote {staged_rows} SDC QID rows to ToolsDB")


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_sdc_cache(
            Path(args.output) if args.output is not None else None,
            args.dump_url,
            force=bool(args.force),
            progress=bool(args.progress),
        )
    )


if __name__ == "__main__":
    main()


SDC_BUILDER = build_sdc_cache
SdcBuilder = build_sdc_cache

__all__ = [
    "SDC_BUILDER",
    "SdcBuilder",
    "build_sdc_cache",
]

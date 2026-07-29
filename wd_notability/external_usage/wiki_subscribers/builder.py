#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from wd_notability.env_loader import load_default_env
from wd_notability.file_lock import acquire_file_lock
from wd_notability.lookup_cache import LookupCache
from wd_notability.lookup_backend import create_lookup_backend
from wd_notability.replica_connection import connect_replica

load_default_env()

DEFAULT_QUERY = """
SELECT DISTINCT cs_entity_id
FROM wb_changes_subscription
WHERE cs_row_id >= %s
  AND cs_row_id < %s
  AND cs_entity_id >= 'Q1'
  AND cs_entity_id < 'Q:'
  AND cs_subscriber_id <> 'wikidatawiki'
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the wiki-subscriber lookup cache.")
    parser.add_argument(
        "--output",
        default=None,
        help="Directory or file path used only for the cache lock",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=100_000,
        help="Number of wb_changes_subscription rows to scan per query block",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Pause between query blocks",
    )
    parser.add_argument(
        "--defaults-file",
        default=None,
        help="Toolforge replica defaults file",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Replica database name",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Replica host",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show a tqdm progress bar while scanning blocks",
    )
    return parser.parse_args()


def _connect(args: argparse.Namespace):
    import pymysql

    host = args.host or os.getenv("REPLICADB_HOST", "wikidatawiki.analytics.db.svc.wikimedia.cloud")
    port_value = getattr(args, "port", None)
    port = int(port_value or os.getenv("REPLICADB_PORT", "3306") or 3306)
    database = args.database or os.getenv("REPLICADB_DATABASE", "wikidatawiki_p")
    return connect_replica(pymysql, host=host, port=port, database=database)


def _fetch_scalar(conn, query: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _fetch_block(conn, start: int, end: int) -> set[str]:
    with conn.cursor() as cursor:
        cursor.execute(DEFAULT_QUERY, (start, end))
        rows = cursor.fetchall()
    qids: set[str] = set()
    for (qid,) in rows:
        if isinstance(qid, bytes):
            try:
                qid = qid.decode("utf-8")
            except UnicodeDecodeError:
                continue
        if isinstance(qid, str) and len(qid) > 1 and qid[0] == "Q" and qid[1:].isdigit():
            qids.add(qid)
    return qids


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


async def build_wikisub_cache(
    output: Path | None,
    block_size: int,
    sleep_seconds: float,
    args: argparse.Namespace,
    *,
    progress: bool = True,
) -> None:
    lock_target = output if output is not None else Path("/tmp/wd-notability/wikisub-cache")

    with acquire_file_lock(lock_target, "wikisub"):
        backend = create_lookup_backend()
        cache = LookupCache(backend=backend)
        cache.initialize()

        conn = _connect(args)
        try:
            max_row_id = _fetch_scalar(conn, "SELECT MAX(cs_row_id) FROM wb_changes_subscription")
            if max_row_id <= 0:
                backend.set_lookup_state("wikisub_high_water_mark", "0")
                raise RuntimeError("wb_changes_subscription is empty; no wiki-subscriber rows were found")

            total_blocks = max(1, (max_row_id + max(1, block_size)) // max(1, block_size))
            block_iter = range(0, max_row_id + 1, max(1, block_size))
            progress_bar = None
            if progress:
                try:
                    from tqdm import tqdm
                except ImportError:
                    print("tqdm is not installed; continuing without a progress bar")
                else:
                    progress_bar = tqdm(total=total_blocks, desc="wiki-subscriber blocks")

            try:
                total_added = 0
                started = time.perf_counter()
                with backend._connect() as db:  # noqa: SLF001
                    cursor = db.cursor()
                    cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_wiki_subscribers")
                    cursor.execute(
                        """
                        CREATE TEMPORARY TABLE temp_wiki_subscribers (
                            qid BIGINT UNSIGNED NOT NULL PRIMARY KEY
                        )
                        """
                    )

                    for block_index, start in enumerate(block_iter, start=1):
                        end = min(max_row_id + 1, start + max(1, block_size))
                        qids = _fetch_block(conn, start, end)
                        added = 0
                        if qids:
                            rows = [(int(qid[1:]),) for qid in sorted(qids)]
                            cursor.executemany(
                                """
                                INSERT INTO temp_wiki_subscribers (qid)
                                VALUES (%s)
                                ON DUPLICATE KEY UPDATE qid = qid
                                """,
                                rows,
                            )
                            db.commit()
                            added = len(rows)
                            total_added += added
                        if sleep_seconds > 0:
                            await asyncio.sleep(sleep_seconds)
                        if progress_bar is not None:
                            progress_bar.update(1)
                        elapsed = time.perf_counter() - started
                        avg_per_block = elapsed / block_index
                        remaining_blocks = total_blocks - block_index
                        eta_seconds = avg_per_block * remaining_blocks
                        eta_at = datetime.now(UTC).timestamp() + eta_seconds
                        print(
                            f"[wikisub] Block {block_index}/{total_blocks}: "
                            f"{len(qids)} unique QID(s), +{added} staged, "
                            f"elapsed {_format_duration(elapsed)}, "
                            f"ETA {_format_duration(eta_seconds)} "
                            f"(around {datetime.fromtimestamp(eta_at, tz=UTC).isoformat()})"
                        )

                    cursor.execute("SELECT COUNT(*) FROM temp_wiki_subscribers")
                    row = cursor.fetchone()
                    staged_rows = int(row[0]) if row and row[0] is not None else 0
                    print(f"Fetched {staged_rows} unique wiki-subscriber QID(s).")
                    print(
                        f"Swapping staged wiki-subscriber rows into ToolsDB from "
                        f"{((max_row_id + block_size) // block_size)} block(s)"
                    )
                    cursor.execute("START TRANSACTION")
                    try:
                        cursor.execute("DELETE FROM wiki_subscribers")
                        cursor.execute(
                            """
                            INSERT INTO wiki_subscribers (qid)
                            SELECT qid
                            FROM temp_wiki_subscribers
                            ORDER BY qid
                            """
                        )
                        db.commit()
                    except Exception:
                        db.rollback()
                        raise
                    finally:
                        cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_wiki_subscribers")

                backend.set_lookup_state("wikisub_high_water_mark", str(max_row_id))
                print(f"Wrote {staged_rows} wiki-subscriber QID rows to ToolsDB")
                print(f"High-water mark: {max_row_id}")
                print(f"Blocks processed: {((max_row_id + block_size) // block_size)}")
                print(f"Inserted rows: {total_added}")
            finally:
                if progress_bar is not None:
                    progress_bar.close()
        finally:
            conn.close()


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_wikisub_cache(
            output=Path(args.output) if args.output is not None else None,
            block_size=max(1, args.block_size),
            sleep_seconds=max(0.0, args.sleep_seconds),
            args=args,
            progress=bool(args.progress),
        )
    )


if __name__ == "__main__":
    main()


WIKI_SUBSCRIBERS_BUILDER = build_wikisub_cache
WikiSubscribersBuilder = build_wikisub_cache

__all__ = [
    "WIKI_SUBSCRIBERS_BUILDER",
    "WikiSubscribersBuilder",
    "build_wikisub_cache",
]

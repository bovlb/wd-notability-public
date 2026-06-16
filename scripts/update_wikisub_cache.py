#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from wd_notability.env_loader import load_default_env
from wd_notability.file_lock import acquire_file_lock
from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.lookup_cache import LookupCache
from wd_notability.models import NotabilityCriterion, NotabilityLevel

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
    parser = argparse.ArgumentParser(description="Advance the wiki-subscriber ratchet cache.")
    parser.add_argument(
        "--lookup-cache",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "lookup_cache.db"),
        help="Lookup cache database path",
    )
    parser.add_argument(
        "--main-cache",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "evaluation_cache.sqlite3"),
        help="Main evaluation cache path",
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
        default=str(Path.home() / "replica.my.cnf"),
        help="Toolforge replica defaults file",
    )
    parser.add_argument(
        "--database",
        default="wikidatawiki_p",
        help="Replica database name",
    )
    parser.add_argument(
        "--host",
        default="wikidatawiki.analytics.db.svc.wikimedia.cloud",
        help="Replica host",
    )
    return parser.parse_args()


def _connect(args: argparse.Namespace):
    import pymysql

    return pymysql.connect(
        read_default_file=args.defaults_file,
        host=args.host,
        database=args.database,
        charset="utf8mb4",
    )


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


async def update_wikisub_cache(
    lookup_cache_path: Path,
    main_cache_path: Path,
    block_size: int,
    sleep_seconds: float,
    args: argparse.Namespace,
) -> int:
    with acquire_file_lock(lookup_cache_path, "wikisub"):
        cache = LookupCache(lookup_cache_path)
        last_high_water = cache.get_lookup_state("wikisub_high_water_mark")
        start_row = int(last_high_water or 0) + 1

        conn = _connect(args)
        try:
            current_max = _fetch_scalar(conn, "SELECT MAX(cs_row_id) FROM wb_changes_subscription")
            if current_max < start_row:
                return 0

            new_qids: set[str] = set()
            for start in range(start_row, current_max + 1, max(1, block_size)):
                end = min(current_max + 1, start + max(1, block_size))
                qids = _fetch_block(conn, start, end)
                if qids:
                    cache.upsert_wiki_subscribers(qids)
                    new_qids.update(qids)
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)

            cache.set_lookup_state("wikisub_high_water_mark", str(current_max))
            if new_qids:
                with acquire_file_lock(main_cache_path, "main"):
                    main = EvaluationCache(main_cache_path)
                    await main.elevate(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.STRONG, new_qids)

            print(f"Advanced wiki-subscriber high-water mark from {start_row - 1} to {current_max}")
            print(f"Added {len(new_qids)} wiki-subscriber QIDs")
            return len(new_qids)
        finally:
            conn.close()


def main() -> None:
    args = parse_args()
    asyncio.run(
        update_wikisub_cache(
            lookup_cache_path=Path(args.lookup_cache),
            main_cache_path=Path(args.main_cache),
            block_size=max(1, args.block_size),
            sleep_seconds=max(0.0, args.sleep_seconds),
            args=args,
        )
    )


if __name__ == "__main__":
    main()

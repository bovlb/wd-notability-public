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
    parser = argparse.ArgumentParser(description="Build the wiki-subscriber lookup cache.")
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "lookup_cache.db"),
        help="Output lookup cache database path",
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
        "--sync-main-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Synchronize N3_wikisub in the main evaluation cache after the lookup cache is rebuilt",
    )
    parser.add_argument(
        "--sync-main-cache-only",
        action="store_true",
        help="Skip the subscription scan and only resync N3_wikisub from the existing lookup cache",
    )
    parser.add_argument(
        "--main-cache",
        default=str(Path(__file__).resolve().parents[1] / "wd_notability" / "data" / "evaluation_cache.sqlite3"),
        help="Main evaluation cache path",
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
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Show a tqdm progress bar while scanning blocks",
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


async def build_wikisub_cache(
    output: Path,
    block_size: int,
    sleep_seconds: float,
    sync_main_cache: bool,
    sync_main_cache_only: bool,
    main_cache: Path,
    args: argparse.Namespace,
    *,
    progress: bool = False,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    with acquire_file_lock(output, "wikisub"):
        cache = LookupCache(output)
        cache.initialize()

        if sync_main_cache_only:
            subscribers = cache.get_wiki_subscribers()
            if not subscribers:
                raise RuntimeError(
                    "Lookup cache has no wiki-subscriber rows to sync. "
                    "Run the full wikisub builder first."
                )
            print(f"Loaded {len(subscribers)} wiki-subscriber QID rows from {output}")
            if sync_main_cache:
                main = EvaluationCache(main_cache)
                await main.sync_criterion(
                    NotabilityCriterion.N3_WIKISUB,
                    NotabilityLevel.STRONG,
                    subscribers,
                )
            return

        conn = _connect(args)
        try:
            max_row_id = _fetch_scalar(conn, "SELECT MAX(cs_row_id) FROM wb_changes_subscription")
            if max_row_id <= 0:
                cache.replace_wiki_subscribers(set())
                cache.set_lookup_state("wikisub_high_water_mark", "0")
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
                for start in block_iter:
                    end = min(max_row_id + 1, start + max(1, block_size))
                    qids = _fetch_block(conn, start, end)
                    if qids:
                        total_added += cache.upsert_wiki_subscribers(qids)
                    if sleep_seconds > 0:
                        await asyncio.sleep(sleep_seconds)
                    if progress_bar is not None:
                        progress_bar.update(1)

                subscribers = cache.get_wiki_subscribers()
                print(f"Fetched {len(subscribers)} unique wiki-subscriber QIDs.")
                cache.replace_wiki_subscribers(subscribers)
                cache.set_lookup_state("wikisub_high_water_mark", str(max_row_id))
                print(f"Wrote {len(subscribers)} wiki-subscriber QID rows to {output}")
                print(f"High-water mark: {max_row_id}")
                print(f"Blocks processed: {((max_row_id + block_size) // block_size)}")
                print(f"Inserted rows: {total_added}")

                if sync_main_cache:
                    print(f"Syncing {len(subscribers)} wiki-subscriber QIDs to main cache...")
                    main = EvaluationCache(main_cache)
                    await main.sync_criterion(
                        NotabilityCriterion.N3_WIKISUB,
                        NotabilityLevel.STRONG,
                        subscribers,
                    )
            finally:
                if progress_bar is not None:
                    progress_bar.close()
        finally:
            conn.close()


def main() -> None:
    args = parse_args()
    asyncio.run(
        build_wikisub_cache(
            output=Path(args.output),
            block_size=max(1, args.block_size),
            sleep_seconds=max(0.0, args.sleep_seconds),
            sync_main_cache=bool(args.sync_main_cache),
            sync_main_cache_only=bool(args.sync_main_cache_only),
            main_cache=Path(args.main_cache),
            args=args,
            progress=bool(args.progress),
        )
    )


if __name__ == "__main__":
    main()

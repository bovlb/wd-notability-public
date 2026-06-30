from __future__ import annotations

import argparse
import asyncio
import os
import time
from pathlib import Path

from wd_notability.env_loader import load_default_env
from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.file_lock import acquire_file_lock
from wd_notability.localdb_paths import EVALUATION_CACHE_PATH, LOOKUP_CACHE_PATH
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

WIKISUB_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "wikisub_worker"
WIKISUB_LOOKUP_CACHE_PATH = LOOKUP_CACHE_PATH
WIKISUB_MAIN_CACHE_PATH = EVALUATION_CACHE_PATH
WIKISUB_BLOCK_SIZE = 100_000
WIKISUB_SLEEP_SECONDS = 1.0
WIKISUB_WORKER_POLL_SECONDS = 60.0
WIKISUB_DEFAULTS_FILE = Path.home() / "replica.my.cnf"
WIKISUB_DATABASE = "wikidatawiki_p"
WIKISUB_HOST = os.getenv("WD_NOTABILITY_REPLICA_HOST", "wikidatawiki.analytics.db.svc.wikimedia.cloud")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Advance the wiki-subscriber ratchet cache.")
    parser.add_argument(
        "--lookup-cache",
        default=str(WIKISUB_LOOKUP_CACHE_PATH),
        help="Lookup cache database path",
    )
    parser.add_argument(
        "--main-cache",
        default=str(WIKISUB_MAIN_CACHE_PATH),
        help="Main evaluation cache path",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=_env_int("WD_NOTABILITY_WIKISUB_BLOCK_SIZE", WIKISUB_BLOCK_SIZE),
        help="Number of wb_changes_subscription rows to scan per query block",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=_env_float("WD_NOTABILITY_WIKISUB_SLEEP_SECONDS", WIKISUB_SLEEP_SECONDS),
        help="Pause between query blocks",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=_env_float("WD_NOTABILITY_WIKISUB_POLL_SECONDS", WIKISUB_WORKER_POLL_SECONDS),
        help="Delay between successive replica polls",
    )
    parser.add_argument(
        "--defaults-file",
        default=str(WIKISUB_DEFAULTS_FILE),
        help="Toolforge replica defaults file",
    )
    parser.add_argument(
        "--database",
        default=WIKISUB_DATABASE,
        help="Replica database name",
    )
    parser.add_argument(
        "--host",
        default=WIKISUB_HOST,
        help="Replica host",
    )
    return parser.parse_args()


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


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


async def update_wikisub_cache_once(
    lookup_cache_path: Path,
    main_cache_path: Path,
    block_size: int,
    sleep_seconds: float,
    args: argparse.Namespace,
) -> int:
    with acquire_file_lock(lookup_cache_path, "wikisub"):
        cache = LookupCache(lookup_cache_path)
        cache.initialize()
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


async def wikisub_worker_loop(
    *,
    lookup_cache_path: Path,
    main_cache_path: Path,
    block_size: int,
    sleep_seconds: float,
    poll_seconds: float = WIKISUB_WORKER_POLL_SECONDS,
    args: argparse.Namespace,
) -> None:
    with acquire_file_lock(WIKISUB_WORKER_LOCK_TARGET):
        while True:
            run_started = time.monotonic()
            try:
                processed = await update_wikisub_cache_once(
                    lookup_cache_path=lookup_cache_path,
                    main_cache_path=main_cache_path,
                    block_size=block_size,
                    sleep_seconds=sleep_seconds,
                    args=args,
                )
                print(f"Wikisub worker processed {processed} qid(s)")
            except Exception as exc:  # noqa: BLE001
                print(f"Wikisub worker failed: {exc}")

            sleep_for = max(0.0, poll_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)


def main() -> None:
    args = parse_args()
    asyncio.run(
        wikisub_worker_loop(
            lookup_cache_path=Path(args.lookup_cache),
            main_cache_path=Path(args.main_cache),
            block_size=max(1, args.block_size),
            sleep_seconds=max(0.0, args.sleep_seconds),
            poll_seconds=max(0.0, args.poll_seconds),
            args=args,
        )
    )


if __name__ == "__main__":
    main()

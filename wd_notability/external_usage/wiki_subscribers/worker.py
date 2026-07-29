from __future__ import annotations

import asyncio
import os
import time
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

WIKISUB_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "wikisub_worker"
WIKISUB_BLOCK_SIZE = 25_000
WIKISUB_SLEEP_SECONDS = 1.0
WIKISUB_WORKER_POLL_SECONDS = 60.0


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


def _connect(args):
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


async def update_wikisub_cache_once(
    lookup_cache_path: Path | None,
    block_size: int,
    sleep_seconds: float,
    args,
) -> int:
    lock_target = lookup_cache_path if lookup_cache_path is not None else Path("/tmp/wd-notability/wikisub-cache")
    with acquire_file_lock(lock_target, "wikisub"):
        backend = create_lookup_backend()
        cache = LookupCache(backend=backend)
        cache.initialize()
        last_high_water = backend.get_lookup_state("wikisub_high_water_mark")
        start_row = int(last_high_water or 0) + 1

        conn = _connect(args)
        try:
            current_max = _fetch_scalar(conn, "SELECT MAX(cs_row_id) FROM wb_changes_subscription")
            if current_max < start_row:
                return 0

            processed_qids = 0
            for start in range(start_row, current_max + 1, max(1, block_size)):
                end = min(current_max + 1, start + max(1, block_size))
                qids = _fetch_block(conn, start, end)
                if qids:
                    processed_qids += cache.upsert_wiki_subscribers(qids)
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)

            backend.set_lookup_state("wikisub_high_water_mark", str(current_max))
            print(f"Advanced wiki-subscriber high-water mark from {start_row - 1} to {current_max}")
            print(f"Added {processed_qids} wiki-subscriber QIDs")
            return processed_qids
        finally:
            conn.close()


async def wikisub_worker_loop(
    *,
    lookup_cache_path: Path | None,
    block_size: int,
    sleep_seconds: float,
    poll_seconds: float = WIKISUB_WORKER_POLL_SECONDS,
    args,
) -> None:
    with acquire_file_lock(WIKISUB_WORKER_LOCK_TARGET):
        while True:
            run_started = time.monotonic()
            try:
                processed = await update_wikisub_cache_once(
                    lookup_cache_path=lookup_cache_path,
                    block_size=block_size,
                    sleep_seconds=sleep_seconds,
                    args=args,
                )
                print(f"Wikisub worker processed {processed} qid(s)")
            except Exception as exc:  # noqa: BLE001
                print(f"Wikisub worker failed: {exc}")

            sleep_for = max(0.0, poll_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)

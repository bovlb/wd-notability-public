from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


async def upsert_pubsub_interest_rows(
    cache: "EvaluationCache",
    *,
    worker_id: str,
    qids: list[int],
    priority: int,
    wants_creation: bool,
    wants_content: bool,
    wants_inlinks: bool,
) -> int:
    await cache.initialize()

    worker = worker_id.strip()
    if not worker:
        raise ValueError("worker_id must not be empty")

    worker_priority = cache._as_uint32(priority, "priority")
    qid_nums: list[int] = []
    seen: set[int] = set()
    for qid in qids:
        qid_num = cache._as_uint32(qid, "qid")
        if qid_num in seen:
            continue
        seen.add(qid_num)
        qid_nums.append(qid_num)
    if not qid_nums:
        return 0

    started = time.perf_counter()
    async with cache._write_guard():
        async with cache._connect() as db:
            rows = [
                (
                    worker,
                    qid_num,
                    worker_priority,
                    1 if wants_creation else 0,
                    1 if wants_content else 0,
                    1 if wants_inlinks else 0,
                )
                for qid_num in qid_nums
            ]
            if cache._backend_name == "mariadb":
                await db.executemany(
                    """
                    INSERT INTO interest (
                        worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        priority = VALUES(priority),
                        wants_creation = VALUES(wants_creation),
                        wants_content = VALUES(wants_content),
                        wants_inlinks = VALUES(wants_inlinks)
                    """,
                    rows,
                )
            else:
                await db.executemany(
                    """
                    INSERT INTO interest (
                        worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(worker_id, qid) DO UPDATE SET
                        priority = excluded.priority,
                        wants_creation = excluded.wants_creation,
                        wants_content = excluded.wants_content,
                        wants_inlinks = excluded.wants_inlinks
                    """,
                    rows,
                )
    cache._warn_slow_write("upsert_pubsub_interest_rows", started, row_count=len(qid_nums))
    return len(qid_nums)


async def delete_pubsub_interest_rows(
    cache: "EvaluationCache",
    *,
    worker_id: str,
    qids: list[int],
) -> int:
    await cache.initialize()

    worker = worker_id.strip()
    if not worker:
        raise ValueError("worker_id must not be empty")

    qid_nums: list[int] = []
    seen: set[int] = set()
    for qid in qids:
        qid_num = cache._as_uint32(qid, "qid")
        if qid_num in seen:
            continue
        seen.add(qid_num)
        qid_nums.append(qid_num)
    if not qid_nums:
        return 0

    started = time.perf_counter()
    async with cache._write_guard():
        async with cache._connect() as db:
            placeholders = ", ".join("?" for _ in qid_nums)
            cursor = await db.execute(
                f"DELETE FROM interest WHERE worker_id = ? AND qid IN ({placeholders})",
                [worker, *qid_nums],
            )
    cache._warn_slow_write("delete_pubsub_interest_rows", started, row_count=int(cursor.rowcount))
    return int(cursor.rowcount)


async def delete_interest_for_owner(cache: "EvaluationCache", *, owner_id: str) -> int:
    owner = cache._normalize_owner_id(owner_id)
    started = time.perf_counter()
    async with cache._write_guard():
        async with cache._connect() as db:
            cursor = await db.execute(
                """
                DELETE FROM interest
                WHERE worker_id = ?
                   OR worker_id LIKE ?
                """,
                (owner, f"{owner}:%"),
            )
    cache._warn_slow_write("delete_interest_for_owner", started, row_count=int(cursor.rowcount))
    return int(cursor.rowcount)


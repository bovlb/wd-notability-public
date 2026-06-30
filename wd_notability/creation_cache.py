from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

from wd_notability.creations import CreationMetadata, _to_epoch_seconds

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


def _creation_fields_from_item(cache: "EvaluationCache", item: object) -> tuple[int, int, int] | None:
    qid = getattr(item, "qid", None)
    qid_num = cache._parse_qid(qid)
    creator_actor_id = getattr(item, "creator_actor_id", None)
    creation_time = getattr(item, "creation_time", None)
    if qid_num is None or creator_actor_id is None or creation_time is None:
        return None
    creation_time_num = _to_epoch_seconds(creation_time)
    if creation_time_num is None:
        return None
    try:
        creator_actor_id_num = cache._as_uint32(creator_actor_id, "creator_actor_id")
    except ValueError:
        return None
    return qid_num, creation_time_num, creator_actor_id_num


async def upsert_creation_metadata_many(cache: "EvaluationCache", items: Sequence[object]) -> int:
    await cache.initialize()

    if not items:
        return 0

    normalized: list[tuple[int, int, int]] = []
    seen: set[int] = set()
    for item in items:
        normalized_item = _creation_fields_from_item(cache, item)
        if normalized_item is None:
            continue
        qid_num, creation_time, creator_actor_id = normalized_item
        if qid_num in seen:
            continue
        seen.add(qid_num)
        normalized.append((qid_num, creation_time, creator_actor_id))

    if not normalized:
        return 0

    started = time.perf_counter()
    updated = 0

    if cache._backend_name == "mariadb":
        async with cache._write_guard():
            async with cache._connect() as db:
                for chunk in cache._chunked(normalized):
                    await db.execute("BEGIN IMMEDIATE")
                    values_sql = ", ".join("(%s, 0, %s, %s)" for _ in chunk)
                    params: list[int] = []
                    for qid_num, creation_time, creator_actor_id in chunk:
                        params.extend([qid_num, creation_time, creator_actor_id])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, creation_time, creator_actor_id)
                        VALUES {values_sql}
                        ON DUPLICATE KEY UPDATE
                            creation_time = VALUES(creation_time),
                            creator_actor_id = VALUES(creator_actor_id)
                        RETURNING qid
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    updated += len(rows)
                    await db.commit()
        cache._warn_slow_write("upsert_creation_metadata_many", started, row_count=len(normalized))
        return updated

    async with cache._write_guard():
        async with cache._connect() as db:
            for chunk in cache._chunked(normalized):
                await db.execute("BEGIN IMMEDIATE")
                values_sql = ", ".join("(?, 0, ?, ?)" for _ in chunk)
                params: list[int] = []
                for qid_num, creation_time, creator_actor_id in chunk:
                    params.extend([qid_num, creation_time, creator_actor_id])
                cursor = await db.execute(
                    f"""
                    INSERT INTO evaluation_cache (qid, summary, creation_time, creator_actor_id)
                    VALUES {values_sql}
                    ON CONFLICT(qid) DO UPDATE SET
                        creation_time = excluded.creation_time,
                        creator_actor_id = excluded.creator_actor_id
                    RETURNING qid
                    """,
                    params,
                )
                rows = await cursor.fetchall()
                updated += len(rows)
                await db.commit()

    cache._warn_slow_write("upsert_creation_metadata_many", started, row_count=len(normalized))
    return updated


async def list_missing_creation_qids(cache: "EvaluationCache", limit: int | None = None) -> list[str]:
    await cache.initialize()

    async with cache._connect() as db:
        if limit is None:
            cursor = await db.execute(
                """
                SELECT qid
                FROM evaluation_cache
                WHERE creation_time IS NULL
                   OR creator_actor_id IS NULL
                ORDER BY qid DESC
                """
            )
        else:
            cursor = await db.execute(
                """
                SELECT qid
                FROM evaluation_cache
                WHERE creation_time IS NULL
                   OR creator_actor_id IS NULL
                ORDER BY qid DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cursor.fetchall()

    return [f"Q{int(row[0])}" for row in rows]


async def count_missing_creation_qids(cache: "EvaluationCache") -> int:
    await cache.initialize()

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM evaluation_cache
            WHERE creation_time IS NULL
               OR creator_actor_id IS NULL
            """
        )
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


async def list_creation_metadata(
    cache: "EvaluationCache",
    *,
    start: str | None = None,
    end: str | None = None,
    creator_actor_ids: Sequence[object] | None = None,
) -> list[CreationMetadata]:
    await cache.initialize()

    start_epoch = _to_epoch_seconds(start) if start is not None else None
    end_epoch = _to_epoch_seconds(end) if end is not None else None

    creator_ids: list[int] = []
    seen: set[int] = set()
    for value in creator_actor_ids or []:
        try:
            creator_id = cache._as_uint32(value, "creator_actor_id")
        except ValueError:
            continue
        if creator_id in seen:
            continue
        seen.add(creator_id)
        creator_ids.append(creator_id)

    where_clauses = [
        "creation_time IS NOT NULL",
        "creator_actor_id IS NOT NULL",
    ]
    params: list[object] = []
    if start_epoch is not None:
        where_clauses.append("creation_time >= ?")
        params.append(start_epoch)
    if end_epoch is not None:
        where_clauses.append("creation_time < ?")
        params.append(end_epoch)
    if creator_ids:
        placeholders = ", ".join(["?"] * len(creator_ids))
        where_clauses.append(f"creator_actor_id IN ({placeholders})")
        params.extend(creator_ids)

    query = f"""
        SELECT qid, creator_actor_id, creation_time
        FROM evaluation_cache
        WHERE {' AND '.join(where_clauses)}
        ORDER BY creation_time ASC, qid ASC
    """

    async with cache._connect() as db:
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()

    result: list[CreationMetadata] = []
    for qid, creator_actor_id, creation_time in rows:
        normalized_qid = f"Q{int(qid)}"
        try:
            creator_actor_id_num = int(creator_actor_id)
        except (TypeError, ValueError):
            continue
        if creation_time is None:
            continue
        creation_time_num = _to_epoch_seconds(creation_time)
        if creation_time_num is None:
            continue
        result.append(
            CreationMetadata(
                qid=normalized_qid,
                creator_actor_id=creator_actor_id_num,
                creation_time=creation_time_num,
            )
        )
    return result


async def get_creation_metadata_many(
    cache: "EvaluationCache",
    qids: Sequence[object],
) -> dict[str, CreationMetadata]:
    await cache.initialize()

    qid_nums: list[int] = []
    qid_lookup: dict[int, str] = {}
    seen: set[int] = set()
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in seen:
            continue
        seen.add(qid_num)
        qid_nums.append(qid_num)
        qid_lookup[qid_num] = cache._normalize_qid(qid)

    if not qid_nums:
        return {}

    result: dict[str, CreationMetadata] = {}
    chunk_size = 500
    async with cache._connect() as db:
        for start in range(0, len(qid_nums), chunk_size):
            chunk = qid_nums[start : start + chunk_size]
            placeholders = ", ".join(["?"] * len(chunk))
            cursor = await db.execute(
                f"""
                SELECT qid, creator_actor_id, creation_time
                FROM evaluation_cache
                WHERE qid IN ({placeholders})
                  AND creator_actor_id IS NOT NULL
                  AND creation_time IS NOT NULL
                ORDER BY qid ASC
                """,
                chunk,
            )
            rows = await cursor.fetchall()
            for qid, creator_actor_id, creation_time in rows:
                try:
                    creator_actor_id_num = int(creator_actor_id)
                except (TypeError, ValueError):
                    continue
                if creation_time is None:
                    continue
                creation_time_num = _to_epoch_seconds(creation_time)
                if creation_time_num is None:
                    continue
                normalized_qid = qid_lookup.get(int(qid))
                if normalized_qid is None:
                    continue
                result[normalized_qid] = CreationMetadata(
                    qid=normalized_qid,
                    creator_actor_id=creator_actor_id_num,
                    creation_time=creation_time_num,
                )
    return result

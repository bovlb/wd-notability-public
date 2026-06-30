from __future__ import annotations

from typing import TYPE_CHECKING
import time

from wd_notability import summary as summary_bits
from wd_notability.models import NotabilityCriterion, NotabilityLevel

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


async def list_unknown_inlinks_qids(cache: "EvaluationCache", limit: int | None = None) -> list[str]:
    await cache.initialize()

    n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
    n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)

    async with cache._connect() as db:
        if limit is None:
            cursor = await db.execute(
                """
                SELECT qid
                FROM evaluation_cache
                WHERE (summary & ?) = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pubsub_sessions s
                      WHERE s.qid = evaluation_cache.qid
                        AND s.qid != 0
                        AND s.wants_inlinks = 1
                        AND s.owner_id != 'inlinks'
                  )
                ORDER BY qid ASC
                """,
                (n3_inlinks_mask, n3_inlinks_unknown),
            )
        else:
            cursor = await db.execute(
                """
                SELECT qid
                FROM evaluation_cache
                WHERE (summary & ?) = ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pubsub_sessions s
                      WHERE s.qid = evaluation_cache.qid
                        AND s.qid != 0
                        AND s.wants_inlinks = 1
                        AND s.owner_id != 'inlinks'
                  )
                ORDER BY qid ASC
                LIMIT ?
                """,
                (n3_inlinks_mask, n3_inlinks_unknown, limit),
            )
        rows = await cursor.fetchall()

    return [f"Q{int(row[0])}" for row in rows]


async def count_unknown_inlinks_qids(cache: "EvaluationCache") -> int:
    await cache.initialize()

    n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
    n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM evaluation_cache
            WHERE (summary & ?) = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM pubsub_sessions s
                  WHERE s.qid = evaluation_cache.qid
                    AND s.qid != 0
                    AND s.wants_inlinks = 1
                    AND s.owner_id != 'inlinks'
              )
            """,
            (n3_inlinks_mask, n3_inlinks_unknown),
        )
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


async def list_known_inlinks_refresh_candidates(
    cache: "EvaluationCache",
    limit: int | None = None,
) -> list[tuple[str, str, int]]:
    await cache.initialize()

    n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
    n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)
    deleted_mask = summary_bits.DELETED

    async with cache._connect() as db:
        base_query = """
            SELECT
                ec.qid,
                ec.creation_time,
                ec.inlinks_last_evaluated
            FROM evaluation_cache ec
            WHERE ec.qid != 0
              AND ec.creation_time IS NOT NULL
              AND ec.inlinks_last_evaluated IS NOT NULL
              AND (ec.summary & ?) != ?
              AND (ec.summary & ?) = 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM pubsub_sessions s
                  WHERE s.qid = ec.qid
                    AND s.qid != 0
                    AND s.wants_inlinks = 1
                    AND s.owner_id != 'inlinks'
              )
            ORDER BY ec.creation_time ASC, ec.inlinks_last_evaluated ASC, ec.qid ASC
        """
        if limit is None:
            cursor = await db.execute(base_query, (n3_inlinks_mask, n3_inlinks_unknown, deleted_mask))
        else:
            cursor = await db.execute(base_query + " LIMIT ?", (n3_inlinks_mask, n3_inlinks_unknown, deleted_mask, limit))
        rows = await cursor.fetchall()

    result: list[tuple[str, int, int]] = []
    for qid, creation_time, inlinks_last_evaluated in rows:
        if creation_time is None or inlinks_last_evaluated is None:
            continue
        from wd_notability.evaluation_cache import _to_epoch_seconds

        creation_time_num = _to_epoch_seconds(creation_time)
        if creation_time_num is None:
            continue
        result.append((f"Q{int(qid)}", creation_time_num, int(inlinks_last_evaluated)))
    return result


async def count_known_inlinks_refresh_candidates(cache: "EvaluationCache") -> int:
    await cache.initialize()

    n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
    n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)
    deleted_mask = summary_bits.DELETED

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM evaluation_cache ec
            WHERE ec.qid != 0
              AND ec.creation_time IS NOT NULL
              AND ec.inlinks_last_evaluated IS NOT NULL
              AND (ec.summary & ?) != ?
              AND (ec.summary & ?) = 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM pubsub_sessions s
                  WHERE s.qid = ec.qid
                    AND s.qid != 0
                    AND s.wants_inlinks = 1
                    AND s.owner_id != 'inlinks'
              )
            """,
            (n3_inlinks_mask, n3_inlinks_unknown, deleted_mask),
        )
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


async def upsert_inlinks_strong_many(
    cache: "EvaluationCache",
    qids: list[str | int],
    *,
    inlinks_last_evaluated: int,
) -> list[tuple[str, int]]:
    await cache.initialize()

    normalized: list[int] = []
    seen: set[int] = set()
    for qid in qids:
        try:
            qid_num = cache._parse_qid(qid)
        except ValueError:
            continue
        if qid_num in seen:
            continue
        seen.add(qid_num)
        normalized.append(qid_num)

    if not normalized:
        return []

    started = time.perf_counter()
    changed_rows: list[tuple[str, int]] = []
    inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
    strong_value = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.STRONG)
    chunk_size = 500

    async with cache._write_guard():
        async with cache._connect() as db:
            for chunk_start in range(0, len(normalized), chunk_size):
                chunk = normalized[chunk_start : chunk_start + chunk_size]
                await db.execute("BEGIN IMMEDIATE")
                values_sql = ", ".join(("(%s, %s, %s, %s)" if cache._backend_name == "mariadb" else "(?, ?, ?, ?)") for _ in chunk)
                params: list[int] = []
                for qid_num in chunk:
                    params.extend([qid_num, strong_value, inlinks_last_evaluated, inlinks_last_evaluated])
                if cache._backend_name == "mariadb":
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, last_updated, inlinks_last_evaluated)
                        VALUES {values_sql}
                        ON DUPLICATE KEY UPDATE
                            summary = (evaluation_cache.summary & ~{inlinks_mask}) | (VALUES(summary) & {inlinks_mask}),
                            last_updated = IF(
                                (evaluation_cache.summary & ~{inlinks_mask}) | (VALUES(summary) & {inlinks_mask}) <> evaluation_cache.summary,
                                VALUES(last_updated),
                                evaluation_cache.last_updated
                            ),
                            inlinks_last_evaluated = VALUES(inlinks_last_evaluated)
                        RETURNING qid, summary, last_updated, inlinks_last_evaluated
                        """,
                        params,
                    )
                else:
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, last_updated, inlinks_last_evaluated)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{inlinks_mask}) | (excluded.summary & {inlinks_mask}),
                            last_updated = CASE
                                WHEN (evaluation_cache.summary & ~{inlinks_mask}) | (excluded.summary & {inlinks_mask}) <> evaluation_cache.summary
                                THEN excluded.last_updated
                                ELSE evaluation_cache.last_updated
                            END,
                            inlinks_last_evaluated = excluded.inlinks_last_evaluated
                        RETURNING qid, summary, last_updated, inlinks_last_evaluated
                        """,
                        params,
                    )
                rows = await cursor.fetchall()
                changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                await db.commit()

    cache._warn_slow_write("upsert_inlinks_strong_many", started, row_count=len(normalized))
    return changed_rows

"""Report-facing SQL helpers for inlinks queue/debug views."""

from __future__ import annotations

from typing import TYPE_CHECKING

from wd_notability.inlinks.db_read import _to_optional_epoch_seconds

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


async def list_unknown_inlinks_qids(cache: "EvaluationCache", limit: int | None = None) -> list[str]:
    """Used by server.report_api._fetch_queue_report() for the cache-only fallback queue."""
    await cache.initialize()
    async with cache._connect() as db:
        if limit is None:
            cursor = await db.execute(
                """
                SELECT ic.qid
                FROM inlinks_cache ic
                LEFT JOIN content_evaluation ce
                  ON ce.qid = ic.qid
                WHERE COALESCE(ic.n3_inlinks, 4) = 4
                  AND NOT EXISTS (
                      SELECT 1
                      FROM interest s
                      WHERE s.qid = ic.qid
                        AND s.wants_inlinks = 1
                  )
                ORDER BY ic.qid ASC
                """,
            )
        else:
            cursor = await db.execute(
                """
                SELECT ic.qid
                FROM inlinks_cache ic
                LEFT JOIN content_evaluation ce
                  ON ce.qid = ic.qid
                WHERE COALESCE(ic.n3_inlinks, 4) = 4
                  AND NOT EXISTS (
                      SELECT 1
                      FROM interest s
                      WHERE s.qid = ic.qid
                        AND s.wants_inlinks = 1
                  )
                ORDER BY ic.qid ASC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cursor.fetchall()
    return [f"Q{int(row[0])}" for row in rows]


async def count_unknown_inlinks_qids(cache: "EvaluationCache") -> int:
    """Used alongside the queue report to expose the cache-only fallback total."""
    await cache.initialize()
    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM inlinks_cache ic
            LEFT JOIN content_evaluation ce
              ON ce.qid = ic.qid
            WHERE COALESCE(ic.n3_inlinks, 4) = 4
              AND NOT EXISTS (
                  SELECT 1
                  FROM interest s
                  WHERE s.qid = ic.qid
                    AND s.wants_inlinks = 1
              )
            """,
        )
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


async def list_known_inlinks_refresh_candidates(
    cache: "EvaluationCache",
    limit: int | None = None,
) -> list[tuple[str, str, int]]:
    """Used by server.report_api._fetch_queue_report() for the low-priority refresh queue."""
    await cache.initialize()
    async with cache._connect() as db:
        base_query = """
            SELECT
                ic.qid,
                rc.creation_time,
                ic.inlinks_last_evaluated
            FROM inlinks_cache ic
            LEFT JOIN content_evaluation ce
              ON ce.qid = ic.qid
            LEFT JOIN recent_changes_cache rc
              ON rc.qid = ic.qid
            WHERE ic.qid != 0
              AND rc.creation_time IS NOT NULL
              AND ic.inlinks_last_evaluated IS NOT NULL
              AND COALESCE(ic.inlinks_count, 0) = 0
              AND COALESCE(ic.n3_inlinks, 4) != 4
              AND COALESCE(ce.deleted, 0) = 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM interest s
                  WHERE s.qid = ic.qid
                    AND s.qid != 0
                    AND s.wants_inlinks = 1
                    AND s.worker_id != 'inlinks'
              )
            ORDER BY rc.creation_time ASC, ic.inlinks_last_evaluated ASC, ic.qid ASC
        """
        if limit is None:
            cursor = await db.execute(base_query)
        else:
            cursor = await db.execute(base_query + " LIMIT ?", (limit,))
        rows = await cursor.fetchall()
    result: list[tuple[str, int, int]] = []
    for qid, creation_time, inlinks_last_evaluated in rows:
        if creation_time is None or inlinks_last_evaluated is None:
            continue
        creation_time_num = _to_optional_epoch_seconds(creation_time)
        if creation_time_num is None:
            continue
        result.append((f"Q{int(qid)}", creation_time_num, int(inlinks_last_evaluated)))
    return result


async def count_known_inlinks_refresh_candidates(cache: "EvaluationCache") -> int:
    """Used alongside the queue report to expose the low-priority refresh total."""
    await cache.initialize()
    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM inlinks_cache ic
            LEFT JOIN content_evaluation ce
              ON ce.qid = ic.qid
            LEFT JOIN recent_changes_cache rc
              ON rc.qid = ic.qid
            WHERE ic.qid != 0
              AND rc.creation_time IS NOT NULL
              AND ic.inlinks_last_evaluated IS NOT NULL
              AND COALESCE(ic.inlinks_count, 0) = 0
              AND COALESCE(ic.n3_inlinks, 4) != 4
              AND COALESCE(ce.deleted, 0) = 0
              AND NOT EXISTS (
                  SELECT 1
                  FROM interest s
                  WHERE s.qid = ic.qid
                    AND s.qid != 0
                    AND s.wants_inlinks = 1
                    AND s.worker_id != 'inlinks'
              )
            """,
        )
        row = await cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else 0

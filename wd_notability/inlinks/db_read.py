"""Worker-facing SQL helpers for the inlinks pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


def _to_optional_epoch_seconds(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value.astimezone(
            UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(dt.timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def list_inlinks_work_candidates(
    cache: "EvaluationCache",
    limit: int | None = None,
) -> list[tuple[str, int | None, int, bool]]:
    """Used by server.routes_api.api_inlinks_candidates() for the inlinks candidate API."""
    await cache.initialize()
    async with cache._connect() as db:
        base_query = """
            WITH active_interest AS (
                SELECT
                    s.qid AS qid,
                    COALESCE(SUM(COALESCE(s.priority, 0)), 0) AS active_priority
                FROM interest s
                WHERE s.wants_inlinks = 1
                GROUP BY s.qid
            ),
            candidate_base AS (
                SELECT
                    ai.qid,
                    ic.inlinks_last_evaluated,
                    COALESCE(ai.active_priority, 0) AS active_priority,
                    CASE WHEN COALESCE(ic.n3_inlinks, 4) = 4 THEN 1 ELSE 0 END AS is_unknown
                FROM active_interest ai
                LEFT JOIN inlinks_cache ic
                  ON ic.qid = ai.qid
            ),
            scored_candidates AS (
                SELECT
                    qid,
                    inlinks_last_evaluated,
                    active_priority,
                    is_unknown
                FROM candidate_base
            )
            SELECT
                qid,
                inlinks_last_evaluated,
                active_priority,
                is_unknown
            FROM scored_candidates
            ORDER BY
                CASE
                    WHEN inlinks_last_evaluated IS NULL THEN 0
                    WHEN is_unknown = 1 THEN 1
                    ELSE 2
                END,
                COALESCE(inlinks_last_evaluated, 0) ASC,
                active_priority DESC,
                qid ASC
        """
        if limit is None:
            cursor = await db.execute(base_query, [])
        else:
            cursor = await db.execute(f"{base_query}\nLIMIT ?", [limit])
        rows = await cursor.fetchall()
    result: list[tuple[str, int | None, int, bool]] = []
    for qid, inlinks_last_evaluated, active_priority, is_unknown in rows:
        result.append(
            (
                f"Q{int(qid)}",
                _to_optional_epoch_seconds(inlinks_last_evaluated),
                int(active_priority) if active_priority is not None else 0,
                bool(is_unknown),
            )
        )
    return result


async def count_inlinks_work_candidates(cache: "EvaluationCache") -> dict[str, int]:
    await cache.initialize()
    async with cache._connect() as db:
        cursor = await db.execute(
            """
            WITH active_interest AS (
                SELECT
                    s.qid AS qid,
                    COALESCE(SUM(COALESCE(s.priority, 0)), 0) AS active_priority
                FROM interest s
                WHERE s.wants_inlinks = 1
                GROUP BY s.qid
            )
            SELECT
                CASE WHEN COALESCE(ic.n3_inlinks, 4) = 4 THEN 1 ELSE 0 END AS is_unknown,
                CASE WHEN COALESCE(ai.active_priority, 0) > 0 THEN 1 ELSE 0 END AS has_interest,
                COUNT(DISTINCT ai.qid) AS count
            FROM active_interest ai
            LEFT JOIN inlinks_cache ic
              ON ic.qid = ai.qid
            WHERE ai.qid != 0
              AND COALESCE(ic.inlinks_count, 0) = 0
              AND (COALESCE(ic.n3_inlinks, 4) = 4 OR ic.inlinks_last_evaluated IS NOT NULL)
            GROUP BY is_unknown, has_interest
            """,
            (),
        )
        rows = await cursor.fetchall()
    counts = {
        "unknown_active": 0,
        "unknown_idle": 0,
        "refresh_active": 0,
        "refresh_idle": 0,
        "total": 0,
    }
    for is_unknown, has_interest, count in rows:
        bucket = (
            ("unknown" if int(is_unknown) else "refresh")
            + "_"
            + ("active" if int(has_interest) else "idle")
        )
        counts[bucket] = int(count)
        counts["total"] += int(count)
    return counts


async def list_pubsub_inlinks_targets_with_state(
    cache: "EvaluationCache",
    limit: int | None = None,
) -> list[tuple[str, int, int | None, int | None, int | None]]:
    await cache.initialize()

    async with cache._connect() as db:
        if limit is None:
            cursor = await db.execute(
                """
                SELECT
                    s.qid,
                    COALESCE(SUM(COALESCE(s.priority, 10)), 0) AS subscriber_priority,
                    ic.inlinks_count AS inlinks_count,
                    COALESCE(ic.n3_inlinks, 4) AS n3_inlinks,
                    ic.inlinks_last_evaluated
                FROM interest s
                LEFT JOIN inlinks_cache ic
                  ON ic.qid = s.qid
                WHERE s.qid != 0
                  AND s.wants_inlinks = 1
                  AND s.worker_id != 'inlinks'
                GROUP BY s.qid
                ORDER BY subscriber_priority DESC, s.qid ASC
                """,
                (),
            )
        else:
            cursor = await db.execute(
                """
                SELECT
                    s.qid,
                    COALESCE(SUM(COALESCE(s.priority, 10)), 0) AS subscriber_priority,
                    ic.inlinks_count AS inlinks_count,
                    COALESCE(ic.n3_inlinks, 4) AS n3_inlinks,
                    ic.inlinks_last_evaluated
                FROM interest s
                LEFT JOIN inlinks_cache ic
                  ON ic.qid = s.qid
                WHERE s.qid != 0
                  AND s.wants_inlinks = 1
                  AND s.worker_id != 'inlinks'
                GROUP BY s.qid
                ORDER BY subscriber_priority DESC, s.qid ASC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cursor.fetchall()

    result: list[tuple[str, int, int | None, int | None, int | None]] = []
    for row in rows:
        qid = f"Q{int(row[0])}"
        priority = int(row[1]) if row[1] is not None else 0
        inlinks_count = int(row[2]) if row[2] is not None else None
        n3_inlinks = int(row[3]) if row[3] is not None else None
        inlinks_last_evaluated = _to_optional_epoch_seconds(row[4])
        result.append((qid, priority, inlinks_count,
                      n3_inlinks, inlinks_last_evaluated))
    return result


async def list_pubsub_inlinks_targets(cache: "EvaluationCache", limit: int | None = None) -> list[str]:
    rows = await list_pubsub_inlinks_targets_with_state(cache, limit=limit)
    return [qid for qid, *_rest in rows]


async def count_pubsub_inlinks_targets(cache: "EvaluationCache") -> int:
    await cache.initialize()

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT COUNT(DISTINCT s.qid)
            FROM interest s
            WHERE s.qid != 0
              AND s.wants_inlinks = 1
              AND s.worker_id != 'inlinks'
            """,
            (),
        )
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


async def has_pubsub_inlinks_interest(cache: "EvaluationCache", qid: str | int) -> bool:
    await cache.initialize()

    qid_num = cache._parse_qid(qid)
    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT 1
            FROM interest
            WHERE qid = ?
              AND qid != 0
              AND wants_inlinks = 1
              AND worker_id != 'inlinks'
            LIMIT 1
            """,
            (qid_num,),
        )
        row = await cursor.fetchone()
    return row is not None

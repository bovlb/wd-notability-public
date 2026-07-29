from __future__ import annotations

import calendar
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from wd_notability.creations import CreationMetadata
from wd_notability.lookup_cache import lookup_cache
from wd_notability.models import (
    EvaluationResult,
    NotabilityCriterion,
    NotabilityLevel,
    deduce_n2,
    external_usage_level,
)

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


def _to_epoch_seconds(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(dt.timestamp())
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return int(text)
        except ValueError:
            return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(dt.timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def get_many(cache: "EvaluationCache", qids: list[str | int]) -> dict[str, EvaluationResult]:
    await cache.initialize()

    qid_nums: list[int] = []
    qid_lookup: dict[int, str] = {}
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in qid_lookup:
            continue
        qid_nums.append(qid_num)
        qid_lookup[qid_num] = f"Q{qid_num}"

    if not qid_nums:
        return {}

    chunk_size = 500
    rows: dict[str, EvaluationResult] = {}
    async with cache._connect() as db:
        for start in range(0, len(qid_nums), chunk_size):
            chunk = qid_nums[start: start + chunk_size]
            values_sql = " UNION ALL ".join(
                ["SELECT ? AS qid", *(["SELECT ?"] * (len(chunk) - 1))]
            )
            cursor = await db.execute(
                f"""
                WITH qids AS (
                    {values_sql}
                )
                SELECT
                    q.qid,
                    ce.content_last_revid,
                    ce.redirect_target,
                    ce.has_sitelinks_count,
                    ce.has_claims_count,
                    ce.deleted,
                    ce.n1,
                    ce.n2a,
                    ce.n2b,
                    COALESCE(ic.inlinks_count, 0),
                    COALESCE(ic.n3_inlinks, 4),
                    ic.inlinks_last_evaluated,
                    rc.recent_changes_last_revid,
                    ou.count_all,
                    ou.count_nodes,
                    ou.count_ways,
                    ou.count_relations,
                    su.usage_count,
                    ws.qid
                FROM qids q
                LEFT JOIN content_evaluation ce
                  ON ce.qid = q.qid
                LEFT JOIN inlinks_cache ic
                  ON ic.qid = q.qid
                LEFT JOIN recent_changes_cache rc
                  ON rc.qid = q.qid
                LEFT JOIN osm_usage ou
                  ON ou.qid = q.qid
                LEFT JOIN sdc_usage su
                  ON su.qid = q.qid
                LEFT JOIN wiki_subscribers ws
                  ON ws.qid = q.qid
                ORDER BY q.qid
                """,
                chunk,
            )
            for row in await cursor.fetchall():
                qid_num = int(row[0])
                qid_text = qid_lookup.get(qid_num)
                if qid_text is None:
                    continue

                content_last_revid = None if row[1] is None else int(row[1])
                redirect_target = row[2]
                has_sitelinks_count = int(row[3]) if row[3] is not None else 0
                has_claims_count = int(row[4]) if row[4] is not None else 0
                is_deleted = bool(row[5]) if row[5] is not None else False
                n1 = NotabilityLevel(int(row[6])) if row[6] is not None else NotabilityLevel.UNKNOWN
                n2a = NotabilityLevel(int(row[7])) if row[7] is not None else NotabilityLevel.UNKNOWN
                n2b = NotabilityLevel(int(row[8])) if row[8] is not None else NotabilityLevel.UNKNOWN
                inlinks_count = int(row[9]) if row[9] is not None else 0
                n3_inlinks = NotabilityLevel(int(row[10])) if row[10] is not None else NotabilityLevel.UNKNOWN
                recent_changes_last_revid = None if row[12] is None else int(row[12])

                if row[1] is None and row[9] is None and row[12] is None:
                    continue

                result = EvaluationResult(
                    qid=qid_text,
                    n1=n1,
                    n2a=n2a,
                    n2b=n2b,
                    n3_inlinks=n3_inlinks,
                    inlinks_count=inlinks_count,
                    has_claims_count=has_claims_count,
                    has_claims_known=row[1] is not None,
                    has_sitelinks_count=has_sitelinks_count,
                    is_redirect=redirect_target is not None,
                    redirect_target=None if redirect_target is None else int(redirect_target),
                    is_deleted=is_deleted,
                    content_last_revid=content_last_revid,
                    recent_changes_last_revid=recent_changes_last_revid,
                )
                result.set(
                    NotabilityCriterion.N3_OSM,
                    external_usage_level(NotabilityCriterion.N3_OSM) if any(value is not None for value in row[13:17]) else NotabilityLevel.NONE,
                )
                result.set(
                    NotabilityCriterion.N3_SDC,
                    external_usage_level(NotabilityCriterion.N3_SDC) if row[17] is not None else NotabilityLevel.NONE,
                )
                result.set(
                    NotabilityCriterion.N3_WIKISUB,
                    external_usage_level(NotabilityCriterion.N3_WIKISUB) if row[18] is not None else NotabilityLevel.NONE,
                )
                rows[qid_text] = result

    return rows


async def get_many_with_creation_metadata(
    cache: "EvaluationCache",
    qids: list[str | int],
) -> tuple[dict[str, EvaluationResult], dict[str, CreationMetadata]]:
    await cache.initialize()

    qid_nums: list[int] = []
    qid_lookup: dict[int, str] = {}
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in qid_lookup:
            continue
        qid_nums.append(qid_num)
        qid_lookup[qid_num] = cache._normalize_qid(qid)

    if not qid_nums:
        return {}, {}

    chunk_size = 500
    rows: dict[int, dict[str, object]] = {}
    creation_metadata: dict[str, CreationMetadata] = {}
    async with cache._connect() as db:
        for start in range(0, len(qid_nums), chunk_size):
            chunk = qid_nums[start: start + chunk_size]
            values_sql = " UNION ALL ".join(
                ["SELECT ? AS qid", *(["SELECT ?"] * (len(chunk) - 1))]
            )
            cursor = await db.execute(
                f"""
                WITH qids AS (
                    {values_sql}
                )
                SELECT
                    q.qid,
                    ce.qid,
                    ce.content_last_revid,
                    ce.redirect_target,
                    ce.has_sitelinks_count,
                    ce.has_claims_count,
                    ce.deleted,
                    ce.n1,
                    ce.n2a,
                    ce.n2b,
                    ic.qid,
                    ic.inlinks_count,
                    ic.n3_inlinks,
                    ic.inlinks_last_evaluated,
                    rc.qid,
                    rc.recent_changes_last_revid,
                    rc.creator_actor_id,
                    rc.creation_time
                FROM qids q
                LEFT JOIN content_evaluation ce
                  ON ce.qid = q.qid
                LEFT JOIN inlinks_cache ic
                  ON ic.qid = q.qid
                LEFT JOIN recent_changes_cache rc
                  ON rc.qid = q.qid
                ORDER BY q.qid
                """,
                chunk,
            )
            for row in await cursor.fetchall():
                qid_num = int(row[0])
                row_data = rows.setdefault(qid_num, {})
                row_data["content"] = row[2:10] if row[1] is not None else row_data.get("content")
                row_data["inlinks"] = row[11:14] if row[10] is not None else row_data.get("inlinks")
                row_data["recent_changes_last_revid"] = None if row[15] is None else int(row[15])
                creator_actor_id = row[16]
                creation_time = row[17]
                if creator_actor_id is None or creation_time is None:
                    continue
                try:
                    creator_actor_id_num = int(creator_actor_id)
                except (TypeError, ValueError):
                    continue
                creation_time_num = _to_optional_int(creation_time)
                if creation_time_num is None:
                    continue
                qid_text = qid_lookup.get(qid_num)
                if qid_text is None:
                    continue
                creation_metadata[qid_text] = CreationMetadata(
                    qid=qid_text,
                    creator_actor_id=creator_actor_id_num,
                    creation_time=creation_time_num,
                )

    result: dict[str, EvaluationResult] = {}
    for qid_num, data in rows.items():
        qid_text = qid_lookup.get(qid_num)
        if qid_text is None:
            continue

        content = data.get("content")
        inlinks = data.get("inlinks")
        recent_changes_last_revid = data.get("recent_changes_last_revid")
        if content is None and inlinks is None and recent_changes_last_revid is None:
            continue

        content_last_revid = None
        redirect_target = None
        has_sitelinks_count = 0
        has_claims_count = 0
        is_deleted = False
        n1 = NotabilityLevel.UNKNOWN
        n2a = NotabilityLevel.UNKNOWN
        n2b = NotabilityLevel.UNKNOWN
        if content is not None:
            content_last_revid = int(content[0]) if content[0] is not None else None
            redirect_target = content[1]
            has_sitelinks_count = int(content[2]) if content[2] is not None else 0
            has_claims_count = int(content[3]) if content[3] is not None else 0
            is_deleted = bool(content[4])
            if content_last_revid is not None:
                n1 = NotabilityLevel(int(content[5]))
                n2a = NotabilityLevel(int(content[6]))
                n2b = NotabilityLevel(int(content[7]))

        n3_inlinks = NotabilityLevel(int(inlinks[1])) if inlinks is not None else NotabilityLevel.UNKNOWN
        inlinks_count = int(inlinks[0]) if inlinks is not None and inlinks[0] is not None else 0
        result[qid_text] = EvaluationResult(
            qid=qid_text,
            n1=n1,
            n2a=n2a,
            n2b=n2b,
            n3_inlinks=n3_inlinks,
            inlinks_count=inlinks_count,
            has_claims_count=has_claims_count,
            has_claims_known=content is not None,
            has_sitelinks_count=has_sitelinks_count,
            is_redirect=redirect_target is not None,
            redirect_target=redirect_target,
            is_deleted=is_deleted,
            content_last_revid=content_last_revid,
            recent_changes_last_revid=recent_changes_last_revid,
        )

    return result, creation_metadata


async def list_qids(cache: "EvaluationCache") -> list[str]:
    await cache.initialize()
    async with cache._connect() as db:
        cursor = await db.execute(
            """
            SELECT qid
            FROM content_evaluation
            ORDER BY qid
            """
        )
        rows = await cursor.fetchall()
    return [f"Q{int(row[0])}" for row in rows]


async def stats(cache: "EvaluationCache") -> dict[str, int | None | str]:
    stats_started = time.perf_counter()
    await cache.initialize()
    initialized_at = time.perf_counter()

    async with cache._connect() as db:
        connected_at = time.perf_counter()
        eval_cursor = await db.execute(
            """
            SELECT
                COUNT(*),
                MIN(content_last_revid),
                MAX(content_last_revid)
            FROM content_evaluation
            """,
        )
        eval_row = await eval_cursor.fetchone()
        evaluations_at = time.perf_counter()

        recent_changes_cursor = await db.execute(
            """
            SELECT
                MIN(recent_changes_last_revid),
                MAX(recent_changes_last_revid)
            FROM recent_changes_cache
            """,
        )
        recent_changes_row = await recent_changes_cursor.fetchone()
        recent_changes_at = time.perf_counter()

        qid_cursor = await db.execute("SELECT qid FROM content_evaluation")
        qid_rows = await qid_cursor.fetchall()

    entries = int(eval_row[0]) if eval_row and eval_row[0] is not None else 0
    oldest_content = int(eval_row[1]) if eval_row and eval_row[1] is not None else None
    newest_content = int(eval_row[2]) if eval_row and eval_row[2] is not None else None
    oldest_recent_changes = int(recent_changes_row[0]) if recent_changes_row and recent_changes_row[0] is not None else None
    newest_recent_changes = int(recent_changes_row[1]) if recent_changes_row and recent_changes_row[1] is not None else None
    qids = [f"Q{int(row[0])}" for row in qid_rows if row and row[0] is not None]
    external_usage = lookup_cache.get_external_usage(qids)
    wikisub_entries = sum(1 for entry in external_usage.values() if entry.get("wikisub"))
    return {
        "evaluations": {
            "entries": entries,
            "oldest_content_last_revid": oldest_content,
            "newest_content_last_revid": newest_content,
            "oldest_recent_changes_last_revid": oldest_recent_changes,
            "newest_recent_changes_last_revid": newest_recent_changes,
            "wikisub_entries": wikisub_entries,
        },
        "timing": {
            "total_seconds": evaluations_at - stats_started,
            "initialize_seconds": initialized_at - stats_started,
            "connect_seconds": connected_at - initialized_at,
            "evaluations_query_seconds": evaluations_at - connected_at,
            "recent_changes_query_seconds": recent_changes_at - evaluations_at,
        },
        "db_path": cache.database,
    }


async def breakdown(cache: "EvaluationCache") -> dict[str, Any]:
    await cache.initialize()
    flags = (
        ("redirect", "redirect_target"),
        ("has_sitelinks", "has_sitelinks_count"),
        ("has_claims", "has_claims_count"),
        ("deleted", "deleted"),
    )
    levels = (
        ("unknown", 2),
        ("none", 0),
        ("partial-weak", 1),
        ("partial-strong", 2),
        ("weak", 3),
        ("strong", 5),
    )

    async with cache._connect() as db:
        total_cursor = await db.execute("SELECT COUNT(*) FROM content_evaluation")
        total_row = await total_cursor.fetchone()
        total_rows = int(total_row[0]) if total_row and total_row[0] is not None else 0
        cursor = await db.execute(
            """
            SELECT
                ce.qid,
                ce.content_last_revid,
                ce.redirect_target,
                ce.has_sitelinks_count,
                ce.has_claims_count,
                ce.deleted,
                ce.n1,
                ce.n2a,
                ce.n2b,
                COALESCE(ic.inlinks_count, 0),
                COALESCE(ic.n3_inlinks, 4)
            FROM content_evaluation ce
            LEFT JOIN inlinks_cache ic
              ON ic.qid = ce.qid
            ORDER BY ce.qid ASC
            """
        )
        rows = await cursor.fetchall()

    qids = [f"Q{int(row[0])}" for row in rows if row[0] is not None]
    external_usage = lookup_cache.get_external_usage(qids)
    osm_qids = {qid for qid, entry in external_usage.items() if entry.get("osm") is not None}
    sdc_qids = {qid for qid, entry in external_usage.items() if entry.get("sdc") is not None}
    wikisub_qids = {qid for qid, entry in external_usage.items() if entry.get("wikisub")}

    def _level_counts() -> dict[str, int]:
        return {label: 0 for label, _ in levels}

    def _bump(counts: dict[str, int], level: NotabilityLevel) -> None:
        counts[str(level)] += 1

    flag_counts = {name: {"unknown": 0, "no": 0, "yes": 0} for name, _ in flags}
    detected_criteria_counts = {
        "N1": _level_counts(),
        "N2a": _level_counts(),
        "N2b": _level_counts(),
        "N3_inlinks": _level_counts(),
        "N3_osm": _level_counts(),
        "N3_wikisub": _level_counts(),
        "N3_sdc": _level_counts(),
    }
    deduced_criteria_counts = {
        "N2": _level_counts(),
        "N12": _level_counts(),
        "N3": _level_counts(),
        "N": _level_counts(),
    }

    for (
        qid_num,
        content_last_revid,
        redirect_target,
        has_sitelinks_count,
        has_claims_count,
        deleted_flag,
        n1_value,
        n2a_value,
        n2b_value,
        inlinks_count,
        n3_inlinks_value,
    ) in rows:
        qid = f"Q{int(qid_num)}"
        summary_level = {
            "N1": NotabilityLevel(int(n1_value)),
            "N2a": NotabilityLevel(int(n2a_value)),
            "N2b": NotabilityLevel(int(n2b_value)),
            "N3_inlinks": NotabilityLevel(int(n3_inlinks_value)),
        }
        external_levels = {
            "N3_osm": NotabilityLevel.WEAK if qid in osm_qids else NotabilityLevel.NONE,
            "N3_wikisub": NotabilityLevel.WEAK if qid in wikisub_qids else NotabilityLevel.NONE,
            "N3_sdc": NotabilityLevel.STRONG if qid in sdc_qids else NotabilityLevel.NONE,
        }

        for name, field_name in flags:
            if content_last_revid is None:
                flag_counts[name]["unknown"] += 1
            elif name == "redirect":
                flag_counts[name]["yes" if redirect_target is not None else "no"] += 1
            elif name == "has_sitelinks":
                flag_counts[name]["yes" if int(has_sitelinks_count or 0) > 0 else "no"] += 1
            elif name == "has_claims":
                flag_counts[name]["yes" if int(has_claims_count or 0) > 0 else "no"] += 1
            elif name == "deleted":
                flag_counts[name]["yes" if bool(deleted_flag) else "no"] += 1

        for name, level in summary_level.items():
            _bump(detected_criteria_counts[name], level)
        for name, level in external_levels.items():
            _bump(detected_criteria_counts[name], level)

        n2 = deduce_n2(summary_level["N2a"], summary_level["N2b"])
        n12 = max(summary_level["N1"], n2)
        n3 = max(summary_level["N3_inlinks"], external_levels["N3_osm"], external_levels["N3_wikisub"], external_levels["N3_sdc"])
        n = max(n12, n3)

        for name, level in (("N2", n2), ("N12", n12), ("N3", n3), ("N", n)):
            _bump(deduced_criteria_counts[name], level)

    return {
        "counts": {
            "rows": total_rows,
            "flag_counts": flag_counts,
            "detected_criteria_counts": detected_criteria_counts,
            "deduced_criteria_counts": deduced_criteria_counts,
        },
        "external_usage": {
            "osm_qids": len(osm_qids),
            "sdc_qids": len(sdc_qids),
            "wikisub_qids": len(wikisub_qids),
        },
    }


async def list_pubsub_content_candidates(
    cache: "EvaluationCache",
    limit: int | None = None,
    *,
    exclude_qids: Sequence[str | int] | None = None,
) -> list[str]:
    await cache.initialize()

    deletion_events_cte = """
        deletion_events AS (
            SELECT qid, MAX(event_timestamp) AS last_event_timestamp
            FROM content_deletion_events
            GROUP BY qid
        )
    """
    never_evaluated_expr = "(ce.qid IS NULL OR ce.content_last_revid IS NULL)"
    recent_changes_stale_expr = "(rc.recent_changes_last_revid IS NOT NULL AND ce.content_last_revid < rc.recent_changes_last_revid)"
    redirect_stale_expr = cache._redirect_target_stale_expr()
    deletion_stale_expr = (
        "(de.last_event_timestamp IS NOT NULL AND "
        "(ce.last_updated IS NULL OR de.last_event_timestamp > ce.last_updated))"
    )
    content_policy_stale_expr = cache._content_policy_stale_expr()
    not_deleted_expr = f"(ce.qid IS NULL OR COALESCE(ce.deleted, 0) = 0 OR {deletion_stale_expr})"
    redirect_target_join_clause = cache._redirect_target_join_clause()
    excluded_qids = [
        qid_num
        for qid in (exclude_qids or ())
        if (qid_num := cache._parse_qid(qid)) is not None
    ]
    excluded_clause = ""
    excluded_params: list[int] = []
    if excluded_qids:
        excluded_clause = f"AND s.qid NOT IN ({', '.join('?' for _ in excluded_qids)})"
        excluded_params = excluded_qids
    interested_query = f"""
        SELECT
            s.qid AS qid,
            COALESCE(SUM(COALESCE(s.priority, 10)), 0) AS subscriber_priority,
            CASE WHEN ce.qid IS NULL OR ce.content_last_revid IS NULL THEN 1 ELSE 0 END AS never_evaluated,
            1 AS has_interest
        FROM interest s
        LEFT JOIN content_evaluation ce
          ON ce.qid = s.qid
        LEFT JOIN recent_changes_cache rc
          ON rc.qid = ce.qid
        {redirect_target_join_clause}
        LEFT JOIN deletion_events de
          ON de.qid = ce.qid
        {cache._content_policy_join_clause()}
        WHERE s.qid != 0
          AND s.wants_content = 1
          AND {not_deleted_expr}
          {excluded_clause}
          AND ({never_evaluated_expr} OR {recent_changes_stale_expr} OR {redirect_stale_expr} OR {deletion_stale_expr} OR {content_policy_stale_expr})
        GROUP BY s.qid
    """
    query = f"""
        WITH {deletion_events_cte}, interested AS ({interested_query})
        SELECT qid
        FROM interested
        ORDER BY has_interest DESC, subscriber_priority DESC, never_evaluated DESC, qid ASC
    """

    async with cache._connect() as db:
        if limit is None:
            cursor = await db.execute(query, excluded_params)
        else:
            cursor = await db.execute(
                f"{query}\nLIMIT ?",
                [*excluded_params, limit],
            )
        rows = await cursor.fetchall()

    return [f"Q{int(row[0])}" for row in rows]


async def count_pubsub_content_candidates(cache: "EvaluationCache") -> int:
    await cache.initialize()

    deletion_events_cte = """
        deletion_events AS (
            SELECT qid, MAX(event_timestamp) AS last_event_timestamp
            FROM content_deletion_events
            GROUP BY qid
        )
    """
    never_evaluated_expr = "(ce.qid IS NULL OR ce.content_last_revid IS NULL)"
    recent_changes_stale_expr = "(rc.recent_changes_last_revid IS NOT NULL AND ce.content_last_revid < rc.recent_changes_last_revid)"
    redirect_stale_expr = cache._redirect_target_stale_expr()
    deletion_stale_expr = (
        "(de.last_event_timestamp IS NOT NULL AND "
        "(ce.last_updated IS NULL OR de.last_event_timestamp > ce.last_updated))"
    )
    content_policy_stale_expr = cache._content_policy_stale_expr()
    not_deleted_expr = f"(ce.qid IS NULL OR COALESCE(ce.deleted, 0) = 0 OR {deletion_stale_expr})"
    redirect_target_join_clause = cache._redirect_target_join_clause()
    interested_query = f"""
        SELECT s.qid AS qid
        FROM interest s
        LEFT JOIN content_evaluation ce
          ON ce.qid = s.qid
        LEFT JOIN recent_changes_cache rc
          ON rc.qid = ce.qid
        {redirect_target_join_clause}
        LEFT JOIN deletion_events de
          ON de.qid = ce.qid
        {cache._content_policy_join_clause()}
        WHERE s.qid != 0
          AND s.wants_content = 1
          AND {not_deleted_expr}
          AND ({never_evaluated_expr} OR {recent_changes_stale_expr} OR {redirect_stale_expr} OR {deletion_stale_expr} OR {content_policy_stale_expr})
        GROUP BY s.qid
    """
    query = f"""
        WITH {deletion_events_cte}, interested AS ({interested_query})
        SELECT COUNT(*)
        FROM interested
    """

    async with cache._connect() as db:
        cursor = await db.execute(query)
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


def _staleness_breakdown_clauses(cache: "EvaluationCache") -> tuple[str, str, str, str, str, str, str, str]:
    deletion_events_cte = """
        deletion_events AS (
            SELECT qid, MAX(event_timestamp) AS last_event_timestamp
            FROM content_deletion_events
            GROUP BY qid
        )
    """
    never_evaluated_expr = "(ce.qid IS NULL OR ce.content_last_revid IS NULL)"
    recent_changes_missing_expr = "(rc.recent_changes_last_revid IS NULL)"
    recent_changes_stale_expr = "(rc.recent_changes_last_revid IS NOT NULL AND ce.content_last_revid < rc.recent_changes_last_revid)"
    redirect_stale_expr = cache._redirect_target_stale_expr()
    deletion_stale_expr = (
        "(de.last_event_timestamp IS NOT NULL AND "
        "(ce.last_updated IS NULL OR de.last_event_timestamp > ce.last_updated))"
    )
    content_policy_stale_expr = cache._content_policy_stale_expr()
    not_deleted_expr = f"(ce.qid IS NULL OR COALESCE(ce.deleted, 0) = 0 OR {deletion_stale_expr})"
    return (
        deletion_events_cte,
        never_evaluated_expr,
        recent_changes_missing_expr,
        recent_changes_stale_expr,
        redirect_stale_expr,
        deletion_stale_expr,
        content_policy_stale_expr,
        not_deleted_expr,
    )


async def list_stale_content_qids(
    cache: "EvaluationCache",
    limit: int | None = None,
) -> list[str]:
    await cache.initialize()

    (
        deletion_events_cte,
        _never_evaluated_expr,
        _recent_changes_missing_expr,
        recent_changes_stale_expr,
        redirect_stale_expr,
        deletion_stale_expr,
        content_policy_stale_expr,
        _not_deleted_expr,
    ) = _staleness_breakdown_clauses(cache)
    stale_expr = """
        (
            ce.content_last_revid IS NULL
            OR (
                rc.recent_changes_last_revid IS NOT NULL
                AND ce.content_last_revid < rc.recent_changes_last_revid
            )
            OR {redirect_target_stale_expr}
            OR (
                de.last_event_timestamp IS NOT NULL
                AND (
                    ce.last_updated IS NULL
                    OR de.last_event_timestamp > ce.last_updated
                )
            )
            OR {content_policy_stale_expr}
        )
    """.format(
        content_policy_stale_expr=content_policy_stale_expr,
        redirect_target_stale_expr=redirect_stale_expr,
    )
    content_policy_join_clause = cache._content_policy_join_clause()
    redirect_target_join_clause = cache._redirect_target_join_clause()

    async with cache._connect() as db:
        if limit is None:
            cursor = await db.execute(
                """
                WITH {deletion_events_cte}
                SELECT ce.qid
                FROM content_evaluation ce
                LEFT JOIN recent_changes_cache rc
                  ON rc.qid = ce.qid
                {redirect_target_join_clause}
                LEFT JOIN deletion_events de
                  ON de.qid = ce.qid
                {content_policy_join_clause}
                WHERE {stale_expr}
                ORDER BY ce.qid DESC
                """.format(
                    deletion_events_cte=deletion_events_cte,
                    redirect_target_join_clause=redirect_target_join_clause,
                    content_policy_join_clause=content_policy_join_clause,
                    stale_expr=stale_expr,
                )
            )
        else:
            cursor = await db.execute(
                """
                WITH {deletion_events_cte}
                SELECT ce.qid
                FROM content_evaluation ce
                LEFT JOIN recent_changes_cache rc
                  ON rc.qid = ce.qid
                {redirect_target_join_clause}
                LEFT JOIN deletion_events de
                  ON de.qid = ce.qid
                {content_policy_join_clause}
                WHERE {stale_expr}
                ORDER BY ce.qid DESC
                LIMIT ?
                """.format(
                    deletion_events_cte=deletion_events_cte,
                    redirect_target_join_clause=redirect_target_join_clause,
                    content_policy_join_clause=content_policy_join_clause,
                    stale_expr=stale_expr,
                ),
                (limit,),
            )
        rows = await cursor.fetchall()

    return [f"Q{int(row[0])}" for row in rows]


async def get_content_staleness_for_qids(
    cache: "EvaluationCache",
    qids: Sequence[str | int],
) -> dict[str, bool]:
    await cache.initialize()

    qid_nums: list[int] = []
    qid_lookup: dict[int, str] = {}
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in qid_lookup:
            continue
        qid_nums.append(qid_num)
        qid_lookup[qid_num] = f"Q{qid_num}"

    if not qid_nums:
        return {}

    (
        deletion_events_cte,
        _never_evaluated_expr,
        _recent_changes_missing_expr,
        _recent_changes_stale_expr,
        redirect_stale_expr,
        _deletion_stale_expr,
        content_policy_stale_expr,
        _not_deleted_expr,
    ) = _staleness_breakdown_clauses(cache)
    stale_expr = """
        (
            ce.content_last_revid IS NULL
            OR (
                rc.recent_changes_last_revid IS NOT NULL
                AND ce.content_last_revid < rc.recent_changes_last_revid
            )
            OR {redirect_target_stale_expr}
            OR (
                de.last_event_timestamp IS NOT NULL
                AND (
                    ce.last_updated IS NULL
                    OR de.last_event_timestamp > ce.last_updated
                )
            )
            OR {content_policy_stale_expr}
        )
    """.format(
        content_policy_stale_expr=content_policy_stale_expr,
        redirect_target_stale_expr=redirect_stale_expr,
    )
    content_policy_join_clause = cache._content_policy_join_clause()
    redirect_target_join_clause = cache._redirect_target_join_clause()

    placeholders = ", ".join("?" for _ in qid_nums)
    async with cache._connect() as db:
        cursor = await db.execute(
            """
            WITH {deletion_events_cte}
            SELECT ce.qid, CASE WHEN {stale_expr} THEN 1 ELSE 0 END AS content_stale
            FROM content_evaluation ce
            LEFT JOIN recent_changes_cache rc
              ON rc.qid = ce.qid
            {redirect_target_join_clause}
            LEFT JOIN deletion_events de
              ON de.qid = ce.qid
            {content_policy_join_clause}
            WHERE ce.qid IN ({placeholders})
            """.format(
                deletion_events_cte=deletion_events_cte,
                redirect_target_join_clause=redirect_target_join_clause,
                content_policy_join_clause=content_policy_join_clause,
                stale_expr=stale_expr,
                placeholders=placeholders,
            ),
            tuple(qid_nums),
        )
        rows = await cursor.fetchall()

    results: dict[str, bool] = {qid: True for qid in qid_lookup.values()}
    for row in rows:
        qid_num = int(row[0])
        qid_text = qid_lookup.get(qid_num)
        if qid_text is None:
            continue
        results[qid_text] = bool(int(row[1])) if row[1] is not None else True
    return results


async def count_stale_content_qids(cache: "EvaluationCache") -> int:
    await cache.initialize()

    (
        deletion_events_cte,
        _never_evaluated_expr,
        _recent_changes_missing_expr,
        _recent_changes_stale_expr,
        redirect_stale_expr,
        _deletion_stale_expr,
        content_policy_stale_expr,
        _not_deleted_expr,
    ) = _staleness_breakdown_clauses(cache)
    stale_expr = """
        (
            ce.content_last_revid IS NULL
            OR (
                rc.recent_changes_last_revid IS NOT NULL
                AND ce.content_last_revid < rc.recent_changes_last_revid
            )
            OR {redirect_target_stale_expr}
            OR (
                de.last_event_timestamp IS NOT NULL
                AND (
                    ce.last_updated IS NULL
                    OR de.last_event_timestamp > ce.last_updated
                )
            )
            OR {content_policy_stale_expr}
        )
    """.format(
        content_policy_stale_expr=content_policy_stale_expr,
        redirect_target_stale_expr=redirect_stale_expr,
    )
    content_policy_join_clause = cache._content_policy_join_clause()
    redirect_target_join_clause = cache._redirect_target_join_clause()

    async with cache._connect() as db:
        cursor = await db.execute(
            """
            WITH {deletion_events_cte}
            SELECT COUNT(*)
            FROM content_evaluation ce
            LEFT JOIN recent_changes_cache rc
              ON rc.qid = ce.qid
            {redirect_target_join_clause}
            LEFT JOIN deletion_events de
              ON de.qid = ce.qid
            {content_policy_join_clause}
            WHERE {stale_expr}
            """.format(
                deletion_events_cte=deletion_events_cte,
                redirect_target_join_clause=redirect_target_join_clause,
                content_policy_join_clause=content_policy_join_clause,
                stale_expr=stale_expr,
            )
        )
        row = await cursor.fetchone()

    return int(row[0]) if row and row[0] is not None else 0


async def _list_pubsub_content_candidate_staleness_rows(
    cache: "EvaluationCache",
    *,
    qid_nums: Sequence[int] | None = None,
) -> list[tuple[int, str]]:
    await cache.initialize()

    (
        deletion_events_cte,
        never_evaluated_expr,
        recent_changes_missing_expr,
        recent_changes_stale_expr,
        redirect_stale_expr,
        deletion_stale_expr,
        content_policy_stale_expr,
        not_deleted_expr,
    ) = _staleness_breakdown_clauses(cache)
    redirect_target_join_clause = cache._redirect_target_join_clause()
    qid_filter_clause = ""
    params: list[int] = []
    if qid_nums:
        qid_filter_clause = f" AND s.qid IN ({', '.join('?' for _ in qid_nums)})"
        params.extend(int(qid_num) for qid_num in qid_nums)
    interested_query = f"""
        SELECT
            s.qid AS qid,
            CASE
                WHEN {never_evaluated_expr} THEN 'never_evaluated'
                WHEN {deletion_stale_expr} THEN 'deletion_events'
                WHEN {content_policy_stale_expr} THEN 'content_policy'
                WHEN {redirect_stale_expr} THEN 'redirect_target'
                WHEN {recent_changes_stale_expr} THEN 'recent_changes'
                WHEN {recent_changes_missing_expr} THEN 'recent_changes_missing'
                ELSE NULL
            END AS reason
        FROM interest s
        LEFT JOIN content_evaluation ce
          ON ce.qid = s.qid
        LEFT JOIN recent_changes_cache rc
          ON rc.qid = ce.qid
        {redirect_target_join_clause}
        LEFT JOIN deletion_events de
          ON de.qid = ce.qid
        {cache._content_policy_join_clause()}
        WHERE s.qid != 0
          AND s.wants_content = 1
          {qid_filter_clause}
          AND {not_deleted_expr}
          AND (
            {never_evaluated_expr}
            OR {recent_changes_stale_expr}
            OR {redirect_stale_expr}
            OR {deletion_stale_expr}
            OR {content_policy_stale_expr}
          )
        GROUP BY s.qid
    """
    query = f"""
        WITH {deletion_events_cte}, interested AS ({interested_query})
        SELECT qid, reason
        FROM interested
    """

    async with cache._connect() as db:
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()

    return [
        (int(row[0]), str(row[1]))
        for row in rows
        if row[0] is not None and row[1] is not None
    ]


async def count_pubsub_content_candidates_by_staleness(cache: "EvaluationCache") -> dict[str, int]:
    return await _count_pubsub_content_candidates_by_staleness(cache)


async def _count_pubsub_content_candidates_by_staleness(
    cache: "EvaluationCache",
    *,
    qid_nums: Sequence[int] | None = None,
) -> dict[str, int]:
    rows = await _list_pubsub_content_candidate_staleness_rows(cache, qid_nums=qid_nums)

    counts = {
        "total": 0,
        "never_evaluated": 0,
        "recent_changes_missing": 0,
        "recent_changes": 0,
        "redirect_target": 0,
        "deletion_events": 0,
        "content_policy": 0,
    }
    for _qid_num, reason in rows:
        counts["total"] += 1
        if reason in counts:
            counts[reason] += 1
    return counts


async def count_pubsub_content_candidate_staleness_for_qids(
    cache: "EvaluationCache",
    qids: Sequence[str | int],
) -> dict[str, int]:
    qid_nums: list[int] = []
    seen: set[int] = set()
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in seen:
            continue
        seen.add(qid_num)
        qid_nums.append(qid_num)
    if not qid_nums:
        return {
            "total": 0,
            "never_evaluated": 0,
            "recent_changes_missing": 0,
            "recent_changes": 0,
            "redirect_target": 0,
            "deletion_events": 0,
            "content_policy": 0,
        }
    return await _count_pubsub_content_candidates_by_staleness(cache, qid_nums=qid_nums)


async def list_pubsub_content_candidate_reasons(
    cache: "EvaluationCache",
    qids: Sequence[str | int],
) -> dict[str, str]:
    qid_nums: list[int] = []
    seen: set[int] = set()
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in seen:
            continue
        seen.add(qid_num)
        qid_nums.append(qid_num)
    if not qid_nums:
        return {}

    rows = await _list_pubsub_content_candidate_staleness_rows(cache, qid_nums=qid_nums)
    return {f"Q{qid_num}": reason for qid_num, reason in rows}

from __future__ import annotations

import asyncio
import os
import time
from datetime import UTC, datetime
from typing import Any

import server.app as app_module
from server.render_helpers import _badge_hovercard_html_from_report, _badge_tooltip_from_report
from wd_notability.inlinks.pipeline import _get_inlinks_n12_many
from wd_notability.models import (
    EvaluationResult,
    NotabilityCriterion,
    NotabilityLevel,
    external_usage_level,
)
from wd_notability.web.creations import lookup_creator_names as web_lookup_creator_names


SOURCE_CONTEXT_TIMEOUT_SECONDS = float(
    os.getenv("WD_NOTABILITY_SIGNAL_DEBUGGER_SOURCE_TIMEOUT_SECONDS", "15")
)

def _report_payload(result) -> dict:
    return app_module.web_build_signal_debug_payload(result)


def _epoch_seconds(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(dt.timestamp())
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _item_link_html(qid: str | None) -> str:
    if not qid:
        return ""
    escaped_qid = app_module.escape(qid)
    href = f"https://www.wikidata.org/wiki/{escaped_qid}"
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">{escaped_qid}</a>'


def _render_report_html(report: dict | None) -> str:
    return app_module.web_render_signal_debug_html(report)


def _utc_isoformat(value: object | None) -> str | None:
    if value is None:
        return None
    timestamp = _epoch_seconds(value)
    if timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _cache_snapshot_payload(
    result: EvaluationResult,
    content_last_revid: int | None,
    recent_changes_last_revid: int | None,
    *,
    content_stale: object | None = None,
    creator: object | None = None,
    creation_time: object | None = None,
    last_updated: object | None = None,
    inlinks_last_evaluated: object | None = None,
) -> dict[str, object]:
    return {
        "qid": result.qid,
        "levels": result.levels_str,
        "errors": result.errors,
        "has_claims_count": result.has_claims_count,
        "has_sitelinks_count": result.has_sitelinks_count,
        "is_redirect": result.is_redirect,
        "redirect_target": result.redirect_target,
        "is_deleted": result.is_deleted,
        "inlinks_count": result.inlinks_count,
        "content_stale": content_stale,
        "creator": None if creator is None else str(creator),
        "creation_time": _epoch_seconds(creation_time),
        "creation_time_iso": _utc_isoformat(creation_time),
        "last_updated": _epoch_seconds(last_updated),
        "last_updated_iso": _utc_isoformat(last_updated),
        "inlinks_last_evaluated": _epoch_seconds(inlinks_last_evaluated),
        "inlinks_last_evaluated_iso": _utc_isoformat(inlinks_last_evaluated),
        "content_last_revid": content_last_revid,
        "recent_changes_last_revid": recent_changes_last_revid,
        "badge_tooltip": _badge_tooltip_from_report({
            "levels": result.levels_str,
            "is_redirect": result.is_redirect,
            "redirect_target": result.redirect_target,
            "is_deleted": result.is_deleted,
            "has_sitelinks_count": result.has_sitelinks_count,
            "has_claims_count": result.has_claims_count,
            "inlinks_count": result.inlinks_count,
            "content_last_revid": content_last_revid,
            "recent_changes_last_revid": recent_changes_last_revid,
            "content_stale": content_stale,
            "creator": None if creator is None else str(creator),
            "creation_time_iso": _utc_isoformat(creation_time),
            "last_updated_iso": _utc_isoformat(last_updated),
            "inlinks_last_evaluated_iso": _utc_isoformat(inlinks_last_evaluated),
        }),
        "badge_hovercard": _badge_hovercard_html_from_report({
            "levels": result.levels_str,
            "is_redirect": result.is_redirect,
            "redirect_target": result.redirect_target,
            "is_deleted": result.is_deleted,
            "has_sitelinks_count": result.has_sitelinks_count,
            "has_claims_count": result.has_claims_count,
            "inlinks_count": result.inlinks_count,
            "content_last_revid": content_last_revid,
            "recent_changes_last_revid": recent_changes_last_revid,
            "content_stale": content_stale,
            "creator": None if creator is None else str(creator),
            "creation_time_iso": _utc_isoformat(creation_time),
            "last_updated_iso": _utc_isoformat(last_updated),
            "inlinks_last_evaluated_iso": _utc_isoformat(inlinks_last_evaluated),
        }),
    }


def _compare_report_to_cache(
    live_report: dict[str, Any],
    cached_snapshot: dict[str, Any] | None,
    *,
    comparison_levels: tuple[str, ...] | list[str] | None = None,
) -> dict[str, object]:
    if not isinstance(cached_snapshot, dict):
        return {"status": "missing", "items": [{"field": "cache", "cache": "", "live": "missing"}]}

    discrepancies: list[dict[str, object]] = []
    live_levels = live_report.get("levels", {})
    cached_levels = cached_snapshot.get("levels", {})
    level_keys: tuple[str, ...]
    if isinstance(comparison_levels, (list, tuple)) and comparison_levels:
        level_keys = tuple(str(key) for key in comparison_levels)
    else:
        level_keys = tuple(sorted(set(live_levels) | set(cached_levels))) if isinstance(
            live_levels, dict) and isinstance(cached_levels, dict) else ()
    if isinstance(live_levels, dict) and isinstance(cached_levels, dict):
        for key in level_keys:
            live_value = live_levels.get(key)
            cache_value = cached_levels.get(key)
            if str(live_value) != str(cache_value):
                discrepancies.append(
                    {
                        "field": f"levels.{key}",
                        "cache": cache_value,
                        "live": live_value,
                    }
                )

    for field in ("has_claims_count", "has_sitelinks_count", "inlinks_count", "is_redirect", "is_deleted", "content_last_revid"):
        live_value = live_report.get(field)
        cache_value = cached_snapshot.get(field)
        if live_value != cache_value:
            discrepancies.append(
                {
                    "field": field,
                    "cache": cache_value,
                    "live": live_value,
                }
            )

    return {
        "status": "ok",
        "count": len(discrepancies),
        "items": discrepancies,
    }


async def _get_source_contexts_with_timeout(source, qid_list: list[str]) -> dict[str, object]:
    if not qid_list:
        return {}

    try:
        return await asyncio.wait_for(
            source.get_contexts(qid_list),
            timeout=SOURCE_CONTEXT_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        timeout_error = TimeoutError(
            f"Source {getattr(source, 'name', 'unknown')} timed out after "
            f"{SOURCE_CONTEXT_TIMEOUT_SECONDS:.0f}s"
        )
        return {qid: timeout_error for qid in qid_list}
    except Exception as exc:  # noqa: BLE001
        return {qid: exc for qid in qid_list}


async def _evaluate_live_reports(qids: list[str]) -> dict[str, EvaluationResult]:
    qid_list = [qid for qid in qids if isinstance(qid, str) and app_module._is_valid_qid(qid)]
    if not qid_list:
        return {}

    part_buckets: dict[str, list[EvaluationResult]] = {
        qid: [] for qid in qid_list}
    # Debugger evaluations are intentionally ephemeral: they should inspect live
    # detector output, but never write the result back into the cache.
    async with app_module.foreground_evaluation():
        contexts_by_source = await asyncio.gather(
            *(_get_source_contexts_with_timeout(source, qid_list) for source in app_module.EVALUATION_SOURCES)
        )
        for source, contexts in zip(app_module.EVALUATION_SOURCES, contexts_by_source, strict=True):
            for qid in qid_list:
                context = contexts.get(qid)
                if isinstance(context, app_module.EntityDeletedError):
                    part = EvaluationResult(qid=qid, is_deleted=True)
                elif isinstance(context, Exception):
                    part = EvaluationResult(qid=qid)
                    for detector in source.detectors:
                        part.add_error(detector, context)
                elif qid not in contexts:
                    part = EvaluationResult(qid=qid)
                    for detector in source.detectors:
                        part.add_error(detector, RuntimeError(
                            f"Source {source.name} did not return context for {qid}"))
                else:
                    part = await source.run_context(qid, context)
                part_buckets[qid].append(part)

    return {qid: EvaluationResult.combine(qid, parts) for qid, parts in part_buckets.items()}


async def _fetch_cached_snapshot(qid: str) -> dict[str, object] | None:
    await app_module.CACHE.initialize()
    qid_num = app_module.CACHE._parse_qid(qid)
    async with app_module.CACHE._connect() as db:
        cursor = await db.execute(
            """
            SELECT
                ce.qid,
                ce.last_updated,
                ce.content_last_revid,
                rc.recent_changes_last_revid,
                ce.redirect_target,
                ce.has_sitelinks_count,
                ce.has_claims_count,
                ce.deleted,
                ce.n1,
                ce.n2a,
                ce.n2b,
                COALESCE(ic.inlinks_count, 0) AS inlinks_count,
                ic.inlinks_last_evaluated,
                COALESCE(ic.n3_inlinks, 4) AS n3_inlinks,
                CASE
                    WHEN ce.content_last_revid IS NULL THEN 1
                    WHEN rc.recent_changes_last_revid IS NOT NULL
                      AND ce.content_last_revid < rc.recent_changes_last_revid THEN 1
                    WHEN ce.redirect_target IS NOT NULL
                      AND (
                        ce.last_updated IS NULL
                        OR (
                            target_ce.last_updated IS NOT NULL
                            AND ce.last_updated < target_ce.last_updated
                        )
                      ) THEN 1
                    WHEN de.last_event_timestamp IS NOT NULL
                      AND (
                        ce.last_updated IS NULL
                        OR de.last_event_timestamp > ce.last_updated
                      ) THEN 1
                    WHEN policy.value IS NOT NULL
                      AND (
                        ce.last_updated IS NULL
                        OR ce.last_updated < (CAST(policy.value AS UNSIGNED) * 1000000)
                      ) THEN 1
                    ELSE 0
                END AS content_stale
            FROM content_evaluation ce
            LEFT JOIN recent_changes_cache rc
              ON rc.qid = ce.qid
            LEFT JOIN inlinks_cache ic
              ON ic.qid = ce.qid
            LEFT JOIN content_evaluation target_ce
              ON target_ce.qid = ce.redirect_target
            LEFT JOIN (
                SELECT qid, MAX(event_timestamp) AS last_event_timestamp
                FROM content_deletion_events
                GROUP BY qid
            ) de
              ON de.qid = ce.qid
            LEFT JOIN lookup_state policy
              ON policy.`key` = 'content_policy_updated_at'
            WHERE ce.qid = ?
            """,
            (qid_num,),
        )
        row = await cursor.fetchone()

    if row is None:
        cached_result = EvaluationResult(qid=qid)
        cached_result.n1 = NotabilityLevel.UNKNOWN
        cached_result.n2a = NotabilityLevel.UNKNOWN
        cached_result.n2b = NotabilityLevel.UNKNOWN
        cached_result.n3_inlinks = NotabilityLevel.UNKNOWN
        await _enrich_cached_result(cached_result)
        if cached_result.levels_str == {key: "unknown" for key in cached_result.levels_str}:
            return None
        return _cache_snapshot_payload(
            cached_result,
            None,
            None,
            content_stale=True,
        )

    cached_result = _evaluation_result_from_cache_row(qid, row)
    await _enrich_cached_result(cached_result)
    cached_timestamps = await _fetch_cached_snapshot_timestamps([qid])
    snapshot_timestamps = cached_timestamps.get(qid, {})
    return _cache_snapshot_payload(
        cached_result,
        _epoch_seconds(row[2]),
        _epoch_seconds(row[3]),
        content_stale=bool(int(row[14])) if len(row) > 14 and row[14] is not None else True,
        creator=snapshot_timestamps.get("creator"),
        creation_time=snapshot_timestamps.get("creation_time"),
        last_updated=snapshot_timestamps.get("last_updated"),
        inlinks_last_evaluated=snapshot_timestamps.get("inlinks_last_evaluated"),
    )


async def _fetch_cached_snapshot_timestamps(qids: list[str]) -> dict[str, dict[str, object | None]]:
    if not qids:
        return {}

    await app_module.CACHE.initialize()
    qid_nums: list[int] = []
    qid_lookup: dict[int, str] = {}
    for qid in qids:
        if not isinstance(qid, str) or not app_module._is_valid_qid(qid):
            continue
        qid_num = app_module.CACHE._parse_qid(qid)
        if qid_num in qid_lookup:
            continue
        qid_nums.append(qid_num)
        qid_lookup[qid_num] = qid

    if not qid_nums:
        return {}

    rows: dict[str, dict[str, object | None]] = {}
    creator_actor_ids: dict[str, int] = {}
    async with app_module.CACHE._connect() as db:
        for start in range(0, len(qid_nums), 500):
            chunk = qid_nums[start:start + 500]
            placeholders = ",".join("?" for _ in chunk)
            cursor = await db.execute(
                f"""
                SELECT
                    ce.qid,
                    ce.last_updated,
                    rc.creation_time,
                    rc.creator_actor_id,
                    ic.inlinks_last_evaluated
                FROM content_evaluation ce
                LEFT JOIN recent_changes_cache rc
                  ON rc.qid = ce.qid
                LEFT JOIN inlinks_cache ic
                  ON ic.qid = ce.qid
                WHERE ce.qid IN ({placeholders})
                """,
                chunk,
            )
            for row in await cursor.fetchall():
                qid_num = int(row[0])
                qid_text = qid_lookup.get(qid_num)
                if qid_text is None:
                    continue
                rows[qid_text] = {
                    "last_updated": _epoch_seconds(row[1]),
                    "creation_time": _epoch_seconds(row[2]),
                    "inlinks_last_evaluated": _epoch_seconds(row[4]),
                }
                creator_actor_id = row[3]
                if creator_actor_id is not None:
                    try:
                        creator_actor_ids[qid_text] = int(creator_actor_id)
                    except (TypeError, ValueError):
                        continue

    if creator_actor_ids:
        creator_names = await web_lookup_creator_names(list(dict.fromkeys(creator_actor_ids.values())))
        for qid_text, creator_actor_id in creator_actor_ids.items():
            rows.setdefault(qid_text, {})["creator"] = creator_names.get(creator_actor_id, "Unknown creator")
    for row in rows.values():
        row.setdefault("creator", None)
    return rows


async def _external_usage_sets(qids: list[str]) -> dict[str, set[str]]:
    if not qids:
        return {"osm": set(), "sdc": set(), "wikisub": set()}

    external_usage = await asyncio.to_thread(app_module.lookup_cache.get_external_usage, qids)
    return {
        "osm": {qid for qid, usage in external_usage.items() if usage.get("osm") is not None},
        "sdc": {qid for qid, usage in external_usage.items() if usage.get("sdc") is not None},
        "wikisub": {qid for qid, usage in external_usage.items() if usage.get("wikisub")},
    }


def _apply_external_usage(result: EvaluationResult, external: dict[str, set[str]]) -> None:
    qid = result.qid
    result.set(
        NotabilityCriterion.N3_OSM,
        external_usage_level(NotabilityCriterion.N3_OSM) if qid in external["osm"] else NotabilityLevel.NONE,
    )
    result.set(
        NotabilityCriterion.N3_SDC,
        external_usage_level(NotabilityCriterion.N3_SDC) if qid in external["sdc"] else NotabilityLevel.NONE,
    )
    result.set(
        NotabilityCriterion.N3_WIKISUB,
        external_usage_level(NotabilityCriterion.N3_WIKISUB) if qid in external["wikisub"] else NotabilityLevel.NONE,
    )


async def _enrich_cached_result(result: EvaluationResult) -> None:
    external = await _external_usage_sets([result.qid])
    _apply_external_usage(result, external)


async def _enrich_cached_results(
    cached_rows: dict[str, EvaluationResult]
) -> dict[str, EvaluationResult]:
    if not cached_rows:
        return {}

    qids = list(cached_rows)
    external = await _external_usage_sets(qids)

    results: dict[str, EvaluationResult] = {}
    for qid, result in cached_rows.items():
        _apply_external_usage(result, external)
        results[qid] = result
    return results


def _evaluation_result_from_cache_row(
    qid: str,
    row: tuple[object, ...],
) -> EvaluationResult:
    content_last_revid = None if row[2] is None else int(row[2])
    n3_inlinks = NotabilityLevel.UNKNOWN if row[13] is None else NotabilityLevel(int(row[13]))
    return EvaluationResult(
        qid=qid,
        n1=NotabilityLevel.UNKNOWN if content_last_revid is None else NotabilityLevel(int(row[8])),
        n2a=NotabilityLevel.UNKNOWN if content_last_revid is None else NotabilityLevel(int(row[9])),
        n2b=NotabilityLevel.UNKNOWN if content_last_revid is None else NotabilityLevel(int(row[10])),
        n3_inlinks=n3_inlinks,
        inlinks_count=int(row[11] or 0),
        has_claims_count=int(row[6] or 0),
        has_claims_known=True,
        has_sitelinks_count=int(row[5] or 0),
        is_redirect=row[4] is not None,
        is_deleted=bool(row[7]),
        content_last_revid=content_last_revid,
        recent_changes_last_revid=None if row[3] is None else int(row[3]),
    )


async def _fetch_interest_report(qid: str) -> dict[str, object] | None:
    await app_module.CACHE.initialize()
    async with app_module.CACHE._connect() as db:
        cursor = await db.execute(
            """
            SELECT
                worker_id,
                COUNT(*) AS session_rows,
                SUM(COALESCE(priority, 0)) AS total_priority,
                SUM(CASE WHEN wants_creation = 1 THEN 1 ELSE 0 END) AS wants_creation_rows,
                SUM(CASE WHEN wants_content = 1 THEN 1 ELSE 0 END) AS wants_content_rows,
                SUM(CASE WHEN wants_inlinks = 1 THEN 1 ELSE 0 END) AS wants_inlinks_rows,
                MAX(CASE WHEN wants_creation = 1 THEN 1 ELSE 0 END) AS wants_creation,
                MAX(CASE WHEN wants_content = 1 THEN 1 ELSE 0 END) AS wants_content,
                MAX(CASE WHEN wants_inlinks = 1 THEN 1 ELSE 0 END) AS wants_inlinks
            FROM interest
            WHERE qid = ?
              AND qid != 0
            GROUP BY worker_id
            ORDER BY worker_id ASC
            """,
            (app_module.CACHE._parse_qid(qid),),
        )
        rows = await cursor.fetchall()

    if not rows:
        return None

    workers = []
    session_rows = 0
    total_priority = 0
    for row in rows:
        worker_id = str(row[0])
        row_session_rows = int(row[1]) if row[1] is not None else 0
        row_priority = int(row[2]) if row[2] is not None else 0
        session_rows += row_session_rows
        total_priority += row_priority
        workers.append(
            {
                "worker_id": worker_id,
                "session_rows": row_session_rows,
                "lease_rows": row_session_rows,
                "total_priority": row_priority,
                "wants_creation_rows": int(row[3]) if row[3] is not None else 0,
                "wants_content_rows": int(row[4]) if row[4] is not None else 0,
                "wants_inlinks_rows": int(row[5]) if row[5] is not None else 0,
                "wants_creation": bool(row[6]),
                "wants_content": bool(row[7]),
                "wants_inlinks": bool(row[8]),
            }
        )

    return {
        "session_rows": session_rows,
        "lease_rows": session_rows,
        "owner_count": len(workers),
        "total_priority": total_priority,
        "workers": workers,
    }


async def _fetch_queue_report(qid: str, interest: dict[str, object] | None = None) -> dict[str, object]:
    if interest is None:
        interest = await _fetch_interest_report(qid)
    normalized_qid = qid.strip().upper()
    active_batch_size = max(
        1, int(getattr(app_module.inlinks_worker, "INLINKS_BATCH_SIZE_CURRENT", 100)))
    cache_only_batch_size = max(
        1,
        int(getattr(app_module.inlinks_worker,
            "INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE", active_batch_size)),
    )
    low_priority_max_in_flight = max(
        1,
        int(getattr(app_module.inlinks_worker, "INLINKS_LOW_PRIORITY_MAX_IN_FLIGHT", 10)),
    )
    active_targets, cache_only_candidates, refresh_candidates = await asyncio.gather(
        app_module.CACHE.list_pubsub_inlinks_targets(),
        app_module.CACHE.list_unknown_inlinks_qids(),
        app_module.CACHE.list_known_inlinks_refresh_candidates(),
    )

    def _path_report(
        *,
        name: str,
        rule: str,
        items: list[str],
        active: bool,
        batch_size: int,
        max_in_flight: int | None = None,
        requires_idle_worker: bool = False,
    ) -> dict[str, object]:
        present = normalized_qid in items
        position = items.index(normalized_qid) + 1 if present else None
        ahead = None if position is None else max(0, position - 1)
        if position is None:
            estimate = "not queued"
        elif ahead == 0:
            estimate = "next batch"
        else:
            estimate = f"queued behind {ahead} item(s)"
        if active:
            status = "active"
        elif present and requires_idle_worker:
            status = "waiting for idle worker"
        elif present:
            status = "eligible"
        else:
            status = "not queued"
        return {
            "name": name,
            "rule": rule,
            "active": active,
            "present": present,
            "status": status,
            "position": position,
            "ahead": ahead,
            "batch_size": batch_size,
            "max_in_flight": max_in_flight,
            "estimate": estimate,
        }

    active_path = normalized_qid in active_targets
    cache_only_path = normalized_qid in cache_only_candidates and not active_path
    refresh_path = normalized_qid in refresh_candidates and not active_path and not cache_only_path

    return {
        "paths": [
            _path_report(
                name="Subscribed targets",
                rule="active pubsub interest",
                items=active_targets,
                active=active_path,
                batch_size=active_batch_size,
            ),
            _path_report(
                name="Cache-only fallback",
                rule="no active interest and no active state",
                items=cache_only_candidates,
                active=cache_only_path,
                batch_size=cache_only_batch_size,
                requires_idle_worker=True,
            ),
            _path_report(
                name="Low-priority refresh",
                rule="stale known items with no active interest",
                items=refresh_candidates,
                active=refresh_path,
                batch_size=active_batch_size,
                max_in_flight=low_priority_max_in_flight,
            ),
        ],
        "active_targets": len(active_targets),
        "cache_only_candidates": len(cache_only_candidates),
        "refresh_candidates": len(refresh_candidates),
        "interest": interest,
    }


async def _build_inlinks_scan_report(qid: str) -> dict[str, object] | None:
    contexts = await _get_source_contexts_with_timeout(app_module.INLINKS_SOURCE, [qid])
    context = contexts.get(qid)
    if not isinstance(context, dict):
        return {
            "visible_inlinks": [],
            "truncated": False,
            "reports": [],
            "error": str(context) if isinstance(context, Exception) else "Inlinks scan unavailable",
        }

    raw_inlinks = context.get("inlinks", [])
    if not isinstance(raw_inlinks, list):
        raw_inlinks = []

    visible_inlinks = [inlink for inlink in raw_inlinks if isinstance(
        inlink, str) and app_module._is_valid_qid(inlink)]
    if not visible_inlinks:
        return {
            "visible_inlinks": [],
            "truncated": bool(context.get("truncated", False)),
            "reports": [],
        }

    n12_levels = await _get_inlinks_n12_many(visible_inlinks)
    reports: list[dict[str, object]] = []
    for inlink_qid in visible_inlinks:
        level = n12_levels.get(inlink_qid, NotabilityLevel.UNKNOWN)
        level_text = level.value_str if isinstance(level, NotabilityLevel) else str(level)
        signal = {
            "criterion": "N3_inlinks",
            "level": level_text,
            "detector": "inlinks",
            "key": "inlinks" if level_text != "unknown" else "inlinks_unknown",
            "properties": {
                "qid": inlink_qid,
                "n12": level_text,
            },
        }
        cached_snapshot = {
            "levels": {"N12": level_text},
        }
        report = {
            "qid": inlink_qid,
            "levels": {"N12": level_text},
            "errors": {},
            "has_claims_count": 0,
            "has_sitelinks_count": 0,
            "inlinks_count": 0,
            "is_redirect": False,
            "redirect_target": None,
            "is_deleted": False,
            "report_variant": "inlinks",
            "comparison_levels": ("N12",),
            "signals": [signal],
            "signals_by_detected_criterion": {"N3_inlinks": [signal]},
            "source_contexts": {},
            "source_urls": [],
            "cached_snapshot": cached_snapshot,
            "content_stale": None,
        }
        report["html"] = _render_report_html(report)
        reports.append(report)

    return {
        "visible_inlinks": visible_inlinks,
        "truncated": bool(context.get("truncated", False)),
        "reports": reports,
    }


async def _evaluate_or_404(qid: str) -> dict:
    if not app_module._is_valid_qid(qid):
        raise HTTPException(status_code=400, detail="qid must look like Q42")
    try:
        live_reports = await _evaluate_live_reports([qid])
        live_result = live_reports[qid]
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    payload = _report_payload(live_result)
    cached_snapshot, interest, inlinks_scan = await asyncio.gather(
        _fetch_cached_snapshot(qid),
        _fetch_interest_report(qid),
        _build_inlinks_scan_report(qid),
    )
    payload["cached_snapshot"] = cached_snapshot
    payload["content_stale"] = (
        cached_snapshot["content_stale"]
        if isinstance(cached_snapshot, dict)
        else None
    )
    payload["interest"] = interest
    payload["inlinks_scan"] = inlinks_scan
    payload["html"] = _render_report_html(payload)
    return payload


async def _cached_or_404(qid: str) -> dict:
    if not app_module._is_valid_qid(qid):
        raise HTTPException(status_code=400, detail="qid must look like Q42")

    cached_snapshot = await _fetch_cached_snapshot(qid)
    if cached_snapshot is None:
        raise HTTPException(
            status_code=404, detail="No cached result for this QID")
    return cached_snapshot

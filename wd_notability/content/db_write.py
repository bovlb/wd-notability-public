from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from wd_notability.creations import _normalize_text
from wd_notability.models import NotabilityLevel

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


def _to_utc_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, float):
        return datetime.fromtimestamp(value, tz=UTC)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(int(text), tz=UTC)
        except (OverflowError, ValueError):
            return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _summary_update_timestamp_sql(cache: "EvaluationCache") -> str:
    return cache._summary_update_timestamp_sql()


async def upsert_content_many(
    cache: "EvaluationCache",
    items: list[object],
) -> list[tuple[str, int]]:
    await cache.initialize()

    if not items:
        return []

    normalized: list[tuple[int, int | None, int | None, int, int, int, int, int, int, int | None]] = []
    seen: set[int] = set()
    for item in items:
        qid = getattr(item, "qid")
        qid_num = cache._parse_qid(qid)
        if qid_num in seen:
            continue
        seen.add(qid_num)
        content_last_revid = getattr(item, "content_last_revid", None)
        redirect_target = getattr(item, "redirect_target", None)
        has_sitelinks_count, has_claims_count, deleted_flag, n1_value, n2a_value, n2b_value = cache._content_counts_from_item(item)
        recent_changes_last_revid = getattr(item, "recent_changes_last_revid", None)
        content_last_revid_num = None if content_last_revid is None else cache._as_uint32(content_last_revid, "content_last_revid")
        redirect_target_num = cache._optional_uint32(redirect_target, "redirect_target")
        normalized.append(
            (
                qid_num,
                content_last_revid_num,
                redirect_target_num,
                has_sitelinks_count,
                has_claims_count,
                deleted_flag,
                n1_value,
                n2a_value,
                n2b_value,
                None if recent_changes_last_revid is None else cache._as_uint32(recent_changes_last_revid, "recent_changes_last_revid"),
            )
        )

    if not normalized:
        return []

    started = time.perf_counter()
    changed_rows: list[tuple[str, int]] = []
    timestamp_sql = _summary_update_timestamp_sql(cache)

    async with cache._write_guard():
        async with cache._connect() as db:
            for chunk in cache._chunked(normalized):
                await db.execute("BEGIN IMMEDIATE")
                values_sql = ", ".join(
                    f"(%s, {timestamp_sql}, %s, %s, %s, %s, %s, %s, %s, %s)" for _ in chunk
                ) if cache._backend_name == "mariadb" else ", ".join(
                    f"(?, {timestamp_sql}, ?, ?, ?, ?, ?, ?, ?, ?)" for _ in chunk
                )
                params: list[int | None] = []
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
                    _recent_changes_last_revid,
                ) in chunk:
                    params.extend([
                        qid_num,
                        content_last_revid,
                        redirect_target,
                        has_sitelinks_count,
                        has_claims_count,
                        deleted_flag,
                        n1_value,
                        n2a_value,
                        n2b_value,
                    ])
                cursor = await db.execute(
                    f"""
                    INSERT INTO content_evaluation (
                        qid,
                        last_updated,
                        content_last_revid,
                        redirect_target,
                        has_sitelinks_count,
                        has_claims_count,
                        deleted,
                        n1,
                        n2a,
                        n2b
                    )
                    VALUES {values_sql}
                    ON DUPLICATE KEY UPDATE
                        last_updated = VALUES(last_updated),
                        content_last_revid = VALUES(content_last_revid),
                        redirect_target = VALUES(redirect_target),
                        has_sitelinks_count = VALUES(has_sitelinks_count),
                        has_claims_count = VALUES(has_claims_count),
                        deleted = VALUES(deleted),
                        n1 = VALUES(n1),
                        n2a = VALUES(n2a),
                        n2b = VALUES(n2b)
                    RETURNING qid
                    """ if cache._backend_name == "mariadb" else f"""
                    INSERT INTO content_evaluation (
                        qid,
                        last_updated,
                        content_last_revid,
                        redirect_target,
                        has_sitelinks_count,
                        has_claims_count,
                        deleted,
                        n1,
                        n2a,
                        n2b
                    )
                    VALUES {values_sql}
                    ON CONFLICT(qid) DO UPDATE SET
                        last_updated = excluded.last_updated,
                        content_last_revid = excluded.content_last_revid,
                        redirect_target = excluded.redirect_target,
                        has_sitelinks_count = excluded.has_sitelinks_count,
                        has_claims_count = excluded.has_claims_count,
                        deleted = excluded.deleted,
                        n1 = excluded.n1,
                        n2a = excluded.n2a,
                        n2b = excluded.n2b
                    RETURNING qid
                    """,
                    params,
                )
                rows = await cursor.fetchall()
                changed_rows.extend((f"Q{int(row[0])}", 1) for row in rows)
                recent_changes_rows = [
                    (qid_num, recent_changes_last_revid)
                    for qid_num, _content_last_revid, _redirect_target, _has_sitelinks_count, _has_claims_count, _deleted_flag, _n1_value, _n2a_value, _n2b_value, recent_changes_last_revid in chunk
                    if recent_changes_last_revid is not None
                ]
                if recent_changes_rows:
                    recent_values_sql = ", ".join("(%s, %s)" for _ in recent_changes_rows) if cache._backend_name == "mariadb" else ", ".join("(?, ?)" for _ in recent_changes_rows)
                    recent_changes_params: list[int] = []
                    for qid_num, recent_changes_last_revid in recent_changes_rows:
                        recent_changes_params.extend([qid_num, recent_changes_last_revid])
                    await db.execute(
                        f"""
                        INSERT INTO recent_changes_cache (
                            qid, recent_changes_last_revid
                        )
                        VALUES {recent_values_sql}
                        ON DUPLICATE KEY UPDATE
                            recent_changes_last_revid = CASE
                                WHEN recent_changes_cache.recent_changes_last_revid IS NULL
                                  OR recent_changes_cache.recent_changes_last_revid < VALUES(recent_changes_last_revid)
                                THEN VALUES(recent_changes_last_revid)
                                ELSE recent_changes_cache.recent_changes_last_revid
                            END
                        """ if cache._backend_name == "mariadb" else f"""
                        INSERT INTO recent_changes_cache (
                            qid, recent_changes_last_revid
                        )
                        VALUES {recent_values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            recent_changes_last_revid = CASE
                                WHEN recent_changes_cache.recent_changes_last_revid IS NULL
                                  OR recent_changes_cache.recent_changes_last_revid < excluded.recent_changes_last_revid
                                THEN excluded.recent_changes_last_revid
                                ELSE recent_changes_cache.recent_changes_last_revid
                            END
                        """,
                        recent_changes_params,
                    )
                await db.commit()

    cache._warn_slow_write("upsert_content_many", started, row_count=len(normalized))
    return changed_rows


async def upsert_content_deletion_events(
    cache: "EvaluationCache",
    events: Sequence[tuple[str | int, int, str, int]],
) -> int:
    await cache.initialize()

    normalized: list[tuple[int, int, str, datetime]] = []
    seen_log_ids: set[int] = set()
    for qid, log_id, event_type, event_timestamp in events:
        qid_num = cache._parse_qid(qid)
        if qid_num is None:
            continue
        log_id_num = cache._as_uint64(log_id, "log_id")
        if log_id_num in seen_log_ids:
            continue
        event_type_text = _normalize_text(event_type)
        if event_type_text is None:
            continue
        event_type_text = event_type_text.lower()
        if event_type_text not in {"delete", "undelete"}:
            continue
        event_timestamp_num = _to_utc_datetime(event_timestamp)
        if event_timestamp_num is None:
            continue
        seen_log_ids.add(log_id_num)
        normalized.append((log_id_num, qid_num, event_type_text, event_timestamp_num))

    if not normalized:
        return 0

    started = time.perf_counter()
    updated = 0

    async with cache._write_guard():
        async with cache._connect() as db:
            for chunk in cache._chunked(normalized):
                await db.execute("BEGIN IMMEDIATE")
                if cache._backend_name == "mariadb":
                    values_sql = ", ".join("(%s, %s, %s, %s)" for _ in chunk)
                    params: list[int | str] = []
                    for log_id_num, qid_num, event_type_text, event_timestamp_num in chunk:
                        params.extend([log_id_num, qid_num, event_type_text, event_timestamp_num])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO content_deletion_events (
                            log_id, qid, event_type, event_timestamp
                        )
                        VALUES {values_sql}
                        ON DUPLICATE KEY UPDATE
                            qid = VALUES(qid),
                            event_type = VALUES(event_type),
                            event_timestamp = VALUES(event_timestamp)
                        RETURNING log_id
                        """,
                        params,
                    )
                else:
                    values_sql = ", ".join("(?, ?, ?, ?)" for _ in chunk)
                    params: list[int | str] = []
                    for log_id_num, qid_num, event_type_text, event_timestamp_num in chunk:
                        params.extend([log_id_num, qid_num, event_type_text, event_timestamp_num])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO content_deletion_events (
                            log_id, qid, event_type, event_timestamp
                        )
                        VALUES {values_sql}
                        ON CONFLICT(log_id) DO UPDATE SET
                            qid = excluded.qid,
                            event_type = excluded.event_type,
                            event_timestamp = excluded.event_timestamp
                        RETURNING log_id
                        """,
                        params,
                    )
                rows = await cursor.fetchall()
                updated += len(rows)
                await db.commit()

    cache._warn_slow_write("upsert_content_deletion_events", started, row_count=updated)
    return updated


async def clear_content_last_revids(cache: "EvaluationCache", qids: Sequence[str | int]) -> int:
    await cache.initialize()

    qid_nums: list[int] = []
    seen: set[int] = set()
    for qid in qids:
        qid_num = cache._parse_qid(qid)
        if qid_num in seen:
            continue
        seen.add(qid_num)
        qid_nums.append(qid_num)

    if not qid_nums:
        return 0

    started = time.perf_counter()
    updated = 0
    timestamp_sql = _summary_update_timestamp_sql(cache)

    async with cache._write_guard():
        async with cache._connect() as db:
            for chunk in cache._chunked(qid_nums):
                await db.execute("BEGIN IMMEDIATE")
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"""
                    UPDATE content_evaluation
                    SET content_last_revid = NULL,
                        last_updated = {timestamp_sql}
                    WHERE qid IN ({placeholders})
                    RETURNING qid
                    """,
                    chunk,
                )
                rows = await cursor.fetchall()
                updated += len(rows)
                await db.commit()

    cache._warn_slow_write("clear_content_last_revids", started, row_count=updated)
    return updated


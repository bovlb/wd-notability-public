from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING
from uuid import uuid4

from wd_notability.creations import CREATIONS, CreationMetadata
from wd_notability.item_trace import ItemTraceRecord
from wd_notability.metadata import read as rc_read
from wd_notability import user_history

if TYPE_CHECKING:
    from collections.abc import Sequence
    from wd_notability.evaluation_cache import EvaluationCache


def _format_iso8601_epoch(epoch_seconds: int | float | None) -> str:
    if epoch_seconds is None:
        return "unknown"
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(epoch_seconds)))
    except (TypeError, ValueError, OverflowError):
        return "unknown"


async def _record_recent_changes_trace(cache: "EvaluationCache", records: list[ItemTraceRecord]) -> None:
    trace = getattr(cache, "item_trace", None)
    if not records or not getattr(trace, "enabled", True):
        return
    await trace.record_events(records)


def _recent_changes_written_details(revid: int, *, source: str) -> dict[str, object]:
    return {
        "committed": True,
        "source": source,
        "recent_changes_last_revid": revid,
    }


def _creation_metadata_written_details(
    row: CreationMetadata,
    *,
    source: str,
) -> dict[str, object]:
    return {
        "committed": True,
        "source": source,
        "creator_actor_id": row.creator_actor_id,
        "creation_time": row.creation_time,
    }


async def _upsert_creation_metadata(
    cache: "EvaluationCache",
    qids: list[str],
    *,
    trace_batch_id: str | None = None,
    trace_source: str = "recent_changes_scan",
) -> tuple[int, str | None]:
    if not qids:
        return 0, None

    metadata_rows = await asyncio.to_thread(CREATIONS.fetch_creation_metadata_many, qids)
    if not metadata_rows:
        return 0, None

    updated = await cache.upsert_creation_metadata_many(metadata_rows)
    creation_range = None
    if metadata_rows:
        creation_range = f"{_format_iso8601_epoch(metadata_rows[0].creation_time)}..{_format_iso8601_epoch(metadata_rows[-1].creation_time)}"
    try:
        trace = getattr(cache, "item_trace", None)
        if getattr(trace, "enabled", True):
            await _record_recent_changes_trace(
                cache,
                [
                    ItemTraceRecord(
                        qid=row.qid,
                        event_type="creation_metadata_written",
                        worker_name="recent_changes",
                        batch_id=trace_batch_id,
                        details=_creation_metadata_written_details(row, source=trace_source),
                    )
                    for row in metadata_rows
                ],
            )
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes monitor creation metadata trace emit failed: {exc}")
    return updated, creation_range


async def run_creation_interest_backfill(cache: "EvaluationCache", limit: int) -> tuple[int, str | None]:
    creation_qids: set[str] = set()
    try:
        creation_qids.update(
            await cache.interest.list_interest_creation_targets(limit=limit)
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes monitor creation-interest lookup failed: {exc}")
        return 0, None

    if not creation_qids:
        return 0, None

    try:
        return await _upsert_creation_metadata(
            cache,
            sorted(creation_qids),
            trace_source="creation_interest_backfill",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes monitor creation-interest backfill failed: {exc}")
        return 0, None


async def run_user_creation_backfill(cache: "EvaluationCache", limit: int) -> tuple[int, str | None]:
    try:
        requests = await user_history.list_user_history_requests(cache, limit)
    except Exception as exc:  # noqa: BLE001
        print(f"Recent changes monitor user-creation lookup failed: {exc}")
        return 0, None

    if not requests:
        return 0, None

    total_updated = 0
    summaries: list[str] = []
    for record in requests:
        started_at = int(time.time())
        try:
            await user_history.upsert_user_history(
                cache,
                username=record.username,
                window_start=record.window_start,
                window_end=record.window_end,
                requested_at=record.requested_at if record.requested_at is not None else started_at,
                started_at=started_at,
                finished_at=None,
                last_refresh_at=record.last_refresh_at,
                error_text=None,
                row_count=record.row_count,
            )
            metadata_rows = await asyncio.to_thread(
                CREATIONS.fetch_creation_metadata_for_creators,
                start=record.window_start,
                end=record.window_end,
                creators=[record.username],
            )
            if metadata_rows:
                updated = await cache.upsert_creation_metadata_many(metadata_rows)
                try:
                    await _record_recent_changes_trace(
                        cache,
                        [
                            ItemTraceRecord(
                                qid=row.qid,
                                event_type="creation_metadata_written",
                                worker_name="recent_changes",
                                batch_id=f"user_creation:{record.username}",
                                details=_creation_metadata_written_details(
                                    row,
                                    source="user_creation_backfill",
                                ),
                            )
                            for row in metadata_rows
                        ],
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"Recent changes monitor user-creation trace emit failed for {record.username}: {exc}")
            else:
                updated = 0
            total_updated += updated
            finished_at = int(time.time())
            await user_history.upsert_user_history(
                cache,
                username=record.username,
                window_start=record.window_start,
                window_end=record.window_end,
                requested_at=record.requested_at if record.requested_at is not None else started_at,
                started_at=started_at,
                finished_at=finished_at,
                last_refresh_at=finished_at,
                error_text=None,
                row_count=updated,
            )
            summaries.append(f"{record.username}:{updated}")
        except Exception as exc:  # noqa: BLE001
            finished_at = int(time.time())
            try:
                await user_history.upsert_user_history(
                    cache,
                    username=record.username,
                    window_start=record.window_start,
                    window_end=record.window_end,
                    requested_at=record.requested_at if record.requested_at is not None else started_at,
                    started_at=started_at,
                    finished_at=finished_at,
                    last_refresh_at=finished_at,
                    error_text=str(exc),
                    row_count=record.row_count,
                )
            except Exception as update_exc:  # noqa: BLE001
                print(f"Recent changes monitor failed to record user-creation error for {record.username}: {update_exc}")
            print(f"Recent changes monitor user-creation backfill failed for {record.username}: {exc}")
            summaries.append(f"{record.username}:error")

    return total_updated, ", ".join(summaries) if summaries else None


async def run_recent_changes_scan_pass(
    cache: "EvaluationCache",
    start_epoch: float,
    start_rc_id: int = 0,
) -> tuple[int, int, tuple[float | None, int | None, float | None, float]]:
    trace_batch_id = str(uuid4())
    latest_seen = start_epoch
    latest_creation_seen: float | None = None
    qid_to_revid: dict[str, int] = {}
    creation_rows: dict[str, CreationMetadata] = {}
    cursor_timestamp = start_epoch
    cursor_id = start_rc_id
    total_fetched = 0
    oldest_timestamp = None
    newest_timestamp = None

    while True:
        replica_config = getattr(rc_read.RECENT_CHANGES_REPLICA, "_config", None)
        replica_enabled = bool(getattr(replica_config, "enabled", True))
        print(
            "Recent changes monitor fetch starting: "
            f"start_epoch={_format_iso8601_epoch(cursor_timestamp)}, "
            f"start_rc_id={cursor_id}, "
            f"limit={replica_enabled and 1000 or 0}"
        )
        changes, last_cursor = await asyncio.to_thread(
            rc_read.RECENT_CHANGES_REPLICA.fetch_recent_changes,
            start_epoch=cursor_timestamp,
            start_rc_id=cursor_id,
            limit=1000,
        )
        print(
            "Recent changes monitor fetch finished: "
            f"entries={len(changes)}, "
            f"last_cursor={_format_iso8601_epoch(last_cursor[0]) if last_cursor[0] is not None else 'unknown'}, "
            f"last_rc_id={last_cursor[1]}"
        )
        total_fetched += len(changes)
        if last_cursor[0] is not None:
            cursor_timestamp, cursor_id = last_cursor[0], last_cursor[1] or 0
        for change in changes:
            if not isinstance(change, dict):
                continue
            oldest_timestamp = min(oldest_timestamp, change.get("timestamp")) if oldest_timestamp is not None else change.get("timestamp")
            newest_timestamp = max(newest_timestamp, change.get("timestamp")) if newest_timestamp is not None else change.get("timestamp")
            title = change.get("title")
            qid = title if isinstance(title, str) and title.startswith("Q") and title[1:].isdigit() else None
            revid = change.get("revid")
            rc_source = change.get("rc_source")
            timestamp = rc_read._parse_replica_timestamp(change.get("timestamp"))
            creator_actor_id = change.get("creator_actor_id")
            creator_actor_id_num = creator_actor_id if isinstance(creator_actor_id, int) else None
            if qid is None or not isinstance(revid, int):
                continue
            if timestamp is not None:
                latest_seen = max(latest_seen, timestamp)
            previous = qid_to_revid.get(qid)
            if previous is None or revid > previous:
                qid_to_revid[qid] = revid
            if (
                timestamp is not None
                and creator_actor_id_num is not None
                and isinstance(rc_source, str)
                and rc_source == "mw.new"
                and qid not in creation_rows
            ):
                creation_rows[qid] = CreationMetadata(
                    qid=qid,
                    creator_actor_id=creator_actor_id_num,
                    creation_time=int(timestamp),
                )
                latest_creation_seen = max(latest_creation_seen, timestamp) if latest_creation_seen is not None else timestamp

        if len(changes) < 1000 or last_cursor[0] is None:
            break

    updated = 0
    creation_updated = 0
    if qid_to_revid:
        updated = await cache.update_recent_changes_last_revids(qid_to_revid)
        try:
            await _record_recent_changes_trace(
                cache,
                [
                    ItemTraceRecord(
                        qid=qid,
                        event_type="recent_changes_written",
                        worker_name="recent_changes",
                        batch_id=trace_batch_id,
                        details=_recent_changes_written_details(revid, source="recent_changes_scan"),
                    )
                    for qid, revid in qid_to_revid.items()
                ],
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Recent changes monitor recent-changes trace emit failed: {exc}")
    if creation_rows:
        creation_updated = await cache.upsert_creation_metadata_many(list(creation_rows.values()))
        try:
            await _record_recent_changes_trace(
                cache,
                [
                    ItemTraceRecord(
                        qid=row.qid,
                        event_type="creation_metadata_written",
                        worker_name="recent_changes",
                        batch_id=trace_batch_id,
                        details=_creation_metadata_written_details(row, source="recent_changes_scan"),
                    )
                    for row in creation_rows.values()
                ],
            )
        except Exception as exc:  # noqa: BLE001
            print(f"Recent changes monitor creation trace emit failed: {exc}")

    return updated, creation_updated, (latest_seen, cursor_id, latest_creation_seen, start_epoch)

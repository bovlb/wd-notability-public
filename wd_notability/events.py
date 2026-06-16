from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import aiosqlite

from wd_notability import summary as summary_bits
from wd_notability.models import NotabilityCriterion, NotabilityLevel

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


class EventLogStore:
    def __init__(self, cache: EvaluationCache):
        self.cache = cache

    async def _append_event_log(
        self,
        db: aiosqlite.Connection,
        events: list[tuple[int, int, str, int, int]],
    ) -> None:
        if not events:
            return
        await db.executemany(
            """
            INSERT INTO pubsub_events (timestamp, qid, event_type, summary, mask)
            VALUES (?, ?, ?, ?, ?)
            """,
            events,
        )

    async def append_summary_updates(
        self,
        *,
        event_type: str,
        summary_updates: list[tuple[str | int, int]],
        mask: int,
    ) -> int:
        await self.cache.initialize()

        if not summary_updates:
            return 0

        started = time.perf_counter()
        timestamp = int(time.time())
        rows = [
            (timestamp, self.cache._parse_qid(qid), event_type, self.cache._as_uint32(summary, "summary"), mask)
            for qid, summary in summary_updates
        ]
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                await self._append_event_log(db, rows)
                await db.commit()

        self.cache._warn_slow_write("append_summary_updates", started, row_count=len(rows))
        return len(rows)

    async def append_summary_updates_in_txn(
        self,
        db: aiosqlite.Connection,
        *,
        event_type: str,
        summary_updates: list[tuple[str | int, int]],
        mask: int,
    ) -> int:
        if not summary_updates:
            return 0

        timestamp = int(time.time())
        rows = [
            (timestamp, self.cache._parse_qid(qid), event_type, self.cache._as_uint32(summary, "summary"), mask)
            for qid, summary in summary_updates
        ]
        await self._append_event_log(db, rows)
        return len(rows)

    async def purge_expired_pubsub_events(self, *, now: int | None = None, max_age_seconds: int = 3600) -> int:
        await self.cache.initialize()

        cutoff_epoch = max(0, (int(time.time()) if now is None else now) - max(0, int(max_age_seconds)))
        cutoff = self.cache._as_uint32(cutoff_epoch, "cutoff")
        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    """
                    DELETE FROM pubsub_events
                    WHERE timestamp < ?
                    """,
                    (cutoff,),
                )
                await db.commit()
        self.cache._warn_slow_write("purge_expired_pubsub_events", started, row_count=int(cursor.rowcount))
        return int(cursor.rowcount)

    async def event_log_stats(self) -> dict[str, Any]:
        await self.cache.initialize()

        now = int(time.time())
        async with self.cache._connect() as db:
            total_cursor = await db.execute(
                """
                SELECT
                    COUNT(*),
                    MIN(timestamp),
                    MAX(timestamp),
                    SUM(CASE WHEN timestamp > ? THEN 1 ELSE 0 END),
                    SUM(CASE WHEN timestamp > ? THEN 1 ELSE 0 END),
                    SUM(CASE WHEN timestamp > ? THEN 1 ELSE 0 END)
                FROM pubsub_events
                """,
                (now - 60, now - 300, now - 3600),
            )
            total_row = await total_cursor.fetchone()

            field_cursor = await db.execute(
                f"""
                SELECT
                    SUM(CASE WHEN (mask & {summary_bits.REDIRECT}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.HAS_SITELINKS}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.HAS_CLAIMS}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.DELETED}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N1)}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N2a)}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N2b)}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N3_INLINKS)}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N3_OSM)}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N3_WIKISUB)}) != 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN (mask & {summary_bits.mask(NotabilityCriterion.N3_SDC)}) != 0 THEN 1 ELSE 0 END)
                FROM pubsub_events
                """
            )
            field_row = await field_cursor.fetchone()

            type_cursor = await db.execute(
                """
                SELECT event_type, COUNT(*)
                FROM pubsub_events
                GROUP BY event_type
                ORDER BY event_type ASC
                """
            )
            type_rows = await type_cursor.fetchall()

            max_per_second_cursor = await db.execute(
                """
                SELECT MAX(bucket_count)
                FROM (
                    SELECT timestamp AS bucket, COUNT(*) AS bucket_count
                    FROM pubsub_events
                    GROUP BY bucket
                )
                """
            )
            max_per_second_row = await max_per_second_cursor.fetchone()

            max_per_minute_cursor = await db.execute(
                """
                SELECT MAX(bucket_count)
                FROM (
                    SELECT CAST(timestamp / 60 AS INTEGER) AS bucket, COUNT(*) AS bucket_count
                    FROM pubsub_events
                    GROUP BY bucket
                )
                """
            )
            max_per_minute_row = await max_per_minute_cursor.fetchone()

        last_60 = int(total_row[3]) if total_row and total_row[3] is not None else 0
        last_300 = int(total_row[4]) if total_row and total_row[4] is not None else 0
        last_3600 = int(total_row[5]) if total_row and total_row[5] is not None else 0
        return {
            "entries": int(total_row[0]) if total_row and total_row[0] is not None else 0,
            "oldest_timestamp": int(total_row[1]) if total_row and total_row[1] is not None else None,
            "newest_timestamp": int(total_row[2]) if total_row and total_row[2] is not None else None,
            "throughput": {
                "last_60_seconds": last_60,
                "last_300_seconds": last_300,
                "last_3600_seconds": last_3600,
                "per_second_last_60_seconds": last_60 / 60,
                "per_second_last_300_seconds": last_300 / 300,
                "per_second_last_3600_seconds": last_3600 / 3600,
                "max_events_per_second": int(max_per_second_row[0]) if max_per_second_row and max_per_second_row[0] is not None else 0,
                "max_events_per_minute": int(max_per_minute_row[0]) if max_per_minute_row and max_per_minute_row[0] is not None else 0,
            },
            "by_event_type": {str(row[0]): int(row[1]) for row in type_rows},
            "by_field": {
                "summary": int(field_row[0]) if field_row and field_row[0] is not None else 0,
                "redirect": int(field_row[1]) if field_row and field_row[1] is not None else 0,
                "has_sitelinks": int(field_row[2]) if field_row and field_row[2] is not None else 0,
                "has_claims": int(field_row[3]) if field_row and field_row[3] is not None else 0,
                "deleted": int(field_row[4]) if field_row and field_row[4] is not None else 0,
                "N1": int(field_row[5]) if field_row and field_row[5] is not None else 0,
                "N2a": int(field_row[6]) if field_row and field_row[6] is not None else 0,
                "N2b": int(field_row[7]) if field_row and field_row[7] is not None else 0,
                "N3_inlinks": int(field_row[8]) if field_row and field_row[8] is not None else 0,
                "N3_osm": int(field_row[9]) if field_row and field_row[9] is not None else 0,
                "N3_wikisub": int(field_row[10]) if field_row and field_row[10] is not None else 0,
                "N3_sdc": int(field_row[11]) if field_row and field_row[11] is not None else 0,
            },
        }

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from wd_notability import summary as summary_bits
from wd_notability.models import NotabilityCriterion, NotabilityLevel

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache


class PubSubStore:
    def __init__(self, cache: EvaluationCache):
        self.cache = cache

    async def purge_expired_pubsub_sessions(self, *, now: int | None = None) -> int:
        await self.cache.initialize()

        expires_before = self.cache._as_uint32(int(time.time()) if now is None else now, "now")
        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    """
                    DELETE FROM pubsub_sessions
                    WHERE expires_at <= ?
                    """,
                    (expires_before,),
                )
                await db.commit()
        self.cache._warn_slow_write("purge_expired_pubsub_sessions", started, row_count=int(cursor.rowcount))
        return int(cursor.rowcount)

    async def create_pubsub_session(
        self,
        *,
        owner_id: str,
        session_id: str,
        ttl_seconds: int,
        priority: int = 10,
        wants_entitydata: bool,
        wants_inlinks: bool,
        wants_sync: bool,
        qids: list[str | int] | None = None,
    ) -> int:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")

        ttl = self.cache._as_uint32(ttl_seconds, "ttl_seconds")
        session_priority = self.cache._as_uint32(priority, "priority")
        expires_at = self.cache._as_uint32(int(time.time()) + ttl, "expires_at")
        qid_nums: list[int] = []
        seen: set[int] = set()
        for qid in qids or []:
            qid_num = self.cache._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            qid_nums.append(qid_num)

        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    """
                    DELETE FROM pubsub_sessions
                    WHERE session_id = ? AND owner_id = ?
                    """,
                    (session, owner),
                )
                if qid_nums:
                    rows = [(
                        session,
                        owner,
                        0,
                        expires_at,
                        session_priority,
                        1 if wants_entitydata else 0,
                        1 if wants_inlinks else 0,
                        1 if wants_sync else 0,
                    )]
                    rows.extend(
                        (
                            session,
                            owner,
                            qid_num,
                            expires_at,
                            session_priority,
                            1 if wants_entitydata else 0,
                            1 if wants_inlinks else 0,
                            1 if wants_sync else 0,
                        )
                        for qid_num in qid_nums
                    )
                    await db.executemany(
                        """
                        INSERT INTO pubsub_sessions (
                            session_id, owner_id, qid, expires_at, priority, wants_entitydata, wants_inlinks, wants_sync
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                else:
                    await db.execute(
                        """
                        INSERT INTO pubsub_sessions (
                            session_id, owner_id, qid, expires_at, priority, wants_entitydata, wants_inlinks, wants_sync
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            session,
                            owner,
                            0,
                            expires_at,
                            session_priority,
                            1 if wants_entitydata else 0,
                            1 if wants_inlinks else 0,
                            1 if wants_sync else 0,
                        ),
                    )
                await db.commit()
        self.cache._warn_slow_write("create_pubsub_session", started, row_count=len(qid_nums) + 1)
        return len(qid_nums)

    async def add_pubsub_session_qids(
        self,
        *,
        owner_id: str,
        session_id: str,
        qids: list[str | int],
        priority: int = 10,
        wants_entitydata: bool | None = None,
        wants_inlinks: bool | None = None,
        wants_sync: bool | None = None,
    ) -> int:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")

        qid_nums: list[int] = []
        seen: set[int] = set()
        for qid in qids:
            qid_num = self.cache._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            qid_nums.append(qid_num)

        started = time.perf_counter()
        affected_rows = 0
        session_priority = self.cache._as_uint32(priority, "priority")
        backend_name = self.cache._backend_name
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    """
                    SELECT expires_at, priority, wants_entitydata, wants_inlinks, wants_sync
                    FROM pubsub_sessions
                    WHERE session_id = ? AND owner_id = ? AND qid = 0
                    LIMIT 1
                    """,
                    (session, owner),
                )
                row = await cursor.fetchone()
                if row is None:
                    await db.commit()
                    return 0
                expires_at = int(row[0])
                current_priority = int(row[1]) if row[1] is not None else 10
                current_wants_entitydata = bool(row[2])
                current_wants_inlinks = bool(row[3])
                current_wants_sync = bool(row[4])
                session_wants_entitydata = current_wants_entitydata if wants_entitydata is None else bool(wants_entitydata)
                session_wants_inlinks = current_wants_inlinks if wants_inlinks is None else bool(wants_inlinks)
                session_wants_sync = current_wants_sync if wants_sync is None else bool(wants_sync)
                session_priority = max(current_priority, session_priority)
                await db.execute(
                    """
                    UPDATE pubsub_sessions
                    SET priority = ?,
                        wants_entitydata = ?,
                        wants_inlinks = ?,
                        wants_sync = ?
                    WHERE session_id = ? AND owner_id = ? AND qid = 0
                    """,
                    (
                        session_priority,
                        1 if session_wants_entitydata else 0,
                        1 if session_wants_inlinks else 0,
                        1 if session_wants_sync else 0,
                        session,
                        owner,
                    ),
                )
                if qid_nums:
                    rows = [
                        (
                            session,
                            owner,
                            qid_num,
                            expires_at,
                            session_priority,
                            1 if session_wants_entitydata else 0,
                            1 if session_wants_inlinks else 0,
                            1 if session_wants_sync else 0,
                        )
                        for qid_num in qid_nums
                    ]
                    if backend_name == "mariadb":
                        await db.executemany(
                            """
                            INSERT INTO pubsub_sessions (
                                session_id, owner_id, qid, expires_at, priority, wants_entitydata, wants_inlinks, wants_sync
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE
                                expires_at = VALUES(expires_at),
                                priority = VALUES(priority),
                                wants_entitydata = VALUES(wants_entitydata),
                                wants_inlinks = VALUES(wants_inlinks),
                                wants_sync = VALUES(wants_sync)
                            """,
                            rows,
                        )
                    else:
                        await db.executemany(
                            """
                            INSERT INTO pubsub_sessions (
                                session_id, owner_id, qid, expires_at, priority, wants_entitydata, wants_inlinks, wants_sync
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(session_id, owner_id, qid) DO UPDATE SET
                                expires_at = excluded.expires_at,
                                priority = excluded.priority,
                                wants_entitydata = excluded.wants_entitydata,
                                wants_inlinks = excluded.wants_inlinks,
                                wants_sync = excluded.wants_sync
                            """,
                            rows,
                        )
                    affected_rows += len(qid_nums)
                affected_rows += 1
                await db.commit()
        self.cache._warn_slow_write("add_pubsub_session_qids", started, row_count=affected_rows)
        return affected_rows

    async def refresh_pubsub_session(
        self,
        *,
        owner_id: str,
        session_id: str,
        ttl_seconds: int,
    ) -> int:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")
        ttl = self.cache._as_uint32(ttl_seconds, "ttl_seconds")
        expires_at = self.cache._as_uint32(int(time.time()) + ttl, "expires_at")

        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    """
                    UPDATE pubsub_sessions
                    SET expires_at = ?
                    WHERE session_id = ? AND owner_id = ?
                    """,
                    (expires_at, session, owner),
                )
                await db.commit()
        self.cache._warn_slow_write("refresh_pubsub_session", started, row_count=int(cursor.rowcount))
        return int(cursor.rowcount)

    async def delete_pubsub_session(self, *, owner_id: str, session_id: str) -> int:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")

        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    """
                    DELETE FROM pubsub_sessions
                    WHERE session_id = ? AND owner_id = ?
                    """,
                    (session, owner),
                )
                await db.commit()
        self.cache._warn_slow_write("delete_pubsub_session", started, row_count=int(cursor.rowcount))
        return int(cursor.rowcount)

    async def list_pubsub_events_for_session(
        self,
        *,
        owner_id: str,
        session_id: str,
        after_event_id: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, int | str | None]]:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")
        after = self.cache._as_uint32(after_event_id, "after_event_id")

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT e.event_id, e.timestamp, e.qid, e.event_type, e.summary, e.mask
                    FROM pubsub_events e
                    WHERE e.event_id > ?
                      AND EXISTS (
                          SELECT 1
                          FROM pubsub_sessions s
                          WHERE s.owner_id = ?
                            AND s.session_id = ?
                            AND s.qid = e.qid
                      )
                    ORDER BY e.event_id ASC
                    """,
                    (after, owner, session),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT e.event_id, e.timestamp, e.qid, e.event_type, e.summary, e.mask
                    FROM pubsub_events e
                    WHERE e.event_id > ?
                      AND EXISTS (
                          SELECT 1
                          FROM pubsub_sessions s
                          WHERE s.owner_id = ?
                            AND s.session_id = ?
                            AND s.qid = e.qid
                      )
                    ORDER BY e.event_id ASC
                    LIMIT ?
                    """,
                    (after, owner, session, limit),
                )
            rows = await cursor.fetchall()

        return [
            {
                "event_id": int(row[0]),
                "timestamp": int(row[1]),
                "qid": int(row[2]),
                "event_type": str(row[3]),
                "summary": int(row[4]),
                "mask": int(row[5]),
            }
            for row in rows
        ]

    async def list_pubsub_entitydata_candidates(self, limit: int | None = None) -> list[str]:
        await self.cache.initialize()

        never_evaluated_expr = "(ec.qid IS NULL OR ec.entitydata_last_revid IS NULL)"
        stale_expr = "(ec.recent_changes_last_revid IS NOT NULL AND ec.entitydata_last_revid < ec.recent_changes_last_revid)"

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT
                        s.qid,
                        SUM(COALESCE(s.priority, 10)) AS subscriber_priority,
                        CASE WHEN ec.qid IS NULL OR ec.entitydata_last_revid IS NULL THEN 1 ELSE 0 END AS never_evaluated
                    FROM pubsub_sessions s
                    LEFT JOIN evaluation_cache ec
                      ON ec.qid = s.qid
                    WHERE s.qid != 0
                      AND s.wants_entitydata = 1
                      AND (""" + never_evaluated_expr + """ OR """ + stale_expr + """)
                    GROUP BY s.qid
                    ORDER BY subscriber_priority DESC, never_evaluated DESC, s.qid ASC
                    """
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT
                        s.qid,
                        SUM(COALESCE(s.priority, 10)) AS subscriber_priority,
                        CASE WHEN ec.qid IS NULL OR ec.entitydata_last_revid IS NULL THEN 1 ELSE 0 END AS never_evaluated
                    FROM pubsub_sessions s
                    LEFT JOIN evaluation_cache ec
                      ON ec.qid = s.qid
                    WHERE s.qid != 0
                      AND s.wants_entitydata = 1
                      AND (""" + never_evaluated_expr + """ OR """ + stale_expr + """)
                    GROUP BY s.qid
                    ORDER BY subscriber_priority DESC, never_evaluated DESC, s.qid ASC
                    LIMIT ?
                    """,
                    (limit,),
                )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def list_pubsub_sync_qids(self, limit: int | None = None) -> list[str]:
        await self.cache.initialize()

        n3_osm_mask = summary_bits.mask(NotabilityCriterion.N3_OSM)
        n3_osm_unknown = summary_bits.value(NotabilityCriterion.N3_OSM, NotabilityLevel.UNKNOWN)
        n3_wikisub_mask = summary_bits.mask(NotabilityCriterion.N3_WIKISUB)
        n3_wikisub_unknown = summary_bits.value(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.UNKNOWN)
        n3_sdc_mask = summary_bits.mask(NotabilityCriterion.N3_SDC)
        n3_sdc_unknown = summary_bits.value(NotabilityCriterion.N3_SDC, NotabilityLevel.UNKNOWN)

        needs_sync_expr = (
            f"ec.qid IS NULL OR "
            f"(ec.summary & {n3_osm_mask}) = {n3_osm_unknown} OR "
            f"(ec.summary & {n3_wikisub_mask}) = {n3_wikisub_unknown} OR "
            f"(ec.summary & {n3_sdc_mask}) = {n3_sdc_unknown}"
        )

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT
                        s.qid,
                        SUM(COALESCE(s.priority, 10)) AS subscriber_priority
                    FROM pubsub_sessions s
                    LEFT JOIN evaluation_cache ec
                      ON ec.qid = s.qid
                    WHERE s.qid != 0
                      AND s.wants_sync = 1
                      AND (""" + needs_sync_expr + """)
                    GROUP BY s.qid
                    ORDER BY subscriber_priority DESC, s.qid ASC
                    """
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT
                        s.qid,
                        SUM(COALESCE(s.priority, 10)) AS subscriber_priority
                    FROM pubsub_sessions s
                    LEFT JOIN evaluation_cache ec
                      ON ec.qid = s.qid
                    WHERE s.qid != 0
                      AND s.wants_sync = 1
                      AND (""" + needs_sync_expr + """)
                    GROUP BY s.qid
                    ORDER BY subscriber_priority DESC, s.qid ASC
                    LIMIT ?
                    """,
                    (limit,),
                )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def list_pubsub_inlinks_targets(self, limit: int | None = None) -> list[str]:
        await self.cache.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT s.qid, SUM(COALESCE(s.priority, 10)) AS subscriber_priority
                    FROM pubsub_sessions s
                    LEFT JOIN evaluation_cache ec
                      ON ec.qid = s.qid
                    WHERE s.qid != 0
                      AND s.wants_inlinks = 1
                      AND s.owner_id != 'inlinks'
                      AND (ec.qid IS NULL OR (ec.summary & ?) = ?)
                    GROUP BY s.qid
                    ORDER BY subscriber_priority DESC, s.qid ASC
                    """,
                    (n3_inlinks_mask, n3_inlinks_unknown),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT s.qid, SUM(COALESCE(s.priority, 10)) AS subscriber_priority
                    FROM pubsub_sessions s
                    LEFT JOIN evaluation_cache ec
                      ON ec.qid = s.qid
                    WHERE s.qid != 0
                      AND s.wants_inlinks = 1
                      AND s.owner_id != 'inlinks'
                      AND (ec.qid IS NULL OR (ec.summary & ?) = ?)
                    GROUP BY s.qid
                    ORDER BY subscriber_priority DESC, s.qid ASC
                    LIMIT ?
                    """,
                    (n3_inlinks_mask, n3_inlinks_unknown, limit),
                )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def list_pubsub_session_ids(self, owner_id: str, limit: int | None = None) -> list[str]:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT DISTINCT session_id
                    FROM pubsub_sessions
                    WHERE owner_id = ?
                      AND qid != 0
                    ORDER BY session_id ASC
                    """,
                    (owner,),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT DISTINCT session_id
                    FROM pubsub_sessions
                    WHERE owner_id = ?
                      AND qid != 0
                    ORDER BY session_id ASC
                    LIMIT ?
                    """,
                    (owner, limit),
                )
            rows = await cursor.fetchall()

        return [str(row[0]) for row in rows]

    async def list_pubsub_session_qids(self, *, owner_id: str, session_id: str) -> list[str]:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")

        async with self.cache._connect() as db:
            cursor = await db.execute(
                """
                SELECT qid
                FROM pubsub_sessions
                WHERE owner_id = ?
                  AND session_id = ?
                  AND qid != 0
                ORDER BY qid ASC
                """,
                (owner, session),
            )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def has_pubsub_inlinks_interest(self, qid: str | int) -> bool:
        await self.cache.initialize()

        qid_num = self.cache._parse_qid(qid)
        async with self.cache._connect() as db:
            cursor = await db.execute(
                """
                SELECT 1
                FROM pubsub_sessions
                WHERE qid = ?
                  AND qid != 0
                  AND wants_inlinks = 1
                  AND owner_id != 'inlinks'
                LIMIT 1
                """,
                (qid_num,),
            )
            row = await cursor.fetchone()
        return row is not None

    async def pubsub_stats(self) -> dict[str, Any]:
        await self.cache.initialize()

        now = int(time.time())
        async with self.cache._connect() as db:
            total_cursor = await db.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT session_id || '|' || owner_id),
                    COUNT(DISTINCT CASE WHEN qid != 0 THEN qid END),
                    MIN(expires_at),
                    MAX(expires_at),
                    SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END),
                    SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END)
                FROM pubsub_sessions
                """,
                (now, now + 60, now + 300, now + 3600),
            )
            total_row = await total_cursor.fetchone()

            owner_cursor = await db.execute(
                """
                SELECT owner_id, COUNT(*)
                FROM pubsub_sessions
                GROUP BY owner_id
                ORDER BY owner_id
                """
            )
            owner_rows = await owner_cursor.fetchall()

            flag_counts: dict[str, dict[str, int]] = {}
            for name in ("wants_entitydata", "wants_inlinks", "wants_sync"):
                cursor = await db.execute(
                    f"""
                    SELECT ({name} != 0) AS has_value, COUNT(*)
                    FROM pubsub_sessions
                    GROUP BY has_value
                    """
                )
                rows = await cursor.fetchall()
                counts = {0: 0, 1: 0}
                for has_value, count in rows:
                    counts[int(has_value)] = int(count)
                flag_counts[name] = {"yes": counts[1], "no": counts[0]}

        return {
            "entries": int(total_row[0]) if total_row and total_row[0] is not None else 0,
            "distinct_sessions": int(total_row[1]) if total_row and total_row[1] is not None else 0,
            "distinct_qids": int(total_row[2]) if total_row and total_row[2] is not None else 0,
            "oldest_expires_at": int(total_row[3]) if total_row and total_row[3] is not None else None,
            "newest_expires_at": int(total_row[4]) if total_row and total_row[4] is not None else None,
            "expiring": {
                "now": int(total_row[5]) if total_row and total_row[5] is not None else 0,
                "next_60_seconds": int(total_row[6]) if total_row and total_row[6] is not None else 0,
                "next_300_seconds": int(total_row[7]) if total_row and total_row[7] is not None else 0,
                "next_3600_seconds": int(total_row[8]) if total_row and total_row[8] is not None else 0,
            },
            "by_owner": {str(row[0]): int(row[1]) for row in owner_rows},
            "flags": flag_counts,
        }

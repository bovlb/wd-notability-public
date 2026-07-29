from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from wd_notability.content import db_read as content_db_read
from wd_notability.inlinks import db_read as inlinks_db_read
from wd_notability.inlinks import db_write as inlinks_db_write
from wd_notability.interest.manager import InterestManager
from wd_notability.interest.session import InterestSession
from wd_notability.item_trace import ItemTraceRecord
from wd_notability.item_trace import ITEM_TRACE_ENABLED
from wd_notability.models import NotabilityCriterion, external_usage_level

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache

logger = logging.getLogger(__name__)

class InterestStore:
    def __init__(self, cache: EvaluationCache):
        self.cache = cache

    async def _record_interest_started(
        self,
        *,
        worker_name: str,
        qids: list[str | int],
        interest_type: str,
        details: dict[str, object] | None = None,
        batch_id: str | None = None,
        timestamp: int | float | None = None,
    ) -> None:
        if not qids:
            return

        trace = getattr(self.cache, "item_trace", None)
        if trace is None or not getattr(trace, "enabled", ITEM_TRACE_ENABLED):
            return

        payload: dict[str, object] = {"interest_type": interest_type}
        if details:
            payload.update(details)

        emit_many = getattr(trace, "record_interest_started_many", None)
        if callable(emit_many):
            await emit_many(
                worker_name=worker_name,
                qids=qids,
                interest_type=interest_type,
                details=details,
                batch_id=batch_id,
                timestamp=timestamp,
            )
            return

        emit_one = getattr(trace, "record_event", None)
        if not callable(emit_one):
            return

        for qid in qids:
            await emit_one(
                ItemTraceRecord(
                    qid=qid,
                    event_type="interest_started",
                    worker_name=worker_name,
                    batch_id=batch_id,
                    details=payload,
                    timestamp=timestamp,
                )
            )

    @staticmethod
    def _legacy_worker_id(owner_id: str, session_id: str | None = None) -> str:
        owner = owner_id.strip()
        if not owner:
            raise ValueError("owner_id must not be empty")
        if session_id is None:
            return owner
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")
        return f"{owner}:{session}"

    @staticmethod
    def _split_legacy_worker_id(worker_id: str) -> tuple[str, str | None]:
        if ":" not in worker_id:
            return worker_id, None
        owner_id, session_id = worker_id.split(":", 1)
        return owner_id, session_id or None

    async def create_interest_manager(
        self,
        *,
        worker_id: str,
        priority: int,
        wants_creation: bool = False,
        wants_content: bool = False,
        wants_inlinks: bool = False,
    ) -> InterestManager:
        await self.cache.initialize()
        return await InterestManager.create(
            self,
            worker_id=worker_id,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
        )

    async def create_interest_session(
        self,
        *,
        worker_id: str,
        priority: int,
        wants_creation: bool = False,
        wants_content: bool = False,
        wants_inlinks: bool = False,
    ) -> tuple[InterestManager, InterestSession]:
        manager = await self.create_interest_manager(
            worker_id=worker_id,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
        )
        return manager, manager.create_session()

    async def upsert_interest_rows(
        self,
        *,
        worker_id: str,
        qids: list[int],
        priority: int,
        wants_creation: bool,
        wants_content: bool,
        wants_inlinks: bool,
    ) -> int:
        return await inlinks_db_write.upsert_pubsub_interest_rows(
            self.cache,
            worker_id=worker_id,
            qids=qids,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
        )

    async def upsert_pubsub_interest_rows(
        self,
        *,
        worker_id: str,
        qids: list[int],
        priority: int,
        wants_creation: bool,
        wants_content: bool,
        wants_inlinks: bool,
    ) -> int:
        return await self.upsert_interest_rows(
            worker_id=worker_id,
            qids=qids,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
        )

    async def delete_interest_rows(
        self,
        *,
        worker_id: str,
        qids: list[int],
    ) -> int:
        return await inlinks_db_write.delete_pubsub_interest_rows(
            self.cache,
            worker_id=worker_id,
            qids=qids,
        )

    async def delete_pubsub_interest_rows(
        self,
        *,
        worker_id: str,
        qids: list[int],
    ) -> int:
        return await self.delete_interest_rows(worker_id=worker_id, qids=qids)

    async def delete_pubsub_interest_for_worker(self, *, worker_id: str) -> int:
        await self.cache.initialize()

        worker = worker_id.strip()
        if not worker:
            raise ValueError("worker_id must not be empty")

        started = time.perf_counter()
        deleted = 0
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                while True:
                    cursor = await db.execute(
                        """
                        DELETE FROM interest
                        WHERE worker_id = ?
                        LIMIT 5000
                        """,
                        (worker,),
                    )
                    batch_deleted = max(0, int(cursor.rowcount))
                    deleted += batch_deleted
                    if batch_deleted == 0:
                        break
        self.cache._warn_slow_write("delete_pubsub_interest_for_worker", started, row_count=deleted)
        return deleted

    async def list_pubsub_interest_qids_for_worker(self, *, worker_id: str) -> list[str]:
        await self.cache.initialize()

        worker = worker_id.strip()
        if not worker:
            raise ValueError("worker_id must not be empty")

        async with self.cache._connect() as db:
            cursor = await db.execute(
                """
                SELECT qid
                FROM interest
                WHERE worker_id = ?
                ORDER BY qid ASC
                """,
                (worker,),
            )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def create_pubsub_lease(
        self,
        *,
        owner_id: str,
        lease_id: str,
        ttl_seconds: int,
        priority: int = 10,
        wants_creation: bool = False,
        wants_content: bool,
        wants_inlinks: bool,
        qids: list[str | int] | None = None,
    ) -> int:
        return await self.create_pubsub_session(
            owner_id=owner_id,
            session_id=lease_id,
            ttl_seconds=ttl_seconds,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
            qids=qids,
        )

    async def add_pubsub_lease_qids(
        self,
        *,
        owner_id: str,
        lease_id: str,
        qids: list[str | int],
        priority: int = 10,
        wants_creation: bool | None = None,
        wants_content: bool | None = None,
        wants_inlinks: bool | None = None,
    ) -> int:
        return await self.add_pubsub_session_qids(
            owner_id=owner_id,
            session_id=lease_id,
            qids=qids,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
        )

    async def refresh_pubsub_lease(
        self,
        *,
        owner_id: str,
        lease_id: str,
        ttl_seconds: int,
    ) -> int:
        return await self.refresh_pubsub_session(
            owner_id=owner_id,
            session_id=lease_id,
            ttl_seconds=ttl_seconds,
        )

    async def delete_pubsub_lease(self, *, owner_id: str, lease_id: str) -> int:
        return await self.delete_pubsub_session(owner_id=owner_id, session_id=lease_id)

    async def list_pubsub_events_for_lease(
        self,
        *,
        owner_id: str,
        lease_id: str,
        after_event_id: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, int | str | None]]:
        return await self.list_pubsub_events_for_session(
            owner_id=owner_id,
            session_id=lease_id,
            after_event_id=after_event_id,
            limit=limit,
        )

    async def list_pubsub_lease_ids(self, owner_id: str, limit: int | None = None) -> list[str]:
        return await self.list_pubsub_session_ids(owner_id=owner_id, limit=limit)

    async def list_pubsub_lease_qids(self, *, owner_id: str, lease_id: str) -> list[str]:
        return await self.list_pubsub_session_qids(owner_id=owner_id, session_id=lease_id)

    async def purge_expired_interest(self, *, now: int | float | None = None) -> int:
        del now
        return 0

    async def delete_interest_for_owner(self, *, owner_id: str) -> int:
        return await inlinks_db_write.delete_interest_for_owner(self.cache, owner_id=owner_id)

    async def create_pubsub_session(
        self,
        *,
        owner_id: str,
        session_id: str,
        ttl_seconds: int,
        priority: int = 10,
        wants_creation: bool = False,
        wants_content: bool,
        wants_inlinks: bool,
        qids: list[str | int] | None = None,
    ) -> int:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")

        session_priority = self.cache._as_uint32(priority, "priority")
        worker_id = self._legacy_worker_id(owner, session)
        qid_nums = [
            self.cache._parse_qid(qid)
            for qid in qids or []
        ]
        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute(
                    "DELETE FROM interest WHERE worker_id = ? OR worker_id LIKE ?",
                    (worker_id, f"{worker_id}:%"),
                )
                if qid_nums:
                    await db.executemany(
                        """
                        INSERT INTO interest (
                            worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            priority = VALUES(priority),
                            wants_creation = VALUES(wants_creation),
                            wants_content = VALUES(wants_content),
                            wants_inlinks = VALUES(wants_inlinks)
                        """,
                        [
                            (
                                worker_id,
                                qid_num,
                                session_priority,
                                1 if wants_creation else 0,
                                1 if wants_content else 0,
                                1 if wants_inlinks else 0,
                            )
                            for qid_num in sorted(set(qid_nums))
                        ],
                        )
                await db.commit()
        self.cache._warn_slow_write(
            "create_pubsub_session", started, row_count=len(set(qid_nums)))
        normalized_qids = sorted(set(qid_nums))
        await self._record_interest_started(
            worker_name="pubsub",
            qids=normalized_qids,
            interest_type="pubsub",
            details={
                "owner_id": owner,
                "session_id": session,
                "worker_id": worker_id,
                "priority": session_priority,
                "wants_creation": wants_creation,
                "wants_content": wants_content,
                "wants_inlinks": wants_inlinks,
            },
        )
        return len(normalized_qids)

    async def add_pubsub_session_qids(
        self,
        *,
        owner_id: str,
        session_id: str,
        qids: list[str | int],
        priority: int = 10,
        wants_creation: bool | None = None,
        wants_content: bool | None = None,
        wants_inlinks: bool | None = None,
    ) -> int:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")

        qid_nums = [self.cache._parse_qid(qid) for qid in qids]
        worker_id = self._legacy_worker_id(owner, session)
        session_priority = self.cache._as_uint32(priority, "priority")
        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                rows = [
                    (
                        worker_id,
                        qid_num,
                        session_priority,
                        1 if wants_creation else 0,
                        1 if wants_content else 0,
                        1 if wants_inlinks else 0,
                    )
                    for qid_num in sorted(set(qid_nums))
                ]
                if rows:
                    if self.cache._backend_name == "mariadb":
                        await db.executemany(
                            """
                            INSERT INTO interest (
                                worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON DUPLICATE KEY UPDATE
                                priority = VALUES(priority),
                                wants_creation = VALUES(wants_creation),
                                wants_content = VALUES(wants_content),
                                wants_inlinks = VALUES(wants_inlinks)
                            """,
                            rows,
                        )
                    else:
                        await db.executemany(
                            """
                            INSERT INTO interest (
                                worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(worker_id, qid) DO UPDATE SET
                                priority = excluded.priority,
                                wants_creation = excluded.wants_creation,
                                wants_content = excluded.wants_content,
                                wants_inlinks = excluded.wants_inlinks
                            """,
                            rows,
                        )
                await db.commit()
        self.cache._warn_slow_write(
            "add_pubsub_session_qids", started, row_count=len(set(qid_nums)))
        normalized_qids = sorted(set(qid_nums))
        await self._record_interest_started(
            worker_name="pubsub",
            qids=normalized_qids,
            interest_type="pubsub",
            details={
                "owner_id": owner,
                "session_id": session,
                "worker_id": worker_id,
                "priority": session_priority,
                "wants_creation": wants_creation,
                "wants_content": wants_content,
                "wants_inlinks": wants_inlinks,
            },
        )
        return len(normalized_qids)

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
        del ttl_seconds
        worker_id = self._legacy_worker_id(owner, session)
        async with self.cache._connect() as db:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM interest WHERE worker_id = ?",
                (worker_id,),
            )
            row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    async def delete_pubsub_session(self, *, owner_id: str, session_id: str) -> int:
        owner = self.cache._normalize_owner_id(owner_id)
        session = session_id.strip()
        if not session:
            raise ValueError("session_id must not be empty")
        worker_id = self._legacy_worker_id(owner, session)
        return await self.delete_pubsub_interest_for_worker(worker_id=worker_id)

    async def list_pubsub_events_for_session(
        self,
        *,
        owner_id: str,
        session_id: str,
        after_event_id: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, int | str | None]]:
        await self.cache.initialize()

        worker_id = self._legacy_worker_id(self.cache._normalize_owner_id(owner_id), session_id)

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    f"""
                    SELECT
                        s.qid,
                        ce.content_last_revid,
                        ce.redirect_target,
                        COALESCE(ce.has_sitelinks_count, 0) AS has_sitelinks_count,
                        COALESCE(ce.has_claims_count, 0) AS has_claims_count,
                        COALESCE(ce.deleted, 0) AS deleted,
                        COALESCE(ce.n1, 0) AS n1,
                        COALESCE(ce.n2a, 0) AS n2a,
                        COALESCE(ce.n2b, 0) AS n2b,
                        COALESCE(ic.inlinks_count, 0) AS inlinks_count,
                        COALESCE(ic.n3_inlinks, 4) AS n3_inlinks,
                        CASE WHEN ou.qid IS NULL THEN 0 ELSE {int(external_usage_level(NotabilityCriterion.N3_OSM))} END AS n3_osm,
                        CASE WHEN su.qid IS NULL THEN 0 ELSE {int(external_usage_level(NotabilityCriterion.N3_SDC))} END AS n3_sdc,
                        CASE WHEN ws.qid IS NULL THEN 0 ELSE {int(external_usage_level(NotabilityCriterion.N3_WIKISUB))} END AS n3_wikisub
                    FROM interest s
                    LEFT JOIN content_evaluation ce
                      ON ce.qid = s.qid
                    LEFT JOIN inlinks_cache ic
                      ON ic.qid = s.qid
                    LEFT JOIN osm_usage ou
                      ON ou.qid = s.qid
                    LEFT JOIN sdc_usage su
                      ON su.qid = s.qid
                    LEFT JOIN wiki_subscribers ws
                      ON ws.qid = s.qid
                    WHERE s.worker_id = ?
                      AND s.qid != 0
                    ORDER BY s.qid ASC
                    """,
                    (worker_id,),
                )
            else:
                cursor = await db.execute(
                    f"""
                    SELECT
                        s.qid,
                        ce.content_last_revid,
                        ce.redirect_target,
                        COALESCE(ce.has_sitelinks_count, 0) AS has_sitelinks_count,
                        COALESCE(ce.has_claims_count, 0) AS has_claims_count,
                        COALESCE(ce.deleted, 0) AS deleted,
                        COALESCE(ce.n1, 0) AS n1,
                        COALESCE(ce.n2a, 0) AS n2a,
                        COALESCE(ce.n2b, 0) AS n2b,
                        COALESCE(ic.inlinks_count, 0) AS inlinks_count,
                        COALESCE(ic.n3_inlinks, 4) AS n3_inlinks,
                        CASE WHEN ou.qid IS NULL THEN 0 ELSE {int(external_usage_level(NotabilityCriterion.N3_OSM))} END AS n3_osm,
                        CASE WHEN su.qid IS NULL THEN 0 ELSE {int(external_usage_level(NotabilityCriterion.N3_SDC))} END AS n3_sdc,
                        CASE WHEN ws.qid IS NULL THEN 0 ELSE {int(external_usage_level(NotabilityCriterion.N3_WIKISUB))} END AS n3_wikisub
                    FROM interest s
                    LEFT JOIN content_evaluation ce
                      ON ce.qid = s.qid
                    LEFT JOIN inlinks_cache ic
                      ON ic.qid = s.qid
                    LEFT JOIN osm_usage ou
                      ON ou.qid = s.qid
                    LEFT JOIN sdc_usage su
                      ON su.qid = s.qid
                    LEFT JOIN wiki_subscribers ws
                      ON ws.qid = s.qid
                    WHERE s.worker_id = ?
                      AND s.qid != 0
                    ORDER BY s.qid ASC
                    LIMIT ?
                    """,
                    (worker_id, limit),
                )
            rows = await cursor.fetchall()

        return [
            {
                "qid": int(row[0]),
                "event_type": "summary_change",
                "content_last_revid": None if row[1] is None else int(row[1]),
                "redirect_target": None if row[2] is None else int(row[2]),
                "has_sitelinks_count": None if row[3] is None else int(row[3]),
                "has_claims_count": None if row[4] is None else int(row[4]),
                "deleted": None if row[5] is None else int(row[5]),
                "n1": None if row[6] is None else int(row[6]),
                "n2a": None if row[7] is None else int(row[7]),
                "n2b": None if row[8] is None else int(row[8]),
                "inlinks_count": int(row[9]),
                "n3_inlinks": int(row[10]),
                "n3_osm": int(row[11]),
                "n3_sdc": int(row[12]),
                "n3_wikisub": int(row[13]),
            }
            for row in rows
        ]

    async def list_pubsub_content_candidates(
        self,
        limit: int | None = None,
        *,
        exclude_qids: Sequence[str | int] | None = None,
    ) -> list[str]:
        return await content_db_read.list_pubsub_content_candidates(
            self.cache,
            limit,
            exclude_qids=exclude_qids,
        )

    async def count_pubsub_content_candidates(self) -> int:
        return await content_db_read.count_pubsub_content_candidates(self.cache)

    async def count_pubsub_content_candidates_by_staleness(self) -> dict[str, int]:
        return await content_db_read.count_pubsub_content_candidates_by_staleness(self.cache)

    async def count_pubsub_content_candidate_staleness_for_qids(self, qids: Sequence[str | int]) -> dict[str, int]:
        return await content_db_read.count_pubsub_content_candidate_staleness_for_qids(self.cache, qids)

    async def list_pubsub_content_candidate_reasons(self, qids: Sequence[str | int]) -> dict[str, str]:
        return await content_db_read.list_pubsub_content_candidate_reasons(self.cache, qids)

    async def list_pubsub_inlinks_targets_with_state(
        self,
        limit: int | None = None,
    ) -> list[tuple[str, int, int | None, int | None, int | None]]:
        return await inlinks_db_read.list_pubsub_inlinks_targets_with_state(self.cache, limit=limit)

    async def list_pubsub_inlinks_targets(self, limit: int | None = None) -> list[str]:
        return await inlinks_db_read.list_pubsub_inlinks_targets(self.cache, limit=limit)

    async def count_pubsub_inlinks_targets(self) -> int:
        return await inlinks_db_read.count_pubsub_inlinks_targets(self.cache)

    async def has_pubsub_inlinks_interest(self, qid: str | int) -> bool:
        return await inlinks_db_read.has_pubsub_inlinks_interest(self.cache, qid)

    async def list_pubsub_session_ids(self, owner_id: str, limit: int | None = None) -> list[str]:
        await self.cache.initialize()

        owner = self.cache._normalize_owner_id(owner_id)
        prefix = f"{owner}:"
        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                """
                    SELECT DISTINCT worker_id
                    FROM interest
                    WHERE (worker_id = ? OR worker_id LIKE ?)
                      AND qid != 0
                    ORDER BY worker_id ASC
                    """,
                    (owner, f"{prefix}%"),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT DISTINCT worker_id
                    FROM interest
                    WHERE (worker_id = ? OR worker_id LIKE ?)
                      AND qid != 0
                    ORDER BY worker_id ASC
                    LIMIT ?
                    """,
                    (owner, f"{prefix}%", limit),
                )
            rows = await cursor.fetchall()

        sessions: list[str] = []
        for row in rows:
            worker_id = str(row[0])
            if worker_id == owner:
                continue
            if not worker_id.startswith(prefix):
                continue
            sessions.append(worker_id.removeprefix(prefix))
        return sessions[:limit] if limit is not None else sessions

    async def list_pubsub_session_qids(self, *, owner_id: str, session_id: str) -> list[str]:
        await self.cache.initialize()

        worker_id = self._legacy_worker_id(self.cache._normalize_owner_id(owner_id), session_id)

        async with self.cache._connect() as db:
            cursor = await db.execute(
                """
                SELECT qid
                FROM interest
                WHERE worker_id = ?
                  AND qid != 0
                ORDER BY qid ASC
                """,
                (worker_id,),
            )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def list_pubsub_creation_targets(self, limit: int | None = None) -> list[str]:
        await self.cache.initialize()

        query = """
            SELECT
                s.qid,
                COALESCE(SUM(COALESCE(s.priority, 10)), 0) AS subscriber_priority
            FROM interest s
            LEFT JOIN recent_changes_cache rc
              ON rc.qid = s.qid
            WHERE s.qid != 0
              AND s.wants_creation = 1
              AND (rc.qid IS NULL OR rc.creation_time IS NULL OR rc.creator_actor_id IS NULL)
            GROUP BY s.qid
            ORDER BY subscriber_priority DESC, s.qid ASC
        """

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(query)
            else:
                cursor = await db.execute(f"{query}\nLIMIT ?", (limit,))
            rows = await cursor.fetchall()
        return [f"Q{int(row[0])}" for row in rows]

    async def count_pubsub_creation_targets(self) -> int:
        await self.cache.initialize()

        query = """
            SELECT COUNT(*)
            FROM (
                SELECT s.qid
                FROM interest s
                LEFT JOIN recent_changes_cache rc
                  ON rc.qid = s.qid
                WHERE s.qid != 0
                  AND s.wants_creation = 1
                  AND (rc.qid IS NULL OR rc.creation_time IS NULL OR rc.creator_actor_id IS NULL)
                GROUP BY s.qid
            ) creation_targets
        """

        async with self.cache._connect() as db:
            cursor = await db.execute(query)
            row = await cursor.fetchone()

        return int(row[0]) if row and row[0] is not None else 0

    async def pubsub_stats(self) -> dict[str, Any]:
        await self.cache.initialize()

        async with self.cache._connect() as db:
            total_cursor = await db.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT worker_id),
                    COUNT(DISTINCT CASE WHEN qid != 0 THEN qid END),
                    MIN(CASE WHEN qid != 0 THEN NULL END),
                    MAX(CASE WHEN qid != 0 THEN NULL END),
                    0,
                    0,
                    0,
                    0
                FROM interest
                """,
                (),
            )
            total_row = await total_cursor.fetchone()

            owner_cursor = await db.execute(
                """
                SELECT
                    CASE
                        WHEN INSTR(worker_id, ':') > 0 THEN SUBSTR(worker_id, 1, INSTR(worker_id, ':') - 1)
                        ELSE worker_id
                    END AS owner_id,
                    COUNT(*)
                FROM interest
                GROUP BY owner_id
                ORDER BY owner_id
                """
            )
            owner_rows = await owner_cursor.fetchall()

        return {
            "entries": int(total_row[0]) if total_row and total_row[0] is not None else 0,
            "distinct_sessions": int(total_row[1]) if total_row and total_row[1] is not None else 0,
            "distinct_leases": int(total_row[1]) if total_row and total_row[1] is not None else 0,
            "distinct_qids": int(total_row[2]) if total_row and total_row[2] is not None else 0,
            "oldest_expires_at": None,
            "newest_expires_at": None,
            "expiring": {
                "now": 0,
                "next_60_seconds": 0,
                "next_300_seconds": 0,
                "next_3600_seconds": 0,
            },
            "by_worker": {str(row[0]): int(row[1]) for row in owner_rows},
            "flags": {
                "wants_creation": {"yes": 0, "no": 0},
                "wants_content": {"yes": 0, "no": 0},
                "wants_inlinks": {"yes": 0, "no": 0},
            },
        }

    async def list_pubsub_interest_items(self, limit: int | None = None) -> list[dict[str, Any]]:
        await self.cache.initialize()

        async with self.cache._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT
                        qid,
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
                    WHERE qid != 0
                      AND (
                        wants_creation = 1
                        OR wants_content = 1
                        OR wants_inlinks = 1
                      )
                    GROUP BY qid, worker_id
                    ORDER BY qid ASC, worker_id ASC
                    """,
                    (),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT
                        qid,
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
                    WHERE qid != 0
                      AND (
                        wants_creation = 1
                        OR wants_content = 1
                        OR wants_inlinks = 1
                      )
                    GROUP BY qid, worker_id
                    ORDER BY qid ASC, worker_id ASC
                    LIMIT ?
                    """,
                    (limit,),
                )
            rows = await cursor.fetchall()

        grouped: dict[int, dict[str, Any]] = {}
        for row in rows:
            qid = int(row[0])
            worker_id = str(row[1])
            item = grouped.setdefault(
                qid,
                {
                    "qid": f"Q{qid}",
                    "session_rows": 0,
                    "lease_rows": 0,
                    "total_priority": 0,
                    "wants_creation": False,
                    "wants_content": False,
                    "wants_inlinks": False,
                    "owner_count": 0,
                    "workers": [],
                },
            )
            session_rows = int(row[2]) if row[2] is not None else 0
            total_priority = int(row[3]) if row[3] is not None else 0
            wants_creation_rows = int(row[4]) if row[4] is not None else 0
            wants_content_rows = int(row[5]) if row[5] is not None else 0
            wants_inlinks_rows = int(row[6]) if row[6] is not None else 0
            wants_creation = bool(row[7])
            wants_content = bool(row[8])
            wants_inlinks = bool(row[9])

            item["session_rows"] += session_rows
            item["lease_rows"] += session_rows
            item["total_priority"] += total_priority
            item["wants_creation"] = item["wants_creation"] or wants_creation
            item["wants_content"] = item["wants_content"] or wants_content
            item["wants_inlinks"] = item["wants_inlinks"] or wants_inlinks
            item["workers"].append(
                {
                    "worker_id": worker_id,
                    "session_rows": session_rows,
                    "total_priority": total_priority,
                    "wants_creation": wants_creation,
                    "wants_content": wants_content,
                    "wants_inlinks": wants_inlinks,
                    "wants_creation_rows": wants_creation_rows,
                    "wants_content_rows": wants_content_rows,
                    "wants_inlinks_rows": wants_inlinks_rows,
                }
            )

        for item in grouped.values():
            item["owner_count"] = len(item["workers"])

        result = list(grouped.values())
        result.sort(
            key=lambda item: (-int(item["total_priority"]), str(item["qid"])))
        return result

    upsert_pubsub_interest_rows = upsert_interest_rows
    delete_pubsub_interest_rows = delete_interest_rows
    delete_interest_for_worker = delete_pubsub_interest_for_worker
    list_interest_qids_for_worker = list_pubsub_interest_qids_for_worker
    create_interest_lease = create_pubsub_lease
    add_interest_lease_qids = add_pubsub_lease_qids
    refresh_interest_lease = refresh_pubsub_lease
    delete_interest_lease = delete_pubsub_lease
    list_interest_events_for_lease = list_pubsub_events_for_lease
    list_interest_lease_ids = list_pubsub_lease_ids
    list_interest_lease_qids = list_pubsub_lease_qids
    list_interest_content_candidates = list_pubsub_content_candidates
    count_interest_content_candidates = count_pubsub_content_candidates
    count_interest_content_candidates_by_staleness = count_pubsub_content_candidates_by_staleness
    count_interest_content_candidate_staleness_for_qids = count_pubsub_content_candidate_staleness_for_qids
    list_interest_content_candidate_reasons = list_pubsub_content_candidate_reasons
    count_interest_inlinks_targets = count_pubsub_inlinks_targets
    list_interest_session_ids = list_pubsub_session_ids
    list_interest_session_qids = list_pubsub_session_qids
    has_interest_inlinks_interest = has_pubsub_inlinks_interest
    list_interest_creation_targets = list_pubsub_creation_targets
    count_interest_creation_targets = count_pubsub_creation_targets
    interest_stats = pubsub_stats
    list_interest_items = list_pubsub_interest_items

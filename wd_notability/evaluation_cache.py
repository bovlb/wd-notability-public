from __future__ import annotations

import asyncio
import configparser
import logging
import os
from collections.abc import Sequence
from typing import Any
import time
from pathlib import Path

import aiosqlite
import pymysql

from wd_notability.events import EventLogStore
from wd_notability.models import (
    QID,
    EvaluationResult,
    NotabilityCriterion,
    NotabilityLevel,
)
from wd_notability.pubsub import PubSubStore
from wd_notability import summary as summary_bits

UINT32_MAX = 2**32 - 1
DEFAULT_PROCESSING_TIMEOUT_SECONDS = 300
DEFAULT_SLOW_WRITE_WARNING_SECONDS = float(os.environ.get("WD_NOTABILITY_SLOW_WRITE_WARNING_SECONDS", "1.0"))
DEFAULT_WRITE_CHUNK_SIZE = 500

logger = logging.getLogger(__name__)


class _MariaDBCursorAdapter:
    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    async def fetchall(self):
        return await asyncio.to_thread(self._cursor.fetchall)

    async def fetchone(self):
        return await asyncio.to_thread(self._cursor.fetchone)


class _MariaDBConnectionAdapter:
    def __init__(self, connection):
        self._connection = connection

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.to_thread(self._connection.close)

    @staticmethod
    def _translate_sql(sql: str) -> str:
        sql = sql.replace("?", "%s")
        if sql.strip().upper() == "BEGIN IMMEDIATE":
            return "START TRANSACTION"
        if "INSERT OR IGNORE" in sql.upper():
            sql = sql.replace("INSERT OR IGNORE", "INSERT IGNORE")
        return sql

    async def execute(self, sql: str, params: Sequence[Any] | None = None):
        translated = self._translate_sql(sql)

        def _run():
            cursor = self._connection.cursor()
            cursor.execute(translated, params or ())
            return cursor

        cursor = await asyncio.to_thread(_run)
        return _MariaDBCursorAdapter(cursor)

    async def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any]]):
        translated = self._translate_sql(sql)

        def _run():
            cursor = self._connection.cursor()
            cursor.executemany(translated, seq_of_params)
            return cursor

        cursor = await asyncio.to_thread(_run)
        return _MariaDBCursorAdapter(cursor)

    async def commit(self) -> None:
        await asyncio.to_thread(self._connection.commit)


class EvaluationCache:
    """Cache for evaluation summaries, backed by SQLite locally or MariaDB on Toolforge."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        processing_timeout_seconds: int | None = None,
    ):
        backend_name = os.getenv("WD_NOTABILITY_CACHE_BACKEND", "sqlite").strip().lower()
        self._backend_name = "mariadb" if backend_name in {"mariadb", "toolforge"} else "sqlite"
        default_path = Path(__file__).resolve().parent / "data" / "evaluation_cache.sqlite3"
        self.db_path = Path(db_path) if db_path is not None else default_path
        self.database = os.getenv("WD_NOTABILITY_EVAL_DATABASE", os.getenv("WD_NOTABILITY_CACHE_DATABASE", "wd_notability"))
        self.host = os.getenv("WD_NOTABILITY_EVAL_HOST", os.getenv("WD_NOTABILITY_CACHE_HOST", "tools.db.svc.wikimedia.cloud"))
        self.defaults_file = Path(
            os.getenv("WD_NOTABILITY_EVAL_DEFAULTS_FILE", os.getenv("WD_NOTABILITY_CACHE_DEFAULTS_FILE", str(Path.home() / "replica.my.cnf")))
        )
        timeout_from_env = int(os.environ.get("WD_NOTABILITY_TASK_PROCESSING_TIMEOUT_SECONDS", DEFAULT_PROCESSING_TIMEOUT_SECONDS))
        timeout = timeout_from_env if processing_timeout_seconds is None else processing_timeout_seconds
        self.processing_timeout_seconds = max(1, int(timeout))
        self._initialized = False
        self._write_lock = asyncio.Lock()
        self.events = EventLogStore(self)
        self.pubsub = PubSubStore(self)

    async def initialize(self) -> None:
        if self._initialized:
            return

        if self._backend_name == "sqlite":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiosqlite.connect(self.db_path) as db:
                # WAL mode allows concurrent reads while a write is in progress,
                # preventing the subscribe endpoint from blocking on worker writes.
                await db.execute("PRAGMA journal_mode=WAL")
                # Give contending connections up to 10 seconds to acquire the lock
                # instead of failing immediately.
                await db.execute("PRAGMA busy_timeout=10000")
                await db.commit()
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS evaluation_cache (
                        qid INTEGER PRIMARY KEY NOT NULL CHECK(qid >= 0 AND qid <= 4294967295),
                        summary INTEGER NOT NULL CHECK(summary >= 0 AND summary <= 4294967295),
                        entitydata_last_revid INTEGER CHECK(entitydata_last_revid IS NULL OR (entitydata_last_revid >= 0 AND entitydata_last_revid <= 4294967295)),
                        recent_changes_last_revid INTEGER CHECK(recent_changes_last_revid IS NULL OR (recent_changes_last_revid >= 0 AND recent_changes_last_revid <= 4294967295))
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pubsub_sessions (
                        session_id TEXT NOT NULL,
                        owner_id TEXT NOT NULL CHECK(owner_id IN ('gadget', 'report', 'inlinks')),
                        qid INTEGER NOT NULL CHECK(qid >= 0 AND qid <= 4294967295),
                        expires_at INTEGER NOT NULL CHECK(expires_at >= 0 AND expires_at <= 4294967295),
                        priority INTEGER NOT NULL DEFAULT 10 CHECK(priority >= 0 AND priority <= 1000),
                        wants_entitydata INTEGER NOT NULL CHECK(wants_entitydata IN (0, 1)),
                        wants_inlinks INTEGER NOT NULL CHECK(wants_inlinks IN (0, 1)),
                        wants_sync INTEGER NOT NULL CHECK(wants_sync IN (0, 1)),
                        PRIMARY KEY (session_id, owner_id, qid)
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pubsub_events (
                        event_id INTEGER PRIMARY KEY,
                        timestamp INTEGER NOT NULL CHECK(timestamp >= 0 AND timestamp <= 4294967295),
                        qid INTEGER NOT NULL CHECK(qid >= 0 AND qid <= 4294967295),
                        event_type TEXT NOT NULL,
                        summary INTEGER NOT NULL CHECK(summary >= 0 AND summary <= 4294967295),
                        mask INTEGER NOT NULL CHECK(mask >= 0 AND mask <= 4294967295)
                    )
                    """
                )
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_session_id ON pubsub_sessions(session_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_qid ON pubsub_sessions(qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_expires_at ON pubsub_sessions(expires_at)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_wants_sync_qid ON pubsub_sessions(wants_sync, qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_events_timestamp ON pubsub_events(timestamp)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_events_qid ON pubsub_events(qid)")
                cursor = await db.execute("PRAGMA table_info(evaluation_cache)")
                columns = {str(row[1]) for row in await cursor.fetchall()}
                if "entitydata_last_revid" not in columns:
                    await db.execute(
                        "ALTER TABLE evaluation_cache ADD COLUMN entitydata_last_revid INTEGER CHECK(entitydata_last_revid IS NULL OR (entitydata_last_revid >= 0 AND entitydata_last_revid <= 4294967295))"
                    )
                if "recent_changes_last_revid" not in columns:
                    await db.execute(
                        "ALTER TABLE evaluation_cache ADD COLUMN recent_changes_last_revid INTEGER CHECK(recent_changes_last_revid IS NULL OR (recent_changes_last_revid >= 0 AND recent_changes_last_revid <= 4294967295))"
                    )
                if "last_updated" in columns:
                    try:
                        await db.execute("ALTER TABLE evaluation_cache DROP COLUMN last_updated")
                    except aiosqlite.OperationalError:
                        pass
                cursor = await db.execute("PRAGMA table_info(pubsub_sessions)")
                session_columns = {str(row[1]) for row in await cursor.fetchall()}
                if "priority" not in session_columns:
                    await db.execute(
                        "ALTER TABLE pubsub_sessions ADD COLUMN priority INTEGER NOT NULL DEFAULT 10 CHECK(priority >= 0 AND priority <= 1000)"
                    )
                cursor = await db.execute("PRAGMA table_info(pubsub_events)")
                event_columns = {str(row[1]) for row in await cursor.fetchall()}
                desired_event_columns = {"event_id", "timestamp", "qid", "event_type", "summary", "mask"}
                if event_columns and event_columns != desired_event_columns:
                    await db.execute("DROP TABLE IF EXISTS pubsub_events")
                    await db.execute(
                        """
                        CREATE TABLE pubsub_events (
                            event_id INTEGER PRIMARY KEY,
                            timestamp INTEGER NOT NULL CHECK(timestamp >= 0 AND timestamp <= 4294967295),
                            qid INTEGER NOT NULL CHECK(qid >= 0 AND qid <= 4294967295),
                            event_type TEXT NOT NULL,
                            summary INTEGER NOT NULL CHECK(summary >= 0 AND summary <= 4294967295),
                            mask INTEGER NOT NULL CHECK(mask >= 0 AND mask <= 4294967295)
                        )
                        """
                    )
                    await db.execute("CREATE INDEX IF NOT EXISTS pubsub_events_timestamp ON pubsub_events(timestamp)")
                    await db.execute("CREATE INDEX IF NOT EXISTS pubsub_events_qid ON pubsub_events(qid)")
                await db.commit()
        else:
            async with self._connect() as db:
                cursor = await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS evaluation_cache (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        summary BIGINT UNSIGNED NOT NULL,
                        entitydata_last_revid BIGINT UNSIGNED NULL,
                        recent_changes_last_revid BIGINT UNSIGNED NULL
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pubsub_sessions (
                        session_id VARCHAR(255) NOT NULL,
                        owner_id VARCHAR(32) NOT NULL,
                        qid BIGINT UNSIGNED NOT NULL,
                        expires_at BIGINT UNSIGNED NOT NULL,
                        priority INT NOT NULL DEFAULT 10,
                        wants_entitydata TINYINT(1) NOT NULL,
                        wants_inlinks TINYINT(1) NOT NULL,
                        wants_sync TINYINT(1) NOT NULL,
                        PRIMARY KEY (session_id, owner_id, qid)
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pubsub_events (
                        event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        timestamp BIGINT UNSIGNED NOT NULL,
                        qid BIGINT UNSIGNED NOT NULL,
                        event_type VARCHAR(64) NOT NULL,
                        summary BIGINT UNSIGNED NOT NULL,
                        mask BIGINT UNSIGNED NOT NULL
                    )
                    """
                )
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_session_id ON pubsub_sessions(session_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_qid ON pubsub_sessions(qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_expires_at ON pubsub_sessions(expires_at)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_wants_sync_qid ON pubsub_sessions(wants_sync, qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_events_timestamp ON pubsub_events(timestamp)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_events_qid ON pubsub_events(qid)")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'entitydata_last_revid'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN entitydata_last_revid BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'recent_changes_last_revid'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN recent_changes_last_revid BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM pubsub_sessions LIKE 'priority'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE pubsub_sessions ADD COLUMN priority INT NOT NULL DEFAULT 10")
                await db.commit()

        self._initialized = True

    def _connect(self):
        """Open a connection with a busy timeout so reads never hang on writer locks."""
        if self._backend_name == "sqlite":
            return aiosqlite.connect(self.db_path, timeout=10)

        if not self.defaults_file.exists():
            raise RuntimeError(f"Toolforge credential file is missing: {self.defaults_file}")
        config = configparser.ConfigParser(interpolation=None)
        config.read(self.defaults_file)
        client = config["client"] if "client" in config else {}
        connection = pymysql.connect(
            user=client.get("user"),
            password=client.get("password"),
            host=self.host,
            port=3306,
            database=self.database,
            charset="utf8mb4",
            autocommit=False,
        )
        return _MariaDBConnectionAdapter(connection)

    def _warn_slow_write(self, operation: str, started: float, *, row_count: int | None = None) -> None:
        elapsed = time.perf_counter() - started
        if elapsed < DEFAULT_SLOW_WRITE_WARNING_SECONDS:
            return

        extra = f", rows={row_count}" if row_count is not None else ""
        logger.warning("Slow cache write: %s took %.3fs%s", operation, elapsed, extra)

    def _write_guard(self):
        return self._write_lock

    @staticmethod
    def _chunked(values: Sequence[object], size: int = DEFAULT_WRITE_CHUNK_SIZE) -> list[list[object]]:
        if size < 1:
            raise ValueError("size must be at least 1")
        return [list(values[index : index + size]) for index in range(0, len(values), size)]

    @staticmethod
    def _normalize_owner_id(owner_id: str) -> str:
        owner = owner_id.strip().lower()
        if owner not in {"gadget", "report", "inlinks"}:
            raise ValueError("owner_id must be gadget, report, or inlinks")
        return owner

    @staticmethod
    def _summary_mask() -> int:
        return (
            summary_bits.REDIRECT
            | summary_bits.HAS_SITELINKS
            | summary_bits.HAS_CLAIMS
            | summary_bits.DELETED
            | summary_bits.mask(NotabilityCriterion.N1)
            | summary_bits.mask(NotabilityCriterion.N2a)
            | summary_bits.mask(NotabilityCriterion.N2b)
            | summary_bits.mask(NotabilityCriterion.N3_INLINKS)
            | summary_bits.mask(NotabilityCriterion.N3_OSM)
            | summary_bits.mask(NotabilityCriterion.N3_WIKISUB)
            | summary_bits.mask(NotabilityCriterion.N3_SDC)
        )

    @staticmethod
    def _entitydata_mask() -> int:
        return (
            summary_bits.REDIRECT
            | summary_bits.HAS_SITELINKS
            | summary_bits.HAS_CLAIMS
            | summary_bits.DELETED
            | summary_bits.mask(NotabilityCriterion.N1)
            | summary_bits.mask(NotabilityCriterion.N2a)
            | summary_bits.mask(NotabilityCriterion.N2b)
        )

    @staticmethod
    def _cache_sync_mask() -> int:
        return (
            summary_bits.mask(NotabilityCriterion.N3_OSM)
            | summary_bits.mask(NotabilityCriterion.N3_WIKISUB)
            | summary_bits.mask(NotabilityCriterion.N3_SDC)
        )

    async def clear(self) -> None:
        await self.initialize()

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute("DELETE FROM evaluation_cache")
                await db.execute("DELETE FROM pubsub_events")
                await db.execute("DELETE FROM pubsub_sessions")
                await db.commit()

    async def upsert_entitydata_many(
        self,
        items: list[object],
    ) -> list[tuple[str, int]]:
        await self.initialize()

        if not items:
            return []

        normalized: list[tuple[int, int, int | None]] = []
        seen: set[int] = set()
        for item in items:
            qid = getattr(item, "qid")
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            summary = self._entitydata_summary_from_item(item)
            entitydata_last_revid = getattr(item, "entitydata_last_revid", None)
            normalized.append(
                (
                    qid_num,
                    self._as_uint32(summary, "summary"),
                    None if entitydata_last_revid is None else self._as_uint32(entitydata_last_revid, "entitydata_last_revid"),
                )
            )

        if not normalized:
            return []

        started = time.perf_counter()
        changed_rows: list[tuple[str, int]] = []
        entitydata_mask = self._entitydata_mask()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    await db.execute("BEGIN IMMEDIATE")
                    for chunk in self._chunked(normalized):
                        qid_list = [qid_num for qid_num, _, _ in chunk]
                        placeholders = ",".join("?" for _ in qid_list)
                        cursor = await db.execute(
                            f"""
                            SELECT qid, summary
                            FROM evaluation_cache
                            WHERE qid IN ({placeholders})
                            """,
                            qid_list,
                        )
                        current_rows = await cursor.fetchall()
                        current_summaries = {int(row[0]): int(row[1]) for row in current_rows}
                        for qid_num, summary, _entitydata_last_revid in chunk:
                            current_summary = current_summaries.get(qid_num)
                            new_summary = summary if current_summary is None else (
                                (current_summary & ~entitydata_mask) | (summary & entitydata_mask)
                            )
                            if current_summary != new_summary:
                                changed_rows.append((f"Q{qid_num}", new_summary))

                        values_sql = ", ".join("(%s, %s, %s)" for _ in chunk)
                        params: list[int | None] = []
                        for qid_num, summary, entitydata_last_revid in chunk:
                            params.extend([qid_num, summary, entitydata_last_revid])
                        await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = (evaluation_cache.summary & ~{entitydata_mask}) | (VALUES(summary) & {entitydata_mask}),
                                entitydata_last_revid = VALUES(entitydata_last_revid)
                            """,
                            params,
                        )
                    await db.commit()
            self._warn_slow_write("upsert_entitydata_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(normalized):
                    values_sql = ", ".join("(?, ?, ?)" for _ in chunk)
                    params: list[int | None] = []
                    for qid_num, summary, entitydata_last_revid in chunk:
                        params.extend([qid_num, summary, entitydata_last_revid])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{entitydata_mask}) | (excluded.summary & {entitydata_mask}),
                            entitydata_last_revid = excluded.entitydata_last_revid
                        RETURNING qid, summary
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                await db.commit()

        self._warn_slow_write("upsert_entitydata_many", started, row_count=len(normalized))
        return changed_rows

    async def upsert_cache_sync_many(self, items: Sequence[object]) -> list[tuple[str, int]]:
        await self.initialize()

        if not items:
            return []

        normalized: list[tuple[int, int]] = []
        seen: set[int] = set()
        for item in items:
            qid = getattr(item, "qid")
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            summary = 0
            summary = summary_bits.set(summary, NotabilityCriterion.N3_OSM, getattr(item, "n3_osm"))
            summary = summary_bits.set(summary, NotabilityCriterion.N3_WIKISUB, getattr(item, "n3_wikisub"))
            summary = summary_bits.set(summary, NotabilityCriterion.N3_SDC, getattr(item, "n3_sdc"))
            normalized.append((qid_num, self._as_uint32(summary, "summary")))

        if not normalized:
            return []

        started = time.perf_counter()
        changed_rows: list[tuple[str, int]] = []
        cache_sync_mask = self._cache_sync_mask()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    await db.execute("BEGIN IMMEDIATE")
                    for chunk in self._chunked(normalized):
                        qid_list = [qid_num for qid_num, _ in chunk]
                        placeholders = ",".join("?" for _ in qid_list)
                        cursor = await db.execute(
                            f"""
                            SELECT qid, summary
                            FROM evaluation_cache
                            WHERE qid IN ({placeholders})
                            """,
                            qid_list,
                        )
                        current_rows = await cursor.fetchall()
                        current_summaries = {int(row[0]): int(row[1]) for row in current_rows}
                        for qid_num, summary in chunk:
                            current_summary = current_summaries.get(qid_num)
                            new_summary = summary if current_summary is None else (
                                (current_summary & ~cache_sync_mask) | (summary & cache_sync_mask)
                            )
                            if current_summary != new_summary:
                                changed_rows.append((f"Q{qid_num}", new_summary))
                        values_sql = ", ".join("(%s, %s)" for _ in chunk)
                        params: list[int] = []
                        for qid_num, summary in chunk:
                            params.extend([qid_num, summary])
                        await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = (evaluation_cache.summary & ~{cache_sync_mask}) | (VALUES(summary) & {cache_sync_mask})
                            """,
                            params,
                        )
                    await db.commit()
            self._warn_slow_write("upsert_cache_sync_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(normalized):
                    values_sql = ", ".join("(?, ?)" for _ in chunk)
                    params: list[int] = []
                    for qid_num, summary in chunk:
                        params.extend([qid_num, summary])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{cache_sync_mask}) | (excluded.summary & {cache_sync_mask})
                        WHERE ((evaluation_cache.summary & ~{cache_sync_mask}) | (excluded.summary & {cache_sync_mask})) != evaluation_cache.summary
                        RETURNING qid, summary
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    event_rows = [
                        (int(time.time()), int(row[0]), "cache_sync", int(row[1]), cache_sync_mask)
                        for row in rows
                        if int(row[1]) != 0
                    ]
                    await self._append_event_log(db, event_rows)
                    changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                await db.commit()

        self._warn_slow_write("upsert_cache_sync_many", started, row_count=len(normalized))
        return changed_rows

    async def upsert_inlinks_many(self, items: Sequence[object]) -> list[tuple[str, int]]:
        await self.initialize()

        if not items:
            return []

        normalized: list[tuple[int, int]] = []
        seen: set[int] = set()
        for item in items:
            qid = getattr(item, "qid")
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            level = getattr(item, "n3_inlinks")
            summary = self._set_summary_level(0, NotabilityCriterion.N3_INLINKS, level)
            normalized.append((qid_num, self._as_uint32(summary, "summary")))

        if not normalized:
            return []

        started = time.perf_counter()
        changed_rows: list[tuple[str, int]] = []
        inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    await db.execute("BEGIN IMMEDIATE")
                    for chunk in self._chunked(normalized):
                        qid_list = [qid_num for qid_num, _ in chunk]
                        placeholders = ",".join("?" for _ in qid_list)
                        cursor = await db.execute(
                            f"""
                            SELECT qid, summary
                            FROM evaluation_cache
                            WHERE qid IN ({placeholders})
                            """,
                            qid_list,
                        )
                        current_rows = await cursor.fetchall()
                        current_summaries = {int(row[0]): int(row[1]) for row in current_rows}
                        for qid_num, summary in chunk:
                            current_summary = current_summaries.get(qid_num)
                            new_summary = summary if current_summary is None else (
                                (current_summary & ~inlinks_mask) | (summary & inlinks_mask)
                            )
                            if current_summary != new_summary:
                                changed_rows.append((f"Q{qid_num}", new_summary))
                        values_sql = ", ".join("(%s, %s)" for _ in chunk)
                        params: list[int] = []
                        for qid_num, summary in chunk:
                            params.extend([qid_num, summary])
                        await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = (evaluation_cache.summary & ~{inlinks_mask}) | (VALUES(summary) & {inlinks_mask})
                            """,
                            params,
                        )
                    await db.commit()
            self._warn_slow_write("upsert_inlinks_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(normalized):
                    values_sql = ", ".join("(?, ?)" for _ in chunk)
                    params: list[int] = []
                    for qid_num, summary in chunk:
                        params.extend([qid_num, summary])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{inlinks_mask}) | (excluded.summary & {inlinks_mask})
                        WHERE ((evaluation_cache.summary & ~{inlinks_mask}) | (excluded.summary & {inlinks_mask})) != evaluation_cache.summary
                        RETURNING qid, summary
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    event_rows = [
                        (int(time.time()), int(row[0]), "inlinks", int(row[1]), inlinks_mask)
                        for row in rows
                        if int(row[1]) != 0
                    ]
                    await self._append_event_log(db, event_rows)
                    changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                await db.commit()

        self._warn_slow_write("upsert_inlinks_many", started, row_count=len(normalized))
        return changed_rows

    @staticmethod
    def _entitydata_summary_from_item(item: object) -> int:
        summary = 0
        if bool(getattr(item, "is_redirect", False)):
            summary |= summary_bits.REDIRECT
        if bool(getattr(item, "has_sitelinks", False)):
            summary |= summary_bits.HAS_SITELINKS
        if bool(getattr(item, "has_claims", False)):
            summary |= summary_bits.HAS_CLAIMS
        if bool(getattr(item, "is_deleted", False)):
            summary |= summary_bits.DELETED

        summary = summary_bits.set(summary, NotabilityCriterion.N1, getattr(item, "n1"))
        summary = summary_bits.set(summary, NotabilityCriterion.N2a, getattr(item, "n2a"))
        summary = summary_bits.set(summary, NotabilityCriterion.N2b, getattr(item, "n2b"))
        return summary

    async def _append_event_log(
        self,
        db: aiosqlite.Connection,
        event_rows: list[tuple[int, int, str, int, int]],
    ) -> int:
        if not event_rows:
            return 0

        inserted = 0
        for chunk in self._chunked(event_rows):
            await db.executemany(
                """
                INSERT INTO pubsub_events (timestamp, qid, event_type, summary, mask)
                VALUES (?, ?, ?, ?, ?)
                """,
                chunk,
            )
            inserted += len(chunk)
        return inserted

    @staticmethod
    def _normalize_qid(qid: str | int) -> str:
        return f"Q{EvaluationCache._parse_qid(qid)}"

    async def upsert(
        self,
        qid: str | int,
        summary: int,
        entitydata_last_revid: int | None = None,
        recent_changes_last_revid: int | None = None,
    ) -> None:
        await self.initialize()

        qid_num = self._parse_qid(qid)
        summary_num = self._as_uint32(summary, "summary")
        entitydata_revid = (
            None if entitydata_last_revid is None else self._as_uint32(entitydata_last_revid, "entitydata_last_revid")
        )
        recent_changes_revid = (
            None if recent_changes_last_revid is None else self._as_uint32(recent_changes_last_revid, "recent_changes_last_revid")
        )
        started = time.perf_counter()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    await db.execute("BEGIN IMMEDIATE")
                    await db.execute(
                        """
                        INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid, recent_changes_last_revid)
                        VALUES (?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            summary = VALUES(summary),
                            entitydata_last_revid = COALESCE(VALUES(entitydata_last_revid), evaluation_cache.entitydata_last_revid),
                            recent_changes_last_revid = COALESCE(VALUES(recent_changes_last_revid), evaluation_cache.recent_changes_last_revid)
                        """,
                        (qid_num, summary_num, entitydata_revid, recent_changes_revid),
                    )
                    await db.commit()
            self._warn_slow_write("upsert", started, row_count=1)
            return

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    """
                    INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid, recent_changes_last_revid)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(qid) DO UPDATE SET
                        summary = excluded.summary,
                        entitydata_last_revid = COALESCE(excluded.entitydata_last_revid, evaluation_cache.entitydata_last_revid),
                        recent_changes_last_revid = COALESCE(excluded.recent_changes_last_revid, evaluation_cache.recent_changes_last_revid)
                    """,
                    (qid_num, summary_num, entitydata_revid, recent_changes_revid),
                )
                await db.commit()

        self._warn_slow_write("upsert", started, row_count=1)

    async def update_recent_changes_last_revids(self, qids: dict[str | int, int]) -> int:
        await self.initialize()

        if not qids:
            return 0

        normalized: dict[int, int] = {}
        for qid, revid in qids.items():
            qid_num = self._parse_qid(qid)
            revid_num = self._as_uint32(revid, "recent_changes_last_revid")
            previous = normalized.get(qid_num)
            if previous is None or revid_num > previous:
                normalized[qid_num] = revid_num

        if not normalized:
            return 0

        started = time.perf_counter()
        updated = 0
        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(list(normalized.items())):
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"""
                        SELECT qid, recent_changes_last_revid
                        FROM evaluation_cache
                        WHERE qid IN ({placeholders})
                        """,
                        [qid_num for qid_num, _ in chunk],
                    )
                    current_rows = await cursor.fetchall()
                    current_values = {int(row[0]): None if row[1] is None else int(row[1]) for row in current_rows}
                    update_rows = []
                    for qid_num, revid in chunk:
                        previous = current_values.get(qid_num)
                        if previous is not None and previous >= revid:
                            continue
                        update_rows.append((revid, qid_num))
                    if update_rows:
                        await db.executemany(
                            """
                            UPDATE evaluation_cache
                            SET recent_changes_last_revid = ?
                            WHERE qid = ?
                            """,
                            update_rows,
                        )
                        updated += len(update_rows)
                await db.commit()
        self._warn_slow_write("update_recent_changes_last_revids", started, row_count=updated)
        return updated


    async def elevate(
        self,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
        qids: set[str | int],
    ) -> int:
        await self.initialize()

        if criterion not in {
            NotabilityCriterion.N1,
            NotabilityCriterion.N2a,
            NotabilityCriterion.N2b,
            NotabilityCriterion.N3_INLINKS,
            NotabilityCriterion.N3_OSM,
            NotabilityCriterion.N3_WIKISUB,
            NotabilityCriterion.N3_SDC,
        }:
            raise ValueError(f"Cannot elevate derived criterion {criterion.value}")

        qid_nums: list[int] = []
        qid_seen: set[int] = set()
        for qid in qids:
            qid_num = self._parse_qid(qid)
            if qid_num in qid_seen:
                continue
            qid_nums.append(qid_num)
            qid_seen.add(qid_num)

        if not qid_nums:
            return 0

        base_summary = self._unknown_direct_summary()
        started = time.perf_counter()
        timestamp = int(time.time())
        event_rows: list[tuple[int, int, str, int, int]] = []
        changed_rows = 0

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(qid_nums):
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"""
                        SELECT qid, summary, entitydata_last_revid, recent_changes_last_revid
                        FROM evaluation_cache
                        WHERE qid IN ({placeholders})
                        """,
                        chunk,
                    )
                    existing_rows = await cursor.fetchall()
                    existing_summaries = {
                        int(row[0]): (
                            int(row[1]),
                            None if row[2] is None else int(row[2]),
                            None if row[3] is None else int(row[3]),
                        )
                        for row in existing_rows
                    }

                    inserts: list[tuple[int, int, int | None, int | None]] = []
                    updates: list[tuple[int, int, int | None, int | None]] = []

                    for qid_num in chunk:
                        current_summary = existing_summaries.get(qid_num)
                        if current_summary is None:
                            summary = self._set_summary_level(base_summary, criterion, level)
                            inserts.append((qid_num, summary, None, None))
                            if summary != 0:
                                event_rows.append((timestamp, qid_num, criterion.value, summary, criterion_mask))
                            continue

                        current_level = self._summary_level(current_summary[0], criterion)
                        if level > current_level:
                            new_summary = self._set_summary_level(current_summary[0], criterion, level)
                            updates.append((qid_num, new_summary, current_summary[1], current_summary[2]))
                            event_rows.append((timestamp, qid_num, criterion.value, new_summary, criterion_mask))

                    if inserts:
                        await db.executemany(
                            """
                            INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid, recent_changes_last_revid)
                            VALUES (?, ?, ?, ?)
                            """,
                            inserts,
                        )
                    if updates:
                        await db.executemany(
                            """
                            UPDATE evaluation_cache
                            SET summary = ?,
                                entitydata_last_revid = ?,
                                recent_changes_last_revid = ?
                            WHERE qid = ?
                            """,
                            [
                                (summary, entitydata_last_revid, recent_changes_last_revid, qid_num)
                                for qid_num, summary, entitydata_last_revid, recent_changes_last_revid in updates
                            ],
                        )
                    changed_rows += len(inserts) + len(updates)

                await self._append_event_log(db, event_rows)
                await db.commit()
        self._warn_slow_write("elevate", started, row_count=changed_rows)

        return changed_rows

    async def set_criterion(
        self,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
        qids: set[str | int],
        *,
        clear_missing: bool = False,
    ) -> int:
        await self.initialize()

        if criterion not in {
            NotabilityCriterion.N1,
            NotabilityCriterion.N2a,
            NotabilityCriterion.N2b,
            NotabilityCriterion.N3_INLINKS,
            NotabilityCriterion.N3_OSM,
            NotabilityCriterion.N3_WIKISUB,
            NotabilityCriterion.N3_SDC,
        }:
            raise ValueError(f"Cannot set derived criterion {criterion.value}")

        qid_nums: list[int] = []
        qid_seen: set[int] = set()
        for qid in qids:
            qid_num = self._parse_qid(qid)
            if qid_num in qid_seen:
                continue
            qid_nums.append(qid_num)
            qid_seen.add(qid_num)

        criterion_mask = summary_bits.mask(criterion)
        criterion_value = summary_bits.value(criterion, level)
        criterion_none = summary_bits.value(criterion, NotabilityLevel.NONE)
        insert_summary = criterion_value
        started = time.perf_counter()
        timestamp = int(time.time())
        event_rows: list[tuple[int, int, str, int, int]] = []
        inserted = 0
        updated = 0
        wanted_qids = set(qid_nums)

        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            if qid_nums:
                for chunk in self._chunked(qid_nums):
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"""
                        SELECT qid, summary
                        FROM evaluation_cache
                        WHERE qid IN ({placeholders})
                        """,
                        chunk,
                    )
                    existing_rows = await cursor.fetchall()
                    existing_summaries = {int(row[0]): int(row[1]) for row in existing_rows}
                    existing_qids = set(existing_summaries)

                    insert_rows = [
                        (qid_num, insert_summary)
                        for qid_num in chunk
                        if qid_num not in existing_qids
                    ]
                    if insert_rows:
                        await db.executemany(
                            """
                            INSERT INTO evaluation_cache (qid, summary)
                            VALUES (?, ?)
                            """,
                            insert_rows,
                        )
                        inserted += len(insert_rows)
                        for qid_num, summary in insert_rows:
                            if summary != 0:
                                event_rows.append((timestamp, qid_num, criterion.value, summary, criterion_mask))

                    update_rows = []
                    for qid_num in chunk:
                        previous_summary = existing_summaries.get(qid_num)
                        if previous_summary is None:
                            continue
                        new_summary = (previous_summary & ~criterion_mask) | criterion_value
                        if new_summary != previous_summary:
                            update_rows.append((new_summary, qid_num))
                            event_rows.append((timestamp, qid_num, criterion.value, new_summary, criterion_mask))
                    if update_rows:
                        await db.executemany(
                            """
                            UPDATE evaluation_cache
                            SET summary = ?
                            WHERE qid = ?
                            """,
                            update_rows,
                        )
                        updated += len(update_rows)

                if clear_missing:
                    cursor = await db.execute(
                        """
                        SELECT qid, summary
                        FROM evaluation_cache
                        ORDER BY qid
                        """
                    )
                    all_rows = await cursor.fetchall()
                    for chunk in self._chunked(all_rows):
                        missing_updates = []
                        for row in chunk:
                            qid_num = int(row[0])
                            if qid_num in wanted_qids:
                                continue
                            previous_summary = int(row[1])
                            new_summary = (previous_summary & ~criterion_mask) | criterion_none
                            if new_summary != previous_summary:
                                missing_updates.append((new_summary, qid_num))
                                event_rows.append((timestamp, qid_num, criterion.value, new_summary, criterion_mask))
                        if missing_updates:
                            await db.executemany(
                                """
                                UPDATE evaluation_cache
                                SET summary = ?
                                WHERE qid = ?
                                """,
                                missing_updates,
                            )
                            updated += len(missing_updates)
            elif clear_missing:
                cursor = await db.execute(
                    """
                    SELECT qid, summary
                    FROM evaluation_cache
                    """
                )
                all_rows = await cursor.fetchall()
                clear_updates = []
                for row in all_rows:
                    qid_num = int(row[0])
                    previous_summary = int(row[1])
                    new_summary = (previous_summary & ~criterion_mask) | criterion_none
                    if new_summary != previous_summary:
                        clear_updates.append((new_summary, qid_num))
                        event_rows.append((timestamp, qid_num, criterion.value, new_summary, criterion_mask))
                if clear_updates:
                    await db.executemany(
                        """
                        UPDATE evaluation_cache
                        SET summary = ?
                        WHERE qid = ?
                        """,
                        clear_updates,
                    )
                    updated += len(clear_updates)

            await self._append_event_log(db, event_rows)
            await db.commit()
        self._warn_slow_write("set_criterion", started, row_count=inserted + updated)

        return inserted + updated

    async def sync_criterion(
        self,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
        qids: set[str | int],
    ) -> int:
        return await self.set_criterion(criterion, level, qids, clear_missing=True)

    async def get(self, qid: QID) -> tuple[EvaluationResult | None, int | None, int | None]:
        await self.initialize()

        rows = await self.get_many([qid])
        row = rows.get(self._normalize_qid(qid))
        if row is None:
            return None, None, None

        summary, entitydata_last_revid, recent_changes_last_revid = row
        return EvaluationResult.from_summary(qid=qid, summary=summary), entitydata_last_revid, recent_changes_last_revid

    async def get_many(self, qids: list[str | int]) -> dict[str, tuple[int, int | None, int | None]]:
        await self.initialize()

        qid_nums: list[int] = []
        qid_lookup: dict[int, str] = {}
        for qid in qids:
            qid_num = self._parse_qid(qid)
            if qid_num in qid_lookup:
                continue
            qid_nums.append(qid_num)
            qid_lookup[qid_num] = self._normalize_qid(qid)

        if not qid_nums:
            return {}

        chunk_size = 500
        rows: list[tuple[int, int, int | None, int | None]] = []
        async with self._connect() as db:
            for start in range(0, len(qid_nums), chunk_size):
                chunk = qid_nums[start : start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"""
                    SELECT qid, summary, entitydata_last_revid, recent_changes_last_revid
                    FROM evaluation_cache
                    WHERE qid IN ({placeholders})
                    ORDER BY qid
                    """,
                    chunk,
                )
                rows.extend(await cursor.fetchall())

        return {
            qid_lookup[int(row[0])]: (int(row[1]), None if row[2] is None else int(row[2]), None if row[3] is None else int(row[3]))
            for row in rows
        }

    async def list_qids(self) -> list[str]:
        await self.initialize()

        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT qid
                FROM evaluation_cache
                ORDER BY qid
                """
            )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def list_unknown_inlinks_qids(self, limit: int | None = None) -> list[str]:
        await self.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)

        async with self._connect() as db:
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

    async def update_summary_bits(
        self,
        qids: set[str | int],
        *,
        set_bits: int = 0,
        clear_bits: int = 0,
    ) -> int:
        await self.initialize()

        qid_nums: list[int] = []
        qid_seen: set[int] = set()
        for qid in qids:
            qid_num = self._parse_qid(qid)
            if qid_num in qid_seen:
                continue
            qid_nums.append(qid_num)
            qid_seen.add(qid_num)

        if not qid_nums:
            return 0

        started = time.perf_counter()
        timestamp = int(time.time())
        event_rows: list[tuple[int, int, str, int, int]] = []
        updated = 0

        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            for chunk in self._chunked(qid_nums):
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"""
                    SELECT qid, summary
                    FROM evaluation_cache
                    WHERE qid IN ({placeholders})
                    """,
                    chunk,
                )
                existing_rows = await cursor.fetchall()
                existing_summaries = {int(row[0]): int(row[1]) for row in existing_rows}
                updates = []
                for qid_num, previous_summary in existing_summaries.items():
                    new_summary = previous_summary
                    if set_bits:
                        new_summary |= int(set_bits)
                    if clear_bits:
                        new_summary &= ~int(clear_bits)
                    if new_summary != previous_summary:
                        updates.append((new_summary, qid_num))
                        event_rows.append((timestamp, qid_num, "summary_bits", new_summary, int(set_bits) | int(clear_bits)))
                if updates:
                    await db.executemany(
                        """
                        UPDATE evaluation_cache
                        SET summary = ?
                        WHERE qid = ?
                        """,
                        updates,
                    )
                    updated += len(updates)
            await self._append_event_log(db, event_rows)
            await db.commit()
        self._warn_slow_write("update_summary_bits", started, row_count=updated)

        return updated

    async def stats(self) -> dict[str, int | None | str]:
        stats_started = time.perf_counter()
        await self.initialize()
        initialized_at = time.perf_counter()

        async with self._connect() as db:
            connected_at = time.perf_counter()
            eval_cursor = await db.execute(
                """
                SELECT
                    COUNT(*),
                    MIN(entitydata_last_revid),
                    MAX(entitydata_last_revid),
                    MIN(recent_changes_last_revid),
                    MAX(recent_changes_last_revid),
                    SUM(CASE WHEN (summary & ?) != ? THEN 1 ELSE 0 END)
                FROM evaluation_cache
                """,
                (
                    summary_bits.mask(NotabilityCriterion.N3_WIKISUB),
                    summary_bits.value(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.UNKNOWN),
                ),
            )
            eval_row = await eval_cursor.fetchone()
            evaluations_at = time.perf_counter()

        entries = int(eval_row[0]) if eval_row and eval_row[0] is not None else 0
        oldest_entitydata = int(eval_row[1]) if eval_row and eval_row[1] is not None else None
        newest_entitydata = int(eval_row[2]) if eval_row and eval_row[2] is not None else None
        oldest_recent_changes = int(eval_row[3]) if eval_row and eval_row[3] is not None else None
        newest_recent_changes = int(eval_row[4]) if eval_row and eval_row[4] is not None else None
        wikisub_entries = int(eval_row[5]) if eval_row and eval_row[5] is not None else 0
        return {
            "evaluations": {
                "entries": entries,
                "oldest_entitydata_last_revid": oldest_entitydata,
                "newest_entitydata_last_revid": newest_entitydata,
                "oldest_recent_changes_last_revid": oldest_recent_changes,
                "newest_recent_changes_last_revid": newest_recent_changes,
                "wikisub_entries": wikisub_entries,
            },
            "timing": {
                "total_seconds": evaluations_at - stats_started,
                "initialize_seconds": initialized_at - stats_started,
                "connect_seconds": connected_at - initialized_at,
                "evaluations_query_seconds": evaluations_at - connected_at,
            },
            "db_path": str(self.db_path),
        }


    async def breakdown(self) -> dict[str, Any]:
        await self.initialize()

        level_unknown = int(NotabilityLevel.UNKNOWN)
        level_none = int(NotabilityLevel.NONE)
        level_weak = int(NotabilityLevel.WEAK)
        level_strong = int(NotabilityLevel.STRONG)

        flags = (
            ("redirect", summary_bits.REDIRECT),
            ("has_sitelinks", summary_bits.HAS_SITELINKS),
            ("has_claims", summary_bits.HAS_CLAIMS),
            ("deleted", summary_bits.DELETED),
        )
        levels = (
            ("unknown", 2),
            ("none", 0),
            ("weak", 1),
            ("strong", 3),
        )

        async with self._connect() as db:
            total_cursor = await db.execute("SELECT COUNT(*) FROM evaluation_cache")
            total_row = await total_cursor.fetchone()
            total_rows = int(total_row[0]) if total_row and total_row[0] is not None else 0

            flag_counts: dict[str, dict[str, int]] = {}
            for name, bit in flags:
                cursor = await db.execute(
                    f"""
                    SELECT ((summary & {bit}) != 0) AS has_value, COUNT(*)
                    FROM evaluation_cache
                    GROUP BY has_value
                    """
                )
                rows = await cursor.fetchall()
                counts = {0: 0, 1: 0}
                for has_value, count in rows:
                    counts[int(has_value)] = int(count)
                flag_counts[name] = {"yes": counts[1], "no": counts[0]}

            criterion_counts: dict[str, dict[str, int]] = {}
            for criterion_key in summary_bits.direct_criteria():
                criterion = NotabilityCriterion(criterion_key)
                mask = summary_bits.mask(criterion)
                cursor = await db.execute(
                    f"""
                    SELECT
                        CASE
                            WHEN (summary & {mask}) = 0 THEN 'unknown'
                            WHEN (summary & {mask}) = {summary_bits.value(criterion, NotabilityLevel.NONE)} THEN 'none'
                            WHEN (summary & {mask}) = {summary_bits.value(criterion, NotabilityLevel.WEAK)} THEN 'weak'
                            WHEN (summary & {mask}) = {summary_bits.value(criterion, NotabilityLevel.STRONG)} THEN 'strong'
                            ELSE 'unknown'
                        END AS level_name,
                        COUNT(*)
                    FROM evaluation_cache
                    GROUP BY level_name
                    """
                )
                rows = await cursor.fetchall()
                counts = {label: 0 for label, _ in levels}
                for level_name, count in rows:
                    counts[str(level_name)] = int(count)
                criterion_counts[criterion_key] = counts

            n2_expr = f"""
                CASE
                    WHEN (summary & {summary_bits.mask(NotabilityCriterion.N2a)}) = {summary_bits.value(NotabilityCriterion.N2a, level_unknown)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N2b)}) = {summary_bits.value(NotabilityCriterion.N2b, level_unknown)}
                        THEN {level_unknown}
                    WHEN (summary & {summary_bits.mask(NotabilityCriterion.N2a)}) = {summary_bits.value(NotabilityCriterion.N2a, level_strong)}
                     AND (summary & {summary_bits.mask(NotabilityCriterion.N2b)}) = {summary_bits.value(NotabilityCriterion.N2b, level_strong)}
                        THEN {level_strong}
                    WHEN (summary & {summary_bits.mask(NotabilityCriterion.N2a)}) = {summary_bits.value(NotabilityCriterion.N2a, level_weak)}
                     OR (summary & {summary_bits.mask(NotabilityCriterion.N2b)}) = {summary_bits.value(NotabilityCriterion.N2b, level_weak)}
                        THEN {level_weak}
                    ELSE {level_none}
                END
            """
            n3_expr = f"""
                CASE
                    WHEN (summary & {summary_bits.mask(NotabilityCriterion.N3_INLINKS)}) = {summary_bits.value(NotabilityCriterion.N3_INLINKS, level_strong)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_OSM)}) = {summary_bits.value(NotabilityCriterion.N3_OSM, level_strong)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_WIKISUB)}) = {summary_bits.value(NotabilityCriterion.N3_WIKISUB, level_strong)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_SDC)}) = {summary_bits.value(NotabilityCriterion.N3_SDC, level_strong)}
                        THEN {level_strong}
                    WHEN (summary & {summary_bits.mask(NotabilityCriterion.N3_INLINKS)}) = {summary_bits.value(NotabilityCriterion.N3_INLINKS, level_unknown)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_OSM)}) = {summary_bits.value(NotabilityCriterion.N3_OSM, level_unknown)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_WIKISUB)}) = {summary_bits.value(NotabilityCriterion.N3_WIKISUB, level_unknown)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_SDC)}) = {summary_bits.value(NotabilityCriterion.N3_SDC, level_unknown)}
                        THEN {level_unknown}
                    WHEN (summary & {summary_bits.mask(NotabilityCriterion.N3_INLINKS)}) = {summary_bits.value(NotabilityCriterion.N3_INLINKS, level_weak)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_OSM)}) = {summary_bits.value(NotabilityCriterion.N3_OSM, level_weak)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_WIKISUB)}) = {summary_bits.value(NotabilityCriterion.N3_WIKISUB, level_weak)}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N3_SDC)}) = {summary_bits.value(NotabilityCriterion.N3_SDC, level_weak)}
                        THEN {level_weak}
                    ELSE {level_none}
                END
            """
            n12_expr = f"""
                CASE
                    WHEN {n2_expr} = {level_strong}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N1)}) = {summary_bits.value(NotabilityCriterion.N1, level_strong)}
                        THEN {level_strong}
                    WHEN {n2_expr} = {level_unknown}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N1)}) = {summary_bits.value(NotabilityCriterion.N1, level_unknown)}
                        THEN {level_unknown}
                    WHEN {n2_expr} = {level_weak}
                      OR (summary & {summary_bits.mask(NotabilityCriterion.N1)}) = {summary_bits.value(NotabilityCriterion.N1, level_weak)}
                        THEN {level_weak}
                    ELSE {level_none}
                END
            """
            n_expr = f"""
                CASE
                    WHEN {n12_expr} = {level_strong}
                      OR {n3_expr} = {level_strong}
                        THEN {level_strong}
                    WHEN {n12_expr} = {level_unknown}
                      OR {n3_expr} = {level_unknown}
                        THEN {level_unknown}
                    WHEN {n12_expr} = {level_weak}
                      OR {n3_expr} = {level_weak}
                        THEN {level_weak}
                    ELSE {level_none}
                END
            """

            for name, expr in (
                ("N2", n2_expr),
                ("N12", n12_expr),
                ("N3", n3_expr),
                ("N", n_expr),
            ):
                cursor = await db.execute(
                    f"""
                    SELECT
                        SUM(CASE WHEN {expr} = {level_unknown} THEN 1 ELSE 0 END) AS unknown,
                        SUM(CASE WHEN {expr} = {level_none} THEN 1 ELSE 0 END) AS none,
                        SUM(CASE WHEN {expr} = {level_weak} THEN 1 ELSE 0 END) AS weak,
                        SUM(CASE WHEN {expr} = {level_strong} THEN 1 ELSE 0 END) AS strong
                    FROM evaluation_cache
                    """
                )
                row = await cursor.fetchone()
                criterion_counts[name] = {
                    "unknown": int(row[0]) if row and row[0] is not None else 0,
                    "none": int(row[1]) if row and row[1] is not None else 0,
                    "weak": int(row[2]) if row and row[2] is not None else 0,
                    "strong": int(row[3]) if row and row[3] is not None else 0,
                }

        return {
            "entries": total_rows,
            "flags": flag_counts,
            "criteria": criterion_counts,
        }

    @staticmethod
    def _as_uint32(value: int, field_name: str) -> int:
        if not isinstance(value, int):
            raise ValueError(f"{field_name} must be an integer")
        if value < 0 or value > UINT32_MAX:
            raise ValueError(f"{field_name} must fit in uint32")
        return value

    @staticmethod
    def _direct_criteria() -> tuple[NotabilityCriterion, ...]:
        return (
            NotabilityCriterion.N1,
            NotabilityCriterion.N2a,
            NotabilityCriterion.N2b,
            NotabilityCriterion.N3_INLINKS,
            NotabilityCriterion.N3_OSM,
            NotabilityCriterion.N3_WIKISUB,
            NotabilityCriterion.N3_SDC,
        )

    @classmethod
    def _unknown_direct_summary(cls) -> int:
        return 0

    @classmethod
    def _criterion_mask(cls, criterion: NotabilityCriterion) -> int:
        return summary_bits.mask(criterion)

    @classmethod
    def _summary_level(cls, summary: int, criterion: NotabilityCriterion) -> NotabilityLevel:
        return NotabilityLevel(summary_bits.get(summary, criterion))

    @classmethod
    def _set_summary_level(
        cls,
        summary: int,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
    ) -> int:
        return summary_bits.set(summary, criterion, level)

    @classmethod
    def _parse_qid(cls, qid: str | int) -> int:
        if isinstance(qid, int):
            return cls._as_uint32(qid, "qid")

        if not isinstance(qid, str) or len(qid) < 2 or qid[0] != "Q" or not qid[1:].isdigit():
            raise ValueError("qid must look like Q42")

        return cls._as_uint32(int(qid[1:]), "qid")

CACHE = EvaluationCache()

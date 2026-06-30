from __future__ import annotations

import asyncio
import calendar
import configparser
import logging
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Sequence
from typing import Any
import time
from pathlib import Path
from datetime import UTC, datetime

import aiosqlite
import pymysql

from wd_notability.creations import CreationMetadata, _normalize_text
from wd_notability.observability import ObservabilityStore
import wd_notability.creation_cache as creation_cache
import wd_notability.inlinks.cache as inlinks_cache
from wd_notability.localdb_paths import EVALUATION_CACHE_PATH
from wd_notability.toolforge_defaults import toolforge_database_name, toolforge_defaults_file_exists
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
DEFAULT_EVALUATION_CACHE_PATH = EVALUATION_CACHE_PATH

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
        await self.close()

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

    async def close(self) -> None:
        await asyncio.to_thread(self._connection.close)


def _format_debug_query(query: str, params: Sequence[Any]) -> str:
    rendered = query
    for param in params:
        if param is None:
            replacement = "NULL"
        elif isinstance(param, str):
            replacement = "'" + param.replace("\\", "\\\\").replace("'", "''") + "'"
        else:
            replacement = str(param)
        rendered = rendered.replace("?", replacement, 1)
    return rendered


def _to_epoch_seconds(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
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
    return int(calendar.timegm(dt.utctimetuple()))


class EvaluationCache:
    """Cache for evaluation summaries, backed by SQLite locally or MariaDB on Toolforge."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        processing_timeout_seconds: int | None = None,
    ):
        backend_name = os.getenv("WD_NOTABILITY_DB_BACKEND", os.getenv("WD_NOTABILITY_CACHE_BACKEND"))
        if backend_name is None:
            backend_name = "mariadb" if toolforge_defaults_file_exists() else ("sqlite" if db_path is not None else "mariadb")
        backend_name = backend_name.strip().lower()
        self._backend_name = "mariadb" if backend_name in {"mariadb", "toolforge"} else "sqlite"
        self.db_path = Path(db_path) if db_path is not None else DEFAULT_EVALUATION_CACHE_PATH
        self.database = os.getenv(
            "WD_NOTABILITY_EVAL_DATABASE",
            os.getenv(
                "WD_NOTABILITY_CACHE_DATABASE",
                toolforge_database_name(default_database="wd_notability"),
            ),
        )
        self.host = os.getenv(
            "WD_NOTABILITY_EVAL_HOST",
            os.getenv("WD_NOTABILITY_DB_HOST", os.getenv("WD_NOTABILITY_CACHE_HOST", "tools.db.svc.wikimedia.cloud")),
        )
        self.defaults_file = Path(
            os.getenv("WD_NOTABILITY_EVAL_DEFAULTS_FILE", os.getenv("WD_NOTABILITY_CACHE_DEFAULTS_FILE", str(Path.home() / "replica.my.cnf")))
        )
        timeout_from_env = int(os.environ.get("WD_NOTABILITY_TASK_PROCESSING_TIMEOUT_SECONDS", DEFAULT_PROCESSING_TIMEOUT_SECONDS))
        timeout = timeout_from_env if processing_timeout_seconds is None else processing_timeout_seconds
        self.processing_timeout_seconds = max(1, int(timeout))
        self._initialized = False
        self._write_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._connection = None
        self.observability = ObservabilityStore(self)
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
                        last_updated INTEGER CHECK(last_updated IS NULL OR (last_updated >= 0 AND last_updated <= 9223372036854775807)),
                        creation_time INTEGER CHECK(creation_time IS NULL OR (creation_time >= 0 AND creation_time <= 9223372036854775807)),
                        creator_actor_id INTEGER CHECK(creator_actor_id IS NULL OR (creator_actor_id >= 0 AND creator_actor_id <= 4294967295)),
                        entitydata_last_revid INTEGER CHECK(entitydata_last_revid IS NULL OR (entitydata_last_revid >= 0 AND entitydata_last_revid <= 4294967295)),
                        recent_changes_last_revid INTEGER CHECK(recent_changes_last_revid IS NULL OR (recent_changes_last_revid >= 0 AND recent_changes_last_revid <= 4294967295)),
                        inlinks_last_evaluated INTEGER CHECK(inlinks_last_evaluated IS NULL OR (inlinks_last_evaluated >= 0 AND inlinks_last_evaluated <= 9223372036854775807))
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
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_session_id ON pubsub_sessions(session_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_qid ON pubsub_sessions(qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_expires_at ON pubsub_sessions(expires_at)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_wants_sync_qid ON pubsub_sessions(wants_sync, qid)")
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS lookup_state (
                        key TEXT PRIMARY KEY NOT NULL,
                        value TEXT NOT NULL
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_observability_log (
                        `timestamp` INTEGER NOT NULL CHECK(`timestamp` >= 0 AND `timestamp` <= 9223372036854775807),
                        worker_name TEXT NOT NULL,
                        data TEXT NOT NULL
                    )
                    """
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS worker_observability_log_timestamp ON worker_observability_log(`timestamp`)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS worker_observability_log_worker_timestamp ON worker_observability_log(worker_name, `timestamp`)"
                )
                cursor = await db.execute("PRAGMA table_info(evaluation_cache)")
                columns = {str(row[1]) for row in await cursor.fetchall()}
                if "creation_time" not in columns:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN creation_time INTEGER")
                if "creator_actor_id" not in columns:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN creator_actor_id INTEGER")
                if "entitydata_last_revid" not in columns:
                    await db.execute(
                        "ALTER TABLE evaluation_cache ADD COLUMN entitydata_last_revid INTEGER CHECK(entitydata_last_revid IS NULL OR (entitydata_last_revid >= 0 AND entitydata_last_revid <= 4294967295))"
                    )
                if "recent_changes_last_revid" not in columns:
                    await db.execute(
                        "ALTER TABLE evaluation_cache ADD COLUMN recent_changes_last_revid INTEGER CHECK(recent_changes_last_revid IS NULL OR (recent_changes_last_revid >= 0 AND recent_changes_last_revid <= 4294967295))"
                    )
                if "inlinks_last_evaluated" not in columns:
                    await db.execute(
                        "ALTER TABLE evaluation_cache ADD COLUMN inlinks_last_evaluated INTEGER CHECK(inlinks_last_evaluated IS NULL OR (inlinks_last_evaluated >= 0 AND inlinks_last_evaluated <= 9223372036854775807))"
                    )
                if "last_updated" not in columns:
                    await db.execute(
                        "ALTER TABLE evaluation_cache ADD COLUMN last_updated INTEGER CHECK(last_updated IS NULL OR (last_updated >= 0 AND last_updated <= 9223372036854775807))"
                    )
                await db.execute(
                    """
                    UPDATE evaluation_cache
                    SET creation_time = CAST(strftime('%s', creation_time) AS INTEGER)
                    WHERE creation_time IS NOT NULL
                      AND typeof(creation_time) = 'text'
                      AND creation_time LIKE '____-__-__T__:%:%Z'
                    """
                )
                cursor = await db.execute("PRAGMA table_info(pubsub_sessions)")
                session_columns = {str(row[1]) for row in await cursor.fetchall()}
                if "priority" not in session_columns:
                    await db.execute(
                        "ALTER TABLE pubsub_sessions ADD COLUMN priority INTEGER NOT NULL DEFAULT 10 CHECK(priority >= 0 AND priority <= 1000)"
                    )
                await db.commit()
        else:
            async with self._connect() as db:
                cursor = await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS evaluation_cache (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        summary BIGINT UNSIGNED NOT NULL,
                        last_updated BIGINT UNSIGNED NULL,
                        creation_time BIGINT UNSIGNED NULL,
                        creator_actor_id BIGINT UNSIGNED NULL,
                        entitydata_last_revid BIGINT UNSIGNED NULL,
                        recent_changes_last_revid BIGINT UNSIGNED NULL,
                        inlinks_last_evaluated BIGINT UNSIGNED NULL
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
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_session_id ON pubsub_sessions(session_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_qid ON pubsub_sessions(qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_expires_at ON pubsub_sessions(expires_at)")
                await db.execute("CREATE INDEX IF NOT EXISTS pubsub_sessions_wants_sync_qid ON pubsub_sessions(wants_sync, qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_last_updated ON evaluation_cache(last_updated)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_inlinks_last_evaluated ON evaluation_cache(inlinks_last_evaluated)")
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS lookup_state (
                        `key` VARCHAR(255) NOT NULL PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_observability_log (
                        `timestamp` BIGINT UNSIGNED NOT NULL,
                        worker_name VARCHAR(255) NOT NULL,
                        data LONGTEXT NOT NULL
                    )
                    """
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS worker_observability_log_timestamp ON worker_observability_log(`timestamp`)"
                )
                await db.execute(
                    "CREATE INDEX IF NOT EXISTS worker_observability_log_worker_timestamp ON worker_observability_log(worker_name, `timestamp`)"
                )
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'entitydata_last_revid'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN entitydata_last_revid BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'creation_time'")
                creation_time_column = await cursor.fetchone()
                if creation_time_column is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN creation_time BIGINT UNSIGNED NULL")
                elif str(creation_time_column[1]).lower().startswith("varchar"):
                    await db.execute(
                        """
                        UPDATE evaluation_cache
                        SET creation_time = UNIX_TIMESTAMP(STR_TO_DATE(creation_time, '%Y-%m-%dT%H:%i:%sZ'))
                        WHERE creation_time IS NOT NULL
                          AND creation_time REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
                        """
                    )
                    await db.execute("ALTER TABLE evaluation_cache MODIFY COLUMN creation_time BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'creator_actor_id'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN creator_actor_id BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'recent_changes_last_revid'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN recent_changes_last_revid BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'last_updated'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN last_updated BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM evaluation_cache LIKE 'inlinks_last_evaluated'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE evaluation_cache ADD COLUMN inlinks_last_evaluated BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM pubsub_sessions LIKE 'priority'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE pubsub_sessions ADD COLUMN priority INT NOT NULL DEFAULT 10")
                await db.commit()

        if self._backend_name == "sqlite":
            async with self._connect() as db:
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_creation_time ON evaluation_cache(creation_time)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_creator_actor_id ON evaluation_cache(creator_actor_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_last_updated ON evaluation_cache(last_updated)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_inlinks_last_evaluated ON evaluation_cache(inlinks_last_evaluated)")
                await db.execute("CREATE INDEX IF NOT EXISTS worker_observability_log_timestamp ON worker_observability_log(`timestamp`)")
                await db.execute("CREATE INDEX IF NOT EXISTS worker_observability_log_worker_timestamp ON worker_observability_log(worker_name, `timestamp`)")
                await db.commit()
        else:
            async with self._connect() as db:
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_creation_time ON evaluation_cache(creation_time)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_creator_actor_id ON evaluation_cache(creator_actor_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_last_updated ON evaluation_cache(last_updated)")
                await db.execute("CREATE INDEX IF NOT EXISTS evaluation_cache_inlinks_last_evaluated ON evaluation_cache(inlinks_last_evaluated)")
                await db.execute("CREATE INDEX IF NOT EXISTS worker_observability_log_timestamp ON worker_observability_log(`timestamp`)")
                await db.execute("CREATE INDEX IF NOT EXISTS worker_observability_log_worker_timestamp ON worker_observability_log(worker_name, `timestamp`)")
                await db.commit()

        self._initialized = True

    async def get_lookup_state(self, key: str) -> str | None:
        await self.initialize()

        async with self._connect() as db:
            if self._backend_name == "sqlite":
                cursor = await db.execute(
                    "SELECT value FROM lookup_state WHERE key = ?",
                    (key,),
                )
            else:
                cursor = await db.execute(
                    "SELECT value FROM lookup_state WHERE `key` = %s",
                    (key,),
                )
            row = await cursor.fetchone()
        if row is None:
            return None
        return _normalize_text(row[0])

    async def set_lookup_state(self, key: str, value: str) -> None:
        await self.initialize()

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                if self._backend_name == "sqlite":
                    await db.execute(
                        """
                        INSERT INTO lookup_state (key, value)
                        VALUES (?, ?)
                        ON CONFLICT(key) DO UPDATE SET value = excluded.value
                        """,
                        (key, value),
                    )
                else:
                    await db.execute(
                        """
                        INSERT INTO lookup_state (`key`, value)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE value = VALUES(value)
                        """,
                        (key, value),
                    )
                await db.commit()

    async def _open_connection(self):
        """Open a connection with a busy timeout so reads never hang on writer locks."""
        if self._backend_name == "sqlite":
            connection = await aiosqlite.connect(self.db_path, timeout=10)
            await connection.execute("PRAGMA busy_timeout=10000")
            return connection

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

    async def _reset_connection(self) -> None:
        connection = self._connection
        if connection is None:
            return
        self._connection = None
        close = getattr(connection, "close", None)
        if close is None:
            return
        result = close()
        if asyncio.iscoroutine(result):
            await result

    async def close(self) -> None:
        await self._reset_connection()

    @asynccontextmanager
    async def _connect(self):
        await self._connection_lock.acquire()
        try:
            connection = self._connection
            if connection is None:
                connection = await self._open_connection()
                self._connection = connection
            yield connection
        except Exception:
            await self._reset_connection()
            raise
        finally:
            self._connection_lock.release()

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

    @staticmethod
    def _creation_fields_from_item(item: object) -> tuple[int, int, int] | None:
        qid = getattr(item, "qid", None)
        qid_num = EvaluationCache._parse_qid(qid)
        creator_actor_id = getattr(item, "creator_actor_id", None)
        creation_time = getattr(item, "creation_time", None)
        if qid_num is None or creator_actor_id is None or creation_time is None:
            return None
        creation_time_num = _to_epoch_seconds(creation_time)
        if creation_time_num is None:
            return None
        try:
            creator_actor_id_num = EvaluationCache._as_uint32(creator_actor_id, "creator_actor_id")
        except ValueError:
            return None
        return qid_num, creation_time_num, creator_actor_id_num

    async def clear(self) -> None:
        await self.initialize()

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute("DELETE FROM evaluation_cache")
                await db.execute("DELETE FROM pubsub_sessions")
                await db.execute("DELETE FROM worker_observability_log")
                await db.commit()

    async def upsert_entitydata_many(
        self,
        items: list[object],
    ) -> list[tuple[str, int]]:
        await self.initialize()

        if not items:
            return []

        normalized: list[tuple[int, int, int | None, int | None]] = []
        seen: set[int] = set()
        for item in items:
            qid = getattr(item, "qid")
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            summary = self._entitydata_summary_from_item(item)
            entitydata_last_revid = getattr(item, "entitydata_last_revid", None)
            entitydata_last_revid_num = (
                None if entitydata_last_revid is None else self._as_uint32(entitydata_last_revid, "entitydata_last_revid")
            )
            normalized.append(
                (
                    qid_num,
                    self._as_uint32(summary, "summary"),
                    entitydata_last_revid_num,
                    entitydata_last_revid_num,
                )
            )

        if not normalized:
            return []

        started = time.perf_counter()
        changed_rows: list[tuple[str, int]] = []
        entitydata_mask = self._entitydata_mask()
        timestamp_sql = self._summary_update_timestamp_sql()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    for chunk in self._chunked(normalized):
                        await db.execute("BEGIN IMMEDIATE")
                        values_sql = ", ".join(f"(%s, %s, %s, %s, {timestamp_sql})" for _ in chunk)
                        params: list[int | None] = []
                        for qid_num, summary, entitydata_last_revid, recent_changes_last_revid in chunk:
                            params.extend([qid_num, summary, entitydata_last_revid, recent_changes_last_revid])
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid, recent_changes_last_revid, last_updated)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = (evaluation_cache.summary & ~{entitydata_mask}) | (VALUES(summary) & {entitydata_mask}),
                                last_updated = IF(
                                    (evaluation_cache.summary & ~{entitydata_mask}) | (VALUES(summary) & {entitydata_mask}) <> evaluation_cache.summary,
                                    VALUES(last_updated),
                                    evaluation_cache.last_updated
                                ),
                                entitydata_last_revid = VALUES(entitydata_last_revid),
                                recent_changes_last_revid = COALESCE(
                                    evaluation_cache.recent_changes_last_revid,
                                    VALUES(entitydata_last_revid)
                                )
                            RETURNING qid, summary, last_updated, (
                                VALUES(summary) <> evaluation_cache.summary
                                OR COALESCE(VALUES(entitydata_last_revid), -1) <> COALESCE(evaluation_cache.entitydata_last_revid, -1)
                            ) AS changed
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows if int(row[3]) == 1)
                        await db.commit()
            self._warn_slow_write("upsert_entitydata_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                for chunk in self._chunked(normalized):
                    await db.execute("BEGIN IMMEDIATE")
                    values_sql = ", ".join(f"(?, ?, ?, ?, {timestamp_sql})" for _ in chunk)
                    params: list[int | None] = []
                    for qid_num, summary, entitydata_last_revid, recent_changes_last_revid in chunk:
                        params.extend([qid_num, summary, entitydata_last_revid, recent_changes_last_revid])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, entitydata_last_revid, recent_changes_last_revid, last_updated)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{entitydata_mask}) | (excluded.summary & {entitydata_mask}),
                            last_updated = CASE
                                WHEN (evaluation_cache.summary & ~{entitydata_mask}) | (excluded.summary & {entitydata_mask}) <> evaluation_cache.summary
                                THEN excluded.last_updated
                                ELSE evaluation_cache.last_updated
                            END,
                            entitydata_last_revid = excluded.entitydata_last_revid,
                            recent_changes_last_revid = COALESCE(
                                evaluation_cache.recent_changes_last_revid,
                                excluded.entitydata_last_revid
                            )
                        WHERE excluded.summary != evaluation_cache.summary
                           OR COALESCE(excluded.entitydata_last_revid, -1) != COALESCE(evaluation_cache.entitydata_last_revid, -1)
                        RETURNING qid, summary, last_updated, 1 AS changed
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                    await db.commit()

        self._warn_slow_write("upsert_entitydata_many", started, row_count=len(normalized))
        return changed_rows

    async def upsert_many(
        self,
        items: Sequence[object],
    ) -> list[tuple[str, int]]:
        await self.initialize()

        if not items:
            return []

        normalized: list[tuple[int, int, int | None, int | None]] = []
        seen: set[int] = set()
        for item in items:
            if isinstance(item, dict):
                qid = item.get("qid")
                summary = item.get("summary")
                entitydata_last_revid = item.get("entitydata_last_revid")
                recent_changes_last_revid = item.get("recent_changes_last_revid")
            else:
                qid = item[0] if len(item) > 0 else None  # type: ignore[index]
                summary = item[1] if len(item) > 1 else None  # type: ignore[index]
                entitydata_last_revid = item[2] if len(item) > 2 else None  # type: ignore[index]
                recent_changes_last_revid = item[3] if len(item) > 3 else None  # type: ignore[index]

            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            if summary is None:
                continue
            normalized.append(
                (
                    qid_num,
                    self._as_uint32(summary, "summary"),
                    None if entitydata_last_revid is None else self._as_uint32(entitydata_last_revid, "entitydata_last_revid"),
                    None if recent_changes_last_revid is None else self._as_uint32(recent_changes_last_revid, "recent_changes_last_revid"),
                )
            )

        if not normalized:
            return []

        started = time.perf_counter()
        changed_rows: list[tuple[str, int]] = []
        timestamp_sql = self._summary_update_timestamp_sql()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    for chunk in self._chunked(normalized):
                        await db.execute("BEGIN IMMEDIATE")
                        values_sql = ", ".join(f"(%s, %s, %s, {timestamp_sql})" for _ in chunk)
                        params: list[int | None] = []
                        for qid_num, summary, entitydata_last_revid, recent_changes_last_revid in chunk:
                            params.extend([qid_num, summary, entitydata_last_revid, recent_changes_last_revid])
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (
                                qid, summary, entitydata_last_revid, recent_changes_last_revid, last_updated
                            )
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = VALUES(summary),
                                last_updated = IF(VALUES(summary) <> evaluation_cache.summary, VALUES(last_updated), evaluation_cache.last_updated),
                                entitydata_last_revid = COALESCE(VALUES(entitydata_last_revid), evaluation_cache.entitydata_last_revid),
                                recent_changes_last_revid = COALESCE(VALUES(recent_changes_last_revid), evaluation_cache.recent_changes_last_revid)
                            RETURNING qid, summary, last_updated, (last_updated = VALUES(last_updated)) AS changed
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows if int(row[3]) == 1)
                        await db.commit()
            self._warn_slow_write("upsert_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                for chunk in self._chunked(normalized):
                    await db.execute("BEGIN IMMEDIATE")
                    values_sql = ", ".join(f"(?, ?, ?, {timestamp_sql})" for _ in chunk)
                    params: list[int | None] = []
                    for qid_num, summary, entitydata_last_revid, recent_changes_last_revid in chunk:
                        params.extend([qid_num, summary, entitydata_last_revid, recent_changes_last_revid])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (
                            qid, summary, entitydata_last_revid, recent_changes_last_revid, last_updated
                        )
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = excluded.summary,
                            last_updated = CASE WHEN excluded.summary != evaluation_cache.summary THEN excluded.last_updated ELSE evaluation_cache.last_updated END,
                            entitydata_last_revid = COALESCE(excluded.entitydata_last_revid, evaluation_cache.entitydata_last_revid),
                            recent_changes_last_revid = COALESCE(excluded.recent_changes_last_revid, evaluation_cache.recent_changes_last_revid)
                        WHERE excluded.summary != evaluation_cache.summary
                        RETURNING qid, summary, last_updated, 1 AS changed
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                    await db.commit()

        self._warn_slow_write("upsert_many", started, row_count=len(normalized))
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
        timestamp_sql = self._summary_update_timestamp_sql()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    for chunk in self._chunked(normalized):
                        await db.execute("BEGIN IMMEDIATE")
                        values_sql = ", ".join(f"(%s, %s, {timestamp_sql})" for _ in chunk)
                        params: list[int] = []
                        for qid_num, summary in chunk:
                            params.extend([qid_num, summary])
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, last_updated)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = (evaluation_cache.summary & ~{cache_sync_mask}) | (VALUES(summary) & {cache_sync_mask}),
                                last_updated = IF(
                                    (evaluation_cache.summary & ~{cache_sync_mask}) | (VALUES(summary) & {cache_sync_mask}) <> evaluation_cache.summary,
                                    VALUES(last_updated),
                                    evaluation_cache.last_updated
                                )
                            RETURNING qid, summary, last_updated, (last_updated = VALUES(last_updated)) AS changed
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        event_rows = [
                            (int(time.time()), int(row[0]), "cache_sync", int(row[1]), cache_sync_mask)
                            for row in rows
                            if int(row[3]) == 1 and int(row[1]) != 0
                        ]
                        changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows if int(row[3]) == 1)
                        await db.commit()
            self._warn_slow_write("upsert_cache_sync_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(normalized):
                    values_sql = ", ".join(f"(?, ?, {timestamp_sql})" for _ in chunk)
                    params: list[int] = []
                    for qid_num, summary in chunk:
                        params.extend([qid_num, summary])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, last_updated)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{cache_sync_mask}) | (excluded.summary & {cache_sync_mask}),
                            last_updated = excluded.last_updated
                        WHERE ((evaluation_cache.summary & ~{cache_sync_mask}) | (excluded.summary & {cache_sync_mask})) != evaluation_cache.summary
                        RETURNING qid, summary, last_updated, 1 AS changed
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    event_rows = [
                        (int(time.time()), int(row[0]), "cache_sync", int(row[1]), cache_sync_mask)
                        for row in rows
                        if int(row[1]) != 0
                    ]
                    changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                await db.commit()

        self._warn_slow_write("upsert_cache_sync_many", started, row_count=len(normalized))
        return changed_rows

    async def upsert_creation_metadata_many(self, items: Sequence[object]) -> int:
        await self.initialize()

        if not items:
            return 0

        normalized: list[tuple[int, int, int]] = []
        seen: set[int] = set()
        for item in items:
            normalized_item = self._creation_fields_from_item(item)
            if normalized_item is None:
                continue
            qid_num, creation_time, creator_actor_id = normalized_item
            if qid_num in seen:
                continue
            seen.add(qid_num)
            normalized.append((qid_num, creation_time, creator_actor_id))

        if not normalized:
            return 0

        started = time.perf_counter()
        updated = 0

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    for chunk in self._chunked(normalized):
                        await db.execute("BEGIN IMMEDIATE")
                        values_sql = ", ".join("(%s, 0, %s, %s)" for _ in chunk)
                        params: list[int] = []
                        for qid_num, creation_time, creator_actor_id in chunk:
                            params.extend([qid_num, creation_time, creator_actor_id])
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, creation_time, creator_actor_id)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                creation_time = VALUES(creation_time),
                                creator_actor_id = VALUES(creator_actor_id)
                            RETURNING qid
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        updated += len(rows)
                        await db.commit()
            self._warn_slow_write("upsert_creation_metadata_many", started, row_count=len(normalized))
            return updated

        async with self._write_guard():
            async with self._connect() as db:
                for chunk in self._chunked(normalized):
                    await db.execute("BEGIN IMMEDIATE")
                    values_sql = ", ".join("(?, 0, ?, ?)" for _ in chunk)
                    params: list[int] = []
                    for qid_num, creation_time, creator_actor_id in chunk:
                        params.extend([qid_num, creation_time, creator_actor_id])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, creation_time, creator_actor_id)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            creation_time = excluded.creation_time,
                            creator_actor_id = excluded.creator_actor_id
                        RETURNING qid
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    updated += len(rows)
                    await db.commit()

        self._warn_slow_write("upsert_creation_metadata_many", started, row_count=len(normalized))
        return updated

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
        timestamp_sql = self._summary_update_timestamp_sql()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    for chunk in self._chunked(normalized):
                        await db.execute("BEGIN IMMEDIATE")
                        values_sql = ", ".join(f"(%s, %s, {timestamp_sql}, {timestamp_sql})" for _ in chunk)
                        params: list[int] = []
                        for qid_num, summary in chunk:
                            params.extend([qid_num, summary])
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, last_updated, inlinks_last_evaluated)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                summary = (evaluation_cache.summary & ~{inlinks_mask}) | (VALUES(summary) & {inlinks_mask}),
                                last_updated = IF(
                                    (evaluation_cache.summary & ~{inlinks_mask}) | (VALUES(summary) & {inlinks_mask}) <> evaluation_cache.summary,
                                    VALUES(last_updated),
                                    evaluation_cache.last_updated
                                ),
                                inlinks_last_evaluated = VALUES(inlinks_last_evaluated)
                            RETURNING qid, summary, last_updated, inlinks_last_evaluated
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        event_rows = [
                            (int(time.time()), int(row[0]), "inlinks", int(row[1]), inlinks_mask)
                            for row in rows
                            if int(row[1]) != 0
                        ]
                        changed_rows.extend((f"Q{int(row[0])}", int(row[1])) for row in rows)
                        await db.commit()
            self._warn_slow_write("upsert_inlinks_many", started, row_count=len(normalized))
            return changed_rows

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(normalized):
                    values_sql = ", ".join(f"(?, ?, {timestamp_sql}, {timestamp_sql})" for _ in chunk)
                    params: list[int] = []
                    for qid_num, summary in chunk:
                        params.extend([qid_num, summary])
                    cursor = await db.execute(
                        f"""
                        INSERT INTO evaluation_cache (qid, summary, last_updated, inlinks_last_evaluated)
                        VALUES {values_sql}
                        ON CONFLICT(qid) DO UPDATE SET
                            summary = (evaluation_cache.summary & ~{inlinks_mask}) | (excluded.summary & {inlinks_mask}),
                            last_updated = CASE
                                WHEN (evaluation_cache.summary & ~{inlinks_mask}) | (excluded.summary & {inlinks_mask}) != evaluation_cache.summary
                                THEN excluded.last_updated
                                ELSE evaluation_cache.last_updated
                            END,
                            inlinks_last_evaluated = excluded.inlinks_last_evaluated
                        RETURNING qid, summary, last_updated, inlinks_last_evaluated
                        """,
                        params,
                    )
                    rows = await cursor.fetchall()
                    event_rows = [
                        (int(time.time()), int(row[0]), "inlinks", int(row[1]), inlinks_mask)
                        for row in rows
                        if int(row[1]) != 0
                    ]
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

    def _summary_update_timestamp_sql(self) -> str:
        if self._backend_name == "mariadb":
            return "TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', CURRENT_TIMESTAMP(6))"
        return "CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)"

    async def _sync_criterion_bulk(
        self,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
        qids: set[str | int],
        *,
        clear_missing: bool,
    ) -> int:
        if criterion == NotabilityCriterion.N3_INLINKS:
            raise ValueError("N3_inlinks may only be set by the inlinks worker")

        criterion_mask = summary_bits.mask(criterion)
        criterion_value = summary_bits.value(criterion, level)
        criterion_none = summary_bits.value(criterion, NotabilityLevel.NONE)
        timestamp_sql = self._summary_update_timestamp_sql()
        filter_table = "_evaluation_cache_sync_filter"
        started = time.perf_counter()
        changed_rows = 0

        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                f"""
                CREATE TEMPORARY TABLE IF NOT EXISTS {filter_table} (
                    qid INTEGER PRIMARY KEY
                )
                """
            )
            await db.execute(f"DELETE FROM {filter_table}")

            pending: list[int] = []
            for qid in qids:
                qid_num = self._parse_qid(qid)
                if qid_num is None:
                    continue
                pending.append(qid_num)
                if len(pending) < DEFAULT_WRITE_CHUNK_SIZE:
                    continue

                placeholders = ",".join("(?)" for _ in pending)
                params = [qid_num for qid_num in pending]
                if self._backend_name == "mariadb":
                    insert_sql = f"INSERT IGNORE INTO {filter_table} (qid) VALUES {placeholders}"
                else:
                    insert_sql = f"INSERT OR IGNORE INTO {filter_table} (qid) VALUES {placeholders}"
                await db.execute(insert_sql, params)
                pending.clear()

            if pending:
                placeholders = ",".join("(?)" for _ in pending)
                params = [qid_num for qid_num in pending]
                if self._backend_name == "mariadb":
                    insert_sql = f"INSERT IGNORE INTO {filter_table} (qid) VALUES {placeholders}"
                else:
                    insert_sql = f"INSERT OR IGNORE INTO {filter_table} (qid) VALUES {placeholders}"
                await db.execute(insert_sql, params)

            count_cursor = await db.execute(f"SELECT COUNT(*) FROM {filter_table}")
            filter_count_row = await count_cursor.fetchone()
            filter_count = int(filter_count_row[0] or 0) if filter_count_row is not None else 0
            if filter_count == 0:
                await db.commit()
                return 0

            if clear_missing:
                clear_cursor = await db.execute(
                    f"""
                    UPDATE evaluation_cache
                    SET summary = (summary & ~?) | ?,
                        last_updated = {timestamp_sql}
                    WHERE (summary & ?) != ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {filter_table}
                          WHERE {filter_table}.qid = evaluation_cache.qid
                      )
                    """,
                    (
                        criterion_mask,
                        criterion_none,
                        criterion_mask,
                        criterion_none,
                    ),
                )
                changed_rows += max(0, int(clear_cursor.rowcount))

            if self._backend_name == "mariadb":
                upsert_sql = f"""
                INSERT INTO evaluation_cache (qid, summary, last_updated)
                SELECT qid, %s, {timestamp_sql}
                FROM {filter_table}
                ON DUPLICATE KEY UPDATE
                    summary = (evaluation_cache.summary & ~{criterion_mask}) | (VALUES(summary) & {criterion_mask}),
                    last_updated = IF(
                        (evaluation_cache.summary & ~{criterion_mask}) | (VALUES(summary) & {criterion_mask}) <> evaluation_cache.summary,
                        VALUES(last_updated),
                        evaluation_cache.last_updated
                    )
                """
                upsert_cursor = await db.execute(upsert_sql, (criterion_value,))
            else:
                update_cursor = await db.execute(
                    f"""
                    UPDATE evaluation_cache
                    SET summary = (summary & ~?) | ?,
                        last_updated = CASE
                            WHEN (summary & ~?) | ? <> summary
                            THEN {timestamp_sql}
                            ELSE last_updated
                        END
                    WHERE qid IN (
                        SELECT qid
                        FROM {filter_table}
                    )
                    """,
                    (
                        criterion_mask,
                        criterion_value,
                        criterion_mask,
                        criterion_value,
                    ),
                )
                changed_rows += max(0, int(update_cursor.rowcount))
                insert_cursor = await db.execute(
                    f"""
                    INSERT OR IGNORE INTO evaluation_cache (qid, summary, last_updated)
                    SELECT qid, ?, {timestamp_sql}
                    FROM {filter_table}
                    """,
                    (criterion_value,),
                )
                changed_rows += max(0, int(insert_cursor.rowcount))
            await db.commit()

        self._warn_slow_write("sync_criterion_bulk", started, row_count=changed_rows)
        return changed_rows

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
        timestamp_sql = self._summary_update_timestamp_sql()

        if self._backend_name == "mariadb":
            async with self._write_guard():
                async with self._connect() as db:
                    await db.execute("BEGIN IMMEDIATE")
                    await db.execute(
                        """
                        INSERT INTO evaluation_cache (qid, summary, last_updated, entitydata_last_revid, recent_changes_last_revid)
                        VALUES (?, ?, """ + timestamp_sql + """, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            summary = VALUES(summary),
                            last_updated = IF(VALUES(summary) <> evaluation_cache.summary, VALUES(last_updated), evaluation_cache.last_updated),
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
                    INSERT INTO evaluation_cache (qid, summary, last_updated, entitydata_last_revid, recent_changes_last_revid)
                    VALUES (?, ?, """ + timestamp_sql + """, ?, ?)
                    ON CONFLICT(qid) DO UPDATE SET
                        summary = excluded.summary,
                        last_updated = CASE WHEN excluded.summary != evaluation_cache.summary THEN excluded.last_updated ELSE evaluation_cache.last_updated END,
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
                for chunk in self._chunked(list(normalized.items())):
                    await db.execute("BEGIN IMMEDIATE")
                    values_sql = ", ".join("(%s, 0, %s)" for _ in chunk) if self._backend_name == "mariadb" else ", ".join("(?, 0, ?)" for _ in chunk)
                    params: list[int] = []
                    for qid_num, revid in chunk:
                        params.extend([qid_num, revid])
                    if self._backend_name == "mariadb":
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, recent_changes_last_revid)
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                recent_changes_last_revid = CASE
                                    WHEN evaluation_cache.recent_changes_last_revid IS NULL
                                      OR evaluation_cache.recent_changes_last_revid < VALUES(recent_changes_last_revid)
                                    THEN VALUES(recent_changes_last_revid)
                                    ELSE evaluation_cache.recent_changes_last_revid
                                END
                            RETURNING qid
                            """,
                            params,
                        )
                    else:
                        cursor = await db.execute(
                            f"""
                            INSERT INTO evaluation_cache (qid, summary, recent_changes_last_revid)
                            VALUES {values_sql}
                            ON CONFLICT(qid) DO UPDATE SET
                                recent_changes_last_revid = CASE
                                    WHEN evaluation_cache.recent_changes_last_revid IS NULL
                                      OR evaluation_cache.recent_changes_last_revid < excluded.recent_changes_last_revid
                                    THEN excluded.recent_changes_last_revid
                                    ELSE evaluation_cache.recent_changes_last_revid
                                END
                            RETURNING qid
                            """,
                            params,
                        )
                    rows = await cursor.fetchall()
                    updated += len(rows)
                    await db.commit()
        self._warn_slow_write("update_recent_changes_last_revids", started, row_count=updated)
        return updated

    async def clear_entitydata_last_revids(self, qids: Sequence[str | int]) -> int:
        await self.initialize()

        qid_nums: list[int] = []
        seen: set[int] = set()
        for qid in qids:
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            qid_nums.append(qid_num)

        if not qid_nums:
            return 0

        started = time.perf_counter()
        updated = 0
        timestamp_sql = self._summary_update_timestamp_sql()

        async with self._write_guard():
            async with self._connect() as db:
                for chunk in self._chunked(qid_nums):
                    await db.execute("BEGIN IMMEDIATE")
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"""
                        UPDATE evaluation_cache
                        SET entitydata_last_revid = NULL,
                            last_updated = {timestamp_sql}
                        WHERE qid IN ({placeholders})
                        RETURNING qid
                        """,
                        chunk,
                    )
                    rows = await cursor.fetchall()
                    updated += len(rows)
                    await db.commit()

        self._warn_slow_write("clear_entitydata_last_revids", started, row_count=updated)
        return updated

    async def list_missing_creation_qids(self, limit: int | None = None) -> list[str]:
        await self.initialize()

        async with self._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT qid
                    FROM evaluation_cache
                    WHERE creation_time IS NULL
                       OR creator_actor_id IS NULL
                    ORDER BY qid DESC
                    """
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT qid
                    FROM evaluation_cache
                    WHERE creation_time IS NULL
                       OR creator_actor_id IS NULL
                    ORDER BY qid DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def list_stale_entitydata_qids(self, limit: int | None = None) -> list[str]:
        await self.initialize()

        async with self._connect() as db:
            if limit is None:
                cursor = await db.execute(
                    """
                    SELECT qid
                    FROM evaluation_cache
                    WHERE entitydata_last_revid IS NULL
                    ORDER BY qid DESC
                    """
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT qid
                    FROM evaluation_cache
                    WHERE entitydata_last_revid IS NULL
                    ORDER BY qid DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
            rows = await cursor.fetchall()

        return [f"Q{int(row[0])}" for row in rows]

    async def count_stale_entitydata_qids(self) -> int:
        await self.initialize()

        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM evaluation_cache
                WHERE entitydata_last_revid IS NULL
                """
            )
            row = await cursor.fetchone()

        return int(row[0]) if row and row[0] is not None else 0

    async def count_missing_creation_qids(self) -> int:
        await self.initialize()

        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM evaluation_cache
                WHERE creation_time IS NULL
                   OR creator_actor_id IS NULL
                """
            )
            row = await cursor.fetchone()

        return int(row[0]) if row and row[0] is not None else 0

    async def list_creation_metadata(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        creator_actor_ids: Sequence[object] | None = None,
    ) -> list[CreationMetadata]:
        await self.initialize()

        start_epoch = _to_epoch_seconds(start) if start is not None else None
        end_epoch = _to_epoch_seconds(end) if end is not None else None

        creator_ids: list[int] = []
        seen: set[int] = set()
        for value in creator_actor_ids or []:
            try:
                creator_id = self._as_uint32(value, "creator_actor_id")
            except ValueError:
                continue
            if creator_id in seen:
                continue
            seen.add(creator_id)
            creator_ids.append(creator_id)

        where_clauses = [
            "creation_time IS NOT NULL",
            "creator_actor_id IS NOT NULL",
        ]
        params: list[object] = []
        if start_epoch is not None:
            where_clauses.append("creation_time >= ?")
            params.append(start_epoch)
        if end_epoch is not None:
            where_clauses.append("creation_time < ?")
            params.append(end_epoch)
        if creator_ids:
            placeholders = ", ".join(["?"] * len(creator_ids))
            where_clauses.append(f"creator_actor_id IN ({placeholders})")
            params.extend(creator_ids)

        query = f"""
            SELECT qid, creator_actor_id, creation_time
            FROM evaluation_cache
            WHERE {' AND '.join(where_clauses)}
            ORDER BY creation_time ASC, qid ASC
        """

        async with self._connect() as db:
            # debug_sql = _format_debug_query(query, params)
            # print(f"Executing SQL: {debug_sql}")
            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()

        result: list[CreationMetadata] = []
        for qid, creator_actor_id, creation_time in rows:
            normalized_qid = f"Q{int(qid)}"
            try:
                creator_actor_id_num = int(creator_actor_id)
            except (TypeError, ValueError):
                continue
            if creation_time is None:
                continue
            creation_time_num = _to_epoch_seconds(creation_time)
            if creation_time_num is None:
                continue
            result.append(
                CreationMetadata(
                    qid=normalized_qid,
                    creator_actor_id=creator_actor_id_num,
                    creation_time=creation_time_num,
                )
            )
        return result

    async def get_creation_metadata_many(
        self,
        qids: Sequence[object],
    ) -> dict[str, CreationMetadata]:
        await self.initialize()

        qid_nums: list[int] = []
        qid_lookup: dict[int, str] = {}
        seen: set[int] = set()
        for qid in qids:
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            qid_nums.append(qid_num)
            qid_lookup[qid_num] = self._normalize_qid(qid)

        if not qid_nums:
            return {}

        result: dict[str, CreationMetadata] = {}
        chunk_size = 500
        async with self._connect() as db:
            for start in range(0, len(qid_nums), chunk_size):
                chunk = qid_nums[start : start + chunk_size]
                placeholders = ", ".join(["?"] * len(chunk))
                cursor = await db.execute(
                    f"""
                    SELECT qid, creator_actor_id, creation_time
                    FROM evaluation_cache
                    WHERE qid IN ({placeholders})
                      AND creator_actor_id IS NOT NULL
                      AND creation_time IS NOT NULL
                    ORDER BY qid ASC
                    """,
                    chunk,
                )
                rows = await cursor.fetchall()
                for qid, creator_actor_id, creation_time in rows:
                    try:
                        creator_actor_id_num = int(creator_actor_id)
                    except (TypeError, ValueError):
                        continue
                    if creation_time is None:
                        continue
                    creation_time_num = _to_epoch_seconds(creation_time)
                    if creation_time_num is None:
                        continue
                    normalized_qid = qid_lookup.get(int(qid))
                    if normalized_qid is None:
                        continue
                    result[normalized_qid] = CreationMetadata(
                        qid=normalized_qid,
                        creator_actor_id=creator_actor_id_num,
                        creation_time=creation_time_num,
                    )
        return result


    async def elevate(
        self,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
        qids: set[str | int],
    ) -> int:
        await self.initialize()

        if criterion == NotabilityCriterion.N3_INLINKS:
            raise ValueError("N3_inlinks may only be set by the inlinks worker")

        if criterion not in {
            NotabilityCriterion.N1,
            NotabilityCriterion.N2a,
            NotabilityCriterion.N2b,
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

        criterion_mask = summary_bits.mask(criterion)
        criterion_none = summary_bits.value(criterion, NotabilityLevel.NONE)
        criterion_weak = summary_bits.value(criterion, NotabilityLevel.WEAK)
        criterion_unknown = summary_bits.value(criterion, NotabilityLevel.UNKNOWN)
        criterion_strong = summary_bits.value(criterion, NotabilityLevel.STRONG)
        desired_rank = int(level)
        started = time.perf_counter()
        timestamp = int(time.time())
        event_rows: list[tuple[int, int, str, int, int]] = []
        changed_rows = 0
        timestamp_sql = self._summary_update_timestamp_sql()

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(qid_nums):
                    insert_placeholders = ", ".join(f"(?, ?, {timestamp_sql})" for _ in chunk)
                    insert_params: list[int] = []
                    for qid_num in chunk:
                        insert_params.extend((qid_num, self._set_summary_level(0, criterion, level)))
                    if self._backend_name == "mariadb":
                        insert_sql = f"""
                        INSERT IGNORE INTO evaluation_cache (qid, summary, last_updated)
                        VALUES {insert_placeholders}
                        RETURNING qid, summary, last_updated, 1 AS changed
                        """
                    else:
                        insert_sql = f"""
                        INSERT INTO evaluation_cache (qid, summary, last_updated)
                        VALUES {insert_placeholders}
                        ON CONFLICT(qid) DO NOTHING
                        RETURNING qid, summary, last_updated, 1 AS changed
                        """
                    insert_cursor = await db.execute(
                        insert_sql,
                        insert_params,
                    )
                    inserted_rows = await insert_cursor.fetchall()
                    changed_rows += len(inserted_rows)
                    for row in inserted_rows:
                        qid_num = int(row[0])
                        summary = int(row[1])
                        if summary != 0:
                            event_rows.append((timestamp, qid_num, criterion.value, summary, criterion_mask))

                    placeholders = ",".join("?" for _ in chunk)
                    update_cursor = await db.execute(
                        f"""
                        UPDATE evaluation_cache
                        SET summary = (summary & ~?) | ?,
                            last_updated = {timestamp_sql}
                        WHERE qid IN ({placeholders})
                          AND CASE (summary & ?)
                                  WHEN ? THEN 0
                                  WHEN ? THEN 1
                                  WHEN ? THEN 2
                                  WHEN ? THEN 3
                                  ELSE 2
                              END < ?
                        RETURNING qid, summary, last_updated
                        """,
                        (
                            criterion_mask,
                            summary_bits.value(criterion, level),
                            *chunk,
                            criterion_mask,
                            criterion_none,
                            criterion_weak,
                            criterion_unknown,
                            criterion_strong,
                            desired_rank,
                        ),
                    )
                    updated_rows = await update_cursor.fetchall()
                    changed_rows += len(updated_rows)
                    for row in updated_rows:
                        event_rows.append((timestamp, int(row[0]), criterion.value, int(row[1]), criterion_mask))

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

        if criterion == NotabilityCriterion.N3_INLINKS:
            raise ValueError("N3_inlinks may only be set by the inlinks worker")

        if criterion not in {
            NotabilityCriterion.N1,
            NotabilityCriterion.N2a,
            NotabilityCriterion.N2b,
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
        timestamp_sql = self._summary_update_timestamp_sql()

        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            if clear_missing:
                if qid_nums:
                    filter_table = "_evaluation_cache_sync_filter"
                    await db.execute(
                        f"""
                        CREATE TEMPORARY TABLE IF NOT EXISTS {filter_table} (
                            qid INTEGER PRIMARY KEY
                        )
                        """
                    )
                    await db.execute(f"DELETE FROM {filter_table}")
                    for chunk in self._chunked(qid_nums):
                        placeholders = ",".join("(?)" for _ in chunk)
                        params = [qid_num for qid_num in chunk]
                        if self._backend_name == "mariadb":
                            insert_sql = f"INSERT IGNORE INTO {filter_table} (qid) VALUES {placeholders}"
                        else:
                            insert_sql = f"INSERT OR IGNORE INTO {filter_table} (qid) VALUES {placeholders}"
                        await db.execute(insert_sql, params)

                    cursor = await db.execute(
                        f"""
                        UPDATE evaluation_cache
                        SET summary = (summary & ~?) | ?,
                            last_updated = {timestamp_sql}
                        WHERE (summary & ?) != ?
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {filter_table}
                              WHERE {filter_table}.qid = evaluation_cache.qid
                          )
                        RETURNING qid, summary, last_updated
                        """,
                        (
                            criterion_mask,
                            criterion_none,
                            criterion_mask,
                            criterion_none,
                        ),
                    )
                else:
                    cursor = await db.execute(
                        f"""
                        UPDATE evaluation_cache
                        SET summary = (summary & ~?) | ?,
                            last_updated = {timestamp_sql}
                        WHERE (summary & ?) != ?
                        RETURNING qid, summary, last_updated
                        """,
                        (
                            criterion_mask,
                            criterion_none,
                            criterion_mask,
                            criterion_none,
                        ),
                    )
                cleared_rows = await cursor.fetchall()
                updated += len(cleared_rows)
                for row in cleared_rows:
                    event_rows.append((timestamp, int(row[0]), criterion.value, int(row[1]), criterion_mask))

            if qid_nums:
                for chunk in self._chunked(qid_nums):
                    insert_placeholders = ", ".join(f"(?, ?, {timestamp_sql})" for _ in chunk)
                    insert_params: list[int] = []
                    for qid_num in chunk:
                        insert_params.extend((qid_num, insert_summary))
                    if self._backend_name == "mariadb":
                        insert_sql = f"""
                        INSERT IGNORE INTO evaluation_cache (qid, summary, last_updated)
                        VALUES {insert_placeholders}
                        RETURNING qid, summary, last_updated, 1 AS changed
                        """
                        insert_cursor = await db.execute(
                            insert_sql,
                            insert_params,
                        )
                        inserted_rows = await insert_cursor.fetchall()
                        inserted += len(inserted_rows)
                        for row in inserted_rows:
                            qid_num = int(row[0])
                            summary = int(row[1])
                            if summary != 0:
                                event_rows.append((timestamp, qid_num, criterion.value, summary, criterion_mask))

                        update_cursor = await db.execute(
                            f"""
                            UPDATE evaluation_cache
                            SET summary = (summary & ~?) | ?,
                                last_updated = {timestamp_sql}
                            WHERE qid IN ({",".join("?" for _ in chunk)})
                              AND (summary & ?) != ?
                            RETURNING qid, summary, last_updated
                            """,
                            (criterion_mask, criterion_value, *chunk, criterion_mask, criterion_value),
                        )
                        updated_rows = await update_cursor.fetchall()
                        updated += len(updated_rows)
                        for row in updated_rows:
                            event_rows.append((timestamp, int(row[0]), criterion.value, int(row[1]), criterion_mask))
                    else:
                        update_cursor = await db.execute(
                            f"""
                            UPDATE evaluation_cache
                            SET summary = (summary & ~?) | ?,
                                last_updated = CASE
                                    WHEN (summary & ~?) | ? <> summary
                                    THEN {timestamp_sql}
                                    ELSE last_updated
                                END
                            WHERE qid IN ({",".join("?" for _ in chunk)})
                              AND (summary & ?) != ?
                            """,
                            (criterion_mask, criterion_value, criterion_mask, criterion_value, *chunk, criterion_mask, criterion_value),
                        )
                        updated += max(0, int(update_cursor.rowcount))
                        insert_cursor = await db.execute(
                            f"""
                            INSERT OR IGNORE INTO evaluation_cache (qid, summary, last_updated)
                            VALUES {insert_placeholders}
                            """,
                            insert_params,
                        )
                        inserted += max(0, int(insert_cursor.rowcount))
                        cursor = await db.execute(
                            f"""
                            SELECT qid, summary
                            FROM evaluation_cache
                            WHERE qid IN ({",".join("?" for _ in chunk)})
                            ORDER BY qid
                            """,
                            chunk,
                        )
                        current_rows = await cursor.fetchall()
                        for row in current_rows:
                            event_rows.append((timestamp, int(row[0]), criterion.value, int(row[1]), criterion_mask))

            await db.commit()
        self._warn_slow_write("set_criterion", started, row_count=inserted + updated)

        return inserted + updated

    async def sync_criterion(
        self,
        criterion: NotabilityCriterion,
        level: NotabilityLevel,
        qids: set[str | int],
        *,
        clear_missing: bool = True,
    ) -> int:
        if clear_missing and len(qids) >= 5000:
            return await self._sync_criterion_bulk(
                criterion,
                level,
                qids,
                clear_missing=clear_missing,
            )
        return await self.set_criterion(criterion, level, qids, clear_missing=clear_missing)

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

    async def iter_qid_summary_chunks(
        self,
        chunk_size: int = DEFAULT_WRITE_CHUNK_SIZE,
    ) -> AsyncIterator[list[tuple[str, int]]]:
        await self.initialize()

        if chunk_size < 1:
            raise ValueError("chunk_size must be at least 1")

        last_qid = -1
        while True:
            async with self._connect() as db:
                cursor = await db.execute(
                    """
                    SELECT qid, summary
                    FROM evaluation_cache
                    WHERE qid > ?
                    ORDER BY qid ASC
                    LIMIT ?
                    """,
                    (last_qid, chunk_size),
                )
                rows = await cursor.fetchall()

            if not rows:
                break

            chunk: list[tuple[str, int]] = []
            for qid, summary in rows:
                if qid is None or summary is None:
                    continue
                chunk.append((f"Q{int(qid)}", int(summary)))
            if chunk:
                last_qid = int(rows[-1][0])
                yield chunk

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

    async def count_unknown_inlinks_qids(self) -> int:
        await self.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)

        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT COUNT(*)
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
                """,
                (n3_inlinks_mask, n3_inlinks_unknown),
            )
            row = await cursor.fetchone()

        return int(row[0]) if row and row[0] is not None else 0

    async def list_known_inlinks_refresh_candidates(
        self,
        limit: int | None = None,
    ) -> list[tuple[str, str, int]]:
        await self.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)
        deleted_mask = summary_bits.DELETED

        async with self._connect() as db:
            base_query = """
                SELECT
                    ec.qid,
                    ec.creation_time,
                    ec.inlinks_last_evaluated
                FROM evaluation_cache ec
                WHERE ec.qid != 0
                  AND ec.creation_time IS NOT NULL
                  AND ec.inlinks_last_evaluated IS NOT NULL
                  AND (ec.summary & ?) != ?
                  AND (ec.summary & ?) = 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pubsub_sessions s
                      WHERE s.qid = ec.qid
                        AND s.qid != 0
                        AND s.wants_inlinks = 1
                        AND s.owner_id != 'inlinks'
                  )
                ORDER BY ec.creation_time ASC, ec.inlinks_last_evaluated ASC, ec.qid ASC
            """
            if limit is None:
                cursor = await db.execute(base_query, (n3_inlinks_mask, n3_inlinks_unknown, deleted_mask))
            else:
                cursor = await db.execute(base_query + " LIMIT ?", (n3_inlinks_mask, n3_inlinks_unknown, deleted_mask, limit))
            rows = await cursor.fetchall()

        result: list[tuple[str, int, int]] = []
        for qid, creation_time, inlinks_last_evaluated in rows:
            if creation_time is None or inlinks_last_evaluated is None:
                continue
            creation_time_num = _to_epoch_seconds(creation_time)
            if creation_time_num is None:
                continue
            result.append((f"Q{int(qid)}", creation_time_num, int(inlinks_last_evaluated)))
        return result

    async def count_known_inlinks_refresh_candidates(self) -> int:
        await self.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)
        deleted_mask = summary_bits.DELETED

        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT COUNT(*)
                FROM evaluation_cache ec
                WHERE ec.qid != 0
                  AND ec.creation_time IS NOT NULL
                  AND ec.inlinks_last_evaluated IS NOT NULL
                  AND (ec.summary & ?) != ?
                  AND (ec.summary & ?) = 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pubsub_sessions s
                      WHERE s.qid = ec.qid
                        AND s.qid != 0
                        AND s.wants_inlinks = 1
                        AND s.owner_id != 'inlinks'
                  )
                """,
                (n3_inlinks_mask, n3_inlinks_unknown, deleted_mask),
            )
            row = await cursor.fetchone()

        return int(row[0]) if row and row[0] is not None else 0

    async def list_inlinks_work_candidates(
        self,
        limit: int | None = None,
    ) -> list[tuple[str, int | None, int | None, int, bool]]:
        await self.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)
        deleted_mask = summary_bits.DELETED
        now = int(time.time())

        async with self._connect() as db:
            base_query = """
                WITH active_interest AS (
                    SELECT
                        s.qid AS qid,
                        COALESCE(SUM(COALESCE(s.priority, 0)), 0) AS active_priority
                    FROM pubsub_sessions s
                    WHERE s.qid != 0
                      AND s.wants_inlinks = 1
                      AND s.owner_id != 'inlinks'
                      AND s.expires_at > ? 
                    GROUP BY s.qid
                ),
                candidate_base AS (
                    SELECT
                        ec.qid,
                        ec.creation_time,
                        ec.inlinks_last_evaluated,
                        COALESCE(ai.active_priority, 0) AS active_priority,
                        CASE WHEN (ec.summary & ?) = ? THEN 1 ELSE 0 END AS is_unknown,
                        CASE
                            WHEN ec.creation_time IS NULL THEN 0
                            WHEN ? > ec.creation_time THEN ? - ec.creation_time
                            ELSE 0
                        END AS item_age_seconds,
                        CASE
                            WHEN ec.creation_time IS NULL OR ec.inlinks_last_evaluated IS NULL THEN NULL
                            WHEN ec.inlinks_last_evaluated > ec.creation_time THEN ec.inlinks_last_evaluated - ec.creation_time
                            ELSE 0
                        END AS age_at_last_refresh_seconds
                    FROM evaluation_cache ec
                    LEFT JOIN active_interest ai
                      ON ai.qid = ec.qid
                    WHERE ec.qid != 0
                      AND (ec.summary & ?) = 0
                ),
                scored_candidates AS (
                    SELECT
                        qid,
                        creation_time,
                        inlinks_last_evaluated,
                        active_priority,
                        is_unknown,
                        item_age_seconds,
                        age_at_last_refresh_seconds,
                        CASE
                            WHEN age_at_last_refresh_seconds IS NULL OR age_at_last_refresh_seconds <= 0 THEN
                                1.0 * item_age_seconds / 3600.0
                            ELSE
                                1.0 * item_age_seconds /
                                CASE
                                    WHEN age_at_last_refresh_seconds < 3600 THEN 3600
                                    WHEN age_at_last_refresh_seconds > 31536000 THEN 31536000
                                    ELSE age_at_last_refresh_seconds
                            END
                        END AS refresh_ratio
                    FROM candidate_base
                )
                SELECT
                    qid,
                    creation_time,
                    inlinks_last_evaluated,
                    active_priority,
                    is_unknown
                FROM scored_candidates
                ORDER BY
                    is_unknown DESC,
                    active_priority DESC,
                    refresh_ratio DESC,
                    item_age_seconds DESC,
                    qid ASC
            """
            params: list[object] = [
                now,
                n3_inlinks_mask,
                n3_inlinks_unknown,
                now,
                now,
                deleted_mask,
            ]
            if limit is None:
                cursor = await db.execute(base_query, params)
            else:
                cursor = await db.execute(f"{base_query}\nLIMIT ?", [*params, limit])
            rows = await cursor.fetchall()

        result: list[tuple[str, int | None, int | None, int, bool]] = []
        for qid, creation_time, inlinks_last_evaluated, active_priority, is_unknown in rows:
            creation_time_num = _to_epoch_seconds(creation_time)
            result.append(
                (
                    f"Q{int(qid)}",
                    creation_time_num,
                    None if inlinks_last_evaluated is None else int(inlinks_last_evaluated),
                    int(active_priority) if active_priority is not None else 0,
                    bool(is_unknown),
                )
            )
        return result

    async def count_inlinks_work_candidates(self) -> dict[str, int]:
        await self.initialize()

        n3_inlinks_mask = summary_bits.mask(NotabilityCriterion.N3_INLINKS)
        n3_inlinks_unknown = summary_bits.value(NotabilityCriterion.N3_INLINKS, NotabilityLevel.UNKNOWN)
        deleted_mask = summary_bits.DELETED
        now = int(time.time())

        async with self._connect() as db:
            cursor = await db.execute(
                """
                WITH active_interest AS (
                    SELECT
                        s.qid AS qid,
                        COALESCE(SUM(COALESCE(s.priority, 0)), 0) AS active_priority
                    FROM pubsub_sessions s
                    WHERE s.qid != 0
                      AND s.wants_inlinks = 1
                      AND s.owner_id != 'inlinks'
                      AND s.expires_at > ?
                    GROUP BY s.qid
                )
                SELECT
                    CASE WHEN (ec.summary & ?) = ? THEN 1 ELSE 0 END AS is_unknown,
                    CASE WHEN COALESCE(ai.active_priority, 0) > 0 THEN 1 ELSE 0 END AS has_interest,
                    COUNT(DISTINCT ec.qid) AS count
                FROM evaluation_cache ec
                LEFT JOIN active_interest ai
                  ON ai.qid = ec.qid
                WHERE ec.qid != 0
                  AND (ec.summary & ?) = 0
                  AND ((ec.summary & ?) = ? OR ec.inlinks_last_evaluated IS NOT NULL)
                GROUP BY is_unknown, has_interest
                """,
                (now, n3_inlinks_mask, n3_inlinks_unknown, deleted_mask, n3_inlinks_mask, n3_inlinks_unknown),
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

    async def touch_inlinks_last_evaluated_many(
        self,
        qids: Sequence[str | int],
        *,
        inlinks_last_evaluated: int,
    ) -> int:
        await self.initialize()

        normalized: list[int] = []
        seen: set[int] = set()
        for qid in qids:
            try:
                qid_num = self._parse_qid(qid)
            except ValueError:
                continue
            if qid_num in seen:
                continue
            seen.add(qid_num)
            normalized.append(qid_num)

        if not normalized:
            return 0

        started = time.perf_counter()
        updated = 0
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE

        async with self._write_guard():
            async with self._connect() as db:
                for chunk_start in range(0, len(normalized), chunk_size):
                    chunk = normalized[chunk_start : chunk_start + chunk_size]
                    placeholders = ", ".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"""
                        UPDATE evaluation_cache
                        SET inlinks_last_evaluated = ?,
                            last_updated = ?
                        WHERE qid IN ({placeholders})
                        RETURNING qid
                        """,
                        (inlinks_last_evaluated, inlinks_last_evaluated, *chunk),
                    )
                    rows = await cursor.fetchall()
                    updated += len(rows)
                await db.commit()

        self._warn_slow_write("touch_inlinks_last_evaluated_many", started, row_count=updated)
        return updated

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
        timestamp_sql = self._summary_update_timestamp_sql()

        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            for chunk in self._chunked(qid_nums):
                placeholders = ",".join("?" for _ in chunk)
                cursor = await db.execute(
                    f"""
                    UPDATE evaluation_cache
                    SET summary = (summary | ?) & ~?,
                        last_updated = {timestamp_sql}
                    WHERE qid IN ({placeholders})
                      AND ((summary | ?) & ~?) != summary
                    RETURNING qid, summary, last_updated
                    """,
                    (set_bits, clear_bits, *chunk, set_bits, clear_bits),
                )
                updated_rows = await cursor.fetchall()
                updated += len(updated_rows)
                for row in updated_rows:
                    event_rows.append((timestamp, int(row[0]), "summary_bits", int(row[1]), int(set_bits) | int(clear_bits)))
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

        async def count_level_distribution(expr: str) -> dict[str, int]:
            cursor = await db.execute(
                f"""
                SELECT
                    CASE
                        WHEN {expr} = {level_unknown} THEN 'unknown'
                        WHEN {expr} = {level_none} THEN 'none'
                        WHEN {expr} = {level_weak} THEN 'weak'
                        WHEN {expr} = {level_strong} THEN 'strong'
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
            return counts

        detected_criteria_counts: dict[str, dict[str, int]] = {}
        for criterion_key in summary_bits.direct_criteria():
            criterion = NotabilityCriterion(criterion_key)
            mask = summary_bits.mask(criterion)
            expr = f"""
                CASE
                    WHEN (summary & {mask}) = 0 THEN {level_unknown}
                    WHEN (summary & {mask}) = {summary_bits.value(criterion, NotabilityLevel.NONE)} THEN {level_none}
                    WHEN (summary & {mask}) = {summary_bits.value(criterion, NotabilityLevel.WEAK)} THEN {level_weak}
                    WHEN (summary & {mask}) = {summary_bits.value(criterion, NotabilityLevel.STRONG)} THEN {level_strong}
                    ELSE {level_unknown}
                END
            """
            detected_criteria_counts[criterion_key] = await count_level_distribution(expr)

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

            deduced_criteria_counts: dict[str, dict[str, int]] = {}
            for name, expr in (
                ("N2", n2_expr),
                ("N12", n12_expr),
                ("N3", n3_expr),
                ("N", n_expr),
            ):
                deduced_criteria_counts[name] = await count_level_distribution(expr)

        return {
            "entries": total_rows,
            "flags": flag_counts,
            "criteria": {
                **detected_criteria_counts,
                **deduced_criteria_counts,
            },
            "criteria_detected": detected_criteria_counts,
            "criteria_deduced": deduced_criteria_counts,
        }

    @staticmethod
    def _as_uint32(value: int, field_name: str) -> int:
        if not isinstance(value, int):
            raise ValueError(f"{field_name} must be an integer")
        if value < 0 or value > UINT32_MAX:
            raise ValueError(f"{field_name} must fit in uint32")
        return value

    @staticmethod
    def _as_uint64(value: int, field_name: str) -> int:
        if not isinstance(value, int):
            raise ValueError(f"{field_name} must be an integer")
        if value < 0 or value > 2**64 - 1:
            raise ValueError(f"{field_name} must fit in uint64")
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

    async def upsert_creation_metadata_many(self, items: Sequence[object]) -> int:
        return await creation_cache.upsert_creation_metadata_many(self, items)

    async def list_missing_creation_qids(self, limit: int | None = None) -> list[str]:
        return await creation_cache.list_missing_creation_qids(self, limit)

    async def count_missing_creation_qids(self) -> int:
        return await creation_cache.count_missing_creation_qids(self)

    async def list_creation_metadata(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        creator_actor_ids: Sequence[object] | None = None,
    ) -> list[CreationMetadata]:
        return await creation_cache.list_creation_metadata(
            self,
            start=start,
            end=end,
            creator_actor_ids=creator_actor_ids,
        )

    async def get_creation_metadata_many(
        self,
        qids: Sequence[object],
    ) -> dict[str, CreationMetadata]:
        return await creation_cache.get_creation_metadata_many(self, qids)

    async def list_unknown_inlinks_qids(self, limit: int | None = None) -> list[str]:
        return await inlinks_cache.list_unknown_inlinks_qids(self, limit)

    async def count_unknown_inlinks_qids(self) -> int:
        return await inlinks_cache.count_unknown_inlinks_qids(self)

    async def list_known_inlinks_refresh_candidates(
        self,
        limit: int | None = None,
    ) -> list[tuple[str, str, int]]:
        return await inlinks_cache.list_known_inlinks_refresh_candidates(self, limit)

    async def count_known_inlinks_refresh_candidates(self) -> int:
        return await inlinks_cache.count_known_inlinks_refresh_candidates(self)


async def reset_main_cache(main_cache: str | Path) -> None:
    cache = EvaluationCache(main_cache)
    await cache.initialize()

    await cache.clear()
    async with cache._write_guard():
        async with cache._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute("DELETE FROM lookup_state")
            await db.commit()

    print("Reset main cache and flushed work queue")

CACHE = EvaluationCache()

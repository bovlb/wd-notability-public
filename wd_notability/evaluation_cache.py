from __future__ import annotations

import asyncio
import calendar
import logging
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any
import time
from pathlib import Path
from datetime import UTC, datetime

import pymysql

from wd_notability.creations import CREATIONS, CreationMetadata, _normalize_text
from wd_notability.db_env import credentials_from_env, require_env_value
import wd_notability.content.db_read as content_db_read
import wd_notability.content.db_write as content_db_write
from wd_notability.observability import ObservabilityStore
import wd_notability.creation_cache as creation_cache
import wd_notability.inlinks.cache as inlinks_cache
import wd_notability.inlinks.db_read as inlinks_db_read
import wd_notability.inlinks.report_db_read as inlinks_report_db_read
import wd_notability.cache_state as cache_state
import wd_notability.user_history as user_history
from wd_notability.item_trace import ItemTraceStore
from wd_notability.models import (
    QID,
    EvaluationResult,
    NotabilityCriterion,
    NotabilityLevel,
)
from wd_notability.interest import InterestStore

# Upper bound used when clamping unsigned cache fields.
UINT32_MAX = 2**32 - 1
# Default timeout for cache processing tasks when the environment does not override it.
DEFAULT_PROCESSING_TIMEOUT_SECONDS = 300
# Warn when a cache write exceeds this many seconds.
DEFAULT_SLOW_WRITE_WARNING_SECONDS = float(os.environ.get(
    "WD_NOTABILITY_SLOW_WRITE_WARNING_SECONDS", "1.0"))
# Default batch size for multi-row writes into the cache.
DEFAULT_WRITE_CHUNK_SIZE = 500
CONTENT_POLICY_UPDATED_AT_LOOKUP_STATE_KEY = cache_state.CONTENT_POLICY_UPDATED_AT_LOOKUP_STATE_KEY

logger = logging.getLogger(__name__)

LEVEL_NONE = int(NotabilityLevel.NONE)
LEVEL_PARTIAL_WEAK = int(NotabilityLevel.PARTIAL_WEAK)
LEVEL_PARTIAL_STRONG = int(NotabilityLevel.PARTIAL_STRONG)
LEVEL_WEAK = int(NotabilityLevel.WEAK)
LEVEL_UNKNOWN = int(NotabilityLevel.UNKNOWN)
LEVEL_STRONG = int(NotabilityLevel.STRONG)


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


def _to_epoch_seconds(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        dt = value.astimezone(
            UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(calendar.timegm(dt.utctimetuple()))
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


def _to_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, datetime):
        dt = value.astimezone(
            UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return int(calendar.timegm(dt.utctimetuple()))
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _to_utc_datetime_string(value: object) -> str | None:
    dt = _to_utc_datetime(value)
    if dt is None:
        return None
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


UserHistoryRecord = user_history.UserHistoryRecord


class EvaluationCache:
    """Cache for evaluation summaries, backed by MariaDB."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        processing_timeout_seconds: int | None = None,
    ):
        self._backend_name = "mariadb"
        self.db_path = ""
        self.database = require_env_value("TOOLSDB_DATABASE")
        self.host = require_env_value("TOOLSDB_HOST")
        timeout_from_env = int(os.environ.get(
            "WD_NOTABILITY_TASK_PROCESSING_TIMEOUT_SECONDS", DEFAULT_PROCESSING_TIMEOUT_SECONDS))
        timeout = timeout_from_env if processing_timeout_seconds is None else processing_timeout_seconds
        self.processing_timeout_seconds = max(1, int(timeout))
        self._initialized = False
        self._initialize_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self.observability = ObservabilityStore(self)
        self.item_trace = ItemTraceStore(self)
        self.interest = InterestStore(self)
        self.pubsub = self.interest

    async def initialize(self) -> None:
        if self._initialized:
            return

        async with self._initialize_lock:
            if self._initialized:
                return
            async with self._connect() as db:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS content_evaluation (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        last_updated DATETIME(6) NULL,
                        content_last_revid BIGINT UNSIGNED NULL,
                        redirect_target BIGINT UNSIGNED NULL,
                        has_sitelinks_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
                        has_claims_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
                        deleted TINYINT(1) NOT NULL DEFAULT 0,
                        n1 TINYINT UNSIGNED NOT NULL DEFAULT 0,
                        n2a TINYINT UNSIGNED NOT NULL DEFAULT 0,
                        n2b TINYINT UNSIGNED NOT NULL DEFAULT 0
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS recent_changes_cache (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        creation_time DATETIME(6) NULL,
                        creator_actor_id BIGINT UNSIGNED NULL,
                        recent_changes_last_revid BIGINT UNSIGNED NULL
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS content_deletion_events (
                        log_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        qid BIGINT UNSIGNED NOT NULL,
                        event_type VARCHAR(16) NOT NULL,
                        event_timestamp DATETIME(6) NOT NULL
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS inlinks_cache (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        inlinks_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        n3_inlinks TINYINT UNSIGNED NOT NULL DEFAULT 4,
                        inlinks_last_evaluated DATETIME(6) NULL
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS interest (
                        worker_id VARCHAR(255) NOT NULL,
                        qid BIGINT UNSIGNED NOT NULL,
                        priority INT NOT NULL DEFAULT 10,
                        wants_creation TINYINT(1) NOT NULL,
                        wants_content TINYINT(1) NOT NULL,
                        wants_inlinks TINYINT(1) NOT NULL,
                        PRIMARY KEY (worker_id, qid)
                    )
                    """
                )
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS item_trace_events (
                        ts DATETIME(6) NOT NULL,
                        qid BIGINT UNSIGNED NOT NULL,
                        event_type VARCHAR(32) NOT NULL,
                        worker_name VARCHAR(64) NOT NULL,
                        batch_id CHAR(36) NULL,
                        details JSON NOT NULL
                    )
                    """
                )
                await db.execute("CREATE INDEX IF NOT EXISTS interest_worker_id ON interest(worker_id)")
                await db.execute("CREATE INDEX IF NOT EXISTS interest_qid ON interest(qid)")
                await db.execute("CREATE INDEX IF NOT EXISTS content_evaluation_last_updated ON content_evaluation(last_updated)")
                await db.execute("CREATE INDEX IF NOT EXISTS content_evaluation_content_last_revid ON content_evaluation(content_last_revid)")
                await db.execute("CREATE INDEX IF NOT EXISTS inlinks_cache_inlinks_count ON inlinks_cache(inlinks_count)")
                await db.execute("CREATE INDEX IF NOT EXISTS inlinks_cache_inlinks_last_evaluated ON inlinks_cache(inlinks_last_evaluated)")
                await db.execute("CREATE INDEX IF NOT EXISTS recent_changes_cache_creation_time ON recent_changes_cache(creation_time)")
                await db.execute("CREATE INDEX IF NOT EXISTS recent_changes_cache_recent_changes_last_revid ON recent_changes_cache(recent_changes_last_revid)")
                await db.execute("CREATE INDEX IF NOT EXISTS content_deletion_events_qid_timestamp ON content_deletion_events(qid, event_timestamp)")
                await db.execute("CREATE INDEX IF NOT EXISTS content_deletion_events_timestamp ON content_deletion_events(event_timestamp)")
                await db.execute("CREATE INDEX IF NOT EXISTS item_trace_events_qid_ts ON item_trace_events(qid, ts)")
                await db.execute("CREATE INDEX IF NOT EXISTS item_trace_events_ts ON item_trace_events(ts)")
                await db.execute("CREATE INDEX IF NOT EXISTS item_trace_events_event_ts ON item_trace_events(event_type, ts)")
                cursor = await db.execute("SHOW COLUMNS FROM content_evaluation LIKE 'entitydata_last_revid'")
                if await cursor.fetchone() is not None:
                    cursor = await db.execute("SHOW COLUMNS FROM content_evaluation LIKE 'content_last_revid'")
                    if await cursor.fetchone() is None:
                        await db.execute("ALTER TABLE content_evaluation RENAME COLUMN entitydata_last_revid TO content_last_revid")
                cursor = await db.execute("SHOW TABLES LIKE 'pubsub_sessions'")
                if await cursor.fetchone() is not None:
                    cursor = await db.execute("SHOW COLUMNS FROM pubsub_sessions LIKE 'worker_id'")
                    has_worker_id = await cursor.fetchone() is not None
                    if self._backend_name == "mariadb":
                        if has_worker_id:
                            await db.execute(
                                """
                                INSERT INTO interest (
                                    worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                                )
                                SELECT
                                    worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                                FROM pubsub_sessions
                                WHERE qid != 0
                                ON DUPLICATE KEY UPDATE
                                    priority = VALUES(priority),
                                    wants_creation = VALUES(wants_creation),
                                    wants_content = VALUES(wants_content),
                                    wants_inlinks = VALUES(wants_inlinks)
                                """
                            )
                        else:
                            await db.execute(
                                """
                                INSERT INTO interest (
                                    worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                                )
                                SELECT
                                    CONCAT(owner_id, ':', session_id) AS worker_id,
                                    qid,
                                    priority,
                                    wants_creation,
                                    wants_content,
                                    wants_inlinks
                                FROM pubsub_sessions
                                WHERE qid != 0
                                ON DUPLICATE KEY UPDATE
                                    priority = VALUES(priority),
                                    wants_creation = VALUES(wants_creation),
                                    wants_content = VALUES(wants_content),
                                    wants_inlinks = VALUES(wants_inlinks)
                                """
                            )
                    else:
                        if has_worker_id:
                            await db.execute(
                                """
                                INSERT OR IGNORE INTO interest (
                                    worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                                )
                                SELECT
                                    worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                                FROM pubsub_sessions
                                WHERE qid != 0
                                """
                            )
                        else:
                            await db.execute(
                                """
                                INSERT OR IGNORE INTO interest (
                                    worker_id, qid, priority, wants_creation, wants_content, wants_inlinks
                                )
                                SELECT
                                    owner_id || ':' || session_id AS worker_id,
                                    qid,
                                    priority,
                                    wants_creation,
                                    wants_content,
                                    wants_inlinks
                                FROM pubsub_sessions
                                WHERE qid != 0
                                """
                            )
                    await db.execute("DROP TABLE pubsub_sessions")
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS worker_observability_log (
                        `timestamp` DATETIME(6) NOT NULL,
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
                await cache_state.ensure_schema(db)
                await user_history.ensure_schema(db)
                timestamp_migrations: list[tuple[str, str, str, str]] = [
                    ("content_evaluation", "last_updated",
                     "seconds_or_mw_ts", "NULL"),
                    ("recent_changes_cache", "creation_time",
                     "seconds_or_mw_ts", "NULL"),
                    ("content_deletion_events", "event_timestamp",
                     "microseconds", "NOT NULL"),
                    ("inlinks_cache", "inlinks_last_evaluated",
                     "seconds_or_mw_ts", "NULL"),
                    ("item_trace_events", "ts", "seconds_or_mw_ts", "NOT NULL"),
                    ("worker_observability_log", "`timestamp`",
                     "seconds_or_mw_ts", "NOT NULL"),
                ]
                for table_name, column_name, unit, nullability in timestamp_migrations:
                    cursor = await db.execute(
                        f"SHOW COLUMNS FROM {table_name} LIKE %s",
                        (column_name.strip("`"),),
                    )
                    row = await cursor.fetchone()
                    if row is None:
                        continue
                    column_type = str(row[1]).lower()
                    if column_type.startswith("datetime"):
                        continue
                    if unit == "microseconds":
                        expression = f"FROM_UNIXTIME({column_name} / 1000000.0)"
                    elif unit == "seconds_or_mw_ts":
                        expression = (
                            "CASE "
                            f"WHEN CHAR_LENGTH(CAST({column_name} AS CHAR)) = 14 "
                            f"AND CAST({column_name} AS CHAR) REGEXP '^[0-9]{{14}}$' "
                            f"THEN STR_TO_DATE(CAST({column_name} AS CHAR), '%%Y%%m%%d%%H%%i%%s') "
                            f"ELSE FROM_UNIXTIME({column_name}) "
                            "END"
                        )
                    else:
                        expression = f"FROM_UNIXTIME({column_name})"
                    await db.execute(
                        f"""
                        UPDATE {table_name}
                        SET {column_name} = {expression}
                        WHERE {column_name} IS NOT NULL
                        """
                    )
                    await db.execute(
                        f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} DATETIME(6) {nullability}"
                    )
                await db.execute(
                    f"""
                    CREATE OR REPLACE VIEW content_evaluation_view AS
                    SELECT
                        qid,
                        last_updated,
                        content_last_revid,
                        redirect_target,
                        has_sitelinks_count,
                        has_claims_count,
                        deleted,
                        n1,
                        n2a,
                        n2b,
                        CASE
                            WHEN n1 = {LEVEL_UNKNOWN}
                              OR n2a = {LEVEL_UNKNOWN}
                              OR n2b = {LEVEL_UNKNOWN}
                            THEN {LEVEL_UNKNOWN}
                            WHEN n2a = {LEVEL_NONE} AND n2b = {LEVEL_NONE}
                            THEN {LEVEL_NONE}
                            WHEN n2a = {LEVEL_NONE}
                            THEN CASE
                                WHEN n2b = {LEVEL_STRONG} THEN {LEVEL_PARTIAL_STRONG}
                                ELSE {LEVEL_PARTIAL_WEAK}
                            END
                            WHEN n2b = {LEVEL_NONE}
                            THEN CASE
                                WHEN n2a = {LEVEL_STRONG} THEN {LEVEL_PARTIAL_STRONG}
                                ELSE {LEVEL_PARTIAL_WEAK}
                            END
                            ELSE LEAST(n2a, n2b)
                        END AS n2,
                        GREATEST(
                            n1,
                            CASE
                                WHEN n1 = {LEVEL_UNKNOWN}
                                  OR n2a = {LEVEL_UNKNOWN}
                                  OR n2b = {LEVEL_UNKNOWN}
                                THEN {LEVEL_UNKNOWN}
                                WHEN n2a = {LEVEL_NONE} AND n2b = {LEVEL_NONE}
                                THEN {LEVEL_NONE}
                                WHEN n2a = {LEVEL_NONE}
                                THEN CASE
                                    WHEN n2b = {LEVEL_STRONG} THEN {LEVEL_PARTIAL_STRONG}
                                    ELSE {LEVEL_PARTIAL_WEAK}
                                END
                                WHEN n2b = {LEVEL_NONE}
                                THEN CASE
                                    WHEN n2a = {LEVEL_STRONG} THEN {LEVEL_PARTIAL_STRONG}
                                    ELSE {LEVEL_PARTIAL_WEAK}
                                END
                                ELSE LEAST(n2a, n2b)
                            END
                        ) AS n12
                    FROM content_evaluation
                    """
                )
                cursor = await db.execute("SHOW TABLES LIKE 'content_deletion_events'")
                if await cursor.fetchone() is None:
                    await db.execute(
                        """
                        CREATE TABLE content_deletion_events (
                            log_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                            qid BIGINT UNSIGNED NOT NULL,
                            event_type VARCHAR(16) NOT NULL,
                            event_timestamp BIGINT UNSIGNED NOT NULL
                        )
                        """
                    )
                cursor = await db.execute("SHOW INDEX FROM content_deletion_events WHERE Key_name = 'content_deletion_events_qid_timestamp'")
                if await cursor.fetchone() is None:
                    await db.execute("CREATE INDEX content_deletion_events_qid_timestamp ON content_deletion_events(qid, event_timestamp)")
                cursor = await db.execute("SHOW INDEX FROM content_deletion_events WHERE Key_name = 'content_deletion_events_timestamp'")
                if await cursor.fetchone() is None:
                    await db.execute("CREATE INDEX content_deletion_events_timestamp ON content_deletion_events(event_timestamp)")
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS inlinks_cache (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        inlinks_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
                        n3_inlinks TINYINT UNSIGNED NOT NULL DEFAULT 2,
                        inlinks_last_evaluated BIGINT UNSIGNED NULL
                    )
                    """
                )
                cursor = await db.execute("SHOW COLUMNS FROM inlinks_cache LIKE 'inlinks_count'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE inlinks_cache ADD COLUMN inlinks_count BIGINT UNSIGNED NOT NULL DEFAULT 0")
                cursor = await db.execute("SHOW COLUMNS FROM inlinks_cache LIKE 'n3_inlinks'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE inlinks_cache ADD COLUMN n3_inlinks TINYINT UNSIGNED NOT NULL DEFAULT 2")
                cursor = await db.execute("SHOW COLUMNS FROM inlinks_cache LIKE 'inlinks_last_evaluated'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE inlinks_cache ADD COLUMN inlinks_last_evaluated BIGINT UNSIGNED NULL")
                cursor = await db.execute("SHOW COLUMNS FROM interest LIKE 'priority'")
                if await cursor.fetchone() is None:
                    await db.execute("ALTER TABLE interest ADD COLUMN priority INT NOT NULL DEFAULT 10")
                await db.commit()

            self._initialized = True

    async def _open_connection(self):
        """Open a connection with a busy timeout so reads never hang on writer locks."""
        credentials = credentials_from_env(
            "TOOLSDB_USER",
            "TOOLSDB_PASSWORD",
        )
        connection = pymysql.connect(
            user=credentials.user,
            password=credentials.password,
            host=self.host,
            port=int(require_env_value("TOOLSDB_PORT")),
            database=self.database,
            charset="utf8mb4",
            # Keep read-only dispatcher/API lookups from holding an implicit
            # transaction open across polls. Explicit BEGIN IMMEDIATE is still
            # used for every write path.
            autocommit=True,
        )
        return _MariaDBConnectionAdapter(connection)

    async def close(self) -> None:
        return None

    async def checkpoint_wal(self, *, truncate: bool = True) -> tuple[int, int, int] | None:
        return None

    @asynccontextmanager
    async def _connect(self):
        connection = await self._open_connection()
        try:
            yield connection
        finally:
            close = getattr(connection, "close", None)
            if close is not None:
                result = close()
                if asyncio.iscoroutine(result):
                    await result

    def _warn_slow_write(self, operation: str, started: float, *, row_count: int | None = None) -> None:
        elapsed = time.perf_counter() - started
        if elapsed < DEFAULT_SLOW_WRITE_WARNING_SECONDS:
            return

        extra = f", rows={row_count}" if row_count is not None else ""
        logger.warning("Slow cache write: %s took %.3fs%s",
                       operation, elapsed, extra)

    def _write_guard(self):
        return self._write_lock

    @staticmethod
    def _chunked(values: Sequence[object], size: int = DEFAULT_WRITE_CHUNK_SIZE) -> list[list[object]]:
        if size < 1:
            raise ValueError("size must be at least 1")
        return [list(values[index: index + size]) for index in range(0, len(values), size)]

    @staticmethod
    def _normalize_owner_id(owner_id: str) -> str:
        owner = owner_id.strip().lower()
        if owner not in {"gadget", "report", "inlinks", "web"}:
            raise ValueError(
                "owner_id must be gadget, report, inlinks, or web")
        return owner

    @staticmethod
    def _content_policy_join_clause() -> str:
        return """
            LEFT JOIN lookup_state policy
              ON policy.`key` = 'content_policy_updated_at'
        """

    @staticmethod
    def _redirect_target_join_clause() -> str:
        return """
            LEFT JOIN content_evaluation target_ce
              ON target_ce.qid = ce.redirect_target
        """

    @staticmethod
    def _content_policy_stale_expr() -> str:
        return """
            (
                policy.value IS NOT NULL
                AND (
                    ce.last_updated IS NULL
                    OR ce.last_updated < STR_TO_DATE(policy.value, '%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ')
                )
            )
        """

    @staticmethod
    def _redirect_target_stale_expr() -> str:
        return """
            (
                ce.redirect_target IS NOT NULL
                AND (
                    ce.last_updated IS NULL
                    OR (
                        target_ce.last_updated IS NOT NULL
                        AND ce.last_updated < target_ce.last_updated
                    )
                )
            )
        """

    async def clear(self) -> None:
        await self.initialize()

        await self.clear_interest()

        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                await db.execute("DELETE FROM content_evaluation")
                await db.execute("DELETE FROM recent_changes_cache")
                await db.execute("DELETE FROM content_deletion_events")
                await db.execute("DELETE FROM inlinks_cache")
                await db.execute("DELETE FROM item_trace_events")
                await db.execute("DELETE FROM worker_observability_log")
                await db.commit()

    async def clear_interest(self) -> int:
        await self.initialize()

        started = time.perf_counter()
        deleted = 0
        async with self._write_guard():
            async with self._connect() as db:
                while True:
                    cursor = await db.execute("DELETE FROM interest LIMIT 5000")
                    batch_deleted = max(0, int(cursor.rowcount))
                    deleted += batch_deleted
                    if batch_deleted == 0:
                        break
                    await db.commit()
        self._warn_slow_write("clear_interest", started, row_count=deleted)
        return deleted

    @staticmethod
    def _source_lookup_state_keys(source: str) -> tuple[str, ...]:
        return ()

    async def reset_sources(self, sources: Sequence[str]) -> int:
        await self.initialize()

        normalized_sources = [str(source).strip().lower()
                              for source in sources if str(source).strip()]
        if not normalized_sources:
            return 0

        clear_bits = 0
        lookup_state_keys: set[str] = set()
        clear_content_evaluation = False
        clear_inlinks_cache = False
        for source in normalized_sources:
            lookup_state_keys.update(self._source_lookup_state_keys(source))
            if source == "content":
                clear_content_evaluation = True
            if source == "inlinks":
                clear_inlinks_cache = True

        started = time.perf_counter()
        updated = 0

        if "interest" in normalized_sources:
            updated += await self.clear_interest()

        if clear_content_evaluation or lookup_state_keys or clear_inlinks_cache:
            async with self._write_guard():
                async with self._connect() as db:
                    await db.execute("BEGIN IMMEDIATE")
                    if clear_content_evaluation:
                        cursor = await db.execute("DELETE FROM content_evaluation")
                        updated += max(0, int(cursor.rowcount))

                    if lookup_state_keys:
                        placeholders = ", ".join(
                            "?" for _ in lookup_state_keys)
                        await db.execute(
                            f"DELETE FROM lookup_state WHERE `key` IN ({placeholders})",
                            tuple(sorted(lookup_state_keys)),
                        )
                    if clear_inlinks_cache:
                        cursor = await db.execute("DELETE FROM inlinks_cache")
                        updated += max(0, int(cursor.rowcount))

                    await db.commit()

        self._warn_slow_write("reset_sources", started, row_count=updated)
        return updated

    async def upsert_content_many(
        self,
        items: list[object],
    ) -> list[tuple[str, int]]:
        return await content_db_write.upsert_content_many(self, items)

    async def upsert_creation_metadata_many(self, items: Sequence[object]) -> int:
        return await creation_cache.upsert_creation_metadata_many(self, items)

    async def upsert_inlinks_many(self, items: Sequence[object]) -> list[tuple[str, int]]:
        await self.initialize()

        if not items:
            return []

        normalized: list[tuple[int, int, int, int]] = []
        seen: set[int] = set()
        for item in items:
            qid = getattr(item, "qid")
            qid_num = self._parse_qid(qid)
            if qid_num in seen:
                continue
            seen.add(qid_num)
            level = getattr(item, "n3_inlinks")
            inlinks_count_value = getattr(item, "inlinks_count", None)
            if inlinks_count_value is None:
                inlinks_count_value = 1 if bool(
                    getattr(item, "has_inlinks", False)) else 0
            inlinks_count = max(0, int(inlinks_count_value))
            inlinks_last_evaluated_value = getattr(
                item, "inlinks_last_evaluated", None)
            if inlinks_last_evaluated_value is None:
                inlinks_last_evaluated_value = datetime.now(UTC)
            normalized.append(
                (
                    qid_num,
                    int(level),
                    inlinks_count,
                    _to_utc_datetime(inlinks_last_evaluated_value),
                )
            )

        if not normalized:
            return []

        started = time.perf_counter()
        changed_rows: list[tuple[str, int]] = []
        async with self._write_guard():
            async with self._connect() as db:
                for chunk in self._chunked(normalized):
                    await db.execute("BEGIN IMMEDIATE")
                    values_sql = ", ".join(
                        "(%s, %s, %s, %s)" for _ in chunk
                    ) if self._backend_name == "mariadb" else ", ".join(
                        "(?, ?, ?, ?)" for _ in chunk
                    )
                    params: list[int] = []
                    for qid_num, n3_inlinks, inlinks_count, inlinks_last_evaluated in chunk:
                        params.extend(
                            [qid_num, inlinks_count, n3_inlinks, inlinks_last_evaluated])
                    if self._backend_name == "mariadb":
                        cursor = await db.execute(
                            f"""
                            INSERT INTO inlinks_cache (
                                qid, inlinks_count, n3_inlinks, inlinks_last_evaluated
                            )
                            VALUES {values_sql}
                            ON DUPLICATE KEY UPDATE
                                inlinks_count = VALUES(inlinks_count),
                                n3_inlinks = VALUES(n3_inlinks),
                                inlinks_last_evaluated = VALUES(inlinks_last_evaluated)
                            RETURNING qid, n3_inlinks
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        changed_rows.extend(
                            (f"Q{int(row[0])}", int(row[1])) for row in rows)
                    else:
                        cursor = await db.execute(
                            f"""
                            INSERT INTO inlinks_cache (
                                qid, inlinks_count, n3_inlinks, inlinks_last_evaluated
                            )
                            VALUES {values_sql}
                            ON CONFLICT(qid) DO UPDATE SET
                                inlinks_count = excluded.inlinks_count,
                                n3_inlinks = excluded.n3_inlinks,
                                inlinks_last_evaluated = excluded.inlinks_last_evaluated
                            RETURNING qid, n3_inlinks
                            """,
                            params,
                        )
                        rows = await cursor.fetchall()
                        changed_rows.extend(
                            (f"Q{int(row[0])}", int(row[1])) for row in rows)
                    await db.commit()

        self._warn_slow_write("upsert_inlinks_many",
                              started, row_count=len(normalized))
        return changed_rows

    @staticmethod
    def _content_counts_from_item(item: object) -> tuple[int, int, int, int, int, int]:
        has_sitelinks_value = getattr(item, "has_sitelinks_count", None)
        if has_sitelinks_value is None:
            has_sitelinks_value = 1 if bool(
                getattr(item, "has_sitelinks", False)) else 0
        has_claims_value = getattr(item, "has_claims_count", None)
        if has_claims_value is None:
            has_claims_value = 1 if bool(
                getattr(item, "has_claims", False)) else 0
        deleted_value = 1 if bool(getattr(item, "is_deleted", False)) else 0
        return (
            max(0, int(has_sitelinks_value)),
            max(0, int(has_claims_value)),
            deleted_value,
            int(getattr(item, "n1")),
            int(getattr(item, "n2a")),
            int(getattr(item, "n2b")),
        )

    @staticmethod
    def _optional_uint32(value: object, field_name: str) -> int | None:
        if value is None:
            return None
        return EvaluationCache._as_uint32(value, field_name)

    def _summary_update_timestamp_sql(self) -> str:
        return "CURRENT_TIMESTAMP(6)"

    @staticmethod
    def _normalize_qid(qid: str | int) -> str:
        return f"Q{EvaluationCache._parse_qid(qid)}"

    async def update_recent_changes_last_revids(self, qids: dict[str | int, int]) -> int:
        return await creation_cache.upsert_recent_changes_last_revids(self, qids)

    async def clear_content_last_revids(self, qids: Sequence[str | int]) -> int:
        return await content_db_write.clear_content_last_revids(self, qids)

    async def upsert_content_deletion_events(self, events: Sequence[tuple[str | int, int, str, int]]) -> int:
        return await content_db_write.upsert_content_deletion_events(self, events)

    async def list_missing_creation_qids(self, limit: int | None = None) -> list[str]:
        return await creation_cache.list_missing_creation_qids(self, limit)

    async def list_stale_content_qids(self, limit: int | None = None) -> list[str]:
        return await content_db_read.list_stale_content_qids(self, limit)

    async def is_stale_content_qid(self, qid: str | int) -> bool | None:
        staleness = await self.get_content_staleness_for_qids([qid])
        return staleness.get(self._normalize_qid(qid))

    async def get_content_staleness_for_qids(
        self,
        qids: Sequence[str | int],
    ) -> dict[str, bool]:
        return await content_db_read.get_content_staleness_for_qids(self, qids)

    async def count_stale_content_qids(self) -> int:
        return await content_db_read.count_stale_content_qids(self)

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

    async def get(self, qid: QID) -> tuple[EvaluationResult | None, int | None, int | None]:
        await self.initialize()

        rows = await self.get_many([qid])
        row = rows.get(self._normalize_qid(qid))
        if row is None:
            return None, None, None
        return row, row.content_last_revid, row.recent_changes_last_revid

    async def get_many(self, qids: list[str | int]) -> dict[str, EvaluationResult]:
        return await content_db_read.get_many(self, qids)

    async def get_many_with_creation_metadata(
        self,
        qids: list[str | int],
    ) -> tuple[dict[str, EvaluationResult], dict[str, CreationMetadata]]:
        return await content_db_read.get_many_with_creation_metadata(self, qids)

    async def list_qids(self) -> list[str]:
        return await content_db_read.list_qids(self)

    async def list_unknown_inlinks_qids(self, limit: int | None = None) -> list[str]:
        return await inlinks_report_db_read.list_unknown_inlinks_qids(self, limit)

    async def count_unknown_inlinks_qids(self) -> int:
        return await inlinks_report_db_read.count_unknown_inlinks_qids(self)

    async def list_known_inlinks_refresh_candidates(
        self,
        limit: int | None = None,
    ) -> list[tuple[str, str, int]]:
        return await inlinks_report_db_read.list_known_inlinks_refresh_candidates(self, limit)

    async def count_known_inlinks_refresh_candidates(self) -> int:
        return await inlinks_report_db_read.count_known_inlinks_refresh_candidates(self)

    async def list_inlinks_work_candidates(
        self,
        limit: int | None = None,
    ) -> list[tuple[str, int | None, int, bool]]:
        return await inlinks_db_read.list_inlinks_work_candidates(self, limit)

    async def count_inlinks_work_candidates(self) -> dict[str, int]:
        return await inlinks_db_read.count_inlinks_work_candidates(self)

    async def touch_inlinks_last_evaluated_many(
        self,
        qids: Sequence[str | int],
        *,
        inlinks_last_evaluated: datetime | int | float | str,
    ) -> int:
        await self.initialize()

        timestamp_value = _to_utc_datetime(inlinks_last_evaluated)
        if timestamp_value is None:
            raise ValueError(
                "inlinks_last_evaluated must be a UTC datetime, epoch seconds, or ISO-8601 value")

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
                    chunk = normalized[chunk_start: chunk_start + chunk_size]
                    placeholders = ", ".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"""
                        UPDATE inlinks_cache
                        SET inlinks_last_evaluated = ?
                        WHERE qid IN ({placeholders})
                        RETURNING qid
                        """,
                        (timestamp_value, *chunk),
                    )
                    rows = await cursor.fetchall()
                    updated += len(rows)
                await db.commit()

        self._warn_slow_write(
            "touch_inlinks_last_evaluated_many", started, row_count=updated)
        return updated

    async def delete_inlinks_many(self, qids: Sequence[str | int]) -> int:
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
        deleted = 0
        async with self._write_guard():
            async with self._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                for chunk in self._chunked(normalized):
                    placeholders = ", ".join("?" for _ in chunk)
                    cursor = await db.execute(
                        f"DELETE FROM inlinks_cache WHERE qid IN ({placeholders})",
                        chunk,
                    )
                    deleted += max(0, int(cursor.rowcount))
                await db.commit()

        self._warn_slow_write("delete_inlinks_many",
                              started, row_count=deleted)
        return deleted

    async def stats(self) -> dict[str, int | None | str]:
        return await content_db_read.stats(self)

    async def breakdown(self) -> dict[str, Any]:
        return await content_db_read.breakdown(self)

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
    def _parse_qid(cls, qid: str | int) -> int:
        if isinstance(qid, int):
            return cls._as_uint32(qid, "qid")

        if not isinstance(qid, str) or len(qid) < 2 or qid[0] != "Q" or not qid[1:].isdigit():
            raise ValueError(f"qid must look like Q42, got {qid!r}")

        return cls._as_uint32(int(qid[1:]), "qid")


async def reset_main_cache(
    main_cache: str | Path | None,
    *,
    sources: Sequence[str] | None = None,
    user_history_only: bool = False,
    content_policy_updated_at: object | None = None,
) -> None:
    cache = EvaluationCache(
        main_cache) if main_cache is not None else EvaluationCache()
    await cache.initialize()

    if user_history_only:
        deleted = await user_history.clear_user_history(cache)
        print(f"Reset user history table ({deleted} row(s) deleted)")
        if content_policy_updated_at is not None:
            timestamp = await cache_state.set_content_policy_updated_at(cache, content_policy_updated_at)
            print(
                "Set content policy cutoff to "
                f"{timestamp.isoformat(timespec='microseconds').replace('+00:00', 'Z')}"
            )
        return

    if sources:
        normalized_sources = {str(source).strip().lower()
                              for source in sources if str(source).strip()}
        updated = await cache.reset_sources(sources)
        print(
            f"Reset {len(normalized_sources)} source(s) in the main cache ({updated} row(s) updated)")
        if content_policy_updated_at is not None:
            timestamp = await cache_state.set_content_policy_updated_at(cache, content_policy_updated_at)
            print(
                "Set content policy cutoff to "
                f"{timestamp.isoformat(timespec='microseconds').replace('+00:00', 'Z')}"
            )
        return

    if content_policy_updated_at is not None:
        timestamp = await cache_state.set_content_policy_updated_at(cache, content_policy_updated_at)
        print(
            "Set content policy cutoff to "
            f"{timestamp.isoformat(timespec='microseconds').replace('+00:00', 'Z')}"
        )
        return

    await cache.clear()
    await cache_state.clear_lookup_state(cache)

    print("Reset main cache and flushed work queue")

# Shared cache instance used across the application.


# Deprecated: use worker-local cache objects instead.
CACHE = EvaluationCache()

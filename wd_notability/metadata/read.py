from __future__ import annotations

import calendar
import json
import os
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from wd_notability import cache_state
from wd_notability.replica_connection import connect_replica

if TYPE_CHECKING:
    from collections.abc import Sequence

RECENT_CHANGES_WORKER_REWIND_SECONDS = 86400.0
RECENT_CHANGES_REPLICA_QUERY_LIMIT = 1000
RECENT_CHANGES_LOOKUP_STATE_KEY = "recent_changes_worker_cursor"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True, frozen=True)
class ReplicaConfig:
    enabled: bool
    host: str
    port: int
    database: str
    defaults_file: Path

    @classmethod
    def from_env(cls) -> "ReplicaConfig":
        defaults_file = Path(
            os.getenv(
                "WD_NOTABILITY_RECENT_CHANGES_REPLICA_DEFAULTS_FILE",
                os.path.expanduser("~/replica.my.cnf"),
            )
        )
        has_replica_env = all(
            os.getenv(name)
            for name in ("REPLICADB_HOST", "REPLICADB_PORT", "REPLICADB_DATABASE")
        )
        return cls(
            enabled=_env_flag("WD_NOTABILITY_RECENT_CHANGES_REPLICA_ENABLED", default=has_replica_env),
            host=os.getenv("REPLICADB_HOST", ""),
            port=int(os.getenv("REPLICADB_PORT", "0") or 0),
            database=os.getenv("REPLICADB_DATABASE", ""),
            defaults_file=defaults_file,
        )


def _normalize_qid(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    value = value.strip().upper()
    return value if value.startswith("Q") and value[1:].isdigit() else None


def _parse_replica_timestamp(timestamp: object) -> float | None:
    if isinstance(timestamp, bytes):
        try:
            timestamp = timestamp.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(timestamp, str):
        return None
    text = timestamp.strip()
    if len(text) < 14 or not text[:14].isdigit():
        return None
    try:
        return float(calendar.timegm(time.strptime(text[:14], "%Y%m%d%H%M%S")))
    except (ValueError, OverflowError):
        return None


def _format_replica_timestamp(epoch_seconds: float) -> str:
    return time.strftime("%Y%m%d%H%M%S", time.gmtime(epoch_seconds))


async def load_recent_changes_state() -> tuple[float | None, int | None, float | None]:
    from wd_notability.evaluation_cache import CACHE
    payload = await cache_state.get_lookup_state(CACHE, RECENT_CHANGES_LOOKUP_STATE_KEY)
    if not payload:
        return None, None, None
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        return None, None, None
    if not isinstance(data, dict):
        return None, None, None
    cursor_ts = _parse_replica_timestamp(data.get("rc_timestamp"))
    cursor_id = data.get("rc_id")
    try:
        cursor_id_num = int(cursor_id) if cursor_id is not None else None
    except (TypeError, ValueError):
        cursor_id_num = None
    creation_ts = _parse_replica_timestamp(data.get("creation_timestamp"))
    return cursor_ts, cursor_id_num, creation_ts


async def save_recent_changes_state(
    cursor_timestamp: float | None,
    cursor_id: int | None,
    creation_timestamp: float | None,
) -> None:
    from wd_notability.evaluation_cache import CACHE
    payload = json.dumps(
        {
            "rc_timestamp": None if cursor_timestamp is None else _format_replica_timestamp(cursor_timestamp),
            "rc_id": cursor_id,
            "creation_timestamp": None if creation_timestamp is None else _format_replica_timestamp(creation_timestamp),
        }
    )
    await cache_state.set_lookup_state(CACHE, RECENT_CHANGES_LOOKUP_STATE_KEY, payload)


class RecentChangesReplicaSource:
    def __init__(self) -> None:
        self._config = ReplicaConfig.from_env()

    @staticmethod
    def _pymysql_module():
        try:
            import pymysql  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError(
                "Recent changes replica access requires the optional 'pymysql' Python package."
            ) from exc
        return pymysql

    def _connect_replica(self):
        pymysql = self._pymysql_module()
        return connect_replica(
            pymysql,
            host=self._config.host,
            port=self._config.port,
            database=self._config.database,
        )

    def fetch_recent_changes(
        self,
        *,
        start_epoch: float,
        start_rc_id: int = 0,
        limit: int = RECENT_CHANGES_REPLICA_QUERY_LIMIT,
    ) -> tuple[list[dict[str, object]], tuple[float | None, int | None]]:
        if not self._config.enabled:
            raise RuntimeError("Recent changes replica access is disabled or unavailable")

        start_timestamp = _format_replica_timestamp(start_epoch)
        query = """
            SELECT
                rc_id,
                rc_timestamp,
                rc_title,
                rc_actor,
                rc_this_oldid,
                rc_last_oldid,
                rc_source,
                rc_log_type
            FROM recentchanges
            WHERE rc_namespace = 0
              AND (rc_log_type IS NULL OR rc_log_type <> 'delete')
              AND (
                    rc_timestamp > %s
                 OR (rc_timestamp = %s AND rc_id > %s)
              )
            ORDER BY rc_timestamp ASC, rc_id ASC
            LIMIT %s
        """

        changes: list[dict[str, object]] = []
        last_cursor: tuple[float | None, int | None] = (None, None)
        with closing(self._connect_replica()) as db:
            cursor = db.cursor()
            cursor.execute(query, (start_timestamp, start_timestamp, start_rc_id, limit))
            for rc_id, rc_timestamp, title, rc_actor, this_oldid, old_revid, rc_source, rc_log_type in cursor.fetchall():
                timestamp_epoch = _parse_replica_timestamp(rc_timestamp)
                normalized_qid = _normalize_qid(title)
                if timestamp_epoch is None or normalized_qid is None:
                    continue
                try:
                    creator_actor_id_num = int(rc_actor)
                except (TypeError, ValueError):
                    creator_actor_id_num = None
                try:
                    old_revid_num = int(old_revid)
                except (TypeError, ValueError):
                    old_revid_num = None
                changes.append(
                    {
                        "title": normalized_qid,
                        "creator_actor_id": creator_actor_id_num,
                        "this_oldid": int(this_oldid) if isinstance(this_oldid, int) else None,
                        "revid": int(this_oldid) if isinstance(this_oldid, int) else None,
                        "old_revid": old_revid_num,
                        "rc_source": rc_source.decode("utf-8") if isinstance(rc_source, bytes) else rc_source,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp_epoch)),
                    }
                )
                try:
                    last_cursor = (timestamp_epoch, int(rc_id))
                except (TypeError, ValueError):
                    last_cursor = (timestamp_epoch, None)

        return changes, last_cursor


RECENT_CHANGES_REPLICA = RecentChangesReplicaSource()


async def count_recent_changes_backlog() -> int | None:
    if not RECENT_CHANGES_REPLICA._config.enabled:
        return None

    saved_cursor_ts, saved_cursor_id, _saved_creation_ts = await load_recent_changes_state()
    if saved_cursor_ts is None:
        start_epoch = max(0.0, time.time() - max(0.0, RECENT_CHANGES_WORKER_REWIND_SECONDS))
        start_rc_id = 0
    else:
        start_epoch = max(0.0, saved_cursor_ts)
        start_rc_id = max(0, saved_cursor_id or 0)

    start_timestamp = _format_replica_timestamp(start_epoch)
    query = """
        SELECT COUNT(*)
        FROM recentchanges
        WHERE rc_namespace = 0
          AND (
                rc_timestamp > %s
             OR (rc_timestamp = %s AND rc_id > %s)
          )
    """
    with closing(RECENT_CHANGES_REPLICA._connect_replica()) as db:
        cursor = db.cursor()
        cursor.execute(query, (start_timestamp, start_timestamp, start_rc_id))
        row = cursor.fetchone()

    if not row or row[0] is None:
        return 0
    try:
        return max(0, int(row[0]))
    except (TypeError, ValueError):
        return None

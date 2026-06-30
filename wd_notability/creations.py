from __future__ import annotations

import calendar
import os
from contextlib import closing
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wd_notability.replica_connection import connect_replica


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _is_qid(value: object) -> bool:
    return isinstance(value, str) and len(value) > 1 and value[0] == "Q" and value[1:].isdigit()


def _normalize_qid(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None

    value = value.strip().upper()
    return value if _is_qid(value) else None


def _normalize_text(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    text = value.strip()
    return text or None


def _normalize_creators(creators: Iterable[object] | None) -> list[str]:
    if creators is None:
        return []
    deduped: list[str] = []
    seen: set[str] = set()
    for creator in creators:
        if not isinstance(creator, str):
            continue
        candidate = creator.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _mysql_timestamp_to_epoch_seconds(value: object) -> int | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if len(text) != 14 or not text.isdigit():
        return None
    try:
        dt = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None
    return int(calendar.timegm(dt.utctimetuple()))


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


@dataclass(slots=True, frozen=True)
class CreationsConfig:
    enabled: bool
    host: str
    port: int
    database: str
    defaults_file: Path

    @classmethod
    def from_env(cls) -> "CreationsConfig":
        defaults_file = Path(os.getenv("WD_NOTABILITY_CREATIONS_DEFAULTS_FILE", os.path.expanduser("~/replica.my.cnf")))
        return cls(
            enabled=_env_flag("WD_NOTABILITY_CREATIONS_ENABLED", default=defaults_file.exists()),
            host=os.getenv(
                "WD_NOTABILITY_REPLICA_HOST",
                os.getenv("WD_NOTABILITY_CREATIONS_HOST", "wikidatawiki.analytics.db.svc.wikimedia.cloud"),
            ),
            port=int(os.getenv("WD_NOTABILITY_CREATIONS_PORT", "3306")),
            database=os.getenv("WD_NOTABILITY_CREATIONS_DATABASE", "wikidatawiki_p"),
            defaults_file=defaults_file,
        )


@dataclass(slots=True, frozen=True)
class CreationRow:
    qid: str
    creator: str
    creation_time: int


@dataclass(slots=True, frozen=True)
class CreationMetadata:
    qid: str
    creator_actor_id: int
    creation_time: int


class CreationStore:
    DEFAULT_WINDOW_DAYS = 1

    def __init__(self) -> None:
        self._config = CreationsConfig.from_env()

    @classmethod
    def default_window(cls) -> tuple[str, str]:
        end = datetime.now(UTC)
        start = end - timedelta(days=cls.DEFAULT_WINDOW_DAYS)
        return (
            start.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            end.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )

    @staticmethod
    def _pymysql_module():
        try:
            import pymysql  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError("Creation population queries require the optional 'pymysql' package.") from exc
        return pymysql

    def _connect_replica(self):
        pymysql = self._pymysql_module()
        return connect_replica(
            pymysql,
            defaults_file=self._config.defaults_file,
            host=self._config.host,
            port=self._config.port,
            database=self._config.database,
        )

    @staticmethod
    def _creator_filter_sql(creators: list[str], *, column: str) -> tuple[str, list[object]]:
        if not creators:
            return "", []
        placeholders = ", ".join(["%s"] * len(creators))
        return f" AND {column} IN ({placeholders})", list(creators)

    def lookup_actor_ids(self, creators: list[str]) -> dict[str, int]:
        if not creators:
            return {}

        placeholders = ", ".join(["%s"] * len(creators))
        query = f"""
            SELECT actor_name, actor_id
            FROM actor
            WHERE actor_name IN ({placeholders})
        """

        with closing(self._connect_replica()) as db:
            cursor = db.cursor()
            cursor.execute(query, creators)
            result: dict[str, int] = {}
            for actor_name, actor_id in cursor.fetchall():
                if isinstance(actor_name, bytes):
                    try:
                        actor_name = actor_name.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                if not isinstance(actor_name, str):
                    continue
                try:
                    result[actor_name.strip()] = int(actor_id)
                except (TypeError, ValueError):
                    continue
            return result

    def lookup_actor_names(self, actor_ids: list[int]) -> dict[int, str]:
        if not actor_ids:
            return {}

        unique_ids: list[int] = []
        seen: set[int] = set()
        for actor_id in actor_ids:
            try:
                actor_id_num = int(actor_id)
            except (TypeError, ValueError):
                continue
            if actor_id_num in seen:
                continue
            seen.add(actor_id_num)
            unique_ids.append(actor_id_num)

        if not unique_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(unique_ids))
        query = f"""
            SELECT actor_id, actor_name
            FROM actor
            WHERE actor_id IN ({placeholders})
        """

        with closing(self._connect_replica()) as db:
            cursor = db.cursor()
            cursor.execute(query, unique_ids)
            result: dict[int, str] = {}
            for actor_id, actor_name in cursor.fetchall():
                if isinstance(actor_name, bytes):
                    try:
                        actor_name = actor_name.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                if not isinstance(actor_name, str):
                    continue
                try:
                    result[int(actor_id)] = actor_name.strip()
                except (TypeError, ValueError):
                    continue
            return result

    def fetch_creations(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        creators: Iterable[object] | None = None,
    ) -> list[CreationRow]:
        if not self._config.enabled:
            raise RuntimeError("Creation population queries are disabled or unavailable")

        if isinstance(start, str) and not start.strip():
            start = None
        if isinstance(end, str) and not end.strip():
            end = None
        if start is None or end is None:
            default_start, default_end = self.default_window()
            start = default_start if start is None else start
            end = default_end if end is None else end

        start_ts = _parse_iso8601_utc(start)
        end_ts = _parse_iso8601_utc(end)
        creator_list = _normalize_creators(creators)
        creator_actor_ids = self.lookup_actor_ids(creator_list)
        if creator_list and not creator_actor_ids:
            print(f"No actor IDs found for creators: {creator_list}")
            return []

        current_creator_filter_sql, current_creator_params = self._creator_filter_sql(
            list(creator_actor_ids.values()),
            column="r.rev_actor",
        )
        archive_creator_filter_sql, archive_creator_params = self._creator_filter_sql(
            list(creator_actor_ids.values()),
            column="a.ar_actor",
        )

        current_where = "WHERE p.page_namespace = 0 AND r.rev_parent_id = 0"
        archive_where = "WHERE a.ar_namespace = 0 AND a.ar_parent_id = 0"
        current_time_clauses: list[str] = []
        archive_time_clauses: list[str] = []
        if start_ts is not None:
            current_time_clauses.append("r.rev_timestamp >= %s")
            archive_time_clauses.append("a.ar_timestamp >= %s")
        if end_ts is not None:
            current_time_clauses.append("r.rev_timestamp < %s")
            archive_time_clauses.append("a.ar_timestamp < %s")
        current_time_suffix = f" AND {' AND '.join(current_time_clauses)}" if current_time_clauses else ""
        archive_time_suffix = f" AND {' AND '.join(archive_time_clauses)}" if archive_time_clauses else ""

        current_sql = f"""
            SELECT
                p.page_title AS qid,
                act.actor_name AS creator,
                r.rev_timestamp AS creation_time
            FROM page p
            JOIN revision r
              ON r.rev_page = p.page_id
            JOIN actor act
              ON act.actor_id = r.rev_actor
            {current_where}
            {current_time_suffix}
            {current_creator_filter_sql}
            ORDER BY r.rev_timestamp ASC, p.page_title ASC
        """
        archive_sql = f"""
            SELECT
                a.ar_title AS qid,
                act.actor_name AS creator,
                a.ar_timestamp AS creation_time
            FROM archive a
            JOIN actor act
              ON act.actor_id = a.ar_actor
            {archive_where}
            {archive_time_suffix}
            {archive_creator_filter_sql}
            ORDER BY a.ar_timestamp ASC, a.ar_title ASC
        """

        rows: list[CreationRow] = []
        with closing(self._connect_replica()) as db:
            cursor = db.cursor()
            current_params: list[object] = []
            archive_params: list[object] = []
            if start_ts is not None:
                current_params.append(start_ts)
                archive_params.append(start_ts)
            if end_ts is not None:
                current_params.append(end_ts)
                archive_params.append(end_ts)
            current_params.extend(current_creator_params)
            archive_params.extend(archive_creator_params)

            for query, query_params in ((current_sql, current_params), (archive_sql, archive_params)):
                debug_query = cursor.mogrify(query, query_params) if hasattr(cursor, "mogrify") else query
                print(f"Executing creation fetch query:\n{debug_query}")
                cursor.execute(query, query_params)
                for qid, creator, creation_time in cursor.fetchall():
                    normalized_qid = _normalize_qid(qid)
                    if normalized_qid is None:
                        continue
                    creator_text = _normalize_text(creator)
                    creation_time_epoch = _mysql_timestamp_to_epoch_seconds(creation_time)
                    if not creator_text or creation_time_epoch is None:
                        continue
                    rows.append(
                        CreationRow(
                            qid=normalized_qid,
                            creator=creator_text,
                            creation_time=creation_time_epoch,
                        )
                    )

        rows.sort(key=lambda row: (row.creation_time, row.qid))
        return rows

    def fetch_creation_metadata_many(self, qids: Iterable[object]) -> list[CreationMetadata]:
        if not self._config.enabled:
            raise RuntimeError("Creation population queries are disabled or unavailable")

        normalized_qids = []
        seen: set[str] = set()
        for qid in qids:
            normalized_qid = _normalize_qid(qid)
            if normalized_qid is None or normalized_qid in seen:
                continue
            seen.add(normalized_qid)
            normalized_qids.append(normalized_qid)

        if not normalized_qids:
            return []

        rows_by_qid: dict[str, CreationMetadata] = {}
        chunk_size = 500

        with closing(self._connect_replica()) as db:
            cursor = db.cursor()
            for start in range(0, len(normalized_qids), chunk_size):
                chunk = normalized_qids[start : start + chunk_size]
                placeholders = ", ".join(["%s"] * len(chunk))

                current_sql = f"""
                    SELECT
                        p.page_title AS qid,
                        act.actor_id AS creator_actor_id,
                        r.rev_timestamp AS creation_time
                    FROM page p
                    JOIN revision r
                      ON r.rev_page = p.page_id
                     AND r.rev_parent_id = 0
                    JOIN actor act
                      ON act.actor_id = r.rev_actor
                    WHERE p.page_namespace = 0
                      AND p.page_title IN ({placeholders})
                    ORDER BY r.rev_timestamp ASC, p.page_title ASC
                """
                archive_sql = f"""
                    SELECT
                        a.ar_title AS qid,
                        act.actor_id AS creator_actor_id,
                        a.ar_timestamp AS creation_time
                    FROM archive a
                    JOIN actor act
                      ON act.actor_id = a.ar_actor
                    WHERE a.ar_namespace = 0
                      AND a.ar_parent_id = 0
                      AND a.ar_title IN ({placeholders})
                    ORDER BY a.ar_timestamp ASC, a.ar_title ASC
                """

                for query in (current_sql, archive_sql):
                    cursor.execute(query, chunk)
                    for qid, creator_actor_id, creation_time in cursor.fetchall():
                        normalized_qid = _normalize_qid(qid)
                        if normalized_qid is None:
                            continue
                        creation_time_epoch = _mysql_timestamp_to_epoch_seconds(creation_time)
                        if creation_time_epoch is None:
                            continue
                        try:
                            creator_actor_id_num = int(creator_actor_id)
                        except (TypeError, ValueError):
                            continue
                        previous = rows_by_qid.get(normalized_qid)
                        if previous is None or creation_time_epoch < previous.creation_time:
                            rows_by_qid[normalized_qid] = CreationMetadata(
                                qid=normalized_qid,
                                creator_actor_id=creator_actor_id_num,
                                creation_time=creation_time_epoch,
                            )

        return sorted(rows_by_qid.values(), key=lambda row: (row.creation_time, row.qid))


CREATIONS = CreationStore()

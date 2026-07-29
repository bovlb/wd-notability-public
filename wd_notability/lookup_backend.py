from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Protocol

from wd_notability.db_env import (
    credentials_from_env,
    require_env_value,
)
from warnings import deprecated


_logger = logging.getLogger(__name__)
UINT32_MAX = 2**32 - 1


def _normalize_text(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _normalize_qid(value: object) -> str | None:
    text = _normalize_text(value)
    if text is None:
        return None
    if len(text) > 1 and text[0] == "Q" and text[1:].isdigit():
        try:
            if int(text[1:]) <= UINT32_MAX:
                return text
        except ValueError:
            return None
    return None


def _normalize_qid_int(value: object) -> int | None:
    if isinstance(value, int):
        return value if 0 <= value <= UINT32_MAX else None

    text = _normalize_text(value)
    if text is None:
        return None
    if len(text) > 1 and text[0] == "Q" and text[1:].isdigit():
        text = text[1:]
    elif not text.isdigit():
        return None

    try:
        qid = int(text)
    except ValueError:
        return None
    return qid if 0 <= qid <= UINT32_MAX else None


def _normalize_property_id(value: object) -> str | None:
    text = _normalize_text(value)
    if text is None:
        return None
    if len(text) > 1 and text[0] == "P" and text[1:].isdigit():
        return text
    return None


def _warn_dropped_rows(context: str, dropped: int, total: int | None = None) -> None:
    if dropped <= 0:
        return
    if total is None:
        _logger.warning(
            "Lookup cache %s dropped %d malformed row(s)", context, dropped)
        return
    _logger.warning(
        "Lookup cache %s dropped %d malformed row(s) out of %d", context, dropped, total)


def _normalize_qid_query_list(qids: Iterable[object]) -> tuple[list[str], int]:
    unique_qids = list(dict.fromkeys(qids))
    normalized: list[str] = []
    dropped = 0
    for qid in unique_qids:
        qid_text = _normalize_qid(qid)
        if qid_text is None:
            dropped += 1
            continue
        normalized.append(qid_text)
    return normalized, dropped


def _normalize_qid_query_int_list(qids: Iterable[object]) -> tuple[list[int], int]:
    unique_qids = list(dict.fromkeys(qids))
    normalized: list[int] = []
    dropped = 0
    seen: set[int] = set()
    for qid in unique_qids:
        qid_num = _normalize_qid_int(qid)
        if qid_num is None:
            dropped += 1
            continue
        if qid_num in seen:
            continue
        seen.add(qid_num)
        normalized.append(qid_num)
    return normalized, dropped


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start: start + size]


def _qid_text_from_int(value: object) -> str | None:
    qid_num = _normalize_qid_int(value)
    if qid_num is None:
        return None
    return f"Q{qid_num}"


@dataclass(frozen=True)
class LookupSnapshot:
    namespaces_by_site: dict[str, dict[str, int]]
    site_api_urls: dict[str, str]
    property_instances_by_qid: dict[str, set[str]]


class LookupBackend(ABC):
    @abstractmethod
    def ensure_schema(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def state_token(self) -> object | None:
        raise NotImplementedError

    @abstractmethod
    def load_snapshot(self) -> LookupSnapshot:
        raise NotImplementedError

    @abstractmethod
    @deprecated("Use worker-local methods instead")
    def get_osm_usage(self, qids: Iterable[str] | None = None) -> dict[str, dict[str, int]]:
        raise NotImplementedError

    @abstractmethod
    def get_sdc_usage(self, qids: Iterable[str] | None = None) -> dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
        raise NotImplementedError

    @abstractmethod
    def get_external_usage(self, qids: Iterable[str]) -> dict[str, dict[str, object]]:
        raise NotImplementedError

    @abstractmethod
    def replace_namespace_data(
        self,
        *,
        namespaces_by_site: dict[str, dict[str, int]],
        site_api_urls: dict[str, str],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def replace_property_instances(self, property_instances_by_qid: dict[str, list[str] | set[str]]) -> None:
        raise NotImplementedError

    @abstractmethod
    @deprecated("Use worker-local methods instead")
    def replace_osm_usage(self, osm_usage_by_qid: dict[str, dict[str, int]]) -> None:
        raise NotImplementedError

    @abstractmethod
    @deprecated("Use worker-local methods instead")
    def replace_sdc_usage(self, sdc_usage_by_qid: dict[str, int]) -> None:
        raise NotImplementedError

    @abstractmethod
    @deprecated("Use worker-local methods instead")
    def replace_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    @deprecated("Use worker-local methods instead")
    def upsert_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_lookup_state(self, key: str) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def set_lookup_state(self, key: str, value: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def assert_ready(self, required_property_qids: Iterable[str] = ()) -> None:
        raise NotImplementedError


class _DbConnection(Protocol):
    def cursor(self): ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


class MariaDBLookupBackend(LookupBackend):
    """Toolforge-oriented MariaDB backend."""

    _QUERY_CHUNK_SIZE = 500
    _BINARY_TEXT_KEY = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"

    def __init__(
        self,
        database: str,
        *,
        readonly: bool = False,
    ) -> None:
        self.database = database
        self.host = require_env_value("TOOLSDB_HOST")
        self.port = int(require_env_value("TOOLSDB_PORT"))
        self.readonly = readonly

    @staticmethod
    def _pymysql_module():
        try:
            import pymysql  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError(
                "MariaDB backend requires the optional 'pymysql' Python package."
            ) from exc
        return pymysql

    def _connect(self):
        pymysql = self._pymysql_module()
        credentials = credentials_from_env(
            "TOOLSDB_USER",
            "TOOLSDB_PASSWORD",
        )
        return pymysql.connect(
            user=credentials.user,
            password=credentials.password,
            host=self.host,
            port=self.port,
            database=self.database,
            charset="utf8mb4",
        )

    def ensure_schema(self) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS namespace_prefixes (
                    site_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    prefix VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    ns_id INT NOT NULL,
                    PRIMARY KEY (site_key, prefix)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS site_api_urls (
                    site_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                    api_url TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                ALTER TABLE namespace_prefixes
                    MODIFY site_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    MODIFY prefix VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL
                """
            )
            cursor.execute(
                """
                ALTER TABLE site_api_urls
                    MODIFY site_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS property_instances (
                    class_qid VARCHAR(32) NOT NULL,
                    property_qid VARCHAR(32) NOT NULL,
                    PRIMARY KEY (class_qid, property_qid)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS osm_usage (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    count_all BIGINT NOT NULL DEFAULT 0,
                    count_nodes BIGINT NOT NULL DEFAULT 0,
                    count_ways BIGINT NOT NULL DEFAULT 0,
                    count_relations BIGINT NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS sdc_usage (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    usage_count BIGINT NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS wiki_subscribers (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS lookup_state (
                    `key` VARCHAR(255) NOT NULL PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            for table_name in ("osm_usage", "sdc_usage", "wiki_subscribers"):
                cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE 'qid'")
                row = cursor.fetchone()
                if row is None:
                    continue
                column_type = str(row[1]).lower()
                if column_type.startswith("bigint"):
                    continue
                staging_table = f"{table_name}_qid_migration"
                backup_table = f"{table_name}_qid_migration_old"
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE NOT (qid REGEXP '^[0-9]+$' OR qid REGEXP '^Q[0-9]+$')
                    """
                )
                malformed_rows = int(cursor.fetchone()[0] or 0)
                if malformed_rows:
                    _warn_dropped_rows(
                        f"{table_name} qid migration", malformed_rows)
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                if table_name == "osm_usage":
                    cursor.execute(
                        f"""
                        CREATE TABLE {staging_table} (
                            qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                            count_all BIGINT NOT NULL DEFAULT 0,
                            count_nodes BIGINT NOT NULL DEFAULT 0,
                            count_ways BIGINT NOT NULL DEFAULT 0,
                            count_relations BIGINT NOT NULL DEFAULT 0
                        )
                        """
                    )
                    cursor.execute(
                        f"""
                        INSERT INTO {staging_table} (qid, count_all, count_nodes, count_ways, count_relations)
                        SELECT
                            CASE
                                WHEN qid REGEXP '^Q[0-9]+$' THEN CAST(SUBSTRING(qid, 2) AS UNSIGNED)
                                ELSE CAST(qid AS UNSIGNED)
                            END AS normalized_qid,
                            MAX(count_all),
                            MAX(count_nodes),
                            MAX(count_ways),
                            MAX(count_relations)
                        FROM {table_name}
                        WHERE qid REGEXP '^[0-9]+$' OR qid REGEXP '^Q[0-9]+$'
                        GROUP BY normalized_qid
                        """
                    )
                elif table_name == "sdc_usage":
                    cursor.execute(
                        f"""
                        CREATE TABLE {staging_table} (
                            qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                            usage_count BIGINT NOT NULL DEFAULT 0
                        )
                        """
                    )
                    cursor.execute(
                        f"""
                        INSERT INTO {staging_table} (qid, usage_count)
                        SELECT
                            CASE
                                WHEN qid REGEXP '^Q[0-9]+$' THEN CAST(SUBSTRING(qid, 2) AS UNSIGNED)
                                ELSE CAST(qid AS UNSIGNED)
                            END AS normalized_qid,
                            MAX(usage_count)
                        FROM {table_name}
                        WHERE qid REGEXP '^[0-9]+$' OR qid REGEXP '^Q[0-9]+$'
                        GROUP BY normalized_qid
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        CREATE TABLE {staging_table} (
                            qid BIGINT UNSIGNED NOT NULL PRIMARY KEY
                        )
                        """
                    )
                    cursor.execute(
                        f"""
                        INSERT INTO {staging_table} (qid)
                        SELECT DISTINCT
                            CASE
                                WHEN qid REGEXP '^Q[0-9]+$' THEN CAST(SUBSTRING(qid, 2) AS UNSIGNED)
                                ELSE CAST(qid AS UNSIGNED)
                            END AS normalized_qid
                        FROM {table_name}
                        WHERE qid REGEXP '^[0-9]+$' OR qid REGEXP '^Q[0-9]+$'
                        """
                    )
                cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
                cursor.execute(
                    f"RENAME TABLE {table_name} TO {backup_table}, {staging_table} TO {table_name}")
                cursor.execute(f"DROP TABLE {backup_table}")
            db.commit()

    def state_token(self) -> object | None:
        return None

    def load_snapshot(self) -> LookupSnapshot:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                "SELECT site_key, prefix, ns_id FROM namespace_prefixes")
            namespace_rows = cursor.fetchall()
            cursor.execute("SELECT site_key, api_url FROM site_api_urls")
            api_url_rows = cursor.fetchall()
            cursor.execute(
                "SELECT class_qid, property_qid FROM property_instances")
            property_rows = cursor.fetchall()

        namespaces_by_site: dict[str, dict[str, int]] = {}
        for site_key, prefix, ns_id in namespace_rows:
            site_key_text = _normalize_text(site_key)
            prefix_text = _normalize_text(prefix)
            if site_key_text is not None and prefix_text is not None and isinstance(ns_id, int):
                namespaces_by_site.setdefault(site_key_text, {})[
                    prefix_text.lower()] = ns_id

        site_api_urls: dict[str, str] = {}
        dropped_site_api_rows = 0
        for site_key, api_url in api_url_rows:
            site_key_text = _normalize_text(site_key)
            api_url_text = _normalize_text(api_url)
            if site_key_text is None or api_url_text is None:
                dropped_site_api_rows += 1
                continue
            site_api_urls[site_key_text] = api_url_text
        _warn_dropped_rows(
            "site_api_urls", dropped_site_api_rows, len(api_url_rows))

        property_instances_by_qid: dict[str, set[str]] = {}
        for class_qid, property_qid in property_rows:
            class_qid_text = _normalize_qid(class_qid)
            property_qid_text = _normalize_property_id(property_qid)
            if class_qid_text is not None and property_qid_text is not None:
                property_instances_by_qid.setdefault(
                    class_qid_text, set()).add(property_qid_text)

        return LookupSnapshot(
            namespaces_by_site=namespaces_by_site,
            site_api_urls=site_api_urls,
            property_instances_by_qid=property_instances_by_qid,
        )

    def get_osm_usage(self, qids: Iterable[str] | None = None) -> dict[str, dict[str, int]]:
        with self._connect() as db:
            cursor = db.cursor()
            if qids is None:
                cursor.execute(
                    "SELECT qid, count_all, count_nodes, count_ways, count_relations FROM osm_usage")
                rows = cursor.fetchall()
            else:
                qid_list, dropped_qids = _normalize_qid_query_int_list(qids)
                _warn_dropped_rows("osm_usage query qids",
                                   dropped_qids, dropped_qids + len(qid_list))
                if not qid_list:
                    return {}
                rows = []
                for chunk in _chunked(qid_list, self._QUERY_CHUNK_SIZE):
                    placeholders = ",".join("%s" for _ in chunk)
                    cursor.execute(
                        f"""
                        SELECT qid, count_all, count_nodes, count_ways, count_relations
                        FROM osm_usage
                        WHERE qid IN ({placeholders})
                        ORDER BY qid
                        """,
                        chunk,
                    )
                    rows.extend(cursor.fetchall())

        osm_usage_by_qid: dict[str, dict[str, int]] = {}
        dropped_rows = 0
        for qid, count_all, count_nodes, count_ways, count_relations in rows:
            qid_text = _qid_text_from_int(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            osm_usage_by_qid[qid_text] = {
                "count_all": int(count_all or 0),
                "count_nodes": int(count_nodes or 0),
                "count_ways": int(count_ways or 0),
                "count_relations": int(count_relations or 0),
            }
        _warn_dropped_rows("osm_usage query result", dropped_rows, len(rows))
        return osm_usage_by_qid

    def replace_namespace_data(
        self,
        *,
        namespaces_by_site: dict[str, dict[str, int]],
        site_api_urls: dict[str, str],
    ) -> None:
        self.ensure_schema()
        with self._connect() as db:
            cursor = db.cursor()
            dropped_namespace_rows = 0
            namespace_rows_by_key: dict[tuple[str, str], int] = {}
            for site_key, mapping in namespaces_by_site.items():
                for prefix, ns_id in mapping.items():
                    if isinstance(site_key, str) and isinstance(prefix, str) and isinstance(ns_id, int):
                        row_key = (site_key, prefix.lower())
                        if row_key in namespace_rows_by_key:
                            dropped_namespace_rows += 1
                        namespace_rows_by_key[row_key] = int(ns_id)
                    else:
                        dropped_namespace_rows += 1
            dropped_site_rows = 0
            site_rows_by_key: dict[str, str] = {}
            for site_key, api_url in site_api_urls.items():
                if isinstance(site_key, str) and isinstance(api_url, str):
                    if site_key in site_rows_by_key:
                        dropped_site_rows += 1
                    site_rows_by_key[site_key] = api_url
                else:
                    dropped_site_rows += 1
            cursor.execute(
                "DROP TEMPORARY TABLE IF EXISTS temp_namespace_prefixes")
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_site_api_urls")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_namespace_prefixes (
                    site_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    prefix VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    ns_id INT NOT NULL,
                    PRIMARY KEY (site_key, prefix)
                )
                """
            )
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_site_api_urls (
                    site_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                    api_url TEXT NOT NULL
                )
                """
            )
            cursor.executemany(
                "INSERT INTO temp_namespace_prefixes (site_key, prefix, ns_id) VALUES (%s, %s, %s)",
                [
                    (site_key, prefix, ns_id)
                    for (site_key, prefix), ns_id in sorted(namespace_rows_by_key.items())
                ],
            )
            cursor.executemany(
                "INSERT INTO temp_site_api_urls (site_key, api_url) VALUES (%s, %s)",
                sorted(site_rows_by_key.items()),
            )
            cursor.execute("DELETE FROM namespace_prefixes")
            cursor.execute("DELETE FROM site_api_urls")
            cursor.execute(
                """
                INSERT INTO namespace_prefixes (site_key, prefix, ns_id)
                SELECT site_key, prefix, ns_id
                FROM temp_namespace_prefixes
                ORDER BY site_key, prefix
                """
            )
            cursor.execute(
                """
                INSERT INTO site_api_urls (site_key, api_url)
                SELECT site_key, api_url
                FROM temp_site_api_urls
                ORDER BY site_key
                """
            )
            cursor.execute("DROP TEMPORARY TABLE temp_namespace_prefixes")
            cursor.execute("DROP TEMPORARY TABLE temp_site_api_urls")
            _warn_dropped_rows("namespace_prefixes write",
                               dropped_namespace_rows)
            _warn_dropped_rows("site_api_urls write", dropped_site_rows)
            db.commit()

    def replace_property_instances(self, property_instances_by_qid: dict[str, list[str] | set[str]]) -> None:
        self.ensure_schema()
        with self._connect() as db:
            cursor = db.cursor()
            dropped_property_rows = 0
            property_rows: list[tuple[str, str]] = []
            for class_qid, props in property_instances_by_qid.items():
                if not isinstance(class_qid, str):
                    dropped_property_rows += 1
                    continue
                valid_props: list[str] = []
                for prop in props:
                    if isinstance(prop, str) and prop.startswith("P"):
                        valid_props.append(prop)
                    else:
                        dropped_property_rows += 1
                property_rows.extend((class_qid, property_qid)
                                     for property_qid in sorted(set(valid_props)))
            cursor.execute(
                "DROP TEMPORARY TABLE IF EXISTS temp_property_instances")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_property_instances (
                    class_qid VARCHAR(32) NOT NULL,
                    property_qid VARCHAR(32) NOT NULL,
                    PRIMARY KEY (class_qid, property_qid)
                )
                """
            )
            cursor.executemany(
                "INSERT INTO temp_property_instances (class_qid, property_qid) VALUES (%s, %s)",
                property_rows,
            )
            cursor.execute("DELETE FROM property_instances")
            cursor.execute(
                """
                INSERT INTO property_instances (class_qid, property_qid)
                SELECT class_qid, property_qid
                FROM temp_property_instances
                ORDER BY class_qid, property_qid
                """
            )
            cursor.execute("DROP TEMPORARY TABLE temp_property_instances")
            _warn_dropped_rows("property_instances write",
                               dropped_property_rows)
            db.commit()

    def replace_osm_usage(self, osm_usage_by_qid: dict[str, dict[str, int]]) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            dropped_osm_rows = 0
            osm_rows: list[tuple[int, int, int, int, int]] = []
            for qid, row in osm_usage_by_qid.items():
                qid_num = _normalize_qid_int(qid)
                if qid_num is not None and isinstance(row, dict):
                    osm_rows.append(
                        (
                            qid_num,
                            int(row.get("count_all", 0)),
                            int(row.get("count_nodes", 0)),
                            int(row.get("count_ways", 0)),
                            int(row.get("count_relations", 0)),
                        )
                    )
                else:
                    dropped_osm_rows += 1
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_osm_usage")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_osm_usage (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    count_all BIGINT NOT NULL DEFAULT 0,
                    count_nodes BIGINT NOT NULL DEFAULT 0,
                    count_ways BIGINT NOT NULL DEFAULT 0,
                    count_relations BIGINT NOT NULL DEFAULT 0
                )
                """
            )
            cursor.executemany(
                "INSERT INTO temp_osm_usage (qid, count_all, count_nodes, count_ways, count_relations) VALUES (%s, %s, %s, %s, %s)",
                osm_rows,
            )
            cursor.execute("DELETE FROM osm_usage")
            cursor.execute(
                """
                INSERT INTO osm_usage (qid, count_all, count_nodes, count_ways, count_relations)
                SELECT qid, count_all, count_nodes, count_ways, count_relations
                FROM temp_osm_usage
                ORDER BY qid
                """
            )
            cursor.execute("DROP TEMPORARY TABLE temp_osm_usage")
            _warn_dropped_rows("osm_usage write", dropped_osm_rows)
            db.commit()

    def replace_sdc_usage(self, sdc_usage_by_qid: dict[str, int]) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            dropped_sdc_rows = 0
            sdc_rows: list[tuple[int, int]] = []
            for qid, usage_count in sdc_usage_by_qid.items():
                qid_num = _normalize_qid_int(qid)
                if qid_num is not None:
                    sdc_rows.append((qid_num, int(usage_count)))
                else:
                    dropped_sdc_rows += 1
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_sdc_usage")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_sdc_usage (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    usage_count BIGINT NOT NULL DEFAULT 0
                )
                """
            )
            cursor.executemany(
                "INSERT INTO temp_sdc_usage (qid, usage_count) VALUES (%s, %s)",
                sdc_rows,
            )
            cursor.execute("DELETE FROM sdc_usage")
            cursor.execute(
                """
                INSERT INTO sdc_usage (qid, usage_count)
                SELECT qid, usage_count
                FROM temp_sdc_usage
                ORDER BY qid
                """
            )
            cursor.execute("DROP TEMPORARY TABLE temp_sdc_usage")
            _warn_dropped_rows("sdc_usage write", dropped_sdc_rows)
            db.commit()

    def replace_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                "DROP TEMPORARY TABLE IF EXISTS temp_wiki_subscribers")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_wiki_subscribers (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY
                )
                """
            )
            rows: list[tuple[int]] = []
            dropped_rows = 0
            seen: set[int] = set()
            for qid in wiki_subscribers:
                qid_num = _normalize_qid_int(qid)
                if qid_num is None:
                    dropped_rows += 1
                    continue
                if qid_num in seen:
                    continue
                seen.add(qid_num)
                rows.append((qid_num,))
            cursor.executemany(
                "INSERT INTO temp_wiki_subscribers (qid) VALUES (%s)",
                rows,
            )
            cursor.execute("DELETE FROM wiki_subscribers")
            cursor.execute(
                """
                INSERT INTO wiki_subscribers (qid)
                SELECT qid
                FROM temp_wiki_subscribers
                ORDER BY qid
                """
            )
            cursor.execute("DROP TEMPORARY TABLE temp_wiki_subscribers")
            _warn_dropped_rows("wiki_subscribers write", dropped_rows)
            db.commit()

    def upsert_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> int:
        rows: list[tuple[int]] = []
        dropped_rows = 0
        seen: set[int] = set()
        for qid in wiki_subscribers:
            qid_num = _normalize_qid_int(qid)
            if qid_num is None:
                dropped_rows += 1
                continue
            if qid_num in seen:
                continue
            seen.add(qid_num)
            rows.append((qid_num,))

        if not rows:
            _warn_dropped_rows("wiki_subscribers upsert", dropped_rows)
            return 0

        with self._connect() as db:
            cursor = db.cursor()
            cursor.executemany(
                """
                INSERT IGNORE INTO wiki_subscribers (qid)
                VALUES (%s)
                """,
                rows,
            )
            db.commit()

        _warn_dropped_rows("wiki_subscribers upsert", dropped_rows)
        return len(rows)

    @deprecated("Use worker-local methods instead")
    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
        with self._connect() as db:
            cursor = db.cursor()
            if qids is None:
                cursor.execute("SELECT qid FROM wiki_subscribers")
                rows = cursor.fetchall()
            else:
                qid_list, dropped_qids = _normalize_qid_query_int_list(qids)
                _warn_dropped_rows("wiki_subscribers query qids",
                                   dropped_qids, dropped_qids + len(qid_list))
                if not qid_list:
                    return set()
                rows = []
                for chunk in _chunked(qid_list, self._QUERY_CHUNK_SIZE):
                    placeholders = ",".join("%s" for _ in chunk)
                    cursor.execute(
                        f"""
                        SELECT qid
                        FROM wiki_subscribers
                        WHERE qid IN ({placeholders})
                        ORDER BY qid
                        """,
                        chunk,
                    )
                    rows.extend(cursor.fetchall())

        wiki_subscribers: set[str] = set()
        for (qid,) in rows:
            qid_text = _qid_text_from_int(qid)
            if qid_text is not None:
                wiki_subscribers.add(qid_text)
        return wiki_subscribers

    @deprecated("Use worker-local methods instead")
    def get_external_usage(self, qids: Iterable[str]) -> dict[str, dict[str, object]]:
        qid_list, dropped_qids = _normalize_qid_query_int_list(qids)
        _warn_dropped_rows("external_usage query qids",
                           dropped_qids, dropped_qids + len(qid_list))
        if not qid_list:
            return {}

        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                "DROP TEMPORARY TABLE IF EXISTS temp_external_usage_qids")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_external_usage_qids (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY
                )
                """
            )
            cursor.executemany(
                "INSERT INTO temp_external_usage_qids (qid) VALUES (%s)",
                [(qid,) for qid in qid_list],
            )
            cursor.execute(
                """
                SELECT
                    tq.qid,
                    ou.count_all,
                    ou.count_nodes,
                    ou.count_ways,
                    ou.count_relations,
                    su.usage_count,
                    CASE WHEN ws.qid IS NULL THEN 0 ELSE 1 END AS has_wikisub
                FROM temp_external_usage_qids tq
                LEFT JOIN osm_usage ou
                  ON ou.qid = tq.qid
                LEFT JOIN sdc_usage su
                  ON su.qid = tq.qid
                LEFT JOIN wiki_subscribers ws
                  ON ws.qid = tq.qid
                ORDER BY tq.qid
                """
            )
            rows = cursor.fetchall()
            cursor.execute("DROP TEMPORARY TABLE temp_external_usage_qids")

        external_usage: dict[str, dict[str, object]] = {}
        for qid, count_all, count_nodes, count_ways, count_relations, usage_count, has_wikisub in rows:
            qid_text = _qid_text_from_int(qid)
            if qid_text is None:
                continue
            external_usage[qid_text] = {
                "osm": None if all(value is None for value in (count_all, count_nodes, count_ways, count_relations)) else {
                    "count_all": int(count_all or 0),
                    "count_nodes": int(count_nodes or 0),
                    "count_ways": int(count_ways or 0),
                    "count_relations": int(count_relations or 0),
                },
                "sdc": None if usage_count is None else int(usage_count or 0),
                "wikisub": bool(has_wikisub),
            }
        return external_usage

    def get_lookup_state(self, key: str) -> str | None:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                "SELECT value FROM lookup_state WHERE `key` = %s",
                (key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return _normalize_text(row[0])

    def set_lookup_state(self, key: str, value: str) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                """
                INSERT INTO lookup_state (`key`, value)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE value = VALUES(value)
                """,
                (key, value),
            )
            db.commit()

    def get_sdc_usage(self, qids: Iterable[str] | None = None) -> dict[str, int]:
        with self._connect() as db:
            cursor = db.cursor()
            if qids is None:
                cursor.execute("SELECT qid, usage_count FROM sdc_usage")
                rows = cursor.fetchall()
            else:
                qid_list, dropped_qids = _normalize_qid_query_int_list(qids)
                _warn_dropped_rows("sdc_usage query qids",
                                   dropped_qids, dropped_qids + len(qid_list))
                if not qid_list:
                    return {}
                rows = []
                for chunk in _chunked(qid_list, self._QUERY_CHUNK_SIZE):
                    placeholders = ",".join("%s" for _ in chunk)
                    cursor.execute(
                        f"""
                        SELECT qid, usage_count
                        FROM sdc_usage
                        WHERE qid IN ({placeholders})
                        ORDER BY qid
                        """,
                        chunk,
                    )
                    rows.extend(cursor.fetchall())

        sdc_usage_by_qid: dict[str, int] = {}
        dropped_rows = 0
        for qid, usage_count in rows:
            qid_text = _qid_text_from_int(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            sdc_usage_by_qid[qid_text] = int(usage_count or 0)
        _warn_dropped_rows("sdc_usage query result", dropped_rows, len(rows))
        return sdc_usage_by_qid

    def assert_ready(self, required_property_qids: Iterable[str] = ()) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(*) FROM namespace_prefixes")
            namespace_count = int(cursor.fetchone()[0] or 0)
            cursor.execute("SELECT COUNT(*) FROM site_api_urls")
            site_url_count = int(cursor.fetchone()[0] or 0)
            cursor.execute("SELECT COUNT(*) FROM wiki_subscribers")
            wiki_subscriber_count = int(cursor.fetchone()[0] or 0)

            missing_property_qids: list[str] = []
            for qid in required_property_qids:
                cursor.execute(
                    "SELECT COUNT(*) FROM property_instances WHERE class_qid = %s",
                    (qid,),
                )
                if int(cursor.fetchone()[0] or 0) == 0:
                    missing_property_qids.append(qid)

        if namespace_count == 0:
            raise RuntimeError(
                "Lookup cache database has no namespace prefixes.")
        if site_url_count == 0:
            raise RuntimeError("Lookup cache database has no site API URLs.")
        if wiki_subscriber_count == 0:
            raise RuntimeError(
                "Lookup cache database has no wiki subscriber rows.")
        if missing_property_qids:
            raise RuntimeError(
                "Lookup cache database is missing required property-instance rows for: "
                f"{', '.join(sorted(missing_property_qids))}."
            )


def create_lookup_backend(
    db_path: str | Path | None = None,
) -> LookupBackend:
    readonly = os.getenv("WD_NOTABILITY_LOOKUP_READONLY",
                         "0").strip().lower() in {"1", "true", "yes"}
    database = require_env_value("TOOLSDB_DATABASE")
    return MariaDBLookupBackend(database=database, readonly=readonly)

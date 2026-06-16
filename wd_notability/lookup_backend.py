from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
import configparser
import logging
import os
from pathlib import Path
import sqlite3
from typing import Protocol


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
        _logger.warning("Lookup cache %s dropped %d malformed row(s)", context, dropped)
        return
    _logger.warning("Lookup cache %s dropped %d malformed row(s) out of %d", context, dropped, total)


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


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


@dataclass(frozen=True)
class LookupSnapshot:
    namespaces_by_site: dict[str, dict[str, int]]
    site_api_urls: dict[str, str]
    property_instances_by_qid: dict[str, set[str]]
    osm_usage_by_qid: dict[str, dict[str, int]]
    sdc_usage_by_qid: dict[str, int]
    wiki_subscribers_by_qid: set[str]


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
    def get_osm_usage(self, qids: Iterable[str] | None = None) -> dict[str, dict[str, int]]:
        raise NotImplementedError

    @abstractmethod
    def get_sdc_usage(self, qids: Iterable[str] | None = None) -> dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
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
    def replace_osm_usage(self, osm_usage_by_qid: dict[str, dict[str, int]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def replace_sdc_usage(self, sdc_usage_by_qid: dict[str, int]) -> None:
        raise NotImplementedError

    @abstractmethod
    def replace_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> None:
        raise NotImplementedError

    @abstractmethod
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


class SqliteLookupBackend(LookupBackend):
    _QUERY_CHUNK_SIZE = 500

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def ensure_schema(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS namespace_prefixes (
                    site_key TEXT NOT NULL,
                    prefix TEXT NOT NULL,
                    ns_id INTEGER NOT NULL,
                    PRIMARY KEY (site_key, prefix)
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS site_api_urls (
                    site_key TEXT PRIMARY KEY NOT NULL,
                    api_url TEXT NOT NULL
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS property_instances (
                    class_qid TEXT NOT NULL,
                    property_qid TEXT NOT NULL,
                    PRIMARY KEY (class_qid, property_qid)
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS osm_usage (
                    qid TEXT PRIMARY KEY NOT NULL,
                    count_all INTEGER NOT NULL DEFAULT 0,
                    count_nodes INTEGER NOT NULL DEFAULT 0,
                    count_ways INTEGER NOT NULL DEFAULT 0,
                    count_relations INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS sdc_usage (
                    qid TEXT PRIMARY KEY NOT NULL,
                    usage_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS wiki_subscribers (
                    qid TEXT PRIMARY KEY NOT NULL
                )
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS lookup_state (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL
                )
                """
            )
            db.commit()

    def state_token(self) -> int | None:
        if not self.db_path.exists():
            return None
        return self.db_path.stat().st_mtime_ns

    def load_snapshot(self) -> LookupSnapshot:
        if not self.db_path.exists():
            raise RuntimeError(
                f"Lookup cache database is missing: {self.db_path}. "
                "Run scripts/build_namespace_cache.py and scripts/build_property_cache.py first."
            )

        with self._connect() as db:
            namespace_rows = db.execute(
                """
                SELECT site_key, prefix, ns_id
                FROM namespace_prefixes
                """
            ).fetchall()
            api_url_rows = db.execute(
                """
                SELECT site_key, api_url
                FROM site_api_urls
                """
            ).fetchall()
            property_rows = db.execute(
                """
                SELECT class_qid, property_qid
                FROM property_instances
                """
            ).fetchall()
            osm_rows = db.execute(
                """
                SELECT qid, count_all, count_nodes, count_ways, count_relations
                FROM osm_usage
                """
            ).fetchall()
            sdc_rows = db.execute(
                """
                SELECT qid, usage_count
                FROM sdc_usage
                """
            ).fetchall()
            wiki_subscriber_rows = db.execute(
                """
                SELECT qid
                FROM wiki_subscribers
                """
            ).fetchall()

        namespaces_by_site: dict[str, dict[str, int]] = {}
        dropped_namespace_rows = 0
        for site_key, prefix, ns_id in namespace_rows:
            site_key_text = _normalize_text(site_key)
            prefix_text = _normalize_text(prefix)
            if site_key_text is None or prefix_text is None or not isinstance(ns_id, int):
                dropped_namespace_rows += 1
                continue
            namespaces_by_site.setdefault(site_key_text, {})[prefix_text.lower()] = ns_id
        _warn_dropped_rows("namespace_prefixes", dropped_namespace_rows, len(namespace_rows))

        site_api_urls: dict[str, str] = {}
        dropped_site_api_rows = 0
        for site_key, api_url in api_url_rows:
            site_key_text = _normalize_text(site_key)
            api_url_text = _normalize_text(api_url)
            if site_key_text is None or api_url_text is None:
                dropped_site_api_rows += 1
                continue
            site_api_urls[site_key_text] = api_url_text
        _warn_dropped_rows("site_api_urls", dropped_site_api_rows, len(api_url_rows))

        property_instances_by_qid: dict[str, set[str]] = {}
        dropped_property_rows = 0
        for class_qid, property_qid in property_rows:
            class_qid_text = _normalize_qid(class_qid)
            property_qid_text = _normalize_property_id(property_qid)
            if class_qid_text is None or property_qid_text is None:
                dropped_property_rows += 1
                continue
            property_instances_by_qid.setdefault(class_qid_text, set()).add(property_qid_text)
        _warn_dropped_rows("property_instances", dropped_property_rows, len(property_rows))

        osm_usage_by_qid: dict[str, dict[str, int]] = {}
        dropped_osm_rows = 0
        for qid, count_all, count_nodes, count_ways, count_relations in osm_rows:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_osm_rows += 1
                continue
            osm_usage_by_qid[qid_text] = {
                "count_all": int(count_all or 0),
                "count_nodes": int(count_nodes or 0),
                "count_ways": int(count_ways or 0),
                "count_relations": int(count_relations or 0),
            }
        _warn_dropped_rows("osm_usage", dropped_osm_rows, len(osm_rows))

        sdc_usage_by_qid: dict[str, int] = {}
        dropped_sdc_rows = 0
        for qid, usage_count in sdc_rows:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_sdc_rows += 1
                continue
            sdc_usage_by_qid[qid_text] = int(usage_count or 0)
        _warn_dropped_rows("sdc_usage", dropped_sdc_rows, len(sdc_rows))

        wiki_subscribers_by_qid: set[str] = set()
        dropped_wiki_subscriber_rows = 0
        for (qid,) in wiki_subscriber_rows:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_wiki_subscriber_rows += 1
                continue
            wiki_subscribers_by_qid.add(qid_text)
        _warn_dropped_rows("wiki_subscribers", dropped_wiki_subscriber_rows, len(wiki_subscriber_rows))

        return LookupSnapshot(
            namespaces_by_site=namespaces_by_site,
            site_api_urls=site_api_urls,
            property_instances_by_qid=property_instances_by_qid,
            osm_usage_by_qid=osm_usage_by_qid,
            sdc_usage_by_qid=sdc_usage_by_qid,
            wiki_subscribers_by_qid=wiki_subscribers_by_qid,
        )

    def _select_osm_usage(self, db: sqlite3.Connection, qids: Iterable[str] | None) -> dict[str, dict[str, int]]:
        if qids is None:
            rows = db.execute(
                """
                SELECT qid, count_all, count_nodes, count_ways, count_relations
                FROM osm_usage
                """
            ).fetchall()
        else:
            qid_list, dropped_qids = _normalize_qid_query_list(qids)
            _warn_dropped_rows("osm_usage query qids", dropped_qids, dropped_qids + len(qid_list))
            if not qid_list:
                return {}
            rows = []
            for chunk in _chunked(qid_list, self._QUERY_CHUNK_SIZE):
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    db.execute(
                        f"""
                        SELECT qid, count_all, count_nodes, count_ways, count_relations
                        FROM osm_usage
                        WHERE qid IN ({placeholders})
                        ORDER BY qid
                        """,
                        chunk,
                    ).fetchall()
                )

        usage_by_qid: dict[str, dict[str, int]] = {}
        dropped_rows = 0
        for qid, count_all, count_nodes, count_ways, count_relations in rows:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            usage_by_qid[qid_text] = {
                "count_all": int(count_all or 0),
                "count_nodes": int(count_nodes or 0),
                "count_ways": int(count_ways or 0),
                "count_relations": int(count_relations or 0),
            }
        _warn_dropped_rows("osm_usage query result", dropped_rows, len(rows))
        return usage_by_qid

    def _select_sdc_usage(self, db: sqlite3.Connection, qids: Iterable[str] | None) -> dict[str, int]:
        if qids is None:
            rows = db.execute(
                """
                SELECT qid, usage_count
                FROM sdc_usage
                """
            ).fetchall()
        else:
            qid_list, dropped_qids = _normalize_qid_query_list(qids)
            _warn_dropped_rows("sdc_usage query qids", dropped_qids, dropped_qids + len(qid_list))
            if not qid_list:
                return {}
            rows = []
            for chunk in _chunked(qid_list, self._QUERY_CHUNK_SIZE):
                placeholders = ",".join("?" for _ in chunk)
                rows.extend(
                    db.execute(
                        f"""
                        SELECT qid, usage_count
                        FROM sdc_usage
                        WHERE qid IN ({placeholders})
                        ORDER BY qid
                        """,
                        chunk,
                    ).fetchall()
                )

        usage_by_qid: dict[str, int] = {}
        dropped_rows = 0
        for qid, usage_count in rows:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            usage_by_qid[qid_text] = int(usage_count or 0)
        _warn_dropped_rows("sdc_usage query result", dropped_rows, len(rows))
        return usage_by_qid

    def replace_namespace_data(
        self,
        *,
        namespaces_by_site: dict[str, dict[str, int]],
        site_api_urls: dict[str, str],
    ) -> None:
        self.ensure_schema()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            dropped_namespace_rows = 0
            namespace_rows: list[tuple[str, str, int]] = []
            for site_key, mapping in namespaces_by_site.items():
                for prefix, ns_id in mapping.items():
                    if isinstance(site_key, str) and isinstance(prefix, str) and isinstance(ns_id, int):
                        namespace_rows.append((site_key, prefix.lower(), int(ns_id)))
                    else:
                        dropped_namespace_rows += 1
            dropped_site_rows = 0
            site_rows: list[tuple[str, str]] = []
            for site_key, api_url in site_api_urls.items():
                if isinstance(site_key, str) and isinstance(api_url, str):
                    site_rows.append((site_key, api_url))
                else:
                    dropped_site_rows += 1
            db.execute("DROP TABLE IF EXISTS temp_namespace_prefixes")
            db.execute("DROP TABLE IF EXISTS temp_site_api_urls")
            db.execute(
                """
                CREATE TEMP TABLE temp_namespace_prefixes (
                    site_key TEXT NOT NULL,
                    prefix TEXT NOT NULL,
                    ns_id INTEGER NOT NULL,
                    PRIMARY KEY (site_key, prefix)
                )
                """
            )
            db.execute(
                """
                CREATE TEMP TABLE temp_site_api_urls (
                    site_key TEXT NOT NULL PRIMARY KEY,
                    api_url TEXT NOT NULL
                )
                """
            )
            db.executemany(
                "INSERT INTO temp_namespace_prefixes (site_key, prefix, ns_id) VALUES (?, ?, ?)",
                namespace_rows,
            )
            db.executemany(
                "INSERT INTO temp_site_api_urls (site_key, api_url) VALUES (?, ?)",
                site_rows,
            )
            db.execute("DELETE FROM namespace_prefixes")
            db.execute("DELETE FROM site_api_urls")
            db.execute(
                """
                INSERT INTO namespace_prefixes (site_key, prefix, ns_id)
                SELECT site_key, prefix, ns_id
                FROM temp_namespace_prefixes
                ORDER BY site_key, prefix
                """
            )
            db.execute(
                """
                INSERT INTO site_api_urls (site_key, api_url)
                SELECT site_key, api_url
                FROM temp_site_api_urls
                ORDER BY site_key
                """
            )
            db.execute("DROP TABLE temp_namespace_prefixes")
            db.execute("DROP TABLE temp_site_api_urls")
            _warn_dropped_rows("namespace_prefixes write", dropped_namespace_rows)
            _warn_dropped_rows("site_api_urls write", dropped_site_rows)
            db.commit()

    def replace_property_instances(self, property_instances_by_qid: dict[str, list[str] | set[str]]) -> None:
        self.ensure_schema()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
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
                property_rows.extend((class_qid, property_qid) for property_qid in sorted(set(valid_props)))
            db.execute("DROP TABLE IF EXISTS temp_property_instances")
            db.execute(
                """
                CREATE TEMP TABLE temp_property_instances (
                    class_qid TEXT NOT NULL,
                    property_qid TEXT NOT NULL,
                    PRIMARY KEY (class_qid, property_qid)
                )
                """
            )
            db.executemany(
                "INSERT INTO temp_property_instances (class_qid, property_qid) VALUES (?, ?)",
                property_rows,
            )
            db.execute("DELETE FROM property_instances")
            db.execute(
                """
                INSERT INTO property_instances (class_qid, property_qid)
                SELECT class_qid, property_qid
                FROM temp_property_instances
                ORDER BY class_qid, property_qid
                """
            )
            db.execute("DROP TABLE temp_property_instances")
            _warn_dropped_rows("property_instances write", dropped_property_rows)
            db.commit()

    def replace_osm_usage(self, osm_usage_by_qid: dict[str, dict[str, int]]) -> None:
        self.ensure_schema()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            dropped_osm_rows = 0
            osm_rows: list[tuple[str, int, int, int, int]] = []
            for qid, row in osm_usage_by_qid.items():
                if isinstance(qid, str) and isinstance(row, dict):
                    osm_rows.append(
                        (
                            qid,
                            int(row.get("count_all", 0)),
                            int(row.get("count_nodes", 0)),
                            int(row.get("count_ways", 0)),
                            int(row.get("count_relations", 0)),
                        )
                    )
                else:
                    dropped_osm_rows += 1
            db.execute("DROP TABLE IF EXISTS temp_osm_usage")
            db.execute(
                """
                CREATE TEMP TABLE temp_osm_usage (
                    qid TEXT NOT NULL PRIMARY KEY,
                    count_all INTEGER NOT NULL DEFAULT 0,
                    count_nodes INTEGER NOT NULL DEFAULT 0,
                    count_ways INTEGER NOT NULL DEFAULT 0,
                    count_relations INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            db.executemany(
                """
                INSERT INTO temp_osm_usage (qid, count_all, count_nodes, count_ways, count_relations)
                VALUES (?, ?, ?, ?, ?)
                """,
                osm_rows,
            )
            db.execute("DELETE FROM osm_usage")
            db.execute(
                """
                INSERT INTO osm_usage (qid, count_all, count_nodes, count_ways, count_relations)
                SELECT qid, count_all, count_nodes, count_ways, count_relations
                FROM temp_osm_usage
                ORDER BY qid
                """
            )
            db.execute("DROP TABLE temp_osm_usage")
            _warn_dropped_rows("osm_usage write", dropped_osm_rows)
            db.commit()

    def get_osm_usage(self, qids: Iterable[str] | None = None) -> dict[str, dict[str, int]]:
        with self._connect() as db:
            return self._select_osm_usage(db, qids)

    def replace_sdc_usage(self, sdc_usage_by_qid: dict[str, int]) -> None:
        self.ensure_schema()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            dropped_sdc_rows = 0
            sdc_rows: list[tuple[str, int]] = []
            for qid, usage_count in sdc_usage_by_qid.items():
                if isinstance(qid, str):
                    sdc_rows.append((qid, int(usage_count)))
                else:
                    dropped_sdc_rows += 1
            db.execute("DROP TABLE IF EXISTS temp_sdc_usage")
            db.execute(
                """
                CREATE TEMP TABLE temp_sdc_usage (
                    qid TEXT NOT NULL PRIMARY KEY,
                    usage_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            db.executemany(
                "INSERT INTO temp_sdc_usage (qid, usage_count) VALUES (?, ?)",
                sdc_rows,
            )
            db.execute("DELETE FROM sdc_usage")
            db.execute(
                """
                INSERT INTO sdc_usage (qid, usage_count)
                SELECT qid, usage_count
                FROM temp_sdc_usage
                ORDER BY qid
                """
            )
            db.execute("DROP TABLE temp_sdc_usage")
            _warn_dropped_rows("sdc_usage write", dropped_sdc_rows)
            db.commit()

    def replace_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> None:
        self.ensure_schema()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DROP TABLE IF EXISTS temp_wiki_subscribers")
            db.execute(
                """
                CREATE TEMP TABLE temp_wiki_subscribers (
                    qid TEXT NOT NULL PRIMARY KEY
                )
                """
            )
            rows: list[tuple[str]] = []
            dropped_rows = 0
            seen: set[str] = set()
            for qid in wiki_subscribers:
                qid_text = _normalize_qid(qid)
                if qid_text is None:
                    dropped_rows += 1
                    continue
                if qid_text in seen:
                    continue
                seen.add(qid_text)
                rows.append((qid_text,))
            db.executemany("INSERT INTO temp_wiki_subscribers (qid) VALUES (?)", rows)
            db.execute("DELETE FROM wiki_subscribers")
            db.execute(
                """
                INSERT INTO wiki_subscribers (qid)
                SELECT qid
                FROM temp_wiki_subscribers
                ORDER BY qid
                """
            )
            db.execute("DROP TABLE temp_wiki_subscribers")
            _warn_dropped_rows("wiki_subscribers write", dropped_rows)
            db.commit()

    def upsert_wiki_subscribers(self, wiki_subscribers: Iterable[str]) -> int:
        self.ensure_schema()
        rows: list[tuple[str]] = []
        dropped_rows = 0
        seen: set[str] = set()
        for qid in wiki_subscribers:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            if qid_text in seen:
                continue
            seen.add(qid_text)
            rows.append((qid_text,))

        if not rows:
            _warn_dropped_rows("wiki_subscribers upsert", dropped_rows)
            return 0

        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.executemany(
                """
                INSERT OR IGNORE INTO wiki_subscribers (qid)
                VALUES (?)
                """,
                rows,
            )
            db.commit()

        _warn_dropped_rows("wiki_subscribers upsert", dropped_rows)
        return len(rows)

    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
        with self._connect() as db:
            cursor = db.cursor()
            if qids is None:
                cursor.execute("SELECT qid FROM wiki_subscribers")
                rows = cursor.fetchall()
            else:
                qid_list, dropped_qids = _normalize_qid_query_list(qids)
                _warn_dropped_rows("wiki_subscribers query qids", dropped_qids, dropped_qids + len(qid_list))
                if not qid_list:
                    return set()
                rows: list[tuple[str]] = []
                for chunk in _chunked(qid_list, self._QUERY_CHUNK_SIZE):
                    placeholders = ",".join("?" for _ in chunk)
                    rows.extend(
                        cursor.execute(
                            f"""
                            SELECT qid
                            FROM wiki_subscribers
                            WHERE qid IN ({placeholders})
                            ORDER BY qid
                            """,
                            chunk,
                        ).fetchall()
                    )

        wiki_subscribers: set[str] = set()
        dropped_rows = 0
        for (qid,) in rows:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            wiki_subscribers.add(qid_text)
        _warn_dropped_rows("wiki_subscribers query result", dropped_rows, len(rows))
        return wiki_subscribers

    def get_lookup_state(self, key: str) -> str | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT value FROM lookup_state WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return _normalize_text(row[0])

    def set_lookup_state(self, key: str, value: str) -> None:
        self.ensure_schema()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """
                INSERT INTO lookup_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            db.commit()

    def get_sdc_usage(self, qids: Iterable[str] | None = None) -> dict[str, int]:
        with self._connect() as db:
            return self._select_sdc_usage(db, qids)

    def assert_ready(self, required_property_qids: Iterable[str] = ()) -> None:
        if not self.db_path.exists():
            raise RuntimeError(
                f"Lookup cache database is missing: {self.db_path}. "
                "Run scripts/build_namespace_cache.py and scripts/build_property_cache.py first."
            )

        with self._connect() as db:
            namespace_count = int(db.execute("SELECT COUNT(*) FROM namespace_prefixes").fetchone()[0] or 0)
            site_url_count = int(db.execute("SELECT COUNT(*) FROM site_api_urls").fetchone()[0] or 0)
            wiki_subscriber_count = int(db.execute("SELECT COUNT(*) FROM wiki_subscribers").fetchone()[0] or 0)

            missing_property_qids: list[str] = []
            for qid in required_property_qids:
                row = db.execute(
                    "SELECT COUNT(*) FROM property_instances WHERE class_qid = ?",
                    (qid,),
                ).fetchone()
                if row is None or int(row[0]) == 0:
                    missing_property_qids.append(qid)

        if namespace_count == 0:
            raise RuntimeError(
                f"Lookup cache database has no namespace prefixes: {self.db_path}. "
                "Run scripts/build_namespace_cache.py first."
            )
        if site_url_count == 0:
            raise RuntimeError(
                f"Lookup cache database has no site API URLs: {self.db_path}. "
                "Run scripts/build_namespace_cache.py first."
            )
        if wiki_subscriber_count == 0:
            raise RuntimeError(
                f"Lookup cache database has no wiki subscriber rows: {self.db_path}. "
                "Run scripts/build_wikisub_cache.py first."
            )
        if missing_property_qids:
            raise RuntimeError(
                f"Lookup cache database is missing required property-instance rows for: "
                f"{', '.join(sorted(missing_property_qids))}. "
                "Run scripts/build_property_cache.py first."
            )


class MariaDBLookupBackend(LookupBackend):
    """Toolforge-oriented MariaDB backend.

    This backend is intentionally separate from the local SQLite backend so the
    rest of the application only depends on the LookupBackend contract. On
    Toolforge, connections normally use the shared MariaDB host and the
    per-tool credential file described in the Toolforge ToolsDB docs.
    """

    _QUERY_CHUNK_SIZE = 500

    def __init__(
        self,
        database: str,
        *,
        host: str | None = None,
        defaults_file: str | Path | None = None,
        readonly: bool = False,
    ) -> None:
        self.database = database
        self.host = host or (
            "tools-readonly.db.svc.wikimedia.cloud" if readonly else "tools.db.svc.wikimedia.cloud"
        )
        self.defaults_file = Path(defaults_file) if defaults_file is not None else Path(
            os.environ.get("HOME", "~")
        ).expanduser() / "replica.my.cnf"
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
        config = configparser.ConfigParser(interpolation=None)
        if not self.defaults_file.exists():
            raise RuntimeError(
                f"Toolforge credential file is missing: {self.defaults_file}"
            )
        config.read(self.defaults_file)
        client = config["client"] if "client" in config else {}
        return pymysql.connect(
            user=client.get("user"),
            password=client.get("password"),
            host=self.host,
            port=3306,
            database=self.database,
            charset="utf8mb4",
        )

    def ensure_schema(self) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS namespace_prefixes (
                    site_key VARCHAR(255) NOT NULL,
                    prefix VARCHAR(255) NOT NULL,
                    ns_id INT NOT NULL,
                    PRIMARY KEY (site_key, prefix)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS site_api_urls (
                    site_key VARCHAR(255) NOT NULL PRIMARY KEY,
                    api_url TEXT NOT NULL
                )
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
                    qid VARCHAR(32) NOT NULL PRIMARY KEY,
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
                    qid VARCHAR(32) NOT NULL PRIMARY KEY,
                    usage_count BIGINT NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS wiki_subscribers (
                    qid VARCHAR(32) NOT NULL PRIMARY KEY
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
            db.commit()

    def state_token(self) -> object | None:
        return None

    def load_snapshot(self) -> LookupSnapshot:
        with self._connect() as db:
            cursor = db.cursor()
            cursor.execute("SELECT site_key, prefix, ns_id FROM namespace_prefixes")
            namespace_rows = cursor.fetchall()
            cursor.execute("SELECT site_key, api_url FROM site_api_urls")
            api_url_rows = cursor.fetchall()
            cursor.execute("SELECT class_qid, property_qid FROM property_instances")
            property_rows = cursor.fetchall()
            cursor.execute("SELECT qid, count_all, count_nodes, count_ways, count_relations FROM osm_usage")
            osm_rows = cursor.fetchall()
            cursor.execute("SELECT qid, usage_count FROM sdc_usage")
            sdc_rows = cursor.fetchall()
            cursor.execute("SELECT qid FROM wiki_subscribers")
            wiki_subscriber_rows = cursor.fetchall()

        namespaces_by_site: dict[str, dict[str, int]] = {}
        for site_key, prefix, ns_id in namespace_rows:
            site_key_text = _normalize_text(site_key)
            prefix_text = _normalize_text(prefix)
            if site_key_text is not None and prefix_text is not None and isinstance(ns_id, int):
                namespaces_by_site.setdefault(site_key_text, {})[prefix_text.lower()] = ns_id

        site_api_urls: dict[str, str] = {}
        dropped_site_api_rows = 0
        for site_key, api_url in api_url_rows:
            site_key_text = _normalize_text(site_key)
            api_url_text = _normalize_text(api_url)
            if site_key_text is None or api_url_text is None:
                dropped_site_api_rows += 1
                continue
            site_api_urls[site_key_text] = api_url_text
        _warn_dropped_rows("site_api_urls", dropped_site_api_rows, len(api_url_rows))

        property_instances_by_qid: dict[str, set[str]] = {}
        for class_qid, property_qid in property_rows:
            class_qid_text = _normalize_qid(class_qid)
            property_qid_text = _normalize_property_id(property_qid)
            if class_qid_text is not None and property_qid_text is not None:
                property_instances_by_qid.setdefault(class_qid_text, set()).add(property_qid_text)

        osm_usage_by_qid: dict[str, dict[str, int]] = {}
        for qid, count_all, count_nodes, count_ways, count_relations in osm_rows:
            qid_text = _normalize_qid(qid)
            if qid_text is not None:
                osm_usage_by_qid[qid_text] = {
                    "count_all": int(count_all or 0),
                    "count_nodes": int(count_nodes or 0),
                    "count_ways": int(count_ways or 0),
                    "count_relations": int(count_relations or 0),
                }

        sdc_usage_by_qid: dict[str, int] = {}
        for qid, usage_count in sdc_rows:
            qid_text = _normalize_qid(qid)
            if qid_text is not None:
                sdc_usage_by_qid[qid_text] = int(usage_count or 0)

        wiki_subscribers_by_qid: set[str] = set()
        for (qid,) in wiki_subscriber_rows:
            qid_text = _normalize_qid(qid)
            if qid_text is not None:
                wiki_subscribers_by_qid.add(qid_text)

        return LookupSnapshot(
            namespaces_by_site=namespaces_by_site,
            site_api_urls=site_api_urls,
            property_instances_by_qid=property_instances_by_qid,
            osm_usage_by_qid=osm_usage_by_qid,
            sdc_usage_by_qid=sdc_usage_by_qid,
            wiki_subscribers_by_qid=wiki_subscribers_by_qid,
        )

    def get_osm_usage(self, qids: Iterable[str] | None = None) -> dict[str, dict[str, int]]:
        with self._connect() as db:
            cursor = db.cursor()
            if qids is None:
                cursor.execute("SELECT qid, count_all, count_nodes, count_ways, count_relations FROM osm_usage")
            else:
                qid_list, dropped_qids = _normalize_qid_query_list(qids)
                _warn_dropped_rows("osm_usage query qids", dropped_qids, dropped_qids + len(qid_list))
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
            rows = cursor.fetchall() if qids is None else rows

        osm_usage_by_qid: dict[str, dict[str, int]] = {}
        dropped_rows = 0
        for qid, count_all, count_nodes, count_ways, count_relations in rows:
            qid_text = _normalize_qid(qid)
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
        with self._connect() as db:
            cursor = db.cursor()
            dropped_namespace_rows = 0
            namespace_rows: list[tuple[str, str, int]] = []
            for site_key, mapping in namespaces_by_site.items():
                for prefix, ns_id in mapping.items():
                    if isinstance(site_key, str) and isinstance(prefix, str) and isinstance(ns_id, int):
                        namespace_rows.append((site_key, prefix.lower(), int(ns_id)))
                    else:
                        dropped_namespace_rows += 1
            dropped_site_rows = 0
            site_rows: list[tuple[str, str]] = []
            for site_key, api_url in site_api_urls.items():
                if isinstance(site_key, str) and isinstance(api_url, str):
                    site_rows.append((site_key, api_url))
                else:
                    dropped_site_rows += 1
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_namespace_prefixes")
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_site_api_urls")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_namespace_prefixes (
                    site_key VARCHAR(255) NOT NULL,
                    prefix VARCHAR(255) NOT NULL,
                    ns_id INT NOT NULL,
                    PRIMARY KEY (site_key, prefix)
                )
                """
            )
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_site_api_urls (
                    site_key VARCHAR(255) NOT NULL PRIMARY KEY,
                    api_url TEXT NOT NULL
                )
                """
            )
            cursor.executemany(
                "INSERT INTO temp_namespace_prefixes (site_key, prefix, ns_id) VALUES (%s, %s, %s)",
                namespace_rows,
            )
            cursor.executemany(
                "INSERT INTO temp_site_api_urls (site_key, api_url) VALUES (%s, %s)",
                site_rows,
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
            _warn_dropped_rows("namespace_prefixes write", dropped_namespace_rows)
            _warn_dropped_rows("site_api_urls write", dropped_site_rows)
            db.commit()

    def replace_property_instances(self, property_instances_by_qid: dict[str, list[str] | set[str]]) -> None:
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
                property_rows.extend((class_qid, property_qid) for property_qid in sorted(set(valid_props)))
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_property_instances")
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
            _warn_dropped_rows("property_instances write", dropped_property_rows)
            db.commit()

    def replace_osm_usage(self, osm_usage_by_qid: dict[str, dict[str, int]]) -> None:
        with self._connect() as db:
            cursor = db.cursor()
            dropped_osm_rows = 0
            osm_rows: list[tuple[str, int, int, int, int]] = []
            for qid, row in osm_usage_by_qid.items():
                if isinstance(qid, str) and isinstance(row, dict):
                    osm_rows.append(
                        (
                            qid,
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
                    qid VARCHAR(32) NOT NULL PRIMARY KEY,
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
            sdc_rows: list[tuple[str, int]] = []
            for qid, usage_count in sdc_usage_by_qid.items():
                if isinstance(qid, str):
                    sdc_rows.append((qid, int(usage_count)))
                else:
                    dropped_sdc_rows += 1
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_sdc_usage")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_sdc_usage (
                    qid VARCHAR(32) NOT NULL PRIMARY KEY,
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
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_wiki_subscribers")
            cursor.execute(
                """
                CREATE TEMPORARY TABLE temp_wiki_subscribers (
                    qid VARCHAR(32) NOT NULL PRIMARY KEY
                )
                """
            )
            rows: list[tuple[str]] = []
            dropped_rows = 0
            seen: set[str] = set()
            for qid in wiki_subscribers:
                qid_text = _normalize_qid(qid)
                if qid_text is None:
                    dropped_rows += 1
                    continue
                if qid_text in seen:
                    continue
                seen.add(qid_text)
                rows.append((qid_text,))
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
        rows: list[tuple[str]] = []
        dropped_rows = 0
        seen: set[str] = set()
        for qid in wiki_subscribers:
            qid_text = _normalize_qid(qid)
            if qid_text is None:
                dropped_rows += 1
                continue
            if qid_text in seen:
                continue
            seen.add(qid_text)
            rows.append((qid_text,))

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

    def get_wiki_subscribers(self, qids: Iterable[str] | None = None) -> set[str]:
        with self._connect() as db:
            cursor = db.cursor()
            if qids is None:
                cursor.execute("SELECT qid FROM wiki_subscribers")
                rows = cursor.fetchall()
            else:
                qid_list, dropped_qids = _normalize_qid_query_list(qids)
                _warn_dropped_rows("wiki_subscribers query qids", dropped_qids, dropped_qids + len(qid_list))
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
            qid_text = _normalize_qid(qid)
            if qid_text is not None:
                wiki_subscribers.add(qid_text)
        return wiki_subscribers

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
                qid_list, dropped_qids = _normalize_qid_query_list(qids)
                _warn_dropped_rows("sdc_usage query qids", dropped_qids, dropped_qids + len(qid_list))
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
            qid_text = _normalize_qid(qid)
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
            raise RuntimeError("Lookup cache database has no namespace prefixes.")
        if site_url_count == 0:
            raise RuntimeError("Lookup cache database has no site API URLs.")
        if wiki_subscriber_count == 0:
            raise RuntimeError("Lookup cache database has no wiki subscriber rows.")
        if missing_property_qids:
            raise RuntimeError(
                "Lookup cache database is missing required property-instance rows for: "
                f"{', '.join(sorted(missing_property_qids))}."
            )


def create_lookup_backend(
    db_path: str | Path | None = None,
    *,
    default_database: str = "wd_notability",
) -> LookupBackend:
    backend_name = os.getenv("WD_NOTABILITY_LOOKUP_BACKEND", "sqlite").strip().lower()
    if backend_name in {"mariadb", "toolforge"}:
        database = os.getenv("WD_NOTABILITY_LOOKUP_DATABASE", default_database).strip()
        host = os.getenv("WD_NOTABILITY_LOOKUP_HOST")
        readonly = os.getenv("WD_NOTABILITY_LOOKUP_READONLY", "0").strip().lower() in {"1", "true", "yes"}
        return MariaDBLookupBackend(database=database, host=host, readonly=readonly)

    return SqliteLookupBackend(db_path if db_path is not None else Path(__file__).resolve().parent / "data" / "lookup_cache.db")

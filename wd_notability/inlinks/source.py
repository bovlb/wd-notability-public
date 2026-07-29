from __future__ import annotations

import asyncio
import os
from collections.abc import Collection
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from wd_notability.inlinks.detector import INLINKS_DETECTOR
from wd_notability.models import QID, Source
from wd_notability.replica_connection import connect_replica

# Chunk replica reads so large inlinks lookups stay within a manageable query size.
REPLICA_BATCH_SIZE = 5000
# Bound per-target inlinks payloads so one high-fanout item cannot explode memory.
INLINKS_CONTEXT_LIMIT = int(
    os.getenv("WD_NOTABILITY_INLINKS_CONTEXT_LIMIT", "1000")
)


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

    value = value.strip()
    return value if _is_qid(value) else None


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
                "WD_NOTABILITY_INLINKS_REPLICA_DEFAULTS_FILE",
                os.path.expanduser("~/replica.my.cnf"),
            )
        )
        has_replica_env = all(
            os.getenv(name)
            for name in ("REPLICADB_HOST", "REPLICADB_PORT", "REPLICADB_DATABASE")
        )
        return cls(
            enabled=_env_flag("WD_NOTABILITY_INLINKS_REPLICA_ENABLED", default=has_replica_env),
            host=os.getenv("REPLICADB_HOST", ""),
            port=int(os.getenv("REPLICADB_PORT", "0") or 0),
            database=os.getenv("REPLICADB_DATABASE", ""),
            defaults_file=defaults_file,
        )


class InlinksSource(Source):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._replica_config = ReplicaConfig.from_env()

    async def update_result(self, result, context: dict) -> None:
        inlinks = context.get("inlinks", [])
        if isinstance(inlinks, list):
            result.inlinks_count = len(inlinks)

    @staticmethod
    def _pymysql_module():
        try:
            import pymysql  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError(
                "MariaDB inlinks source requires the optional 'pymysql' Python package."
            ) from exc
        return pymysql

    def _connect_replica(self):
        pymysql = self._pymysql_module()
        return connect_replica(
            pymysql,
            host=self._replica_config.host,
            port=self._replica_config.port,
            database=self._replica_config.database,
        )

    def _query_replica_inlinks_on_connection(
        self,
        db,
        qid: str,
        *,
        limit: int | None,
    ) -> tuple[list[str], bool, dict[str, float]]:
        start = perf_counter()
        cursor = db.cursor()

        if limit is not None and limit > 0:
            query = """
                SELECT DISTINCT
                    src.page_title AS source_qid
                FROM pagelinks pl
                JOIN linktarget lt
                  ON lt.lt_id = pl.pl_target_id
                JOIN page src
                  ON src.page_id = pl.pl_from
                WHERE pl.pl_from_namespace = 0
                  AND lt.lt_namespace = 0
                  AND lt.lt_title = %s
                  AND src.page_namespace = 0
                  AND src.page_is_redirect = 0
                  AND src.page_title <> lt.lt_title
                LIMIT %s
            """
            query_started = perf_counter()
            cursor.execute(query, (qid, limit + 1))
        else:
            query = """
                SELECT DISTINCT
                    src.page_title AS source_qid
                FROM pagelinks pl
                JOIN linktarget lt
                  ON lt.lt_id = pl.pl_target_id
                JOIN page src
                  ON src.page_id = pl.pl_from
                WHERE pl.pl_from_namespace = 0
                  AND lt.lt_namespace = 0
                  AND lt.lt_title = %s
                  AND src.page_namespace = 0
                  AND src.page_is_redirect = 0
                  AND src.page_title <> lt.lt_title
            """
            query_started = perf_counter()
            cursor.execute(query, (qid,))

        query_elapsed = perf_counter() - query_started
        fetch_started = perf_counter()
        rows = cursor.fetchall()
        fetch_elapsed = perf_counter() - fetch_started
        query_total_elapsed = perf_counter() - start

        inlinks: list[str] = []
        seen: set[str] = set()
        for row in rows:
            source_value = row[0] if isinstance(row, (tuple, list)) and row else row
            source_qid = _normalize_qid(source_value)
            if source_qid is None or source_qid in seen:
                continue
            seen.add(source_qid)
            inlinks.append(source_qid)

        truncated = limit is not None and limit > 0 and len(inlinks) > limit
        if truncated:
            inlinks = inlinks[:limit]

        inlinks.sort()
        return inlinks, truncated, {
            "get_context_query": query_total_elapsed,
            "get_context_limiter_wait": 0.0,
            "get_context_retry_wait": 0.0,
            "get_context_replica_query": query_elapsed,
            "get_context_replica_fetch": fetch_elapsed,
            "get_context_replica_normalize": 0.0,
        }

    def _query_replica_inlinks_many_on_connection(
        self,
        db,
        qids: list[str],
    ) -> tuple[dict[str, list[str]], dict[str, bool], dict[str, float]]:
        if not qids:
            return {}, {}, {
                "get_context_query": 0.0,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
                "get_context_replica_query": 0.0,
                "get_context_replica_fetch": 0.0,
                "get_context_replica_normalize": 0.0,
            }

        start = perf_counter()
        placeholders = ", ".join(["%s"] * len(qids))
        query = f"""
            SELECT DISTINCT
                lt.lt_title AS target_qid,
                src.page_title AS source_qid
            FROM pagelinks pl
            JOIN linktarget lt
              ON lt.lt_id = pl.pl_target_id
            JOIN page src
              ON src.page_id = pl.pl_from
            WHERE pl.pl_from_namespace = 0
              AND lt.lt_namespace = 0
              AND lt.lt_title IN ({placeholders})
              AND src.page_namespace = 0
              AND src.page_is_redirect = 0
              AND src.page_title <> lt.lt_title
        """
        cursor = db.cursor()
        query_started = perf_counter()
        cursor.execute(query, tuple(qids))
        query_elapsed = perf_counter() - query_started
        fetch_started = perf_counter()
        rows = cursor.fetchall()
        fetch_elapsed = perf_counter() - fetch_started
        query_total_elapsed = perf_counter() - start

        inlinks_by_qid: dict[str, list[str]] = {qid: [] for qid in qids}
        seen_by_qid: dict[str, set[str]] = {qid: set() for qid in qids}
        normalize_started = perf_counter()
        for row in rows:
            if not isinstance(row, (tuple, list)) or len(row) < 2:
                continue
            target_qid = _normalize_qid(row[0])
            source_qid = _normalize_qid(row[1])
            if target_qid is None or source_qid is None:
                continue
            seen = seen_by_qid.setdefault(target_qid, set())
            if source_qid in seen:
                continue
            seen.add(source_qid)
            inlinks_by_qid.setdefault(target_qid, []).append(source_qid)

        for qid in inlinks_by_qid:
            inlinks_by_qid[qid].sort()
        normalize_elapsed = perf_counter() - normalize_started

        return inlinks_by_qid, {qid: False for qid in qids}, {
            "get_context_query": query_total_elapsed,
            "get_context_limiter_wait": 0.0,
            "get_context_retry_wait": 0.0,
            "get_context_replica_query": query_elapsed,
            "get_context_replica_fetch": fetch_elapsed,
            "get_context_replica_normalize": normalize_elapsed,
        }

    def _query_replica_inlink_counts_on_connection(
        self,
        db,
        qids: list[str],
    ) -> tuple[dict[str, int], dict[str, float]]:
        if not qids:
            return {}, {
                "count_inlinks": 0.0,
            }

        start = perf_counter()
        placeholders = ", ".join(["%s"] * len(qids))
        query = f"""
            SELECT
                lt.lt_title AS target_qid,
                COUNT(*) AS inlink_count
            FROM pagelinks pl
            JOIN linktarget lt
              ON lt.lt_id = pl.pl_target_id
            JOIN page src
              ON src.page_id = pl.pl_from
            WHERE pl.pl_from_namespace = 0
              AND lt.lt_namespace = 0
              AND lt.lt_title IN ({placeholders})
              AND src.page_namespace = 0
              AND src.page_is_redirect = 0
            GROUP BY lt.lt_title
        """

        cursor = db.cursor()
        query_started = perf_counter()
        cursor.execute(query, tuple(qids))
        query_elapsed = perf_counter() - query_started
        fetch_started = perf_counter()
        rows = cursor.fetchall()
        fetch_elapsed = perf_counter() - fetch_started
        total_elapsed = perf_counter() - start

        counts_by_qid: dict[str, int] = {qid: 0 for qid in qids}
        for target_qid, inlink_count in rows:
            normalized_target = _normalize_qid(target_qid)
            if normalized_target is None:
                continue
            try:
                counts_by_qid[normalized_target] = int(inlink_count)
            except (TypeError, ValueError):
                continue

        return counts_by_qid, {
            "count_inlinks": total_elapsed,
            "count_inlinks_query": query_elapsed,
            "count_inlinks_fetch": fetch_elapsed,
        }

    def _query_replica_inlinks(self, qids: list[str]) -> tuple[dict[str, list[str]], dict[str, bool], dict[str, float]]:
        if not qids:
            return {}, {}, {
                "get_context_query": 0.0,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
                "get_context_replica_connect": 0.0,
                "get_context_replica_query": 0.0,
                "get_context_replica_fetch": 0.0,
                "get_context_replica_normalize": 0.0,
            }

        aggregate_inlinks: dict[str, list[str]] = {}
        aggregate_truncated: dict[str, bool] = {}
        aggregate_timings: dict[str, float] = {
            "get_context_query": 0.0,
            "get_context_limiter_wait": 0.0,
            "get_context_retry_wait": 0.0,
            "get_context_replica_connect": 0.0,
            "get_context_replica_query": 0.0,
            "get_context_replica_fetch": 0.0,
            "get_context_replica_normalize": 0.0,
        }

        connect_started = perf_counter()
        with closing(self._connect_replica()) as db:
            aggregate_timings["get_context_replica_connect"] = perf_counter(
            ) - connect_started
            counts_by_qid: dict[str, int] = {}
            count_timings: dict[str, float] = {}
            if INLINKS_CONTEXT_LIMIT > 0:
                counts_by_qid, count_timings = self._query_replica_inlink_counts_on_connection(
                    db, qids)
                for key, value in count_timings.items():
                    if isinstance(value, (int, float)):
                        aggregate_timings[key] = aggregate_timings.get(
                            key, 0.0) + float(value)
            else:
                counts_by_qid = {qid: 0 for qid in qids}

            small_qids = [
                qid for qid in qids
                if INLINKS_CONTEXT_LIMIT <= 0 or counts_by_qid.get(qid, 0) <= INLINKS_CONTEXT_LIMIT
            ]
            huge_qids = [
                qid for qid in qids
                if INLINKS_CONTEXT_LIMIT > 0 and counts_by_qid.get(qid, 0) > INLINKS_CONTEXT_LIMIT
            ]

            for start in range(0, len(small_qids), REPLICA_BATCH_SIZE):
                chunk = small_qids[start: start + REPLICA_BATCH_SIZE]
                inlinks_by_qid, truncated_by_qid, timings = self._query_replica_inlinks_many_on_connection(
                    db, chunk)
                aggregate_inlinks.update(inlinks_by_qid)
                aggregate_truncated.update(truncated_by_qid)
                for key, value in timings.items():
                    if isinstance(value, (int, float)):
                        aggregate_timings[key] = aggregate_timings.get(
                            key, 0.0) + float(value)

            for qid in huge_qids:
                inlinks, truncated, timings = self._query_replica_inlinks_on_connection(
                    db, qid, limit=INLINKS_CONTEXT_LIMIT)
                aggregate_inlinks[qid] = inlinks
                aggregate_truncated[qid] = truncated
                for key, value in timings.items():
                    if isinstance(value, (int, float)):
                        aggregate_timings[key] = aggregate_timings.get(
                            key, 0.0) + float(value)

        return aggregate_inlinks, aggregate_truncated, aggregate_timings

    def _query_replica_inlink_counts(self, qids: list[str]) -> tuple[dict[str, int], dict[str, float]]:
        if not qids:
            return {}, {
                "count_inlinks": 0.0,
                "count_inlinks_query": 0.0,
                "count_inlinks_fetch": 0.0,
            }

        aggregate_counts: dict[str, int] = {}
        aggregate_timings: dict[str, float] = {
            "count_inlinks": 0.0,
            "count_inlinks_query": 0.0,
            "count_inlinks_fetch": 0.0,
        }

        connect_started = perf_counter()
        with closing(self._connect_replica()) as db:
            aggregate_timings["count_inlinks_connect"] = perf_counter(
            ) - connect_started
            for start in range(0, len(qids), REPLICA_BATCH_SIZE):
                chunk = qids[start: start + REPLICA_BATCH_SIZE]
                counts_by_qid, timings = self._query_replica_inlink_counts_on_connection(
                    db, chunk)
                aggregate_counts.update(counts_by_qid)
                for key, value in timings.items():
                    if isinstance(value, (int, float)):
                        aggregate_timings[key] = aggregate_timings.get(
                            key, 0.0) + float(value)

        return aggregate_counts, aggregate_timings

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict]:
        qid_list = [qid for qid in qids if _is_qid(qid)]
        contexts: dict[QID, dict] = {}

        if not self._replica_config.enabled:
            raise RuntimeError(
                "Inlinks replica access is disabled or unavailable")
        if not qid_list:
            return contexts

        if len(qid_list) == 1:
            with closing(self._connect_replica()) as db:
                aggregate_timings = {
                    "get_context_query": 0.0,
                    "get_context_limiter_wait": 0.0,
                    "get_context_retry_wait": 0.0,
                    "get_context_replica_connect": 0.0,
                    "get_context_replica_query": 0.0,
                    "get_context_replica_fetch": 0.0,
                    "get_context_replica_normalize": 0.0,
                }
                single_qid = qid_list[0]
                inlinks, truncated, timings = self._query_replica_inlinks_on_connection(
                    db, single_qid, limit=INLINKS_CONTEXT_LIMIT
                )
                for key, value in timings.items():
                    if isinstance(value, (int, float)):
                        aggregate_timings[key] = aggregate_timings.get(key, 0.0) + float(value)
                inlinks_by_qid = {single_qid: inlinks}
                truncated_by_qid = {single_qid: truncated}
        else:
            inlinks_by_qid, truncated_by_qid, aggregate_timings = await asyncio.to_thread(
                self._query_replica_inlinks,
                qid_list,
            )

        for qid in qid_list:
            normalized_inlinks: list[str] = []
            raw_inlinks = inlinks_by_qid.get(qid, [])
            if not isinstance(raw_inlinks, list):
                raw_inlinks = []
            for inlink in raw_inlinks:
                normalized_inlink = _normalize_qid(inlink)
                if normalized_inlink is not None:
                    normalized_inlinks.append(normalized_inlink)
            contexts[qid] = {
                "id": qid,
                "inlinks": normalized_inlinks,
                "truncated": bool(truncated_by_qid.get(qid, False)),
                "_timings": aggregate_timings,
            }

        return contexts

    async def count_inlinks(self, qids: Collection[QID]) -> tuple[dict[QID, int], dict[str, float]]:
        """Count the number of inlinks for each QID.

        Args:
            qids: Collection of Wikidata QIDs to count inlinks for.

        Returns:
            Tuple of (counts dict mapping QID to inlink count, timings dict with performance metrics).
        """
        qid_list = [qid for qid in qids if _is_qid(qid)]
        counts: dict[QID, int] = {}

        if not self._replica_config.enabled:
            raise RuntimeError(
                "Inlinks replica access is disabled or unavailable")
        if not qid_list:
            return counts, {
                "count_inlinks": 0.0,
                "count_inlinks_query": 0.0,
                "count_inlinks_fetch": 0.0,
            }

        counts_by_qid, aggregate_timings = await asyncio.to_thread(
            self._query_replica_inlink_counts,
            qid_list,
        )

        for qid in qid_list:
            counts[qid] = int(counts_by_qid.get(qid, 0) or 0)

        return counts, aggregate_timings

    def report_urls(self, qid: QID, context: dict) -> dict[str, str]:
        return {
            "api_url": (
                "https://www.wikidata.org/w/api.php"
                f"?action=query&list=backlinks&bltitle={qid}&blnamespace=0&bllimit=max&format=json"
            ),
            "ui_url": f"https://www.wikidata.org/wiki/Special:WhatLinksHere/{qid}",
        }


# Shared source instance used by the inlinks worker and the web/API paths.
INLINKS_SOURCE = InlinksSource(name="inlinks", detectors={INLINKS_DETECTOR})

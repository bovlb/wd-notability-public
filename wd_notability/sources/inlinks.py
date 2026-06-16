from __future__ import annotations

import asyncio
import configparser
import os
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import ClassVar

from wd_notability.detectors.inlinks import INLINKS_DETECTOR
from wd_notability.models import QID, Source


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
        return cls(
            enabled=_env_flag(
                "WD_NOTABILITY_INLINKS_REPLICA_ENABLED",
                default=defaults_file.exists(),
            ),
            host=os.getenv("WD_NOTABILITY_INLINKS_REPLICA_HOST", "localhost"),
            port=int(os.getenv("WD_NOTABILITY_INLINKS_REPLICA_PORT", "3306")),
            database=os.getenv("WD_NOTABILITY_INLINKS_REPLICA_DATABASE", "wikidatawiki_p"),
            defaults_file=defaults_file,
        )


class InlinksSource(Source):
    WIKIDATA_API_URL: ClassVar[str] = "https://www.wikidata.org/w/api.php"
    MAX_INLINKS_PER_TARGET: ClassVar[int] = 100

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._replica_config = ReplicaConfig.from_env()

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
        if not self._replica_config.defaults_file.exists():
            raise RuntimeError(
                f"Toolforge replica credential file is missing: {self._replica_config.defaults_file}"
            )

        config = configparser.ConfigParser(interpolation=None)
        config.read(self._replica_config.defaults_file)
        client = config["client"] if "client" in config else {}
        return pymysql.connect(
            user=client.get("user"),
            password=client.get("password"),
            host=self._replica_config.host,
            port=self._replica_config.port,
            database=self._replica_config.database,
            charset="utf8mb4",
        )

    def _query_replica_inlinks(self, qids: list[str]) -> tuple[dict[str, list[str]], dict[str, bool], dict[str, float]]:
        if not qids:
            return {}, {}, {
                "get_context_query": 0.0,
                "get_context_limiter_wait": 0.0,
                "get_context_retry_wait": 0.0,
            }

        start = perf_counter()
        # Note sort by page_id as cheap proxy for sorting by creation time, to ensure we get the oldest inlinks when truncating.
        query = """
            SELECT src.page_title AS source_qid
            FROM pagelinks pl
            JOIN linktarget lt
              ON lt.lt_id = pl.pl_target_id
            JOIN page src
              ON src.page_id = pl.pl_from
            WHERE pl.pl_from_namespace = 0
              AND lt.lt_namespace = 0
              AND lt.lt_title = %s
              AND src.page_namespace = 0
            ORDER BY src.page_id 
            LIMIT %s
        """

        with self._connect_replica() as db:
            cursor = db.cursor()
            rows_by_target: dict[str, list[str]] = {}
            truncated_by_qid: dict[str, bool] = {}
            for qid in qids:
                cursor.execute(query, (qid, self.MAX_INLINKS_PER_TARGET + 1))
                rows = cursor.fetchall()
                rows_by_target[qid] = [row[0] for row in rows[: self.MAX_INLINKS_PER_TARGET]]
                truncated_by_qid[qid] = len(rows) > self.MAX_INLINKS_PER_TARGET
        elapsed = perf_counter() - start

        inlinks_by_qid: dict[str, list[str]] = {qid: [] for qid in qids}
        for target_qid, raw_inlinks in rows_by_target.items():
            seen: set[str] = set()
            for source_qid in raw_inlinks:
                normalized_target = _normalize_qid(target_qid)
                normalized_source = _normalize_qid(source_qid)
                if normalized_target is None or normalized_source is None:
                    continue
                if normalized_source in seen:
                    continue
                seen.add(normalized_source)
                inlinks_by_qid[normalized_target].append(normalized_source)

        for qid in inlinks_by_qid:
            inlinks_by_qid[qid].sort()

        return inlinks_by_qid, truncated_by_qid, {
            "get_context_query": elapsed,
            "get_context_limiter_wait": 0.0,
            "get_context_retry_wait": 0.0,
        }

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict]:
        qid_list = [qid for qid in qids if _is_qid(qid)]
        contexts: dict[QID, dict] = {}

        if not self._replica_config.enabled:
            raise RuntimeError("Inlinks replica access is disabled or unavailable")

        for start in range(0, len(qid_list), 500):
            chunk = qid_list[start : start + 500]
            inlinks_by_qid, truncated_by_qid, timings = await asyncio.to_thread(self._query_replica_inlinks, chunk)
            for qid in chunk:
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
                    "_timings": timings,
                }

        return contexts

    def report_urls(self, qid: QID, context: dict) -> dict[str, str]:
        return {
            "api_url": (
                "https://www.wikidata.org/w/api.php"
                f"?action=query&list=backlinks&bltitle={qid}&blnamespace=0&bllimit=max&format=json"
            ),
            "ui_url": f"https://www.wikidata.org/wiki/Special:WhatLinksHere/{qid}",
        }


INLINKS_SOURCE = InlinksSource(name="inlinks", detectors={INLINKS_DETECTOR})

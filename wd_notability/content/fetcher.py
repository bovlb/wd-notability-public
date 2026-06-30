from __future__ import annotations

import asyncio
from collections.abc import Collection
import os
from dataclasses import dataclass
from contextlib import closing
from pathlib import Path
from time import perf_counter

from wd_notability.content.detectors import IDENTIFIERS_DETECTOR, SITELINKS_DETECTOR, SOURCES_DETECTOR
from wd_notability.content.outlinks import extract_outlinks
from wd_notability.models import QID, Source
from wd_notability.replica_connection import connect_replica
from wd_notability.wikidata import EntityDeletedError
from wd_notability.wikidata_api import (
    WIKIDATA_API_URL,
    WikidataBackoffActiveError,
    WikidataRetryAfterError,
    wikidata_session,
)
from wd_notability.async_limiters import WIKIDATA_ACTION_API_LIMITER

ENTITYDATA_FETCH_CHUNK_SIZE = 50
ENTITYDATA_FETCH_CONCURRENCY = max(1, int(os.getenv("WD_NOTABILITY_ENTITYDATA_FETCH_CONCURRENCY", "10")))


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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


@dataclass(slots=True, frozen=True)
class _ReplicaConfig:
    enabled: bool
    host: str
    port: int
    database: str
    defaults_file: Path

    @classmethod
    def from_env(cls) -> "_ReplicaConfig":
        defaults_file = Path(
            os.getenv(
                "WD_NOTABILITY_ENTITYDATA_REPLICA_DEFAULTS_FILE",
                os.path.expanduser("~/replica.my.cnf"),
            )
        )
        return cls(
            enabled=_env_flag(
                "WD_NOTABILITY_ENTITYDATA_REPLICA_ENABLED",
                default=defaults_file.exists(),
            ),
            host=os.getenv(
                "WD_NOTABILITY_REPLICA_HOST",
                os.getenv("WD_NOTABILITY_ENTITYDATA_REPLICA_HOST", "wikidatawiki.analytics.db.svc.wikimedia.cloud"),
            ),
            port=int(os.getenv("WD_NOTABILITY_ENTITYDATA_REPLICA_PORT", "3306")),
            database=os.getenv("WD_NOTABILITY_ENTITYDATA_REPLICA_DATABASE", "wikidatawiki_p"),
            defaults_file=defaults_file,
        )


class _ContentFetcher(Source):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._replica_config = _ReplicaConfig.from_env()

    def _parse_lastrevid(self, value: object) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    @staticmethod
    def _pymysql_module():
        try:
            import pymysql  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError("Content replica access requires the optional 'pymysql' Python package.") from exc
        return pymysql

    def _connect_replica(self):
        pymysql = self._pymysql_module()
        return connect_replica(
            pymysql,
            defaults_file=self._replica_config.defaults_file,
            host=self._replica_config.host,
            port=self._replica_config.port,
            database=self._replica_config.database,
        )

    def _extract_entity(self, qid: str, payload: dict) -> tuple[dict, bool] | Exception:
        entities = payload.get("entities", {}) if isinstance(payload, dict) else {}
        if not isinstance(entities, dict) or not entities:
            return ValueError(f"Entity {qid} not found in data")

        if qid in entities:
            entity = entities[qid]
            if not isinstance(entity, dict):
                return ValueError(f"Entity {qid} not found in data")
            if entity.get("missing") is not None or entity.get("deleted") is not None:
                return EntityDeletedError(qid)
            entity_id = entity.get("id")
            is_redirect = isinstance(entity_id, str) and entity_id != qid
            return entity, is_redirect

        if len(entities) != 1:
            return ValueError(f"Entity {qid} not found in data")

        entity = next(iter(entities.values()))
        if not isinstance(entity, dict):
            return ValueError(f"Entity {qid} not found in data")
        if entity.get("missing") is not None or entity.get("deleted") is not None:
            return EntityDeletedError(qid)

        entity_id = entity.get("id")
        if not isinstance(entity_id, str):
            return ValueError(f"Entity {qid} not found in data")

        return entity, entity_id != qid

    def _context_from_payload(self, qid: str, payload: dict) -> dict | Exception:
        extracted = self._extract_entity(qid, payload)
        if isinstance(extracted, Exception):
            return extracted

        entity, is_redirect = extracted
        outlinks = sorted(extract_outlinks(entity, self_qid=qid))
        return {
            "entity": entity,
            "is_redirect": is_redirect,
            "has_claims": "claims" in entity and bool(entity["claims"]),
            "has_sitelinks": "sitelinks" in entity and bool(entity["sitelinks"]),
            "lastrevid": self._parse_lastrevid(entity.get("lastrevid")),
            "outlinks": outlinks,
        }

    async def _get_context_chunk(self, chunk: list[str]) -> dict[str, dict | Exception]:
        if not chunk:
            return {}

        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "props": "info|claims|sitelinks",
            "format": "json",
        }
        try:
            response, timings = await wikidata_session.get_with_timings(
                WIKIDATA_API_URL,
                limiter=WIKIDATA_ACTION_API_LIMITER,
                params=params,
            )
            response.raise_for_status()
            global_timings = await wikidata_session.timing_snapshot()
            print(f"Fetched entity data: {len(chunk)} items {timings}; {global_timings}")
            payload = response.json()
        except (WikidataBackoffActiveError, WikidataRetryAfterError):
            raise
        except Exception as exc:  # noqa: BLE001
            return {qid: exc for qid in chunk}

        entities = payload.get("entities", {}) if isinstance(payload, dict) else {}
        replica_state: dict[str, tuple[int | None, str | None]] = {}
        redirect_qids = {
            qid
            for qid in chunk
            if isinstance(entities.get(qid), dict)
            and isinstance(entities[qid].get("id"), str)
            and entities[qid].get("id") != qid
        }
        if self._replica_config.enabled and redirect_qids:
            try:
                replica_start = perf_counter()
                replica_state = await asyncio.to_thread(self._query_replica_page_state, sorted(redirect_qids))
                replica_elapsed = perf_counter() - replica_start
            except Exception as exc:  # noqa: BLE001
                return {qid: exc for qid in redirect_qids}
            else:
                replica_elapsed = max(0.0, replica_elapsed)
                redirect_timing_qid = sorted(redirect_qids)[0] if redirect_qids else None
        else:
            replica_elapsed = 0.0
            redirect_timing_qid = None

        chunk_contexts: dict[str, dict | Exception] = {}
        for qid in chunk:
            context = self._context_from_payload(qid, payload)
            if isinstance(context, dict):
                if self._replica_config.enabled and context["is_redirect"]:
                    replica_row = replica_state.get(qid)
                    if replica_row is None:
                        context = ValueError(f"Entity {qid} not found in replica page table")
                    else:
                        replica_lastrevid, replica_redirect_target = replica_row
                        if replica_lastrevid is None:
                            context = ValueError(f"Entity {qid} has no source revision on replica")
                        else:
                            entity_target = _normalize_qid(context["entity"].get("id"))
                            if replica_redirect_target != entity_target:
                                context = ValueError(
                                    f"Entity {qid} redirect target changed while evaluating: "
                                    f"expected {entity_target}, found {replica_redirect_target}"
                                )
                            else:
                                context["entity_lastrevid"] = context.get("lastrevid")
                                context["redirect_target"] = replica_redirect_target
                                context["source_lastrevid"] = replica_lastrevid
                                context["lastrevid"] = replica_lastrevid
                else:
                    context["entity_lastrevid"] = context.get("lastrevid")
                    context["source_lastrevid"] = context.get("lastrevid")
                context["_timings"] = timings.as_dict("get_context")
                if self._replica_config.enabled and context["is_redirect"] and qid == redirect_timing_qid:
                    context["_timings"]["redirect_replica_lookup"] = replica_elapsed
            chunk_contexts[qid] = context

        return chunk_contexts

    def _query_replica_page_state(self, qids: list[str]) -> dict[str, tuple[int | None, str | None]]:
        if not qids:
            return {}

        placeholders = ", ".join(["%s"] * len(qids))
        query = f"""
            SELECT
                p.page_title,
                p.page_latest,
                rd.rd_namespace,
                rd.rd_title
            FROM page p
            LEFT JOIN redirect rd
              ON rd.rd_from = p.page_id
            WHERE p.page_namespace = 0
              AND p.page_title IN ({placeholders})
        """

        result: dict[str, tuple[int | None, str | None]] = {}
        with closing(self._connect_replica()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, qids)
                for page_title, page_latest, rd_namespace, rd_title in cursor.fetchall():
                    normalized_qid = _normalize_qid(page_title)
                    if normalized_qid is None:
                        continue
                    latest_revid = None
                    try:
                        latest_revid = int(page_latest)
                    except (TypeError, ValueError):
                        latest_revid = None
                    redirect_target: str | None = None
                    try:
                        if int(rd_namespace) == 0 and rd_title is not None:
                            redirect_target = _normalize_qid(rd_title)
                    except (TypeError, ValueError):
                        redirect_target = None
                    result[normalized_qid] = (latest_revid, redirect_target)
        return result

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict | Exception]:
        qid_list = [qid for qid in qids if isinstance(qid, str)]
        contexts: dict[QID, dict | Exception] = {}

        chunks = [qid_list[start : start + ENTITYDATA_FETCH_CHUNK_SIZE] for start in range(0, len(qid_list), ENTITYDATA_FETCH_CHUNK_SIZE)]
        if not chunks:
            return contexts

        semaphore = asyncio.Semaphore(min(len(chunks), ENTITYDATA_FETCH_CONCURRENCY))

        async def _run_chunk(chunk: list[str]) -> dict[str, dict | Exception]:
            async with semaphore:
                return await self._get_context_chunk(chunk)

        chunk_contexts = await asyncio.gather(*(_run_chunk(chunk) for chunk in chunks))
        for chunk_context in chunk_contexts:
            contexts.update(chunk_context)

        return contexts

    async def update_result(self, result, context: dict) -> None:
        result.is_redirect = context["is_redirect"]
        result.has_claims = context["has_claims"]
        result.has_claims_known = True
        result.has_sitelinks = context["has_sitelinks"]
        result.entitydata_last_revid = context.get("lastrevid")

    def detector_context(self, context: dict) -> dict:
        return context["entity"]

    def report_urls(self, qid: QID, context: dict) -> dict[str, str]:
        target_qid = context.get("entity", {}).get("id", qid)
        return {
            "api_url": f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            "ui_url": f"https://www.wikidata.org/wiki/{target_qid}",
        }


CONTENT_SOURCE = _ContentFetcher(
    name="entity_data",
    detectors={
        SITELINKS_DETECTOR,
        IDENTIFIERS_DETECTOR,
        SOURCES_DETECTOR,
    },
)

ENTITY_DATA_SOURCE = CONTENT_SOURCE

__all__ = [
    "CONTENT_SOURCE",
    "ENTITY_DATA_SOURCE",
]

from collections.abc import Collection

from wd_notability.detectors.identifiers import IDENTIFIERS_DETECTOR
from wd_notability.detectors.sitelinks import SITELINKS_DETECTOR
from wd_notability.detectors.sources import SOURCES_DETECTOR
from wd_notability.models import QID, Source
from wd_notability.wikidata_api import (
    WIKIDATA_API_URL,
    WikidataBackoffActiveError,
    WikidataRetryAfterError,
    wikidata_session,
)
from wd_notability.async_limiters import WIKIDATA_ACTION_API_LIMITER


class EntityDataSource(Source):
    def _parse_lastrevid(self, value: object) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    def _extract_entity(self, qid: str, payload: dict) -> tuple[dict, bool] | Exception:
        entities = payload.get("entities", {}) if isinstance(payload, dict) else {}
        if not isinstance(entities, dict) or not entities:
            return ValueError(f"Entity {qid} not found in data")

        if qid in entities:
            entity = entities[qid]
            if not isinstance(entity, dict):
                return ValueError(f"Entity {qid} not found in data")
            entity_id = entity.get("id")
            is_redirect = isinstance(entity_id, str) and entity_id != qid
            return entity, is_redirect

        if len(entities) != 1:
            return ValueError(f"Entity {qid} not found in data")

        entity = next(iter(entities.values()))
        if not isinstance(entity, dict):
            return ValueError(f"Entity {qid} not found in data")

        entity_id = entity.get("id")
        if not isinstance(entity_id, str):
            return ValueError(f"Entity {qid} not found in data")

        return entity, entity_id != qid

    def _context_from_payload(self, qid: str, payload: dict) -> dict | Exception:
        extracted = self._extract_entity(qid, payload)
        if isinstance(extracted, Exception):
            return extracted

        entity, is_redirect = extracted
        return {
            "entity": entity,
            "is_redirect": is_redirect,
            "has_claims": "claims" in entity and bool(entity["claims"]),
            "has_sitelinks": "sitelinks" in entity and bool(entity["sitelinks"]),
            "lastrevid": self._parse_lastrevid(entity.get("lastrevid")),
        }

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict | Exception]:
        qid_list = [qid for qid in qids if isinstance(qid, str)]
        contexts: dict[QID, dict | Exception] = {}

        for start in range(0, len(qid_list), 50):
            chunk = qid_list[start : start + 50]
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
                print(f"Fetched entity data: {len(chunk)} items {timings}")
                response.raise_for_status()
                payload = response.json()
            except (WikidataBackoffActiveError, WikidataRetryAfterError):
                raise
            except Exception as exc:  # noqa: BLE001
                for qid in chunk:
                    contexts[qid] = exc
                continue

            for qid in chunk:
                context = self._context_from_payload(qid, payload)
                if isinstance(context, dict):
                    context["_timings"] = timings.as_dict("get_context")
                contexts[qid] = context

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


ENTITY_DATA_SOURCE = EntityDataSource(
    name="entity_data",
    detectors={
        SITELINKS_DETECTOR, 
        IDENTIFIERS_DETECTOR, 
        SOURCES_DETECTOR,
    },
)

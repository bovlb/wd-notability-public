from __future__ import annotations

from collections.abc import Collection
from urllib.parse import quote_plus

from wd_notability.external_usage.osm.detector import OSM_DETECTOR
from wd_notability.lookup_cache import lookup_cache
from wd_notability.models import NotabilityCriterion, NotabilityLevel, QID, Source


class OsmSource(Source):
    def _overpass_turbo_url(self, qid: str) -> str:
        query = f"""[out:json][timeout:25];\n\
nwr["wikidata"="{qid}"];\n\
out geom;"""
        return f"https://overpass-turbo.eu/?Q={quote_plus(query)}"

    def report_urls(self, qid: QID, context: dict) -> dict[str, str]:
        return {"ui_url": self._overpass_turbo_url(qid)}

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict]:
        qid_list = [qid for qid in qids if isinstance(qid, str)]
        usage_by_qid = lookup_cache.get_osm_usage_for(qid_list)
        return {
            qid: {
                "qid": qid,
                "row": usage_by_qid.get(qid, {}),
                "object_explorer_url": self._overpass_turbo_url(qid),
            }
            for qid in qid_list
        }

    async def refresh_cache(
        self,
        cache,
        usage_by_qid: dict[str, dict[str, int]],
        *,
        clear_missing: bool = True,
        create_missing: bool = True,
    ) -> int:
        raise RuntimeError("OSM cache materialization has been removed")


OSM_SOURCE = OsmSource(name="osm", detectors={OSM_DETECTOR})

__all__ = [
    "OSM_SOURCE",
    "OsmSource",
]

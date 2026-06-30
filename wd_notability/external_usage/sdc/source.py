from __future__ import annotations

from collections.abc import Collection
from typing import ClassVar
from urllib.parse import quote

from wd_notability.external_usage.sdc.detector import SDC_USAGE_DETECTOR
from wd_notability.lookup_cache import lookup_cache
from wd_notability.models import NotabilityCriterion, NotabilityLevel, QID, Source


class SdcSource(Source):
    COMMONS_API_URL: ClassVar[str] = "https://commons.wikimedia.org/w/api.php"

    SDC_ITEM_PROPERTIES: ClassVar[list[str]] = [
        "P180",
        "P921",
        "P170",
        "P195",
        "P186",
        "P276",
        "P1071",
        "P6243",
    ]

    def _search_query(self, qid: str) -> str:
        clauses = [f"{prop}={qid}" for prop in self.SDC_ITEM_PROPERTIES]
        if not clauses:
            return ""
        return f"haswbstatement:{clauses[0]}|{'|'.join(clauses[1:])}"

    def report_urls(self, qid: QID, context: dict) -> dict[str, str]:
        search_query = self._search_query(qid)
        return {
            "ui_url": (
                "https://commons.wikimedia.org/wiki/Special:MediaSearch"
                f"?type=image&search={quote(search_query, safe='')}"
            )
        }

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict]:
        qid_list = [qid for qid in qids if isinstance(qid, str)]
        usage_by_qid = lookup_cache.get_sdc_usage_for(qid_list)
        return {
            qid: {
                "qid": qid,
                "search_query": self._search_query(qid),
                "usage_count": int(usage_by_qid.get(qid, 0)),
            }
            for qid in qid_list
        }

    async def refresh_cache(self, cache, usage_by_qid: dict[str, int]) -> int:
        qid_list = [qid for qid in usage_by_qid if isinstance(qid, str)]
        if not qid_list:
            return 0
        return await cache.sync_criterion(
            NotabilityCriterion.N3_SDC,
            NotabilityLevel.STRONG,
            set(qid_list),
        )


SDC_SOURCE = SdcSource(name="sdc", detectors={SDC_USAGE_DETECTOR})

__all__ = [
    "SDC_SOURCE",
    "SdcSource",
]

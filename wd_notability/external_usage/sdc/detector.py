from __future__ import annotations

from collections.abc import AsyncGenerator
from urllib.parse import quote

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult


class SdcUsageDetector(Detector):
    def __init__(self) -> None:
        super().__init__("sdc_usage", NotabilityCriterion.N3_SDC)

    def _media_search_url(self, query: str) -> str:
        return (
            "https://commons.wikimedia.org/wiki/Special:MediaSearch"
            f"?type=image&search={quote(query, safe='')}"
        )

    async def detect(self, context: dict) -> AsyncGenerator[SignalResult, None]:
        qid = context.get("qid")
        if not isinstance(qid, str):
            return

        try:
            usage_count = int(context.get("usage_count", 0))
        except (TypeError, ValueError):
            usage_count = 0

        if usage_count <= 0:
            yield self.make_signal(
                level=NotabilityLevel.NONE,
                key="sdc_usage_none",
                properties={
                    "count": 0,
                    "url": self._media_search_url(str(context.get("search_query", ""))),
                },
            )
            return

        yield self.make_signal(
            level=NotabilityLevel.STRONG,
            key="sdc_usage",
            properties={
                "count": usage_count,
                "url": self._media_search_url(str(context.get("search_query", ""))),
            },
        )


SDC_USAGE_DETECTOR = SdcUsageDetector()

__all__ = [
    "SDC_USAGE_DETECTOR",
    "SdcUsageDetector",
]

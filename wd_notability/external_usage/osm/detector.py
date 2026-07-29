from __future__ import annotations

from collections.abc import AsyncGenerator

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult


class OsmDetector(Detector):
    def __init__(self) -> None:
        super().__init__("osm", NotabilityCriterion.N3_OSM)

    def _to_int(self, value: object) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    async def detect(self, context: dict) -> AsyncGenerator[SignalResult, None]:
        qid = context.get("qid")
        if not isinstance(qid, str):
            return

        row = context.get("row", {})
        if not isinstance(row, dict):
            return

        usage_count = self._to_int(row.get("count_all"))
        if usage_count <= 0:
            yield self.make_signal(
                level=NotabilityLevel.NONE,
                key="osm_none",
                properties={
                    "usage_count": 0,
                    "count_nodes": self._to_int(row.get("count_nodes")),
                    "count_ways": self._to_int(row.get("count_ways")),
                    "count_relations": self._to_int(row.get("count_relations")),
                    "object_explorer_url": context.get("object_explorer_url"),
                },
            )
            return

        yield self.make_signal(
            level=NotabilityLevel.WEAK,
            key="osm",
            properties={
                "usage_count": usage_count,
                "count_nodes": self._to_int(row.get("count_nodes")),
                "count_ways": self._to_int(row.get("count_ways")),
                "count_relations": self._to_int(row.get("count_relations")),
                "object_explorer_url": context.get("object_explorer_url"),
            },
        )


OSM_DETECTOR = OsmDetector()

__all__ = [
    "OSM_DETECTOR",
    "OsmDetector",
]

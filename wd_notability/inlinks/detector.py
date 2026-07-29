from __future__ import annotations

from collections.abc import AsyncGenerator

from wd_notability.evaluation_cache import CACHE
from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult


class InlinksDetector(Detector):
    def __init__(self) -> None:
        super().__init__("inlinks", NotabilityCriterion.N3_INLINKS)

    async def detect(self, context: dict) -> AsyncGenerator[SignalResult, None]:
        qid = context.get("id")
        if not isinstance(qid, str):
            return

        inlinks = context.get("inlinks", [])
        if not isinstance(inlinks, list):
            return

        inlinks = [inlink for inlink in inlinks if isinstance(inlink, str)]
        truncated = bool(context.get("truncated", False))
        if not inlinks:
            if truncated:
                yield self.make_signal(
                    level=NotabilityLevel.UNKNOWN,
                    key="truncated_so_unknown",
                    properties={"truncated": True},
                )
                return
            yield self.make_signal(level=NotabilityLevel.NONE, key="inlinks_none")
            return

        cached_inlinks = await CACHE.get_many(inlinks)
        saw_unknown = False
        saw_strong = False

        for inlink in inlinks:
            if inlink == qid:
                continue
            cached_row = cached_inlinks.get(inlink)
            if cached_row is None:
                saw_unknown = True
                yield self.make_signal(
                    level=NotabilityLevel.UNKNOWN,
                    key="inlinks_unknown",
                    properties={"qid": inlink},
                )
                continue
            level = cached_row.n12
            if level == NotabilityLevel.STRONG:
                saw_strong = True
            elif level == NotabilityLevel.UNKNOWN:
                saw_unknown = True
            yield self.make_signal(level=level, key="inlinks", properties={"qid": inlink})

        if truncated and saw_strong:
            yield self.make_signal(
                level=NotabilityLevel.STRONG,
                key="truncated_but_strong",
                properties={"truncated": True},
            )
            return

        if truncated:
            yield self.make_signal(
                level=NotabilityLevel.UNKNOWN,
                key="truncated_so_unknown",
                properties={"truncated": True},
            )


# Shared detector instance registered with the inlinks source.
INLINKS_DETECTOR = InlinksDetector()

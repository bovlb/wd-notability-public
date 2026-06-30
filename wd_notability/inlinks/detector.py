from __future__ import annotations

from collections.abc import AsyncGenerator

from wd_notability.evaluation_cache import CACHE
from wd_notability.models import Detector, EvaluationResult, NotabilityCriterion, NotabilityLevel, SignalResult


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
        if not inlinks:
            yield self.make_signal(level=NotabilityLevel.NONE, key="inlinks_none")
            return

        cached_inlinks = await CACHE.get_many(inlinks)

        unresolved_inlinks = False
        for inlink in inlinks:
            if inlink == qid:
                continue
            cached_row = cached_inlinks.get(inlink)
            if cached_row is None:
                unresolved_inlinks = True
                continue
            er = EvaluationResult.from_summary(qid=inlink, summary=cached_row[0])
            level = er.levels.get(NotabilityCriterion.N12, NotabilityLevel.NONE)
            yield self.make_signal(level=level, key="inlinks", properties={"qid": inlink})
            if level == NotabilityLevel.STRONG:
                return

        if unresolved_inlinks:
            yield self.make_signal(level=NotabilityLevel.UNKNOWN, key="inlinks_unknown")


INLINKS_DETECTOR = InlinksDetector()

from __future__ import annotations

from collections.abc import AsyncGenerator

from wd_notability.models import Detector, NotabilityCriterion, NotabilityLevel, SignalResult


class WikiSubscribersDetector(Detector):
    def __init__(self) -> None:
        super().__init__("wiki_subscribers", NotabilityCriterion.N3_WIKISUB)

    async def detect(self, context: dict) -> AsyncGenerator[SignalResult, None]:
        qid = context.get("qid")
        if not isinstance(qid, str):
            return

        is_subscribed = context.get("is_subscribed")
        if not isinstance(is_subscribed, bool):
            return

        if is_subscribed:
            yield self.make_signal(
                level=NotabilityLevel.STRONG,
                key="wikis_subscribed_to_entity",
                properties={
                    "url": f"https://www.wikidata.org/w/index.php?title={qid}&action=info",
                },
            )
        else:
            yield self.make_signal(
                level=NotabilityLevel.NONE,
                key="wikis_subscribed_to_entity_none",
            )


WIKI_SUBSCRIBERS_DETECTOR = WikiSubscribersDetector()

from __future__ import annotations

from collections.abc import Collection
from time import perf_counter

from wd_notability.detectors.wiki_subscribers import WIKI_SUBSCRIBERS_DETECTOR
from wd_notability.lookup_cache import lookup_cache
from wd_notability.models import QID, Source


class WikiUsageSource(Source):
    def report_urls(self, qid: QID, context: dict) -> dict[str, str]:
        return {
            "ui_url": f"https://www.wikidata.org/w/index.php?title={qid}&action=info",
        }

    async def get_contexts(self, qids: Collection[QID]) -> dict[QID, dict]:
        qid_list = [qid for qid in qids if isinstance(qid, str)]
        if not qid_list:
            return {}

        start = perf_counter()
        subscribed_qids = lookup_cache.get_wiki_subscribers_for(qid_list)
        elapsed = perf_counter() - start

        return {
            qid: {
                "qid": qid,
                "is_subscribed": qid in subscribed_qids,
                "_timings": {
                    "get_context": elapsed,
                },
            }
            for qid in qid_list
        }


WIKI_USAGE_SOURCE = WikiUsageSource(name="wiki_usage", detectors={WIKI_SUBSCRIBERS_DETECTOR})

import pytest

from wd_notability.detectors.wiki_subscribers import WikiSubscribersDetector
from wd_notability.models import NotabilityLevel


async def collect_signals(detector, context):
    return [signal async for signal in detector.detect(context)]


@pytest.mark.asyncio
async def test_wiki_subscribers_detector_emits_none_when_no_subscribers():
    detector = WikiSubscribersDetector()

    signals = await collect_signals(detector, {"qid": "Q42", "is_subscribed": False})

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.NONE
    assert signals[0].key == "wikis_subscribed_to_entity_none"
    assert signals[0].properties == {}


@pytest.mark.asyncio
async def test_wiki_subscribers_detector_emits_signal_without_site_url():
    detector = WikiSubscribersDetector()

    signals = await collect_signals(
        detector,
        {"qid": "Q42", "is_subscribed": True},
    )

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.STRONG
    assert signals[0].properties["url"] == "https://www.wikidata.org/w/index.php?title=Q42&action=info"

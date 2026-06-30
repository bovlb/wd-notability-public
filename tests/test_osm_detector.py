import pytest

from wd_notability.external_usage.osm.detector import OsmDetector
from wd_notability.models import NotabilityLevel


async def collect_signals(detector, context):
    return [signal async for signal in detector.detect(context)]


@pytest.mark.asyncio
async def test_osm_detector_emits_none_when_usage_is_missing():
    detector = OsmDetector()

    signals = await collect_signals(detector, {"qid": "Q42", "row": {}})

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.NONE
    assert signals[0].key == "osm_none"
    assert signals[0].properties["usage_count"] == 0

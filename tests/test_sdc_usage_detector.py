import pytest

from wd_notability.external_usage.sdc.detector import SdcUsageDetector
from wd_notability.models import NotabilityLevel


async def collect_signals(detector, context):
    return [signal async for signal in detector.detect(context)]


@pytest.mark.asyncio
async def test_sdc_usage_detector_emits_none_when_usage_is_missing():
    detector = SdcUsageDetector()

    signals = await collect_signals(detector, {"qid": "Q42", "usage_count": 0, "search_query": "Q42"})

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.NONE
    assert signals[0].key == "sdc_usage_none"
    assert signals[0].properties["count"] == 0

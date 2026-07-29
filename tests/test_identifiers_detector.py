import pytest

from wd_notability.content.identifiers import IdentifiersDetector
from wd_notability.models import NotabilityLevel


async def collect_signals(detector, entity):
    return [signal async for signal in detector.detect(entity)]


@pytest.mark.asyncio
async def test_identifiers_detector_treats_p963_as_weak(monkeypatch):
    detector = IdentifiersDetector()
    monkeypatch.setattr(IdentifiersDetector, "ONLINE_ACCOUNTS_PROPERTIES", set())
    monkeypatch.setattr(IdentifiersDetector, "AUTHORITY_CONTROL_PROPERTIES", set())

    signals = await collect_signals(
        detector,
        {
            "claims": {
                "P963": [
                    {
                        "mainsnak": {
                            "datatype": "url",
                            "datavalue": {"value": "https://example.com/live"},
                        }
                    }
                ]
            }
        },
    )

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.WEAK
    assert signals[0].key == "identifiers_not_identifier_weak"
    assert signals[0].properties["property"] == "P963"

import pytest

from wd_notability.detectors.sitelinks import SitelinksDetector
from wd_notability.models import NotabilityLevel
from wd_notability.project_types import ProjectType
from wd_notability.namespaces import Namespace


async def collect_signals(detector, entity):
    return [signal async for signal in detector.detect(entity)]


@pytest.mark.asyncio
async def test_sitelinks_detector_handles_missing_url(monkeypatch):
    detector = SitelinksDetector()

    async def fake_get_namespace(sitelink):
        return Namespace.MAIN

    monkeypatch.setattr(detector, "get_project_type", lambda sitelink: ProjectType.WIKIPEDIA)
    monkeypatch.setattr(detector, "get_namespace", fake_get_namespace)
    monkeypatch.setattr(detector, "get_subpage", lambda sitelink: None)
    monkeypatch.setattr(detector, "get_suffix", lambda sitelink: None)

    signals = await collect_signals(
        detector,
        {
            "sitelinks": {
                "enwiki": {
                    "site": "enwiki",
                    "title": "Example",
                }
            }
        },
    )

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.STRONG
    assert signals[0].properties["site"] == "enwiki"
    assert signals[0].properties["title"] == "Example"
    assert "url" not in signals[0].properties

import pytest

from wd_notability.content.sitelinks import SitelinksDetector
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


@pytest.mark.asyncio
async def test_sitelinks_detector_downgrades_redirect_badges(monkeypatch):
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
                    "badges": ["Q70894304"],
                }
            }
        },
    )

    assert len(signals) == 1
    assert signals[0].level == NotabilityLevel.WEAK
    assert signals[0].key == "sitelinks_redirect_badge"
    assert signals[0].properties["badges"] == ["Q70894304"]


@pytest.mark.asyncio
async def test_sitelinks_detector_excludes_wiktionary_mainspace_and_citation_pages(monkeypatch):
    detector = SitelinksDetector()

    async def fake_get_namespace(sitelink):
        title = sitelink["title"]
        if title == "Mainspace":
            return Namespace.MAIN
        return Namespace.CITATIONS

    monkeypatch.setattr(detector, "get_project_type", lambda sitelink: ProjectType.WIKTIONARY)
    monkeypatch.setattr(detector, "get_namespace", fake_get_namespace)
    monkeypatch.setattr(detector, "get_subpage", lambda sitelink: None)
    monkeypatch.setattr(detector, "get_suffix", lambda sitelink: None)

    signals = await collect_signals(
        detector,
        {
            "sitelinks": {
                "enwiktionary": {
                    "site": "enwiktionary",
                    "title": "Mainspace",
                },
                "frwiktionary": {
                    "site": "frwiktionary",
                    "title": "Citation",
                },
            }
        },
    )

    assert len(signals) == 2
    assert all(signal.level == NotabilityLevel.NONE for signal in signals)
    assert {signal.key for signal in signals} == {"sitelinks_invalid_wiktionary_namespace"}
    assert {signal.properties["namespace"] for signal in signals} == {"main", "citations"}


@pytest.mark.asyncio
async def test_sitelinks_detector_emits_none_when_classification_fails(monkeypatch):
    detector = SitelinksDetector()

    async def fake_get_namespace(sitelink):
        raise RuntimeError("namespace lookup failed")

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
    assert signals[0].level == NotabilityLevel.NONE
    assert signals[0].key == "sitelink_error"
    assert signals[0].properties["site"] == "enwiki"
    assert signals[0].properties["title"] == "Example"

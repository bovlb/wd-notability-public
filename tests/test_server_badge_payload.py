from fastapi.testclient import TestClient

from server.app import _badge_payload, _cached_payload, _render_properties_html, app
from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel


class ExampleDetector:
    name = "example"
    criterion = NotabilityCriterion.N2a


def test_badge_payload_hides_incomplete_non_strong_levels():
    result = EvaluationResult(qid="Q42")
    result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    result.add_error(ExampleDetector(), RuntimeError("failed"))

    payload = _badge_payload("Q42", result)

    assert payload["n1"] == "weak"
    assert payload["n2a"] == "unknown"
    assert payload["n"] == "unknown"


def test_badge_payload_keeps_incomplete_strong_levels():
    result = EvaluationResult(qid="Q42")
    result.set(NotabilityCriterion.N2a, NotabilityLevel.STRONG)
    result.set(NotabilityCriterion.N3_INLINKS, NotabilityLevel.STRONG)
    result.add_error(ExampleDetector(), RuntimeError("failed"))

    payload = _cached_payload("Q42", result, last_updated=123)

    assert payload["n2a"] == "strong"
    assert payload["n3"] == "strong"
    assert payload["n"] == "strong"


def test_api_evaluate_uses_full_evaluation(monkeypatch):
    calls = []

    async def fake_evaluate_full(qid, **kwargs):
        calls.append(qid)
        result = EvaluationResult(qid=qid)
        result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
        result.set(NotabilityCriterion.N3_OSM, NotabilityLevel.WEAK)
        result.source_urls.append(
            {
                "source": "entity_data",
                "api_url": "https://www.wikidata.org/wiki/Special:EntityData/Q42.json",
                "ui_url": "https://www.wikidata.org/wiki/Q42",
            }
        )
        return result

    monkeypatch.setattr("server.app.evaluate_full", fake_evaluate_full)
    client = TestClient(app)

    response = client.get("/api/items/Q42/signals")

    assert response.status_code == 200
    assert calls == ["Q42"]
    assert response.json()["levels"]["N3"] == "weak"
    assert response.json()["source_urls"][0]["source"] == "entity_data"


def test_api_evaluate_returns_complete_report(monkeypatch):
    async def fake_evaluate_full(qid, **kwargs):
        result = EvaluationResult(qid=qid)
        result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
        result.source_urls.append(
            {
                "source": "entity_data",
                "api_url": "https://www.wikidata.org/wiki/Special:EntityData/Q42.json",
                "ui_url": "https://www.wikidata.org/wiki/Q42",
            }
        )
        return result

    monkeypatch.setattr("server.app.evaluate_full", fake_evaluate_full)
    client = TestClient(app)

    response = client.get("/api/items/Q42/signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["qid"] == "Q42"
    assert payload["levels"]["N1"] == "strong"
    assert "html" in payload
    assert "Special:EntityData/Q42.json" in payload["html"]
    assert '<td>Item</td><td><a href="https://www.wikidata.org/wiki/Q42"' in payload["html"]


def test_detectors_markdown_served_at_root():
    client = TestClient(app)

    response = client.get("/detectors.md")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "<h1>Notability Detectors</h1>" in response.text
    assert "<strong>Criterion:</strong> N1, sitelinks" in response.text
    assert "@media (prefers-color-scheme: dark)" in response.text
    assert '<a href="https://www.wikidata.org/wiki/Property:P373">Commons category (P373)</a>' in response.text
    assert '<a href="https://www.wikidata.org/wiki/Q105388954">online account identifier collection (Q105388954)</a>' in response.text


def test_badge_markdown_renders_html_image():
    client = TestClient(app)

    response = client.get("/badge.md")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert '<img src="/static/favicon.svg" height="144px" width="144px" />' in response.text


def test_property_renderer_links_url_suffixes():
    html = _render_properties_html(
        {
            "url": "https://example.org/root",
            "api_url": "https://example.org/api",
            "source_url": "https://example.org/source",
            "sources": ["https://example.org/one", "https://example.org/two"],
            "qid": "Q42",
            "linked_qids": ["Q1", "not-a-qid"],
            "note": "plain text",
        }
    )

    assert "<a href='https://example.org/root'" in html
    assert "<a href='https://example.org/api'" in html
    assert "<a href='https://example.org/source'" in html
    assert "<a href='https://example.org/one'" in html
    assert "<a href='https://example.org/two'" in html
    assert "<a href='https://www.wikidata.org/wiki/Q42'" in html
    assert "<a href='https://www.wikidata.org/wiki/Q1'" in html
    assert "not-a-qid" in html
    assert "plain text" in html


def test_home_page_does_not_evaluate_without_qid(monkeypatch):
    async def fail_evaluate_or_404(qid):
        raise AssertionError("empty home page should not evaluate a default QID")

    monkeypatch.setattr("server.app._evaluate_or_404", fail_evaluate_or_404)
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    assert 'value=""' in response.text
    assert '<a href="/detectors.md">Detector help</a>' in response.text
    assert 'data-field="n" data-value="unknown"' in response.text


def test_home_page_includes_detector_help_link_and_badge(monkeypatch):
    async def fail_evaluate_or_404(qid):
        raise AssertionError("home page should not evaluate during initial render")

    monkeypatch.setattr("server.app._evaluate_or_404", fail_evaluate_or_404)
    client = TestClient(app)

    response = client.get("/?qid=Q42")

    assert response.status_code == 200
    assert '<a href="/detectors.md">Detector help</a>' in response.text
    assert "@media (prefers-color-scheme: dark)" in response.text
    assert '<a class="report-badge-link" href="/badge.md" aria-label="Open badge help">' in response.text
    assert 'class="report-badge"' in response.text
    assert 'data-field="has_claims" d="M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,28.62 A11.32,11.32 0 0,1 14.1,28.62 Z"\n        fill="#fff"' in response.text
    assert '<div class="item-link">Item: <a href="https://www.wikidata.org/wiki/Q42"' in response.text
    assert 'data-field="n" data-value="unknown"' in response.text
    assert '.report-badge [data-field="redirect"] { display: none; }' in response.text
    assert 'const evaluationQid = "Q42";' in response.text
    assert "fetch(`/api/items/${encodeURIComponent(evaluationQid)}/signals`)" in response.text
    assert "Evaluation complete." in response.text
    assert 'data-field="has_claims"' in response.text
    assert 'data-value="true"' in response.text

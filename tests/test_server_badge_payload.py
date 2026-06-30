from fastapi.testclient import TestClient
from datetime import UTC, datetime

from server.app import _badge_payload, _cache_snapshot_payload, _cached_payload, _render_properties_html, app
from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel, SignalResult


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

    payload = _cached_payload(
        "Q42",
        result,
        123,
        None,
        creator="ExampleUser",
        creation_time=int(datetime(2026, 6, 17, 2, 35, 22, tzinfo=UTC).timestamp()),
    )

    assert payload["n2a"] == "strong"
    assert payload["n3"] == "strong"
    assert payload["n"] == "strong"
    assert payload["creator"] == "ExampleUser"
    assert payload["creation_time"] == int(datetime(2026, 6, 17, 2, 35, 22, tzinfo=UTC).timestamp())


def test_cache_snapshot_payload_formats_cache_dates_as_iso():
    result = EvaluationResult(qid="Q42")
    creation_time = int(datetime(2026, 6, 17, 2, 35, 22, tzinfo=UTC).timestamp())
    last_updated = int(datetime(2026, 6, 18, 3, 4, 5, tzinfo=UTC).timestamp())
    inlinks_last_evaluated = int(datetime(2026, 6, 19, 4, 5, 6, tzinfo=UTC).timestamp())

    payload = _cache_snapshot_payload(
        result,
        123,
        456,
        creation_time=creation_time,
        last_updated=last_updated,
        inlinks_last_evaluated=inlinks_last_evaluated,
    )

    assert payload["creation_time_iso"] == "2026-06-17T02:35:22Z"
    assert payload["last_updated_iso"] == "2026-06-18T03:04:05Z"
    assert payload["inlinks_last_evaluated_iso"] == "2026-06-19T04:05:06Z"


def test_api_evaluate_refreshes_targeted_lanes(monkeypatch):
    calls = []

    async def fake_entity_contexts(qids):
        calls.append(("entitydata", list(qids)))
        return {
            qid: {
                "qid": qid,
                "entity": {"id": qid},
                "is_redirect": False,
                "has_claims": False,
                "has_sitelinks": True,
                "lastrevid": 123,
                "_timings": {},
            }
            for qid in qids
        }

    async def fake_osm_contexts(qids):
        calls.append(("osm", list(qids)))
        return {qid: {"qid": qid, "row": {"count_all": 1}, "object_explorer_url": "https://example.org/osm"} for qid in qids}

    async def fake_sdc_contexts(qids):
        calls.append(("sdc", list(qids)))
        return {qid: {"qid": qid, "search_query": "haswbstatement:P180=Q42", "usage_count": 2} for qid in qids}

    async def fake_wikisub_contexts(qids):
        calls.append(("wiki_usage", list(qids)))
        return {qid: {"qid": qid, "is_subscribed": True} for qid in qids}

    async def fake_entity_run_context(qid, context):
        result = EvaluationResult(qid=qid)
        result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
        result.signals.append(
            SignalResult(
                detector="sitelinks",
                criterion=NotabilityCriterion.N1,
                level=NotabilityLevel.STRONG,
                key="valid_sitelink",
                properties={"site": "enwiki", "title": "Example"},
            )
        )
        result.has_sitelinks = True
        result.entitydata_last_revid = 123
        result.source_contexts["entity_data"] = context
        return result

    async def fake_osm_run_context(qid, context):
        result = EvaluationResult(qid=qid)
        result.set(NotabilityCriterion.N3_OSM, NotabilityLevel.WEAK)
        result.signals.append(
            SignalResult(
                detector="osm",
                criterion=NotabilityCriterion.N3_OSM,
                level=NotabilityLevel.WEAK,
                key="osm",
                properties={"qid": qid},
            )
        )
        result.source_contexts["osm"] = context
        return result

    async def fake_sdc_run_context(qid, context):
        result = EvaluationResult(qid=qid)
        result.set(NotabilityCriterion.N3_SDC, NotabilityLevel.STRONG)
        result.signals.append(
            SignalResult(
                detector="sdc",
                criterion=NotabilityCriterion.N3_SDC,
                level=NotabilityLevel.STRONG,
                key="sdc_usage",
                properties={"qid": qid},
            )
        )
        result.source_contexts["sdc"] = context
        return result

    async def fake_wikisub_run_context(qid, context):
        result = EvaluationResult(qid=qid)
        result.set(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.STRONG)
        result.signals.append(
            SignalResult(
                detector="wiki_subscribers",
                criterion=NotabilityCriterion.N3_WIKISUB,
                level=NotabilityLevel.STRONG,
                key="wikis_subscribed_to_entity",
                properties={"qid": qid},
            )
        )
        result.source_contexts["wiki_usage"] = context
        return result

    monkeypatch.setattr("server.app.ENTITY_DATA_SOURCE.get_contexts", fake_entity_contexts)
    monkeypatch.setattr("server.app.ENTITY_DATA_SOURCE.run_context", fake_entity_run_context)
    monkeypatch.setattr("server.app.OSM_SOURCE.get_contexts", fake_osm_contexts)
    monkeypatch.setattr("server.app.OSM_SOURCE.run_context", fake_osm_run_context)
    monkeypatch.setattr("server.app.SDC_SOURCE.get_contexts", fake_sdc_contexts)
    monkeypatch.setattr("server.app.SDC_SOURCE.run_context", fake_sdc_run_context)
    monkeypatch.setattr("server.app.WIKI_USAGE_SOURCE.get_contexts", fake_wikisub_contexts)
    monkeypatch.setattr("server.app.WIKI_USAGE_SOURCE.run_context", fake_wikisub_run_context)
    client = TestClient(app)

    response = client.get("/api/items/Q42/signals")

    assert response.status_code == 200
    assert calls == [("entitydata", ["Q42"]), ("osm", ["Q42"]), ("sdc", ["Q42"]), ("wiki_usage", ["Q42"])]
    assert response.json()["levels"]["N1"] == "strong"
    assert response.json()["levels"]["N3"] == "strong"
    assert response.json()["signals_by_detected_criterion"]["N1"][0]["key"] == "valid_sitelink"
    assert response.json()["signals_by_detected_criterion"]["N3_osm"][0]["key"] == "osm"
    assert response.json()["signals_by_detected_criterion"]["N3_sdc"][0]["key"] == "sdc_usage"
    assert response.json()["signals_by_detected_criterion"]["N3_wikisub"][0]["key"] == "wikis_subscribed_to_entity"
    assert response.json()["cached_snapshot"] is None


def test_api_evaluate_returns_report_from_cache(monkeypatch):
    async def fake_empty_contexts(qids):
        return {qid: {"qid": qid} for qid in qids}

    async def fake_empty_run_context(qid, context):
        result = EvaluationResult(qid=qid)
        result.source_contexts["dummy"] = context
        return result

    monkeypatch.setattr("server.app.ENTITY_DATA_SOURCE.get_contexts", fake_empty_contexts)
    monkeypatch.setattr("server.app.ENTITY_DATA_SOURCE.run_context", fake_empty_run_context)
    monkeypatch.setattr("server.app.OSM_SOURCE.get_contexts", fake_empty_contexts)
    monkeypatch.setattr("server.app.OSM_SOURCE.run_context", fake_empty_run_context)
    monkeypatch.setattr("server.app.SDC_SOURCE.get_contexts", fake_empty_contexts)
    monkeypatch.setattr("server.app.SDC_SOURCE.run_context", fake_empty_run_context)
    monkeypatch.setattr("server.app.WIKI_USAGE_SOURCE.get_contexts", fake_empty_contexts)
    monkeypatch.setattr("server.app.WIKI_USAGE_SOURCE.run_context", fake_empty_run_context)
    client = TestClient(app)

    response = client.get("/api/items/Q42/signals")

    assert response.status_code == 200
    payload = response.json()
    assert payload["qid"] == "Q42"
    assert payload["levels"]["N1"] == "unknown"
    assert "html" in payload
    assert "<h2>Cache vs Live</h2>" in payload["html"]
    assert "<th>Cache</th>" in payload["html"]
    assert "<th>Live</th>" in payload["html"]
    assert "Cache snapshot" in payload["html"]
    assert "<h2>Queue Position</h2>" in payload["html"]
    assert "<h2>Source Context</h2>" in payload["html"]


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
    assert '<a href="/help.md">Help</a>' in response.text
    assert 'data-field="n" data-value="unknown"' in response.text


def test_home_page_includes_detector_help_link_and_badge(monkeypatch):
    async def fail_evaluate_or_404(qid):
        raise AssertionError("home page should not evaluate during initial render")

    monkeypatch.setattr("server.app._evaluate_or_404", fail_evaluate_or_404)
    client = TestClient(app)

    response = client.get("/?qid=Q42")

    assert response.status_code == 200
    assert '<a href="/help.md">Help</a>' in response.text
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

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import pytest
from fastapi.testclient import TestClient

from server.app import _badge_payload, _cache_snapshot_payload, _cached_payload, _fetch_cached_snapshot, _render_properties_html, app
from server import report_api as report_api_module
from server.home_page import _report_problem_url, ui_home
from server.report_api import _fetch_cached_snapshot_timestamps
from server.render_helpers import _badge_hovercard_html_from_report, _badge_tooltip_from_report
from wd_notability.content.debug import build_signal_debug_payload, render_signal_debug_html
from wd_notability.models import EvaluationResult, NotabilityCriterion, NotabilityLevel, SignalResult


class ExampleDetector:
    name = "example"
    criterion = NotabilityCriterion.N2a


def test_badge_payload_hides_incomplete_non_strong_levels():
    result = EvaluationResult(qid="Q42")
    result.set(NotabilityCriterion.N1, NotabilityLevel.WEAK)
    result.add_error(ExampleDetector(), RuntimeError("failed"))

    payload = _badge_payload("Q42", result)

    assert payload["levels"]["N1"] == "weak"
    assert payload["levels"]["N2a"] == "unknown"
    assert payload["levels"]["N"] == "unknown"
    assert payload["has_claims_count"] == 0
    assert payload["has_sitelinks_count"] == 0


def test_cached_snapshot_marks_direct_levels_unknown_without_content_revid():
    row = (42, None, None, 123, None, 0, 0, 0, 0, 0, 0, None, None, None)

    cached_result = report_api_module._evaluation_result_from_cache_row("Q42", row)
    payload = _cache_snapshot_payload(cached_result, None, 123)

    assert payload["levels"]["N1"] == "unknown"
    assert payload["levels"]["N2a"] == "unknown"
    assert payload["levels"]["N2b"] == "unknown"


def test_cached_snapshot_preserves_none_n3_inlinks():
    row = (42, 123456, 234567, 345678, None, 1, 2, 0, 3, 1, 0, 0, 456789, 0)

    cached_result = report_api_module._evaluation_result_from_cache_row("Q42", row)
    payload = _cache_snapshot_payload(cached_result, 123456, 234567)

    assert payload["levels"]["N3_inlinks"] == "none"


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
        creation_time=int(datetime(2026, 6, 17, 2, 35, 22, tzinfo=timezone.utc).timestamp()),
    )

    assert payload["levels"]["N2a"] == "strong"
    assert payload["levels"]["N3"] == "strong"
    assert payload["levels"]["N"] == "strong"
    assert payload["creator"] == "ExampleUser"
    assert payload["creation_time"] == int(datetime(2026, 6, 17, 2, 35, 22, tzinfo=timezone.utc).timestamp())


def test_live_evaluation_sources_include_inlinks():
    from server import app as server_app

    assert server_app.INLINKS_SOURCE in server_app.EVALUATION_SOURCES


def test_cache_snapshot_payload_formats_cache_dates_as_iso():
    result = EvaluationResult(qid="Q42")
    creation_time = int(datetime(2026, 6, 17, 2, 35, 22, tzinfo=timezone.utc).timestamp())
    last_updated = int(datetime(2026, 6, 18, 3, 4, 5, tzinfo=timezone.utc).timestamp())
    inlinks_last_evaluated = int(datetime(2026, 6, 19, 4, 5, 6, tzinfo=timezone.utc).timestamp())

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
    assert payload["creator"] is None


def test_badge_tooltip_from_report_includes_snapshot_metadata():
    tooltip = _badge_tooltip_from_report(
        {
            "levels": {
                "N": "strong",
                "N12": "strong",
                "N1": "strong",
                "N2": "strong",
                "N2a": "strong",
                "N2b": "strong",
                "N3": "none",
                "N3_inlinks": "none",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
            "cached_snapshot": {
                "creator": "ExampleUser",
                "creation_time_iso": "2026-06-17T02:35:22Z",
                "last_updated_iso": "2026-06-18T03:04:05Z",
                "inlinks_last_evaluated_iso": "2026-06-19T04:05:06Z",
            },
            "is_redirect": False,
            "is_deleted": False,
            "content_stale": True,
            "content_last_revid": 123,
            "has_sitelinks_count": 1,
            "has_claims_count": 1,
            "redirect_target": 42,
        }
    )

    assert "Created: 2026-06-17T02:35:22Z" in tooltip
    assert "Creator: ExampleUser" in tooltip
    assert "Last updated: 2026-06-18T03:04:05Z" in tooltip
    assert "Modified: 2026-06-19T04:05:06Z" in tooltip
    assert "Content stale: YES" in tooltip
    assert "Has sitelinks: YES" in tooltip
    assert "Has claims: YES" in tooltip
    assert "N12 intrinsic: STRONG" in tooltip
    assert "N3 extrinsic: NONE" in tooltip
    assert "Redirect target: Q42" not in tooltip
    assert "Deleted: YES" not in tooltip


def test_badge_hovercard_html_places_intrinsic_and_extrinsic_inline():
    hovercard = _badge_hovercard_html_from_report(
        {
            "levels": {
                "N": "strong",
                "N12": "strong",
                "N1": "strong",
                "N2": "strong",
                "N2a": "strong",
                "N2b": "strong",
                "N3": "none",
                "N3_inlinks": "none",
                "N3_osm": "none",
                "N3_wikisub": "none",
                "N3_sdc": "none",
            },
        }
    )

    assert "N12 intrinsic" in hovercard
    assert "N3 extrinsic" in hovercard
    assert "notability-badge-hovercard-label-subtitle" not in hovercard


def test_badge_hovercard_html_hides_inlinks_count_when_unknown():
    hovercard = _badge_hovercard_html_from_report(
        {
            "levels": {
                "N": "unknown",
                "N3_inlinks": "unknown",
            },
            "inlinks_count": 0,
        }
    )

    assert "Inlinks count" not in hovercard


def test_badge_hovercard_html_shows_nonzero_inlinks_count_even_when_unknown():
    hovercard = _badge_hovercard_html_from_report(
        {
            "levels": {
                "N": "unknown",
                "N3_inlinks": "unknown",
            },
            "inlinks_count": 7,
        }
    )

    assert "Inlinks count" in hovercard
    assert ">7<" in hovercard
    assert hovercard.index("N3_inlinks") < hovercard.index("Inlinks count") < hovercard.index("N3_osm")


def test_badge_hovercard_html_splits_partial_text_colors():
    hovercard = _badge_hovercard_html_from_report(
        {
            "levels": {
                "N": "partial-weak",
                "N12": "partial-strong",
            },
        }
    )

    assert "<span class=\"level-partial-prefix\">PARTIAL-</span><span class=\"level-weak\">WEAK</span>" in hovercard
    assert "<span class=\"level-partial-prefix\">PARTIAL-</span><span class=\"level-strong\">STRONG</span>" in hovercard


def test_render_signal_debug_html_splits_partial_summary_text():
    html = render_signal_debug_html(
        {
            "qid": "Q42",
            "levels": {"N1": "partial-weak"},
            "errors": {},
            "has_claims_count": 0,
            "has_sitelinks_count": 0,
            "inlinks_count": 0,
            "is_redirect": False,
            "is_deleted": False,
            "signals_by_detected_criterion": {
                "N1": [{"criterion": "N1", "level": "partial-weak"}],
            },
        }
    )

    assert "<td><span class='level-partial-weak'><span class=\"level-partial-prefix\">PARTIAL-</span><span class=\"level-weak\">WEAK</span></span></td>" in html


def test_render_signal_debug_html_splits_partial_strong_signal_rows():
    html = render_signal_debug_html(
        {
            "qid": "Q42",
            "levels": {"N3_inlinks": "partial-strong"},
            "errors": {},
            "has_claims_count": 0,
            "has_sitelinks_count": 0,
            "inlinks_count": 0,
            "is_redirect": False,
            "is_deleted": False,
            "signals_by_detected_criterion": {
                "N3_inlinks": [
                    {
                        "criterion": "N3_inlinks",
                        "level": "partial-strong",
                        "detector": "inlinks",
                        "key": "inlinks",
                        "properties": {
                            "qid": "Q2",
                            "n12": "partial-strong",
                        },
                    }
                ],
            },
        }
    )

    assert "<td class='level-partial-strong'><span class=\"level-partial-prefix\">PARTIAL-</span><span class=\"level-strong\">STRONG</span></td>" in html


def test_render_signal_debug_html_shows_inlinks_unknown_as_a_regular_signal():
    html = render_signal_debug_html(
        {
            "qid": "Q42",
            "report_variant": "inlinks",
            "levels": {"N3_inlinks": "unknown"},
            "errors": {},
            "has_claims_count": 0,
            "has_sitelinks_count": 0,
            "inlinks_count": 0,
            "is_redirect": False,
            "is_deleted": False,
            "signals_by_detected_criterion": {
                "N3_inlinks": [
                    {
                        "criterion": "N3_inlinks",
                        "level": "unknown",
                        "detector": "inlinks",
                        "key": "inlinks_unknown",
                        "properties": {
                            "best_level": "none",
                            "unknown_inlinks": ["Q1", "Q2"],
                        },
                    }
                ],
            },
        }
    )

    assert "<h2>Detectors</h2>" in html
    assert "<summary>N3_inlinks from linked item N12 - 1 signal <span class='level-unknown'>UNKNOWN</span></summary>" in html
    assert "inlinks_unknown" in html
    assert "unknown_inlinks" in html


@pytest.mark.asyncio
async def test_fetch_cached_snapshot_uses_lookup_usage_without_main_row(monkeypatch):
    from server import app as server_app

    monkeypatch.setattr(
        server_app.lookup_cache,
        "get_osm_usage_for",
        lambda qids: {qid: {"count_all": 1} for qid in qids if qid == "Q42"},
    )
    monkeypatch.setattr(server_app.lookup_cache, "get_sdc_usage_for", lambda qids: {})
    monkeypatch.setattr(server_app.lookup_cache, "get_wiki_subscribers_for", lambda qids: set())

    class FakeCursor:
        async def fetchone(self):
            return None

    class FakeDB:
        async def execute(self, *_args, **_kwargs):
            return FakeCursor()

    @asynccontextmanager
    async def fake_connect():
        yield FakeDB()

    async def fake_initialize():
        return None

    monkeypatch.setattr(server_app.CACHE, "initialize", fake_initialize)
    monkeypatch.setattr(server_app.CACHE, "_parse_qid", lambda qid: 42)
    monkeypatch.setattr(server_app.CACHE, "_connect", fake_connect)

    snapshot = await _fetch_cached_snapshot("Q42")

    assert snapshot is not None
    assert snapshot["levels"]["N3_osm"] == "weak"
    assert snapshot["levels"]["N1"] == "unknown"
    assert snapshot["content_last_revid"] is None


@pytest.mark.asyncio
async def test_fetch_cached_snapshot_includes_creator_metadata(monkeypatch):
    from server import app as server_app

    class FakeCursor:
        async def fetchone(self):
            return (42, 123456, 111222, 333444, None, 1, 0, 0, 2, 0, 0, 3, 444555, 1)

    class FakeDB:
        async def execute(self, *_args, **_kwargs):
            return FakeCursor()

    @asynccontextmanager
    async def fake_connect():
        yield FakeDB()

    async def fake_initialize():
        return None

    async def fake_fetch_timestamps(qids):
        assert qids == ["Q42"]
        return {
            "Q42": {
                "creator": "ExampleUser",
                "creation_time": 223344,
                "last_updated": 123456,
                "inlinks_last_evaluated": 444555,
            }
        }

    monkeypatch.setattr(server_app.CACHE, "initialize", fake_initialize)
    monkeypatch.setattr(server_app.CACHE, "_parse_qid", lambda qid: 42)
    monkeypatch.setattr(server_app.CACHE, "_connect", fake_connect)
    monkeypatch.setattr(report_api_module, "_fetch_cached_snapshot_timestamps", fake_fetch_timestamps)

    snapshot = await _fetch_cached_snapshot("Q42")

    assert snapshot is not None
    assert snapshot["creator"] == "ExampleUser"
    assert snapshot["creation_time"] == 223344
    assert snapshot["creation_time_iso"] == "1970-01-03T14:02:24Z"


@pytest.mark.asyncio
async def test_build_inlinks_scan_report_uses_n12_only(monkeypatch):
    from server import app as server_app

    class FakeContextSource:
        async def get_contexts(self, qids):
            assert qids == ["Q1"]
            return {"Q1": {"inlinks": ["Q2", "Q3"], "truncated": False}}

    async def fake_get_n12_many(qids):
        assert qids == ["Q2", "Q3"]
        return {"Q2": NotabilityLevel.STRONG, "Q3": NotabilityLevel.UNKNOWN}

    async def fail_get_many(*_args, **_kwargs):
        raise AssertionError("should not fetch full cache rows")

    async def fail_get_staleness(*_args, **_kwargs):
        raise AssertionError("should not fetch staleness")

    monkeypatch.setattr(server_app, "INLINKS_SOURCE", FakeContextSource())
    monkeypatch.setattr(report_api_module, "_get_inlinks_n12_many", fake_get_n12_many)
    monkeypatch.setattr(server_app.CACHE, "get_many", fail_get_many)
    monkeypatch.setattr(server_app.CACHE, "get_content_staleness_for_qids", fail_get_staleness)

    report = await report_api_module._build_inlinks_scan_report("Q1")

    assert report is not None
    assert report["visible_inlinks"] == ["Q2", "Q3"]
    assert [item["cached_snapshot"]["levels"]["N12"] for item in report["reports"]] == ["strong", "unknown"]
    assert report["reports"][0]["signals_by_detected_criterion"]["N3_inlinks"][0]["properties"]["n12"] == "strong"


@pytest.mark.asyncio
async def test_evaluate_live_reports_times_out_slow_inlinks_source(monkeypatch):
    from server import app as server_app

    class FastSource:
        name = "fast"
        detectors = (ExampleDetector(),)

        async def get_contexts(self, qids):
            return {qid: {"qid": qid} for qid in qids}

        async def run_context(self, qid, context):
            result = EvaluationResult(qid=qid)
            result.set(NotabilityCriterion.N1, NotabilityLevel.STRONG)
            result.source_contexts[self.name] = context
            return result

    class SlowInlinksSource:
        name = "inlinks"
        detectors = (type("SlowDetector", (), {"name": "slow", "criterion": NotabilityCriterion.N3_INLINKS})(),)

        async def get_contexts(self, qids):
            await report_api_module.asyncio.sleep(999)

        async def run_context(self, qid, context):
            raise AssertionError("should not be reached")

    monkeypatch.setattr(report_api_module, "SOURCE_CONTEXT_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(server_app, "EVALUATION_SOURCES", (FastSource(), SlowInlinksSource()))

    reports = await report_api_module._evaluate_live_reports(["Q42"])

    assert reports["Q42"].levels_str["N1"] == "strong"
    assert reports["Q42"].levels_str["N3_inlinks"] == "unknown"
    assert any("timed out after" in message for message in reports["Q42"].errors["N3_inlinks"])


@pytest.mark.asyncio
async def test_evaluate_live_reports_marks_truncated_inlinks_count_as_lower_bound(monkeypatch):
    result = EvaluationResult(qid="Q30")
    result.inlinks_count = 1
    result.source_contexts["inlinks"] = {"inlinks": ["Q1"], "truncated": True}

    payload = build_signal_debug_payload(result)

    assert payload["inlinks_count_display"] == ">1000"


@pytest.mark.asyncio
async def test_build_inlinks_scan_report_returns_error_on_timeout(monkeypatch):
    from server import app as server_app

    class SlowSource:
        name = "inlinks"
        detectors = ()

        async def get_contexts(self, qids):
            await report_api_module.asyncio.sleep(999)

    monkeypatch.setattr(report_api_module, "SOURCE_CONTEXT_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(server_app, "INLINKS_SOURCE", SlowSource())

    report = await report_api_module._build_inlinks_scan_report("Q42")

    assert report is not None
    assert report["visible_inlinks"] == []
    assert report["reports"] == []
    assert "timed out after" in report["error"]


@pytest.mark.asyncio
async def test_fetch_cached_snapshot_timestamps_returns_last_updated(monkeypatch):
    from server import app as server_app

    class FakeCursor:
        async def fetchall(self):
            return [(42, 123456, 223344, 1, 334455)]

    class FakeDB:
        async def execute(self, query, params):
            assert "ce.last_updated" in query
            assert params == [42]
            return FakeCursor()

    async def fake_lookup_creator_names(actor_ids):
        assert actor_ids == [1]
        return {1: "ExampleUser"}

    @asynccontextmanager
    async def fake_connect():
        yield FakeDB()

    async def fake_initialize():
        return None

    monkeypatch.setattr(server_app.CACHE, "initialize", fake_initialize)
    monkeypatch.setattr(server_app.CACHE, "_parse_qid", lambda qid: 42)
    monkeypatch.setattr(server_app.CACHE, "_connect", fake_connect)
    monkeypatch.setattr(report_api_module, "web_lookup_creator_names", fake_lookup_creator_names)

    rows = await _fetch_cached_snapshot_timestamps(["Q42"])

    assert rows["Q42"]["last_updated"] == 123456
    assert rows["Q42"]["creation_time"] == 223344
    assert rows["Q42"]["inlinks_last_evaluated"] == 334455
    assert rows["Q42"]["creator"] == "ExampleUser"


def test_api_evaluate_refreshes_targeted_lanes(monkeypatch):
    from server import app as server_app

    calls = []

    async def fake_entity_contexts(qids):
        calls.append(("content", list(qids)))
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
        result.content_last_revid = 123
        result.source_contexts["content"] = context
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
        result.set(NotabilityCriterion.N3_WIKISUB, NotabilityLevel.WEAK)
        result.signals.append(
            SignalResult(
                detector="wiki_subscribers",
                criterion=NotabilityCriterion.N3_WIKISUB,
                level=NotabilityLevel.WEAK,
                key="wikis_subscribed_to_entity",
                properties={"qid": qid},
            )
        )
        result.source_contexts["wiki_usage"] = context
        return result

    class FakeContentSource:
        name = "content"

        async def get_contexts(self, qids):
            return await fake_entity_contexts(qids)

        async def run_context(self, qid, context):
            return await fake_entity_run_context(qid, context)

    class FakeOsmSource:
        name = "osm"

        async def get_contexts(self, qids):
            return await fake_osm_contexts(qids)

        async def run_context(self, qid, context):
            return await fake_osm_run_context(qid, context)

    class FakeSdcSource:
        name = "sdc"

        async def get_contexts(self, qids):
            return await fake_sdc_contexts(qids)

        async def run_context(self, qid, context):
            return await fake_sdc_run_context(qid, context)

    class FakeWikiUsageSource:
        name = "wiki_usage"

        async def get_contexts(self, qids):
            return await fake_wikisub_contexts(qids)

        async def run_context(self, qid, context):
            return await fake_wikisub_run_context(qid, context)

    monkeypatch.setattr(
        server_app,
        "EVALUATION_SOURCES",
        (FakeContentSource(), FakeOsmSource(), FakeSdcSource(), FakeWikiUsageSource()),
    )
    client = TestClient(app)

    response = client.get("/api/items/Q42/signals")

    assert response.status_code == 200
    assert calls[:4] == [("content", ["Q42"]), ("osm", ["Q42"]), ("sdc", ["Q42"]), ("wiki_usage", ["Q42"])]
    assert response.json()["levels"]["N1"] == "strong"
    assert response.json()["levels"]["N3"] == "strong"
    assert response.json()["badge_tooltip"].startswith("Overall: ")
    assert "badge_hovercard" in response.json()
    assert response.json()["signals_by_detected_criterion"]["N1"][0]["key"] == "valid_sitelink"
    assert response.json()["signals_by_detected_criterion"]["N3_osm"][0]["key"] == "osm"
    assert response.json()["signals_by_detected_criterion"]["N3_sdc"][0]["key"] == "sdc_usage"
    assert response.json()["signals_by_detected_criterion"]["N3_wikisub"][0]["key"] == "wikis_subscribed_to_entity"


def test_private_network_preflight_gets_allow_header(monkeypatch):
    from server import app as server_app

    monkeypatch.setattr(server_app.lookup_cache, "assert_ready", lambda *args, **kwargs: None)

    with TestClient(app) as client:
        response = client.options(
            "/subscribe",
            headers={
                "Origin": "https://www.wikidata.org",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Private-Network": "true",
            },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://www.wikidata.org"
    assert response.headers["access-control-allow-private-network"] == "true"


def test_api_evaluate_returns_report_from_cache(monkeypatch):
    from server import app as server_app

    async def fake_empty_contexts(qids):
        return {qid: {"qid": qid} for qid in qids}

    async def fake_empty_run_context(qid, context):
        result = EvaluationResult(qid=qid)
        result.source_contexts["dummy"] = context
        return result

    monkeypatch.setattr(type(server_app.CONTENT_SOURCE), "get_contexts", fake_empty_contexts, raising=False)
    monkeypatch.setattr(type(server_app.CONTENT_SOURCE), "run_context", fake_empty_run_context, raising=False)
    monkeypatch.setattr(type(server_app.OSM_SOURCE), "get_contexts", fake_empty_contexts, raising=False)
    monkeypatch.setattr(type(server_app.OSM_SOURCE), "run_context", fake_empty_run_context, raising=False)
    monkeypatch.setattr(type(server_app.SDC_SOURCE), "get_contexts", fake_empty_contexts, raising=False)
    monkeypatch.setattr(type(server_app.SDC_SOURCE), "run_context", fake_empty_run_context, raising=False)
    monkeypatch.setattr(type(server_app.WIKI_USAGE_SOURCE), "get_contexts", fake_empty_contexts, raising=False)
    monkeypatch.setattr(type(server_app.WIKI_USAGE_SOURCE), "run_context", fake_empty_run_context, raising=False)
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
    assert "<h2>Detectors</h2>" in payload["html"]
    assert "Interest" not in payload["html"]
    assert "Raw context" not in payload["html"]


def test_render_signal_debug_html_shows_metadata_and_collapsible_detector_sections():
    report = {
        "qid": "Q42",
        "levels": {"N": "strong"},
        "errors": {},
        "cached_snapshot": {
            "qid": "Q42",
            "content_last_revid": 123,
            "recent_changes_last_revid": 456,
            "has_claims_count": 2,
            "has_sitelinks_count": 4,
            "inlinks_count": 6,
        },
        "signals_by_detected_criterion": {
            "N1": [
                {
                    "criterion": "N1",
                    "level": "strong",
                    "detector": "example",
                    "key": "sitelinks",
                    "properties": {
                        "site": "enwiki",
                        "property": "P123",
                        "item": "Q42",
                        "url": "https://example.org/path",
                    },
                }
            ],
            "N2a": [
                {
                    "criterion": "N2a",
                    "level": "weak",
                    "detector": "example",
                    "key": "identifiers",
                    "properties": {"property": "P123"},
                }
            ],
            "N2b": [
                {
                    "criterion": "N2b",
                    "level": "strong",
                    "detector": "example",
                    "key": "sources",
                    "properties": {"references": 3},
                }
            ],
            "N3_inlinks": [
                {
                    "criterion": "N3_inlinks",
                    "level": "strong",
                    "detector": "example",
                    "key": "inlinks",
                    "properties": {"count": 1},
                }
            ],
            "N3_osm": [
                {
                    "criterion": "N3_osm",
                    "level": "unknown",
                    "detector": "example",
                    "key": "osm",
                    "properties": {"count": 0},
                }
            ],
            "N3_wikisub": [
                {
                    "criterion": "N3_wikisub",
                    "level": "weak",
                    "detector": "example",
                    "key": "wikisub",
                    "properties": {"count": 2},
                }
            ],
            "N3_sdc": [
                {
                    "criterion": "N3_sdc",
                    "level": "strong",
                    "detector": "example",
                    "key": "sdc",
                    "properties": {"count": 5},
                }
            ],
        },
        "signals": [
            {
                "criterion": "N1",
                "level": "strong",
                "detector": "example",
                "key": "sitelinks",
                "properties": {
                    "site": "enwiki",
                    "property": "P123",
                    "item": "Q42",
                    "url": "https://example.org/path",
                },
            }
        ],
        "source_contexts": {},
        "source_urls": [],
    }

    html = render_signal_debug_html(report)

    assert "<h2>Cache vs Live</h2>" in html
    assert "<h2>Detectors</h2>" in html
    assert html.count("<details class='criterion-section'>") == 7
    assert "<summary>N1 - 1 signal <span class='level-strong'>STRONG</span></summary>" in html
    assert "<summary>N2a - 1 signal <span class='level-weak'>WEAK</span></summary>" in html
    assert "<summary>N2b - 1 signal <span class='level-strong'>STRONG</span></summary>" in html
    assert "<summary>N3_inlinks from linked item N12 - 1 signal <span class='level-strong'>STRONG</span></summary>" in html
    assert "<summary>N3_osm - 1 signal <span class='level-unknown'>UNKNOWN</span></summary>" in html
    assert "<summary>N3_wikisub - 1 signal <span class='level-weak'>WEAK</span></summary>" in html
    assert "<summary>N3_sdc - 1 signal <span class='level-strong'>STRONG</span></summary>" in html
    assert "<a href='https://www.wikidata.org/wiki/Property:P123'" in html
    assert "<a href='https://www.wikidata.org/wiki/Q42'" in html
    assert "<a href='https://example.org/path'" in html
    assert "Interest" not in html
    assert "Raw context" not in html


def test_render_signal_debug_html_hides_unknown_count_zeros_in_comparison_table():
    report = {
        "qid": "Q42",
        "levels": {
            "N": "unknown",
            "N1": "unknown",
            "N2a": "unknown",
            "N2b": "unknown",
            "N3": "unknown",
            "N3_inlinks": "unknown",
            "N3_osm": "unknown",
            "N3_wikisub": "unknown",
            "N3_sdc": "unknown",
        },
        "errors": {},
        "has_claims_count": 0,
        "has_sitelinks_count": 0,
        "inlinks_count": 0,
        "is_redirect": False,
        "is_deleted": False,
        "cached_snapshot": {
            "qid": "Q42",
            "has_claims_count": 0,
            "has_sitelinks_count": 0,
            "inlinks_count": 0,
            "content_last_revid": None,
            "inlinks_last_evaluated": None,
        },
        "signals_by_detected_criterion": {},
        "signals": [],
        "source_contexts": {},
        "source_urls": [],
    }

    html = render_signal_debug_html(report)

    assert "<td>Claims count</td><td>&mdash;</td><td>&mdash;</td>" in html
    assert "<td>Sitelinks count</td><td>&mdash;</td><td>&mdash;</td>" in html
    assert "<td>Inlinks count</td><td>&mdash;</td><td>&mdash;</td>" in html


def test_render_signal_debug_html_falls_back_to_cached_inlinks_count_when_truncated():
    report = {
        "qid": "Q30",
        "levels": {
            "N": "strong",
            "N1": "strong",
            "N2a": "strong",
            "N2b": "strong",
            "N2": "strong",
            "N12": "strong",
            "N3": "strong",
            "N3_inlinks": "strong",
            "N3_osm": "none",
            "N3_wikisub": "none",
            "N3_sdc": "none",
        },
        "errors": {},
        "has_claims_count": 0,
        "has_sitelinks_count": 0,
        "inlinks_count": 1000,
        "inlinks_count_display": ">1000",
        "is_redirect": False,
        "is_deleted": False,
        "cached_snapshot": {
            "qid": "Q30",
            "has_claims_count": 0,
            "has_sitelinks_count": 0,
            "inlinks_count": 1,
            "content_last_revid": 123,
            "inlinks_last_evaluated": 456,
        },
        "inlinks_scan": {
            "visible_inlinks": ["Q1"],
            "truncated": True,
            "reports": [],
        },
        "signals_by_detected_criterion": {},
        "signals": [],
        "source_contexts": {},
        "source_urls": [],
    }

    html = render_signal_debug_html(report)

    assert "<td>Inlinks count</td><td>1</td><td>&gt;1000</td>" in html


def test_build_signal_debug_payload_keeps_live_count_fields():
    result = EvaluationResult(qid="Q42")
    result.has_claims_count = 11
    result.has_sitelinks_count = 2
    result.content_last_revid = 123

    report = build_signal_debug_payload(result)
    html = render_signal_debug_html(report)

    assert report["content_last_revid"] == 123
    assert report["recent_changes_last_revid"] is None
    assert "<td>Claims count</td><td>&mdash;</td><td>11</td>" in html
    assert "<td>Sitelinks count</td><td>&mdash;</td><td>2</td>" in html


def test_render_signal_debug_html_shows_creation_interest():
    report = {
        "qid": "Q42",
        "levels": {"N": "unknown"},
        "errors": {},
        "cached_snapshot": {
            "qid": "Q42",
            "creator": "ExampleUser",
            "creation_time_iso": "2026-06-17T02:35:22Z",
        },
        "signals_by_detected_criterion": {},
        "signals": [],
        "source_contexts": {},
        "source_urls": [],
    }

    html = render_signal_debug_html(report)

    assert "<h2>Cache vs Live</h2>" in html
    assert "<h2>Detectors</h2>" in html


def test_render_signal_debug_html_shows_inlinks_live_error_when_no_signals():
    report = {
        "qid": "Q30",
        "report_variant": "inlinks",
        "levels": {"N3_inlinks": "unknown"},
        "errors": {},
        "has_claims_count": 0,
        "has_sitelinks_count": 0,
        "inlinks_count": 0,
        "is_redirect": False,
        "is_deleted": False,
        "error": "Source inlinks timed out after 15s",
        "signals_by_detected_criterion": {},
        "signals": [],
        "source_contexts": {},
        "source_urls": [],
    }

    html = render_signal_debug_html(report)

    assert "Live source error:" in html
    assert "Source inlinks timed out after 15s" in html
    assert "<h2>Detectors</h2>" not in html


def test_render_signal_debug_html_shows_inlinks_errors_when_no_signals():
    report = {
        "qid": "Q30",
        "report_variant": "inlinks",
        "levels": {"N3_inlinks": "unknown"},
        "errors": {"N3_inlinks": ["inlinks: timed out after 15s"]},
        "has_claims_count": 0,
        "has_sitelinks_count": 0,
        "inlinks_count": 0,
        "is_redirect": False,
        "is_deleted": False,
        "signals_by_detected_criterion": {},
        "signals": [],
        "source_contexts": {},
        "source_urls": [],
    }

    html = render_signal_debug_html(report)

    assert "<h2>Errors</h2>" in html
    assert "N3: Inlinks" in html
    assert "inlinks: timed out after 15s" in html
    assert "<h2>Detectors</h2>" not in html


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
    assert "Labeled notability badge segments" in response.text
    assert ">N strong<" in response.text
    assert ">N1 sitelinks<" in response.text
    assert ">N2a identifiers<" in response.text
    assert ">N2b sources<" in response.text
    assert ">N3 structural need<" in response.text
    assert 'href="api/badge-examples"' in response.text
    assert '<a href="api/badge-examples/n3-partial-weak.svg">' in response.text
    assert '<img src="api/badge-examples/n3-partial-weak.svg"' in response.text
    assert '<a href="api/badge-examples/n3-partial-strong.svg">' in response.text
    assert '<img src="api/badge-examples/n3-partial-strong.svg"' in response.text
    assert '<a href="api/badge-examples/partial-weak.svg">' in response.text
    assert '<img src="api/badge-examples/partial-weak.svg"' in response.text
    assert '<a href="api/badge-examples/deleted.svg">' in response.text
    assert '<img src="api/badge-examples/deleted.svg"' in response.text


def test_badge_examples_api_returns_shared_svg_examples():
    client = TestClient(app)

    response = client.get("/api/badge-examples")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 8
    assert any(item["id"] == "partial-weak" for item in payload["items"])
    assert any(item["id"] == "n3-partial-weak" for item in payload["items"])
    assert any(item["svg_url"] == "/api/badge-examples/deleted.svg" for item in payload["items"])

    svg_response = client.get("/api/badge-examples/n3-partial-weak.svg")
    assert svg_response.status_code == 200
    assert svg_response.headers["content-type"].startswith("image/svg+xml")
    assert 'data-value="partial-weak"' in svg_response.text
    assert 'class="report-badge"' in svg_response.text
    assert 'data-field="n3_inlinks"' not in svg_response.text
    assert 'data-field="n3_osm"' not in svg_response.text
    assert 'data-field="n3_wikisub"' not in svg_response.text
    assert 'data-field="n3_sdc"' not in svg_response.text

    ET.fromstring(svg_response.text)
    assert "notability-badge-hovercard" not in svg_response.text

    redirect_response = client.get("/api/badge-examples/redirect.svg")
    assert redirect_response.status_code == 200
    assert 'data-field="redirect"' in redirect_response.text
    assert 'fill="#6a1b9a"' in redirect_response.text
    assert 'marker-end=' not in redirect_response.text
    assert 'M1.5 15.0 H15.2 V10.5 L23.22 18.0 L15.2 25.5 V21.0 H1.5 Z' in redirect_response.text


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


def test_report_problem_url_prefills_qid_title():
    url = _report_problem_url("Q42")

    assert url.startswith("https://www.wikidata.org/w/index.php?")
    assert "title=User_talk%3ABovlb%2Fwd-notability" in url
    assert "action=edit" in url
    assert "section=new" in url
    assert "dtpreload=1" in url
    assert "preloadtitle=Problem+with+Q42" in url
    assert "preload=User%3ABovlb%2Fwd-notability%2Freport-preload" in url


@pytest.mark.asyncio
async def test_home_page_does_not_evaluate_without_qid(monkeypatch):
    async def fail_evaluate_or_404(qid):
        raise AssertionError("empty home page should not evaluate a default QID")

    monkeypatch.setattr("server.app._evaluate_or_404", fail_evaluate_or_404)

    response = await ui_home(qid="")

    assert response.status_code == 200
    assert 'value=""' in response.body.decode()
    assert '<a href="/help.md">Help</a>' in response.body.decode()
    assert '<a href="/api/items/' not in response.body.decode()
    assert '<a href="/api/item-trace?qid=' not in response.body.decode()
    assert 'data-field="n" data-value="unknown"' in response.body.decode()


@pytest.mark.asyncio
async def test_home_page_includes_detector_help_link_and_badge(monkeypatch):
    async def fail_evaluate_or_404(qid):
        raise AssertionError("home page should not evaluate during initial render")

    monkeypatch.setattr("server.app.ITEM_TRACE_ENABLED", False)
    monkeypatch.setattr("server.app._evaluate_or_404", fail_evaluate_or_404)

    response = await ui_home(qid="Q42")

    assert response.status_code == 200
    body = response.body.decode()
    assert '<a href="/help.md">Help</a>' in body
    assert '<a href="/api/items/Q42/signals">API</a>' in body
    assert '<a href="/item-trace?qid=Q42">Trace</a>' not in body
    assert 'id="report-problem-link"' in body
    assert 'Report a problem' in body
    assert 'id="copy-report-details-button"' in body
    assert 'Copy details to clipboard' in body
    assert 'params.set("dtpreload", "1");' in body
    assert 'params.set("preload", "User:Bovlb/wd-notability/report-preload");' in body
    assert 'params.append("preloadparams[]", value);' in body
    assert 'function buildReportProblemSummary(report)' in body
    assert 'function buildReportProblemText(report)' in body
    assert 'N12 ${String(levels.N12 ?? "unknown").toUpperCase()}' in body
    assert 'N2a ${String(levels.N2a ?? "unknown").toUpperCase()}' in body
    assert 'N3_sdc ${String(levels.N3_sdc ?? "unknown").toUpperCase()}' in body
    assert '* Item: {{Q|${qid || "UNKNOWN"}}}' in body
    assert '* URL: ${reportUrl}' in body
    assert '* Date: ${reportDate}' in body
    assert '* Summary: ${summary || "UNKNOWN"}' in body
    assert "@media (prefers-color-scheme: dark)" in body
    assert body.count('class="report-badge-link" href="/badge.md" aria-label="Open badge help"') == 2
    assert 'data-badge-role="cache"' in body
    assert 'data-badge-role="live"' in body
    assert "<h2>Cache</h2>" in body
    assert "<h2>Live</h2>" in body
    assert 'class="report-badge"' in body
    assert 'class="notability-badge-hovercard"' in body
    assert 'notability-badge-hovercard-label-subtitle' in body
    assert '<div class="item-link">Item: <a href="https://www.wikidata.org/wiki/Q42"' in body
    assert 'data-field="n" data-value="unknown"' in body
    assert 'const evaluationQid = "Q42";' in body
    assert "fetch(`/api/items/${encodeURIComponent(evaluationQid)}/signals`)" in body
    assert "Evaluation complete." in body
    assert 'data-field="has_claims"' in body
    assert 'data-value="true"' in body


def test_notability_styles_use_green_for_partial_strong_ring():
    styles = Path("/home/gavin/github/wd-notability/server/static/notability.js").read_text()

    assert "--level-partial-label: #b04d1a;" in styles
    assert "--level-partial-weak-second: #c05d00;" in styles
    assert "--level-partial-strong-second: #1b7f2a;" in styles
    assert "--level-partial-label: #ff8c68;" in styles
    assert "--level-partial-weak-second: #ffb566;" in styles
    assert "--level-partial-strong-second: #8bd98f;" in styles
    assert '[data-field="n"][data-value="strong"] { fill: none; }' in styles


def test_badge_example_svg_keeps_partial_outer_ring_stroked():
    client = TestClient(app)

    response = client.get("/api/badge-examples/n3-partial-weak.svg")

    assert response.status_code == 200
    assert response.text.count('stroke-linecap="butt"') >= 4

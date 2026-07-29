from __future__ import annotations

from pathlib import Path

from fastapi import HTTPException
from fastapi.testclient import TestClient
import pytest

import server.app as app_module
from server.routes_api import item_trace_page
from wd_notability.content import worker as content_worker
from wd_notability.inlinks import worker as inlinks_worker
from wd_notability.metadata import worker as recent_changes_worker
from wd_notability import cache_observability as cache_observability_worker
from wd_notability.evaluation_cache import EvaluationCache


@pytest.mark.asyncio
async def test_observability_store_derives_smoothed_throughput(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    try:
        await cache.initialize()

        await cache.observability.record_worker_snapshots(
            [
                ("content/1", {"queue": {"total": 3, "pubsub": 2}, "throughput": {"total_processed": 10}}, 100),
                ("content/2", {"queue": {"total": 4, "in_flight": 1}, "throughput": {"total_processed": 20}, "note": "idle"}, 100),
                ("content/1", {"queue": {"total": 5}, "throughput": {"total_processed": 35}}, 160),
                ("content/2", {"queue": {"total": 7, "in_flight": 2}, "throughput": {"total_processed": 45}}, 160),
                ("content/1", {"queue": {"total": 8}, "throughput": {"total_processed": 75}}, 220),
                ("content/2", {"queue": {"total": 10, "in_flight": 3}, "throughput": {"total_processed": 80}}, 220),
                ("content/1", {"queue": {"total": 13}, "throughput": {"total_processed": 130}}, 280),
                ("content/2", {"queue": {"total": 15, "in_flight": 4}, "throughput": {"total_processed": 120}}, 280),
                ("inlinks/1", {"queue": {"total": 1}}, 120),
            ]
        )

        series, workers = await cache.observability.snapshot_views(since=0)

        assert series["queue.total"] == [(100, 7.0), (120, 8.0), (160, 13.0), (220, 19.0), (280, 29.0)]
        assert series["queue.in_flight"] == [(100, 1.0), (120, 1.0), (160, 2.0), (220, 3.0), (280, 4.0)]
        assert series["throughput.total_processed"] == [(100, 30.0), (160, 80.0), (220, 155.0), (280, 250.0)]
        assert series["throughput.rate_per_second"] == [
            (100, 0.0),
            (160, pytest.approx(0.8333333333)),
            (220, pytest.approx(1.0416666667)),
            (280, pytest.approx(1.2222222222)),
        ]
        assert workers["content"]["throughput.rate_per_second"] == [
            (100, 0.0),
            (160, pytest.approx(0.8333333333)),
            (220, pytest.approx(1.0416666667)),
            (280, pytest.approx(1.2222222222)),
        ]
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_observability_store_derives_cache_growth_rate(tmp_path):
    cache = EvaluationCache(db_path=tmp_path)
    try:
        await cache.initialize()

        await cache.observability.record_worker_snapshots(
            [
                ("cache", {"items": {"total": 10}}, 100),
                ("cache", {"items": {"total": 20}}, 110),
                ("cache", {"items": {"total": 40}}, 120),
                ("cache", {"items": {"total": 70}}, 130),
            ]
        )

        series, workers = await cache.observability.snapshot_views(since=0)

        assert series["items.total"] == [(100, 10.0), (110, 20.0), (120, 40.0), (130, 70.0)]
        assert series["items.rate_per_second"] == [
            (100, 0.0),
            (110, pytest.approx(1.0)),
            (120, pytest.approx(1.5)),
            (130, pytest.approx(2.0)),
        ]
        assert workers["cache"]["items.rate_per_second"] == [
            (100, 0.0),
            (110, pytest.approx(1.0)),
            (120, pytest.approx(1.5)),
            (130, pytest.approx(2.0)),
        ]
    finally:
        await cache.close()


@pytest.mark.asyncio
async def test_api_observability_uses_period_and_worker_filters(monkeypatch):
    calls: list[tuple[int, int, tuple[str, ...] | None]] = []

    class FakeObservability:
        async def snapshot_views(self, *, since, until, worker_names, limit=None):
            calls.append((since, until, None if worker_names is None else tuple(worker_names)))
            return (
                {
                    "queue.total": [(1, 2)],
                    "throughput.rate_per_second": [(1, 3.5)],
                    "items.rate_per_second": [(1, 0.75)],
                },
                {
                    "content": {
                        "queue.total": [(1, 2)],
                        "throughput.rate_per_second": [(1, 3.5)],
                        "items.rate_per_second": [(1, 0.75)],
                    },
                    "inlinks": {"queue.total": [(3, 4)], "throughput.rate_per_second": [(3, 1.25)]},
                },
            )

    class FakeCache:
        observability = FakeObservability()

    monkeypatch.setattr(app_module, "CACHE", FakeCache())

    payload = await app_module.api_observability(period="2h", workers=[" content ", ""])

    assert payload["period_seconds"] == 7200
    assert payload["period_label"] == "2 hour(s)"
    assert payload["fields"]["queue.total"] == [(1, 2)]
    assert payload["fields"]["throughput.rate_per_second"] == [(1, 3.5)]
    assert payload["fields"]["items.rate_per_second"] == [(1, 0.75)]
    assert payload["workers"]["content"]["queue.total"] == [(1, 2)]
    assert payload["workers"]["content"]["throughput.rate_per_second"] == [(1, 3.5)]
    assert payload["workers"]["content"]["items.rate_per_second"] == [(1, 0.75)]
    assert any(metric["field"] == "queue.total" for metric in payload["metrics"])
    assert any(metric["field"] == "throughput.rate_per_second" for metric in payload["metrics"])
    assert any(metric["field"] == "items.rate_per_second" for metric in payload["metrics"])
    assert calls[0][2] == ("content",)
    assert calls[0][1] - calls[0][0] == 7200


def test_favicon_route_serves_the_shipped_icon():
    client = TestClient(app_module.app)

    response = client.get("/favicon.ico")

    assert response.status_code == 200
    assert response.content == (app_module.STATIC_DIR / "favicon.ico").read_bytes()
    assert response.headers["content-type"].startswith("image/vnd.microsoft.icon")


@pytest.mark.asyncio
async def test_content_observability_emit_includes_queue_and_timings(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

    async def fake_queue_stats():
        return {"pubsub": 7, "total": 9, "in_flight": 3}

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "queue_stats", fake_queue_stats)
    monkeypatch.setattr(content_worker.os, "getpid", lambda: 12345)

    content_worker.CONTENT_OBSERVABILITY_LAST_EMITTED = 0.0
    async with content_worker.CONTENT_THROUGHPUT_LOCK:
        content_worker.CONTENT_THROUGHPUT_STARTED_AT = 10.0
        content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 50
    async with content_worker.CONTENT_TIMING_LOCK:
        for key in content_worker.CONTENT_TIMING_TOTALS:
            content_worker.CONTENT_TIMING_TOTALS[key] = 0.0
        content_worker.CONTENT_TIMING_TOTALS["selection"] = 1.25
    async with content_worker.CONTENT_FAILURE_LOCK:
        for key in content_worker.CONTENT_FAILURE_TOTALS:
            content_worker.CONTENT_FAILURE_TOTALS[key] = 0
        content_worker.CONTENT_FAILURE_TOTALS["validation_rejected"] = 2

    await content_worker._emit_content_observability(3, poll_seconds=5.0)

    assert captured[0][0] == "content"
    assert captured[0][1]["pid"] == 12345
    assert captured[0][1]["queue"] == {"pubsub": 7, "total": 9, "in_flight": 3}
    assert captured[0][1]["throughput"]["total_processed"] == 50
    assert captured[0][1]["failures"]["validation_rejected"] == 2
    assert captured[0][1]["timings"]["selection"] == 1.25


@pytest.mark.asyncio
async def test_content_throughput_snapshot_uses_recent_window(monkeypatch):
    class FakeLoop:
        def time(self):
            return 12.0

    monkeypatch.setattr(content_worker.asyncio, "get_running_loop", lambda: FakeLoop())
    monkeypatch.setattr(content_worker.time, "time", lambda: 40.0)

    class FakeTrace:
        async def count_events(self, **kwargs):
            assert kwargs["worker_names"] == ["content"]
            assert kwargs["event_types"] == ["results_written"]
            assert kwargs["since"] == 10
            return 60

    class FakeCache:
        item_trace = FakeTrace()

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "ITEM_TRACE_ENABLED", True)

    async with content_worker.CONTENT_THROUGHPUT_LOCK:
        content_worker.CONTENT_THROUGHPUT_STARTED_AT = 0.0
        content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 1000
        content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.clear()
        content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.extend([(10.0, 5), (11.0, 15)])

    try:
        snapshot = await content_worker._content_throughput_snapshot()
        assert snapshot["total_processed"] == 1000
        assert snapshot["elapsed_seconds"] == 12.0
        assert snapshot["rate_per_second"] == pytest.approx(2.0)
    finally:
        async with content_worker.CONTENT_THROUGHPUT_LOCK:
            content_worker.CONTENT_THROUGHPUT_STARTED_AT = None
            content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 0
            content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.clear()


@pytest.mark.asyncio
async def test_content_throughput_snapshot_skips_item_trace_when_disabled(monkeypatch):
    class FakeCache:
        class FakeTrace:
            async def count_events(self, **kwargs):
                raise AssertionError("count_events should not be called when item trace is disabled")

        item_trace = FakeTrace()

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "ITEM_TRACE_ENABLED", False)

    async with content_worker.CONTENT_THROUGHPUT_LOCK:
        content_worker.CONTENT_THROUGHPUT_STARTED_AT = 0.0
        content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 1000
        content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.clear()
        content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.extend([(10.0, 5), (11.0, 15)])

    try:
        snapshot = await content_worker._content_throughput_snapshot()
        assert snapshot["total_processed"] == 1000
        assert snapshot["rate_per_second"] == pytest.approx(20.0)
    finally:
        async with content_worker.CONTENT_THROUGHPUT_LOCK:
            content_worker.CONTENT_THROUGHPUT_STARTED_AT = None
            content_worker.CONTENT_THROUGHPUT_TOTAL_PROCESSED = 0
            content_worker.CONTENT_THROUGHPUT_RECENT_BATCHES.clear()


@pytest.mark.asyncio
async def test_inlinks_observability_emit_includes_queue_breakdown(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

    async def fake_queue_stats():
        return {
            "total": 19,
            "by_priority": {
                "unknown_active": {"depth": 11},
                "unknown_idle": {"depth": 5},
                "refresh_active": {"depth": 3},
                "refresh_idle": {"depth": 0},
            },
        }

    async def fake_throughput_snapshot():
        return {
            "total_processed": 42,
            "started_at": 10.0,
            "elapsed_seconds": 21.0,
            "rate_per_second": 2.0,
        }

    monkeypatch.setattr(inlinks_worker, "CACHE", FakeCache())
    monkeypatch.setattr(inlinks_worker, "queue_stats", fake_queue_stats)
    monkeypatch.setattr(inlinks_worker, "_inlinks_throughput_snapshot", fake_throughput_snapshot)
    monkeypatch.setattr(inlinks_worker, "INLINKS_OBSERVABILITY_LAST_EMITTED", 0.0)
    monkeypatch.setattr(
        inlinks_worker,
        "INLINKS_LAST_BATCH_OBSERVABILITY_SNAPSHOT",
        {
            "selected": 9,
            "processed": 9,
            "finalized": 4,
            "deferred": 5,
            "distinct_inlinks_found": 0,
            "truncated_targets": 0,
            "distinct_unknown_inlinks": 0,
            "distinct_interest_qids": 0,
            "interests_emitted": 7,
            "by_priority": {
                "unknown_active": {"selected": 4, "processed": 4, "finalized": 2, "deferred": 2, "interests_emitted": 3, "queue_depth": 4, "avg_age_seconds": 12.0, "p95_age_seconds": 21.0},
                "unknown_idle": {"selected": 2, "processed": 2, "finalized": 1, "deferred": 1, "interests_emitted": 1, "queue_depth": 2, "avg_age_seconds": 8.0, "p95_age_seconds": 9.0},
                "refresh_active": {"selected": 2, "processed": 2, "finalized": 1, "deferred": 1, "interests_emitted": 2, "queue_depth": 2, "avg_age_seconds": 30.0, "p95_age_seconds": 32.0},
                "refresh_idle": {"selected": 1, "processed": 1, "finalized": 0, "deferred": 1, "interests_emitted": 1, "queue_depth": 1, "avg_age_seconds": 44.0, "p95_age_seconds": 44.0},
            },
        },
    )

    await inlinks_worker._emit_inlinks_observability()

    assert captured[0][0] == "inlinks"
    assert captured[0][1]["queue"] == {
        "total": 19,
        "by_priority": {
            "unknown_active": {"depth": 11},
            "unknown_idle": {"depth": 5},
            "refresh_active": {"depth": 3},
            "refresh_idle": {"depth": 0},
        },
    }
    assert captured[0][1]["throughput"] == {
        "total_processed": 42,
        "started_at": 10.0,
        "elapsed_seconds": 21.0,
        "rate_per_second": 2.0,
    }
    assert captured[0][1]["batch"] == {
        "selected": 9,
        "processed": 9,
        "finalized": 4,
        "deferred": 5,
        "distinct_inlinks_found": 0,
        "truncated_targets": 0,
        "distinct_unknown_inlinks": 0,
        "distinct_interest_qids": 0,
        "interests_emitted": 7,
        "by_priority": {
            "unknown_active": {"selected": 4, "processed": 4, "finalized": 2, "deferred": 2, "interests_emitted": 3, "queue_depth": 4, "avg_age_seconds": 12.0, "p95_age_seconds": 21.0},
            "unknown_idle": {"selected": 2, "processed": 2, "finalized": 1, "deferred": 1, "interests_emitted": 1, "queue_depth": 2, "avg_age_seconds": 8.0, "p95_age_seconds": 9.0},
            "refresh_active": {"selected": 2, "processed": 2, "finalized": 1, "deferred": 1, "interests_emitted": 2, "queue_depth": 2, "avg_age_seconds": 30.0, "p95_age_seconds": 32.0},
            "refresh_idle": {"selected": 1, "processed": 1, "finalized": 0, "deferred": 1, "interests_emitted": 1, "queue_depth": 1, "avg_age_seconds": 44.0, "p95_age_seconds": 44.0},
        },
    }


def test_inlinks_priority_metrics_registered_in_observability_metadata():
    for field in [
        "queue.by_priority.unknown_active.depth",
        "batch.by_priority.unknown_active.processed",
        "batch.by_priority.unknown_active.finalized",
        "batch.by_priority.refresh_idle.deferred",
        "batch.by_priority.refresh_idle.p95_age_seconds",
    ]:
        assert field in app_module.OBSERVABILITY_FIELD_METADATA


def test_cache_flag_metadata_includes_unknown():
    for field in [
        "flags.redirect.unknown",
        "flags.redirect.no",
        "flags.redirect.yes",
    ]:
        assert field in app_module.OBSERVABILITY_FIELD_METADATA


@pytest.mark.asyncio
async def test_recent_changes_observability_emit_includes_throughput(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

    monkeypatch.setattr(recent_changes_worker, "CACHE", FakeCache())
    monkeypatch.setattr(recent_changes_worker, "RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED", 0.0)

    await recent_changes_worker._emit_recent_changes_observability(
        worker_name="recent_changes_scan",
        queue={"recent_changes": 13, "total": 13},
        throughput={
            "total_processed": 99,
            "started_at": 5.0,
            "elapsed_seconds": 33.0,
            "rate_per_second": 3.0,
        },
    )
    await recent_changes_worker._emit_recent_changes_observability(
        worker_name="recent_changes_creation_interest",
        queue={"creation_interest_backfill": 7, "total": 7},
        throughput={
            "total_processed": 19,
            "started_at": 3.0,
            "elapsed_seconds": 5.0,
            "rate_per_second": 3.8,
        },
    )
    await recent_changes_worker._emit_recent_changes_observability(
        worker_name="recent_changes_user_creation",
        queue={"user_creation_backfill": 0, "total": 0},
        throughput={
            "total_processed": 0,
            "started_at": None,
            "elapsed_seconds": 0.0,
            "rate_per_second": 0.0,
        },
    )

    assert captured[0][0] == "recent_changes_scan"
    assert captured[0][1]["queue"]["recent_changes"] == 13
    assert captured[0][1]["queue"]["total"] == 13
    assert captured[0][1]["throughput"] == {
        "total_processed": 99,
        "started_at": 5.0,
        "elapsed_seconds": 33.0,
        "rate_per_second": 3.0,
    }
    assert captured[1][0] == "recent_changes_creation_interest"
    assert captured[1][1]["queue"]["creation_interest_backfill"] == 7
    assert captured[1][1]["queue"]["total"] == 7
    assert captured[2][0] == "recent_changes_user_creation"
    assert captured[2][1]["queue"]["user_creation_backfill"] == 0
    assert captured[2][1]["queue"]["total"] == 0


@pytest.mark.asyncio
async def test_cache_observability_emit_includes_breakdown(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

        async def breakdown(self):
            return {
                "evaluations": {
                    "entries": 7,
                    "oldest_content_last_revid": 1,
                    "newest_content_last_revid": 7,
                    "oldest_recent_changes_last_revid": None,
                    "newest_recent_changes_last_revid": None,
                    "wikisub_entries": 0,
                },
                "flags": {"redirect": {"unknown": 1, "yes": 2, "no": 4}},
                "criteria_detected": {
                    "N1": {
                        "unknown": 0,
                        "none": 5,
                        "partial-weak": 0,
                        "partial-strong": 0,
                        "weak": 1,
                        "strong": 1,
                    }
                },
                "criteria_deduced": {
                    "N3": {
                        "unknown": 1,
                        "none": 4,
                        "partial-weak": 0,
                        "partial-strong": 0,
                        "weak": 1,
                        "strong": 1,
                    }
                },
            }

    monkeypatch.setattr(cache_observability_worker, "CACHE", FakeCache())
    monkeypatch.setattr(cache_observability_worker, "CACHE_OBSERVABILITY_LAST_EMITTED", 0.0)

    await cache_observability_worker._emit_cache_observability()

    assert captured[0][0] == "cache"
    assert captured[0][1] == {
        "items": {"total": 7},
        "flags": {"redirect": {"unknown": 1, "yes": 2, "no": 4}},
        "criteria": {
            "detected": {
                "N1": {
                    "unknown": 0,
                    "none": 5,
                    "partial-weak": 0,
                    "partial-strong": 0,
                    "weak": 1,
                    "strong": 1,
                }
            },
            "deduced": {
                "N3": {
                    "unknown": 1,
                    "none": 4,
                    "partial-weak": 0,
                    "partial-strong": 0,
                    "weak": 1,
                    "strong": 1,
                }
            },
        },
    }


def test_observability_page_renders_html():
    client = TestClient(app_module.app)

    response = client.get("/observability")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "Observability" in response.text
    assert "/static/observability.js" in response.text
    assert "echarts.min.js" in response.text
    assert 'id="period"' in response.text
    assert 'id="refresh"' in response.text
    assert 'id="autorefresh" type="checkbox" />' in response.text


@pytest.mark.asyncio
async def test_item_trace_page_renders_html(monkeypatch):
    monkeypatch.setattr(app_module, "ITEM_TRACE_ENABLED", False)

    with pytest.raises(HTTPException) as excinfo:
        response = await item_trace_page()

    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "Item trace is disabled"


def test_stacked_area_tooltip_includes_total():
    js_source = Path(__file__).resolve().parents[1] / "server" / "static" / "observability.js"
    contents = js_source.read_text()

    assert "Total:" in contents
    assert "formatMetricValue(total)" in contents
    assert "Cache growth rate" in contents


def test_pubsub_debug_page_and_api_render(monkeypatch):
    class FakePubsub:
        async def list_pubsub_interest_items(self, limit=None):
                return [
                    {
                        "qid": "Q42",
                        "session_rows": 2,
                        "total_priority": 12,
                        "oldest_expires_at": 1000,
                        "newest_expires_at": 1002,
                        "owner_count": 2,
                        "owner_ids": ["gadget", "report"],
                        "wants_content": True,
                        "wants_inlinks": True,
                        "workers": [
                            {
                                "owner_id": "gadget",
                                "session_rows": 1,
                                "total_priority": 5,
                                "oldest_expires_at": 1000,
                                "newest_expires_at": 1000,
                                "wants_content": True,
                                "wants_inlinks": True,
                                "wants_content_rows": 1,
                                "wants_inlinks_rows": 1,
                            }
                        ],
                    }
                ]

        async def pubsub_stats(self):
            return {"entries": 1}

    class FakeCache:
        pubsub = FakePubsub()

    monkeypatch.setattr(app_module, "CACHE", FakeCache())

    client = TestClient(app_module.app)
    page_response = client.get("/pubsub")
    api_response = client.get("/api/pubsub/debug")

    assert page_response.status_code == 200
    assert "PubSub debugger" in page_response.text
    assert "/api/pubsub/debug" in page_response.text
    assert "Expires" in page_response.text
    assert "gadget" in page_response.text
    assert api_response.status_code == 200
    assert api_response.json()["items"][0]["qid"] == "Q42"

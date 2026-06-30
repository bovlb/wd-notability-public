from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

import server.app as app_module
from wd_notability.content import worker as entitydata_worker
from wd_notability.inlinks import worker as inlinks_worker
from wd_notability.content import recent_changes as recent_changes_worker
from wd_notability.external_usage import worker as cache_sync_worker
from wd_notability import cache_observability as cache_observability_worker
from wd_notability.evaluation_cache import EvaluationCache


@pytest.mark.asyncio
async def test_observability_store_derives_smoothed_throughput(tmp_path):
    cache = EvaluationCache(db_path=tmp_path / "observability.sqlite3")
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
async def test_api_observability_uses_period_and_worker_filters(monkeypatch):
    calls: list[tuple[int, int, tuple[str, ...] | None]] = []

    class FakeObservability:
        async def snapshot_views(self, *, since, until, worker_names, limit=None):
            calls.append((since, until, None if worker_names is None else tuple(worker_names)))
            return (
                {"queue.total": [(1, 2)], "throughput.rate_per_second": [(1, 3.5)]},
                {
                    "content": {"queue.total": [(1, 2)], "throughput.rate_per_second": [(1, 3.5)]},
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
    assert payload["workers"]["content"]["queue.total"] == [(1, 2)]
    assert payload["workers"]["content"]["throughput.rate_per_second"] == [(1, 3.5)]
    assert any(metric["field"] == "queue.total" for metric in payload["metrics"])
    assert any(metric["field"] == "throughput.rate_per_second" for metric in payload["metrics"])
    assert calls[0][2] == ("content",)
    assert calls[0][1] - calls[0][0] == 7200


@pytest.mark.asyncio
async def test_entitydata_observability_emit_includes_queue_and_timings(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

    async def fake_queue_stats():
        return {"pubsub": 7, "total": 9, "in_flight": 3}

    monkeypatch.setattr(entitydata_worker, "CACHE", FakeCache())
    monkeypatch.setattr(entitydata_worker, "queue_stats", fake_queue_stats)
    monkeypatch.setattr(entitydata_worker.os, "getpid", lambda: 12345)

    entitydata_worker.ENTITYDATA_OBSERVABILITY_LAST_EMITTED.clear()
    async with entitydata_worker.ENTITYDATA_THROUGHPUT_LOCK:
        entitydata_worker.ENTITYDATA_THROUGHPUT_STARTED_AT = 10.0
        entitydata_worker.ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED = 50
    async with entitydata_worker.ENTITYDATA_TIMING_LOCK:
        for key in entitydata_worker.ENTITYDATA_TIMING_TOTALS:
            entitydata_worker.ENTITYDATA_TIMING_TOTALS[key] = 0.0
        entitydata_worker.ENTITYDATA_TIMING_TOTALS["selection"] = 1.25
    async with entitydata_worker.ENTITYDATA_FAILURE_LOCK:
        for key in entitydata_worker.ENTITYDATA_FAILURE_TOTALS:
            entitydata_worker.ENTITYDATA_FAILURE_TOTALS[key] = 0
        entitydata_worker.ENTITYDATA_FAILURE_TOTALS["validation_rejected"] = 2

    await entitydata_worker._emit_entitydata_observability(3, poll_seconds=5.0)

    assert captured[0][0] == "content"
    assert captured[0][1]["pid"] == 12345
    assert captured[0][1]["queue"] == {"pubsub": 7, "total": 9, "in_flight": 3}
    assert captured[0][1]["throughput"]["total_processed"] == 50
    assert captured[0][1]["failures"]["validation_rejected"] == 2
    assert captured[0][1]["timings"]["selection"] == 1.25


@pytest.mark.asyncio
async def test_entitydata_throughput_snapshot_uses_recent_window(monkeypatch):
    class FakeLoop:
        def time(self):
            return 12.0

    monkeypatch.setattr(entitydata_worker.asyncio, "get_running_loop", lambda: FakeLoop())

    async with entitydata_worker.ENTITYDATA_THROUGHPUT_LOCK:
        entitydata_worker.ENTITYDATA_THROUGHPUT_STARTED_AT = 0.0
        entitydata_worker.ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED = 1000
        entitydata_worker.ENTITYDATA_THROUGHPUT_RECENT_BATCHES.clear()
        entitydata_worker.ENTITYDATA_THROUGHPUT_RECENT_BATCHES.extend([(10.0, 5), (11.0, 15)])

    try:
        snapshot = await entitydata_worker._entitydata_throughput_snapshot()
        assert snapshot["total_processed"] == 1000
        assert snapshot["elapsed_seconds"] == 12.0
        assert snapshot["rate_per_second"] == pytest.approx(20.0)
    finally:
        async with entitydata_worker.ENTITYDATA_THROUGHPUT_LOCK:
            entitydata_worker.ENTITYDATA_THROUGHPUT_STARTED_AT = None
            entitydata_worker.ENTITYDATA_THROUGHPUT_TOTAL_PROCESSED = 0
            entitydata_worker.ENTITYDATA_THROUGHPUT_RECENT_BATCHES.clear()


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


@pytest.mark.asyncio
async def test_recent_changes_observability_emit_includes_throughput(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

        async def count_missing_creation_qids(self):
            return 7

    async def fake_queue_stats():
        return {"recent_changes": 13, "creation_backfill": 7, "total": 20}

    async def fake_throughput_snapshot():
        return {
            "total_processed": 99,
            "started_at": 5.0,
            "elapsed_seconds": 33.0,
            "rate_per_second": 3.0,
        }

    monkeypatch.setattr(recent_changes_worker, "CACHE", FakeCache())
    monkeypatch.setattr(recent_changes_worker, "queue_stats", fake_queue_stats)
    monkeypatch.setattr(recent_changes_worker, "_recent_changes_throughput_snapshot", fake_throughput_snapshot)
    monkeypatch.setattr(recent_changes_worker, "RECENT_CHANGES_OBSERVABILITY_LAST_EMITTED", 0.0)

    await recent_changes_worker._emit_recent_changes_observability()

    assert captured[0][0] == "recent_changes"
    assert captured[0][1]["queue"] == {"recent_changes": 13, "creation_backfill": 7, "total": 20}
    assert captured[0][1]["throughput"] == {
        "total_processed": 99,
        "started_at": 5.0,
        "elapsed_seconds": 33.0,
        "rate_per_second": 3.0,
    }


@pytest.mark.asyncio
async def test_cache_sync_observability_emit_includes_throughput(monkeypatch):
    captured: list[tuple[str, dict[str, object]]] = []

    class FakeObservability:
        async def record_worker_snapshot(self, *, worker_name, data, timestamp=None):
            captured.append((worker_name, data))

    class FakeCache:
        observability = FakeObservability()

    async def fake_queue_stats():
        return {"candidates": 12, "total": 12}

    async def fake_throughput_snapshot():
        return {
            "total_processed": 44,
            "started_at": 8.0,
            "elapsed_seconds": 11.0,
            "rate_per_second": 4.0,
        }

    monkeypatch.setattr(cache_sync_worker, "CACHE", FakeCache())
    monkeypatch.setattr(cache_sync_worker, "queue_stats", fake_queue_stats)
    monkeypatch.setattr(cache_sync_worker, "_cache_sync_throughput_snapshot", fake_throughput_snapshot)
    monkeypatch.setattr(cache_sync_worker, "CACHE_SYNC_OBSERVABILITY_LAST_EMITTED", 0.0)

    await cache_sync_worker._emit_cache_sync_observability()

    assert captured[0][0] == "cache_sync"
    assert captured[0][1]["queue"] == {"candidates": 12, "total": 12}
    assert captured[0][1]["throughput"] == {
        "total_processed": 44,
        "started_at": 8.0,
        "elapsed_seconds": 11.0,
        "rate_per_second": 4.0,
    }


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
                "entries": 7,
                "flags": {"redirect": {"yes": 2, "no": 5}},
                "criteria_detected": {"N1": {"unknown": 0, "none": 5, "weak": 1, "strong": 1}},
                "criteria_deduced": {"N3": {"unknown": 1, "none": 4, "weak": 1, "strong": 1}},
            }

    monkeypatch.setattr(cache_observability_worker, "CACHE", FakeCache())
    monkeypatch.setattr(cache_observability_worker, "CACHE_OBSERVABILITY_LAST_EMITTED", 0.0)

    await cache_observability_worker._emit_cache_observability()

    assert captured[0][0] == "cache"
    assert captured[0][1] == {
        "items": {"total": 7},
        "flags": {"redirect": {"yes": 2, "no": 5}},
        "criteria": {
            "detected": {"N1": {"unknown": 0, "none": 5, "weak": 1, "strong": 1}},
            "deduced": {"N3": {"unknown": 1, "none": 4, "weak": 1, "strong": 1}},
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


def test_pubsub_debug_page_and_api_render(monkeypatch):
    class FakePubsub:
        async def list_pubsub_interest_items(self, limit=None):
            return [
                {
                    "qid": "Q42",
                    "session_rows": 2,
                    "total_priority": 12,
                    "owner_count": 2,
                    "owner_ids": ["gadget", "report"],
                    "wants_entitydata": True,
                    "wants_inlinks": True,
                    "wants_sync": False,
                    "workers": [
                        {
                            "owner_id": "gadget",
                            "session_rows": 1,
                            "total_priority": 5,
                            "wants_entitydata": True,
                            "wants_inlinks": True,
                            "wants_sync": False,
                            "wants_entitydata_rows": 1,
                            "wants_inlinks_rows": 1,
                            "wants_sync_rows": 0,
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
    assert "gadget" in page_response.text
    assert api_response.status_code == 200
    assert api_response.json()["items"][0]["qid"] == "Q42"

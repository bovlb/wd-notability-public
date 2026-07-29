from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

from wd_notability.content import worker as content_worker
from wd_notability.item_trace import ItemTraceRecord, ItemTraceStore
from wd_notability.models import NotabilityLevel


class _DummyWriteGuard:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeTraceCacheBase:
    _backend_name = "mariadb"

    async def initialize(self) -> None:
        return None

    def _parse_qid(self, qid):
        if isinstance(qid, int):
            return qid
        if isinstance(qid, str) and qid.upper().startswith("Q") and qid[1:].isdigit():
            return int(qid[1:])
        return None

    def _chunked(self, values, size=500):
        return [list(values[index:index + size]) for index in range(0, len(values), size)]

    def _write_guard(self):
        return _DummyWriteGuard()

    def _warn_slow_write(self, operation, started, *, row_count=None):
        return None

    def _as_uint32(self, value, field_name):
        return int(value)


@pytest.mark.asyncio
async def test_item_trace_store_batches_and_lists_events(monkeypatch):
    cache = _FakeTraceCacheBase()
    store = ItemTraceStore(cache)  # type: ignore[arg-type]
    store.enabled = True

    written: list[list[tuple[int, int, str, str, str | None, str]]] = []
    pruned: list[int] = []

    async def fake_write_rows(rows):
        written.append(list(rows))
        return len(rows)

    monkeypatch.setattr(store, "_write_rows", fake_write_rows)

    accepted = await store.record_events(
        [
            ItemTraceRecord(
                qid="Q42",
                event_type="interest_started",
                worker_name="content",
                batch_id="batch-1",
                details={"interest_type": "content"},
            ),
            ItemTraceRecord(
                qid="Q42",
                event_type="results_written",
                worker_name="content",
                batch_id="batch-1",
                details={"changed_rows": 1},
            ),
        ]
    )
    assert accepted == 2
    assert written == []

    flushed = await store.flush()
    assert flushed == 2
    assert len(written) == 1
    assert written[0][0][2] == "interest_started"
    assert written[0][1][2] == "results_written"
    assert pruned == []
    await store.close()

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return list(self._rows)

    class FakeDB:
        async def execute(self, sql, params=None):
            return FakeCursor(
                [
                    (datetime(2026, 7, 20, 3, 49, 8, 123456, tzinfo=UTC), 42, "interest_started", "content", "batch-1", '{"interest_type":"content"}'),
                    (datetime(2026, 7, 20, 3, 49, 9, 654321, tzinfo=UTC), 42, "results_written", "content", "batch-1", '{"changed_rows":1}'),
                ]
            )

    @asynccontextmanager
    async def fake_connect():
        yield FakeDB()

    store.cache._connect = fake_connect  # type: ignore[attr-defined]
    rows = await store.list_events(qid="Q42")
    assert [row["event_type"] for row in rows] == ["interest_started", "results_written"]
    assert rows[0]["timestamp"] == "2026-07-20T03:49:08.123456Z"
    assert rows[1]["timestamp"] == "2026-07-20T03:49:09.654321Z"
    assert rows[0]["details"]["interest_type"] == "content"
    assert rows[1]["details"]["changed_rows"] == 1


@pytest.mark.asyncio
async def test_item_trace_store_prune_loop_runs_out_of_band(monkeypatch):
    cache = _FakeTraceCacheBase()
    store = ItemTraceStore(cache)  # type: ignore[arg-type]
    store.enabled = True

    pruned: list[int] = []

    async def fake_sleep(seconds):
        return None

    async def fake_maybe_prune():
        pruned.append(1)
        raise asyncio.CancelledError

    monkeypatch.setattr("wd_notability.item_trace.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(store, "_maybe_prune", fake_maybe_prune)

    with pytest.raises(asyncio.CancelledError):
        await store._prune_loop()

    assert pruned == [1]


@pytest.mark.asyncio
async def test_persist_content_chunk_records_results_written(monkeypatch):
    captured: list[list[ItemTraceRecord]] = []

    class FakeTrace:
        async def record_events(self, records):
            captured.append(list(records))
            return len(records)

        async def flush(self):
            return 0

    class FakeCache:
        item_trace = FakeTrace()

    async def fake_upsert_content_updates(updates):
        return [("Q42", 1)]

    async def fake_debug_verify_completed_content_batch(qids):
        return None

    monkeypatch.setattr(content_worker, "CACHE", FakeCache())
    monkeypatch.setattr(content_worker, "upsert_content_updates", fake_upsert_content_updates)
    monkeypatch.setattr(content_worker, "_debug_verify_completed_content_batch", fake_debug_verify_completed_content_batch)

    update = content_worker.ContentUpdate(
        qid="Q42",
        is_redirect=False,
        has_claims_count=1,
        has_sitelinks_count=1,
        is_deleted=False,
        n1=NotabilityLevel.NONE,
        n2a=NotabilityLevel.NONE,
        n2b=NotabilityLevel.NONE,
        content_last_revid=123,
        redirect_target=None,
    )

    changed = await content_worker._persist_content_chunk([update], {"upsert": 0.0}, batch_id="batch-1")

    assert changed == [("Q42", 1)]
    assert len(captured) == 1
    assert len(captured[0]) == 1
    record = captured[0][0]
    assert record.worker_name == "content"
    assert record.batch_id == "batch-1"
    assert record.qid == "Q42"
    assert record.event_type == "results_written"
    assert record.details is not None
    assert record.details["changed_rows"] == 1
    assert record.details["content_last_revid"] == 123
    assert "updates" not in record.details

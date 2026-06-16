import pytest
from fastapi import BackgroundTasks

import server.app as app_module
from server.app import SubscribeItem, SubscribeRequest, _normalize_subscription_items, _subscription_event_stream
from wd_notability.models import EvaluationReason, EvaluationResult, NotabilityCriterion


async def _run_background_tasks(background_tasks: BackgroundTasks) -> None:
    for task in background_tasks.tasks:
        result = task.func(*task.args, **task.kwargs)
        if hasattr(result, "__await__"):
            await result


def test_normalize_subscription_items_accepts_legacy_qids():
    request = SubscribeRequest(qids=["q42", "Q42", "not-a-qid"])

    assert _normalize_subscription_items(request) == {"Q42": EvaluationReason.PAGE}


def test_normalize_subscription_items_keeps_highest_reason():
    request = SubscribeRequest(
        qids=["Q42"],
        items=[
            SubscribeItem(qid="q42", reason="text"),
            SubscribeItem(qid="Q42", reason="create"),
            SubscribeItem(qid="Q99", reason="use"),
            SubscribeItem(qid="Q100", reason="not-real"),
        ],
    )

    assert _normalize_subscription_items(request) == {
        "Q42": EvaluationReason.PAGE,
        "Q99": EvaluationReason.USE,
        "Q100": EvaluationReason.PAGE,
    }


class ExampleDetector:
    name = "example"
    criterion = NotabilityCriterion.N3_INLINKS


class FakeRequest:
    def __init__(self, disconnected=False):
        self.disconnected = disconnected

    async def is_disconnected(self):
        return self.disconnected


@pytest.mark.asyncio
async def test_subscribe_queues_incomplete_cached_items(monkeypatch):
    incomplete = EvaluationResult(qid="Q42")
    incomplete.add_error(ExampleDetector(), RuntimeError("failed"))

    class FakeCache:
        async def get_many(self, qids):
            return {qid: (incomplete.summary, 123) for qid in qids}

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    background_tasks = BackgroundTasks()

    payload = await app_module.api_subscribe(
        SubscribeRequest(items=[SubscribeItem(qid="Q42", reason="page")]),
        background_tasks,
    )
    await _run_background_tasks(background_tasks)

    assert payload["cached_items"][0]["qid"] == "Q42"
    assert payload["cache_misses"] == ["Q42"]


@pytest.mark.asyncio
async def test_subscribe_includes_complete_cached_items_in_subscription(monkeypatch):
    complete = EvaluationResult(qid="Q42")

    class FakeCache:
        async def get_many(self, qids):
            return {qid: (complete.summary, 123) for qid in qids}

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    background_tasks = BackgroundTasks()

    payload = await app_module.api_subscribe(
        SubscribeRequest(items=[SubscribeItem(qid="Q42", reason="page")]),
        background_tasks,
    )
    await _run_background_tasks(background_tasks)

    assert payload["cached_items"][0]["qid"] == "Q42"
    assert payload["cache_misses"] == []
    assert payload["subscription_id"]
    assert app_module.SUBSCRIPTIONS[payload["subscription_id"]] == {"Q42"}


@pytest.mark.asyncio
async def test_subscribe_uses_batch_enqueue_when_available(monkeypatch):
    incomplete = EvaluationResult(qid="Q42")
    incomplete.add_error(ExampleDetector(), RuntimeError("failed"))

    class FakeCache:
        async def get_many(self, qids):
            return {qid: (incomplete.summary, 123) for qid in qids}

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    background_tasks = BackgroundTasks()

    payload = await app_module.api_subscribe(
        SubscribeRequest(items=[SubscribeItem(qid="Q42", reason="page")]),
        background_tasks,
    )
    await _run_background_tasks(background_tasks)

    assert payload["cache_misses"] == ["Q42"]


@pytest.mark.asyncio
async def test_event_stream_exits_for_disconnected_request(monkeypatch):
    class FakeCache:
        async def get_many(self, qids):
            raise AssertionError("disconnected streams should not read the cache")

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "SHUTDOWN_EVENT", None)

    messages = [
        message
        async for message in _subscription_event_stream(
            "sub",
            {"Q42"},
            FakeRequest(disconnected=True),
        )
    ]

    assert messages == []


@pytest.mark.asyncio
async def test_event_stream_stops_when_shutdown_sleep_wakes(monkeypatch):
    class FakeCache:
        async def get_many(self, qids):
            return {}

    async def fake_sleep_or_shutdown(seconds):
        return True

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "SHUTDOWN_EVENT", None)
    monkeypatch.setattr(app_module, "_sleep_or_shutdown", fake_sleep_or_shutdown)

    messages = [
        message
        async for message in _subscription_event_stream(
            "sub",
            {"Q42"},
            FakeRequest(),
        )
    ]

    assert messages == ['data: {"event": "keepalive"}\n\n']


@pytest.mark.asyncio
async def test_event_stream_sends_stream_end_at_lifetime_limit(monkeypatch):
    class FakeCache:
        async def get_many(self, qids):
            raise AssertionError("expired streams should not read the cache")

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "SHUTDOWN_EVENT", None)
    monkeypatch.setattr(app_module, "SSE_STREAM_MAX_SECONDS", 0)

    messages = [
        message
        async for message in _subscription_event_stream(
            "sub",
            {"Q42"},
            FakeRequest(),
        )
    ]

    assert messages == ['data: {"event": "stream_end"}\n\n']

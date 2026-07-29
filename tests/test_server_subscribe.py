from types import SimpleNamespace
import json

import pytest

import server.app as app_module
import server.routes_api as routes_api
from server.app import _normalize_subscription_items, _subscription_event_stream
from server.schemas import SubscribeItem, SubscribeRequest
from wd_notability.models import EvaluationReason, EvaluationResult


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


@pytest.mark.asyncio
async def test_api_creations_uses_replica_creation_query(monkeypatch):
    expected_rows = [
        SimpleNamespace(qid="Q42", creator="Alice", creation_time=123),
        SimpleNamespace(qid="Q99", creator="Bob", creation_time=456),
    ]

    def fake_fetch_creations(*, start, end, creators):
        assert start == "2026-07-17T22:13:28Z"
        assert end == "2026-07-18T22:13:28Z"
        assert creators == []
        return expected_rows

    monkeypatch.setattr(app_module.CREATIONS, "fetch_creations", fake_fetch_creations)

    payload = await routes_api.api_creations(
        start="2026-07-17T22:13:28Z",
        end="2026-07-18T22:13:28Z",
        creators=[],
    )

    assert payload["items"] == [
        {"qid": "Q42", "creator": "Alice", "creation_time": 123},
        {"qid": "Q99", "creator": "Bob", "creation_time": 456},
    ]


class FakeRequest:
    def __init__(self, disconnected=False):
        self.disconnected = disconnected

    async def is_disconnected(self):
        return self.disconnected


@pytest.mark.asyncio
async def test_subscription_event_stream_traces_badge_served(monkeypatch):
    captured: list = []

    class FakeTrace:
        async def record_event(self, record):
            captured.append(record)
            return 1

    class FakeSession:
        async def replace(self, qids):
            return None

        async def clear(self):
            return None

        async def close(self):
            return None

    class FakeManager:
        def create_session(self):
            return FakeSession()

        async def close(self):
            return None

    class FakeInterest:
        async def create_interest_manager(self, **kwargs):
            return FakeManager()

    class FakeCache:
        item_trace = FakeTrace()
        interest = FakeInterest()

        async def get_content_staleness_for_qids(self, qids):
            assert qids == ["Q42"]
            return {"Q42": True}

    fake_result = SimpleNamespace(
        levels_str={
            "N": "none",
            "N1": "none",
            "N2a": "none",
            "N2b": "none",
            "N3": "none",
            "N3_inlinks": "none",
            "N3_osm": "unknown",
            "N3_wikisub": "unknown",
            "N3_sdc": "unknown",
        },
        n1="none",
        n2a="none",
        n2b="none",
        n3_inlinks="none",
        n3_osm="unknown",
        n3_wikisub="unknown",
        n3_sdc="unknown",
        is_redirect=False,
        has_claims_count=1,
        has_sitelinks_count=1,
        inlinks_count=1,
        is_deleted=False,
        redirect_target=None,
        content_last_revid=123,
        recent_changes_last_revid=456,
    )

    async def fake_resolve_creation_bootstrap(qids):
        return {qid: fake_result for qid in qids}, {qid: {"creator": "Alice", "creation_time": 123} for qid in qids}

    async def fake_sleep_or_shutdown(seconds):
        return True

    async def fake_touch(subscription_id):
        return None

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "SHUTDOWN_EVENT", None)
    monkeypatch.setattr(app_module, "WEB_INTEREST_MANAGER", None)
    monkeypatch.setattr(app_module, "web_resolve_creation_bootstrap", fake_resolve_creation_bootstrap)
    monkeypatch.setattr(app_module, "_sleep_or_shutdown", fake_sleep_or_shutdown)
    monkeypatch.setattr(app_module, "_touch_gadget_subscription", fake_touch)

    messages = [
        message
        async for message in app_module._subscription_event_stream(
            "sub-1",
            {"Q42"},
            FakeRequest(),
        )
    ]

    assert messages[0] == 'data: {"event": "primed", "subscription_id": "sub-1", "qid_count": 1}\n\n'
    assert len(captured) == 1
    assert captured[0].event_type == "badge_served"
    assert captured[0].worker_name == "sse"
    assert captured[0].details["creator"] == "Alice"
    assert captured[0].details["creation_time"] == 123
    assert "changed_fields" in captured[0].details
    assert "content_last_revid" in captured[0].details["changed_fields"]
    assert "content_stale" in captured[0].details["changed_fields"]
    assert "creator" in captured[0].details["changed_fields"]
    assert captured[0].details["stream"] == "gadget_subscription"
    assert '"content_stale": true' in messages[1]


@pytest.mark.asyncio
async def test_start_interest_stream_reuses_web_manager(monkeypatch):
    calls: list[dict[str, object]] = []

    class FakeSession:
        async def replace(self, qids):
            calls.append({"replace": list(qids)})

    class FakeManager:
        def create_session(self):
            calls.append({"create_session": True})
            return FakeSession()

    class FakeInterest:
        async def create_interest_manager(self, **kwargs):
            calls.append(dict(kwargs))
            return FakeManager()

    class FakeCache:
        interest = FakeInterest()

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "WEB_INTEREST_MANAGER", None)

    first = await app_module._start_interest_stream(
        owner_id="gadget",
        session_id="sub-1",
        qids=["Q1"],
        worker_id="gadget:sub-1",
        priority=10,
        wants_creation=True,
        wants_content=True,
        wants_inlinks=True,
    )
    second = await app_module._start_interest_stream(
        owner_id="gadget",
        session_id="sub-2",
        qids=["Q2"],
        worker_id="gadget:sub-2",
        priority=10,
        wants_creation=True,
        wants_content=True,
        wants_inlinks=True,
    )

    assert calls[0]["worker_id"] == "web"
    assert calls[0]["priority"] == 10
    assert calls[0]["wants_creation"] is True
    assert calls[0]["wants_content"] is True
    assert calls[0]["wants_inlinks"] is True
    assert calls.count({"create_session": True}) == 2
    assert calls.count({"replace": ["Q1"]}) == 1
    assert calls.count({"replace": ["Q2"]}) == 1
    assert first[0] is second[0]
    assert first[1] is not second[1]


@pytest.mark.asyncio
async def test_api_pubsub_debug_includes_worker_ids(monkeypatch):
    class FakeInterest:
        async def list_interest_items(self, limit=None):
            return [
                {
                    "qid": "Q42",
                    "session_rows": 2,
                    "lease_rows": 2,
                    "total_priority": 20,
                    "wants_creation": True,
                    "wants_content": True,
                    "wants_inlinks": False,
                    "owner_count": 1,
                    "workers": [
                        {
                            "worker_id": "web",
                            "session_rows": 2,
                            "total_priority": 20,
                            "wants_creation": True,
                            "wants_content": True,
                            "wants_inlinks": False,
                            "wants_creation_rows": 1,
                            "wants_content_rows": 1,
                            "wants_inlinks_rows": 0,
                        }
                    ],
                }
            ]

        async def interest_stats(self):
            return {"entries": 1}

    class FakeCache:
        interest = FakeInterest()

    monkeypatch.setattr(app_module, "CACHE", FakeCache())

    payload = await routes_api.api_pubsub_debug()

    assert payload["items"][0]["workers"][0]["worker_id"] == "web"


@pytest.mark.asyncio
async def test_pubsub_event_stream_traces_badge_served(monkeypatch):
    calls: list[str] = []

    class FakeSession:
        async def replace(self, qids):
            calls.append(f"replace:{','.join(qids)}")

        async def close(self):
            calls.append("session_closed")

    class FakeManager:
        def create_session(self):
            calls.append("create_session")
            return FakeSession()

        async def close(self):
            calls.append("manager_closed")

    class FakeInterest:
        async def create_interest_lease(self, **kwargs):
            calls.append(f"create_interest_lease:{kwargs['owner_id']}:{kwargs['lease_id']}")
            return None

        async def create_interest_manager(self, **kwargs):
            calls.append(f"create_interest_manager:{kwargs['worker_id']}")
            return FakeManager()

    class FakePubsub:
        async def create_interest_lease(self, **kwargs):
            return None

        async def refresh_interest_lease(self, **kwargs):
            return None

        async def create_pubsub_lease(self, **kwargs):
            return None

        async def refresh_pubsub_lease(self, **kwargs):
            return None

    class FakeCache:
        pubsub = FakePubsub()
        interest = FakeInterest()

        async def get_many(self, qids):
            return {qid: fake_result for qid in qids}

        async def get_content_staleness_for_qids(self, qids):
            assert qids == ["Q42"]
            return {"Q42": True}

    fake_result = SimpleNamespace(
        levels_str={
            "N": "none",
            "N1": "none",
            "N2a": "none",
            "N2b": "none",
            "N3": "none",
            "N3_inlinks": "none",
            "N3_osm": "unknown",
            "N3_wikisub": "unknown",
            "N3_sdc": "unknown",
        },
        n1="none",
        n2a="none",
        n2b="none",
        n3_inlinks="none",
        n3_osm="unknown",
        n3_wikisub="unknown",
        n3_sdc="unknown",
        is_redirect=False,
        has_claims_count=1,
        has_sitelinks_count=1,
        inlinks_count=1,
        is_deleted=True,
        redirect_target=None,
        content_last_revid=123,
        recent_changes_last_revid=456,
    )

    async def fake_resolve_creation_metadata(qids):
        return {qid: {"creator": "Alice", "creation_time": 123} for qid in qids}

    async def fake_sleep_or_shutdown(seconds):
        return True

    async def fake_get_pubsub_subscription_qids(session_id):
        return ["Q42"]

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "SHUTDOWN_EVENT", None)
    monkeypatch.setattr(app_module, "WEB_INTEREST_MANAGER", None)
    monkeypatch.setattr(app_module, "_get_pubsub_subscription_qids", fake_get_pubsub_subscription_qids)
    monkeypatch.setattr(routes_api, "web_resolve_creation_metadata", fake_resolve_creation_metadata)
    monkeypatch.setattr(app_module, "_sleep_or_shutdown", fake_sleep_or_shutdown)

    messages = [
        message
        async for message in routes_api._pubsub_event_stream(
            "gadget",
            "sub-1",
            FakeRequest(),
            after_event_id=0,
        )
    ]

    assert messages[0] == 'data: {"event": "primed", "owner_id": "gadget", "session_id": "sub-1", "qid_count": 1}\n\n'
    assert messages[1] == 'data: {"event": "primed_count", "owner_id": "gadget", "session_id": "sub-1", "qid_count": 1}\n\n'
    assert any(message.startswith('data: {"event": "summary_change"') for message in messages)
    assert "create_interest_lease:gadget:sub-1" in calls
    assert "create_interest_manager:web" not in calls
    summary_change = next(message for message in messages if '"event": "summary_change"' in message)
    payload = json.loads(summary_change.removeprefix("data: ").strip())
    assert "n3_wikisub" not in payload
    assert "n3_inlinks" not in payload
    assert payload["levels"]["N3_wikisub"] == "unknown"
    assert '"content_stale": true' in "".join(messages)


@pytest.mark.asyncio
async def test_creator_dashboard_event_stream_emits_initial_snapshot(monkeypatch):
    class FakePubsub:
        async def create_pubsub_lease(self, **kwargs):
            return None

        async def refresh_pubsub_lease(self, **kwargs):
            return None

    class FakeCache:
        pubsub = FakePubsub()

        async def get_user_history(self, username):
            return SimpleNamespace(
                username=username,
                window_start="2026-07-01T00:00:00Z",
                window_end="2026-07-02T00:00:00Z",
                requested_at=123,
                started_at=124,
                finished_at=None,
                last_refresh_at=125,
                error_text=None,
                row_count=1,
            )

        async def list_creation_metadata(self, *, start, end, creator_actor_ids):
            return [SimpleNamespace(qid="Q42", creation_time=123)]

        async def get_many(self, qids):
            return {
                qid: fake_result
                for qid in qids
            }

    fake_result = SimpleNamespace(
        levels_str={
            "N": "none",
            "N1": "none",
            "N2a": "none",
            "N2b": "none",
            "N3": "none",
            "N3_inlinks": "none",
            "N3_osm": "unknown",
            "N3_wikisub": "unknown",
            "N3_sdc": "unknown",
        },
        n1="none",
        n2a="none",
        n2b="none",
        n3_inlinks="none",
        n3_osm="unknown",
        n3_wikisub="unknown",
        n3_sdc="unknown",
        is_redirect=False,
        has_claims_count=1,
        has_sitelinks_count=1,
        inlinks_count=1,
        is_deleted=False,
        redirect_target=None,
        content_last_revid=123,
        recent_changes_last_revid=456,
    )

    async def fake_resolve_creation_metadata(qids):
        return {qid: {"creator": "Alice", "creation_time": 123} for qid in qids}

    async def fake_sleep_or_shutdown(seconds):
        return True

    monkeypatch.setattr(app_module, "CACHE", FakeCache())
    monkeypatch.setattr(app_module, "SHUTDOWN_EVENT", None)
    monkeypatch.setattr(app_module.CREATIONS, "lookup_actor_ids", lambda creators: {"alice": 1})
    monkeypatch.setattr(routes_api, "web_resolve_creation_metadata", fake_resolve_creation_metadata)
    monkeypatch.setattr(app_module, "_sleep_or_shutdown", fake_sleep_or_shutdown)

    messages = [
        message
        async for message in routes_api._creator_dashboard_event_stream(
            "alice",
            FakeRequest(),
            poll_seconds=0.1,
        )
    ]

    assert messages[0] == 'data: {"event": "history_update", "username": "alice", "history": {"username": "alice", "window_start": "2026-07-01T00:00:00Z", "window_end": "2026-07-02T00:00:00Z", "requested_at": 123, "started_at": 124, "finished_at": null, "last_refresh_at": 125, "error_text": null, "row_count": 1}}\n\n'
    assert any(message.startswith('data: {"event": "update"') for message in messages)


@pytest.mark.asyncio
async def test_subscribe_queues_incomplete_cached_items(monkeypatch):
    calls: list[tuple[str, list[str]]] = []

    class FakeRequest:
        headers = {"content-type": "application/json"}

        async def json(self):
            return {"items": [{"qid": "Q42", "reason": "page"}]}

        async def body(self):
            return b""

    async def fake_store(subscription_id, qids):
        calls.append((subscription_id, list(qids)))

    monkeypatch.setattr(app_module, "_store_pubsub_subscription_qids", fake_store)

    payload = await routes_api.api_subscribe(FakeRequest())

    assert payload["subscription_id"]
    assert payload["reevaluate"] is True
    assert calls == [(payload["subscription_id"], ["Q42"])]


@pytest.mark.asyncio
async def test_subscribe_includes_complete_cached_items_in_subscription(monkeypatch):
    calls: list[tuple[str, list[str]]] = []

    class FakeRequest:
        headers = {"content-type": "application/json"}

        async def json(self):
            return {"items": [{"qid": "Q42", "reason": "page"}], "session_id": "sub-1"}

        async def body(self):
            return b""

    async def fake_store(subscription_id, qids):
        calls.append((subscription_id, list(qids)))

    monkeypatch.setattr(app_module, "_store_pubsub_subscription_qids", fake_store)

    payload = await routes_api.api_subscribe(FakeRequest())

    assert payload["subscription_id"] == "sub-1"
    assert payload["reevaluate"] is True
    assert calls == [("sub-1", ["Q42"])]


@pytest.mark.asyncio
async def test_subscribe_uses_batch_enqueue_when_available(monkeypatch):
    calls: list[tuple[str, list[str]]] = []

    class FakeRequest:
        headers = {"content-type": "application/json"}

        async def json(self):
            return {"items": [{"qid": "Q42", "reason": "page"}, {"qid": "Q99", "reason": "create"}]}

        async def body(self):
            return b""

    async def fake_store(subscription_id, qids):
        calls.append((subscription_id, list(qids)))

    monkeypatch.setattr(app_module, "_store_pubsub_subscription_qids", fake_store)

    payload = await routes_api.api_subscribe(FakeRequest())

    assert payload["subscription_id"]
    assert payload["reevaluate"] is True
    assert calls == [(payload["subscription_id"], ["Q42", "Q99"])]


@pytest.mark.asyncio
async def test_event_stream_exits_for_disconnected_request(monkeypatch):
    class FakeCache:
        async def get_many(self, qids):
            raise AssertionError("disconnected streams should not read the cache")

        async def get_creation_metadata_many(self, qids):
            raise AssertionError("disconnected streams should not read metadata")

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

        async def get_creation_metadata_many(self, qids):
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

        async def get_creation_metadata_many(self, qids):
            raise AssertionError("expired streams should not read metadata")

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

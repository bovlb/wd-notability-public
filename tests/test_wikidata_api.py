from __future__ import annotations

import httpx
import pytest

from wd_notability.wikidata_api import WikidataAuthConfig, WikidataSession


class NullLimiter:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class FakeClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, str, dict | None, dict | None]] = []
        self.is_closed = False

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
    ) -> httpx.Response:
        self.requests.append((method, url, params, data))
        return self.responses.pop(0)

    async def aclose(self) -> None:
        self.is_closed = True


def response(status_code: int, *, headers: dict[str, str] | None = None, json_data: object | None = None) -> httpx.Response:
    request = httpx.Request("GET", "https://www.wikidata.org/w/api.php")
    if json_data is None:
        return httpx.Response(status_code=status_code, headers=headers or {}, request=request)
    return httpx.Response(status_code=status_code, headers=headers or {}, json=json_data, request=request)


@pytest.mark.asyncio
async def test_wikidata_session_logs_in_once_and_reuses_cookie_session(monkeypatch):
    client = FakeClient(
        [
            response(200, json_data={"query": {"tokens": {"logintoken": "TOKEN"}}}),
            response(200, json_data={"clientlogin": {"status": "PASS"}}),
            response(200, json_data={"query": {"backlinks": []}}),
            response(200, json_data={"query": {"backlinks": []}}),
        ]
    )
    session = WikidataSession(
        auth=WikidataAuthConfig(
            enabled=True,
            username="ExampleBot",
            bot_password="secret",
        ),
        client_factory=lambda **kwargs: client,
    )
    monkeypatch.setattr("wd_notability.wikidata_api.WIKIDATA_ACTION_API_LIMITER", NullLimiter())

    response_1, timings_1 = await session.get_with_timings(
        "https://www.wikidata.org/w/api.php",
        limiter=NullLimiter(),
        params={"action": "query"},
    )
    response_2, timings_2 = await session.get_with_timings(
        "https://www.wikidata.org/w/api.php",
        limiter=NullLimiter(),
        params={"action": "query"},
    )

    assert response_1.status_code == 200
    assert response_2.status_code == 200
    assert len(client.requests) == 4
    assert client.requests[0][0] == "GET"
    assert client.requests[1][0] == "POST"
    assert client.requests[2][0] == "GET"
    assert client.requests[3][0] == "GET"
    assert timings_1.query_seconds >= 0
    assert timings_1.limiter_wait_seconds >= 0
    assert timings_1.retry_wait_seconds == 0
    assert timings_2.retry_wait_seconds == 0


@pytest.mark.asyncio
async def test_wikidata_session_retries_429_after_login(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("wd_notability.wikidata_api.asyncio.sleep", fake_sleep)

    client = FakeClient(
        [
            response(200, json_data={"query": {"tokens": {"logintoken": "TOKEN"}}}),
            response(200, json_data={"clientlogin": {"status": "PASS"}}),
            response(429, headers={"Retry-After": "2"}),
            response(200, json_data={"query": {"backlinks": []}}),
        ]
    )
    session = WikidataSession(
        auth=WikidataAuthConfig(
            enabled=True,
            username="ExampleBot",
            bot_password="secret",
        ),
        client_factory=lambda **kwargs: client,
    )
    monkeypatch.setattr("wd_notability.wikidata_api.WIKIDATA_ACTION_API_LIMITER", NullLimiter())

    response_1, timings = await session.get_with_timings(
        "https://www.wikidata.org/w/api.php",
        limiter=NullLimiter(),
        params={"action": "query"},
    )

    assert response_1.status_code == 200
    assert sleeps == [2.0]
    assert timings.retry_wait_seconds == 2.0
    assert len(client.requests) == 4


@pytest.mark.asyncio
async def test_wikidata_session_requires_credentials_when_login_enabled(monkeypatch):
    session = WikidataSession(
        auth=WikidataAuthConfig(enabled=True, username=None, bot_password=None),
        client_factory=lambda **kwargs: FakeClient([]),
    )
    monkeypatch.setattr("wd_notability.wikidata_api.WIKIDATA_ACTION_API_LIMITER", NullLimiter())

    with pytest.raises(RuntimeError, match="must both be set"):
        await session.get_with_timings(
            "https://www.wikidata.org/w/api.php",
            limiter=NullLimiter(),
            params={"action": "query"},
        )

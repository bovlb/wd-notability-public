from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
import pytest

from wd_notability.http_client import (
    BACKGROUND_PRIORITY,
    FOREGROUND_PRIORITY,
    foreground_http_requests,
    limited_get_with_retries,
    retry_after_seconds,
)


class NullLimiter:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class RecordingPriorityLimiter:
    def __init__(self) -> None:
        self.priorities: list[int] = []

    @asynccontextmanager
    async def limit(self, *, priority: int):
        self.priorities.append(priority)
        yield


class FakeClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict | None]] = []

    async def get(self, url: str, params: dict | None = None) -> httpx.Response:
        self.calls.append((url, params))
        return self.responses.pop(0)


def response(status_code: int, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status_code=status_code, headers=headers or {})


def test_retry_after_seconds_reads_delta_seconds() -> None:
    assert retry_after_seconds(response(429, {"Retry-After": "12"})) == 12


@pytest.mark.asyncio
async def test_limited_get_with_retries_honors_retry_after(monkeypatch) -> None:
    sleeps = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("wd_notability.http_client.asyncio.sleep", fake_sleep)
    client = FakeClient(
        [
            response(429, {"Retry-After": "7"}),
            response(200),
        ]
    )

    result = await limited_get_with_retries(
        client,
        "https://example.test/api",
        limiter=NullLimiter(),
        params={"q": "Q42"},
    )

    assert result.status_code == 200
    assert sleeps == [7]
    assert client.calls == [
        ("https://example.test/api", {"q": "Q42"}),
        ("https://example.test/api", {"q": "Q42"}),
    ]


@pytest.mark.asyncio
async def test_limited_get_with_retries_uses_exponential_backoff_without_retry_after(monkeypatch) -> None:
    sleeps = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("wd_notability.http_client.asyncio.sleep", fake_sleep)
    client = FakeClient(
        [
            response(429),
            response(429),
            response(200),
        ]
    )

    result = await limited_get_with_retries(
        client,
        "https://example.test/api",
        limiter=NullLimiter(),
    )

    assert result.status_code == 200
    assert sleeps == [1.0, 2.0]


@pytest.mark.asyncio
async def test_limited_get_with_retries_returns_last_429_after_max_attempts(monkeypatch) -> None:
    sleeps = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("wd_notability.http_client.asyncio.sleep", fake_sleep)
    client = FakeClient([response(429), response(429)])

    result = await limited_get_with_retries(
        client,
        "https://example.test/api",
        limiter=NullLimiter(),
        max_attempts=2,
    )

    assert result.status_code == 429
    assert sleeps == [1.0]
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_limited_get_with_retries_uses_background_priority_by_default() -> None:
    limiter = RecordingPriorityLimiter()
    client = FakeClient([response(200)])

    await limited_get_with_retries(client, "https://example.test/api", limiter=limiter)

    assert limiter.priorities == [BACKGROUND_PRIORITY]


@pytest.mark.asyncio
async def test_limited_get_with_retries_uses_foreground_priority_in_context() -> None:
    limiter = RecordingPriorityLimiter()
    client = FakeClient([response(200)])

    with foreground_http_requests():
        await limited_get_with_retries(client, "https://example.test/api", limiter=limiter)

    assert limiter.priorities == [FOREGROUND_PRIORITY]

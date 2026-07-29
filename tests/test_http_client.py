from __future__ import annotations

import httpx
import pytest

from wd_notability.http_client import (
    limited_get_with_retries,
    retry_after_seconds,
)


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
        max_attempts=2,
    )

    assert result.status_code == 429
    assert sleeps == [1.0]
    assert len(client.calls) == 2

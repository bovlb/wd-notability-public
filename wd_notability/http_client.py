from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_MIN_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 16.0


def retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None

    try:
        delay = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        delay = (retry_at - datetime.now(UTC)).total_seconds()

    if delay < 0:
        return None
    return delay


def exponential_backoff_seconds(
    attempt_index: int,
    *,
    minimum: float = DEFAULT_BACKOFF_MIN_SECONDS,
    maximum: float = DEFAULT_BACKOFF_MAX_SECONDS,
) -> float:
    return min(maximum, max(minimum, minimum * (2 ** attempt_index)))


async def limited_get_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> httpx.Response:
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt_index in range(max_attempts):
        response = await client.get(url, params=params)

        if response.status_code != 429 or attempt_index == max_attempts - 1:
            return response

        delay = retry_after_seconds(response)
        if delay is None:
            delay = exponential_backoff_seconds(attempt_index)
        await asyncio.sleep(delay)

    raise RuntimeError("unreachable")

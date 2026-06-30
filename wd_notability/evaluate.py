from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from wd_notability.http_client import foreground_http_requests

_foreground_condition = asyncio.Condition()
_foreground_evaluations = 0


@asynccontextmanager
async def foreground_evaluation() -> AsyncGenerator[None, None]:
    global _foreground_evaluations

    async with _foreground_condition:
        _foreground_evaluations += 1
        _foreground_condition.notify_all()

    try:
        with foreground_http_requests():
            yield
    finally:
        async with _foreground_condition:
            _foreground_evaluations -= 1
            _foreground_condition.notify_all()


async def wait_for_foreground_evaluations() -> None:
    async with _foreground_condition:
        while _foreground_evaluations > 0:
            await _foreground_condition.wait()

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from wd_notability.evaluate_batch import evaluate_many
from wd_notability.http_client import foreground_http_requests
from wd_notability.models import (
    EvaluationResult,
    NotabilityLevel,
    QID,
)
from wd_notability.sources import SOURCES

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


async def evaluate_full(qid: QID, *, parallel: bool = False) -> EvaluationResult:
    print(f"Evaluating {qid} fully...")
    return (
        await evaluate_many(
            [qid],
            sources=SOURCES,
            stop_on_strong=False,
            update_cache=True,
            possible_sources=SOURCES,
        )
    )[qid]

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from wd_notability.evaluation_cache import CACHE
from wd_notability.file_lock import acquire_file_lock

CACHE_OBSERVABILITY_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[1] / "data" / "cache_observability_worker"
CACHE_OBSERVABILITY_WORKER_RUN_INTERVAL_SECONDS = 60.0
CACHE_OBSERVABILITY_SAMPLE_SECONDS = 60.0
CACHE_OBSERVABILITY_LOCK = asyncio.Lock()
CACHE_OBSERVABILITY_LAST_EMITTED = 0.0


async def _cache_observability_snapshot() -> dict[str, Any]:
    breakdown = await CACHE.breakdown()
    return {
        "items": {
            "total": breakdown["entries"],
        },
        "flags": breakdown["flags"],
        "criteria": {
            "detected": breakdown["criteria_detected"],
            "deduced": breakdown["criteria_deduced"],
        },
    }


async def _emit_cache_observability() -> None:
    global CACHE_OBSERVABILITY_LAST_EMITTED

    async with CACHE_OBSERVABILITY_LOCK:
        now = time.monotonic()
        if now - CACHE_OBSERVABILITY_LAST_EMITTED < CACHE_OBSERVABILITY_SAMPLE_SECONDS:
            return
        CACHE_OBSERVABILITY_LAST_EMITTED = now

    try:
        await CACHE.observability.record_worker_snapshot(
            worker_name="cache",
            data=await _cache_observability_snapshot(),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Cache observability emit failed: {exc}")


async def cache_observability_worker_loop(
    *,
    run_interval_seconds: float = CACHE_OBSERVABILITY_WORKER_RUN_INTERVAL_SECONDS,
) -> None:
    with acquire_file_lock(CACHE_OBSERVABILITY_WORKER_LOCK_TARGET):
        while True:
            run_started = time.monotonic()
            try:
                await _emit_cache_observability()
            except Exception as exc:  # noqa: BLE001
                print(f"Cache observability worker failed: {exc}")

            sleep_for = max(0.0, run_interval_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)

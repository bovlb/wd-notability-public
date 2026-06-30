import asyncio

import pytest

from wd_notability import evaluate as evaluate_module


@pytest.mark.asyncio
async def test_foreground_evaluation_blocks_worker_waits():
    blocked = asyncio.Event()
    released = asyncio.Event()

    async def waiter():
        blocked.set()
        await evaluate_module.wait_for_foreground_evaluations()
        released.set()

    task = asyncio.create_task(waiter())
    await blocked.wait()
    await asyncio.sleep(0)
    assert not released.is_set()

    async with evaluate_module.foreground_evaluation():
        await asyncio.sleep(0)
        assert not released.is_set()

    await asyncio.wait_for(released.wait(), timeout=1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

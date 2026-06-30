from __future__ import annotations

import asyncio
import heapq
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from aiolimiter import AsyncLimiter


class PriorityAsyncLimiter:
    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        self._limiter = AsyncLimiter(max_rate, time_period)
        self._condition = asyncio.Condition()
        self._queue: list[tuple[int, int, asyncio.Future[None]]] = []
        self._ticket = 0
        self._acquiring = False

    @asynccontextmanager
    async def limit(self, *, priority: int = 1) -> AsyncGenerator[None, None]:
        await self.acquire(priority=priority)
        yield

    async def acquire(self, *, priority: int = 1) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        popped = False

        async with self._condition:
            ticket = self._ticket
            self._ticket += 1
            heapq.heappush(self._queue, (priority, ticket, future))
            self._condition.notify_all()

            try:
                while True:
                    if (
                        not self._acquiring
                        and self._queue
                        and self._queue[0][2] is future
                    ):
                        self._acquiring = True
                        heapq.heappop(self._queue)
                        popped = True
                        break
                    await self._condition.wait()
            except BaseException:
                if not popped:
                    self._queue = [
                        entry for entry in self._queue
                        if entry[2] is not future
                    ]
                    heapq.heapify(self._queue)
                    self._condition.notify_all()
                raise

        try:
            await self._limiter.acquire()
        finally:
            async with self._condition:
                self._acquiring = False
                self._condition.notify_all()


# Shared per-API rate limiters for outbound requests.
# Limits are intentionally conservative and can be tuned later.
#WIKIDATA_ENTITYDATA_LIMITER = PriorityAsyncLimiter(3, 1)
WIKIDATA_ACTION_API_LIMITER = PriorityAsyncLimiter(10, 1)
COMMONS_ACTION_API_LIMITER = PriorityAsyncLimiter(3, 1)
TAGINFO_API_LIMITER = PriorityAsyncLimiter(1, 1)
WIKIDATA_SPARQL_LIMITER = PriorityAsyncLimiter(1, 1)
WIKIMEDIA_SITEINFO_LIMITER = PriorityAsyncLimiter(2, 1)

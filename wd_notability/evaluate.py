from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator


@asynccontextmanager
async def foreground_evaluation() -> AsyncGenerator[None, None]:
    yield

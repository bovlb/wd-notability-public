from typing import Any

import httpx
from aiolimiter import AsyncLimiter

from wd_notability.http_client import limited_get_with_retries
from wd_notability.models import Source


class HttpSource(Source):
    async def limited_get(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        limiter: AsyncLimiter,
        params: dict[str, Any],
    ) -> httpx.Response:
        return await limited_get_with_retries(client, url, limiter=limiter, params=params)

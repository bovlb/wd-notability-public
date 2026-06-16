from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Callable

import httpx

from wd_notability.api_backoff import get_retry_after_remaining, set_retry_after_seconds
from wd_notability.async_limiters import WIKIDATA_ACTION_API_LIMITER
from wd_notability.http_client import current_http_priority, exponential_backoff_seconds, retry_after_seconds

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
USER_AGENT = "wd-notability/1.0 (contact:User:Bovlb)"
DEFAULT_LOGIN_RETURN_URL = "https://www.wikidata.org/"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class RequestTimings:
    query_seconds: float = 0.0
    limiter_wait_seconds: float = 0.0
    retry_wait_seconds: float = 0.0

    def as_dict(self, prefix: str) -> dict[str, float]:
        return {
            f"{prefix}_query": self.query_seconds,
            f"{prefix}_limiter_wait": self.limiter_wait_seconds,
            f"{prefix}_retry_wait": self.retry_wait_seconds,
        }


class WikidataBackoffActiveError(RuntimeError):
    def __init__(self, remaining_seconds: float) -> None:
        super().__init__(f"Wikidata action API backoff active for {remaining_seconds:.1f}s")
        self.remaining_seconds = max(0.0, remaining_seconds)


class WikidataRetryAfterError(RuntimeError):
    def __init__(self, delay_seconds: float, response: httpx.Response) -> None:
        super().__init__(f"Wikidata action API returned 429 Retry-After={delay_seconds:.1f}s")
        self.delay_seconds = max(0.0, delay_seconds)
        self.response = response


@dataclass(slots=True, frozen=True)
class WikidataAuthConfig:
    enabled: bool
    username: str | None
    bot_password: str | None
    login_return_url: str = DEFAULT_LOGIN_RETURN_URL

    @classmethod
    def from_env(cls) -> "WikidataAuthConfig":
        return cls(
            enabled=_env_flag("WD_NOTABILITY_WIKIDATA_LOGIN_ENABLED", default=False),
            username=os.getenv("WD_NOTABILITY_WIKIDATA_USERNAME"),
            bot_password=os.getenv("WD_NOTABILITY_WIKIDATA_BOTPASSWORD"),
            login_return_url=os.getenv("WD_NOTABILITY_WIKIDATA_LOGIN_RETURN_URL", DEFAULT_LOGIN_RETURN_URL),
        )


class WikidataSession:
    def __init__(
        self,
        *,
        auth: WikidataAuthConfig | None = None,
        client_factory: Callable[..., httpx.AsyncClient] = httpx.AsyncClient,
    ) -> None:
        self._auth = auth or WikidataAuthConfig.from_env()
        self._client_factory = client_factory
        self._client: httpx.AsyncClient | None = None
        self._login_lock = asyncio.Lock()
        self._logged_in = False

    async def close(self) -> None:
        client = self._client
        if client is None or client.is_closed:
            self._client = None
            self._logged_in = False
            return

        await client.aclose()
        self._client = None
        self._logged_in = False

    async def _ensure_client(self) -> httpx.AsyncClient:
        client = self._client
        if client is not None and not client.is_closed:
            return client

        self._client = self._client_factory(
            timeout=30,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        self._logged_in = False
        return self._client

    async def _request_with_retries(
        self,
        method: str,
        url: str,
        *,
        limiter,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        max_attempts: int = 5,
    ) -> tuple[httpx.Response, RequestTimings]:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        client = await self._ensure_client()
        timings = RequestTimings()

        for attempt_index in range(max_attempts):
            remaining_backoff = get_retry_after_remaining()
            if remaining_backoff > 0:
                print(f"Wikidata request abandoned due to shared backoff: {remaining_backoff:.1f}s remaining")
                raise WikidataBackoffActiveError(remaining_backoff)

            if hasattr(limiter, "limit"):
                limit_context = limiter.limit(priority=current_http_priority())
            else:
                limit_context = limiter

            limiter_wait_start = perf_counter()
            async with limit_context:
                timings.limiter_wait_seconds += perf_counter() - limiter_wait_start
                query_start = perf_counter()
                try:
                    response = await client.request(method, url, params=params, data=data)
                except httpx.TransportError:
                    timings.query_seconds += perf_counter() - query_start
                    if attempt_index == max_attempts - 1:
                        raise
                    delay = exponential_backoff_seconds(attempt_index)
                    timings.retry_wait_seconds += delay
                    await asyncio.sleep(delay)
                    continue

            timings.query_seconds += perf_counter() - query_start
            if response.status_code != 429 or attempt_index == max_attempts - 1:
                return response, timings

            delay = retry_after_seconds(response)
            if delay is None:
                delay = exponential_backoff_seconds(attempt_index)
            timings.retry_wait_seconds += delay
            set_retry_after_seconds(delay, reason="429")
            print(f"Wikidata request abandoned after 429: Retry-After={delay:.1f}s")
            raise WikidataRetryAfterError(delay, response)

        raise RuntimeError("unreachable")

    async def _ensure_logged_in(self) -> None:
        if not self._auth.enabled:
            return

        if self._logged_in:
            return

        async with self._login_lock:
            if self._logged_in:
                return
            await self._log_in()
            self._logged_in = True

    async def _log_in(self) -> None:
        if not self._auth.username or not self._auth.bot_password:
            raise RuntimeError(
                "Wikidata login is enabled but WD_NOTABILITY_WIKIDATA_USERNAME and "
                "WD_NOTABILITY_WIKIDATA_BOTPASSWORD must both be set"
            )

        token_response, _ = await self._request_with_retries(
            "GET",
            WIKIDATA_API_URL,
            limiter=WIKIDATA_ACTION_API_LIMITER,
            params={
                "action": "query",
                "meta": "tokens",
                "type": "login",
                "format": "json",
            },
        )
        token_response.raise_for_status()
        token_payload = token_response.json()
        login_token = token_payload.get("query", {}).get("tokens", {}).get("logintoken")
        if not isinstance(login_token, str) or not login_token:
            raise RuntimeError(f"Failed to fetch Wikidata login token: {token_payload}")

        login_response, _ = await self._request_with_retries(
            "POST",
            WIKIDATA_API_URL,
            limiter=WIKIDATA_ACTION_API_LIMITER,
            data={
                "action": "clientlogin",
                "username": self._auth.username,
                "password": self._auth.bot_password,
                "loginreturnurl": self._auth.login_return_url,
                "logintoken": login_token,
                "format": "json",
            },
        )
        login_response.raise_for_status()
        login_payload = login_response.json()
        login_result = login_payload.get("clientlogin", {})
        status = None
        if isinstance(login_result, dict):
            status = login_result.get("status") or login_result.get("result")

        if status != "PASS":
            raise RuntimeError(f"Wikidata login failed: {login_payload}")

    async def request_with_timings(
        self,
        method: str,
        url: str,
        *,
        limiter,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        max_attempts: int = 5,
    ) -> tuple[httpx.Response, RequestTimings]:
        await self._ensure_logged_in()
        return await self._request_with_retries(
            method,
            url,
            limiter=limiter,
            params=params,
            data=data,
            max_attempts=max_attempts,
        )

    async def get_with_timings(
        self,
        url: str,
        *,
        limiter,
        params: dict[str, Any] | None = None,
        max_attempts: int = 5,
    ) -> tuple[httpx.Response, RequestTimings]:
        return await self.request_with_timings(
            "GET",
            url,
            limiter=limiter,
            params=params,
            max_attempts=max_attempts,
        )

    async def get(
        self,
        url: str,
        *,
        limiter,
        params: dict[str, Any] | None = None,
        max_attempts: int = 5,
    ) -> httpx.Response:
        response, _ = await self.get_with_timings(
            url,
            limiter=limiter,
            params=params,
            max_attempts=max_attempts,
        )
        return response


wikidata_session = WikidataSession()


async def close_wikidata_session() -> None:
    await wikidata_session.close()

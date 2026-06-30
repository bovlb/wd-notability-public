from __future__ import annotations

import bz2
from pathlib import Path

import pytest

import wd_notability.external_usage.sdc.builder as build_sdc_cache_module


class _FakeResponse:
    def __init__(self, chunks: list[bytes], headers: dict[str, str] | None = None):
        self._chunks = chunks
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        return None

    async def aiter_bytes(self):
        for chunk in self._chunks:
            yield chunk


class _FakeStream:
    def __init__(self, response: _FakeResponse):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeClient:
    def __init__(self, chunks: list[bytes], headers: dict[str, str] | None = None):
        self._stream_response = _FakeResponse(chunks, headers=headers)
        self._head_response = _FakeResponse([], headers=headers or {})

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def stream(self, method: str, url: str):
        return _FakeStream(self._stream_response)

    async def head(self, url: str, follow_redirects: bool = True):
        return self._head_response


@pytest.mark.asyncio
async def test_build_sdc_cache_replaces_lookup_rows(monkeypatch, tmp_path):
    ttl = "wd:Q1 wd:Q2\nwd:Q2 wd:Q3\n"
    compressed = bz2.compress(ttl.encode("utf-8"))

    class FakeCache:
        def __init__(self, output: Path):
            self.output = output
            self.replace_called = None

        def replace_sdc_usage(self, sdc_usage_by_qid):
            self.replace_called = sdc_usage_by_qid

    refresh_calls = []

    async def fake_refresh_cache(self, cache, usage_by_qid):
        refresh_calls.append((cache, dict(usage_by_qid)))
        return len(usage_by_qid)

    fake_cache = FakeCache(tmp_path / "lookup_cache.db")
    monkeypatch.setattr(build_sdc_cache_module.httpx, "AsyncClient", lambda *args, **kwargs: _FakeClient([compressed]))
    monkeypatch.setattr(build_sdc_cache_module, "LookupCache", lambda output: fake_cache)
    monkeypatch.setattr(type(build_sdc_cache_module.SDC_SOURCE), "refresh_cache", fake_refresh_cache)

    await build_sdc_cache_module.build_sdc_cache(
        tmp_path / "lookup_cache.db",
        dump_url="https://example.invalid/dump.ttl.bz2",
        progress=False,
    )

    assert fake_cache.replace_called == {"Q1": 1, "Q2": 2, "Q3": 1}
    assert refresh_calls == [
        (fake_cache, {"Q1": 1, "Q2": 2, "Q3": 1}),
    ]


@pytest.mark.asyncio
async def test_build_sdc_cache_updates_progress_bar(monkeypatch, tmp_path):
    ttl = "wd:Q1 wd:Q2\n"
    compressed = bz2.compress(ttl.encode("utf-8"))
    content_length = str(len(compressed))

    class FakeProgressBar:
        def __init__(self, total):
            self.total = total
            self.updates = []
            self.closed = False

        def update(self, amount):
            self.updates.append(amount)

        def close(self):
            self.closed = True

    class FakeCache:
        def __init__(self, output: Path):
            self.output = output

        def replace_sdc_usage(self, sdc_usage_by_qid):
            self.replace_called = sdc_usage_by_qid

        def set_lookup_state(self, key, value):
            self.lookup_state = (key, value)

    refresh_calls = []
    progress_bars = []

    async def fake_refresh_cache(self, cache, usage_by_qid):
        refresh_calls.append((cache, dict(usage_by_qid)))
        return len(usage_by_qid)

    def fake_progress_bar(total):
        bar = FakeProgressBar(total)
        progress_bars.append(bar)
        return bar

    fake_cache = FakeCache(tmp_path / "lookup_cache.db")
    monkeypatch.setattr(
        build_sdc_cache_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _FakeClient([compressed], headers={"Content-Length": content_length}),
    )
    monkeypatch.setattr(build_sdc_cache_module, "LookupCache", lambda output: fake_cache)
    monkeypatch.setattr(type(build_sdc_cache_module.SDC_SOURCE), "refresh_cache", fake_refresh_cache)
    monkeypatch.setattr(build_sdc_cache_module, "_make_progress_bar", fake_progress_bar)

    await build_sdc_cache_module.build_sdc_cache(
        tmp_path / "lookup_cache.db",
        dump_url="https://example.invalid/dump.ttl.bz2",
        progress=True,
    )

    assert progress_bars and progress_bars[0].total == len(compressed)
    assert progress_bars[0].updates == [len(compressed)]
    assert progress_bars[0].closed is True
    assert fake_cache.replace_called == {"Q1": 1, "Q2": 1}
    assert refresh_calls == [
        (fake_cache, {"Q1": 1, "Q2": 1}),
    ]

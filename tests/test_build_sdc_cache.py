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

    class FakeCursor:
        def __init__(self, db):
            self.db = db
            self.executed = []
            self.mode = None

        def execute(self, sql, params=None):
            self.executed.append(sql.strip())
            if sql.strip().startswith("SELECT COUNT(*) FROM temp_sdc_usage"):
                self.mode = "count"

        def executemany(self, sql, params):
            self.executed.append(sql.strip())
            for qid, usage_count in params:
                self.db.temp_rows[qid] = self.db.temp_rows.get(qid, 0) + usage_count

        def fetchone(self):
            if self.mode == "count":
                return (len(self.db.temp_rows),)
            return None

    class FakeDB:
        def __init__(self):
            self.temp_rows = {}
            self.commit_calls = 0
            self.rollback_calls = 0
            self.cursor_obj = FakeCursor(self)

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commit_calls += 1

        def rollback(self):
            self.rollback_calls += 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeCache:
        def __init__(self, backend):
            self.initialized = False

        def initialize(self):
            self.initialized = True

    class FakeBackend:
        def __init__(self):
            self.lookup_state = None
            self.db = FakeDB()

        def get_lookup_state(self, key):
            return self.lookup_state

        def set_lookup_state(self, key, value):
            self.lookup_state = (key, value)

        def _connect(self):
            return self.db

    fake_backend = FakeBackend()
    monkeypatch.setattr(
        build_sdc_cache_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _FakeClient(
            [compressed],
            headers={"Last-Modified": "Tue, 01 Jul 2026 00:00:00 GMT"},
        ),
    )
    monkeypatch.setattr(build_sdc_cache_module, "create_lookup_backend", lambda: fake_backend)
    monkeypatch.setattr(build_sdc_cache_module, "LookupCache", FakeCache)

    await build_sdc_cache_module.build_sdc_cache(
        tmp_path,
        dump_url="https://example.invalid/dump.ttl.bz2",
        progress=False,
    )

    assert fake_backend.db.temp_rows == {1: 1, 2: 2, 3: 1}
    assert fake_backend.db.commit_calls == 2
    assert fake_backend.db.rollback_calls == 0
    assert fake_backend.lookup_state == ("sdc_dump_last_modified", "2026-07-01T00:00:00+00:00")
    assert fake_backend.db.cursor_obj.executed == [
        "DROP TEMPORARY TABLE IF EXISTS temp_sdc_usage",
        """
                    CREATE TEMPORARY TABLE temp_sdc_usage (
                        qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                        usage_count BIGINT NOT NULL DEFAULT 0
                    )
                    """.strip(),
        """
                                INSERT INTO temp_sdc_usage (qid, usage_count)
                                VALUES (%s, %s)
                                ON DUPLICATE KEY UPDATE
                                    usage_count = usage_count + VALUES(usage_count)
                                """.strip(),
        "SELECT COUNT(*) FROM temp_sdc_usage",
        "START TRANSACTION",
        "DELETE FROM sdc_usage",
        """
                        INSERT INTO sdc_usage (qid, usage_count)
                        SELECT qid, usage_count
                        FROM temp_sdc_usage
                        ORDER BY qid
                        """.strip(),
        "DROP TEMPORARY TABLE IF EXISTS temp_sdc_usage",
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

    class FakeCursor:
        def __init__(self, db):
            self.db = db
            self.executed = []
            self.mode = None

        def execute(self, sql, params=None):
            self.executed.append(sql.strip())
            if sql.strip().startswith("SELECT COUNT(*) FROM temp_sdc_usage"):
                self.mode = "count"

        def executemany(self, sql, params):
            self.executed.append(sql.strip())
            for qid, usage_count in params:
                self.db.temp_rows[qid] = self.db.temp_rows.get(qid, 0) + usage_count

        def fetchone(self):
            if self.mode == "count":
                return (len(self.db.temp_rows),)
            return None

    class FakeDB:
        def __init__(self):
            self.temp_rows = {}
            self.commit_calls = 0
            self.rollback_calls = 0
            self.cursor_obj = FakeCursor(self)

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commit_calls += 1

        def rollback(self):
            self.rollback_calls += 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeCache:
        def __init__(self, backend):
            self.initialized = False

        def initialize(self):
            self.initialized = True

    class FakeBackend:
        def __init__(self):
            self.lookup_state = None
            self.db = FakeDB()

        def get_lookup_state(self, key):
            return self.lookup_state

        def set_lookup_state(self, key, value):
            self.lookup_state = (key, value)

        def _connect(self):
            return self.db

    progress_bars = []

    def fake_progress_bar(total):
        bar = FakeProgressBar(total)
        progress_bars.append(bar)
        return bar

    fake_cache = FakeCache(tmp_path)
    fake_backend = FakeBackend()
    monkeypatch.setattr(
        build_sdc_cache_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: _FakeClient(
            [compressed],
            headers={
                "Content-Length": content_length,
                "Last-Modified": "Tue, 01 Jul 2026 00:00:00 GMT",
            },
        ),
    )
    monkeypatch.setattr(build_sdc_cache_module, "create_lookup_backend", lambda: fake_backend)
    monkeypatch.setattr(build_sdc_cache_module, "LookupCache", FakeCache)
    monkeypatch.setattr(build_sdc_cache_module, "_make_progress_bar", fake_progress_bar)

    await build_sdc_cache_module.build_sdc_cache(
        tmp_path,
        dump_url="https://example.invalid/dump.ttl.bz2",
        progress=True,
    )

    assert progress_bars and progress_bars[0].total == len(compressed)
    assert progress_bars[0].updates == [len(compressed)]
    assert progress_bars[0].closed is True
    assert fake_backend.db.temp_rows == {1: 1, 2: 1}
    assert fake_backend.db.commit_calls == 2

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from wd_notability.external_usage.wiki_subscribers import worker as wikisub_worker


@pytest.mark.asyncio
async def test_update_wikisub_cache_once_initializes_lookup_cache(monkeypatch, tmp_path):
    calls: list[str] = []

    class FakeLookupCache:
        def __init__(self, _path):
            self.initialized = False

        def initialize(self):
            calls.append("initialize")
            self.initialized = True

        def get_lookup_state(self, key: str):
            calls.append(f"get:{key}")
            assert self.initialized
            return None

        def upsert_wiki_subscribers(self, wiki_subscribers):
            calls.append(f"upsert:{len(set(wiki_subscribers))}")
            return len(set(wiki_subscribers))

        def set_lookup_state(self, key: str, value: str):
            calls.append(f"set:{key}={value}")

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            return None

        def fetchone(self):
            return (0,)

        def fetchall(self):
            return []

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def close(self):
            calls.append("close")

    monkeypatch.setattr(wikisub_worker, "LookupCache", FakeLookupCache)
    monkeypatch.setattr(wikisub_worker, "_connect", lambda args: FakeConn())

    processed = await wikisub_worker.update_wikisub_cache_once(
        lookup_cache_path=tmp_path / "lookup.sqlite3",
        main_cache_path=tmp_path / "main.sqlite3",
        block_size=10,
        sleep_seconds=0.0,
        args=Namespace(defaults_file=Path.home() / "replica.my.cnf", host="localhost", database="wikidatawiki_p"),
    )

    assert processed == 0
    assert calls[:2] == ["initialize", "get:wikisub_high_water_mark"]

from __future__ import annotations

import argparse
import sys
import types
import builtins
from pathlib import Path

import pytest

import wd_notability.external_usage.wiki_subscribers.builder as build_wikisub_cache_module


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.query = query
        self.params = params

    def fetchone(self):
        return (250000,)

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self):
        self.block_rows = {
            (0, 100000): [("Q1",), ("Q2",)],
            (100000, 200000): [("Q3",)],
            (200000, 250001): [("Q4",)],
        }

    def cursor(self):
        return _FakeCursor(getattr(self, "current_rows", []))

    def close(self):
        self.closed = True


class _FakeCache:
    def __init__(self, output: Path):
        self.output = output
        self.added = []
        self.subscribers = set()

    def initialize(self):
        self.initialized = True

    def upsert_wiki_subscribers(self, qids):
        self.added.append(set(qids))
        self.subscribers.update(qids)
        return len(qids)

    def get_wiki_subscribers(self):
        return set(self.subscribers)

    def replace_wiki_subscribers(self, subscribers):
        self.replaced = set(subscribers)

    def set_lookup_state(self, key, value):
        self.lookup_state = (key, value)


class _FakeProgressBar:
    def __init__(self, total, desc=None):
        self.total = total
        self.desc = desc
        self.updates = []
        self.closed = False

    def update(self, amount):
        self.updates.append(amount)

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_build_wikisub_cache_shows_progress_by_default(monkeypatch, tmp_path):
    fake_cache = _FakeCache(tmp_path / "lookup_cache.db")
    fake_conn = _FakeConn()
    progress_bars = []

    def fake_connect(args):
        return fake_conn

    def fake_tqdm(*, total, desc):
        bar = _FakeProgressBar(total, desc=desc)
        progress_bars.append(bar)
        return bar

    fake_tqdm_module = types.ModuleType("tqdm")
    fake_tqdm_module.tqdm = fake_tqdm

    printed = []

    def fake_fetch_block(conn, start, end):
        conn.current_rows = conn.block_rows[(start, end)]
        return {qid for (qid,) in conn.current_rows}

    def fake_print(*args, **kwargs):
        printed.append(" ".join(str(arg) for arg in args))

    monkeypatch.setattr(build_wikisub_cache_module, "LookupCache", lambda output: fake_cache)
    monkeypatch.setattr(build_wikisub_cache_module, "_connect", fake_connect)
    monkeypatch.setattr(build_wikisub_cache_module, "_fetch_block", fake_fetch_block)
    monkeypatch.setitem(sys.modules, "tqdm", fake_tqdm_module)
    monkeypatch.setattr(builtins, "print", fake_print)

    await build_wikisub_cache_module.build_wikisub_cache(
        output=tmp_path / "lookup_cache.db",
        block_size=100_000,
        sleep_seconds=0.0,
        sync_main_cache=False,
        sync_main_cache_only=False,
        main_cache=tmp_path / "evaluation_cache.sqlite3",
        args=argparse.Namespace(defaults_file=str(tmp_path / "replica.my.cnf"), host="localhost", database="wikidatawiki_p"),
    )

    assert progress_bars and progress_bars[0].total == 3
    assert progress_bars[0].updates == [1, 1, 1]
    assert progress_bars[0].closed is True
    assert fake_cache.added == [{"Q1", "Q2"}, {"Q3"}, {"Q4"}]
    assert fake_conn.closed is True
    assert any("ETA" in line for line in printed)

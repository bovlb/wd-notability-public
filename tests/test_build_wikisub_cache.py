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
        self.initialized = False

    def initialize(self):
        self.initialized = True

    def upsert_wiki_subscribers(self, qids):
        return len(qids)

    def get_wiki_subscribers(self):
        return set()

    def replace_wiki_subscribers(self, subscribers):
        self.replaced = set(subscribers)


class _FakeDBCursor:
    def __init__(self, db):
        self.db = db
        self.executed = []
        self.mode = None

    def execute(self, query, params=None):
        self.executed.append(query.strip())
        if query.strip().startswith("SELECT COUNT(*) FROM temp_wiki_subscribers"):
            self.mode = "count"

    def executemany(self, query, params):
        self.executed.append(query.strip())
        for (qid,) in params:
            self.db.temp_rows.add(qid)

    def fetchone(self):
        if self.mode == "count":
            return (len(self.db.temp_rows),)
        return None


class _FakeDB:
    def __init__(self):
        self.temp_rows = set()
        self.commit_calls = 0
        self.rollback_calls = 0
        self.cursor_obj = _FakeDBCursor(self)

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


class _FakeBackend:
    def __init__(self):
        self.lookup_state = None
        self.db = _FakeDB()

    def get_lookup_state(self, key):
        return self.lookup_state

    def set_lookup_state(self, key, value):
        self.lookup_state = (key, value)

    def _connect(self):
        return self.db


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
    fake_cache = _FakeCache(tmp_path)
    fake_backend = _FakeBackend()
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

    monkeypatch.setattr(build_wikisub_cache_module, "create_lookup_backend", lambda: fake_backend)
    monkeypatch.setattr(build_wikisub_cache_module, "LookupCache", lambda backend: fake_cache)
    monkeypatch.setattr(build_wikisub_cache_module, "_connect", fake_connect)
    monkeypatch.setattr(build_wikisub_cache_module, "_fetch_block", fake_fetch_block)
    monkeypatch.setitem(sys.modules, "tqdm", fake_tqdm_module)
    monkeypatch.setattr(builtins, "print", fake_print)

    await build_wikisub_cache_module.build_wikisub_cache(
        output=tmp_path,
        block_size=100_000,
        sleep_seconds=0.0,
        args=argparse.Namespace(defaults_file=str(tmp_path / "replica.my.cnf"), host="localhost", database="wikidatawiki_p"),
    )

    assert progress_bars and progress_bars[0].total == 3
    assert progress_bars[0].updates == [1, 1, 1]
    assert progress_bars[0].closed is True
    assert fake_backend.db.temp_rows == {1, 2, 3, 4}
    assert fake_backend.db.commit_calls == 4
    assert fake_backend.db.rollback_calls == 0
    assert fake_backend.db.cursor_obj.executed == [
        "DROP TEMPORARY TABLE IF EXISTS temp_wiki_subscribers",
        """
                        CREATE TEMPORARY TABLE temp_wiki_subscribers (
                            qid BIGINT UNSIGNED NOT NULL PRIMARY KEY
                        )
                        """.strip(),
        """
                                INSERT INTO temp_wiki_subscribers (qid)
                                VALUES (%s)
                                ON DUPLICATE KEY UPDATE qid = qid
                                """.strip(),
        """
                                INSERT INTO temp_wiki_subscribers (qid)
                                VALUES (%s)
                                ON DUPLICATE KEY UPDATE qid = qid
                                """.strip(),
        """
                                INSERT INTO temp_wiki_subscribers (qid)
                                VALUES (%s)
                                ON DUPLICATE KEY UPDATE qid = qid
                                """.strip(),
        "SELECT COUNT(*) FROM temp_wiki_subscribers",
        "START TRANSACTION",
        "DELETE FROM wiki_subscribers",
        """
                            INSERT INTO wiki_subscribers (qid)
                            SELECT qid
                            FROM temp_wiki_subscribers
                            ORDER BY qid
                            """.strip(),
        "DROP TEMPORARY TABLE IF EXISTS temp_wiki_subscribers",
    ]
    assert fake_conn.closed is True
    assert any("ETA" in line for line in printed)


def test_connect_uses_replica_defaults(monkeypatch, tmp_path):
    captured = {}

    class FakePyMySQL:
        @staticmethod
        def connect(**kwargs):
            captured.update(kwargs)
            return object()

    monkeypatch.setitem(sys.modules, "pymysql", FakePyMySQL)
    monkeypatch.setenv("REPLICADB_USER", "tool-user")
    monkeypatch.setenv("REPLICADB_PASSWORD", "tool-password")
    monkeypatch.setenv("REPLICADB_HOST", "localhost")
    monkeypatch.setenv("REPLICADB_PORT", "3306")

    conn = build_wikisub_cache_module._connect(
        argparse.Namespace(defaults_file=None, host=None, database=None)
    )

    assert conn is not None
    assert captured["host"] == "localhost"
    assert captured["port"] == 3306
    assert captured["database"] == "wikidatawiki_p"
    assert captured["charset"] == "utf8mb4"
    assert captured["user"] == "tool-user"
    assert captured["password"] == "tool-password"


def test_connect_prefers_replicadb_env(monkeypatch):
    captured = {}

    class FakePyMySQL:
        @staticmethod
        def connect(**kwargs):
            captured.update(kwargs)
            return object()

    monkeypatch.setitem(sys.modules, "pymysql", FakePyMySQL)
    monkeypatch.setenv("REPLICADB_USER", "tool-user")
    monkeypatch.setenv("REPLICADB_PASSWORD", "tool-password")
    monkeypatch.setenv("REPLICADB_HOST", "localhost")
    monkeypatch.setenv("REPLICADB_PORT", "3306")

    conn = build_wikisub_cache_module._connect(
        argparse.Namespace(defaults_file=None, host=None, database=None, port=None)
    )

    assert conn is not None
    assert captured["host"] == "localhost"
    assert captured["port"] == 3306
    assert captured["database"] == "wikidatawiki_p"
    assert captured["user"] == "tool-user"
    assert captured["password"] == "tool-password"

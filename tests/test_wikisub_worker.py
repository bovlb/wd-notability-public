from __future__ import annotations

from argparse import Namespace
import sys
from pathlib import Path

import pytest

from wd_notability.external_usage.wiki_subscribers import worker as wikisub_worker


@pytest.mark.asyncio
async def test_update_wikisub_cache_once_initializes_lookup_cache(monkeypatch, tmp_path):
    calls: list[str] = []

    class FakeLookupCache:
        def __init__(self, _path=None, *, backend=None):
            self.initialized = False
            self.backend = backend

        def initialize(self):
            calls.append("initialize")
            self.initialized = True

        def upsert_wiki_subscribers(self, wiki_subscribers):
            calls.append(f"upsert:{len(set(wiki_subscribers))}")
            return len(set(wiki_subscribers))

    class FakeBackend:
        def get_lookup_state(self, key: str):
            calls.append(f"get:{key}")
            return None

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
    monkeypatch.setattr(wikisub_worker, "create_lookup_backend", lambda: FakeBackend())
    monkeypatch.setattr(wikisub_worker, "_connect", lambda args: FakeConn())

    processed = await wikisub_worker.update_wikisub_cache_once(
        lookup_cache_path=tmp_path,
        block_size=10,
        sleep_seconds=0.0,
        args=Namespace(defaults_file=Path.home() / "replica.my.cnf", host="localhost", database="wikidatawiki_p"),
    )

    assert processed == 0
    assert calls[:2] == ["initialize", "get:wikisub_high_water_mark"]


def test_connect_uses_replica_defaults(monkeypatch):
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

    conn = wikisub_worker._connect(
        Namespace(defaults_file=None, host=None, database=None)
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

    conn = wikisub_worker._connect(
        Namespace(defaults_file=None, host=None, database=None, port=None)
    )

    assert conn is not None
    assert captured["host"] == "localhost"
    assert captured["port"] == 3306
    assert captured["database"] == "wikidatawiki_p"
    assert captured["user"] == "tool-user"
    assert captured["password"] == "tool-password"

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from wd_notability.inlinks import db_read, db_write


class _FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]] | None = None, rowcount: int = 0):
        self._rows = rows or []
        self.rowcount = rowcount

    async def fetchall(self):
        return list(self._rows)

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeDB:
    def __init__(self, *, rows: list[tuple[object, ...]] | None = None, rowcount: int = 0):
        self.rows = rows or []
        self.rowcount = rowcount
        self.executed: list[tuple[str, tuple[object, ...] | list[object]]] = []

    async def execute(self, sql: str, params=None):
        self.executed.append((sql, tuple(params or ())))
        return _FakeCursor(self.rows, self.rowcount)

    async def executemany(self, sql: str, rows):
        self.executed.append((sql, [tuple(row) for row in rows]))
        return _FakeCursor(rowcount=self.rowcount)


class _FakeCache:
    def __init__(self, db: _FakeDB):
        self._db = db
        self._backend_name = "mariadb"
        self.initialized = 0
        self.warned: list[tuple[str, int, dict[str, object]]] = []

    async def initialize(self):
        self.initialized += 1

    def _as_uint32(self, value, _name):
        return int(value)

    def _parse_qid(self, qid):
        if isinstance(qid, int):
            return qid
        if isinstance(qid, str) and qid.startswith("Q") and qid[1:].isdigit():
            return int(qid[1:])
        raise ValueError("bad qid")

    @asynccontextmanager
    async def _connect(self):
        yield self._db

    @asynccontextmanager
    async def _write_guard(self):
        yield None

    def _normalize_owner_id(self, owner_id: str) -> str:
        return owner_id.strip()

    def _warn_slow_write(self, label: str, _started: float, **kwargs):
        self.warned.append((label, 0, kwargs))


@pytest.mark.asyncio
async def test_inlinks_write_upsert_interest_rows_deduplicates_qids():
    db = _FakeDB()
    cache = _FakeCache(db)

    written = await db_write.upsert_pubsub_interest_rows(
        cache,
        worker_id="worker-a",
        qids=[1, 2, 2, 3],
        priority=7,
        wants_creation=False,
        wants_content=True,
        wants_inlinks=True,
    )

    assert written == 3
    sql, rows = db.executed[0]
    assert "INSERT INTO interest" in sql
    assert rows == [
        ("worker-a", 1, 7, 0, 1, 1),
        ("worker-a", 2, 7, 0, 1, 1),
        ("worker-a", 3, 7, 0, 1, 1),
    ]


@pytest.mark.asyncio
async def test_inlinks_write_delete_interest_rows_uses_qid_list():
    db = _FakeDB(rowcount=2)
    cache = _FakeCache(db)

    deleted = await db_write.delete_pubsub_interest_rows(
        cache,
        worker_id="worker-a",
        qids=[3, 3, 4],
    )

    assert deleted == 2
    sql, params = db.executed[0]
    assert "DELETE FROM interest" in sql
    assert params == ("worker-a", 3, 4)


@pytest.mark.asyncio
async def test_inlinks_read_list_targets_converts_rows_and_limit():
    timestamp = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    db = _FakeDB(rows=[(42, 11, 5, 2, timestamp)])
    cache = _FakeCache(db)

    rows = await db_read.list_pubsub_inlinks_targets_with_state(cache, limit=5)

    assert rows == [("Q42", 11, 5, 2, int(timestamp.timestamp()))]
    sql, params = db.executed[0]
    assert "LIMIT ?" in sql
    assert params == (5,)


@pytest.mark.asyncio
async def test_inlinks_read_list_targets_preserves_missing_count():
    timestamp = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    db = _FakeDB(rows=[(42, 11, None, 2, timestamp)])
    cache = _FakeCache(db)

    rows = await db_read.list_pubsub_inlinks_targets_with_state(cache, limit=5)

    assert rows == [("Q42", 11, None, 2, int(timestamp.timestamp()))]


@pytest.mark.asyncio
async def test_inlinks_read_has_interest_uses_query_filter():
    db = _FakeDB(rows=[(1,)])
    cache = _FakeCache(db)

    assert await db_read.has_pubsub_inlinks_interest(cache, "Q42") is True
    sql, params = db.executed[0]
    assert "wants_inlinks = 1" in sql
    assert params == (42,)

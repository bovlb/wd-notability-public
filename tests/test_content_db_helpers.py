from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from wd_notability.content import db_read, db_write
from wd_notability.models import NotabilityLevel


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

    async def commit(self):
        return None


class _FakeCache:
    def __init__(self, db: _FakeDB):
        self._db = db
        self._backend_name = "mariadb"
        self.initialized = 0
        self.warned: list[tuple[str, int, dict[str, object]]] = []

    async def initialize(self):
        self.initialized += 1

    def _parse_qid(self, qid):
        if isinstance(qid, int):
            return qid
        if isinstance(qid, str) and qid.startswith("Q") and qid[1:].isdigit():
            return int(qid[1:])
        raise ValueError("bad qid")

    def _as_uint32(self, value, _name):
        return int(value)

    def _as_uint64(self, value, _name):
        return int(value)

    def _optional_uint32(self, value, _name):
        return None if value is None else int(value)

    def _content_counts_from_item(self, item):
        return (
            int(getattr(item, "has_sitelinks_count", 0)),
            int(getattr(item, "has_claims_count", 0)),
            1 if getattr(item, "is_deleted", False) else 0,
            int(getattr(item, "n1")),
            int(getattr(item, "n2a")),
            int(getattr(item, "n2b")),
        )

    def _summary_update_timestamp_sql(self):
        return "CURRENT_TIMESTAMP(6)"

    def _redirect_target_stale_expr(self):
        return "0"

    def _content_policy_stale_expr(self):
        return "0"

    def _redirect_target_join_clause(self):
        return ""

    def _content_policy_join_clause(self):
        return ""

    def _chunked(self, values, size=500):
        for index in range(0, len(values), size):
            yield values[index:index + size]

    @asynccontextmanager
    async def _connect(self):
        yield self._db

    @asynccontextmanager
    async def _write_guard(self):
        yield None

    def _warn_slow_write(self, label: str, _started: float, **kwargs):
        self.warned.append((label, 0, kwargs))


@dataclass
class _ContentItem:
    qid: str
    content_last_revid: int | None = None
    redirect_target: int | None = None
    has_sitelinks_count: int = 0
    has_claims_count: int = 0
    is_deleted: bool = False
    n1: NotabilityLevel = NotabilityLevel.NONE
    n2a: NotabilityLevel = NotabilityLevel.NONE
    n2b: NotabilityLevel = NotabilityLevel.NONE
    recent_changes_last_revid: int | None = None


@pytest.mark.asyncio
async def test_content_write_upsert_content_many_deduplicates_qids():
    db = _FakeDB(rows=[(1,), (2,)], rowcount=2)
    cache = _FakeCache(db)

    changed = await db_write.upsert_content_many(
        cache,
        [
            _ContentItem(qid="Q1", content_last_revid=10),
            _ContentItem(qid="Q1", content_last_revid=20),
            _ContentItem(qid="Q2", content_last_revid=30, recent_changes_last_revid=40),
        ],
    )

    assert changed == [("Q1", 1), ("Q2", 1)]
    sql, params = db.executed[1]
    assert "INSERT INTO content_evaluation" in sql
    assert params[0] == 1


@pytest.mark.asyncio
async def test_content_write_clear_last_revids_uses_batch_update():
    db = _FakeDB(rows=[(1,)], rowcount=1)
    cache = _FakeCache(db)

    updated = await db_write.clear_content_last_revids(cache, ["Q1", "Q1", "Q2"])

    assert updated == 1
    sql, params = db.executed[1]
    assert "UPDATE content_evaluation" in sql
    assert params == (1, 2)


@pytest.mark.asyncio
async def test_content_read_candidate_helpers_use_content_filters():
    db = _FakeDB(rows=[(42,)])
    cache = _FakeCache(db)

    candidates = await db_read.list_pubsub_content_candidates(cache, limit=5, exclude_qids=["Q7"])
    assert candidates == ["Q42"]

    count = await db_read.count_pubsub_content_candidates(cache)
    assert count == 42

    sql, params = db.executed[0]
    assert "wants_content = 1" in sql
    assert params == (7, 5)

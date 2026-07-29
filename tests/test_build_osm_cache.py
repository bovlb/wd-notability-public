from __future__ import annotations

import asyncio

import pytest

import wd_notability.external_usage.osm.builder as build_osm_cache_module


@pytest.mark.asyncio
async def test_build_osm_cache_stages_then_swaps_rows(monkeypatch, tmp_path):
    rows = [
        {"value": "Q1", "count_all": 3, "count_nodes": 1, "count_ways": 1, "count_relations": 1},
        {"value": "Q2", "count": 4},
    ]

    async def fake_fetch_page(client, page, page_size):
        return rows if page == 1 else []

    class FakeCursor:
        def __init__(self):
            self.executed = []
            self.executemany_calls = []

        def execute(self, sql):
            self.executed.append(sql.strip())

        def executemany(self, sql, params):
            self.executemany_calls.append((sql.strip(), list(params)))

    class FakeDB:
        def __init__(self):
            self.cursor_obj = FakeCursor()
            self.commit_calls = 0
            self.rollback_calls = 0

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

    class FakeBackend:
        def __init__(self):
            self.db = FakeDB()

        def _connect(self):
            return self.db

    class FakeCache:
        def __init__(self, backend):
            self.backend = backend
            self.initialize_called = False

        def initialize(self):
            self.initialize_called = True

    fake_backend = FakeBackend()
    monkeypatch.setattr(build_osm_cache_module, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(build_osm_cache_module, "create_lookup_backend", lambda: fake_backend)
    monkeypatch.setattr(build_osm_cache_module, "LookupCache", FakeCache)

    await build_osm_cache_module.build_osm_cache(tmp_path, page_size=999)

    assert fake_backend.db.commit_calls == 2
    assert fake_backend.db.rollback_calls == 0
    assert fake_backend.db.cursor_obj.executed == [
        "DROP TEMPORARY TABLE IF EXISTS temp_osm_usage",
        """
                CREATE TEMPORARY TABLE temp_osm_usage (
                    qid BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                    count_all BIGINT NOT NULL DEFAULT 0,
                    count_nodes BIGINT NOT NULL DEFAULT 0,
                    count_ways BIGINT NOT NULL DEFAULT 0,
                    count_relations BIGINT NOT NULL DEFAULT 0
                )
                """.strip(),
        "START TRANSACTION",
        "DELETE FROM osm_usage",
        """
                    INSERT INTO osm_usage (qid, count_all, count_nodes, count_ways, count_relations)
                    SELECT qid, count_all, count_nodes, count_ways, count_relations
                    FROM temp_osm_usage
                    ORDER BY qid
                    """.strip(),
        "DROP TEMPORARY TABLE IF EXISTS temp_osm_usage",
    ]
    assert fake_backend.db.cursor_obj.executemany_calls == [
        (
            """
                            INSERT INTO temp_osm_usage
                                (qid, count_all, count_nodes, count_ways, count_relations)
                            VALUES (%s, %s, %s, %s, %s)
                            """.strip(),
            [
                (1, 3, 1, 1, 1),
                (2, 4, 0, 0, 0),
            ],
        )
    ]


@pytest.mark.asyncio
async def test_build_osm_cache_honors_limit(monkeypatch, tmp_path):
    rows = [{"value": "Q1", "count_all": 1}]

    async def fake_fetch_page(client, page, page_size):
        return rows if page == 1 else []

    class FakeCursor:
        def __init__(self):
            self.executemany_calls = []
            self.executed = []

        def execute(self, sql):
            self.executed.append(sql.strip())

        def executemany(self, sql, params):
            self.executemany_calls.append((sql.strip(), list(params)))

    class FakeDB:
        def __init__(self):
            self.cursor_obj = FakeCursor()
            self.commit_calls = 0

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.commit_calls += 1

        def rollback(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeBackend:
        def __init__(self):
            self.db = FakeDB()

        def _connect(self):
            return self.db

    class FakeCache:
        def __init__(self, backend):
            self.backend = backend
            self.initialize_called = False

        def initialize(self):
            self.initialize_called = True

    fake_backend = FakeBackend()
    monkeypatch.setattr(build_osm_cache_module, "_fetch_page", fake_fetch_page)
    monkeypatch.setattr(build_osm_cache_module, "create_lookup_backend", lambda: fake_backend)
    monkeypatch.setattr(build_osm_cache_module, "LookupCache", FakeCache)

    await build_osm_cache_module.build_osm_cache(tmp_path, page_size=999, limit=1)

    assert fake_backend.db.commit_calls == 2
    assert fake_backend.db.cursor_obj.executemany_calls == [
        (
            """
                            INSERT INTO temp_osm_usage
                                (qid, count_all, count_nodes, count_ways, count_relations)
                            VALUES (%s, %s, %s, %s, %s)
                            """.strip(),
            [(1, 1, 0, 0, 0)],
        )
    ]

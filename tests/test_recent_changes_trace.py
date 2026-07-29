from __future__ import annotations

import pytest

from wd_notability.metadata import worker as recent_changes_worker


@pytest.mark.asyncio
async def test_recent_changes_scan_pass_traces_writes(monkeypatch):
    captured: list = []

    class FakeTrace:
        async def record_events(self, records):
            captured.extend(records)
            return len(records)

    class FakeCache:
        item_trace = FakeTrace()

        async def update_recent_changes_last_revids(self, qids):
            return len(qids)

        async def upsert_creation_metadata_many(self, rows):
            return len(rows)

    class FakeReplicaSource:
        def fetch_recent_changes(self, *, start_epoch, start_rc_id=0, limit=1000):
            return (
                [
                    {
                        "title": "Q42",
                        "creator_actor_id": 7,
                        "this_oldid": 111,
                        "revid": 111,
                        "old_revid": 110,
                        "rc_source": "edit",
                        "timestamp": "20260718000000",
                    },
                    {
                        "title": "Q99",
                        "creator_actor_id": 8,
                        "this_oldid": 222,
                        "revid": 222,
                        "old_revid": 221,
                        "rc_source": "mw.new",
                        "timestamp": "20260718000100",
                    },
                ],
                (1_784_398_800.0, 2),
            )

    monkeypatch.setattr(recent_changes_worker, "CACHE", FakeCache())
    monkeypatch.setattr(recent_changes_worker, "_RECENT_CHANGES_REPLICA", FakeReplicaSource())

    updated, creation_updated, _cursor = await recent_changes_worker._run_recent_changes_scan_pass(0)

    assert updated == 2
    assert creation_updated == 1
    assert [record.event_type for record in captured] == [
        "recent_changes_written",
        "recent_changes_written",
        "creation_metadata_written",
    ]
    assert all(record.details["committed"] is True for record in captured)
    assert captured[0].details["recent_changes_last_revid"] == 111
    assert captured[2].details["source"] == "recent_changes_scan"

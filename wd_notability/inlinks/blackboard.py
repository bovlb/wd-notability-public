from __future__ import annotations

from dataclasses import dataclass, replace
from time import time
from typing import Iterable

from wd_notability.models import NotabilityLevel
from wd_notability.models import QID


@dataclass(frozen=True, slots=True)
class InlinksBoardEntry:
    qid: QID
    interest_seen_at: int
    interest_priority: int = 0
    inlinks_count: int | None = None
    count_fetched_at: int | None = None
    inlinks: tuple[QID, ...] = ()
    graph_fetched_at: int | None = None
    graph_truncated: bool = False
    evaluated_at: int | None = None
    n3_inlinks: NotabilityLevel | None = None

    @property
    def has_graph(self) -> bool:
        return self.graph_fetched_at is not None

    @property
    def count_last_processed_at(self) -> int | None:
        return self.count_fetched_at

    @property
    def graph_last_processed_at(self) -> int | None:
        return self.graph_fetched_at

    @property
    def evaluation_last_processed_at(self) -> int | None:
        return self.evaluated_at


class InlinksBlackboard:
    def __init__(self) -> None:
        self._entries: dict[QID, InlinksBoardEntry] = {}

    def snapshot(self) -> dict[QID, InlinksBoardEntry]:
        return dict(self._entries)

    @staticmethod
    def _status_rank(level: NotabilityLevel | None) -> int:
        if level is None or level == NotabilityLevel.UNKNOWN:
            return 0
        if level == NotabilityLevel.STRONG:
            return 2
        return 1

    @staticmethod
    def _normalize_interest_row(item: object) -> tuple[QID, int, int | None, NotabilityLevel | None, int | None]:
        if isinstance(item, tuple) or isinstance(item, list):
            if len(item) < 1:
                raise ValueError("interest row must include a qid")
            qid = item[0]
            priority = int(item[1]) if len(item) > 1 and item[1] is not None else 0
            inlinks_count = int(item[2]) if len(item) > 2 and item[2] is not None else None
            n3_raw = item[3] if len(item) > 3 else None
            last_updated = int(item[4]) if len(item) > 4 and item[4] is not None else None
        else:
            qid = item
            priority = 0
            inlinks_count = None
            n3_raw = None
            last_updated = None

        if isinstance(n3_raw, NotabilityLevel):
            n3_inlinks = n3_raw
        elif n3_raw is None:
            n3_inlinks = None
        else:
            n3_inlinks = NotabilityLevel(int(n3_raw))

        return str(qid), priority, inlinks_count, n3_inlinks, last_updated

    def apply_interest(
        self,
        qids: Iterable[QID],
        *,
        observed_at: int | None = None,
    ) -> bool:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        changed = False
        interest: list[QID] = []
        seen_interest: set[QID] = set()
        for qid in qids:
            if not isinstance(qid, str) or not qid.startswith("Q") or not qid[1:].isdigit():
                continue
            if qid in seen_interest:
                continue
            seen_interest.add(qid)
            interest.append(qid)

        for qid in interest:
            current = self._entries.get(qid)
            if current is None:
                self._entries[qid] = InlinksBoardEntry(qid=qid, interest_seen_at=observed_at)
                changed = True
            elif current.interest_seen_at != observed_at:
                self._entries[qid] = replace(current, interest_seen_at=observed_at)

        return changed

    def apply_interest_rows(
        self,
        rows: Iterable[object],
        *,
        observed_at: int | None = None,
    ) -> tuple[bool, list[QID]]:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        changed = False
        new_qids: list[QID] = []
        seen_interest: set[QID] = set()
        for row in rows:
            qid, priority, inlinks_count, n3_inlinks, last_updated = self._normalize_interest_row(row)
            if not isinstance(qid, str) or not qid.startswith("Q") or not qid[1:].isdigit():
                continue
            if qid in seen_interest:
                continue
            seen_interest.add(qid)

            current = self._entries.get(qid)
            if current is None:
                self._entries[qid] = InlinksBoardEntry(
                    qid=qid,
                    interest_seen_at=observed_at,
                    interest_priority=priority,
                    inlinks_count=inlinks_count,
                    count_fetched_at=last_updated,
                    evaluated_at=last_updated,
                    n3_inlinks=n3_inlinks,
                )
                new_qids.append(qid)
                changed = True
                continue

            updates: dict[str, object] = {}
            if current.interest_seen_at != observed_at:
                updates["interest_seen_at"] = observed_at
            if current.interest_priority != priority:
                updates["interest_priority"] = priority
            if current.inlinks_count is None and inlinks_count is not None:
                updates["inlinks_count"] = inlinks_count
            if current.count_fetched_at is None and last_updated is not None:
                updates["count_fetched_at"] = last_updated
            if current.evaluated_at is None and last_updated is not None:
                updates["evaluated_at"] = last_updated
            if current.n3_inlinks is None and n3_inlinks is not None:
                updates["n3_inlinks"] = n3_inlinks
            if updates:
                self._entries[qid] = replace(current, **updates)
                changed = True
        return changed, new_qids

    def prune_stale_interest(self, *, stale_after_seconds: int, observed_at: int | None = None) -> list[QID]:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        removed: list[QID] = []
        for qid, entry in list(self._entries.items()):
            if observed_at - entry.interest_seen_at < stale_after_seconds:
                continue
            del self._entries[qid]
            removed.append(qid)
        return removed

    def record_count(self, qid: QID, count: int, *, observed_at: int | None = None) -> bool:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        current = self._entries.get(qid)
        if current is None:
            return False
        if current.inlinks_count == count and current.count_fetched_at == observed_at:
            return False
        self._entries[qid] = replace(
            current,
            inlinks_count=max(0, int(count)),
            count_fetched_at=observed_at,
        )
        return True

    def record_count_if_missing(
        self,
        qid: QID,
        count: int,
        *,
        observed_at: int | None = None,
    ) -> bool:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        current = self._entries.get(qid)
        if current is None:
            return False
        if current.inlinks_count is not None and current.inlinks_count > 0:
            return False
        if current.inlinks_count == count and current.count_fetched_at == observed_at:
            return False
        self._entries[qid] = replace(
            current,
            inlinks_count=max(0, int(count)),
            count_fetched_at=observed_at,
        )
        return True

    def record_graph(
        self,
        qid: QID,
        inlinks: Iterable[QID],
        *,
        observed_at: int | None = None,
        truncated: bool = False,
    ) -> bool:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        current = self._entries.get(qid)
        if current is None:
            return False
        normalized: list[QID] = []
        seen: set[QID] = set()
        for inlink in inlinks:
            if not isinstance(inlink, str) or not inlink.startswith("Q") or not inlink[1:].isdigit():
                continue
            if inlink == qid or inlink in seen:
                continue
            seen.add(inlink)
            normalized.append(inlink)
        normalized_tuple = tuple(normalized)
        if (
            current.inlinks == normalized_tuple
            and current.graph_fetched_at == observed_at
            and current.graph_truncated == bool(truncated)
        ):
            return False
        self._entries[qid] = replace(
            current,
            inlinks=normalized_tuple,
            graph_fetched_at=observed_at,
            graph_truncated=bool(truncated),
        )
        return True

    def mark_evaluated(
        self,
        qid: QID,
        *,
        observed_at: int | None = None,
        n3_inlinks: NotabilityLevel | None = None,
    ) -> None:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        current = self._entries.get(qid)
        if current is None:
            return
        self._entries[qid] = replace(
            current,
            evaluated_at=observed_at,
            n3_inlinks=n3_inlinks if n3_inlinks is not None else current.n3_inlinks,
        )

    def record_empty_graph(self, qid: QID, *, observed_at: int | None = None) -> bool:
        observed_at = int(time()) if observed_at is None else int(observed_at)
        current = self._entries.get(qid)
        if current is None:
            return False
        if current.inlinks_count == 0 and current.evaluated_at == observed_at:
            return False
        self._entries[qid] = replace(
            current,
            inlinks=(),
            graph_fetched_at=observed_at,
            inlinks_count=0 if current.inlinks_count is None else current.inlinks_count,
            evaluated_at=observed_at,
            n3_inlinks=NotabilityLevel.NONE,
        )
        return True

    def _priority_key(
        self,
        entry: InlinksBoardEntry,
        *,
        reference_at: int | None,
    ) -> tuple[int, int, int, int, str]:
        return (
            -int(entry.interest_priority),
            self._status_rank(entry.n3_inlinks),
            int(reference_at or 0),
            int(entry.inlinks_count if entry.inlinks_count is not None else 2**63 - 1),
            entry.qid,
        )

    def interest_candidates(self, *, limit: int | None = None) -> list[QID]:
        entries = sorted(
            self._entries.values(),
            key=lambda entry: self._priority_key(
                entry,
                reference_at=entry.graph_fetched_at or entry.evaluated_at or entry.interest_seen_at,
            ),
        )
        qids = [entry.qid for entry in entries]
        return qids[:limit] if limit is not None else qids

    def all_inlinks(self) -> list[QID]:
        seen: set[QID] = set()
        ordered: list[QID] = []
        for entry in self._entries.values():
            for inlink in entry.inlinks:
                if inlink in seen:
                    continue
                seen.add(inlink)
                ordered.append(inlink)
        return ordered

    def count_candidates(self, *, stale_after_seconds: int) -> list[QID]:
        now = int(time())
        missing: list[tuple[int, int, QID]] = []
        stale: list[tuple[int, int, QID]] = []
        for qid, entry in self._entries.items():
            if entry.inlinks_count is None:
                missing.append((entry.interest_priority, entry.interest_seen_at, qid))
                continue
            if entry.inlinks_count > 0:
                continue
            if entry.count_fetched_at is None or now - entry.count_fetched_at >= stale_after_seconds:
                stale.append((entry.interest_priority, entry.count_fetched_at or 0, qid))
        missing.sort(key=lambda item: (-item[0], item[1], item[2]))
        stale.sort(key=lambda item: (-item[0], item[1], item[2]))
        return [qid for _priority, _timestamp, qid in missing] + [qid for _priority, _timestamp, qid in stale]

    def graph_candidates(self, *, stale_after_seconds: int) -> list[QID]:
        now = int(time())
        missing: list[tuple[int, int, int, int, QID]] = []
        stale: list[tuple[int, int, int, int, QID]] = []
        for qid, entry in self._entries.items():
            if entry.inlinks_count is None:
                continue
            if entry.inlinks_count <= 0:
                continue
            if entry.graph_truncated and entry.graph_fetched_at is not None:
                continue
            if entry.graph_fetched_at is None:
                missing.append((
                    -entry.interest_priority,
                    self._status_rank(entry.n3_inlinks),
                    entry.interest_seen_at,
                    entry.inlinks_count,
                    qid,
                ))
                continue
            if now - entry.graph_fetched_at >= stale_after_seconds:
                stale.append((
                    -entry.interest_priority,
                    self._status_rank(entry.n3_inlinks),
                    entry.graph_fetched_at,
                    entry.inlinks_count,
                    qid,
                ))
        missing.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        stale.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        return [qid for _priority, _status, _timestamp, _count, qid in missing] + [qid for _priority, _status, _timestamp, _count, qid in stale]

    def evaluation_candidates(self, *, stale_after_seconds: int) -> list[InlinksBoardEntry]:
        now = int(time())
        bucketed: list[tuple[int, int, int, int, QID, InlinksBoardEntry]] = []
        for qid, entry in self._entries.items():
            if entry.inlinks_count is None:
                continue
            if entry.inlinks_count > 0 and entry.graph_fetched_at is None:
                continue
            if entry.graph_truncated and entry.evaluated_at is not None:
                continue
            if entry.evaluated_at is None:
                last_processed_at = entry.interest_seen_at
            else:
                age = now - entry.evaluated_at
                if age < stale_after_seconds:
                    continue
                last_processed_at = entry.evaluated_at
            bucketed.append((
                self._status_rank(entry.n3_inlinks),
                -entry.interest_priority,
                last_processed_at,
                entry.inlinks_count or 0,
                qid,
                entry,
            ))
        bucketed.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        return [entry for _status, _priority, _last_processed_at, _count, _qid, entry in bucketed]

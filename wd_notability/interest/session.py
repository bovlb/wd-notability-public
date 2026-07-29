from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from wd_notability.interest.manager import InterestManager


def _normalize_qids(qids: list[str | int]) -> list[int]:
    seen: set[int] = set()
    normalized: list[int] = []
    for qid in qids:
        if isinstance(qid, int):
            qid_num = qid
        elif isinstance(qid, str) and qid.startswith("Q") and qid[1:].isdigit():
            qid_num = int(qid[1:])
        else:
            raise ValueError("qid must look like Q42 or be an integer")
        if qid_num < 0:
            raise ValueError("qid must be non-negative")
        if qid_num in seen:
            continue
        seen.add(qid_num)
        normalized.append(qid_num)
    return normalized


@dataclass(slots=True)
class _InterestMessage:
    additions: tuple[int, ...]
    deletions: tuple[int, ...]


class InterestSession:
    def __init__(self, manager: "InterestManager") -> None:
        self._manager = manager
        self._qids: set[int] = set()
        self._closed = False

    async def replace(self, qids: list[str | int]) -> None:
        if self._closed:
            raise RuntimeError("interest session is closed")
        replacement = set(_normalize_qids(qids))
        additions = tuple(sorted(replacement - self._qids))
        deletions = tuple(sorted(self._qids - replacement))
        self._qids = replacement
        await self._manager.enqueue(additions=additions, deletions=deletions)

    async def clear(self) -> None:
        if self._closed:
            return
        if self._qids:
            deletions = tuple(sorted(self._qids))
            self._qids.clear()
            await self._manager.enqueue(additions=(), deletions=deletions)

    async def close(self) -> None:
        await self.clear()
        self._closed = True

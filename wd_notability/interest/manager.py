from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from wd_notability.item_trace import ItemTraceRecord
from wd_notability.interest.session import InterestSession, _InterestMessage, _normalize_qids

logger = logging.getLogger(__name__)

_FLUSH_TIMEOUT_SECONDS = 1.0


class InterestManager:
    """
    IMPORTANT: This class should be treated as singleton within a given process. 
    """

    def __init__(
        self,
        pubsub: Any,
        *,
        worker_id: str,
        priority: int,
        wants_creation: bool = False,
        wants_content: bool = False,
        wants_inlinks: bool = False,
    ) -> None:
        self.pubsub = pubsub
        self.worker_id = worker_id.strip()
        if not self.worker_id:
            raise ValueError("worker_id must not be empty")
        self.priority = int(priority)
        self.wants_creation = bool(wants_creation)
        self.wants_content = bool(wants_content)
        self.wants_inlinks = bool(wants_inlinks)
        assert self.wants_content or self.wants_inlinks or self.wants_creation, "At least one of wants_content, wants_inlinks, or wants_creation must be True"
        self.current: set[int] = set()
        self.persisted: set[int] = set()
        self._queue: asyncio.Queue[_InterestMessage | None] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._owner_id = self.worker_id.split(":", 1)[0]

    @classmethod
    async def create(
        cls,
        pubsub: Any,
        *,
        worker_id: str,
        priority: int,
        wants_creation: bool = False,
        wants_content: bool = False,
        wants_inlinks: bool = False,
    ) -> "InterestManager":
        manager = cls(
            pubsub,
            worker_id=worker_id,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
        )
        await manager.start()
        return manager

    def create_session(self) -> InterestSession:
        return InterestSession(self)

    async def start(self) -> None:
        await self._purge_startup_interest()
        if self._task is None:
            self._task = asyncio.create_task(
                self._run(), name=f"interest:{self.worker_id}")

    async def enqueue(self, *, additions: tuple[int, ...], deletions: tuple[int, ...]) -> None:
        if self._stop.is_set():
            logger.warning(
                "InterestManager enqueue called after stop for %s", self.worker_id
            )
            return
        logger.debug(
            "InterestManager enqueue: %d additions, %d deletions for %s",
            len(additions), len(deletions), self.worker_id
        )
        await self._queue.put(_InterestMessage(additions=additions, deletions=deletions))

    async def close(self) -> None:
        self._stop.set()
        await self._queue.put(None)
        if self._task is not None:
            try:
                await self._task
            finally:
                self._task = None
        deleted = await self._delete_all_interest()
        if deleted and self.persisted:
            await self._record_interest_removed(tuple(sorted(self.persisted)))

    async def _purge_startup_interest(self) -> None:
        delete_owner = getattr(self.pubsub, "delete_interest_for_owner", None)
        if callable(delete_owner):
            await delete_owner(owner_id=self._owner_id)
            return
        delete_worker = getattr(
            self.pubsub, "delete_pubsub_interest_for_worker", None)
        if callable(delete_worker):
            await delete_worker(worker_id=self.worker_id)

    async def _delete_all_interest(self) -> int:
        delete_worker = getattr(
            self.pubsub, "delete_pubsub_interest_for_worker", None)
        if callable(delete_worker):
            try:
                return int(await delete_worker(worker_id=self.worker_id))
            except Exception:
                logger.exception(
                    "InterestManager delete failed for %s", self.worker_id)
        return 0

    async def _record_interest_added(self, qids: tuple[int, ...]) -> None:
        if not qids:
            return

        trace = getattr(getattr(self.pubsub, "cache", None),
                        "item_trace", None)
        if trace is None:
            return

        details = {
            "worker_id": self.worker_id,
            "priority": self.priority,
            "wants_creation": self.wants_creation,
            "wants_content": self.wants_content,
            "wants_inlinks": self.wants_inlinks,
        }
        emit_many = getattr(trace, "record_interest_added_many", None)
        if not callable(emit_many):
            emit_many = getattr(trace, "record_interest_started_many", None)
        if callable(emit_many):
            await emit_many(
                worker_name=self.worker_id.split(":", 1)[0],
                qids=qids,
                interest_type=self.worker_id,
                details=details,
            )
            return

        emit_one = getattr(trace, "record_event", None)
        if not callable(emit_one):
            return

        for qid in qids:
            await emit_one(
                ItemTraceRecord(
                    qid=qid,
                    event_type="interest_added",
                    worker_name=self.worker_id.split(":", 1)[0],
                    details={"interest_type": self.worker_id, **details},
                )
            )

    async def _record_interest_removed(self, qids: tuple[int, ...]) -> None:
        if not qids:
            return

        trace = getattr(getattr(self.pubsub, "cache", None),
                        "item_trace", None)
        if trace is None:
            return

        details = {
            "worker_id": self.worker_id,
            "priority": self.priority,
            "wants_creation": self.wants_creation,
            "wants_content": self.wants_content,
            "wants_inlinks": self.wants_inlinks,
        }
        emit_many = getattr(trace, "record_interest_removed_many", None)
        if not callable(emit_many):
            emit_many = getattr(trace, "record_interest_expired_many", None)
        if callable(emit_many):
            await emit_many(
                worker_name=self.worker_id.split(":", 1)[0],
                qids=qids,
                interest_type=self.worker_id,
                details=details,
            )
            return

        emit_one = getattr(trace, "record_event", None)
        if not callable(emit_one):
            return

        for qid in qids:
            await emit_one(
                ItemTraceRecord(
                    qid=qid,
                    event_type="interest_removed",
                    worker_name=self.worker_id.split(":", 1)[0],
                    details={"interest_type": self.worker_id, **details},
                )
            )

    async def _record_interest_started(self, qids: tuple[int, ...]) -> None:
        await self._record_interest_added(qids)

    async def _record_interest_expired(self, qids: tuple[int, ...]) -> None:
        await self._record_interest_removed(qids)

    async def _flush(self) -> None:
        adds = self.current - self.persisted
        deletes = self.persisted - self.current
        if not adds and not deletes:
            return
        try:
            if adds:
                await self.pubsub.upsert_interest_rows(
                    worker_id=self.worker_id,
                    qids=sorted(adds),
                    priority=self.priority,
                    wants_creation=self.wants_creation,
                    wants_content=self.wants_content,
                    wants_inlinks=self.wants_inlinks,
                )
            if deletes:
                await self.pubsub.delete_interest_rows(
                    worker_id=self.worker_id,
                    qids=sorted(deletes),
                )
        except Exception:
            logger.exception("Interest flush failed for %s", self.worker_id)
            return
        await self._record_interest_added(tuple(sorted(adds)))
        await self._record_interest_removed(tuple(sorted(deletes)))
        self.persisted = set(self.current)

    def _apply_message(self, message: _InterestMessage) -> None:
        old_size = len(self.current)
        self.current.update(message.additions)
        self.current.difference_update(message.deletions)
        logger.debug(
            "InterestManager applied message: %d additions, %d deletions for %s (old size %d, new size %d)",
            len(message.additions), len(
                message.deletions), self.worker_id, old_size, len(self.current)
        )

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                message = await self._queue.get()
            except asyncio.CancelledError:
                logger.exception(
                    "InterestManager run cancelled for %s", self.worker_id)
                break
            if message is None:
                logger.error(
                    "InterestManager run received empty message for %s", self.worker_id)
                break
            self._apply_message(message)
            pending_since = time.monotonic()

            while not self._stop.is_set():
                if self._queue.empty():
                    break
                timeout = max(0.0, _FLUSH_TIMEOUT_SECONDS -
                              (time.monotonic() - pending_since))
                if timeout <= 0:
                    break
                try:
                    message = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                except TimeoutError:
                    break
                if message is None:
                    self._stop.set()
                    break
                self._apply_message(message)
            await self._flush()

        await self._flush()

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from wd_notability.evaluation_cache import EvaluationCache

logger = logging.getLogger(__name__)


def _env_flag(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    text = value.strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


ITEM_TRACE_ENABLED = _env_flag("WD_NOTABILITY_ITEM_TRACE_ENABLED", default=True)

# Maximum number of trace rows to batch before flushing immediately.
DEFAULT_ITEM_TRACE_FLUSH_BATCH_SIZE = int(
    os.getenv("WD_NOTABILITY_ITEM_TRACE_FLUSH_BATCH_SIZE", "200")
)
# How long to wait before flushing a partial batch in the background.
DEFAULT_ITEM_TRACE_FLUSH_SECONDS = float(
    os.getenv("WD_NOTABILITY_ITEM_TRACE_FLUSH_SECONDS", "2.0")
)
# Keep only recent trace rows so the table stays bounded.
DEFAULT_ITEM_TRACE_RETENTION_SECONDS = int(
    os.getenv("WD_NOTABILITY_ITEM_TRACE_RETENTION_SECONDS", "3600")
)
# Avoid running prune on every flush.
DEFAULT_ITEM_TRACE_PRUNE_INTERVAL_SECONDS = int(
    os.getenv("WD_NOTABILITY_ITEM_TRACE_PRUNE_INTERVAL_SECONDS", "300")
)


@dataclass(slots=True, frozen=True)
class ItemTraceRecord:
    qid: str | int
    event_type: str
    worker_name: str
    details: Mapping[str, Any] | None = None
    batch_id: str | None = None
    timestamp: int | float | None = None


def _to_utc_datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str) and value.strip():
        try:
            text = value.strip()
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        return dt.astimezone(UTC) if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
    return None


def _to_iso_datetime(value: object | None) -> str | None:
    dt = _to_utc_datetime(value)
    return None if dt is None else dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


class ItemTraceStore:
    def __init__(self, cache: EvaluationCache):
        self.cache = cache
        self.enabled = ITEM_TRACE_ENABLED
        self._buffer: list[tuple[datetime, int, str, str, str | None, str]] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._prune_task: asyncio.Task | None = None
        self._last_pruned_at = 0.0

    @staticmethod
    def _normalize_worker_name(worker_name: str) -> str:
        worker = worker_name.strip()
        if not worker:
            raise ValueError("worker_name must not be empty")
        return worker

    @staticmethod
    def _normalize_event_type(event_type: str) -> str:
        event = event_type.strip().lower()
        if not event:
            raise ValueError("event_type must not be empty")
        return event

    def _normalize_record(
        self,
        record: ItemTraceRecord,
    ) -> tuple[datetime, int, str, str, str | None, str] | None:
        if not self.enabled:
            return None
        qid_num = self.cache._parse_qid(record.qid)
        if qid_num is None:
            return None
        event_type = self._normalize_event_type(record.event_type)
        worker_name = self._normalize_worker_name(record.worker_name)
        timestamp = _to_utc_datetime(datetime.now(UTC) if record.timestamp is None else record.timestamp)
        if timestamp is None:
            return None
        batch_id = None if record.batch_id is None else str(record.batch_id).strip() or None
        details_payload: object
        if record.details is None:
            details_payload = {}
        elif isinstance(record.details, Mapping):
            details_payload = dict(record.details)
        else:
            details_payload = {"value": record.details}
        details = json.dumps(
            details_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=_json_default,
        )
        return timestamp, qid_num, event_type, worker_name, batch_id, details

    def _should_prune(self, now: float) -> bool:
        return (now - self._last_pruned_at) >= DEFAULT_ITEM_TRACE_PRUNE_INTERVAL_SECONDS

    async def _write_rows(self, rows: Sequence[tuple[datetime, int, str, str, str | None, str]]) -> int:
        if not self.enabled or not rows:
            return 0

        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                for chunk in self.cache._chunked(rows):
                    await db.execute("BEGIN IMMEDIATE")
                    if self.cache._backend_name == "mariadb":
                        values_sql = ", ".join("(%s, %s, %s, %s, %s, %s)" for _ in chunk)
                    else:
                        values_sql = ", ".join("(?, ?, ?, ?, ?, ?)" for _ in chunk)
                    params: list[Any] = []
                    for timestamp, qid_num, event_type, worker_name, batch_id, details in chunk:
                        params.extend([timestamp, qid_num, event_type, worker_name, batch_id, details])
                    await db.execute(
                        f"""
                        INSERT INTO item_trace_events (
                            ts, qid, event_type, worker_name, batch_id, details
                        )
                        VALUES {values_sql}
                        """,
                        params,
                    )
                    await db.commit()
        self.cache._warn_slow_write("item_trace_write", started, row_count=len(rows))
        return len(rows)

    async def _maybe_prune(self) -> int:
        if not self.enabled:
            return 0
        now = time.monotonic()
        if not self._should_prune(now):
            return 0

        cutoff = datetime.now(UTC) - timedelta(seconds=DEFAULT_ITEM_TRACE_RETENTION_SECONDS)
        started = time.perf_counter()
        async with self.cache._write_guard():
            async with self.cache._connect() as db:
                await db.execute("BEGIN IMMEDIATE")
                cursor = await db.execute(
                    "DELETE FROM item_trace_events WHERE ts < ?",
                    (cutoff,),
                )
                await db.commit()
        self._last_pruned_at = now
        pruned = max(0, int(cursor.rowcount))
        if pruned:
            self.cache._warn_slow_write("item_trace_prune", started, row_count=pruned)
        return pruned

    async def _prune_loop(self) -> None:
        if not self.enabled:
            self._prune_task = None
            return
        try:
            while True:
                await asyncio.sleep(DEFAULT_ITEM_TRACE_PRUNE_INTERVAL_SECONDS)
                try:
                    await self._maybe_prune()
                except Exception:  # noqa: BLE001
                    logger.exception("Item trace prune loop failed")
        except asyncio.CancelledError:
            raise
        finally:
            self._prune_task = None

    def _ensure_prune_task(self) -> None:
        if not self.enabled:
            return
        if self._prune_task is not None and not self._prune_task.done():
            return
        self._prune_task = asyncio.create_task(self._prune_loop())

    async def _flush_pending(self) -> int:
        if not self.enabled:
            return 0
        async with self._lock:
            rows = self._buffer
            self._buffer = []
            self._flush_task = None

        if not rows:
            return 0

        written = await self._write_rows(rows)
        return written

    async def _delayed_flush(self) -> None:
        if not self.enabled:
            return
        try:
            await asyncio.sleep(DEFAULT_ITEM_TRACE_FLUSH_SECONDS)
            await self._flush_pending()
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001
            logger.exception("Item trace delayed flush failed")

    async def record_events(self, records: Sequence[ItemTraceRecord]) -> int:
        if not self.enabled:
            return 0
        await self.cache.initialize()

        normalized = [record for record in (
            self._normalize_record(record) for record in records
        ) if record is not None]
        if not normalized:
            return 0

        should_flush = False
        now = datetime.now(UTC)
        async with self._lock:
            self._buffer.extend(normalized)
            if len(self._buffer) >= DEFAULT_ITEM_TRACE_FLUSH_BATCH_SIZE:
                should_flush = True
            elif self._buffer and (now - self._buffer[0][0]).total_seconds() >= DEFAULT_ITEM_TRACE_FLUSH_SECONDS:
                should_flush = True
            elif self._flush_task is None or self._flush_task.done():
                self._flush_task = asyncio.create_task(self._delayed_flush())
            if self._prune_task is None or self._prune_task.done():
                self._ensure_prune_task()

        if should_flush:
            return await self._flush_pending()
        return len(normalized)

    async def record_event(self, record: ItemTraceRecord) -> int:
        return await self.record_events([record])

    async def flush(self) -> int:
        if not self.enabled:
            return 0
        await self.cache.initialize()
        self._ensure_prune_task()
        async with self._lock:
            if self._flush_task is not None and self._flush_task is not asyncio.current_task():
                self._flush_task.cancel()
        return await self._flush_pending()

    async def close(self) -> None:
        if not self.enabled:
            self._prune_task = None
            self._flush_task = None
            return
        task = self._prune_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._prune_task = None

    async def record_interest_added_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        interest_type: str,
        details: Mapping[str, Any] | None = None,
        batch_id: str | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        payload = {"interest_type": interest_type}
        if details:
            payload.update(details)
        return await self.record_events(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type="interest_added",
                    worker_name=worker_name,
                    batch_id=batch_id,
                    details=payload,
                    timestamp=timestamp,
                )
                for qid in qids
            ]
        )

    async def record_interest_removed_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        interest_type: str,
        details: Mapping[str, Any] | None = None,
        batch_id: str | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        payload = {"interest_type": interest_type}
        if details:
            payload.update(details)
        return await self.record_events(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type="interest_removed",
                    worker_name=worker_name,
                    batch_id=batch_id,
                    details=payload,
                    timestamp=timestamp,
                )
                for qid in qids
            ]
        )

    async def record_interest_started_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        interest_type: str,
        details: Mapping[str, Any] | None = None,
        batch_id: str | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        return await self.record_interest_added_many(
            worker_name=worker_name,
            qids=qids,
            interest_type=interest_type,
            details=details,
            batch_id=batch_id,
            timestamp=timestamp,
        )

    async def record_interest_expired_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        interest_type: str,
        details: Mapping[str, Any] | None = None,
        batch_id: str | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        return await self.record_interest_removed_many(
            worker_name=worker_name,
            qids=qids,
            interest_type=interest_type,
            details=details,
            batch_id=batch_id,
            timestamp=timestamp,
        )

    async def record_work_claimed_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        work_reason: str,
        batch_id: str,
        details: Mapping[str, Any] | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        payload = {"work_reason": work_reason}
        if details:
            payload.update(details)
        return await self.record_events(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type="work_claimed",
                    worker_name=worker_name,
                    batch_id=batch_id,
                    details=payload,
                    timestamp=timestamp,
                )
                for qid in qids
            ]
        )

    async def record_results_written_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        batch_id: str,
        details: Mapping[str, Any] | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        payload = {"writeback": True}
        if details:
            payload.update(details)
        return await self.record_events(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type="results_written",
                    worker_name=worker_name,
                    batch_id=batch_id,
                    details=payload,
                    timestamp=timestamp,
                )
                for qid in qids
            ]
        )

    async def record_work_abandoned_many(
        self,
        *,
        worker_name: str,
        qids: Sequence[str | int],
        abandon_reason: str,
        batch_id: str | None = None,
        details: Mapping[str, Any] | None = None,
        timestamp: int | float | None = None,
    ) -> int:
        payload = {"abandon_reason": abandon_reason}
        if details:
            payload.update(details)
        return await self.record_events(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type="work_abandoned",
                    worker_name=worker_name,
                    batch_id=batch_id,
                    details=payload,
                    timestamp=timestamp,
                )
                for qid in qids
            ]
        )

    async def list_events(
        self,
        *,
        qid: str | int | None = None,
        since: int | None = None,
        until: int | None = None,
        event_types: Sequence[str] | None = None,
        worker_names: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        await self.cache.initialize()

        clauses = ["1=1"]
        params: list[Any] = []
        if qid is not None:
            qid_num = self.cache._parse_qid(qid)
            if qid_num is None:
                return []
            clauses.append("qid = ?")
            params.append(qid_num)
        if since is not None:
            clauses.append("ts >= ?")
            params.append(_to_utc_datetime(since))
        if until is not None:
            clauses.append("ts <= ?")
            params.append(_to_utc_datetime(until))
        if event_types:
            normalized_types = [self._normalize_event_type(event_type) for event_type in event_types if str(event_type).strip()]
            if normalized_types:
                placeholders = ", ".join("?" for _ in normalized_types)
                clauses.append(f"event_type IN ({placeholders})")
                params.extend(normalized_types)
        if worker_names:
            normalized_workers = [self._normalize_worker_name(worker_name) for worker_name in worker_names if str(worker_name).strip()]
            if normalized_workers:
                placeholders = ", ".join("?" for _ in normalized_workers)
                clauses.append(f"worker_name IN ({placeholders})")
                params.extend(normalized_workers)

        sql = (
            "SELECT ts, qid, event_type, worker_name, batch_id, details "
            "FROM item_trace_events "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY ts ASC, qid ASC, event_type ASC, worker_name ASC"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        async with self.cache._connect() as db:
            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()

        results: list[dict[str, Any]] = []
        for ts, qid_num, event_type, worker_name, batch_id, details in rows:
            if isinstance(details, bytes):
                details = details.decode("utf-8")
            try:
                payload = json.loads(details)
            except (TypeError, ValueError):
                payload = {}
            if not isinstance(payload, dict):
                payload = {"value": payload}
            results.append(
                {
                    "timestamp": _to_iso_datetime(ts),
                    "qid": f"Q{int(qid_num)}",
                    "event_type": str(event_type),
                    "worker_name": str(worker_name),
                    "batch_id": None if batch_id is None else str(batch_id),
                    "details": payload,
                }
            )
        return results

    async def count_events(
        self,
        *,
        qid: str | int | None = None,
        since: int | None = None,
        until: int | None = None,
        event_types: Sequence[str] | None = None,
        worker_names: Sequence[str] | None = None,
    ) -> int:
        if not self.enabled:
            return 0
        await self.cache.initialize()

        clauses = ["1=1"]
        params: list[Any] = []
        if qid is not None:
            qid_num = self.cache._parse_qid(qid)
            if qid_num is None:
                return 0
            clauses.append("qid = ?")
            params.append(qid_num)
        if since is not None:
            clauses.append("ts >= ?")
            params.append(_to_utc_datetime(since))
        if until is not None:
            clauses.append("ts <= ?")
            params.append(_to_utc_datetime(until))
        if event_types:
            normalized_types = [
                self._normalize_event_type(event_type)
                for event_type in event_types
                if str(event_type).strip()
            ]
            if normalized_types:
                placeholders = ", ".join("?" for _ in normalized_types)
                clauses.append(f"event_type IN ({placeholders})")
                params.extend(normalized_types)
        if worker_names:
            normalized_workers = [
                self._normalize_worker_name(worker_name)
                for worker_name in worker_names
                if str(worker_name).strip()
            ]
            if normalized_workers:
                placeholders = ", ".join("?" for _ in normalized_workers)
                clauses.append(f"worker_name IN ({placeholders})")
                params.extend(normalized_workers)

        sql = (
            "SELECT COUNT(*) "
            "FROM item_trace_events "
            f"WHERE {' AND '.join(clauses)}"
        )

        async with self.cache._connect() as db:
            cursor = await db.execute(sql, params)
            row = await cursor.fetchone()

        if not row:
            return 0
        return int(row[0] or 0)

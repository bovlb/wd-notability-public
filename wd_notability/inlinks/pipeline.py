from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from time import time
from types import SimpleNamespace
from typing import Any, Sequence

from wd_notability.evaluation_cache import EvaluationCache
from wd_notability.inlinks.blackboard import InlinksBlackboard
from wd_notability.inlinks import db_read as inlinks_db_read
from wd_notability.inlinks.source import INLINKS_SOURCE
from wd_notability.item_trace import ITEM_TRACE_ENABLED, ItemTraceRecord
from wd_notability.models import NotabilityLevel, deduce_n2

logger = logging.getLogger(__name__)

# Poll interval for refreshing inlinks interest from the cache.
INLINKS_POLL_SECONDS = 1.0
# Minimum age before a cached inlinks count is treated as stale.
INLINKS_COUNTS_REFRESH_SECONDS = 10.0
# Minimum age before a cached inlinks graph is treated as stale.
INLINKS_GRAPH_REFRESH_SECONDS = 10.0
# Minimum age before an inlinks evaluation is revisited.
INLINKS_EVALUATION_REFRESH_SECONDS = 1.0
# Batch size for fetching inlinks counts.
INLINKS_COUNT_BATCH_SIZE = 1000
# Wait time before re-checking inlinks counts.
INLINKS_COUNTS_WAIT_SECONDS = 1.0
# Wait time before re-checking inlinks graph.
INLINKS_GRAPH_WAIT_SECONDS = 1.0
# Grace period for inlinks interest before it is considered stale.
INLINKS_INTEREST_GRACE_SECONDS = 1.0
# Wait time before re-checking inlinks evaluations.
INLINKS_EVALUATION_WAIT_SECONDS = 1.0
# Maximum items the interest publisher emits per poll.
INLINKS_INTEREST_PUBLISH_LIMIT = 10_000

cache = EvaluationCache()


@dataclass(slots=True)
class InlinksPipelineStats:
    interest_changed: int = 0
    counts_changed: int = 0
    graph_updated: int = 0
    evaluated: int = 0
    published: int = 0


def _trace_records(
    qids: list[str],
    *,
    event_type: str,
    details: dict[str, object] | None = None,
) -> list[ItemTraceRecord]:
    if not ITEM_TRACE_ENABLED:
        return []
    return [
        ItemTraceRecord(
            qid=qid,
            event_type=event_type,
            worker_name="inlinks",
            details=details,
        )
        for qid in qids
    ]


def _graph_fetched_details(inlinks: Sequence[str]) -> dict[str, object]:
    details: dict[str, object] = {
        "graph_size": len(inlinks),
    }
    if len(inlinks) <= 5:
        details["inlinks"] = list(inlinks)
    return details


def _published_item_qids_details(*, item_inlinks: Sequence[str], published_qids: set[str]) -> dict[str, object]:
    published_item_qids = [
        qid for qid in item_inlinks if qid in published_qids]
    details: dict[str, object] = {
        "published_qid_count": len(published_item_qids),
    }
    if len(published_item_qids) <= 5:
        details["published_qids"] = published_item_qids
    return details


def _interest_started_details(entry) -> dict[str, object]:
    details: dict[str, object] = {
        "interest_type": "inlinks",
        "source": "interest",
    }
    if entry is None:
        return details
    if entry.inlinks_count is not None:
        details["prior_inlinks_count"] = int(entry.inlinks_count)
    if entry.n3_inlinks is not None:
        details["prior_n3_inlinks"] = int(entry.n3_inlinks)
        details["prior_n3_inlinks_label"] = str(entry.n3_inlinks)
    return details


async def _emit_trace(
    qids: list[str],
    *,
    event_type: str,
    details: dict[str, object] | None = None,
    error_message: str,
) -> None:
    if not qids or not ITEM_TRACE_ENABLED:
        return
    try:
        await cache.item_trace.record_events(
            _trace_records(qids, event_type=event_type, details=details)
        )
        await cache.item_trace.flush()
    except Exception:
        logger.exception(error_message)


async def _emit_trace_records(
    records: list[ItemTraceRecord],
    *,
    error_message: str,
) -> None:
    if not records or not ITEM_TRACE_ENABLED:
        return
    try:
        await cache.item_trace.record_events(records)
        await cache.item_trace.flush()
    except Exception:
        logger.exception(error_message)


async def _get_inlinks_n12_many(qids: list[str]) -> dict[str, NotabilityLevel]:
    qid_nums: list[int] = []
    qid_lookup: dict[int, str] = {}
    for qid in qids:
        if not (isinstance(qid, str) and qid.startswith("Q") and qid[1:].isdigit()):
            continue
        qid_num = int(qid[1:])
        if qid_num in qid_lookup:
            continue
        qid_nums.append(qid_num)
        qid_lookup[qid_num] = qid

    if not qid_nums:
        return {}

    chunk_size = 500
    levels: dict[str, NotabilityLevel] = {}
    async with cache._connect() as db:
        for start in range(0, len(qid_nums), chunk_size):
            chunk = qid_nums[start: start + chunk_size]
            values_sql = " UNION ALL ".join(
                ["SELECT ? AS qid", *(["SELECT ?"] * (len(chunk) - 1))]
            )
            cursor = await db.execute(
                f"""
                WITH qids AS (
                    {values_sql}
                )
                SELECT
                    q.qid,
                    ce.qid,
                    ce.n1,
                    ce.n2a,
                    ce.n2b
                FROM qids q
                LEFT JOIN content_evaluation ce
                  ON ce.qid = q.qid
                ORDER BY q.qid
                """,
                chunk,
            )
            for row in await cursor.fetchall():
                qid_num = int(row[0])
                qid_text = qid_lookup.get(qid_num)
                if qid_text is None:
                    continue
                if row[1] is None:
                    levels[qid_text] = NotabilityLevel.UNKNOWN
                    continue

                n1 = NotabilityLevel(
                    int(row[2])) if row[2] is not None else NotabilityLevel.UNKNOWN
                n2a = NotabilityLevel(
                    int(row[3])) if row[3] is not None else NotabilityLevel.UNKNOWN
                n2b = NotabilityLevel(
                    int(row[4])) if row[4] is not None else NotabilityLevel.UNKNOWN
                n2 = deduce_n2(n2a, n2b)
                levels[qid_text] = max(n1, n2)

    return levels


async def _list_interest_inlinks_rows(limit: int | None = None) -> list[tuple[str, int, int | None, int | None, int | None]]:
    return await inlinks_db_read.list_pubsub_inlinks_targets_with_state(cache, limit=limit)


async def _write_zero_evaluations(
    rows: Sequence[tuple[str, int]],
    *,
    observed_at: int,
) -> None:
    if not rows:
        return

    await cache.upsert_inlinks_many(
        [
            SimpleNamespace(
                qid=qid,
                n3_inlinks=NotabilityLevel.NONE,
                inlinks_count=count,
                inlinks_last_evaluated=observed_at,
            )
            for qid, count in rows
        ]
    )
    await cache.item_trace.record_events(
        [
            ItemTraceRecord(
                qid=qid,
                event_type="results_written",
                worker_name="inlinks",
                details={
                    "n3_inlinks": int(NotabilityLevel.NONE),
                    "n3_inlinks_label": str(NotabilityLevel.NONE),
                    "inlinks_count": count,
                    "inlinks_last_evaluated": observed_at,
                },
            )
            for qid, count in rows
        ]
    )
    await cache.item_trace.flush()


async def _fetch_graph_candidates(
    pipeline: "InlinksPipeline",
    candidates: list[str],
) -> tuple[bool, int, int, list[str]]:
    changed = False
    refreshed = 0
    observed_at = int(time())
    snapshot = pipeline.blackboard.snapshot()
    batch: list[str] = []
    batch_total_count = 0
    calls = 0
    empty_qids: list[str] = []

    async def flush_batch() -> None:
        nonlocal changed, refreshed, batch, batch_total_count, calls, observed_at
        if not batch:
            return
        processed_batch = list(batch)
        calls += 1
        try:
            await _emit_trace(
                processed_batch,
                event_type="work_claimed",
                details={
                    "work_reason": "fetch_inlinks",
                    "chunk_size": len(processed_batch),
                },
                error_message="Item trace emit failed while recording inlinks graph work claim",
            )
            contexts = await INLINKS_SOURCE.get_contexts(processed_batch)
        except Exception:
            batch = []
            batch_total_count = 0
            return

        refreshed += len(batch)
        empty_graphs: list[tuple[str, int]] = []
        for batch_qid in batch:
            context = contexts.get(batch_qid) or {}
            inlinks = list(context.get("inlinks", []))
            truncated = bool(context.get("truncated", False))
            count = len(inlinks)
            current = snapshot.get(batch_qid)
            if truncated and current is not None and current.inlinks_count is not None:
                count = int(current.inlinks_count)
            changed = pipeline.blackboard.record_count(
                batch_qid,
                count,
                observed_at=observed_at,
            ) or changed
            if count <= 0:
                empty_graphs.append((batch_qid, 0))
                continue
            changed = pipeline.blackboard.record_graph(
                batch_qid,
                inlinks,
                observed_at=observed_at,
                truncated=truncated,
            ) or changed
        for empty_qid, count in empty_graphs:
            empty_qids.append(empty_qid)
            changed = pipeline.blackboard.record_empty_graph(
                empty_qid,
                observed_at=observed_at,
            ) or changed
        logger.info(
            "Inlinks graph chunk processed %s candidate(s): %s graph(s), %s empty graph(s)",
            len(processed_batch),
            len(processed_batch) - len(empty_graphs),
            len(empty_graphs),
        )
        print(
            f"Inlinks graph_fetcher chunk: candidates={len(processed_batch)}, graphs={len(processed_batch) - len(empty_graphs)}, empty_graphs={len(empty_graphs)}")
        await _write_zero_evaluations(empty_graphs, observed_at=observed_at)

        if batch:
            non_empty_qids = [batch_qid for batch_qid in batch if batch_qid not in {
                qid for qid, _count in empty_graphs}]
            if non_empty_qids:
                await _emit_trace_records(
                    [
                        ItemTraceRecord(
                            qid=qid,
                            event_type="graph_fetched",
                            worker_name="inlinks",
                            details=_graph_fetched_details(
                                list((contexts.get(qid) or {}).get("inlinks", []))
                            ),
                        )
                        for qid in non_empty_qids
                    ],
                    error_message="Item trace emit failed while recording inlinks graphs",
                )
        if empty_graphs:
            await _emit_trace_records(
                [
                    ItemTraceRecord(
                        qid=qid,
                        event_type="graph_fetched",
                        worker_name="inlinks",
                        details={"graph_size": 0, "empty_graph": True},
                    )
                    for qid, _count in empty_graphs
                ],
                error_message="Item trace emit failed while recording inlinks graphs",
            )
        if processed_batch or empty_graphs:
            pipeline.stats.graph_updated += 1
            pipeline.graph_updated.set()
        batch = []
        batch_total_count = 0

    for qid in candidates:
        entry = snapshot.get(qid)
        if entry is None or entry.inlinks_count is None or entry.inlinks_count <= 0:
            continue
        count = int(entry.inlinks_count)
        if batch and batch_total_count + count > 10_000:
            await flush_batch()
        batch.append(qid)
        batch_total_count += count
        if batch_total_count >= 10_000:
            await flush_batch()

    await flush_batch()

    return changed, refreshed, calls, empty_qids


class InlinksPipeline:
    def __init__(self) -> None:
        self.blackboard = InlinksBlackboard()
        self.interest_changed = asyncio.Event()
        self.counts_changed = asyncio.Event()
        self.graph_changed = asyncio.Event()
        self.graph_updated = asyncio.Event()
        self._stop = asyncio.Event()
        self.stats = InlinksPipelineStats()
        self._last_published_qids: tuple[str, ...] = ()

    def stop(self) -> None:
        self._stop.set()

    async def _sleep_or_stop(self, seconds: float) -> float:
        start_time = int(time())
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            wait_time = int(time()) - start_time
            return wait_time
        finally:
            wait_time = int(time()) - start_time
            return wait_time

    async def _wait_or_tick(self, event: asyncio.Event, seconds: float) -> bool:
        try:
            start_time = int(time())
            await asyncio.wait_for(event.wait(), timeout=seconds)
        except TimeoutError:
            return False, int(time()) - start_time
        wait_time = int(time()) - start_time
        event.clear()
        return True, wait_time

    async def interest_fetcher(self) -> None:
        while not self._stop.is_set():
            try:
                start_time = int(time())
                observed_at = int(time())
                rows = await _list_interest_inlinks_rows()
                interest_changed, new_qids = self.blackboard.apply_interest_rows(
                    rows, observed_at=observed_at)
                pruned_qids = self.blackboard.prune_stale_interest(
                    stale_after_seconds=INLINKS_INTEREST_GRACE_SECONDS,
                    observed_at=observed_at,
                )
                changed = interest_changed or bool(pruned_qids)
                if new_qids:
                    snapshot = self.blackboard.snapshot()
                    await _emit_trace_records(
                        [
                            ItemTraceRecord(
                                qid=qid,
                                event_type="interest_started",
                                worker_name="inlinks",
                                details=_interest_started_details(
                                    snapshot.get(qid)),
                            )
                            for qid in new_qids
                        ],
                        error_message="Item trace emit failed while recording inlinks interest",
                    )
                if pruned_qids:
                    await _emit_trace(
                        list(pruned_qids),
                        event_type="interest_expired",
                        details={
                            "interest_type": "inlinks",
                            "source": "interest",
                        },
                        error_message="Item trace emit failed while recording inlinks interest expiry",
                    )
                if interest_changed or pruned_qids:
                    self.stats.interest_changed += 1
                    logger.info(
                        "Inlinks interest updated: %s active target(s), %s change event(s) seen",
                        len(rows),
                        self.stats.interest_changed,
                    )
                    self.interest_changed.set()
                if changed:
                    self.graph_changed.set()
                print(
                    f"Inlinks interest_fetcher: changed={changed}, qids={len(rows)}, new_qids={len(new_qids)}, expired_qids={len(pruned_qids)}, duration={int(time()) - start_time}")
            except Exception:
                logger.exception("Inlinks interest_fetcher failed")
            sleep_time = await self._sleep_or_stop(INLINKS_POLL_SECONDS)
            print(f"Inlinks interest_fetcher: sleep_time={sleep_time}")

    async def counts_fetcher(self) -> None:
        while not self._stop.is_set():
            start_time = int(time())
            wait_result = await self._wait_or_tick(self.interest_changed, INLINKS_COUNTS_WAIT_SECONDS)
            wait_time = wait_result[1] if isinstance(wait_result, tuple) else 0
            candidates = self.blackboard.count_candidates(
                stale_after_seconds=INLINKS_COUNTS_REFRESH_SECONDS)
            if not candidates:
                continue

            changed = False
            refreshed = 0
            calls = 0
            counts_seen = 0
            zero_count = 0
            for start in range(0, len(candidates), INLINKS_COUNT_BATCH_SIZE):
                chunk = candidates[start:start + INLINKS_COUNT_BATCH_SIZE]
                try:
                    await _emit_trace(
                        list(chunk),
                        event_type="work_claimed",
                        details={
                            "work_reason": "count_inlinks",
                            "chunk_size": len(chunk),
                        },
                        error_message="Item trace emit failed while recording inlinks count work claim",
                    )
                    counts, _timings = await INLINKS_SOURCE.count_inlinks(chunk)
                    print(
                        f"Inlinks counts_fetcher: fetched counts: {_timings}")
                except Exception:
                    logger.exception("Inlinks counts_fetcher failed")
                    continue
                calls += 1
                refreshed += len(chunk)
                observed_at = int(time())
                zero_rows: list[tuple[str, int]] = []
                for qid, count in counts.items():
                    counts_seen += 1
                    count_value = int(count)
                    if count_value <= 0:
                        zero_count += 1
                        changed = self.blackboard.record_count(
                            qid, count_value, observed_at=observed_at) or changed
                        self.blackboard.mark_evaluated(
                            qid,
                            observed_at=observed_at,
                            n3_inlinks=NotabilityLevel.NONE,
                        )
                        zero_rows.append((qid, count_value))
                        continue
                    changed = self.blackboard.record_count(
                        qid, count_value, observed_at=observed_at) or changed
                logger.info(
                    "Inlinks count chunk processed %s candidate(s): %s count(s), %s zero(s)",
                    len(chunk),
                    len(counts),
                    len(zero_rows),
                )
                print(
                    f"Inlinks counts_fetcher chunk: candidates={len(chunk)}, counts={len(counts)}, zeroes={len(zero_rows)}")
                await _write_zero_evaluations(
                    zero_rows,
                    observed_at=observed_at,
                )
                await _emit_trace_records(
                    [
                        ItemTraceRecord(
                            qid=qid,
                            event_type="count_fetched",
                            worker_name="inlinks",
                            details={"count": int(count)},
                        )
                        for qid, count in counts.items()
                        if int(count) > 0
                    ],
                    error_message="Item trace emit failed while recording inlinks counts",
                )
            if changed:
                self.stats.counts_changed += 1
                logger.info(
                    "Inlinks count refresh updated %s candidate(s); change event %s",
                    refreshed,
                    self.stats.counts_changed,
                )
                self.counts_changed.set()
                self.graph_changed.set()
            if refreshed:
                self.graph_updated.set()
            logger.info(
                "Inlinks count refresh finished: %s candidate(s) examined, %s count(s) observed, %s zero(s), %s chunk(s)",
                refreshed,
                counts_seen,
                zero_count,
                calls,
            )
            print(
                f"Inlinks counts_fetcher: changed={changed}, refreshed={refreshed}, counts={counts_seen}, zeroes={zero_count}, calls={calls}, wait_time={wait_time}, duration={int(time()) - start_time - wait_time}")

    async def graph_fetcher(self) -> None:
        while not self._stop.is_set():
            start_time = int(time())
            wait_result = await self._wait_or_tick(self.graph_changed, INLINKS_GRAPH_WAIT_SECONDS)
            wait_time = wait_result[1] if isinstance(wait_result, tuple) else 0
            candidates = self.blackboard.graph_candidates(
                stale_after_seconds=INLINKS_GRAPH_REFRESH_SECONDS)
            if not candidates:
                continue

            changed, refreshed, calls, _empty_qids = await _fetch_graph_candidates(self, candidates)
            if changed:
                logger.info(
                    "Inlinks graph refresh examined %s candidate(s)",
                    refreshed,
                )
            logger.info(
                "Inlinks graph refresh finished: %s candidate(s) examined, %s chunk(s)",
                refreshed,
                calls,
            )
            print(
                f"Inlinks graph_fetcher: changed={changed}, refreshed={refreshed}, calls={calls}, wait_time={wait_time}, duration={int(time()) - start_time - wait_time}")

    @staticmethod
    def _evaluate_target(
        qid: str,
        inlinks: list[str],
        cached_levels: dict[str, NotabilityLevel],
        *,
        truncated: bool = False,
    ) -> tuple[NotabilityLevel, NotabilityLevel, list[str]]:
        if not inlinks:
            return NotabilityLevel.NONE, NotabilityLevel.NONE, []

        best_level = NotabilityLevel.NONE
        unresolved = False
        unknown_inlinks: list[str] = []
        for inlink in inlinks:
            cached_level = cached_levels.get(inlink)
            if cached_level is None:
                unresolved = True
                continue
            if cached_level == NotabilityLevel.STRONG:
                return NotabilityLevel.STRONG, NotabilityLevel.STRONG, []
            if cached_level == NotabilityLevel.UNKNOWN:
                unresolved = True
                unknown_inlinks.append(inlink)
                continue
            best_level = max(best_level, cached_level)

        if unresolved and best_level == NotabilityLevel.NONE:
            return NotabilityLevel.UNKNOWN, best_level, unknown_inlinks
        if unresolved:
            return NotabilityLevel.UNKNOWN, best_level, unknown_inlinks
        # If we did not download all inlinks for this target, and it's not strong, mark it as unknown.
        if truncated and best_level != NotabilityLevel.STRONG:
            return NotabilityLevel.UNKNOWN, best_level, unknown_inlinks
        return best_level, best_level, []

    async def evaluator(self) -> None:
        while not self._stop.is_set():
            start_time = int(time())
            wait_result = await self._wait_or_tick(self.graph_updated, INLINKS_EVALUATION_WAIT_SECONDS)
            wait_time = wait_result[1] if isinstance(wait_result, tuple) else 0
            entries = self.blackboard.evaluation_candidates(
                stale_after_seconds=INLINKS_EVALUATION_REFRESH_SECONDS)
            if not entries:
                continue

            for entry in entries:
                await _emit_trace(
                    [entry.qid],
                    event_type="evaluation_attempted",
                    details={
                        "loaded_inlinks_count": len(entry.inlinks),
                        "truncated": bool(getattr(entry, "graph_truncated", False)),
                    },
                    error_message="Item trace emit failed while recording inlinks evaluation attempts",
                )
            all_inlinks: list[str] = []
            for entry in entries:
                all_inlinks.extend(entry.inlinks)
            unique_inlinks = []
            seen = set()
            for qid in all_inlinks:
                if qid in seen:
                    continue
                seen.add(qid)
                unique_inlinks.append(qid)
            cached_levels = await _get_inlinks_n12_many(unique_inlinks)

            updates = []
            evaluation_details: list[tuple[str,
                                           NotabilityLevel, NotabilityLevel, list[str]]] = []
            delete_qids: list[str] = []
            observed_at = int(time())
            for entry in entries:
                level, best_level, unknown_inlinks = self._evaluate_target(
                    entry.qid,
                    list(entry.inlinks),
                    cached_levels,
                    truncated=bool(getattr(entry, "graph_truncated", False)),
                )
                evaluation_details.append(
                    (entry.qid, level, best_level, unknown_inlinks))
                updates.append(
                    SimpleNamespace(
                        qid=entry.qid,
                        n3_inlinks=level,
                        inlinks_count=entry.inlinks_count or len(
                            entry.inlinks),
                        inlinks_last_evaluated=observed_at,
                    )
                )
                if level == NotabilityLevel.UNKNOWN:
                    delete_qids.append(entry.qid)
                self.blackboard.mark_evaluated(
                    entry.qid, observed_at=observed_at, n3_inlinks=level)

            if updates:
                await cache.upsert_inlinks_many(updates)
                self.stats.evaluated += len(updates)
                logger.info(
                    "Inlinks evaluation wrote %s update(s); total evaluated %s",
                    len(updates),
                    self.stats.evaluated,
                )
                try:
                    await cache.item_trace.record_events(
                        [
                            ItemTraceRecord(
                                qid=update.qid,
                                event_type="results_written",
                                worker_name="inlinks",
                                details={
                                    "n3_inlinks": int(update.n3_inlinks),
                                    "n3_inlinks_label": str(update.n3_inlinks),
                                    "inlinks_count": int(update.inlinks_count or 0),
                                    "inlinks_last_evaluated": int(update.inlinks_last_evaluated),
                                    **(
                                        {
                                            "best_level": int(best_level),
                                            "best_level_label": str(best_level),
                                            "unknown_inlinks": unknown_inlinks,
                                        }
                                        if update.n3_inlinks == NotabilityLevel.UNKNOWN
                                        else {}
                                    ),
                                },
                            )
                            for update, (_qid, _level, best_level, unknown_inlinks) in zip(
                                updates, evaluation_details, strict=True
                            )
                        ]
                    )
                    await cache.item_trace.flush()
                except Exception:
                    logger.exception(
                        "Item trace emit failed while writing inlinks updates")
            if delete_qids:
                await cache.delete_inlinks_many(delete_qids)
                logger.info(
                    "Inlinks evaluation marked %s item(s) unknown", len(delete_qids))

            print(
                f"Inlinks evaluation: wrote={len(updates)}, deleted={len(delete_qids)}, wait_time={wait_time}, duration={int(time()) - start_time - wait_time}")

    async def interest_publisher(self) -> None:
        manager = await cache.interest.create_interest_manager(
            worker_id="inlinks",
            priority=1,
            # This publisher emits dependency content interest for items that
            # are needed to resolve inlinks evaluation.
            wants_content=True,
            wants_inlinks=False,
        )
        session = manager.create_session()
        try:
            while not self._stop.is_set():
                start_time = int(time())
                qids = self.blackboard.interest_candidates(
                    limit=INLINKS_INTEREST_PUBLISH_LIMIT,
                )
                sleep_time = 0
                if not qids:
                    self._last_published_qids = ()
                    await session.clear()
                    sleep_time = await self._sleep_or_stop(INLINKS_POLL_SECONDS) or 0
                    continue

                current_qids = tuple(qids)
                if current_qids != self._last_published_qids:
                    self._last_published_qids = current_qids
                    published_qids = self.blackboard.all_inlinks()
                    await session.replace(published_qids)
                    snapshot = self.blackboard.snapshot()
                    trace_records: list[ItemTraceRecord] = []
                    published_qids_set = set(published_qids)
                    for qid in qids:
                        entry = snapshot.get(qid)
                        item_inlinks = entry.inlinks if entry is not None else ()
                        details = _published_item_qids_details(
                            item_inlinks=item_inlinks,
                            published_qids=published_qids_set,
                        )
                        if details["published_qid_count"] == 0:
                            continue
                        trace_records.append(
                            ItemTraceRecord(
                                qid=qid,
                                event_type="interest_published",
                                worker_name="inlinks",
                                details=details,
                            )
                        )
                    await _emit_trace_records(
                        trace_records,
                        error_message="Item trace emit failed while recording inlinks published interest",
                    )

                self.stats.published += 1
                sleep_time2 = await self._sleep_or_stop(INLINKS_POLL_SECONDS) or 0
                print(
                    f"Inlinks interest_publisher: published={len(qids)}, sleep={sleep_time}, sleep2={sleep_time2}, duration={int(time()) - start_time - sleep_time - sleep_time2}")
        except Exception:
            logger.exception("Inlinks interest_publisher failed")
            raise
        finally:
            await session.close()
            await manager.close()

    async def run(self) -> InlinksPipelineStats:
        await asyncio.gather(
            self.interest_fetcher(),
            self.counts_fetcher(),
            self.graph_fetcher(),
            self.evaluator(),
            self.interest_publisher(),
        )
        return self.stats


async def run_inlinks_pass(limit: int | None = None) -> int:
    pipeline = InlinksPipeline()
    rows = await _list_interest_inlinks_rows(limit=limit)
    if not rows:
        return 0

    qids = [qid for qid, *_rest in rows]
    pipeline.blackboard.apply_interest_rows(rows, observed_at=int(time()))
    snapshot = pipeline.blackboard.snapshot()
    immediate_qids: set[str] = set()
    await _emit_trace_records(
        [
            ItemTraceRecord(
                qid=qid,
                event_type="interest_started",
                worker_name="inlinks",
                details=_interest_started_details(snapshot.get(qid)),
            )
            for qid in qids
        ],
        error_message="Item trace emit failed while recording inlinks interest",
    )

    count_candidates = pipeline.blackboard.count_candidates(
        stale_after_seconds=0)
    if limit is not None:
        count_candidates = count_candidates[:limit]
    if count_candidates:
        await _emit_trace(
            list(count_candidates),
            event_type="work_claimed",
            details={
                "work_reason": "count_inlinks",
                "chunk_size": len(count_candidates),
            },
            error_message="Item trace emit failed while recording inlinks count work claim",
        )
        counts, _timings = await INLINKS_SOURCE.count_inlinks(count_candidates)
        observed_at = int(time())
        for qid, count in counts.items():
            count_value = int(count)
            if count_value <= 0:
                immediate_qids.add(qid)
                pipeline.blackboard.record_count(
                    qid, count_value, observed_at=observed_at)
                pipeline.blackboard.mark_evaluated(
                    qid,
                    observed_at=observed_at,
                    n3_inlinks=NotabilityLevel.NONE,
                )
                continue
            pipeline.blackboard.record_count(
                qid, count_value, observed_at=observed_at)
        await _write_zero_evaluations(
            [(qid, int(count))
             for qid, count in counts.items() if int(count) <= 0],
            observed_at=observed_at,
        )
        await _emit_trace_records(
            [
                ItemTraceRecord(
                    qid=qid,
                    event_type="count_fetched",
                    worker_name="inlinks",
                    details={"count": int(count)},
                )
                for qid, count in counts.items()
                if int(count) > 0
            ],
            error_message="Item trace emit failed while recording inlinks counts",
        )
        logger.info(
            "Inlinks pass counted %s candidate(s): %s count(s), %s zero(s)",
            len(count_candidates),
            len(counts),
            sum(1 for count in counts.values() if int(count) <= 0),
        )

    graph_candidates = pipeline.blackboard.graph_candidates(
        stale_after_seconds=0)
    if limit is not None:
        graph_candidates = graph_candidates[:limit]
    if graph_candidates:
        _, _refreshed, _calls, empty_qids = await _fetch_graph_candidates(pipeline, graph_candidates)
        immediate_qids.update(empty_qids)

    entries = pipeline.blackboard.evaluation_candidates(
        stale_after_seconds=INLINKS_EVALUATION_REFRESH_SECONDS)
    if limit is not None:
        entries = entries[:limit]
    if immediate_qids:
        entries = [entry for entry in entries if entry.qid not in immediate_qids]
    if not entries:
        return len(qids)

    for entry in entries:
        await _emit_trace(
            [entry.qid],
            event_type="evaluation_attempted",
            details={
                "loaded_inlinks_count": len(entry.inlinks),
                "truncated": bool(getattr(entry, "graph_truncated", False)),
            },
            error_message="Item trace emit failed while recording inlinks evaluation attempts",
        )
    all_inlinks: list[str] = []
    for entry in entries:
        all_inlinks.extend(entry.inlinks)
    unique_inlinks: list[str] = []
    seen: set[str] = set()
    for qid in all_inlinks:
        if qid in seen:
            continue
        seen.add(qid)
        unique_inlinks.append(qid)
    cached_levels = await _get_inlinks_n12_many(unique_inlinks)

    updates = []
    evaluation_details: list[tuple[str,
                                   NotabilityLevel, NotabilityLevel, list[str]]] = []
    for entry in entries:
        level, best_level, unknown_inlinks = pipeline._evaluate_target(
            entry.qid,
            list(entry.inlinks),
            cached_levels,
            truncated=bool(getattr(entry, "graph_truncated", False)),
        )
        evaluation_details.append(
            (entry.qid, level, best_level, unknown_inlinks))
        updates.append(
            SimpleNamespace(
                qid=entry.qid,
                n3_inlinks=level,
                inlinks_count=entry.inlinks_count or len(entry.inlinks),
                inlinks_last_evaluated=int(time()),
            )
        )
        pipeline.blackboard.mark_evaluated(
            entry.qid,
            observed_at=updates[-1].inlinks_last_evaluated,
            n3_inlinks=level,
        )
    if updates:
        await cache.upsert_inlinks_many(updates)
        await cache.item_trace.record_events(
            [
                ItemTraceRecord(
                    qid=update.qid,
                    event_type="results_written",
                    worker_name="inlinks",
                    details={
                        "n3_inlinks": int(update.n3_inlinks),
                        "n3_inlinks_label": str(update.n3_inlinks),
                        "inlinks_count": int(update.inlinks_count or 0),
                        "inlinks_last_evaluated": int(update.inlinks_last_evaluated),
                        **(
                            {
                                "best_level": int(best_level),
                                "best_level_label": str(best_level),
                                "unknown_inlinks": unknown_inlinks,
                            }
                            if update.n3_inlinks == NotabilityLevel.UNKNOWN
                            else {}
                        ),
                    },
                )
                for update, (_qid, _level, best_level, unknown_inlinks) in zip(
                    updates, evaluation_details, strict=True
                )
            ]
        )
        await cache.item_trace.flush()
    return len(entries)


async def run_inlinks_pipeline() -> InlinksPipelineStats:
    pipeline = InlinksPipeline()
    return await pipeline.run()

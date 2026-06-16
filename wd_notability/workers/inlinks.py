from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from wd_notability.evaluation_cache import CACHE
from wd_notability.evaluate import wait_for_foreground_evaluations
from wd_notability.file_lock import acquire_file_lock
from wd_notability.models import EvaluationResult, NotabilityLevel, QID
from wd_notability.sources import INLINKS_SOURCE

INLINKS_VISIBLE_LIMIT = 100
INLINKS_WATCH_WINDOW_SIZE = 10
INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE = 50
INLINKS_WORKER_CACHE_ONLY_COOLDOWN_SECONDS = 900
INLINKS_WORKER_RUN_INTERVAL_SECONDS = 600
INLINKS_WORKER_EVENT_POLL_SECONDS = 2.0
INLINKS_WORKER_SESSION_TTL_SECONDS = 3600
INLINKS_WORKER_LOCK_TARGET = Path(__file__).resolve().parents[2] / "data" / "inlinks_worker"

INLINKS_CACHE_ONLY_LAST_CHECKED: dict[QID, float] = {}
INLINKS_CACHE_ONLY_LOCK = asyncio.Lock()
INLINKS_TARGET_STATES: dict[QID, "InlinksTargetState"] = {}


@dataclass(frozen=True)
class InlinksUpdate:
    qid: QID
    n3_inlinks: NotabilityLevel


@dataclass
class InlinksTargetState:
    target_qid: QID
    visible_inlinks: list[QID]
    truncated: bool
    watched_inlinks: list[QID] = field(default_factory=list)
    queued_entitydata: set[QID] = field(default_factory=set)
    cursor: int = 0


@dataclass(frozen=True)
class InlinksDecision:
    final_level: NotabilityLevel | None
    unresolved: list[QID]
    keep_state: bool


def _chunked(values: list[QID], size: int) -> list[list[QID]]:
    if size < 1:
        raise ValueError("size must be at least 1")
    return [values[index : index + size] for index in range(0, len(values), size)]


def _normalize_qid_list(values: Iterable[object]) -> list[QID]:
    qids: list[QID] = []
    seen: set[QID] = set()
    for value in values:
        if not isinstance(value, str) or not value.startswith("Q") or not value[1:].isdigit():
            continue
        if value in seen:
            continue
        seen.add(value)
        qids.append(value)
    return qids


async def _mark_cache_only_checked(qids: list[QID]) -> None:
    now = time.monotonic()
    async with INLINKS_CACHE_ONLY_LOCK:
        for qid in qids:
            INLINKS_CACHE_ONLY_LAST_CHECKED[qid] = now


async def _filter_recent_cache_only_qids(qids: list[QID]) -> list[QID]:
    cutoff = time.monotonic() - INLINKS_WORKER_CACHE_ONLY_COOLDOWN_SECONDS
    async with INLINKS_CACHE_ONLY_LOCK:
        stale_qids = [qid for qid, checked_at in INLINKS_CACHE_ONLY_LAST_CHECKED.items() if checked_at < cutoff]
        for qid in stale_qids:
            INLINKS_CACHE_ONLY_LAST_CHECKED.pop(qid, None)
        return [qid for qid in qids if INLINKS_CACHE_ONLY_LAST_CHECKED.get(qid, 0.0) < cutoff]


def _snapshot_from_context(qid: QID, context: dict[str, object]) -> InlinksTargetState:
    raw_inlinks = context.get("inlinks", [])
    if not isinstance(raw_inlinks, list):
        raw_inlinks = []

    visible_inlinks: list[QID] = []
    seen: set[QID] = set()
    for inlink in raw_inlinks:
        if not isinstance(inlink, str) or not inlink.startswith("Q") or not inlink[1:].isdigit():
            continue
        if inlink == qid or inlink in seen:
            continue
        seen.add(inlink)
        visible_inlinks.append(inlink)

    return InlinksTargetState(
        target_qid=qid,
        visible_inlinks=visible_inlinks,
        truncated=bool(context.get("truncated", False)),
    )


def _evaluate_snapshot(
    state: InlinksTargetState,
    cached_rows: dict[QID, tuple[int, int | None, int | None]],
) -> InlinksDecision:
    if not state.visible_inlinks:
        return InlinksDecision(
            final_level=NotabilityLevel.NONE if not state.truncated else None,
            unresolved=[],
            keep_state=False,
        )

    best_level = NotabilityLevel.NONE
    unresolved: list[QID] = []
    for inlink in state.visible_inlinks:
        cached_row = cached_rows.get(inlink)
        if cached_row is None:
            unresolved.append(inlink)
            continue

        cached_result = EvaluationResult.from_summary(qid=inlink, summary=cached_row[0])
        level = cached_result.n12
        if level == NotabilityLevel.UNKNOWN:
            unresolved.append(inlink)
            continue

        best_level = max(best_level, level)
        if level == NotabilityLevel.STRONG:
            return InlinksDecision(final_level=NotabilityLevel.STRONG, unresolved=unresolved, keep_state=False)

    if unresolved:
        return InlinksDecision(final_level=None, unresolved=unresolved, keep_state=True)

    if state.truncated:
        return InlinksDecision(final_level=None, unresolved=[], keep_state=False)

    return InlinksDecision(final_level=best_level, unresolved=[], keep_state=False)


def _next_watch_window(state: InlinksTargetState, unresolved: list[QID]) -> list[QID]:
    window: list[QID] = []
    for qid in unresolved:
        if qid in state.queued_entitydata:
            continue
        window.append(qid)
        if len(window) >= INLINKS_WATCH_WINDOW_SIZE:
            break
    return window


async def _create_state_session(state: InlinksTargetState, initial_window: list[QID]) -> None:
    await CACHE.pubsub.create_pubsub_session(
        owner_id="inlinks",
        session_id=state.target_qid,
        ttl_seconds=INLINKS_WORKER_SESSION_TTL_SECONDS,
        priority=1,
        wants_entitydata=False,
        wants_inlinks=False,
        wants_sync=False,
        qids=[],
    )
    if initial_window:
        await CACHE.pubsub.add_pubsub_session_qids(
            owner_id="inlinks",
            session_id=state.target_qid,
            qids=initial_window,
            priority=1,
            wants_entitydata=True,
            wants_inlinks=False,
            wants_sync=False,
        )
        state.watched_inlinks = list(initial_window)
        state.queued_entitydata.update(initial_window)


async def _drop_state(state: InlinksTargetState) -> None:
    await CACHE.pubsub.delete_pubsub_session(owner_id="inlinks", session_id=state.target_qid)
    INLINKS_TARGET_STATES.pop(state.target_qid, None)


async def _refresh_state(state: InlinksTargetState) -> None:
    await CACHE.pubsub.refresh_pubsub_session(
        owner_id="inlinks",
        session_id=state.target_qid,
        ttl_seconds=INLINKS_WORKER_SESSION_TTL_SECONDS,
    )


async def _poll_state_events(state: InlinksTargetState) -> list[dict[str, int | str | None]]:
    return await CACHE.pubsub.list_pubsub_events_for_session(
        owner_id="inlinks",
        session_id=state.target_qid,
        after_event_id=state.cursor,
        limit=500,
    )


async def _rebuild_states_from_pubsub() -> None:
    session_ids = await CACHE.pubsub.list_pubsub_session_ids(owner_id="inlinks")
    if not session_ids:
        return

    target_qids = _normalize_qid_list(session_ids)
    if not target_qids:
        return

    missing_target_qids = [qid for qid in target_qids if qid not in INLINKS_TARGET_STATES]
    if not missing_target_qids:
        return

    contexts = await INLINKS_SOURCE.get_contexts(missing_target_qids)
    for target_qid in missing_target_qids:
        context = contexts.get(target_qid)
        if not isinstance(context, dict):
            continue

        state = _snapshot_from_context(target_qid, context)
        watched_inlinks = await CACHE.pubsub.list_pubsub_session_qids(
            owner_id="inlinks",
            session_id=target_qid,
        )
        state.watched_inlinks = _normalize_qid_list(watched_inlinks)
        state.queued_entitydata.update(state.watched_inlinks)
        INLINKS_TARGET_STATES[target_qid] = state


async def _finalize_targets(final_updates: list[InlinksUpdate]) -> int:
    if not final_updates:
        return 0
    changed = await CACHE.upsert_inlinks_many(final_updates)
    return len(changed)


async def _initialize_new_targets(target_qids: list[QID]) -> tuple[int, list[InlinksUpdate]]:
    if not target_qids:
        return 0, []

    contexts = await INLINKS_SOURCE.get_contexts(target_qids)
    visible_union: list[QID] = []
    snapshots: dict[QID, InlinksTargetState] = {}
    for qid in target_qids:
        context = contexts.get(qid)
        if not isinstance(context, dict):
            continue
        state = _snapshot_from_context(qid, context)
        snapshots[qid] = state
        visible_union.extend(state.visible_inlinks)

    cached_rows = await CACHE.get_many(_normalize_qid_list(visible_union))
    final_updates: list[InlinksUpdate] = []
    processed = 0

    for qid in target_qids:
        state = snapshots.get(qid)
        if state is None:
            continue
        processed += 1
        decision = _evaluate_snapshot(state, cached_rows)

        if decision.final_level is not None:
            final_updates.append(InlinksUpdate(qid=qid, n3_inlinks=decision.final_level))
            continue

        if not decision.keep_state:
            continue

        initial_window = _next_watch_window(state, decision.unresolved)
        INLINKS_TARGET_STATES[qid] = state
        await _create_state_session(state, initial_window)

    return processed, final_updates


async def _reconcile_states(interested_targets: set[QID]) -> None:
    stale_targets = [qid for qid in list(INLINKS_TARGET_STATES) if qid not in interested_targets]
    for qid in stale_targets:
        await _drop_state(INLINKS_TARGET_STATES[qid])


async def _recompute_state_batch(target_qids: list[QID]) -> tuple[list[InlinksUpdate], list[QID]]:
    if not target_qids:
        return [], []

    union_visible = _normalize_qid_list(
        inlink
        for qid in target_qids
        for inlink in INLINKS_TARGET_STATES[qid].visible_inlinks
    )
    cached_rows = await CACHE.get_many(union_visible)

    final_updates: list[InlinksUpdate] = []
    finalized_targets: list[QID] = []

    for qid in target_qids:
        state = INLINKS_TARGET_STATES.get(qid)
        if state is None:
            continue

        decision = _evaluate_snapshot(state, cached_rows)
        if decision.final_level is not None:
            final_updates.append(InlinksUpdate(qid=qid, n3_inlinks=decision.final_level))
            finalized_targets.append(qid)
            continue

        if not decision.keep_state:
            finalized_targets.append(qid)
            continue

        window_complete = all(
            inlink in cached_rows
            and EvaluationResult.from_summary(qid=inlink, summary=cached_rows[inlink][0]).n12 != NotabilityLevel.UNKNOWN
            for inlink in state.watched_inlinks
        )
        if window_complete:
            next_window = _next_watch_window(state, decision.unresolved)
            if next_window:
                await CACHE.pubsub.add_pubsub_session_qids(
                    owner_id="inlinks",
                    session_id=qid,
                    qids=next_window,
                    priority=1,
                    wants_entitydata=True,
                    wants_inlinks=False,
                    wants_sync=False,
                )
                state.watched_inlinks = list(next_window)
                state.queued_entitydata.update(next_window)
            else:
                await _refresh_state(state)

    return final_updates, finalized_targets


async def _poll_active_states() -> list[QID]:
    affected: list[QID] = []
    for state in INLINKS_TARGET_STATES.values():
        events = await _poll_state_events(state)
        if not events:
            continue
        state.cursor = max(state.cursor, max(int(event["event_id"]) for event in events))
        affected.append(state.target_qid)
    return affected


async def work_inlinks_pass(batch_size: int = INLINKS_VISIBLE_LIMIT, limit: int | None = None) -> int:
    processed = 0

    await _rebuild_states_from_pubsub()

    interest_targets = await CACHE.pubsub.list_pubsub_inlinks_targets(limit=limit)
    interested_set = set(interest_targets)
    print(f"Inlinks worker loaded {len(interest_targets)} interested target qid(s)")

    await _reconcile_states(interested_set)

    new_targets = [qid for qid in interest_targets if qid not in INLINKS_TARGET_STATES]
    for batch in _chunked(new_targets, batch_size):
        batch_started = time.perf_counter()
        batch_processed, final_updates = await _initialize_new_targets(batch)
        if final_updates:
            processed += await _finalize_targets(final_updates)
        processed += batch_processed
        print(
            f"Inlinks batch initialized {batch_processed} target qid(s) "
            f"in {time.perf_counter() - batch_started:.2f}s"
        )

    affected_targets = await _poll_active_states()
    if affected_targets:
        final_updates, finalized_targets = await _recompute_state_batch(affected_targets)
        if final_updates:
            processed += await _finalize_targets(final_updates)
        for qid in finalized_targets:
            state = INLINKS_TARGET_STATES.get(qid)
            if state is not None:
                await _drop_state(state)
        print(
            f"Inlinks batch recomputed {len(affected_targets)} active target qid(s), "
            f"finalized {len(finalized_targets)}"
        )

    if not interest_targets and not INLINKS_TARGET_STATES:
        cache_only_limit = INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE if limit is None else min(limit, INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE)
        cache_only_candidates = await CACHE.list_unknown_inlinks_qids(limit=cache_only_limit * 4)
        cache_only_candidates = await _filter_recent_cache_only_qids(cache_only_candidates)
        if cache_only_candidates:
            batch = cache_only_candidates[:cache_only_limit]
            batch_started = time.perf_counter()
            batch_processed, final_updates = await _initialize_new_targets(batch)
            if final_updates:
                processed += await _finalize_targets(final_updates)
            processed += batch_processed
            print(
                f"Inlinks cache-only batch initialized {batch_processed} target qid(s) "
                f"in {time.perf_counter() - batch_started:.2f}s"
            )
            await _mark_cache_only_checked(batch)

    return processed


async def inlinks_worker_loop(
    *,
    batch_size: int = INLINKS_VISIBLE_LIMIT,
    run_interval_seconds: float = INLINKS_WORKER_RUN_INTERVAL_SECONDS,
) -> None:
    print("Inlinks worker starting; acquiring worker lock...")
    with acquire_file_lock(INLINKS_WORKER_LOCK_TARGET):
        print("Inlinks worker acquired lock; waiting for foreground evaluations...")
        while True:
            run_started = time.monotonic()
            try:
                await wait_for_foreground_evaluations()
                print("Inlinks worker foreground wait complete; running pass...")
                processed = await work_inlinks_pass(batch_size=batch_size)
                print(f"Inlinks worker processed {processed} candidate qid(s)")
            except Exception as exc:  # noqa: BLE001
                print(f"Inlinks worker failed: {exc}")

            sleep_for = max(0.0, run_interval_seconds - (time.monotonic() - run_started))
            await asyncio.sleep(sleep_for)


async def _work_inlinks_batch(qids: list[QID], *, cache_only: bool = False) -> tuple[int, int]:
    if not qids:
        return 0, 0

    contexts = await INLINKS_SOURCE.get_contexts(qids)
    processed_targets = 0
    finalized_targets = 0
    for qid in qids:
        context = contexts.get(qid)
        if not isinstance(context, dict):
            continue

        processed_targets += 1
        state = _snapshot_from_context(qid, context)
        decision = _evaluate_snapshot(
            state,
            await CACHE.get_many(state.visible_inlinks),
        )

        if decision.final_level is not None:
            await CACHE.upsert_inlinks_many([InlinksUpdate(qid=qid, n3_inlinks=decision.final_level)])
            finalized_targets += 1
            continue

        if cache_only or not decision.keep_state:
            continue

        # Compatibility helper used by tests; the worker loop uses the
        # stateful reconcile path above.
        initial_window = _next_watch_window(state, decision.unresolved)
        await CACHE.pubsub.create_pubsub_session(
            owner_id="inlinks",
            session_id=qid,
            ttl_seconds=INLINKS_WORKER_SESSION_TTL_SECONDS,
            priority=1,
            wants_entitydata=False,
            wants_inlinks=False,
            wants_sync=False,
            qids=[],
        )
        if initial_window:
            await CACHE.pubsub.add_pubsub_session_qids(
                owner_id="inlinks",
                session_id=qid,
                qids=initial_window,
                priority=1,
                wants_entitydata=True,
                wants_inlinks=False,
                wants_sync=False,
            )

    return processed_targets, finalized_targets

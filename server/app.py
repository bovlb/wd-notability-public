from __future__ import annotations
from server.home_page import router as home_router
from server.routes_api import router as api_router
from server.render_helpers import (
    _badge_field_value,
    _badge_level,
    _badge_payload,
    _badge_tooltip,
    _badge_tooltip_from_levels,
    _badge_tooltip_from_report,
    _cached_payload,
    _creator_history_payload,
    _group_subscription_qids_by_priority,
    _is_property_id,
    _is_qid_like,
    _is_valid_qid,
    _level_class,
    _normalize_creator_username,
    _normalize_owner_id,
    _normalize_qids,
    _normalize_subscription_items,
    _render_errors_cell,
    _render_properties_html,
    _render_property_value,
    _render_report_badge,
    _subscription_priority_for_reason,
    _wikidata_item_url,
)
from server.report_api import (
    _build_inlinks_scan_report,
    _cached_or_404,
    _cache_snapshot_payload,
    _compare_report_to_cache,
    _evaluate_live_reports,
    _evaluate_or_404,
    _external_usage_sets,
    _fetch_cached_snapshot,
    _fetch_interest_report,
    _fetch_queue_report,
    _item_link_html,
    _report_payload,
    _render_report_html,
    _utc_isoformat,
)

import asyncio
import hashlib
import json
import os
import time
from html import escape
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from server.schemas import (
    CreatorHistoryRequest,
    PubSubAddRequest,
    PubSubCreateRequest,
    PubSubRefreshRequest,
)

from wd_notability.evaluation_cache import CACHE
from wd_notability.item_trace import ITEM_TRACE_ENABLED, ItemTraceRecord
from wd_notability.evaluate import foreground_evaluation
from wd_notability.content.deletion import queue_stats as deletion_queue_stats
from wd_notability.content.worker import queue_stats as content_queue_stats
from wd_notability.inlinks import worker as inlinks_worker
from wd_notability.inlinks.worker import queue_stats as inlinks_queue_stats
from wd_notability.inlinks.source import INLINKS_SOURCE
from wd_notability.content.recent_changes import queue_stats as recent_changes_queue_stats
from wd_notability.lookup_cache import lookup_cache
from wd_notability.content.debug import build_signal_debug_payload as web_build_signal_debug_payload
from wd_notability.content.debug import render_signal_debug_html as web_render_signal_debug_html
from wd_notability.content.fetcher import CONTENT_SOURCE
from wd_notability.creations import CREATIONS
from wd_notability.external_usage.osm.source import OSM_SOURCE
from wd_notability.external_usage.sdc.source import SDC_SOURCE
from wd_notability.external_usage.wiki_subscribers.source import WIKI_USAGE_SOURCE
from wd_notability.web.creations import resolve_creation_bootstrap as web_resolve_creation_bootstrap
from wd_notability.wikidata import EntityDeletedError
from wd_notability.wikidata_api import close_wikidata_session
from server.page_renderers import (
    _render_observability_dashboard_html,
    _render_pubsub_debugger_html,
    _render_static_markdown_page,
)


REVALUATE_ON_SUBSCRIBE = True
SHUTDOWN_EVENT: asyncio.Event | None = None
SSE_STREAM_MAX_SECONDS = float(
    os.getenv("WD_NOTABILITY_SSE_STREAM_MAX_SECONDS", "60"))
PUBSUB_REAPER_TASK: asyncio.Task | None = None
PUBSUB_REAPER_INTERVAL_SECONDS = 60.0
PUBSUB_GADGET_SESSION_TTL_SECONDS = 3
PUBSUB_GADGET_SESSION_GRACE_SECONDS = 3.0
# PUBSUB_GADGET_SESSION_PURGE_SECONDS = 5.0
PUBSUB_GADGET_EVENT_CHUNK_SIZE = int(os.getenv("WD_NOTABILITY_GADGET_EVENT_CHUNK_SIZE", "100"))
PUBSUB_SUBSCRIPTION_SNAPSHOT_TTL_SECONDS = 3600.0
WEB_INTEREST_MANAGER: Any | None = None
WEB_INTEREST_MANAGER_LOCK = asyncio.Lock()
ACTIVE_STREAM_TASKS: set[asyncio.Task] = set()
ACTIVE_STREAM_TASKS_LOCK = asyncio.Lock()
GADGET_SUBSCRIPTION_TOUCHES: dict[str, float] = {}
GADGET_SUBSCRIPTION_TOUCHES_LOCK = asyncio.Lock()
PUBSUB_SUBSCRIPTION_QIDS: dict[str, tuple[float, tuple[str, ...]]] = {}
PUBSUB_SUBSCRIPTION_QIDS_LOCK = asyncio.Lock()
SUBSCRIPTIONS: dict[str, set[str]] = {}
_DEFAULT_WEB_RESOLVE_CREATION_BOOTSTRAP = web_resolve_creation_bootstrap


app = FastAPI(title="wd_notability")
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "favicon.ico",
        media_type="image/vnd.microsoft.icon",
    )


OBSERVABILITY_JS_VERSION = int(
    (STATIC_DIR / "observability.js").stat().st_mtime)
ITEM_TRACE_JS_VERSION = int(
    (STATIC_DIR / "item-trace.js").stat().st_mtime)

_cors_origins_raw = os.getenv("WD_NOTABILITY_CORS_ORIGINS")
if _cors_origins_raw is None or not _cors_origins_raw.strip():
    # Wikidata is the normal browser origin for the gadget; callers can still
    # override this with WD_NOTABILITY_CORS_ORIGINS when they need a different
    # development origin.
    _cors_origins = ["https://www.wikidata.org"]
else:
    _cors_origins = [origin.strip()
                     for origin in _cors_origins_raw.split(",") if origin.strip()]
    if not _cors_origins:
        _cors_origins = ["https://www.wikidata.org"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    allow_private_network=True,
)


DETECTED_CRITERIA = ("N1", "N2a", "N2b", "N3_inlinks",
                     "N3_osm", "N3_wikisub", "N3_sdc")
# Centralized label map to make future i18n straightforward.
DETECTED_CRITERION_LABELS = {
    "N1": "N1: Sitelinks",
    "N2a": "N2a: Identifiers",
    "N2b": "N2b: Sources",
    "N3_inlinks": "N3: Inlinks",
    "N3_osm": "N3: OSM",
    "N3_wikisub": "N3: Wiki subscribers",
    "N3_sdc": "N3: SDC",
}
BADGE_TOOLTIP_FIELDS = (
    ("N1", "N1 sitelinks"),
    ("N2a", "N2a identifiers"),
    ("N2b", "N2b sources"),
    ("N3", "N3 structural need"),
)
BADGE_TOOLTIP_N3_COMPONENTS = (
    ("N3_inlinks", "inlinks"),
    ("N3_osm", "OSM"),
    ("N3_wikisub", "wikisub"),
    ("N3_sdc", "SDC"),
)

EVALUATION_SOURCES = (
    CONTENT_SOURCE,
    INLINKS_SOURCE,
    OSM_SOURCE,
    SDC_SOURCE,
    WIKI_USAGE_SOURCE,
)

OBSERVABILITY_FIELD_METADATA = {
    "queue.total": "Total queued or processed items observed by the worker.",
    "queue.by_priority.unknown_active.depth": "Unknown inlinks targets with active interest.",
    "queue.by_priority.unknown_idle.depth": "Unknown inlinks targets without active interest.",
    "queue.by_priority.refresh_active.depth": "Known inlinks targets with active interest.",
    "queue.by_priority.refresh_idle.depth": "Known inlinks targets without active interest.",
    "queue.recent_changes": "Recent changes rows waiting to be scanned for new updates.",
    "queue.creation_backfill": "Items still missing creation metadata from the recent changes monitor.",
    "queue.creation_interest_backfill": "Items still missing creation metadata from the creation-interest queue.",
    "queue.user_history_backfill": "Creation requests waiting in the user-history queue.",
    "queue.user_creation_backfill": "Creation requests waiting in the user-creation queue.",
    "queue.candidates": "External usage QIDs waiting to be refreshed from lookup sources.",
    "queue.pubsub": "Items available from the worker's pubsub-backed queue.",
    "queue.inlinks_waiting": "Content-adjacent items waiting on inlinks evaluation work.",
    "throughput.total_processed": "Cumulative items processed since the worker started.",
    "throughput.elapsed_seconds": "Seconds since the worker began tracking throughput.",
    "throughput.rate_per_second": "Recent processing rate from the worker's observability window.",
    "batch.selected": "Inlinks targets selected for the current batch.",
    "batch.processed": "Inlinks targets examined in the current batch.",
    "batch.finalized": "Inlinks targets finalized by the current batch.",
    "batch.deferred": "Inlinks targets left unknown by the current batch.",
    "batch.interests_emitted": "Dependency interests emitted by the current batch.",
    "batch.by_priority.unknown_active.selected": "Selected unknown inlinks targets with active interest.",
    "batch.by_priority.unknown_active.processed": "Processed unknown inlinks targets with active interest.",
    "batch.by_priority.unknown_active.finalized": "Finalized unknown inlinks targets with active interest.",
    "batch.by_priority.unknown_active.deferred": "Deferred unknown inlinks targets with active interest.",
    "batch.by_priority.unknown_active.interests_emitted": "Dependency interests emitted for active unknown inlinks targets.",
    "batch.by_priority.unknown_active.queue_depth": "Queue depth for active unknown inlinks targets.",
    "batch.by_priority.unknown_active.avg_age_seconds": "Average age for active unknown inlinks targets.",
    "batch.by_priority.unknown_active.p95_age_seconds": "P95 age for active unknown inlinks targets.",
    "batch.by_priority.unknown_idle.selected": "Selected unknown inlinks targets without active interest.",
    "batch.by_priority.unknown_idle.processed": "Processed unknown inlinks targets without active interest.",
    "batch.by_priority.unknown_idle.finalized": "Finalized unknown inlinks targets without active interest.",
    "batch.by_priority.unknown_idle.deferred": "Deferred unknown inlinks targets without active interest.",
    "batch.by_priority.unknown_idle.interests_emitted": "Dependency interests emitted for idle unknown inlinks targets.",
    "batch.by_priority.unknown_idle.queue_depth": "Queue depth for idle unknown inlinks targets.",
    "batch.by_priority.unknown_idle.avg_age_seconds": "Average age for idle unknown inlinks targets.",
    "batch.by_priority.unknown_idle.p95_age_seconds": "P95 age for idle unknown inlinks targets.",
    "batch.by_priority.refresh_active.selected": "Selected refresh candidates with active interest.",
    "batch.by_priority.refresh_active.processed": "Processed refresh candidates with active interest.",
    "batch.by_priority.refresh_active.finalized": "Finalized refresh candidates with active interest.",
    "batch.by_priority.refresh_active.deferred": "Deferred refresh candidates with active interest.",
    "batch.by_priority.refresh_active.interests_emitted": "Dependency interests emitted for active refresh candidates.",
    "batch.by_priority.refresh_active.queue_depth": "Queue depth for active refresh candidates.",
    "batch.by_priority.refresh_active.avg_age_seconds": "Average age for active refresh candidates.",
    "batch.by_priority.refresh_active.p95_age_seconds": "P95 age for active refresh candidates.",
    "batch.by_priority.refresh_idle.selected": "Selected refresh candidates without active interest.",
    "batch.by_priority.refresh_idle.processed": "Processed refresh candidates without active interest.",
    "batch.by_priority.refresh_idle.finalized": "Finalized refresh candidates without active interest.",
    "batch.by_priority.refresh_idle.deferred": "Deferred refresh candidates without active interest.",
    "batch.by_priority.refresh_idle.interests_emitted": "Dependency interests emitted for idle refresh candidates.",
    "batch.by_priority.refresh_idle.queue_depth": "Queue depth for idle refresh candidates.",
    "batch.by_priority.refresh_idle.avg_age_seconds": "Average age for idle refresh candidates.",
    "batch.by_priority.refresh_idle.p95_age_seconds": "P95 age for idle refresh candidates.",
    "batch.in_flight.targets": "Inlinks targets currently waiting on unresolved dependencies.",
    "batch.in_flight.age_seconds.count": "Inlinks targets with a known first-interest time in the in-flight set.",
    "batch.in_flight.age_seconds.avg_seconds": "Average time in flight since first observed interest.",
    "batch.in_flight.age_seconds.p95_seconds": "P95 time in flight since first observed interest.",
    "batch.turnaround.first_interest_marked": "Targets that first showed active interest in the current batch.",
    "batch.turnaround.creation_to_finalize.count": "Finalized targets with a known creation time.",
    "batch.turnaround.creation_to_finalize.avg_seconds": "Average creation-to-finalize age for finalized targets.",
    "batch.turnaround.creation_to_finalize.p95_seconds": "P95 creation-to-finalize age for finalized targets.",
    "batch.turnaround.interest_to_finalize.count": "Finalized targets with a known first-interest time.",
    "batch.turnaround.interest_to_finalize.avg_seconds": "Average first-interest-to-finalize age for finalized targets.",
    "batch.turnaround.interest_to_finalize.p95_seconds": "P95 first-interest-to-finalize age for finalized targets.",
    "failures.context_errors": "Item evaluations that returned source or detector errors.",
    "failures.missing_lastrevid": "Evaluated items missing a usable last revision id.",
    "failures.unknown_live_result": "Live items that still evaluated to an unknown notability state.",
    "failures.validation_rejected": "Updates rejected because the batch was incomplete or invalid.",
    "failures.worker_exceptions": "Worker loop exceptions that prevented a batch from completing.",
    "timings.selection": "Total time spent choosing the next batch of work.",
    "timings.find_work": "Total time spent selecting inlinks work candidates from the cache.",
    "timings.get_inlinks": "Total time spent fetching inlinks from the Wikidata replica.",
    "timings.get_context_replica_connect": "Total time spent opening replica connections for inlinks fetches.",
    "timings.get_context_replica_query": "Total time spent running replica backlink queries.",
    "timings.get_context_replica_fetch": "Total time spent fetching replica query results into memory.",
    "timings.get_context_replica_normalize": "Total time spent normalizing replica backlink rows.",
    "timings.check_cache": "Total time spent resolving visible inlinks against the cache.",
    "timings.process": "Total time spent classifying inlinks targets in the current batch.",
    "timings.finalize": "Total time spent writing finalized inlinks state back to the cache.",
    "timings.register_interest": "Total time spent emitting dependency-interest sessions.",
    "timings.work_pass": "Total wall-clock time spent in one inlinks pass.",
    "timings.fetch_contexts": "Total time spent loading source contexts.",
    "timings.detector_sitelinks": "Total time spent running the sitelinks detector.",
    "timings.detector_identifiers": "Total time spent running the identifiers detector.",
    "timings.detector_sources": "Total time spent running the sources detector.",
    "timings.evaluate": "Total time spent combining detector results.",
    "timings.upsert": "Total time spent writing updates to the cache.",
    "timings.verify": "Total time spent verifying worker outputs.",
    "timings.event_log": "Total time spent writing event log rows.",
    "timings.release": "Total time spent releasing in-flight work.",
    "timings.other": "Total time not attributed to a named worker phase.",
    "poll_seconds": "Configured polling delay for the worker loop.",
}


def _cache_observability_field_metadata() -> dict[str, str]:
    metadata = {
        "items.total": "Total cached items currently stored.",
        "items.rate_per_second": "Recent cache growth rate derived from cached item totals.",
    }

    for flag_name in ("redirect", "has_sitelinks", "has_claims", "deleted"):
        pretty_name = flag_name.replace("_", " ")
        metadata[f"flags.{flag_name}.unknown"] = (
            f"Cached items without a content revid, so {pretty_name} is unknowable."
        )
        metadata[f"flags.{flag_name}.no"] = f"Cached items with a content revid and {pretty_name} disabled."
        metadata[f"flags.{flag_name}.yes"] = f"Cached items with a content revid and {pretty_name} enabled."

    for prefix, label in (("detected", "Detected"), ("deduced", "Deduced")):
        for criterion in (*DETECTED_CRITERIA, "N2", "N12", "N3", "N"):
            for level_name in ("unknown", "none", "partial-weak", "partial-strong", "weak", "strong"):
                metadata[f"criteria.{prefix}.{criterion}.{level_name}"] = f"{label} criterion {criterion} items at {level_name}."

    return metadata


OBSERVABILITY_FIELD_METADATA.update(_cache_observability_field_metadata())


def _parse_observability_period(period: str | None) -> int:
    text = "24h" if period is None else str(period).strip().lower()
    if not text:
        text = "24h"
    if text.isdigit():
        seconds = int(text)
        if seconds <= 0:
            raise HTTPException(
                status_code=400, detail="period must be positive")
        return seconds
    unit_multipliers = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }
    if len(text) < 2 or text[-1] not in unit_multipliers:
        raise HTTPException(
            status_code=400, detail="period must look like 24h, 90m, or 86400")
    amount_text = text[:-1]
    if not amount_text.isdigit():
        raise HTTPException(
            status_code=400, detail="period must look like 24h, 90m, or 86400")
    amount = int(amount_text)
    if amount <= 0:
        raise HTTPException(status_code=400, detail="period must be positive")
    return amount * unit_multipliers[text[-1]]


def _format_observability_title(period_seconds: int) -> str:
    if period_seconds % 604800 == 0:
        return f"{period_seconds // 604800} week(s)"
    if period_seconds % 86400 == 0:
        return f"{period_seconds // 86400} day(s)"
    if period_seconds % 3600 == 0:
        return f"{period_seconds // 3600} hour(s)"
    if period_seconds % 60 == 0:
        return f"{period_seconds // 60} minute(s)"
    return f"{period_seconds} second(s)"


def _observability_metrics_payload() -> list[dict[str, str]]:
    return [
        {"field": field, "description": description}
        for field, description in sorted(OBSERVABILITY_FIELD_METADATA.items())
    ]


def _sse_message(payload: dict[str, object]) -> str:
    return f"data: {json.dumps(payload)}\n\n"


def _sse_event(event: str, payload: dict[str, object]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def _payload_signature(payload: dict[str, object]) -> int:
    serialized = json.dumps(payload, sort_keys=True,
                            separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.blake2b(serialized.encode(
        "utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False)


BADGE_STATE_FIELD_NAMES = (
    "N",
    "N1",
    "N2a",
    "N2b",
    "N3",
    "N3_inlinks",
    "N3_osm",
    "N3_wikisub",
    "N3_sdc",
    "has_sitelinks_count",
    "has_claims_count",
    "inlinks_count",
    "redirect_target",
    "content_last_revid",
    "recent_changes_last_revid",
    "content_stale",
    "creator",
    "creation_time",
)


def _badge_state_snapshot(
    result,
    *,
    content_stale: bool | None = None,
    creator: str | None = None,
    creation_time: int | None = None,
) -> tuple[object, ...]:
    return (
        result.levels_str.get("N"),
        result.levels_str.get("N1"),
        result.levels_str.get("N2a"),
        result.levels_str.get("N2b"),
        result.levels_str.get("N3"),
        result.levels_str.get("N3_inlinks"),
        result.levels_str.get("N3_osm"),
        result.levels_str.get("N3_wikisub"),
        result.levels_str.get("N3_sdc"),
        result.has_sitelinks_count,
        result.has_claims_count,
        result.inlinks_count,
        result.redirect_target,
        result.content_last_revid,
        result.recent_changes_last_revid,
        content_stale,
        creator,
        creation_time,
    )


def _badge_state_changed_fields(
    previous: tuple[object, ...] | None,
    current: tuple[object, ...],
) -> list[str]:
    if previous is None:
        return [
            field_name
            for field_name, value in zip(BADGE_STATE_FIELD_NAMES, current, strict=True)
            if value is not None and str(value).lower() != "unknown"
        ]
    return [
        field_name
        for field_name, old_value, new_value in zip(
            BADGE_STATE_FIELD_NAMES,
            previous,
            current,
            strict=True,
        )
        if old_value != new_value
    ]


def _badge_payload_has_meaningful_state(payload: dict[str, object]) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("is_deleted") is True:
        return True
    if payload.get("redirect") is True:
        return True
    if payload.get("content_last_revid") is not None:
        return True

    levels = payload.get("levels", {})
    if isinstance(levels, dict):
        for value in levels.values():
            if value is None:
                continue
            if str(value).lower() != "unknown":
                return True
    return False


async def _record_badge_served(
    *,
    qid: str,
    payload: dict[str, object],
    stream_name: str,
    batch_id: str | None = None,
    changed_fields: list[str] | None = None,
) -> None:
    if not ITEM_TRACE_ENABLED:
        return
    details: dict[str, object] = {
        "stream": stream_name,
        "payload_event": payload.get("event"),
        "payload_signature": _payload_signature(payload),
    }
    for key in ("creator", "creation_time", "content_last_revid", "recent_changes_last_revid"):
        value = payload.get(key)
        if value is not None:
            details[key] = value
    if changed_fields:
        details["changed_fields"] = changed_fields
    await CACHE.item_trace.record_event(
        ItemTraceRecord(
            qid=qid,
            event_type="badge_served",
            worker_name="sse",
            batch_id=batch_id,
            details=details,
        )
    )


async def _sleep_or_shutdown(seconds: float) -> bool:
    event = SHUTDOWN_EVENT
    if event is None:
        await asyncio.sleep(seconds)
        return False

    try:
        await asyncio.wait_for(event.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True


async def _register_stream_task() -> asyncio.Task:
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("Stream task is not running in an asyncio task")

    async with ACTIVE_STREAM_TASKS_LOCK:
        ACTIVE_STREAM_TASKS.add(task)
    return task


async def _unregister_stream_task(task: asyncio.Task | None) -> None:
    if task is None:
        return
    async with ACTIVE_STREAM_TASKS_LOCK:
        ACTIVE_STREAM_TASKS.discard(task)


async def _cancel_active_stream_tasks() -> None:
    async with ACTIVE_STREAM_TASKS_LOCK:
        tasks = [task for task in ACTIVE_STREAM_TASKS if not task.done()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _pubsub_reaper_loop() -> None:
    while SHUTDOWN_EVENT is None or not SHUTDOWN_EVENT.is_set():
        interest_store = getattr(CACHE, "interest", None) or getattr(CACHE, "pubsub", None)
        try:
            await _purge_stale_gadget_subscriptions()
        except Exception as exc:  # noqa: BLE001
            print(f"Gadget subscription purge failed: {exc}")
        try:
            try:
                purge_expired = getattr(interest_store, "purge_expired_interest", None)
                if callable(purge_expired):
                    await purge_expired()
                elif interest_store is not None:
                    await interest_store.purge_expired_interest()
            except Exception as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
            await _purge_expired_pubsub_subscription_qids()
        except Exception as exc:  # noqa: BLE001
            print(f"PubSub reaper failed: {exc}")
        try:
            await _sleep_or_shutdown(PUBSUB_REAPER_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise


async def _touch_gadget_subscription(subscription_id: str) -> None:
    if not subscription_id:
        return

    async with GADGET_SUBSCRIPTION_TOUCHES_LOCK:
        GADGET_SUBSCRIPTION_TOUCHES[subscription_id] = time.monotonic()


async def _store_pubsub_subscription_qids(subscription_id: str, qids: list[str]) -> None:
    if not subscription_id:
        return

    expires_at = time.monotonic() + PUBSUB_SUBSCRIPTION_SNAPSHOT_TTL_SECONDS
    snapshot_qids = tuple(dict.fromkeys(qids))
    async with PUBSUB_SUBSCRIPTION_QIDS_LOCK:
        PUBSUB_SUBSCRIPTION_QIDS[subscription_id] = (expires_at, snapshot_qids)


async def _get_pubsub_subscription_qids(subscription_id: str) -> list[str] | None:
    if not subscription_id:
        return None

    now = time.monotonic()
    async with PUBSUB_SUBSCRIPTION_QIDS_LOCK:
        snapshot = PUBSUB_SUBSCRIPTION_QIDS.get(subscription_id)
        if snapshot is None:
            return None
        expires_at, qids = snapshot
        if expires_at <= now:
            del PUBSUB_SUBSCRIPTION_QIDS[subscription_id]
            return None
        return list(qids)


async def _purge_expired_pubsub_subscription_qids() -> None:
    now = time.monotonic()
    async with PUBSUB_SUBSCRIPTION_QIDS_LOCK:
        expired_ids = [
            subscription_id
            for subscription_id, (expires_at, _qids) in PUBSUB_SUBSCRIPTION_QIDS.items()
            if expires_at <= now
        ]
        for subscription_id in expired_ids:
            del PUBSUB_SUBSCRIPTION_QIDS[subscription_id]


async def _purge_stale_gadget_subscriptions() -> None:
    now = time.monotonic()
    stale_ids: list[str] = []
    async with GADGET_SUBSCRIPTION_TOUCHES_LOCK:
        for subscription_id, touched_at in list(GADGET_SUBSCRIPTION_TOUCHES.items()):
            if now - touched_at >= PUBSUB_GADGET_SESSION_GRACE_SECONDS:
                stale_ids.append(subscription_id)
                del GADGET_SUBSCRIPTION_TOUCHES[subscription_id]

    for subscription_id in stale_ids:
        try:
            interest_store = getattr(CACHE, "interest", None) or getattr(CACHE, "pubsub", None)
            if interest_store is not None:
                await interest_store.delete_interest_lease(owner_id="gadget", lease_id=subscription_id)
        except Exception as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                async with GADGET_SUBSCRIPTION_TOUCHES_LOCK:
                    GADGET_SUBSCRIPTION_TOUCHES[subscription_id] = now
                continue
            raise


async def _start_interest_stream(
    *,
    owner_id: str,
    session_id: str,
    qids: list[str],
    worker_id: str,
    priority: int,
    wants_creation: bool,
    wants_content: bool,
    wants_inlinks: bool,
) -> tuple[Any | None, Any | None, Any | None]:
    interest_store = getattr(CACHE, "interest", None) or getattr(CACHE, "pubsub", None)
    global WEB_INTEREST_MANAGER
    if interest_store is not None:
        async with WEB_INTEREST_MANAGER_LOCK:
            if WEB_INTEREST_MANAGER is None:
                create_manager = getattr(interest_store, "create_interest_manager", None)
                if callable(create_manager):
                    WEB_INTEREST_MANAGER = await create_manager(
                        worker_id="web",
                        priority=priority,
                        wants_creation=wants_creation,
                        wants_content=wants_content,
                        wants_inlinks=wants_inlinks,
                    )
            manager = WEB_INTEREST_MANAGER
        if manager is not None:
            session = manager.create_session()
            await session.replace(qids)
            return manager, session, interest_store
    if interest_store is not None:
        await interest_store.create_interest_lease(
            owner_id=owner_id,
            lease_id=session_id,
            ttl_seconds=PUBSUB_GADGET_SESSION_TTL_SECONDS,
            priority=priority,
            wants_creation=wants_creation,
            wants_content=wants_content,
            wants_inlinks=wants_inlinks,
            qids=qids,
        )
    return None, None, interest_store


async def _subscription_event_stream(subscription_id: str, qids: set[str], request: Request):
    stream_task = await _register_stream_task()
    last_seen: dict[str, tuple[object, ...]] = {}
    deadline = time.monotonic() + SSE_STREAM_MAX_SECONDS
    qid_list = sorted(qids)
    qid_chunks = [
        qid_list[start: start + PUBSUB_GADGET_EVENT_CHUNK_SIZE]
        for start in range(0, len(qid_list), PUBSUB_GADGET_EVENT_CHUNK_SIZE)
    ]
    await _touch_gadget_subscription(subscription_id)
    if time.monotonic() >= deadline:
        try:
            yield _sse_message({"event": "stream_end"})
        finally:
            await _unregister_stream_task(stream_task)
        return
    if await request.is_disconnected():
        await _unregister_stream_task(stream_task)
        return
    manager = None
    session = None
    primed_sent = False
    try:
        manager, session, interest_store = await _start_interest_stream(
            owner_id="gadget",
            session_id=subscription_id,
            qids=qid_list,
            worker_id=f"gadget:{subscription_id}",
            priority=10,
            wants_creation=True,
            wants_content=True,
            wants_inlinks=True,
        )
        while time.monotonic() < deadline:
            if SHUTDOWN_EVENT is not None and SHUTDOWN_EVENT.is_set():
                break
            if await request.is_disconnected():
                break
            trace_batch_id = str(uuid4())
            await _touch_gadget_subscription(subscription_id)
            if session is None and interest_store is not None:
                await interest_store.refresh_interest_lease(
                    owner_id="gadget",
                    lease_id=subscription_id,
                    ttl_seconds=PUBSUB_GADGET_SESSION_TTL_SECONDS,
                )

            emitted = set()
            for chunk in qid_chunks:
                if await request.is_disconnected():
                    return

                if web_resolve_creation_bootstrap is _DEFAULT_WEB_RESOLVE_CREATION_BOOTSTRAP:
                    cached_rows = await CACHE.get_many(chunk)
                    creation_metadata = await CACHE.get_creation_metadata_many(chunk)
                else:
                    cached_rows, creation_metadata = await web_resolve_creation_bootstrap(chunk)
                content_staleness_fetcher = getattr(CACHE, "get_content_staleness_for_qids", None)
                if content_staleness_fetcher is None:
                    content_staleness = {}
                else:
                    content_staleness = await content_staleness_fetcher(chunk)
                for qid in chunk:
                    if await request.is_disconnected():
                        return

                    cached_result = cached_rows.get(qid)
                    if cached_result is None:
                        continue
                    metadata = creation_metadata.get(qid, {})
                    current_seen = _badge_state_snapshot(
                        cached_result,
                        content_stale=content_staleness.get(qid),
                        creator=metadata.get("creator"),
                        creation_time=metadata.get("creation_time"),
                    )
                    previous_seen = last_seen.get(qid)
                    if previous_seen == current_seen:
                        continue

                    last_seen[qid] = current_seen
                    changed_fields = _badge_state_changed_fields(previous_seen, current_seen)
                    payload = _badge_payload(
                        qid,
                        cached_result,
                        content_stale=content_staleness.get(qid),
                        creator=metadata.get("creator"),
                        creation_time=metadata.get("creation_time"),
                    )
                    if _badge_payload_has_meaningful_state(payload):
                        emitted.add(qid)
                        if not primed_sent:
                            yield _sse_message({
                                "event": "primed",
                                "subscription_id": subscription_id,
                                "qid_count": len(qid_list),
                            })
                            primed_sent = True
                        try:
                            await _record_badge_served(
                                qid=qid,
                                payload=payload,
                                stream_name="gadget_subscription",
                                batch_id=trace_batch_id,
                                changed_fields=changed_fields,
                            )
                        except Exception as exc:  # noqa: BLE001
                            print(f"Gadget badge SSE trace emit failed for {qid}: {exc}")
                        yield _sse_message(payload)

            if await request.is_disconnected():
                break

            if not emitted:
                yield _sse_message({"event": "keepalive"})
            else:
                print(
                    f"Emitted updates for {len(emitted)} QIDs in subscription {subscription_id}")

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if await _sleep_or_shutdown(min(1.0, remaining)):
                break

        if (
            time.monotonic() >= deadline
            and (SHUTDOWN_EVENT is None or not SHUTDOWN_EVENT.is_set())
            and not await request.is_disconnected()
        ):
            yield _sse_message({"event": "stream_end"})
    finally:
        if session is not None:
            await session.close()
        await _unregister_stream_task(stream_task)


@app.on_event("startup")
async def startup_event() -> None:
    global SHUTDOWN_EVENT, PUBSUB_REAPER_TASK, WEB_INTEREST_MANAGER
    lookup_cache.assert_ready(
        required_property_qids=(
            "Q105388954",  # online account identifier
            "Q18614948",  # authority control
            "Q62589316",  # collection of properties that suggest notability
        )
    )
    async with GADGET_SUBSCRIPTION_TOUCHES_LOCK:
        GADGET_SUBSCRIPTION_TOUCHES.clear()
    async with PUBSUB_SUBSCRIPTION_QIDS_LOCK:
        PUBSUB_SUBSCRIPTION_QIDS.clear()
    SHUTDOWN_EVENT = asyncio.Event()
    PUBSUB_REAPER_TASK = asyncio.create_task(_pubsub_reaper_loop())
    interest_store = getattr(CACHE, "interest", None) or getattr(CACHE, "pubsub", None)
    if interest_store is not None and WEB_INTEREST_MANAGER is None:
        create_manager = getattr(interest_store, "create_interest_manager", None)
        if callable(create_manager):
            WEB_INTEREST_MANAGER = await create_manager(
                worker_id="web",
                priority=10,
                wants_creation=True,
                wants_content=True,
                wants_inlinks=True,
            )


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global PUBSUB_REAPER_TASK, WEB_INTEREST_MANAGER
    if SHUTDOWN_EVENT is not None:
        SHUTDOWN_EVENT.set()
    await _cancel_active_stream_tasks()
    async with PUBSUB_SUBSCRIPTION_QIDS_LOCK:
        PUBSUB_SUBSCRIPTION_QIDS.clear()
    if PUBSUB_REAPER_TASK is not None:
        PUBSUB_REAPER_TASK.cancel()
        try:
            await PUBSUB_REAPER_TASK
        except asyncio.CancelledError:
            pass
        PUBSUB_REAPER_TASK = None
    if WEB_INTEREST_MANAGER is not None:
        await WEB_INTEREST_MANAGER.close()
        WEB_INTEREST_MANAGER = None
    await close_wikidata_session()


app.include_router(api_router)
app.include_router(home_router)

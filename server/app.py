from __future__ import annotations

import asyncio
import json
import os
import sqlite3
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
from markdown_it import MarkdownIt
from pydantic import BaseModel, Field

from wd_notability.evaluation_cache import CACHE
from wd_notability.evaluate import foreground_evaluation
from wd_notability.content.deletion import queue_stats as deletion_queue_stats
from wd_notability.content.worker import queue_stats as entitydata_queue_stats
from wd_notability.inlinks.worker import (
    INLINKS_LOW_PRIORITY_CONSIDER_LIMIT,
    INLINKS_LOW_PRIORITY_MAX_IN_FLIGHT,
    INLINKS_VISIBLE_LIMIT,
    INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE,
    queue_stats as inlinks_queue_stats,
)
from wd_notability.inlinks.source import INLINKS_SOURCE
from wd_notability.external_usage.worker import queue_stats as cache_sync_queue_stats
from wd_notability.content.recent_changes import queue_stats as recent_changes_queue_stats
from wd_notability.lookup_cache import lookup_cache
from wd_notability.models import EvaluationReason, EvaluationResult, NotabilityLevel
from wd_notability.web.creations import (
    lookup_creator_names as web_lookup_creator_names,
    resolve_creation_metadata as web_resolve_creation_metadata,
    render_creations_dashboard_html as web_render_creations_dashboard_html,
)
from wd_notability.content.debug import build_signal_debug_payload as web_build_signal_debug_payload
from wd_notability.content.debug import render_signal_debug_html as web_render_signal_debug_html
from wd_notability.content.fetcher import ENTITY_DATA_SOURCE
from wd_notability.external_usage.osm.source import OSM_SOURCE
from wd_notability.external_usage.sdc.source import SDC_SOURCE
from wd_notability.external_usage.wiki_subscribers.source import WIKI_USAGE_SOURCE
from wd_notability.wikidata import EntityDeletedError
from wd_notability.wikidata_api import close_wikidata_session


REVALUATE_ON_SUBSCRIBE = True
SHUTDOWN_EVENT: asyncio.Event | None = None
SSE_STREAM_MAX_SECONDS = float(os.getenv("WD_NOTABILITY_SSE_STREAM_MAX_SECONDS", "60"))
PUBSUB_REAPER_TASK: asyncio.Task | None = None
PUBSUB_REAPER_INTERVAL_SECONDS = 60.0
PUBSUB_GADGET_SESSION_TTL_SECONDS = 3600
PUBSUB_GADGET_SESSION_GRACE_SECONDS = 10.0
PUBSUB_GADGET_SESSION_PURGE_SECONDS = 5.0
ACTIVE_STREAM_TASKS: set[asyncio.Task] = set()
ACTIVE_STREAM_TASKS_LOCK = asyncio.Lock()
GADGET_SUBSCRIPTION_TOUCHES: dict[str, float] = {}
GADGET_SUBSCRIPTION_TOUCHES_LOCK = asyncio.Lock()


app = FastAPI(title="wd_notability")
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
OBSERVABILITY_JS_VERSION = int((STATIC_DIR / "observability.js").stat().st_mtime)

_cors_origins_raw = os.getenv("WD_NOTABILITY_CORS_ORIGINS", "*")
_cors_origins = [origin.strip() for origin in _cors_origins_raw.split(",") if origin.strip()]
if not _cors_origins:
    _cors_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

DETECTED_CRITERIA = ("N1", "N2a", "N2b", "N3_inlinks", "N3_osm", "N3_wikisub", "N3_sdc")
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

EVALUATION_SOURCES = (ENTITY_DATA_SOURCE, OSM_SOURCE, SDC_SOURCE, WIKI_USAGE_SOURCE)

OBSERVABILITY_FIELD_METADATA = {
    "queue.total": "Total queued or processed items observed by the worker.",
    "queue.by_priority.unknown_active.depth": "Unknown inlinks targets with active interest.",
    "queue.by_priority.unknown_idle.depth": "Unknown inlinks targets without active interest.",
    "queue.by_priority.refresh_active.depth": "Known inlinks targets with active interest.",
    "queue.by_priority.refresh_idle.depth": "Known inlinks targets without active interest.",
    "queue.recent_changes": "Recent changes rows waiting to be scanned for new updates.",
    "queue.creation_backfill": "Items still missing creation metadata from the recent changes worker.",
    "queue.candidates": "Cache sync QIDs waiting to be refreshed from external usage sources.",
    "queue.pubsub": "Items available from the worker's pubsub-backed queue.",
    "throughput.total_processed": "Cumulative items processed since the worker started.",
    "throughput.elapsed_seconds": "Seconds since the worker began tracking throughput.",
    "throughput.rate_per_second": "Recent processing rate from the worker's rolling sample window.",
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
    "failures.context_errors": "Item evaluations that returned source or detector errors.",
    "failures.missing_lastrevid": "Evaluated items missing a usable last revision id.",
    "failures.unknown_live_result": "Live items that still evaluated to an unknown notability state.",
    "failures.validation_rejected": "Updates rejected because the batch was incomplete or invalid.",
    "failures.worker_exceptions": "Worker loop exceptions that prevented a batch from completing.",
    "timings.selection": "Total time spent choosing the next batch of work.",
    "timings.fetch_contexts": "Total time spent loading source contexts.",
    "timings.detector_sitelinks": "Total time spent running the sitelinks detector.",
    "timings.detector_identifiers": "Total time spent running the identifiers detector.",
    "timings.detector_sources": "Total time spent running the sources detector.",
    "timings.evaluate": "Total time spent combining detector results.",
    "timings.upsert": "Total time spent writing updates to the cache.",
    "timings.verify": "Total time spent verifying worker outputs.",
    "timings.wait_foreground": "Total time spent waiting for foreground evaluations to finish.",
    "timings.event_log": "Total time spent writing event log rows.",
    "timings.release": "Total time spent releasing in-flight work.",
    "timings.other": "Total time not attributed to a named worker phase.",
    "poll_seconds": "Configured polling delay for the worker loop.",
}


def _cache_observability_field_metadata() -> dict[str, str]:
    metadata = {
        "items.total": "Total cached items currently stored.",
    }

    for flag_name in ("redirect", "has_sitelinks", "has_claims", "deleted"):
        pretty_name = flag_name.replace("_", " ")
        metadata[f"flags.{flag_name}.yes"] = f"Cached items with {pretty_name} enabled."
        metadata[f"flags.{flag_name}.no"] = f"Cached items without {pretty_name} enabled."

    for prefix, label in (("detected", "Detected"), ("deduced", "Deduced")):
        for criterion in (*DETECTED_CRITERIA, "N2", "N12", "N3", "N"):
            for level_name in ("unknown", "none", "weak", "strong"):
                metadata[f"criteria.{prefix}.{criterion}.{level_name}"] = f"{label} criterion {criterion} items at {level_name}."

    return metadata


OBSERVABILITY_FIELD_METADATA.update(_cache_observability_field_metadata())

MARKDOWN_RENDERER = MarkdownIt("commonmark", {"html": True})


class SubscribeItem(BaseModel):
    qid: str
    reason: str | None = None


class SubscribeRequest(BaseModel):
    qids: list[str] = Field(default_factory=list)
    items: list[SubscribeItem] = Field(default_factory=list)
    session_id: str | None = None


class PubSubCreateRequest(BaseModel):
    ttl_seconds: int = Field(gt=0)
    priority: int = Field(default=10, ge=0, le=1000)
    wants_entitydata: bool = False
    wants_inlinks: bool = False
    wants_sync: bool = False
    qids: list[str] = Field(default_factory=list)


class PubSubAddRequest(BaseModel):
    qids: list[str] = Field(default_factory=list)
    priority: int = Field(default=10, ge=0, le=1000)
    wants_entitydata: bool | None = None
    wants_inlinks: bool | None = None
    wants_sync: bool | None = None


class PubSubRefreshRequest(BaseModel):
    ttl_seconds: int = Field(gt=0)


def _parse_observability_period(period: str | None) -> int:
    text = "24h" if period is None else str(period).strip().lower()
    if not text:
        text = "24h"
    if text.isdigit():
        seconds = int(text)
        if seconds <= 0:
            raise HTTPException(status_code=400, detail="period must be positive")
        return seconds
    unit_multipliers = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }
    if len(text) < 2 or text[-1] not in unit_multipliers:
        raise HTTPException(status_code=400, detail="period must look like 24h, 90m, or 86400")
    amount_text = text[:-1]
    if not amount_text.isdigit():
        raise HTTPException(status_code=400, detail="period must look like 24h, 90m, or 86400")
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


def _normalize_qids(qids: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for qid in qids:
        if not isinstance(qid, str):
            continue
        candidate = qid.strip().upper()
        if not _is_valid_qid(candidate) or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _normalize_subscription_items(request: SubscribeRequest) -> dict[str, EvaluationReason]:
    items: dict[str, EvaluationReason] = {}

    for qid in _normalize_qids(request.qids):
        items[qid] = EvaluationReason.PAGE

    for item in request.items:
        qid = item.qid.strip().upper() if isinstance(item.qid, str) else ""
        if not _is_valid_qid(qid):
            continue

        try:
            reason = EvaluationReason.from_str(item.reason or "page")
        except ValueError:
            reason = EvaluationReason.PAGE

        existing_reason = items.get(qid)
        if existing_reason is None or reason.priority > existing_reason.priority:
            items[qid] = reason

    return items


def _normalize_owner_id(owner_id: str) -> str:
    owner = owner_id.strip().lower()
    if owner not in {"gadget", "report", "inlinks"}:
        raise HTTPException(status_code=400, detail="owner_id must be gadget, report, or inlinks")
    return owner


def _subscription_priority_for_reason(reason: EvaluationReason) -> int:
    if reason is EvaluationReason.PAGE:
        return 100
    if reason is EvaluationReason.INLINK:
        return 1
    return 10


def _group_subscription_qids_by_priority(items: dict[str, EvaluationReason]) -> dict[int, list[str]]:
    grouped: dict[int, list[str]] = {}
    for qid, reason in items.items():
        priority = _subscription_priority_for_reason(reason)
        grouped.setdefault(priority, []).append(qid)
    return grouped


def _sse_message(payload: dict[str, object]) -> str:
    return f"data: {json.dumps(payload)}\n\n"


def _sse_event(event: str, payload: dict[str, object]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


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
        try:
            await _purge_stale_gadget_subscriptions()
        except Exception as exc:  # noqa: BLE001
            print(f"Gadget subscription purge failed: {exc}")
        try:
            try:
                await CACHE.pubsub.purge_expired_pubsub_sessions()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
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
            await CACHE.pubsub.delete_pubsub_session(owner_id="gadget", session_id=subscription_id)
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                async with GADGET_SUBSCRIPTION_TOUCHES_LOCK:
                    GADGET_SUBSCRIPTION_TOUCHES[subscription_id] = now
                continue
            raise


async def _subscription_event_stream(subscription_id: str, qids: set[str], request: Request):
    stream_task = await _register_stream_task()
    last_seen: dict[str, tuple[int, int | None, int | None]] = {}
    deadline = time.monotonic() + SSE_STREAM_MAX_SECONDS
    qid_list = sorted(qids)
    primed = False
    creation_metadata = await web_resolve_creation_metadata(qid_list)
    await _touch_gadget_subscription(subscription_id)
    try:
        while time.monotonic() < deadline:
            if SHUTDOWN_EVENT is not None and SHUTDOWN_EVENT.is_set():
                break
            if await request.is_disconnected():
                break
            await _touch_gadget_subscription(subscription_id)

            emitted = set()
            cached_rows = await CACHE.get_many(qid_list)
            for qid in qid_list:
                if await request.is_disconnected():
                    return

                row = cached_rows.get(qid)
                if row is None:
                    continue
                summary, entitydata_last_revid, recent_changes_last_revid = row
                cached_result = EvaluationResult.from_summary(qid=qid, summary=summary)
                current_seen = (summary, entitydata_last_revid, recent_changes_last_revid)
                if last_seen.get(qid) == current_seen:
                    continue

                last_seen[qid] = current_seen
                if not primed:
                    continue
                emitted.add(qid)
                yield _sse_message(
                    _badge_payload(
                        qid,
                        cached_result,
                        creator=creation_metadata.get(qid, {}).get("creator"),
                        creation_time=creation_metadata.get(qid, {}).get("creation_time"),
                    )
                )

            primed = True
            if await request.is_disconnected():
                break

            if not emitted:
                yield _sse_message({"event": "keepalive"})
            else:
                print(f"Emitted updates for {len(emitted)} QIDs in subscription {subscription_id}")

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if await _sleep_or_shutdown(min(2.0, remaining)):
                break

        if (
            time.monotonic() >= deadline
            and (SHUTDOWN_EVENT is None or not SHUTDOWN_EVENT.is_set())
            and not await request.is_disconnected()
        ):
            yield _sse_message({"event": "stream_end"})
    finally:
        await _unregister_stream_task(stream_task)


def _render_markdown_html(markdown: str) -> str:
    return MARKDOWN_RENDERER.render(markdown)


def _markdown_title(markdown: str, fallback: str) -> str:
    for line in markdown.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip() or fallback
    return fallback


def _render_markdown_document(markdown: str, *, title: str) -> str:
    body = _render_markdown_html(markdown)
    escaped_title = escape(title)
    return f"""
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <link rel=\"icon\" href=\"/static/favicon.svg\" type=\"image/svg+xml\" />
    <link rel=\"icon\" href=\"/static/favicon-32.png\" type=\"image/png\" sizes=\"32x32\" />
    <link rel=\"icon\" href=\"/static/favicon-16.png\" type=\"image/png\" sizes=\"16x16\" />
    <link rel=\"shortcut icon\" href=\"/favicon.ico\" />
    <title>{escaped_title}</title>
    <style>
      :root {{ color-scheme: light dark; --bg: #fff; --text: #111; --border: #ddd; --code-bg: #f3f4f6; --link: #0645ad; }}
      @media (prefers-color-scheme: dark) {{
        :root {{ --bg: #111418; --text: #e8eaed; --border: #3a3f46; --code-bg: #252a31; --link: #8ab4f8; }}
      }}
      body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem auto; max-width: 52rem; padding: 0 1rem; background: var(--bg); color: var(--text); line-height: 1.55; }}
      a {{ color: var(--link); }}
      h1 {{ font-size: 2rem; line-height: 1.15; margin: 0 0 1rem; }}
      h2 {{ font-size: 1.35rem; margin: 2rem 0 .5rem; border-top: 1px solid var(--border); padding-top: 1rem; }}
      p {{ margin: .75rem 0; }}
      ul {{ padding-left: 1.4rem; }}
      li {{ margin: .35rem 0; }}
      code {{ background: var(--code-bg); border-radius: 4px; padding: .08rem .25rem; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .95em; }}
      strong {{ font-weight: 700; }}
    </style>
  </head>
  <body>
    {body}
  </body>
</html>
"""


def _render_static_markdown_page(filename: str) -> HTMLResponse:
    if "/" in filename or "\\" in filename or not filename.endswith(".md"):
        raise HTTPException(status_code=404, detail="Markdown page not found")

    path = STATIC_DIR / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Markdown page not found")

    markdown = path.read_text(encoding="utf-8")
    title = _markdown_title(markdown, fallback=path.stem.replace("-", " ").title())
    return HTMLResponse(content=_render_markdown_document(markdown, title=title))


def _badge_level(result, criterion: str) -> str:
    return result.levels_str[criterion]


def _badge_tooltip_from_levels(levels: dict[str, str]) -> str:
    def _level(field: str) -> str:
        value = levels.get(field, "unknown")
        return str(value).upper()

    lines = [f"Overall: {_level('N')}"]
    for field, label in BADGE_TOOLTIP_FIELDS:
        line = f"{label}: {_level(field)}"
        if field == "n3":
            n3_level = _level("n3")
            contributors = [
                component
                for component_field, component in BADGE_TOOLTIP_N3_COMPONENTS
                if _level(component_field) == n3_level and n3_level != "UNKNOWN"
            ]
            if contributors:
                line += f" ({', '.join(contributors)})"
        lines.append(line)
    return "\n".join(lines)


def _badge_tooltip(result) -> str:
    return _badge_tooltip_from_levels(result.levels_str)


def _badge_tooltip_from_report(report: dict) -> str:
    levels = report.get("levels", {}) if isinstance(report, dict) else {}
    lines = [_badge_tooltip_from_levels(levels if isinstance(levels, dict) else {})]
    if bool(report.get("is_redirect")):
        lines.append("Redirect: YES")
    if bool(report.get("is_deleted")):
        lines.append("Deleted: YES")
    if not bool(report.get("has_sitelinks")):
        lines.append("Has sitelinks: NO")
    levels = report.get("levels", {}) if isinstance(report, dict) else {}
    if (
        isinstance(levels, dict)
        and str(levels.get("N2a", "unknown")).lower() != "unknown"
        and str(levels.get("N2b", "unknown")).lower() != "unknown"
        and not bool(report.get("has_claims"))
    ):
        lines.append("Has claims: NO")
    return "\n".join(lines)


def _badge_payload(
    qid: str,
    result,
    *,
    creator: str | None = None,
    creation_time: int | None = None,
) -> dict[str, object]:
    payload = {
        "event": "update",
        "qid": qid,
        "n": _badge_level(result, "N"),
        "n1": _badge_level(result, "N1"),
        "n2a": _badge_level(result, "N2a"),
        "n2b": _badge_level(result, "N2b"),
        "n3": _badge_level(result, "N3"),
        "n3_inlinks": _badge_level(result, "N3_inlinks"),
        "n3_osm": _badge_level(result, "N3_osm"),
        "n3_wikisub": _badge_level(result, "N3_wikisub"),
        "n3_sdc": _badge_level(result, "N3_sdc"),
        "redirect": result.is_redirect,
        "has_claims": result.has_claims,
        "has_claims_known": result.has_claims_known,
        "has_sitelinks": result.has_sitelinks,
        "is_deleted": result.is_deleted,
    }
    if creator is not None:
        payload["creator"] = creator
    if creation_time is not None:
        payload["creation_time"] = creation_time
    return payload


def _cached_payload(
    qid: str,
    result,
    entitydata_last_revid: int | None,
    recent_changes_last_revid: int | None,
    *,
    creator: str | None = None,
    creation_time: int | None = None,
) -> dict[str, object]:
    payload = {
        "event": "cache",
        "qid": qid,
        "n": _badge_level(result, "N"),
        "n1": _badge_level(result, "N1"),
        "n2a": _badge_level(result, "N2a"),
        "n2b": _badge_level(result, "N2b"),
        "n3": _badge_level(result, "N3"),
        "n3_inlinks": _badge_level(result, "N3_inlinks"),
        "n3_osm": _badge_level(result, "N3_osm"),
        "n3_wikisub": _badge_level(result, "N3_wikisub"),
        "n3_sdc": _badge_level(result, "N3_sdc"),
        "redirect": result.is_redirect,
        "has_claims": result.has_claims,
        "has_claims_known": result.has_claims_known,
        "has_sitelinks": result.has_sitelinks,
        "is_deleted": result.is_deleted,
        "summary": result.summary,
        "entitydata_last_revid": entitydata_last_revid,
        "recent_changes_last_revid": recent_changes_last_revid,
    }
    if creator is not None:
        payload["creator"] = creator
    if creation_time is not None:
        payload["creation_time"] = creation_time
    return payload


def _badge_field_value(report: dict | None, field: str, default: str = "unknown") -> str:
    if report is None:
        return default

    if field in {"n", "n1", "n2a", "n2b", "n3"}:
        level_keys = {
            "n": "N",
            "n1": "N1",
            "n2a": "N2a",
            "n2b": "N2b",
            "n3": "N3",
        }
        levels = report.get("levels", {})
        if not isinstance(levels, dict):
            return default
        value = levels.get(level_keys[field])
        return str(value) if value is not None else default

    report_key = "is_redirect" if field == "redirect" else field
    if field == "has_claims":
        levels = report.get("levels", {})
        if not isinstance(levels, dict):
            return default
        if str(levels.get("N2a", "unknown")).lower() == "unknown" or str(levels.get("N2b", "unknown")).lower() == "unknown":
            return default
    value = report.get(report_key)
    if isinstance(value, bool):
        return str(value).lower()
    return default


def _render_report_badge(report: dict | None, qid: str) -> str:
    values = {
        field: escape(_badge_field_value(report, field), quote=True)
        for field in ("n", "n1", "n2a", "n2b", "n3", "redirect", "has_claims")
    }
    tooltip = escape(
        _badge_tooltip_from_report(report) if isinstance(report, dict) else "Notability badge",
        quote=True,
    )
    label = escape(
        f"Notability badge for {qid}: overall {_badge_field_value(report, 'n')}",
        quote=True,
    )
    return f"""
<svg class=\"report-badge\" role=\"img\" aria-label=\"{label}\" baseProfile=\"full\" version=\"1.1\" viewBox=\"0 0 36 36\"
     xmlns=\"http://www.w3.org/2000/svg\" title=\"{tooltip}\">
  <defs>
    <marker id=\"report-redirect-arrowhead\" markerWidth=\"2\" markerHeight=\"2\"
            refX=\"0\" refY=\"1\" orient=\"auto\" markerUnits=\"strokeWidth\">
      <path d=\"M0,0 L0,2 L2,1 Z\" fill=\"#6a1b9a\" />
    </marker>
  </defs>
  <circle cx=\"18.0\" cy=\"18.0\" r=\"14.66\" fill=\"none\" stroke-width=\"3.8\"
         data-field=\"n\" data-value=\"{values['n']}\"/>
  <path data-field=\"n1\" d=\"M12.78,28.04 A11.32,11.32 0 0,1 12.78,7.96 Z\" data-value=\"{values['n1']}\" />
  <path data-field=\"n3\" d=\"M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z\" data-value=\"{values['n3']}\" />
  <path data-field=\"n2a\" d=\"M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,17.28 L14.1,17.28 Z\"
         data-value=\"{values['n2a']}\" />
  <path data-field=\"n2b\" d=\"M14.1,28.62 A11.32,11.32 0 0,0 21.9,28.62 L21.9,18.72 L14.1,18.72 Z\"
        data-value=\"{values['n2b']}\" />
  <path data-field=\"has_claims\" d=\"M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,28.62 A11.32,11.32 0 0,1 14.1,28.62 Z\"
        fill=\"#fff\" data-value=\"{values['has_claims']}\" />
  <g data-field=\"redirect\" data-value=\"{values['redirect']}\">
    <path class=\"redirect-ring\"
          d=\"M18 32.66 A14.66 14.66 0 1 1 32.66 18\"
          fill=\"none\" stroke=\"#6a1b9a\" stroke-width=\"1.5\"
          marker-end=\"url(#report-redirect-arrowhead)\" />
  </g>
</svg>
"""


@app.on_event("startup")
async def startup_event() -> None:
    global SHUTDOWN_EVENT, PUBSUB_REAPER_TASK
    lookup_cache.assert_ready(
        required_property_qids=("Q105388954", "Q18614948", "Q62589316")
    )
    async with GADGET_SUBSCRIPTION_TOUCHES_LOCK:
        GADGET_SUBSCRIPTION_TOUCHES.clear()
    SHUTDOWN_EVENT = asyncio.Event()
    PUBSUB_REAPER_TASK = asyncio.create_task(_pubsub_reaper_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global PUBSUB_REAPER_TASK
    if SHUTDOWN_EVENT is not None:
        SHUTDOWN_EVENT.set()
    await _cancel_active_stream_tasks()
    if PUBSUB_REAPER_TASK is not None:
        PUBSUB_REAPER_TASK.cancel()
        try:
            await PUBSUB_REAPER_TASK
        except asyncio.CancelledError:
            pass
        PUBSUB_REAPER_TASK = None
    await close_wikidata_session()


def _is_valid_qid(qid: str) -> bool:
    return len(qid) >= 2 and qid[0] == "Q" and qid[1:].isdigit()


def _level_class(level: object) -> str:
    value = str(level).lower()
    if value == "none":
        return "level-none"
    if value == "weak":
        return "level-weak"
    if value == "strong":
        return "level-strong"
    return ""


def _is_property_id(value: object) -> bool:
    if not isinstance(value, str):
        return False
    return len(value) >= 2 and value[0] == "P" and value[1:].isdigit()


def _is_qid_like(value: object) -> bool:
    if not isinstance(value, str):
        return False
    return len(value) >= 2 and value[0] == "Q" and value[1:].isdigit()


def _wikidata_item_url(value: str) -> str:
    return f"https://www.wikidata.org/wiki/{value}"


def _render_property_value(key: str, value: object) -> str:
    if isinstance(value, str) and value.startswith(("http://", "https://")):
        escaped = escape(value)
        return f"<a href='{escaped}' target='_blank' rel='noopener noreferrer'>{escaped}</a>"

    if _is_qid_like(value):
        qid = str(value)
        href = _wikidata_item_url(qid)
        return f"<a href='{escape(href)}' target='_blank' rel='noopener noreferrer'>{escape(qid)}</a>"

    if key in {"property", "prop"} and _is_property_id(value):
        prop_id = str(value)
        href = f"https://www.wikidata.org/wiki/Property:{prop_id}"
        return f"<a href='{escape(href)}' target='_blank' rel='noopener noreferrer'>{escape(prop_id)}</a>"

    if isinstance(value, (list, tuple, set)):
        items = []
        for item in value:
            rendered = _render_property_value(key, item)
            items.append(f"<div>{rendered}</div>")
        return "".join(items) if items else "<em>empty</em>"

    return escape(json.dumps(value, ensure_ascii=False))


def _render_properties_html(properties: object) -> str:
    if not isinstance(properties, dict):
        return f"<pre>{escape(json.dumps(properties, indent=2))}</pre>"

    rows = "".join(
        "<tr>"
        f"<td>{escape(str(k))}</td>"
        f"<td>{_render_property_value(str(k), v)}</td>"
        "</tr>"
        for k, v in properties.items()
    )
    if not rows:
        rows = "<tr><td colspan='2'><em>empty</em></td></tr>"

    return (
        "<table class='props-table'><thead><tr><th>Property</th><th>Value</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def _render_errors_cell(criterion: object, errors: object) -> str:
    criterion_key = str(criterion)
    if criterion_key not in DETECTED_CRITERIA:
        return ""

    if not isinstance(errors, dict):
        return "<em>No errors</em>"

    criterion_errors = errors.get(criterion_key, [])
    if not isinstance(criterion_errors, list) or not criterion_errors:
        return "<em>No errors</em>"

    return "".join(f"<div>{escape(str(msg))}</div>" for msg in criterion_errors)


def _report_payload(result) -> dict:
    return web_build_signal_debug_payload(result)


def _item_link_html(qid: str | None) -> str:
    if not qid:
        return ""
    escaped_qid = escape(qid)
    href = f"https://www.wikidata.org/wiki/{escaped_qid}"
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">{escaped_qid}</a>'


def _render_report_html(report: dict | None) -> str:
    return web_render_signal_debug_html(report)


def _utc_isoformat(value: object | None) -> str | None:
    if value is None:
        return None
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _cache_snapshot_payload(
    result: EvaluationResult,
    entitydata_last_revid: int | None,
    recent_changes_last_revid: int | None,
    *,
    creation_time: object | None = None,
    last_updated: object | None = None,
    inlinks_last_evaluated: object | None = None,
) -> dict[str, object]:
    return {
        "qid": result.qid,
        "levels": result.levels_str,
        "errors": result.errors,
        "has_claims": result.has_claims,
        "has_claims_known": result.has_claims_known,
        "has_sitelinks": result.has_sitelinks,
        "is_redirect": result.is_redirect,
        "is_deleted": result.is_deleted,
        "summary": result.summary,
        "creation_time": None if creation_time is None else int(creation_time),
        "creation_time_iso": _utc_isoformat(creation_time),
        "last_updated": None if last_updated is None else int(last_updated),
        "last_updated_iso": _utc_isoformat(last_updated),
        "inlinks_last_evaluated": None if inlinks_last_evaluated is None else int(inlinks_last_evaluated),
        "inlinks_last_evaluated_iso": _utc_isoformat(inlinks_last_evaluated),
        "entitydata_last_revid": entitydata_last_revid,
        "recent_changes_last_revid": recent_changes_last_revid,
    }


def _compare_report_to_cache(
    live_report: dict[str, Any],
    cached_snapshot: dict[str, Any] | None,
    *,
    comparison_levels: tuple[str, ...] | list[str] | None = None,
) -> dict[str, object]:
    if not isinstance(cached_snapshot, dict):
        return {"status": "missing", "items": [{"field": "cache", "cache": "", "live": "missing"}]}

    discrepancies: list[dict[str, object]] = []
    live_levels = live_report.get("levels", {})
    cached_levels = cached_snapshot.get("levels", {})
    level_keys: tuple[str, ...]
    if isinstance(comparison_levels, (list, tuple)) and comparison_levels:
        level_keys = tuple(str(key) for key in comparison_levels)
    else:
        level_keys = tuple(sorted(set(live_levels) | set(cached_levels))) if isinstance(live_levels, dict) and isinstance(cached_levels, dict) else ()
    if isinstance(live_levels, dict) and isinstance(cached_levels, dict):
        for key in level_keys:
            live_value = live_levels.get(key)
            cache_value = cached_levels.get(key)
            if str(live_value) != str(cache_value):
                discrepancies.append(
                    {
                        "field": f"levels.{key}",
                        "cache": cache_value,
                        "live": live_value,
                    }
                )

    for field in ("summary", "has_claims", "has_claims_known", "has_sitelinks", "is_redirect", "is_deleted"):
        live_value = live_report.get(field)
        cache_value = cached_snapshot.get(field)
        if live_value != cache_value:
            discrepancies.append(
                {
                    "field": field,
                    "cache": cache_value,
                    "live": live_value,
                }
            )

    return {
        "status": "ok",
        "count": len(discrepancies),
        "items": discrepancies,
    }


async def _evaluate_live_reports(qids: list[str]) -> dict[str, EvaluationResult]:
    qid_list = [qid for qid in qids if isinstance(qid, str) and _is_valid_qid(qid)]
    if not qid_list:
        return {}

    part_buckets: dict[str, list[EvaluationResult]] = {qid: [] for qid in qid_list}
    async with foreground_evaluation():
        contexts_by_source = await asyncio.gather(*(source.get_contexts(qid_list) for source in EVALUATION_SOURCES))
        for source, contexts in zip(EVALUATION_SOURCES, contexts_by_source, strict=True):
            for qid in qid_list:
                context = contexts.get(qid)
                if isinstance(context, EntityDeletedError):
                    part = EvaluationResult(qid=qid, is_deleted=True)
                elif isinstance(context, Exception):
                    part = EvaluationResult(qid=qid)
                    for detector in source.detectors:
                        part.add_error(detector, context)
                elif qid not in contexts:
                    part = EvaluationResult(qid=qid)
                    for detector in source.detectors:
                        part.add_error(detector, RuntimeError(f"Source {source.name} did not return context for {qid}"))
                else:
                    part = await source.run_context(qid, context)
                part_buckets[qid].append(part)

    return {qid: EvaluationResult.combine(qid, parts) for qid, parts in part_buckets.items()}


async def _fetch_cached_snapshot(qid: str) -> dict[str, object] | None:
    await CACHE.initialize()
    qid_num = CACHE._parse_qid(qid)
    async with CACHE._connect() as db:
        cursor = await db.execute(
            """
            SELECT
                qid,
                summary,
                last_updated,
                creation_time,
                entitydata_last_revid,
                recent_changes_last_revid,
                inlinks_last_evaluated
            FROM evaluation_cache
            WHERE qid = ?
            """,
            (qid_num,),
        )
        row = await cursor.fetchone()

    if row is None:
        return None

    cached_result = EvaluationResult.from_summary(qid=qid, summary=int(row[1]))
    return _cache_snapshot_payload(
        cached_result,
        None if row[4] is None else int(row[4]),
        None if row[5] is None else int(row[5]),
        creation_time=row[3],
        last_updated=row[2],
        inlinks_last_evaluated=row[6],
    )


async def _fetch_interest_report(qid: str) -> dict[str, object] | None:
    await CACHE.initialize()
    now = int(time.time())
    async with CACHE._connect() as db:
        cursor = await db.execute(
            """
            SELECT
                owner_id,
                COUNT(*) AS session_rows,
                SUM(COALESCE(priority, 0)) AS total_priority,
                SUM(CASE WHEN wants_entitydata = 1 THEN 1 ELSE 0 END) AS wants_entitydata_rows,
                SUM(CASE WHEN wants_inlinks = 1 THEN 1 ELSE 0 END) AS wants_inlinks_rows,
                SUM(CASE WHEN wants_sync = 1 THEN 1 ELSE 0 END) AS wants_sync_rows,
                MAX(CASE WHEN wants_entitydata = 1 THEN 1 ELSE 0 END) AS wants_entitydata,
                MAX(CASE WHEN wants_inlinks = 1 THEN 1 ELSE 0 END) AS wants_inlinks,
                MAX(CASE WHEN wants_sync = 1 THEN 1 ELSE 0 END) AS wants_sync
            FROM pubsub_sessions
            WHERE qid = ?
              AND qid != 0
              AND expires_at > ?
            GROUP BY owner_id
            ORDER BY owner_id ASC
            """,
            (CACHE._parse_qid(qid), now),
        )
        rows = await cursor.fetchall()

    if not rows:
        return None

    workers = []
    session_rows = 0
    total_priority = 0
    for row in rows:
        owner_id = str(row[0])
        row_session_rows = int(row[1]) if row[1] is not None else 0
        row_priority = int(row[2]) if row[2] is not None else 0
        session_rows += row_session_rows
        total_priority += row_priority
        workers.append(
            {
                "owner_id": owner_id,
                "session_rows": row_session_rows,
                "total_priority": row_priority,
                "wants_entitydata_rows": int(row[3]) if row[3] is not None else 0,
                "wants_inlinks_rows": int(row[4]) if row[4] is not None else 0,
                "wants_sync_rows": int(row[5]) if row[5] is not None else 0,
                "wants_entitydata": bool(row[6]),
                "wants_inlinks": bool(row[7]),
                "wants_sync": bool(row[8]),
            }
        )

    return {
        "session_rows": session_rows,
        "owner_count": len(workers),
        "total_priority": total_priority,
        "workers": workers,
    }


async def _fetch_queue_report(qid: str, interest: dict[str, object] | None = None) -> dict[str, object]:
    if interest is None:
        interest = await _fetch_interest_report(qid)
    normalized_qid = qid.strip().upper()
    active_targets, cache_only_candidates, refresh_candidates = await asyncio.gather(
        CACHE.pubsub.list_pubsub_inlinks_targets(),
        CACHE.list_unknown_inlinks_qids(),
        CACHE.list_known_inlinks_refresh_candidates(),
    )

    def _path_report(
        *,
        name: str,
        rule: str,
        items: list[str],
        batch_size: int,
        active: bool,
        requires_idle_worker: bool = False,
        max_in_flight: int | None = None,
    ) -> dict[str, object]:
        present = normalized_qid in items
        position = items.index(normalized_qid) + 1 if present else None
        ahead = None if position is None else max(0, position - 1)
        if position is None:
            estimate = "not queued"
        elif ahead == 0:
            estimate = "next batch"
        else:
            estimate = f"about {ahead // max(1, batch_size) + 1} batch(es)"
        if active:
            status = "active"
        elif present and requires_idle_worker:
            status = "waiting for idle worker"
        elif present:
            status = "eligible"
        else:
            status = "not queued"
        return {
            "name": name,
            "rule": rule,
            "active": active,
            "present": present,
            "status": status,
            "position": position,
            "ahead": ahead,
            "batch_size": batch_size,
            "max_in_flight": max_in_flight,
            "estimate": estimate,
        }

    active_path = normalized_qid in active_targets
    cache_only_path = normalized_qid in cache_only_candidates and not active_path
    refresh_path = normalized_qid in refresh_candidates and not active_path and not cache_only_path

    return {
        "paths": [
            _path_report(
                name="Subscribed targets",
                rule="active pubsub interest",
                items=active_targets,
                batch_size=INLINKS_VISIBLE_LIMIT,
                active=active_path,
            ),
            _path_report(
                name="Cache-only fallback",
                rule="no active interest and no active state",
                items=cache_only_candidates,
                batch_size=INLINKS_WORKER_CACHE_ONLY_BATCH_SIZE,
                active=cache_only_path,
                requires_idle_worker=True,
            ),
            _path_report(
                name="Low-priority refresh",
                rule="stale known items with no active interest",
                items=refresh_candidates,
                batch_size=INLINKS_LOW_PRIORITY_CONSIDER_LIMIT,
                active=refresh_path,
                max_in_flight=INLINKS_LOW_PRIORITY_MAX_IN_FLIGHT,
            ),
        ],
        "active_targets": len(active_targets),
        "cache_only_candidates": len(cache_only_candidates),
        "refresh_candidates": len(refresh_candidates),
        "interest": interest,
    }


async def _build_inlinks_scan_report(qid: str) -> dict[str, object] | None:
    contexts = await INLINKS_SOURCE.get_contexts([qid])
    context = contexts.get(qid)
    if not isinstance(context, dict):
        return None

    raw_inlinks = context.get("inlinks", [])
    if not isinstance(raw_inlinks, list):
        raw_inlinks = []

    visible_inlinks = [inlink for inlink in raw_inlinks if isinstance(inlink, str) and _is_valid_qid(inlink)]
    if not visible_inlinks:
        return {
            "visible_inlinks": [],
            "truncated": bool(context.get("truncated", False)),
            "reports": [],
        }

    cached_rows, inlink_live_reports = await asyncio.gather(
        CACHE.get_many(visible_inlinks),
        _evaluate_live_reports(visible_inlinks),
    )
    reports: list[dict[str, object]] = []
    for inlink_qid in visible_inlinks:
        live_result = inlink_live_reports.get(inlink_qid)
        if live_result is None:
            continue
        report = _report_payload(live_result)
        cached_row = cached_rows.get(inlink_qid)
        if cached_row is not None:
            cached_result = EvaluationResult.from_summary(qid=inlink_qid, summary=cached_row[0])
            report["cached_snapshot"] = _cache_snapshot_payload(
                cached_result,
                cached_row[1],
                cached_row[2],
            )
        else:
            report["cached_snapshot"] = None
        report["report_variant"] = "inlinks"
        report["comparison_levels"] = ("N1", "N2")
        report["cache_discrepancies"] = _compare_report_to_cache(
            report,
            report["cached_snapshot"],
            comparison_levels=("N1", "N2"),
        )
        report["html"] = _render_report_html(report)
        reports.append(report)

    return {
        "visible_inlinks": visible_inlinks,
        "truncated": bool(context.get("truncated", False)),
        "reports": reports,
    }


async def _evaluate_or_404(qid: str) -> dict:
    if not _is_valid_qid(qid):
        raise HTTPException(status_code=400, detail="qid must look like Q42")
    try:
        live_reports = await _evaluate_live_reports([qid])
        live_result = live_reports[qid]
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    payload = _report_payload(live_result)
    cached_snapshot, interest, inlinks_scan = await asyncio.gather(
        _fetch_cached_snapshot(qid),
        _fetch_interest_report(qid),
        _build_inlinks_scan_report(qid),
    )
    payload["cached_snapshot"] = cached_snapshot
    payload["interest"] = interest
    payload["queue"] = await _fetch_queue_report(qid, interest)
    payload["inlinks_scan"] = inlinks_scan
    payload["html"] = _render_report_html(payload)
    return payload


async def _cached_or_404(qid: str) -> dict:
    if not _is_valid_qid(qid):
        raise HTTPException(status_code=400, detail="qid must look like Q42")

    result, entitydata_last_revid, recent_changes_last_revid = await CACHE.get(qid)
    if result is None:
        raise HTTPException(status_code=404, detail="No cached result for this QID")

    payload: dict[str, object] = {
        "qid": result.qid,
        "levels": result.levels_str,
        "errors": result.errors,
        "has_claims": result.has_claims,
        "has_sitelinks": result.has_sitelinks,
        "is_redirect": result.is_redirect,
        "is_deleted": result.is_deleted,
        "summary": result.summary,
        "entitydata_last_revid": entitydata_last_revid,
        "recent_changes_last_revid": recent_changes_last_revid,
    }
    return payload


def _render_creations_dashboard_html() -> HTMLResponse:
    return HTMLResponse(
        content="""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
    <link rel="icon" href="/static/favicon-32.png" type="image/png" sizes="32x32" />
    <link rel="icon" href="/static/favicon-16.png" type="image/png" sizes="16x16" />
    <link rel="shortcut icon" href="/favicon.ico" />
    <title>wd_notability creations dashboard</title>
    <style>
      :root {
        color-scheme: light dark;
        --bg: #f7f5ef;
        --panel: #fff;
        --panel-2: #f2ede3;
        --text: #1d1b17;
        --muted: #665f54;
        --border: #d8d0c3;
        --link: #0645ad;
        --accent: #8a5b1f;
        --strong: #1b7f2a;
        --weak: #b26a00;
        --none: #b00020;
        --unknown: #6c727a;
      }
      @media (prefers-color-scheme: dark) {
        :root {
          --bg: #121212;
          --panel: #1a1d21;
          --panel-2: #20252b;
          --text: #e8eaed;
          --muted: #b2b6bb;
          --border: #333943;
          --link: #8ab4f8;
          --accent: #d3a15d;
          --strong: #81c995;
          --weak: #ffd166;
          --none: #ff8a80;
          --unknown: #9aa0a6;
        }
      }
      body { margin: 0; font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); }
      header { padding: 1.25rem 1.5rem; border-bottom: 1px solid var(--border); background: linear-gradient(135deg, var(--panel), var(--panel-2)); }
      h1 { margin: 0 0 .35rem; font-size: 1.6rem; }
      .subtle { color: var(--muted); margin: 0; }
      main { padding: 1rem 1.5rem 2rem; display: grid; gap: 1rem; }
      .controls, .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 1rem; }
      .controls-group { display: grid; gap: .75rem; grid-template-columns: repeat(12, minmax(0, 1fr)); align-items: end; }
      .controls-group.population { grid-template-columns: minmax(14rem, 1.1fr) minmax(14rem, 1.1fr) minmax(18rem, 1.4fr); }
      .controls-group h2 { grid-column: 1 / -1; margin: 0; font-size: 1rem; color: var(--muted); letter-spacing: .02em; text-transform: uppercase; }
      .controls form { display: grid; gap: .75rem; grid-template-columns: repeat(12, minmax(0, 1fr)); align-items: end; }
      .controls label { display: grid; gap: .25rem; font-size: .92rem; }
      .controls label input, .controls label select { width: 100%; box-sizing: border-box; }
      .controls input, .controls select, .controls button {
        padding: .55rem .65rem; border-radius: 8px; border: 1px solid var(--border); background: var(--panel); color: var(--text);
      }
      .controls .span-2 { grid-column: span 2; }
      .controls .span-3 { grid-column: span 3; }
      .controls .span-4 { grid-column: span 4; }
      .controls .span-6 { grid-column: 1 / -1; }
      .controls .stack { display: grid; gap: .75rem; }
      .controls .checkline {
        display: flex;
        align-items: center;
        gap: .5rem;
        font-size: .92rem;
        padding: .45rem .55rem;
        border: 1px solid var(--border);
        border-radius: 8px;
        background: var(--panel);
      }
      .meta { display: flex; flex-wrap: wrap; gap: 1rem; color: var(--muted); font-size: .92rem; }
      .nav { display: flex; gap: 1rem; flex-wrap: wrap; margin-top: .5rem; }
      .nav a { color: var(--link); text-decoration: none; }
      .status { margin: 0; }
      .status.error { color: var(--none); font-weight: 700; }
      .dashboard { display: grid; gap: 1rem; }
      .hidden { display: none !important; }
      table { width: 100%; border-collapse: collapse; }
      th, td { border-top: 1px solid var(--border); padding: .45rem .5rem; text-align: left; vertical-align: top; }
      th { background: var(--panel-2); position: sticky; top: 0; z-index: 1; }
      .bucket-strong { color: var(--strong); font-weight: 700; }
      .bucket-weak { color: var(--weak); font-weight: 700; }
      .bucket-none { color: var(--none); font-weight: 700; }
      .bucket-unknown { color: var(--unknown); font-weight: 700; }
      .bucket-empty { color: var(--accent); font-weight: 700; }
      .bucket-redirect, .bucket-deleted { font-weight: 800; }
      .cards { display: grid; gap: .75rem; grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr)); }
      .card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: .85rem; }
      .card .label { color: var(--muted); font-size: .86rem; }
      .card .value { font-size: 1.6rem; font-weight: 800; }
      .stack { display: grid; gap: .5rem; }
      .bar { display: flex; height: 1rem; border-radius: 999px; overflow: hidden; border: 1px solid var(--border); }
      .bar > span { display: block; min-width: 0; box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.18); }
      .bar > span.bucket-strong, .swatch.bucket-strong { background: var(--strong); }
      .bar > span.bucket-weak, .swatch.bucket-weak { background: var(--weak); }
      .bar > span.bucket-none, .swatch.bucket-none { background: var(--none); }
      .bar > span.bucket-unknown, .swatch.bucket-unknown { background: var(--unknown); }
      .bar > span.bucket-empty, .swatch.bucket-empty { background: #fff; }
      .bar > span.bucket-redirect, .swatch.bucket-redirect { background: #7b1fa2; }
      .bar > span.bucket-deleted, .swatch.bucket-deleted { background: #000; }
      .bar > span.bucket-empty,
      .swatch.bucket-none, .swatch.bucket-empty {
        box-shadow: inset 0 0 0 1px var(--border);
      }
      .legend { display: flex; flex-wrap: wrap; gap: .75rem 1rem; color: var(--muted); font-size: .92rem; }
      .legend span { display: inline-flex; align-items: center; gap: .35rem; }
      .swatch { width: .75rem; height: .75rem; border-radius: 999px; display: inline-block; border: 1px solid var(--border); box-sizing: border-box; }
      .label-cell { display: flex; flex-direction: column; gap: .1rem; }
      .label-cell .qid { color: var(--muted); font-size: .9rem; }
      .table-wrap { overflow: auto; border: 1px solid var(--border); border-radius: 12px; }
      .timeline-grid { display: grid; gap: .5rem; }
      .timeline-row { display: grid; grid-template-columns: 12rem 1fr; gap: .75rem; align-items: center; }
      .timeline-key { color: var(--muted); font-size: .92rem; display: flex; align-items: center; gap: .4rem; }
      .timeline-total { color: var(--text); font-weight: 700; font-size: .88rem; }
      .user-key { color: var(--text); font-weight: 700; }
      .user-key-label { color: var(--link); }
      .timeline-bar { height: 1.1rem; }
      @media (max-width: 900px) {
        .controls-group, .controls form { grid-template-columns: 1fr; }
        .controls .span-2, .controls .span-3, .controls .span-4, .controls .span-6 { grid-column: 1 / -1; }
      }
    </style>
  </head>
  <body>
    <header>
      <h1>Notability Creations Dashboard</h1>
      <p class="subtle">Population is fixed for the selected window. Evaluation stays live through the existing subscribe stream.</p>
      <nav class="nav">
        <a href="/help.md">Help</a>
        <a href="/badge.md">Badge</a>
        <a href="/observability">Observability</a>
        <a href="/">Item report</a>
      </nav>
    </header>
    <main>
      <section class="controls panel">
        <form id="query-form">
          <div class="controls-group population">
            <h2>Population</h2>
            <label class="span-4">Start UTC <input id="start" name="start" type="text" placeholder="2026-06-01T00:00:00Z or 1h" /></label>
            <label class="span-4">End UTC <input id="end" name="end" type="text" placeholder="2026-07-01T00:00:00Z" /></label>
            <label class="span-4">Creators <input id="creators" name="creators" type="text" placeholder="comma-separated usernames" /></label>
            <div class="span-6">
              <button type="submit">Load report</button>
            </div>
          </div>
        </form>
      </section>
      <section class="controls panel">
        <div class="controls-group">
          <h2>Live settings</h2>
          <label class="span-4">Bucket by
            <select id="group_by" name="group_by">
              <option value="" selected hidden></option>
              <option value="hour">Hour</option>
              <option value="user">User</option>
              <option value="day">Day</option>
              <option value="week">Week</option>
              <option value="month">Month</option>
              <option value="year">Year</option>
            </select>
          </label>
          <label class="span-4">Sort by
            <select id="bucket_sort" name="bucket_sort">
              <option value="time_desc">Time (desc)</option>
              <option value="lexical_asc">Lexical (asc)</option>
              <option value="count_desc">Number of items (desc)</option>
              <option value="strong_rate_asc">Strong rate (asc)</option>
            </select>
          </label>
          <label class="span-4">Only include users with at least
            <input id="min_user_items" name="min_user_items" type="number" min="1" step="1" placeholder="N items" />
          </label>
          <label class="span-6">
            Aggregate temporary users
            <div class="checkline">
              <input id="aggregate_temporary_users" name="aggregate_temporary_users" type="checkbox" />
              <span>Fold temporary-user rows together</span>
            </div>
          </label>
          <label class="span-6">
            N2 mode
            <div class="checkline">
              <input id="allow_either_n2" name="allow_either_n2" type="checkbox" />
              <span>Allow either N2a or N2b</span>
            </div>
          </label>
        </div>
      </section>
      <section class="panel">
        <p id="status" class="status">Choose a window and load the report.</p>
        <div class="meta">
          <span id="population-count">Population: 0</span>
          <span id="evaluated-count">Evaluated: 0</span>
          <span id="updated-count">Updated: 0</span>
        </div>
      </section>
      <section class="dashboard">
        <section id="overview-panel" class="panel">
          <div class="cards" id="overview-cards"></div>
          <div class="stack" style="margin-top: .9rem;">
            <div class="bar" id="overview-bar"></div>
            <div class="legend" id="overview-legend"></div>
          </div>
        </section>
        <section id="timeline-panel" class="panel">
          <div class="timeline-grid" id="timeline-grid"></div>
        </section>
      </section>
    </main>
    <script src="/static/creations.js"></script>
  </body>
    </html>
        """
    )


def _render_observability_dashboard_html() -> HTMLResponse:
    return HTMLResponse(
        content="""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
    <link rel="icon" href="/static/favicon-32.png" type="image/png" sizes="32x32" />
    <link rel="icon" href="/static/favicon-16.png" type="image/png" sizes="16x16" />
    <link rel="shortcut icon" href="/favicon.ico" />
    <title>wd_notability observability</title>
    <style>
      :root {
        color-scheme: light dark;
        --bg: #f5f7fb;
        --panel: rgba(255, 255, 255, 0.86);
        --panel-strong: #ffffff;
        --text: #172033;
        --muted: #5c677d;
        --border: rgba(71, 85, 105, 0.2);
        --accent: #0f766e;
        --accent-2: #2563eb;
      }
      @media (prefers-color-scheme: dark) {
        :root {
          --bg: #0b1220;
          --panel: rgba(15, 23, 42, 0.86);
          --panel-strong: #111827;
          --text: #e5eefc;
          --muted: #9ca9bf;
          --border: rgba(148, 163, 184, 0.22);
          --accent: #5eead4;
          --accent-2: #60a5fa;
        }
      }
      body {
        margin: 0;
        min-height: 100vh;
        font-family: ui-sans-serif, system-ui, sans-serif;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(37, 99, 235, 0.18), transparent 26%),
          radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 24%),
          linear-gradient(180deg, var(--bg), var(--bg));
      }
      header {
        padding: 1.25rem 1.5rem 1rem;
        border-bottom: 1px solid var(--border);
        background: linear-gradient(135deg, var(--panel-strong), var(--panel));
        backdrop-filter: blur(14px);
      }
      h1 {
        margin: 0;
        font-size: clamp(1.7rem, 4vw, 2.6rem);
        letter-spacing: -0.04em;
      }
      .subtle {
        margin: .45rem 0 0;
        color: var(--muted);
        max-width: 60rem;
      }
      .nav {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-top: .8rem;
      }
      .nav a { color: var(--accent-2); text-decoration: none; }
      main {
        padding: 1rem 1.5rem 2rem;
        display: grid;
        gap: 1rem;
      }
      .panel {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
        backdrop-filter: blur(10px);
      }
      .summary {
        display: grid;
        gap: .75rem;
        grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
      }
      .summary .card {
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: .85rem 1rem;
      }
      .summary .label {
        color: var(--muted);
        font-size: .88rem;
        text-transform: uppercase;
        letter-spacing: .05em;
      }
      .summary .value {
        font-size: 1.4rem;
        font-weight: 750;
        margin-top: .2rem;
      }
      .toolbar {
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        gap: .85rem 1rem;
        align-items: end;
      }
      .controls {
        display: flex;
        flex-wrap: wrap;
        gap: .65rem .75rem;
        align-items: end;
      }
      .controls label {
        display: grid;
        gap: .3rem;
        color: var(--muted);
        font-size: .88rem;
      }
      .controls input[type="search"] {
        min-width: min(100%, 24rem);
        padding: .72rem .85rem;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        font: inherit;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
      }
      .controls input[type="search"]::placeholder {
        color: var(--muted);
      }
      .controls select {
        min-width: 8.5rem;
        padding: .72rem .85rem;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        font: inherit;
      }
      .controls button {
        appearance: none;
        border: 1px solid rgba(37, 99, 235, 0.24);
        background: linear-gradient(180deg, rgba(37, 99, 235, 0.12), rgba(37, 99, 235, 0.06));
        color: var(--text);
        border-radius: 12px;
        padding: .72rem 1rem;
        font: inherit;
        font-weight: 650;
        cursor: pointer;
      }
      .controls button:hover {
        border-color: rgba(37, 99, 235, 0.45);
      }
      .controls button:active {
        transform: translateY(1px);
      }
      .toggle {
        display: inline-flex;
        align-items: center;
        gap: .5rem;
        padding: .72rem .9rem;
        border: 1px solid var(--border);
        border-radius: 12px;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        user-select: none;
      }
      .toggle input {
        margin: 0;
      }
      .worker-grid {
        display: grid;
        gap: 1rem;
      }
      details.worker-section {
        border: 1px solid var(--border);
        border-radius: 16px;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        overflow: hidden;
      }
      details.worker-section[open] {
        box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
      }
      summary.worker-summary {
        list-style: none;
        cursor: pointer;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 1rem;
        padding: 1rem 1.1rem;
        border-bottom: 1px solid var(--border);
      }
      summary.worker-summary::-webkit-details-marker {
        display: none;
      }
      .worker-title {
        display: grid;
        gap: .2rem;
      }
      .worker-title h2 {
        margin: 0;
        font-size: 1.05rem;
      }
      .worker-title .meta {
        color: var(--muted);
        font-size: .92rem;
      }
      .worker-body {
        display: grid;
        gap: .85rem;
        padding: 1rem 1.1rem 1.1rem;
      }
      .metric-grid {
        display: grid;
        gap: .75rem;
        grid-template-columns: repeat(auto-fill, minmax(15rem, 1fr));
      }
      .cache-breakdown-grid {
        display: grid;
        gap: .85rem;
        grid-template-columns: repeat(auto-fit, minmax(19rem, 1fr));
      }
      .cache-breakdown-section {
        display: grid;
        gap: .8rem;
      }
      .cache-breakdown-section + .cache-breakdown-section {
        margin-top: .3rem;
      }
      .cache-breakdown-section .section-head {
        display: grid;
        gap: .15rem;
      }
      .cache-breakdown-section .section-head .title {
        font-size: 1rem;
        font-weight: 700;
      }
      .cache-breakdown-section .section-head .subtitle {
        font-size: .84rem;
        line-height: 1.25;
        color: var(--muted);
      }
      .stacked-chart-card {
        display: grid;
        gap: .65rem;
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: .9rem .95rem 1rem;
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.98), rgba(20, 31, 51, 0.92));
        color: #f8fafc;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 8px 24px rgba(0, 0, 0, 0.18);
      }
      .stacked-chart-card .stacked-chart-head {
        display: grid;
        gap: .15rem;
      }
      .stacked-chart-card .title {
        font-size: 1rem;
        font-weight: 700;
      }
      .stacked-chart-card .subtitle {
        font-size: .82rem;
        line-height: 1.25;
        color: rgba(226, 232, 240, 0.76);
      }
      .stacked-chart {
        width: 100%;
        height: 220px;
      }
      .metric-tile {
        appearance: none;
        border: 1px solid var(--border);
        border-radius: 18px;
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.98), rgba(20, 31, 51, 0.92));
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 8px 24px rgba(0, 0, 0, 0.18);
        color: #f8fafc;
        padding: 1rem 1rem .85rem;
        text-align: left;
        aspect-ratio: 1;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        gap: .45rem;
        cursor: pointer;
        transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
        overflow: hidden;
      }
      .metric-tile:hover {
        transform: translateY(-1px);
        border-color: rgba(94, 234, 212, 0.55);
        box-shadow: 0 12px 28px rgba(0, 0, 0, 0.24);
      }
      .metric-tile .tile-head {
        display: flex;
        justify-content: space-between;
        gap: .75rem;
        align-items: flex-start;
      }
      .metric-tile .label-block {
        display: grid;
        gap: .15rem;
        min-width: 0;
        flex: 1 1 auto;
      }
      .metric-tile .field {
        font-size: 1rem;
        font-weight: 700;
        line-height: 1.15;
        word-break: break-word;
        min-width: 0;
        color: #f8fafc;
      }
      .metric-tile .subtitle {
        font-size: .8rem;
        line-height: 1.2;
        color: rgba(226, 232, 240, 0.76);
        word-break: break-word;
      }
      .metric-tile .value {
        font-size: 2rem;
        font-weight: 800;
        line-height: 1;
        white-space: nowrap;
        color: #f8fafc;
        text-shadow: 0 1px 0 rgba(0, 0, 0, 0.2);
      }
      .metric-tile .sparkline-shell {
        display: grid;
        grid-template-columns: auto 1fr;
        gap: .5rem;
        align-items: stretch;
        min-height: 8.5rem;
      }
      .metric-tile .scale-y {
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        color: rgba(226, 232, 240, 0.82);
        font-size: .82rem;
        line-height: 1;
        min-width: 3.3rem;
        padding: .2rem 0;
        font-variant-numeric: tabular-nums;
      }
      .metric-tile .scale-y span {
        white-space: nowrap;
      }
      .metric-tile .sparkline {
        flex: 1 1 auto;
        display: flex;
        align-items: center;
        min-width: 0;
      }
      .metric-tile .sparkline svg {
        width: 100%;
        height: 7.5rem;
        display: block;
      }
      .metric-tile .stamp {
        color: rgba(226, 232, 240, 0.72);
        font-size: .84rem;
        line-height: 1.2;
        font-variant-numeric: tabular-nums;
      }
      @media (prefers-color-scheme: light) {
        .stacked-chart-card {
          background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(247, 250, 252, 0.94));
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 8px 24px rgba(15, 23, 42, 0.10);
          color: #172033;
        }
        .stacked-chart-card .subtitle {
          color: #52627a;
        }
        .metric-tile {
          background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(247, 250, 252, 0.94));
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 8px 24px rgba(15, 23, 42, 0.10);
          color: #172033;
        }
        .metric-tile:hover {
          border-color: rgba(37, 99, 235, 0.35);
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.12);
        }
        .metric-tile .field,
        .metric-tile .value {
          color: #172033;
          text-shadow: none;
        }
        .metric-tile .subtitle,
        .metric-tile .scale-y,
        .metric-tile .stamp {
          color: #52627a;
        }
      }
      .zoom-panel {
        display: grid;
        gap: .5rem;
        border: 1px solid var(--border);
        border-radius: 16px;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        padding: .75rem;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04), 0 8px 24px rgba(0, 0, 0, 0.12);
      }
      .zoom-panel .zoom-title {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        align-items: baseline;
        color: var(--text);
        font-size: .9rem;
      }
      .zoom-panel .zoom-title span:last-child {
        color: var(--muted);
      }
      .zoom-chart {
        width: 100%;
        height: 280px;
      }
      .zoom-placeholder {
        color: var(--muted);
        font-size: .92rem;
        padding: .5rem 0;
      }
      .empty-state {
        color: var(--muted);
        padding: 1rem 0;
      }
      @media (prefers-color-scheme: light) {
        .zoom-panel {
          background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(247, 250, 252, 0.96));
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 8px 24px rgba(15, 23, 42, 0.08);
        }
        .zoom-panel .zoom-title {
          color: #172033;
        }
        .zoom-panel .zoom-title span:last-child {
          color: #52627a;
        }
      }
      @media (max-width: 900px) {
        main { padding: .75rem; }
        .cache-breakdown-grid { grid-template-columns: 1fr; }
        .metric-grid { grid-template-columns: repeat(auto-fill, minmax(12rem, 1fr)); }
        .stacked-chart { height: 220px; }
        .zoom-chart { height: 260px; }
      }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  </head>
  <body>
    <header>
      <h1>Observability</h1>
      <p class="subtle">Periodic worker snapshots aggregated across all workers. The page defaults to the last 24 hours and uses built-in zoom for time navigation.</p>
      <nav class="nav">
        <a href="/help.md">Help</a>
        <a href="/badge.md">Badge</a>
        <a href="/creations">Creations</a>
        <a href="/pubsub">PubSub debugger</a>
        <a href="/">Item report</a>
      </nav>
    </header>
    <main>
      <section class="panel">
        <div class="summary" id="summary"></div>
      </section>
      <section class="panel">
        <div class="toolbar">
          <div class="controls">
            <label for="period">
              Window
              <select id="period">
                <option value="1h">1 hour</option>
                <option value="6h">6 hours</option>
                <option value="24h" selected>24 hours</option>
                <option value="7d">7 days</option>
              </select>
            </label>
            <button id="refresh" type="button">Refresh</button>
          </div>
          <div class="controls">
            <label class="toggle" for="autorefresh">
              <input id="autorefresh" type="checkbox" />
              Auto-refresh
            </label>
          </div>
        </div>
      </section>
      <section class="panel">
        <div id="worker-grid" class="worker-grid"></div>
        <div id="empty-state" class="empty-state hidden">No worker snapshots found for the selected window.</div>
      </section>
    </main>
    <script src="/static/observability.js?v={OBSERVABILITY_JS_VERSION}"></script>
  </body>
    </html>
        """
    )


def _render_pubsub_debugger_html() -> HTMLResponse:
    return HTMLResponse(
        content="""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
    <link rel="icon" href="/static/favicon-32.png" type="image/png" sizes="32x32" />
    <link rel="icon" href="/static/favicon-16.png" type="image/png" sizes="16x16" />
    <link rel="shortcut icon" href="/favicon.ico" />
    <title>wd_notability pubsub debugger</title>
    <style>
      :root {
        color-scheme: light dark;
        --bg: #f6f2e8;
        --panel: rgba(255, 255, 255, 0.86);
        --panel-strong: #ffffff;
        --text: #1b1d1f;
        --muted: #5e665e;
        --border: rgba(84, 93, 87, 0.18);
        --accent: #8a5a00;
        --accent-2: #0f766e;
        --chip: #f3ead7;
      }
      @media (prefers-color-scheme: dark) {
        :root {
          --bg: #101411;
          --panel: rgba(18, 24, 18, 0.88);
          --panel-strong: #151a15;
          --text: #edf3ea;
          --muted: #9eaa9a;
          --border: rgba(150, 167, 147, 0.18);
          --accent: #fbbf24;
          --accent-2: #5eead4;
          --chip: rgba(15, 23, 15, 0.8);
        }
      }
      body {
        margin: 0;
        min-height: 100vh;
        font-family: ui-sans-serif, system-ui, sans-serif;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(138, 90, 0, 0.18), transparent 26%),
          radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 24%),
          linear-gradient(180deg, var(--bg), var(--bg));
      }
      header {
        padding: 1.25rem 1.5rem 1rem;
        border-bottom: 1px solid var(--border);
        background: linear-gradient(135deg, var(--panel-strong), var(--panel));
        backdrop-filter: blur(14px);
      }
      h1 {
        margin: 0;
        font-size: clamp(1.7rem, 4vw, 2.6rem);
        letter-spacing: -0.04em;
      }
      .subtle {
        margin: .45rem 0 0;
        color: var(--muted);
        max-width: 62rem;
      }
      .nav {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-top: .8rem;
      }
      .nav a { color: var(--accent-2); text-decoration: none; }
      main {
        padding: 1rem 1.5rem 2rem;
        display: grid;
        gap: 1rem;
      }
      .panel {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
        backdrop-filter: blur(10px);
      }
      .summary {
        display: grid;
        gap: .75rem;
        grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
      }
      .card {
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: .85rem 1rem;
      }
      .card .label {
        color: var(--muted);
        font-size: .88rem;
        text-transform: uppercase;
        letter-spacing: .05em;
      }
      .card .value {
        font-size: 1.45rem;
        font-weight: 750;
        margin-top: .2rem;
      }
      .toolbar {
        display: flex;
        flex-wrap: wrap;
        gap: .75rem;
        align-items: center;
        justify-content: space-between;
        margin-bottom: .9rem;
      }
      .toolbar .controls {
        display: flex;
        flex-wrap: wrap;
        gap: .75rem;
        align-items: center;
      }
      .toolbar label {
        color: var(--muted);
        font-size: .92rem;
      }
      .toolbar input {
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: .55rem .7rem;
        background: var(--panel-strong);
        color: var(--text);
        min-width: 16rem;
      }
      .toolbar button {
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: .55rem .8rem;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        cursor: pointer;
      }
      .table-shell {
        overflow-x: auto;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        min-width: 960px;
      }
      thead th {
        text-align: left;
        font-size: .82rem;
        text-transform: uppercase;
        letter-spacing: .06em;
        color: var(--muted);
        border-bottom: 1px solid var(--border);
        padding: .7rem .6rem;
        position: sticky;
        top: 0;
        background: var(--panel);
        z-index: 1;
      }
      tbody td {
        border-bottom: 1px solid var(--border);
        padding: .75rem .6rem;
        vertical-align: top;
      }
      tbody tr:hover {
        background: rgba(15, 118, 110, 0.06);
      }
      .mono {
        font-variant-numeric: tabular-nums;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      }
      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: .4rem;
      }
      .chip {
        display: inline-flex;
        align-items: center;
        gap: .3rem;
        padding: .25rem .5rem;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: var(--chip);
        font-size: .84rem;
        color: var(--text);
      }
      .chip[data-yes="true"] {
        border-color: rgba(15, 118, 110, 0.32);
        color: var(--accent-2);
      }
      .chip[data-yes="false"] {
        color: var(--muted);
      }
      .workers {
        display: grid;
        gap: .4rem;
      }
      .worker-line {
        display: flex;
        flex-wrap: wrap;
        gap: .35rem .5rem;
        align-items: baseline;
      }
      .worker-line strong {
        font-weight: 650;
      }
      .empty-state {
        color: var(--muted);
        padding: 1rem 0 .2rem;
      }
      @media (max-width: 900px) {
        main { padding: .75rem; }
        .toolbar input { min-width: 0; width: 100%; }
      }
    </style>
  </head>
  <body>
    <header>
      <h1>PubSub debugger</h1>
      <p class="subtle">Aggregated pubsub interest grouped by QID. The table shows total priority, active session count, wants flags, and which owners currently hold interest.</p>
      <nav class="nav">
        <a href="/observability">Observability</a>
        <a href="/help.md">Help</a>
        <a href="/badge.md">Badge</a>
        <a href="/">Item report</a>
      </nav>
    </header>
    <main>
      <section class="panel">
        <div class="summary" id="summary"></div>
      </section>
      <section class="panel">
        <div class="toolbar">
          <div class="controls">
            <label for="filter">Filter QID or owner</label>
            <input id="filter" type="search" placeholder="Q123, gadget, inlinks" autocomplete="off" />
            <button id="refresh" type="button">Refresh</button>
          </div>
          <div class="controls">
            <label for="autorefresh">
              <input id="autorefresh" type="checkbox" checked />
              Auto-refresh
            </label>
          </div>
        </div>
        <div class="table-shell">
          <table>
            <thead>
              <tr>
                <th>QID</th>
                <th class="mono">Priority</th>
                <th class="mono">Sessions</th>
                <th class="mono">Owners</th>
                <th>Wants</th>
                <th>Owner IDs</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
        <div class="empty-state hidden" id="empty-state">No active pubsub interest found.</div>
      </section>
    </main>
    <script>
      (() => {
        const summary = document.getElementById("summary");
        const rows = document.getElementById("rows");
        const emptyState = document.getElementById("empty-state");
        const filterInput = document.getElementById("filter");
        const refreshButton = document.getElementById("refresh");
        const autorefresh = document.getElementById("autorefresh");
        let latestPayload = null;
        let refreshTimer = null;

        function escapeHtml(value) {
          return String(value)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
        }

        function chip(label, yes) {
          return `<span class="chip" data-yes="${yes ? "true" : "false"}">${escapeHtml(label)}</span>`;
        }

        function renderSummary(data) {
          const items = Array.isArray(data.items) ? data.items : [];
          const totalSessions = items.reduce((acc, item) => acc + Number(item.session_rows || 0), 0);
          const totalPriority = items.reduce((acc, item) => acc + Number(item.total_priority || 0), 0);
          const totalOwners = items.reduce((acc, item) => acc + Number(item.owner_count || 0), 0);
          summary.innerHTML = [
            ["Items", items.length],
            ["Sessions", totalSessions],
            ["Total priority", totalPriority],
            ["Owners", totalOwners],
          ].map(([label, value]) => `<div class="card"><div class="label">${escapeHtml(label)}</div><div class="value mono">${escapeHtml(value)}</div></div>`).join("");
        }

        function matchesFilter(item, filterValue) {
          if (!filterValue) return true;
          const haystack = [
            item.qid,
            ...(Array.isArray(item.workers) ? item.workers.map((worker) => worker.owner_id) : []),
          ].join(" ").toLowerCase();
          return haystack.includes(filterValue);
        }

        function renderRows(data) {
          const items = Array.isArray(data.items) ? data.items : [];
          const filterValue = filterInput.value.trim().toLowerCase();
          const filtered = items.filter((item) => matchesFilter(item, filterValue));
          rows.innerHTML = filtered.map((item) => {
            const workers = Array.isArray(item.workers) ? item.workers : [];
            const wants = [
              chip("entitydata", !!item.wants_entitydata),
              chip("inlinks", !!item.wants_inlinks),
              chip("sync", !!item.wants_sync),
            ].join("");
            const ownerIds = Array.isArray(item.owner_ids) && item.owner_ids.length
              ? item.owner_ids
              : workers.map((worker) => worker.owner_id);
            const ownerChips = ownerIds.map((ownerId) => chip(ownerId, true)).join("");
            return `
              <tr>
                <td class="mono">${escapeHtml(item.qid)}</td>
                <td class="mono">${escapeHtml(item.total_priority || 0)}</td>
                <td class="mono">${escapeHtml(item.session_rows || 0)}</td>
                <td class="mono">${escapeHtml(item.owner_count || 0)}</td>
                <td><div class="chips">${wants}</div></td>
                <td><div class="chips">${ownerChips || '<span class="mono">No owners</span>'}</div></td>
              </tr>
            `;
          }).join("");
          emptyState.classList.toggle("hidden", filtered.length !== 0);
        }

        async function loadData() {
          const response = await fetch("/api/pubsub/debug");
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          latestPayload = await response.json();
          renderSummary(latestPayload);
          renderRows(latestPayload);
        }

        async function refresh() {
          try {
            await loadData();
          } catch (error) {
            rows.innerHTML = `<tr><td colspan="6">Unable to load pubsub debug data: ${escapeHtml(error.message)}</td></tr>`;
            emptyState.classList.add("hidden");
          }
        }

        function scheduleRefresh() {
          if (refreshTimer) {
            clearTimeout(refreshTimer);
            refreshTimer = null;
          }
          if (!autorefresh.checked) {
            return;
          }
          refreshTimer = setTimeout(async () => {
            await refresh();
            scheduleRefresh();
          }, 15000);
        }

        filterInput.addEventListener("input", () => {
          if (latestPayload) {
            renderRows(latestPayload);
          }
        });
        refreshButton.addEventListener("click", async () => {
          await refresh();
          scheduleRefresh();
        });
        autorefresh.addEventListener("change", () => {
          scheduleRefresh();
        });

        refresh();
        scheduleRefresh();
      })();
    </script>
  </body>
</html>
        """
    )


async def _pubsub_event_stream(
    owner_id: str,
    session_id: str,
    request: Request,
    *,
    after_event_id: int = 0,
    poll_seconds: float = 2.0,
) -> AsyncGenerator[str, None]:
    stream_task = await _register_stream_task()
    cursor = max(0, after_event_id)
    deadline = time.monotonic() + SSE_STREAM_MAX_SECONDS
    try:
        while time.monotonic() < deadline:
            if SHUTDOWN_EVENT is not None and SHUTDOWN_EVENT.is_set():
                break
            if await request.is_disconnected():
                break

            rows = await CACHE.pubsub.list_pubsub_events_for_session(
                owner_id=owner_id,
                session_id=session_id,
                after_event_id=cursor,
                limit=500,
            )
            creation_metadata = await web_resolve_creation_metadata([f"Q{row['qid']}" for row in rows])
            emitted = False
            for row in rows:
                cursor = max(cursor, int(row["event_id"]))
                emitted = True
                summary_value = int(row["summary"])
                cached_result = EvaluationResult.from_summary(qid=f"Q{row['qid']}", summary=summary_value)
                qid = f"Q{row['qid']}"
                metadata = creation_metadata.get(qid, {})
                payload = {
                    "event": "summary_change",
                    "event_id": row["event_id"],
                    "timestamp": row["timestamp"],
                    "qid": qid,
                    "event_type": row["event_type"],
                    "summary": row["summary"],
                    "mask": row["mask"],
                }
                payload.update(
                    _badge_payload(
                        qid,
                        cached_result,
                        creator=metadata.get("creator"),
                        creation_time=metadata.get("creation_time"),
                    )
                )
                yield _sse_message(payload)

            if not emitted:
                yield _sse_message({"event": "keepalive"})

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if await _sleep_or_shutdown(min(poll_seconds, remaining)):
                break

        if (
            time.monotonic() >= deadline
            and (SHUTDOWN_EVENT is None or not SHUTDOWN_EVENT.is_set())
            and not await request.is_disconnected()
        ):
            yield _sse_message({"event": "stream_end"})
    finally:
        await _unregister_stream_task(stream_task)


@app.get("/api/items/{qid}/signals")
async def api_item_signals(qid: str):
    return await _evaluate_or_404(qid)


@app.get("/api/evaluate/{qid}")
async def api_evaluate_compat(qid: str):
    return await _evaluate_or_404(qid)


@app.get("/api/cache/stats")
async def api_cache_stats():
    return {
        **await CACHE.stats(),
        "lookup_cache": lookup_cache.stats(),
    }


@app.get("/api/cache/breakdown")
async def api_cache_breakdown():
    return await CACHE.breakdown()


@app.get("/api/cache/pubsub-stats")
async def api_cache_pubsub_stats():
    return await CACHE.pubsub.pubsub_stats()


@app.get("/api/pubsub/debug")
async def api_pubsub_debug(limit: int | None = Query(default=None)):
    items = await CACHE.pubsub.list_pubsub_interest_items(limit=limit)
    stats = await CACHE.pubsub.pubsub_stats()
    return {
        "generated_at": int(time.time()),
        "stats": stats,
        "items": items,
    }


@app.get("/api/worker-queue-stats")
async def api_worker_queue_stats():
    entitydata, deletion, inlinks, cache_sync, recent_changes = await asyncio.gather(
        entitydata_queue_stats(),
        deletion_queue_stats(),
        inlinks_queue_stats(),
        cache_sync_queue_stats(),
        recent_changes_queue_stats(),
    )
    return {
        "entitydata": entitydata,
        "deletion": deletion,
        "inlinks": inlinks,
        "cache_sync": cache_sync,
        "recent_changes": recent_changes,
    }


@app.get("/api/observability")
async def api_observability(
    period: str | None = Query(default="24h"),
    workers: list[str] = Query(default_factory=list),
):
    period_seconds = _parse_observability_period(period)
    until = int(time.time())
    since = until - period_seconds
    worker_filters = [worker.strip() for worker in workers if isinstance(worker, str) and worker.strip()]
    series, workers = await CACHE.observability.snapshot_views(
        since=since,
        until=until,
        worker_names=worker_filters or None,
    )
    chartable_fields = {field: points for field, points in series.items() if field in OBSERVABILITY_FIELD_METADATA}
    chartable_workers = {
        worker_name: {field: points for field, points in fields.items() if field in OBSERVABILITY_FIELD_METADATA}
        for worker_name, fields in workers.items()
    }
    return {
        "period": period if period is not None else "24h",
        "period_seconds": period_seconds,
        "period_label": _format_observability_title(period_seconds),
        "since": since,
        "until": until,
        "fields": chartable_fields,
        "workers": chartable_workers,
        "metrics": _observability_metrics_payload(),
    }


@app.get("/api/cache/items/{qid}")
async def api_cache_item(qid: str):
    return await _cached_or_404(qid)


@app.get("/api/creations")
async def api_creations(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    creators: list[str] = Query(default_factory=list),
):
    if isinstance(start, str) and not start.strip():
        start = None
    if isinstance(end, str) and not end.strip():
        end = None
    if start is None or end is None:
        from wd_notability.creations import CREATIONS
        default_start, default_end = CREATIONS.default_window()
        start = default_start if start is None else start
        end = default_end if end is None else end

    try:
        creator_actor_ids: list[int] = []
        creator_names_by_id: dict[int, str] = {}
        if creators:
            creator_actor_map = await asyncio.to_thread(CREATIONS.lookup_actor_ids, creators)
            creator_actor_ids = list(creator_actor_map.values())
            if not creator_actor_ids:
                return {
                    "start": start,
                    "end": end,
                    "creators": creators,
                    "items": [],
                }
        rows = await CACHE.list_creation_metadata(
            start=start,
            end=end,
            creator_actor_ids=creator_actor_ids,
        )
        print(f"Queried creations metadata for start={start}, end={end}, creator_actor_ids={creator_actor_ids}: got {len(rows)} rows")
        creator_names_by_id = await web_lookup_creator_names([row.creator_actor_id for row in rows])
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "start": start,
        "end": end,
        "creators": creators,
        "items": [
            {
                "qid": row.qid,
                "creator": creator_names_by_id.get(row.creator_actor_id, "Unknown creator"),
                "creation_time": row.creation_time,
            }
            for row in rows
        ],
    }


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(STATIC_DIR / "favicon.ico", media_type="image/x-icon")


@app.get("/{filename}.md", include_in_schema=False)
async def static_markdown_page(filename: str):
    return _render_static_markdown_page(f"{filename}.md")


@app.get("/observability", response_class=HTMLResponse)
async def observability_page():
    return _render_observability_dashboard_html()


@app.get("/pubsub", response_class=HTMLResponse)
async def pubsub_debug_page():
    return _render_pubsub_debugger_html()


@app.post("/subscribe")
async def api_subscribe(request: SubscribeRequest):
    items = _normalize_subscription_items(request)
    if not items:
        raise HTTPException(status_code=400, detail="qids must include at least one valid QID")

    qids = list(items)
    cached_items: list[dict[str, object]] = []
    missing_qids: list[str] = []
    creation_metadata = await web_resolve_creation_metadata(qids)
    cached_rows = await CACHE.get_many(qids)
    for qid in qids:
        row = cached_rows.get(qid)
        if row is None:
            missing_qids.append(qid)
            continue

        summary, entitydata_last_revid, recent_changes_last_revid = row
        cached_result = EvaluationResult.from_summary(qid=qid, summary=summary)
        cached_items.append(
            _cached_payload(
                qid,
                cached_result,
                entitydata_last_revid,
                recent_changes_last_revid,
                creator=creation_metadata.get(qid, {}).get("creator"),
                creation_time=creation_metadata.get(qid, {}).get("creation_time"),
            )
        )
        if cached_result.n == NotabilityLevel.UNKNOWN:
            missing_qids.append(qid)

    print(f"Subscription request for {len(qids)} QIDs: {len(cached_items)} cached, {len(missing_qids)} missing")

    subscription_id = request.session_id.strip() if isinstance(request.session_id, str) else ""
    grouped_qids = _group_subscription_qids_by_priority(items)
    ordered_priorities = sorted(grouped_qids, reverse=True)
    if not subscription_id:
        subscription_id = str(uuid4())
        first_priority = ordered_priorities[0]
        await CACHE.pubsub.create_pubsub_session(
            owner_id="gadget",
            session_id=subscription_id,
            ttl_seconds=PUBSUB_GADGET_SESSION_TTL_SECONDS,
            priority=first_priority,
            wants_entitydata=True,
            wants_inlinks=True,
            wants_sync=True,
            qids=grouped_qids[first_priority],
        )
        for priority in ordered_priorities[1:]:
            await CACHE.pubsub.add_pubsub_session_qids(
                owner_id="gadget",
                session_id=subscription_id,
                qids=grouped_qids[priority],
                priority=priority,
                wants_entitydata=True,
                wants_inlinks=True,
                wants_sync=True,
            )
    else:
        existing_qids = await CACHE.pubsub.list_pubsub_session_qids(
            owner_id="gadget",
            session_id=subscription_id,
        )
        if not existing_qids:
            first_priority = ordered_priorities[0]
            await CACHE.pubsub.create_pubsub_session(
                owner_id="gadget",
                session_id=subscription_id,
                ttl_seconds=PUBSUB_GADGET_SESSION_TTL_SECONDS,
                priority=first_priority,
                wants_entitydata=True,
                wants_inlinks=True,
                wants_sync=True,
                qids=grouped_qids[first_priority],
            )
            for priority in ordered_priorities[1:]:
                await CACHE.pubsub.add_pubsub_session_qids(
                    owner_id="gadget",
                    session_id=subscription_id,
                    qids=grouped_qids[priority],
                    priority=priority,
                    wants_entitydata=True,
                    wants_inlinks=True,
                    wants_sync=True,
                )
        else:
            for priority in ordered_priorities:
                await CACHE.pubsub.add_pubsub_session_qids(
                    owner_id="gadget",
                    session_id=subscription_id,
                    qids=grouped_qids[priority],
                    priority=priority,
                    wants_entitydata=True,
                    wants_inlinks=True,
                    wants_sync=True,
                )
            await CACHE.pubsub.refresh_pubsub_session(
                owner_id="gadget",
                session_id=subscription_id,
                ttl_seconds=PUBSUB_GADGET_SESSION_TTL_SECONDS,
            )

    return {
        "subscription_id": subscription_id,
        "reevaluate": REVALUATE_ON_SUBSCRIBE,
        "cached_items": cached_items,
        "cache_misses": missing_qids,
    }


@app.post("/api/pubsub/sessions/{owner_id}/{session_id}")
async def api_pubsub_create_session(owner_id: str, session_id: str, request: PubSubCreateRequest):
    owner = _normalize_owner_id(owner_id)
    qids = _normalize_qids(request.qids)
    created = await CACHE.pubsub.create_pubsub_session(
        owner_id=owner,
        session_id=session_id,
        ttl_seconds=request.ttl_seconds,
        priority=request.priority,
        wants_entitydata=request.wants_entitydata,
        wants_inlinks=request.wants_inlinks,
        wants_sync=request.wants_sync,
        qids=qids,
    )
    return {
        "owner_id": owner,
        "session_id": session_id,
        "ttl_seconds": request.ttl_seconds,
        "qids": qids,
        "created_rows": created,
    }


@app.post("/api/pubsub/sessions/{owner_id}/{session_id}/qids")
async def api_pubsub_add_session_qids(owner_id: str, session_id: str, request: PubSubAddRequest):
    owner = _normalize_owner_id(owner_id)
    qids = _normalize_qids(request.qids)
    added = await CACHE.pubsub.add_pubsub_session_qids(
        owner_id=owner,
        session_id=session_id,
        qids=qids,
        priority=request.priority,
        wants_entitydata=request.wants_entitydata,
        wants_inlinks=request.wants_inlinks,
        wants_sync=request.wants_sync,
    )
    return {
        "owner_id": owner,
        "session_id": session_id,
        "qids": qids,
        "added_rows": added,
    }


@app.patch("/api/pubsub/sessions/{owner_id}/{session_id}")
async def api_pubsub_refresh_session(owner_id: str, session_id: str, request: PubSubRefreshRequest):
    owner = _normalize_owner_id(owner_id)
    refreshed = await CACHE.pubsub.refresh_pubsub_session(
        owner_id=owner,
        session_id=session_id,
        ttl_seconds=request.ttl_seconds,
    )
    return {
        "owner_id": owner,
        "session_id": session_id,
        "ttl_seconds": request.ttl_seconds,
        "refreshed_rows": refreshed,
    }


@app.delete("/api/pubsub/sessions/{owner_id}/{session_id}")
async def api_pubsub_delete_session(owner_id: str, session_id: str):
    owner = _normalize_owner_id(owner_id)
    deleted = await CACHE.pubsub.delete_pubsub_session(owner_id=owner, session_id=session_id)
    return {
        "owner_id": owner,
        "session_id": session_id,
        "deleted_rows": deleted,
    }


@app.get("/api/pubsub/sessions/{owner_id}/{session_id}/events")
async def api_pubsub_session_events(
    owner_id: str,
    session_id: str,
    request: Request,
    after_event_id: int = Query(default=0, ge=0),
):
    owner = _normalize_owner_id(owner_id)
    return StreamingResponse(
        _pubsub_event_stream(owner, session_id, request, after_event_id=after_event_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@app.get("/", response_class=HTMLResponse)
async def ui_home(qid: str = Query(default="")):
    error: str | None = None
    should_load = False

    qid = qid.strip().upper()
    if qid and _is_valid_qid(qid):
        should_load = True
    elif qid:
        error = "400: qid must look like Q42"

    escaped_qid = escape(qid)
    badge_html = _render_report_badge(None, qid)
    item_link_html = _item_link_html(qid) if should_load else ""
    report_html = "<p class='status' id='evaluation-status'>Enter a QID to evaluate an item.</p>"

    if error:
        report_html = f"<p class='error'>{escape(error)}</p>"
    elif should_load:
        report_html = (
            "<section class='result-panel' aria-live='polite'>"
            "<p class='status' id='evaluation-status'>Evaluating...</p>"
            "<section id='report-output'></section>"
            "</section>"
        )

    return HTMLResponse(
        content=f"""
<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <link rel=\"icon\" href=\"/static/favicon.svg\" type=\"image/svg+xml\" />
    <link rel=\"icon\" href=\"/static/favicon-32.png\" type=\"image/png\" sizes=\"32x32\" />
    <link rel=\"icon\" href=\"/static/favicon-16.png\" type=\"image/png\" sizes=\"16x16\" />
    <link rel=\"shortcut icon\" href=\"/favicon.ico\" />
    <title>wd_notability signal report</title>
    <style>
      :root {{
        color-scheme: light dark;
        --bg: #fff;
        --text: #111;
        --border: #ddd;
        --muted-border: #eee;
        --header-bg: #f6f6f6;
        --nested-header-bg: #fbfbfb;
        --control-bg: #fff;
        --control-text: #111;
        --link: #0645ad;
        --error: #a10000;
        --level-none: #b00020;
        --level-weak: #b26a00;
        --level-strong: #1b7f2a;
      }}
      @media (prefers-color-scheme: dark) {{
        :root {{
          --bg: #111418;
          --text: #e8eaed;
          --border: #3a3f46;
          --muted-border: #2e333a;
          --header-bg: #20252c;
          --nested-header-bg: #1a1f25;
          --control-bg: #171b21;
          --control-text: #e8eaed;
          --link: #8ab4f8;
          --error: #ff8a80;
          --level-none: #ff8a80;
          --level-weak: #ffd166;
          --level-strong: #81c995;
        }}
      }}
      body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem; background: var(--bg); color: var(--text); }}
      a {{ color: var(--link); }}
      .report-header {{ display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }}
      .report-header h1 {{ margin: 0; }}
      .report-badge-link {{ display: inline-flex; flex: 0 0 auto; border-radius: 6px; }}
      .report-badge-link:focus-visible {{ outline: 2px solid var(--link); outline-offset: 4px; }}
      .report-badge {{ width: 7rem; height: 7rem; flex: 0 0 auto; }}
      .report-badge [data-field][data-value=\"unknown\"] {{ stroke: grey; fill: grey; }}
      .report-badge [data-field][data-value=\"none\"]  {{ stroke: red; fill: red; }}
      .report-badge [data-field][data-value=\"weak\"]  {{ stroke: orange; fill: orange; }}
      .report-badge [data-field][data-value=\"strong\"] {{ stroke: green; fill: green; }}
      .report-badge [data-field=\"redirect\"] {{ display: none; }}
      .report-badge [data-field=\"redirect\"][data-value=\"true\"] {{ display: block; }}
      .report-badge [data-field=\"has_claims\"][data-value=\"unknown\"] {{ display: none; }}
      .report-badge [data-field=\"has_claims\"][data-value=\"true\"] {{ display: none; }}
      .report-badge [data-field=\"has_claims\"][data-value=\"false\"] {{ display: block; }}
      form {{ display: flex; gap: .75rem; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; }}
      input[type=text] {{ padding: .5rem .6rem; min-width: 14rem; background: var(--control-bg); color: var(--control-text); border: 1px solid var(--border); }}
      button {{ padding: .5rem .8rem; background: var(--control-bg); color: var(--control-text); border: 1px solid var(--border); }}
      .status {{ color: var(--text); }}
      .comparison-grid {{ display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(24rem, 1fr)); margin-bottom: 1rem; }}
      .comparison-card {{ border: 1px solid var(--border); border-radius: 12px; padding: 1rem; background: var(--panel, transparent); }}
      .comparison-card h2 {{ margin-top: 0; }}
      .comparison-table, .levels-table, .queue-table {{ border-collapse: collapse; width: 100%; margin-bottom: 1rem; table-layout: fixed; }}
      .comparison-table th, .comparison-table td, .levels-table th, .levels-table td, .queue-table th, .queue-table td {{ border: 1px solid var(--border); padding: .4rem .5rem; text-align: left; vertical-align: top; }}
      .comparison-table th, .levels-table th, .queue-table th {{ background: var(--header-bg); }}
      .comparison-table .status-cell {{ width: 2rem; text-align: center; font-weight: 700; }}
      .comparison-table .diff td {{ background: rgba(161, 0, 0, 0.06); }}
      .comparison-table .same .status-cell {{ color: transparent; }}
      .inlinks-report {{ display: block; margin: 0 0 1rem; padding: .75rem; border: 1px solid var(--border); border-radius: 10px; background: var(--panel, transparent); }}
      .inlinks-report > summary {{ cursor: pointer; font-weight: 700; }}
      .inlinks-report[open] > summary {{ margin-bottom: .75rem; }}
      .subtle {{ color: var(--muted, #666); margin: 0 0 .75rem; }}
      pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; }}
    .props-table {{ margin: 0; width: 100%; table-layout: fixed; }}
    .props-table th, .props-table td {{ border: 1px solid var(--muted-border); padding: .3rem .4rem; font-size: 0.92em; vertical-align: top; }}
    .props-table th:first-child, .props-table td:first-child {{ width: 10rem; }}
    .props-table td {{ overflow-wrap: anywhere; word-break: break-word; }}
    .props-table a {{ overflow-wrap: anywhere; word-break: break-word; }}
    .props-table th {{ background: var(--nested-header-bg); }}
      .error {{ color: var(--error); font-weight: 600; }}
      .links {{ margin: .5rem 0 1rem; }}
            .level-none {{ color: var(--level-none); font-weight: 600; }}
            .level-weak {{ color: var(--level-weak); font-weight: 600; }}
            .level-strong {{ color: var(--level-strong); font-weight: 700; }}
    </style>
  </head>
  <body>
    <div class=\"report-header\">
      <a class=\"report-badge-link\" href=\"/badge.md\" aria-label=\"Open badge help\">
        {badge_html}
      </a>
      <div>
        <h1>Wikidata Notability Signal Report</h1>
        {f'<div class="item-link">Item: {item_link_html}</div>' if item_link_html else ''}
      </div>
    </div>
    <form method=\"get\" action=\"/\">
      <label>QID <input type=\"text\" name=\"qid\" value=\"{escaped_qid}\" placeholder=\"Q42\" /></label>
      <button type=\"submit\">Evaluate</button>
    </form>
    <div class=\"links\">
            <a href=\"/api/items/{escaped_qid}/signals\">JSON API for this QID</a>
            <span> | </span>
            <a href=\"/api/cache/stats\">Cache stats</a>
            <span> | </span>
            <a href=\"/observability\">Observability</a>
            <span> | </span>
            <a href=\"/help.md\">Help</a>
            <span>
    </div>
    {report_html}
    <script>
      const evaluationQid = {json.dumps(qid if should_load else "")};
      function badgeLevel(data, field, levelKey) {{
        if (data && Object.prototype.hasOwnProperty.call(data, field)) {{
          return String(data[field] == null ? "unknown" : data[field]).toUpperCase();
        }}
        if (data && data.levels && levelKey && Object.prototype.hasOwnProperty.call(data.levels, levelKey)) {{
          return String(data.levels[levelKey] == null ? "unknown" : data.levels[levelKey]).toUpperCase();
        }}
        return "UNKNOWN";
      }}
      function renderBadgeFromReport(report) {{
        const badge = document.querySelector(".report-badge");
        if (!badge || !report) return;
        const levels = report.levels || {{}};
        const data = {{
          n: levels.N,
          n1: levels.N1,
          n2a: levels.N2a,
          n2b: levels.N2b,
          n3: levels.N3,
          n3_inlinks: levels.N3_inlinks,
          n3_osm: levels.N3_osm,
          n3_wikisub: levels.N3_wikisub,
          n3_sdc: levels.N3_sdc,
          redirect: report.is_redirect,
          has_claims: report.has_claims,
          has_sitelinks: report.has_sitelinks,
          is_deleted: report.is_deleted,
        }};
        for (const field of ["n", "n1", "n2a", "n2b", "n3", "redirect", "has_claims"]) {{
          const el = badge.querySelector(`[data-field="${{field}}"]`);
          if (!el) continue;
          if (field === "has_claims" && (badgeLevel(data, "n2a", "N2a") === "UNKNOWN" || badgeLevel(data, "n2b", "N2b") === "UNKNOWN")) {{
            el.setAttribute("data-value", "unknown");
          }} else {{
            el.setAttribute("data-value", data[field] == null ? "unknown" : String(data[field]));
          }}
        }}
        const tooltip = [
          `Overall: ${{badgeLevel(data, "n", "N")}}`,
          `N1 sitelinks: ${{badgeLevel(data, "n1", "N1")}}`,
          `N2a identifiers: ${{badgeLevel(data, "n2a", "N2a")}}`,
          `N2b sources: ${{badgeLevel(data, "n2b", "N2b")}}`,
          `N3 inlinks: ${{badgeLevel(data, "n3_inlinks", "N3_inlinks")}}`,
          `N3 OSM: ${{badgeLevel(data, "n3_osm", "N3_osm")}}`,
          `N3 wikisub: ${{badgeLevel(data, "n3_wikisub", "N3_wikisub")}}`,
          `N3 SDC: ${{badgeLevel(data, "n3_sdc", "N3_sdc")}}`,
          report.is_redirect === true ? "Redirect: YES" : null,
          report.is_deleted === true ? "Deleted: YES" : null,
          report.has_sitelinks === false ? "Has sitelinks: NO" : null,
          (badgeLevel(data, "n2a", "N2a") !== "UNKNOWN" && badgeLevel(data, "n2b", "N2b") !== "UNKNOWN" && report.has_claims === false) ? "Has claims: NO" : null,
        ].filter(Boolean).join("\\n");
        badge.title = tooltip;
        badge.setAttribute("aria-label", tooltip);
      }}
      async function loadEvaluation() {{
        if (!evaluationQid) return;
        const status = document.getElementById("evaluation-status");
        const output = document.getElementById("report-output");
        try {{
          const response = await fetch(`/api/items/${{encodeURIComponent(evaluationQid)}}/signals`);
          if (!response.ok) {{
            throw new Error(`Request failed: ${{response.status}}`);
          }}
          const report = await response.json();
          renderBadgeFromReport(report);
          if (status) status.textContent = "Evaluation complete.";
          if (output) output.innerHTML = report.html || "";
        }} catch (error) {{
          if (status) {{
            status.textContent = error instanceof Error ? error.message : "Evaluation failed.";
            status.classList.add("error");
          }}
        }}
      }}
      loadEvaluation();
    </script>
  </body>
</html>
"""
    )


@app.get("/creations", response_class=HTMLResponse)
async def ui_creations():
    return web_render_creations_dashboard_html()

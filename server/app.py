from __future__ import annotations

import asyncio
import json
import os
import time
from html import escape
from pathlib import Path
from uuid import uuid4
from collections.abc import AsyncGenerator

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from markdown_it import MarkdownIt
from pydantic import BaseModel, Field

from wd_notability.evaluation_cache import CACHE
from wd_notability.evaluate import evaluate_full, foreground_evaluation
from wd_notability.lookup_cache import lookup_cache
from wd_notability.models import EvaluationReason, EvaluationResult, NotabilityLevel
from wd_notability.wikidata_api import close_wikidata_session


REVALUATE_ON_SUBSCRIBE = True
SHUTDOWN_EVENT: asyncio.Event | None = None
SSE_STREAM_MAX_SECONDS = 10.0
PUBSUB_REAPER_TASK: asyncio.Task | None = None
PUBSUB_REAPER_INTERVAL_SECONDS = 60.0
PUBSUB_GADGET_SESSION_TTL_SECONDS = 3600


app = FastAPI(title="wd_notability")
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

_cors_origins_raw = os.getenv("WD_NOTABILITY_CORS_ORIGINS", "*")
_cors_origins = [origin.strip() for origin in _cors_origins_raw.split(",") if origin.strip()]
if not _cors_origins:
    _cors_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
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


async def _pubsub_reaper_loop() -> None:
    while SHUTDOWN_EVENT is None or not SHUTDOWN_EVENT.is_set():
        try:
            await CACHE.pubsub.purge_expired_pubsub_sessions()
            await CACHE.events.purge_expired_pubsub_events()
        except Exception as exc:  # noqa: BLE001
            print(f"PubSub reaper failed: {exc}")
        try:
            await _sleep_or_shutdown(PUBSUB_REAPER_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            raise


async def _subscription_event_stream(subscription_id: str, qids: set[str], request: Request):
    last_seen: dict[str, tuple[int, int | None, int | None]] = {}
    deadline = time.monotonic() + SSE_STREAM_MAX_SECONDS
    qid_list = sorted(qids)
    primed = False

    while time.monotonic() < deadline:
        if SHUTDOWN_EVENT is not None and SHUTDOWN_EVENT.is_set():
            break
        if await request.is_disconnected():
            break

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
            yield _sse_message(_badge_payload(qid, cached_result))

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


def _badge_payload(qid: str, result) -> dict[str, object]:
    return {
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


def _cached_payload(
    qid: str,
    result,
    entitydata_last_revid: int | None,
    recent_changes_last_revid: int | None,
) -> dict[str, object]:
    return {
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
    if not lookup_cache.get_osm_usage():
        raise RuntimeError("Lookup cache database has no OSM usage rows. Run scripts/build_osm_cache.py first.")
    if not lookup_cache.get_sdc_usage():
        raise RuntimeError("Lookup cache database has no SDC usage rows. Run scripts/build_sdc_cache.py first.")
    SHUTDOWN_EVENT = asyncio.Event()
    PUBSUB_REAPER_TASK = asyncio.create_task(_pubsub_reaper_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    global PUBSUB_REAPER_TASK
    if SHUTDOWN_EVENT is not None:
        SHUTDOWN_EVENT.set()
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
    payload: dict[str, object] = {
        "qid": result.qid,
        "levels": result.levels_str,
        "errors": result.errors,
        "has_claims": result.has_claims,
        "has_claims_known": result.has_claims_known,
        "has_sitelinks": result.has_sitelinks,
        "is_redirect": result.is_redirect,
        "is_deleted": result.is_deleted,
        "summary": result.summary,
        "source_urls": result.source_urls,
        "source_contexts": result.source_contexts,
    }
    grouped: dict[str, list[dict]] = {criterion: [] for criterion in DETECTED_CRITERIA}
    for signal_model in result.signals:
        signal = signal_model.model_dump(mode="json")
        if not isinstance(signal, dict):
            continue
        signal["level"] = signal_model.level.value_str
        criterion = signal.get("criterion")
        if isinstance(criterion, str) and criterion in grouped:
            grouped[criterion].append(signal)
    payload["signals_by_detected_criterion"] = grouped
    return payload


def _item_link_html(qid: str | None) -> str:
    if not qid:
        return ""
    escaped_qid = escape(qid)
    href = f"https://www.wikidata.org/wiki/{escaped_qid}"
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">{escaped_qid}</a>'


def _render_report_html(report: dict | None) -> str:
    if report is None:
        return ""

    qid = report.get("qid")
    levels = report.get("levels", {})
    errors = report.get("errors", {})
    grouped_signals = report.get("signals_by_detected_criterion", {})
    source_contexts = report.get("source_contexts", {})
    source_urls = report.get("source_urls", [])
    cached_snapshot = report.get("cached_snapshot")
    qid_row = f"<tr><td>Item</td><td>{_item_link_html(qid if isinstance(qid, str) else None)}</td></tr>"
    metadata_rows = qid_row + "".join(
        f"<tr><td>{escape(label)}</td><td>{escape(str(report.get(key, False)))}</td></tr>"
        for key, label in (
            ("is_redirect", "Redirect"),
            ("is_deleted", "Deleted"),
            ("has_sitelinks", "Has sitelinks"),
            ("has_claims", "Has claims"),
        )
    )
    level_rows = "".join(
        (
            "<tr>"
            f"<td>{escape(str(k))}</td>"
            f"<td class='{_level_class(v)}'>{escape(str(v))}</td>"
            f"<td>{_render_errors_cell(k, errors)}</td>"
            "</tr>"
        )
        for k, v in levels.items()
    )

    grouped_sections = ""
    if not isinstance(grouped_signals, dict):
        grouped_signals = {}
    for criterion in DETECTED_CRITERIA:
        bucket = grouped_signals.get(criterion, [])
        title = DETECTED_CRITERION_LABELS.get(criterion, criterion)
        rows = "".join(
            "<tr>"
            f"<td class='{_level_class(s.get('level', ''))}'>{escape(str(s.get('level', '')))}</td>"
            f"<td>{escape(str(s.get('detector', '')))}</td>"
            f"<td>{escape(str(s.get('key', '')))}</td>"
            f"<td>{_render_properties_html(s.get('properties', {}))}</td>"
            "</tr>"
            for s in bucket
            if isinstance(s, dict)
        )
        if not rows:
            rows = "<tr><td colspan='4'><em>No signals</em></td></tr>"
        grouped_sections += (
            f"<h3>{escape(title)}</h3>"
            "<table><thead><tr><th>Level</th><th>Detector</th><th>Key</th><th>Properties</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    source_rows = ""
    if isinstance(source_urls, list):
        for source in source_urls:
            if not isinstance(source, dict):
                continue
            source_name = escape(str(source.get("source", "")))
            api_url = source.get("api_url")
            ui_url = source.get("ui_url")
            api_link = (
                f"<a href='{escape(str(api_url), quote=True)}' target='_blank' rel='noopener noreferrer'>API</a>"
                if isinstance(api_url, str) and api_url
                else ""
            )
            ui_link = (
                f"<a href='{escape(str(ui_url), quote=True)}' target='_blank' rel='noopener noreferrer'>UI</a>"
                if isinstance(ui_url, str) and ui_url
                else ""
            )
            source_rows += f"<tr><td>{source_name}</td><td>{api_link}</td><td>{ui_link}</td></tr>"
    if not source_rows:
        source_rows = "<tr><td colspan='3'><em>No source links</em></td></tr>"

    cache_rows = ""
    if isinstance(cached_snapshot, dict):
        cache_rows = "".join(
            f"<tr><td>{escape(str(k))}</td><td>{escape(str(v))}</td></tr>"
            for k, v in cached_snapshot.items()
        )
    if not cache_rows:
        cache_rows = "<tr><td colspan='2'><em>No cached snapshot</em></td></tr>"

    source_context_sections = ""
    if isinstance(source_contexts, dict):
        for source_name in ("entity_data", "inlinks", "sdc", "osm", "wiki_usage"):
            context = source_contexts.get(source_name)
            if context is None:
                continue
            source_context_sections += (
                f"<h3>{escape(source_name)}</h3>"
                f"<details><summary>Raw context</summary><pre>{escape(json.dumps(context, indent=2, ensure_ascii=False, default=str))}</pre></details>"
            )

    return (
        "<h2>Item State</h2>"
        "<table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{metadata_rows}</tbody></table>"
        "<h2>Cache Snapshot</h2>"
        "<table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{cache_rows}</tbody></table>"
        "<h2>Levels</h2>"
        "<table><thead><tr><th>Criterion</th><th>Level</th><th>Errors</th></tr></thead>"
        f"<tbody>{level_rows}</tbody></table>"
        "<h2>Sources</h2>"
        "<table><thead><tr><th>Source</th><th>API</th><th>UI</th></tr></thead>"
        f"<tbody>{source_rows}</tbody></table>"
        "<h2>Source Context</h2>"
        f"{source_context_sections or '<p><em>No source context available</em></p>'}"
        "<h2>Signals by Detected Criterion</h2>"
        f"{grouped_sections}"
    )


async def _evaluate_or_404(qid: str) -> dict:
    if not _is_valid_qid(qid):
        raise HTTPException(status_code=400, detail="qid must look like Q42")
    try:
        cached_result, entitydata_last_revid, recent_changes_last_revid = await CACHE.get(qid)
        async with foreground_evaluation():
            result = await evaluate_full(qid, parallel=True)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    payload = _report_payload(result)
    payload["html"] = _render_report_html(payload)
    if cached_result is not None:
        payload["cached_snapshot"] = {
            "qid": cached_result.qid,
            "summary": cached_result.summary,
            "levels": cached_result.levels_str,
            "entitydata_last_revid": entitydata_last_revid,
            "recent_changes_last_revid": recent_changes_last_revid,
        }
    else:
        payload["cached_snapshot"] = None
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


async def _pubsub_event_stream(
    owner_id: str,
    session_id: str,
    request: Request,
    *,
    after_event_id: int = 0,
    poll_seconds: float = 2.0,
) -> AsyncGenerator[str, None]:
    cursor = max(0, after_event_id)
    deadline = time.monotonic() + SSE_STREAM_MAX_SECONDS

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
        emitted = False
        for row in rows:
            cursor = max(cursor, int(row["event_id"]))
            emitted = True
            summary_value = int(row["summary"])
            cached_result = EvaluationResult.from_summary(qid=f"Q{row['qid']}", summary=summary_value)
            payload = {
                "event": "summary_change",
                "event_id": row["event_id"],
                "timestamp": row["timestamp"],
                "qid": f"Q{row['qid']}",
                "event_type": row["event_type"],
                "summary": row["summary"],
                "mask": row["mask"],
            }
            payload.update(_badge_payload(f"Q{row['qid']}", cached_result))
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


@app.get("/api/cache/event-log-stats")
async def api_cache_event_log_stats():
    return await CACHE.events.event_log_stats()


@app.get("/api/cache/items/{qid}")
async def api_cache_item(qid: str):
    return await _cached_or_404(qid)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(STATIC_DIR / "favicon.ico", media_type="image/x-icon")


@app.get("/{filename}.md", include_in_schema=False)
async def static_markdown_page(filename: str):
    return _render_static_markdown_page(f"{filename}.md")


@app.post("/subscribe")
async def api_subscribe(request: SubscribeRequest):
    items = _normalize_subscription_items(request)
    if not items:
        raise HTTPException(status_code=400, detail="qids must include at least one valid QID")

    qids = list(items)
    cached_items: list[dict[str, object]] = []
    missing_qids: list[str] = []
    cached_rows = await CACHE.get_many(qids)
    for qid in qids:
        row = cached_rows.get(qid)
        if row is None:
            missing_qids.append(qid)
            continue

        summary, entitydata_last_revid, recent_changes_last_revid = row
        cached_result = EvaluationResult.from_summary(qid=qid, summary=summary)
        cached_items.append(
            _cached_payload(qid, cached_result, entitydata_last_revid, recent_changes_last_revid)
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
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 1rem; table-layout: fixed; }}
      th, td {{ border: 1px solid var(--border); padding: .4rem .5rem; text-align: left; vertical-align: top; }}
      th {{ background: var(--header-bg); }}
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

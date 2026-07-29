from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from html import escape
from typing import Any

from server.render_helpers import _badge_hovercard_html_from_report, _badge_tooltip_from_report, _level_text_html
from wd_notability.inlinks.source import INLINKS_CONTEXT_LIMIT
from wd_notability.item_trace import ITEM_TRACE_ENABLED
from wd_notability.models import EvaluationResult

# Criteria shown in the debug report.
INTRINSIC_CRITERIA = ("N1", "N2a", "N2b")
EXTRINSIC_CRITERIA = ("N3_inlinks", "N3_osm", "N3_wikisub", "N3_sdc")
DETECTED_CRITERIA = INTRINSIC_CRITERIA + EXTRINSIC_CRITERIA
DETECTED_CRITERION_LABELS = {
    "N1": "N1: Sitelinks",
    "N2a": "N2a: Identifiers",
    "N2b": "N2b: Sources",
    "N3_inlinks": "N3: Inlinks",
    "N3_osm": "N3: OSM",
    "N3_wikisub": "N3: Wiki subscribers",
    "N3_sdc": "N3: SDC",
}
# Cache/live comparison levels rendered in the debug report.
COMPARISON_LEVELS = ("N", "N1", "N2a", "N2b", "N2", "N12", "N3", "N3_inlinks", "N3_osm", "N3_wikisub", "N3_sdc")
# Comparison levels relevant to the inlinks-specific debug view.
INLINKS_COMPARISON_LEVELS = ("N1", "N2")
# Source names used by the signal payload.
CRITERION_SOURCE_NAMES: dict[str, str] = {
    "N1": "content",
    "N2a": "content",
    "N2b": "content",
    "N3_inlinks": "inlinks",
    "N3_osm": "osm",
    "N3_wikisub": "wiki_usage",
    "N3_sdc": "sdc",
}
# Fields compared between the cache snapshot and live result.
COMPARISON_FIELDS = (
    ("has_claims_count", "Claims count", True),
    ("has_sitelinks_count", "Sitelinks count", True),
    ("inlinks_count", "Inlinks count", True),
    ("redirect_target", "Redirect target", True),
    ("is_redirect", "Redirect", True),
    ("is_deleted", "Deleted", True),
    ("creation_time_iso", "Creation time", False),
    ("last_updated_iso", "Last updated", False),
    ("inlinks_last_evaluated_iso", "Inlinks evaluated", False),
    ("content_last_revid", "Content rev", False),
    ("recent_changes_last_revid", "Recent changes rev", False),
)
METADATA_FIELDS = (
    ("creator", "Creator"),
    ("creation_time_iso", "Created"),
    ("last_updated_iso", "Last updated"),
    ("inlinks_last_evaluated_iso", "Modified"),
    ("content_last_revid", "Content rev"),
    ("recent_changes_last_revid", "Recent changes rev"),
)


def _level_class(level: object) -> str:
    text = str(level).lower()
    if text == "strong":
        return "level-strong"
    if text == "weak":
        return "level-weak"
    if text == "partial-strong":
        return "level-partial-strong"
    if text == "partial-weak":
        return "level-partial-weak"
    if text == "unknown":
        return "level-unknown"
    return "level-none"


def _render_property_value(value: object) -> str:
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False, default=str)

    pattern = re.compile(r"https?://[^\s<>\"]+|[PQ]\d+\b")
    rendered = []
    last_index = 0
    for match in pattern.finditer(text):
        start, end = match.span()
        if start > last_index:
            rendered.append(escape(text[last_index:start]))
        token = match.group(0)
        if token.startswith("http://") or token.startswith("https://"):
            href = escape(token, quote=True)
            rendered.append(f"<a href='{href}' target='_blank' rel='noopener noreferrer'>{escape(token)}</a>")
        elif token.startswith("P"):
            href = f"https://www.wikidata.org/wiki/Property:{escape(token, quote=True)}"
            rendered.append(f"<a href='{href}' target='_blank' rel='noopener noreferrer'>{escape(token)}</a>")
        else:
            href = f"https://www.wikidata.org/wiki/{escape(token, quote=True)}"
            rendered.append(f"<a href='{href}' target='_blank' rel='noopener noreferrer'>{escape(token)}</a>")
        last_index = end
    if last_index < len(text):
        rendered.append(escape(text[last_index:]))
    return "".join(rendered)


def _render_properties_html(properties: object) -> str:
    if not isinstance(properties, dict):
        return escape(json.dumps(properties, ensure_ascii=False, default=str))

    rows = "".join(
        f"<tr><td>{escape(str(k))}</td><td>{_render_property_value(v)}</td></tr>"
        for k, v in properties.items()
    )
    if not rows:
        rows = "<tr><td colspan='2'><em>empty</em></td></tr>"
    return (
        "<table class='props-table'>"
        f"<tbody>{rows}</tbody></table>"
    )


def _format_debug_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _comparison_status(cache_value: object, live_value: object, compare: bool) -> str:
    if not compare:
        return ""
    if cache_value == live_value:
        return ""
    return "&#9888;"


def _comparison_count_value(
    *,
    field: str,
    value: object,
    report: dict[str, Any],
    cached_snapshot: dict[str, Any] | None,
    is_cache: bool,
) -> object:
    """Hide count fields when the underlying evaluation is still unknown."""
    if field in {"has_claims_count", "has_sitelinks_count"}:
        source = cached_snapshot if is_cache else report
        if not isinstance(source, dict) or source.get("content_last_revid") is None:
            return None
        return value

    if field == "inlinks_count":
        if is_cache:
            if not isinstance(cached_snapshot, dict) or cached_snapshot.get("inlinks_last_evaluated") is None:
                return None
        else:
            display_value = report.get("inlinks_count_display")
            if display_value is not None:
                return display_value
            levels = report.get("levels", {})
            if not isinstance(levels, dict) or str(levels.get("N3_inlinks", "unknown")).lower() == "unknown":
                return None
        return value

    return value


def _comparison_time_value(
    *,
    field: str,
    value: object,
    is_cache: bool,
    render_time: datetime,
) -> object:
    if value is not None:
        return value
    if is_cache:
        return None
    if field in {"last_updated_iso", "inlinks_last_evaluated_iso"}:
        return render_time.isoformat().replace("+00:00", "Z")
    return None


def _render_comparison_table(report: dict[str, Any]) -> str:
    cached_snapshot = report.get("cached_snapshot")
    cache_report = cached_snapshot if isinstance(cached_snapshot, dict) else {}
    comparison_levels = report.get("comparison_levels", COMPARISON_LEVELS)
    if not isinstance(comparison_levels, (list, tuple)):
        comparison_levels = COMPARISON_LEVELS
    render_time = datetime.now(UTC)

    def _row(
        label: str,
        cache_value: object,
        live_value: object,
        *,
        compare: bool = True,
        html: bool = False,
        level: bool = False,
    ) -> str:
        status = _comparison_status(cache_value, live_value, compare)
        cache_cell = cache_value if html else escape(_format_debug_value(cache_value))
        live_cell = live_value if html else escape(_format_debug_value(live_value))
        if html:
            cache_cell = str(cache_value or "")
            live_cell = str(live_value or "")
        if level:
            cache_text = _level_text_html(cache_value)
            live_text = _level_text_html(live_value)
            cache_cell = (
                f"<span class='{_level_class(cache_value)}'>{cache_text}</span>"
                if cache_text and cache_text != "&mdash;"
                else cache_text
            )
            live_cell = (
                f"<span class='{_level_class(live_value)}'>{live_text}</span>"
                if live_text and live_text != "&mdash;"
                else live_text
            )
        return (
            f"<tr class='{'diff' if status else 'same'}'>"
            f"<td>{escape(label)}</td>"
            f"<td>{cache_cell or '&mdash;'}</td>"
            f"<td>{live_cell or '&mdash;'}</td>"
            f"<td class='status-cell'>{status}</td>"
            "</tr>"
        )

    rows = []
    for field, label, compare in COMPARISON_FIELDS:
        cache_value = _comparison_count_value(
            field=field,
            value=cache_report.get(field),
            report=report,
            cached_snapshot=cache_report,
            is_cache=True,
        )
        if field in {"creation_time_iso", "last_updated_iso", "inlinks_last_evaluated_iso"}:
            cache_value = _comparison_time_value(
                field=field,
                value=cache_value,
                is_cache=True,
                render_time=render_time,
            )
        live_value = _comparison_count_value(
            field=field,
            value=report.get(field),
            report=report,
            cached_snapshot=cache_report,
            is_cache=False,
        )
        if field in {"creation_time_iso", "last_updated_iso", "inlinks_last_evaluated_iso"}:
            live_value = _comparison_time_value(
                field=field,
                value=live_value,
                is_cache=False,
                render_time=render_time,
            )
        rows.append(_row(label, cache_value, live_value, compare=compare))

    cache_levels = cache_report.get("levels", {}) if isinstance(cache_report, dict) else {}
    live_levels = report.get("levels", {})
    rows.extend(
        _row(
            f"Level: {criterion}",
            cache_levels.get(criterion),
            live_levels.get(criterion),
            level=True,
        )
        for criterion in comparison_levels
    )

    return (
        "<table class='comparison-table'><thead><tr><th>Field</th><th>Cache</th><th>Live</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _render_levels_table(levels: object, errors: object | None = None) -> str:
    if not isinstance(levels, dict):
        return "<p><em>No levels available</em></p>"

    rows = "".join(
        (
            "<tr>"
            f"<td>{escape(str(criterion))}</td>"
            f"<td class='{_level_class(level)}'>{_level_text_html(level)}</td>"
            f"<td>{_render_errors_cell(criterion, errors)}</td>"
            "</tr>"
        )
        for criterion, level in levels.items()
    )
    return (
        "<table class='levels-table'><thead><tr><th>Criterion</th><th>Level</th><th>Errors</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def _section_strength(rows: list[dict[str, Any]]) -> str:
    priority = {
        "strong": 5,
        "weak": 4,
        "partial-strong": 3,
        "partial-weak": 2,
        "unknown": 1,
        "none": 0,
    }
    best = "none"
    for signal in rows:
        level = str(signal.get("level", "none")).lower()
        if priority.get(level, -1) > priority.get(best, -1):
            best = level
    return best


def _section_label(criterion: str) -> str:
    if criterion == "N3_inlinks":
        return "N3_inlinks from linked item N12"
    return criterion


def _render_signal_section(*, criterion: str, grouped_signals: dict[str, list[dict[str, Any]]] | None) -> str:
    rows = grouped_signals.get(criterion, []) if isinstance(grouped_signals, dict) else []
    row_count = len(rows) if isinstance(rows, list) else 0
    strength = _section_strength(rows if isinstance(rows, list) else [])
    label = _section_label(criterion)
    summary = (
        f"{label} - {row_count} signal{'s' if row_count != 1 else ''} "
        f"<span class='{_level_class(strength)}'>{_level_text_html(strength)}</span>"
    )
    body = "".join(
        "<tr>"
        f"<td>{escape(str(signal.get('criterion', '')))}</td>"
        f"<td class='{_level_class(signal.get('level'))}'>{_level_text_html(signal.get('level'))}</td>"
        f"<td>{escape(str(signal.get('detector', '')))}</td>"
        f"<td>{escape(str(signal.get('key', '')))}</td>"
        f"<td>{_render_properties_html(signal.get('properties', {}))}</td>"
        "</tr>"
        for signal in rows
        if isinstance(signal, dict)
    )
    if not body:
        body = "<tr><td colspan='5'><em>No signals available</em></td></tr>"
    sections = []
    sections.append(
        "<details class='criterion-section'>"
        f"<summary>{summary}</summary>"
        "<table><thead><tr><th>Criterion</th><th>Level</th><th>Detector</th><th>Key</th><th>Properties</th></tr></thead>"
        f"<tbody>{body}</tbody></table>"
        "</details>"
    )
    return "".join(sections)


def _render_report_error(report: dict[str, Any]) -> str:
    error = report.get("error")
    if not isinstance(error, str) or not error.strip():
        return ""
    return (
        "<div class='report-error'>"
        "<strong>Live source error:</strong> "
        f"{escape(error)}"
        "</div>"
    )


def _render_errors_section(errors: dict[str, Any]) -> str:
    if not isinstance(errors, dict):
        return ""

    rows: list[str] = []
    for criterion in DETECTED_CRITERIA:
        messages = errors.get(criterion, [])
        if not isinstance(messages, list) or not messages:
            continue
        label = DETECTED_CRITERION_LABELS.get(criterion, criterion)
        rendered_messages = "".join(
            f"<li>{escape(str(message))}</li>"
            for message in messages
            if isinstance(message, str) and message.strip()
        )
        if not rendered_messages:
            continue
        rows.append(
            "<li>"
            f"<strong>{escape(label)}</strong>"
            f"<ul>{rendered_messages}</ul>"
            "</li>"
        )

    if not rows:
        return ""

    return (
        "<h2>Errors</h2>"
        "<section class='error-section'>"
        f"<ul>{''.join(rows)}</ul>"
        "</section>"
    )


def _item_link_html(qid: str | None) -> str:
    if not qid:
        return ""
    escaped_qid = escape(qid)
    href = f"https://www.wikidata.org/wiki/{escaped_qid}"
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">{escaped_qid}</a>'


def _item_trace_link_html(qid: str | None) -> str:
    if not qid or not ITEM_TRACE_ENABLED:
        return ""
    escaped_qid = escape(qid)
    href = f"/item-trace?qid={escaped_qid}"
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">Item trace</a>'


def build_signal_debug_payload(result: EvaluationResult) -> dict[str, Any]:
    inlinks_count_display = None
    inlinks_context = result.source_contexts.get("inlinks")
    if isinstance(inlinks_context, dict) and inlinks_context.get("truncated") is True:
        inlinks_count_display = f">{INLINKS_CONTEXT_LIMIT}"

    payload: dict[str, Any] = {
        "qid": result.qid,
        "levels": result.levels_str,
        "errors": result.errors,
        "has_claims_count": result.has_claims_count,
        "has_sitelinks_count": result.has_sitelinks_count,
        "inlinks_count": result.inlinks_count,
        "inlinks_count_display": inlinks_count_display,
        "redirect_target": result.redirect_target,
        "is_redirect": result.is_redirect,
        "is_deleted": result.is_deleted,
        "content_last_revid": result.content_last_revid,
        "recent_changes_last_revid": result.recent_changes_last_revid,
        "source_urls": result.source_urls,
        "source_contexts": result.source_contexts,
        "signals": [signal.model_dump(mode="json") for signal in result.signals],
    }
    grouped: dict[str, list[dict[str, Any]]] = {criterion: [] for criterion in DETECTED_CRITERIA}
    for signal_model in result.signals:
        signal = signal_model.model_dump(mode="json")
        if not isinstance(signal, dict):
            continue
        signal["level"] = signal_model.level.value_str
        criterion = signal.get("criterion")
        if isinstance(criterion, str) and criterion in grouped:
            grouped[criterion].append(signal)
    payload["signals_by_detected_criterion"] = grouped
    payload["badge_tooltip"] = _badge_tooltip_from_report(payload)
    payload["badge_hovercard"] = _badge_hovercard_html_from_report(payload)
    return payload


def render_signal_debug_html(report: dict[str, Any] | None) -> str:
    if report is None:
        return ""

    grouped_signals = report.get("signals_by_detected_criterion", {})
    report_error = _render_report_error(report if isinstance(report, dict) else {})
    error_section = _render_errors_section(report.get("errors", {}) if isinstance(report, dict) else {})
    has_detected_signals = isinstance(grouped_signals, dict) and any(
        isinstance(rows, list) and rows for rows in grouped_signals.values()
    )
    if report.get("report_variant") == "inlinks" and not has_detected_signals:
        return (
            f"{report_error}"
            f"{error_section}"
            "<h2>Cache vs Live</h2>"
            f"{_render_comparison_table(report)}"
        )

    detector_sections = "".join(
        _render_signal_section(criterion=criterion, grouped_signals=grouped_signals if isinstance(grouped_signals, dict) else {})
        for criterion in DETECTED_CRITERIA
    )

    return (
        f"{report_error}"
        f"{error_section}"
        "<h2>Cache vs Live</h2>"
        f"{_render_comparison_table(report)}"
        "<h2>Detectors</h2>"
        f"{detector_sections}"
    )


__all__ = [
    "build_signal_debug_payload",
    "render_signal_debug_html",
]

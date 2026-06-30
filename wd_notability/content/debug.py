from __future__ import annotations

import json
from html import escape
from typing import Any

from wd_notability.models import EvaluationResult

DETECTED_CRITERIA = ("N1", "N2a", "N2b", "N3_inlinks", "N3_osm", "N3_wikisub", "N3_sdc")
COMPARISON_LEVELS = ("N", "N1", "N2a", "N2b", "N12", "N3", "N3_inlinks", "N3_osm", "N3_wikisub", "N3_sdc")
INLINKS_COMPARISON_LEVELS = ("N1", "N2")
COMPARISON_FIELDS = (
    ("summary", "Summary", True),
    ("has_claims", "Has claims", True),
    ("has_claims_known", "Has claims known", True),
    ("has_sitelinks", "Has sitelinks", True),
    ("is_redirect", "Redirect", True),
    ("is_deleted", "Deleted", True),
    ("creation_time_iso", "Creation time", False),
    ("last_updated_iso", "Last updated", False),
    ("inlinks_last_evaluated_iso", "Inlinks evaluated", False),
    ("entitydata_last_revid", "EntityData rev", False),
    ("recent_changes_last_revid", "Recent changes rev", False),
)


def _level_class(level: object) -> str:
    text = str(level).lower()
    if text == "strong":
        return "level-strong"
    if text == "weak":
        return "level-weak"
    if text == "unknown":
        return "level-unknown"
    return "level-none"


def _render_property_value(value: object) -> str:
    if isinstance(value, str):
        return escape(value)
    return escape(json.dumps(value, ensure_ascii=False, default=str))


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
        "<table class='props-table'><thead><tr><th>Property</th><th>Value</th></tr></thead>"
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


def _render_comparison_table(report: dict[str, Any]) -> str:
    cached_snapshot = report.get("cached_snapshot")
    cache_report = cached_snapshot if isinstance(cached_snapshot, dict) else {}
    qid = report.get("qid")
    cache_qid = cache_report.get("qid") if isinstance(cache_report, dict) else None
    comparison_levels = report.get("comparison_levels", COMPARISON_LEVELS)
    if not isinstance(comparison_levels, (list, tuple)):
        comparison_levels = COMPARISON_LEVELS

    def _row(label: str, cache_value: object, live_value: object, *, compare: bool = True, html: bool = False) -> str:
        status = _comparison_status(cache_value, live_value, compare)
        cache_cell = cache_value if html else escape(_format_debug_value(cache_value))
        live_cell = live_value if html else escape(_format_debug_value(live_value))
        if html:
            cache_cell = str(cache_value or "")
            live_cell = str(live_value or "")
        return (
            f"<tr class='{'diff' if status else 'same'}'>"
            f"<td>{escape(label)}</td>"
            f"<td>{cache_cell or '&mdash;'}</td>"
            f"<td>{live_cell or '&mdash;'}</td>"
            f"<td class='status-cell'>{status}</td>"
            "</tr>"
        )

    rows = [
        _row("Cache snapshot", "present" if cache_report else "missing", "present", compare=False),
        _row("Item", _item_link_html(cache_qid if isinstance(cache_qid, str) else None), _item_link_html(qid if isinstance(qid, str) else None), html=True),
    ]
    for field, label, compare in COMPARISON_FIELDS:
        rows.append(_row(label, cache_report.get(field), report.get(field), compare=compare))

    cache_levels = cache_report.get("levels", {}) if isinstance(cache_report, dict) else {}
    live_levels = report.get("levels", {})
    rows.extend(
        _row(f"Level: {criterion}", cache_levels.get(criterion), live_levels.get(criterion))
        for criterion in comparison_levels
    )

    return (
        "<table class='comparison-table'><thead><tr><th>Field</th><th>Cache</th><th>Live</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _render_inlinks_comparison_table(report: dict[str, Any]) -> str:
    cached_snapshot = report.get("cached_snapshot")
    cache_report = cached_snapshot if isinstance(cached_snapshot, dict) else {}
    qid = report.get("qid")
    cache_qid = cache_report.get("qid") if isinstance(cache_report, dict) else None
    cache_levels = cache_report.get("levels", {}) if isinstance(cache_report, dict) else {}
    live_levels = report.get("levels", {})

    def _row(label: str, cache_value: object, live_value: object) -> str:
        status = _comparison_status(cache_value, live_value, True)
        cache_cell = str(cache_value or "")
        live_cell = str(live_value or "")
        return (
            f"<tr class='{'diff' if status else 'same'}'>"
            f"<td>{escape(label)}</td>"
            f"<td>{cache_cell or '&mdash;'}</td>"
            f"<td>{live_cell or '&mdash;'}</td>"
            f"<td class='status-cell'>{status}</td>"
            "</tr>"
        )

    rows = [
        _row("Item", _item_link_html(cache_qid if isinstance(cache_qid, str) else None), _item_link_html(qid if isinstance(qid, str) else None)),
        _row("N1", cache_levels.get("N1"), live_levels.get("N1") if isinstance(live_levels, dict) else None),
        _row("N2", cache_levels.get("N2"), live_levels.get("N2") if isinstance(live_levels, dict) else None),
    ]
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
            f"<td class='{_level_class(level)}'>{escape(str(level))}</td>"
            f"<td>{_render_errors_cell(criterion, errors)}</td>"
            "</tr>"
        )
        for criterion, level in levels.items()
    )
    return (
        "<table class='levels-table'><thead><tr><th>Criterion</th><th>Level</th><th>Errors</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def _render_interest_table(interest: dict[str, Any] | None) -> str:
    if not isinstance(interest, dict):
        return "<p><em>No active interest</em></p>"

    workers = interest.get("workers", [])
    worker_rows = ""
    if isinstance(workers, list) and workers:
        worker_rows = "".join(
            "<tr>"
            f"<td>{escape(str(worker.get('owner_id', '')))}</td>"
            f"<td>{escape(str(worker.get('session_rows', 0)))}</td>"
            f"<td>{escape(str(worker.get('total_priority', 0)))}</td>"
            f"<td>{escape(str(worker.get('wants_entitydata', False)))}</td>"
            f"<td>{escape(str(worker.get('wants_inlinks', False)))}</td>"
            f"<td>{escape(str(worker.get('wants_sync', False)))}</td>"
            "</tr>"
            for worker in workers
            if isinstance(worker, dict)
        )
    if not worker_rows:
        worker_rows = "<tr><td colspan='6'><em>No interest rows</em></td></tr>"

    summary_rows = "".join(
        f"<tr><td>{escape(str(label))}</td><td>{escape(str(interest.get(key, 0)))}</td></tr>"
        for key, label in (
            ("session_rows", "Session rows"),
            ("owner_count", "Owners"),
            ("total_priority", "Total priority"),
        )
    )
    return (
        "<table><thead><tr><th>Metric</th><th>Value</th></tr></thead>"
        f"<tbody>{summary_rows}</tbody></table>"
        "<table><thead><tr><th>Owner</th><th>Rows</th><th>Priority</th><th>EntityData</th><th>Inlinks</th><th>Sync</th></tr></thead>"
        f"<tbody>{worker_rows}</tbody></table>"
    )


def _render_queue_table(queue: dict[str, Any] | None) -> str:
    if not isinstance(queue, dict):
        return "<p><em>No queue position available</em></p>"

    paths = queue.get("paths", [])
    if not isinstance(paths, list) or not paths:
        return "<p><em>No queue position available</em></p>"

    def _cell(value: object) -> str:
        text = _format_debug_value(value)
        return escape(text) if text else "&mdash;"

    rows = "".join(
        "<tr>"
        f"<td>{escape(str(path.get('name', '')))}</td>"
        f"<td>{_cell(path.get('status'))}</td>"
        f"<td>{_cell(path.get('position'))}</td>"
        f"<td>{_cell(path.get('ahead'))}</td>"
        f"<td>{_cell(path.get('batch_size'))}</td>"
        f"<td>{_cell(path.get('estimate'))}</td>"
        f"<td>{_cell(path.get('rule'))}</td>"
        "</tr>"
        for path in paths
        if isinstance(path, dict)
    )
    return (
        "<table class='queue-table'><thead><tr><th>Path</th><th>Status</th><th>Position</th><th>Ahead</th><th>Batch</th><th>Estimate</th><th>Rule</th></tr></thead>"
        f"<tbody>{rows or '<tr><td colspan=\"7\"><em>No queue position available</em></td></tr>'}</tbody></table>"
    )


def _render_discrepancy_table(discrepancies: object) -> str:
    if not isinstance(discrepancies, dict):
        return ""

    status = str(discrepancies.get("status", ""))
    items = discrepancies.get("items", [])
    if status == "missing":
        return "<p class='error'>No cache entry for this inlink.</p>"

    if not isinstance(items, list) or not items:
        return "<p><em>No cache discrepancies detected</em></p>"

    rows = "".join(
        "<tr>"
        f"<td>{escape(str(item.get('field', '')))}</td>"
        f"<td>{escape(str(item.get('cache', '')))}</td>"
        f"<td>{escape(str(item.get('live', '')))}</td>"
        "</tr>"
        for item in items
        if isinstance(item, dict)
    )
    if not rows:
        return "<p><em>No cache discrepancies detected</em></p>"

    return (
        "<table><thead><tr><th>Field</th><th>Cache</th><th>Live</th></tr></thead>"
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


def _item_link_html(qid: str | None) -> str:
    if not qid:
        return ""
    escaped_qid = escape(qid)
    href = f"https://www.wikidata.org/wiki/{escaped_qid}"
    return f'<a href="{href}" target="_blank" rel="noopener noreferrer">{escaped_qid}</a>'


def build_signal_debug_payload(result: EvaluationResult) -> dict[str, Any]:
    payload: dict[str, Any] = {
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
    return payload


def render_signal_debug_html(report: dict[str, Any] | None) -> str:
    if report is None:
        return ""

    if report.get("report_variant") == "inlinks":
        qid = report.get("qid")
        levels = report.get("levels", {})
        errors = report.get("errors", {})
        comparison_html = (
            "<h2>Cache vs Live</h2>"
            f"{_render_inlinks_comparison_table(report)}"
            "<h3>Errors</h3>"
            f"{_render_levels_table({key: levels.get(key) for key in INLINKS_COMPARISON_LEVELS if isinstance(levels, dict)}, errors)}"
        )
        discrepancy_html = ""
        discrepancies = report.get("cache_discrepancies")
        if discrepancies is not None:
            discrepancy_html = (
                "<h2>Cache Discrepancies</h2>"
                f"{_render_discrepancy_table(discrepancies)}"
            )
        return (
            f"{comparison_html}"
            f"{discrepancy_html}"
            "<h2>Source Context</h2>"
            "<p><em>Inlinks scan detail only reports N1 and N2.</em></p>"
        )

    qid = report.get("qid")
    levels = report.get("levels", {})
    errors = report.get("errors", {})
    grouped_signals = report.get("signals_by_detected_criterion", {})
    source_contexts = report.get("source_contexts", {})
    source_urls = report.get("source_urls", [])
    cached_snapshot = report.get("cached_snapshot")
    interest = report.get("interest")
    queue = report.get("queue")
    inlinks_scan = report.get("inlinks_scan")
    discrepancies = report.get("cache_discrepancies")

    comparison_html = (
        "<h2>Cache vs Live</h2>"
        f"{_render_comparison_table(report)}"
        "<h3>Errors</h3>"
        f"{_render_levels_table(levels, errors)}"
    )

    grouped_sections = ""
    if not isinstance(grouped_signals, dict):
        grouped_signals = {}
    for criterion in DETECTED_CRITERIA:
        rows = grouped_signals.get(criterion, [])
        if not rows:
            continue
        body = "".join(
            "<tr>"
            f"<td>{escape(str(signal.get('criterion', '')))}</td>"
            f"<td class='{_level_class(signal.get('level'))}'>{escape(str(signal.get('level', '')))}</td>"
            f"<td>{escape(str(signal.get('detector', '')))}</td>"
            f"<td>{escape(str(signal.get('key', '')))}</td>"
            f"<td>{_render_properties_html(signal.get('properties', {}))}</td>"
            "</tr>"
            for signal in rows
        )
        grouped_sections += (
            f"<h3>{escape(criterion)}</h3>"
            "<table><thead><tr><th>Criterion</th><th>Level</th><th>Detector</th><th>Key</th><th>Properties</th></tr></thead>"
            f"<tbody>{body}</tbody></table>"
        )

    all_signals_rows = "".join(
        "<tr>"
        f"<td>{escape(str(signal.get('criterion', '')))}</td>"
        f"<td class='{_level_class(signal.get('level'))}'>{escape(str(signal.get('level', '')))}</td>"
        f"<td>{escape(str(signal.get('detector', '')))}</td>"
        f"<td>{escape(str(signal.get('key', '')))}</td>"
        f"<td>{_render_properties_html(signal.get('properties', {}))}</td>"
        "</tr>"
        for signal in report.get("signals", [])
        if isinstance(signal, dict)
    )

    source_rows = ""
    if isinstance(source_urls, list):
        source_rows = "".join(
            f"<tr><td>{escape(str(source.get('source', '')))}</td><td>{escape(str(source.get('api_url', '')))}</td><td>{escape(str(source.get('ui_url', '')))}</td></tr>"
            for source in source_urls
            if isinstance(source, dict)
        )
    if not source_rows:
        source_rows = "<tr><td colspan='3'><em>No sources</em></td></tr>"

    source_context_sections = ""
    if isinstance(source_contexts, dict):
        for source_name, context in source_contexts.items():
            source_context_sections += (
                f"<h3>{escape(str(source_name))}</h3>"
                f"<details><summary>Raw context</summary><pre>{escape(json.dumps(context, indent=2, ensure_ascii=False, default=str))}</pre></details>"
            )

    if not source_context_sections:
        source_context_sections = "<p><em>No source context available</em></p>"

    interest_html = (
        "<h2>Interest</h2>"
        f"{_render_interest_table(interest if isinstance(interest, dict) else None)}"
    )

    queue_html = (
        "<h2>Queue Position</h2>"
        f"{_render_queue_table(queue if isinstance(queue, dict) else None)}"
    )

    discrepancy_html = ""
    if discrepancies is not None:
        discrepancy_html = (
            "<h2>Cache Discrepancies</h2>"
            f"{_render_discrepancy_table(discrepancies)}"
        )

    inlinks_html = ""
    if isinstance(inlinks_scan, dict):
        visible_inlinks = inlinks_scan.get("visible_inlinks", [])
        truncated = bool(inlinks_scan.get("truncated", False))
        reports = inlinks_scan.get("reports", [])
        visible_text = ""
        if isinstance(visible_inlinks, list):
            visible_text = ", ".join(escape(str(qid_value)) for qid_value in visible_inlinks if isinstance(qid_value, str))
        report_sections = ""
        if isinstance(reports, list):
            report_sections = "".join(
                "<details class='inlinks-report'>"
                f"<summary>{escape(str(item.get('qid', '')))} - {escape(str((item.get('levels') or {}).get('N12', 'unknown')).upper())}"
                f"{' · cache missing' if isinstance(item.get('cache_discrepancies'), dict) and item.get('cache_discrepancies', {}).get('status') == 'missing' else ''}"
                f"{' · ' + str(item.get('cache_discrepancies', {}).get('count', 0)) + ' discrepancy(s)' if isinstance(item.get('cache_discrepancies'), dict) and item.get('cache_discrepancies', {}).get('status') == 'ok' and int(item.get('cache_discrepancies', {}).get('count', 0)) else ''}"
                "</summary>"
                f"{item.get('html', '')}"
                "</details>"
                for item in reports
                if isinstance(item, dict)
            )
        if not report_sections:
            report_sections = "<p><em>No inlinks reports</em></p>"
        inlinks_html = (
            "<h2>Inlinks Scan</h2>"
            f"<p class='subtle'>Visible inlinks: {visible_text or 'none'}{'; truncated' if truncated else ''}</p>"
            f"{report_sections}"
        )

    return (
        f"{comparison_html}"
        f"{interest_html}"
        f"{queue_html}"
        f"{discrepancy_html}"
        f"{inlinks_html}"
        "<h2>Sources</h2>"
        "<table><thead><tr><th>Source</th><th>API</th><th>UI</th></tr></thead>"
        f"<tbody>{source_rows}</tbody></table>"
        "<h2>Source Context</h2>"
        f"{source_context_sections}"
        "<h2>Signals by Detected Criterion</h2>"
        f"{grouped_sections}"
        "<h2>All Signals</h2>"
        "<table><thead><tr><th>Criterion</th><th>Level</th><th>Detector</th><th>Key</th><th>Properties</th></tr></thead>"
        f"<tbody>{all_signals_rows}</tbody></table>"
    )


__all__ = [
    "build_signal_debug_payload",
    "render_signal_debug_html",
]

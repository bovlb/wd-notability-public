from __future__ import annotations

import json
from datetime import UTC, datetime
from html import escape

from fastapi import HTTPException

from wd_notability.creations import _normalize_text as normalize_text
from wd_notability.models import EvaluationReason, EvaluationResult, NotabilityLevel

DETECTED_CRITERIA = ("N1", "N2a", "N2b", "N3_inlinks", "N3_osm", "N3_wikisub", "N3_sdc")
BADGE_TOOLTIP_FIELDS = (
    ("N12", "N12 intrinsic"),
    ("N1", "N1 sitelinks"),
    ("N2", "N2"),
    ("N2a", "N2a identifiers"),
    ("N2b", "N2b sources"),
    ("N3", "N3 extrinsic"),
    ("N3_inlinks", "N3_inlinks"),
    ("N3_osm", "N3_osm"),
    ("N3_sdc", "N3_sdc"),
    ("N3_wikisub", "N3_wikisub"),
)
BADGE_LEVEL_CLASS_NAMES = {
    "none": "level-none",
    "weak": "level-weak",
    "strong": "level-strong",
    "partial-weak": "level-partial-weak",
    "partial-strong": "level-partial-strong",
    "unknown": "level-unknown",
}


def _report_levels(report: dict | None) -> dict[str, object]:
    if not isinstance(report, dict):
        return {}
    levels = report.get("levels", {})
    return levels if isinstance(levels, dict) else {}


def _bool_text(value: object) -> str:
    if value is True:
        return "YES"
    if value is False:
        return "NO"
    return "UNKNOWN"


def _count_text(report: dict | None, field: str) -> str:
    if not isinstance(report, dict):
        return "UNKNOWN"
    if report.get("content_last_revid") is None:
        return "UNKNOWN"
    value = report.get(field)
    try:
        return "YES" if int(value) > 0 else "NO"
    except (TypeError, ValueError):
        return "UNKNOWN"


def _value_text(value: object, default: str = "UNKNOWN") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return "YES" if value else "NO"
    return str(value)


def _level_class_name(level: object) -> str:
    return BADGE_LEVEL_CLASS_NAMES.get(str(level).lower(), "level-none")


def _level_text_html(level: object) -> str:
    text = str(level if level is not None else "unknown").lower()
    if text == "unknown":
        return "UNKNOWN / PENDING"
    if text == "partial-weak":
        return (
            '<span class="level-partial-prefix">PARTIAL-</span>'
            '<span class="level-weak">WEAK</span>'
        )
    if text == "partial-strong":
        return (
            '<span class="level-partial-prefix">PARTIAL-</span>'
            '<span class="level-strong">STRONG</span>'
        )
    return escape(text.upper())


def _hovercard_label_html(label: str, subtitle: str | None = None) -> str:
    if subtitle:
        return (
            '<span class="notability-badge-hovercard-label">'
            f'<span class="notability-badge-hovercard-label-title">{escape(label)}</span>'
            f'<span class="notability-badge-hovercard-label-subtitle">{escape(subtitle)}</span>'
            "</span>"
        )
    return f'<span class="notability-badge-hovercard-label">{escape(label)}</span>'


def _hovercard_row(
    label: str,
    value: object,
    *,
    depth: int = 0,
    value_class: str | None = None,
    subtitle: str | None = None,
) -> str:
    classes = "notability-badge-hovercard-row"
    if value_class:
        classes += f" {value_class}"
    return (
        f'<div class="{classes}" style="--badge-hovercard-depth: {depth}">'
        f'{_hovercard_label_html(label, subtitle)}'
        f'<span class="notability-badge-hovercard-value">{escape(_value_text(value))}</span>'
        "</div>"
    )


def _hovercard_level_row(
    label: str,
    value: object,
    *,
    depth: int = 0,
    subtitle: str | None = None,
) -> str:
    level_text = str(value if value is not None else "unknown")
    return (
        f'<div class="notability-badge-hovercard-row" style="--badge-hovercard-depth: {depth}">'
        f'{_hovercard_label_html(label, subtitle)}'
        f'<span class="notability-badge-hovercard-value {_level_class_name(level_text)}">{_level_text_html(level_text)}</span>'
        "</div>"
    )


def _hovercard_qid_row(label: str, value: object, *, depth: int = 0) -> str:
    if isinstance(value, int):
        text = f"Q{value}"
    elif isinstance(value, str) and value:
        text = value
    else:
        text = "UNKNOWN"
    return _hovercard_row(label, text, depth=depth)


def _badge_hovercard_html_from_report(report: dict | None) -> str:
    levels = _report_levels(report)
    snapshot = _report_snapshot(report)
    report = report if isinstance(report, dict) else {}
    rows = [
        _hovercard_level_row("Overall", levels.get("N"), depth=0),
        _hovercard_level_row("N12 intrinsic", levels.get("N12"), depth=1),
        _hovercard_level_row("N1 sitelinks", levels.get("N1"), depth=2),
        _hovercard_level_row("N2", levels.get("N2"), depth=2),
        _hovercard_level_row("N2a identifiers", levels.get("N2a"), depth=3),
        _hovercard_level_row("N2b sources", levels.get("N2b"), depth=3),
        _hovercard_row("Sitelinks count", report.get("has_sitelinks_count"), depth=3),
        _hovercard_row("Claims count", report.get("has_claims_count"), depth=3),
        _hovercard_row("Content stale", _bool_text(snapshot.get("content_stale", report.get("content_stale"))), depth=2),
    ]

    if report.get("is_deleted") is True:
        rows.append(_hovercard_row("Deleted", "YES", depth=2))

    if report.get("is_redirect") is True:
        rows.append(_hovercard_qid_row("Redirect target", report.get("redirect_target"), depth=2))

    rows.extend([
        _hovercard_level_row("N3 extrinsic", levels.get("N3"), depth=1),
        _hovercard_level_row("N3_inlinks", levels.get("N3_inlinks"), depth=2),
        _hovercard_level_row("N3_osm", levels.get("N3_osm"), depth=2),
        _hovercard_level_row("N3_sdc", levels.get("N3_sdc"), depth=2),
        _hovercard_level_row("N3_wikisub", levels.get("N3_wikisub"), depth=2),
    ])

    inlinks_count = report.get("inlinks_count")
    inlinks_count_display = report.get("inlinks_count_display")
    if isinstance(inlinks_count_display, str) and inlinks_count_display.strip():
        rows.insert(-3, _hovercard_row("Inlinks count", inlinks_count_display, depth=3))
    elif isinstance(inlinks_count, int) and inlinks_count > 0:
        rows.insert(-3, _hovercard_row("Inlinks count", inlinks_count, depth=3))

    creator = snapshot.get("creator")
    if creator:
        rows.append(_hovercard_row("Creator", creator, depth=1))

    created = snapshot.get("creation_time_iso") or _utc_isoformat(snapshot.get("creation_time"))
    if created:
        rows.append(_hovercard_row("Created", created, depth=1))

    updated = snapshot.get("last_updated_iso") or _utc_isoformat(snapshot.get("last_updated"))
    if updated:
        rows.append(_hovercard_row("Last updated", updated, depth=1))

    modified = snapshot.get("inlinks_last_evaluated_iso") or _utc_isoformat(snapshot.get("inlinks_last_evaluated"))
    if modified:
        rows.append(_hovercard_row("Modified", modified, depth=1))

    return (
        '<div class="notability-badge-hovercard" role="tooltip" aria-hidden="true">'
        '<div class="notability-badge-hovercard-body">'
        f"{''.join(rows)}"
        "</div></div>"
    )


def _utc_isoformat(value: object | None) -> str | None:
    if value is None:
        return None
    try:
        if isinstance(value, datetime):
            timestamp = int((value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)).timestamp())
        else:
            timestamp = int(value)
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _report_snapshot(report: dict | None) -> dict:
    if not isinstance(report, dict):
        return {}
    cached_snapshot = report.get("cached_snapshot")
    if isinstance(cached_snapshot, dict):
        return cached_snapshot
    return report


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


def _normalize_subscription_items(request) -> dict[str, EvaluationReason]:
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
        raise HTTPException(
            status_code=400, detail="owner_id must be gadget, report, or inlinks")
    return owner


def _normalize_creator_username(username: str) -> str:
    normalized = normalize_text(username)
    if normalized is None:
        raise HTTPException(
            status_code=400, detail="username must not be empty")
    return normalized


def _creator_history_payload(history) -> dict[str, object] | None:
    if history is None:
        return None
    return {
        "username": history.username,
        "window_start": history.window_start,
        "window_end": history.window_end,
        "requested_at": history.requested_at,
        "started_at": history.started_at,
        "finished_at": history.finished_at,
        "last_refresh_at": history.last_refresh_at,
        "error_text": history.error_text,
        "row_count": history.row_count,
    }


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


def _badge_level(result, criterion: str) -> str:
    return result.levels_str[criterion]


def _badge_tooltip_from_levels(levels: dict[str, str]) -> str:
    def _level(field: str) -> str:
        value = levels.get(field, "unknown")
        if str(value).lower() == "unknown":
            return "UNKNOWN / PENDING"
        return str(value).upper()

    lines = [f"Overall: {_level('N')}"]
    for field, label in BADGE_TOOLTIP_FIELDS:
        lines.append(f"{label}: {_level(field)}")
    return "\n".join(lines)


def _badge_tooltip(result) -> str:
    return _badge_tooltip_from_levels(result.levels_str)


def _badge_tooltip_from_report(report: dict) -> str:
    levels = _report_levels(report)
    snapshot = _report_snapshot(report)
    lines = [_badge_tooltip_from_levels(levels)]

    creator = snapshot.get("creator")
    if creator:
        lines.append(f"Creator: {creator}")

    created = snapshot.get("creation_time_iso") or _utc_isoformat(snapshot.get("creation_time"))
    if created:
        lines.append(f"Created: {created}")

    updated = snapshot.get("last_updated_iso") or _utc_isoformat(snapshot.get("last_updated"))
    if updated:
        lines.append(f"Last updated: {updated}")

    modified = snapshot.get("inlinks_last_evaluated_iso") or _utc_isoformat(snapshot.get("inlinks_last_evaluated"))
    if modified:
        lines.append(f"Modified: {modified}")

    lines.append(f"Content stale: {_bool_text(snapshot.get('content_stale', report.get('content_stale')))}")
    lines.append(f"Has sitelinks: {_count_text(report, 'has_sitelinks_count')}")
    lines.append(f"Has claims: {_count_text(report, 'has_claims_count')}")
    if report.get("is_deleted") is True:
        lines.append("Deleted: YES")
    if report.get("is_redirect") is True:
        redirect_target = report.get("redirect_target")
        if redirect_target is not None:
            lines.append(f"Redirect target: Q{redirect_target}")
        else:
            lines.append("Redirect target: UNKNOWN")
    if str(levels.get("N3_inlinks", "unknown")).lower() != "unknown":
        inlinks_count = report.get("inlinks_count")
        if isinstance(inlinks_count, int):
            lines.append(f"Inlinks count: {inlinks_count}")
    return "\n".join(lines)


def _badge_payload(
    qid: str,
    result,
    *,
    content_stale: bool | None = None,
    creator: str | None = None,
    creation_time: int | None = None,
) -> dict[str, object]:
    payload = {
        "event": "update",
        "qid": qid,
        "levels": result.levels_str,
        "redirect": result.is_redirect,
        "redirect_target": result.redirect_target,
        "has_claims_count": result.has_claims_count,
        "has_sitelinks_count": result.has_sitelinks_count,
        "inlinks_count": result.inlinks_count,
        "is_deleted": result.is_deleted,
        "content_last_revid": result.content_last_revid,
        "recent_changes_last_revid": result.recent_changes_last_revid,
        "content_stale": content_stale,
    }
    if creator is not None:
        payload["creator"] = creator
    if creation_time is not None:
        payload["creation_time"] = creation_time
    payload["badge_tooltip"] = _badge_tooltip_from_report(payload)
    payload["badge_hovercard"] = _badge_hovercard_html_from_report(payload)
    return payload


def _cached_payload(
    qid: str,
    result,
    content_last_revid: int | None,
    recent_changes_last_revid: int | None,
    *,
    content_stale: bool | None = None,
    creator: str | None = None,
    creation_time: int | None = None,
) -> dict[str, object]:
    payload = {
        "event": "cache",
        "qid": qid,
        "levels": result.levels_str,
        "redirect": result.is_redirect,
        "redirect_target": result.redirect_target,
        "has_claims_count": result.has_claims_count,
        "has_sitelinks_count": result.has_sitelinks_count,
        "inlinks_count": result.inlinks_count,
        "is_deleted": result.is_deleted,
        "content_last_revid": content_last_revid,
        "recent_changes_last_revid": recent_changes_last_revid,
        "content_stale": content_stale,
    }
    if creator is not None:
        payload["creator"] = creator
    if creation_time is not None:
        payload["creation_time"] = creation_time
    payload["badge_tooltip"] = _badge_tooltip_from_report(payload)
    payload["badge_hovercard"] = _badge_hovercard_html_from_report(payload)
    return payload


def _badge_field_value(report: dict | None, field: str, default: str = "unknown") -> str:
    if report is None:
        return default

    if field in {"n", "n_ring", "n1", "n2a", "n2b", "n3", "n3_halves"}:
        level_keys = {
            "n": "N",
            "n_ring": "N",
            "n1": "N1",
            "n2a": "N2a",
            "n2b": "N2b",
            "n3": "N3",
            "n3_halves": "N3",
        }
        levels = _report_levels(report)
        if not levels:
            return default
        value = levels.get(level_keys[field])
        return str(value) if value is not None else default

    if field == "has_claims":
        if report.get("content_last_revid") is None:
            return default
        value = report.get("has_claims_count")
        try:
            return "true" if int(value) > 0 else "false"
        except (TypeError, ValueError):
            return default

    report_key = "is_redirect" if field == "redirect" else field
    value = report.get(report_key)
    if isinstance(value, bool):
        return str(value).lower()
    return default


def _render_report_badge(report: dict | None, qid: str, badge_suffix: str = "") -> str:
    suffix = f"-{badge_suffix}" if badge_suffix else ""
    values = {
        field: escape(_badge_field_value(report, field), quote=True)
        for field in (
            "n",
            "n_ring",
            "n1",
            "n2a",
            "n2b",
            "n3",
            "n3_halves",
            "redirect",
            "has_claims",
            "is_deleted",
        )
    }
    tooltip = escape(
        _badge_tooltip_from_report(report) if isinstance(
            report, dict) else "Notability badge",
        quote=True,
    )
    hovercard = _badge_hovercard_html_from_report(report if isinstance(report, dict) else None)
    label = escape(
        f"Notability badge for {qid}: overall {_badge_field_value(report, 'n')}",
        quote=True,
    )
    return f"""
<svg class=\"report-badge\" role=\"img\" aria-label=\"{label}\" baseProfile=\"full\" version=\"1.1\" viewBox=\"0 0 36 36\"
    xmlns=\"http://www.w3.org/2000/svg\" data-deleted=\"{escape(_badge_field_value(report, 'is_deleted'), quote=True) if isinstance(report, dict) else 'false'}\">
  <defs>
    <clipPath id=\"report-n3-half-top{suffix}\">
      <rect x=\"0\" y=\"0\" width=\"36\" height=\"18\" />
    </clipPath>
    <clipPath id=\"report-n3-half-bottom{suffix}\">
      <rect x=\"0\" y=\"18\" width=\"36\" height=\"18\" />
    </clipPath>
  </defs>
  <style>
    [data-field][data-value=\"unknown\"] {{ stroke: grey; fill: grey; }}
    [data-field][data-value=\"none\"] {{ stroke: var(--level-none, #b00020); fill: var(--level-none, #b00020); }}
    [data-field][data-value=\"weak\"] {{ stroke: var(--level-weak, #b26a00); fill: var(--level-weak, #b26a00); }}
    [data-field][data-value=\"strong\"] {{ stroke: var(--level-strong, #1b7f2a); fill: var(--level-strong, #1b7f2a); }}
    [data-field=\"n\"][data-value=\"partial-weak\"],
    [data-field=\"n\"][data-value=\"partial-strong\"] {{ display: none; }}
    [data-field=\"n\"][data-value=\"none\"],
    [data-field=\"n\"][data-value=\"weak\"],
    [data-field=\"n\"][data-value=\"strong\"] {{ fill: none; }}
    [data-field=\"n_ring\"] {{ display: none; }}
    [data-field=\"n_ring\"][data-value=\"partial-weak\"],
    [data-field=\"n_ring\"][data-value=\"partial-strong\"] {{ display: block; }}
    [data-field=\"n_ring\"][data-value=\"partial-weak\"] .n-ring-none,
    [data-field=\"n_ring\"][data-value=\"partial-strong\"] .n-ring-none {{ stroke: var(--level-none, #b00020); fill: none; }}
    [data-field=\"n_ring\"][data-value=\"partial-weak\"] .n-ring-second {{ stroke: var(--level-partial-weak-second, #c05d00); fill: none; }}
    [data-field=\"n_ring\"][data-value=\"partial-strong\"] .n-ring-second {{ stroke: var(--level-partial-strong-second, #1b7f2a); fill: none; }}
    [data-field=\"n3\"][data-value=\"partial-weak\"],
    [data-field=\"n3\"][data-value=\"partial-strong\"] {{ display: none; }}
    [data-field=\"n3_halves\"] {{ display: none; }}
    [data-field=\"n3_halves\"][data-value=\"partial-weak\"],
    [data-field=\"n3_halves\"][data-value=\"partial-strong\"] {{ display: block; }}
    [data-field=\"n3_halves\"][data-value=\"partial-weak\"] .n3-half-none,
    [data-field=\"n3_halves\"][data-value=\"partial-strong\"] .n3-half-none {{ stroke: var(--level-none, #b00020); fill: var(--level-none, #b00020); }}
    [data-field=\"n3_halves\"][data-value=\"partial-weak\"] .n3-half-second {{ stroke: var(--level-partial-weak-second, #c05d00); fill: var(--level-partial-weak-second, #c05d00); }}
    [data-field=\"n3_halves\"][data-value=\"partial-strong\"] .n3-half-second {{ stroke: var(--level-partial-strong-second, #1b7f2a); fill: var(--level-partial-strong-second, #1b7f2a); }}
    [data-field=\"redirect\"] {{ display: none; }}
    [data-field=\"redirect\"][data-value=\"true\"] {{ display: block; }}
    [data-field=\"is_deleted\"] {{ display: none; }}
    [data-field=\"is_deleted\"][data-value=\"true\"] {{ display: block; }}
    svg[data-deleted=\"true\"] [data-field=\"normal\"] {{ display: none; }}
    svg[data-deleted=\"true\"] [data-field=\"is_deleted\"] {{ display: block; }}
    [data-field=\"has_claims\"][data-value=\"unknown\"] {{ display: none; }}
    [data-field=\"has_claims\"][data-value=\"true\"] {{ display: none; }}
    [data-field=\"has_claims\"][data-value=\"false\"] {{ display: block; }}
  </style>
  <g data-field=\"normal\" data-value=\"unknown\">
    <circle cx=\"18.0\" cy=\"18.0\" r=\"14.66\" fill=\"none\" stroke-width=\"3.8\"
           data-field=\"n\" data-value=\"{values['n']}\"/>
    <g data-field=\"n_ring\" data-value=\"{values['n_ring']}\">
      <path class=\"n-ring-none\" d=\"M18.00 3.34 A14.66 14.66 0 0 1 32.66 18.00\" fill=\"none\" stroke-width=\"3.8\" stroke-linecap=\"butt\" />
      <path class=\"n-ring-second\" d=\"M32.66 18.00 A14.66 14.66 0 0 1 18.00 32.66\" fill=\"none\" stroke-width=\"3.8\" stroke-linecap=\"butt\" />
      <path class=\"n-ring-none\" d=\"M18.00 32.66 A14.66 14.66 0 0 1 3.34 18.00\" fill=\"none\" stroke-width=\"3.8\" stroke-linecap=\"butt\" />
      <path class=\"n-ring-second\" d=\"M3.34 18.00 A14.66 14.66 0 0 1 18.00 3.34\" fill=\"none\" stroke-width=\"3.8\" stroke-linecap=\"butt\" />
    </g>
    <path data-field=\"n1\" d=\"M12.78,28.04 A11.32,11.32 0 0,1 12.78,7.96 Z\" data-value=\"{values['n1']}\" />
    <g data-field=\"n3_halves\" data-value=\"{values['n3_halves']}\">
      <path class=\"n3-half-none\" clip-path=\"url(#report-n3-half-top{suffix})\"
            d=\"M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z\" data-value=\"{values['n3']}\" />
      <path class=\"n3-half-second\" clip-path=\"url(#report-n3-half-bottom{suffix})\"
            d=\"M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z\" data-value=\"{values['n3']}\" />
    </g>
    <path data-field=\"n3\" d=\"M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z\" data-value=\"{values['n3']}\" />
    <path data-field=\"n2a\" d=\"M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,17.28 L14.1,17.28 Z\"
           data-value=\"{values['n2a']}\" />
    <path data-field=\"n2b\" d=\"M14.1,28.62 A11.32,11.32 0 0,0 21.9,28.62 L21.9,18.72 L14.1,18.72 Z\"
          data-value=\"{values['n2b']}\" />
    <path data-field=\"has_claims\" d=\"M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,28.62 A11.32,11.32 0 0,1 14.1,28.62 Z\"
          fill=\"#fff\" data-value=\"{values['has_claims']}\" />
    <path data-field=\"redirect\" data-value=\"{values['redirect']}\"
          d=\"M1.5 15.0 H15.2 V10.5 L23.22 18.0 L15.2 25.5 V21.0 H1.5 Z\"
          fill=\"#6a1b9a\" />
  </g>
  <g data-field=\"is_deleted\" data-value=\"{values['is_deleted']}\" fill=\"none\" stroke=\"#c62828\" stroke-width=\"4.2\" stroke-linecap=\"round\">
    <path d=\"M7 7 L29 29\" />
    <path d=\"M29 7 L7 29\" />
  </g>
</svg>
{hovercard}
"""


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

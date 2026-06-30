from __future__ import annotations

import json
from html import escape
from typing import Any

from wd_notability.models import NotabilityLevel


def build_inlinks_debug_payload(context: dict[str, Any] | None) -> dict[str, Any]:
    if context is None:
        return {"context": None}

    payload: dict[str, Any] = {
        "target_qid": context.get("target_qid"),
        "visible_inlinks": list(context.get("visible_inlinks", [])) if isinstance(context.get("visible_inlinks"), list) else [],
        "truncated": bool(context.get("truncated", False)),
        "watched_inlinks": list(context.get("watched_inlinks", [])) if isinstance(context.get("watched_inlinks"), list) else [],
        "queued_entitydata": sorted(context.get("queued_entitydata", [])) if isinstance(context.get("queued_entitydata"), (set, list, tuple)) else [],
        "cursor": context.get("cursor"),
        "final_level": str(context.get("final_level")) if context.get("final_level") is not None else None,
        "raw": context,
    }
    return payload


def render_inlinks_debug_html(report: dict[str, Any] | None) -> str:
    if report is None:
        return ""
    rows = "".join(
        f"<tr><td>{escape(str(key))}</td><td>{escape(json.dumps(value, ensure_ascii=False, default=str) if not isinstance(value, str) else value)}</td></tr>"
        for key, value in report.items()
        if key != "raw"
    )
    raw = report.get("raw")
    raw_html = escape(json.dumps(raw, indent=2, ensure_ascii=False, default=str)) if raw is not None else "<em>none</em>"
    return (
        "<h2>Inlinks Debug</h2>"
        "<table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{rows or '<tr><td colspan=\"2\"><em>empty</em></td></tr>'}</tbody></table>"
        "<h3>Raw context</h3>"
        f"<pre>{raw_html}</pre>"
    )


__all__ = [
    "build_inlinks_debug_payload",
    "render_inlinks_debug_html",
]

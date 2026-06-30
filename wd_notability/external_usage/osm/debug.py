from __future__ import annotations

import json
from html import escape
from typing import Any


def build_osm_debug_payload(context: dict[str, Any] | None) -> dict[str, Any]:
    if context is None:
        return {"context": None}
    row = context.get("row", {})
    return {
        "qid": context.get("qid"),
        "usage_count": int(row.get("count_all", 0)) if isinstance(row, dict) else 0,
        "count_nodes": int(row.get("count_nodes", 0)) if isinstance(row, dict) else 0,
        "count_ways": int(row.get("count_ways", 0)) if isinstance(row, dict) else 0,
        "count_relations": int(row.get("count_relations", 0)) if isinstance(row, dict) else 0,
        "object_explorer_url": context.get("object_explorer_url"),
        "raw": context,
    }


def render_osm_debug_html(report: dict[str, Any] | None) -> str:
    if report is None:
        return ""
    rows = "".join(f"<tr><td>{escape(str(k))}</td><td>{escape(str(v))}</td></tr>" for k, v in report.items() if k != "raw")
    raw = report.get("raw")
    raw_html = escape(json.dumps(raw, indent=2, ensure_ascii=False, default=str)) if raw is not None else "<em>none</em>"
    return (
        "<h2>OSM Debug</h2>"
        "<table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{rows or '<tr><td colspan=\"2\"><em>empty</em></td></tr>'}</tbody></table>"
        "<h3>Raw context</h3>"
        f"<pre>{raw_html}</pre>"
    )


OSM_DEBUG = {
    "build": build_osm_debug_payload,
    "render": render_osm_debug_html,
}


from __future__ import annotations

import json
from html import escape
from typing import Any


def build_wikisub_debug_payload(context: dict[str, Any] | None) -> dict[str, Any]:
    if context is None:
        return {"context": None}
    return {
        "qid": context.get("qid"),
        "is_subscribed": context.get("is_subscribed"),
        "raw": context,
    }


def render_wikisub_debug_html(report: dict[str, Any] | None) -> str:
    if report is None:
        return ""
    rows = "".join(f"<tr><td>{escape(str(k))}</td><td>{escape(str(v))}</td></tr>" for k, v in report.items() if k != "raw")
    raw = report.get("raw")
    raw_html = escape(json.dumps(raw, indent=2, ensure_ascii=False, default=str)) if raw is not None else "<em>none</em>"
    return (
        "<h2>Wiki subscribers Debug</h2>"
        "<table><thead><tr><th>Field</th><th>Value</th></tr></thead>"
        f"<tbody>{rows or '<tr><td colspan=\"2\"><em>empty</em></td></tr>'}</tbody></table>"
        "<h3>Raw context</h3>"
        f"<pre>{raw_html}</pre>"
    )


WIKI_SUBSCRIBERS_DEBUG = {
    "build": build_wikisub_debug_payload,
    "render": render_wikisub_debug_html,
}


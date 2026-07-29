from __future__ import annotations

import html
import re
from html import escape
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import HTMLResponse
from markdown_it import MarkdownIt

MARKDOWN_RENDERER = MarkdownIt("commonmark", {"html": True})
MERMAID_FENCE_RE = re.compile(
    r"<pre><code class=\"language-mermaid\">(.*?)</code></pre>",
    re.DOTALL,
)


def _render_markdown_html(markdown: str) -> str:
    rendered = MARKDOWN_RENDERER.render(markdown)
    return MERMAID_FENCE_RE.sub(
        lambda match: f'<div class="mermaid">{html.unescape(match.group(1))}</div>',
        rendered,
    )


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
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
    <link rel="icon" href="/static/favicon-32.png" type="image/png" sizes="32x32" />
    <link rel="icon" href="/static/favicon-16.png" type="image/png" sizes="16x16" />
    <link rel="shortcut icon" href="/favicon.ico" />
    <title>{escaped_title}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
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
      .mermaid {{ overflow-x: auto; }}
      strong {{ font-weight: 700; }}
    </style>
    <script>
      window.addEventListener("DOMContentLoaded", () => {{
        if (!window.mermaid) {{
          return;
        }}
        mermaid.initialize({{
          startOnLoad: true,
          securityLevel: "loose",
          theme: matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "default",
        }});
      }});
    </script>
  </head>
  <body>
    {body}
  </body>
</html>
"""


def _render_static_markdown_page(static_dir: Path, filename: str) -> HTMLResponse:
    if "/" in filename or "\\" in filename or not filename.endswith(".md"):
        raise HTTPException(status_code=404, detail="Markdown page not found")

    path = static_dir / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Markdown page not found")

    markdown = path.read_text(encoding="utf-8")
    title = _markdown_title(
        markdown, fallback=path.stem.replace("-", " ").title())
    return HTMLResponse(content=_render_markdown_document(markdown, title=title))


def _render_observability_dashboard_html(observability_js_version: int) -> HTMLResponse:
    return HTMLResponse(
        content=f"""
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
      :root {{
        color-scheme: light dark;
        --bg: #f5f7fb;
        --panel: rgba(255, 255, 255, 0.86);
        --panel-strong: #ffffff;
        --text: #172033;
        --muted: #5c677d;
        --border: rgba(71, 85, 105, 0.2);
        --accent: #0f766e;
        --accent-2: #2563eb;
      }}
      @media (prefers-color-scheme: dark) {{
        :root {{
          --bg: #0b1220;
          --panel: rgba(15, 23, 42, 0.86);
          --panel-strong: #111827;
          --text: #e5eefc;
          --muted: #9ca9bf;
          --border: rgba(148, 163, 184, 0.22);
          --accent: #5eead4;
          --accent-2: #60a5fa;
        }}
      }}
      body {{
        margin: 0;
        min-height: 100vh;
        font-family: ui-sans-serif, system-ui, sans-serif;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(37, 99, 235, 0.18), transparent 26%),
          radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 24%),
          linear-gradient(180deg, var(--bg), var(--bg));
      }}
      header {{
        padding: 1.25rem 1.5rem 1rem;
        border-bottom: 1px solid var(--border);
        background: linear-gradient(135deg, var(--panel-strong), var(--panel));
        backdrop-filter: blur(14px);
      }}
      h1 {{
        margin: 0;
        font-size: clamp(1.7rem, 4vw, 2.6rem);
        letter-spacing: -0.04em;
      }}
      .subtle {{
        margin: .45rem 0 0;
        color: var(--muted);
        max-width: 60rem;
      }}
      .nav {{
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-top: .8rem;
      }}
      .nav a {{ color: var(--accent-2); text-decoration: none; }}
      main {{
        padding: 1rem 1.5rem 2rem;
        display: grid;
        gap: 1rem;
      }}
      .panel {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
        backdrop-filter: blur(10px);
      }}
      .summary {{
        display: grid;
        gap: .75rem;
        grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr));
      }}
      .summary .card {{
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: .85rem 1rem;
      }}
      .summary .label {{
        color: var(--muted);
        font-size: .88rem;
        text-transform: uppercase;
        letter-spacing: .05em;
      }}
      .summary .value {{
        font-size: 1.4rem;
        font-weight: 750;
        margin-top: .2rem;
      }}
      .toolbar {{
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        gap: .85rem 1rem;
        align-items: end;
      }}
      .controls {{
        display: flex;
        flex-wrap: wrap;
        gap: .65rem .75rem;
        align-items: end;
      }}
      .controls label {{
        display: grid;
        gap: .3rem;
        color: var(--muted);
        font-size: .88rem;
      }}
      .controls input[type="search"] {{
        min-width: min(100%, 24rem);
        padding: .72rem .85rem;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        font: inherit;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
      }}
      .controls input[type="search"]::placeholder {{
        color: var(--muted);
      }}
      .controls select {{
        min-width: 8.5rem;
        padding: .72rem .85rem;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        font: inherit;
      }}
      .controls button {{
        appearance: none;
        border: 1px solid rgba(37, 99, 235, 0.24);
        background: linear-gradient(180deg, rgba(37, 99, 235, 0.12), rgba(37, 99, 235, 0.06));
        color: var(--text);
        border-radius: 12px;
        padding: .72rem 1rem;
        font: inherit;
        font-weight: 650;
        cursor: pointer;
      }}
      .controls button:hover {{
        border-color: rgba(37, 99, 235, 0.45);
      }}
      .controls button:active {{
        transform: translateY(1px);
      }}
      .toggle {{
        display: inline-flex;
        align-items: center;
        gap: .5rem;
        padding: .72rem .9rem;
        border: 1px solid var(--border);
        border-radius: 12px;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        user-select: none;
      }}
      .toggle input {{
        margin: 0;
      }}
      .worker-grid {{
        display: grid;
        gap: 1rem;
      }}
      details.worker-section {{
        border: 1px solid var(--border);
        border-radius: 16px;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        overflow: hidden;
      }}
      details.worker-section[open] {{
        box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
      }}
      summary.worker-summary {{
        list-style: none;
        cursor: pointer;
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 1rem;
        padding: 1rem 1.1rem;
        border-bottom: 1px solid var(--border);
      }}
      summary.worker-summary::-webkit-details-marker {{
        display: none;
      }}
      .worker-title {{
        display: grid;
        gap: .2rem;
      }}
      .worker-title h2 {{
        margin: 0;
        font-size: 1.05rem;
      }}
      .worker-title .meta {{
        color: var(--muted);
        font-size: .92rem;
      }}
      .worker-body {{
        display: grid;
        gap: .85rem;
        padding: 1rem 1.1rem 1.1rem;
      }}
      .metric-grid {{
        display: grid;
        gap: .75rem;
        grid-template-columns: repeat(auto-fill, minmax(15rem, 1fr));
      }}
      .cache-breakdown-grid {{
        display: grid;
        gap: .85rem;
        grid-template-columns: repeat(auto-fit, minmax(19rem, 1fr));
      }}
      .cache-breakdown-section {{
        display: grid;
        gap: .8rem;
      }}
      .cache-breakdown-section + .cache-breakdown-section {{
        margin-top: .3rem;
      }}
      .cache-breakdown-section .section-head {{
        display: grid;
        gap: .15rem;
      }}
      .cache-breakdown-section .section-head .title {{
        font-size: 1rem;
        font-weight: 700;
      }}
      .cache-breakdown-section .section-head .subtitle {{
        font-size: .84rem;
        line-height: 1.25;
        color: var(--muted);
      }}
      .stacked-chart-card {{
        display: grid;
        gap: .65rem;
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: .9rem .95rem 1rem;
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.98), rgba(20, 31, 51, 0.92));
        color: #f8fafc;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 8px 24px rgba(0, 0, 0, 0.18);
      }}
      .stacked-chart-card .stacked-chart-head {{
        display: grid;
        gap: .15rem;
      }}
      .stacked-chart-card .title {{
        font-size: 1rem;
        font-weight: 700;
      }}
      .stacked-chart-card .subtitle {{
        font-size: .82rem;
        line-height: 1.25;
        color: rgba(226, 232, 240, 0.76);
      }}
      .stacked-chart {{
        width: 100%;
        height: 220px;
      }}
      .metric-tile {{
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
      }}
      .metric-tile:hover {{
        transform: translateY(-1px);
        border-color: rgba(94, 234, 212, 0.55);
        box-shadow: 0 12px 28px rgba(0, 0, 0, 0.24);
      }}
      .metric-tile .tile-head {{
        display: flex;
        justify-content: space-between;
        gap: .75rem;
        align-items: flex-start;
      }}
      .metric-tile .label-block {{
        display: grid;
        gap: .15rem;
        min-width: 0;
        flex: 1 1 auto;
      }}
      .metric-tile .field {{
        font-size: 1rem;
        font-weight: 700;
        line-height: 1.15;
        word-break: break-word;
        min-width: 0;
        color: #f8fafc;
      }}
      .metric-tile .subtitle {{
        font-size: .8rem;
        line-height: 1.2;
        color: rgba(226, 232, 240, 0.76);
        word-break: break-word;
      }}
      .metric-tile .value {{
        font-size: 2rem;
        font-weight: 800;
        line-height: 1;
        white-space: nowrap;
        color: #f8fafc;
        text-shadow: 0 1px 0 rgba(0, 0, 0, 0.2);
      }}
      .metric-tile .sparkline-shell {{
        display: grid;
        grid-template-columns: auto 1fr;
        gap: .5rem;
        align-items: stretch;
        min-height: 8.5rem;
      }}
      .metric-tile .scale-y {{
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        color: rgba(226, 232, 240, 0.82);
        font-size: .82rem;
        line-height: 1;
        min-width: 3.3rem;
        padding: .2rem 0;
        font-variant-numeric: tabular-nums;
      }}
      .metric-tile .scale-y span {{
        white-space: nowrap;
      }}
      .metric-tile .sparkline {{
        flex: 1 1 auto;
        display: flex;
        align-items: center;
        min-width: 0;
      }}
      .metric-tile .sparkline svg {{
        width: 100%;
        height: 7.5rem;
        display: block;
      }}
      .metric-tile .stamp {{
        color: rgba(226, 232, 240, 0.72);
        font-size: .84rem;
        line-height: 1.2;
        font-variant-numeric: tabular-nums;
      }}
      @media (prefers-color-scheme: light) {{
        .stacked-chart-card {{
          background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(247, 250, 252, 0.94));
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 8px 24px rgba(15, 23, 42, 0.10);
          color: #172033;
        }}
        .stacked-chart-card .subtitle {{
          color: #52627a;
        }}
        .metric-tile {{
          background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(247, 250, 252, 0.94));
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 8px 24px rgba(15, 23, 42, 0.10);
          color: #172033;
        }}
        .metric-tile:hover {{
          border-color: rgba(37, 99, 235, 0.35);
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.12);
        }}
        .metric-tile .field,
        .metric-tile .value {{
          color: #172033;
          text-shadow: none;
        }}
        .metric-tile .subtitle,
        .metric-tile .scale-y,
        .metric-tile .stamp {{
          color: #52627a;
        }}
      }}
      .zoom-panel {{
        display: grid;
        gap: .5rem;
        border: 1px solid var(--border);
        border-radius: 16px;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        padding: .75rem;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04), 0 8px 24px rgba(0, 0, 0, 0.12);
      }}
      .zoom-panel .zoom-title {{
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        align-items: baseline;
        color: var(--text);
        font-size: .9rem;
      }}
      .zoom-panel .zoom-title span:last-child {{
        color: var(--muted);
      }}
      .zoom-chart {{
        width: 100%;
        height: 280px;
      }}
      .zoom-placeholder {{
        color: var(--muted);
        font-size: .92rem;
        padding: .5rem 0;
      }}
      .empty-state {{
        color: var(--muted);
        padding: 1rem 0;
      }}
      @media (prefers-color-scheme: light) {{
        .zoom-panel {{
          background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(247, 250, 252, 0.96));
          box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7), 0 8px 24px rgba(15, 23, 42, 0.08);
        }}
        .zoom-panel .zoom-title {{
          color: #172033;
        }}
        .zoom-panel .zoom-title span:last-child {{
          color: #52627a;
        }}
      }}
      @media (max-width: 900px) {{
        main {{ padding: .75rem; }}
        .cache-breakdown-grid {{ grid-template-columns: 1fr; }}
        .metric-grid {{ grid-template-columns: repeat(auto-fill, minmax(12rem, 1fr)); }}
        .stacked-chart {{ height: 220px; }}
        .zoom-chart {{ height: 260px; }}
      }}
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
    <script src="/static/observability.js?v={observability_js_version}"></script>
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
      <p class="subtle">Aggregated pubsub interest grouped by QID. The table shows total priority, active lease count, wants flags, and which owners currently hold interest.</p>
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
                <th class="mono">Leases</th>
                <th class="mono">Expires</th>
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

        function formatExpiry(value) {
          if (value == null || value === "") {
            return "Unknown";
          }
          const numeric = Number(value);
          if (!Number.isFinite(numeric)) {
            return escapeHtml(value);
          }
          const d = new Date(numeric * 1000);
          if (!Number.isFinite(d.getTime())) {
            return escapeHtml(value);
          }
          return d.toISOString().slice(0, 19) + "Z";
        }

        function renderExpiryRange(item) {
          const oldest = item.oldest_expires_at;
          const newest = item.newest_expires_at;
          if (oldest == null && newest == null) {
            return "Unknown";
          }
          if (oldest == null || newest == null || Number(oldest) === Number(newest)) {
            return formatExpiry(oldest ?? newest);
          }
          return `${formatExpiry(oldest)} → ${formatExpiry(newest)}`;
        }

        function renderSummary(data) {
          const items = Array.isArray(data.items) ? data.items : [];
          const totalLeases = items.reduce((acc, item) => acc + Number(item.lease_rows || item.session_rows || 0), 0);
          const totalPriority = items.reduce((acc, item) => acc + Number(item.total_priority || 0), 0);
          const totalWorkers = items.reduce((acc, item) => acc + Number(item.owner_count || 0), 0);
          const earliestExpiry = items
            .map((item) => Number(item.oldest_expires_at))
            .filter((value) => Number.isFinite(value) && value > 0)
            .sort((a, b) => a - b)[0];
          summary.innerHTML = [
            ["Items", items.length],
            ["Leases", totalLeases],
            ["Total priority", totalPriority],
            ["Workers", totalWorkers],
            ["Next expiry", earliestExpiry ? formatExpiry(earliestExpiry) : "Unknown"],
          ].map(([label, value]) => `<div class="card"><div class="label">${escapeHtml(label)}</div><div class="value mono">${escapeHtml(value)}</div></div>`).join("");
        }

        function matchesFilter(item, filterValue) {
          if (!filterValue) return true;
          const haystack = [
            item.qid,
            ...(Array.isArray(item.workers) ? item.workers.map((worker) => worker.worker_id) : []),
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
              chip("creation", !!item.wants_creation),
              chip("content", !!item.wants_content),
              chip("inlinks", !!item.wants_inlinks),
            ].join("");
            const workerIds = workers.map((worker) => worker.worker_id).filter(Boolean);
            const workerChips = workerIds.map((workerId) => chip(workerId, true)).join("");
            return `
              <tr>
                <td class="mono">${escapeHtml(item.qid)}</td>
                <td class="mono">${escapeHtml(item.total_priority || 0)}</td>
                <td class="mono">${escapeHtml(item.lease_rows || item.session_rows || 0)}</td>
                <td class="mono">${escapeHtml(renderExpiryRange(item))}</td>
                <td class="mono">${escapeHtml(item.owner_count || 0)}</td>
                <td><div class="chips">${wants}</div></td>
                <td><div class="chips">${workerChips || '<span class="mono">No workers</span>'}</div></td>
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
            rows.innerHTML = `<tr><td colspan="7">Unable to load pubsub debug data: ${escapeHtml(error.message)}</td></tr>`;
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


def _render_item_trace_html(item_trace_js_version: int) -> HTMLResponse:
    return HTMLResponse(
        content=f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="icon" href="/static/favicon.svg" type="image/svg+xml" />
    <link rel="icon" href="/static/favicon-32.png" type="image/png" sizes="32x32" />
    <link rel="icon" href="/static/favicon-16.png" type="image/png" sizes="16x16" />
    <link rel="shortcut icon" href="/favicon.ico" />
    <title>wd_notability item trace</title>
    <style>
      :root {{
        color-scheme: light dark;
        --bg: #f5f7fb;
        --panel: rgba(255, 255, 255, 0.88);
        --panel-strong: #ffffff;
        --text: #172033;
        --muted: #5c677d;
        --border: rgba(71, 85, 105, 0.2);
        --accent: #0f766e;
        --accent-2: #2563eb;
      }}
      @media (prefers-color-scheme: dark) {{
        :root {{
          --bg: #0b1220;
          --panel: rgba(15, 23, 42, 0.9);
          --panel-strong: #111827;
          --text: #e5eefc;
          --muted: #9ca9bf;
          --border: rgba(148, 163, 184, 0.22);
          --accent: #5eead4;
          --accent-2: #60a5fa;
        }}
      }}
      body {{
        margin: 0;
        min-height: 100vh;
        font-family: ui-sans-serif, system-ui, sans-serif;
        color: var(--text);
        background:
          radial-gradient(circle at top left, rgba(37, 99, 235, 0.16), transparent 26%),
          radial-gradient(circle at top right, rgba(15, 118, 110, 0.16), transparent 24%),
          linear-gradient(180deg, var(--bg), var(--bg));
      }}
      header {{
        padding: 1.25rem 1.5rem 1rem;
        border-bottom: 1px solid var(--border);
        background: linear-gradient(135deg, var(--panel-strong), var(--panel));
        backdrop-filter: blur(14px);
      }}
      h1 {{
        margin: 0;
        font-size: clamp(1.7rem, 4vw, 2.6rem);
        letter-spacing: -0.04em;
      }}
      .subtle {{
        margin: .45rem 0 0;
        color: var(--muted);
        max-width: 62rem;
      }}
      .nav {{
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-top: .8rem;
      }}
      .nav a {{ color: var(--accent-2); text-decoration: none; }}
      main {{
        padding: 1rem 1.5rem 2rem;
        display: grid;
        gap: 1rem;
      }}
      .panel {{
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1rem;
        box-shadow: 0 12px 34px rgba(15, 23, 42, 0.08);
        backdrop-filter: blur(10px);
      }}
      .toolbar {{
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        gap: .85rem 1rem;
        align-items: end;
      }}
      .controls {{
        display: flex;
        flex-wrap: wrap;
        gap: .65rem .75rem;
        align-items: end;
      }}
      .controls label {{
        display: grid;
        gap: .3rem;
        color: var(--muted);
        font-size: .88rem;
      }}
      .controls input[type="search"],
      .controls input[type="number"] {{
        min-width: min(100%, 18rem);
        padding: .72rem .85rem;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        font: inherit;
      }}
      .controls input[type="number"] {{
        min-width: 8rem;
      }}
      .controls button {{
        appearance: none;
        border: 1px solid rgba(37, 99, 235, 0.24);
        background: linear-gradient(180deg, rgba(37, 99, 235, 0.12), rgba(37, 99, 235, 0.06));
        color: var(--text);
        border-radius: 12px;
        padding: .72rem 1rem;
        font: inherit;
        font-weight: 650;
        cursor: pointer;
      }}
      .controls button:hover {{
        border-color: rgba(37, 99, 235, 0.45);
      }}
      .toggle {{
        display: inline-flex;
        gap: .45rem;
        align-items: center;
        color: var(--muted);
      }}
      .table-shell {{
        overflow-x: auto;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        min-width: 1100px;
      }}
      thead th {{
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
      }}
      tbody td {{
        border-bottom: 1px solid var(--border);
        padding: .75rem .6rem;
        vertical-align: top;
      }}
      tbody tr:hover {{
        background: rgba(15, 118, 110, 0.06);
      }}
      .mono {{
        font-variant-numeric: tabular-nums;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      }}
      .chip {{
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: .2rem .55rem;
        border: 1px solid var(--chip-border, var(--border));
        background: var(--chip-bg, rgba(148, 163, 184, 0.12));
        color: var(--chip-fg, var(--text));
        font-size: .82rem;
        white-space: nowrap;
      }}
      .chips {{
        display: flex;
        flex-wrap: wrap;
        gap: .35rem;
      }}
      .details {{
        max-width: 32rem;
        white-space: pre-wrap;
        word-break: break-word;
      }}
      .details a.qid-link {{
        color: var(--accent-2);
        text-decoration: none;
        border-bottom: 1px solid rgba(37, 99, 235, 0.28);
        transition: color 120ms ease, border-color 120ms ease, background-color 120ms ease;
      }}
      .details a.qid-link:hover {{
        color: var(--accent);
        border-bottom-color: rgba(15, 118, 110, 0.45);
        background: rgba(15, 118, 110, 0.08);
      }}
      .empty-state {{
        color: var(--muted);
        padding: 1rem 0 .2rem;
      }}
      @media (max-width: 900px) {{
        main {{ padding: .75rem; }}
        .controls input[type="search"] {{ min-width: 0; width: 100%; }}
      }}
    </style>
  </head>
  <body>
    <header>
      <h1>Item trace</h1>
      <p class="subtle">Chronological worker events. `t` is seconds since the first event in view, and `delta` is seconds since the previous event.</p>
      <nav class="nav">
        <a href="/observability">Observability</a>
        <a href="/pubsub">PubSub debugger</a>
        <a href="/help.md">Help</a>
        <a href="/">Item report</a>
      </nav>
    </header>
    <main>
      <section class="panel">
        <div class="toolbar">
          <div class="controls">
            <label for="qid">
              QID
              <input id="qid" type="search" placeholder="Q42" autocomplete="off" />
            </label>
            <label for="limit">
              Limit
              <input id="limit" type="number" min="1" step="1" value="200" />
            </label>
            <label for="cutoff">
              Cutoff (m)
              <input id="cutoff" type="number" min="1" step="1" value="5" />
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
        <div class="table-shell">
          <table>
            <thead>
              <tr>
                <th class="mono">t</th>
                <th class="mono">delta</th>
                <th>Timestamp</th>
                <th>QID</th>
                <th>Worker</th>
                <th>Event</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
        <div class="empty-state hidden" id="empty-state">No trace rows found.</div>
      </section>
    </main>
    <script src="/static/item-trace.js?v={item_trace_js_version}"></script>
  </body>
</html>
        """
    )

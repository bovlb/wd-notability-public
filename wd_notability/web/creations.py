from __future__ import annotations

import asyncio
import time

from fastapi.responses import HTMLResponse

from wd_notability.creations import CREATIONS
from wd_notability.evaluation_cache import CACHE

CREATOR_NAME_CACHE_TTL_SECONDS = 24 * 60 * 60
CREATOR_NAME_CACHE: dict[int, tuple[str, float]] = {}
CREATOR_NAME_CACHE_LOCK = asyncio.Lock()


async def lookup_creator_names(actor_ids: list[int]) -> dict[int, str]:
    unique_ids: list[int] = []
    seen: set[int] = set()
    now = time.monotonic()
    for actor_id in actor_ids:
        if actor_id in seen:
            continue
        seen.add(actor_id)
        cached = CREATOR_NAME_CACHE.get(actor_id)
        if cached is not None and cached[1] > now:
            continue
        unique_ids.append(actor_id)

    if unique_ids:
        fresh_names = await asyncio.to_thread(CREATIONS.lookup_actor_names, unique_ids)
        expires_at = time.monotonic() + CREATOR_NAME_CACHE_TTL_SECONDS
        async with CREATOR_NAME_CACHE_LOCK:
            for actor_id, actor_name in fresh_names.items():
                CREATOR_NAME_CACHE[actor_id] = (actor_name, expires_at)

    resolved: dict[int, str] = {}
    for actor_id in actor_ids:
        cached = CREATOR_NAME_CACHE.get(actor_id)
        if cached is None:
            continue
        if cached[1] <= time.monotonic():
            continue
        resolved[actor_id] = cached[0]
    return resolved


async def resolve_creation_metadata(qids: list[str]) -> dict[str, dict[str, object]]:
    metadata = await CACHE.get_creation_metadata_many(qids)
    if not metadata:
        return {}

    creator_names_by_id = await lookup_creator_names([row.creator_actor_id for row in metadata.values()])
    resolved: dict[str, dict[str, object]] = {}
    for qid, row in metadata.items():
        resolved[qid] = {
            "creator": creator_names_by_id.get(row.creator_actor_id, "Unknown creator"),
            "creation_time": row.creation_time,
        }
    return resolved


def render_creations_dashboard_html() -> HTMLResponse:
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


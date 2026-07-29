from __future__ import annotations

import json
from urllib.parse import urlencode

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

import server.app as app_module

router = APIRouter()


def _report_problem_url(qid: str) -> str:
    return (
        "https://www.wikidata.org/w/index.php?"
        + urlencode(
            {
                "title": "User_talk:Bovlb/wd-notability",
                "action": "edit",
                "section": "new",
                "dtpreload": "1",
                "preloadtitle": f"Problem with {qid}",
                "preload": "User:Bovlb/wd-notability/report-preload",
            }
        )
    )


@router.get("/", response_class=HTMLResponse)
async def ui_home(qid: str = Query(default="")):
    error: str | None = None
    should_load = False

    qid = qid.strip().upper()
    if qid and app_module._is_valid_qid(qid):
        should_load = True
    elif qid:
        error = "400: qid must look like Q42"

    escaped_qid = app_module.escape(qid)
    cache_badge_html = app_module._render_report_badge(None, qid, "cache")
    live_badge_html = app_module._render_report_badge(None, qid, "live")
    item_link_html = app_module._item_link_html(qid) if should_load else ""
    api_link_html = f"/api/items/{escaped_qid}/signals" if should_load else ""
    trace_link_html = f"/item-trace?qid={escaped_qid}" if should_load and app_module.ITEM_TRACE_ENABLED else ""
    report_problem_url = _report_problem_url(qid) if should_load else ""
    report_html = "<p class='status' id='evaluation-status'>Enter a QID to evaluate an item.</p>"

    if error:
        report_html = f"<p class='error'>{app_module.escape(error)}</p>"
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
        --level-partial-label: #b04d1a;
        --level-partial-weak-second: #c05d00;
        --level-partial-strong-second: #1b7f2a;
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
          --level-none: #ff3b30;
          --level-weak: #ffd166;
          --level-strong: #81c995;
          --level-partial-label: #ff8c68;
          --level-partial-weak-second: #ffb566;
          --level-partial-strong-second: #8bd98f;
        }}
      }}
      body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem; background: var(--bg); color: var(--text); }}
      a {{ color: var(--link); }}
      .report-header {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 1.5rem; margin-bottom: .25rem; }}
      .report-title {{ min-width: 0; display: flex; flex-direction: column; gap: .35rem; }}
      .report-header h1 {{ margin: 0; }}
      .report-badge-grid {{ display: grid; gap: 1rem; grid-template-columns: repeat(2, minmax(12rem, 1fr)); align-items: start; }}
      .report-badge-card {{ display: flex; flex-direction: column; align-items: center; gap: .5rem; padding: .75rem; border: 1px solid var(--border); border-radius: 12px; background: var(--panel, transparent); }}
      .report-badge-card h2 {{ margin: 0; font-size: 1rem; }}
      .report-badge-link {{ display: inline-flex; flex: 0 0 auto; border-radius: 6px; position: relative; overflow: visible; }}
      .report-badge-link:focus-visible {{ outline: 2px solid var(--link); outline-offset: 4px; }}
      .report-badge {{ width: 9.5rem; height: 9.5rem; flex: 0 0 auto; }}
      .notability-badge-hovercard {{
        position: absolute;
        left: 50%;
        top: calc(100% + 8px);
        transform: translateX(-50%) translateY(4px);
        min-width: 17rem;
        max-width: min(22rem, 90vw);
        padding: .6rem .7rem;
        border: 1px solid var(--border);
        border-radius: 12px;
        background: var(--bg);
        color: var(--text);
        box-shadow: 0 12px 30px rgba(0, 0, 0, .22);
        opacity: 0;
        visibility: hidden;
        pointer-events: none;
        transition: opacity .12s ease, transform .12s ease, visibility .12s ease;
        z-index: 20;
      }}
      .report-badge-link:hover .notability-badge-hovercard,
      .report-badge-link:focus-visible .notability-badge-hovercard,
      .report-badge-link:focus-within .notability-badge-hovercard {{
        opacity: 1;
        visibility: visible;
        transform: translateX(-50%) translateY(0);
      }}
      .notability-badge-hovercard-body {{
        display: flex;
        flex-direction: column;
        gap: .14rem;
      }}
      .notability-badge-hovercard-row {{
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: .75rem;
        padding-left: calc(var(--badge-hovercard-depth, 0) * .85rem);
        line-height: 1.1;
      }}
      .notability-badge-hovercard-label {{
        font-size: .84rem;
        font-weight: 600;
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: .08rem;
      }}
      .notability-badge-hovercard-label-title {{ line-height: 1.05; }}
      .notability-badge-hovercard-label-subtitle {{
        font-size: .66rem;
        font-weight: 700;
        letter-spacing: .08em;
        line-height: 1;
        text-transform: uppercase;
        color: var(--muted, #666);
      }}
      .notability-badge-hovercard-value {{
        font-size: .84rem;
        font-weight: 700;
        text-align: right;
        white-space: nowrap;
      }}
      .notability-badge-hovercard-value.level-none {{ color: var(--level-none); }}
      .notability-badge-hovercard-value.level-weak {{ color: var(--level-weak); }}
      .notability-badge-hovercard-value.level-strong {{ color: var(--level-strong); }}
      .notability-badge-hovercard-value.level-unknown {{ color: var(--muted, #666); }}
      .notability-badge-hovercard-value .level-partial-prefix,
      .level-partial-prefix {{ color: var(--level-none); font-weight: 700; }}
      .notability-badge-hovercard-value .level-partial-weak .level-weak,
      .level-partial-weak .level-weak {{ color: var(--level-weak); }}
      .notability-badge-hovercard-value .level-partial-strong .level-strong,
      .level-partial-strong .level-strong {{ color: var(--level-strong); }}
      .report-form {{ display: flex; gap: .75rem; align-items: center; flex-wrap: wrap; margin: 0; }}
      input[type=text] {{ padding: .5rem .6rem; min-width: 14rem; background: var(--control-bg); color: var(--control-text); border: 1px solid var(--border); }}
      button {{ padding: .5rem .8rem; background: var(--control-bg); color: var(--control-text); border: 1px solid var(--border); }}
      .status {{ color: var(--text); }}
      .comparison-grid {{ display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(24rem, 1fr)); margin-bottom: 1rem; }}
      .comparison-card {{ border: 1px solid var(--border); border-radius: 12px; padding: 1rem; background: var(--panel, transparent); }}
      .comparison-card h2 {{ margin-top: 0; }}
      .criterion-section {{ display: block; margin: 0 0 1rem; padding: .75rem; border: 1px solid var(--border); border-radius: 10px; background: var(--panel, transparent); }}
      .criterion-section > summary {{ cursor: pointer; font-weight: 700; }}
      .criterion-section[open] > summary {{ margin-bottom: .75rem; }}
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
      .links {{ display: flex; gap: .75rem; align-items: center; margin: 0; flex-wrap: wrap; }}
      .links button {{
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: .45rem .7rem;
        background: linear-gradient(180deg, var(--panel-strong), var(--panel));
        color: var(--text);
        cursor: pointer;
      }}
            .level-none {{ color: var(--level-none); font-weight: 600; }}
            .level-unknown {{ color: var(--muted, #666); font-weight: 600; }}
            .level-weak {{ color: var(--level-weak); font-weight: 600; }}
            .level-strong {{ color: var(--level-strong); font-weight: 700; }}
            .level-partial-weak,
            .level-partial-strong {{ font-weight: 700; }}
            .level-partial-prefix {{ color: var(--level-none); font-weight: 700; }}
            .level-partial-weak .level-weak {{ color: var(--level-weak); }}
            .level-partial-strong .level-strong {{ color: var(--level-strong); }}
    </style>
  </head>
  <body>
    <div class=\"report-header\">
      <div class=\"report-title\">
        <h1>Wikidata Notability Signal Report</h1>
        {f'<div class="item-link">Item: {item_link_html}</div>' if item_link_html else ''}
        <form class=\"report-form\" method=\"get\" action=\"/\">
          <label>QID <input type=\"text\" name=\"qid\" value=\"{escaped_qid}\" placeholder=\"Q42\" /></label>
          <button type=\"submit\">Evaluate</button>
        </form>
        <div class=\"links\">
          <a href=\"/help.md\">Help</a>
          {f'<a href="{api_link_html}">API</a>' if api_link_html else ''}
          {f'<a href="{trace_link_html}">Trace</a>' if trace_link_html else ''}
          {f'<a id="report-problem-link" href="{report_problem_url}" target="_blank" rel="noopener noreferrer">Report a problem</a>' if report_problem_url else ''}
          {f'<button id="copy-report-details-button" type="button">Copy details to clipboard</button>' if report_problem_url else ''}
        </div>
      </div>
      <div class=\"report-badge-grid\" aria-label=\"Cache and live report badges\">
        <section class=\"report-badge-card\" data-badge-role=\"cache\">
          <h2>Cache</h2>
          <a class=\"report-badge-link\" href=\"/badge.md\" aria-label=\"Open badge help\">
            {cache_badge_html}
          </a>
        </section>
        <section class=\"report-badge-card\" data-badge-role=\"live\">
          <h2>Live</h2>
          <a class=\"report-badge-link\" href=\"/badge.md\" aria-label=\"Open badge help\">
            {live_badge_html}
          </a>
        </section>
      </div>
    </div>
    {report_html}
    <script>
      const evaluationQid = {json.dumps(qid if should_load else "")};
      const reportProblemBaseUrl = "https://www.wikidata.org/w/index.php";
      const reportProblemTitle = "User_talk:Bovlb/wd-notability";
      function badgeLevel(data, field, levelKey) {{
        if (data && Object.prototype.hasOwnProperty.call(data, field)) {{
          return String(data[field] == null ? "unknown" : data[field]).toUpperCase();
        }}
        if (data && data.levels && levelKey && Object.prototype.hasOwnProperty.call(data.levels, levelKey)) {{
          return String(data.levels[levelKey] == null ? "unknown" : data.levels[levelKey]).toUpperCase();
        }}
        return "UNKNOWN";
      }}
      function updateBadge(badge, report) {{
        if (!badge || !report) return;
        const svg = badge.querySelector(".report-badge");
        const levels = report.levels || {{}};
        const levelKeys = {{
          n: "N",
          n_ring: "N",
          n1: "N1",
          n2a: "N2a",
          n2b: "N2b",
          n3: "N3",
          n3_halves: "N3",
          n3_inlinks: "N3_inlinks",
          n3_osm: "N3_osm",
          n3_wikisub: "N3_wikisub",
          n3_sdc: "N3_sdc",
        }};
        const data = {{
          redirect: report.is_redirect,
          has_claims_count: report.has_claims_count,
          has_sitelinks_count: report.has_sitelinks_count,
          is_deleted: report.is_deleted,
          levels,
        }};
        if (svg) {{
          svg.setAttribute("data-deleted", report.is_deleted ? "true" : "false");
          svg.setAttribute("data-has-inlinks", Number(report.inlinks_count) > 0 ? "true" : "false");
        }}
        for (const field of ["n", "n_ring", "n1", "n2a", "n2b", "n3", "n3_halves", "n3_inlinks", "n3_osm", "n3_wikisub", "n3_sdc", "redirect", "has_claims", "is_deleted"]) {{
          const el = svg ? svg.querySelector(`[data-field="${{field}}"]`) : null;
          if (!el) continue;
          if (field === "has_claims" && (badgeLevel(data, "n2a", "N2a") === "UNKNOWN" || badgeLevel(data, "n2b", "N2b") === "UNKNOWN")) {{
            el.setAttribute("data-value", "unknown");
          }} else if (field === "has_claims") {{
            const numeric = Number(data.has_claims_count);
            if (!Number.isFinite(numeric)) {{
              el.setAttribute("data-value", "unknown");
            }} else {{
              el.setAttribute("data-value", numeric > 0 ? "true" : "false");
            }}
          }} else if (Object.prototype.hasOwnProperty.call(levelKeys, field)) {{
            el.setAttribute("data-value", levels[levelKeys[field]] == null ? "unknown" : String(levels[levelKeys[field]]));
          }} else {{
            el.setAttribute("data-value", data[field] == null ? "unknown" : String(data[field]));
          }}
        }}
        const tooltip = typeof report.badge_tooltip === "string" && report.badge_tooltip
          ? report.badge_tooltip
          : "";
        badge.setAttribute("aria-label", tooltip);
        const hovercard = badge.querySelector(".notability-badge-hovercard");
        if (hovercard) {{
          hovercard.innerHTML = typeof report.badge_hovercard === "string"
            ? report.badge_hovercard
            : "";
        }}
      }}
      function renderBadgeFromReport(report) {{
        const cacheBadge = document.querySelector('[data-badge-role="cache"] .report-badge-link');
        const liveBadge = document.querySelector('[data-badge-role="live"] .report-badge-link');
        const cacheReport = report && report.cached_snapshot ? report.cached_snapshot : report;
        updateBadge(cacheBadge, cacheReport);
        updateBadge(liveBadge, report);
      }}
      function buildReportProblemTitle(report) {{
        const qid = String(report && report.qid ? report.qid : evaluationQid || "").trim().toUpperCase();
        return qid ? `Problem with ${{qid}}` : "Problem with wd-notability";
      }}
      function buildReportProblemSummary(report) {{
        const levels = report && report.levels ? report.levels : {{}};
        const parts = [
          `overall ${{String(levels.N ?? "unknown").toUpperCase()}}`,
          `N12 ${{String(levels.N12 ?? "unknown").toUpperCase()}}`,
          `N1 ${{String(levels.N1 ?? "unknown").toUpperCase()}}`,
          `N2 ${{String(levels.N2 ?? "unknown").toUpperCase()}}`,
          `N2a ${{String(levels.N2a ?? "unknown").toUpperCase()}}`,
          `N2b ${{String(levels.N2b ?? "unknown").toUpperCase()}}`,
          `N3 ${{String(levels.N3 ?? "unknown").toUpperCase()}}`,
          `N3_inlinks ${{String(levels.N3_inlinks ?? "unknown").toUpperCase()}}`,
          `N3_osm ${{String(levels.N3_osm ?? "unknown").toUpperCase()}}`,
          `N3_wikisub ${{String(levels.N3_wikisub ?? "unknown").toUpperCase()}}`,
          `N3_sdc ${{String(levels.N3_sdc ?? "unknown").toUpperCase()}}`,
        ];
        if (report && report.is_redirect) {{
          parts.push("redirect");
        }}
        if (report && report.is_deleted) {{
          parts.push("deleted");
        }}
        if (report && report.content_stale) {{
          parts.push("stale");
        }}
        return parts.join("; ");
      }}
      function buildReportProblemText(report) {{
        const qid = String(report && report.qid ? report.qid : evaluationQid || "").trim().toUpperCase();
        const reportUrl = window.location.href;
        const reportDate = new Date().toISOString();
        const summary = buildReportProblemSummary(report);
        return [
          `* Item: {{{{Q|${{qid || "UNKNOWN"}}}}}}`,
          `* URL: ${{reportUrl}}`,
          `* Date: ${{reportDate}}`,
          `* Summary: ${{summary || "UNKNOWN"}}`,
        ].join("\\n");
      }}
      function buildReportProblemPreloadParams(report) {{
        const qid = String(report && report.qid ? report.qid : evaluationQid || "").trim().toUpperCase();
        const reportUrl = window.location.href;
        const reportDate = new Date().toISOString();
        const summary = buildReportProblemSummary(report);
        return [qid, reportUrl, reportDate, summary];
      }}
      function updateReportProblemLink(report) {{
        const link = document.getElementById("report-problem-link");
        if (!link) return;
        const params = new URLSearchParams();
        params.set("title", reportProblemTitle);
        params.set("action", "edit");
        params.set("section", "new");
        params.set("dtpreload", "1");
        params.set("preloadtitle", buildReportProblemTitle(report));
        params.set("preload", "User:Bovlb/wd-notability/report-preload");
        for (const value of buildReportProblemPreloadParams(report)) {{
          params.append("preloadparams[]", value);
        }}
        link.href = `${{reportProblemBaseUrl}}?${{params.toString()}}`;
      }}
      async function copyReportProblemText(report) {{
        const text = buildReportProblemText(report);
        if (navigator.clipboard && navigator.clipboard.writeText) {{
          await navigator.clipboard.writeText(text);
          return;
        }}
        const textarea = document.createElement("textarea");
        textarea.value = text;
        textarea.style.position = "fixed";
        textarea.style.opacity = "0";
        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        document.execCommand("copy");
        textarea.remove();
      }}
      function bindReportProblemCopy(report) {{
        const button = document.getElementById("copy-report-details-button");
        if (!button) return;
        button.addEventListener("click", async () => {{
          const originalText = button.textContent;
          try {{
            await copyReportProblemText(report);
            button.textContent = "Copied";
            window.setTimeout(() => {{
              button.textContent = originalText;
            }}, 1500);
          }} catch (error) {{
            button.textContent = "Copy failed";
            window.setTimeout(() => {{
              button.textContent = originalText;
            }}, 1500);
          }}
        }});
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
          updateReportProblemLink(report);
          bindReportProblemCopy(report);
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
""")

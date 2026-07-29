(function () {
  "use strict";

  const AUTO_REFRESH_MS = 15000;
  const DEFAULT_LIMIT = 200;
  const DEFAULT_CUTOFF_MINUTES = 5;
  const WORKER_STYLES = {
    content: {
      bg: "rgba(37, 99, 235, 0.12)",
      border: "rgba(37, 99, 235, 0.3)",
      fg: "#2563eb",
    },
    inlinks: {
      bg: "rgba(15, 118, 110, 0.12)",
      border: "rgba(15, 118, 110, 0.3)",
      fg: "#0f766e",
    },
    pubsub: {
      bg: "rgba(138, 90, 0, 0.12)",
      border: "rgba(138, 90, 0, 0.28)",
      fg: "#8a5a00",
    },
    recent_changes: {
      bg: "rgba(124, 58, 237, 0.12)",
      border: "rgba(124, 58, 237, 0.3)",
      fg: "#7c3aed",
    },
    report: {
      bg: "rgba(100, 116, 139, 0.12)",
      border: "rgba(100, 116, 139, 0.3)",
      fg: "#475569",
    },
  };
  const EVENT_STYLES = {
    interest_added: {
      bg: "rgba(37, 99, 235, 0.12)",
      border: "rgba(37, 99, 235, 0.3)",
      fg: "#2563eb",
    },
    interest_started: {
      bg: "rgba(37, 99, 235, 0.12)",
      border: "rgba(37, 99, 235, 0.3)",
      fg: "#2563eb",
    },
    interest_removed: {
      bg: "rgba(100, 116, 139, 0.12)",
      border: "rgba(100, 116, 139, 0.3)",
      fg: "#475569",
    },
    interest_expired: {
      bg: "rgba(15, 118, 110, 0.12)",
      border: "rgba(15, 118, 110, 0.3)",
      fg: "#0f766e",
    },
    interest_published: {
      bg: "rgba(15, 118, 110, 0.12)",
      border: "rgba(15, 118, 110, 0.3)",
      fg: "#0f766e",
    },
    work_claimed: {
      bg: "rgba(245, 158, 11, 0.14)",
      border: "rgba(245, 158, 11, 0.32)",
      fg: "#b45309",
    },
    count_fetched: {
      bg: "rgba(20, 184, 166, 0.14)",
      border: "rgba(20, 184, 166, 0.32)",
      fg: "#0f766e",
    },
    graph_fetched: {
      bg: "rgba(34, 197, 94, 0.14)",
      border: "rgba(34, 197, 94, 0.32)",
      fg: "#15803d",
    },
    evaluation_attempted: {
      bg: "rgba(139, 92, 246, 0.14)",
      border: "rgba(139, 92, 246, 0.32)",
      fg: "#7c3aed",
    },
    results_written: {
      bg: "rgba(16, 185, 129, 0.14)",
      border: "rgba(16, 185, 129, 0.32)",
      fg: "#047857",
    },
    work_abandoned: {
      bg: "rgba(220, 38, 38, 0.12)",
      border: "rgba(220, 38, 38, 0.32)",
      fg: "#b91c1c",
    },
  };

  let refreshTimer = null;
  let latestRows = [];

  function qs(id) {
    return document.getElementById(id);
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function chipStyle(style) {
    return `--chip-bg:${style.bg};--chip-border:${style.border};--chip-fg:${style.fg};`;
  }

  function chip(label, style) {
    const inlineStyle = style ? ` style="${chipStyle(style)}"` : "";
    return `<span class="chip"${inlineStyle}>${escapeHtml(label)}</span>`;
  }

  function formatSeconds(value) {
    if (!Number.isFinite(value)) {
      return "—";
    }
    const rounded = Math.round(value * 1000) / 1000;
    return String(rounded).replace(/\.?0+$/, "");
  }

  function parseTimestamp(value) {
    if (typeof value !== "string") {
      return Number.NaN;
    }
    const millis = Date.parse(value);
    return Number.isFinite(millis) ? millis : Number.NaN;
  }

  function normalizeDetails(details) {
    if (details == null) {
      return {};
    }
    if (typeof details === "object" && !Array.isArray(details)) {
      return details;
    }
    return { value: details };
  }

  function itemTraceLink(qid) {
    const safeQid = String(qid || "").trim().toUpperCase();
    if (!/^Q\d+$/.test(safeQid)) {
      return escapeHtml(String(qid || ""));
    }
    const href = `/item-trace?qid=${encodeURIComponent(safeQid)}`;
    return `<a class="qid-link" href="${href}">${escapeHtml(safeQid)}</a>`;
  }

  function renderDetailsHtml(details, currentQid) {
    const current = String(currentQid || "").trim().toUpperCase();
    const jsonText = JSON.stringify(details, null, 2) || "{}";
    const escaped = escapeHtml(jsonText);
    return escaped.replace(/\bQ\d+\b/g, (match) => (
      match === current ? match : itemTraceLink(match)
    ));
  }

  function styleForWorker(workerName) {
    return WORKER_STYLES[String(workerName || "").toLowerCase()] || WORKER_STYLES.report;
  }

  function styleForEvent(eventType) {
    return EVENT_STYLES[String(eventType || "").toLowerCase()] || EVENT_STYLES.work_claimed;
  }

  function readFiltersFromLocation() {
    const params = new URLSearchParams(window.location.search);
    const qid = qs("qid");
    const limit = qs("limit");
    const cutoff = qs("cutoff");
    if (qid && params.has("qid")) {
      qid.value = params.get("qid") || "";
    }
    if (limit && params.has("limit")) {
      const parsed = Number(params.get("limit"));
      if (Number.isFinite(parsed) && parsed > 0) {
        limit.value = String(Math.trunc(parsed));
      }
    }
    if (cutoff && params.has("cutoff")) {
      const parsed = Number(params.get("cutoff"));
      if (Number.isFinite(parsed) && parsed > 0) {
        cutoff.value = String(Math.trunc(parsed));
      }
    }
  }

  function readPositiveIntegerInput(id, fallback) {
    const element = qs(id);
    if (!element) {
      return fallback;
    }
    const parsed = Number(element.value);
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.trunc(parsed);
    }
    return fallback;
  }

  function updateLocation() {
    const params = new URLSearchParams();
    const qid = qs("qid");
    const limit = readPositiveIntegerInput("limit", DEFAULT_LIMIT);
    const cutoff = readPositiveIntegerInput("cutoff", DEFAULT_CUTOFF_MINUTES);
    if (qid && qid.value.trim()) {
      params.set("qid", qid.value.trim());
    }
    params.set("limit", String(limit));
    params.set("cutoff", String(cutoff));
    const query = params.toString();
    const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}`;
    window.history.replaceState({}, "", nextUrl);
  }

  function clearRefreshTimer() {
    if (refreshTimer) {
      clearTimeout(refreshTimer);
      refreshTimer = null;
    }
  }

  function scheduleRefresh() {
    clearRefreshTimer();
    const autorefresh = qs("autorefresh");
    if (!autorefresh || !autorefresh.checked) {
      return;
    }
    refreshTimer = setTimeout(async () => {
      await refresh();
      scheduleRefresh();
    }, AUTO_REFRESH_MS);
  }

  function renderRows(items) {
    const rows = qs("rows");
    const emptyState = qs("empty-state");
    const normalized = Array.isArray(items) ? items : [];
    const parsed = normalized
      .map((item) => ({
        ...item,
        _ts: parseTimestamp(item.timestamp),
      }))
      .filter((item) => Number.isFinite(item._ts))
      .sort((left, right) => left._ts - right._ts);

    latestRows = parsed;

    if (!parsed.length) {
      rows.innerHTML = "";
      emptyState.classList.remove("hidden");
      return;
    }

    emptyState.classList.add("hidden");
    const firstTs = parsed[0]._ts;
    let previousTs = null;
    rows.innerHTML = parsed.map((item) => {
      const tSeconds = (item._ts - firstTs) / 1000;
      const deltaSeconds = previousTs == null ? null : (item._ts - previousTs) / 1000;
      previousTs = item._ts;
      const details = normalizeDetails(item.details);
      return `
        <tr>
          <td class="mono">${escapeHtml(formatSeconds(tSeconds))}</td>
          <td class="mono">${escapeHtml(deltaSeconds == null ? "—" : formatSeconds(deltaSeconds))}</td>
          <td class="mono">${escapeHtml(item.timestamp || "")}</td>
          <td class="mono">${escapeHtml(item.qid || "")}</td>
          <td><div class="chips">${chip(item.worker_name || "unknown", styleForWorker(item.worker_name))}</div></td>
          <td><div class="chips">${chip(item.event_type || "unknown", styleForEvent(item.event_type))}</div></td>
          <td><div class="details mono">${renderDetailsHtml(details, item.qid)}</div></td>
        </tr>
      `;
    }).join("");
  }

  function renderError(message) {
    const rows = qs("rows");
    const emptyState = qs("empty-state");
    rows.innerHTML = `<tr><td colspan="7">${escapeHtml(message)}</td></tr>`;
    emptyState.classList.add("hidden");
  }

  async function refresh() {
    const qid = qs("qid");
    const limit = readPositiveIntegerInput("limit", DEFAULT_LIMIT);
    const cutoffMinutes = readPositiveIntegerInput("cutoff", DEFAULT_CUTOFF_MINUTES);
    const params = new URLSearchParams();
    if (qid && qid.value.trim()) {
      params.set("qid", qid.value.trim());
    }
    params.set("limit", String(limit));
    params.set("since", String(Math.max(0, Math.floor(Date.now() / 1000) - (cutoffMinutes * 60))));
    updateLocation();

    try {
      const response = await fetch(`/api/item-trace?${params.toString()}`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const payload = await response.json();
      renderRows(payload.items);
    } catch (error) {
      renderError(`Unable to load item trace data: ${error.message}`);
    }
  }

  function bind() {
    readFiltersFromLocation();

    const qid = qs("qid");
    const limit = qs("limit");
    const cutoff = qs("cutoff");
    const refreshButton = qs("refresh");
    const autorefresh = qs("autorefresh");

    if (qid) {
      qid.addEventListener("input", () => {
        updateLocation();
        refresh();
        scheduleRefresh();
      });
    }
    if (limit) {
      limit.addEventListener("input", () => {
        updateLocation();
        refresh();
        scheduleRefresh();
      });
    }
    if (cutoff) {
      cutoff.addEventListener("input", () => {
        updateLocation();
        refresh();
        scheduleRefresh();
      });
    }
    if (refreshButton) {
      refreshButton.addEventListener("click", async () => {
        await refresh();
        scheduleRefresh();
      });
    }
    if (autorefresh) {
      autorefresh.addEventListener("change", () => {
        scheduleRefresh();
      });
    }
    window.addEventListener("beforeunload", clearRefreshTimer);

    refresh();
    scheduleRefresh();
  }

  document.addEventListener("DOMContentLoaded", bind);
})();

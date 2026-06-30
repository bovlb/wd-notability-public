(function () {
  "use strict";

  const BUCKET_ORDER = ["DELETED", "REDIRECT", "EMPTY", "NONE", "WEAK", "STRONG", "UNKNOWN"];
  const BUCKET_CLASSES = {
    DELETED: "bucket-deleted",
    REDIRECT: "bucket-redirect",
    EMPTY: "bucket-empty",
    NONE: "bucket-none",
    WEAK: "bucket-weak",
    STRONG: "bucket-strong",
    UNKNOWN: "bucket-unknown",
  };

  const DEFAULT_WINDOW_DAYS = 1;
  const DEFAULT_BUCKET_SORT = "time_desc";
  const LEVEL_RANK = {
    NONE: 0,
    WEAK: 1,
    UNKNOWN: 2,
    STRONG: 3,
  };
  const state = {
    population: [],
    evaluations: new Map(),
    subscriptionId: null,
    eventSource: null,
    currentQuery: null,
    loadGeneration: 0,
  };
  let teardownHandlersInstalled = false;

  function qs(id) {
    return document.getElementById(id);
  }

  function isoNow() {
    return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  }

  function daysAgoIso(days) {
    const d = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    return d.toISOString().replace(/\.\d{3}Z$/, "Z");
  }

  function parsePositiveInt(value) {
    const text = String(value ?? "").trim();
    if (!text) return 0;
    const parsed = Number.parseInt(text, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
  }

  function parseDurationToMs(value) {
    const text = String(value || "").trim().toLowerCase();
    const match = text.match(/^(\d+(?:\.\d+)?)([smhdw])$/);
    if (!match) return null;

    const amount = Number(match[1]);
    if (!Number.isFinite(amount)) return null;

    const unitMs = {
      s: 1000,
      m: 60 * 1000,
      h: 60 * 60 * 1000,
      d: 24 * 60 * 60 * 1000,
      w: 7 * 24 * 60 * 60 * 1000,
    }[match[2]];

    return unitMs ? amount * unitMs : null;
  }

  function resolvePopulationWindow(params) {
    const endText = String(params.end || "").trim() || isoNow();
    const parsedEnd = asUtcDate(endText) || new Date(endText);
    const endDate = Number.isFinite(parsedEnd.getTime()) ? parsedEnd : new Date();

    const startText = String(params.start || "").trim();
    const durationMs = parseDurationToMs(startText);
    const parsedStart = durationMs != null
      ? new Date(endDate.getTime() - durationMs)
      : (asUtcDate(startText) || new Date(startText));
    const startDate = Number.isFinite(parsedStart.getTime()) ? parsedStart : null;

    return {
      start: startDate ? startDate.toISOString().replace(/\.\d{3}Z$/, "Z") : startText,
      end: endDate.toISOString().replace(/\.\d{3}Z$/, "Z"),
    };
  }

  function parseParams() {
    const params = new URLSearchParams(window.location.search);
    const creators = [];
    for (const value of params.getAll("creator")) {
      if (value) creators.push(value);
    }
    const creatorsCsv = params.get("creators");
    if (creatorsCsv) {
      for (const part of creatorsCsv.split(",")) {
        const value = part.trim();
        if (value) creators.push(value);
      }
    }
    const dedupedCreators = Array.from(new Set(creators));
    return {
      start: params.get("start") || daysAgoIso(DEFAULT_WINDOW_DAYS),
      end: params.get("end") || isoNow(),
      creators: dedupedCreators,
      group_by: params.get("group_by") || "",
      bucket_sort: params.get("bucket_sort") || DEFAULT_BUCKET_SORT,
      min_user_items: parsePositiveInt(params.get("min_user_items")),
      aggregate_temporary_users: params.get("aggregate_temporary_users") === "1",
      allow_either_n2: params.get("allow_either_n2") === "1",
    };
  }

  function syncControls(params) {
    qs("start").value = params.start;
    qs("end").value = params.end;
    qs("creators").value = params.creators.join(", ");
    qs("group_by").value = params.group_by || "";
    qs("bucket_sort").value = params.bucket_sort || DEFAULT_BUCKET_SORT;
    qs("min_user_items").value = params.min_user_items > 0 ? String(params.min_user_items) : "";
    qs("aggregate_temporary_users").checked = Boolean(params.aggregate_temporary_users);
    qs("allow_either_n2").checked = Boolean(params.allow_either_n2);
  }

  function readPopulationControls() {
    return {
      start: qs("start").value.trim(),
      end: qs("end").value.trim(),
      creators: qs("creators").value
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean),
    };
  }

  function readDisplayControls() {
    return {
      group_by: qs("group_by").value || "",
      bucket_sort: qs("bucket_sort").value || DEFAULT_BUCKET_SORT,
      min_user_items: parsePositiveInt(qs("min_user_items").value),
      aggregate_temporary_users: Boolean(qs("aggregate_temporary_users").checked),
      allow_either_n2: Boolean(qs("allow_either_n2").checked),
    };
  }

  function setStateFromControls() {
    state.currentQuery = readPopulationControls();
    updateUrl(state.currentQuery);
  }

  function updateUrl(params) {
    const url = new URL(window.location.href);
    url.searchParams.set("start", params.start);
    url.searchParams.set("end", params.end);
    if (params.creators.length) {
      url.searchParams.set("creators", params.creators.join(","));
    } else {
      url.searchParams.delete("creators");
    }
    if (params.group_by) {
      url.searchParams.set("group_by", params.group_by);
    } else {
      url.searchParams.delete("group_by");
    }
    if (params.bucket_sort && params.bucket_sort !== DEFAULT_BUCKET_SORT) {
      url.searchParams.set("bucket_sort", params.bucket_sort);
    } else {
      url.searchParams.delete("bucket_sort");
    }
    if (params.min_user_items && params.min_user_items > 0) {
      url.searchParams.set("min_user_items", String(params.min_user_items));
    } else {
      url.searchParams.delete("min_user_items");
    }
    if (params.aggregate_temporary_users) {
      url.searchParams.set("aggregate_temporary_users", "1");
    } else {
      url.searchParams.delete("aggregate_temporary_users");
    }
    if (params.allow_either_n2) {
      url.searchParams.set("allow_either_n2", "1");
    } else {
      url.searchParams.delete("allow_either_n2");
    }
    window.history.replaceState(null, "", url.toString());
  }

  function asUtcDate(value) {
    if (value == null || value === "") return null;
    if (typeof value === "number" && Number.isFinite(value)) {
      const millis = value < 1e12 ? value * 1000 : value;
      const d = new Date(millis);
      return Number.isFinite(d.getTime()) ? d : null;
    }
    if (typeof value === "string") {
      const text = value.trim();
      if (!text) return null;
      if (/^\d+$/.test(text)) {
        const numeric = Number(text);
        if (!Number.isFinite(numeric)) return null;
        const millis = text.length <= 10 ? numeric * 1000 : numeric;
        const d = new Date(millis);
        return Number.isFinite(d.getTime()) ? d : null;
      }
      const d = new Date(text);
      return Number.isFinite(d.getTime()) ? d : null;
    }
    const d = new Date(value);
    return Number.isFinite(d.getTime()) ? d : null;
  }

  function normalizeLevel(value) {
    const level = String(value == null ? "UNKNOWN" : value).toUpperCase();
    return Object.prototype.hasOwnProperty.call(LEVEL_RANK, level) ? level : "UNKNOWN";
  }

  function mergeLevels(levels) {
    const normalized = levels.map(normalizeLevel);
    if (normalized.includes("STRONG")) return "STRONG";
    if (normalized.includes("WEAK")) return "WEAK";
    if (normalized.some((level) => level === "UNKNOWN")) return "UNKNOWN";
    return "NONE";
  }

  function effectiveNotability(item, params) {
    if (!params.allow_either_n2) {
      return normalizeLevel(item.n);
    }

    const n2 = mergeLevels([item.n2a, item.n2b]);
    return mergeLevels([item.n1, n2, item.n3]);
  }

  function bucketOf(item, params) {
    if (!item) return "UNKNOWN";
    if (item.is_deleted === true) return "DELETED";
    if (item.redirect === true) return "REDIRECT";
    const notability = effectiveNotability(item, params);
    const empty = item.has_sitelinks === false && item.has_claims === false;
    if (notability === "NONE" && empty) return "EMPTY";
    return notability;
  }

  function bucketClass(bucket) {
    return BUCKET_CLASSES[bucket] || "bucket-unknown";
  }

  function labelFor(item) {
    return item.qid;
  }

  function mergedItems() {
    const params = state.currentQuery || parseParams();
    return state.population.map((item) => {
      const evalData = state.evaluations.get(item.qid) || {};
      return {
        ...item,
        ...evalData,
        bucket: bucketOf({ ...item, ...evalData }, params),
      };
    });
  }

  function formatIso(value) {
    const d = asUtcDate(value);
    if (!d) return String(value || "");
    return d.toISOString().replace(/\.\d{3}Z$/, "Z");
  }

  function floorDateToGroup(date, groupBy) {
    const d = new Date(date.getTime());
    d.setUTCMinutes(0, 0, 0);
    if (groupBy === "hour") {
      return d;
    }
    d.setUTCHours(0, 0, 0, 0);
    if (groupBy === "day") {
      return d;
    }
    if (groupBy === "week") {
      const dayOfWeek = (d.getUTCDay() + 6) % 7;
      d.setUTCDate(d.getUTCDate() - dayOfWeek);
      return d;
    }
    if (groupBy === "month") {
      d.setUTCDate(1);
      return d;
    }
    if (groupBy === "year") {
      d.setUTCMonth(0, 1);
      return d;
    }
    return d;
  }

  function groupKeyForDate(date, groupBy) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hour = String(date.getUTCHours()).padStart(2, "0");
    if (groupBy === "hour") return `${year}-${month}-${day} ${hour}:00 UTC`;
    if (groupBy === "week") return `${date.toISOString().slice(0, 10)} week`;
    if (groupBy === "month") return `${year}-${month}`;
    if (groupBy === "year") return String(year);
    return `${year}-${month}-${day}`;
  }

  function autoGroupBy(items) {
    const dates = items
      .map((item) => asUtcDate(item.creation_time))
      .filter(Boolean)
      .sort((a, b) => a - b);
    if (dates.length < 2) return "day";

    const spanMs = dates[dates.length - 1].getTime() - dates[0].getTime();
    const spanHours = spanMs / (60 * 60 * 1000);
    if (spanHours <= 72) return "hour";
    if (spanHours <= 90 * 24) return "day";
    if (spanHours <= 3 * 365 * 24) return "week";
    if (spanHours <= 10 * 365 * 24) return "month";
    return "year";
  }

  function effectiveGroupBy(items, params) {
    const explicit = params.group_by || "";
    if (explicit) return explicit;
    return autoGroupBy(items);
  }

  function groupLabelForItem(item, groupBy, params) {
    if (groupBy === "user") {
      const creator = item.creator || "Unknown creator";
      if (params.aggregate_temporary_users && typeof creator === "string" && creator.trim().startsWith("~")) {
        return "Temporary users";
      }
      return creator;
    }
    const d = asUtcDate(item.creation_time);
    if (!d) return "Unknown";
    return groupKeyForDate(floorDateToGroup(d, groupBy), groupBy);
  }

  function groupSortTime(item, groupBy, params) {
    const liveTimestamp = asUtcDate(item.timestamp);
    const creationTime = asUtcDate(item.creation_time);
    const sortDate = liveTimestamp || creationTime;
    if (groupBy === "user") {
      return sortDate;
    }
    const d = sortDate;
    return d ? floorDateToGroup(d, groupBy) : null;
  }

  function contributionsUrlForUser(userName) {
    const name = String(userName || "").trim();
    if (!name || name === "Unknown creator" || name === "Temporary users") {
      return null;
    }

    const url = new URL("https://www.wikidata.org/w/index.php");
    url.searchParams.set("title", "Special:Contributions");
    url.searchParams.set("target", name);
    url.searchParams.set("namespace", "0");
    url.searchParams.set("newOnly", "1");
    url.searchParams.set("limit", "500");
    return url.toString();
  }

  function compareGroups(left, right, sortMode) {
    const leftLabel = left.label;
    const rightLabel = right.label;
    if (sortMode === "lexical_asc") {
      return leftLabel.localeCompare(rightLabel, undefined, { sensitivity: "base" });
    }

    if (sortMode === "count_desc") {
      if (right.total !== left.total) return right.total - left.total;
      return leftLabel.localeCompare(rightLabel, undefined, { sensitivity: "base" });
    }

    if (sortMode === "strong_rate_asc") {
      if (left.strongRate !== right.strongRate) return left.strongRate - right.strongRate;
      if (left.strongOrWeakRate !== right.strongOrWeakRate) {
        return left.strongOrWeakRate - right.strongOrWeakRate;
      }
      if (left.total !== right.total) return left.total - right.total;
      return leftLabel.localeCompare(rightLabel, undefined, { sensitivity: "base" });
    }

    const leftTime = left.sortTime ? left.sortTime.getTime() : -Infinity;
    const rightTime = right.sortTime ? right.sortTime.getTime() : -Infinity;
    if (rightTime !== leftTime) return rightTime - leftTime;
    return leftLabel.localeCompare(rightLabel, undefined, { sensitivity: "base" });
  }

  function renderOverview(items) {
    const counts = Object.fromEntries(BUCKET_ORDER.map((bucket) => [bucket, 0]));
    for (const item of items) {
      counts[item.bucket] = (counts[item.bucket] || 0) + 1;
    }
    const total = items.length || 1;
    const cards = qs("overview-cards");
    const bar = qs("overview-bar");
    const legend = qs("overview-legend");
    cards.innerHTML = "";
    bar.innerHTML = "";
    legend.innerHTML = "";

    const summary = [
      ["Total", items.length],
      ["Evaluated", items.filter((item) => state.evaluations.has(item.qid)).length],
      ["Granularity", effectiveGroupBy(items, state.currentQuery || parseParams()).toUpperCase()],
      ["Deleted", counts.DELETED],
    ];
    for (const [label, value] of summary) {
      const card = document.createElement("div");
      card.className = "card";
      card.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
      cards.appendChild(card);
    }

    for (const bucket of BUCKET_ORDER) {
      const count = counts[bucket] || 0;
      if (!count) continue;
      const segment = document.createElement("span");
      segment.className = bucketClass(bucket);
      segment.style.width = `${(count / total) * 100}%`;
      segment.title = `${bucket}: ${count}`;
      bar.appendChild(segment);

      const legendItem = document.createElement("span");
      legendItem.innerHTML = `<span class="swatch ${bucketClass(bucket)}"></span>${bucket} (${count})`;
      legend.appendChild(legendItem);
    }
  }

  function renderTimeline(items, params) {
    const grid = qs("timeline-grid");
    grid.innerHTML = "";
    const groupBy = effectiveGroupBy(items, params);
    const sortMode = params.bucket_sort || DEFAULT_BUCKET_SORT;
    const minUserItems = parsePositiveInt(params.min_user_items);
    const groups = new Map();
    for (const item of items) {
      const key = groupLabelForItem(item, groupBy, params);
      if (!groups.has(key)) {
        groups.set(key, {
          label: key,
          bucketCounts: new Map(),
          total: 0,
          strongRate: 0,
          strongOrWeakRate: 0,
          sortTime: null,
        });
      }
      const group = groups.get(key);
      group.bucketCounts.set(item.bucket, (group.bucketCounts.get(item.bucket) || 0) + 1);
      group.total += 1;
      group.strongRate = group.total ? (group.bucketCounts.get("STRONG") || 0) / group.total : 0;
      group.strongOrWeakRate = group.total
        ? ((group.bucketCounts.get("STRONG") || 0) + (group.bucketCounts.get("WEAK") || 0)) / group.total
        : 0;
      const itemSortTime = groupSortTime(item, groupBy, params);
      if (itemSortTime && (!group.sortTime || itemSortTime.getTime() > group.sortTime.getTime())) {
        group.sortTime = itemSortTime;
      }
    }
    const sortedGroups = Array.from(groups.values())
      .filter((group) => groupBy !== "user" || minUserItems <= 0 || group.total >= minUserItems)
      .sort((a, b) => compareGroups(a, b, sortMode));
    for (const group of sortedGroups) {
      const row = document.createElement("div");
      row.className = "timeline-row";
      const bar = document.createElement("div");
      bar.className = "bar timeline-bar";
      for (const bucket of BUCKET_ORDER) {
        const count = group.bucketCounts.get(bucket) || 0;
        if (!count) continue;
        const segment = document.createElement("span");
        segment.className = bucketClass(bucket);
        segment.style.width = `${(count / (group.total || 1)) * 100}%`;
        segment.title = `${bucket}: ${count}`;
        bar.appendChild(segment);
      }
      if (groupBy === "user") {
        const key = document.createElement("div");
        key.className = "timeline-key user-key";
        const link = contributionsUrlForUser(group.label);
        if (link) {
          const anchor = document.createElement("a");
          anchor.className = "user-key-label";
          anchor.href = link;
          anchor.textContent = group.label;
          anchor.target = "_blank";
          anchor.rel = "noopener noreferrer";
          key.appendChild(anchor);
        } else {
          const label = document.createElement("span");
          label.className = "user-key-label";
          label.textContent = group.label;
          key.appendChild(label);
        }
        const total = document.createElement("span");
        total.className = "timeline-total";
        total.textContent = String(group.total);
        key.appendChild(document.createTextNode(" "));
        key.appendChild(total);
        row.appendChild(key);
      } else {
        row.innerHTML = `<div class="timeline-key">${group.label} <span class="timeline-total">${group.total}</span></div>`;
      }
      row.appendChild(bar);
      grid.appendChild(row);
    }
  }

  function render() {
    const params = state.currentQuery || parseParams();
    const items = mergedItems();
    qs("population-count").textContent = `Population: ${state.population.length}`;
    qs("evaluated-count").textContent = `Evaluated: ${items.filter((item) => state.evaluations.has(item.qid)).length}`;
    qs("updated-count").textContent = `Updated: ${state.evaluations.size}`;
    renderOverview(items);
    renderTimeline(items, params);
  }

  function mergeEvaluation(payload) {
    if (!payload || !payload.qid) return;
    state.evaluations.set(payload.qid, payload);
  }

  function canReconnectStream(subscriptionId, generation) {
    return (
      generation === state.loadGeneration &&
      subscriptionId &&
      state.subscriptionId === subscriptionId &&
      document.visibilityState === "visible"
    );
  }

  function openEventStream(subscriptionId, generation = state.loadGeneration) {
    if (!subscriptionId) return;
    if (generation !== state.loadGeneration) return;
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }

    state.eventSource = new EventSource(`/api/pubsub/sessions/gadget/${subscriptionId}/events`);
    state.eventSource.onmessage = (event) => {
      if (generation !== state.loadGeneration) return;
      const data = JSON.parse(event.data);
      if (data.event === "keepalive") return;
      if (data.event === "stream_end") {
        if (generation !== state.loadGeneration) return;
        if (state.eventSource) {
          state.eventSource.close();
          state.eventSource = null;
        }
        if (canReconnectStream(subscriptionId, generation)) {
          setTimeout(() => openEventStream(subscriptionId, generation), 1000);
        }
        return;
      }
      if (!data.qid) return;
      mergeEvaluation(data);
      render();
    };
    state.eventSource.onerror = () => {
      if (generation !== state.loadGeneration) return;
      if (state.eventSource) {
        state.eventSource.close();
        state.eventSource = null;
      }
      if (canReconnectStream(subscriptionId, generation)) {
        setTimeout(() => openEventStream(subscriptionId, generation), 1000);
      }
    };
  }

  async function deleteSubscription() {
    const subscriptionId = state.subscriptionId;
    if (!subscriptionId) return;

    state.subscriptionId = null;
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }

    try {
      await fetch(`/api/pubsub/sessions/gadget/${encodeURIComponent(subscriptionId)}`, {
        method: "DELETE",
        keepalive: true,
      });
    } catch (error) {
      console.debug("Creations subscription cleanup failed", error);
    }
  }

  async function subscribeToPopulation(items, generation = state.loadGeneration) {
    if (!items.length) return;
    if (generation !== state.loadGeneration) return;

    const response = await fetch("/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        items: items.map((item) => ({ qid: item.qid, reason: "page" })),
        session_id: state.subscriptionId,
      }),
    });
    if (!response.ok) {
      throw new Error(`subscribe failed: ${response.status}`);
    }
    if (generation !== state.loadGeneration) return;
    const payload = await response.json();
    if (generation !== state.loadGeneration) return;
    for (const item of payload.cached_items || []) {
      mergeEvaluation(item);
    }
    if (payload.subscription_id) {
      state.subscriptionId = payload.subscription_id;
      openEventStream(payload.subscription_id, generation);
    }
  }

  async function loadPopulation(params) {
    state.loadGeneration += 1;
    const windowParams = resolvePopulationWindow(params);
    const query = new URLSearchParams();
    query.set("start", windowParams.start);
    query.set("end", windowParams.end);
    for (const creator of params.creators) {
      query.append("creators", creator);
    }
    const response = await fetch(`/api/creations?${query.toString()}`);
    if (!response.ok) {
      throw new Error(`population request failed: ${response.status}`);
    }
    const payload = await response.json();
    state.population = Array.isArray(payload.items) ? payload.items : [];
    state.evaluations = new Map();
    state.subscriptionId = null;
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    return state.population;
  }

  function setupTeardownHandlers() {
    if (teardownHandlersInstalled) return;
    teardownHandlersInstalled = true;

    const cleanup = () => {
      void deleteSubscription();
    };
    window.addEventListener("pagehide", cleanup);
    window.addEventListener("beforeunload", cleanup);
  }

  async function run() {
    setupTeardownHandlers();
    const form = qs("query-form");
    const displayControls = ["group_by", "bucket_sort", "min_user_items", "aggregate_temporary_users", "allow_either_n2"];
    for (const id of displayControls) {
      const el = qs(id);
      if (!el) continue;
      el.addEventListener("change", () => {
        const current = state.currentQuery || parseParams();
        state.currentQuery = {
          ...current,
          ...readDisplayControls(),
        };
        updateUrl(state.currentQuery);
        render();
      });
    }
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const params = {
        ...readPopulationControls(),
        ...readDisplayControls(),
      };
      state.currentQuery = params;
      updateUrl(params);
      qs("status").textContent = "Loading population...";
      qs("status").classList.remove("error");
    try {
      await loadPopulation(params);
      syncControls(params);
      await subscribeToPopulation(state.population, state.loadGeneration);
      qs("status").textContent = "Report loaded.";
      render();
      } catch (error) {
        qs("status").textContent = error instanceof Error ? error.message : "Failed to load report.";
        qs("status").classList.add("error");
      }
    });

    const params = parseParams();
    state.currentQuery = params;
    syncControls(params);
    updateUrl(params);
    qs("status").textContent = "Loading population...";
    try {
      await loadPopulation(params);
      await subscribeToPopulation(state.population, state.loadGeneration);
      qs("status").textContent = "Report loaded.";
      render();
    } catch (error) {
      qs("status").textContent = error instanceof Error ? error.message : "Failed to load report.";
      qs("status").classList.add("error");
    }
  }

  run().catch((error) => {
    const status = qs("status");
    if (status) {
      status.textContent = error instanceof Error ? error.message : String(error);
      status.classList.add("error");
    }
    console.error(error);
  });
})();

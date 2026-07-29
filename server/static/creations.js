(function () {
  "use strict";

  const BUCKET_ORDER = [
    "DELETED",
    "REDIRECT",
    "EMPTY",
    "NONE",
    "PARTIAL-WEAK",
    "PARTIAL-STRONG",
    "WEAK",
    "STRONG",
    "UNKNOWN",
  ];
  const BUCKET_CLASSES = {
    DELETED: "bucket-deleted",
    REDIRECT: "bucket-redirect",
    EMPTY: "bucket-empty",
    NONE: "bucket-none",
    "PARTIAL-WEAK": "bucket-partial-weak",
    "PARTIAL-STRONG": "bucket-partial-strong",
    WEAK: "bucket-weak",
    STRONG: "bucket-strong",
    UNKNOWN: "bucket-unknown",
  };
  const QUALITY_BUCKET_ORDER = [
    "DELETED",
    "REDIRECT",
    "EMPTY",
    "NONE",
    "PARTIAL-WEAK",
    "PARTIAL-STRONG",
    "WEAK",
    "UNKNOWN",
    "STRONG",
  ];

  const DEFAULT_WINDOW_DAYS = 1;
  const DEFAULT_BUCKET_SORT = "time_desc";
  const LEVEL_RANK = {
    NONE: 0,
    "PARTIAL-WEAK": 1,
    "PARTIAL-STRONG": 2,
    WEAK: 3,
    UNKNOWN: 4,
    STRONG: 5,
  };
  const LEVEL_FIELDS = {
    n: "N",
    n1: "N1",
    n2a: "N2a",
    n2b: "N2b",
    n3: "N3",
    n3_inlinks: "N3_inlinks",
    n3_osm: "N3_osm",
    n3_wikisub: "N3_wikisub",
    n3_sdc: "N3_sdc",
  };
  const CONNECTION_STATE_LABELS = {
    loading: "Loading",
    connecting: "Connecting",
    live: "Live",
    reconnecting: "Reconnecting",
    offline: "Offline",
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
  let reconnectTimer = null;

  function qs(id) {
    return document.getElementById(id);
  }

  function setConnectionState(value) {
    const el = qs("connection-status");
    if (!el) return;
    const normalized = Object.prototype.hasOwnProperty.call(CONNECTION_STATE_LABELS, value)
      ? value
      : "offline";
    el.dataset.state = normalized;
    el.textContent = CONNECTION_STATE_LABELS[normalized];
  }

  function clearReconnectTimer() {
    if (reconnectTimer) {
      window.clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
  }

  function scheduleReconnect(subscriptionId, generation) {
    if (!canReconnectStream(subscriptionId, generation)) return;
    setConnectionState("reconnecting");
    clearReconnectTimer();
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = null;
      openEventStream(subscriptionId, generation);
    }, 1000);
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

  function bucketLabel(bucket) {
    return String(bucket || "").replaceAll("_", "-");
  }

  function fieldValue(item, evaluation, field) {
    if (evaluation && evaluation[field] !== undefined) {
      return evaluation[field];
    }
    if (evaluation && evaluation.levels && Object.prototype.hasOwnProperty.call(LEVEL_FIELDS, field)) {
      const levelValue = evaluation.levels[LEVEL_FIELDS[field]];
      if (levelValue !== undefined) return levelValue;
    }
    return item[field];
  }

  function contentKnown(item, evaluation) {
    return fieldValue(item, evaluation, "content_last_revid") != null;
  }

  function effectiveNotability(item, evaluation, params) {
    return normalizeLevel(fieldValue(item, evaluation, "n"));
  }

  function partialBucket(item, evaluation) {
    const n2a = normalizeLevel(fieldValue(item, evaluation, "n2a"));
    const n2b = normalizeLevel(fieldValue(item, evaluation, "n2b"));
    const activeLevels = [n2a, n2b].filter((level) => level === "WEAK" || level === "STRONG");

    if (activeLevels.length !== 1) return null;
    return activeLevels[0] === "STRONG" ? "PARTIAL-STRONG" : "PARTIAL-WEAK";
  }

  function bucketOf(item, evaluation, params) {
    if (!item) return "UNKNOWN";
    if (fieldValue(item, evaluation, "is_deleted") === true) return "DELETED";
    if (fieldValue(item, evaluation, "redirect") === true) return "REDIRECT";
    const notability = effectiveNotability(item, evaluation, params);
    if (notability === "NONE") {
      const partial = partialBucket(item, evaluation);
      if (partial) return partial;
      const sitelinksCount = Number(fieldValue(item, evaluation, "has_sitelinks_count"));
      const claimsCount = Number(fieldValue(item, evaluation, "has_claims_count"));
      const empty = contentKnown(item, evaluation) && Number.isFinite(sitelinksCount) && Number.isFinite(claimsCount) && sitelinksCount === 0 && claimsCount === 0;
      if (empty) return "EMPTY";
    }
    return notability;
  }

  function bucketClass(bucket) {
    return BUCKET_CLASSES[bucket] || "bucket-unknown";
  }

  function qualityCumulativeRates(group) {
    const rates = [];
    let cumulative = 0;
    for (const bucket of QUALITY_BUCKET_ORDER) {
      cumulative += group.bucketCounts.get(bucket) || 0;
      rates.push(group.total ? cumulative / group.total : 0);
    }
    return rates;
  }

  function labelFor(item) {
    return item.qid;
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
    let minTime = null;
    let maxTime = null;
    let seen = 0;
    for (const item of items) {
      const date = asUtcDate(item.creation_time);
      if (!date) continue;
      const time = date.getTime();
      if (!Number.isFinite(time)) continue;
      seen += 1;
      if (minTime == null || time < minTime) {
        minTime = time;
      }
      if (maxTime == null || time > maxTime) {
        maxTime = time;
      }
    }
    if (seen < 2 || minTime == null || maxTime == null) return "day";

    const spanMs = maxTime - minTime;
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

  function groupSortTime(item, evaluation, groupBy) {
    const liveTimestamp = asUtcDate(evaluation?.timestamp);
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
      const leftQuality = qualityCumulativeRates(left);
      const rightQuality = qualityCumulativeRates(right);
      for (let index = 0; index < leftQuality.length; index += 1) {
        if (leftQuality[index] !== rightQuality[index]) {
          return rightQuality[index] - leftQuality[index];
        }
      }
      if (left.total !== right.total) return left.total - right.total;
      return leftLabel.localeCompare(rightLabel, undefined, { sensitivity: "base" });
    }

    const leftTime = left.sortTime ? left.sortTime.getTime() : -Infinity;
    const rightTime = right.sortTime ? right.sortTime.getTime() : -Infinity;
    if (rightTime !== leftTime) return rightTime - leftTime;
    return leftLabel.localeCompare(rightLabel, undefined, { sensitivity: "base" });
  }

  function renderOverview(summary) {
    const { counts, total, evaluatedCount, groupBy } = summary;
    const cards = qs("overview-cards");
    const bar = qs("overview-bar");
    const legend = qs("overview-legend");
    cards.innerHTML = "";
    bar.innerHTML = "";
    legend.innerHTML = "";

    const cardsSummary = [
      ["Total", total],
      ["Evaluated", evaluatedCount],
      ["Granularity", groupBy.toUpperCase()],
      ["Deleted", counts.DELETED],
    ];
    for (const [label, value] of cardsSummary) {
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
      segment.title = `${bucketLabel(bucket)}: ${count}`;
      bar.appendChild(segment);

      const legendItem = document.createElement("span");
      legendItem.innerHTML = `<span class="swatch ${bucketClass(bucket)}"></span>${bucketLabel(bucket)} (${count})`;
      legend.appendChild(legendItem);
    }
  }

  function renderTimeline(groups) {
    const grid = qs("timeline-grid");
    grid.innerHTML = "";
    for (const group of groups) {
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
        segment.title = `${bucketLabel(bucket)}: ${count}`;
        bar.appendChild(segment);
      }
      if (group.groupBy === "user") {
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

  function collectDashboardData(items, params) {
    const groupBy = effectiveGroupBy(items, params);
    const sortMode = params.bucket_sort || DEFAULT_BUCKET_SORT;
    const minUserItems = parsePositiveInt(params.min_user_items);
    const counts = Object.fromEntries(BUCKET_ORDER.map((bucket) => [bucket, 0]));
    const groups = new Map();
    let evaluatedCount = 0;
    for (const item of items) {
      const evaluation = state.evaluations.get(item.qid) || null;
      if (evaluation) {
        evaluatedCount += 1;
      }
      const bucket = bucketOf(item, evaluation, params);
      counts[bucket] = (counts[bucket] || 0) + 1;
      const key = groupLabelForItem(item, groupBy, params);
      if (!groups.has(key)) {
        groups.set(key, {
          label: key,
          bucketCounts: new Map(),
          total: 0,
          strongRate: 0,
          strongOrWeakRate: 0,
          partialStrongRate: 0,
          partialWeakRate: 0,
          sortTime: null,
          groupBy,
        });
      }
      const group = groups.get(key);
      group.bucketCounts.set(bucket, (group.bucketCounts.get(bucket) || 0) + 1);
      group.total += 1;
      group.strongRate = group.total ? (group.bucketCounts.get("STRONG") || 0) / group.total : 0;
      group.strongOrWeakRate = group.total
        ? ((group.bucketCounts.get("STRONG") || 0) + (group.bucketCounts.get("WEAK") || 0)) / group.total
        : 0;
      group.partialStrongRate = group.total ? (group.bucketCounts.get("PARTIAL_STRONG") || 0) / group.total : 0;
      group.partialWeakRate = group.total ? (group.bucketCounts.get("PARTIAL_WEAK") || 0) / group.total : 0;
      const itemSortTime = groupSortTime(item, evaluation, groupBy);
      if (itemSortTime && (!group.sortTime || itemSortTime.getTime() > group.sortTime.getTime())) {
        group.sortTime = itemSortTime;
      }
    }
    const sortedGroups = Array.from(groups.values())
      .filter((group) => groupBy !== "user" || minUserItems <= 0 || group.total >= minUserItems)
      .sort((a, b) => compareGroups(a, b, sortMode));
    return {
      groupBy,
      counts,
      evaluatedCount,
      total: items.length,
      groups: sortedGroups,
    };
  }

  function render() {
    const params = state.currentQuery || parseParams();
    const summary = collectDashboardData(state.population, params);
    qs("population-count").textContent = `Population: ${summary.total}`;
    qs("evaluated-count").textContent = `Evaluated: ${summary.evaluatedCount}`;
    qs("updated-count").textContent = `Updated: ${state.evaluations.size}`;
    renderOverview(summary);
    renderTimeline(summary.groups);
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
    clearReconnectTimer();
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }

    state.eventSource = new EventSource(`/api/pubsub/sessions/gadget/${subscriptionId}/events`);
    setConnectionState("connecting");
    state.eventSource.onopen = () => {
      if (generation !== state.loadGeneration) return;
      setConnectionState("live");
    };
    state.eventSource.onmessage = (event) => {
      if (generation !== state.loadGeneration) return;
      const data = JSON.parse(event.data);
      if (data.event === "keepalive") return;
      if (data.event === "primed") {
        console.debug("Creations stream primed:", data.qid_count);
        return;
      }
      if (data.event === "stream_end") {
        if (generation !== state.loadGeneration) return;
        if (state.eventSource) {
          state.eventSource.close();
          state.eventSource = null;
        }
        scheduleReconnect(subscriptionId, generation);
        if (!canReconnectStream(subscriptionId, generation)) {
          setConnectionState("offline");
        }
        return;
      }
      if (!data.qid) return;
      setConnectionState("live");
      mergeEvaluation(data);
      render();
    };
    state.eventSource.onerror = () => {
      if (generation !== state.loadGeneration) return;
      if (state.eventSource) {
        state.eventSource.close();
        state.eventSource = null;
      }
      scheduleReconnect(subscriptionId, generation);
      if (!canReconnectStream(subscriptionId, generation)) {
        setConnectionState("offline");
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
    clearReconnectTimer();
    setConnectionState("offline");

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
    setConnectionState("connecting");

    const response = await fetch("/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        qids: items.map((item) => item.qid),
        session_id: state.subscriptionId,
      }),
    });
    if (!response.ok) {
      throw new Error(`subscribe failed: ${response.status}`);
    }
    if (generation !== state.loadGeneration) return;
    const payload = await response.json();
    if (generation !== state.loadGeneration) return;
    if (payload.subscription_id) {
      state.subscriptionId = payload.subscription_id;
      openEventStream(payload.subscription_id, generation);
    } else {
      setConnectionState("offline");
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
    clearReconnectTimer();
    setConnectionState("offline");
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
    setConnectionState("loading");
    const form = qs("query-form");
    const connectionStatus = qs("connection-status");
    if (connectionStatus) {
      connectionStatus.addEventListener("click", () => {
        if (connectionStatus.dataset.state !== "offline") return;
        if (!state.subscriptionId) return;
        openEventStream(state.subscriptionId, state.loadGeneration);
      });
    }
    const displayControls = ["group_by", "bucket_sort", "min_user_items", "aggregate_temporary_users"];
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
      setConnectionState("loading");
      try {
        await loadPopulation(params);
        syncControls(params);
        await subscribeToPopulation(state.population, state.loadGeneration);
        qs("status").textContent = "Report loaded.";
        render();
      } catch (error) {
        qs("status").textContent = error instanceof Error ? error.message : "Failed to load report.";
        qs("status").classList.add("error");
        setConnectionState("offline");
      }
    });

    const params = parseParams();
    state.currentQuery = params;
    syncControls(params);
    updateUrl(params);
    qs("status").textContent = "Loading population...";
    setConnectionState("loading");
    try {
      await loadPopulation(params);
      await subscribeToPopulation(state.population, state.loadGeneration);
      qs("status").textContent = "Report loaded.";
      render();
    } catch (error) {
      qs("status").textContent = error instanceof Error ? error.message : "Failed to load report.";
      qs("status").classList.add("error");
      setConnectionState("offline");
    }
  }

  run().catch((error) => {
    const status = qs("status");
    clearReconnectTimer();
    setConnectionState("offline");
    if (status) {
      status.textContent = error instanceof Error ? error.message : String(error);
      status.classList.add("error");
    }
    console.error(error);
  });
})();

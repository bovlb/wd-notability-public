(function () {
  "use strict";

  const DEFAULT_PERIOD = "24h";
  const SERIES_COLORS = [
    "#2563eb",
    "#0f766e",
    "#d97706",
    "#dc2626",
    "#7c3aed",
    "#0891b2",
    "#4f46e5",
    "#15803d",
  ];
  const CRITERION_SERIES_COLORS = [
    "#9ca3af",
    "#dc2626",
    "#f59e0b",
    "#16a34a",
  ];

  const activeCharts = [];
  let metricMetadata = new Map();
  let selectedPeriod = DEFAULT_PERIOD;
  let refreshTimer = null;

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

  function setSummaryCard(label, value) {
    const summary = qs("summary");
    if (!summary) return;
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
    summary.appendChild(card);
  }

  function toSeriesPoints(points) {
    return (Array.isArray(points) ? points : [])
      .map((point) => {
        if (!Array.isArray(point) || point.length < 2) return null;
        const timestamp = Number(point[0]);
        const value = point[1];
        if (!Number.isFinite(timestamp) || typeof value !== "number" || !Number.isFinite(value)) {
          return null;
        }
        return [timestamp * 1000, value];
      })
      .filter(Boolean);
  }

  function toChartPoints(points) {
    return (Array.isArray(points) ? points : [])
      .map((point) => {
        if (!Array.isArray(point) || point.length < 2) return null;
        const timestamp = Number(point[0]);
        const value = point[1];
        if (!Number.isFinite(timestamp) || typeof value !== "number" || !Number.isFinite(value)) {
          return null;
        }
        return [timestamp, value];
      })
      .filter(Boolean);
  }

  function formatLocalTimestamp(value) {
    const millis = Number(value);
    if (!Number.isFinite(millis)) {
      return "";
    }
    const date = new Date(millis);
    if (!Number.isFinite(date.getTime())) {
      return "";
    }
    return date.toLocaleString();
  }

  function formatAxisTimestamp(value) {
    const millis = Number(value);
    if (!Number.isFinite(millis)) {
      return "";
    }
    const date = new Date(millis);
    if (!Number.isFinite(date.getTime())) {
      return "";
    }
    return new Intl.DateTimeFormat(undefined, {
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    }).format(date);
  }

  function chartTheme() {
    if (window.matchMedia("(prefers-color-scheme: dark)").matches) {
      return {
        textColor: "#e5eefc",
        mutedColor: "#9ca9bf",
        borderColor: "rgba(148, 163, 184, 0.22)",
        panelStrongColor: "#111827",
        accentColor: "#5eead4",
        accent2Color: "#60a5fa",
        gridColor: "rgba(226, 232, 240, 0.14)",
      };
    }
    return {
      textColor: "#172033",
      mutedColor: "#5c677d",
      borderColor: "rgba(71, 85, 105, 0.2)",
      panelStrongColor: "#ffffff",
      accentColor: "#0f766e",
      accent2Color: "#2563eb",
      gridColor: "rgba(71, 85, 105, 0.12)",
    };
  }

  function disposeExistingCharts() {
    while (activeCharts.length) {
      const chart = activeCharts.pop();
      try {
        chart.dispose();
      } catch (_error) {
        // Ignore disposal races on page refresh.
      }
    }
  }

  function renderEmptyState(message) {
    const emptyState = qs("empty-state");
    if (emptyState) {
      emptyState.textContent = message;
      emptyState.classList.remove("hidden");
    }
  }

  function hideEmptyState() {
    const emptyState = qs("empty-state");
    if (emptyState) {
      emptyState.classList.add("hidden");
    }
  }

  function clearRefreshTimer() {
    if (!refreshTimer) return;
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }

  function periodValue() {
    const periodSelect = qs("period");
    return periodSelect && periodSelect.value ? periodSelect.value : selectedPeriod;
  }

  function descriptionForField(fieldName) {
    return metricMetadata.get(fieldName) || "No description available.";
  }

  function latestPoint(points) {
    const seriesPoints = toSeriesPoints(points);
    if (!seriesPoints.length) return null;
    return seriesPoints[seriesPoints.length - 1];
  }

  function formatMetricValue(value) {
    if (!Number.isFinite(value)) {
      return "n/a";
    }
    const fractionDigits = Number.isInteger(value) ? 0 : value >= 10 ? 1 : 2;
    return new Intl.NumberFormat(undefined, {
      maximumFractionDigits: fractionDigits,
      minimumFractionDigits: fractionDigits,
    }).format(value);
  }

  function colorWithAlpha(color, alpha) {
    if (typeof color !== "string") {
      return `rgba(148, 163, 184, ${alpha})`;
    }
    const hex = color.trim();
    if (/^#[0-9a-fA-F]{6}$/.test(hex)) {
      const red = Number.parseInt(hex.slice(1, 3), 16);
      const green = Number.parseInt(hex.slice(3, 5), 16);
      const blue = Number.parseInt(hex.slice(5, 7), 16);
      return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
    }
    return color;
  }

  function pointsToMap(points) {
    const map = new Map();
    for (const point of toSeriesPoints(points)) {
      map.set(point[0], point[1]);
    }
    return map;
  }

  function seriesFromMap(pointMap) {
    return Array.from(pointMap.entries())
      .sort((left, right) => left[0] - right[0])
      .map(([timestamp, value]) => [timestamp, value]);
  }

  function buildCriterionSeries(fields, prefix, sourceLabel) {
    const levels = ["unknown", "none", "weak", "strong"];
    const criteria = new Map();
    const prefixWithDot = `${prefix}.`;

    for (const [fieldName, points] of Object.entries(fields || {})) {
      if (!fieldName.startsWith(prefixWithDot)) continue;
      const suffix = fieldName.slice(prefixWithDot.length);
      const parts = suffix.split(".");
      const levelName = parts.pop();
      if (!levels.includes(levelName)) continue;
      const criterionName = parts.join(".");
      if (!criterionName) continue;
      if (!criteria.has(criterionName)) {
        criteria.set(criterionName, Object.fromEntries(levels.map((level) => [level, new Map()])));
      }
      const seriesByLevel = criteria.get(criterionName);
      const target = seriesByLevel[levelName];
      for (const [timestamp, value] of pointsToMap(points)) {
        target.set(timestamp, (target.get(timestamp) || 0) + value);
      }
    }

    return Array.from(criteria.entries())
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([criterionName, seriesByLevel]) => {
        const normalizedSeries = Object.fromEntries(
          levels
            .map((level) => [level, seriesFromMap(seriesByLevel[level])])
            .filter(([, points]) => points.length > 0)
        );
        return {
          criterionName,
          sourceLabel,
          seriesByLevel: normalizedSeries,
        };
      })
      .filter((entry) => Object.keys(entry.seriesByLevel).length > 0);
  }

  function buildPrioritySeries(fields, prefix, metricName) {
    const priorities = ["unknown_active", "unknown_idle", "refresh_active", "refresh_idle"];
    const seriesByLevel = Object.fromEntries(priorities.map((priority) => [priority, []]));
    const prefixWithDot = `${prefix}.`;
    const suffix = `.${metricName}`;

    for (const [fieldName, points] of Object.entries(fields || {})) {
      if (!fieldName.startsWith(prefixWithDot) || !fieldName.endsWith(suffix)) continue;
      const priorityName = fieldName.slice(prefixWithDot.length, fieldName.length - suffix.length);
      if (!priorities.includes(priorityName)) continue;
      seriesByLevel[priorityName] = points;
    }

    return {
      priorities,
      seriesByLevel,
    };
  }

  function buildFlagSeries(fields) {
    const states = ["no", "yes"];
    const flags = new Map();
    const prefixWithDot = "flags.";

    for (const [fieldName, points] of Object.entries(fields || {})) {
      if (!fieldName.startsWith(prefixWithDot)) continue;
      const suffix = fieldName.slice(prefixWithDot.length);
      const parts = suffix.split(".");
      const stateName = parts.pop();
      if (!states.includes(stateName)) continue;
      const flagName = parts.join(".");
      if (!flagName) continue;
      if (!flags.has(flagName)) {
        flags.set(flagName, Object.fromEntries(states.map((state) => [state, new Map()])));
      }
      const seriesByState = flags.get(flagName);
      const target = seriesByState[stateName];
      for (const [timestamp, value] of pointsToMap(points)) {
        target.set(timestamp, (target.get(timestamp) || 0) + value);
      }
    }

    return Array.from(flags.entries())
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([flagName, seriesByState]) => {
        const normalizedSeries = Object.fromEntries(
          states
            .map((state) => [state, seriesFromMap(seriesByState[state])])
            .filter(([, points]) => points.length > 0)
        );
        return {
          flagName,
          seriesByLevel: normalizedSeries,
        };
      })
      .filter((entry) => Object.keys(entry.seriesByLevel).length > 0);
  }

  function renderStackedAreaChart(chartNode, {
    title,
    subtitle,
    seriesByLevel,
    levelOrder = ["unknown", "none", "weak", "strong"],
    levelLabels = {
      unknown: "unknown",
      none: "none",
      weak: "weak",
      strong: "strong",
    },
    seriesColors = SERIES_COLORS,
    levelColors = null,
    showLegend = false,
    showDataZoom = false,
    compact = false,
  }) {
    const palette = Array.isArray(seriesColors) && seriesColors.length ? seriesColors : SERIES_COLORS;
    const seriesEntries = levelOrder
      .map((level, index) => {
        const points = toChartPoints(seriesByLevel && seriesByLevel[level] ? seriesByLevel[level] : []);
        if (!points.length) return null;
        return {
          level,
          points,
          color: palette[index % palette.length],
        };
      })
      .filter(Boolean);
    const resolvedLevelColors = levelColors || Object.fromEntries(
      levelOrder.map((level, index) => [level, colorWithAlpha(palette[index % palette.length], 0.24)])
    );
    if (!chartNode || !seriesEntries.length) {
      if (chartNode) {
        chartNode.innerHTML = '<div class="zoom-placeholder">No breakdown samples in this window.</div>';
      }
      return null;
    }

    const { textColor, mutedColor, borderColor, panelStrongColor, accentColor, accent2Color, gridColor } = chartTheme();
    chartNode.innerHTML = "";
    chartNode.style.width = "100%";
    chartNode.style.height = compact ? "220px" : "320px";
    const chart = echarts.init(chartNode, null, { renderer: "canvas" });
    activeCharts.push(chart);

    chart.setOption({
      animation: true,
      backgroundColor: "transparent",
      color: palette,
      textStyle: {
        color: textColor,
      },
      grid: showLegend ? { left: 56, right: 28, top: 28, bottom: 86, containLabel: true } : { left: 56, right: 28, top: 28, bottom: 44, containLabel: true },
      legend: showLegend
        ? {
            bottom: 0,
            left: 8,
            right: 8,
            textStyle: {
              color: mutedColor,
            },
          }
        : { show: false },
      tooltip: {
        trigger: "axis",
        backgroundColor: panelStrongColor,
        borderColor: borderColor,
        borderWidth: 1,
        textStyle: {
          color: textColor,
        },
        axisPointer: {
          type: "cross",
          lineStyle: { color: accentColor, width: 1 },
          crossStyle: { color: accent2Color, width: 1 },
        },
        extraCssText: "backdrop-filter: blur(12px); box-shadow: 0 18px 40px rgba(15, 23, 42, 0.28);",
        formatter: (params) => {
          const list = Array.isArray(params) ? params : [params];
          const timestamp = list[0] && list[0].data ? formatLocalTimestamp(list[0].data[0]) : "";
          const rows = [`<strong>${escapeHtml(title)}</strong>`];
          if (subtitle) {
            rows.push(`<span>${escapeHtml(subtitle)}</span>`);
          }
          if (timestamp) {
            rows.push(`<span>${escapeHtml(timestamp)}</span>`);
          }
          for (const item of list) {
            const rawValue = Number(item.value && item.value[1]);
            const label = escapeHtml(item.seriesName);
            const valueText = Number.isFinite(rawValue) ? formatMetricValue(rawValue) : "n/a";
            rows.push(`<span>${item.marker} ${label}: ${escapeHtml(valueText)}</span>`);
          }
          return rows.join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        axisLabel: {
          hideOverlap: true,
          color: mutedColor,
          fontSize: compact ? 10 : 12,
          margin: 16,
          formatter: (value) => formatAxisTimestamp(value),
        },
        axisLine: {
          lineStyle: {
            color: borderColor,
          },
        },
        axisTick: {
          lineStyle: {
            color: borderColor,
          },
        },
        splitLine: {
          show: false,
        },
        axisPointer: {
          label: {
            backgroundColor: panelStrongColor,
            borderColor: borderColor,
            color: textColor,
          },
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        min: 0,
        axisLabel: {
          color: mutedColor,
          fontSize: compact ? 10 : 12,
          margin: 14,
        },
        axisLine: {
          lineStyle: {
            color: borderColor,
          },
        },
        axisTick: {
          lineStyle: {
            color: borderColor,
          },
        },
        splitLine: {
          lineStyle: {
            color: gridColor,
            width: 1,
          },
        },
      },
      dataZoom: showDataZoom
        ? [
            {
              type: "inside",
              xAxisIndex: 0,
              filterMode: "none",
              moveOnMouseMove: true,
              zoomOnMouseWheel: true,
              moveOnMouseWheel: false,
            },
            {
              type: "slider",
              xAxisIndex: 0,
              height: 18,
              bottom: 20,
              filterMode: "none",
              backgroundColor: panelStrongColor,
              fillerColor: "rgba(96, 165, 250, 0.24)",
              borderColor: borderColor,
              handleStyle: {
                color: accentColor,
                borderColor: accentColor,
              },
              moveHandleSize: 12,
              dataBackground: {
                lineStyle: {
                  color: accent2Color,
                },
                areaStyle: {
                  color: "rgba(96, 165, 250, 0.16)",
                },
              },
              textStyle: {
                color: mutedColor,
              },
            },
          ]
        : [],
      series: seriesEntries.map((entry, index) => ({
        name: levelLabels[entry.level] || entry.level,
        type: "line",
        showSymbol: false,
        smooth: true,
        stack: "total",
        sampling: "lttb",
        lineStyle: { width: compact ? 1.5 : 2, color: entry.color },
        areaStyle: {
          color: resolvedLevelColors[entry.level] || colorWithAlpha(entry.color, 0.24),
        },
        emphasis: { focus: "series" },
        data: entry.points,
        color: entry.color,
        itemStyle: {
          color: entry.color,
        },
      })),
    });

    requestAnimationFrame(() => chart.resize());
    return chart;
  }

  function renderCacheBreakdownPanels(fields, container) {
    if (!container) return;
    const flags = buildFlagSeries(fields);
    const detectedCriteria = buildCriterionSeries(fields, "criteria.detected", "detected");
    const deducedCriteria = buildCriterionSeries(fields, "criteria.deduced", "deduced");
    const criteria = [...detectedCriteria, ...deducedCriteria];
    if (!flags.length && !criteria.length) {
      container.remove();
      return;
    }

    const items = [
      ...flags.map((item) => ({
        kind: "flag",
        title: item.flagName,
        subtitle: "Flag breakdown",
        seriesByLevel: item.seriesByLevel,
        sourceLabel: null,
        levelOrder: ["no", "yes"],
        levelLabels: {
          no: "no",
          yes: "yes",
        },
        levelColors: {
          no: "rgba(148, 163, 184, 0.42)",
          yes: "rgba(15, 118, 110, 0.68)",
        },
      })),
      ...criteria.map((item) => ({
        kind: "criterion",
        title: item.criterionName,
        subtitle: item.sourceLabel === "detected" ? "Detected criterion" : "Deduced criterion",
        seriesByLevel: item.seriesByLevel,
        sourceLabel: item.sourceLabel,
        levelOrder: ["unknown", "none", "weak", "strong"],
        levelLabels: {
          unknown: "unknown",
          none: "none",
          weak: "weak",
          strong: "strong",
        },
        seriesColors: CRITERION_SERIES_COLORS,
      })),
    ];

    container.innerHTML = "";
    const wrapper = document.createElement("section");
    wrapper.className = "cache-breakdown-section";
    wrapper.innerHTML = `
      <div class="section-head">
        <div class="title">Cache breakdown</div>
        <div class="subtitle">Flags and criteria together. Click any card to zoom.</div>
      </div>
      <div class="cache-breakdown-grid"></div>
      <div class="zoom-panel">
        <div class="zoom-placeholder">Click a card to zoom.</div>
      </div>
    `;
    container.appendChild(wrapper);
    const grid = wrapper.querySelector(".cache-breakdown-grid");
    const zoomPanel = wrapper.querySelector(".zoom-panel");
    for (const item of items) {
      const card = document.createElement("section");
      card.className = "stacked-chart-card";
      card.title = `${item.title} - ${item.subtitle}${item.sourceLabel ? ` - ${item.sourceLabel}` : ""}`;
      card.innerHTML = `
        <div class="stacked-chart-head">
          <div class="title">${escapeHtml(item.title)}</div>
          <div class="subtitle">${escapeHtml(item.subtitle)}${item.sourceLabel ? ` · ${escapeHtml(item.sourceLabel)}` : ""}</div>
        </div>
        <div class="stacked-chart"></div>
      `;
      grid.appendChild(card);
      const chartOptions = {
        title: item.title,
        subtitle: item.subtitle,
        seriesByLevel: item.seriesByLevel,
        levelOrder: item.levelOrder,
        levelLabels: item.levelLabels,
        seriesColors: item.seriesColors,
        levelColors: item.levelColors,
        showLegend: false,
        showDataZoom: false,
        compact: true,
      };
      renderStackedAreaChart(card.querySelector(".stacked-chart"), chartOptions);
      card.addEventListener("click", () => {
        if (!zoomPanel) return;
        zoomPanel.innerHTML = `
          <div class="zoom-title">
            <span>${escapeHtml(item.title)}</span>
            <span>${escapeHtml(item.subtitle)}${item.sourceLabel ? ` · ${escapeHtml(item.sourceLabel)}` : ""}</span>
          </div>
          <div class="zoom-chart">
          </div>
        `;
        const zoomChartNode = zoomPanel.querySelector(".zoom-chart");
        renderStackedAreaChart(zoomChartNode, {
          ...chartOptions,
          showLegend: true,
          showDataZoom: true,
          compact: false,
        });
      });
    }
  }

  function renderSparklineSvg(points) {
    const seriesPoints = toSeriesPoints(points);
    if (seriesPoints.length === 0) {
      return '<svg viewBox="0 0 240 64" preserveAspectRatio="none" aria-hidden="true"><rect x="0" y="0" width="240" height="64" rx="12" fill="rgba(148,163,184,.08)"/><text x="120" y="36" text-anchor="middle" font-size="12" fill="#52627a">No samples</text></svg>';
    }

    const width = 240;
    const height = 64;
    const paddingX = 6;
    const paddingY = 8;
    const values = seriesPoints.map((point) => point[1]);
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const valueRange = maxValue - minValue || 1;
    const count = seriesPoints.length;
    const xStep = count > 1 ? (width - paddingX * 2) / (count - 1) : 0;
    const coords = seriesPoints.map((point, index) => {
      const x = paddingX + xStep * index;
      const normalized = (point[1] - minValue) / valueRange;
      const y = height - paddingY - normalized * (height - paddingY * 2);
      return [x, y];
    });
    const linePoints = coords.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
    const areaPath = `${coords.map(([x, y], index) => `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`).join(" ")} L ${coords[coords.length - 1][0].toFixed(1)} ${height - paddingY} L ${coords[0][0].toFixed(1)} ${height - paddingY} Z`;
    const sparkColor = "#0f766e";
    const minText = formatMetricValue(minValue);
    const maxText = formatMetricValue(maxValue);
    const midText = formatMetricValue(minValue + (valueRange / 2));

    return `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
        <line x1="4" y1="8" x2="4" y2="${height - 8}" stroke="rgba(82,98,122,.25)" stroke-width="1" />
        <path d="${areaPath}" fill="rgba(15,118,110,0.16)"></path>
        <polyline points="${linePoints}" fill="none" stroke="${sparkColor}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"></polyline>
        <text x="12" y="14" font-size="10" fill="#52627a">${escapeHtml(maxText)}</text>
        <text x="12" y="${height / 2 + 3}" font-size="10" fill="#52627a">${escapeHtml(midText)}</text>
        <text x="12" y="${height - 6}" font-size="10" fill="#52627a">${escapeHtml(minText)}</text>
      </svg>
    `;
  }

  function renderZoomChart(chartNode, workerName, fieldName, points) {
    const seriesPoints = toSeriesPoints(points);
    if (!chartNode) return;
    chartNode.innerHTML = "";
    if (!seriesPoints.length) {
      chartNode.innerHTML = '<div class="zoom-placeholder">No samples in this window.</div>';
      return;
    }

    const { textColor, mutedColor, borderColor, panelStrongColor, accentColor, accent2Color, gridColor } = chartTheme();

    const chart = echarts.init(chartNode, null, { renderer: "canvas" });
    activeCharts.push(chart);
    chart.setOption({
      animation: true,
      backgroundColor: "transparent",
      color: SERIES_COLORS,
      textStyle: {
        color: textColor,
      },
      grid: { left: 56, right: 28, top: 28, bottom: 74, containLabel: true },
      tooltip: {
        trigger: "axis",
        backgroundColor: panelStrongColor,
        borderColor: borderColor,
        borderWidth: 1,
        textStyle: {
          color: textColor,
        },
        axisPointer: {
          type: "cross",
          lineStyle: { color: accentColor, width: 1 },
          crossStyle: { color: accent2Color, width: 1 },
        },
        extraCssText: "backdrop-filter: blur(12px); box-shadow: 0 18px 40px rgba(15, 23, 42, 0.28);",
        formatter: (params) => {
          const list = Array.isArray(params) ? params : [params];
          const timestamp = list[0] && list[0].data ? formatLocalTimestamp(list[0].data[0]) : "";
          const description = descriptionForField(fieldName);
          const rows = [
            `<strong>${escapeHtml(workerName)} · ${escapeHtml(fieldName)}</strong>`,
            `<span>${escapeHtml(description)}</span>`,
          ];
          if (timestamp) {
            rows.push(`<span>${escapeHtml(timestamp)}</span>`);
          }
          for (const item of list) {
            const value = Number(item.value && item.value[1]);
            rows.push(`<span>${item.marker} ${escapeHtml(item.seriesName)}: ${escapeHtml(formatMetricValue(value))}</span>`);
          }
          return rows.join("<br/>");
        },
      },
      xAxis: {
        type: "time",
        axisLabel: {
          hideOverlap: true,
          color: mutedColor,
          fontSize: 12,
          margin: 16,
          formatter: (value) => formatAxisTimestamp(value),
        },
        axisLine: {
          lineStyle: {
            color: borderColor,
          },
        },
        axisTick: {
          lineStyle: {
            color: borderColor,
          },
        },
        splitLine: {
          show: false,
        },
        axisPointer: {
          label: {
            backgroundColor: panelStrongColor,
            borderColor: borderColor,
            color: textColor,
          },
        },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          color: mutedColor,
          fontSize: 12,
          margin: 14,
        },
        axisLine: {
          lineStyle: {
            color: borderColor,
          },
        },
        axisTick: {
          lineStyle: {
            color: borderColor,
          },
        },
        splitLine: {
          lineStyle: {
            color: gridColor,
            width: 1,
          },
        },
      },
      dataZoom: [
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          moveOnMouseMove: true,
          zoomOnMouseWheel: true,
          moveOnMouseWheel: false,
        },
        {
          type: "slider",
          xAxisIndex: 0,
          height: 18,
          bottom: 20,
          filterMode: "none",
          backgroundColor: panelStrongColor,
          fillerColor: "rgba(96, 165, 250, 0.24)",
          borderColor: borderColor,
          handleStyle: {
            color: accentColor,
            borderColor: accentColor,
          },
          moveHandleSize: 12,
          dataBackground: {
            lineStyle: {
              color: accent2Color,
            },
            areaStyle: {
              color: "rgba(96, 165, 250, 0.16)",
            },
          },
          textStyle: {
            color: mutedColor,
          },
        },
      ],
      series: [{
        name: fieldName,
        type: "line",
        showSymbol: false,
        smooth: true,
        sampling: "lttb",
        lineStyle: { width: 2.5, color: accent2Color },
        areaStyle: {
          color: "rgba(96, 165, 250, 0.16)",
        },
        emphasis: { focus: "series" },
        data: seriesPoints,
        color: accent2Color,
      }],
    });
  }

  function renderInlinksPriorityPanels(fields, container) {
    if (!container) return;

    const processedSeries = buildPrioritySeries(fields, "batch.by_priority", "processed");
    const finalizedSeries = buildPrioritySeries(fields, "batch.by_priority", "finalized");
    const selectedSeries = buildPrioritySeries(fields, "batch.by_priority", "selected");

    const hasProcessed = processedSeries.priorities.some((priority) => (processedSeries.seriesByLevel[priority] || []).length > 0);
    const hasFinalized = finalizedSeries.priorities.some((priority) => (finalizedSeries.seriesByLevel[priority] || []).length > 0);
    const hasSelected = selectedSeries.priorities.some((priority) => (selectedSeries.seriesByLevel[priority] || []).length > 0);
    if (!hasProcessed && !hasFinalized && !hasSelected) {
      container.remove();
      return;
    }

    const items = [
      {
        title: "Processing throughput",
        subtitle: "Targets examined by priority",
        seriesByLevel: processedSeries.seriesByLevel,
      },
      {
        title: "Finalization throughput",
        subtitle: "Targets finalized by priority",
        seriesByLevel: finalizedSeries.seriesByLevel,
      },
    ].filter((item) => Object.values(item.seriesByLevel).some((points) => Array.isArray(points) && points.length > 0));

    if (!items.length) {
      container.remove();
      return;
    }

    const levelLabels = {
      unknown_active: "active unknown",
      unknown_idle: "idle unknown",
      refresh_active: "active refresh",
      refresh_idle: "idle refresh",
    };
    const levelColors = {
      unknown_active: "rgba(37, 99, 235, 0.56)",
      unknown_idle: "rgba(96, 165, 250, 0.40)",
      refresh_active: "rgba(15, 118, 110, 0.56)",
      refresh_idle: "rgba(20, 184, 166, 0.34)",
    };

    container.innerHTML = "";
    const wrapper = document.createElement("section");
    wrapper.className = "cache-breakdown-section";
    wrapper.innerHTML = `
      <div class="section-head">
        <div class="title">Inlinks priority flow</div>
        <div class="subtitle">Processing and finalization stacked by priority bucket. Click a card to zoom.</div>
      </div>
      <div class="cache-breakdown-grid"></div>
      <div class="zoom-panel">
        <div class="zoom-placeholder">Click a card to zoom.</div>
      </div>
    `;
    container.appendChild(wrapper);
    const grid = wrapper.querySelector(".cache-breakdown-grid");
    const zoomPanel = wrapper.querySelector(".zoom-panel");

    for (const item of items) {
      const card = document.createElement("section");
      card.className = "stacked-chart-card";
      card.title = `${item.title} - ${item.subtitle}`;
      card.innerHTML = `
        <div class="stacked-chart-head">
          <div class="title">${escapeHtml(item.title)}</div>
          <div class="subtitle">${escapeHtml(item.subtitle)}</div>
        </div>
        <div class="stacked-chart"></div>
      `;
      grid.appendChild(card);

      const chartOptions = {
        title: item.title,
        subtitle: item.subtitle,
        seriesByLevel: item.seriesByLevel,
        levelOrder: processedSeries.priorities,
        levelLabels,
        levelColors,
        showLegend: false,
        showDataZoom: false,
        compact: true,
      };
      renderStackedAreaChart(card.querySelector(".stacked-chart"), chartOptions);
      card.addEventListener("click", () => {
        if (!zoomPanel) return;
        zoomPanel.innerHTML = `
          <div class="zoom-title">
            <span>${escapeHtml(item.title)}</span>
            <span>${escapeHtml(item.subtitle)}</span>
          </div>
          <div class="zoom-chart"></div>
        `;
        const zoomChartNode = zoomPanel.querySelector(".zoom-chart");
        renderStackedAreaChart(zoomChartNode, {
          ...chartOptions,
          showLegend: true,
          showDataZoom: true,
          compact: false,
        });
      });
    }
  }

  function renderMetricTile(workerName, fieldName, points, zoomNode) {
    const tile = document.createElement("button");
    tile.type = "button";
    tile.className = "metric-tile";
    const description = descriptionForField(fieldName);
    const seriesPoints = toSeriesPoints(points);
    const latest = latestPoint(points);
    const latestValue = latest ? formatMetricValue(latest[1]) : "n/a";
    const latestStamp = latest ? formatLocalTimestamp(latest[0]) : "n/a";
    tile.title = `${fieldName}\n${description}`;
    tile.innerHTML = `
      <div class="tile-head">
        <div class="label-block">
          <div class="field">${escapeHtml(fieldName)}</div>
          <div class="subtitle">${escapeHtml(description)}</div>
        </div>
        <div class="value">${escapeHtml(latestValue)}</div>
      </div>
      <div class="sparkline-shell">
        <div class="scale-y" aria-hidden="true">
          <span>${escapeHtml(seriesPoints.length ? formatMetricValue(Math.max(...seriesPoints.map((point) => point[1]))) : "n/a")}</span>
          <span>${escapeHtml(seriesPoints.length ? formatMetricValue(seriesPoints.reduce((sum, point) => sum + point[1], 0) / seriesPoints.length) : "n/a")}</span>
          <span>${escapeHtml(seriesPoints.length ? formatMetricValue(Math.min(...seriesPoints.map((point) => point[1]))) : "n/a")}</span>
        </div>
        <div class="sparkline">${renderSparklineSvg(seriesPoints)}</div>
      </div>
      <div class="stamp">${escapeHtml(latestStamp)}</div>
    `;
    tile.addEventListener("click", () => {
      if (zoomNode) {
        zoomNode.innerHTML = `
          <div class="zoom-title">
            <span>${escapeHtml(workerName)} · ${escapeHtml(fieldName)}</span>
            <span>${escapeHtml(description)}</span>
          </div>
          <div class="zoom-chart"></div>
        `;
        const chartNode = zoomNode.querySelector(".zoom-chart");
        renderZoomChart(chartNode, workerName, fieldName, points);
      }
    });
    return tile;
  }

  function renderWorkerSection(workerName, fields, period) {
    const workerGrid = qs("worker-grid");
    if (!workerGrid) return;

    const fieldEntries = Object.entries(fields || {})
      .map(([fieldName, points]) => ({
        fieldName,
        points: Array.isArray(points) ? points : [],
      }))
      .filter((entry) => {
        if (entry.points.length === 0) return false;
        if (workerName === "inlinks" && entry.fieldName.startsWith("batch.by_priority.")) {
          return false;
        }
        return true;
      });

    const details = document.createElement("details");
    details.className = "worker-section";
    details.open = workerGrid.childElementCount === 0;
    details.innerHTML = `
      <summary class="worker-summary">
        <div class="worker-title">
          <h2>${escapeHtml(workerName)}</h2>
          <div class="meta">${fieldEntries.length} metric(s) over ${escapeHtml(period)}</div>
        </div>
        <div class="meta">Click a tile to zoom</div>
      </summary>
      <div class="worker-body">
        <div class="inlinks-priority-grid hidden"></div>
        <div class="cache-breakdown-grid hidden"></div>
        <div class="metric-grid"></div>
        <div class="zoom-panel">
          <div class="zoom-placeholder">Click a square to zoom.</div>
        </div>
      </div>
    `;

    const metricGrid = details.querySelector(".metric-grid");
    const inlinksPriorityGrid = details.querySelector(".inlinks-priority-grid");
    const cacheBreakdownGrid = details.querySelector(".cache-breakdown-grid");
    if (workerName === "inlinks" && inlinksPriorityGrid) {
      inlinksPriorityGrid.classList.remove("hidden");
      renderInlinksPriorityPanels(fields, inlinksPriorityGrid);
    } else if (inlinksPriorityGrid) {
      inlinksPriorityGrid.remove();
    }
    if (workerName === "cache" && cacheBreakdownGrid) {
      cacheBreakdownGrid.classList.remove("hidden");
      renderCacheBreakdownPanels(fields, cacheBreakdownGrid);
      if (metricGrid) {
        metricGrid.remove();
      }
    } else if (cacheBreakdownGrid) {
      cacheBreakdownGrid.remove();
    }
    const zoomNode = details.querySelector(".zoom-panel");
    if (metricGrid && zoomNode) {
      if (!fieldEntries.length) {
        metricGrid.innerHTML = '<div class="zoom-placeholder">No numeric metrics in this worker window.</div>';
      } else {
        for (const { fieldName, points } of fieldEntries) {
          metricGrid.appendChild(renderMetricTile(workerName, fieldName, points, zoomNode));
        }
      }
    }

    workerGrid.appendChild(details);
  }

  async function loadObservability() {
    const summary = qs("summary");
    const workerGrid = qs("worker-grid");
    if (summary) {
      summary.innerHTML = "";
    }
    if (workerGrid) {
      disposeExistingCharts();
      workerGrid.innerHTML = "";
    }
    hideEmptyState();

    try {
      const period = periodValue();
      selectedPeriod = period;
      const response = await fetch(`/api/observability?period=${encodeURIComponent(period)}`);
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const payload = await response.json();
      const fields = payload.fields && typeof payload.fields === "object" ? payload.fields : {};
      const workers = payload.workers && typeof payload.workers === "object" ? payload.workers : {};
      const workerEntries = Object.entries(workers);
      const metrics = Array.isArray(payload.metrics) ? payload.metrics : [];
      metricMetadata = new Map(
        metrics
          .filter((metric) => metric && typeof metric.field === "string")
          .map((metric) => [metric.field, String(metric.description || "")])
      );

      setSummaryCard("Window", payload.period_label || payload.period || period);
      setSummaryCard("Workers", String(workerEntries.length));
      setSummaryCard("Fields", String(Object.keys(fields).length));
      setSummaryCard("Since", payload.since ? new Date(payload.since * 1000).toISOString() : "n/a");
      setSummaryCard("Until", payload.until ? new Date(payload.until * 1000).toISOString() : "n/a");

      if (!workerEntries.length) {
        renderEmptyState("No worker snapshots found for the selected window.");
        return;
      }

      const periodLabel = payload.period || period;
      for (const [workerName, workerFields] of workerEntries) {
        renderWorkerSection(workerName, workerFields, periodLabel);
      }
    } catch (error) {
      renderEmptyState(`Unable to load observability data: ${error.message}`);
    }
  }

  function scheduleRefresh() {
    clearRefreshTimer();
    const autorefresh = qs("autorefresh");
    if (!autorefresh || !autorefresh.checked) {
      return;
    }
    refreshTimer = setTimeout(async () => {
      await loadObservability();
      scheduleRefresh();
    }, 15000);
  }

  async function refreshObservability() {
    await loadObservability();
    scheduleRefresh();
  }

  window.addEventListener("resize", () => {
    for (const chart of activeCharts) {
      chart.resize();
    }
  }, { passive: true });

  window.addEventListener("DOMContentLoaded", () => {
    const periodSelect = qs("period");
    const refreshButton = qs("refresh");
    const autorefresh = qs("autorefresh");
    if (periodSelect) {
      periodSelect.value = selectedPeriod;
      periodSelect.addEventListener("change", async () => {
        await refreshObservability();
      });
    }
    if (refreshButton) {
      refreshButton.addEventListener("click", async () => {
        await refreshObservability();
      });
    }
    if (autorefresh) {
      autorefresh.addEventListener("change", () => {
        scheduleRefresh();
      });
    }
    refreshObservability();
  });
})();

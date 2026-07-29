function buildBadgeSvg(suffix) {
    return `
<svg baseProfile="full" height="18px" version="1.1" viewBox="0 0 36 36" width="18px"
     xmlns="http://www.w3.org/2000/svg">
  <defs>
    <clipPath id="badge-n3-half-top-${suffix}">
      <rect x="0" y="0" width="36" height="18" />
    </clipPath>
    <clipPath id="badge-n3-half-bottom-${suffix}">
      <rect x="0" y="18" width="36" height="18" />
    </clipPath>
  </defs>
  <style>
    [data-field][data-value="unknown"] { stroke: grey; fill: grey; }
    [data-field][data-value="none"] { stroke: var(--level-none); fill: var(--level-none); }
    [data-field][data-value="weak"] { stroke: var(--level-weak); fill: var(--level-weak); }
    [data-field][data-value="strong"] { stroke: var(--level-strong); fill: var(--level-strong); }
    [data-field="n"][data-value="partial-weak"],
    [data-field="n"][data-value="partial-strong"] { display: none; }
    [data-field="n"][data-value="none"],
    [data-field="n"][data-value="weak"],
    [data-field="n"][data-value="strong"] { fill: none; }
    [data-field="n_ring"] { display: none; }
    [data-field="n_ring"][data-value="partial-weak"],
    [data-field="n_ring"][data-value="partial-strong"] { display: block; }
    [data-field="n_ring"][data-value="partial-weak"] .n-ring-none,
    [data-field="n_ring"][data-value="partial-strong"] .n-ring-none { stroke: var(--level-none); fill: none; }
    [data-field="n_ring"][data-value="partial-weak"] .n-ring-second { stroke: var(--level-partial-weak-second); fill: none; }
    [data-field="n_ring"][data-value="partial-strong"] .n-ring-second { stroke: var(--level-partial-strong-second); fill: none; }
    [data-field="n3"][data-value="partial-weak"],
    [data-field="n3"][data-value="partial-strong"] { display: none; }
    [data-field="n3_halves"] { display: none; }
    [data-field="n3_halves"][data-value="partial-weak"],
    [data-field="n3_halves"][data-value="partial-strong"] { display: block; }
    [data-field="n3_halves"][data-value="partial-weak"] .n3-half-none,
    [data-field="n3_halves"][data-value="partial-strong"] .n3-half-none { stroke: var(--level-none); fill: var(--level-none); }
    [data-field="n3_halves"][data-value="partial-weak"] .n3-half-second { stroke: var(--level-partial-weak-second); fill: var(--level-partial-weak-second); }
    [data-field="n3_halves"][data-value="partial-strong"] .n3-half-second { stroke: var(--level-partial-strong-second); fill: var(--level-partial-strong-second); }
    [data-field="redirect"] { display: none; }
    [data-field="redirect"][data-value="true"] { display: block; }
    [data-field="is_deleted"] { display: none; }
    [data-field="is_deleted"][data-value="true"] { display: block; }
    svg[data-deleted="true"] [data-field="normal"] { display: none; }
    svg[data-deleted="true"] [data-field="is_deleted"] { display: block; }
    [data-field="has_claims"][data-value="unknown"] { display: none; }
    [data-field="has_claims"][data-value="true"] { display: none; }
    [data-field="has_claims"][data-value="false"] { display: block; }
  </style>
  <g data-field="normal" data-value="unknown">
    <circle cx="18.0" cy="18.0" r="14.66" fill="none" stroke-width="3.8"
           data-field="n" data-value="unknown"/>
    <g data-field="n_ring" data-value="unknown">
      <path class="n-ring-none" d="M18.00 3.34 A14.66 14.66 0 0 1 32.66 18.00" fill="none" stroke-width="3.8" stroke-linecap="butt" />
      <path class="n-ring-second" d="M32.66 18.00 A14.66 14.66 0 0 1 18.00 32.66" fill="none" stroke-width="3.8" stroke-linecap="butt" />
      <path class="n-ring-none" d="M18.00 32.66 A14.66 14.66 0 0 1 3.34 18.00" fill="none" stroke-width="3.8" stroke-linecap="butt" />
      <path class="n-ring-second" d="M3.34 18.00 A14.66 14.66 0 0 1 18.00 3.34" fill="none" stroke-width="3.8" stroke-linecap="butt" />
    </g>
    <path data-field="n1" d="M12.78,28.04 A11.32,11.32 0 0,1 12.78,7.96 Z" data-value="unknown" />
    <g data-field="n3_halves" data-value="unknown">
      <path class="n3-half-none" clip-path="url(#badge-n3-half-top-${suffix})"
            d="M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z" data-value="unknown" />
      <path class="n3-half-second" clip-path="url(#badge-n3-half-bottom-${suffix})"
            d="M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z" data-value="unknown" />
    </g>
    <path data-field="n3" d="M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z" data-value="unknown" />
    <path data-field="n2a" d="M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,17.28 L14.1,17.28 Z"
           data-value="unknown" />
    <path data-field="n2b" d="M14.1,28.62 A11.32,11.32 0 0,0 21.9,28.62 L21.9,18.72 L14.1,18.72 Z"
          data-value="unknown" />
    <path data-field="has_claims" d="M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,28.62 A11.32,11.32 0 0,1 14.1,28.62 Z"
          fill="#fff" data-value="unknown" />
    <path data-field="redirect" data-value="unknown"
          d="M1.5 15.0 H15.2 V10.5 L23.22 18.0 L15.2 25.5 V21.0 H1.5 Z"
          fill="#6a1b9a" />
  </g>
  <g data-field="is_deleted" data-value="unknown" fill="none" stroke="#c62828" stroke-width="4.2" stroke-linecap="round">
    <path d="M7 7 L29 29" />
    <path d="M29 7 L7 29" />
  </g>
</svg>
`;
}

const DEFAULT_NOTABILITY_API_BASE = "https://wd-notability.toolforge.org";
// const DEFAULT_NOTABILITY_API_BASE = "http://localhost:12345";

function normalizeNotabilityApiBase(value) {
    if (typeof value !== "string") {
        return DEFAULT_NOTABILITY_API_BASE;
    }

    const trimmed = value.trim();
    if (!trimmed) {
        return DEFAULT_NOTABILITY_API_BASE;
    }

    return trimmed.replace(/\/+$/, "");
}

function resolveNotabilityApiBase() {
    const configuredBase = typeof window.NOTABILITY_CONFIG === "object" && window.NOTABILITY_CONFIG && typeof window.NOTABILITY_CONFIG.apiBase === "string"
        ? window.NOTABILITY_CONFIG.apiBase
        : DEFAULT_NOTABILITY_API_BASE;

    return normalizeNotabilityApiBase(configuredBase);
}

const NOTABILITY_API_BASE = resolveNotabilityApiBase();

let badgeInstanceCounter = 0;

(function (mw, $, wb) {
    "use strict";
    
    const knownQIDs = new Set();
    const qidReasons = new Map();
    const qidBadgeData = new Map();
    const qidBadgeElements = new Map();
    const creationQIDs = new Set();
    let creationSummarySignature = "";
    const REASON_PRIORITY = {
        text: 0,
        use: 1,
        edit: 3,
        create: 4,
        page: 5,
    };
    let eventSource = null;
    let resubscribeTimer = null;
    let subscribeInFlight = false;
    let currentSubscriptionId = null;
    let currentEventId = 0;
    let focusHandlersInstalled = false;

    function apiUrl(path) {
        return `${NOTABILITY_API_BASE}${path}`;
    }

    function injectNotabilityStyles() {
        const style = document.createElement("style");
        style.textContent = `
        :root {
            --level-none: #c45b63;
            --level-weak: #c78a47;
            --level-strong: #6f9f74;
            --level-partial-label: #b97a56;
            --level-partial-weak-second: #d19b59;
            --level-partial-strong-second: #7ea87f;
            --creation-strong: #6f9f74;
            --creation-weak: #c78a47;
            --creation-none: #c45b63;
            --creation-unknown: #8a9098;
            --creation-partial-weak: #d19b59;
            --creation-partial-strong: #7ea87f;
            --creation-empty: #ffffff;
        }
        .notability-badge {
            width: 18px;
            height: 18px;
            line-height: 0;
            overflow: visible;
            position: relative;
        }
        .notability-badge[data-loaded="false"] {
            visibility: hidden;
        }
        .notability-badge-hovercard {
            position: absolute;
            left: 50%;
            top: calc(100% + 8px);
            transform: translateX(-50%) translateY(4px);
            min-width: 17rem;
            max-width: min(22rem, 90vw);
            padding: .6rem .7rem;
            border: 1px solid var(--border, rgba(0, 0, 0, .18));
            border-radius: 12px;
            background: var(--panel, #fff);
            color: var(--text, #111);
            box-shadow: 0 12px 30px rgba(0, 0, 0, .22);
            opacity: 0;
            visibility: hidden;
            pointer-events: none;
            transition: opacity .12s ease, transform .12s ease, visibility .12s ease;
            z-index: 20;
        }
        .notability-badge:hover .notability-badge-hovercard,
        .notability-badge:focus-visible .notability-badge-hovercard,
        .notability-badge:focus-within .notability-badge-hovercard {
            opacity: 1;
            visibility: visible;
            transform: translateX(-50%) translateY(0);
        }
        .notability-badge-hovercard-body {
            display: flex;
            flex-direction: column;
            gap: .14rem;
        }
        .notability-badge-hovercard-row {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            gap: .75rem;
            padding-left: calc(var(--badge-hovercard-depth, 0) * .85rem);
            line-height: 1.1;
        }
        .notability-badge-hovercard-label {
            font-size: .84rem;
            font-weight: 600;
            display: flex;
            flex-direction: column;
            align-items: flex-start;
            gap: .08rem;
        }
        .notability-badge-hovercard-label-title {
            line-height: 1.05;
        }
        .notability-badge-hovercard-label-subtitle {
            font-size: .66rem;
            font-weight: 700;
            letter-spacing: .08em;
            line-height: 1;
            text-transform: uppercase;
            color: var(--muted, #666);
        }
        .notability-badge-hovercard-value {
            font-size: .84rem;
            font-weight: 700;
            text-align: right;
            white-space: nowrap;
        }
        .notability-badge-hovercard-value.level-none { color: var(--level-none); }
        .notability-badge-hovercard-value.level-weak { color: var(--level-weak); }
        .notability-badge-hovercard-value.level-strong { color: var(--level-strong); }
        .notability-badge-hovercard-value.level-unknown { color: var(--muted, #666); }
        .notability-badge-hovercard-value .level-partial-prefix,
        .level-partial-prefix { color: var(--level-none); font-weight: 700; }
        .notability-badge-hovercard-value .level-partial-weak .level-weak,
        .level-partial-weak .level-weak { color: var(--level-weak); }
        .notability-badge-hovercard-value .level-partial-strong .level-strong,
        .level-partial-strong .level-strong { color: var(--level-strong); }
        .notability-badge svg {
            display: block;
            width: 24px;
            height: 24px;
            max-width: none;
            position: absolute;
            left: -3px;
            top: -3px;
        }
        .notability-creation-summary {
            display: none;
            margin: 0 0 .35rem;
        }
        .notability-creation-summary[data-visible="true"] {
            display: block;
        }
        .notability-creation-summary-bar {
            display: block;
            width: 100%;
            height: 12px;
            overflow: hidden;
        `;
        document.head.appendChild(style);
    }

    function normalizeReason(reason) {
        return Object.prototype.hasOwnProperty.call(REASON_PRIORITY, reason) ? reason : "page";
    }

    function rememberQID(qid, reason) {
        const normalizedReason = normalizeReason(reason);
        const previousReason = qidReasons.get(qid);
        const isNewQID = !knownQIDs.has(qid);
        knownQIDs.add(qid);

        if (
            !previousReason ||
            REASON_PRIORITY[normalizedReason] > REASON_PRIORITY[previousReason]
        ) {
            qidReasons.set(qid, normalizedReason);
            return true;
        }

        return isNewQID;
    }

    function levelText(value) {
        return String(value == null ? "unknown" : value).toUpperCase();
    }

    function levelTooltipText(value) {
        return levelText(value) === "UNKNOWN" ? "UNKNOWN / PENDING" : levelText(value);
    }

    function boolText(value) {
        if (value === true) return "YES";
        if (value === false) return "NO";
        return "UNKNOWN";
    }

    function qidText(value) {
        if (typeof value === "number" && Number.isFinite(value)) return `Q${value}`;
        if (typeof value === "string") {
            const text = value.trim();
            if (text) return /^Q\d+$/.test(text) ? text.toUpperCase() : text;
        }
        return "UNKNOWN";
    }

    function countText(data, field) {
        if (!data || data.content_last_revid == null) return "UNKNOWN";
        const numeric = Number(data[field]);
        if (!Number.isFinite(numeric)) return "UNKNOWN";
        return numeric > 0 ? "YES" : "NO";
    }

    function rememberCreationQID(qid) {
        if (!qid) return false;
        const before = creationQIDs.size;
        creationQIDs.add(qid);
        return creationQIDs.size !== before;
    }

    function levelValue(data, field) {
        if (!data || typeof data !== "object") return undefined;
        if (data[field] !== undefined) return data[field];
        const levels = data.levels && typeof data.levels === "object" ? data.levels : null;
        if (!levels) return undefined;
        const levelKey = {
            n: "N",
            n1: "N1",
            n2a: "N2a",
            n2b: "N2b",
            n3: "N3",
            n3_inlinks: "N3_inlinks",
            n3_osm: "N3_osm",
            n3_wikisub: "N3_wikisub",
            n3_sdc: "N3_sdc",
        }[field];
        return levelKey ? levels[levelKey] : undefined;
    }

    function normalizeCreationLevel(value) {
        const level = String(value == null ? "UNKNOWN" : value).toUpperCase();
        return Object.prototype.hasOwnProperty.call({
            NONE: true,
            "PARTIAL-WEAK": true,
            "PARTIAL-STRONG": true,
            WEAK: true,
            UNKNOWN: true,
            STRONG: true,
        }, level) ? level : "UNKNOWN";
    }

    function creationPartialBucket(data) {
        const n2a = normalizeCreationLevel(levelValue(data, "n2a"));
        const n2b = normalizeCreationLevel(levelValue(data, "n2b"));
        const activeLevels = [n2a, n2b].filter((level) => level === "WEAK" || level === "STRONG");

        if (activeLevels.length !== 1) return null;
        return activeLevels[0] === "STRONG" ? "PARTIAL-STRONG" : "PARTIAL-WEAK";
    }

    function creationBucketOf(data) {
        if (!data || typeof data !== "object") return "UNKNOWN";
        if (data.is_deleted === true) return "DELETED";
        if (data.is_redirect === true) return "REDIRECT";

        const notability = normalizeCreationLevel(levelValue(data, "n"));
        if (notability === "NONE") {
            const partial = creationPartialBucket(data);
            if (partial) return partial;
            const sitelinksCount = Number(data.has_sitelinks_count);
            const claimsCount = Number(data.has_claims_count);
            const empty = data.content_last_revid != null && Number.isFinite(sitelinksCount) && Number.isFinite(claimsCount) && sitelinksCount === 0 && claimsCount === 0;
            if (empty) return "EMPTY";
        }
        return notability;
    }

    function bucketLabel(bucket) {
        return String(bucket || "").replaceAll("_", "-");
    }

    function formatPercent(count, total) {
        if (!total) return "0%";
        const value = (count / total) * 100;
        const rounded = Math.round(value * 10) / 10;
        return `${String(rounded).replace(/\.0$/, "")}%`;
    }

    function getCreationSummaryMount() {
        return document.querySelector("#bodyContent") || document.querySelector("#mw-content-text") || document.body;
    }

    function ensureCreationSummaryPanel() {
        const existing = document.querySelector(".notability-creation-summary");
        if (existing) return existing;

        const mount = getCreationSummaryMount();
        if (!mount) return null;

        const panel = document.createElement("section");
        panel.className = "notability-creation-summary";
        panel.innerHTML = '<svg class="notability-creation-summary-bar" aria-hidden="true" focusable="false" role="img" preserveAspectRatio="none" viewBox="0 0 100 12"><title></title></svg>';

        const firstChild = mount.firstElementChild;
        if (firstChild) {
            mount.insertBefore(panel, firstChild);
        } else {
            mount.appendChild(panel);
        }

        return panel;
    }

    function updateCreationSummary() {
        const counts = {
            DELETED: 0,
            REDIRECT: 0,
            EMPTY: 0,
            NONE: 0,
            "PARTIAL-WEAK": 0,
            "PARTIAL-STRONG": 0,
            WEAK: 0,
            UNKNOWN: 0,
            STRONG: 0,
        };
        let total = 0;
        for (const qid of creationQIDs) {
            total += 1;
            const bucket = creationBucketOf(qidBadgeData.get(qid));
            counts[bucket] = (counts[bucket] || 0) + 1;
        }

        const signature = `${total}|${["DELETED", "REDIRECT", "EMPTY", "NONE", "PARTIAL-WEAK", "PARTIAL-STRONG", "WEAK", "UNKNOWN", "STRONG"].map((bucket) => counts[bucket] || 0).join(",")}`;
        if (signature === creationSummarySignature) {
            return;
        }
        creationSummarySignature = signature;

        if (!total) {
            const existing = document.querySelector(".notability-creation-summary");
            if (existing) {
                existing.dataset.visible = "false";
                const existingBar = existing.querySelector(".notability-creation-summary-bar");
                if (existingBar) {
                    existingBar.innerHTML = "<title></title>";
                    existingBar.removeAttribute("title");
                    existingBar.removeAttribute("aria-label");
                }
            }
            return;
        }

        const panel = ensureCreationSummaryPanel();
        if (!panel) return;

        const bar = panel.querySelector(".notability-creation-summary-bar");
        if (!bar) return;

        bar.innerHTML = "";
        const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
        panel.dataset.visible = "true";
        const summaryLines = [`Creations shown: ${total}`];
        let offset = 0;
        const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
        const partialWeakPattern = document.createElementNS("http://www.w3.org/2000/svg", "pattern");
        partialWeakPattern.setAttribute("id", "notability-creation-partial-weak");
        partialWeakPattern.setAttribute("patternUnits", "userSpaceOnUse");
        partialWeakPattern.setAttribute("width", "16");
        partialWeakPattern.setAttribute("height", "16");
        partialWeakPattern.setAttribute("patternTransform", "rotate(135)");
        const partialWeakBase = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        partialWeakBase.setAttribute("width", "16");
        partialWeakBase.setAttribute("height", "16");
        partialWeakBase.setAttribute("fill", "#c45b63");
        const partialWeakStripe = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        partialWeakStripe.setAttribute("x", "0");
        partialWeakStripe.setAttribute("width", "8");
        partialWeakStripe.setAttribute("height", "16");
        partialWeakStripe.setAttribute("fill", "#d19b59");
        partialWeakPattern.append(partialWeakBase, partialWeakStripe);

        const partialStrongPattern = document.createElementNS("http://www.w3.org/2000/svg", "pattern");
        partialStrongPattern.setAttribute("id", "notability-creation-partial-strong");
        partialStrongPattern.setAttribute("patternUnits", "userSpaceOnUse");
        partialStrongPattern.setAttribute("width", "16");
        partialStrongPattern.setAttribute("height", "16");
        partialStrongPattern.setAttribute("patternTransform", "rotate(135)");
        const partialStrongBase = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        partialStrongBase.setAttribute("width", "16");
        partialStrongBase.setAttribute("height", "16");
        partialStrongBase.setAttribute("fill", "#c45b63");
        const partialStrongStripe = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        partialStrongStripe.setAttribute("x", "0");
        partialStrongStripe.setAttribute("width", "8");
        partialStrongStripe.setAttribute("height", "16");
        partialStrongStripe.setAttribute("fill", "#7ea87f");
        partialStrongPattern.append(partialStrongBase, partialStrongStripe);
        defs.append(partialWeakPattern, partialStrongPattern);
        bar.appendChild(defs);

        for (const bucket of ["DELETED", "REDIRECT", "EMPTY", "NONE", "PARTIAL-WEAK", "PARTIAL-STRONG", "WEAK", "UNKNOWN", "STRONG"]) {
            const count = counts[bucket] || 0;
            if (!count) continue;
            const segment = document.createElementNS("http://www.w3.org/2000/svg", "rect");
            segment.setAttribute("x", String(offset));
            segment.setAttribute("y", "0");
            segment.setAttribute("width", String((count / total) * 100));
            segment.setAttribute("height", "12");
            segment.setAttribute("shape-rendering", "geometricPrecision");
            if (bucket === "PARTIAL-WEAK") {
                segment.setAttribute("fill", "url(#notability-creation-partial-weak)");
            } else if (bucket === "PARTIAL-STRONG") {
                segment.setAttribute("fill", "url(#notability-creation-partial-strong)");
            } else if (bucket === "DELETED") {
                segment.setAttribute("fill", "#000");
            } else if (bucket === "REDIRECT") {
                segment.setAttribute("fill", "#7b1fa2");
            } else if (bucket === "EMPTY") {
                segment.setAttribute("fill", "#fff");
                segment.setAttribute("stroke", "#d7dbe1");
                segment.setAttribute("stroke-width", "0.75");
            } else if (bucket === "UNKNOWN") {
                segment.setAttribute("fill", "#8a9098");
            } else if (bucket === "NONE") {
                segment.setAttribute("fill", "#c45b63");
            } else if (bucket === "WEAK") {
                segment.setAttribute("fill", "#c78a47");
            } else if (bucket === "STRONG") {
                segment.setAttribute("fill", "#6f9f74");
            }
            bar.appendChild(segment);
            offset += (count / total) * 100;
            summaryLines.push(`${bucketLabel(bucket)}: ${count} (${formatPercent(count, total)})`);
        }
        const summaryText = summaryLines.join("\n");
        title.textContent = summaryText;
        bar.appendChild(title);
        bar.title = summaryText;
        bar.setAttribute("aria-label", summaryText);
    }

    function mutationTouchesCreationSummary(mutations) {
        for (const mutation of mutations) {
            const nodes = [
                mutation.target,
                ...Array.from(mutation.addedNodes || []),
                ...Array.from(mutation.removedNodes || []),
            ];
            for (const node of nodes) {
                if (!node || typeof node.closest !== "function") continue;
                if (node.closest(".notability-creation-summary")) {
                    return true;
                }
            }
        }
        return false;
    }

    function formatCreationTime(value) {
        if (!value) return "";
        if (typeof value === "number" && Number.isFinite(value)) {
            const d = new Date(value < 1e12 ? value * 1000 : value);
            return Number.isFinite(d.getTime()) ? d.toISOString().replace(/\.\d{3}Z$/, "Z") : String(value);
        }
        const text = String(value).trim();
        if (/^\d+$/.test(text)) {
            const numeric = Number(text);
            if (Number.isFinite(numeric)) {
                const d = new Date(text.length <= 10 ? numeric * 1000 : numeric);
                if (Number.isFinite(d.getTime())) {
                    return d.toISOString().replace(/\.\d{3}Z$/, "Z");
                }
            }
        }
        const match = text.match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/);
        if (!match) return text;
        return `${match[1]}-${match[2]}-${match[3]} ${match[4]}:${match[5]}:${match[6]} UTC`;
    }

    function buildBadgeTooltip(data) {
        const report = data && typeof data === "object" ? data : {};
        const levels = report.levels && typeof report.levels === "object" ? report.levels : report;
        const snapshot = report.cached_snapshot && typeof report.cached_snapshot === "object"
            ? report.cached_snapshot
            : report;

        const lines = [
            `Overall: ${levelTooltipText(levels.N)}`,
            `N12 intrinsic: ${levelTooltipText(levels.N12)}`,
            `N1 sitelinks: ${levelTooltipText(levels.N1)}`,
            `N2: ${levelTooltipText(levels.N2)}`,
            `N2a identifiers: ${levelTooltipText(levels.N2a)}`,
            `N2b sources: ${levelTooltipText(levels.N2b)}`,
            `Sitelinks count: ${data.has_sitelinks_count ?? "UNKNOWN"}`,
            `Claims count: ${data.has_claims_count ?? "UNKNOWN"}`,
            `Content stale: ${boolText(snapshot.content_stale ?? report.content_stale)}`,
            `N3 extrinsic: ${levelTooltipText(levels.N3)}`,
            `N3_inlinks: ${levelTooltipText(levels.N3_inlinks)}`,
            `Inlinks count: ${data.inlinks_count ?? "UNKNOWN"}`,
            `N3_osm: ${levelTooltipText(levels.N3_osm)}`,
            `N3_sdc: ${levelTooltipText(levels.N3_sdc)}`,
            `N3_wikisub: ${levelTooltipText(levels.N3_wikisub)}`,
        ];
        if (data.is_deleted === true) {
            lines.push("Deleted: YES");
        }
        if (data.is_redirect === true) {
            lines.push(`Redirect target: ${qidText(data.redirect_target)}`);
        }
        if (snapshot.creator) {
            lines.push(`Creator: ${String(snapshot.creator)}`);
        }
        if (snapshot.creation_time || snapshot.creation_time_iso) {
            lines.push(`Created: ${formatCreationTime(snapshot.creation_time_iso || snapshot.creation_time)}`);
        }
        if (snapshot.last_updated || snapshot.last_updated_iso) {
            lines.push(`Last updated: ${formatCreationTime(snapshot.last_updated_iso || snapshot.last_updated)}`);
        }
        if (snapshot.inlinks_last_evaluated || snapshot.inlinks_last_evaluated_iso) {
            lines.push(`Modified: ${formatCreationTime(snapshot.inlinks_last_evaluated_iso || snapshot.inlinks_last_evaluated)}`);
        }
        return lines.join("\n");
    }

    function setBadgeTooltip(elt, data) {
        if (!elt) return;
        const tooltip = typeof data?.badge_tooltip === "string" && data.badge_tooltip
            ? data.badge_tooltip
            : buildBadgeTooltip(data);
        elt.setAttribute("aria-label", tooltip);
        const hovercard = elt.querySelector(".notability-badge-hovercard");
        if (hovercard) {
            hovercard.innerHTML = typeof data?.badge_hovercard === "string" ? data.badge_hovercard : "";
        }
    }

    function rememberBadgeData(qid, data) {
        if (!qid || !data || typeof data !== "object") return;
        qidBadgeData.set(qid, data);
    }

    function rememberBadgeElement(qid, badge) {
        if (!qid || !badge) return;
        let badges = qidBadgeElements.get(qid);
        if (!badges) {
            badges = new Set();
            qidBadgeElements.set(qid, badges);
        }
        badges.add(badge);
    }

    function isBadgeReady(data) {
        if (!data || typeof data !== "object") return false;
        if (data.is_deleted === true) return true;
        const contentLastRevid = data.content_last_revid;
        if (contentLastRevid == null) return false;
        return true;
    }

    function initializeBadgeFromCache(badge, qid) {
        if (!badge) return;
        const cachedData = qidBadgeData.get(qid);
        if (!cachedData) return;

        rememberBadgeData(qid, cachedData);
        const svg = badge.querySelector("svg");
        if (svg) {
            updateSVG(svg, cachedData);
        }
        setBadgeTooltip(badge, cachedData);
        if (isBadgeReady(cachedData)) {
            badge.dataset.loaded = "true";
        }
    }

    function setBadgeLoaded(badge) {
        if (!badge) return;
        badge.dataset.loaded = "true";
    }

    function addBadge(elt, qid, reason, options = {}) {
        if (elt.dataset.notabilityBadge === "true") return false;
        elt.dataset.notabilityBadge = "true";
        rememberQID(qid, reason);
        const badgeSuffix = `b${Date.now().toString(36)}${++badgeInstanceCounter}`;

        const wrapper = document.createElement("a");
        wrapper.innerHTML = buildBadgeSvg(badgeSuffix);
        wrapper.href = apiUrl(`/?qid=${encodeURIComponent(qid)}`);
        wrapper.target = "_blank";
        wrapper.rel = "noopener noreferrer";
        wrapper.setAttribute("data-qid", qid);
        wrapper.setAttribute("aria-label", "Open notability report");
        wrapper.classList.add("notability-badge");
        wrapper.style.display = "inline-block";
        wrapper.style.marginLeft = "4px";
        wrapper.style.verticalAlign = "middle";
        wrapper.style.width = "18px";
        wrapper.style.height = "18px";
        wrapper.style.lineHeight = "0";
        wrapper.style.overflow = "visible";
        wrapper.style.position = "relative";
        wrapper.dataset.loaded = "false";
        wrapper.insertAdjacentHTML("beforeend", '<div class="notability-badge-hovercard" role="tooltip" aria-hidden="true"><div class="notability-badge-hovercard-body"></div></div>');
        if (options.pageTitle === true) {
            wrapper.classList.add("notability-badge-page-title");
            wrapper.style.width = "22px";
            wrapper.style.height = "22px";
            wrapper.style.marginLeft = "6px";
            wrapper.style.verticalAlign = "baseline";
        }

        elt.after(wrapper);
        rememberBadgeElement(qid, wrapper);
        initializeBadgeFromCache(wrapper, qid);
        return true;
    }

    function getPageQID() {
        const entityId = mw.config.get("wbEntityId") || mw.config.get("wgRelevantPageName");
        if (typeof entityId !== "string") return null;

        const match = entityId.trim().match(/^Q\d+$/);
        return match ? match[0] : null;
    }

    function findPageTitleTarget(pageQID) {
        const titleSelectors = [
            ".wikibase-title-id",
            "#firstHeading .mw-page-title-main",
            "#firstHeading .mw-headline",
            "#firstHeading span",
            "#firstHeading a",
            "#firstHeading",
        ];

        for (const selector of titleSelectors) {
            const titleElements = document.querySelectorAll(selector);
            for (const elt of titleElements) {
                const text = typeof elt.textContent === "string" ? elt.textContent.trim() : "";
                if (selector === ".wikibase-title-id" || (pageQID && text.includes(pageQID))) {
                    return elt;
                }
            }
        }

        return null;
    }

    function extractQIDFromLink(elt) {
        if (!elt || typeof elt.getAttribute !== "function") return null;
        const rawHref = elt.getAttribute("href");
        if (typeof rawHref !== "string" || !rawHref) return null;

        let pathname = rawHref;
        try {
            pathname = new URL(rawHref, window.location.href).pathname;
        } catch (_err) {
            // Fall back to the raw href below.
        }

        const match = pathname.match(/\/wiki\/(Q\d+)(?:$|[?#])/i);
        return match ? match[1].toUpperCase() : null;
    }

    function getChangeListContainer(elt) {
        return elt.closest(
            ".mw-changeslist-line, .mw-changeslist-line-edit, .mw-history-line, " +
            "li.mw-contributions-list, .mw-contributions-list li"
        );
    }

    function isCreationContext(container) {
        if (!container) return false;

        const classText = container.className || "";
        if (/\b(mw-changeslist-line-new|mw-newpages-pagename|newpage)\b/.test(classText)) {
            return true;
        }

        return Boolean(
            container.querySelector(
                ".mw-changeslist-line-new, .mw-newpages-pagename, .newpage, " +
                ".mw-tag-marker-new-page, abbr.newpage"
            )
        );
    }

    function inferEvaluationReason(elt, qid, pageQID) {
        if (qid === pageQID || elt.closest(".wikibase-title-id")) {
            return "page";
        }

        const changeListContainer = getChangeListContainer(elt);
        if (changeListContainer) {
            return isCreationContext(changeListContainer) ? "create" : "edit";
        }

        if (
            elt.closest(
                ".wikibase-statementview, .wikibase-snakview, " +
                ".wikibase-referenceview, .wikibase-listview"
            )
        ) {
            return "use";
        }

        return "text";
    }

    function updateSVG(svg, data) {
        const levels = data?.levels && typeof data.levels === "object" ? data.levels : {};
        const fields = ["n", "n_ring", "n1", "n2a", "n2b", "n3", "n3_halves", "is_deleted", "redirect", "has_claims"];
        svg.setAttribute("data-deleted", data?.is_deleted ? "true" : "false");
        for (const field of fields) {
            const el = svg.querySelector(`[data-field="${field}"]`);
            if (!el) continue;

            if (field === "has_claims" && (levelText(levels.N2a) === "UNKNOWN" || levelText(levels.N2b) === "UNKNOWN")) {
                el.setAttribute("data-value", "unknown");
                continue;
            }

            if (field === "has_claims") {
                const numeric = Number(data?.has_claims_count);
                if (!data || data.content_last_revid == null || !Number.isFinite(numeric)) {
                    el.setAttribute("data-value", "unknown");
                } else {
                    el.setAttribute("data-value", numeric > 0 ? "true" : "false");
                }
                continue;
            }

            let val = data[field];
            if (field === "n" || field === "n_ring" || field === "n1" || field === "n2a" || field === "n2b" || field === "n3" || field === "n3_halves") {
                const levelKey = {
                    n: "N",
                    n_ring: "N",
                    n1: "N1",
                    n2a: "N2a",
                    n2b: "N2b",
                    n3: "N3",
                    n3_halves: "N3",
                }[field];
                val = levels[levelKey];
            }
            el.setAttribute("data-value", val == null ? "unknown" : String(val));
        }
    }

    function updateBadges(qid, data) {
        rememberBadgeData(qid, data);
        const badges = qidBadgeElements.get(qid);
        const badgeList = badges ? Array.from(badges).filter((badge) => badge && badge.isConnected) : Array.from(document.querySelectorAll(`.notability-badge[data-qid="${qid}"]`));
        if (badges) {
            for (const badge of Array.from(badges)) {
                if (!badge || !badge.isConnected) {
                    badges.delete(badge);
                }
            }
        }
        for (const badge of badgeList) {
            const svg = badge.querySelector("svg");
            if (svg) {
                updateSVG(svg, data);
            }
            setBadgeTooltip(badge, data);
            if (isBadgeReady(data)) {
                setBadgeLoaded(badge);
            }
        }
        updateCreationSummary();
    }

    function applyCachedItems(items) {
        if (!Array.isArray(items)) return;

        for (const item of items) {
            if (!item || typeof item !== "object" || !item.qid) continue;
            updateBadges(item.qid, item);
        }
    }

    function subscribedQIDs() {
        return Array.from(knownQIDs);
    }

    function scheduleResubscribe(delayMs = 1000) {
        if (!shouldPoll()) return;
        if (knownQIDs.size === 0 || resubscribeTimer) return;

        resubscribeTimer = window.setTimeout(() => {
            resubscribeTimer = null;
            subscribeToKnownQIDs().catch((err) => {
                console.error("Notability resubscribe failed", err);
                scheduleResubscribe(5000);
            });
        }, delayMs);
    }

    async function subscribeToKnownQIDs() {
        if (!shouldPoll()) return;
        if (subscribeInFlight || knownQIDs.size === 0) return;
        subscribeInFlight = true;

        try {
            const body = new URLSearchParams({
                qids: JSON.stringify(subscribedQIDs()),
                session_id: currentSubscriptionId || "",
            });
            const res = await fetch(apiUrl(`/subscribe`), {
                method: "POST",
                body,
            });

            if (!res.ok) {
                throw new Error(`subscribe failed: ${res.status}`);
            }
            const payload = await res.json();
            console.debug("Notability subscribe response:", {
                subscription_id: payload.subscription_id,
            });
            if (payload.subscription_id && payload.subscription_id !== currentSubscriptionId) {
                currentEventId = 0;
            }
            if (payload.subscription_id) {
                currentSubscriptionId = payload.subscription_id;
            }

            if (payload.subscription_id) {
                listenForEvents(payload.subscription_id);
            } else if (eventSource) {
                eventSource.close();
                eventSource = null;
            }
        } catch (err) {
            scheduleResubscribe(5000);
            throw err;
        } finally {
            subscribeInFlight = false;
        }
    }

    function listenForEvents(subscriptionId) {
        if (!shouldPoll()) return;
        if (eventSource) {
            eventSource.close();
        }

        const afterEventId = currentEventId > 0 ? `?after_event_id=${encodeURIComponent(String(currentEventId))}` : "";
        const pollSeconds = "0.5";
        eventSource = new EventSource(apiUrl(`/api/pubsub/sessions/gadget/${subscriptionId}/events${afterEventId}${afterEventId ? "&" : "?"}poll_seconds=${encodeURIComponent(pollSeconds)}`));
        eventSource.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.event === "keepalive") return;
            if (data.event === "primed") {
                console.debug("Notability stream primed:", data.qid_count);
                return;
            }
            if (data.event === "stream_end") {
                eventSource.close();
                eventSource = null;
                scheduleResubscribe(1000);
                return;
            }
            if (!data.qid) return;
            if (data.event_id != null) {
                const eventId = Number(data.event_id);
                if (Number.isFinite(eventId)) {
                    currentEventId = eventId;
                }
            }
            updateBadges(data.qid, data);
        };

        eventSource.onerror = () => {
            console.warn("Notability stream disconnected");
            eventSource.close();
            eventSource = null;
            scheduleResubscribe(1000);
        };
    }

    function shouldPoll() {
        return document.visibilityState === "visible";
    }

    function stopPolling() {
        if (resubscribeTimer) {
            window.clearTimeout(resubscribeTimer);
            resubscribeTimer = null;
        }
        if (eventSource) {
            eventSource.close();
            eventSource = null;
        }
    }

    function resumePolling() {
        if (!shouldPoll()) return;
        if (knownQIDs.size === 0) return;
        subscribeToKnownQIDs().catch((err) => {
            console.error("Notability resume failed", err);
            scheduleResubscribe(5000);
        });
    }

    function setupFocusHandlers() {
        if (focusHandlersInstalled) return;
        focusHandlersInstalled = true;

        window.addEventListener("focus", resumePolling);
        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === "hidden") {
                stopPolling();
            } else {
                resumePolling();
            }
        });
    }

    function scanDOM() {
        let subscriptionChanged = false;

        const pageQID = getPageQID();
        if (pageQID) {
            subscriptionChanged = rememberQID(pageQID, "page") || subscriptionChanged;

            // Try to attach a badge to the visible page title so item-ish views keep the same affordance.
            const titleEl = findPageTitleTarget(pageQID);
            if (titleEl && addBadge(titleEl, pageQID, "page", { pageTitle: true })) {
                subscriptionChanged = true;
            }
        }
    
        // Pass 1: direct entity links like <a href="/wiki/Q123"> or absolute Wikidata URLs.
        const linkElements = document.querySelectorAll('a[href*="/wiki/Q"]');
        for (const elt of linkElements) {
            if (elt.dataset.notabilityBadge === "true") continue;

            const qid = extractQIDFromLink(elt);
            if (!qid) continue;
            if (qid === pageQID) continue;
            const reason = inferEvaluationReason(elt, qid, pageQID);
            if (reason === "create") {
                rememberCreationQID(qid);
            }
            subscriptionChanged = rememberQID(qid, reason) || subscriptionChanged;
            if (addBadge(elt, qid, reason)) {
                subscriptionChanged = true;
            }
        }
    
        // Pass 2: page title QID like <span class="wikibase-title-id">(Q123)</span>
        const titleElements = document.querySelectorAll('.wikibase-title-id');
        for (const elt of titleElements) {
            if (elt.dataset.notabilityBadge === "true") continue;
    
            const match = elt.textContent.match(/\b(Q\d+)\b/);
            if (!match) continue;

            const qid = match[1];
            if (qid === pageQID) continue;
            const reason = inferEvaluationReason(elt, qid, pageQID);
            if (reason === "create") {
                rememberCreationQID(qid);
            }
            subscriptionChanged = rememberQID(qid, reason) || subscriptionChanged;
            if (addBadge(elt, qid, reason)) {
                subscriptionChanged = true;
            }
        }
    
        if (subscriptionChanged) {
            subscribeToKnownQIDs().catch((err) => {
                console.error("Notability subscribe failed", err);
            });
        }

        updateCreationSummary();
    }

    function setupDOMObserver() {
        const observer = new MutationObserver((mutations) => {
            if (mutationTouchesCreationSummary(mutations)) {
                return;
            }
            scanDOM();
        });
        observer.observe(document.body, {
            childList: true,
            subtree: true,
        });
    }

    // Bootstraps the script once DOM is ready
    function init() {
        injectNotabilityStyles();
        setupFocusHandlers();
        scanDOM();
        setupDOMObserver();
    }

    $(init);
}(mediaWiki, jQuery, wikibase || {}));

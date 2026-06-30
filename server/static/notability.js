const BADGE_SVG = `
<svg baseProfile="full" height="18px" version="1.1" viewBox="0 0 36 36" width="18px"
     xmlns="http://www.w3.org/2000/svg">
  <defs>
<marker id="redirect-arrowhead"
        markerWidth="5"
        markerHeight="5"
        refX="0"
        refY="2.5"
        orient="auto"
        markerUnits="strokeWidth">
  <path d="M0,0 L0,5 L5,2.5 Z"
        fill="#6a1b9a" />
</marker>  </defs>
  <circle cx="18.0" cy="18.0" r="14.66" fill="none" stroke-width="3.8"
         data-field="n" data-value="unknown"/>
  <path data-field="n1" d="M12.78,28.04 A11.32,11.32 0 0,1 12.78,7.96 Z" data-value="unknown" />
  <path data-field="n3" d="M23.22,28.04 A11.32,11.32 0 0,0 23.22,7.96 Z" data-value="unknown" />
  <path data-field="n2a" d="M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,17.28 L14.1,17.28 Z"
         data-value="unknown" />
  <path data-field="n2b" d="M14.1,28.62 A11.32,11.32 0 0,0 21.9,28.62 L21.9,18.72 L14.1,18.72 Z"
        data-value="unknown" />
    <path data-field="has_claims" d="M14.1,7.38 A11.32,11.32 0 0,1 21.9,7.38 L21.9,28.62 A11.32,11.32 0 0,1 14.1,28.62 Z"
        fill="#fff"  data-value="unknown" />
  <g data-field="is_deleted" data-value="unknown">
    <rect x="0" y="0" width="6" height="6" fill="#c62828" />
    <rect x="30.0" y="0" width="6" height="6" fill="#c62828" />
    <rect x="0" y="30.0" width="6" height="6" fill="#c62828" />
    <rect x="30.0" y="30.0" width="6" height="6" fill="#c62828" />
  </g>
<g data-field="redirect" data-value="unknown"
   fill="none" stroke="#6a1b9a" stroke-width="2.1"
   stroke-linecap="round" marker-end="url(#redirect-arrowhead)">
  <path d="M30.70 25.33 A14.66 14.66 0 0 1 18.00 32.66" />
  <path d="M5.30 25.33 A14.66 14.66 0 0 1 5.30 10.67" />
  <path d="M18.00 3.34 A14.66 14.66 0 0 1 30.70 10.67" />
</g> 
</svg>
`;

const NOTABILITY_API_BASE = "http://localhost:12345";

(function (mw, $, wb) {
    "use strict";
    
    const knownQIDs = new Set();
    const qidReasons = new Map();
    const qidBadgeData = new Map();
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
    let teardownHandlersInstalled = false;

    function apiUrl(path) {
        return `${NOTABILITY_API_BASE}${path}`;
    }

    function injectNotabilityStyles() {
        const style = document.createElement("style");
        style.textContent = `
        .notability-badge [data-field][data-value="unknown"] { stroke: grey; fill: grey; }
        .notability-badge [data-field][data-value="none"]  { stroke: red;  fill: red; }
        .notability-badge [data-field][data-value="weak"]  { stroke: orange; fill: orange; }
        .notability-badge [data-field][data-value="strong"] { stroke: green; fill: green; }
        .notability-badge [data-field="is_deleted"] { display: none; }
        .notability-badge [data-field="is_deleted"][data-value="true"] { display: block; }
        .notability-badge [data-field="redirect"] { display: none; }
        .notability-badge [data-field="redirect"][data-value="true"] { display: block; }
        .notability-badge [data-field="has_claims"][data-value="true"] { display: none; }
        .notability-badge [data-field="has_claims"][data-value="false"] { display: block; }
        .notability-badge {
            width: 18px;
            height: 18px;
            line-height: 0;
            overflow: visible;
            position: relative;
        }
        .notability-badge svg {
            display: block;
            width: 22px;
            height: 22px;
            max-width: none;
            position: absolute;
            left: -2px;
            top: -2px;
        }
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
        const levels = data && typeof data === "object" ? data : {};

        const lines = [
            `Overall: ${levelText(levels.n)}`,
            `N1 sitelinks: ${levelText(levels.n1)}`,
            `N2a identifiers: ${levelText(levels.n2a)}`,
            `N2b sources: ${levelText(levels.n2b)}`,
            `N3 inlinks: ${levelText(levels.n3_inlinks)}`,
            `N3 OSM: ${levelText(levels.n3_osm)}`,
            `N3 wikisub: ${levelText(levels.n3_wikisub)}`,
            `N3 SDC: ${levelText(levels.n3_sdc)}`,
        ];
        if (levels.creator) {
            lines.push(`Creator: ${String(levels.creator)}`);
        }
        if (levels.creation_time) {
            lines.push(`Created: ${formatCreationTime(levels.creation_time)}`);
        }
        if (levels.is_redirect === true) {
            lines.push("Redirect: YES");
        }
        if (levels.is_deleted === true) {
            lines.push("Deleted: YES");
        }
        if (levels.has_sitelinks === false) {
            lines.push("Has sitelinks: NO");
        }
        if (levelText(levels.n2a) !== "UNKNOWN" && levelText(levels.n2b) !== "UNKNOWN" && levels.has_claims === false) {
            lines.push("Has claims: NO");
        }
        return lines.join("\n");
    }

    function setBadgeTooltip(elt, data) {
        if (!elt) return;
        const tooltip = buildBadgeTooltip(data);
        elt.title = tooltip;
        elt.setAttribute("aria-label", tooltip);
    }

    function rememberBadgeData(qid, data) {
        if (!qid || !data || typeof data !== "object") return;
        qidBadgeData.set(qid, data);
    }

    function initializeBadgeFromCache(badge, qid) {
        if (!badge) return;
        const cachedData = qidBadgeData.get(qid);
        if (!cachedData) return;

        const svg = badge.querySelector("svg");
        if (svg) {
            updateSVG(svg, cachedData);
        }
        setBadgeTooltip(badge, cachedData);
    }

    function addBadge(elt, qid, reason, options = {}) {
        if (elt.dataset.notabilityBadge === "true") return false;
        elt.dataset.notabilityBadge = "true";
        rememberQID(qid, reason);

        const wrapper = document.createElement("a");
        wrapper.innerHTML = BADGE_SVG;
        wrapper.href = apiUrl(`/?qid=${encodeURIComponent(qid)}`);
        wrapper.target = "_blank";
        wrapper.rel = "noopener noreferrer";
        wrapper.setAttribute("data-qid", qid);
        wrapper.title = "Open notability report";
        wrapper.classList.add("notability-badge");
        wrapper.style.display = "inline-block";
        wrapper.style.marginLeft = "4px";
        wrapper.style.verticalAlign = "middle";
        wrapper.style.width = "18px";
        wrapper.style.height = "18px";
        wrapper.style.lineHeight = "0";
        wrapper.style.overflow = "visible";
        wrapper.style.position = "relative";
        if (options.pageTitle === true) {
            wrapper.classList.add("notability-badge-page-title");
            wrapper.style.width = "22px";
            wrapper.style.height = "22px";
            wrapper.style.marginLeft = "6px";
            wrapper.style.verticalAlign = "baseline";
        }

        elt.after(wrapper);
        initializeBadgeFromCache(wrapper, qid);
        return true;
    }

    function getPageQID() {
        const entityId = mw.config.get("wbEntityId") || mw.config.get("wgRelevantPageName");
        if (typeof entityId !== "string") return null;

        const match = entityId.trim().match(/^Q\d+$/);
        return match ? match[0] : null;
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
        const fields = ["n", "n1", "n2a", "n2b", "n3", "is_deleted", "redirect", "has_claims"];
        for (const field of fields) {
            const el = svg.querySelector(`[data-field="${field}"]`);
            if (!el) continue;

            if (field === "has_claims" && (levelText(data?.n2a) === "UNKNOWN" || levelText(data?.n2b) === "UNKNOWN")) {
                el.setAttribute("data-value", "unknown");
                continue;
            }

            const val = data[field];
            el.setAttribute("data-value", val == null ? "unknown" : String(val));
        }
    }

    function updateBadges(qid, data) {
        rememberBadgeData(qid, data);
        const badges = document.querySelectorAll(`.notability-badge[data-qid="${qid}"]`);
        for (const badge of badges) {
            const svg = badge.querySelector("svg");
            if (svg) {
                updateSVG(svg, data);
            }
            setBadgeTooltip(badge, data);
        }
    }

    function applyCachedItems(items) {
        if (!Array.isArray(items)) return;

        for (const item of items) {
            if (!item || typeof item !== "object" || !item.qid) continue;
            updateBadges(item.qid, item);
        }
    }

    function subscribedItems() {
        return Array.from(knownQIDs).map((qid) => ({
            qid,
            reason: qidReasons.get(qid) || "page",
        }));
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
            const res = await fetch(apiUrl(`/subscribe`), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    items: subscribedItems(),
                    session_id: currentSubscriptionId,
                }),
            });

            if (!res.ok) {
                throw new Error(`subscribe failed: ${res.status}`);
            }
            const payload = await res.json();
            console.debug("Notability subscribe response:", {
                cached: payload.cached_items?.length ?? 0,
                misses: payload.cache_misses,
                subscription_id: payload.subscription_id,
            });
            if (payload.subscription_id && payload.subscription_id !== currentSubscriptionId) {
                currentEventId = 0;
            }
            if (payload.subscription_id) {
                currentSubscriptionId = payload.subscription_id;
            }
            applyCachedItems(payload.cached_items);

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
        eventSource = new EventSource(apiUrl(`/api/pubsub/sessions/gadget/${subscriptionId}/events${afterEventId}`));
        eventSource.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.event === "keepalive") return;
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
                    currentEventId = Math.max(currentEventId, eventId);
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

    async function deleteCurrentSubscription() {
        if (!currentSubscriptionId) return;

        const subscriptionId = currentSubscriptionId;
        currentSubscriptionId = null;
        stopPolling();

        try {
            await fetch(apiUrl(`/api/pubsub/sessions/gadget/${encodeURIComponent(subscriptionId)}`), {
                method: "DELETE",
                keepalive: true,
            });
        } catch (err) {
            console.debug("Notability subscription cleanup failed", err);
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

    function setupTeardownHandlers() {
        if (teardownHandlersInstalled) return;
        teardownHandlersInstalled = true;

        const cleanup = () => {
            void deleteCurrentSubscription();
        };
        window.addEventListener("pagehide", cleanup);
        window.addEventListener("beforeunload", cleanup);
    }

    function scanDOM() {
        let subscriptionChanged = false;

        const pageQID = getPageQID();
        if (pageQID) {
            subscriptionChanged = rememberQID(pageQID, "page") || subscriptionChanged;

            // Try to attach a badge to the title QID span so it gets painted like any other link.
            const titleEl = document.querySelector('.wikibase-title-id');
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
    }

    function setupDOMObserver() {
        const observer = new MutationObserver(scanDOM);
        observer.observe(document.body, {
            childList: true,
            subtree: true,
        });
    }

    // Bootstraps the script once DOM is ready
    function init() {
        injectNotabilityStyles();
        setupFocusHandlers();
        setupTeardownHandlers();
        scanDOM();
        setupDOMObserver();
    }

    $(init);
}(mediaWiki, jQuery, wikibase || {}));

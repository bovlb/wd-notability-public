(function (mw) {
    "use strict";

    const OWNER = "bovlb";
    const STORAGE_API_BASE = "notability2.meta.apiBase";
    const STORAGE_SCRIPT_TITLE = "notability2.meta.scriptTitle";
    const PROD_API_BASE = "https://wd-notability.toolforge.org";
    const LOCAL_API_BASE = "http://localhost:12345";
    const PROD_SCRIPT_TITLE = "User:Bovlb/notability2.js";
    const STAGING_SCRIPT_TITLE = "User:Bovlb/notability2_staging.js";

    function currentUserIsOwner() {
        const userName = mw && mw.config && mw.config.get("wgUserName");
        return typeof userName === "string" && userName.trim().toLowerCase() === OWNER;
    }

    function readStorage(key, fallback) {
        try {
            return localStorage.getItem(key) || fallback;
        } catch (_error) {
            return fallback;
        }
    }

    function writeStorage(key, value) {
        try {
            localStorage.setItem(key, value);
        } catch (_error) {
            // Ignore storage failures and keep the current session working.
        }
    }

    function normalizeApiBase(value) {
        if (typeof value !== "string") {
            return PROD_API_BASE;
        }
        const trimmed = value.trim();
        return trimmed ? trimmed.replace(/\/+$/, "") : PROD_API_BASE;
    }

    function wikiRawScriptUrl(title) {
        const params = new URLSearchParams({
            title,
            action: "raw",
            ctype: "text/javascript",
        });
        return `${mw.util.wikiScript("index")}?${params.toString()}`;
    }

    function shortApiLabel(apiBase) {
        return apiBase === LOCAL_API_BASE ? "L" : "T";
    }

    function shortBundleLabel(scriptTitle) {
        return scriptTitle === STAGING_SCRIPT_TITLE ? "S" : "P";
    }

    function apiOptionLabel(apiBase) {
        return apiBase === LOCAL_API_BASE ? "LocalHost" : "ToolForge";
    }

    function statusTitle(apiBase, scriptTitle) {
        const apiLabel = apiBase === LOCAL_API_BASE ? "localhost staging" : "toolforge prod";
        const scriptLabel = scriptTitle === STAGING_SCRIPT_TITLE ? "staging bundle" : "production bundle";
        return `API: ${apiLabel}; bundle: ${scriptLabel}`;
    }

    function createChoiceButton(label, selected, onClick, title) {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = label;
        button.title = title || label;
        button.style.padding = "0.28rem 0.55rem";
        button.style.border = selected ? "1px solid rgba(17, 17, 17, 0.65)" : "1px solid rgba(0, 0, 0, 0.15)";
        button.style.borderRadius = "999px";
        button.style.background = selected ? "#111" : "#fff";
        button.style.color = selected ? "#fff" : "#111";
        button.style.font = "inherit";
        button.style.fontSize = "0.8rem";
        button.style.fontWeight = "700";
        button.style.cursor = "pointer";
        button.addEventListener("click", onClick);
        return button;
    }

    function loadNotabilityBundle(scriptTitle, apiBase) {
        window.NOTABILITY_CONFIG = { apiBase: normalizeApiBase(apiBase) };

        const script = document.createElement("script");
        script.src = wikiRawScriptUrl(scriptTitle);
        script.async = true;
        document.head.appendChild(script);
    }

    function renderWidget(apiBase, scriptTitle) {
        const root = document.createElement("div");
        root.style.position = "fixed";
        root.style.top = "8px";
        root.style.left = "8px";
        root.style.zIndex = "100000";
        root.style.fontFamily = "system-ui, sans-serif";
        root.style.color = "#111";

        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.textContent = `${shortApiLabel(apiBase)}/${shortBundleLabel(scriptTitle)}`;
        toggle.title = statusTitle(apiBase, scriptTitle);
        toggle.style.display = "inline-flex";
        toggle.style.alignItems = "center";
        toggle.style.gap = "0.35rem";
        toggle.style.padding = "0.35rem 0.7rem";
        toggle.style.border = "1px solid rgba(0, 0, 0, 0.16)";
        toggle.style.borderRadius = "999px";
        toggle.style.background = "rgba(255, 255, 255, 0.94)";
        toggle.style.boxShadow = "0 8px 22px rgba(0, 0, 0, 0.12)";
        toggle.style.font = "inherit";
        toggle.style.fontSize = "0.8rem";
        toggle.style.fontWeight = "800";
        toggle.style.letterSpacing = "0.04em";
        toggle.style.cursor = "pointer";
        toggle.style.backdropFilter = "blur(4px)";
        toggle.setAttribute("aria-haspopup", "menu");
        toggle.setAttribute("aria-expanded", "false");

        const caret = document.createElement("span");
        caret.textContent = "▾";
        caret.style.fontSize = "0.7rem";
        caret.style.fontWeight = "700";
        caret.style.opacity = "0.75";
        toggle.appendChild(caret);

        const menu = document.createElement("div");
        menu.setAttribute("role", "menu");
        menu.style.position = "absolute";
        menu.style.top = "calc(100% + 8px)";
        menu.style.left = "50%";
        menu.style.transform = "translateX(-50%)";
        menu.style.minWidth = "16rem";
        menu.style.padding = "0.7rem";
        menu.style.border = "1px solid rgba(0, 0, 0, 0.16)";
        menu.style.borderRadius = "14px";
        menu.style.background = "rgba(255, 255, 255, 0.98)";
        menu.style.boxShadow = "0 14px 34px rgba(0, 0, 0, 0.18)";
        menu.style.display = "none";
        menu.style.gap = "0.65rem";
        menu.style.backdropFilter = "blur(4px)";

        const apiRow = document.createElement("div");
        apiRow.style.display = "flex";
        apiRow.style.alignItems = "center";
        apiRow.style.justifyContent = "space-between";
        apiRow.style.gap = "0.75rem";

        const apiLabel = document.createElement("span");
        apiLabel.textContent = "API";
        apiLabel.style.fontSize = "0.74rem";
        apiLabel.style.fontWeight = "800";
        apiLabel.style.letterSpacing = "0.08em";
        apiLabel.style.textTransform = "uppercase";
        apiLabel.style.color = "#666";
        apiRow.appendChild(apiLabel);

        const apiChoices = document.createElement("div");
        apiChoices.style.display = "flex";
        apiChoices.style.gap = "0.4rem";
        apiChoices.appendChild(createChoiceButton("ToolForge", apiBase !== LOCAL_API_BASE, () => {
            writeStorage(STORAGE_API_BASE, PROD_API_BASE);
            window.location.reload();
        }, "Toolforge prod"));
        apiChoices.appendChild(createChoiceButton("LocalHost", apiBase === LOCAL_API_BASE, () => {
            writeStorage(STORAGE_API_BASE, LOCAL_API_BASE);
            window.location.reload();
        }, "Localhost staging"));
        apiRow.appendChild(apiChoices);

        const bundleRow = document.createElement("div");
        bundleRow.style.display = "flex";
        bundleRow.style.alignItems = "center";
        bundleRow.style.justifyContent = "space-between";
        bundleRow.style.gap = "0.75rem";

        const bundleLabel = document.createElement("span");
        bundleLabel.textContent = "Bundle";
        bundleLabel.style.fontSize = "0.74rem";
        bundleLabel.style.fontWeight = "800";
        bundleLabel.style.letterSpacing = "0.08em";
        bundleLabel.style.textTransform = "uppercase";
        bundleLabel.style.color = "#666";
        bundleRow.appendChild(bundleLabel);

        const bundleChoices = document.createElement("div");
        bundleChoices.style.display = "flex";
        bundleChoices.style.gap = "0.4rem";
        bundleChoices.appendChild(createChoiceButton("Prod", scriptTitle !== STAGING_SCRIPT_TITLE, () => {
            writeStorage(STORAGE_SCRIPT_TITLE, PROD_SCRIPT_TITLE);
            window.location.reload();
        }, "Production gadget"));
        bundleChoices.appendChild(createChoiceButton("Staging", scriptTitle === STAGING_SCRIPT_TITLE, () => {
            writeStorage(STORAGE_SCRIPT_TITLE, STAGING_SCRIPT_TITLE);
            window.location.reload();
        }, "Staging gadget"));
        bundleRow.appendChild(bundleChoices);

        const note = document.createElement("div");
        note.textContent = "Changes reload the page.";
        note.style.fontSize = "0.74rem";
        note.style.color = "#666";

        menu.appendChild(apiRow);
        menu.appendChild(bundleRow);
        menu.appendChild(note);

        function setMenuOpen(open) {
            menu.style.display = open ? "grid" : "none";
            toggle.setAttribute("aria-expanded", open ? "true" : "false");
        }

        function toggleMenu() {
            setMenuOpen(menu.style.display === "none");
        }

        toggle.addEventListener("click", (event) => {
            event.preventDefault();
            event.stopPropagation();
            toggleMenu();
        });

        menu.addEventListener("click", (event) => {
            event.stopPropagation();
        });

        const closeMenu = () => setMenuOpen(false);
        document.addEventListener("click", closeMenu);
        document.addEventListener("keydown", (event) => {
            if (event.key === "Escape") {
                closeMenu();
            }
        });

        root.appendChild(toggle);
        root.appendChild(menu);
        document.body.appendChild(root);
    }

    function init() {
        const apiBase = normalizeApiBase(readStorage(STORAGE_API_BASE, PROD_API_BASE));
        const scriptTitle = readStorage(STORAGE_SCRIPT_TITLE, PROD_SCRIPT_TITLE);
        loadNotabilityBundle(scriptTitle, apiBase);
        if (currentUserIsOwner()) {
            renderWidget(apiBase, scriptTitle);
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
}(mediaWiki));

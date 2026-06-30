/* global mw */
(function () {
  var special = mw.config.get("wgCanonicalSpecialPageName");
  var params = new URLSearchParams(location.search);

  var isContrib = (special === "Contributions");
  var isWLH = (special === "Whatlinkshere" || special === "WhatLinksHere"); // paranoia

  // v1 safety gates:
  if (isContrib) {
    if (params.get("newOnly") !== "1") return;
  } else if (!isWLH) {
    return; // only run on those two special pages
  }

  var api = new mw.Api();
  var groups = mw.config.get("wgUserGroups") || [];
  var isSysop = groups.indexOf("sysop") !== -1;

  var MAX_DELETE = 1000;
  var DELETE_THROTTLE_MS = 20;

  // ---------- Styling (row highlight) ----------
  function ensureStyles() {
    if (document.getElementById("ct-style")) return;
    var style = document.createElement("style");
    style.id = "ct-style";
    style.textContent =
      ".ct-selected-row{ background:#fff8cc !important; }" +  // pale yellow
      "input.ct-qid-checkbox{ margin-right:0.5em; vertical-align:middle; }";
    document.head.appendChild(style);
  }

  function setRowHighlight(row, on) {
    if (!row) return;
    if (on) row.classList.add("ct-selected-row");
    else row.classList.remove("ct-selected-row");
  }

  // ---------- Helpers ----------
  function chunk(arr, n) {
    var out = [];
    for (var i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
  }

  function qidFromHref(href) {
    href = href || "";
    // Accept common shapes:
    // /wiki/Q123
    // https://www.wikidata.org/wiki/Q123
    // /w/index.php?title=Q123
    // /wiki/Special:EntityData/Q123.json
    var m =
      href.match(/\/wiki\/(Q\d+)(?:$|[?#/])/)
      || href.match(/[?&]title=(Q\d+)\b/)
      || href.match(/\/Special:EntityData\/(Q\d+)\b/);
    return m ? m[1] : null;
  }

  function sortQids(qids) {
    return qids.slice().sort(function (a, b) {
      return Number(a.slice(1)) - Number(b.slice(1));
    });
  }

  function copyToClipboard(text) {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        return navigator.clipboard.writeText(text)["catch"](function () {
          window.prompt("Copy this text:", text);
        });
      }
    } catch (e) {}
    window.prompt("Copy this text:", text);
    return Promise.resolve();
  }

  function normalizeReasonText(text) {
    return String(text || "").replace(/\s+/g, " ").trim().replace(/;+\s*$/, "");
  }

  function mergeReasonParts(parts) {
    var out = [];
    var seen = new Set();
    for (var i = 0; i < parts.length; i++) {
      var part = normalizeReasonText(parts[i]);
      if (!part) continue;
      var key = part.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(part);
    }
    return out.join("; ");
  }

  function sleep(ms) {
    return new Promise(function (r) { setTimeout(r, ms); });
  }

  // ---------- Page-specific row detection ----------
  function getRows() {
    if (isContrib) {
      // Contributions are grouped by date: multiple ULs.
      var uls = document.querySelectorAll("ul.mw-contributions-list");
      var rows = [];
      for (var i = 0; i < uls.length; i++) {
        var lis = uls[i].querySelectorAll("li");
        for (var j = 0; j < lis.length; j++) rows.push(lis[j]);
      }
      return rows;
    }

    // WhatLinksHere results:
    // Typically: ul#mw-whatlinkshere-list li
    var wlh = document.querySelector("#mw-whatlinkshere-list");
    if (wlh) return Array.prototype.slice.call(wlh.querySelectorAll("li"));

    // Fallback: try the generic list
    var ul2 = document.querySelector("ul.mw-specialpage-list");
    if (ul2) return Array.prototype.slice.call(ul2.querySelectorAll("li"));

    return [];
  }

  function findRowQid(row) {
    // Find any link to a Q-item in the row.
    var links = row.querySelectorAll("a[href]");
    for (var i = 0; i < links.length; i++) {
      var qid = qidFromHref(links[i].getAttribute("href"));
      if (qid && /^Q\d+$/.test(qid)) return qid;
    }
    return null;
  }
  
  function isRed(el) {
  if (!el) return false;
  // computed color, usually "rgb(255, 0, 0)" for red
  var c = window.getComputedStyle(el).backgroundColor;
  return c === "rgb(255, 0, 0)" || c === "red";
}

function rowHasNoNotabilityIndication(row, qid) {
  // Find the notability table for this QID
  var table = row.querySelector("table.notability_" + qid);
  if (!table) return false; // unknown/not evaluated yet => don't auto-select

  var n1  = row.querySelector("td.notability1_"  + qid);
  var n2a = row.querySelector("td.notability2a_" + qid);
  var n2b = row.querySelector("td.notability2b_" + qid);
  var n3  = row.querySelector("td.notability3_"  + qid);

  // Only select if all four are red
  return isRed(n1) && isRed(n2a) && isRed(n2b) && isRed(n3);
}

function rowHasNoNotabilityBadgeIndication(row) {
  var badge = row.querySelector(".notability-badge svg");
  if (!badge) return false;

  var fields = ["n", "n1", "n2a", "n2b", "n3"];
  for (var i = 0; i < fields.length; i++) {
    var field = fields[i];
    var el = badge.querySelector('[data-field="' + field + '"]');
    if (!el || el.getAttribute("data-value") !== "none") {
      return false;
    }
  }

  return true;
}

  function rowHasRedirectBadgeIndication(row) {
    var badge = row.querySelector(".notability-badge svg");
    if (!badge) return false;

    var el = badge.querySelector('[data-field="redirect"]');
    return !!el && el.getAttribute("data-value") === "true";
  }

  function anyUnknownBadges() {
    var badges = document.querySelectorAll(".notability-badge svg [data-field]");
    for (var i = 0; i < badges.length; i++) {
      if (badges[i].getAttribute("data-value") === "unknown") {
        return true;
      }
    }
    return false;
  }

function selectAllNoNotability() {
  var boxes = document.querySelectorAll("input.ct-qid-checkbox");
  var changed = 0;

  for (var i = 0; i < boxes.length; i++) {
    var cb = boxes[i];
    var qid = cb.title;
    if (!qid) continue;

    var row = cb.closest("li");
    if (!row) continue;

    if (rowHasNoNotabilityIndication(row, qid)) {
      if (!cb.checked) {
        cb.checked = true;
        cb.dispatchEvent(new Event("change"));
        changed++;
      }
    }
  }

  setPanelStatus("Selected " + changed + " item(s) with no notability indications.");
}

function selectAllNoNotabilityBadge() {
  var boxes = document.querySelectorAll("input.ct-qid-checkbox");
  var changed = 0;

  for (var i = 0; i < boxes.length; i++) {
    var cb = boxes[i];
    var qid = cb.title;
    if (!qid) continue;

    var row = cb.closest("li");
    if (!row) continue;

    if (!rowHasRedirectBadgeIndication(row) && rowHasNoNotabilityBadgeIndication(row)) {
      if (!cb.checked) {
        cb.checked = true;
        cb.dispatchEvent(new Event("change"));
        changed++;
      }
    }
  }

  setPanelStatus("Selected " + changed + " item(s) with no notability badge indications.");
}

function selectAllRows() {
  var boxes = document.querySelectorAll("input.ct-qid-checkbox");
  var changed = 0;

  for (var i = 0; i < boxes.length; i++) {
    var cb = boxes[i];
    var qid = cb.title;
    if (!qid) continue;

    if (!cb.checked) {
      cb.checked = true;
      cb.dispatchEvent(new Event("change"));
      changed++;
    }
  }

  setPanelStatus("Selected " + changed + " item(s).");
}

  // ---------- Selection state ----------
  // Make storage key depend on page + target:
  // - Contributions uses ?target=...
  // - WhatLinksHere uses wgTitle (target page) + optional namespace
  var target =
    (isContrib ? (params.get("target") || "(unknown)") : (mw.config.get("wgTitle") || "(unknown)"));

  var scope =
    isContrib ? ("contrib:newOnly:" + target) :
    ("whatlinkshere:" + target);

  var storageKey = "contribTriage:selected:" + scope;

  var selected = new Set();
  try {
    var saved = JSON.parse(localStorage.getItem(storageKey) || "[]");
    if (Array.isArray(saved)) {
      for (var i = 0; i < saved.length; i++) {
        var x = saved[i];
        if (/^Q\d+$/.test(x)) selected.add(x);
      }
    }
  } catch (e) {}

  function saveSelected() {
    try { localStorage.setItem(storageKey, JSON.stringify(Array.from(selected))); } catch (e) {}
  }

  function setSelected(qid, value) {
    if (!qid) return;
    if (value) selected.add(qid);
    else selected.delete(qid);
    saveSelected();
    updatePanel();
  }

  function clearSelected() {
    selected.clear();
    saveSelected();
    var boxes = document.querySelectorAll("input.ct-qid-checkbox");
    for (var i = 0; i < boxes.length; i++) {
      boxes[i].checked = false;
      setRowHighlight(boxes[i].closest("li"), false);
    }
    updatePanel();
  }

  // ---------- Inject checkboxes + highlighting ----------
  function injectCheckboxes() {
    var rows = getRows();
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      if (row.querySelector("input.ct-qid-checkbox")) {
        // Make sure highlight matches stored state
        var existing = row.querySelector("input.ct-qid-checkbox");
        if (existing && existing.title && /^Q\d+$/.test(existing.title)) {
          setRowHighlight(row, selected.has(existing.title));
        }
        continue;
      }

      var qid;
      try {
        qid = findRowQid(row);
      } catch (e) {
        console.warn("triage: failed to parse row", row, e);
        continue;
      }
      if (!qid) continue;

      var cb = document.createElement("input");
      cb.type = "checkbox";
      cb.className = "ct-qid-checkbox";
      cb.title = qid;
      cb.checked = selected.has(qid);

      // Highlight row immediately based on stored state
      setRowHighlight(row, cb.checked);

      (function (qidCaptured, rowCaptured, cbCaptured) {
        cbCaptured.addEventListener("change", function () {
          setSelected(qidCaptured, cbCaptured.checked);
          setRowHighlight(rowCaptured, cbCaptured.checked);
        });
      })(qid, row, cb);

      // Put checkbox at front
      row.insertBefore(cb, row.firstChild);
    }
  }

  // ---------- Panel UI ----------
  function makeButton(label, onClick, title) {
    var b = document.createElement("button");
    b.type = "button";
    b.textContent = label;
    b.title = title || "";
    b.style.cursor = "pointer";
    b.style.padding = "4px 8px";
    b.style.border = "1px solid #a2a9b1";
    b.style.borderRadius = "6px";
    b.style.background = "#f8f9fa";
    b.addEventListener("click", onClick);
    return b;
  }

  function makeLabelCheckbox(labelText, value, checked) {
    var label = document.createElement("label");
    label.style.display = "flex";
    label.style.alignItems = "center";
    label.style.gap = "6px";
    label.style.margin = "4px 0";
    label.style.cursor = "pointer";

    var input = document.createElement("input");
    input.type = "checkbox";
    input.className = "ct-delete-reason-checkbox";
    input.value = value;
    input.checked = !!checked;

    var span = document.createElement("span");
    span.textContent = labelText;

    label.appendChild(input);
    label.appendChild(span);
    return label;
  }

  function openDeletionDialog(qids, onSubmit) {
    var existing = document.getElementById("ct-delete-dialog");
    if (existing) existing.remove();

    var overlay = document.createElement("div");
    overlay.id = "ct-delete-dialog";
    overlay.style.position = "fixed";
    overlay.style.inset = "0";
    overlay.style.background = "rgba(0,0,0,0.35)";
    overlay.style.zIndex = "10000";
    overlay.style.display = "flex";
    overlay.style.alignItems = "center";
    overlay.style.justifyContent = "center";

    var dialog = document.createElement("div");
    dialog.style.width = "min(520px, calc(100vw - 32px))";
    dialog.style.background = "#fff";
    dialog.style.border = "1px solid #a2a9b1";
    dialog.style.borderRadius = "10px";
    dialog.style.boxShadow = "0 8px 24px rgba(0,0,0,0.2)";
    dialog.style.padding = "14px";
    dialog.style.fontSize = "14px";

    var title = document.createElement("div");
    title.style.fontWeight = "600";
    title.style.marginBottom = "8px";
    title.textContent = "Delete " + qids.length + " selected item(s)";
    dialog.appendChild(title);

    var prompt = document.createElement("div");
    prompt.style.marginBottom = "8px";
    prompt.textContent = "Enter a deletion reason and optionally add one or more qualifiers.";
    dialog.appendChild(prompt);

    var reasonLabel = document.createElement("label");
    reasonLabel.style.display = "block";
    reasonLabel.style.fontWeight = "600";
    reasonLabel.style.marginBottom = "4px";
    reasonLabel.textContent = "Reason";
    dialog.appendChild(reasonLabel);

    var reasonInput = document.createElement("textarea");
    reasonInput.rows = 3;
    reasonInput.style.width = "100%";
    reasonInput.style.boxSizing = "border-box";
    reasonInput.style.marginBottom = "10px";
    reasonInput.placeholder = "Base reason for deletion";
    dialog.appendChild(reasonInput);

    var optionsLabel = document.createElement("div");
    optionsLabel.style.fontWeight = "600";
    optionsLabel.style.margin = "8px 0 4px";
    optionsLabel.textContent = "Reason options";
    dialog.appendChild(optionsLabel);

    var optionsBox = document.createElement("div");
    optionsBox.style.border = "1px solid #eaecf0";
    optionsBox.style.borderRadius = "8px";
    optionsBox.style.padding = "8px 10px";
    optionsBox.appendChild(makeLabelCheckbox("no indication of notability", "no indication of notability", false));
    optionsBox.appendChild(makeLabelCheckbox("creator not responsive", "creator not responsive", false));
    optionsBox.appendChild(makeLabelCheckbox("creator unable to fix", "creator unable to fix", false));
    optionsBox.appendChild(makeLabelCheckbox("temporary account", "temporary account", false));
    dialog.appendChild(optionsBox);

    var buttons = document.createElement("div");
    buttons.style.display = "flex";
    buttons.style.justifyContent = "flex-end";
    buttons.style.gap = "8px";
    buttons.style.marginTop = "12px";

    var cancel = makeButton("Cancel", function () {
      overlay.remove();
    }, "Cancel deletion");
    buttons.appendChild(cancel);

    var submit = makeButton("Delete", function () {
      var reasonParts = [reasonInput.value];
      var boxes = dialog.querySelectorAll("input.ct-delete-reason-checkbox");
      for (var i = 0; i < boxes.length; i++) {
        if (boxes[i].checked) reasonParts.push(boxes[i].value);
      }
      var reason = mergeReasonParts(reasonParts);
      if (!reason) {
        alert("Please enter a deletion reason.");
        return;
      }
      overlay.remove();
      onSubmit(reason);
    }, "Delete selected pages");
    submit.style.background = "#d33";
    submit.style.color = "#fff";
    submit.style.borderColor = "#b32424";
    buttons.appendChild(submit);

    dialog.appendChild(buttons);
    overlay.appendChild(dialog);
    overlay.addEventListener("click", function (ev) {
      if (ev.target === overlay) overlay.remove();
    });
    document.body.appendChild(overlay);
    reasonInput.focus();
  }

  function makePanel() {
    if (document.getElementById("ct-panel")) return;

    var panel = document.createElement("div");
    panel.id = "ct-panel";
    panel.style.position = "fixed";
    panel.style.right = "16px";
    panel.style.bottom = "16px";
    panel.style.zIndex = "9999";
    panel.style.background = "white";
    panel.style.border = "1px solid #a2a9b1";
    panel.style.borderRadius = "10px";
    panel.style.padding = "10px";
    panel.style.boxShadow = "0 2px 10px rgba(0,0,0,0.15)";
    panel.style.fontSize = "13px";
    panel.style.maxWidth = "360px";

    var header = document.createElement("div");
    header.style.display = "flex";
    header.style.alignItems = "center";
    header.style.justifyContent = "space-between";
    header.style.gap = "10px";

    var title = document.createElement("div");
    title.style.fontWeight = "600";
    title.textContent = isContrib ? "Contrib triage (newOnly)" : "WhatLinksHere triage";
    header.appendChild(title);

    var badgeReady = document.createElement("div");
    badgeReady.id = "ct-badge-ready";
    badgeReady.title = "Badge readiness";
    badgeReady.style.minWidth = "20px";
    badgeReady.style.textAlign = "right";
    badgeReady.style.fontWeight = "700";
    badgeReady.style.fontSize = "16px";
    badgeReady.style.lineHeight = "1";
    header.appendChild(badgeReady);

    panel.appendChild(header);

    var hint = document.createElement("div");
    hint.style.fontSize = "11px";
    hint.style.color = "#54595d";
    hint.style.marginTop = "4px";
    hint.textContent = isContrib ? ("target: " + target) : ("page: " + target);
    panel.appendChild(hint);

    var count = document.createElement("div");
    count.id = "ct-count";
    count.style.margin = "8px 0";
    panel.appendChild(count);

    var status = document.createElement("div");
    status.id = "ct-status";
    status.style.margin = "0 0 8px";
    status.style.fontSize = "11px";
    status.style.color = "#54595d";
    status.style.minHeight = "1.2em";
    panel.appendChild(status);

    var btnRow = document.createElement("div");
    btnRow.style.display = "flex";
    btnRow.style.flexWrap = "wrap";
    btnRow.style.gap = "6px";
    
    btnRow.appendChild(makeButton(
	  "Select no-notability",
	  function () { selectAllNoNotability(); },
	  "Select rows where N1/N2a/N2b/N3 are all red"
	));

    btnRow.appendChild(makeButton(
      "Select no-notability badge",
      function () { selectAllNoNotabilityBadge(); },
      "Select rows whose compact badge is entirely red"
    ));

    btnRow.appendChild(makeButton(
      "Select all",
      function () { selectAllRows(); },
      "Select every row on the page"
    ));

    btnRow.appendChild(makeButton(
      "Copy QIDs",
      function () {
        var qids = sortQids(Array.from(selected));
        if (!qids.length) return;
        copyToClipboard(qids.join(" | "));
      },
      "Copy as: Q1 | Q2 | Q3"
    ));

    // NEW: copy as {{Q|...}}
    btnRow.appendChild(makeButton(
      "Copy {{Q|…}}",
      function () {
        var qids = sortQids(Array.from(selected));
        if (!qids.length) return;
        var out = qids.map(function (q) { return "{{Q|" + q + "}}"; }).join(", ");
        copyToClipboard(out);
      },
      "Copy as: {{Q|Q1}} {{Q|Q2}} …"
    ));

    btnRow.appendChild(makeButton(
      "Generate bulk RFD",
      function () {
        var qids = sortQids(Array.from(selected));
        if (!qids.length) return;

        var reason = prompt("Enter deletion reason for these " + qids.length + " item(s):");
        if (!reason || !reason.trim()) return;

        var groups = chunk(qids, 10);
        var out = "==Bulk deletion request: " + (isContrib ? target : ("WhatLinksHere: " + target)) + "==\n\n";

        for (var i = 0; i < groups.length; i++) {
          var g = groups[i];
          out += "{{subst:Rfd group";
          for (var j = 0; j < g.length; j++) out += " | " + g[j];
          out += " | reason = " + reason + " }}\n\n";
        }

        copyToClipboard(out);
      },
      "Generate \{\{Rfd group\}\} blocks (requires a reason)"
    ));

    if (isSysop) {
      var deleteReasonSummary = document.createElement("div");
      deleteReasonSummary.style.margin = "6px 0 8px";
      deleteReasonSummary.style.padding = "8px 10px";
      deleteReasonSummary.style.border = "1px solid #eaecf0";
      deleteReasonSummary.style.borderRadius = "8px";
      deleteReasonSummary.style.background = "#f8f9fa";

      var deleteReasonTitle = document.createElement("div");
      deleteReasonTitle.style.fontWeight = "600";
      deleteReasonTitle.textContent = "Delete reason helpers";
      deleteReasonSummary.appendChild(deleteReasonTitle);

      var deleteReasonHint = document.createElement("div");
      deleteReasonHint.style.fontSize = "11px";
      deleteReasonHint.style.color = "#54595d";
      deleteReasonHint.style.marginTop = "3px";
      deleteReasonHint.textContent = "These options are appended to the reason field with semicolons.";
      deleteReasonSummary.appendChild(deleteReasonHint);

      deleteReasonSummary.appendChild(makeLabelCheckbox("no indication of notability", "no indication of notability", false));
      deleteReasonSummary.appendChild(makeLabelCheckbox("creator not responsive", "creator not responsive", false));
      deleteReasonSummary.appendChild(makeLabelCheckbox("creator unable to fix", "creator unable to fix", false));
      deleteReasonSummary.appendChild(makeLabelCheckbox("temporary account", "temporary account", false));

      panel.appendChild(deleteReasonSummary);

      btnRow.appendChild(makeButton(
        "Delete selected…",
        function (ev) {
          var qids = sortQids(Array.from(selected));
          if (!qids.length) return;

          if (qids.length > MAX_DELETE) {
            alert("Refusing to delete " + qids.length + ". Max is " + MAX_DELETE + ".");
            return;
          }

          if (!ev || !ev.shiftKey) {
            alert("Shift-click “Delete selected…” to proceed (prevents misclick deletes).");
            return;
          }
          openDeletionDialog(qids, function (reason) {
            if (!confirm("About to delete " + qids.length + " pages.\nProceed?")) return;

            api.get({ action: "query", meta: "tokens", format: "json" }).then(function (tokenResp) {
              var token = null;
              try { token = tokenResp.query.tokens.csrftoken; } catch (e) {}
              if (!token) { alert("Could not get CSRF token."); return; }

              var results = [];
              var p = Promise.resolve();

              qids.forEach(function (qid) {
                p = p.then(function () {
                  return sleep(DELETE_THROTTLE_MS).then(function () {
                    return api.post({
                      action: "delete",
                      title: qid,
                      reason: reason,
                      token: token,
                      format: "json"
                    }).then(function (resp) {
                      results.push({ qid: qid, ok: true, resp: resp });
                      selected.delete(qid);
                      saveSelected();
                      updatePanel();

                      // remove highlight + checkbox state for visible rows
                      var boxes = document.querySelectorAll('input.ct-qid-checkbox[title="' + qid + '"]');
                      for (var i = 0; i < boxes.length; i++) {
                        boxes[i].checked = false;
                        setRowHighlight(boxes[i].closest("li"), false);
                      }
                    }, function (err) {
                      results.push({ qid: qid, ok: false, error: String(err) });
                      throw err;
                    });
                  });
                });
              });

              p.then(function () {
                console.table(results.map(function (r) {
                  return { qid: r.qid, ok: r.ok, error: r.error || "" };
                }));
                alert("Deleted " + results.filter(function (r) { return r.ok; }).length + "/" + qids.length + ".");
              }).catch(function () {
                console.table(results.map(function (r) {
                  return { qid: r.qid, ok: r.ok, error: r.error || "" };
                }));
                alert("Stopped early due to an error. See console.");
              });
            });
          });
        },
        "Sysop only. Shift-click to proceed. Max " + MAX_DELETE + "."
      ));
    }

    btnRow.appendChild(makeButton("Clear", function () { clearSelected(); }, "Clear selection"));

    panel.appendChild(btnRow);

    var footer = document.createElement("div");
    footer.style.marginTop = "8px";
    footer.style.fontSize = "11px";
    footer.style.color = "#54595d";
    footer.textContent = isSysop ? ("Delete cap: " + MAX_DELETE + ". Shift-click required.") : "Selection is local to your browser.";
    panel.appendChild(footer);

    document.body.appendChild(panel);
    updatePanel();
  }

  function updatePanel() {
    var el = document.getElementById("ct-count");
    if (!el) return;
    el.textContent = selected.size + " selected";
    updateBadgeReadinessIndicator();
  }

  function setPanelStatus(message) {
    var el = document.getElementById("ct-status");
    if (!el) return;
    el.textContent = message || "";
  }

  function updateBadgeReadinessIndicator() {
    var el = document.getElementById("ct-badge-ready");
    if (!el) return;
    if (anyUnknownBadges()) {
      el.textContent = "";
      el.title = "Some badges are still unknown";
      el.style.color = "#72777d";
    } else {
      el.textContent = "✓";
      el.title = "All visible badges are resolved";
      el.style.color = "#14866d";
    }
  }

  // ---------- Init ----------
  mw.loader.using(["mediawiki.api"]).then(function () {
    ensureStyles();
    injectCheckboxes();
    makePanel();
    updateBadgeReadinessIndicator();

    // Observe the whole content area (covers multiple lists / re-renders)
    var root = document.getElementById("mw-content-text") || document.body;
    var obs = new MutationObserver(function () {
      injectCheckboxes();
      updateBadgeReadinessIndicator();
    });
    obs.observe(root, {
      childList: true,
      subtree: true,
      attributes: true,
      attributeFilter: ["data-value"],
    });
  });
})();

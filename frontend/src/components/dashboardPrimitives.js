function escapeSafe(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function joinClassNames(parts = []) {
  return parts.filter(Boolean).join(" ").trim();
}

function normalizeTone(tone) {
  const safe = String(tone || "neutral").trim().toLowerCase();
  if (safe === "positive" || safe === "negative" || safe === "warning") {
    return safe;
  }
  return "neutral";
}

export function renderPanel({
  title = "",
  subtitle = "",
  actions = "",
  body = "",
  className = "",
  wide = false,
  sectionId = ""
} = {}) {
  const classes = joinClassNames(["panel", wide ? "wide" : "", className]);
  const idAttr = sectionId ? ` id="${escapeSafe(sectionId)}"` : "";
  const hasHeader = title || subtitle || actions;

  return `
    <article class="${classes}"${idAttr}>
      ${
        hasHeader
          ? `
        <header class="dashboard-panel-head">
          <div>
            ${title ? `<h2>${escapeSafe(title)}</h2>` : ""}
            ${subtitle ? `<p class="muted dashboard-panel-subtitle">${escapeSafe(subtitle)}</p>` : ""}
          </div>
          ${actions ? `<div class="dashboard-panel-actions">${actions}</div>` : ""}
        </header>
      `
          : ""
      }
      <div class="dashboard-panel-body">${body}</div>
    </article>
  `;
}

export function renderStatTile({
  label = "",
  value = "-",
  hint = "",
  tone = "neutral",
  className = ""
} = {}) {
  const normalizedTone = normalizeTone(tone);
  const classes = joinClassNames(["stat-tile", `tone-${normalizedTone}`, className]);

  return `
    <article class="${classes}">
      <span class="stat-tile-label">${escapeSafe(label)}</span>
      <strong class="stat-tile-value">${value}</strong>
      ${hint ? `<small class="stat-tile-hint">${escapeSafe(hint)}</small>` : ""}
    </article>
  `;
}

export function renderStatGrid({
  tiles = [],
  className = ""
} = {}) {
  const classes = joinClassNames(["stat-grid", className]);
  const rows = (Array.isArray(tiles) ? tiles : []).join("");
  return `<div class="${classes}">${rows}</div>`;
}

export function renderKPIBar({
  items = [],
  controls = "",
  status = "",
  className = "",
  rootAttrs = ""
} = {}) {
  const classes = joinClassNames(["dashboard-kpi-bar", className]);
  const attrs = rootAttrs ? ` ${rootAttrs}` : "";
  const rows = (Array.isArray(items) ? items : [])
    .map((item) => {
      const tone = normalizeTone(item?.tone);
      const emphasis = item?.primary ? "kpi-item-value primary" : "kpi-item-value";
      const helper = item?.helper ? `<small class="kpi-item-helper">${escapeSafe(item.helper)}</small>` : "";
      const itemClassName = item?.className ? ` ${escapeSafe(item.className)}` : "";
      return `
        <article class="kpi-item tone-${tone}${itemClassName}">
          <span class="kpi-item-label">${escapeSafe(item?.label || "")}</span>
          <strong class="${emphasis}">${item?.value ?? "-"}</strong>
          ${helper}
        </article>
      `;
    })
    .join("");

  return `
    <section class="${classes}" aria-label="Decision KPIs"${attrs}>
      <div class="kpi-items">${rows}</div>
      <div class="kpi-controls">
        ${status ? `<div class="kpi-refresh-status">${status}</div>` : ""}
        ${controls}
      </div>
    </section>
  `;
}

export function renderDrawer({
  open = false,
  title = "",
  subtitle = "",
  bodyMarkup = "",
  footerMarkup = "",
  closeAttr = "",
  label = "Drawer",
  rootClassName = "",
  overlayAttr = "",
  panelAttr = ""
} = {}) {
  const classes = joinClassNames(["dashboard-drawer-root", open ? "open" : "", rootClassName]);
  const closeButtonAttr = closeAttr ? ` ${closeAttr}` : "";
  const overlayButtonAttr = overlayAttr ? ` ${overlayAttr}` : "";
  const panelDataAttr = panelAttr ? ` ${panelAttr}` : "";

  return `
    <div class="${classes}" ${open ? "" : 'aria-hidden="true"'}>
      <button
        type="button"
        class="dashboard-drawer-overlay"
        aria-label="Close ${escapeSafe(label)}"
        tabindex="-1"
        ${overlayButtonAttr}
      ></button>
      <aside
        class="dashboard-drawer-panel"
        role="dialog"
        aria-modal="true"
        aria-label="${escapeSafe(label)}"
        tabindex="-1"
        ${panelDataAttr}
      >
        <header class="dashboard-drawer-header">
          <div>
            <p class="dashboard-drawer-title">${escapeSafe(title)}</p>
            ${subtitle ? `<p class="dashboard-drawer-subtitle">${escapeSafe(subtitle)}</p>` : ""}
          </div>
          <button
            type="button"
            class="ghost-btn dashboard-drawer-close"
            aria-label="Close ${escapeSafe(label)}"
            ${closeButtonAttr}
          >
            Close
          </button>
        </header>
        <div class="dashboard-drawer-body">${bodyMarkup}</div>
        ${footerMarkup ? `<footer class="dashboard-drawer-footer">${footerMarkup}</footer>` : ""}
      </aside>
    </div>
  `;
}

export function renderModal({
  open = false,
  title = "",
  subtitle = "",
  bodyMarkup = "",
  footerMarkup = "",
  closeAttr = "",
  label = "Modal",
  rootClassName = "",
  overlayAttr = "",
  panelAttr = ""
} = {}) {
  if (!open) return "";
  const rootClasses = joinClassNames(["dashboard-modal-overlay", rootClassName]);
  const closeButtonAttr = closeAttr ? ` ${closeAttr}` : "";
  const overlayButtonAttr = overlayAttr ? ` ${overlayAttr}` : "";
  const panelDataAttr = panelAttr ? ` ${panelAttr}` : "";

  return `
    <div class="${rootClasses}" ${overlayButtonAttr}>
      <section
        class="dashboard-modal-dialog"
        role="dialog"
        aria-modal="true"
        aria-label="${escapeSafe(label)}"
        tabindex="-1"
        ${panelDataAttr}
      >
        <header class="dashboard-modal-header">
          <div>
            <p class="dashboard-modal-title">${escapeSafe(title)}</p>
            ${subtitle ? `<p class="dashboard-modal-subtitle">${escapeSafe(subtitle)}</p>` : ""}
          </div>
          <button
            type="button"
            class="ghost-btn dashboard-modal-close"
            aria-label="Close ${escapeSafe(label)}"
            ${closeButtonAttr}
          >
            Close
          </button>
        </header>
        <div class="dashboard-modal-body">${bodyMarkup}</div>
        ${footerMarkup ? `<footer class="dashboard-modal-footer">${footerMarkup}</footer>` : ""}
      </section>
    </div>
  `;
}

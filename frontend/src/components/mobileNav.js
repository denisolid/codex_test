const fallbackEscape = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

export function renderMobileNav({
  title = "CS2 Portfolio Analyzer",
  drawerOpen = false,
  notificationCount = 0,
  escapeHtml = fallbackEscape
} = {}) {
  const safeCount = Math.max(Number(notificationCount || 0), 0);
  return `
    <header class="mobile-nav" role="banner">
      <button
        type="button"
        class="ghost-btn mobile-nav-toggle ${drawerOpen ? "is-open" : ""}"
        data-mobile-drawer-open
        aria-label="Open navigation menu"
        aria-expanded="${drawerOpen ? "true" : "false"}"
        aria-controls="mobile-drawer-panel"
      >
        <span class="line line-1" aria-hidden="true"></span>
        <span class="line line-2" aria-hidden="true"></span>
        <span class="line line-3" aria-hidden="true"></span>
      </button>
      <div class="mobile-nav-title">${escapeHtml(title)}</div>
      <div class="mobile-nav-actions">
        <button
          type="button"
          class="ghost-btn mobile-nav-action mobile-nav-icon-btn"
          id="mobile-nav-notifications-btn"
          aria-label="Open alerts"
          title="Alerts"
        >
          <span aria-hidden="true">🔔</span>
          ${
            safeCount > 0
              ? `<span class="mobile-nav-badge">${safeCount > 99 ? "99+" : safeCount}</span>`
              : ""
          }
        </button>
        <button
          type="button"
          class="ghost-btn mobile-nav-action"
          id="mobile-nav-refresh-btn"
          aria-label="Refresh prices"
        >
          Refresh
        </button>
      </div>
    </header>
  `;
}

export function renderMobileDrawer({
  open = false,
  tabs = [],
  activeTab = "",
  userLabel = "Signed in",
  userTitle = "",
  globalSearch = "",
  loading = false,
  escapeHtml = fallbackEscape
} = {}) {
  const navItems = (Array.isArray(tabs) ? tabs : [])
    .map(
      (tab) => `
        <button
          type="button"
          class="ghost-btn tab-btn mobile-drawer-tab ${activeTab === tab.id ? "active" : ""}"
          data-tab="${escapeHtml(tab.id)}"
        >
          <span class="mobile-drawer-tab-label-wrap">
            <span class="mobile-drawer-tab-icon" aria-hidden="true">${escapeHtml(tab.icon || "•")}</span>
            <span class="mobile-drawer-tab-label">${escapeHtml(tab.label)}</span>
          </span>
          <small class="mobile-drawer-tab-hint">${escapeHtml(tab.hint || "")}</small>
        </button>
      `
    )
    .join("");

  return `
    <div class="mobile-drawer-root ${open ? "open" : ""}" ${open ? "" : 'aria-hidden="true"'}>
      <button
        type="button"
        class="mobile-drawer-overlay"
        data-mobile-drawer-overlay
        aria-label="Close navigation menu"
        tabindex="-1"
      ></button>
      <aside
        class="mobile-drawer-panel"
        id="mobile-drawer-panel"
        data-mobile-drawer-panel
        role="dialog"
        aria-modal="true"
        aria-label="Navigation menu"
        tabindex="-1"
      >
        <header class="mobile-drawer-header">
          <p class="mobile-drawer-title">Menu</p>
          <button
            type="button"
            class="ghost-btn mobile-drawer-close"
            data-mobile-drawer-close
            aria-label="Close navigation menu"
          >
            Close
          </button>
        </header>
        <div class="mobile-drawer-user">
          <span class="user-chip" title="${escapeHtml(userTitle)}">${escapeHtml(userLabel)}</span>
        </div>
        <label class="mobile-drawer-search" for="global-search-mobile">
          Search
          <input
            id="global-search-mobile"
            type="search"
            placeholder="Search skins..."
            value="${escapeHtml(globalSearch)}"
            aria-label="Search skins"
          />
        </label>
        ${loading ? '<p class="mobile-drawer-loading">Switching section...</p>' : ""}
        <nav class="mobile-drawer-nav">
          ${navItems}
        </nav>
        <div class="mobile-drawer-actions">
          <button type="button" class="ghost-btn" id="mobile-drawer-notifications-btn">Alerts</button>
          <button type="button" class="ghost-btn" id="mobile-drawer-refresh-btn">Refresh</button>
          <button type="button" class="ghost-btn" id="mobile-drawer-logout-btn">Logout</button>
        </div>
      </aside>
    </div>
  `;
}

import "./style.css";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL || "http://localhost:4000/api";
const app = document.querySelector("#app");

const state = {
  authenticated: false,
  portfolio: null,
  history: [],
  alerts: [],
  skin: null,
  inspectedSteamItemId: "",
  error: "",
  syncingInventory: false,
  syncSummary: null
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setError(msg) {
  state.error = msg;
  render();
}

function clearError() {
  if (!state.error) return;
  state.error = "";
}

function formatMoney(amount) {
  return `$${Number(amount || 0).toFixed(2)}`;
}

function formatPercent(value) {
  if (value == null) return "-";
  const sign = Number(value) > 0 ? "+" : "";
  return `${sign}${Number(value).toFixed(2)}%`;
}

function formatNumber(value, digits = 2) {
  if (value == null || Number.isNaN(Number(value))) return "-";
  return Number(value).toFixed(digits);
}

function toTitle(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatConfidence(item) {
  const label = item?.priceConfidenceLabel || "unknown";
  const score = Number(item?.priceConfidenceScore || 0);
  return `${toTitle(label)} (${Math.round(score * 100)}%)`;
}

function formatPriceStatusBadge(status) {
  const safe = String(status || "unknown").toLowerCase();
  return `<span class="status-badge ${escapeHtml(safe)}">${escapeHtml(toTitle(safe))}</span>`;
}

function formatRiskBadge(level) {
  const safe = String(level || "unknown").toLowerCase();
  return `<span class="risk-badge ${escapeHtml(safe)}">${escapeHtml(toTitle(safe))}</span>`;
}

async function api(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {})
  };

  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    credentials: "include",
    headers
  });

  const payload = await res.json().catch(() => ({}));
  if (res.status === 401) {
    state.authenticated = false;
  }
  if (!res.ok) {
    throw new Error(payload.error || "Request failed");
  }

  state.authenticated = true;
  return payload;
}

async function logout() {
  try {
    await api("/auth/logout", { method: "POST" });
  } catch (_err) {
    // Continue local cleanup even if request fails.
  }

  state.authenticated = false;
  state.portfolio = null;
  state.history = [];
  state.alerts = [];
  state.skin = null;
  state.inspectedSteamItemId = "";
  render();
}

async function connectSteam(e) {
  e.preventDefault();
  clearError();
  const steamId64 = document.querySelector("#steam-id").value.trim();

  try {
    await api("/users/me/steam", {
      method: "PATCH",
      body: JSON.stringify({ steamId64 })
    });
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  }
}

async function syncInventory() {
  if (state.syncingInventory) return;
  clearError();
  state.syncingInventory = true;
  render();
  try {
    const result = await api("/inventory/sync", { method: "POST" });
    state.syncSummary = result;
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
    state.alerts = [
      {
        severity: "warning",
        code: "SYNC_FAILED",
        message: `Inventory sync failed: ${err.message}`
      },
      ...(state.alerts || []).filter((a) => a.code !== "SYNC_FAILED")
    ];
  } finally {
    state.syncingInventory = false;
    render();
  }
}

function renderSyncSummary() {
  if (!state.syncSummary) return "";
  const s = state.syncSummary;
  const unpricedPreview = (s.unpricedItems || [])
    .slice(0, 4)
    .map(
      (x) =>
        `<li><strong>${escapeHtml(x.marketHashName)}</strong> <span class="muted">(${escapeHtml(x.reason)})</span></li>`
    )
    .join("");
  const excludedPreview = (s.excludedItems || [])
    .slice(0, 4)
    .map(
      (x) =>
        `<li><strong>${escapeHtml(x.marketHashName)}</strong> <span class="muted">(${escapeHtml(x.reason)})</span></li>`
    )
    .join("");

  return `
    <div class="sync-summary">
      <p><strong>Last sync:</strong> ${escapeHtml(s.syncedAt || "-")}</p>
      <p>Source: ${escapeHtml(s.inventorySource || "-")} | Cache hits: ${Number(
        s.priceCacheHitCount || 0
      )}</p>
      <p>Synced: ${Number(s.itemsSynced || 0)} | Priced: ${Number(
        s.pricedItems || 0
      )} | Unpriced: ${Number(s.unpricedItemsCount || 0)} | Excluded: ${Number(
        s.excludedItemsCount || 0
      )}</p>
      ${
        unpricedPreview
          ? `<p class="muted">Unpriced (first 4):</p><ul class="sync-list">${unpricedPreview}</ul>`
          : ""
      }
      ${
        excludedPreview
          ? `<p class="muted">Excluded (first 4):</p><ul class="sync-list">${excludedPreview}</ul>`
          : ""
      }
    </div>
  `;
}

async function refreshPortfolio() {
  clearError();
  try {
    const [portfolio, history] = await Promise.all([
      api("/portfolio"),
      api("/portfolio/history")
    ]);
    state.portfolio = portfolio;
    state.history = history.points || [];
    state.alerts = Array.isArray(portfolio.alerts) ? portfolio.alerts : [];
    render();
  } catch (err) {
    setError(err.message);
  }
}

async function findSkin(e) {
  e.preventDefault();
  clearError();
  const id = document.querySelector("#steam-item-id").value;
  await inspectSkinBySteamItemId(id);
}

async function inspectSkinBySteamItemId(rawId) {
  const id = String(rawId ?? "").trim();
  if (!id) return;

  if (!/^\d+$/.test(id)) {
    setError("Steam item ID must contain only digits.");
    return;
  }

  try {
    state.skin = await api(`/skins/by-steam-item/${encodeURIComponent(id)}`);
    state.inspectedSteamItemId = id;
    render();
  } catch (err) {
    state.skin = null;
    state.inspectedSteamItemId = "";
    setError(err.message);
  }
}

function renderPortfolioRows() {
  if (!state.portfolio || !state.portfolio.items || !state.portfolio.items.length) {
    return `<tr><td colspan="8" class="muted">No holdings yet. Connect Steam and run sync.</td></tr>`;
  }

  const formatSteamItemIdCell = (item) => {
    const ids = Array.isArray(item.steamItemIds) ? item.steamItemIds : [];
    if (!ids.length) return "-";
    if (ids.length === 1) return escapeHtml(ids[0]);
    return `${escapeHtml(ids[0])} +${ids.length - 1} more`;
  };

  return state.portfolio.items
    .map(
      (item) => `
      <tr>
        <td>${formatSteamItemIdCell(item)}</td>
        <td>
          <div class="skin-name">${escapeHtml(item.marketHashName)}</div>
        </td>
        <td>${item.quantity}</td>
        <td>${formatMoney(item.currentPrice)}</td>
        <td>${formatPriceStatusBadge(item.priceStatus)}</td>
        <td>${escapeHtml(formatConfidence(item))}</td>
        <td><strong>${formatMoney(item.lineValue)}</strong></td>
        <td>
          <button
            type="button"
            class="ghost-btn inspect-skin-btn"
            data-steam-item-id="${escapeHtml(item.primarySteamItemId || "")}"
            ${item.primarySteamItemId ? "" : "disabled"}
          >
            Inspect
          </button>
        </td>
      </tr>
    `
    )
    .join("");
}

function renderHistoryChart() {
  if (!state.history.length) {
    return `<p class="muted">No history yet. Sync inventory to create data points.</p>`;
  }

  const values = state.history.map((p) => Number(p.totalValue || 0));
  const max = Math.max(...values, 1);

  return `
    <div class="mini-chart">
      ${state.history
        .map((point) => {
          const value = Number(point.totalValue || 0);
          const width = Math.max((value / max) * 100, 4);
          return `
            <div class="chart-row">
              <span>${escapeHtml(point.date)}</span>
              <div class="bar-wrap"><div class="bar" style="width:${width}%"></div></div>
              <strong>${formatMoney(value)}</strong>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderSkinDetails() {
  if (!state.skin) {
    return `<p class="muted">Search a skin by Steam item ID to inspect current and historical pricing.</p>`;
  }

  const latest = state.skin.latestPrice;
  const history = Array.isArray(state.skin.priceHistory) ? state.skin.priceHistory : [];
  const historyMarkup = history.length
    ? `<ul class="sync-list">${history
        .slice(0, 10)
        .map(
          (row) =>
            `<li>${escapeHtml(String(row.recorded_at || "").slice(0, 10))}: <strong>${formatMoney(row.price)}</strong> ${escapeHtml(row.currency || "")}</li>`
        )
        .join("")}</ul>`
    : `<p class="muted">No price history yet for this skin.</p>`;

  return `
    <div class="skin-card">
      <h3>${escapeHtml(state.skin.market_hash_name)}</h3>
      <p>Skin ID: <strong>${Number(state.skin.id || 0) || "-"}</strong></p>
      <p>${escapeHtml(state.skin.weapon || "-")} | ${escapeHtml(state.skin.exterior || "-")} | ${escapeHtml(state.skin.rarity || "-")}</p>
      <p>Latest Price: <strong>${latest ? `${formatMoney(latest.price)} ${escapeHtml(latest.currency)}` : "N/A"}</strong></p>
      <p>Price Source: <strong>${latest ? escapeHtml(latest.source || "unknown") : "-"}</strong></p>
      <p>Price Status: ${latest ? formatPriceStatusBadge(latest.status) : "-"}</p>
      <p>Confidence: <strong>${latest ? escapeHtml(formatConfidence(latest)) : "-"}</strong></p>
      ${
        latest && latest.stale
          ? `<p class="muted">Live refresh failed, showing last known price: ${escapeHtml(latest.staleReason || "unknown reason")}</p>`
          : ""
      }
      <p class="muted">Recent prices:</p>
      ${historyMarkup}
    </div>
  `;
}

function renderAlerts() {
  if (!Array.isArray(state.alerts) || !state.alerts.length) return "";

  const rows = state.alerts
    .map((alert) => {
      const severity = String(alert.severity || "info").toLowerCase();
      return `<li class="alert-row ${escapeHtml(severity)}">${escapeHtml(
        alert.message || "Portfolio alert"
      )}</li>`;
    })
    .join("");

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Alerts</h2>
        <ul class="alert-list">${rows}</ul>
      </article>
    </section>
  `;
}

function renderAnalytics() {
  const analytics = state.portfolio?.analytics;
  if (!analytics) return "";

  const topGainer = analytics?.leaders?.topGainer || null;
  const topLoser = analytics?.leaders?.topLoser || null;
  const breadth = analytics?.breadth || {};

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Portfolio Analytics</h2>
        <div class="analytics-grid">
          <div class="analytics-item">
            <span>Holdings</span>
            <strong>${escapeHtml(formatNumber(analytics.holdingsCount, 0))}</strong>
          </div>
          <div class="analytics-item">
            <span>Concentration Risk</span>
            <strong>${formatRiskBadge(analytics.concentrationRisk)}</strong>
          </div>
          <div class="analytics-item">
            <span>Top 1 Weight</span>
            <strong>${escapeHtml(formatPercent(analytics.concentrationTop1Percent))}</strong>
          </div>
          <div class="analytics-item">
            <span>Top 3 Weight</span>
            <strong>${escapeHtml(formatPercent(analytics.concentrationTop3Percent))}</strong>
          </div>
          <div class="analytics-item">
            <span>Effective Holdings</span>
            <strong>${escapeHtml(formatNumber(analytics.effectiveHoldings))}</strong>
          </div>
          <div class="analytics-item">
            <span>Breadth</span>
            <strong>${escapeHtml(
              `${Number(breadth.advancers || 0)}/${Number(breadth.decliners || 0)} (${formatPercent(
                breadth.advancerRatioPercent
              )})`
            )}</strong>
          </div>
          <div class="analytics-item">
            <span>Weighted 7D Move</span>
            <strong>${escapeHtml(formatPercent(analytics.weightedAverageMove7dPercent))}</strong>
          </div>
          <div class="analytics-item">
            <span>Unpriced / Stale</span>
            <strong>${escapeHtml(
              `${Number(state.portfolio?.unpricedItemsCount || 0)} / ${Number(
                state.portfolio?.staleItemsCount || 0
              )}`
            )}</strong>
          </div>
        </div>
        <div class="analytics-leaders">
          <div>
            <p class="muted">Top Gainer (7D)</p>
            <p>
              <strong>${
                topGainer ? escapeHtml(topGainer.marketHashName) : "N/A"
              }</strong>
              <span class="muted"> ${topGainer ? escapeHtml(formatPercent(topGainer.sevenDayChangePercent)) : "-"}</span>
            </p>
          </div>
          <div>
            <p class="muted">Top Loser (7D)</p>
            <p>
              <strong>${topLoser ? escapeHtml(topLoser.marketHashName) : "N/A"}</strong>
              <span class="muted"> ${topLoser ? escapeHtml(formatPercent(topLoser.sevenDayChangePercent)) : "-"}</span>
            </p>
          </div>
        </div>
      </article>
    </section>
  `;
}

function renderPublicHome() {
  app.innerHTML = `
    <main class="layout">
      <nav class="topbar">
        <div class="brand">CS2 Portfolio Analyzer</div>
        <div class="top-actions">
          <a class="link-btn ghost" href="/login.html">Login</a>
          <a class="link-btn" href="/register.html">Start Free</a>
        </div>
      </nav>

      <section class="hero-block">
        <div>
          <p class="eyebrow">SaaS for CS2 traders and collectors</p>
          <h1>See your skins like a real portfolio, not a random inventory list.</h1>
          <p class="hero-copy">Connect Steam, sync your items, and instantly track value, ROI, and 7-day movement in one focused dashboard.</p>
          <div class="hero-actions">
            <a class="link-btn" href="/register.html">Create Account</a>
            <a class="link-btn ghost" href="/login.html">I already have an account</a>
          </div>
        </div>
        <div class="hero-preview panel">
          <h3>What you get</h3>
          <ul class="bullet-list">
            <li>Total portfolio value in USD</li>
            <li>ROI from your stored buy prices</li>
            <li>7-day movement snapshot</li>
            <li>Skin-level lookup and trend data</li>
          </ul>
        </div>
      </section>

      <section class="grid">
        <article class="panel">
          <h2>Built for speed</h2>
          <p class="muted">No spreadsheet maintenance. Sync once and get immediate valuation.</p>
        </article>
        <article class="panel">
          <h2>Built for decisions</h2>
          <p class="muted">Quickly identify winners, laggards, and where your value is concentrated.</p>
        </article>
        <article class="panel">
          <h2>Built for growth</h2>
          <p class="muted">MVP architecture is already ready for live market providers and scheduled sync jobs.</p>
        </article>
      </section>
    </main>
  `;
}

function renderApp() {
  const portfolio = state.portfolio || {};
  const oneDayTrendClass = Number(portfolio.oneDayChangePercent || 0) >= 0 ? "up" : "down";
  const trendClass = Number(portfolio.sevenDayChangePercent || 0) >= 0 ? "up" : "down";

  app.innerHTML = `
    <main class="layout">
      <nav class="topbar">
        <div class="brand">CS2 Portfolio Analyzer</div>
        <div class="top-actions">
          <button id="refresh-btn" class="ghost-btn">Refresh</button>
          <button id="logout-btn" class="ghost-btn">Logout</button>
        </div>
      </nav>
      <p class="helper-text">
        <strong>Refresh</strong> reloads portfolio data from server. <strong>Logout</strong> ends your current session on this browser.
      </p>

      <header class="app-head">
        <div>
          <p class="eyebrow">Dashboard</p>
          <h1>Your skin trading command center</h1>
        </div>
        <div class="kpi-grid">
          <article class="kpi-card">
            <span>Total Value</span>
            <strong>${formatMoney(portfolio.totalValue)}</strong>
          </article>
          <article class="kpi-card">
            <span>ROI</span>
            <strong>${formatPercent(portfolio.roiPercent)}</strong>
          </article>
          <article class="kpi-card ${oneDayTrendClass}">
            <span>24H Change</span>
            <strong>${formatPercent(portfolio.oneDayChangePercent)}</strong>
          </article>
          <article class="kpi-card ${trendClass}">
            <span>7D Change</span>
            <strong>${formatPercent(portfolio.sevenDayChangePercent)}</strong>
          </article>
        </div>
      </header>

      ${state.error ? `<div class="error">${escapeHtml(state.error)}</div>` : ""}
      ${renderAlerts()}
      ${renderAnalytics()}

      <section class="grid dashboard-grid">
        <article class="panel">
          <h2>Steam Sync</h2>
          <p class="helper-text">1) Enter your 17-digit SteamID64 and click <strong>Connect Steam ID</strong> once. 2) Click <strong>Sync Inventory</strong> to import items and latest prices.</p>
          <form id="steam-form" class="form">
            <label>SteamID64
              <input id="steam-id" placeholder="7656119..." />
            </label>
            <button type="submit">Connect Steam ID</button>
          </form>
          <button id="sync-btn" ${state.syncingInventory ? "disabled" : ""}>
            ${
              state.syncingInventory
                ? '<span class="loading-inline"><span class="spinner"></span>Syncing inventory...</span>'
                : "Sync Inventory"
            }
          </button>
          ${
            state.syncingInventory
              ? '<p class="muted sync-note">Fetching inventory and market prices. This can take up to a minute.</p>'
              : ""
          }
          ${renderSyncSummary()}
        </article>

        <article class="panel">
          <h2>Skin Lookup</h2>
          <p class="helper-text">Paste a Steam Item ID from your holdings and click <strong>Inspect Skin</strong> to see current price and recent history.</p>
          <form id="skin-form" class="form">
            <label>Steam Item ID
              <input id="steam-item-id" inputmode="numeric" pattern="[0-9]+" placeholder="e.g. 35719462921" value="${escapeHtml(state.inspectedSteamItemId)}" />
            </label>
            <button type="submit">Inspect Skin</button>
          </form>
          ${renderSkinDetails()}
        </article>
      </section>

      <section class="grid">
        <article class="panel wide">
          <h2>Holdings</h2>
          <p class="helper-text">Use the <strong>Inspect</strong> button in any row to auto-fill that item ID into Skin Lookup.</p>
          <table>
            <thead>
              <tr><th>Steam Item ID</th><th>Skin</th><th>Qty</th><th>Price</th><th>Status</th><th>Confidence</th><th>Value</th><th>Actions</th></tr>
            </thead>
            <tbody>${renderPortfolioRows()}</tbody>
          </table>
        </article>
      </section>

      <section class="grid">
        <article class="panel wide">
          <h2>7-Day Portfolio History</h2>
          ${renderHistoryChart()}
        </article>
      </section>
    </main>
  `;

  document.querySelector("#logout-btn").addEventListener("click", logout);
  document.querySelector("#steam-form").addEventListener("submit", connectSteam);
  document.querySelector("#sync-btn").addEventListener("click", syncInventory);
  document.querySelector("#refresh-btn").addEventListener("click", refreshPortfolio);
  document.querySelector("#skin-form").addEventListener("submit", findSkin);
  document.querySelectorAll(".inspect-skin-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const steamItemId = btn.getAttribute("data-steam-item-id");
      const input = document.querySelector("#steam-item-id");
      if (input) {
        input.value = String(steamItemId || "");
      }
      inspectSkinBySteamItemId(steamItemId);
    });
  });
}

function render() {
  if (!state.authenticated) {
    renderPublicHome();
    return;
  }

  renderApp();
}

render();
refreshPortfolio().catch(() => {
  render();
});

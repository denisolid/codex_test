import "./style.css";
import { API_URL } from "./config";
import { clearAuthToken, getAuthToken, withAuthHeaders } from "./authToken";
import {
  defaultCaseImage,
  defaultSkinImage,
  getRarityColor,
  isCaseLikeItem,
  normalizeRarity,
  resolveItemImageUrl
} from "./rarity";
import { renderSkinCard, renderSkinCardSkeleton } from "./components/skinCard";
import { renderInspectModal } from "./components/inspectModal";
import { renderSection } from "./components/uiPrimitives";
const app = document.querySelector("#app");
const SUPPORTED_CURRENCIES = ["USD", "EUR", "GBP", "UAH", "PLN", "CZK"];
const CURRENCY_STORAGE_KEY = "cs2sa:selected_currency";
const PRICING_MODE_STORAGE_KEY = "cs2sa:pricing_mode";
const PRICING_MODE_LABELS = {
  steam: "Steam Price",
  best_sell_net: "Best Sell Net",
  lowest_buy: "Lowest Buy"
};
const SEARCH_RENDER_DEBOUNCE_MS = 120;

function normalizeCurrencyCode(value) {
  const code = String(value || "")
    .trim()
    .toUpperCase();
  if (SUPPORTED_CURRENCIES.includes(code)) {
    return code;
  }
  return "USD";
}

function normalizePricingMode(value) {
  const mode = String(value || "")
    .trim()
    .toLowerCase();
  if (Object.prototype.hasOwnProperty.call(PRICING_MODE_LABELS, mode)) {
    return mode;
  }
  return "lowest_buy";
}

function createExitWhatIfState() {
  return {
    quantity: "1",
    targetSellPrice: "0",
    commissionPercent: "13",
    result: null,
    loading: false
  };
}

const state = {
  sessionBooting: true,
  authenticated: false,
  authProfile: null,
  portfolio: null,
  portfolioLoading: false,
  history: [],
  transactions: [],
  alertsFeed: [],
  alertEvents: [],
  ownershipAlertEvents: [],
  marketInsight: null,
  alerts: [],
  skin: null,
  inspectedSteamItemId: "",
  inspectModal: {
    open: false,
    loading: false,
    error: "",
    steamItemId: "",
    skin: null,
    marketInsight: null,
    exitWhatIf: createExitWhatIfState()
  },
  activeTab: "dashboard",
  historyDays: 7,
  holdingsView: {
    q: "",
    status: "all",
    sort: "value_desc",
    page: 1,
    pageSize: 10
  },
  transactionsView: {
    q: "",
    type: "all",
    sort: "date_desc",
    page: 1,
    pageSize: 10
  },
  csvImport: {
    running: false,
    summary: null
  },
  exitWhatIf: createExitWhatIfState(),
  error: "",
  syncingInventory: false,
  syncSummary: null,
  currency: normalizeCurrencyCode(localStorage.getItem(CURRENCY_STORAGE_KEY) || "USD"),
  pricingMode: normalizePricingMode(
    localStorage.getItem(PRICING_MODE_STORAGE_KEY) || "lowest_buy"
  ),
  compareExpandedBySkinId: {},
  compareRefreshing: false,
  txSubmitting: false,
  txForm: {
    skinId: "",
    type: "buy",
    quantity: "1",
    unitPrice: "0",
    commissionPercent: "13"
  },
  tradeCalc: {
    buyPrice: "0",
    sellPrice: "0",
    quantity: "1",
    commissionPercent: "13",
    loading: false,
    result: null
  },
  alertForm: {
    mode: "create",
    alertId: null,
    skinId: "",
    targetPrice: "",
    percentChangeThreshold: "",
    direction: "both",
    cooldownMinutes: "60",
    enabled: true,
    submitting: false
  },
  marketTab: {
    skinId: "",
    commissionPercent: "13",
    loading: false,
    inventoryValue: null,
    autoLoaded: false,
    insight: null
  },
  accountNotice: "",
  steamOnboardingPending: false,
  social: {
    scope: "global",
    watchlist: [],
    leaderboard: [],
    newSteamId: "",
    loading: false
  },
  publicPage: {
    steamId64: null,
    loading: false,
    payload: null,
    error: ""
  },
  backtest: {
    days: "90",
    loading: false,
    result: null
  },
  teamDashboard: {
    loading: false,
    payload: null
  }
};

const holdingsValueMemory = new Map();
const metricCounterMemory = new Map();
let delegatedAppEventsBound = false;

function debounce(fn, waitMs = 100) {
  let timer = null;
  return (...args) => {
    if (timer) {
      clearTimeout(timer);
    }
    timer = setTimeout(() => {
      timer = null;
      fn(...args);
    }, waitMs);
  };
}

const debouncedRender = debounce(() => render(), SEARCH_RENDER_DEBOUNCE_MS);

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

function getHeaderEmailLabel() {
  const email = String(state.authProfile?.email || "").trim();
  if (!email) return "Signed in";
  if (email.length <= 32) return email;
  return `${email.slice(0, 29)}...`;
}

function buildSteamAuthStartUrl(mode = "login") {
  const steamMode = String(mode || "login").toLowerCase();
  const path = steamMode === "link" ? "/auth/steam/link/start" : "/auth/steam/start";
  const url = new URL(`${API_URL}${path}`);
  const next = `${window.location.origin}/auth-callback.html`;
  url.searchParams.set("next", next);

  // Linking uses a backend redirect flow where custom auth headers cannot be attached.
  // Include bearer token as fallback for browsers that block cross-site cookies.
  if (steamMode === "link") {
    const token = getAuthToken();
    if (token) {
      url.searchParams.set("accessToken", token);
    }
  }

  return url.toString();
}

function getPublicSteamIdFromPath() {
  const match = String(window.location.pathname || "")
    .trim()
    .match(/^\/u\/(\d{17})\/?$/i);
  return match ? match[1] : null;
}

function buildAuthProfile(payload) {
  const user = payload?.user || null;
  if (!user) return null;

  const profile = payload?.profile || {};
  const metadata = user?.user_metadata || {};
  const explicit = payload?.emailConfirmed;
  const fallback = Boolean(user.email_confirmed_at || user.confirmed_at);
  const steamId64 = String(profile.steamId64 || metadata.steam_id64 || "").trim();

  return {
    email: String(user.email || ""),
    emailConfirmed: typeof explicit === "boolean" ? explicit : fallback,
    steamId64: steamId64 || null,
    steamLinked: Boolean(steamId64) || Boolean(profile.linkedSteam),
    steamDisplayName:
      String(profile.displayName || metadata.display_name || "").trim() || null,
    steamAvatarUrl: String(profile.avatarUrl || metadata.avatar_url || "").trim() || null,
    publicPortfolioEnabled: profile.publicPortfolioEnabled !== false,
    ownershipAlertsEnabled: profile.ownershipAlertsEnabled !== false,
    planTier: String(profile.planTier || "free").toLowerCase(),
    billingStatus: String(profile.billingStatus || "inactive").toLowerCase(),
    planSeats: Number(profile.planSeats || 1),
    planStartedAt: profile.planStartedAt || null,
    entitlements: profile.entitlements || null,
    provider: String(profile.provider || metadata.provider || "").trim() || "email"
  };
}

function formatMoney(amount, currencyCode = state.currency) {
  const value = Number(amount || 0);
  const absValue = Math.abs(value);
  const maxFractionDigits = absValue > 0 && absValue < 1 ? 4 : 2;
  const code = normalizeCurrencyCode(currencyCode);
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: code,
      minimumFractionDigits: 2,
      maximumFractionDigits: maxFractionDigits
    }).format(value);
  } catch (_err) {
    return `${code} ${value.toFixed(maxFractionDigits)}`;
  }
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

function formatRelativeTime(isoValue) {
  if (!isoValue) return "-";
  const ts = new Date(isoValue).getTime();
  if (Number.isNaN(ts)) return "-";
  const deltaSeconds = Math.round((Date.now() - ts) / 1000);
  if (deltaSeconds < 60) return "just now";
  if (deltaSeconds < 3600) return `${Math.floor(deltaSeconds / 60)}m ago`;
  if (deltaSeconds < 86400) return `${Math.floor(deltaSeconds / 3600)}h ago`;
  return `${Math.floor(deltaSeconds / 86400)}d ago`;
}

function getPricingModeLabel(mode) {
  return PRICING_MODE_LABELS[normalizePricingMode(mode)] || PRICING_MODE_LABELS.lowest_buy;
}

function formatSignedMoney(amount, currencyCode = state.currency) {
  const value = Number(amount || 0);
  if (!Number.isFinite(value)) return "-";
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${formatMoney(value, currencyCode)}`;
}

function buildPortfolioSignals() {
  const analytics = state.portfolio?.analytics || {};
  const holdingsCount = Number(
    analytics.holdingsCount || state.portfolio?.items?.length || 0
  );
  const staleItems = Number(state.portfolio?.staleItemsCount || 0);
  const unpricedItems = Number(state.portfolio?.unpricedItemsCount || 0);
  const liquidItems = Math.max(holdingsCount - staleItems - unpricedItems, 0);
  const liquidityScore =
    holdingsCount > 0 ? Math.round((liquidItems / holdingsCount) * 100) : null;
  const liquidityBand =
    liquidityScore == null
      ? "unknown"
      : liquidityScore >= 85
        ? "high"
        : liquidityScore >= 60
          ? "medium"
          : "low";

  const baseRiskScore =
    analytics.concentrationRisk === "high"
      ? 78
      : analytics.concentrationRisk === "medium"
        ? 58
        : 34;
  const breadthPenalty =
    analytics?.breadth?.advancerRatioPercent == null
      ? 0
      : Math.max(0, 50 - Number(analytics.breadth.advancerRatioPercent || 0)) * 0.24;
  const riskScore = Math.min(100, Math.round(baseRiskScore + breadthPenalty));
  const riskBand = riskScore >= 70 ? "high" : riskScore >= 45 ? "medium" : "low";

  return {
    holdingsCount,
    staleItems,
    unpricedItems,
    liquidityScore,
    liquidityBand,
    riskScore,
    riskBand
  };
}

function getHoldingsSortDirection(field) {
  const current = String(state.holdingsView.sort || "value_desc");
  if (field === "name") {
    if (current === "name_asc") return "asc";
    if (current === "name_desc") return "desc";
    return "none";
  }
  if (field === "qty") {
    if (current === "qty_asc") return "asc";
    if (current === "qty_desc") return "desc";
    return "none";
  }
  if (field === "change") {
    if (current === "change_asc") return "asc";
    if (current === "change_desc") return "desc";
    return "none";
  }
  if (field === "value" || field === "price") {
    if (current === "value_asc") return "asc";
    if (current === "value_desc") return "desc";
    return "none";
  }
  return "none";
}

function getNextHoldingsSort(field) {
  const direction = getHoldingsSortDirection(field);
  if (field === "name") {
    return direction === "desc" ? "name_asc" : "name_desc";
  }
  if (field === "qty") {
    return direction === "desc" ? "qty_asc" : "qty_desc";
  }
  if (field === "change") {
    return direction === "desc" ? "change_asc" : "change_desc";
  }
  if (field === "value" || field === "price") {
    return direction === "desc" ? "value_asc" : "value_desc";
  }
  return "value_desc";
}

function renderHoldingsSortButton(label, field) {
  const direction = getHoldingsSortDirection(field);
  const arrow = direction === "asc" ? "\u2191" : direction === "desc" ? "\u2193" : "\u2195";
  const active = direction !== "none" ? "active" : "";

  return `
    <button
      type="button"
      class="table-sort-btn holdings-sort-btn ${active}"
      data-sort-next="${escapeHtml(getNextHoldingsSort(field))}"
      aria-label="Sort by ${escapeHtml(label)}"
    >
      ${escapeHtml(label)} <span class="sort-indicator">${arrow}</span>
    </button>
  `;
}

function formatCounterValue(value, format = "number", currencyCode = state.currency) {
  if (format === "money") return formatMoney(value, currencyCode);
  if (format === "percent") return formatPercent(value);
  if (format === "integer") return formatNumber(value, 0);
  return formatNumber(value, 2);
}

function animateMetricCounters() {
  if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;

  document.querySelectorAll("[data-count-to]").forEach((node, index) => {
    const target = Number(node.getAttribute("data-count-to"));
    if (!Number.isFinite(target)) return;

    const format = String(node.getAttribute("data-count-format") || "number");
    const currencyCode = normalizeCurrencyCode(
      node.getAttribute("data-count-currency") || state.currency
    );
    const key = String(node.getAttribute("data-count-key") || `metric-${index}`);
    const hasPrevious = metricCounterMemory.has(key);
    const previous = hasPrevious ? Number(metricCounterMemory.get(key)) : target * 0.85;

    if (Math.abs(previous - target) < 0.01) {
      node.textContent = formatCounterValue(target, format, currencyCode);
      metricCounterMemory.set(key, target);
      return;
    }

    const duration = 560;
    const startAt = performance.now();
    const startValue = Number.isFinite(previous) ? previous : target;

    const step = (now) => {
      const t = Math.min((now - startAt) / duration, 1);
      const eased = 1 - (1 - t) ** 3;
      const value = startValue + (target - startValue) * eased;
      node.textContent = formatCounterValue(value, format, currencyCode);
      if (t < 1) {
        requestAnimationFrame(step);
      } else {
        node.textContent = formatCounterValue(target, format, currencyCode);
      }
    };

    metricCounterMemory.set(key, target);
    requestAnimationFrame(step);
  });
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

function getItemRarityTheme(item = {}) {
  const rarity = normalizeRarity(item.rarity, item.marketHashName);
  const color = getRarityColor(item.rarity, item.marketHashName, item.rarityColor);
  return { rarity, color };
}

function getItemImageUrl(item = {}) {
  return resolveItemImageUrl(item);
}

function formatManagementClue(clue) {
  if (!clue) return "-";
  const action = String(clue.action || "watch").toLowerCase();
  const confidence = Math.round(Number(clue.confidence || 0));
  const expectedMove = Number(clue?.prediction?.expectedMovePercent);
  const moveLabel = Number.isFinite(expectedMove)
    ? `${expectedMove > 0 ? "+" : ""}${expectedMove.toFixed(2)}% 7D est`
    : "No projection";
  const title = Array.isArray(clue.reasons) ? clue.reasons.join(" | ") : "Item guidance";

  return `
    <div class="hint-cell" title="${escapeHtml(title)}">
      <span class="hint-badge ${escapeHtml(action)}">${escapeHtml(
    `${toTitle(action)} ${confidence}%`
  )}</span>
      <small>${escapeHtml(moveLabel)}</small>
    </div>
  `;
}

async function api(path, options = {}) {
  const headers = withAuthHeaders({
    "Content-Type": "application/json",
    ...(options.headers || {})
  });

  const res = await fetch(`${API_URL}${path}`, {
    ...options,
    credentials: "include",
    headers
  });

  const payload = await res.json().catch(() => ({}));
  if (res.status === 401) {
    clearAuthToken();
    state.authenticated = false;
    state.authProfile = null;
  }
  if (!res.ok) {
    throw new Error(payload.error || "Request failed");
  }

  state.authenticated = true;
  return payload;
}

function withCurrency(path) {
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}currency=${encodeURIComponent(state.currency)}`;
}

function clampInt(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.min(Math.max(Math.floor(n), min), max);
}

function paginate(items, page, pageSize) {
  const total = items.length;
  const safePageSize = Math.max(Number(pageSize) || 10, 1);
  const pages = Math.max(Math.ceil(total / safePageSize), 1);
  const safePage = clampInt(page, 1, pages);
  const start = (safePage - 1) * safePageSize;
  return {
    items: items.slice(start, start + safePageSize),
    page: safePage,
    pages,
    total
  };
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur.trim());
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur.trim());
  return out;
}

function parseTransactionCsv(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    throw new Error("CSV must include header + at least one row.");
  }

  const headerCols = parseCsvLine(lines[0]).map((x) => x.trim().toLowerCase());
  const idx = (name) => headerCols.indexOf(name);

  const required = ["skinid", "type", "quantity", "unitprice"];
  for (const key of required) {
    if (idx(key) === -1) {
      throw new Error(
        `CSV header missing "${key}". Required: skinId,type,quantity,unitPrice.`
      );
    }
  }

  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const raw = parseCsvLine(lines[i]);
    const row = {
      skinId: Number(raw[idx("skinid")]),
      type: String(raw[idx("type")] || "").toLowerCase(),
      quantity: Number(raw[idx("quantity")]),
      unitPrice: Number(raw[idx("unitprice")]),
      commissionPercent:
        idx("commissionpercent") === -1
          ? 13
          : Number(raw[idx("commissionpercent")]),
      executedAt: idx("executedat") === -1 ? undefined : raw[idx("executedat")] || undefined
    };
    rows.push({ row, lineNo: i + 1 });
  }

  return rows;
}

function sanitizeForSearch(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function sortByDate(a, b, key, asc = false) {
  const ta = new Date(a[key] || 0).getTime();
  const tb = new Date(b[key] || 0).getTime();
  return asc ? ta - tb : tb - ta;
}

function buildItemNameMap() {
  return Object.fromEntries(
    (state.portfolio?.items || []).map((item) => [Number(item.skinId), item.marketHashName])
  );
}

function getFilteredHoldings() {
  const items = Array.isArray(state.portfolio?.items) ? [...state.portfolio.items] : [];
  const q = sanitizeForSearch(state.holdingsView.q);
  const status = String(state.holdingsView.status || "all").toLowerCase();
  const sort = String(state.holdingsView.sort || "value_desc");

  const filtered = items.filter((item) => {
    const matchesQ =
      !q ||
      sanitizeForSearch(item.marketHashName).includes(q) ||
      sanitizeForSearch(item.primarySteamItemId).includes(q);
    const matchesStatus = status === "all" || sanitizeForSearch(item.priceStatus) === status;
    return matchesQ && matchesStatus;
  });

  filtered.sort((a, b) => {
    const rankAction = (item, mode) => {
      const action = String(item?.managementClue?.action || "watch").toLowerCase();
      if (mode === "sell_first") {
        if (action === "sell") return 0;
        if (action === "watch") return 1;
        return 2;
      }
      if (action === "hold") return 0;
      if (action === "watch") return 1;
      return 2;
    };
    if (sort === "value_asc") return Number(a.lineValue || 0) - Number(b.lineValue || 0);
    if (sort === "name_asc") return String(a.marketHashName || "").localeCompare(String(b.marketHashName || ""));
    if (sort === "name_desc") return String(b.marketHashName || "").localeCompare(String(a.marketHashName || ""));
    if (sort === "qty_desc") return Number(b.quantity || 0) - Number(a.quantity || 0);
    if (sort === "qty_asc") return Number(a.quantity || 0) - Number(b.quantity || 0);
    if (sort === "change_desc") return Number(b.sevenDayChangePercent || 0) - Number(a.sevenDayChangePercent || 0);
    if (sort === "change_asc") return Number(a.sevenDayChangePercent || 0) - Number(b.sevenDayChangePercent || 0);
    if (sort === "clue_sell_first") {
      return rankAction(a, "sell_first") - rankAction(b, "sell_first");
    }
    if (sort === "clue_hold_first") {
      return rankAction(a, "hold_first") - rankAction(b, "hold_first");
    }
    return Number(b.lineValue || 0) - Number(a.lineValue || 0);
  });

  return paginate(filtered, state.holdingsView.page, state.holdingsView.pageSize);
}

function getFilteredTransactions() {
  const rows = Array.isArray(state.transactions) ? [...state.transactions] : [];
  const q = sanitizeForSearch(state.transactionsView.q);
  const type = String(state.transactionsView.type || "all").toLowerCase();
  const sort = String(state.transactionsView.sort || "date_desc");
  const itemNameBySkinId = buildItemNameMap();

  const filtered = rows.filter((tx) => {
    const txType = sanitizeForSearch(tx.type);
    const name = sanitizeForSearch(itemNameBySkinId[Number(tx.skin_id)] || `skin #${tx.skin_id}`);
    const matchesType = type === "all" || txType === type;
    const matchesQ = !q || name.includes(q) || String(tx.skin_id || "").includes(q);
    return matchesType && matchesQ;
  });

  filtered.sort((a, b) => {
    if (sort === "date_asc") return sortByDate(a, b, "executed_at", true);
    if (sort === "net_desc") return Number(b.net_total || 0) - Number(a.net_total || 0);
    if (sort === "net_asc") return Number(a.net_total || 0) - Number(b.net_total || 0);
    return sortByDate(a, b, "executed_at", false);
  });

  return paginate(filtered, state.transactionsView.page, state.transactionsView.pageSize);
}

function computeItemTradeStats(skinId) {
  const txs = (state.transactions || [])
    .filter((tx) => Number(tx.skin_id) === Number(skinId))
    .sort((a, b) => sortByDate(a, b, "executed_at", true));

  let qty = 0;
  let cost = 0;
  let realized = 0;
  const timeline = [];

  for (const tx of txs) {
    const txQty = Number(tx.quantity || 0);
    const unitPrice = Number(tx.unit_price || 0);
    const txNet =
      tx.net_total != null
        ? Number(tx.net_total)
        : tx.type === "sell"
          ? txQty * unitPrice * (1 - Number(tx.commission_percent || 0) / 100)
          : txQty * unitPrice;

    if (tx.type === "buy") {
      qty += txQty;
      cost += txNet;
    } else if (tx.type === "sell" && qty > 0) {
      const sellQty = Math.min(txQty, qty);
      const avg = cost / qty;
      const proceeds = txNet * (sellQty / txQty);
      const removedCost = avg * sellQty;
      realized += proceeds - removedCost;
      qty -= sellQty;
      cost -= removedCost;
      if (qty <= 0) {
        qty = 0;
        cost = 0;
      }
    }

    timeline.push({
      id: tx.id,
      date: String(tx.executed_at || "").slice(0, 10),
      type: tx.type,
      quantity: txQty,
      unitPrice,
      netTotal: txNet
    });
  }

  return {
    avgEntryPrice: qty > 0 ? cost / qty : null,
    openQuantity: qty,
    realizedPnl: realized,
    timeline: timeline.reverse()
  };
}

function getHoldingsList() {
  return Array.isArray(state.portfolio?.items) ? state.portfolio.items : [];
}

function buildHoldingOptions(selectedSkinId) {
  const holdings = getHoldingsList();
  if (!holdings.length) {
    return '<option value="">No synced items</option>';
  }

  return holdings
    .map((item) => {
      const value = String(item.skinId);
      return `<option value="${escapeHtml(value)}" ${
        value === String(selectedSkinId || "") ? "selected" : ""
      }>${escapeHtml(item.marketHashName)} (ID ${escapeHtml(value)})</option>`;
    })
    .join("");
}

function buildQuery(params) {
  const qs = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value == null || value === "") return;
    qs.set(key, String(value));
  });
  const raw = qs.toString();
  return raw ? `?${raw}` : "";
}

function hydrateTabDefaults() {
  const holdings = getHoldingsList();
  const firstSkinId = holdings[0]?.skinId ? String(holdings[0].skinId) : "";

  if (!state.alertForm.skinId || !holdings.some((h) => String(h.skinId) === String(state.alertForm.skinId))) {
    state.alertForm.skinId = firstSkinId;
  }

  if (!state.marketTab.skinId || !holdings.some((h) => String(h.skinId) === String(state.marketTab.skinId))) {
    state.marketTab.skinId = firstSkinId;
  }
}

function runUiTask(task) {
  Promise.resolve()
    .then(task)
    .catch((err) => {
      const message = String(err?.message || "Request failed");
      setError(message);
    });
}

function syncMarketCommissionFromElement(el) {
  if (!el) return;
  const value = String(el.value || "");
  state.marketTab.commissionPercent = value;
  const primary = document.querySelector("#market-commission");
  const inline = document.querySelector("#market-commission-inline");
  if (primary && primary !== el) primary.value = value;
  if (inline && inline !== el) inline.value = value;
}

async function savePricePreferences(patch = {}, options = {}) {
  const { silent = true } = options;
  try {
    const payload = await api("/market/preferences", {
      method: "PATCH",
      body: JSON.stringify(patch)
    });
    if (payload?.pricingMode) {
      state.pricingMode = normalizePricingMode(payload.pricingMode);
    }
  } catch (err) {
    if (!silent) {
      throw err;
    }
  }
}

async function handleCurrencySelectChange(nextCurrency) {
  if (nextCurrency === state.currency) return;

  state.currency = nextCurrency;
  try {
    localStorage.setItem(CURRENCY_STORAGE_KEY, nextCurrency);
  } catch (_err) {
    // Ignore storage write errors.
  }
  await savePricePreferences(
    {
      preferredCurrency: nextCurrency,
      pricingMode: state.pricingMode
    },
    { silent: true }
  );
  state.marketTab.inventoryValue = null;
  state.marketTab.autoLoaded = false;
  state.marketTab.insight = null;
  state.marketInsight = null;

  await refreshPortfolio();
  if (state.inspectedSteamItemId) {
    await inspectSkinBySteamItemId(state.inspectedSteamItemId);
  }
  if (state.inspectModal.open && state.inspectModal.steamItemId) {
    await openInspectModalBySteamItemId(state.inspectModal.steamItemId);
  }
}

async function handlePricingModeChange(nextMode) {
  const normalized = normalizePricingMode(nextMode);
  if (normalized === state.pricingMode) return;

  state.pricingMode = normalized;
  try {
    localStorage.setItem(PRICING_MODE_STORAGE_KEY, normalized);
  } catch (_err) {
    // Ignore storage write errors.
  }

  await savePricePreferences(
    {
      pricingMode: normalized,
      preferredCurrency: state.currency
    },
    { silent: true }
  );

  await refreshPortfolio({ silent: true });
}

function buildComparisonItemsPayload(items = []) {
  return (Array.isArray(items) ? items : [])
    .map((item) => ({
      skinId: Number(item.skinId || 0) || null,
      marketHashName: String(item.marketHashName || "").trim(),
      quantity: Number(item.quantity || 0),
      steamPrice: Number(item.steamPrice || item.currentPrice || 0),
      steamCurrency: state.portfolio?.currency || state.currency,
      steamRecordedAt: item.currentPriceRecordedAt || null
    }))
    .filter((row) => row.marketHashName);
}

async function refreshVisibleMarketComparisons() {
  if (state.compareRefreshing) return;

  const { items } = getFilteredHoldings();
  const payloadItems = buildComparisonItemsPayload(items);
  if (!payloadItems.length) return;

  clearError();
  state.compareRefreshing = true;
  render();

  try {
    await api("/market/compare", {
      method: "POST",
      body: JSON.stringify({
        items: payloadItems,
        pricingMode: state.pricingMode,
        forceRefresh: true,
        allowLiveFetch: true,
        currency: state.currency
      })
    });
    await refreshPortfolio({ silent: true });
  } catch (err) {
    setError(err.message);
  } finally {
    state.compareRefreshing = false;
    render();
  }
}

async function handleTabSwitch(tab) {
  if (!tab || tab === state.activeTab) return;
  state.activeTab = tab;
  render();

  if (tab === "portfolio") {
    runUiTask(() => refreshVisibleMarketComparisons());
  }

  if (
    tab === "team" &&
    String(state.authProfile?.planTier || "free").toLowerCase() === "team" &&
    !state.teamDashboard.loading &&
    !state.teamDashboard.payload
  ) {
    await refreshTeamDashboard({ silent: true });
  }
}

function triggerInspectBySteamItemId(rawSteamItemId) {
  const steamItemId = String(rawSteamItemId || "").trim();
  if (!steamItemId) return;
  runUiTask(() => openInspectModalBySteamItemId(steamItemId));
}

function closeInspectModal() {
  if (!state.inspectModal.open) return;
  state.inspectModal.open = false;
  state.inspectModal.loading = false;
  state.inspectModal.error = "";
  state.inspectModal.steamItemId = "";
  state.inspectModal.skin = null;
  state.inspectModal.marketInsight = null;
  state.inspectModal.exitWhatIf = createExitWhatIfState();
  render();
}

function onAppClick(event) {
  if (!app) return;
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  if (target.matches("[data-inspect-modal-overlay]")) {
    event.preventDefault();
    closeInspectModal();
    return;
  }

  const button = target.closest("button");

  if (button?.matches("[data-inspect-modal-close]")) {
    event.preventDefault();
    closeInspectModal();
    return;
  }

  if (button?.matches("#logout-btn")) {
    event.preventDefault();
    runUiTask(() => logout());
    return;
  }

  if (button?.matches("#refresh-btn")) {
    event.preventDefault();
    runUiTask(() => refreshPortfolio());
    return;
  }

  if (button?.matches(".tab-btn")) {
    event.preventDefault();
    const tab = button.getAttribute("data-tab");
    runUiTask(() => handleTabSwitch(tab));
    return;
  }

  if (button?.matches(".tab-jump-btn")) {
    event.preventDefault();
    const tab = button.getAttribute("data-tab-target");
    runUiTask(() => handleTabSwitch(tab));
    return;
  }

  if (button?.matches("#sync-btn")) {
    event.preventDefault();
    runUiTask(() => syncInventory());
    return;
  }

  if (button?.matches(".holdings-sort-btn")) {
    event.preventDefault();
    const nextSort = String(button.getAttribute("data-sort-next") || "value_desc");
    state.holdingsView.sort = nextSort;
    const sortSelect = document.querySelector("#holdings-sort");
    if (sortSelect) {
      sortSelect.value = nextSort;
    }
    render();
    return;
  }

  if (button?.matches(".holdings-page-btn")) {
    event.preventDefault();
    state.holdingsView.page = clampInt(
      button.getAttribute("data-page"),
      1,
      Math.max(getFilteredHoldings().pages, 1)
    );
    render();
    return;
  }

  if (button?.matches(".history-range-btn")) {
    event.preventDefault();
    const days = clampInt(button.getAttribute("data-history-days"), 1, 180);
    if (days === state.historyDays) return;
    state.historyDays = days;
    runUiTask(() => refreshPortfolio());
    return;
  }

  const tile = target.closest(".portfolio-skin-card-clickable");
  if (tile) {
    event.preventDefault();
    triggerInspectBySteamItemId(tile.getAttribute("data-steam-item-id"));
    return;
  }

  if (button?.matches(".inspect-skin-btn")) {
    event.preventDefault();
    triggerInspectBySteamItemId(button.getAttribute("data-steam-item-id"));
    return;
  }

  if (button?.matches(".compare-market-btn")) {
    event.preventDefault();
    const skinId = Number(button.getAttribute("data-skin-id") || 0);
    if (!Number.isInteger(skinId) || skinId <= 0) return;
    state.compareExpandedBySkinId[skinId] = !state.compareExpandedBySkinId[skinId];
    render();
    return;
  }

  if (button?.matches("#refresh-market-compare-btn")) {
    event.preventDefault();
    runUiTask(() => refreshVisibleMarketComparisons());
    return;
  }

  if (button?.matches("#export-portfolio-btn")) {
    event.preventDefault();
    runUiTask(() => exportPortfolioCsv());
    return;
  }

  if (button?.matches("#export-transactions-btn")) {
    event.preventDefault();
    runUiTask(() => exportTransactionsCsv());
    return;
  }

  if (button?.matches(".tx-page-btn")) {
    event.preventDefault();
    state.transactionsView.page = clampInt(
      button.getAttribute("data-page"),
      1,
      Math.max(getFilteredTransactions().pages, 1)
    );
    render();
    return;
  }

  if (button?.matches(".tx-delete-btn")) {
    event.preventDefault();
    const txId = button.getAttribute("data-tx-id");
    if (!txId) return;
    runUiTask(() => removeTransaction(txId));
    return;
  }

  if (button?.matches(".alert-edit-btn")) {
    event.preventDefault();
    startEditAlert(button.getAttribute("data-alert-id"));
    return;
  }

  if (button?.matches(".alert-toggle-btn")) {
    event.preventDefault();
    runUiTask(() =>
      toggleAlertEnabled(
        button.getAttribute("data-alert-id"),
        button.getAttribute("data-enabled") === "true"
      )
    );
    return;
  }

  if (button?.matches(".alert-delete-btn")) {
    event.preventDefault();
    runUiTask(() => removeAlert(button.getAttribute("data-alert-id")));
    return;
  }

  if (button?.matches("#alert-cancel-btn")) {
    event.preventDefault();
    cancelEditAlert();
    return;
  }

  if (button?.matches(".watch-remove-btn")) {
    event.preventDefault();
    runUiTask(() => removeWatchlistEntry(button.getAttribute("data-steam-id")));
    return;
  }

  if (button?.matches(".leaderboard-watch-btn")) {
    event.preventDefault();
    runUiTask(() =>
      toggleWatchFromLeaderboard(
        button.getAttribute("data-steam-id"),
        button.getAttribute("data-watching") === "1"
      )
    );
    return;
  }

  if (button?.matches(".plan-switch-btn")) {
    event.preventDefault();
    const planTier = String(button.getAttribute("data-plan-tier") || "").trim();
    if (!planTier) return;
    runUiTask(() => updatePlanTier(planTier));
    return;
  }

  if (button?.matches("#team-refresh-btn")) {
    event.preventDefault();
    runUiTask(() => refreshTeamDashboard());
  }
}

function onAppKeydown(event) {
  if (event.key === "Escape" && state.inspectModal.open) {
    event.preventDefault();
    closeInspectModal();
    return;
  }

  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const tile = target.closest(".portfolio-skin-card-clickable");
  if (!tile) return;
  if (event.key !== "Enter" && event.key !== " ") return;
  event.preventDefault();
  triggerInspectBySteamItemId(tile.getAttribute("data-steam-item-id"));
}

function onAppInput(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  if (target.matches("#backtest-days")) {
    state.backtest.days = String(target.value || "");
    return;
  }

  if (target.matches("#holdings-search")) {
    state.holdingsView.q = target.value;
    state.holdingsView.page = 1;
    debouncedRender();
    return;
  }

  if (target.matches("#tx-search")) {
    state.transactionsView.q = target.value;
    state.transactionsView.page = 1;
    debouncedRender();
    return;
  }

  if (target.matches("#social-watch-steam-id")) {
    state.social.newSteamId = target.value;
    return;
  }

  if (target.matches("#market-commission, #market-commission-inline")) {
    syncMarketCommissionFromElement(target);
  }
}

function onAppChange(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  if (target.matches("#currency-select")) {
    const nextCurrency = normalizeCurrencyCode(target.value);
    runUiTask(() => handleCurrencySelectChange(nextCurrency));
    return;
  }

  if (target.matches("#settings-currency-select")) {
    const nextCurrency = normalizeCurrencyCode(target.value);
    runUiTask(() => handleCurrencySelectChange(nextCurrency));
    return;
  }

  if (target.matches("#pricing-mode-select, #settings-pricing-mode")) {
    const nextMode = normalizePricingMode(target.value);
    runUiTask(() => handlePricingModeChange(nextMode));
    return;
  }

  if (target.matches("#holdings-status")) {
    state.holdingsView.status = target.value;
    state.holdingsView.page = 1;
    render();
    return;
  }

  if (target.matches("#holdings-sort")) {
    state.holdingsView.sort = target.value;
    render();
    return;
  }

  if (target.matches("#holdings-page-size")) {
    state.holdingsView.pageSize = clampInt(target.value, 1, 200);
    state.holdingsView.page = 1;
    render();
    return;
  }

  if (target.matches("#tx-filter-type")) {
    state.transactionsView.type = target.value;
    state.transactionsView.page = 1;
    render();
    return;
  }

  if (target.matches("#tx-sort")) {
    state.transactionsView.sort = target.value;
    render();
    return;
  }

  if (target.matches("#tx-page-size")) {
    state.transactionsView.pageSize = clampInt(target.value, 1, 200);
    state.transactionsView.page = 1;
    render();
    return;
  }

  if (target.matches("#social-scope")) {
    state.social.scope = String(target.value || "global");
    runUiTask(() => refreshSocialData());
  }
}

function onAppSubmit(event) {
  const form = event.target instanceof HTMLFormElement ? event.target : null;
  if (!form) return;

  if (form.id === "skin-form") {
    event.preventDefault();
    runUiTask(() => findSkin(event));
    return;
  }

  if (form.id === "exit-whatif-form") {
    event.preventDefault();
    runUiTask(() => calculateExitWhatIf(event, "inline"));
    return;
  }

  if (form.id === "inspect-modal-exit-whatif-form") {
    event.preventDefault();
    runUiTask(() => calculateExitWhatIf(event, "modal"));
    return;
  }

  if (form.id === "backtest-form") {
    event.preventDefault();
    runUiTask(() => runPortfolioBacktest(event));
    return;
  }

  if (form.id === "tx-form") {
    event.preventDefault();
    runUiTask(() => submitTransaction(event));
    return;
  }

  if (form.id === "tx-csv-form") {
    event.preventDefault();
    runUiTask(() => importTransactionsCsv(event));
    return;
  }

  if (form.id === "trade-calc-form") {
    event.preventDefault();
    runUiTask(() => calculateTrade(event));
    return;
  }

  if (form.id === "alert-form") {
    event.preventDefault();
    runUiTask(() => submitAlertForm(event));
    return;
  }

  if (form.id === "social-watch-form") {
    event.preventDefault();
    runUiTask(() => addWatchlistEntry(event));
    return;
  }

  if (form.id === "social-board-form") {
    event.preventDefault();
    runUiTask(() => refreshSocialData());
    return;
  }

  if (form.id === "market-inventory-form") {
    event.preventDefault();
    runUiTask(() => submitMarketInventoryRefresh(event));
    return;
  }

  if (form.id === "market-item-form") {
    event.preventDefault();
    runUiTask(() => submitMarketAnalyze(event));
    return;
  }

  if (form.id === "public-settings-form") {
    event.preventDefault();
    runUiTask(() => updatePublicPortfolioSettings(event));
    return;
  }

  if (form.id === "ownership-settings-form") {
    event.preventDefault();
    runUiTask(() => updateOwnershipAlertSettings(event));
  }
}

function ensureAppEventDelegation() {
  if (!app || delegatedAppEventsBound) return;

  app.addEventListener("click", onAppClick);
  app.addEventListener("keydown", onAppKeydown);
  app.addEventListener("input", onAppInput);
  app.addEventListener("change", onAppChange);
  app.addEventListener("submit", onAppSubmit);

  delegatedAppEventsBound = true;
}

async function logout() {
  try {
    await api("/auth/logout", { method: "POST" });
  } catch (_err) {
    // Continue local cleanup even if request fails.
  }

  clearAuthToken();
  state.authenticated = false;
  state.authProfile = null;
  state.portfolio = null;
  state.history = [];
  state.transactions = [];
  state.alerts = [];
  state.alertsFeed = [];
  state.alertEvents = [];
  state.ownershipAlertEvents = [];
  state.skin = null;
  state.marketInsight = null;
  state.inspectModal.open = false;
  state.inspectModal.loading = false;
  state.inspectModal.error = "";
  state.inspectModal.steamItemId = "";
  state.inspectModal.skin = null;
  state.inspectModal.marketInsight = null;
  state.inspectModal.exitWhatIf = createExitWhatIfState();
  state.exitWhatIf = createExitWhatIfState();
  resetAlertForm();
  state.marketTab.inventoryValue = null;
  state.marketTab.autoLoaded = false;
  state.marketTab.insight = null;
  state.inspectedSteamItemId = "";
  state.tradeCalc.result = null;
  state.accountNotice = "";
  state.steamOnboardingPending = false;
  state.social.watchlist = [];
  state.social.leaderboard = [];
  state.social.newSteamId = "";
  state.social.loading = false;
  state.backtest.days = "90";
  state.backtest.loading = false;
  state.backtest.result = null;
  state.teamDashboard.loading = false;
  state.teamDashboard.payload = null;
  render();
}

async function syncInventory() {
  if (state.syncingInventory) return;
  clearError();
  state.syncingInventory = true;
  render();
  try {
    const result = await api("/inventory/sync", { method: "POST" });
    state.syncSummary = result;
    if (state.steamOnboardingPending) {
      state.accountNotice = "Inventory synced successfully. Your portfolio is ready.";
      state.steamOnboardingPending = false;
    }
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
  const ownershipPreview = (s.ownershipChanges || [])
    .slice(0, 4)
    .map(
      (x) =>
        `<li><strong>${escapeHtml(x.marketHashName)}</strong> <span class="muted">${escapeHtml(
          toTitle(x.changeType)
        )}: ${Number(x.previousQuantity || 0)} -> ${Number(x.newQuantity || 0)}</span></li>`
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
      )} | Ownership changes: ${Number(s.ownershipChangesCount || 0)}</p>
      ${
        ownershipPreview
          ? `<p class="muted">Ownership changes (first 4):</p><ul class="sync-list">${ownershipPreview}</ul>`
          : ""
      }
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

async function refreshPortfolio(options = {}) {
  const { silent = false } = options;
  clearError();
  state.portfolioLoading = true;
  if (!silent) {
    render();
  }

  try {
    const portfolioPath = withCurrency(
      `/portfolio?pricingMode=${encodeURIComponent(state.pricingMode)}`
    );
    const [
      mePayload,
      portfolio,
      history,
      txPayload,
      alertsPayload,
      eventsPayload,
      ownershipEventsPayload,
      watchlistPayload,
      leaderboardPayload
    ] =
      await Promise.all([
        api("/auth/me"),
        api(portfolioPath),
        api(withCurrency(`/portfolio/history?days=${state.historyDays}`)),
        api("/transactions"),
        api("/alerts").catch(() => ({ items: [] })),
        api("/alerts/events?limit=50").catch(() => ({ items: [] })),
        api("/alerts/ownership-events?limit=50").catch(() => ({ items: [] })),
        api(withCurrency("/social/watchlist")).catch(() => ({ items: [] })),
        api(
          withCurrency(
            `/social/leaderboard?scope=${encodeURIComponent(
              state.social.scope || "global"
            )}&limit=25`
          )
        ).catch(() => ({ items: [] }))
      ]);

    state.authProfile = buildAuthProfile(mePayload);
    state.portfolio = portfolio;
    if (portfolio?.pricing?.mode) {
      state.pricingMode = normalizePricingMode(portfolio.pricing.mode);
      try {
        localStorage.setItem(PRICING_MODE_STORAGE_KEY, state.pricingMode);
      } catch (_err) {
        // Ignore storage write errors.
      }
    }
    state.history = history.points || [];
    state.transactions = Array.isArray(txPayload?.items) ? txPayload.items : [];
    state.alertsFeed = Array.isArray(alertsPayload?.items) ? alertsPayload.items : [];
    state.alertEvents = Array.isArray(eventsPayload?.items) ? eventsPayload.items : [];
    state.ownershipAlertEvents = Array.isArray(ownershipEventsPayload?.items)
      ? ownershipEventsPayload.items
      : [];
    state.social.watchlist = Array.isArray(watchlistPayload?.items)
      ? watchlistPayload.items
      : [];
    state.social.leaderboard = Array.isArray(leaderboardPayload?.items)
      ? leaderboardPayload.items
      : [];
    state.alerts = Array.isArray(portfolio.alerts) ? portfolio.alerts : [];

    const liveSkinIds = new Set(
      (Array.isArray(portfolio?.items) ? portfolio.items : [])
        .map((item) => Number(item?.skinId))
        .filter((id) => Number.isInteger(id) && id > 0)
    );
    for (const skinId of holdingsValueMemory.keys()) {
      if (!liveSkinIds.has(Number(skinId))) {
        holdingsValueMemory.delete(skinId);
      }
    }
    for (const skinId of Object.keys(state.compareExpandedBySkinId || {})) {
      if (!liveSkinIds.has(Number(skinId))) {
        delete state.compareExpandedBySkinId[skinId];
      }
    }

    state.marketTab.autoLoaded = false;
    hydrateTabDefaults();

    const holdings = getHoldingsList();
    if (
      holdings.length &&
      (!state.txForm.skinId ||
        !holdings.some((h) => String(h.skinId) === String(state.txForm.skinId)))
    ) {
      state.txForm.skinId = String(holdings[0].skinId);
    }

    const maxHoldingsPages = Math.max(
      Math.ceil((state.portfolio?.items?.length || 0) / state.holdingsView.pageSize),
      1
    );
    state.holdingsView.page = clampInt(state.holdingsView.page, 1, maxHoldingsPages);
    const maxTxPages = Math.max(
      Math.ceil((state.transactions.length || 0) / state.transactionsView.pageSize),
      1
    );
    state.transactionsView.page = clampInt(state.transactionsView.page, 1, maxTxPages);

    if (state.activeTab === "portfolio" && !silent) {
      runUiTask(() => refreshVisibleMarketComparisons());
    }
    return true;
  } catch (err) {
    if (!silent) {
      setError(err.message);
    }
    return false;
  } finally {
    state.portfolioLoading = false;
    render();
  }
}

async function refreshAuthProfile() {
  try {
    const mePayload = await api("/auth/me");
    state.authProfile = buildAuthProfile(mePayload);
    return true;
  } catch (_err) {
    return false;
  }
}

async function refreshSocialData(options = {}) {
  const { silent = false } = options;
  state.social.loading = true;
  if (!silent) {
    clearError();
  }
  render();

  try {
    const [watchlistPayload, leaderboardPayload] = await Promise.all([
      api(withCurrency("/social/watchlist")),
      api(
        withCurrency(
          `/social/leaderboard?scope=${encodeURIComponent(
            state.social.scope || "global"
          )}&limit=25`
        )
      )
    ]);

    state.social.watchlist = Array.isArray(watchlistPayload?.items)
      ? watchlistPayload.items
      : [];
    state.social.leaderboard = Array.isArray(leaderboardPayload?.items)
      ? leaderboardPayload.items
      : [];
  } catch (err) {
    if (!silent) {
      setError(err.message);
    }
  } finally {
    state.social.loading = false;
    render();
  }
}

async function addWatchlistEntry(e) {
  e.preventDefault();
  const steamId64 = String(state.social.newSteamId || "").trim();
  if (!/^\d{17}$/.test(steamId64)) {
    setError("SteamID64 must be exactly 17 digits.");
    return;
  }

  clearError();
  state.social.loading = true;
  render();

  try {
    await api("/social/watchlist", {
      method: "POST",
      body: JSON.stringify({ steamId64 })
    });
    state.social.newSteamId = "";
    await refreshSocialData({ silent: true });
  } catch (err) {
    setError(err.message);
    state.social.loading = false;
    render();
  }
}

async function removeWatchlistEntry(steamId64) {
  const safeSteamId64 = String(steamId64 || "").trim();
  if (!safeSteamId64) return;

  clearError();
  state.social.loading = true;
  render();

  try {
    await api(`/social/watchlist/${encodeURIComponent(safeSteamId64)}`, {
      method: "DELETE"
    });
    await refreshSocialData({ silent: true });
  } catch (err) {
    setError(err.message);
    state.social.loading = false;
    render();
  }
}

async function toggleWatchFromLeaderboard(steamId64, isWatching) {
  if (isWatching) {
    await removeWatchlistEntry(steamId64);
    return;
  }

  clearError();
  state.social.loading = true;
  render();

  try {
    await api("/social/watchlist", {
      method: "POST",
      body: JSON.stringify({ steamId64: String(steamId64 || "").trim() })
    });
    await refreshSocialData({ silent: true });
  } catch (err) {
    setError(err.message);
    state.social.loading = false;
    render();
  }
}

async function updatePublicPortfolioSettings(e) {
  e.preventDefault();
  clearError();
  const enabled = Boolean(document.querySelector("#public-portfolio-enabled")?.checked);

  try {
    await api("/social/settings", {
      method: "PATCH",
      body: JSON.stringify({ publicPortfolioEnabled: enabled })
    });
    await refreshPortfolio({ silent: true });
    state.accountNotice = enabled
      ? "Public portfolio enabled. Your /u/SteamID page is now visible."
      : "Public portfolio disabled.";
    render();
  } catch (err) {
    setError(err.message);
  }
}

async function updateOwnershipAlertSettings(e) {
  e.preventDefault();
  clearError();
  const enabled = Boolean(document.querySelector("#ownership-alerts-enabled")?.checked);

  try {
    await api("/alerts/ownership-settings", {
      method: "PATCH",
      body: JSON.stringify({ enabled })
    });
    await refreshPortfolio({ silent: true });
    state.accountNotice = enabled
      ? "Ownership-change alerts enabled."
      : "Ownership-change alerts disabled.";
    render();
  } catch (err) {
    setError(err.message);
  }
}

function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function downloadCsv(path, filename) {
  clearError();
  const res = await fetch(`${API_URL}${path}`, {
    credentials: "include",
    headers: withAuthHeaders({})
  });

  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    throw new Error(payload.error || "CSV export failed");
  }

  const blob = await res.blob();
  downloadBlob(blob, filename);
}

async function exportPortfolioCsv() {
  try {
    await downloadCsv(withCurrency("/portfolio/export.csv"), "portfolio-export.csv");
  } catch (err) {
    setError(err.message);
  }
}

async function exportTransactionsCsv() {
  try {
    await downloadCsv("/transactions/export.csv", "transactions-export.csv");
  } catch (err) {
    setError(err.message);
  }
}

async function runPortfolioBacktest(e) {
  e.preventDefault();
  clearError();
  const days = clampInt(state.backtest.days, 7, 1095);
  state.backtest.days = String(days);
  state.backtest.loading = true;
  render();

  try {
    const result = await api(withCurrency(`/portfolio/backtest?days=${days}`));
    state.backtest.result = result;
  } catch (err) {
    setError(err.message);
  } finally {
    state.backtest.loading = false;
    render();
  }
}

async function refreshTeamDashboard(options = {}) {
  const { silent = false } = options;
  if (!silent) {
    clearError();
  }
  state.teamDashboard.loading = true;
  render();

  try {
    const payload = await api(withCurrency("/team/dashboard"));
    state.teamDashboard.payload = payload;
  } catch (err) {
    if (!silent) {
      setError(err.message);
    }
    state.teamDashboard.payload = null;
  } finally {
    state.teamDashboard.loading = false;
    render();
  }
}

async function updatePlanTier(planTier) {
  clearError();
  try {
    await api("/monetization/plan", {
      method: "PATCH",
      body: JSON.stringify({ planTier })
    });
    await refreshPortfolio({ silent: true });
    if (String(planTier || "").toLowerCase() === "team") {
      await refreshTeamDashboard({ silent: true });
    } else {
      state.teamDashboard.loading = false;
      state.teamDashboard.payload = null;
    }
    state.accountNotice = `Plan updated to ${toTitle(planTier)}.`;
    render();
  } catch (err) {
    setError(err.message);
  }
}

async function loadPublicPortfolio(options = {}) {
  const { silent = false } = options;
  const steamId64 = state.publicPage.steamId64;
  if (!steamId64) return false;

  if (!silent) {
    clearError();
  }

  state.publicPage.loading = true;
  state.publicPage.error = "";
  if (!silent) {
    render();
  }

  try {
    const path = withCurrency(
      `/public/u/${encodeURIComponent(steamId64)}?historyDays=30${
        window.location.search ? `&${window.location.search.slice(1)}` : ""
      }`
    );
    const res = await fetch(`${API_URL}${path}`, {
      credentials: "include",
      headers: withAuthHeaders({
        "Content-Type": "application/json"
      })
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(payload.error || "Request failed");
    }
    state.publicPage.payload = payload;
    state.publicPage.error = "";
    return true;
  } catch (err) {
    state.publicPage.payload = null;
    state.publicPage.error = err.message || "Could not load public portfolio.";
    return false;
  } finally {
    state.publicPage.loading = false;
    if (!silent) {
      render();
    }
  }
}

async function findSkin(e) {
  e.preventDefault();
  clearError();
  const id = document.querySelector("#steam-item-id").value;
  await inspectSkinBySteamItemId(id);
}

async function fetchInspectionBundleBySteamItemId(rawId) {
  const id = String(rawId ?? "").trim();
  if (!id) return null;
  if (!/^\d+$/.test(id)) {
    throw new Error("Steam item ID must contain only digits.");
  }

  const skin = await api(withCurrency(`/skins/by-steam-item/${encodeURIComponent(id)}`));
  const holding = (state.portfolio?.items || []).find(
    (item) => Number(item.skinId) === Number(skin.id)
  );

  let marketInsight = null;
  try {
    const [sellSuggestion, liquidity] = await Promise.all([
      api(withCurrency(`/market/items/${skin.id}/sell-suggestion`)),
      api(withCurrency(`/market/items/${skin.id}/liquidity`))
    ]);
    marketInsight = { sellSuggestion, liquidity };
  } catch (_err) {
    marketInsight = null;
  }

  return {
    id,
    skin,
    marketInsight,
    exitDefaults: {
      quantity: String(Math.max(Number(holding?.quantity || 1), 1)),
      targetSellPrice: String(Number(skin?.latestPrice?.price || 0).toFixed(2)),
      result: null
    }
  };
}

async function inspectSkinBySteamItemId(rawId) {
  try {
    const payload = await fetchInspectionBundleBySteamItemId(rawId);
    if (!payload) return;

    state.skin = payload.skin;
    state.marketInsight = payload.marketInsight;
    state.marketTab.skinId = String(payload.skin.id || "");
    state.exitWhatIf = {
      ...state.exitWhatIf,
      ...payload.exitDefaults,
      loading: false
    };
    state.inspectedSteamItemId = payload.id;
    render();
  } catch (err) {
    state.skin = null;
    state.marketInsight = null;
    state.inspectedSteamItemId = "";
    setError(err.message);
  }
}

async function openInspectModalBySteamItemId(rawId) {
  clearError();
  const id = String(rawId ?? "").trim();
  if (!id) return;

  state.inspectModal.open = true;
  state.inspectModal.loading = true;
  state.inspectModal.error = "";
  state.inspectModal.steamItemId = id;
  state.inspectModal.skin = null;
  state.inspectModal.marketInsight = null;
  state.inspectModal.exitWhatIf = createExitWhatIfState();
  render();

  try {
    const payload = await fetchInspectionBundleBySteamItemId(id);
    if (!payload) return;
    state.inspectModal.skin = payload.skin;
    state.inspectModal.marketInsight = payload.marketInsight;
    state.inspectModal.exitWhatIf = {
      ...state.inspectModal.exitWhatIf,
      ...payload.exitDefaults,
      loading: false
    };
  } catch (err) {
    state.inspectModal.error = err.message || "Failed to inspect item.";
  } finally {
    state.inspectModal.loading = false;
    render();
  }
}

async function refreshMarketInventoryValue() {
  const commissionPercent = Number(state.marketTab.commissionPercent);
  if (!Number.isFinite(commissionPercent) || commissionPercent < 0 || commissionPercent >= 100) {
    setError("Market commission must be between 0 and 99.99.");
    return;
  }

  state.marketTab.loading = true;
  render();
  try {
    const query = buildQuery({
      currency: state.currency,
      commissionPercent
    });
    const value = await api(`/market/inventory/value${query}`);
    state.marketTab.inventoryValue = value;
  } catch (err) {
    setError(err.message);
  } finally {
    state.marketTab.autoLoaded = true;
    state.marketTab.loading = false;
    render();
  }
}

async function analyzeMarketItemBySkinId(rawSkinId) {
  clearError();
  const skinId = Number(rawSkinId);
  if (!Number.isInteger(skinId) || skinId <= 0) {
    setError("Select a valid item for market analysis.");
    return;
  }

  const commissionPercent = Number(state.marketTab.commissionPercent);
  if (!Number.isFinite(commissionPercent) || commissionPercent < 0 || commissionPercent >= 100) {
    setError("Market commission must be between 0 and 99.99.");
    return;
  }

  state.marketTab.skinId = String(skinId);
  state.marketTab.insight = null;
  state.marketTab.loading = true;
  render();

  try {
    const query = buildQuery({
      currency: state.currency,
      commissionPercent
    });
    const [sellSuggestion, liquidity] = await Promise.all([
      api(`/market/items/${skinId}/sell-suggestion${query}`),
      api(`/market/items/${skinId}/liquidity`)
    ]);
    state.marketTab.insight = { sellSuggestion, liquidity, skinId };
  } catch (err) {
    setError(err.message);
  } finally {
    state.marketTab.loading = false;
    render();
  }
}

async function submitMarketAnalyze(e) {
  e.preventDefault();
  const skinId = document.querySelector("#market-skin-id")?.value;
  const commissionInput =
    document.querySelector("#market-commission-inline") ||
    document.querySelector("#market-commission");
  state.marketTab.skinId = String(skinId || "");
  state.marketTab.commissionPercent = String(
    commissionInput?.value || state.marketTab.commissionPercent
  );
  await analyzeMarketItemBySkinId(skinId);
}

async function submitMarketInventoryRefresh(e) {
  e.preventDefault();
  state.marketTab.commissionPercent = String(
    document.querySelector("#market-commission")?.value || state.marketTab.commissionPercent
  );
  await refreshMarketInventoryValue();
}

function resetAlertForm() {
  const fallbackSkinId = getHoldingsList()[0]?.skinId;
  state.alertForm = {
    mode: "create",
    alertId: null,
    skinId: fallbackSkinId ? String(fallbackSkinId) : "",
    targetPrice: "",
    percentChangeThreshold: "",
    direction: "both",
    cooldownMinutes: "60",
    enabled: true,
    submitting: false
  };
}

function startEditAlert(rawAlertId) {
  const alertId = Number(rawAlertId);
  const alert = (state.alertsFeed || []).find((row) => Number(row.id) === alertId);
  if (!alert) return;

  state.alertForm.mode = "edit";
  state.alertForm.alertId = alertId;
  state.alertForm.skinId = String(alert.skinId || "");
  state.alertForm.targetPrice =
    alert.targetPrice == null ? "" : String(Number(alert.targetPrice));
  state.alertForm.percentChangeThreshold =
    alert.percentChangeThreshold == null
      ? ""
      : String(Number(alert.percentChangeThreshold));
  state.alertForm.direction = String(alert.direction || "both");
  state.alertForm.cooldownMinutes = String(Number(alert.cooldownMinutes || 60));
  state.alertForm.enabled = Boolean(alert.enabled);
  render();
}

function cancelEditAlert() {
  resetAlertForm();
  render();
}

async function submitAlertForm(e) {
  e.preventDefault();
  clearError();
  if (state.alertForm.submitting) return;

  const skinId = Number(document.querySelector("#alert-skin-id")?.value);
  const targetPriceRaw = document.querySelector("#alert-target-price")?.value ?? "";
  const pctRaw = document.querySelector("#alert-percent-threshold")?.value ?? "";
  const direction = String(document.querySelector("#alert-direction")?.value || "both");
  const cooldownMinutes = Number(document.querySelector("#alert-cooldown")?.value);
  const enabled = Boolean(document.querySelector("#alert-enabled")?.checked);

  state.alertForm.skinId = String(document.querySelector("#alert-skin-id")?.value || "");
  state.alertForm.targetPrice = String(targetPriceRaw);
  state.alertForm.percentChangeThreshold = String(pctRaw);
  state.alertForm.direction = direction;
  state.alertForm.cooldownMinutes = String(
    document.querySelector("#alert-cooldown")?.value || ""
  );
  state.alertForm.enabled = enabled;

  const targetPrice = targetPriceRaw === "" ? null : Number(targetPriceRaw);
  const percentChangeThreshold = pctRaw === "" ? null : Number(pctRaw);

  if (!Number.isInteger(skinId) || skinId <= 0) {
    setError("Pick a valid item for the alert.");
    return;
  }
  if (targetPrice == null && percentChangeThreshold == null) {
    setError("Set at least one trigger: target price or % change.");
    return;
  }
  if (targetPrice != null && (!Number.isFinite(targetPrice) || targetPrice < 0)) {
    setError("Target price must be a number >= 0.");
    return;
  }
  if (
    percentChangeThreshold != null &&
    (!Number.isFinite(percentChangeThreshold) || percentChangeThreshold < 0)
  ) {
    setError("Percent change threshold must be >= 0.");
    return;
  }
  if (!["up", "down", "both"].includes(direction)) {
    setError('Direction must be "up", "down", or "both".');
    return;
  }
  if (!Number.isInteger(cooldownMinutes) || cooldownMinutes < 0) {
    setError("Cooldown must be an integer >= 0.");
    return;
  }

  state.alertForm.submitting = true;
  render();

  try {
    const payload = {
      skinId,
      targetPrice,
      percentChangeThreshold,
      direction,
      cooldownMinutes,
      enabled
    };

    if (state.alertForm.mode === "edit" && state.alertForm.alertId) {
      await api(`/alerts/${Number(state.alertForm.alertId)}`, {
        method: "PATCH",
        body: JSON.stringify(payload)
      });
    } else {
      await api("/alerts", {
        method: "POST",
        body: JSON.stringify(payload)
      });
    }

    resetAlertForm();
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  } finally {
    state.alertForm.submitting = false;
    render();
  }
}

async function removeAlert(rawAlertId) {
  clearError();
  const alertId = Number(rawAlertId);
  if (!Number.isInteger(alertId) || alertId <= 0) return;
  try {
    await api(`/alerts/${alertId}`, { method: "DELETE" });
    if (state.alertForm.mode === "edit" && Number(state.alertForm.alertId) === alertId) {
      resetAlertForm();
    }
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  }
}

async function toggleAlertEnabled(rawAlertId, enabled) {
  clearError();
  const alertId = Number(rawAlertId);
  if (!Number.isInteger(alertId) || alertId <= 0) return;
  try {
    await api(`/alerts/${alertId}`, {
      method: "PATCH",
      body: JSON.stringify({ enabled: Boolean(enabled) })
    });
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  }
}

async function calculateTrade(e) {
  e.preventDefault();
  clearError();

  if (state.tradeCalc.loading) return;

  const buyPrice = Number(document.querySelector("#calc-buy-price")?.value);
  const sellPrice = Number(document.querySelector("#calc-sell-price")?.value);
  const quantity = Number(document.querySelector("#calc-quantity")?.value);
  const commissionPercent = Number(
    document.querySelector("#calc-commission")?.value
  );

  state.tradeCalc.buyPrice = String(
    document.querySelector("#calc-buy-price")?.value ?? state.tradeCalc.buyPrice
  );
  state.tradeCalc.sellPrice = String(
    document.querySelector("#calc-sell-price")?.value ?? state.tradeCalc.sellPrice
  );
  state.tradeCalc.quantity = String(
    document.querySelector("#calc-quantity")?.value ?? state.tradeCalc.quantity
  );
  state.tradeCalc.commissionPercent = String(
    document.querySelector("#calc-commission")?.value ?? state.tradeCalc.commissionPercent
  );

  state.tradeCalc.loading = true;
  render();

  try {
    const result = await api(withCurrency("/trade/calculate"), {
      method: "POST",
      body: JSON.stringify({
        buyPrice,
        sellPrice,
        quantity,
        commissionPercent,
        currency: state.currency
      })
    });

    state.tradeCalc.result = result;
  } catch (err) {
    setError(err.message);
  } finally {
    state.tradeCalc.loading = false;
    render();
  }
}

async function submitTransaction(e) {
  e.preventDefault();
  clearError();
  if (state.txSubmitting) return;

  const skinId = Number(document.querySelector("#tx-skin-id")?.value);
  const type = String(document.querySelector("#tx-type")?.value || "buy").toLowerCase();
  const quantity = Number(document.querySelector("#tx-quantity")?.value);
  const unitPrice = Number(document.querySelector("#tx-unit-price")?.value);
  const commissionPercent = Number(document.querySelector("#tx-commission")?.value);

  state.txForm.skinId = String(document.querySelector("#tx-skin-id")?.value || "");
  state.txForm.type = type;
  state.txForm.quantity = String(document.querySelector("#tx-quantity")?.value || "");
  state.txForm.unitPrice = String(document.querySelector("#tx-unit-price")?.value || "");
  state.txForm.commissionPercent = String(
    document.querySelector("#tx-commission")?.value || ""
  );

  if (!Number.isInteger(skinId) || skinId <= 0) {
    setError("Pick a valid item before saving transaction.");
    return;
  }
  if (!["buy", "sell"].includes(type)) {
    setError('Transaction type must be "buy" or "sell".');
    return;
  }
  if (!Number.isInteger(quantity) || quantity <= 0) {
    setError("Quantity must be a positive integer.");
    return;
  }
  if (!Number.isFinite(unitPrice) || unitPrice < 0) {
    setError("Unit price must be a number >= 0.");
    return;
  }
  if (
    !Number.isFinite(commissionPercent) ||
    commissionPercent < 0 ||
    commissionPercent >= 100
  ) {
    setError("Commission must be between 0 and 99.99.");
    return;
  }

  state.txSubmitting = true;
  render();

  try {
    await api("/transactions", {
      method: "POST",
      body: JSON.stringify({
        skinId,
        type,
        quantity,
        unitPrice,
        commissionPercent,
        currency: "USD"
      })
    });
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  } finally {
    state.txSubmitting = false;
    render();
  }
}

async function removeTransaction(id) {
  clearError();
  try {
    await api(`/transactions/${Number(id)}`, { method: "DELETE" });
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  }
}

async function importTransactionsCsv(e) {
  e.preventDefault();
  clearError();
  const file = document.querySelector("#tx-csv-file")?.files?.[0];
  if (!file) {
    setError("Select a CSV file to import.");
    return;
  }

  if (state.csvImport.running) return;
  state.csvImport.running = true;
  state.csvImport.summary = null;
  render();

  try {
    const text = await file.text();
    const parsed = parseTransactionCsv(text);

    let imported = 0;
    const failed = [];
    for (const entry of parsed) {
      const payload = entry.row;
      try {
        await api("/transactions", {
          method: "POST",
          body: JSON.stringify({
            skinId: payload.skinId,
            type: payload.type,
            quantity: payload.quantity,
            unitPrice: payload.unitPrice,
            commissionPercent: payload.commissionPercent,
            executedAt: payload.executedAt,
            currency: "USD"
          })
        });
        imported += 1;
      } catch (err) {
        failed.push({
          lineNo: entry.lineNo,
          message: err.message
        });
      }
    }

    state.csvImport.summary = {
      total: parsed.length,
      imported,
      failed
    };
    await refreshPortfolio();
  } catch (err) {
    setError(err.message);
  } finally {
    state.csvImport.running = false;
    render();
  }
}

function getInspectContextSnapshot(context = "inline") {
  if (context === "modal") {
    return {
      skin: state.inspectModal.skin,
      exitWhatIf: state.inspectModal.exitWhatIf
    };
  }
  return {
    skin: state.skin,
    exitWhatIf: state.exitWhatIf
  };
}

async function calculateExitWhatIf(e, context = "inline") {
  e.preventDefault();
  clearError();
  const inspectContext = getInspectContextSnapshot(context);
  if (inspectContext.exitWhatIf.loading || !inspectContext.skin) return;

  const form = e.target instanceof HTMLFormElement ? e.target : null;
  const quantityRaw = String(
    form?.querySelector('[data-exit-field="quantity"]')?.value || ""
  );
  const targetPriceRaw = String(
    form?.querySelector('[data-exit-field="target-price"]')?.value || ""
  );
  const commissionRaw = String(
    form?.querySelector('[data-exit-field="commission"]')?.value || ""
  );

  const quantity = Number(quantityRaw);
  const targetSellPrice = Number(targetPriceRaw);
  const commissionPercent = Number(commissionRaw);

  inspectContext.exitWhatIf.quantity = quantityRaw;
  inspectContext.exitWhatIf.targetSellPrice = targetPriceRaw;
  inspectContext.exitWhatIf.commissionPercent = commissionRaw;

  if (!Number.isFinite(quantity) || quantity <= 0) {
    setError("Exit quantity must be > 0.");
    return;
  }
  if (!Number.isFinite(targetSellPrice) || targetSellPrice < 0) {
    setError("Target sell price must be >= 0.");
    return;
  }
  if (
    !Number.isFinite(commissionPercent) ||
    commissionPercent < 0 ||
    commissionPercent >= 100
  ) {
    setError("Commission must be between 0 and 99.99.");
    return;
  }

  const itemStats = computeItemTradeStats(Number(inspectContext.skin.id));
  const buyPrice =
    itemStats.avgEntryPrice || Number(inspectContext.skin.latestPrice?.price || 0);

  inspectContext.exitWhatIf.loading = true;
  render();
  try {
    const result = await api(withCurrency("/trade/calculate"), {
      method: "POST",
      body: JSON.stringify({
        buyPrice,
        sellPrice: targetSellPrice,
        quantity,
        commissionPercent,
        currency: state.currency
      })
    });
    inspectContext.exitWhatIf.result = {
      ...result,
      referenceBuyPrice: buyPrice
    };
  } catch (err) {
    setError(err.message);
  } finally {
    inspectContext.exitWhatIf.loading = false;
    render();
  }
}

function renderPortfolioCardGrid() {
  if (state.portfolioLoading) {
    return `
      <div class="portfolio-cards-grid">
        ${Array.from({ length: 8 }, (_, idx) => renderSkinCardSkeleton(idx)).join("")}
      </div>
    `;
  }

  const { items } = getFilteredHoldings();
  if (!items.length) {
    return `<p class="empty-state">No holdings yet. Sync inventory to populate your portfolio cards.</p>`;
  }

  return `
    <div class="portfolio-cards-grid">
      ${items
        .map((item) =>
          renderSkinCard(item, {
            escapeHtml,
            formatMoney
          })
        )
        .join("")}
    </div>
  `;
}

function formatMarketSourceLabel(source) {
  const map = {
    steam: "Steam",
    skinport: "Skinport",
    csfloat: "CSFloat",
    dmarket: "DMarket"
  };
  return map[String(source || "").toLowerCase()] || toTitle(source || "Market");
}

function renderPortfolioPricingControls() {
  const pricing = state.portfolio?.pricing || {};
  const mode = normalizePricingMode(pricing.mode || state.pricingMode);
  const fees = pricing.fees || {};
  const feeText = [
    `Steam ${formatNumber(fees.steam, 2)}%`,
    `Skinport ${formatNumber(fees.skinport, 2)}%`,
    `CSFloat ${formatNumber(fees.csfloat, 2)}%`,
    `DMarket ${formatNumber(fees.dmarket, 2)}%`
  ].join(" | ");

  return `
    <div class="portfolio-pricing-toolbar">
      <label>Pricing Mode
        <select id="pricing-mode-select">
          <option value="lowest_buy" ${mode === "lowest_buy" ? "selected" : ""}>Lowest Buy</option>
          <option value="steam" ${mode === "steam" ? "selected" : ""}>Steam Price</option>
          <option value="best_sell_net" ${mode === "best_sell_net" ? "selected" : ""}>Best Sell Net</option>
        </select>
      </label>
      <button
        type="button"
        id="refresh-market-compare-btn"
        class="ghost-btn"
        ${state.compareRefreshing ? "disabled" : ""}
      >
        ${state.compareRefreshing ? "Refreshing..." : "Refresh Market Prices"}
      </button>
    </div>
    <p class="helper-text market-fee-note" title="Net sell value subtracts marketplace fees before comparing venues.">
      Portfolio valuation mode: <strong>${escapeHtml(getPricingModeLabel(mode))}</strong>. Fee model: ${escapeHtml(
    feeText
  )}.
    </p>
  `;
}

function renderMarketComparisonPanel(item) {
  const comparison = item?.marketComparison || null;
  if (!comparison || !Array.isArray(comparison.perMarket)) {
    return `
      <div class="market-compare-panel">
        <p class="muted">Market comparison data is unavailable for this item. Use "Refresh Market Prices".</p>
      </div>
    `;
  }

  const bestBuyMarkup = comparison.bestBuy
    ? `${formatMarketSourceLabel(comparison.bestBuy.source)} ${formatMoney(
        comparison.bestBuy.grossPrice,
        item.currency || state.currency
      )}`
    : "N/A";
  const bestSellMarkup = comparison.bestSellNet
    ? `${formatMarketSourceLabel(comparison.bestSellNet.source)} ${formatMoney(
        comparison.bestSellNet.netPriceAfterFees,
        item.currency || state.currency
      )}`
    : "N/A";

  const perMarketMarkup = comparison.perMarket
    .map((row) => {
      const isAvailable = Boolean(row?.available);
      const unavailableReason = String(row?.unavailableReason || "").trim();
      const listingLink = row?.url
        ? `<a class="link-btn ghost market-open-link" href="${escapeHtml(
            row.url
          )}" target="_blank" rel="noreferrer">Open listing</a>`
        : '<span class="muted">No listing</span>';

      return `
        <div class="market-price-card ${isAvailable ? "" : "unavailable"}">
          <div class="market-price-head">
            <strong>${escapeHtml(formatMarketSourceLabel(row.source))}</strong>
            <small>${escapeHtml(
              isAvailable ? formatRelativeTime(row.updatedAt) : "No data"
            )}</small>
          </div>
          ${
            !isAvailable && unavailableReason
              ? `<p class="muted">${escapeHtml(unavailableReason)}</p>`
              : ""
          }
          <p>Gross: <strong>${
            isAvailable ? formatMoney(row.grossPrice, row.currency || state.currency) : "-"
          }</strong></p>
          <p>Net: <strong>${
            isAvailable
              ? formatMoney(row.netPriceAfterFees, row.currency || state.currency)
              : "-"
          }</strong></p>
          ${listingLink}
        </div>
      `;
    })
    .join("");

  return `
    <div class="market-compare-panel">
      <p class="market-compare-summary">
        Best Buy: <strong>${bestBuyMarkup}</strong>
        <span class="muted">|</span>
        Best Sell Net: <strong>${bestSellMarkup}</strong>
      </p>
      <div class="market-price-grid">
        ${perMarketMarkup}
      </div>
    </div>
  `;
}

function renderPortfolioRows() {
  if (state.portfolioLoading) {
    return Array.from({ length: 6 }, (_, idx) => idx)
      .map(
        (idx) => `
          <tr class="holding-row is-skeleton" data-skeleton-index="${idx}">
            <td colspan="10"><div class="table-row-skeleton"></div></td>
          </tr>
        `
      )
      .join("");
  }

  const { items } = getFilteredHoldings();
  if (!items.length) {
    return `<tr><td colspan="10" class="muted empty-table-cell">No holdings yet. Link Steam and run sync.</td></tr>`;
  }

  const formatSteamItemIdCell = (item) => {
    const ids = Array.isArray(item.steamItemIds) ? item.steamItemIds : [];
    if (!ids.length) return "-";
    if (ids.length === 1) return escapeHtml(ids[0]);
    return `${escapeHtml(ids[0])} +${ids.length - 1} more`;
  };

  return items
    .map((item) => {
      const skinId = Number(item.skinId || 0);
      const lineValue = Number(item.lineValue || 0);
      const prevLineValue = holdingsValueMemory.get(skinId);
      const flashClass =
        Number.isFinite(prevLineValue) && Math.abs(prevLineValue - lineValue) >= 0.01
          ? lineValue >= prevLineValue
            ? "flash-up"
            : "flash-down"
          : "";
      holdingsValueMemory.set(skinId, lineValue);

      const oneDayClass = Number(item.oneDayChangePercent || 0) >= 0 ? "up" : "down";
      const sevenDayClass = Number(item.sevenDayChangePercent || 0) >= 0 ? "up" : "down";
      const rarityTheme = getItemRarityTheme(item);
      const itemImageUrl = getItemImageUrl(item);
      const fallbackImage = isCaseLikeItem(item) ? defaultCaseImage : defaultSkinImage;
      const isExpanded = Boolean(state.compareExpandedBySkinId[skinId]);

      return `
        <tr class="holding-row">
          <td class="mono-cell">${formatSteamItemIdCell(item)}</td>
          <td>
            <div class="table-item-cell">
              <img
                class="table-item-thumb"
                style="--rarity-color: ${rarityTheme.color};"
                src="${escapeHtml(itemImageUrl)}"
                alt="${escapeHtml(item.marketHashName || "CS2 item")}"
                loading="lazy"
                onerror="this.onerror=null;this.src='${escapeHtml(fallbackImage)}';"
              />
              <div class="table-item-info">
                <div class="skin-name">${escapeHtml(item.marketHashName)}</div>
                <div class="table-item-subline">
                  <span class="rarity-tag" style="--rarity-color: ${rarityTheme.color};">
                    ${escapeHtml(rarityTheme.rarity)}
                  </span>
                  <small class="muted">${escapeHtml(formatConfidence(item))}</small>
                </div>
              </div>
            </div>
          </td>
          <td>${item.quantity}</td>
          <td><span class="price-cell">${formatMoney(item.currentPrice)}</span></td>
          <td><span class="pnl-chip ${oneDayClass}">${formatPercent(item.oneDayChangePercent)}</span></td>
          <td><span class="pnl-chip ${sevenDayClass}">${formatPercent(item.sevenDayChangePercent)}</span></td>
          <td title="Source: ${escapeHtml(item.selectedPricingSource || item.currentPriceSource || "-")}">${formatPriceStatusBadge(item.priceStatus)}</td>
          <td>${formatManagementClue(item.managementClue)}</td>
          <td>
            <strong class="line-value ${flashClass}">${formatMoney(item.lineValue)}</strong>
          </td>
          <td>
            <div class="row">
              <button
                type="button"
                class="ghost-btn inspect-skin-btn"
                data-steam-item-id="${escapeHtml(item.primarySteamItemId || "")}"
                ${item.primarySteamItemId ? "" : "disabled"}
              >
                Inspect
              </button>
              <button
                type="button"
                class="ghost-btn compare-market-btn"
                data-skin-id="${skinId}"
              >
                ${isExpanded ? "Hide Compare" : "Compare"}
              </button>
            </div>
          </td>
        </tr>
        ${
          isExpanded
            ? `
          <tr class="holding-row-details">
            <td colspan="10">
              ${renderMarketComparisonPanel(item)}
            </td>
          </tr>
        `
            : ""
        }
      `;
    })
    .join("");
}

function renderHistoryChart() {
  const points = (Array.isArray(state.history) ? state.history : [])
    .map((point) => ({
      date: String(point.date || ""),
      value: Number(point.totalValue || 0)
    }))
    .filter((point) => point.date && Number.isFinite(point.value));

  if (!points.length) {
    return `<p class="empty-state">No history yet. Sync inventory to create data points.</p>`;
  }

  const values = points.map((point) => point.value);
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const first = Number(values[0] || 0);
  const last = Number(values[values.length - 1] || 0);
  const change = last - first;
  const trendClass = change >= 0 ? "up" : "down";
  const ranges = [7, 30, 90, 180];
  const currencyCode = state.portfolio?.currency || state.currency;

  const viewWidth = 920;
  const viewHeight = 280;
  const padX = 52;
  const padY = 24;
  const plotWidth = viewWidth - padX * 2;
  const plotHeight = viewHeight - padY * 2;
  const valueRange = Math.max(max - min, 0.01);

  const coords = points.map((point, index) => {
    const x =
      points.length === 1
        ? padX + plotWidth / 2
        : padX + (index / (points.length - 1)) * plotWidth;
    const y = padY + ((max - point.value) / valueRange) * plotHeight;
    return { x, y };
  });

  const linePoints = coords
    .map((coord) => `${coord.x.toFixed(2)},${coord.y.toFixed(2)}`)
    .join(" ");
  const areaPath = `M ${coords[0].x.toFixed(2)},${(viewHeight - padY).toFixed(2)} L ${coords
    .map((coord) => `${coord.x.toFixed(2)},${coord.y.toFixed(2)}`)
    .join(" L ")} L ${coords[coords.length - 1].x.toFixed(2)},${(viewHeight - padY).toFixed(
    2
  )} Z`;

  const yTicks = [max, max - valueRange / 2, min].map((value) => ({
    value,
    y: padY + ((max - value) / valueRange) * plotHeight
  }));

  const midIndex = Math.floor(coords.length / 2);
  const xTicks = [
    { x: coords[0].x, label: points[0].date },
    { x: coords[midIndex].x, label: points[midIndex].date },
    { x: coords[coords.length - 1].x, label: points[points.length - 1].date }
  ];
  const uniqueXTicks = xTicks.filter(
    (tick, index, list) =>
      list.findIndex((candidate) => candidate.label === tick.label) === index
  );

  return `
    <div class="history-toolbar">
      <div class="history-range">
        ${ranges
          .map(
            (d) => `
            <button
              type="button"
              class="ghost-btn history-range-btn ${state.historyDays === d ? "active" : ""}"
              data-history-days="${d}"
            >
              ${d === 180 ? "6M" : `${d}D`}
            </button>
          `
          )
          .join("")}
      </div>
      <div class="history-summary ${trendClass}">
        <span>Range change</span>
        <strong>${formatSignedMoney(change, currencyCode)} (${formatPercent(
    first > 0 ? (change / first) * 100 : null
  )})</strong>
      </div>
      <div class="history-summary">
        <span>Min/Max</span>
        <strong>${formatMoney(min, currencyCode)} - ${formatMoney(max, currencyCode)}</strong>
      </div>
    </div>
    <div class="performance-chart-shell">
      <svg class="value-chart performance-chart" viewBox="0 0 ${viewWidth} ${viewHeight}" role="img" aria-label="Portfolio value performance chart">
        ${yTicks
          .map(
            (tick) => `
              <line x1="${padX}" y1="${tick.y.toFixed(2)}" x2="${viewWidth - padX}" y2="${tick.y.toFixed(
                2
              )}" class="value-chart-grid" />
              <text x="${(padX - 8).toFixed(2)}" y="${(tick.y + 4).toFixed(2)}" text-anchor="end" class="value-chart-label">${escapeHtml(
                formatMoney(tick.value, currencyCode)
              )}</text>
            `
          )
          .join("")}
        <line x1="${padX}" y1="${padY}" x2="${padX}" y2="${viewHeight - padY}" class="value-chart-axis" />
        <line x1="${padX}" y1="${viewHeight - padY}" x2="${viewWidth - padX}" y2="${viewHeight - padY}" class="value-chart-axis" />
        <path d="${areaPath}" class="value-chart-area" />
        <polyline class="value-chart-line" points="${linePoints}" />
        <circle cx="${coords[coords.length - 1].x.toFixed(2)}" cy="${coords[
    coords.length - 1
  ].y.toFixed(2)}" r="4" class="value-chart-dot" />
        ${uniqueXTicks
          .map(
            (tick) => `
              <line x1="${tick.x.toFixed(2)}" y1="${(viewHeight - padY).toFixed(
                2
              )}" x2="${tick.x.toFixed(2)}" y2="${(viewHeight - padY + 4).toFixed(
                2
              )}" class="value-chart-axis" />
              <text x="${tick.x.toFixed(2)}" y="${(viewHeight - padY + 18).toFixed(2)}" text-anchor="middle" class="value-chart-label">${escapeHtml(
                tick.label
              )}</text>
            `
          )
          .join("")}
      </svg>
      <div class="value-chart-meta">
        <span>Points: <strong>${points.length}</strong></span>
        <span>Latest: <strong>${formatMoney(last, currencyCode)}</strong></span>
        <span>Peak: <strong>${formatMoney(max, currencyCode)}</strong></span>
        <span>Floor: <strong>${formatMoney(min, currencyCode)}</strong></span>
        <span class="${trendClass}">Momentum: <strong>${formatPercent(
    first > 0 ? (change / first) * 100 : null
  )}</strong></span>
      </div>
    </div>
  `;
}

function renderDashboardHero() {
  const portfolio = state.portfolio || {};
  const pricingModeLabel = getPricingModeLabel(portfolio?.pricing?.mode || state.pricingMode);
  const currencyCode = portfolio.currency || state.currency;
  const totalValue = Number(portfolio.totalValue || 0);
  const sevenDay = Number(portfolio.sevenDayChangePercent || 0);
  const oneDay = Number(portfolio.oneDayChangePercent || 0);
  const roi = Number(portfolio.roiPercent || 0);
  const sevenDayClass = sevenDay >= 0 ? "up" : "down";
  const oneDayClass = oneDay >= 0 ? "up" : "down";
  const signal = buildPortfolioSignals();

  return `
    <section class="grid">
      <article class="panel wide hero-panel">
        <div class="hero-shell">
          <div>
            <p class="eyebrow">CS2 Trading Command</p>
            <h1>Portfolio Performance Center</h1>
            <p class="hero-copy">
              High-signal view of value, momentum, liquidity quality, and concentration risk.
              Built to keep decision latency low during volatile market windows.
            </p>
            <div class="hero-actions">
              <button type="button" class="ghost-btn tab-jump-btn" data-tab-target="portfolio">Open Holdings</button>
              <button type="button" class="ghost-btn tab-jump-btn" data-tab-target="alerts">Open Alerts</button>
              <button type="button" class="ghost-btn tab-jump-btn" data-tab-target="trades">Open Transactions</button>
            </div>
          </div>
          <div class="hero-metric-stack">
            <article class="hero-metric metric-glow">
              <span>${escapeHtml(pricingModeLabel)} Portfolio Value</span>
              <strong
                class="metric-number"
                data-count-key="hero-total-value"
                data-count-format="money"
                data-count-currency="${escapeHtml(currencyCode)}"
                data-count-to="${totalValue}"
              >
                ${formatMoney(totalValue, currencyCode)}
              </strong>
            </article>
            <article class="hero-metric hero-performance ${sevenDayClass}">
              <span>7D Performance</span>
              <strong
                class="metric-number"
                data-count-key="hero-seven-day"
                data-count-format="percent"
                data-count-to="${sevenDay}"
              >
                ${formatPercent(sevenDay)}
              </strong>
              <small>
                24H:
                <span class="pnl-text ${oneDayClass}">
                  ${formatPercent(oneDay)}
                </span>
                | ROI:
                <span class="pnl-text ${roi >= 0 ? "up" : "down"}">${formatPercent(roi)}</span>
              </small>
            </article>
            <div class="hero-stat-grid">
              <article class="hero-stat">
                <span>Liquidity</span>
                <strong
                  class="metric-number"
                  data-count-key="hero-liquidity-score"
                  data-count-format="integer"
                  data-count-to="${Number(signal.liquidityScore || 0)}"
                >
                  ${signal.liquidityScore == null ? "-" : signal.liquidityScore}
                </strong>
                <small>/100</small>
              </article>
              <article class="hero-stat">
                <span>Risk</span>
                <strong
                  class="metric-number"
                  data-count-key="hero-risk-score"
                  data-count-format="integer"
                  data-count-to="${Number(signal.riskScore || 0)}"
                >
                  ${signal.riskScore}
                </strong>
                <small>/100</small>
              </article>
              <article class="hero-stat">
                <span>Unpriced</span>
                <strong>${signal.unpricedItems}</strong>
              </article>
              <article class="hero-stat">
                <span>Stale</span>
                <strong>${signal.staleItems}</strong>
              </article>
            </div>
          </div>
        </div>
      </article>
    </section>
  `;
}

function renderPnlSummary() {
  const portfolio = state.portfolio;
  if (!portfolio) return "";

  const currency = portfolio.currency || state.currency;
  const unrealized = Number(portfolio.unrealizedProfit || 0);
  const realized = Number(portfolio.realizedProfit || 0);

  const unrealizedClass = unrealized >= 0 ? "up" : "down";
  const realizedClass = realized >= 0 ? "up" : "down";

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>P/L Summary</h2>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card">
            <span>Cost Basis</span>
            <strong>${formatMoney(portfolio.costBasis, currency)}</strong>
          </article>
          <article class="sub-kpi-card ${unrealizedClass}">
            <span>Unrealized P/L</span>
            <strong>${formatMoney(portfolio.unrealizedProfit, currency)}</strong>
          </article>
          <article class="sub-kpi-card ${realizedClass}">
            <span>Realized P/L</span>
            <strong>${formatMoney(portfolio.realizedProfit, currency)}</strong>
          </article>
        </div>
      </article>
    </section>
  `;
}

function renderTradeCalculator() {
  const result = state.tradeCalc.result;
  const calcCurrency = result?.currency || state.currency;
  const resultMarkup = result
    ? `
      <div class="calc-result">
        <p><span>Net Profit</span><strong>${formatMoney(result.netProfit, calcCurrency)}</strong></p>
        <p><span>ROI</span><strong>${formatPercent(result.roiPercent)}</strong></p>
        <p><span>Break-even Sell Price</span><strong>${formatMoney(result.breakEvenSellPrice, calcCurrency)}</strong></p>
        <p><span>Commission</span><strong>${formatMoney(result.commissionAmount, calcCurrency)}</strong></p>
        <p><span>Net Sell</span><strong>${formatMoney(result.netSell, calcCurrency)}</strong></p>
      </div>
    `
    : '<p class="muted">Enter buy/sell values and calculate ROI.</p>';

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Trade ROI Calculator</h2>
        <p class="helper-text">Use buy/sell price, quantity, and commission to estimate ROI and net profit.</p>
        <form id="trade-calc-form" class="trade-calc-grid">
          <label>Buy Price
            <input id="calc-buy-price" type="number" step="0.01" min="0" value="${escapeHtml(
              state.tradeCalc.buyPrice
            )}" />
          </label>
          <label>Sell Price
            <input id="calc-sell-price" type="number" step="0.01" min="0" value="${escapeHtml(
              state.tradeCalc.sellPrice
            )}" />
          </label>
          <label>Quantity
            <input id="calc-quantity" type="number" step="1" min="1" value="${escapeHtml(
              state.tradeCalc.quantity
            )}" />
          </label>
          <label>Commission %
            <input id="calc-commission" type="number" step="0.01" min="0" max="99.99" value="${escapeHtml(
              state.tradeCalc.commissionPercent
            )}" />
          </label>
          <button type="submit" ${state.tradeCalc.loading ? "disabled" : ""}>
            ${state.tradeCalc.loading ? "Calculating..." : "Calculate ROI"}
          </button>
        </form>
        ${resultMarkup}
      </article>
    </section>
  `;
}

function renderTransactionManager() {
  const holdings = getHoldingsList();
  const selectedSkinId = state.txForm.skinId || String(holdings[0]?.skinId || "");
  const itemNameBySkinId = Object.fromEntries(
    holdings.map((item) => [Number(item.skinId), item.marketHashName])
  );

  const itemOptions = buildHoldingOptions(selectedSkinId);

  const txPaginated = getFilteredTransactions();
  const rows = txPaginated.items;
  const rowsMarkup = rows.length
    ? rows
        .map((tx) => {
          const txId = Number(tx.id);
          const type = String(tx.type || "buy").toLowerCase();
          const skinId = Number(tx.skin_id || 0);
          const marketHashName = itemNameBySkinId[skinId] || `Skin #${skinId}`;
          return `
            <tr>
              <td>${escapeHtml(String(tx.executed_at || "").slice(0, 10))}</td>
              <td><span class="status-badge ${escapeHtml(type)}">${escapeHtml(toTitle(type))}</span></td>
              <td>${escapeHtml(marketHashName)}</td>
              <td>${Number(tx.quantity || 0)}</td>
              <td>${formatMoney(tx.unit_price, "USD")} USD</td>
              <td>${Number(tx.commission_percent || 0).toFixed(2)}%</td>
              <td>${tx.net_total == null ? "-" : `${formatMoney(tx.net_total, "USD")} USD`}</td>
              <td>
                <button type="button" class="ghost-btn tx-delete-btn" data-tx-id="${txId}">Delete</button>
              </td>
            </tr>
          `;
        })
        .join("")
    : '<tr><td colspan="8" class="muted empty-table-cell">No buy/sell transactions yet.</td></tr>';

  const csvSummary = state.csvImport.summary;
  const csvSummaryMarkup = csvSummary
    ? `
      <div class="sync-summary">
        <p><strong>CSV Import:</strong> ${csvSummary.imported}/${csvSummary.total} imported</p>
        ${
          csvSummary.failed.length
            ? `<p class="muted">Failed rows: ${csvSummary.failed
                .slice(0, 5)
                .map((f) => `line ${f.lineNo} (${escapeHtml(f.message)})`)
                .join(", ")}</p>`
            : "<p class=\"muted\">No failed rows.</p>"
        }
      </div>
    `
    : "";

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Buy/Sell Transactions</h2>
        <p class="helper-text">
          Save trades here to drive <strong>Cost Basis</strong>, <strong>Realized/Unrealized P/L</strong>, and portfolio ROI.
          Values are stored in <strong>USD</strong>.
        </p>
        <form id="tx-form" class="trade-calc-grid">
          <label>Item
            <select id="tx-skin-id" ${holdings.length ? "" : "disabled"}>${itemOptions}</select>
          </label>
          <label>Type
            <select id="tx-type">
              <option value="buy" ${state.txForm.type === "buy" ? "selected" : ""}>Buy</option>
              <option value="sell" ${state.txForm.type === "sell" ? "selected" : ""}>Sell</option>
            </select>
          </label>
          <label>Quantity
            <input id="tx-quantity" type="number" step="1" min="1" value="${escapeHtml(
              state.txForm.quantity
            )}" />
          </label>
          <label>Unit Price (USD)
            <input id="tx-unit-price" type="number" step="0.01" min="0" value="${escapeHtml(
              state.txForm.unitPrice
            )}" />
          </label>
          <label>Commission %
            <input id="tx-commission" type="number" step="0.01" min="0" max="99.99" value="${escapeHtml(
              state.txForm.commissionPercent
            )}" />
          </label>
          <button type="submit" ${state.txSubmitting || !holdings.length ? "disabled" : ""}>
            ${state.txSubmitting ? "Saving..." : "Save Transaction"}
          </button>
        </form>
        <form id="tx-csv-form" class="trade-calc-grid">
          <label>Bulk CSV
            <input id="tx-csv-file" type="file" accept=".csv,text/csv" />
          </label>
          <div class="helper-text">
            Header: <code>skinId,type,quantity,unitPrice[,commissionPercent,executedAt]</code>
          </div>
          <button type="submit" ${state.csvImport.running ? "disabled" : ""}>
            ${state.csvImport.running ? "Importing..." : "Import CSV"}
          </button>
        </form>
        ${csvSummaryMarkup}
        <div class="list-toolbar">
          <label>Search
            <input
              id="tx-search"
              placeholder="item name or skin id"
              value="${escapeHtml(state.transactionsView.q)}"
            />
          </label>
          <label>Type
            <select id="tx-filter-type">
              <option value="all" ${state.transactionsView.type === "all" ? "selected" : ""}>All</option>
              <option value="buy" ${state.transactionsView.type === "buy" ? "selected" : ""}>Buy</option>
              <option value="sell" ${state.transactionsView.type === "sell" ? "selected" : ""}>Sell</option>
            </select>
          </label>
          <label>Sort
            <select id="tx-sort">
              <option value="date_desc" ${state.transactionsView.sort === "date_desc" ? "selected" : ""}>Newest first</option>
              <option value="date_asc" ${state.transactionsView.sort === "date_asc" ? "selected" : ""}>Oldest first</option>
              <option value="net_desc" ${state.transactionsView.sort === "net_desc" ? "selected" : ""}>Net high to low</option>
              <option value="net_asc" ${state.transactionsView.sort === "net_asc" ? "selected" : ""}>Net low to high</option>
            </select>
          </label>
          <label>Per page
            <select id="tx-page-size">
              ${[10, 20, 50]
                .map(
                  (n) =>
                    `<option value="${n}" ${
                      state.transactionsView.pageSize === n ? "selected" : ""
                    }>${n}</option>`
                )
                .join("")}
            </select>
          </label>
        </div>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Type</th>
              <th>Item</th>
              <th>Qty</th>
              <th>Unit Price</th>
              <th>Commission</th>
              <th>Net Total</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>${rowsMarkup}</tbody>
        </table>
        <div class="pagination-bar">
          <button
            type="button"
            class="ghost-btn tx-page-btn"
            data-page="${Math.max(txPaginated.page - 1, 1)}"
            ${txPaginated.page <= 1 ? "disabled" : ""}
          >
            Prev
          </button>
          <span>Page ${txPaginated.page} / ${txPaginated.pages} (${txPaginated.total} total)</span>
          <button
            type="button"
            class="ghost-btn tx-page-btn"
            data-page="${Math.min(txPaginated.page + 1, txPaginated.pages)}"
            ${txPaginated.page >= txPaginated.pages ? "disabled" : ""}
          >
            Next
          </button>
        </div>
      </article>
    </section>
  `;
}

function renderSkinValueGraph(historyRows) {
  const points = (Array.isArray(historyRows) ? historyRows : [])
    .map((row) => ({
      date: String(row.recorded_at || "").slice(0, 10),
      value: Number(row.price || 0)
    }))
    .filter((row) => row.date && Number.isFinite(row.value))
    .sort((a, b) => a.date.localeCompare(b.date));

  if (!points.length) {
    return `<p class="empty-state">No historical values to graph yet.</p>`;
  }

  const values = points.map((p) => p.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 0.01);
  const first = points[0];
  const latest = points[points.length - 1];
  const middle = points[Math.floor(points.length / 2)];

  const minIndex = values.indexOf(min);
  const maxIndex = values.indexOf(max);
  const minPoint = points[minIndex];
  const maxPoint = points[maxIndex];

  const sixMonthChange = latest.value - first.value;
  const sixMonthChangePercent =
    first.value > 0 ? (sixMonthChange / first.value) * 100 : null;
  const trendClass = sixMonthChange >= 0 ? "up" : "down";

  const viewWidth = 640;
  const viewHeight = 220;
  const padX = 42;
  const padY = 22;
  const plotWidth = viewWidth - padX * 2;
  const plotHeight = viewHeight - padY * 2;
  const baseY = viewHeight - padY;

  const coords = points.map((point, idx) => {
    const x =
      points.length === 1
        ? padX + plotWidth / 2
        : padX + (idx / (points.length - 1)) * plotWidth;
    const y = padY + ((max - point.value) / range) * plotHeight;
    return { x, y };
  });

  const polylinePoints = coords
    .map((coord) => `${coord.x.toFixed(2)},${coord.y.toFixed(2)}`)
    .join(" ");

  const areaPath = `M ${coords[0].x.toFixed(2)},${baseY.toFixed(2)} L ${coords
    .map((coord) => `${coord.x.toFixed(2)},${coord.y.toFixed(2)}`)
    .join(" L ")} L ${coords[coords.length - 1].x.toFixed(2)},${baseY.toFixed(2)} Z`;

  const yTicks = [max, max - range / 2, min].map((value) => ({
    label: formatMoney(value),
    y: padY + ((max - value) / range) * plotHeight
  }));

  const xTickRows = [
    { label: first.date, x: coords[0].x },
    { label: middle.date, x: coords[Math.floor(coords.length / 2)].x },
    { label: latest.date, x: coords[coords.length - 1].x }
  ];

  const latestCoord = coords[coords.length - 1];

  return `
    <div class="value-chart-block">
      <div class="value-insights">
        <div class="value-insight">
          <span>Current</span>
          <strong>${formatMoney(latest.value)}</strong>
        </div>
        <div class="value-insight ${trendClass}">
          <span>6M Change</span>
          <strong>${formatMoney(sixMonthChange)} (${formatPercent(sixMonthChangePercent)})</strong>
        </div>
        <div class="value-insight">
          <span>6M Low</span>
          <strong>${formatMoney(min)}</strong>
          <small>${escapeHtml(minPoint.date)}</small>
        </div>
        <div class="value-insight">
          <span>6M High</span>
          <strong>${formatMoney(max)}</strong>
          <small>${escapeHtml(maxPoint.date)}</small>
        </div>
      </div>
      <svg class="value-chart" viewBox="0 0 ${viewWidth} ${viewHeight}" role="img" aria-label="Item price value over last 6 months">
        ${yTicks
          .map(
            (tick) => `
              <line x1="${padX}" y1="${tick.y.toFixed(2)}" x2="${viewWidth - padX}" y2="${tick.y.toFixed(2)}" class="value-chart-grid" />
              <text x="${(padX - 6).toFixed(2)}" y="${(tick.y + 4).toFixed(2)}" text-anchor="end" class="value-chart-label">${escapeHtml(
                tick.label
              )}</text>
            `
          )
          .join("")}
        <line x1="${padX}" y1="${padY}" x2="${padX}" y2="${viewHeight - padY}" class="value-chart-axis" />
        <line x1="${padX}" y1="${viewHeight - padY}" x2="${viewWidth - padX}" y2="${viewHeight - padY}" class="value-chart-axis" />
        <path d="${areaPath}" class="value-chart-area" />
        <polyline class="value-chart-line" points="${polylinePoints}" />
        <circle cx="${latestCoord.x.toFixed(2)}" cy="${latestCoord.y.toFixed(2)}" r="3.5" class="value-chart-dot" />
        <text x="${Math.min(latestCoord.x + 8, viewWidth - padX - 30).toFixed(2)}" y="${Math.max(latestCoord.y - 8, padY + 10).toFixed(2)}" class="value-chart-point-label">${escapeHtml(
          formatMoney(latest.value)
        )}</text>
        ${xTickRows
          .map(
            (tick) => `
              <line x1="${tick.x.toFixed(2)}" y1="${baseY.toFixed(2)}" x2="${tick.x.toFixed(2)}" y2="${(baseY + 4).toFixed(2)}" class="value-chart-axis" />
              <text x="${tick.x.toFixed(2)}" y="${(baseY + 16).toFixed(2)}" text-anchor="middle" class="value-chart-label">${escapeHtml(
                tick.label
              )}</text>
            `
          )
          .join("")}
      </svg>
      <div class="value-chart-meta">
        <span>${escapeHtml(first.date)}</span>
        <span>Points: <strong>${points.length}</strong></span>
        <span>Min: <strong>${formatMoney(min)}</strong></span>
        <span>Max: <strong>${formatMoney(max)}</strong></span>
        <span>${escapeHtml(latest.date)}</span>
      </div>
    </div>
  `;
}

function renderSkinDetails(context = "inline") {
  const isModal = context === "modal";
  const inspectSkin = isModal ? state.inspectModal.skin : state.skin;
  const inspectMarketInsight = isModal ? state.inspectModal.marketInsight : state.marketInsight;
  const inspectExitWhatIf = isModal ? state.inspectModal.exitWhatIf : state.exitWhatIf;

  if (!inspectSkin) {
    return isModal
      ? `<p class="muted">Select an item to inspect.</p>`
      : `<p class="muted">Search an item by Steam item ID to inspect current and historical pricing.</p>`;
  }

  const skinId = Number(inspectSkin.id || 0);
  const latest = inspectSkin.latestPrice;
  const history = Array.isArray(inspectSkin.priceHistory) ? inspectSkin.priceHistory : [];
  const tradeStats = computeItemTradeStats(skinId);
  const holding = (state.portfolio?.items || []).find(
    (item) => Number(item.skinId) === Number(skinId)
  );
  const managementClue = holding?.managementClue || null;
  const graphMarkup = renderSkinValueGraph(history);
  const timelineMarkup = tradeStats.timeline.length
    ? `<ul class="sync-list">${tradeStats.timeline
        .slice(0, 12)
        .map(
          (tx) =>
            `<li>${escapeHtml(tx.date)} | <strong>${escapeHtml(toTitle(tx.type))}</strong> ${tx.quantity} @ ${formatMoney(
              tx.unitPrice,
              "USD"
            )} (${formatMoney(tx.netTotal, "USD")} net)</li>`
        )
        .join("")}</ul>`
    : "<p class=\"muted\">No transactions for this item yet.</p>";

  const marketInsightMarkup = inspectMarketInsight?.sellSuggestion
    ? `
      <div class="sync-summary">
        <p><strong>Quick Sell Tiers (${escapeHtml(inspectMarketInsight.sellSuggestion.currency || state.currency)}):</strong></p>
        <ul class="sync-list">
          ${(inspectMarketInsight.sellSuggestion.tiers || [])
            .map(
              (tier) =>
                `<li>${escapeHtml(toTitle(tier.tier))}: <strong>${formatMoney(
                  tier.listPrice,
                  tier.currency || state.currency
                )}</strong> (net ${formatMoney(
                  tier.estimatedNet,
                  tier.currency || state.currency
                )})</li>`
            )
            .join("")}
        </ul>
        ${
          inspectMarketInsight?.liquidity
            ? `<p>Liquidity: <strong>${formatNumber(
                inspectMarketInsight.liquidity.score
              )}/100 (${escapeHtml(
                toTitle(inspectMarketInsight.liquidity.band)
              )})</strong></p>`
            : ""
        }
      </div>
    `
    : "";

  const exitResult = inspectExitWhatIf.result;
  const exitResultMarkup = exitResult
    ? `
      <div class="calc-result">
        <p><span>Reference Buy Price</span><strong>${formatMoney(
          exitResult.referenceBuyPrice,
          exitResult.currency || state.currency
        )}</strong></p>
        <p><span>Net Profit</span><strong>${formatMoney(
          exitResult.netProfit,
          exitResult.currency || state.currency
        )}</strong></p>
        <p><span>ROI</span><strong>${formatPercent(exitResult.roiPercent)}</strong></p>
        <p><span>Break-even</span><strong>${formatMoney(
          exitResult.breakEvenSellPrice,
          exitResult.currency || state.currency
        )}</strong></p>
      </div>
    `
    : "";

  const historyMarkup = history.length
    ? `<ul class="sync-list daily-points-list">${history
        .map(
          (row) =>
            `<li>${escapeHtml(String(row.recorded_at || "").slice(0, 10))}: <strong>${formatMoney(
              row.price,
              row.currency || state.currency
            )}</strong> ${escapeHtml(row.currency || "")}</li>`
        )
        .join("")}</ul>`
    : `<p class="muted">No price history yet for this item.</p>`;

  const clueMarkup = managementClue
    ? `
      <div class="sync-summary clue-panel">
        <div class="clue-head">
          <strong>Suggested action:</strong>
          ${formatManagementClue(managementClue)}
        </div>
        <p class="muted">
          Volatility: <strong>${formatPercent(
            managementClue?.metrics?.volatilityDailyPercent
          )}</strong> daily std-dev |
          7D projected move: <strong>${formatPercent(
            managementClue?.prediction?.expectedMovePercent
          )}</strong>
        </p>
        <p class="muted">
          Predicted 7D range:
          <strong>${formatMoney(
            managementClue?.prediction?.rangeLow,
            managementClue?.prediction?.currency || state.portfolio?.currency || state.currency
          )}</strong>
          -
          <strong>${formatMoney(
            managementClue?.prediction?.rangeHigh,
            managementClue?.prediction?.currency || state.portfolio?.currency || state.currency
          )}</strong>
        </p>
        <ul class="sync-list">
          ${(managementClue.reasons || [])
            .map((reason) => `<li>${escapeHtml(reason)}</li>`)
            .join("")}
        </ul>
      </div>
    `
    : "";

  const formId = isModal ? "inspect-modal-exit-whatif-form" : "exit-whatif-form";
  const cardClass = isModal ? "skin-card inspect-modal-skin-card" : "skin-card";

  return `
    <div class="${cardClass}">
      <h3>${escapeHtml(inspectSkin.market_hash_name)}</h3>
      <p>Item ID: <strong>${Number(inspectSkin.id || 0) || "-"}</strong></p>
      <p>${escapeHtml(inspectSkin.weapon || "-")} | ${escapeHtml(inspectSkin.exterior || "-")} | ${escapeHtml(inspectSkin.rarity || "-")}</p>
      <p>Latest Price: <strong>${latest ? `${formatMoney(latest.price)} ${escapeHtml(latest.currency)}` : "N/A"}</strong></p>
      <p>Price Source: <strong>${latest ? escapeHtml(latest.source || "unknown") : "-"}</strong></p>
      <p>Price Status: ${latest ? formatPriceStatusBadge(latest.status) : "-"}</p>
      <p>Confidence: <strong>${latest ? escapeHtml(formatConfidence(latest)) : "-"}</strong></p>
      ${
        latest && latest.stale
          ? `<p class="muted">Live refresh failed, showing last known price: ${escapeHtml(latest.staleReason || "unknown reason")}</p>`
          : ""
      }
      ${clueMarkup}
      ${marketInsightMarkup}
      <p class="muted">6-month value graph:</p>
      ${graphMarkup}
      <p class="muted">Item position from your trades:</p>
      <div class="sub-kpi-grid">
        <article class="sub-kpi-card">
          <span>Open Quantity</span>
          <strong>${formatNumber(tradeStats.openQuantity, 0)}</strong>
        </article>
        <article class="sub-kpi-card">
          <span>Avg Entry</span>
          <strong>${
            tradeStats.avgEntryPrice == null
              ? "-"
              : formatMoney(tradeStats.avgEntryPrice, "USD")
          }</strong>
        </article>
        <article class="sub-kpi-card ${Number(tradeStats.realizedPnl || 0) >= 0 ? "up" : "down"}">
          <span>Realized P/L</span>
          <strong>${formatMoney(tradeStats.realizedPnl, "USD")}</strong>
        </article>
      </div>
      <p class="muted">Transaction timeline (latest 12):</p>
      ${timelineMarkup}
      <form id="${formId}" class="trade-calc-grid inspect-exit-whatif-form" data-inspect-context="${escapeHtml(
        context
      )}">
        <label>Exit Quantity
          <input data-exit-field="quantity" type="number" step="1" min="1" value="${escapeHtml(
            inspectExitWhatIf.quantity
          )}" />
        </label>
        <label>Target Sell Price
          <input data-exit-field="target-price" type="number" step="0.01" min="0" value="${escapeHtml(
            inspectExitWhatIf.targetSellPrice
          )}" />
        </label>
        <label>Commission %
          <input data-exit-field="commission" type="number" step="0.01" min="0" max="99.99" value="${escapeHtml(
            inspectExitWhatIf.commissionPercent
          )}" />
        </label>
        <button type="submit" ${inspectExitWhatIf.loading ? "disabled" : ""}>
          ${inspectExitWhatIf.loading ? "Calculating..." : "What-if Exit"}
        </button>
      </form>
      ${exitResultMarkup}
      <p class="muted">Recent daily points (6 months):</p>
      ${historyMarkup}
    </div>
  `;
}

function renderInspectModalOverlay() {
  const modal = state.inspectModal;
  const heading = modal.skin?.market_hash_name || "Item Inspector";
  const subheading = modal.steamItemId
    ? `Steam Item ID: ${modal.steamItemId}`
    : "";

  return renderInspectModal({
    open: Boolean(modal.open),
    loading: Boolean(modal.loading),
    error: modal.error ? escapeHtml(modal.error) : "",
    heading: escapeHtml(heading),
    subheading: escapeHtml(subheading),
    bodyMarkup: renderSkinDetails("modal")
  });
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

function renderAuthNotices() {
  if (!state.authenticated) return "";

  const infoNotice = state.accountNotice
    ? `<div class="info" role="status" aria-live="polite">${escapeHtml(
        state.accountNotice
      )}</div>`
    : "";

  if (state.authProfile?.emailConfirmed === false) {
    const safeEmail = escapeHtml(state.authProfile.email || "your account");
    return `
      ${infoNotice}
      <div class="error" role="alert" aria-live="assertive">
        Email for <strong>${safeEmail}</strong> is not confirmed.
        Check your inbox and click the confirmation link.
      </div>
    `;
  }

  return infoNotice;
}

function renderManagementSummary() {
  const summary = state.portfolio?.managementSummary;
  if (!summary) return "";

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Inventory Clues</h2>
        <p class="helper-text">Guidance is generated from momentum, volatility, concentration, and short-horizon projection. Use it as decision support, not as financial advice.</p>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card">
            <span>Hold</span>
            <strong>${formatNumber(summary.hold, 0)}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Watch</span>
            <strong>${formatNumber(summary.watch, 0)}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Sell</span>
            <strong>${formatNumber(summary.sell, 0)}</strong>
          </article>
        </div>
      </article>
    </section>
  `;
}

function renderAdvancedAnalytics() {
  const profile = state.authProfile || {};
  const advanced = state.portfolio?.advancedAnalytics;
  if (!advanced) {
    if (profile.planTier === "free") {
      return `
        <section class="grid">
          <article class="panel wide">
            <h2>Advanced Analytics (Pro)</h2>
            <p class="muted">Upgrade to Pro to unlock VaR, tail risk, and deeper portfolio quality diagnostics.</p>
          </article>
        </section>
      `;
    }
    return "";
  }

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Advanced Analytics</h2>
        <div class="analytics-grid">
          <div class="analytics-item">
            <span>Price Quality Score</span>
            <strong>${escapeHtml(formatPercent(advanced.priceQualityScore))}</strong>
          </div>
          <div class="analytics-item">
            <span>Median Daily Volatility</span>
            <strong>${escapeHtml(formatPercent(advanced.medianVolatilityDailyPercent))}</strong>
          </div>
          <div class="analytics-item">
            <span>Tail Risk 7D (P10)</span>
            <strong>${escapeHtml(formatPercent(advanced.tailRisk7dPercentP10))}</strong>
          </div>
          <div class="analytics-item">
            <span>Estimated VaR 95% 1D</span>
            <strong>${escapeHtml(formatPercent(advanced.estimatedVar95OneDayPercent))}</strong>
          </div>
          <div class="analytics-item">
            <span>Top 10 Weight</span>
            <strong>${escapeHtml(formatPercent(advanced.top10WeightPercent))}</strong>
          </div>
          <div class="analytics-item">
            <span>Positions Over 2%</span>
            <strong>${escapeHtml(formatNumber(advanced.diversifiedPositionsOver2Percent, 0))}</strong>
          </div>
        </div>
      </article>
    </section>
  `;
}

function renderBacktestPanel() {
  const profile = state.authProfile || {};
  const entitlements = profile.entitlements || {};
  const canBacktest = Boolean(entitlements.backtesting);
  const result = state.backtest.result;

  if (!canBacktest) {
    return `
      <section class="grid">
        <article class="panel wide">
          <h2>Historical Backtesting (Pro)</h2>
          <p class="muted">Upgrade to Pro to run historical portfolio backtests and risk metrics.</p>
        </article>
      </section>
    `;
  }

  const metrics = result?.metrics || {};

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Historical Backtesting</h2>
        <form id="backtest-form" class="trade-calc-grid">
          <label>Days
            <input id="backtest-days" type="number" min="7" max="1095" value="${escapeHtml(
              state.backtest.days
            )}" />
          </label>
          <button type="submit" ${state.backtest.loading ? "disabled" : ""}>
            ${state.backtest.loading ? "Running..." : "Run Backtest"}
          </button>
          <button
            type="button"
            class="ghost-btn"
            id="export-portfolio-btn"
          >
            Export Portfolio CSV
          </button>
        </form>
        ${
          result
            ? `
          <div class="sub-kpi-grid">
            <article class="sub-kpi-card"><span>Total Return</span><strong>${formatPercent(
              metrics.totalReturnPercent
            )}</strong></article>
            <article class="sub-kpi-card"><span>Annualized Return</span><strong>${formatPercent(
              metrics.annualizedReturnPercent
            )}</strong></article>
            <article class="sub-kpi-card"><span>Max Drawdown</span><strong>${formatPercent(
              metrics.maxDrawdownPercent
            )}</strong></article>
            <article class="sub-kpi-card"><span>Daily Volatility</span><strong>${formatPercent(
              metrics.volatilityDailyPercent
            )}</strong></article>
            <article class="sub-kpi-card"><span>Win Rate</span><strong>${formatPercent(
              metrics.winRatePercent
            )}</strong></article>
            <article class="sub-kpi-card"><span>Period</span><strong>${escapeHtml(
              `${Number(result.days || 0)} days`
            )}</strong></article>
          </div>
        `
            : '<p class="muted">Run a backtest to compute return and risk statistics.</p>'
        }
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
  const signal = buildPortfolioSignals();
  const liquidityScore = signal.liquidityScore == null ? 0 : signal.liquidityScore;
  const liquidityLabel =
    signal.liquidityBand === "high"
      ? "High Liquidity"
      : signal.liquidityBand === "medium"
        ? "Medium Liquidity"
        : signal.liquidityBand === "low"
          ? "Low Liquidity"
          : "No Data";
  const riskLabel =
    signal.riskBand === "high"
      ? "Elevated Risk"
      : signal.riskBand === "medium"
        ? "Balanced Risk"
        : "Controlled Risk";

  return `
    <section class="grid">
      <article class="panel">
        <h2>Top Movers</h2>
        <div class="movers-grid">
          <div class="mover-card up">
            <span>Top Gainer</span>
            <strong>${topGainer ? escapeHtml(topGainer.marketHashName) : "N/A"}</strong>
            <p>${topGainer ? escapeHtml(formatPercent(topGainer.sevenDayChangePercent)) : "-"}</p>
            <small>${topGainer ? formatMoney(topGainer.lineValue, state.portfolio?.currency || state.currency) : ""}</small>
          </div>
          <div class="mover-card down">
            <span>Top Loser</span>
            <strong>${topLoser ? escapeHtml(topLoser.marketHashName) : "N/A"}</strong>
            <p>${topLoser ? escapeHtml(formatPercent(topLoser.sevenDayChangePercent)) : "-"}</p>
            <small>${topLoser ? formatMoney(topLoser.lineValue, state.portfolio?.currency || state.currency) : ""}</small>
          </div>
        </div>
      </article>
      <article class="panel">
        <h2>Liquidity Score</h2>
        <div class="signal-score ${escapeHtml(signal.liquidityBand)}">
          <strong
            class="metric-number"
            data-count-key="liquidity-score"
            data-count-format="integer"
            data-count-to="${liquidityScore}"
          >
            ${liquidityScore}
          </strong>
          <span>/100</span>
        </div>
        <p class="helper-text">${escapeHtml(liquidityLabel)} based on priced inventory freshness.</p>
        <p class="muted">
          Ready: ${signal.holdingsCount - signal.staleItems - signal.unpricedItems} /
          ${signal.holdingsCount} items
        </p>
      </article>
      <article class="panel">
        <h2>Risk Score</h2>
        <div class="signal-score ${escapeHtml(signal.riskBand)}">
          <strong
            class="metric-number"
            data-count-key="risk-score"
            data-count-format="integer"
            data-count-to="${signal.riskScore}"
          >
            ${signal.riskScore}
          </strong>
          <span>/100</span>
        </div>
        <p class="helper-text">${escapeHtml(riskLabel)} from concentration and breadth pressure.</p>
        <p class="muted">
          Concentration ${formatRiskBadge(analytics.concentrationRisk)} |
          Breadth ${escapeHtml(formatPercent(breadth.advancerRatioPercent))}
        </p>
      </article>
      <article class="panel">
        <h2>Portfolio Structure</h2>
        <div class="analytics-grid">
          <div class="analytics-item">
            <span>Holdings</span>
            <strong>${escapeHtml(formatNumber(analytics.holdingsCount, 0))}</strong>
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
      </article>
    </section>
  `;
}

function renderTeamTab() {
  const profile = state.authProfile || {};
  const isTeam = String(profile.planTier || "free") === "team";
  const data = state.teamDashboard.payload;

  if (!isTeam) {
    return `
      <section class="grid">
        <article class="panel wide">
          <h2>Team / Creator Dashboard</h2>
          <p class="muted">This dashboard is available on Team plan for larger inventories and creator operations.</p>
        </article>
      </section>
    `;
  }

  if (state.teamDashboard.loading) {
    return `
      <section class="grid">
        <article class="panel wide">
          <h2>Team / Creator Dashboard</h2>
          <p class="muted">Loading team dashboard...</p>
        </article>
      </section>
    `;
  }

  if (!data) {
    return `
      <section class="grid">
        <article class="panel wide">
          <h2>Team / Creator Dashboard</h2>
          <div class="row">
            <button id="team-refresh-btn" type="button" class="ghost-btn">Refresh Team Metrics</button>
          </div>
          <p class="muted">No dashboard snapshot yet. Click refresh to load team and creator KPIs.</p>
        </article>
      </section>
    `;
  }

  const summary = data?.summary || {};
  const creator = data?.creatorMetrics || {};
  const ops = data?.operations || {};
  const breakdown = ops.ownershipBreakdown || {};

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Team / Creator Dashboard</h2>
        <div class="row">
          <button id="team-refresh-btn" type="button" class="ghost-btn">Refresh Team Metrics</button>
        </div>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card"><span>Total Value</span><strong>${formatMoney(
            summary.totalValue,
            data?.currency || state.currency
          )}</strong></article>
          <article class="sub-kpi-card"><span>Holdings</span><strong>${formatNumber(
            summary.holdingsCount,
            0
          )}</strong></article>
          <article class="sub-kpi-card"><span>Top 5 Weight</span><strong>${formatPercent(
            summary.top5WeightPercent
          )}</strong></article>
          <article class="sub-kpi-card"><span>Followers</span><strong>${formatNumber(
            creator.followers,
            0
          )}</strong></article>
          <article class="sub-kpi-card"><span>Views / Referrals (30D)</span><strong>${escapeHtml(
            `${Number(creator.views30d || 0)} / ${Number(creator.referrals30d || 0)}`
          )}</strong></article>
          <article class="sub-kpi-card"><span>Unpriced / Stale</span><strong>${escapeHtml(
            `${formatPercent(summary.unpricedRatioPercent)} / ${formatPercent(summary.staleRatioPercent)}`
          )}</strong></article>
        </div>
        <p class="helper-text">
          Ownership changes (last 50 sync events): Acquired ${Number(
            breakdown.acquired || 0
          )}, Increased ${Number(breakdown.increased || 0)}, Decreased ${Number(
            breakdown.decreased || 0
          )}, Disposed ${Number(breakdown.disposed || 0)}.
        </p>
      </article>
    </section>
  `;
}

function renderTabNav() {
  const tabs = [
    { id: "dashboard", label: "Dashboard", hint: "Performance" },
    { id: "portfolio", label: "Portfolio", hint: "Holdings" },
    { id: "alerts", label: "Alerts", hint: "Triggers" },
    { id: "trades", label: "Transactions", hint: "Buys/Sells" },
    { id: "social", label: "Watchlist", hint: "Community" },
    { id: "market", label: "Market", hint: "Pricing" },
    { id: "team", label: "Team", hint: "Creator Ops" },
    { id: "settings", label: "Settings", hint: "Account" }
  ];

  return `
    <nav class="sidebar-nav">
      ${tabs
        .map(
          (tab) => `
          <button
            type="button"
            class="ghost-btn tab-btn sidebar-nav-item ${state.activeTab === tab.id ? "active" : ""}"
            data-tab="${tab.id}"
          >
            <span>${escapeHtml(tab.label)}</span>
            <small>${escapeHtml(tab.hint)}</small>
          </button>
        `
        )
        .join("")}
    </nav>
  `;
}

function renderAlertsCenter() {
  const holdings = getHoldingsList();
  const alertOptions = buildHoldingOptions(state.alertForm.skinId);
  const alertRows = Array.isArray(state.alertsFeed) ? state.alertsFeed : [];
  const eventRows = Array.isArray(state.alertEvents) ? state.alertEvents : [];
  const ownershipRows = Array.isArray(state.ownershipAlertEvents)
    ? state.ownershipAlertEvents
    : [];
  const isEditMode = state.alertForm.mode === "edit" && state.alertForm.alertId;

  const configuredMarkup = alertRows.length
    ? `
      <table>
        <thead>
          <tr>
            <th>Item</th>
            <th>Target</th>
            <th>% Trigger</th>
            <th>Direction</th>
            <th>Cooldown</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${alertRows
            .map(
              (alert) => `
                <tr>
                  <td>${escapeHtml(alert.marketHashName || `Skin #${alert.skinId}`)}</td>
                  <td>${
                    alert.targetPrice == null
                      ? "-"
                      : formatMoney(alert.targetPrice, "USD")
                  }</td>
                  <td>${
                    alert.percentChangeThreshold == null
                      ? "-"
                      : formatPercent(alert.percentChangeThreshold)
                  }</td>
                  <td>${escapeHtml(toTitle(alert.direction || "both"))}</td>
                  <td>${Number(alert.cooldownMinutes || 0)}m</td>
                  <td>${
                    alert.enabled
                      ? '<span class="status-badge real">Enabled</span>'
                      : '<span class="status-badge stale">Paused</span>'
                  }</td>
                  <td>
                    <div class="row">
                      <button type="button" class="ghost-btn alert-edit-btn" data-alert-id="${Number(
                        alert.id
                      )}">Edit</button>
                      <button
                        type="button"
                        class="ghost-btn alert-toggle-btn"
                        data-alert-id="${Number(alert.id)}"
                        data-enabled="${alert.enabled ? "false" : "true"}"
                      >
                        ${alert.enabled ? "Pause" : "Enable"}
                      </button>
                      <button type="button" class="ghost-btn alert-delete-btn" data-alert-id="${Number(
                        alert.id
                      )}">Delete</button>
                    </div>
                  </td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No configured alerts yet.</p>';

  const eventsMarkup = eventRows.length
    ? `
      <table>
        <thead>
          <tr><th>When</th><th>Item</th><th>Trigger</th><th>Market Price</th><th>24H %</th></tr>
        </thead>
        <tbody>
          ${eventRows
            .slice(0, 30)
            .map(
              (event) => `
                <tr>
                  <td>${escapeHtml(String(event.triggeredAt || "").slice(0, 19).replace("T", " "))}</td>
                  <td>${escapeHtml(event.marketHashName || `Skin #${event.skinId}`)}</td>
                  <td>${escapeHtml(toTitle(event.triggerType || "-"))}</td>
                  <td>${formatMoney(event.marketPrice, "USD")}</td>
                  <td>${formatPercent(event.changePercent)}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No alert triggers yet.</p>';

  const ownershipMarkup = ownershipRows.length
    ? `
      <table>
        <thead>
          <tr><th>When</th><th>Item</th><th>Change</th><th>Qty</th><th>Est. Value Delta</th></tr>
        </thead>
        <tbody>
          ${ownershipRows
            .slice(0, 30)
            .map(
              (event) => `
                <tr>
                  <td>${escapeHtml(String(event.createdAt || event.syncedAt || "").slice(0, 19).replace("T", " "))}</td>
                  <td>${escapeHtml(event.marketHashName || `Skin #${event.skinId}`)}</td>
                  <td>${escapeHtml(toTitle(event.changeType || "-"))}</td>
                  <td>${Number(event.previousQuantity || 0)} -> ${Number(event.newQuantity || 0)} (${Number(
                event.quantityDelta || 0
              ) > 0 ? "+" : ""}${Number(event.quantityDelta || 0)})</td>
                  <td>${
                    event.estimatedValueDelta == null
                      ? "-"
                      : formatMoney(event.estimatedValueDelta, event.currency || "USD")
                  }</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No ownership-change events yet. Run inventory sync after Steam changes.</p>';

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>${isEditMode ? "Edit Alert" : "Create Alert"}</h2>
        <p class="helper-text">Set trigger by target price and/or percent move. Alert thresholds and checks use <strong>USD</strong> market prices.</p>
        <form id="alert-form" class="trade-calc-grid">
          <label>Item
            <select id="alert-skin-id" ${holdings.length ? "" : "disabled"}>${alertOptions}</select>
          </label>
          <label>Target Price (USD)
            <input id="alert-target-price" type="number" min="0" step="0.01" value="${escapeHtml(
              state.alertForm.targetPrice
            )}" />
          </label>
          <label>% Change Trigger
            <input id="alert-percent-threshold" type="number" min="0" step="0.01" value="${escapeHtml(
              state.alertForm.percentChangeThreshold
            )}" />
          </label>
          <label>Direction
            <select id="alert-direction">
              <option value="both" ${state.alertForm.direction === "both" ? "selected" : ""}>Both</option>
              <option value="up" ${state.alertForm.direction === "up" ? "selected" : ""}>Up</option>
              <option value="down" ${state.alertForm.direction === "down" ? "selected" : ""}>Down</option>
            </select>
          </label>
          <label>Cooldown (minutes)
            <input id="alert-cooldown" type="number" min="0" step="1" value="${escapeHtml(
              state.alertForm.cooldownMinutes
            )}" />
          </label>
          <label>
            Enabled
            <input id="alert-enabled" type="checkbox" ${state.alertForm.enabled ? "checked" : ""} />
          </label>
          <button type="submit" ${state.alertForm.submitting || !holdings.length ? "disabled" : ""}>
            ${
              state.alertForm.submitting
                ? "Saving..."
                : isEditMode
                  ? "Update Alert"
                  : "Create Alert"
            }
          </button>
          ${
            isEditMode
              ? '<button id="alert-cancel-btn" type="button" class="ghost-btn">Cancel Edit</button>'
              : ""
          }
        </form>
      </article>
      <article class="panel wide">
        <h2>Configured Alerts</h2>
        ${configuredMarkup}
      </article>
      <article class="panel wide">
        <h2>Recent Alert Events</h2>
        ${eventsMarkup}
      </article>
      <article class="panel wide">
        <h2>Ownership Change Events</h2>
        ${ownershipMarkup}
      </article>
    </section>
  `;
}

function renderSocialTab() {
  const watchRows = Array.isArray(state.social.watchlist) ? state.social.watchlist : [];
  const boardRows = Array.isArray(state.social.leaderboard) ? state.social.leaderboard : [];
  const watchlistBySteamId = new Set(
    watchRows.map((row) => String(row.steamId64 || "").trim()).filter(Boolean)
  );

  const watchlistMarkup = watchRows.length
    ? `
      <table>
        <thead>
          <tr><th>Player</th><th>Value</th><th>Holdings</th><th>Followers</th><th>Views/Referrals (30D)</th><th>Actions</th></tr>
        </thead>
        <tbody>
          ${watchRows
            .map(
              (row) => `
                <tr>
                  <td>
                    <strong>${escapeHtml(row.displayName || row.steamId64 || "-")}</strong>
                    <div class="muted"><code>${escapeHtml(row.steamId64 || "-")}</code></div>
                  </td>
                  <td>${formatMoney(row.totalValue, row.currency || state.currency)}</td>
                  <td>${Number(row.holdingsCount || 0)} (${Number(row.uniqueItems || 0)} unique)</td>
                  <td>${Number(row.followers || 0)}</td>
                  <td>${Number(row.views30d || 0)} / ${Number(row.referrals30d || 0)}</td>
                  <td>
                    <div class="row">
                      <a class="link-btn ghost" href="/u/${encodeURIComponent(
                        row.steamId64 || ""
                      )}" target="_blank" rel="noreferrer">Open</a>
                      <button type="button" class="ghost-btn watch-remove-btn" data-steam-id="${escapeHtml(
                        row.steamId64 || ""
                      )}">Remove</button>
                    </div>
                  </td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No watchlist entries yet. Add a SteamID64 to follow public portfolios.</p>';

  const leaderboardMarkup = boardRows.length
    ? `
      <table>
        <thead>
          <tr><th>Rank</th><th>Player</th><th>Value</th><th>Holdings</th><th>Followers</th><th>Views/Referrals (30D)</th><th>Action</th></tr>
        </thead>
        <tbody>
          ${boardRows
            .map((row) => {
              const steamId64 = String(row.steamId64 || "").trim();
              const isWatching =
                watchlistBySteamId.has(steamId64) || Boolean(row.inWatchlist);

              return `
                <tr>
                  <td>#${Number(row.rank || 0)}</td>
                  <td>
                    <strong>${escapeHtml(row.displayName || steamId64 || "-")}</strong>
                    <div class="muted"><code>${escapeHtml(steamId64 || "-")}</code></div>
                  </td>
                  <td>${formatMoney(row.totalValue, row.currency || state.currency)}</td>
                  <td>${Number(row.holdingsCount || 0)} (${Number(row.uniqueItems || 0)} unique)</td>
                  <td>${Number(row.followers || 0)}</td>
                  <td>${Number(row.views30d || 0)} / ${Number(row.referrals30d || 0)}</td>
                  <td>
                    <div class="row">
                      <a class="link-btn ghost" href="/u/${encodeURIComponent(
                        steamId64
                      )}" target="_blank" rel="noreferrer">Open</a>
                      <button
                        type="button"
                        class="ghost-btn leaderboard-watch-btn"
                        data-steam-id="${escapeHtml(steamId64)}"
                        data-watching="${isWatching ? "1" : "0"}"
                      >
                        ${isWatching ? "Unwatch" : "Watch"}
                      </button>
                    </div>
                  </td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No leaderboard data for this scope yet.</p>';

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Friends & Watchlist</h2>
        <p class="helper-text">Track other Steam portfolios by SteamID64 and open share pages instantly.</p>
        <form id="social-watch-form" class="trade-calc-grid">
          <label>SteamID64
            <input
              id="social-watch-steam-id"
              inputmode="numeric"
              pattern="[0-9]{17}"
              placeholder="7656119..."
              value="${escapeHtml(state.social.newSteamId || "")}"
            />
          </label>
          <button type="submit" ${state.social.loading ? "disabled" : ""}>
            ${state.social.loading ? "Saving..." : "Add to Watchlist"}
          </button>
        </form>
        ${watchlistMarkup}
      </article>
      <article class="panel wide">
        <h2>Leaderboard</h2>
        <form id="social-board-form" class="trade-calc-grid">
          <label>Scope
            <select id="social-scope" ${state.social.loading ? "disabled" : ""}>
              <option value="global" ${state.social.scope === "global" ? "selected" : ""}>Global Public</option>
              <option value="watchlist" ${state.social.scope === "watchlist" ? "selected" : ""}>My Watchlist</option>
            </select>
          </label>
          <button type="submit" ${state.social.loading ? "disabled" : ""}>
            ${state.social.loading ? "Refreshing..." : "Refresh Leaderboard"}
          </button>
        </form>
        ${leaderboardMarkup}
      </article>
    </section>
  `;
}

function renderMarketTab() {
  const holdings = getHoldingsList();
  const marketOptions = buildHoldingOptions(state.marketTab.skinId);
  const valuation = state.marketTab.inventoryValue;
  const insight = state.marketTab.insight;
  const suggestion = insight?.sellSuggestion || null;
  const liquidity = insight?.liquidity || null;
  const hasSuggestionForSelected =
    suggestion && Number(suggestion.skinId) === Number(state.marketTab.skinId || 0);
  const selectedSkin = holdings.find(
    (item) => String(item.skinId) === String(state.marketTab.skinId || "")
  );
  const selectedName = selectedSkin?.marketHashName || `Skin #${state.marketTab.skinId}`;

  const valuationRows = Array.isArray(valuation?.items) ? valuation.items : [];
  const totalGrossLabel = valuation
    ? formatMoney(valuation.totalValueGross, valuation.currency || state.currency)
    : "-";
  const totalNetLabel = valuation
    ? formatMoney(valuation.totalValueNet, valuation.currency || state.currency)
    : "-";
  const itemsCountLabel = valuation ? formatNumber(valuation.itemsCount, 0) : "-";
  const valuationMarkup = valuationRows.length
    ? `
      <table>
        <thead>
          <tr><th>Item</th><th>Qty</th><th>Price/Item</th><th>Gross</th><th>Net</th></tr>
        </thead>
        <tbody>
          ${valuationRows
            .slice(0, 20)
            .map(
              (row) => `
                <tr>
                  <td>${escapeHtml(row.marketHashName)}</td>
                  <td>${Number(row.quantity || 0)}</td>
                  <td>${formatMoney(row.pricePerItem, valuation.currency || state.currency)}</td>
                  <td>${formatMoney(row.lineValueGross, valuation.currency || state.currency)}</td>
                  <td>${formatMoney(row.lineValueNet, valuation.currency || state.currency)}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No valuation yet. Click "Refresh valuation".</p>';

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Inventory Market Value</h2>
        <form id="market-inventory-form" class="trade-calc-grid">
          <label>Commission %
            <input id="market-commission" type="number" min="0" max="99.99" step="0.01" value="${escapeHtml(
              state.marketTab.commissionPercent
            )}" />
          </label>
          <button type="submit" ${state.marketTab.loading ? "disabled" : ""}>
            ${state.marketTab.loading ? "Loading..." : "Refresh valuation"}
          </button>
        </form>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card">
            <span>Total Gross</span>
            <strong>${totalGrossLabel}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Total Net (after commission)</span>
            <strong>${totalNetLabel}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Items Count</span>
            <strong>${itemsCountLabel}</strong>
          </article>
        </div>
        ${valuationMarkup}
      </article>

      <article class="panel wide">
        <h2>Item Market Intelligence</h2>
        <form id="market-item-form" class="trade-calc-grid">
          <label>Item
            <select id="market-skin-id" ${holdings.length ? "" : "disabled"}>${marketOptions}</select>
          </label>
          <label>Commission %
            <input id="market-commission-inline" type="number" min="0" max="99.99" step="0.01" value="${escapeHtml(
              state.marketTab.commissionPercent
            )}" />
          </label>
          <button type="submit" ${state.marketTab.loading || !holdings.length ? "disabled" : ""}>
            ${state.marketTab.loading ? "Analyzing..." : "Analyze Item"}
          </button>
        </form>
        ${
          !hasSuggestionForSelected
            ? '<p class="muted">Choose an item and run analysis to see quick-sell tiers and liquidity.</p>'
            : `
        <h3>${escapeHtml(selectedName)}</h3>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card">
            <span>Lowest Listing</span>
            <strong>${formatMoney(suggestion.lowestListingPrice, suggestion.currency || state.currency)}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Avg 7D</span>
            <strong>${formatMoney(suggestion.average7dPrice, suggestion.currency || state.currency)}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Volume 24H</span>
            <strong>${formatNumber(suggestion.volume24h, 0)}</strong>
          </article>
        </div>
        <table>
          <thead>
            <tr><th>Tier</th><th>List Price</th><th>Est. Net</th><th>Expected Fill</th></tr>
          </thead>
          <tbody>
            ${(suggestion.tiers || [])
              .map(
                (tier) => `
                  <tr>
                    <td>${escapeHtml(toTitle(tier.tier))}</td>
                    <td>${formatMoney(tier.listPrice, tier.currency || state.currency)}</td>
                    <td>${formatMoney(tier.estimatedNet, tier.currency || state.currency)}</td>
                    <td>${escapeHtml(toTitle(tier.expectedFill))}</td>
                  </tr>
                `
              )
              .join("")}
          </tbody>
        </table>
        ${
          liquidity
            ? `<p class="helper-text">Liquidity: <strong>${formatNumber(
                liquidity.score
              )}/100</strong> (${escapeHtml(toTitle(liquidity.band))})</p>`
            : ""
        }
        `
        }
      </article>
    </section>
  `;
}

function renderSettingsTab() {
  const profile = state.authProfile || {};
  const steamLinked = Boolean(profile.steamLinked);
  const steamLinkUrl = buildSteamAuthStartUrl("link");
  const providerLabel = toTitle(profile.provider || "email");
  const publicPortfolioEnabled = profile.publicPortfolioEnabled !== false;
  const ownershipAlertsEnabled = profile.ownershipAlertsEnabled !== false;
  const planTier = String(profile.planTier || "free").toLowerCase();
  const entitlements = profile.entitlements || {};
  const publicUrl = steamLinked
    ? `${window.location.origin}/u/${encodeURIComponent(profile.steamId64 || "")}`
    : "";
  const pricingMode = normalizePricingMode(state.portfolio?.pricing?.mode || state.pricingMode);
  const currencyOptions = SUPPORTED_CURRENCIES.map(
    (code) =>
      `<option value="${code}" ${code === state.currency ? "selected" : ""}>${code}</option>`
  ).join("");

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Account Settings</h2>
        <p class="helper-text">Use Steam linking to connect your existing email account with Steam login and avoid duplicate profiles.</p>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card">
            <span>Login Provider</span>
            <strong>${escapeHtml(providerLabel)}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Steam Status</span>
            <strong>${steamLinked ? "Linked" : "Not linked"}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>SteamID64</span>
            <strong>${escapeHtml(profile.steamId64 || "-")}</strong>
          </article>
        </div>
        <p class="muted">
          ${steamLinked
            ? "Steam login is connected to this account. You can relink to switch or refresh profile data."
            : "No Steam account linked yet. Link now to enable Steam sign-in and one-account access."}
        </p>
        <div class="row">
          <a class="link-btn" href="${escapeHtml(steamLinkUrl)}">
            ${steamLinked ? "Relink Steam Account" : "Link Steam Account"}
          </a>
        </div>
        ${
          steamLinked
            ? `<p class="helper-text">Public URL: <a href="${escapeHtml(publicUrl)}" target="_blank" rel="noreferrer">${escapeHtml(
                publicUrl
              )}</a></p>`
            : ""
        }
      </article>
      <article class="panel wide">
        <h2>Growth & Visibility</h2>
        <form id="public-settings-form" class="form">
          <label>
            Public portfolio page enabled
            <input id="public-portfolio-enabled" type="checkbox" ${
              publicPortfolioEnabled ? "checked" : ""
            } ${steamLinked ? "" : "disabled"} />
          </label>
          <button type="submit" ${steamLinked ? "" : "disabled"}>Save Public Profile Setting</button>
        </form>
        <form id="ownership-settings-form" class="form">
          <label>
            Ownership-change alerts enabled
            <input id="ownership-alerts-enabled" type="checkbox" ${
              ownershipAlertsEnabled ? "checked" : ""
            } />
          </label>
          <button type="submit">Save Ownership Alert Setting</button>
        </form>
      </article>
      <article class="panel wide">
        <h2>Plan & Monetization</h2>
        <div class="form settings-pricing-form">
          <h3 style="margin:0;">Portfolio Pricing Preferences</h3>
          <label>Pricing mode
            <select id="settings-pricing-mode">
              <option value="lowest_buy" ${pricingMode === "lowest_buy" ? "selected" : ""}>Lowest Buy</option>
              <option value="steam" ${pricingMode === "steam" ? "selected" : ""}>Steam Price</option>
              <option value="best_sell_net" ${pricingMode === "best_sell_net" ? "selected" : ""}>Best Sell Net</option>
            </select>
          </label>
          <label>Display currency
            <select id="settings-currency-select">${currencyOptions}</select>
          </label>
          <p class="helper-text">Pricing mode changes how item price and total value are calculated across Steam, Skinport, CSFloat, and DMarket.</p>
        </div>
        <div class="sub-kpi-grid">
          <article class="sub-kpi-card">
            <span>Current Plan</span>
            <strong>${escapeHtml(toTitle(planTier))}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Billing Status</span>
            <strong>${escapeHtml(toTitle(profile.billingStatus || "inactive"))}</strong>
          </article>
          <article class="sub-kpi-card">
            <span>Seats</span>
            <strong>${escapeHtml(formatNumber(profile.planSeats || 1, 0))}</strong>
          </article>
        </div>
        <p class="helper-text">
          Entitlements: Alerts ${Number(entitlements.maxAlerts || 0)}, History up to ${Number(
            entitlements.maxHistoryDays || 30
          )} days, CSV export ${entitlements.csvExport ? "enabled" : "disabled"}, Backtesting ${
            entitlements.backtesting ? "enabled" : "disabled"
          }.
        </p>
        <div class="row">
          <button
            type="button"
            class="ghost-btn plan-switch-btn"
            data-plan-tier="free"
            ${planTier === "free" ? "disabled" : ""}
          >
            Switch to Free
          </button>
          <button
            type="button"
            class="ghost-btn plan-switch-btn"
            data-plan-tier="pro"
            ${planTier === "pro" ? "disabled" : ""}
          >
            Switch to Pro
          </button>
          <button
            type="button"
            class="ghost-btn plan-switch-btn"
            data-plan-tier="team"
            ${planTier === "team" ? "disabled" : ""}
          >
            Switch to Team
          </button>
        </div>
      </article>
    </section>
  `;
}

function renderPublicPortfolioPage() {
  const steamStartUrl = buildSteamAuthStartUrl("login");
  const page = state.publicPage || {};
  const payload = page.payload || {};
  const profile = payload.profile || {};
  const portfolio = payload.portfolio || {};
  const isSignedIn = Boolean(state.authenticated);
  const userEmailTitle = String(state.authProfile?.email || "").trim();
  const userEmailLabel = getHeaderEmailLabel();
  const historyPoints = Array.isArray(payload?.history?.points) ? payload.history.points : [];
  const items = Array.isArray(portfolio.items) ? portfolio.items : [];
  const historyPreview = historyPoints.slice(-10);

  const historyMarkup = historyPreview.length
    ? `<ul class="sync-list">${historyPreview
        .map(
          (point) =>
            `<li>${escapeHtml(point.date)}: <strong>${formatMoney(
              point.totalValue,
              portfolio.currency || state.currency
            )}</strong></li>`
        )
        .join("")}</ul>`
    : '<p class="muted">No history points yet.</p>';

  const holdingsMarkup = items.length
    ? `
      <table>
        <thead>
          <tr><th>Item</th><th>Qty</th><th>Price</th><th>7D %</th><th>Value</th></tr>
        </thead>
        <tbody>
          ${items
            .slice(0, 30)
            .map((item) => {
              const rarityTheme = getItemRarityTheme(item);
              const fallbackImage = isCaseLikeItem(item)
                ? defaultCaseImage
                : defaultSkinImage;
              return `
                <tr>
                  <td>
                    <div class="table-item-cell">
                      <img
                        class="table-item-thumb"
                        style="--rarity-color: ${rarityTheme.color};"
                        src="${escapeHtml(getItemImageUrl(item))}"
                        alt="${escapeHtml(item.marketHashName || "CS2 item")}"
                        loading="lazy"
                        onerror="this.onerror=null;this.src='${escapeHtml(fallbackImage)}';"
                      />
                      <div class="table-item-info">
                        <div class="skin-name">${escapeHtml(item.marketHashName)}</div>
                        <div class="table-item-subline">
                          <span class="rarity-tag" style="--rarity-color: ${rarityTheme.color};">
                            ${escapeHtml(rarityTheme.rarity)}
                          </span>
                        </div>
                      </div>
                    </div>
                  </td>
                  <td>${Number(item.quantity || 0)}</td>
                  <td>${formatMoney(item.currentPrice, portfolio.currency || state.currency)}</td>
                  <td>${formatPercent(item.sevenDayChangePercent)}</td>
                  <td>${formatMoney(item.lineValue, portfolio.currency || state.currency)}</td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No public holdings available yet.</p>';

  app.innerHTML = `
    <main class="layout">
      <nav class="topbar">
        <div class="brand">CS2 Portfolio Analyzer</div>
        <div class="top-actions">
          ${
            isSignedIn
              ? `
                <span class="user-chip" title="${escapeHtml(userEmailTitle)}">${escapeHtml(
                  userEmailLabel
                )}</span>
                <a class="link-btn ghost" href="/">Back to Dashboard</a>
              `
              : `
                <a class="link-btn ghost" href="${escapeHtml(steamStartUrl)}">Login with Steam</a>
                <a class="link-btn ghost" href="/login.html">Login</a>
                <a class="link-btn" href="/register.html">Start Free</a>
              `
          }
        </div>
      </nav>
      <section class="grid">
        <article class="panel wide">
          <p class="eyebrow">Public Portfolio</p>
          ${
            page.loading
              ? "<h1>Loading profile...</h1>"
              : page.error
                ? `<h1>Could not load profile</h1><div class="error" role="alert" aria-live="assertive">${escapeHtml(
                    page.error
                  )}</div>`
                : `<h1>${escapeHtml(profile.displayName || `Steam ${profile.steamId64 || ""}`)}</h1>`
          }
          ${
            page.error
              ? '<p class="muted">This profile may be private, missing, or temporarily unavailable.</p>'
              : `<p class="helper-text">SteamID64: <code>${escapeHtml(
                  profile.steamId64 || "-"
                )}</code></p>`
          }
          ${
            !page.loading && !page.error
              ? `<div class="sub-kpi-grid">
                  <article class="sub-kpi-card">
                    <span>Total Value</span>
                    <strong>${formatMoney(
                      portfolio.totalValue,
                      portfolio.currency || state.currency
                    )}</strong>
                  </article>
                  <article class="sub-kpi-card">
                    <span>24H Change</span>
                    <strong>${formatPercent(portfolio.oneDayChangePercent)}</strong>
                  </article>
                  <article class="sub-kpi-card">
                    <span>7D Change</span>
                    <strong>${formatPercent(portfolio.sevenDayChangePercent)}</strong>
                  </article>
                </div>`
              : ""
          }
        </article>
      </section>
      ${
        !page.loading && !page.error
          ? `
          <section class="grid">
            <article class="panel wide">
              <h2>Public Holdings</h2>
              ${holdingsMarkup}
            </article>
            <article class="panel wide">
              <h2>30-Day Value Trend</h2>
              ${historyMarkup}
            </article>
          </section>
        `
          : ""
      }
    </main>
  `;
}

function renderPublicHome() {
  const steamStartUrl = buildSteamAuthStartUrl("login");
  const isSignedIn = Boolean(state.authenticated);
  const userEmailTitle = String(state.authProfile?.email || "").trim();
  const userEmailLabel = getHeaderEmailLabel();

  app.innerHTML = `
    <main class="layout">
      <nav class="topbar">
        <div class="brand">CS2 Portfolio Analyzer</div>
        <div class="top-actions">
          ${
            isSignedIn
              ? `
                <span class="user-chip" title="${escapeHtml(userEmailTitle)}">${escapeHtml(
                  userEmailLabel
                )}</span>
                <a class="link-btn ghost" href="/">Back to Dashboard</a>
              `
              : `
                <a class="link-btn ghost" href="${escapeHtml(steamStartUrl)}">Login with Steam</a>
                <a class="link-btn ghost" href="/login.html">Login</a>
                <a class="link-btn" href="/register.html">Start Free</a>
              `
          }
        </div>
      </nav>

      <section class="hero-block">
        <div>
          <p class="eyebrow">SaaS for CS2 traders and collectors</p>
          <h1>See your CS2 items like a real portfolio, not a random inventory list.</h1>
          <p class="hero-copy">Connect Steam, sync your inventory (skins, cases, stickers, music kits, and more), and instantly track value, ROI, and 7-day movement in one focused dashboard.</p>
          <div class="hero-actions">
            <a class="link-btn ghost" href="${escapeHtml(steamStartUrl)}">Continue with Steam</a>
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
            <li>Item-level lookup and trend data</li>
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

function renderSessionBoot() {
  app.innerHTML = `
    <main class="layout auth-layout">
      <article class="panel auth-panel">
        <p class="eyebrow">CS2 Portfolio Analyzer</p>
        <h1>Loading your session</h1>
        <p class="muted">Please wait while we securely restore your account state.</p>
      </article>
    </main>
  `;
}

function renderSteamSyncPanel() {
  const profile = state.authProfile || {};
  const steamLinked = Boolean(profile.steamLinked);
  const steamLinkUrl = buildSteamAuthStartUrl("link");

  if (!steamLinked) {
    return `
      <article class="panel">
        <h2>Steam Sync</h2>
        <p class="helper-text">Link Steam once to auto-connect your SteamID and unlock one-click inventory sync.</p>
        <div class="row">
          <a class="link-btn" href="${escapeHtml(steamLinkUrl)}">Link Steam Account</a>
        </div>
        <p class="muted sync-note">After linking, return here and sync inventory instantly.</p>
      </article>
    `;
  }

  const onboardingNotice = state.steamOnboardingPending
    ? `<div class="info" role="status" aria-live="polite"><strong>You're connected, click sync now.</strong> We will import your inventory and latest prices in one step.</div>`
    : "";

  return `
    <article class="panel">
      <h2>Steam Sync</h2>
      <p class="helper-text">SteamID is connected from your login. Click <strong>Sync Inventory</strong> to import items and refresh market prices.</p>
      ${onboardingNotice}
      <div class="sub-kpi-grid">
        <article class="sub-kpi-card">
          <span>Steam Status</span>
          <strong>Connected</strong>
        </article>
        <article class="sub-kpi-card">
          <span>Persona</span>
          <strong>${escapeHtml(profile.steamDisplayName || "Steam account")}</strong>
        </article>
        <article class="sub-kpi-card">
          <span>SteamID64</span>
          <strong>${escapeHtml(profile.steamId64 || "-")}</strong>
        </article>
      </div>
      <div class="row">
        <button id="sync-btn" ${state.syncingInventory ? "disabled" : ""}>
          ${
            state.syncingInventory
              ? '<span class="loading-inline"><span class="spinner"></span>Syncing inventory...</span>'
              : "Sync Inventory"
          }
        </button>
        <a class="link-btn ghost" href="${escapeHtml(steamLinkUrl)}">Relink Steam</a>
      </div>
      ${
        state.syncingInventory
          ? '<p class="muted sync-note">Fetching inventory and market prices. This can take up to a minute.</p>'
          : ""
      }
      ${renderSyncSummary()}
    </article>
  `;
}

function renderApp() {
  const portfolio = state.portfolio || {};
  const oneDayTrendClass = Number(portfolio.oneDayChangePercent || 0) >= 0 ? "up" : "down";
  const trendClass = Number(portfolio.sevenDayChangePercent || 0) >= 0 ? "up" : "down";
  const holdingsPage = getFilteredHoldings();
  const userEmailTitle = String(state.authProfile?.email || "").trim();
  const userEmailLabel = getHeaderEmailLabel();
  const currencyCode = portfolio.currency || state.currency;
  const topValue = Number(portfolio.totalValue || 0);
  const top7d = Number(portfolio.sevenDayChangePercent || 0);
  const top24h = Number(portfolio.oneDayChangePercent || 0);
  const topValueLabel = getPricingModeLabel(portfolio?.pricing?.mode || state.pricingMode);

  const currencyOptions = SUPPORTED_CURRENCIES.map(
    (code) => `<option value="${code}" ${code === state.currency ? "selected" : ""}>${code}</option>`
  ).join("");

  const dashboardContent = `
    ${renderDashboardHero()}
    <section class="grid">
      <article class="panel wide">
        <h2>${state.historyDays === 180 ? "6-Month" : `${state.historyDays}-Day`} Performance Chart</h2>
        ${renderHistoryChart()}
      </article>
    </section>
    ${renderAnalytics()}
    ${renderAlerts()}
    ${renderPnlSummary()}
    ${renderManagementSummary()}
    ${renderAdvancedAnalytics()}
    ${renderBacktestPanel()}
  `;

  const portfolioContent = `
    <section class="grid dashboard-grid">
      ${renderSteamSyncPanel()}

      <article class="panel">
        <h2>Position Inspector</h2>
        <p class="helper-text">Paste a Steam Item ID to inspect current pricing, full item history, and scenario exits before placing a listing.</p>
        <form id="skin-form" class="form">
          <label>Steam Item ID
            <input id="steam-item-id" inputmode="numeric" pattern="[0-9]+" placeholder="e.g. 35719462921" value="${escapeHtml(state.inspectedSteamItemId)}" />
          </label>
          <button type="submit">Inspect Position</button>
        </form>
        ${renderSkinDetails()}
      </article>
    </section>

    <section class="grid">
      <article class="panel wide">
        <h2>Portfolio Holdings</h2>
        <p class="helper-text">
          Premium marketplace tiles for quick scanning plus an execution table for sorting and trade decisions.
        </p>
        ${renderPortfolioPricingControls()}
        ${renderSection({
          eyebrow: "Portfolio",
          title: "Marketplace Tiles",
          description:
            "Click any tile to inspect instantly in a modal without losing your current place in the table.",
          className: "portfolio-cards-section",
          body: renderPortfolioCardGrid()
        })}
        ${renderSection({
          eyebrow: "Portfolio",
          title: "Execution Table",
          description:
            "Filter, sort, and execute actions directly in-place. Compare rows expand inline under the clicked item.",
          className: "portfolio-table-section",
          body: `
            <div class="list-toolbar">
              <label>Search
                <input id="holdings-search" placeholder="item name or steam item id" value="${escapeHtml(
                  state.holdingsView.q
                )}" />
              </label>
              <label>Status
                <select id="holdings-status">
                  ${["all", "real", "cached", "stale", "unpriced", "mock"]
                    .map(
                      (status) =>
                        `<option value="${status}" ${
                          state.holdingsView.status === status ? "selected" : ""
                        }>${status === "all" ? "All" : toTitle(status)}</option>`
                    )
                    .join("")}
                </select>
              </label>
              <label>Signal Sort
                <select id="holdings-sort">
                  <option value="value_desc" ${state.holdingsView.sort === "value_desc" ? "selected" : ""}>Value high to low</option>
                  <option value="value_asc" ${state.holdingsView.sort === "value_asc" ? "selected" : ""}>Value low to high</option>
                  <option value="name_asc" ${state.holdingsView.sort === "name_asc" ? "selected" : ""}>Name A-Z</option>
                  <option value="name_desc" ${state.holdingsView.sort === "name_desc" ? "selected" : ""}>Name Z-A</option>
                  <option value="qty_desc" ${state.holdingsView.sort === "qty_desc" ? "selected" : ""}>Qty high to low</option>
                  <option value="qty_asc" ${state.holdingsView.sort === "qty_asc" ? "selected" : ""}>Qty low to high</option>
                  <option value="change_desc" ${state.holdingsView.sort === "change_desc" ? "selected" : ""}>7D change high to low</option>
                  <option value="change_asc" ${state.holdingsView.sort === "change_asc" ? "selected" : ""}>7D change low to high</option>
                  <option value="clue_sell_first" ${state.holdingsView.sort === "clue_sell_first" ? "selected" : ""}>Clue sell first</option>
                  <option value="clue_hold_first" ${state.holdingsView.sort === "clue_hold_first" ? "selected" : ""}>Clue hold first</option>
                </select>
              </label>
              <label>Per page
                <select id="holdings-page-size">
                  ${[10, 20, 50]
                    .map(
                      (n) =>
                        `<option value="${n}" ${
                          state.holdingsView.pageSize === n ? "selected" : ""
                        }>${n}</option>`
                    )
                    .join("")}
                </select>
              </label>
            </div>
            <div class="table-wrap">
              <table class="portfolio-table">
                <thead>
                  <tr>
                    <th>Steam Item ID</th>
                    <th>${renderHoldingsSortButton("Item", "name")}</th>
                    <th>${renderHoldingsSortButton("Qty", "qty")}</th>
                    <th>${renderHoldingsSortButton("Mode Price", "price")}</th>
                    <th>24H</th>
                    <th>${renderHoldingsSortButton("7D", "change")}</th>
                    <th>Status</th>
                    <th>Signal</th>
                    <th>${renderHoldingsSortButton("Mode Value", "value")}</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>${renderPortfolioRows()}</tbody>
              </table>
            </div>
          `
        })}
        <div class="pagination-bar">
          <button
            type="button"
            class="ghost-btn holdings-page-btn"
            data-page="${Math.max(holdingsPage.page - 1, 1)}"
            ${holdingsPage.page <= 1 ? "disabled" : ""}
          >
            Prev
          </button>
          <span>Page ${holdingsPage.page} / ${holdingsPage.pages} (${holdingsPage.total} total)</span>
          <button
            type="button"
            class="ghost-btn holdings-page-btn"
            data-page="${Math.min(holdingsPage.page + 1, holdingsPage.pages)}"
            ${holdingsPage.page >= holdingsPage.pages ? "disabled" : ""}
          >
            Next
          </button>
        </div>
      </article>
    </section>
  `;

  const tradesContent = `
    <section class="grid">
      <article class="panel wide">
        <h2>Transaction Exports</h2>
        <p class="helper-text">Export transaction history as CSV for bookkeeping, tax workflows, and back-office analytics.</p>
        <div class="row">
          <button id="export-transactions-btn" type="button">Export Transactions CSV</button>
        </div>
      </article>
    </section>
    ${renderTransactionManager()}
    ${renderTradeCalculator()}
  `;

  const tabContent =
    state.activeTab === "dashboard"
      ? dashboardContent
      : state.activeTab === "portfolio"
        ? portfolioContent
      : state.activeTab === "trades"
        ? tradesContent
        : state.activeTab === "alerts"
          ? renderAlertsCenter()
          : state.activeTab === "social"
            ? renderSocialTab()
            : state.activeTab === "team"
              ? renderTeamTab()
              : state.activeTab === "market"
                ? renderMarketTab()
                : renderSettingsTab();

  app.innerHTML = `
    <main class="layout app-shell">
      <aside class="sidebar">
        <div class="sidebar-header">
          <div class="brand">CS2 Portfolio Analyzer</div>
          <p class="muted sidebar-copy">Premium dark trading interface</p>
        </div>
        ${renderTabNav()}
        <div class="sidebar-footer">
          <span class="user-chip" title="${escapeHtml(userEmailTitle)}">${escapeHtml(
    userEmailLabel
  )}</span>
          <div class="row sidebar-actions">
            <button id="refresh-btn" class="ghost-btn">Refresh</button>
            <button id="logout-btn" class="ghost-btn">Logout</button>
          </div>
        </div>
      </aside>
      <section class="app-main">
        <header class="topbar premium-topbar">
          <div class="topbar-metrics">
            <article class="topbar-metric metric-glow">
              <span>${escapeHtml(topValueLabel)} Value</span>
              <strong
                class="metric-number"
                data-count-key="topbar-total-value"
                data-count-format="money"
                data-count-currency="${escapeHtml(currencyCode)}"
                data-count-to="${topValue}"
              >
                ${formatMoney(topValue, currencyCode)}
              </strong>
            </article>
            <article class="topbar-metric ${trendClass}">
              <span>7D Change</span>
              <strong
                class="metric-number"
                data-count-key="topbar-seven-day"
                data-count-format="percent"
                data-count-to="${top7d}"
              >
                ${formatPercent(top7d)}
              </strong>
              <small class="pnl-text ${oneDayTrendClass}">24H ${formatPercent(top24h)}</small>
            </article>
          </div>
          <div class="top-actions">
            <label class="currency-picker">
              Currency
              <select id="currency-select">${currencyOptions}</select>
            </label>
          </div>
        </header>

        ${
          state.error
            ? `<div class="error" role="alert" aria-live="assertive">${escapeHtml(
                state.error
              )}</div>`
            : ""
        }
        ${renderAuthNotices()}
        ${tabContent}
      </section>
    </main>
    ${renderInspectModalOverlay()}
  `;

  ensureAppEventDelegation();

  if (
    state.activeTab === "market" &&
    !state.marketTab.inventoryValue &&
    !state.marketTab.loading &&
    !state.marketTab.autoLoaded
  ) {
    runUiTask(() => refreshMarketInventoryValue());
  }

  animateMetricCounters();
}

function render() {
  if (state.sessionBooting) {
    renderSessionBoot();
    return;
  }

  if (state.publicPage.steamId64) {
    renderPublicPortfolioPage();
    return;
  }

  if (!state.authenticated) {
    renderPublicHome();
    return;
  }

  renderApp();
}

function hydrateAppNoticesFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const linkedSteam = params.get("linkedSteam") === "1";
  const merged = params.get("merged") === "1";
  const steamOnboarding = params.get("steamOnboarding") === "1";

  if (!linkedSteam && !steamOnboarding) return;

  if (linkedSteam) {
    state.accountNotice = merged
      ? "Steam account linked. Existing Steam-only profile was merged into this account."
      : "Steam account linked successfully.";
  }

  if (steamOnboarding) {
    state.activeTab = "dashboard";
    state.steamOnboardingPending = true;
    state.accountNotice = "Steam connected successfully. You're connected, click sync now.";
  }

  window.history.replaceState({}, "", "/");
}

async function bootstrapSession() {
  state.publicPage.steamId64 = getPublicSteamIdFromPath();
  hydrateAppNoticesFromUrl();
  render();
  if (state.publicPage.steamId64) {
    await Promise.all([
      loadPublicPortfolio({ silent: true }),
      refreshAuthProfile()
    ]);
  } else {
    await refreshPortfolio({ silent: true });
  }
  state.sessionBooting = false;
  render();
}

bootstrapSession().catch(() => {
  state.sessionBooting = false;
  render();
});

import "./style.css";
import "./responsive.css";
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
import { renderAvatarMenu } from "./components/avatarMenu";
import { renderInspectModal } from "./components/inspectModal";
import { renderMobileDrawer, renderMobileNav } from "./components/mobileNav";
import { renderSection } from "./components/uiPrimitives";
import {
  renderDrawer,
  renderKPIBar,
  renderPanel,
  renderStatGrid,
  renderStatTile
} from "./components/dashboardPrimitives";
const app = document.querySelector("#app");
const SUPPORTED_CURRENCIES = ["USD", "EUR", "GBP", "UAH", "PLN", "CZK"];
const CURRENCY_STORAGE_KEY = "cs2sa:selected_currency";
const PRICING_MODE_STORAGE_KEY = "cs2sa:pricing_mode";
const DASHBOARD_DETAILS_STORAGE_KEY = "cs2sa:dashboard_details_open";
const PRICING_MODE_LABELS = {
  steam: "Steam Price",
  best_sell_net: "Best Sell Net",
  lowest_buy: "Lowest Buy"
};
const SEARCH_RENDER_DEBOUNCE_MS = 120;
const TOAST_MAX_VISIBLE = 4;
const TOAST_DEFAULT_TIMEOUT_MS = 5000;
const HISTORY_RANGE_OPTIONS = [7, 30, 90, 180];

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

function readDashboardDetailsPreference() {
  try {
    return localStorage.getItem(DASHBOARD_DETAILS_STORAGE_KEY) === "1";
  } catch (_err) {
    return false;
  }
}

function persistDashboardDetailsPreference(nextOpen) {
  try {
    localStorage.setItem(DASHBOARD_DETAILS_STORAGE_KEY, nextOpen ? "1" : "0");
  } catch (_err) {
    // Ignore storage write errors.
  }
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

function createMarketOpportunitiesState() {
  return {
    loading: false,
    loaded: false,
    error: "",
    generatedAt: null,
    summary: null,
    items: [],
    filters: {
      minProfit: "0.5",
      minSpread: "5",
      minScore: "70",
      market: "all",
      liquidityMin: "0",
      showRisky: "0",
      sortBy: "score",
      limit: "250"
    }
  };
}

function formatCountdownLabel(totalSeconds) {
  const seconds = Math.max(Math.ceil(Number(totalSeconds || 0)), 0);
  if (seconds <= 0) return "0s";
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  if (minutes <= 0) return `${rest}s`;
  return `${minutes}m ${rest}s`;
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
    focusPending: false,
    loading: false,
    error: "",
    steamItemId: "",
    skin: null,
    marketInsight: null,
    exitWhatIf: createExitWhatIfState()
  },
  tabSwitch: {
    loading: false,
    target: ""
  },
  mobileDrawer: {
    open: false,
    focusPending: false
  },
  portfolioControls: {
    open: false,
    focusPending: false
  },
  avatarMenu: {
    open: false
  },
  headerTabMenu: {
    open: false
  },
  tooltip: {
    openId: ""
  },
  globalSearch: "",
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
  syncRateLimitedUntil: 0,
  syncSummary: null,
  currency: normalizeCurrencyCode(localStorage.getItem(CURRENCY_STORAGE_KEY) || "USD"),
  pricingMode: normalizePricingMode(
    localStorage.getItem(PRICING_MODE_STORAGE_KEY) || "lowest_buy"
  ),
  dashboardUi: {
    detailsExpanded: readDashboardDetailsPreference()
  },
  compareDrawer: {
    open: false,
    focusPending: false,
    loading: false,
    error: "",
    skinId: 0,
    marketHashName: "",
    payload: null
  },
  compareRefreshing: false,
  txSubmitting: false,
  txForm: {
    skinId: "",
    type: "buy",
    quantity: "1",
    unitPrice: "0",
    commissionPercent: "13"
  },
  txEditModal: {
    open: false,
    id: null,
    skinId: "",
    type: "buy",
    quantity: "1",
    unitPrice: "0",
    commissionPercent: "13",
    executedAt: "",
    submitting: false
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
    insight: null,
    opportunities: createMarketOpportunitiesState()
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
  },
  authNotice: {
    emailConfirmToastShown: false
  },
  toasts: []
};

const holdingsValueMemory = new Map();
const metricCounterMemory = new Map();
let delegatedAppEventsBound = false;
let tabSwitchTicket = 0;
let mobileDrawerLastFocusedElement = null;
let portfolioControlsLastTriggerElement = null;
let avatarMenuLastFocusedElement = null;
let inspectModalLastTriggerElement = null;
let compareDrawerLastTriggerElement = null;
let compareDrawerRequestTicket = 0;
let txEditModalLastTriggerElement = null;
let bodyScrollLockY = 0;
let bodyScrollLocked = false;
let toastSequence = 0;
const toastTimers = new Map();
let dashboardStickySyncBound = false;
let dashboardStickyRafId = 0;
let historyChartCache = {
  key: "",
  markup: ""
};
const APP_TABS = [
  { id: "dashboard", label: "Dashboard", hint: "Performance" },
  { id: "portfolio", label: "Portfolio", hint: "Holdings" },
  { id: "alerts", label: "Alerts", hint: "Triggers" },
  { id: "trades", label: "Transactions", hint: "Buys/Sells" },
  { id: "social", label: "Watchlist", hint: "Community" },
  { id: "market", label: "Market", hint: "Pricing" },
  { id: "team", label: "Team", hint: "Creator Ops" },
  { id: "settings", label: "Settings", hint: "Account" }
];
const HEADER_PRIMARY_TAB_IDS = new Set(["dashboard", "portfolio"]);
const HEADER_PRIMARY_TABS = APP_TABS.filter((tab) => HEADER_PRIMARY_TAB_IDS.has(tab.id));
const HEADER_MORE_TABS = APP_TABS.filter((tab) => !HEADER_PRIMARY_TAB_IDS.has(tab.id));
const PORTFOLIO_CARD_CACHE_MAX = 800;
const portfolioCardMarkupCache = {
  desktop: new Map(),
  mobile: new Map()
};
const holdingSecondaryDataCache = new Map();
const holdingSecondaryRequestCache = new Map();
const compareDrawerInsightsCache = new WeakMap();

function resetPortfolioCardMarkupCache() {
  portfolioCardMarkupCache.desktop.clear();
  portfolioCardMarkupCache.mobile.clear();
}

function trimPortfolioCardCache(cacheMap) {
  if (!(cacheMap instanceof Map)) return;
  if (cacheMap.size <= PORTFOLIO_CARD_CACHE_MAX) return;
  cacheMap.clear();
}

function normalizeSecondaryText(value) {
  if (value == null) return "-";
  const safe = String(value).trim();
  return safe ? safe : "-";
}

function normalizeSecondaryFloat(value) {
  if (value == null || value === "") return "-";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return formatNumber(parsed, 5);
}

function normalizeSecondaryVolume(value) {
  if (value == null || value === "") return "-";
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return "-";
  return formatNumber(Math.round(parsed), 0);
}

function normalizeSecondaryDate(value) {
  if (!value) return "-";
  const safe = String(value).trim();
  if (!safe) return "-";
  const parsed = new Date(safe);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }
  return safe.slice(0, 10) || "-";
}

function firstDefined(values = []) {
  for (const value of values) {
    if (value == null) continue;
    if (typeof value === "string" && !value.trim()) continue;
    return value;
  }
  return null;
}

function getHoldingSecondarySnapshot(item = {}) {
  const skinId = Number(item.skinId || 0);
  const cached = Number.isInteger(skinId) && skinId > 0 ? holdingSecondaryDataCache.get(skinId) : null;
  if (cached) {
    return {
      status: cached.status || "ready",
      floatValue: normalizeSecondaryText(cached.floatValue),
      pattern: normalizeSecondaryText(cached.pattern),
      lastSaleDate: normalizeSecondaryText(cached.lastSaleDate),
      marketVolume: normalizeSecondaryText(cached.marketVolume),
      listingUrl: normalizeSecondaryText(cached.listingUrl) === "-" ? "" : String(cached.listingUrl || ""),
      error: cached.error || ""
    };
  }

  return {
    status: "idle",
    floatValue: normalizeSecondaryFloat(
      firstDefined([
        item.floatValue,
        item.float_value,
        item.float,
        item.wearFloat,
        item.wear_float,
        item.paintWear,
        item.paint_wear
      ])
    ),
    pattern: normalizeSecondaryText(
      firstDefined([item.pattern, item.patternId, item.paintSeed, item.paint_seed, item.seed])
    ),
    lastSaleDate: normalizeSecondaryDate(firstDefined([item.currentPriceRecordedAt, item.updatedAt])),
    marketVolume: normalizeSecondaryVolume(
      firstDefined([
        item.marketVolume24h,
        item.marketVolume7d,
        item?.marketInsight?.sellSuggestion?.volume24h
      ])
    ),
    listingUrl: resolveMarketListingUrl(item),
    error: ""
  };
}

function getHoldingSecondaryCacheToken(rawSkinId) {
  const skinId = Number(rawSkinId || 0);
  if (!Number.isInteger(skinId) || skinId <= 0) return "secondary-idle";
  const snapshot = holdingSecondaryDataCache.get(skinId);
  if (!snapshot) return "secondary-idle";
  return [
    String(snapshot.status || "ready"),
    String(snapshot.floatValue || "-"),
    String(snapshot.pattern || "-"),
    String(snapshot.lastSaleDate || "-"),
    String(snapshot.marketVolume || "-"),
    String(snapshot.listingUrl || "")
  ].join("~");
}

async function hydrateHoldingSecondaryData(rawSkinId, rawSteamItemId = null) {
  const skinId = Number(rawSkinId || 0);
  if (!Number.isInteger(skinId) || skinId <= 0) return;

  const existingRequest = holdingSecondaryRequestCache.get(skinId);
  if (existingRequest) {
    await existingRequest;
    return;
  }

  const holding = getHoldingBySkinId(skinId);
  if (!holding) return;

  const current = getHoldingSecondarySnapshot(holding);
  holdingSecondaryDataCache.set(skinId, {
    ...current,
    status: "loading",
    error: ""
  });
  render();

  const request = (async () => {
    const steamItemId = String(rawSteamItemId || holding.primarySteamItemId || "").trim();
    const [sellSuggestion, skinDetails] = await Promise.all([
      api(withCurrency(`/market/items/${skinId}/sell-suggestion`)).catch(() => null),
      /^\d+$/.test(steamItemId)
        ? api(withCurrency(`/skins/by-steam-item/${encodeURIComponent(steamItemId)}`)).catch(() => null)
        : api(withCurrency(`/skins/${skinId}`)).catch(() => null)
    ]);

    const floatValue = normalizeSecondaryFloat(
      firstDefined([
        holding.floatValue,
        holding.float_value,
        holding.float,
        holding.wearFloat,
        holding.wear_float,
        holding.paintWear,
        holding.paint_wear,
        skinDetails?.floatValue,
        skinDetails?.float_value,
        skinDetails?.float,
        skinDetails?.wearFloat,
        skinDetails?.wear_float,
        skinDetails?.paintWear,
        skinDetails?.paint_wear
      ])
    );
    const pattern = normalizeSecondaryText(
      firstDefined([
        holding.pattern,
        holding.patternId,
        holding.paintSeed,
        holding.paint_seed,
        holding.seed,
        skinDetails?.pattern,
        skinDetails?.patternId,
        skinDetails?.paintSeed,
        skinDetails?.paint_seed,
        skinDetails?.seed
      ])
    );
    const lastSaleDate = normalizeSecondaryDate(
      firstDefined([
        holding.currentPriceRecordedAt,
        skinDetails?.latestPrice?.recorded_at,
        skinDetails?.latestPrice?.recordedAt,
        sellSuggestion?.snapshotCapturedAt,
        sellSuggestion?.snapshot_captured_at,
        holding.updatedAt
      ])
    );
    const marketVolume = normalizeSecondaryVolume(
      firstDefined([
        sellSuggestion?.volume24h,
        sellSuggestion?.volume_24h,
        holding.marketVolume24h,
        holding.marketVolume7d,
        holding?.marketInsight?.sellSuggestion?.volume24h
      ])
    );

    holdingSecondaryDataCache.set(skinId, {
      status: "ready",
      floatValue,
      pattern,
      lastSaleDate,
      marketVolume,
      listingUrl: resolveMarketListingUrl(holding),
      error: ""
    });
  })()
    .catch((err) => {
      const fallback = getHoldingSecondarySnapshot(holding);
      holdingSecondaryDataCache.set(skinId, {
        ...fallback,
        status: "error",
        error: err?.message || "Failed to refresh secondary data."
      });
    })
    .finally(() => {
      holdingSecondaryRequestCache.delete(skinId);
      render();
    });

  holdingSecondaryRequestCache.set(skinId, request);
  await request;
}

function getPortfolioTransactionsCacheToken() {
  const txRows = Array.isArray(state.transactions) ? state.transactions : [];
  if (!txRows.length) return "0";
  const first = txRows[0];
  const last = txRows[txRows.length - 1];
  return `${txRows.length}|${first?.id || ""}|${first?.executed_at || ""}|${last?.id || ""}|${
    last?.executed_at || ""
  }`;
}

function buildPortfolioCardCacheKey(item, variant = "desktop") {
  const liquidityScoreRaw = Number(
    item?.managementClue?.metrics?.liquidityScore ??
      item?.marketInsight?.liquidity?.score ??
      item?.marketComparison?.liquidityScore ??
      0
  );
  const liquidityScore = Number.isFinite(liquidityScoreRaw)
    ? Math.max(Math.min(Math.round(liquidityScoreRaw), 100), 0)
    : 0;
  const clue = item?.managementClue || {};
  const oneDay = Number(item?.oneDayChangePercent || 0);
  const sevenDay = Number(item?.sevenDayChangePercent || 0);

  return [
    variant,
    state.currency,
    state.pricingMode,
    getPortfolioTransactionsCacheToken(),
    Number(item?.skinId || 0),
    String(item?.marketHashName || ""),
    String(item?.exterior || item?.condition || item?.wearName || ""),
    Number(item?.quantity || 0),
    Number(item?.currentPrice || 0).toFixed(4),
    Number(item?.lineValue || 0).toFixed(4),
    Number.isFinite(oneDay) ? oneDay.toFixed(4) : "0.0000",
    Number.isFinite(sevenDay) ? sevenDay.toFixed(4) : "0.0000",
    String(item?.primarySteamItemId || ""),
    String(clue?.action || ""),
    Math.round(Number(clue?.confidence || 0)),
    liquidityScore,
    getHoldingSecondaryCacheToken(Number(item?.skinId || 0))
  ].join("|");
}

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
  notify("error", msg);
}

function clearError() {
  if (!state.error) return;
  state.error = "";
}

function normalizeToastType(type) {
  const safe = String(type || "info").trim().toLowerCase();
  if (safe === "success" || safe === "warning" || safe === "error") return safe;
  return "info";
}

function dismissToast(rawId) {
  const id = Number(rawId);
  if (!Number.isInteger(id)) return;

  if (toastTimers.has(id)) {
    clearTimeout(toastTimers.get(id));
    toastTimers.delete(id);
  }

  const before = state.toasts.length;
  state.toasts = state.toasts.filter((toast) => Number(toast.id) !== id);
  if (state.toasts.length !== before) {
    renderToastHost();
  }
}

function toggleToastExpanded(rawId) {
  const id = Number(rawId);
  if (!Number.isInteger(id)) return;
  let changed = false;
  state.toasts = state.toasts.map((toast) => {
    if (Number(toast.id) !== id || !toast.details) return toast;
    changed = true;
    return {
      ...toast,
      expanded: !toast.expanded
    };
  });
  if (changed) {
    renderToastHost();
  }
}

function notify(type, message, options = {}) {
  const safeMessage = String(message || "").trim();
  if (!safeMessage) return null;

  const id = ++toastSequence;
  const timeoutMs = Math.max(Number(options.timeoutMs || TOAST_DEFAULT_TIMEOUT_MS), 1200);
  const pinned = Boolean(options.pinned);
  const details = String(options.details || "").trim();
  const toast = {
    id,
    type: normalizeToastType(type),
    message: safeMessage,
    details,
    expanded: false,
    pinned
  };

  state.toasts = [...state.toasts, toast].slice(-TOAST_MAX_VISIBLE);
  const activeIds = new Set(state.toasts.map((row) => Number(row.id)));
  for (const [timerId, timer] of toastTimers.entries()) {
    if (!activeIds.has(Number(timerId))) {
      clearTimeout(timer);
      toastTimers.delete(timerId);
    }
  }

  if (!pinned) {
    const timer = setTimeout(() => {
      dismissToast(id);
    }, timeoutMs);
    toastTimers.set(id, timer);
  }

  renderToastHost();
  return id;
}

if (typeof window !== "undefined") {
  window.notify = notify;
}

function ensureToastHost() {
  let host = document.querySelector("#toast-host");
  if (host) return host;

  host = document.createElement("div");
  host.id = "toast-host";
  host.className = "toast-viewport";
  host.setAttribute("aria-live", "polite");
  host.setAttribute("aria-relevant", "additions");
  document.body.appendChild(host);
  return host;
}

function renderToastHost() {
  const host = ensureToastHost();
  const rows = (Array.isArray(state.toasts) ? state.toasts : [])
    .map((toast) => {
      const role = toast.type === "error" || toast.type === "warning" ? "alert" : "status";
      const hasDetails = Boolean(toast.details);
      return `
        <article class="toast ${escapeHtml(toast.type)}" role="${role}">
          <div class="toast-body">
            <span class="toast-tag">${escapeHtml(toTitle(toast.type))}</span>
            <p>${escapeHtml(toast.message)}</p>
            ${
              hasDetails
                ? `
              <button
                type="button"
                class="toast-expand"
                data-toast-toggle="${Number(toast.id)}"
                aria-expanded="${toast.expanded ? "true" : "false"}"
              >
                ${toast.expanded ? "Hide details" : "Show details"}
              </button>
            `
                : ""
            }
            ${
              hasDetails && toast.expanded
                ? `<pre class="toast-details">${escapeHtml(toast.details)}</pre>`
                : ""
            }
          </div>
          <button
            type="button"
            class="toast-close"
            aria-label="Dismiss notification"
            data-toast-dismiss="${Number(toast.id)}"
          >
            ×
          </button>
        </article>
      `;
    })
    .join("");

  host.innerHTML = rows;
}

function flushAuthNotices() {
  if (!state.authenticated) {
    state.authNotice.emailConfirmToastShown = false;
    return;
  }

  const accountNotice = String(state.accountNotice || "").trim();
  if (accountNotice) {
    notify("info", accountNotice);
    state.accountNotice = "";
  }

  const emailUnconfirmed = state.authProfile?.emailConfirmed === false;
  if (emailUnconfirmed && !state.authNotice.emailConfirmToastShown) {
    notify(
      "warning",
      `Email for ${state.authProfile?.email || "your account"} is not confirmed yet.`,
      { pinned: true, timeoutMs: 12000 }
    );
    state.authNotice.emailConfirmToastShown = true;
  } else if (!emailUnconfirmed) {
    state.authNotice.emailConfirmToastShown = false;
  }
}

function getHeaderEmailLabel() {
  const email = String(state.authProfile?.email || "").trim();
  if (!email) return "Signed in";
  if (email.length <= 32) return email;
  return `${email.slice(0, 29)}...`;
}

function isGlobalUiBlocked() {
  return Boolean(state.syncingInventory);
}

function syncBodyUiLocks() {
  document.body.classList.toggle("mobile-drawer-open", Boolean(state.mobileDrawer.open));
  document.body.classList.toggle("portfolio-controls-open", Boolean(state.portfolioControls.open));
  document.body.classList.toggle("inspect-modal-open", Boolean(state.inspectModal.open));
  document.body.classList.toggle("compare-drawer-open", Boolean(state.compareDrawer.open));
  document.body.classList.toggle("tx-edit-modal-open", Boolean(state.txEditModal.open));
  document.body.classList.toggle("ui-sync-blocked", isGlobalUiBlocked());
  syncBodyScrollLock();
}

function hasBlockingOverlayOpen() {
  return Boolean(
      state.mobileDrawer.open ||
      state.portfolioControls.open ||
      state.inspectModal.open ||
      state.compareDrawer.open ||
      state.txEditModal.open ||
      isGlobalUiBlocked()
  );
}

function syncBodyScrollLock() {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  const shouldLock = hasBlockingOverlayOpen();
  if (shouldLock && !bodyScrollLocked) {
    bodyScrollLockY = Math.max(Number(window.scrollY || 0), 0);
    document.body.style.position = "fixed";
    document.body.style.top = `-${bodyScrollLockY}px`;
    document.body.style.left = "0";
    document.body.style.right = "0";
    document.body.style.width = "100%";
    bodyScrollLocked = true;
    return;
  }

  if (!shouldLock && bodyScrollLocked) {
    document.body.style.position = "";
    document.body.style.top = "";
    document.body.style.left = "";
    document.body.style.right = "";
    document.body.style.width = "";
    window.scrollTo(0, bodyScrollLockY);
    bodyScrollLocked = false;
  }
}

function openMobileDrawer(triggerElement = null) {
  if (state.mobileDrawer.open) return;
  if (state.portfolioControls.open) {
    closePortfolioControlsDrawer({ restoreFocus: false });
  }
  state.mobileDrawer.open = true;
  state.mobileDrawer.focusPending = true;
  const fallbackFocus = document.activeElement;
  mobileDrawerLastFocusedElement =
    triggerElement instanceof HTMLElement
      ? triggerElement
      : fallbackFocus instanceof HTMLElement
        ? fallbackFocus
        : null;
  render();
}

function closeMobileDrawer(options = {}) {
  const { restoreFocus = true } = options;
  if (!state.mobileDrawer.open) return;

  state.mobileDrawer.open = false;
  state.mobileDrawer.focusPending = false;
  render();

  if (restoreFocus && mobileDrawerLastFocusedElement) {
    requestAnimationFrame(() => {
      mobileDrawerLastFocusedElement?.focus?.();
    });
  }
}

function focusMobileDrawerIfNeeded() {
  if (!state.mobileDrawer.open || !state.mobileDrawer.focusPending) return;

  state.mobileDrawer.focusPending = false;
  const drawerPanel = document.querySelector("[data-mobile-drawer-panel]");
  if (!drawerPanel) return;

  const preferredTarget =
    drawerPanel.querySelector("[data-mobile-drawer-close]") ||
    drawerPanel.querySelector(".mobile-drawer-tab.active") ||
    drawerPanel.querySelector(".mobile-drawer-tab");
  if (preferredTarget instanceof HTMLElement) {
    preferredTarget.focus();
    return;
  }
  drawerPanel.focus();
}

function trapMobileDrawerFocus(event) {
  if (!state.mobileDrawer.open || event.key !== "Tab") return;

  const drawerPanel = document.querySelector("[data-mobile-drawer-panel]");
  if (!drawerPanel) return;

  const focusable = Array.from(
    drawerPanel.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )
  ).filter((el) => el instanceof HTMLElement);

  if (!focusable.length) {
    event.preventDefault();
    drawerPanel.focus();
    return;
  }

  const active = document.activeElement;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];

  if (event.shiftKey && (active === first || !drawerPanel.contains(active))) {
    event.preventDefault();
    last.focus();
    return;
  }

  if (!event.shiftKey && (active === last || !drawerPanel.contains(active))) {
    event.preventDefault();
    first.focus();
  }
}

function openPortfolioControlsDrawer(triggerElement = null) {
  if (state.portfolioControls.open) return;
  if (state.mobileDrawer.open) {
    closeMobileDrawer({ restoreFocus: false });
  }
  state.portfolioControls.open = true;
  state.portfolioControls.focusPending = true;
  const fallbackFocus = document.activeElement;
  portfolioControlsLastTriggerElement =
    triggerElement instanceof HTMLElement
      ? triggerElement
      : fallbackFocus instanceof HTMLElement
        ? fallbackFocus
        : null;
  render();
}

function closePortfolioControlsDrawer(options = {}) {
  const { restoreFocus = true } = options;
  if (!state.portfolioControls.open) return;
  state.portfolioControls.open = false;
  state.portfolioControls.focusPending = false;
  render();

  if (restoreFocus && portfolioControlsLastTriggerElement) {
    requestAnimationFrame(() => {
      portfolioControlsLastTriggerElement?.focus?.();
    });
  }
  portfolioControlsLastTriggerElement = null;
}

function focusPortfolioControlsIfNeeded() {
  if (!state.portfolioControls.open || !state.portfolioControls.focusPending) return;
  state.portfolioControls.focusPending = false;

  const panel = document.querySelector("[data-portfolio-controls-panel]");
  if (!panel) return;
  const preferredTarget =
    panel.querySelector("[data-portfolio-controls-close]") ||
    panel.querySelector("input, select, button:not([disabled])");
  if (preferredTarget instanceof HTMLElement) {
    preferredTarget.focus();
    return;
  }
  panel.focus();
}

function trapPortfolioControlsFocus(event) {
  if (!state.portfolioControls.open || event.key !== "Tab") return;
  const panel = document.querySelector("[data-portfolio-controls-panel]");
  if (!panel) return;

  const focusable = Array.from(
    panel.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )
  ).filter((el) => el instanceof HTMLElement);

  if (!focusable.length) {
    event.preventDefault();
    panel.focus();
    return;
  }

  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  const active = document.activeElement;

  if (event.shiftKey && (active === first || !panel.contains(active))) {
    event.preventDefault();
    last.focus();
    return;
  }

  if (!event.shiftKey && (active === last || !panel.contains(active))) {
    event.preventDefault();
    first.focus();
  }
}

function focusInspectModalIfNeeded() {
  if (!state.inspectModal.open || !state.inspectModal.focusPending) return;
  const modal = document.querySelector("[data-inspect-modal-dialog]");
  if (!modal) return;

  state.inspectModal.focusPending = false;
  const firstFocusable = modal.querySelector(
    'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
  );
  if (firstFocusable instanceof HTMLElement) {
    firstFocusable.focus();
    return;
  }
  if (modal instanceof HTMLElement) {
    modal.focus();
  }
}

function trapInspectModalFocus(event) {
  if (!state.inspectModal.open || event.key !== "Tab") return;

  const modal = document.querySelector("[data-inspect-modal-dialog]");
  if (!modal) return;

  const focusable = Array.from(
    modal.querySelectorAll(
      'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )
  ).filter((el) => !el.hasAttribute("disabled"));

  if (!focusable.length) {
    event.preventDefault();
    modal.focus();
    return;
  }

  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  const active = document.activeElement;

  if (event.shiftKey && (active === first || !modal.contains(active))) {
    event.preventDefault();
    last.focus();
    return;
  }

  if (!event.shiftKey && (active === last || !modal.contains(active))) {
    event.preventDefault();
    first.focus();
  }
}

function focusCompareDrawerIfNeeded() {
  if (!state.compareDrawer.open || !state.compareDrawer.focusPending) return;

  state.compareDrawer.focusPending = false;
  const panel = document.querySelector("[data-compare-drawer-panel]");
  if (!panel) return;

  const preferredTarget =
    panel.querySelector("[data-compare-drawer-close]") ||
    panel.querySelector("button:not([disabled])") ||
    panel.querySelector("a[href]");
  if (preferredTarget instanceof HTMLElement) {
    preferredTarget.focus();
    return;
  }

  if (panel instanceof HTMLElement) {
    panel.focus();
  }
}

function trapCompareDrawerFocus(event) {
  if (!state.compareDrawer.open || event.key !== "Tab") return;
  const panel = document.querySelector("[data-compare-drawer-panel]");
  if (!panel) return;

  const focusable = Array.from(
    panel.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )
  ).filter((el) => el instanceof HTMLElement);

  if (!focusable.length) {
    event.preventDefault();
    panel.focus();
    return;
  }

  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  const active = document.activeElement;

  if (event.shiftKey && (active === first || !panel.contains(active))) {
    event.preventDefault();
    last.focus();
    return;
  }

  if (!event.shiftKey && (active === last || !panel.contains(active))) {
    event.preventDefault();
    first.focus();
  }
}

function syncDashboardKpiPinnedClass() {
  if (!app || state.activeTab !== "dashboard") return;
  const kpiBar = app.querySelector("[data-dashboard-kpi]");
  const heroPanel = app.querySelector("#dashboard-hero-panel") || app.querySelector("[data-dashboard-hero]");
  if (!kpiBar || !heroPanel) return;

  const topRaw = window.getComputedStyle(kpiBar).getPropertyValue("top");
  const stickyTop = Number.parseFloat(topRaw) || 0;
  const heroBottom = heroPanel.getBoundingClientRect().bottom;
  const kpiTop = kpiBar.getBoundingClientRect().top;
  const hasCrossedHero = heroBottom <= stickyTop + 1;
  const hasReachedStickyLine = kpiTop <= stickyTop + 1 || kpiBar.classList.contains("is-pinned");
  const isPinned = hasCrossedHero && hasReachedStickyLine;
  const anchor = kpiBar.closest(".dashboard-kpi-anchor");
  if (anchor instanceof HTMLElement) {
    anchor.style.minHeight = `${kpiBar.offsetHeight}px`;
  }

  kpiBar.classList.toggle("is-pinned", isPinned);
}

function scheduleDashboardKpiPinnedSync() {
  if (dashboardStickyRafId) return;
  dashboardStickyRafId = requestAnimationFrame(() => {
    dashboardStickyRafId = 0;
    syncDashboardKpiPinnedClass();
  });
}

function ensureDashboardStickySync() {
  if (dashboardStickySyncBound || typeof window === "undefined") return;
  const listener = () => {
    if (state.activeTab !== "dashboard") return;
    scheduleDashboardKpiPinnedSync();
  };
  window.addEventListener("scroll", listener, { passive: true });
  window.addEventListener("resize", listener, { passive: true });
  dashboardStickySyncBound = true;
}

function trapTxEditModalFocus(event) {
  if (!state.txEditModal.open || event.key !== "Tab") return;
  const dialog = document.querySelector(".tx-edit-dialog");
  if (!dialog) return;

  const focusable = Array.from(
    dialog.querySelectorAll(
      'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )
  ).filter((el) => !el.hasAttribute("disabled"));

  if (!focusable.length) {
    event.preventDefault();
    dialog.focus();
    return;
  }

  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  const active = document.activeElement;

  if (event.shiftKey && (active === first || !dialog.contains(active))) {
    event.preventDefault();
    last.focus();
    return;
  }

  if (!event.shiftKey && (active === last || !dialog.contains(active))) {
    event.preventDefault();
    first.focus();
  }
}

function openAvatarMenu(triggerElement = null) {
  if (state.avatarMenu.open) return;
  state.avatarMenu.open = true;
  const active = document.activeElement;
  avatarMenuLastFocusedElement =
    triggerElement instanceof HTMLElement
      ? triggerElement
      : active instanceof HTMLElement
        ? active
        : null;
  render();
  requestAnimationFrame(() => {
    const first = document.querySelector(".avatar-menu-item");
    if (first instanceof HTMLElement) {
      first.focus();
    }
  });
}

function closeAvatarMenu(options = {}) {
  const { restoreFocus = true } = options;
  if (!state.avatarMenu.open) return;
  state.avatarMenu.open = false;
  render();
  if (restoreFocus && avatarMenuLastFocusedElement) {
    requestAnimationFrame(() => {
      avatarMenuLastFocusedElement?.focus?.();
    });
  }
}

function renderDesktopHeader(userEmailLabel, userEmailTitle) {
  const notificationCount = Number(state.alertEvents?.length || 0);
  const hasMoreTabs = HEADER_MORE_TABS.length > 0;
  const moreTabActive = HEADER_MORE_TABS.some((tab) => tab.id === state.activeTab);
  const syncCooldownSeconds = getSyncCooldownSecondsRemaining();
  const syncDisabled = state.syncingInventory || syncCooldownSeconds > 0;
  const syncTitle =
    syncCooldownSeconds > 0
      ? `Sync rate limited. Try again in ${formatCountdownLabel(syncCooldownSeconds)}.`
      : state.syncingInventory
        ? "Syncing inventory"
        : "Sync inventory";
  return `
    <header class="desktop-header" role="banner">
      <a href="#" class="desktop-brand" data-desktop-home aria-label="Go to dashboard">
        <span class="desktop-brand-dot" aria-hidden="true"></span>
        <span>CS2 Portfolio Analyzer</span>
      </a>
      <nav class="desktop-tab-nav" aria-label="Primary">
        ${HEADER_PRIMARY_TABS.map(
          (tab) => `
          <button
            type="button"
            class="ghost-btn tab-btn desktop-tab-btn ${state.activeTab === tab.id ? "active" : ""}"
            data-tab="${tab.id}"
            title="${escapeHtml(tab.hint)}"
          >
            ${escapeHtml(tab.label)}
          </button>
        `
        ).join("")}
        ${
          hasMoreTabs
            ? `
          <div class="desktop-tab-dropdown" data-header-tab-menu-root>
            <button
              type="button"
              class="ghost-btn tab-btn desktop-tab-btn desktop-tab-menu-toggle ${moreTabActive ? "active" : ""}"
              data-header-tab-menu-toggle
              aria-haspopup="true"
              aria-expanded="${state.headerTabMenu.open ? "true" : "false"}"
              aria-controls="desktop-tab-menu"
            >
              More
              <span class="desktop-tab-menu-caret" aria-hidden="true">&#x25BE;</span>
            </button>
            <div
              class="desktop-tab-menu ${state.headerTabMenu.open ? "open" : ""}"
              id="desktop-tab-menu"
              role="menu"
              aria-label="More sections"
            >
              ${HEADER_MORE_TABS.map(
                (tab) => `
                <button
                  type="button"
                  class="ghost-btn tab-btn desktop-tab-btn desktop-tab-menu-item ${state.activeTab === tab.id ? "active" : ""}"
                  data-tab="${tab.id}"
                  title="${escapeHtml(tab.hint)}"
                  role="menuitem"
                >
                  ${escapeHtml(tab.label)}
                </button>
              `
              ).join("")}
            </div>
          </div>
        `
            : ""
        }
      </nav>
      <div class="desktop-header-actions">
        <label class="desktop-search">
          <input
            id="global-search"
            type="search"
            placeholder="Quick find skins..."
            value="${escapeHtml(state.globalSearch)}"
            aria-label="Quick find skins"
          />
        </label>
        <button
          type="button"
          class="ghost-btn header-icon-btn"
          id="header-sync-btn"
          aria-label="Sync inventory"
          title="${escapeHtml(syncTitle)}"
          ${syncDisabled ? "disabled" : ""}
        >
          &#x21bb;
        </button>
        <button
          type="button"
          class="ghost-btn header-icon-btn"
          data-header-action="create-alert"
          aria-label="Create alert"
          title="Create alert"
        >
          &#x26A1;
        </button>
        <button
          type="button"
          class="ghost-btn header-icon-btn"
          data-header-action="add-transaction"
          aria-label="Add transaction"
          title="Add transaction"
        >
          +
        </button>
        ${renderAvatarMenu({
          open: state.avatarMenu.open,
          userLabel: userEmailLabel,
          userTitle: userEmailTitle,
          notificationCount,
          escapeHtml
        })}
      </div>
    </header>
  `;
}

function renderAppFooter() {
  return `
    <footer class="app-footer">
      <div class="app-footer-left">&copy; ${new Date().getFullYear()} CS2 Portfolio Analyzer</div>
      <nav class="app-footer-links" aria-label="Footer links">
        <a href="https://github.com/denisolid/codex_test" target="_blank" rel="noreferrer" class="ghost-link">About</a>
        <a href="https://github.com/denisolid/codex_test/tree/market-analysis/docs" target="_blank" rel="noreferrer" class="ghost-link">Docs</a>
        <a href="https://github.com/denisolid/codex_test#readme" target="_blank" rel="noreferrer" class="ghost-link">Privacy</a>
      </nav>
      <div class="app-footer-right">v1.0.0 &middot; build MVP</div>
    </footer>
  `;
}
function resolveMarketListingUrl(item = {}) {
  const perMarket = Array.isArray(item?.marketComparison?.perMarket)
    ? item.marketComparison.perMarket
    : [];
  const direct = perMarket.find((row) => typeof row?.url === "string" && row.url.trim());
  if (direct?.url) return String(direct.url);
  if (typeof item?.marketListingUrl === "string" && item.marketListingUrl.trim()) {
    return item.marketListingUrl;
  }
  return "";
}

function renderHoldingInfoTooltip(item = {}) {
  const skinId = Number(item.skinId || 0);
  if (!skinId) return "";

  const tooltipId = `holding-tip-${skinId}`;
  const isOpen = state.tooltip.openId === tooltipId;
  const secondary = getHoldingSecondarySnapshot(item);
  const listingUrl = secondary.listingUrl || resolveMarketListingUrl(item);

  return `
    <span class="tooltip-wrap" data-tooltip-wrap>
      <button
        type="button"
        class="tooltip-toggle"
        data-tooltip-toggle="${escapeHtml(tooltipId)}"
        data-tooltip-skin-id="${skinId}"
        data-tooltip-steam-item-id="${escapeHtml(item.primarySteamItemId || "")}"
        aria-label="Show secondary item details"
        aria-describedby="${escapeHtml(tooltipId)}"
        aria-expanded="${isOpen ? "true" : "false"}"
      >
        i
      </button>
      <span
        id="${escapeHtml(tooltipId)}"
        role="tooltip"
        class="tooltip-bubble ${isOpen ? "open" : ""}"
      >
        <strong>Secondary Data</strong>
        <small>Float: ${escapeHtml(secondary.floatValue)}</small>
        <small>Pattern: ${escapeHtml(secondary.pattern)}</small>
        <small>Last sale: ${escapeHtml(secondary.lastSaleDate)}</small>
        <small>24h volume: ${escapeHtml(secondary.marketVolume)}</small>
        ${
          secondary.status === "loading"
            ? '<small class="muted">Fetching latest secondary data...</small>'
            : ""
        }
        ${
          secondary.status === "error"
            ? '<small class="muted">Live fetch failed. Showing known values.</small>'
            : ""
        }
        ${
          listingUrl
            ? `<a href="${escapeHtml(listingUrl)}" target="_blank" rel="noreferrer">Open listing</a>`
            : ""
        }
      </span>
    </span>
  `;
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

function formatDateTime(isoValue) {
  if (!isoValue) return "-";
  const ts = new Date(isoValue);
  if (Number.isNaN(ts.getTime())) return "-";
  return ts.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
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
  const baseHeaders = {
    "Content-Type": "application/json",
    ...(options.headers || {})
  };
  const hasAuthHeader = (headers) =>
    Object.keys(headers || {}).some((key) => String(key).toLowerCase() === "authorization");
  const request = (headers) =>
    fetch(`${API_URL}${path}`, {
      ...options,
      credentials: "include",
      headers
    });

  let headers = withAuthHeaders(baseHeaders);
  let res = await request(headers);

  if (res.status === 401 && hasAuthHeader(headers)) {
    clearAuthToken();
    headers = { ...baseHeaders };
    res = await request(headers);
  }

  const payload = await res.json().catch(() => ({}));
  if (res.status === 401) {
    clearAuthToken();
    state.authenticated = false;
    state.authProfile = null;
  }
  if (!res.ok) {
    const err = new Error(payload.error || "Request failed");
    err.status = Number(res.status || 0);
    err.code = String(payload?.code || "").trim();
    const retryAfterRaw = res.headers.get("Retry-After");
    const retryAfterSeconds = Number(retryAfterRaw);
    if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
      err.retryAfterSeconds = retryAfterSeconds;
    }
    throw err;
  }

  state.authenticated = true;
  return payload;
}

function withCurrency(path) {
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}currency=${encodeURIComponent(state.currency)}`;
}

function getSyncCooldownSecondsRemaining() {
  const untilTs = Number(state.syncRateLimitedUntil || 0);
  if (!Number.isFinite(untilTs) || untilTs <= 0) return 0;
  return Math.max(Math.ceil((untilTs - Date.now()) / 1000), 0);
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
  state.marketTab.opportunities = createMarketOpportunitiesState();
  state.marketInsight = null;

  await refreshPortfolio();
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

function getHoldingBySkinId(rawSkinId) {
  const skinId = Number(rawSkinId || 0);
  if (!Number.isInteger(skinId) || skinId <= 0) return null;
  const holdings = getHoldingsList();
  return holdings.find((item) => Number(item.skinId || 0) === skinId) || null;
}

function buildCompareDrawerSnapshotFromHolding(holding, options = {}) {
  if (!holding) return null;
  const fallbackImage = isCaseLikeItem(holding) ? defaultCaseImage : defaultSkinImage;
  const imageUrl = getItemImageUrl(holding) || fallbackImage;
  const comparison = holding?.marketComparison || null;

  return {
    skinId: Number(holding.skinId || 0),
    marketHashName: String(holding.marketHashName || options.marketHashName || "Tracked Item"),
    condition: getHoldingConditionLabel(holding),
    quantity: Number(holding.quantity || 0),
    imageUrl,
    currency: holding.currency || state.portfolio?.currency || state.currency,
    currentPrice: Number(holding.currentPrice || 0),
    currentPriceSource: String(holding.selectedPricingSource || holding.currentPriceSource || "").trim(),
    lineValue: Number(holding.lineValue || 0),
    marketComparison: comparison,
    fees: options.fees || state.portfolio?.pricing?.fees || null,
    generatedAt: options.generatedAt || null
  };
}

function closeCompareDrawer(options = {}) {
  const { restoreFocus = true } = options;
  if (!state.compareDrawer.open) return;
  compareDrawerRequestTicket += 1;
  state.compareDrawer.open = false;
  state.compareDrawer.focusPending = false;
  state.compareDrawer.loading = false;
  state.compareDrawer.error = "";
  render();

  if (restoreFocus && compareDrawerLastTriggerElement) {
    requestAnimationFrame(() => {
      compareDrawerLastTriggerElement?.focus?.();
    });
  }
  compareDrawerLastTriggerElement = null;
}

function openCompareDrawerMarketTarget(rawUrl, rawSkinId = state.compareDrawer.skinId) {
  const url = String(rawUrl || "").trim();
  if (url) {
    try {
      const parsed = new URL(url, window.location.origin);
      if (parsed.protocol === "http:" || parsed.protocol === "https:") {
        window.open(parsed.toString(), "_blank", "noopener,noreferrer");
        return true;
      }
    } catch (_err) {
      // Ignore malformed URL and fallback to market tab.
    }
  }

  const skinId = Number(rawSkinId || state.compareDrawer.skinId || 0);
  if (!Number.isInteger(skinId) || skinId <= 0) {
    return false;
  }
  closeCompareDrawer({ restoreFocus: false });
  state.activeTab = "market";
  state.marketTab.skinId = String(skinId);
  render();
  runUiTask(() => analyzeMarketItemBySkinId(skinId));
  return true;
}

async function refreshCompareDrawerData(options = {}) {
  const {
    skinId: rawSkinId = state.compareDrawer.skinId,
    forceRefresh = false,
    notifyOnSuccess = false
  } = options;
  const skinId = Number(rawSkinId || 0);
  if (!Number.isInteger(skinId) || skinId <= 0) return;
  const holding = getHoldingBySkinId(skinId);
  if (!holding) {
    state.compareDrawer.error = "Item was not found in current holdings.";
    state.compareDrawer.loading = false;
    render();
    return;
  }

  const payloadItems = buildComparisonItemsPayload([holding]);
  if (!payloadItems.length) {
    state.compareDrawer.error = "Market comparison payload is unavailable for this item.";
    state.compareDrawer.loading = false;
    render();
    return;
  }

  const ticket = ++compareDrawerRequestTicket;
  state.compareDrawer.loading = true;
  state.compareDrawer.error = "";
  render();

  try {
    const comparisonPayload = await api("/market/compare", {
      method: "POST",
      body: JSON.stringify({
        items: payloadItems,
        pricingMode: state.pricingMode,
        forceRefresh: Boolean(forceRefresh),
        allowLiveFetch: true,
        currency: state.currency
      })
    });
    if (ticket !== compareDrawerRequestTicket) return;
    const comparisonRow =
      (Array.isArray(comparisonPayload?.items) ? comparisonPayload.items : []).find(
        (row) => Number(row?.skinId || 0) === skinId
      ) || null;
    if (!comparisonRow) {
      throw new Error("No market comparison data returned for this item.");
    }

    const updatedHolding = getHoldingBySkinId(skinId) || holding;
    if (updatedHolding) {
      updatedHolding.marketComparison = {
        perMarket: Array.isArray(comparisonRow.perMarket) ? comparisonRow.perMarket : [],
        bestBuy: comparisonRow.bestBuy || null,
        bestSellNet: comparisonRow.bestSellNet || null,
        arbitrage: comparisonRow.arbitrage || null
      };
    }

    const snapshot = buildCompareDrawerSnapshotFromHolding(updatedHolding || holding, {
      fees: comparisonPayload?.fees || state.portfolio?.pricing?.fees || null,
      generatedAt: comparisonPayload?.generatedAt || null
    });
    state.compareDrawer.payload = snapshot;
    state.compareDrawer.marketHashName = snapshot?.marketHashName || state.compareDrawer.marketHashName;
    if (notifyOnSuccess) {
      notify("success", "Comparison data refreshed.");
    }
  } catch (err) {
    if (ticket !== compareDrawerRequestTicket) return;
    state.compareDrawer.error = err.message || "Failed to load comparison data.";
    notify("error", state.compareDrawer.error, {
      details: String(err?.stack || "").trim()
    });
  } finally {
    if (ticket === compareDrawerRequestTicket) {
      state.compareDrawer.loading = false;
      render();
    }
  }
}

function openCompareDrawerBySkinId(rawSkinId, triggerElement = null) {
  const skinId = Number(rawSkinId || 0);
  if (!Number.isInteger(skinId) || skinId <= 0) return;
  const holding = getHoldingBySkinId(skinId);
  if (!holding) {
    setError("Unable to open comparison: item not found.");
    return;
  }

  if (triggerElement instanceof HTMLElement) {
    compareDrawerLastTriggerElement = triggerElement;
  }

  if (state.portfolioControls.open) {
    closePortfolioControlsDrawer({ restoreFocus: false });
  }

  state.compareDrawer.open = true;
  state.compareDrawer.focusPending = true;
  state.compareDrawer.loading = false;
  state.compareDrawer.error = "";
  state.compareDrawer.skinId = skinId;
  state.compareDrawer.payload = buildCompareDrawerSnapshotFromHolding(holding);
  state.compareDrawer.marketHashName = String(holding.marketHashName || `Skin #${skinId}`);
  render();

  const hasPerMarket = Boolean(
    state.compareDrawer.payload?.marketComparison?.perMarket &&
      Array.isArray(state.compareDrawer.payload.marketComparison.perMarket) &&
      state.compareDrawer.payload.marketComparison.perMarket.length
  );
  const hasCsfloatData = Boolean(
    state.compareDrawer.payload?.marketComparison?.perMarket &&
      Array.isArray(state.compareDrawer.payload.marketComparison.perMarket) &&
      state.compareDrawer.payload.marketComparison.perMarket.some((row) => {
        if (String(row?.source || "").trim().toLowerCase() !== "csfloat") return false;
        return Boolean(row?.available) && Number.isFinite(Number(row?.grossPrice));
      })
  );

  if (!hasPerMarket || !hasCsfloatData) {
    runUiTask(
      () => refreshCompareDrawerData({ skinId, forceRefresh: !hasCsfloatData })
    );
  }
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
  const ticket = ++tabSwitchTicket;
  const target = String(tab || "");
  const requiresLoad =
    target === "portfolio" ||
    (target === "team" &&
      String(state.authProfile?.planTier || "free").toLowerCase() === "team" &&
      !state.teamDashboard.loading &&
      !state.teamDashboard.payload);

  if (state.headerTabMenu.open) {
    state.headerTabMenu.open = false;
  }

  if (target !== "dashboard" && state.compareDrawer.open) {
    state.compareDrawer.open = false;
    state.compareDrawer.focusPending = false;
    state.compareDrawer.loading = false;
    state.compareDrawer.error = "";
    compareDrawerRequestTicket += 1;
  }

  if (target !== "portfolio" && state.portfolioControls.open) {
    state.portfolioControls.open = false;
    state.portfolioControls.focusPending = false;
    portfolioControlsLastTriggerElement = null;
  }

  state.activeTab = tab;
  state.tabSwitch.target = target;
  state.tabSwitch.loading = requiresLoad;
  render();

  try {
    if (target === "portfolio") {
      await refreshVisibleMarketComparisons();
    }

    if (
      target === "team" &&
      String(state.authProfile?.planTier || "free").toLowerCase() === "team" &&
      !state.teamDashboard.loading &&
      !state.teamDashboard.payload
    ) {
      await refreshTeamDashboard({ silent: true });
    }
  } finally {
    if (ticket === tabSwitchTicket) {
      state.tabSwitch.loading = false;
      state.tabSwitch.target = "";
      render();
    }
  }
}

function triggerInspectBySteamItemId(rawSteamItemId, triggerElement = null) {
  const steamItemId = String(rawSteamItemId || "").trim();
  if (!steamItemId) return;
  if (state.compareDrawer.open) {
    closeCompareDrawer({ restoreFocus: false });
  }
  if (state.portfolioControls.open) {
    closePortfolioControlsDrawer({ restoreFocus: false });
  }
  if (triggerElement instanceof HTMLElement) {
    inspectModalLastTriggerElement = triggerElement;
  }
  runUiTask(() => openInspectModalBySteamItemId(steamItemId));
}

function closeInspectModal() {
  if (!state.inspectModal.open) return;
  state.inspectModal.open = false;
  state.inspectModal.focusPending = false;
  state.inspectModal.loading = false;
  state.inspectModal.error = "";
  state.inspectModal.steamItemId = "";
  state.inspectModal.skin = null;
  state.inspectModal.marketInsight = null;
  state.inspectModal.exitWhatIf = createExitWhatIfState();
  render();

  if (inspectModalLastTriggerElement) {
    requestAnimationFrame(() => {
      inspectModalLastTriggerElement?.focus?.();
    });
  }
  inspectModalLastTriggerElement = null;
}

function onAppClick(event) {
  if (!app) return;
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  if (state.avatarMenu.open && !target.closest("[data-avatar-menu-root]")) {
    closeAvatarMenu({ restoreFocus: false });
  }

  if (state.headerTabMenu.open && !target.closest("[data-header-tab-menu-root]")) {
    state.headerTabMenu.open = false;
    render();
  }

  if (state.tooltip.openId && !target.closest("[data-tooltip-wrap]")) {
    state.tooltip.openId = "";
    render();
  }

  if (target.matches("[data-mobile-drawer-overlay]")) {
    event.preventDefault();
    closeMobileDrawer();
    return;
  }

  if (target.matches("[data-portfolio-controls-overlay]")) {
    event.preventDefault();
    closePortfolioControlsDrawer();
    return;
  }

  if (target.matches("[data-inspect-modal-overlay]")) {
    event.preventDefault();
    closeInspectModal();
    return;
  }

  if (target.matches("[data-compare-drawer-overlay]")) {
    event.preventDefault();
    closeCompareDrawer();
    return;
  }

  if (target.matches("[data-tx-edit-overlay]")) {
    event.preventDefault();
    closeTxEditModal();
    return;
  }

  const homeLink = target.closest("[data-desktop-home]");
  if (homeLink) {
    event.preventDefault();
    handleTabSwitch("dashboard").catch((err) => setError(err.message || "Request failed"));
    return;
  }

  const button = target.closest("button");

  if (button?.matches("[data-toast-dismiss]")) {
    event.preventDefault();
    dismissToast(button.getAttribute("data-toast-dismiss"));
    return;
  }

  if (button?.matches("[data-toast-toggle]")) {
    event.preventDefault();
    toggleToastExpanded(button.getAttribute("data-toast-toggle"));
    return;
  }

  if (button?.matches("[data-tooltip-toggle]")) {
    event.preventDefault();
    const id = String(button.getAttribute("data-tooltip-toggle") || "").trim();
    const nextOpen = state.tooltip.openId === id ? "" : id;
    state.tooltip.openId = nextOpen;
    render();
    if (nextOpen) {
      const skinId = Number(button.getAttribute("data-tooltip-skin-id") || 0);
      const steamItemId = String(button.getAttribute("data-tooltip-steam-item-id") || "").trim();
      if (Number.isInteger(skinId) && skinId > 0) {
        runUiTask(() => hydrateHoldingSecondaryData(skinId, steamItemId));
      }
    }
    return;
  }

  if (button?.matches("[data-mobile-drawer-open]")) {
    event.preventDefault();
    openMobileDrawer(button);
    return;
  }

  if (button?.matches("[data-header-tab-menu-toggle]")) {
    event.preventDefault();
    state.headerTabMenu.open = !state.headerTabMenu.open;
    if (state.headerTabMenu.open && state.avatarMenu.open) {
      state.avatarMenu.open = false;
    }
    render();
    return;
  }

  if (button?.matches("#avatar-menu-toggle")) {
    event.preventDefault();
    if (state.headerTabMenu.open) {
      state.headerTabMenu.open = false;
    }
    if (state.avatarMenu.open) {
      closeAvatarMenu();
    } else {
      openAvatarMenu(button);
    }
    return;
  }

  if (button?.matches("[data-avatar-action]")) {
    event.preventDefault();
    const action = String(button.getAttribute("data-avatar-action") || "").trim();
    closeAvatarMenu({ restoreFocus: false });
    if (action === "profile" || action === "settings" || action === "billing") {
      handleTabSwitch("settings").catch((err) => setError(err.message || "Request failed"));
      return;
    }
    if (action === "notifications") {
      handleTabSwitch("alerts").catch((err) => setError(err.message || "Request failed"));
      return;
    }
    if (action === "logout") {
      runUiTask(() => logout());
      return;
    }
  }

  if (button?.matches("[data-mobile-drawer-close]")) {
    event.preventDefault();
    closeMobileDrawer();
    return;
  }

  if (button?.matches("[data-portfolio-controls-open]")) {
    event.preventDefault();
    openPortfolioControlsDrawer(button);
    return;
  }

  if (button?.matches("[data-portfolio-controls-close]")) {
    event.preventDefault();
    closePortfolioControlsDrawer();
    return;
  }

  if (button?.matches("[data-inspect-modal-close]")) {
    event.preventDefault();
    closeInspectModal();
    return;
  }

  if (button?.matches("[data-compare-drawer-close]")) {
    event.preventDefault();
    closeCompareDrawer();
    return;
  }

  if (button?.matches("[data-tx-edit-close]")) {
    event.preventDefault();
    closeTxEditModal();
    return;
  }

  if (button?.matches("#logout-btn, #mobile-drawer-logout-btn")) {
    event.preventDefault();
    runUiTask(() => logout());
    return;
  }

  if (
    button?.matches("#refresh-btn, #mobile-nav-refresh-btn, #mobile-drawer-refresh-btn")
  ) {
    event.preventDefault();
    runUiTask(() => refreshPortfolio());
    return;
  }

  if (button?.matches("#header-sync-btn")) {
    event.preventDefault();
    runUiTask(() => syncInventory());
    return;
  }

  if (button?.matches("[data-header-action='create-alert']")) {
    event.preventDefault();
    handleTabSwitch("alerts").catch((err) => setError(err.message || "Request failed"));
    return;
  }

  if (button?.matches("[data-header-action='add-transaction']")) {
    event.preventDefault();
    handleTabSwitch("trades").catch((err) => setError(err.message || "Request failed"));
    return;
  }

  if (button?.matches(".tab-btn")) {
    event.preventDefault();
    const tab = button.getAttribute("data-tab");
    if (state.mobileDrawer.open) {
      closeMobileDrawer({ restoreFocus: false });
    }
    if (state.avatarMenu.open) {
      closeAvatarMenu({ restoreFocus: false });
    }
    if (state.txEditModal.open) {
      closeTxEditModal({ restoreFocus: false });
    }
    handleTabSwitch(tab).catch((err) => setError(err.message || "Request failed"));
    return;
  }

  if (button?.matches(".tab-jump-btn")) {
    event.preventDefault();
    const tab = button.getAttribute("data-tab-target");
    handleTabSwitch(tab).catch((err) => setError(err.message || "Request failed"));
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

  const dashboardArbitrageRow = target.closest(".dashboard-arb-row-clickable");
  if (dashboardArbitrageRow) {
    event.preventDefault();
    openCompareDrawerBySkinId(
      dashboardArbitrageRow.getAttribute("data-skin-id"),
      dashboardArbitrageRow
    );
    return;
  }

  const tile = target.closest(".portfolio-skin-card-clickable");
  if (tile) {
    event.preventDefault();
    triggerInspectBySteamItemId(tile.getAttribute("data-steam-item-id"), tile);
    return;
  }

  if (button?.matches(".inspect-skin-btn")) {
    event.preventDefault();
    triggerInspectBySteamItemId(button.getAttribute("data-steam-item-id"), button);
    return;
  }

  if (button?.matches(".top-mover-compare-btn")) {
    event.preventDefault();
    openCompareDrawerBySkinId(button.getAttribute("data-skin-id"), button);
    return;
  }

  if (button?.matches(".compare-market-btn")) {
    event.preventDefault();
    openCompareDrawerBySkinId(button.getAttribute("data-skin-id"), button);
    return;
  }

  if (button?.matches(".sell-suggestion-btn")) {
    event.preventDefault();
    const skinId = Number(button.getAttribute("data-skin-id") || 0);
    if (!Number.isInteger(skinId) || skinId <= 0) return;
    state.activeTab = "market";
    state.marketTab.skinId = String(skinId);
    render();
    runUiTask(() => analyzeMarketItemBySkinId(skinId));
    return;
  }

  if (button?.matches("[data-refresh-market-compare]")) {
    event.preventDefault();
    runUiTask(() => refreshVisibleMarketComparisons());
    return;
  }

  if (button?.matches("#compare-drawer-open-best-buy-btn")) {
    event.preventDefault();
    openCompareDrawerMarketTarget(
      button.getAttribute("data-market-url"),
      button.getAttribute("data-skin-id")
    );
    return;
  }

  if (button?.matches("#compare-drawer-open-best-sell-btn")) {
    event.preventDefault();
    openCompareDrawerMarketTarget(
      button.getAttribute("data-market-url"),
      button.getAttribute("data-skin-id")
    );
    return;
  }

  if (button?.matches("#compare-drawer-refresh-btn")) {
    event.preventDefault();
    runUiTask(() =>
      refreshCompareDrawerData({
        skinId: state.compareDrawer.skinId,
        forceRefresh: true,
        notifyOnSuccess: true
      })
    );
    return;
  }

  if (button?.matches("[data-dashboard-details-toggle]")) {
    event.preventDefault();
    state.dashboardUi.detailsExpanded = !state.dashboardUi.detailsExpanded;
    persistDashboardDetailsPreference(state.dashboardUi.detailsExpanded);
    render();
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

  if (button?.matches(".tx-edit-btn")) {
    event.preventDefault();
    openTxEditModal(button.getAttribute("data-tx-id"), button);
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
  trapMobileDrawerFocus(event);
  trapPortfolioControlsFocus(event);
  trapInspectModalFocus(event);
  trapCompareDrawerFocus(event);
  trapTxEditModalFocus(event);

  if (event.key === "Escape" && state.inspectModal.open) {
    event.preventDefault();
    closeInspectModal();
    return;
  }

  if (event.key === "Escape" && state.txEditModal.open) {
    event.preventDefault();
    closeTxEditModal();
    return;
  }

  if (event.key === "Escape" && state.compareDrawer.open) {
    event.preventDefault();
    closeCompareDrawer();
    return;
  }

  if (event.key === "Escape" && state.portfolioControls.open) {
    event.preventDefault();
    closePortfolioControlsDrawer();
    return;
  }

  if (event.key === "Escape" && state.headerTabMenu.open) {
    event.preventDefault();
    state.headerTabMenu.open = false;
    render();
    return;
  }

  if (event.key === "Escape" && state.avatarMenu.open) {
    event.preventDefault();
    closeAvatarMenu();
    return;
  }

  if (event.key === "Escape" && state.tooltip.openId) {
    event.preventDefault();
    state.tooltip.openId = "";
    render();
    return;
  }

  if (event.key === "Escape" && state.mobileDrawer.open) {
    event.preventDefault();
    closeMobileDrawer();
    return;
  }

  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  if (state.avatarMenu.open && target.matches(".avatar-menu-item")) {
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
      return;
    }
    event.preventDefault();
    const items = Array.from(document.querySelectorAll(".avatar-menu-item"));
    if (!items.length) return;
    const index = items.indexOf(target);
    if (index < 0) return;
    const nextIndex =
      event.key === "ArrowDown"
        ? (index + 1) % items.length
        : (index - 1 + items.length) % items.length;
    items[nextIndex]?.focus?.();
    return;
  }

  if (event.key !== "Enter" && event.key !== " ") return;

  const dashboardArbitrageRow = target.closest(".dashboard-arb-row-clickable");
  if (dashboardArbitrageRow) {
    event.preventDefault();
    openCompareDrawerBySkinId(
      dashboardArbitrageRow.getAttribute("data-skin-id"),
      dashboardArbitrageRow
    );
    return;
  }

  const tile = target.closest(".portfolio-skin-card-clickable");
  if (!tile) return;
  event.preventDefault();
  triggerInspectBySteamItemId(tile.getAttribute("data-steam-item-id"), tile);
}

function onAppInput(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  if (target.matches("#global-search")) {
    state.globalSearch = String(target.value || "");
    state.holdingsView.q = state.globalSearch;
    state.holdingsView.page = 1;
    if (state.activeTab !== "portfolio") {
      debouncedRender();
      return;
    }
    debouncedRender();
    return;
  }

  if (target.matches("#backtest-days")) {
    state.backtest.days = String(target.value || "");
    return;
  }

  if (target.matches("#holdings-search, #holdings-search-mobile, [data-holdings-search]")) {
    state.holdingsView.q = target.value;
    state.globalSearch = target.value;
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

  if (target.matches("#tx-quantity")) {
    state.txForm.quantity = String(target.value || "");
    syncTransactionPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-unit-price")) {
    state.txForm.unitPrice = String(target.value || "");
    syncTransactionPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-commission")) {
    state.txForm.commissionPercent = String(target.value || "");
    syncTransactionPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-edit-quantity")) {
    state.txEditModal.quantity = String(target.value || "");
    syncTransactionEditPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-edit-unit-price")) {
    state.txEditModal.unitPrice = String(target.value || "");
    syncTransactionEditPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-edit-commission")) {
    state.txEditModal.commissionPercent = String(target.value || "");
    syncTransactionEditPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-edit-executed-at")) {
    state.txEditModal.executedAt = String(target.value || "");
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

  if (
    target.matches(
      "#pricing-mode-select, #pricing-mode-select-mobile, #settings-pricing-mode, [data-pricing-mode-select]"
    )
  ) {
    const nextMode = normalizePricingMode(target.value);
    runUiTask(() => handlePricingModeChange(nextMode));
    return;
  }

  if (target.matches("#holdings-status, #holdings-status-mobile, [data-holdings-status]")) {
    state.holdingsView.status = target.value;
    state.holdingsView.page = 1;
    render();
    return;
  }

  if (target.matches("#holdings-sort, #holdings-sort-mobile, [data-holdings-sort]")) {
    state.holdingsView.sort = target.value;
    render();
    return;
  }

  if (target.matches("#holdings-page-size, #holdings-page-size-mobile, [data-holdings-page-size]")) {
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

  if (target.matches("#tx-type")) {
    state.txForm.type = String(target.value || "buy").toLowerCase();
    syncTransactionPreviewFromInputs();
    return;
  }

  if (target.matches("#tx-skin-id")) {
    state.txForm.skinId = String(target.value || "");
    return;
  }

  if (target.matches("#tx-edit-type")) {
    state.txEditModal.type = String(target.value || "buy").toLowerCase();
    syncTransactionEditPreviewFromInputs();
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

  if (form.id === "tx-edit-form") {
    event.preventDefault();
    runUiTask(() => submitTransactionEdit(event));
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

  if (form.id === "market-opportunities-form") {
    event.preventDefault();
    runUiTask(() => submitMarketOpportunitiesScan(event));
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
  state.inspectModal.focusPending = false;
  state.inspectModal.loading = false;
  state.inspectModal.error = "";
  state.inspectModal.steamItemId = "";
  state.inspectModal.skin = null;
  state.inspectModal.marketInsight = null;
  state.inspectModal.exitWhatIf = createExitWhatIfState();
  state.compareDrawer = {
    open: false,
    focusPending: false,
    loading: false,
    error: "",
    skinId: 0,
    marketHashName: "",
    payload: null
  };
  state.tabSwitch.loading = false;
  state.tabSwitch.target = "";
  state.mobileDrawer.open = false;
  state.mobileDrawer.focusPending = false;
  state.portfolioControls.open = false;
  state.portfolioControls.focusPending = false;
  state.avatarMenu.open = false;
  state.headerTabMenu.open = false;
  state.tooltip.openId = "";
  state.txEditModal = {
    open: false,
    id: null,
    skinId: "",
    type: "buy",
    quantity: "1",
    unitPrice: "0",
    commissionPercent: "13",
    executedAt: "",
    submitting: false
  };
  state.exitWhatIf = createExitWhatIfState();
  resetAlertForm();
  state.marketTab.inventoryValue = null;
  state.marketTab.autoLoaded = false;
  state.marketTab.insight = null;
  state.marketTab.opportunities = createMarketOpportunitiesState();
  state.inspectedSteamItemId = "";
  state.tradeCalc.result = null;
  state.accountNotice = "";
  state.steamOnboardingPending = false;
  state.syncingInventory = false;
  state.syncRateLimitedUntil = 0;
  state.syncSummary = null;
  state.social.watchlist = [];
  state.social.leaderboard = [];
  state.social.newSteamId = "";
  state.social.loading = false;
  state.backtest.days = "90";
  state.backtest.loading = false;
  state.backtest.result = null;
  state.teamDashboard.loading = false;
  state.teamDashboard.payload = null;
  state.authNotice.emailConfirmToastShown = false;
  resetPortfolioCardMarkupCache();
  holdingSecondaryDataCache.clear();
  holdingSecondaryRequestCache.clear();
  inspectModalLastTriggerElement = null;
  compareDrawerLastTriggerElement = null;
  compareDrawerRequestTicket += 1;
  portfolioControlsLastTriggerElement = null;
  txEditModalLastTriggerElement = null;
  render();
}

async function syncInventory(options = {}) {
  const { automatic = false } = options;
  if (state.syncingInventory) return false;

  const cooldownSeconds = getSyncCooldownSecondsRemaining();
  if (cooldownSeconds > 0) {
    if (!automatic) {
      notify(
        "warning",
        `Sync rate limited. Try again in ${formatCountdownLabel(cooldownSeconds)}.`
      );
    }
    return false;
  }

  if (!automatic) {
    clearError();
  }
  state.syncingInventory = true;
  render();

  try {
    const result = await api("/inventory/sync", { method: "POST" });
    state.syncSummary = result;
    state.syncRateLimitedUntil = 0;
    if (state.steamOnboardingPending) {
      state.accountNotice = "Inventory synced successfully. Your portfolio is ready.";
      state.steamOnboardingPending = false;
    }
    await refreshPortfolio({ silent: automatic });
    if (!automatic) {
      notify(
        "success",
        `Inventory synced: ${Number(result?.itemsSynced || 0)} items, ${Number(
          result?.pricedItems || 0
        )} priced.`
      );
    }
    return true;
  } catch (err) {
    const status = Number(err?.status || 0);
    if (status === 429) {
      const retryAfter = Math.max(Math.ceil(Number(err?.retryAfterSeconds || 60)), 1);
      state.syncRateLimitedUntil = Date.now() + retryAfter * 1000;
      if (!automatic) {
        notify(
          "warning",
          `Too many sync attempts. Try again in ${formatCountdownLabel(retryAfter)}.`
        );
      }
    }

    if (!automatic) {
      setError(err.message);
      if (status !== 429) {
        notify("warning", "Inventory sync failed. Please try again.");
      }
      state.alerts = [
        {
          severity: "warning",
          code: "SYNC_FAILED",
          message: `Inventory sync failed: ${err.message}`
        },
        ...(state.alerts || []).filter((a) => a.code !== "SYNC_FAILED")
      ];
    } else if (status !== 429) {
      notify("warning", `Auto inventory sync failed: ${err.message}`);
    }
    return false;
  } finally {
    state.syncingInventory = false;
    render();
  }
}

function shouldAutoSyncInventoryOnSessionBoot() {
  if (!state.authenticated) return false;
  if (state.publicPage.steamId64) return false;
  if (state.syncingInventory) return false;
  if (!state.authProfile?.steamLinked) return false;
  if (getSyncCooldownSecondsRemaining() > 0) return false;
  return true;
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
    resetPortfolioCardMarkupCache();

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
    for (const skinId of holdingSecondaryDataCache.keys()) {
      if (!liveSkinIds.has(Number(skinId))) {
        holdingSecondaryDataCache.delete(skinId);
      }
    }
    for (const skinId of holdingSecondaryRequestCache.keys()) {
      if (!liveSkinIds.has(Number(skinId))) {
        holdingSecondaryRequestCache.delete(skinId);
      }
    }
    if (state.compareDrawer.skinId && !liveSkinIds.has(Number(state.compareDrawer.skinId))) {
      state.compareDrawer.open = false;
      state.compareDrawer.focusPending = false;
      state.compareDrawer.loading = false;
      state.compareDrawer.error = "";
      state.compareDrawer.skinId = 0;
      state.compareDrawer.marketHashName = "";
      state.compareDrawer.payload = null;
      compareDrawerRequestTicket += 1;
    } else if (state.compareDrawer.skinId) {
      const liveHolding = getHoldingBySkinId(state.compareDrawer.skinId);
      if (liveHolding) {
        state.compareDrawer.payload = buildCompareDrawerSnapshotFromHolding(liveHolding, {
          fees: state.compareDrawer.payload?.fees || state.portfolio?.pricing?.fees || null,
          generatedAt: state.compareDrawer.payload?.generatedAt || null
        });
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
    notify(
      "success",
      enabled ? "Public portfolio enabled." : "Public portfolio disabled."
    );
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
    notify(
      "success",
      enabled
        ? "Ownership-change alerts enabled."
        : "Ownership-change alerts disabled."
    );
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
    notify("success", `Plan updated to ${toTitle(planTier)}.`);
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
  const submitter = e?.submitter instanceof HTMLElement ? e.submitter : null;
  if (submitter) {
    inspectModalLastTriggerElement = submitter;
  }
  await openInspectModalBySteamItemId(id);
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

async function openInspectModalBySteamItemId(rawId) {
  clearError();
  const id = String(rawId ?? "").trim();
  if (!id) return;
  state.inspectedSteamItemId = id;

  state.inspectModal.open = true;
  state.inspectModal.focusPending = true;
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
    state.marketTab.skinId = String(payload.skin?.id || state.marketTab.skinId || "");
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

function readMarketOpportunitiesFiltersFromDom() {
  const filters = state.marketTab.opportunities.filters;
  const showRiskyChecked = Boolean(
    document.querySelector("#market-opportunity-show-risky")?.checked
  );
  const next = {
    minProfit:
      document.querySelector("#market-opportunity-min-profit")?.value ?? filters.minProfit,
    minSpread:
      document.querySelector("#market-opportunity-min-spread")?.value ?? filters.minSpread,
    minScore:
      document.querySelector("#market-opportunity-min-score")?.value ?? filters.minScore,
    market: document.querySelector("#market-opportunity-market")?.value ?? filters.market,
    liquidityMin:
      document.querySelector("#market-opportunity-liquidity-min")?.value ?? filters.liquidityMin,
    showRisky: showRiskyChecked ? "1" : "0",
    sortBy: document.querySelector("#market-opportunity-sort-by")?.value ?? filters.sortBy,
    limit: document.querySelector("#market-opportunity-limit")?.value ?? filters.limit
  };
  state.marketTab.opportunities.filters = {
    ...filters,
    ...next
  };
}

async function refreshMarketOpportunities(options = {}) {
  const { silent = false } = options;
  const scanner = state.marketTab.opportunities;
  scanner.loading = true;
  scanner.error = "";
  if (!silent) {
    render();
  }

  try {
    const filters = scanner.filters || {};
    const showRisky = String(filters.showRisky || "0") === "1";
    const minScoreRaw = Number(filters.minScore || 0);
    const minScoreQuery =
      showRisky && minScoreRaw >= 70
        ? 50
        : minScoreRaw;
    const query = buildQuery({
      currency: state.currency,
      minProfit: Number(filters.minProfit || 0),
      minSpread: Number(filters.minSpread || 5),
      minScore: minScoreQuery,
      market: filters.market === "all" ? "" : filters.market,
      liquidityMin: Number(filters.liquidityMin || 0),
      showRisky: showRisky ? "1" : "",
      sortBy: String(filters.sortBy || "score"),
      limit: Math.max(Number(filters.limit || 250), 1)
    });
    const payload = await api(`/market/opportunities${query}`);
    scanner.items = Array.isArray(payload?.items) ? payload.items : [];
    scanner.summary = payload?.summary || null;
    scanner.generatedAt = payload?.generatedAt || null;
    scanner.loaded = true;
  } catch (err) {
    scanner.error = err.message || "Failed to load opportunities.";
    // Prevent auto-fetch loops when endpoint is unavailable (e.g., older backend without this route).
    scanner.loaded = true;
    if (!silent) {
      setError(scanner.error);
    }
  } finally {
    scanner.loading = false;
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

async function submitMarketOpportunitiesScan(e) {
  e.preventDefault();
  readMarketOpportunitiesFiltersFromDom();
  await refreshMarketOpportunities();
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
      notify("success", "Alert updated.");
    } else {
      await api("/alerts", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      notify("success", "Alert created.");
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
    notify("info", "Alert deleted.");
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
    notify("success", enabled ? "Alert enabled." : "Alert paused.");
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
    notify("success", "Transaction saved.");
  } catch (err) {
    setError(err.message);
  } finally {
    state.txSubmitting = false;
    render();
  }
}

function openTxEditModal(rawTxId, triggerElement = null) {
  const txId = Number(rawTxId);
  if (!Number.isInteger(txId) || txId <= 0) return;
  const tx = (state.transactions || []).find((row) => Number(row.id) === txId);
  if (!tx) {
    notify("warning", "Transaction not found.");
    return;
  }

  state.txEditModal = {
    open: true,
    id: txId,
    skinId: String(tx.skin_id || ""),
    type: String(tx.type || "buy").toLowerCase(),
    quantity: String(Math.max(Number(tx.quantity || 1), 1)),
    unitPrice: String(Number(tx.unit_price || 0)),
    commissionPercent: String(Number(tx.commission_percent || 0)),
    executedAt: String(tx.executed_at || "").slice(0, 16),
    submitting: false
  };
  txEditModalLastTriggerElement =
    triggerElement instanceof HTMLElement ? triggerElement : document.activeElement;
  render();
  requestAnimationFrame(() => {
    const firstInput = document.querySelector("#tx-edit-type");
    if (firstInput instanceof HTMLElement) {
      firstInput.focus();
    }
  });
}

function closeTxEditModal(options = {}) {
  const { restoreFocus = true } = options;
  if (!state.txEditModal.open) return;
  state.txEditModal = {
    open: false,
    id: null,
    skinId: "",
    type: "buy",
    quantity: "1",
    unitPrice: "0",
    commissionPercent: "13",
    executedAt: "",
    submitting: false
  };
  render();
  if (restoreFocus && txEditModalLastTriggerElement instanceof HTMLElement) {
    requestAnimationFrame(() => {
      txEditModalLastTriggerElement?.focus?.();
    });
  }
  txEditModalLastTriggerElement = null;
}

async function submitTransactionEdit(event) {
  event.preventDefault();
  clearError();
  if (!state.txEditModal.open || state.txEditModal.submitting) return;

  const txId = Number(state.txEditModal.id);
  const type = String(document.querySelector("#tx-edit-type")?.value || "buy").toLowerCase();
  const quantity = Number(document.querySelector("#tx-edit-quantity")?.value);
  const unitPrice = Number(document.querySelector("#tx-edit-unit-price")?.value);
  const commissionPercent = Number(document.querySelector("#tx-edit-commission")?.value);
  const executedAtRaw = String(document.querySelector("#tx-edit-executed-at")?.value || "").trim();
  const executedAt = executedAtRaw ? new Date(executedAtRaw).toISOString() : null;

  state.txEditModal.type = type;
  state.txEditModal.quantity = String(document.querySelector("#tx-edit-quantity")?.value || "");
  state.txEditModal.unitPrice = String(document.querySelector("#tx-edit-unit-price")?.value || "");
  state.txEditModal.commissionPercent = String(
    document.querySelector("#tx-edit-commission")?.value || ""
  );
  state.txEditModal.executedAt = executedAtRaw;

  if (!Number.isInteger(txId) || txId <= 0) {
    setError("Pick a valid transaction to update.");
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
  if (executedAtRaw && !executedAt) {
    setError("Pick a valid date/time.");
    return;
  }

  state.txEditModal.submitting = true;
  render();

  try {
    await api(`/transactions/${txId}`, {
      method: "PATCH",
      body: JSON.stringify({
        type,
        quantity,
        unitPrice,
        commissionPercent,
        executedAt,
        currency: "USD"
      })
    });
    await refreshPortfolio();
    notify("success", "Transaction updated.");
    closeTxEditModal({ restoreFocus: false });
  } catch (err) {
    setError(err.message);
    state.txEditModal.submitting = false;
    render();
  }
}

async function removeTransaction(id) {
  clearError();
  try {
    await api(`/transactions/${Number(id)}`, { method: "DELETE" });
    await refreshPortfolio();
    notify("info", "Transaction deleted.");
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
    notify(
      failed.length ? "warning" : "success",
      failed.length
        ? `CSV imported ${imported}/${parsed.length}. ${failed.length} rows failed.`
        : `CSV imported ${imported} rows successfully.`
    );
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

function getMarketSourceKey(source) {
  const raw = String(source || "")
    .trim()
    .toLowerCase();
  if (!raw) return "market";
  if (raw === "dm" || raw === "dmarket") return "dmarket";
  if (raw === "steam") return "steam";
  if (raw === "skinport") return "skinport";
  if (raw === "csfloat" || raw === "cs float") return "csfloat";
  return raw;
}

function getMarketIconAbbreviation(source) {
  const key = getMarketSourceKey(source);
  const map = {
    steam: "ST",
    skinport: "SP",
    csfloat: "CF",
    dmarket: "DM"
  };
  return map[key] || "MK";
}

function renderMarketSourceIcon(source) {
  const key = getMarketSourceKey(source);
  const iconText = getMarketIconAbbreviation(key);
  return `<span class="compare-drawer-market-icon market-icon-${escapeHtml(
    key
  )}" aria-hidden="true">${escapeHtml(iconText)}</span>`;
}

function getOpportunityScoreTone(score) {
  const value = Number(score || 0);
  if (value >= 90) return "positive";
  if (value >= 70) return "neutral";
  if (value >= 50) return "warning";
  return "negative";
}

function formatOpportunityLabel(label, score) {
  const direct = String(label || "").trim();
  if (direct) return direct;
  const value = Number(score || 0);
  if (value >= 90) return "Strong";
  if (value >= 70) return "Good";
  if (value >= 50) return "Risky";
  return "Weak";
}

function formatArbitrageReasonLabel(reasonCode) {
  const key = String(reasonCode || "")
    .trim()
    .toLowerCase();
  const map = {
    low_liquidity: "Low liquidity",
    extreme_spread: "Extreme spread",
    insufficient_market_data: "Insufficient market data",
    spread_below_min: "Spread below baseline",
    non_positive_profit: "Non-positive profit"
  };
  return map[key] || "Filtered by anti-fake checks";
}

function isArbitrageDebugEnabled() {
  const host = String(window?.location?.hostname || "").trim().toLowerCase();
  const query = String(window?.location?.search || "").trim().toLowerCase();
  return host === "localhost" || host === "127.0.0.1" || query.includes("debug=1");
}

function resolveCompareDrawerFeePercent(row, feeMap = null) {
  const inlineFee = Number(row?.feePercent);
  if (Number.isFinite(inlineFee)) return inlineFee;
  const sourceKey = String(row?.source || "")
    .trim()
    .toLowerCase();
  const mappedFee = Number(feeMap?.[sourceKey]);
  return Number.isFinite(mappedFee) ? mappedFee : null;
}

function getCompareDrawerInsights(payload = null) {
  if (!payload || typeof payload !== "object") return null;
  const comparison = payload.marketComparison || null;
  const perMarket = Array.isArray(comparison?.perMarket) ? comparison.perMarket : [];
  const feeMap = payload.fees || null;
  const cached = compareDrawerInsightsCache.get(payload);
  if (cached && cached.perMarketRef === perMarket && cached.feeMapRef === feeMap) {
    return cached.value;
  }

  const rows = perMarket.map((row) => {
    const available = Boolean(row?.available);
    const buyValue = Number(row?.grossPrice);
    const sellValue = Number(row?.netPriceAfterFees);
    const sourceKey = getMarketSourceKey(row?.source);
    return {
      source: String(row?.source || ""),
      sourceKey,
      label: formatMarketSourceLabel(sourceKey),
      available,
      buyValue: available && Number.isFinite(buyValue) ? buyValue : null,
      sellValue: available && Number.isFinite(sellValue) ? sellValue : null,
      currency: row?.currency || payload.currency || state.currency,
      updatedAt: row?.updatedAt || null,
      unavailableReason: row?.unavailableReason ? String(row.unavailableReason) : "",
      url: typeof row?.url === "string" ? row.url.trim() : "",
      feePercent: resolveCompareDrawerFeePercent(row, feeMap)
    };
  });

  const buyCandidates = rows
    .filter((entry) => Number.isFinite(entry.buyValue))
    .sort((a, b) => Number(a.buyValue) - Number(b.buyValue));
  const sellCandidates = rows
    .filter((entry) => Number.isFinite(entry.sellValue))
    .sort((a, b) => Number(b.sellValue) - Number(a.sellValue));

  const lowestBuyMarket = buyCandidates[0] || null;
  const highestSellMarket = sellCandidates[0] || null;
  const lowestBuyValue = Number.isFinite(lowestBuyMarket?.buyValue)
    ? Number(lowestBuyMarket.buyValue)
    : null;
  const highestSellNetValue = Number.isFinite(highestSellMarket?.sellValue)
    ? Number(highestSellMarket.sellValue)
    : null;
  const canCalculateSpread =
    Number.isFinite(lowestBuyValue) &&
    Number.isFinite(highestSellNetValue) &&
    Number(lowestBuyValue) > 0;
  const profit = canCalculateSpread
    ? Number(highestSellNetValue) - Number(lowestBuyValue)
    : null;
  const spreadPercent = canCalculateSpread
    ? (Number(profit) / Number(lowestBuyValue)) * 100
    : null;

  const value = {
    rows,
    topBuyMarkets: buyCandidates.slice(0, 3),
    topSellMarkets: sellCandidates.slice(0, 3),
    lowestBuyMarket,
    highestSellMarket,
    lowestBuyValue,
    highestSellNetValue,
    profit,
    spreadPercent
  };
  compareDrawerInsightsCache.set(payload, {
    perMarketRef: perMarket,
    feeMapRef: feeMap,
    value
  });
  return value;
}

function getHoldingConditionLabel(item) {
  const direct = String(
    item?.exterior || item?.condition || item?.wearName || item?.wear || ""
  ).trim();
  if (direct) return direct;
  const marketHashName = String(item?.marketHashName || "");
  const match = marketHashName.match(/\(([^)]+)\)\s*$/);
  return match ? String(match[1]).trim() : "Unknown";
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
    <div class="portfolio-controls-shell">
      <div class="portfolio-mobile-controls">
        <button
          type="button"
          class="ghost-btn"
          data-portfolio-controls-open="1"
          aria-expanded="${state.portfolioControls.open ? "true" : "false"}"
          aria-controls="portfolio-controls-drawer-panel"
        >
          Filters & Sort
        </button>
      </div>
      <div class="portfolio-control-bar">
        ${renderPortfolioControlFields({ mobile: false, mode })}
      </div>
    </div>
    <p class="helper-text market-fee-note" title="Net sell value subtracts marketplace fees before comparing venues.">
      Portfolio valuation mode: <strong>${escapeHtml(getPricingModeLabel(mode))}</strong>. Fee model: ${escapeHtml(
    feeText
  )}.
    </p>
  `;
}

function renderPortfolioControlFields({ mobile = false, mode = state.pricingMode } = {}) {
  const suffix = mobile ? "-mobile" : "";
  const controlClass = mobile ? "portfolio-control-field mobile" : "portfolio-control-field";
  return `
    <label class="${controlClass}">Search
      <input
        id="holdings-search${suffix}"
        data-holdings-search="1"
        placeholder="item name or steam item id"
        value="${escapeHtml(state.holdingsView.q)}"
      />
    </label>
    <label class="${controlClass}">Status
      <select id="holdings-status${suffix}" data-holdings-status="1">
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
    <label class="${controlClass}">Signal Sort
      <select id="holdings-sort${suffix}" data-holdings-sort="1">
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
    <label class="${controlClass}">Per page
      <select id="holdings-page-size${suffix}" data-holdings-page-size="1">
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
    <label class="${controlClass}">Pricing Mode
      <select
        id="${mobile ? "pricing-mode-select-mobile" : "pricing-mode-select"}"
        data-pricing-mode-select="1"
      >
        <option value="lowest_buy" ${mode === "lowest_buy" ? "selected" : ""}>Lowest Buy</option>
        <option value="steam" ${mode === "steam" ? "selected" : ""}>Steam Price</option>
        <option value="best_sell_net" ${mode === "best_sell_net" ? "selected" : ""}>Best Sell Net</option>
      </select>
    </label>
    <button
      type="button"
      class="ghost-btn compare-refresh-btn portfolio-controls-refresh-btn"
      data-refresh-market-compare="1"
      ${state.compareRefreshing ? "disabled" : ""}
    >
      <span class="btn-inline-status">
        <span class="spinner ${state.compareRefreshing ? "" : "is-hidden"}" aria-hidden="true"></span>
        <span>Refresh Prices</span>
      </span>
    </button>
  `;
}

function renderPortfolioControlsDrawer() {
  return renderDrawer({
    open: Boolean(state.portfolioControls.open),
    title: "Portfolio Controls",
    subtitle: "Search, filter, sort, and pricing mode",
    bodyMarkup: `<div class="portfolio-controls-drawer-fields">${renderPortfolioControlFields({
      mobile: true
    })}</div>`,
    footerMarkup: `
      <button
        type="button"
        class="ghost-btn"
        data-portfolio-controls-close="1"
      >
        Done
      </button>
    `,
    closeAttr: 'data-portfolio-controls-close="1"',
    label: "Portfolio controls",
    rootClassName: "portfolio-controls-drawer-root",
    overlayAttr: 'data-portfolio-controls-overlay="1"',
    panelAttr: 'id="portfolio-controls-drawer-panel" data-portfolio-controls-panel="1"'
  });
}

function wrapCompareDrawerBodyWithLoadingState(markup) {
  const isLoading = Boolean(state.compareDrawer.loading);
  return `
    <div class="compare-drawer-content-shell ${isLoading ? "is-loading" : ""}" aria-busy="${
      isLoading ? "true" : "false"
    }">
      <div class="compare-drawer-content-main">
        ${markup}
      </div>
      ${
        isLoading
          ? `
        <div class="compare-drawer-loading-overlay" role="status" aria-live="polite">
          <span class="spinner compare-drawer-loading-spinner" aria-hidden="true"></span>
          <p>Loading comparison data...</p>
        </div>
      `
          : ""
      }
    </div>
  `;
}

function renderCompareDrawerBody() {
  const drawer = state.compareDrawer;
  const payload = drawer.payload;
  if (!payload && drawer.loading) {
    return wrapCompareDrawerBodyWithLoadingState(`
      <div class="compare-drawer-skeleton" aria-hidden="true">
        <div class="compare-drawer-skeleton-head"></div>
        <div class="compare-drawer-skeleton-row"></div>
        <div class="compare-drawer-skeleton-row"></div>
        <div class="compare-drawer-skeleton-row"></div>
      </div>
    `);
  }

  if (!payload) {
    return wrapCompareDrawerBodyWithLoadingState(
      '<p class="muted">Select a mover and open compare to inspect multi-market pricing.</p>'
    );
  }

  const insights = getCompareDrawerInsights(payload);
  const rows = Array.isArray(insights?.rows) ? insights.rows : [];
  const pricingMode = normalizePricingMode(state.portfolio?.pricing?.mode || state.pricingMode);
  const modeLabel = getPricingModeLabel(pricingMode);
  const lowestBuyMarket = insights?.lowestBuyMarket || null;
  const highestSellMarket = insights?.highestSellMarket || null;
  const backendArbitrage = payload?.marketComparison?.arbitrage || null;
  const antiFake = backendArbitrage?.antiFake || null;
  const backendBuyMarketLabel = backendArbitrage?.buyMarket
    ? formatMarketSourceLabel(backendArbitrage.buyMarket)
    : "";
  const backendSellMarketLabel = backendArbitrage?.sellMarket
    ? formatMarketSourceLabel(backendArbitrage.sellMarket)
    : "";
  const lowestBuyValue = Number.isFinite(insights?.lowestBuyValue)
    ? Number(insights.lowestBuyValue)
    : null;
  const highestSellNetValue = Number.isFinite(insights?.highestSellNetValue)
    ? Number(insights.highestSellNetValue)
    : null;
  const arbitrageProfit = Number.isFinite(backendArbitrage?.profit)
    ? Number(backendArbitrage.profit)
    : Number.isFinite(insights?.profit)
      ? Number(insights.profit)
      : null;
  const arbitrageSpreadPercent = Number.isFinite(backendArbitrage?.spreadPercent)
    ? Number(backendArbitrage.spreadPercent)
    : Number.isFinite(insights?.spreadPercent)
      ? Number(insights.spreadPercent)
      : null;
  const arbitrageScore = Number.isFinite(backendArbitrage?.opportunityScore)
    ? Number(backendArbitrage.opportunityScore)
    : null;
  const arbitrageScoreLabel = formatOpportunityLabel(
    backendArbitrage?.scoreCategory,
    arbitrageScore
  );
  const antiFakeReasonCodes = Array.isArray(antiFake?.reasons)
    ? antiFake.reasons.filter(Boolean)
    : [];
  const antiFakeReasonLabels = Array.isArray(antiFake?.reasonLabels)
    ? antiFake.reasonLabels.filter(Boolean)
    : antiFakeReasonCodes.map((reason) => formatArbitrageReasonLabel(reason));
  const hasBackendArbitrageVerdict =
    backendArbitrage && typeof backendArbitrage.isOpportunity === "boolean";
  const isRealisticArbitrage =
    hasBackendArbitrageVerdict &&
    backendArbitrage.isOpportunity === true &&
    (arbitrageScore == null || arbitrageScore >= 50);
  const unrealisticByScore = arbitrageScore != null && arbitrageScore < 50;
  const shouldShowUnrealisticNotice =
    Boolean(backendArbitrage) &&
    (backendArbitrage.isOpportunity !== true || unrealisticByScore);
  const profitableArbitrage = hasBackendArbitrageVerdict
    ? isRealisticArbitrage
    : Number(arbitrageProfit || 0) > 0 && Number(arbitrageSpreadPercent || 0) >= 5;
  const arbitrageProfitClass = arbitrageProfit > 0 ? "is-positive" : "is-neutral";
  const arbitrageScoreTone = getOpportunityScoreTone(arbitrageScore);
  const arbitrageBuySourceKey = getMarketSourceKey(
    backendArbitrage?.buyMarket || lowestBuyMarket?.source
  );
  const arbitrageSellSourceKey = getMarketSourceKey(
    backendArbitrage?.sellMarket || highestSellMarket?.source
  );
  const fallbackReasonLabels = !antiFakeReasonLabels.length && shouldShowUnrealisticNotice
    ? [
        backendArbitrage?.isOpportunity === false
          ? "Not passing anti-fake liquidity/spread checks"
          : "Score below realism threshold"
      ]
    : [];
  const effectiveReasonLabels = antiFakeReasonLabels.length
    ? antiFakeReasonLabels
    : fallbackReasonLabels;
  const debugQuoteSnapshot = backendArbitrage?.debug?.rawQuotesByMarket || null;
  const debugMarkup =
    shouldShowUnrealisticNotice &&
    isArbitrageDebugEnabled() &&
    debugQuoteSnapshot &&
    typeof debugQuoteSnapshot === "object"
      ? `
        <details class="compare-drawer-arb-debug">
          <summary>Why filtered?</summary>
          <pre>${escapeHtml(JSON.stringify(debugQuoteSnapshot, null, 2))}</pre>
        </details>
      `
      : "";

  const renderQuickList = (entries = [], valueKey = "buyValue", emptyText = "No market data") => {
    if (!entries.length) {
      return `<p class="muted compare-drawer-empty">${escapeHtml(emptyText)}</p>`;
    }
    return `
      <ul class="compare-drawer-top-list">
        ${entries
          .map((entry) => {
            const value = Number(entry?.[valueKey]);
            return `
              <li>
                <span class="compare-drawer-top-list-market">
                  ${renderMarketSourceIcon(entry?.sourceKey || entry?.source || entry?.label)}
                  <span>${escapeHtml(entry?.label || "Market")}</span>
                </span>
                <strong>${Number.isFinite(value) ? formatMoney(value, entry?.currency || payload.currency) : "-"}</strong>
              </li>
            `;
          })
          .join("")}
      </ul>
    `;
  };

  const marketRows = rows.length
    ? rows
        .map((entry) => {
          const available = Boolean(entry?.available);
          const buyValue = Number(entry?.buyValue);
          const sellValue = Number(entry?.sellValue);
          const hasBuy = Number.isFinite(buyValue);
          const hasSell = Number.isFinite(sellValue);
          const feeValue = Number(entry?.feePercent);
          const feeDisplay = Number.isFinite(feeValue) ? `${formatNumber(feeValue, 2)}%` : "-";
          const marketProfit =
            Number.isFinite(lowestBuyValue) &&
            Number(lowestBuyValue) > 0 &&
            hasSell
              ? sellValue - Number(lowestBuyValue)
              : null;
          const profitTone =
            marketProfit == null
              ? "profit-neutral"
              : marketProfit > 0
                ? "profit-positive"
                : marketProfit < 0
                  ? "profit-negative"
                  : "profit-neutral";
          const profitLabel =
            marketProfit == null ? "Profit" : marketProfit < 0 ? "Loss" : "Profit";
          const sourceKey = getMarketSourceKey(entry?.sourceKey || entry?.source);
          const isArbBuyMarket =
            profitableArbitrage &&
            sourceKey !== "market" &&
            sourceKey === arbitrageBuySourceKey;
          const isArbSellMarket =
            profitableArbitrage &&
            sourceKey !== "market" &&
            sourceKey === arbitrageSellSourceKey;
          const arbitrageRoleLabel =
            isArbBuyMarket && isArbSellMarket
              ? "Arb Buy/Sell"
              : isArbBuyMarket
                ? "Arb Buy"
                : isArbSellMarket
                  ? "Arb Sell"
                  : "";
          const arbitrageRoleClass =
            isArbBuyMarket && isArbSellMarket
              ? "has-arb-both"
              : isArbBuyMarket
                ? "has-arb-buy"
                : isArbSellMarket
                  ? "has-arb-sell"
                  : "";
          return `
            <article class="compare-drawer-market-card ${available ? "" : "unavailable"} ${escapeHtml(
              arbitrageRoleClass
            )}">
              <div class="compare-drawer-market-head">
                <div class="compare-drawer-market-head-main">
                  ${renderMarketSourceIcon(sourceKey)}
                  <div class="compare-drawer-market-title-wrap">
                    <strong>${escapeHtml(entry?.label || "Market")}</strong>
                    ${
                      arbitrageRoleLabel
                        ? `<span class="compare-drawer-market-arb-tag ${escapeHtml(
                            arbitrageRoleClass
                          )}">${escapeHtml(arbitrageRoleLabel)}</span>`
                        : ""
                    }
                  </div>
                </div>
                <small>${escapeHtml(available ? `Updated ${formatRelativeTime(entry?.updatedAt)}` : "No data")}</small>
              </div>
              ${
                !available && entry?.unavailableReason
                  ? `<p class="muted compare-drawer-empty">${escapeHtml(entry.unavailableReason)}</p>`
                  : ""
              }
              <dl class="compare-drawer-market-metrics">
                <div class="compare-drawer-market-metric">
                  <dt>Best Buy</dt>
                  <dd>${hasBuy ? formatMoney(buyValue, entry?.currency || payload.currency) : "-"}</dd>
                </div>
                <div class="compare-drawer-market-metric">
                  <dt>Best Sell Net</dt>
                  <dd>${hasSell ? formatMoney(sellValue, entry?.currency || payload.currency) : "-"}</dd>
                </div>
                <div class="compare-drawer-market-metric">
                  <dt>Fee</dt>
                  <dd>${escapeHtml(feeDisplay)}</dd>
                </div>
                <div class="compare-drawer-market-metric ${profitTone}">
                  <dt>${escapeHtml(profitLabel)}</dt>
                  <dd>${
                    marketProfit == null
                      ? "-"
                      : `${profitLabel} ${formatSignedMoney(
                          marketProfit,
                          entry?.currency || payload.currency
                        )}`
                  }</dd>
                </div>
              </dl>
              ${
                entry?.url
                  ? `<a class="link-btn market-open-link compare-drawer-market-open-link" href="${escapeHtml(
                      entry.url
                    )}" target="_blank" rel="noreferrer">Open listing</a>`
                  : '<span class="muted compare-drawer-empty">No listing link</span>'
              }
            </article>
          `;
        })
        .join("")
    : '<p class="muted compare-drawer-empty">Market comparison data is unavailable for this item.</p>';

  return wrapCompareDrawerBodyWithLoadingState(`
    <div class="compare-drawer-item-head">
      <img src="${escapeHtml(payload.imageUrl)}" alt="${escapeHtml(payload.marketHashName)}" loading="lazy" />
      <div class="compare-drawer-item-meta">
        <p class="compare-drawer-item-name">${escapeHtml(payload.marketHashName)}</p>
        <div class="compare-drawer-item-badges">
          <span class="compare-drawer-item-badge">Condition ${escapeHtml(
            payload.condition || "Unknown condition"
          )}</span>
          <span class="compare-drawer-item-badge">Qty ${escapeHtml(formatNumber(payload.quantity, 0))}</span>
          ${
            Number.isFinite(Number(payload.lineValue))
              ? `<span class="compare-drawer-item-badge">Position ${formatMoney(
                  payload.lineValue,
                  payload.currency || state.currency
                )}</span>`
              : ""
          }
        </div>
        <small class="muted compare-drawer-item-note">Multi-market pricing and fee-adjusted sell opportunities</small>
      </div>
    </div>
    <section class="compare-drawer-arb-card">
      <p class="compare-drawer-section-eyebrow">Arbitrage Insight</p>
      <div class="compare-drawer-arb-status ${profitableArbitrage ? "is-opportunity" : "is-none"}">
        <span class="compare-drawer-arb-status-dot" aria-hidden="true"></span>
        <strong>${
          profitableArbitrage
            ? "Opportunity Detected"
            : shouldShowUnrealisticNotice
              ? "Not a Realistic Arbitrage Opportunity"
              : "No Arbitrage Signal"
        }</strong>
      </div>
      ${
        profitableArbitrage
          ? `
        <div class="compare-drawer-arb-grid">
          <article class="compare-drawer-arb-leg">
            <span>BUY</span>
            <strong>${escapeHtml(backendBuyMarketLabel || lowestBuyMarket?.label || "N/A")}</strong>
            <small>${
              Number.isFinite(lowestBuyValue)
                ? formatMoney(lowestBuyValue, lowestBuyMarket?.currency || payload.currency)
                : "-"
            }</small>
          </article>
          <article class="compare-drawer-arb-leg">
            <span>SELL</span>
            <strong>${escapeHtml(backendSellMarketLabel || highestSellMarket?.label || "N/A")}</strong>
            <small>${
              Number.isFinite(highestSellNetValue)
                ? formatMoney(highestSellNetValue, highestSellMarket?.currency || payload.currency)
                : "-"
            }</small>
          </article>
        </div>
        <div class="compare-drawer-arb-profit ${arbitrageProfitClass}">
          <span>Estimated Profit</span>
          <strong>${
            arbitrageProfit == null || arbitrageSpreadPercent == null
              ? "N/A"
              : `${formatSignedMoney(arbitrageProfit, payload.currency || state.currency)} (${formatPercent(
                  arbitrageSpreadPercent
                )})`
          }</strong>
        </div>
        <div class="compare-drawer-arb-score ${escapeHtml(arbitrageScoreTone)}">
          <span>Opportunity Score</span>
          <strong>${
            arbitrageScore == null
              ? "N/A"
              : `${formatNumber(arbitrageScore, 0)} / 100`
          }</strong>
          <small>${escapeHtml(arbitrageScoreLabel)}</small>
        </div>
      `
          : shouldShowUnrealisticNotice
            ? `
        <div class="compare-drawer-arb-warning">
          <p class="muted compare-drawer-empty">Not a realistic arbitrage opportunity right now.</p>
          ${
            effectiveReasonLabels.length
              ? `<ul class="compare-drawer-arb-reasons">
                  ${effectiveReasonLabels
                    .map((reason) => `<li>${escapeHtml(reason)}</li>`)
                    .join("")}
                </ul>`
              : ""
          }
          ${debugMarkup}
        </div>
      `
            : '<p class="muted compare-drawer-empty">No profitable arbitrage detected across markets</p>'
      }
    </section>
    <div class="compare-drawer-insights-grid">
      <section class="compare-drawer-insight-card">
        <h4>Best Buy</h4>
        ${renderQuickList(insights?.topBuyMarkets || [], "buyValue", "No buy prices")}
      </section>
      <section class="compare-drawer-insight-card">
        <h4>Best Sell</h4>
        ${renderQuickList(insights?.topSellMarkets || [], "sellValue", "No sell prices")}
      </section>
    </div>
    <div class="compare-drawer-context">
      <p><span>${escapeHtml(modeLabel)}</span><strong>${formatMoney(payload.currentPrice, payload.currency || state.currency)}</strong></p>
      <p><span>Source</span><strong>${escapeHtml(payload.currentPriceSource || "N/A")}</strong></p>
      ${
        payload.generatedAt
          ? `<small class="muted">Snapshot ${escapeHtml(formatRelativeTime(payload.generatedAt))}</small>`
          : ""
      }
    </div>
    <div class="compare-drawer-market-section-head">
      <h4>All Markets</h4>
      <small class="muted">Profit/loss is based on lowest buy price</small>
    </div>
    <div class="compare-drawer-market-grid">${marketRows}</div>
    ${
      drawer.error
        ? `<p class="muted compare-drawer-error" role="status" aria-live="polite">${escapeHtml(drawer.error)}</p>`
        : ""
    }
  `);
}

function renderCompareDrawerOverlay() {
  const drawer = state.compareDrawer;
  const insights = getCompareDrawerInsights(drawer.payload);
  const backendArbitrage = drawer?.payload?.marketComparison?.arbitrage || null;
  const bestBuyUrl = String(
    backendArbitrage?.buyUrl || insights?.lowestBuyMarket?.url || ""
  ).trim();
  const bestSellUrl = String(
    backendArbitrage?.sellUrl || insights?.highestSellMarket?.url || ""
  ).trim();
  const footerMarkup = `
    <button
      type="button"
      class="btn-primary compare-drawer-open-best-buy-btn"
      id="compare-drawer-open-best-buy-btn"
      data-market-url="${escapeHtml(bestBuyUrl)}"
      data-skin-id="${Number(drawer.skinId || 0)}"
      ${drawer.skinId && !drawer.loading ? "" : "disabled"}
    >
      Open Best Buy
    </button>
    <button
      type="button"
      class="ghost-btn btn-secondary compare-drawer-open-best-sell-btn"
      id="compare-drawer-open-best-sell-btn"
      data-market-url="${escapeHtml(bestSellUrl)}"
      data-skin-id="${Number(drawer.skinId || 0)}"
      ${drawer.skinId && !drawer.loading ? "" : "disabled"}
    >
      Open Best Sell
    </button>
    <button
      type="button"
      class="ghost-btn compare-drawer-refresh-btn"
      id="compare-drawer-refresh-btn"
      ${drawer.loading ? "disabled" : ""}
    >
      ${drawer.loading ? "Refreshing..." : "Refresh prices"}
    </button>
  `;

  return renderDrawer({
    open: Boolean(drawer.open),
    title: drawer.marketHashName || "Market Comparison",
    subtitle: "Multi-market pricing and fee-adjusted sell opportunities",
    bodyMarkup: renderCompareDrawerBody(),
    footerMarkup,
    closeAttr: 'data-compare-drawer-close="1"',
    label: "Compare markets",
    rootClassName: "compare-drawer-root",
    overlayAttr: 'data-compare-drawer-overlay="1"',
    panelAttr: 'data-compare-drawer-panel="1"'
  });
}

function renderPortfolioMobileList() {
  if (state.portfolioLoading) {
    return `
      <div class="portfolio-mobile-list">
        ${Array.from({ length: 5 }, (_, idx) => `
          <article class="portfolio-mobile-item is-skeleton" data-skeleton-index="${idx}">
            <div class="table-row-skeleton"></div>
          </article>
        `).join("")}
      </div>
    `;
  }

  const { items } = getFilteredHoldings();
  if (!items.length) {
    return `<p class="muted empty-table-cell">No holdings yet. Link Steam and run sync.</p>`;
  }

  const formatSteamItemIdCell = (item) => {
    const ids = Array.isArray(item.steamItemIds) ? item.steamItemIds : [];
    if (!ids.length) return "-";
    if (ids.length === 1) return escapeHtml(ids[0]);
    return `${escapeHtml(ids[0])} +${ids.length - 1} more`;
  };

  return `
    <div class="portfolio-mobile-list">
      ${items
        .map((item) => {
          const cacheKey = buildPortfolioCardCacheKey(item, "mobile");
          const cachedMarkup = portfolioCardMarkupCache.mobile.get(cacheKey);
          if (cachedMarkup) {
            return cachedMarkup;
          }

          const skinId = Number(item.skinId || 0);
          const rarityTheme = getItemRarityTheme(item);
          const itemImageUrl = getItemImageUrl(item);
          const fallbackImage = isCaseLikeItem(item) ? defaultCaseImage : defaultSkinImage;
          const oneDayClass = Number(item.oneDayChangePercent || 0) >= 0 ? "up" : "down";
          const sevenDayClass = Number(item.sevenDayChangePercent || 0) >= 0 ? "up" : "down";
          const conditionLabel = getHoldingConditionLabel(item);
          const markup = `
            <article class="portfolio-mobile-item">
              <div class="portfolio-mobile-head">
                <img
                  class="portfolio-mobile-thumb"
                  style="--rarity-color: ${rarityTheme.color};"
                  src="${escapeHtml(itemImageUrl)}"
                  alt="${escapeHtml(item.marketHashName || "CS2 item")}"
                  loading="lazy"
                  onerror="this.onerror=null;this.src='${escapeHtml(fallbackImage)}';"
                />
                <div class="portfolio-mobile-meta">
                  <p class="portfolio-mobile-name">${escapeHtml(item.marketHashName || "-")}</p>
                  <div class="portfolio-mobile-subline">
                    <span class="rarity-tag" style="--rarity-color: ${rarityTheme.color};">${escapeHtml(
                      rarityTheme.rarity
                    )}</span>
                    <small class="muted portfolio-condition">${escapeHtml(conditionLabel)}</small>
                    <small class="muted">Qty ${Number(item.quantity || 0)}</small>
                  </div>
                  <small class="muted mono-cell">Steam ID: ${formatSteamItemIdCell(item)}</small>
                </div>
              </div>
              <div class="portfolio-mobile-stats">
                <p><span>Price</span><strong>${formatMoney(item.currentPrice)}</strong></p>
                <p><span>7D</span><strong class="pnl-text ${sevenDayClass}">${formatPercent(
            item.sevenDayChangePercent
          )}</strong></p>
                <p><span>24H</span><strong class="pnl-text ${oneDayClass}">${formatPercent(
            item.oneDayChangePercent
          )}</strong></p>
              </div>
              <div class="row portfolio-mobile-actions">
                <button
                  type="button"
                  class="inspect-skin-btn btn-primary"
                  data-steam-item-id="${escapeHtml(item.primarySteamItemId || "")}"
                  ${item.primarySteamItemId ? "" : "disabled"}
                >
                  Inspect
                </button>
                <button
                  type="button"
                  class="ghost-btn compare-market-btn btn-secondary"
                  data-skin-id="${skinId}"
                >
                  Compare
                </button>
                <button
                  type="button"
                  class="ghost-btn sell-suggestion-btn btn-tertiary"
                  data-skin-id="${skinId}"
                >
                  Sell Suggestion
                </button>
              </div>
            </article>
          `;
          portfolioCardMarkupCache.mobile.set(cacheKey, markup);
          trimPortfolioCardCache(portfolioCardMarkupCache.mobile);
          return markup;
        })
        .join("")}
    </div>
  `;
}

function renderPortfolioDesktopCards() {
  if (state.portfolioLoading) {
    return `
      <div class="portfolio-desktop-cards-grid">
        ${Array.from({ length: 8 }, (_, idx) => `
          <article class="portfolio-desktop-card is-skeleton" data-skeleton-index="${idx}">
            <div class="table-row-skeleton"></div>
          </article>
        `).join("")}
      </div>
    `;
  }

  const { items } = getFilteredHoldings();
  if (!items.length) {
    return `<p class="muted empty-table-cell">No holdings match your filters.</p>`;
  }

  return `
    <div class="portfolio-desktop-cards-grid">
      ${items
        .map((item) => {
          const cacheKey = buildPortfolioCardCacheKey(item, "desktop");
          const cachedMarkup = portfolioCardMarkupCache.desktop.get(cacheKey);
          if (cachedMarkup) {
            return cachedMarkup;
          }

          const skinId = Number(item.skinId || 0);
          const rarityTheme = getItemRarityTheme(item);
          const itemImageUrl = getItemImageUrl(item);
          const fallbackImage = isCaseLikeItem(item) ? defaultCaseImage : defaultSkinImage;
          const sevenDayClass = Number(item.sevenDayChangePercent || 0) >= 0 ? "up" : "down";
          const conditionLabel = getHoldingConditionLabel(item);
          const clueAction = String(item?.managementClue?.action || "watch").toLowerCase();
          const clueConfidence = Math.round(Number(item?.managementClue?.confidence || 0));
          const signalBand =
            clueConfidence >= 75 ? "strong" : clueConfidence <= 45 ? "weak" : "watch";
          const signalLabel = `${toTitle(clueAction)} ${clueConfidence}%`;
          const markup = `
            <article class="portfolio-desktop-card ${sevenDayClass}">
              <header class="portfolio-desktop-card-head">
                <div class="portfolio-desktop-card-meta">
                  <div class="portfolio-desktop-card-title-row">
                    <p class="portfolio-desktop-card-name" title="${escapeHtml(
                      item.marketHashName || "-"
                    )}">
                      ${escapeHtml(item.marketHashName || "-")}
                    </p>
                    ${renderHoldingInfoTooltip(item)}
                  </div>
                  <div class="portfolio-desktop-card-subline">
                    <span class="rarity-tag" style="--rarity-color: ${rarityTheme.color};">${escapeHtml(
            rarityTheme.rarity
          )}</span>
                    <small class="muted portfolio-condition">${escapeHtml(conditionLabel)}</small>
                    <small class="muted">Qty ${Number(item.quantity || 0)}</small>
                    <span class="status-badge signal-${escapeHtml(signalBand)}">${escapeHtml(signalLabel)}</span>
                  </div>
                </div>
                <div class="portfolio-desktop-card-media" style="--rarity-color: ${rarityTheme.color};">
                  <img
                    class="portfolio-desktop-card-thumb"
                    src="${escapeHtml(itemImageUrl)}"
                    alt="${escapeHtml(item.marketHashName || "CS2 item")}"
                    loading="lazy"
                    onerror="this.onerror=null;this.src='${escapeHtml(fallbackImage)}';"
                  />
                </div>
              </header>
              <div class="portfolio-card-core">
                <p class="portfolio-core-price">
                  <span>Current Price</span>
                  <strong>${formatMoney(item.currentPrice)}</strong>
                </p>
                <p class="portfolio-core-move ${sevenDayClass}">
                  <span>7D Change</span>
                  <strong class="pnl-text ${sevenDayClass}">${formatPercent(
            item.sevenDayChangePercent
          )}</strong>
                </p>
              </div>
              <div class="row portfolio-desktop-card-actions">
                <button
                  type="button"
                  class="inspect-skin-btn btn-primary"
                  data-steam-item-id="${escapeHtml(item.primarySteamItemId || "")}"
                  ${item.primarySteamItemId ? "" : "disabled"}
                >
                  Inspect
                </button>
                <button
                  type="button"
                  class="ghost-btn compare-market-btn btn-secondary"
                  data-skin-id="${skinId}"
                >
                  Compare
                </button>
                <button
                  type="button"
                  class="ghost-btn sell-suggestion-btn btn-tertiary"
                  data-skin-id="${skinId}"
                >
                  Sell Suggestion
                </button>
              </div>
            </article>
          `;
          portfolioCardMarkupCache.desktop.set(cacheKey, markup);
          trimPortfolioCardCache(portfolioCardMarkupCache.desktop);
          return markup;
        })
        .join("")}
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
                Compare
              </button>
            </div>
          </td>
        </tr>
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
    return `
      <div class="chart-empty-shell" role="status" aria-live="polite">
        <p class="empty-state">No history yet. Sync inventory to create data points.</p>
      </div>
    `;
  }

  const currencyCode = state.portfolio?.currency || state.currency;
  const cacheKey = `${state.historyDays}|${currencyCode}|${points
    .map((point) => `${point.date}:${point.value.toFixed(4)}`)
    .join("|")}`;
  if (historyChartCache.key === cacheKey) {
    return historyChartCache.markup;
  }

  const values = points.map((point) => point.value);
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const first = Number(values[0] || 0);
  const last = Number(values[values.length - 1] || 0);
  const change = last - first;
  const trendClass = change >= 0 ? "up" : "down";
  const trendPercent = first > 0 ? (change / first) * 100 : null;

  const viewWidth = 920;
  const viewHeight = 208;
  const padX = 50;
  const padY = 20;
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
  const hoverDots = coords
    .map(
      (coord, index) => `
        <circle
          cx="${coord.x.toFixed(2)}"
          cy="${coord.y.toFixed(2)}"
          r="8"
          class="value-chart-hover-dot"
          tabindex="0"
        >
          <title>${escapeHtml(points[index].date)} | ${escapeHtml(formatMoney(points[index].value, currencyCode))}</title>
        </circle>
      `
    )
    .join("");
  const hotspotDots = coords
    .map((coord, index) => {
      const left = (coord.x / viewWidth) * 100;
      const top = (coord.y / viewHeight) * 100;
      return `
        <span class="value-chart-hotspot" style="left:${left.toFixed(2)}%;top:${top.toFixed(2)}%;">
          <span class="value-chart-hotspot-tooltip">
            <strong>${escapeHtml(points[index].date)}</strong>
            <small>${escapeHtml(formatMoney(points[index].value, currencyCode))}</small>
          </span>
        </span>
      `;
    })
    .join("");

  const markup = `
    <div class="history-toolbar history-toolbar-terminal">
      <div class="history-toolbar-left">
        <h3>${state.historyDays === 180 ? "6-Month Performance" : `${state.historyDays}-Day Performance`}</h3>
        <div class="history-range">
          ${HISTORY_RANGE_OPTIONS
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
      </div>
      <div class="history-toolbar-right">
        <div class="history-summary ${trendClass}">
          <span>Range change</span>
          <strong>${formatSignedMoney(change, currencyCode)} (${formatPercent(trendPercent)})</strong>
        </div>
        <div class="history-summary">
          <span>Min/Max</span>
          <strong>${formatMoney(min, currencyCode)} - ${formatMoney(max, currencyCode)}</strong>
        </div>
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
        ${hoverDots}
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
      <div class="value-chart-hotspots" aria-hidden="true">
        ${hotspotDots}
      </div>
      <div class="value-chart-meta">
        <span>Points: <strong>${points.length}</strong></span>
        <span>Latest: <strong>${formatMoney(last, currencyCode)}</strong></span>
        <span>Peak: <strong>${formatMoney(max, currencyCode)}</strong></span>
        <span>Floor: <strong>${formatMoney(min, currencyCode)}</strong></span>
        <span class="${trendClass}">Momentum: <strong>${formatPercent(trendPercent)}</strong></span>
      </div>
    </div>
  `;
  historyChartCache = {
    key: cacheKey,
    markup
  };
  return markup;
}

function renderDashboardKpiBar() {
  const portfolio = state.portfolio || {};
  const currencyCode = portfolio.currency || state.currency;
  const signal = buildPortfolioSignals();
  const pricingModeLabel = getPricingModeLabel(portfolio?.pricing?.mode || state.pricingMode);
  const totalValue = Number(portfolio.totalValue || 0);
  const sevenDay = Number(portfolio.sevenDayChangePercent || 0);
  const unrealized = Number(portfolio.unrealizedProfit || 0);
  const readyCount = Math.max(
    Number(signal.holdingsCount || 0) - Number(signal.unpricedItems || 0) - Number(signal.staleItems || 0),
    0
  );
  const refreshMarker = state.syncSummary?.syncedAt || portfolio?.updatedAt || portfolio?.generatedAt || "";
  const syncCooldownSeconds = getSyncCooldownSecondsRemaining();
  const refreshTone = state.portfolioLoading || state.syncingInventory ? "warning" : "neutral";
  const refreshText = state.syncingInventory
    ? "Syncing inventory..."
    : syncCooldownSeconds > 0
      ? `Sync rate limited (${formatCountdownLabel(syncCooldownSeconds)})`
    : state.portfolioLoading
      ? "Refreshing snapshot..."
      : refreshMarker
        ? `Updated ${formatRelativeTime(refreshMarker)}`
        : "Live snapshot";

  const currencyOptions = SUPPORTED_CURRENCIES.map(
    (code) => `<option value="${code}" ${code === state.currency ? "selected" : ""}>${code}</option>`
  ).join("");

  const pricingChip = `
    <span class="kpi-chip kpi-pricing-chip" title="Current valuation source">
      <span>Pricing</span>
      <strong>${escapeHtml(pricingModeLabel)}</strong>
    </span>
  `;
  const controls = `
    <div class="kpi-tertiary-row">
      ${pricingChip}
      <span class="kpi-readiness-badge" title="Priced and fresh positions">
        Ready <strong>${readyCount}/${Number(signal.holdingsCount || 0)}</strong>
      </span>
    </div>
    <div class="kpi-control-row">
      <label class="currency-picker kpi-currency-picker">
        Currency
        <select id="currency-select">${currencyOptions}</select>
      </label>
    </div>
  `;

  return `
    <section class="grid">
      <div class="dashboard-kpi-anchor">
        ${renderKPIBar({
          className: "dashboard-kpi-sticky",
          rootAttrs: 'data-dashboard-kpi="1"',
          status: `
            <span class="kpi-refresh-dot tone-${escapeHtml(refreshTone)}" aria-hidden="true"></span>
            <span>${escapeHtml(refreshText)}</span>
          `,
          controls,
          items: [
            {
              label: "Total Portfolio Value",
              value: formatMoney(totalValue, currencyCode),
              tone: "neutral",
              primary: true,
              className: "kpi-primary"
            },
            {
              label: "7D Change",
              value: formatPercent(sevenDay),
              tone: sevenDay >= 0 ? "positive" : "negative"
            },
            {
              label: "Unrealized P/L",
              value: formatSignedMoney(unrealized, currencyCode),
              tone: unrealized >= 0 ? "positive" : "negative"
            }
          ]
        })}
      </div>
    </section>
  `;
}

function renderDashboardChartSkeleton() {
  return `
    <div class="chart-skeleton-shell" aria-hidden="true">
      <div class="chart-skeleton-toolbar">
        <span></span>
        <span></span>
      </div>
      <div class="chart-skeleton-plot"></div>
      <div class="chart-skeleton-meta">
        <span></span>
        <span></span>
        <span></span>
      </div>
    </div>
  `;
}

function renderDashboardHero() {
  const portfolio = state.portfolio || {};
  const pricingModeLabel = getPricingModeLabel(portfolio?.pricing?.mode || state.pricingMode);
  const infoTooltipId = "dashboard-hero-info-tip";
  const infoOpen = state.tooltip.openId === infoTooltipId;

  return `
    <section class="grid">
      ${renderPanel({
        wide: true,
        sectionId: "dashboard-hero-panel",
        className: "hero-panel dashboard-hero-panel",
        body: `
          <div class="dashboard-hero-compact" data-dashboard-hero="1">
            <div class="dashboard-hero-copy">
              <p class="eyebrow">Dashboard</p>
              <div class="dashboard-hero-title-row">
                <h1>Portfolio Decision Center</h1>
                <span class="tooltip-wrap" data-tooltip-wrap>
                  <button
                    type="button"
                    class="tooltip-toggle"
                    data-tooltip-toggle="${escapeHtml(infoTooltipId)}"
                    aria-label="Show dashboard context"
                    aria-describedby="${escapeHtml(infoTooltipId)}"
                    aria-expanded="${infoOpen ? "true" : "false"}"
                  >
                    i
                  </button>
                  <span
                    id="${escapeHtml(infoTooltipId)}"
                    role="tooltip"
                    class="tooltip-bubble ${infoOpen ? "open" : ""}"
                  >
                    <strong>Dashboard intent</strong>
                    <small>Primary KPIs stay pinned while deep analytics stay collapsible.</small>
                    <small>Current valuation mode: ${escapeHtml(pricingModeLabel)}</small>
                  </span>
                </span>
              </div>
            </div>
            <div class="dashboard-hero-actions">
              <button type="button" class="tab-jump-btn btn-primary" data-tab-target="portfolio">Open Portfolio</button>
              <button type="button" class="ghost-btn tab-jump-btn btn-secondary dashboard-chip-btn" data-tab-target="alerts">Alerts</button>
              <button type="button" class="ghost-btn tab-jump-btn btn-tertiary dashboard-chip-btn" data-tab-target="trades">Transactions</button>
            </div>
          </div>
        `
      })}
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
  const roiClass = Number(result?.roiPercent || 0) >= 0 ? "up" : "down";
  const resultMarkup = result
    ? `
      <div class="calc-result roi-result ${roiClass}">
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
        <form id="trade-calc-form" class="trade-calc-grid compact-calc">
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
          <button type="submit" class="btn-primary" ${state.tradeCalc.loading ? "disabled" : ""}>
            ${state.tradeCalc.loading ? "Calculating..." : "Calculate ROI"}
          </button>
        </form>
        ${resultMarkup}
      </article>
    </section>
  `;
}

function buildTransactionPreview(type, quantityValue, unitPriceValue, commissionPercentValue) {
  const qty = Math.max(Number(quantityValue || 0), 0);
  const unitPrice = Math.max(Number(unitPriceValue || 0), 0);
  const commissionPercent = Math.max(Number(commissionPercentValue || 0), 0);
  const gross = qty * unitPrice;
  const commissionAmount = gross * (commissionPercent / 100);
  const net = Math.max(gross - commissionAmount, 0);
  const cashImpact = String(type || "buy").toLowerCase() === "sell" ? net : -gross;
  return {
    gross,
    commissionAmount,
    net,
    cashImpact
  };
}

function syncTransactionPreviewFromInputs() {
  const type = String(document.querySelector("#tx-type")?.value || state.txForm.type || "buy");
  const quantityValue = document.querySelector("#tx-quantity")?.value ?? state.txForm.quantity;
  const unitPriceValue = document.querySelector("#tx-unit-price")?.value ?? state.txForm.unitPrice;
  const commissionValue =
    document.querySelector("#tx-commission")?.value ?? state.txForm.commissionPercent;
  const preview = buildTransactionPreview(type, quantityValue, unitPriceValue, commissionValue);

  const grossEl = document.querySelector('[data-tx-preview="gross"]');
  const commissionEl = document.querySelector('[data-tx-preview="commission"]');
  const netEl = document.querySelector('[data-tx-preview="net"]');
  const impactEl = document.querySelector('[data-tx-preview="impact"]');
  const titleEl = document.querySelector('[data-tx-preview="title"]');

  if (titleEl) {
    titleEl.textContent = type === "sell" ? "Sell Order" : "Buy Order";
  }
  if (grossEl) grossEl.textContent = formatMoney(preview.gross, "USD");
  if (commissionEl) commissionEl.textContent = formatMoney(preview.commissionAmount, "USD");
  if (netEl) netEl.textContent = formatMoney(preview.net, "USD");
  if (impactEl) {
    impactEl.textContent = formatSignedMoney(preview.cashImpact, "USD");
    impactEl.classList.remove("up", "down");
    impactEl.classList.add(preview.cashImpact >= 0 ? "up" : "down");
  }
}

function syncTransactionEditPreviewFromInputs() {
  if (!state.txEditModal.open) return;
  const type = String(document.querySelector("#tx-edit-type")?.value || state.txEditModal.type || "buy");
  const quantityValue = document.querySelector("#tx-edit-quantity")?.value ?? state.txEditModal.quantity;
  const unitPriceValue = document.querySelector("#tx-edit-unit-price")?.value ?? state.txEditModal.unitPrice;
  const commissionValue =
    document.querySelector("#tx-edit-commission")?.value ?? state.txEditModal.commissionPercent;
  const preview = buildTransactionPreview(type, quantityValue, unitPriceValue, commissionValue);

  const grossEl = document.querySelector('[data-tx-edit-preview="gross"]');
  const commissionEl = document.querySelector('[data-tx-edit-preview="commission"]');
  const netEl = document.querySelector('[data-tx-edit-preview="net"]');
  const impactEl = document.querySelector('[data-tx-edit-preview="impact"]');

  if (grossEl) grossEl.textContent = formatMoney(preview.gross, "USD");
  if (commissionEl) commissionEl.textContent = formatMoney(preview.commissionAmount, "USD");
  if (netEl) netEl.textContent = formatMoney(preview.net, "USD");
  if (impactEl) {
    impactEl.textContent = formatSignedMoney(preview.cashImpact, "USD");
    impactEl.classList.remove("up", "down");
    impactEl.classList.add(preview.cashImpact >= 0 ? "up" : "down");
  }
}

function renderTransactionManager() {
  const holdings = getHoldingsList();
  const selectedSkinId = state.txForm.skinId || String(holdings[0]?.skinId || "");
  const itemNameBySkinId = Object.fromEntries(
    holdings.map((item) => [Number(item.skinId), item.marketHashName])
  );

  const itemOptions = buildHoldingOptions(selectedSkinId);
  const txPreview = buildTransactionPreview(
    state.txForm.type,
    state.txForm.quantity,
    state.txForm.unitPrice,
    state.txForm.commissionPercent
  );

  const txPaginated = getFilteredTransactions();
  const rows = txPaginated.items;
  const rowsMarkup = rows.length
    ? rows
        .map((tx) => {
          const txId = Number(tx.id);
          const type = String(tx.type || "buy").toLowerCase();
          const skinId = Number(tx.skin_id || 0);
          const marketHashName = itemNameBySkinId[skinId] || `Skin #${skinId}`;
          const fallbackNet =
            Number(tx.quantity || 0) *
            Number(tx.unit_price || 0) *
            (type === "sell" ? 1 - Number(tx.commission_percent || 0) / 100 : 1);
          const netTotal = tx.net_total == null ? fallbackNet : Number(tx.net_total || 0);
          const cashImpact = type === "sell" ? netTotal : -Math.abs(netTotal);
          const cashClass = cashImpact >= 0 ? "up" : "down";
          return `
            <tr class="tx-row ${cashClass}">
              <td>${escapeHtml(String(tx.executed_at || "").slice(0, 10))}</td>
              <td><span class="status-badge ${escapeHtml(type)}">${escapeHtml(toTitle(type))}</span></td>
              <td>${escapeHtml(marketHashName)}</td>
              <td>${Number(tx.quantity || 0)}</td>
              <td>${formatMoney(tx.unit_price, "USD")} USD</td>
              <td>${Number(tx.commission_percent || 0).toFixed(2)}%</td>
              <td>${tx.net_total == null ? "-" : `${formatMoney(tx.net_total, "USD")} USD`}</td>
              <td><strong class="pnl-text ${cashClass}">${formatSignedMoney(cashImpact, "USD")}</strong></td>
              <td>
                <div class="row">
                  <button type="button" class="ghost-btn tx-edit-btn" data-tx-id="${txId}">Edit</button>
                  <button type="button" class="ghost-btn tx-delete-btn" data-tx-id="${txId}">Delete</button>
                </div>
              </td>
            </tr>
          `;
        })
        .join("")
    : '<tr><td colspan="9" class="muted empty-table-cell">No buy/sell transactions yet.</td></tr>';

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
          Trading journal controls cost basis, realized/unrealized P/L, and inventory attribution.
          Values are stored in <strong>USD</strong> for consistency.
        </p>
        <div class="tx-journal-shell">
          <form id="tx-form" class="tx-form-grid">
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
            <button type="submit" class="btn-primary" ${state.txSubmitting || !holdings.length ? "disabled" : ""}>
              ${state.txSubmitting ? "Saving..." : "Save Transaction"}
            </button>
          </form>
          <aside class="tx-live-preview">
            <p class="eyebrow">Live Preview</p>
            <h3 data-tx-preview="title">${state.txForm.type === "sell" ? "Sell Order" : "Buy Order"}</h3>
            <div class="calc-result compact">
              <p><span>Gross</span><strong data-tx-preview="gross">${formatMoney(txPreview.gross, "USD")}</strong></p>
              <p><span>Commission</span><strong data-tx-preview="commission">${formatMoney(txPreview.commissionAmount, "USD")}</strong></p>
              <p><span>Net</span><strong data-tx-preview="net">${formatMoney(txPreview.net, "USD")}</strong></p>
              <p><span>Cash Impact</span><strong data-tx-preview="impact" class="pnl-text ${
                txPreview.cashImpact >= 0 ? "up" : "down"
              }">${formatSignedMoney(
                txPreview.cashImpact,
                "USD"
              )}</strong></p>
            </div>
          </aside>
        </div>
        <form id="tx-csv-form" class="tx-csv-grid">
          <label>Bulk CSV
            <input id="tx-csv-file" type="file" accept=".csv,text/csv" />
            <small class="field-help">Header: <code>skinId,type,quantity,unitPrice[,commissionPercent,executedAt]</code></small>
          </label>
          <button type="submit" class="ghost-btn" ${state.csvImport.running ? "disabled" : ""}>
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
        <table class="tx-journal-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Type</th>
              <th>Item</th>
              <th>Qty</th>
              <th>Unit Price</th>
              <th>Commission</th>
              <th>Net Total</th>
              <th>Cash Impact</th>
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

function renderTransactionEditModal() {
  const modal = state.txEditModal;
  if (!modal.open || !modal.id) return "";

  const preview = buildTransactionPreview(
    modal.type,
    modal.quantity,
    modal.unitPrice,
    modal.commissionPercent
  );

  return `
    <div class="tx-edit-overlay" data-tx-edit-overlay>
      <section
        class="tx-edit-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="tx-edit-title"
        tabindex="-1"
      >
        <header class="tx-edit-header">
          <div>
            <p class="tx-edit-title" id="tx-edit-title">Edit Transaction</p>
            <p class="tx-edit-subtitle">Update journal fields without leaving the table.</p>
          </div>
          <button type="button" class="ghost-btn" data-tx-edit-close>Close</button>
        </header>
        <div class="tx-edit-body">
          <form id="tx-edit-form" class="tx-edit-form-grid">
            <label>Type
              <select id="tx-edit-type">
                <option value="buy" ${modal.type === "buy" ? "selected" : ""}>Buy</option>
                <option value="sell" ${modal.type === "sell" ? "selected" : ""}>Sell</option>
              </select>
            </label>
            <label>Quantity
              <input id="tx-edit-quantity" type="number" step="1" min="1" value="${escapeHtml(
                modal.quantity
              )}" />
            </label>
            <label>Unit Price (USD)
              <input id="tx-edit-unit-price" type="number" step="0.01" min="0" value="${escapeHtml(
                modal.unitPrice
              )}" />
            </label>
            <label>Commission %
              <input id="tx-edit-commission" type="number" step="0.01" min="0" max="99.99" value="${escapeHtml(
                modal.commissionPercent
              )}" />
            </label>
            <label>Executed At
              <input id="tx-edit-executed-at" type="datetime-local" value="${escapeHtml(
                modal.executedAt
              )}" />
            </label>
            <div class="calc-result compact">
              <p><span>Gross</span><strong data-tx-edit-preview="gross">${formatMoney(preview.gross, "USD")}</strong></p>
              <p><span>Commission</span><strong data-tx-edit-preview="commission">${formatMoney(
                preview.commissionAmount,
                "USD"
              )}</strong></p>
              <p><span>Net</span><strong data-tx-edit-preview="net">${formatMoney(preview.net, "USD")}</strong></p>
              <p><span>Cash Impact</span><strong data-tx-edit-preview="impact" class="pnl-text ${
                preview.cashImpact >= 0 ? "up" : "down"
              }">${formatSignedMoney(
                preview.cashImpact,
                "USD"
              )}</strong></p>
            </div>
            <div class="row">
              <button type="submit" class="btn-primary" ${modal.submitting ? "disabled" : ""}>
                ${modal.submitting ? "Saving..." : "Update Transaction"}
              </button>
              <button type="button" class="ghost-btn" data-tx-edit-close>Cancel</button>
            </div>
          </form>
        </div>
      </section>
    </div>
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
      const code = String(alert.code || "").toUpperCase();
      const action =
        code === "SYNC_FAILED"
          ? '<button type="button" id="refresh-btn" class="ghost-btn btn-secondary">Refresh Prices</button>'
          : '<button type="button" class="ghost-btn tab-jump-btn btn-tertiary" data-tab-target="alerts">Open Alerts Center</button>';
      return `
        <article class="dashboard-alert-card ${escapeHtml(severity)}">
          <div class="dashboard-alert-copy">
            <span class="status-badge ${escapeHtml(severity === "warning" ? "unpriced" : severity)}">${escapeHtml(
        toTitle(severity)
      )}</span>
            <p>${escapeHtml(alert.message || "Portfolio alert")}</p>
          </div>
          <div class="dashboard-alert-actions">
            ${action}
          </div>
        </article>
      `;
    })
    .join("");

  return `
    <section class="grid">
      <article class="panel wide">
        <h2>Action Alerts</h2>
        <div class="dashboard-alert-list">${rows}</div>
      </article>
    </section>
  `;
}

function renderAuthNotices() {
  return "";
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
        <details class="analytics-collapsible">
          <summary>
            <span>Advanced Analytics</span>
            <small>Compact by default, expand for deep diagnostics</small>
          </summary>
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
        </details>
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

function renderDashboardArbitragePanel() {
  const arbitrage = state.portfolio?.arbitrage || {};
  const opportunities = Array.isArray(arbitrage?.topOpportunities)
    ? arbitrage.topOpportunities
    : [];
  const currencyCode = state.portfolio?.currency || state.currency;

  const body = opportunities.length
    ? `
      <div class="dashboard-arb-list">
        ${opportunities
          .slice(0, 5)
          .map((row) => {
            const score = Number(row?.opportunityScore || 0);
            const scoreTone = getOpportunityScoreTone(score);
            const scoreLabel = formatOpportunityLabel(row?.scoreCategory, score);
            const skinId = Number(row?.itemId || row?.skinId || 0);
            const isRowClickable = Number.isInteger(skinId) && skinId > 0;
            const itemName = String(row?.itemName || "Tracked Item");
            const rowInteractionAttributes = isRowClickable
              ? `data-skin-id="${escapeHtml(String(skinId))}" tabindex="0" role="button" aria-label="Compare ${escapeHtml(
                  itemName
                )}"`
              : `aria-disabled="true"`;
            return `
              <article class="dashboard-arb-row ${isRowClickable ? "dashboard-arb-row-clickable" : ""}" ${rowInteractionAttributes}>
                <strong class="dashboard-arb-item">${escapeHtml(row?.itemName || "Tracked Item")}</strong>
                <p class="dashboard-arb-leg">
                  <span>Buy ${escapeHtml(formatMarketSourceLabel(row?.buyMarket))}</span>
                  <strong>${formatMoney(row?.buyPrice, currencyCode)}</strong>
                </p>
                <p class="dashboard-arb-leg">
                  <span>Sell ${escapeHtml(formatMarketSourceLabel(row?.sellMarket))}</span>
                  <strong>${formatMoney(row?.sellNet, currencyCode)}</strong>
                </p>
                <p class="dashboard-arb-profit up">
                  <span>Profit</span>
                  <strong>${formatSignedMoney(row?.profit, currencyCode)} (${formatPercent(
                row?.spreadPercent
              )})</strong>
                </p>
                <p class="dashboard-arb-score">
                  <span>Score</span>
                  <strong class="score-pill ${escapeHtml(scoreTone)}">${escapeHtml(
                `${formatNumber(score, 0)}/100`
              )}</strong>
                  <small>${escapeHtml(scoreLabel)}</small>
                </p>
              </article>
            `;
          })
          .join("")}
      </div>
    `
    : '<p class="muted">No profitable arbitrage opportunities detected right now.</p>';

  return renderPanel({
    className: "dashboard-arbitrage-panel",
    title: "Arbitrage Opportunities",
    subtitle: "Top 5 opportunities sorted by profit.",
    actions: `
      <button
        type="button"
        class="ghost-btn tab-jump-btn btn-secondary"
        data-tab-target="market"
      >
        View All Opportunities
      </button>
    `,
    body
  });
}

function renderAnalytics() {
  const analytics = state.portfolio?.analytics;
  if (!analytics) return "";

  const topGainer = analytics?.leaders?.topGainer || null;
  const topLoser = analytics?.leaders?.topLoser || null;
  const holdings = getHoldingsList();
  const resolveMover = (mover) => {
    if (!mover) return null;
    const rawSkinId = Number(mover.skinId || mover.id || 0);
    const holdingById =
      Number.isInteger(rawSkinId) && rawSkinId > 0
        ? holdings.find((item) => Number(item.skinId) === rawSkinId) || null
        : null;
    const holdingByName =
      !holdingById && mover.marketHashName
        ? holdings.find(
            (item) =>
              String(item.marketHashName || "").trim().toLowerCase() ===
              String(mover.marketHashName || "").trim().toLowerCase()
          ) || null
        : null;
    const holding = holdingById || holdingByName;
    const skinId = Number(holding?.skinId || rawSkinId || 0);
    const moverItem = holding || mover;
    const imageUrl = getItemImageUrl(moverItem);
    const fallbackImage = isCaseLikeItem(moverItem) ? defaultCaseImage : defaultSkinImage;
    const inspectId = String(holding?.primarySteamItemId || mover.primarySteamItemId || "").trim();
    return {
      skinId,
      name: mover.marketHashName || holding?.marketHashName || `Skin #${skinId}`,
      change: mover.sevenDayChangePercent,
      price: Number(holding?.currentPrice ?? mover.currentPrice ?? 0),
      lineValue: Number(mover.lineValue ?? holding?.lineValue ?? 0),
      imageUrl,
      fallbackImage,
      inspectId
    };
  };
  const gainer = resolveMover(topGainer);
  const loser = resolveMover(topLoser);
  const currencyCode = state.portfolio?.currency || state.currency;
  const buildMoverCard = (mover, label, trendClass) => {
    if (!mover) {
      return `
        <article class="mover-card ${trendClass}">
          <span>${escapeHtml(label)}</span>
          <div class="mover-head">
            <div class="mover-thumb placeholder" aria-hidden="true"></div>
            <strong>N/A</strong>
          </div>
          <p>-</p>
          <small>No mover data in selected range.</small>
        </article>
      `;
    }

    return `
      <article class="mover-card ${trendClass}">
        <span>${escapeHtml(label)}</span>
        <div class="mover-head">
          <img
            src="${escapeHtml(mover.imageUrl)}"
            alt="${escapeHtml(mover.name)}"
            loading="lazy"
            onerror="this.onerror=null;this.src='${escapeHtml(mover.fallbackImage)}';"
          />
          <strong class="mover-name" title="${escapeHtml(mover.name)}">${escapeHtml(mover.name)}</strong>
        </div>
        <p>${escapeHtml(formatPercent(mover.change))}</p>
        <small>Price ${formatMoney(mover.price, currencyCode)}</small>
        <div class="row mover-actions">
          <button type="button" class="inspect-skin-btn btn-primary" data-steam-item-id="${escapeHtml(
            mover.inspectId
          )}" ${mover.inspectId ? "" : "disabled"}>Inspect</button>
          <button
            type="button"
            class="ghost-btn top-mover-compare-btn btn-secondary"
            data-skin-id="${mover.skinId}"
            ${mover.skinId > 0 ? "" : "disabled"}
          >
            Compare
          </button>
        </div>
      </article>
    `;
  };
  const chartBody =
    state.portfolioLoading && !state.history.length
      ? renderDashboardChartSkeleton()
      : renderHistoryChart();

  return `
    <section class="grid dashboard-operations-grid">
      ${renderPanel({
        className: "dashboard-chart-panel",
        body: chartBody
      })}
      ${renderPanel({
        className: "dashboard-movers-panel",
        title: "Top Movers",
        subtitle: "Actionable gainers and losers in the selected range.",
        body: `
          <div class="movers-grid">
            ${buildMoverCard(gainer, "Top Gainer", "up")}
            ${buildMoverCard(loser, "Top Loser", "down")}
          </div>
        `
      })}
      ${renderDashboardArbitragePanel()}
    </section>
  `;
}

function renderDashboardDeepAnalytics() {
  const analytics = state.portfolio?.analytics;
  if (!analytics) return "";

  const detailsOpen = Boolean(state.dashboardUi.detailsExpanded);
  const signal = buildPortfolioSignals();
  const breadth = analytics?.breadth || {};
  const unpricedItems = Number(signal.unpricedItems || 0);
  const staleItems = Number(signal.staleItems || 0);
  const allFresh = unpricedItems === 0 && staleItems === 0;

  const healthTiles = [
    renderStatTile({
      label: "Liquidity",
      value: signal.liquidityScore == null ? "-" : `${Number(signal.liquidityScore)}/100`,
      hint: "Fresh and priced coverage",
      tone: signal.liquidityBand === "low" ? "negative" : signal.liquidityBand === "medium" ? "warning" : "positive"
    }),
    renderStatTile({
      label: "Risk",
      value: `${Number(signal.riskScore || 0)}/100`,
      hint: `Concentration ${toTitle(analytics.concentrationRisk || "unknown")}`,
      tone: signal.riskBand === "high" ? "negative" : signal.riskBand === "medium" ? "warning" : "positive"
    })
  ];
  if (!allFresh) {
    healthTiles.push(
      renderStatTile({
        label: "Unpriced",
        value: escapeHtml(formatNumber(unpricedItems, 0)),
        hint: "Missing valuation source",
        tone: unpricedItems > 0 ? "warning" : "neutral"
      }),
      renderStatTile({
        label: "Stale",
        value: escapeHtml(formatNumber(staleItems, 0)),
        hint: "Older than freshness threshold",
        tone: staleItems > 0 ? "warning" : "neutral"
      })
    );
  }

  const structureTiles = [
    renderStatTile({
      label: "Holdings",
      value: escapeHtml(formatNumber(analytics.holdingsCount, 0)),
      hint: "Open positions",
      tone: "neutral"
    }),
    renderStatTile({
      label: "Top 1 Weight",
      value: escapeHtml(formatPercent(analytics.concentrationTop1Percent)),
      hint: "Single-position concentration",
      tone: "neutral"
    }),
    renderStatTile({
      label: "Top 3 Weight",
      value: escapeHtml(formatPercent(analytics.concentrationTop3Percent)),
      hint: "Cluster concentration",
      tone: "neutral"
    }),
    renderStatTile({
      label: "Effective Holdings",
      value: escapeHtml(formatNumber(analytics.effectiveHoldings)),
      hint: "Diversification footprint",
      tone: "neutral"
    }),
    renderStatTile({
      label: "Weighted 7D Move",
      value: escapeHtml(formatPercent(analytics.weightedAverageMove7dPercent)),
      hint: "Portfolio-level momentum",
      tone: Number(analytics.weightedAverageMove7dPercent || 0) >= 0 ? "positive" : "negative"
    }),
    renderStatTile({
      label: "Breadth",
      value: escapeHtml(formatPercent(breadth.advancerRatioPercent)),
      hint: "Advancers ratio",
      tone: Number(breadth.advancerRatioPercent || 0) >= 50 ? "positive" : "warning"
    })
  ];

  const detailPanels = detailsOpen
    ? `
      ${renderPnlSummary()}
      ${renderManagementSummary()}
      ${renderAdvancedAnalytics()}
      ${renderBacktestPanel()}
    `
    : "";

  return `
    <section class="grid">
      ${renderPanel({
        wide: true,
        className: "dashboard-deep-panel",
        title: "Deep Analytics",
        subtitle: "Liquidity, risk, structure, and advanced diagnostics.",
        actions: `
          <button
            type="button"
            class="ghost-btn dashboard-details-toggle"
            data-dashboard-details-toggle="1"
            aria-expanded="${detailsOpen ? "true" : "false"}"
          >
            ${detailsOpen ? "Hide details" : "Show details"}
          </button>
        `,
        body: `
          <div class="dashboard-deep-summary">
            ${renderStatGrid({
              className: "dashboard-health-grid",
              tiles: healthTiles
            })}
            ${
              allFresh
                ? '<span class="status-badge real dashboard-fresh-badge">All priced / Fresh</span>'
                : ""
            }
          </div>
          <div class="dashboard-deep-content ${detailsOpen ? "open" : ""}" ${detailsOpen ? "" : "hidden"}>
            <h3>Portfolio Structure</h3>
            ${renderStatGrid({
              className: "dashboard-structure-grid",
              tiles: structureTiles
            })}
          </div>
        `
      })}
    </section>
    ${detailPanels}
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
  return `
    <nav class="sidebar-nav">
      ${APP_TABS
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
      <div class="alert-card-list">
        ${alertRows
          .map((alert) => {
            const nextCheckRaw = alert.nextCheckAt || alert.lastCheckedAt || alert.updatedAt || alert.createdAt;
            const triggerBits = [
              alert.targetPrice == null ? null : `Target ${formatMoney(alert.targetPrice, "USD")}`,
              alert.percentChangeThreshold == null
                ? null
                : `Move ${formatPercent(alert.percentChangeThreshold)}`,
              `Direction ${toTitle(alert.direction || "both")}`
            ]
              .filter(Boolean)
              .join(" | ");
            return `
              <article class="alert-config-card">
                <header>
                  <div>
                    <p class="alert-config-title">${escapeHtml(alert.marketHashName || `Skin #${alert.skinId}`)}</p>
                    <small class="muted">Next check: ${escapeHtml(
                      nextCheckRaw ? `${formatDateTime(nextCheckRaw)} (${formatRelativeTime(nextCheckRaw)})` : "pending"
                    )}</small>
                  </div>
                  ${
                    alert.enabled
                      ? '<span class="status-badge real">Enabled</span>'
                      : '<span class="status-badge stale">Paused</span>'
                  }
                </header>
                <p class="alert-config-logic">${escapeHtml(triggerBits)}</p>
                <p class="muted">Cooldown ${Number(alert.cooldownMinutes || 0)}m</p>
                <div class="row alert-config-actions">
                  <button type="button" class="ghost-btn alert-edit-btn btn-secondary" data-alert-id="${Number(
                    alert.id
                  )}">Edit</button>
                  <button
                    type="button"
                    class="ghost-btn alert-toggle-btn btn-tertiary"
                    data-alert-id="${Number(alert.id)}"
                    data-enabled="${alert.enabled ? "false" : "true"}"
                  >
                    ${alert.enabled ? "Pause" : "Enable"}
                  </button>
                  <button type="button" class="ghost-btn alert-delete-btn btn-tertiary" data-alert-id="${Number(
                    alert.id
                  )}">Delete</button>
                </div>
              </article>
            `;
          })
          .join("")}
      </div>
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
        <p class="helper-text">Target, trigger, and direction are evaluated against <strong>USD</strong> pricing. Use cooldown to prevent notification spam.</p>
        <form id="alert-form" class="alert-form-grid">
          <fieldset class="alert-fieldset">
            <legend>Target</legend>
            <label>Item
              <select id="alert-skin-id" ${holdings.length ? "" : "disabled"}>${alertOptions}</select>
              <small class="field-help">Select a tracked portfolio item.</small>
            </label>
            <label>Target Price (USD)
              <input id="alert-target-price" type="number" min="0" step="0.01" value="${escapeHtml(
                state.alertForm.targetPrice
              )}" />
              <small class="field-help">Trigger when market price reaches this level.</small>
            </label>
          </fieldset>
          <fieldset class="alert-fieldset">
            <legend>Trigger</legend>
            <label>% Change Trigger
              <input id="alert-percent-threshold" type="number" min="0" step="0.01" value="${escapeHtml(
                state.alertForm.percentChangeThreshold
              )}" />
              <small class="field-help">Use for momentum alerts if target price is empty.</small>
            </label>
            <label>Direction
              <select id="alert-direction">
                <option value="both" ${state.alertForm.direction === "both" ? "selected" : ""}>Both</option>
                <option value="up" ${state.alertForm.direction === "up" ? "selected" : ""}>Up</option>
                <option value="down" ${state.alertForm.direction === "down" ? "selected" : ""}>Down</option>
              </select>
              <small class="field-help">Choose whether to fire on rises, drops, or both.</small>
            </label>
          </fieldset>
          <fieldset class="alert-fieldset">
            <legend>Delivery</legend>
            <label>Cooldown (minutes)
              <input id="alert-cooldown" type="number" min="0" step="1" value="${escapeHtml(
                state.alertForm.cooldownMinutes
              )}" />
              <small class="field-help">Minimum delay before same alert can trigger again.</small>
            </label>
            <label class="alert-checkbox-label" title="Toggle whether this alert is active immediately after save.">
              Enabled
              <input id="alert-enabled" type="checkbox" ${state.alertForm.enabled ? "checked" : ""} />
            </label>
          </fieldset>
          <div class="row alert-form-actions">
            <button type="submit" class="btn-primary" ${state.alertForm.submitting || !holdings.length ? "disabled" : ""}>
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
                ? '<button id="alert-cancel-btn" type="button" class="ghost-btn btn-secondary">Cancel Edit</button>'
                : ""
            }
          </div>
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
  const scanner = state.marketTab.opportunities || createMarketOpportunitiesState();
  const scanFilters = scanner.filters || createMarketOpportunitiesState().filters;
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
  const opportunityRows = Array.isArray(scanner.items) ? scanner.items : [];
  const opportunityMarkup = opportunityRows.length
    ? `
      <table>
        <thead>
          <tr>
            <th>Item</th>
            <th>Buy Market</th>
            <th>Buy Price</th>
            <th>Sell Market</th>
            <th>Sell Net</th>
            <th>Profit</th>
            <th>Spread %</th>
            <th>Score</th>
          </tr>
        </thead>
        <tbody>
          ${opportunityRows
            .map((row) => {
              const score = Number(row?.opportunityScore || 0);
              const scoreTone = getOpportunityScoreTone(score);
              return `
                <tr>
                  <td>${escapeHtml(row?.itemName || "Tracked Item")}</td>
                  <td>${escapeHtml(formatMarketSourceLabel(row?.buyMarket))}</td>
                  <td>${formatMoney(row?.buyPrice, state.portfolio?.currency || state.currency)}</td>
                  <td>${escapeHtml(formatMarketSourceLabel(row?.sellMarket))}</td>
                  <td>${formatMoney(row?.sellNet, state.portfolio?.currency || state.currency)}</td>
                  <td><strong class="pnl-text up">${formatSignedMoney(
                    row?.profit,
                    state.portfolio?.currency || state.currency
                  )}</strong></td>
                  <td>${formatPercent(row?.spreadPercent)}</td>
                  <td>
                    <span class="score-pill ${escapeHtml(scoreTone)}">${escapeHtml(
                `${formatNumber(score, 0)}/100`
              )}</span>
                  </td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>
    `
    : '<p class="muted">No profitable opportunities for current filters.</p>';

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

      <article class="panel wide">
        <h2>Market Arbitrage Scanner</h2>
        <form id="market-opportunities-form" class="trade-calc-grid market-opportunity-form">
          <label>Min Profit
            <input
              id="market-opportunity-min-profit"
              type="number"
              min="0"
              step="0.01"
              value="${escapeHtml(scanFilters.minProfit)}"
            />
          </label>
          <label>Min Spread %
            <input
              id="market-opportunity-min-spread"
              type="number"
              min="0"
              step="0.01"
              value="${escapeHtml(scanFilters.minSpread)}"
            />
          </label>
          <label>Min Score
            <input
              id="market-opportunity-min-score"
              type="number"
              min="0"
              max="100"
              step="1"
              value="${escapeHtml(scanFilters.minScore)}"
            />
          </label>
          <label>Market
            <select id="market-opportunity-market">
              <option value="all" ${scanFilters.market === "all" ? "selected" : ""}>All</option>
              <option value="steam" ${scanFilters.market === "steam" ? "selected" : ""}>Steam</option>
              <option value="skinport" ${scanFilters.market === "skinport" ? "selected" : ""}>Skinport</option>
              <option value="csfloat" ${scanFilters.market === "csfloat" ? "selected" : ""}>CSFloat</option>
              <option value="dmarket" ${scanFilters.market === "dmarket" ? "selected" : ""}>DMarket</option>
            </select>
          </label>
          <label>Liquidity Min
            <input
              id="market-opportunity-liquidity-min"
              type="number"
              min="0"
              step="1"
              value="${escapeHtml(scanFilters.liquidityMin)}"
            />
          </label>
          <label>Sort
            <select id="market-opportunity-sort-by">
              <option value="score" ${scanFilters.sortBy === "score" ? "selected" : ""}>Score</option>
              <option value="profit" ${scanFilters.sortBy === "profit" ? "selected" : ""}>Profit</option>
              <option value="spread" ${scanFilters.sortBy === "spread" ? "selected" : ""}>Spread</option>
            </select>
          </label>
          <label class="checkbox-field">
            <span>Show Risky</span>
            <input
              id="market-opportunity-show-risky"
              type="checkbox"
              ${String(scanFilters.showRisky || "0") === "1" ? "checked" : ""}
            />
          </label>
          <label>Limit
            <input
              id="market-opportunity-limit"
              type="number"
              min="1"
              max="1000"
              step="1"
              value="${escapeHtml(scanFilters.limit)}"
            />
          </label>
          <button type="submit" ${scanner.loading ? "disabled" : ""}>
            ${scanner.loading ? "Scanning..." : "Scan Opportunities"}
          </button>
        </form>
        <p class="helper-text">
          ${
            scanner.summary
              ? `Scanned ${Number(scanner.summary.scannedItems || 0)} items. Found ${Number(
                  scanner.summary.opportunities || 0
                )} opportunities.`
              : "Scan your synced items to detect buy/sell arbitrage across Steam, Skinport, CSFloat, and DMarket."
          }
          ${
            scanner.generatedAt
              ? ` Updated ${escapeHtml(formatRelativeTime(scanner.generatedAt))}.`
              : ""
          }
        </p>
        ${scanner.error ? `<p class="muted">${escapeHtml(scanner.error)}</p>` : ""}
        ${opportunityMarkup}
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
  const syncCooldownSeconds = getSyncCooldownSecondsRemaining();
  const syncDisabled = state.syncingInventory || syncCooldownSeconds > 0;
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
    <main class="layout landing-shell">
      <nav class="topbar landing-topbar">
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
                <a class="link-btn ghost" href="/login.html">Login</a>
                <a class="link-btn btn-primary" href="/register.html">Start Free</a>
              `
          }
        </div>
      </nav>

      <section class="hero-block landing-hero">
        <div class="landing-hero-copy">
          <p class="eyebrow">SaaS for CS2 traders and collectors</p>
          <h1>Trade your CS2 inventory with portfolio-grade clarity.</h1>
          <p class="hero-copy">
            Connect Steam once, sync instantly, and monitor value, liquidity, and execution signals in a single command surface.
          </p>
          <div class="hero-actions">
            <a class="link-btn btn-primary landing-primary-cta" href="/register.html">Start Free</a>
            <a class="link-btn ghost btn-secondary" href="${escapeHtml(steamStartUrl)}">Continue with Steam</a>
            <a class="link-btn ghost btn-tertiary" href="/login.html">I already have an account</a>
          </div>
          <div class="landing-trust-grid">
            <article class="landing-trust-item">
              <span>Portfolio Sync</span>
              <strong>1-click</strong>
            </article>
            <article class="landing-trust-item">
              <span>Live Signals</span>
              <strong>7D + 24H</strong>
            </article>
            <article class="landing-trust-item">
              <span>Pricing Surface</span>
              <strong>Multi-market</strong>
            </article>
          </div>
        </div>
        <div class="hero-preview panel landing-preview">
          <h3>Why teams use it</h3>
          <ul class="bullet-list">
            <li>Decision-first dashboard for value and risk</li>
            <li>Inspect modal without losing page context</li>
            <li>Actionable alerts and transaction journal</li>
            <li>Fast workflows tuned for trading sessions</li>
          </ul>
        </div>
      </section>

      <section class="grid landing-feature-grid">
        <article class="panel landing-feature-card">
          <h2>Execution-Speed Workflow</h2>
          <p class="muted">No spreadsheets. Sync once and act with inspect, compare, and sell guidance directly in context.</p>
        </article>
        <article class="panel landing-feature-card">
          <h2>Decision-Grade Signals</h2>
          <p class="muted">Spot winners, laggards, and concentration risk with a clear KPI hierarchy and compact analytics.</p>
        </article>
        <article class="panel landing-feature-card">
          <h2>Built to Scale</h2>
          <p class="muted">Extensible architecture for deeper market providers, automation, and creator/team operations.</p>
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

function renderGlobalSyncOverlay() {
  if (!state.syncingInventory) return "";
  return `
    <div class="ui-sync-blocker-overlay" role="status" aria-live="polite" aria-busy="true">
      <div class="ui-sync-blocker-card">
        <span class="spinner ui-sync-blocker-spinner" aria-hidden="true"></span>
        <p>Syncing inventory...</p>
        <small>Please wait while we refresh your items and prices.</small>
      </div>
    </div>
  `;
}

function renderSteamSyncPanel() {
  const profile = state.authProfile || {};
  const steamLinked = Boolean(profile.steamLinked);
  const steamLinkUrl = buildSteamAuthStartUrl("link");
  const syncCooldownSeconds = getSyncCooldownSecondsRemaining();
  const syncDisabled = state.syncingInventory || syncCooldownSeconds > 0;

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
        <button id="sync-btn" ${syncDisabled ? "disabled" : ""}>
          ${
            state.syncingInventory
              ? '<span class="loading-inline"><span class="spinner"></span>Syncing inventory...</span>'
              : syncCooldownSeconds > 0
                ? `Try again in ${escapeHtml(formatCountdownLabel(syncCooldownSeconds))}`
              : "Sync Inventory"
          }
        </button>
        <a class="link-btn ghost" href="${escapeHtml(steamLinkUrl)}">Relink Steam</a>
      </div>
      ${
        state.syncingInventory
          ? '<p class="muted sync-note">Fetching inventory and market prices. This can take up to a minute.</p>'
          : syncCooldownSeconds > 0
            ? `<p class="muted sync-note">Too many sync attempts. Retry in ${escapeHtml(
                formatCountdownLabel(syncCooldownSeconds)
              )}.</p>`
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
  const showTopbarMetrics = state.activeTab !== "dashboard";

  const currencyOptions = SUPPORTED_CURRENCIES.map(
    (code) => `<option value="${code}" ${code === state.currency ? "selected" : ""}>${code}</option>`
  ).join("");

  const dashboardContent = `
    ${renderDashboardHero()}
    ${renderDashboardKpiBar()}
    ${renderAnalytics()}
    ${renderAlerts()}
    ${renderDashboardDeepAnalytics()}
  `;

  const portfolioContent = `
    <section class="grid dashboard-grid">
      ${renderSteamSyncPanel()}

      <article class="panel">
        <h2>Position Inspector</h2>
        <p class="helper-text">Paste a Steam Item ID to open a centered inspector modal without losing your current scroll position in the portfolio.</p>
        <form id="skin-form" class="form">
          <label>Steam Item ID
            <input id="steam-item-id" inputmode="numeric" pattern="[0-9]+" placeholder="e.g. 35719462921" value="${escapeHtml(state.inspectedSteamItemId)}" />
          </label>
          <button type="submit">Open Inspector</button>
        </form>
        <p class="muted">Item details, market sources, and what-if exit analytics open in the modal.</p>
      </article>
    </section>

    <section class="grid">
      <article class="panel wide">
        <h2>Portfolio Holdings</h2>
        <p class="helper-text">
          Card-first execution surface for scanning, sorting, and acting without losing your place.
        </p>
        ${renderPortfolioPricingControls()}
        ${renderSection({
          eyebrow: "Portfolio",
          title: "Execution Cards",
          description:
            "Inspect opens in a centered modal. Compare opens in a right-side drawer, so cards remain fixed with no grid stretch.",
          className: "portfolio-table-section",
          body: `
            <div class="portfolio-mobile-only">
              ${renderPortfolioMobileList()}
            </div>
            <div class="portfolio-desktop-cards">
              ${renderPortfolioDesktopCards()}
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
      ${renderMobileNav({
        title: "CS2 Portfolio Analyzer",
        drawerOpen: state.mobileDrawer.open,
        escapeHtml
      })}
      ${renderMobileDrawer({
        open: state.mobileDrawer.open,
        tabs: APP_TABS,
        activeTab: state.activeTab,
        userLabel: userEmailLabel,
        userTitle: userEmailTitle,
        loading: state.tabSwitch.loading,
        escapeHtml
      })}
      ${renderDesktopHeader(userEmailLabel, userEmailTitle)}
      <section class="app-main">
        <header class="topbar premium-topbar">
          <div class="topbar-metrics">
            ${
              showTopbarMetrics
                ? `
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
            `
                : `
              <article class="topbar-metric topbar-dashboard-note">
                <span>Dashboard</span>
                <strong>Decision terminal</strong>
                <small>Primary KPIs are pinned below the hero for data-first scanning.</small>
              </article>
            `
            }
          </div>
          <div class="top-actions">
            ${
              state.activeTab === "dashboard"
                ? '<span class="muted topbar-note">Currency and readiness controls are pinned in the KPI bar.</span>'
                : `
              <label class="currency-picker">
                Currency
                <select id="currency-select">${currencyOptions}</select>
              </label>
            `
            }
          </div>
        </header>
        <div class="tab-switch-indicator-slot" aria-live="polite">
          ${
            state.tabSwitch.loading
              ? `<div class="tab-switch-indicator" role="status">Loading ${escapeHtml(
                  toTitle(state.tabSwitch.target || state.activeTab || "tab")
                )}...</div>`
              : '<div class="tab-switch-indicator-placeholder" aria-hidden="true"></div>'
          }
        </div>
        ${renderAuthNotices()}
        ${tabContent}
      </section>
      ${renderAppFooter()}
    </main>
    ${renderInspectModalOverlay()}
    ${renderCompareDrawerOverlay()}
    ${
      state.activeTab === "portfolio" || state.portfolioControls.open
        ? renderPortfolioControlsDrawer()
        : ""
    }
    ${renderTransactionEditModal()}
    ${renderGlobalSyncOverlay()}
  `;

  ensureAppEventDelegation();
  ensureDashboardStickySync();
  focusMobileDrawerIfNeeded();
  focusPortfolioControlsIfNeeded();
  focusInspectModalIfNeeded();
  focusCompareDrawerIfNeeded();
  scheduleDashboardKpiPinnedSync();

  if (
    state.activeTab === "market" &&
    !state.marketTab.inventoryValue &&
    !state.marketTab.loading &&
    !state.marketTab.autoLoaded
  ) {
    runUiTask(() => refreshMarketInventoryValue());
  }
  if (
    state.activeTab === "market" &&
    !state.marketTab.opportunities.loading &&
    !state.marketTab.opportunities.loaded
  ) {
    runUiTask(() => refreshMarketOpportunities({ silent: true }));
  }

  if (state.activeTab === "trades") {
    syncTransactionPreviewFromInputs();
  }
  if (state.txEditModal.open) {
    syncTransactionEditPreviewFromInputs();
  }

  animateMetricCounters();
}

function render() {
  if (state.sessionBooting) {
    renderSessionBoot();
    syncBodyUiLocks();
    renderToastHost();
    return;
  }

  if (state.publicPage.steamId64) {
    state.avatarMenu.open = false;
    state.headerTabMenu.open = false;
    state.tooltip.openId = "";
    state.portfolioControls.open = false;
    state.portfolioControls.focusPending = false;
    state.inspectModal.open = false;
    state.inspectModal.focusPending = false;
    state.compareDrawer.open = false;
    state.compareDrawer.focusPending = false;
    state.compareDrawer.loading = false;
    state.txEditModal = {
      open: false,
      id: null,
      skinId: "",
      type: "buy",
      quantity: "1",
      unitPrice: "0",
      commissionPercent: "13",
      executedAt: "",
      submitting: false
    };
    renderPublicPortfolioPage();
    syncBodyUiLocks();
    renderToastHost();
    return;
  }

  if (!state.authenticated) {
    state.mobileDrawer.open = false;
    state.mobileDrawer.focusPending = false;
    state.portfolioControls.open = false;
    state.portfolioControls.focusPending = false;
    state.avatarMenu.open = false;
    state.headerTabMenu.open = false;
    state.tooltip.openId = "";
    state.inspectModal.open = false;
    state.inspectModal.focusPending = false;
    state.compareDrawer.open = false;
    state.compareDrawer.focusPending = false;
    state.compareDrawer.loading = false;
    state.txEditModal = {
      open: false,
      id: null,
      skinId: "",
      type: "buy",
      quantity: "1",
      unitPrice: "0",
      commissionPercent: "13",
      executedAt: "",
      submitting: false
    };
    renderPublicHome();
    syncBodyUiLocks();
    renderToastHost();
    return;
  }

  flushAuthNotices();
  renderApp();
  syncBodyUiLocks();
  renderToastHost();
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
  if (shouldAutoSyncInventoryOnSessionBoot()) {
    await syncInventory({ automatic: true });
  }
}

bootstrapSession().catch(() => {
  state.sessionBooting = false;
  render();
});


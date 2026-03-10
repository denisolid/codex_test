const portfolioService = require("./portfolioService");
const planService = require("./planService");
const { resolveCurrency } = require("./currencyService");
const arbitrageEngine = require("./arbitrageEngineService");
const arbitrageRules = require("../config/arbitrageRules");

const CACHE_TTL_MS = 60 * 1000;
const MAX_LIMIT = 1000;
const scanCache = new Map();
const DEFAULT_FREE_FILTERS = Object.freeze({
  minProfit:
    arbitrageEngine.DEFAULT_MIN_PROFIT_ABSOLUTE ??
    arbitrageRules.DEFAULT_MIN_PROFIT_ABSOLUTE,
  minSpreadPercent: arbitrageEngine.MIN_SPREAD_PERCENT,
  minScore: arbitrageEngine.DEFAULT_SCORE_CUTOFF ?? arbitrageRules.DEFAULT_SCORE_CUTOFF,
  liquidityMin: 0,
  showRisky: false,
  sortBy: "score",
  markets: ""
});

function toFiniteOrNull(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function clampNumber(value, fallback, min = -Infinity, max = Infinity) {
  const parsed = toFiniteOrNull(value);
  if (parsed == null) return fallback;
  return Math.min(Math.max(parsed, min), max);
}

function normalizeSortBy(value) {
  const safe = String(value || "score")
    .trim()
    .toLowerCase();
  if (safe === "spread") return "spread";
  if (safe === "profit") return "profit";
  return "score";
}

function normalizeMarkets(value) {
  const raw = String(value || "").trim();
  if (!raw || raw === "all") return "";
  return raw
    .split(",")
    .map((token) => String(token || "").trim().toLowerCase())
    .filter(Boolean)
    .join(",");
}

function buildCacheKey(userId, options) {
  return [
    String(userId || ""),
    String(options.planTier || "free"),
    String(options.currency || "USD"),
    String(options.pricingMode || ""),
    String(options.minProfit),
    String(options.minSpreadPercent),
    String(options.minScore),
    String(options.liquidityMin),
    String(options.showRisky),
    String(options.sortBy),
    String(options.markets),
    String(options.limit)
  ].join("|");
}

function normalizeBoolean(value) {
  if (typeof value === "boolean") return value;
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

function getCachedValue(key) {
  const hit = scanCache.get(key);
  if (!hit) return null;
  if (Date.now() >= Number(hit.expiresAt || 0)) {
    scanCache.delete(key);
    return null;
  }
  return hit.value;
}

function setCachedValue(key, value) {
  scanCache.set(key, {
    expiresAt: Date.now() + CACHE_TTL_MS,
    value
  });
}

function extractOpportunityFromPortfolioItem(item = {}) {
  const existing = item?.arbitrage || item?.marketComparison?.arbitrage || null;
  if (existing) return existing;
  return arbitrageEngine.evaluateItemOpportunity({
    skinId: item?.skinId,
    marketHashName: item?.marketHashName,
    perMarket: item?.marketComparison?.perMarket,
    sevenDayChangePercent: item?.sevenDayChangePercent,
    liquiditySales:
      item?.marketVolume24h ??
      item?.marketVolume7d ??
      item?.marketInsight?.sellSuggestion?.volume24h,
    liquidityScore: item?.managementClue?.metrics?.liquidityScore
  });
}

exports.CACHE_TTL_MS = CACHE_TTL_MS;

exports.getArbitrageOpportunities = async (userId, options = {}) => {
  const { planTier, entitlements } = await planService.getUserPlanProfile(userId);
  const advancedFiltersEnabled = Boolean(entitlements?.advancedFilters);
  const visibleFeedLimit = Math.max(Number(entitlements?.visibleFeedLimit || MAX_LIMIT), 1);
  const opportunitiesDailyLimit = Math.max(
    Number(entitlements?.opportunitiesDailyLimit || visibleFeedLimit),
    1
  );
  const hardLimit = Math.min(visibleFeedLimit, opportunitiesDailyLimit, MAX_LIMIT);
  const currency = resolveCurrency(options.currency || "USD");
  const showRisky = normalizeBoolean(options.showRisky);
  const normalized = {
    planTier,
    currency,
    pricingMode: options.pricingMode || null,
    minProfit: clampNumber(
      options.minProfit,
      arbitrageEngine.DEFAULT_MIN_PROFIT_ABSOLUTE ??
        arbitrageRules.DEFAULT_MIN_PROFIT_ABSOLUTE,
      0
    ),
    minSpreadPercent: clampNumber(
      options.minSpreadPercent ?? options.minSpread,
      arbitrageEngine.MIN_SPREAD_PERCENT,
      0
    ),
    minScore: clampNumber(
      options.minScore,
      showRisky
        ? arbitrageEngine.RISKY_SCORE_CUTOFF ?? arbitrageRules.RISKY_SCORE_CUTOFF
        : arbitrageEngine.DEFAULT_SCORE_CUTOFF ?? arbitrageRules.DEFAULT_SCORE_CUTOFF,
      0,
      100
    ),
    liquidityMin: clampNumber(options.liquidityMin, 0, 0),
    showRisky,
    sortBy: normalizeSortBy(options.sortBy || options.sort),
    markets: normalizeMarkets(options.markets || options.market),
    limit: Math.round(clampNumber(options.limit, 250, 1, MAX_LIMIT))
  };

  if (!advancedFiltersEnabled) {
    normalized.minProfit = Number(DEFAULT_FREE_FILTERS.minProfit);
    normalized.minSpreadPercent = Number(DEFAULT_FREE_FILTERS.minSpreadPercent);
    normalized.minScore = Number(DEFAULT_FREE_FILTERS.minScore);
    normalized.liquidityMin = Number(DEFAULT_FREE_FILTERS.liquidityMin);
    normalized.showRisky = Boolean(DEFAULT_FREE_FILTERS.showRisky);
    normalized.sortBy = String(DEFAULT_FREE_FILTERS.sortBy);
    normalized.markets = String(DEFAULT_FREE_FILTERS.markets);
  }
  normalized.limit = Math.min(Math.max(Number(normalized.limit || 0), 1), hardLimit);

  const cacheKey = buildCacheKey(userId, normalized);
  const cached = getCachedValue(cacheKey);
  if (cached) {
    return cached;
  }

  const portfolio = await portfolioService.getPortfolio(userId, {
    currency: normalized.currency,
    pricingMode: normalized.pricingMode,
    planTier,
    entitlements
  });

  const sourceItems = Array.isArray(portfolio?.items) ? portfolio.items : [];
  const scanned = sourceItems.map((item) => extractOpportunityFromPortfolioItem(item));
  const ranked = arbitrageEngine.rankOpportunities(scanned, {
    minProfit: normalized.minProfit,
    minSpreadPercent: normalized.minSpreadPercent,
    minScore: normalized.minScore,
    liquidityMin: normalized.liquidityMin,
    includeRisky: normalized.showRisky,
    sortBy: normalized.sortBy,
    markets: normalized.markets
  });
  const payload = {
    currency: portfolio?.currency || normalized.currency,
    generatedAt: new Date().toISOString(),
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    filters: {
      minProfit: normalized.minProfit,
      minSpreadPercent: normalized.minSpreadPercent,
      minScore: normalized.minScore,
      liquidityMin: normalized.liquidityMin,
      showRisky: normalized.showRisky,
      markets: normalized.markets || "all",
      sortBy: normalized.sortBy,
      limit: normalized.limit
    },
    summary: {
      scannedItems: sourceItems.length,
      opportunities: Math.min(ranked.length, normalized.limit)
    },
    plan: {
      planTier,
      advancedFilters: advancedFiltersEnabled,
      opportunitiesDailyLimit,
      visibleFeedLimit,
      appliedLimit: normalized.limit
    },
    items: ranked.slice(0, normalized.limit)
  };

  setCachedValue(cacheKey, payload);
  return payload;
};

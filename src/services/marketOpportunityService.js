const portfolioService = require("./portfolioService");
const planService = require("./planService");
const { resolveCurrency } = require("./currencyService");
const arbitrageEngine = require("./arbitrageEngineService");

const CACHE_TTL_MS = 60 * 1000;
const MAX_LIMIT = 1000;
const scanCache = new Map();

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
  const safe = String(value || "profit")
    .trim()
    .toLowerCase();
  if (safe === "spread") return "spread";
  if (safe === "score") return "score";
  return "profit";
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
    String(options.currency || "USD"),
    String(options.pricingMode || ""),
    String(options.minProfit),
    String(options.minSpreadPercent),
    String(options.minScore),
    String(options.liquidityMin),
    String(options.sortBy),
    String(options.markets),
    String(options.limit)
  ].join("|");
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
  const currency = resolveCurrency(options.currency || "USD");
  const normalized = {
    currency,
    pricingMode: options.pricingMode || null,
    minProfit: clampNumber(options.minProfit, 0, 0),
    minSpreadPercent: clampNumber(
      options.minSpreadPercent ?? options.minSpread,
      arbitrageEngine.MIN_SPREAD_PERCENT,
      0
    ),
    minScore: clampNumber(options.minScore, 0, 0, 100),
    liquidityMin: clampNumber(options.liquidityMin, 0, 0),
    sortBy: normalizeSortBy(options.sortBy || options.sort),
    markets: normalizeMarkets(options.markets || options.market),
    limit: Math.round(clampNumber(options.limit, 250, 1, MAX_LIMIT))
  };
  const cacheKey = buildCacheKey(userId, normalized);
  const cached = getCachedValue(cacheKey);
  if (cached) {
    return cached;
  }

  const { planTier, entitlements } = await planService.getUserPlanProfile(userId);
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
      markets: normalized.markets || "all",
      sortBy: normalized.sortBy,
      limit: normalized.limit
    },
    summary: {
      scannedItems: sourceItems.length,
      opportunities: ranked.length
    },
    items: ranked.slice(0, normalized.limit)
  };

  setCachedValue(cacheKey, payload);
  return payload;
};

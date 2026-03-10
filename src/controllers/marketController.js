const asyncHandler = require("../utils/asyncHandler");
const marketService = require("../services/marketService");
const marketComparisonService = require("../services/marketComparisonService");
const marketOpportunityService = require("../services/marketOpportunityService");
const planService = require("../services/planService");
const AppError = require("../utils/AppError");

function parseItemsPayload(input) {
  if (!Array.isArray(input)) {
    throw new AppError("items must be an array", 400, "VALIDATION_ERROR");
  }

  const toFiniteOrNull = (value) => {
    if (value == null) return null;
    if (typeof value === "string" && !value.trim()) return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const items = input
    .map((row) => ({
      skinId: row?.skinId,
      marketHashName: row?.marketHashName,
      quantity: row?.quantity,
      steamPrice: row?.steamPrice,
      steamCurrency: row?.steamCurrency,
      steamRecordedAt: row?.steamRecordedAt,
      itemCategory: row?.itemCategory,
      itemSubcategory: row?.itemSubcategory,
      sevenDayChangePercent: toFiniteOrNull(
        row?.sevenDayChangePercent ??
          row?.seven_day_change_percent ??
          row?.change7dPercent ??
          row?.priceChange7dPercent
      ),
      volume7d: toFiniteOrNull(row?.volume7d ?? row?.volume_7d),
      marketVolume7d: toFiniteOrNull(row?.marketVolume7d ?? row?.market_volume_7d),
      liquiditySales: toFiniteOrNull(
        row?.liquiditySales ??
          row?.salesCount ??
          row?.sales ??
          row?.marketVolume24h ??
          row?.marketVolume7d
      ),
      liquidityScore: toFiniteOrNull(row?.liquidityScore ?? row?.liquidity_score)
    }))
    .filter((row) => String(row.marketHashName || "").trim());

  if (!items.length) {
    throw new AppError("items array must include marketHashName values", 400, "VALIDATION_ERROR");
  }

  return items;
}

exports.getInventoryValue = asyncHandler(async (req, res) => {
  const data = await marketService.getInventoryValuation(req.userId, {
    commissionPercent: req.validated?.commissionPercent,
    currency: req.query.currency
  });
  res.json(data);
});

exports.getQuickSellSuggestion = asyncHandler(async (req, res) => {
  const skinId = Number(req.validated?.skinId || req.params.skinId);
  const data = await marketService.getQuickSellSuggestion(skinId, {
    commissionPercent: req.validated?.commissionPercent,
    currency: req.query.currency
  });
  res.json(data);
});

exports.getLiquidityScore = asyncHandler(async (req, res) => {
  const skinId = Number(req.validated?.skinId || req.params.skinId);
  const data = await marketService.getLiquidityScore(skinId);
  res.json(data);
});

exports.getPricePreferences = asyncHandler(async (req, res) => {
  const data = await marketComparisonService.getUserPricePreference(req.userId, {
    currency: req.query.currency
  });
  res.json(data);
});

exports.updatePricePreferences = asyncHandler(async (req, res) => {
  const data = await marketComparisonService.updateUserPricePreference(req.userId, {
    pricingMode: req.body?.pricingMode,
    preferredCurrency: req.body?.preferredCurrency
  });
  res.json(data);
});

exports.compareItems = asyncHandler(async (req, res) => {
  const { planTier, entitlements } = await planService.getUserPlanProfile(req.userId);
  const items = parseItemsPayload(req.body?.items || []);
  const data = await marketComparisonService.compareItems(items, {
    userId: req.userId,
    planTier,
    entitlements,
    pricingMode: req.body?.pricingMode,
    currency: req.query.currency || req.body?.currency,
    allowLiveFetch: req.body?.allowLiveFetch !== false,
    forceRefresh: Boolean(req.body?.forceRefresh)
  });
  res.json(data);
});

exports.refreshComparisonCache = asyncHandler(async (req, res) => {
  const { planTier, entitlements } = await planService.getUserPlanProfile(req.userId);
  const items = parseItemsPayload(req.body?.items || []);
  const data = await marketComparisonService.compareItems(items, {
    userId: req.userId,
    planTier,
    entitlements,
    pricingMode: req.body?.pricingMode,
    currency: req.query.currency || req.body?.currency,
    allowLiveFetch: true,
    forceRefresh: true
  });
  res.json({
    refreshed: true,
    generatedAt: data.generatedAt,
    refreshedItems: data.items.length,
    summary: data.summary
  });
});

exports.getArbitrageOpportunities = asyncHandler(async (req, res) => {
  const data = await marketOpportunityService.getArbitrageOpportunities(req.userId, {
    currency: req.query.currency,
    pricingMode: req.query.pricingMode,
    minProfit: req.validated?.minProfit,
    minSpreadPercent: req.validated?.minSpread,
    minScore: req.validated?.minScore,
    liquidityMin: req.validated?.liquidityMin,
    showRisky: req.query.showRisky,
    sortBy: req.query.sortBy || req.query.sort,
    markets: req.query.markets || req.query.market,
    limit: req.validated?.limit
  });
  res.json(data);
});

exports.__testables = {
  parseItemsPayload
};

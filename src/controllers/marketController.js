const asyncHandler = require("../utils/asyncHandler");
const marketService = require("../services/marketService");
const marketComparisonService = require("../services/marketComparisonService");
const AppError = require("../utils/AppError");

function parseItemsPayload(input) {
  if (!Array.isArray(input)) {
    throw new AppError("items must be an array", 400, "VALIDATION_ERROR");
  }

  const items = input
    .map((row) => ({
      skinId: row?.skinId,
      marketHashName: row?.marketHashName,
      quantity: row?.quantity,
      steamPrice: row?.steamPrice,
      steamCurrency: row?.steamCurrency,
      steamRecordedAt: row?.steamRecordedAt
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
  const items = parseItemsPayload(req.body?.items || []);
  const data = await marketComparisonService.compareItems(items, {
    userId: req.userId,
    pricingMode: req.body?.pricingMode,
    currency: req.query.currency || req.body?.currency,
    allowLiveFetch: req.body?.allowLiveFetch !== false,
    forceRefresh: Boolean(req.body?.forceRefresh)
  });
  res.json(data);
});

exports.refreshComparisonCache = asyncHandler(async (req, res) => {
  const items = parseItemsPayload(req.body?.items || []);
  const data = await marketComparisonService.compareItems(items, {
    userId: req.userId,
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

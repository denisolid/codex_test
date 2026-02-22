const asyncHandler = require("../utils/asyncHandler");
const AppError = require("../utils/AppError");
const marketService = require("../services/marketService");

function parseCommission(queryValue) {
  if (queryValue == null || queryValue === "") {
    return undefined;
  }

  const n = Number(queryValue);
  if (!Number.isFinite(n)) {
    throw new AppError("commissionPercent must be numeric", 400);
  }

  return n;
}

exports.getInventoryValue = asyncHandler(async (req, res) => {
  const data = await marketService.getInventoryValuation(req.userId, {
    commissionPercent: parseCommission(req.query.commissionPercent),
    currency: req.query.currency
  });
  res.json(data);
});

exports.getQuickSellSuggestion = asyncHandler(async (req, res) => {
  const skinId = Number(req.params.skinId);
  if (!Number.isInteger(skinId) || skinId <= 0) {
    throw new AppError("Invalid item id", 400);
  }

  const data = await marketService.getQuickSellSuggestion(skinId, {
    commissionPercent: parseCommission(req.query.commissionPercent),
    currency: req.query.currency
  });
  res.json(data);
});

exports.getLiquidityScore = asyncHandler(async (req, res) => {
  const skinId = Number(req.params.skinId);
  if (!Number.isInteger(skinId) || skinId <= 0) {
    throw new AppError("Invalid item id", 400);
  }

  const data = await marketService.getLiquidityScore(skinId);
  res.json(data);
});

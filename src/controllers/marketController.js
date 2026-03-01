const asyncHandler = require("../utils/asyncHandler");
const marketService = require("../services/marketService");

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

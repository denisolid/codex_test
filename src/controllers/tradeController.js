const asyncHandler = require("../utils/asyncHandler");
const tradeCalculatorService = require("../services/tradeCalculatorService");

exports.calculate = asyncHandler(async (req, res) => {
  const result = tradeCalculatorService.calculateTrade({
    ...(req.body || {}),
    currency: req.body?.currency || req.query.currency
  });
  res.json(result);
});

const asyncHandler = require("../utils/asyncHandler");
const monetizationService = require("../services/monetizationService");

exports.getPricing = asyncHandler(async (_req, res) => {
  res.json(monetizationService.getPricing());
});

exports.getMyPlan = asyncHandler(async (req, res) => {
  const data = await monetizationService.getMyPlan(req.userId);
  res.json(data);
});

exports.updateMyPlan = asyncHandler(async (req, res) => {
  const data = await monetizationService.updateMyPlan(req.userId, req.body || {});
  res.json(data);
});

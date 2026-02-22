const asyncHandler = require("../utils/asyncHandler");
const AppError = require("../utils/AppError");
const extensionApiKeyService = require("../services/extensionApiKeyService");
const marketService = require("../services/marketService");
const tradeCalculatorService = require("../services/tradeCalculatorService");

exports.createApiKey = asyncHandler(async (req, res) => {
  const created = await extensionApiKeyService.createKey(req.userId, req.body || {});
  res.status(201).json(created);
});

exports.listApiKeys = asyncHandler(async (req, res) => {
  const keys = await extensionApiKeyService.listKeys(req.userId);
  res.json({ items: keys });
});

exports.revokeApiKey = asyncHandler(async (req, res) => {
  await extensionApiKeyService.revokeKey(req.userId, Number(req.params.id));
  res.status(204).send();
});

exports.getInventoryValue = asyncHandler(async (req, res) => {
  const data = await marketService.getInventoryValuation(req.userId, {
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
    currency: req.query.currency
  });
  res.json(data);
});

exports.calculateTrade = asyncHandler(async (req, res) => {
  const result = tradeCalculatorService.calculateTrade({
    ...(req.body || {}),
    currency: req.body?.currency || req.query.currency
  });
  res.json(result);
});

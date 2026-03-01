const asyncHandler = require("../utils/asyncHandler");
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
  const keyId = Number(req.validated?.keyId || req.params.id);
  await extensionApiKeyService.revokeKey(req.userId, keyId);
  res.status(204).send();
});

exports.getInventoryValue = asyncHandler(async (req, res) => {
  const data = await marketService.getInventoryValuation(req.userId, {
    currency: req.query.currency
  });
  res.json(data);
});

exports.getQuickSellSuggestion = asyncHandler(async (req, res) => {
  const skinId = Number(req.validated?.skinId || req.params.skinId);
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

const asyncHandler = require("../utils/asyncHandler");
const skinService = require("../services/skinService");

exports.getSkinById = asyncHandler(async (req, res) => {
  const skinId = Number(req.validated?.skinId || req.params.id);
  const skin = await skinService.getSkinDetails(skinId, {
    currency: req.query.currency
  });
  res.json(skin);
});

exports.getSkinBySteamItemId = asyncHandler(async (req, res) => {
  const steamItemId = String(req.validated?.steamItemId || req.params.steamItemId).trim();
  const skin = await skinService.getSkinDetailsBySteamItemId(
    req.userId,
    steamItemId,
    { currency: req.query.currency }
  );
  res.json(skin);
});

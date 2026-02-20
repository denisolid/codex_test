const asyncHandler = require("../utils/asyncHandler");
const skinService = require("../services/skinService");
const AppError = require("../utils/AppError");

exports.getSkinById = asyncHandler(async (req, res) => {
  const skinId = Number(req.params.id);
  if (!Number.isInteger(skinId) || skinId <= 0) {
    throw new AppError("Invalid skin id", 400);
  }

  const skin = await skinService.getSkinDetails(skinId);
  res.json(skin);
});

exports.getSkinBySteamItemId = asyncHandler(async (req, res) => {
  const steamItemId = String(req.params.steamItemId || "").trim();
  if (!/^\d+$/.test(steamItemId)) {
    throw new AppError("Invalid steam item id", 400);
  }

  const skin = await skinService.getSkinDetailsBySteamItemId(
    req.userId,
    steamItemId
  );
  res.json(skin);
});

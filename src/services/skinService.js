const AppError = require("../utils/AppError");
const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");

exports.getSkinDetails = async (skinId) => {
  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Skin not found", 404);
  }

  const latestPrice = await priceRepo.getLatestPriceBySkinId(skinId);
  const history = await priceRepo.getHistoryBySkinId(skinId, 30);

  return {
    ...skin,
    latestPrice,
    priceHistory: history
  };
};

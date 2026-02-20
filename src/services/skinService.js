const AppError = require("../utils/AppError");
const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceProviderService = require("./priceProviderService");

async function refreshSkinPrice(skin) {
  const priced = await priceProviderService.getPrice(skin.market_hash_name);
  const recordedAt = new Date().toISOString();

  await priceRepo.insertPriceRows([
    {
      skin_id: skin.id,
      price: priced.price,
      currency: "USD",
      source: `inspect:${priced.source}`,
      recorded_at: recordedAt
    }
  ]);

  return {
    price: priced.price,
    currency: "USD",
    source: `inspect:${priced.source}`,
    recorded_at: recordedAt
  };
}

exports.getSkinDetails = async (skinId) => {
  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Skin not found", 404);
  }

  let latestPrice = await priceRepo.getLatestPriceBySkinId(skinId);

  try {
    latestPrice = await refreshSkinPrice(skin);
  } catch (err) {
    if (!latestPrice) {
      throw new AppError(`Failed to fetch live skin price: ${err.message}`, 502);
    }
    latestPrice = {
      ...latestPrice,
      stale: true,
      staleReason: err.message
    };
  }

  const history = await priceRepo.getHistoryBySkinId(skinId, 30);

  return {
    ...skin,
    latestPrice,
    priceHistory: history
  };
};

exports.getSkinDetailsBySteamItemId = async (userId, steamItemId) => {
  const inventoryItem = await inventoryRepo.getUserInventoryBySteamItemId(
    userId,
    steamItemId
  );

  if (!inventoryItem) {
    throw new AppError("Steam item ID not found in your holdings", 404);
  }

  return exports.getSkinDetails(Number(inventoryItem.skin_id));
};

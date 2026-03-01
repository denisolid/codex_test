const AppError = require("../utils/AppError");
const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceProviderService = require("./priceProviderService");
const { derivePriceStatus } = require("../utils/priceStatus");
const {
  resolveCurrency,
  convertUsdAmount,
  ensureFreshFxRates
} = require("./currencyService");

function isMockPriceSource(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .includes("mock");
}

async function refreshSkinPrice(skin) {
  const priced = await priceProviderService.getPrice(skin.market_hash_name, {
    allowMockFallback: false,
    allowMockSource: false
  });
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
    ...derivePriceStatus({
      price: priced.price,
      source: `inspect:${priced.source}`,
      recorded_at: recordedAt
    }),
    recorded_at: recordedAt
  };
}

exports.getSkinDetails = async (skinId, options = {}) => {
  await ensureFreshFxRates();
  const displayCurrency = resolveCurrency(options.currency);
  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Item not found", 404);
  }

  let latestPrice = await priceRepo.getLatestPriceBySkinId(skinId);
  if (isMockPriceSource(latestPrice?.source)) {
    latestPrice = null;
  }

  try {
    latestPrice = await refreshSkinPrice(skin);
  } catch (err) {
    if (!latestPrice) {
      throw new AppError(`Failed to fetch live item price: ${err.message}`, 502);
    }
    latestPrice = {
      ...latestPrice,
      ...derivePriceStatus(latestPrice),
      stale: true,
      staleReason: err.message
    };
  }

  const latestPriceConverted = latestPrice
    ? {
        ...latestPrice,
        price: convertUsdAmount(Number(latestPrice.price || 0), displayCurrency),
        currency: displayCurrency
      }
    : null;

  const sixMonthsAgo = new Date();
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

  const historyRaw = await priceRepo.getHistoryBySkinIdSince(
    skinId,
    sixMonthsAgo,
    4000
  );
  const seenDates = new Set();
  const history = [];

  for (const row of historyRaw) {
    const day = String(row.recorded_at || "").slice(0, 10);
    if (!day || seenDates.has(day)) continue;
    seenDates.add(day);
    history.push({
      ...row,
      price: convertUsdAmount(Number(row.price || 0), displayCurrency),
      currency: displayCurrency,
      ...derivePriceStatus(row)
    });
    if (history.length >= 185) break;
  }

  return {
    ...skin,
    latestPrice: latestPriceConverted,
    currency: displayCurrency,
    priceHistory: history
  };
};

exports.getSkinDetailsBySteamItemId = async (userId, steamItemId, options = {}) => {
  const inventoryItem = await inventoryRepo.getUserInventoryBySteamItemId(
    userId,
    steamItemId
  );

  if (!inventoryItem) {
    throw new AppError("Steam item ID not found in your holdings", 404);
  }

  return exports.getSkinDetails(Number(inventoryItem.skin_id), options);
};

const AppError = require("../utils/AppError");
const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceProviderService = require("./priceProviderService");
const { derivePriceStatus } = require("../utils/priceStatus");
const { buildDailyCarryForwardSeries } = require("../utils/historySeries");
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

  const rangeEnd = new Date();
  const sixMonthsAgo = new Date(rangeEnd);
  sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

  const [historyRaw, baselineBySkin] = await Promise.all([
    priceRepo.getHistoryBySkinIdSince(skinId, sixMonthsAgo, 20000),
    priceRepo.getLatestPricesBeforeDate([skinId], sixMonthsAgo)
  ]);

  const baselinePrice = Number(baselineBySkin[skinId]);
  let seedRow = null;
  if (Number.isFinite(baselinePrice) && baselinePrice >= 0) {
    seedRow = {
      price: baselinePrice,
      currency: "USD",
      source: "baseline-before-range",
      recorded_at: sixMonthsAgo.toISOString()
    };
  } else if (latestPrice && Number.isFinite(Number(latestPrice.price))) {
    seedRow = {
      price: Number(latestPrice.price),
      currency: "USD",
      source: latestPrice.source || "latest-price-fallback",
      recorded_at: latestPrice.recorded_at || rangeEnd.toISOString()
    };
  }

  const history = buildDailyCarryForwardSeries(historyRaw, {
    startDate: sixMonthsAgo,
    endDate: rangeEnd,
    seedRow,
    descending: true
  })
    .slice(0, 185)
    .map((row) => ({
      ...row,
      price: convertUsdAmount(Number(row.price || 0), displayCurrency),
      currency: displayCurrency,
      ...derivePriceStatus(row)
    }));

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

const AppError = require("../utils/AppError");
const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceProviderService = require("./priceProviderService");
const premiumCategoryAccessService = require("./premiumCategoryAccessService");
const planService = require("./planService");
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
  const planConfig = planService.getPlanConfig(options?.entitlements || "free");
  const historyDaysLimit = Math.max(
    Number(planConfig?.historyDaysLimit || planConfig?.maxHistoryDays || 7),
    1
  );
  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Item not found", 404);
  }
  premiumCategoryAccessService.assertPremiumCategoryAccess({
    entitlements: options?.entitlements,
    marketHashName: skin.market_hash_name,
    message:
      "Unlock knife and glove opportunities with Full Access to inspect premium market categories."
  });

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
  const historyWindowStart = new Date(rangeEnd);
  historyWindowStart.setDate(historyWindowStart.getDate() - (historyDaysLimit - 1));

  const historyRaw = await priceRepo.getHistoryBySkinIdSince(
    skinId,
    historyWindowStart,
    20000,
    { excludeMock: true }
  );

  const history = buildDailyCarryForwardSeries(historyRaw, {
    startDate: historyWindowStart,
    endDate: rangeEnd,
    backfillFromFirstObserved: false,
    descending: true
  })
    .slice(0, historyDaysLimit)
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
    inspectLimits: {
      historyDaysLimit,
      portfolioInsights: String(planConfig?.portfolioInsights || "basic")
    },
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

const steamMarketPriceService = require("../services/steamMarketPriceService");
const { steamMarketCurrency, steamMarketTimeoutMs } = require("../config/env");
const { buildMarketPriceRecord } = require("./marketUtils");
const { mapWithConcurrency } = require("./marketHttp");

const SOURCE = "steam";

function buildSteamListingUrl(marketHashName) {
  return `https://steamcommunity.com/market/listings/730/${encodeURIComponent(
    String(marketHashName || "")
  )}`;
}

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const overview = await steamMarketPriceService.getPriceOverview(marketHashName, {
    currency: Number(input.steamCurrency || steamMarketCurrency || 1),
    timeoutMs: Number(input.timeoutMs || steamMarketTimeoutMs || 10000)
  });

  const grossPrice =
    overview.lowestPrice != null ? overview.lowestPrice : overview.medianPrice;
  if (grossPrice == null) {
    return null;
  }

  return buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice,
    currency: "USD",
    url: buildSteamListingUrl(marketHashName),
    confidence: overview.lowestPrice != null ? "high" : "medium",
    raw: overview
  });
}

async function batchGetPrices(items = [], options = {}) {
  const list = Array.isArray(items) ? items : [];
  const rows = await mapWithConcurrency(
    list,
    async (item) => {
      const marketHashName = String(item?.marketHashName || "").trim();
      if (!marketHashName) return null;
      try {
        const price = await searchItemPrice({
          marketHashName,
          steamCurrency: options.steamCurrency,
          timeoutMs: options.timeoutMs
        });
        return price
          ? {
              marketHashName,
              price
            }
          : null;
      } catch (_err) {
        return null;
      }
    },
    options.concurrency
  );

  const byName = {};
  for (const row of rows) {
    if (!row?.marketHashName || !row?.price) continue;
    byName[row.marketHashName] = row.price;
  }
  return byName;
}

module.exports = {
  source: SOURCE,
  searchItemPrice,
  batchGetPrices
};

const { dmarketApiUrl, dmarketPublicKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const { buildMarketPriceRecord, normalizePriceNumber } = require("./marketUtils");

const SOURCE = "dmarket";
const DEFAULT_API_URL = "https://api.dmarket.com/exchange/v1";
const DEFAULT_GAME_ID = "a8db";

function toApiBaseUrl() {
  const raw = String(dmarketApiUrl || DEFAULT_API_URL).trim();
  return raw.replace(/\/+$/, "");
}

function buildApiUrl(marketHashName) {
  const params = new URLSearchParams({
    gameId: DEFAULT_GAME_ID,
    title: String(marketHashName || ""),
    limit: "1"
  });

  return `${toApiBaseUrl()}/offers-by-title?${params.toString()}`;
}

function buildListingUrl(marketHashName) {
  return `https://dmarket.com/ingame-items/item-list/csgo-skins?searchTitle=${encodeURIComponent(
    String(marketHashName || "")
  )}`;
}

function buildHeaders() {
  const headers = {
    Accept: "application/json",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };

  if (dmarketPublicKey) {
    headers["X-Api-Key"] = dmarketPublicKey;
  }

  return headers;
}

function extractPrice(offer = {}) {
  const directCandidates = [
    offer.price,
    offer.amount,
    offer.priceUSD,
    offer.priceUsd,
    offer.usdPrice
  ];

  for (const candidate of directCandidates) {
    const parsed = normalizePriceNumber(candidate);
    if (parsed != null) return parsed;
  }

  const priceObj = offer?.price || {};
  const nestedCandidates = [
    priceObj.USD,
    priceObj.usd,
    priceObj.amount,
    priceObj.value,
    priceObj.price
  ];
  for (const candidate of nestedCandidates) {
    const parsed = normalizePriceNumber(candidate);
    if (parsed != null) return parsed;
  }

  return null;
}

function extractBestOffer(payload = {}) {
  const list = Array.isArray(payload?.objects)
    ? payload.objects
    : Array.isArray(payload?.items)
      ? payload.items
      : Array.isArray(payload?.results)
        ? payload.results
        : Array.isArray(payload?.offers)
          ? payload.offers
          : [];

  const first = list[0] || null;
  if (!first) return null;

  const price = extractPrice(first);
  if (price == null) return null;

  return {
    price,
    currency: String(first.currency || payload.currency || "USD").trim().toUpperCase(),
    raw: first
  };
}

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const url = buildApiUrl(marketHashName);
  const payload = await fetchJsonWithRetry(url, {
    timeoutMs: input.timeoutMs,
    maxRetries: input.maxRetries,
    headers: buildHeaders()
  });

  const best = extractBestOffer(payload);
  if (!best) return null;

  return buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice: best.price,
    currency: best.currency || "USD",
    url: buildListingUrl(marketHashName),
    confidence: "low",
    raw: best.raw
  });
}

async function batchGetPrices(items = [], options = {}) {
  const list = Array.from(
    new Set(
      (Array.isArray(items) ? items : [])
        .map((item) => String(item?.marketHashName || "").trim())
        .filter(Boolean)
    )
  );

  if (!list.length) {
    return {};
  }

  const rows = await mapWithConcurrency(
    list,
    async (marketHashName) => {
      try {
        const price = await searchItemPrice({
          marketHashName,
          timeoutMs: options.timeoutMs,
          maxRetries: options.maxRetries
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

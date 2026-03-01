const { csfloatApiUrl, csfloatApiKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const {
  buildMarketPriceRecord,
  normalizePriceFromMinorUnits,
  normalizePriceNumber
} = require("./marketUtils");

const SOURCE = "csfloat";
const DEFAULT_API_URL = "https://csfloat.com/api/v1";

function toApiBaseUrl() {
  const raw = String(csfloatApiUrl || DEFAULT_API_URL).trim();
  return raw.replace(/\/+$/, "");
}

function buildApiUrl(marketHashName) {
  const params = new URLSearchParams({
    type: "buy_now",
    sort_by: "lowest_price",
    limit: "1",
    market_hash_name: String(marketHashName || "")
  });
  return `${toApiBaseUrl()}/listings?${params.toString()}`;
}

function buildListingUrl(marketHashName) {
  return `https://csfloat.com/search?market_hash_name=${encodeURIComponent(
    String(marketHashName || "")
  )}`;
}

function buildHeaders() {
  const headers = {
    Accept: "application/json",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };
  if (csfloatApiKey) {
    headers.Authorization = csfloatApiKey;
  }
  return headers;
}

function normalizeCsfloatPrice(value) {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;

  if (/^\d+$/.test(raw)) {
    return normalizePriceFromMinorUnits(Number(raw));
  }

  return normalizePriceNumber(raw);
}

function extractBestListing(payload) {
  const list = Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(payload?.listings)
      ? payload.listings
      : Array.isArray(payload?.results)
        ? payload.results
        : [];

  const first = list[0] || null;
  if (!first) return null;

  const priceCandidates = [
    first.price,
    first.min_price,
    first?.reference?.predicted_price,
    first?.item?.price
  ];

  for (const candidate of priceCandidates) {
    const parsed = normalizeCsfloatPrice(candidate);
    if (parsed != null) {
      return {
        price: parsed,
        currency: String(first.currency || payload?.currency || "USD")
          .trim()
          .toUpperCase(),
        raw: first
      };
    }
  }

  return null;
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
  const best = extractBestListing(payload);
  if (!best) return null;

  return buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice: best.price,
    currency: best.currency || "USD",
    url: buildListingUrl(marketHashName),
    confidence: "medium",
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

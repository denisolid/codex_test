const { skinportApiUrl, skinportApiKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const { buildMarketPriceRecord, normalizePriceNumber } = require("./marketUtils");

const SOURCE = "skinport";
const DEFAULT_API_URL = "https://api.skinport.com/v1";

function toApiBaseUrl() {
  const raw = String(skinportApiUrl || DEFAULT_API_URL).trim();
  return raw.replace(/\/+$/, "");
}

function buildSkinportUrl(path, params = {}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value == null || value === "") return;
    qs.set(key, String(value));
  });

  return `${toApiBaseUrl()}${path}?${qs.toString()}`;
}

function buildListingUrl(marketHashName) {
  return `https://skinport.com/market?search=${encodeURIComponent(
    String(marketHashName || "")
  )}`;
}

function extractPrice(item = {}) {
  const candidates = [
    item.min_price,
    item.minPrice,
    item.suggested_price,
    item.suggestedPrice,
    item.median_price,
    item.medianPrice,
    item.current_price,
    item.currentPrice,
    item.price
  ];

  for (const candidate of candidates) {
    const parsed = normalizePriceNumber(candidate);
    if (parsed != null) {
      return parsed;
    }
  }

  return null;
}

function normalizeItemsPayload(payload) {
  if (!Array.isArray(payload)) return [];
  return payload
    .map((row) => ({
      marketHashName: String(row?.market_hash_name || row?.marketHashName || "").trim(),
      price: extractPrice(row),
      currency: String(row?.currency || "").trim().toUpperCase() || null,
      raw: row
    }))
    .filter((row) => row.marketHashName && row.price != null);
}

function splitIntoChunks(items = [], chunkSize = 35) {
  const chunks = [];
  for (let index = 0; index < items.length; index += chunkSize) {
    chunks.push(items.slice(index, index + chunkSize));
  }
  return chunks;
}

function buildHeaders() {
  const headers = {
    Accept: "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };
  if (skinportApiKey) {
    headers.Authorization = `Bearer ${skinportApiKey}`;
  }
  return headers;
}

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const rows = await batchGetPrices([{ marketHashName }], input);
  return rows[marketHashName] || null;
}

async function batchGetPrices(items = [], options = {}) {
  const normalizedNames = Array.from(
    new Set(
      (Array.isArray(items) ? items : [])
        .map((item) => String(item?.marketHashName || "").trim())
        .filter(Boolean)
    )
  );

  if (!normalizedNames.length) {
    return {};
  }

  const currency = String(options.currency || "USD").trim().toUpperCase();
  const chunks = splitIntoChunks(normalizedNames);
  const chunkResults = await mapWithConcurrency(
    chunks,
    async (namesChunk) => {
      const url = buildSkinportUrl("/items", {
        app_id: 730,
        currency,
        tradable: 0,
        market_hash_name: namesChunk.join(",")
      });

      try {
        const payload = await fetchJsonWithRetry(url, {
          timeoutMs: options.timeoutMs,
          maxRetries: options.maxRetries,
          headers: buildHeaders()
        });
        return normalizeItemsPayload(payload);
      } catch (_err) {
        return [];
      }
    },
    2
  );

  const byName = {};
  for (const rows of chunkResults) {
    for (const row of rows || []) {
      if (!normalizedNames.includes(row.marketHashName)) continue;
      byName[row.marketHashName] = buildMarketPriceRecord({
        source: SOURCE,
        marketHashName: row.marketHashName,
        grossPrice: row.price,
        currency: row.currency || currency,
        url: buildListingUrl(row.marketHashName),
        confidence: "medium",
        raw: row.raw
      });
    }
  }

  return byName;
}

module.exports = {
  source: SOURCE,
  searchItemPrice,
  batchGetPrices
};

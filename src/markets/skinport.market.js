const { skinportApiUrl, skinportApiKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const { buildMarketPriceRecord, normalizePriceNumber } = require("./marketUtils");

const SOURCE = "skinport";
const DEFAULT_API_URL = "https://api.skinport.com/v1";
const SUPPORTED_API_CURRENCIES = new Set(["USD", "EUR"]);
const LIVE_EXECUTABLE_PRICE_FIELDS = Object.freeze([
  { key: "min_price", label: "min_price" },
  { key: "minPrice", label: "minPrice" },
  { key: "lowest_price", label: "lowest_price" },
  { key: "lowestPrice", label: "lowestPrice" },
  { key: "current_price", label: "current_price" },
  { key: "currentPrice", label: "currentPrice" },
  { key: "price", label: "price" }
]);
const HISTORY_SUMMARY_PRICE_FIELDS = Object.freeze([
  { key: ["last_24_hours", "min"], label: "last_24_hours.min" },
  { key: ["last_24_hours", "median"], label: "last_24_hours.median" },
  { key: ["last_7_days", "min"], label: "last_7_days.min" },
  { key: ["last_7_days", "median"], label: "last_7_days.median" },
  { key: ["last_30_days", "min"], label: "last_30_days.min" },
  { key: ["last_30_days", "median"], label: "last_30_days.median" },
  { key: "suggested_price", label: "suggested_price" },
  { key: "suggestedPrice", label: "suggestedPrice" },
  { key: "median_price", label: "median_price" },
  { key: "medianPrice", label: "medianPrice" }
]);

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

function resolveApiCurrency(input) {
  const candidate = String(input || "USD")
    .trim()
    .toUpperCase();
  return SUPPORTED_API_CURRENCIES.has(candidate) ? candidate : "USD";
}

function toIsoOrNull(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const ts = new Date(raw).getTime();
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toISOString();
}

function readPath(obj, path) {
  if (!obj || typeof obj !== "object") return null;
  const keys = Array.isArray(path) ? path : [path];
  let current = obj;
  for (const key of keys) {
    if (current == null || typeof current !== "object") return null;
    current = current[key];
  }
  return current;
}

function pickPriceFromFields(item = {}, fields = []) {
  for (const field of Array.isArray(fields) ? fields : []) {
    const candidate = readPath(item, field.key);
    const parsed = normalizePriceNumber(candidate);
    if (parsed != null) {
      return {
        price: parsed,
        selectedPriceField: String(field.label || "")
      };
    }
  }
  return {
    price: null,
    selectedPriceField: null
  };
}

function extractLiveExecutableQuote(item = {}) {
  const extracted = pickPriceFromFields(item, LIVE_EXECUTABLE_PRICE_FIELDS);
  if (extracted.price == null) return null;
  return {
    price: extracted.price,
    selectedPriceField: extracted.selectedPriceField,
    quoteType: "live_executable"
  };
}

function extractHistoricalSummaryQuote(item = {}) {
  const extracted = pickPriceFromFields(item, HISTORY_SUMMARY_PRICE_FIELDS);
  if (extracted.price == null) return null;
  return {
    price: extracted.price,
    selectedPriceField: extracted.selectedPriceField,
    quoteType: "historical_summary"
  };
}

function extractPrice(item = {}) {
  const live = extractLiveExecutableQuote(item);
  if (live?.price != null) return live.price;
  return null;
}

function resolveObservedAt(row = {}, fallbackIso = null) {
  const direct = toIsoOrNull(
    row?.observed_at ||
      row?.observedAt ||
      row?.updated_at ||
      row?.updatedAt ||
      row?.last_update ||
      row?.lastUpdate
  );
  if (direct) return direct;
  const fallback = toIsoOrNull(fallbackIso);
  return fallback || new Date().toISOString();
}

function toSafeHttpUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) return null;
    return parsed.toString();
  } catch (_err) {
    return null;
  }
}

function extractSkinportItemSlug(row = {}, itemUrl = "") {
  const directSlug =
    String(row?.item_slug || row?.itemSlug || row?.slug || "").trim() || null;
  if (directSlug) return directSlug;
  const safeUrl = toSafeHttpUrl(itemUrl);
  if (!safeUrl) return null;
  try {
    const parsed = new URL(safeUrl);
    const chunks = parsed.pathname.split("/").filter(Boolean);
    const itemIdx = chunks.findIndex((part) => String(part).toLowerCase() === "item");
    if (itemIdx >= 0 && chunks[itemIdx + 1]) {
      return decodeURIComponent(chunks[itemIdx + 1]);
    }
  } catch (_err) {
    return null;
  }
  return null;
}

function resolveSkinportListingId(row = {}) {
  const candidates = [
    row?.listing_id,
    row?.listingId,
    row?.id,
    row?.item_id,
    row?.itemId
  ];
  for (const candidate of candidates) {
    const text = String(candidate || "").trim();
    if (text) return text;
  }
  return null;
}

function resolvePriceIntegrityStatus({
  quoteType = "",
  marketHashName = "",
  currency = "",
  itemSlug = null,
  listingId = null
} = {}) {
  if (String(quoteType || "").trim().toLowerCase() !== "live_executable") {
    return "unconfirmed";
  }
  if (!String(marketHashName || "").trim()) return "unconfirmed";
  if (!String(currency || "").trim()) return "unconfirmed";
  if (!itemSlug && !listingId) return "unconfirmed";
  return "confirmed";
}

function buildQuoteAuditRow(row = {}, normalized = {}) {
  return {
    marketHashName: normalized.marketHashName || null,
    selectedPriceField: normalized.selectedPriceField || null,
    normalizedPrice: normalized.price ?? null,
    currency: normalized.currency || null,
    observedAt: normalized.observedAt || null,
    itemIdentifier:
      normalized.marketHashName || String(row?.market_hash_name || row?.marketHashName || "").trim() || null,
    itemSlug: normalized.itemSlug || null,
    listingId: normalized.listingId || null,
    quoteType: normalized.quoteType || null,
    priceIntegrityStatus: normalized.priceIntegrityStatus || null,
    rawPayload: row
  };
}

function normalizeItemsPayload(payload, options = {}) {
  if (!Array.isArray(payload)) return [];
  const observedFallbackIso = toIsoOrNull(options.observedAt) || new Date().toISOString();
  return payload
    .map((row) => {
      const marketHashName = String(row?.market_hash_name || row?.marketHashName || "").trim();
      const liveQuote = extractLiveExecutableQuote(row);
      const historicalQuote = extractHistoricalSummaryQuote(row);
      const selectedQuote = liveQuote || historicalQuote;
      const itemUrl =
        String(row?.item_page || row?.itemPage || row?.market_page || row?.marketPage || "").trim() || null;
      const currency = String(row?.currency || "").trim().toUpperCase() || null;
      const observedAt = resolveObservedAt(row, observedFallbackIso);
      const listingId = resolveSkinportListingId(row);
      const itemSlug = extractSkinportItemSlug(row, itemUrl || "");
      const quoteType = selectedQuote?.quoteType || "unavailable";
      const priceIntegrityStatus = resolvePriceIntegrityStatus({
        quoteType,
        marketHashName,
        currency,
        itemSlug,
        listingId
      });

      return {
        marketHashName,
        price: selectedQuote?.price ?? null,
        selectedPriceField: selectedQuote?.selectedPriceField || null,
        quoteType,
        priceIntegrityStatus,
        observedAt,
        currency,
        url: itemUrl,
        itemSlug,
        listingId,
        raw: row
      };
    })
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
  const apiCurrency = resolveApiCurrency(currency);
  const chunks = splitIntoChunks(normalizedNames);
  const concurrency = Math.max(
    Math.min(Number(options.concurrency || 2), 6),
    1
  );
  const chunkResults = await mapWithConcurrency(
    chunks,
    async (namesChunk) => {
      const url = buildSkinportUrl("/sales/history", {
        app_id: 730,
        currency: apiCurrency,
        market_hash_name: namesChunk.join(",")
      });

      try {
        const payload = await fetchJsonWithRetry(url, {
          timeoutMs: options.timeoutMs,
          maxRetries: options.maxRetries,
          headers: buildHeaders()
        });
        return normalizeItemsPayload(payload, {
          observedAt: new Date().toISOString()
        });
      } catch (_err) {
        return [];
      }
    },
    concurrency
  );

  const byName = {};
  for (const rows of chunkResults) {
    for (const row of rows || []) {
      if (!normalizedNames.includes(row.marketHashName)) continue;
      const auditRow = buildQuoteAuditRow(row.raw, row);
      if (options.auditSkinportQuotes !== false) {
        console.info("[skinport-audit]", JSON.stringify(auditRow));
      }
      if (row.quoteType !== "live_executable") {
        continue;
      }
      if (row.priceIntegrityStatus !== "confirmed") {
        continue;
      }
      byName[row.marketHashName] = buildMarketPriceRecord({
        source: SOURCE,
        marketHashName: row.marketHashName,
        grossPrice: row.price,
        currency: row.currency || apiCurrency,
        url: row.url || buildListingUrl(row.marketHashName),
        updatedAt: row.observedAt || new Date().toISOString(),
        confidence: "high",
        raw: {
          ...(row.raw || {}),
          skinport_quote_price: row.price,
          skinport_quote_currency: row.currency || apiCurrency,
          skinport_quote_observed_at: row.observedAt || null,
          skinport_quote_type: row.quoteType,
          skinport_item_slug: row.itemSlug || null,
          skinport_listing_id: row.listingId || null,
          skinport_price_integrity_status: row.priceIntegrityStatus
        }
      });
    }
  }

  return byName;
}

module.exports = {
  source: SOURCE,
  searchItemPrice,
  batchGetPrices,
  __testables: {
    extractPrice,
    extractLiveExecutableQuote,
    extractHistoricalSummaryQuote,
    normalizeItemsPayload,
    resolveApiCurrency,
    extractSkinportItemSlug,
    resolveSkinportListingId,
    resolvePriceIntegrityStatus
  }
};

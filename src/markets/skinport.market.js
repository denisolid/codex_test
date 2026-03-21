const { skinportApiUrl, skinportApiKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const { buildMarketPriceRecord, normalizePriceNumber } = require("./marketUtils");

const SOURCE = "skinport";
const DEFAULT_API_URL = "https://api.skinport.com/v1";
const SUPPORTED_API_CURRENCIES = new Set(["USD", "EUR"]);
const LIVE_EXECUTABLE_PRICE_FIELDS = Object.freeze([
  { key: "current_price", label: "current_price" },
  { key: "currentPrice", label: "currentPrice" },
  { key: "lowest_price", label: "lowest_price" },
  { key: "lowestPrice", label: "lowestPrice" },
  { key: "min_price", label: "min_price" },
  { key: "minPrice", label: "minPrice" },
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
const PRICE_INTEGRITY_STATUS_CONFIRMED = "confirmed";
const PRICE_INTEGRITY_STATUS_UNCONFIRMED = "unconfirmed";
const LIVE_QUOTE_STALE_THRESHOLD_HOURS = 2;
const SKINPORT_PIPELINE_STAGES = Object.freeze({
  RAW_DATA: "raw_data",
  PARSING: "parsing",
  NORMALIZATION: "normalization",
  LIVE_QUOTE_VALIDATION: "live_quote_validation",
  MAPPING: "mapping"
});

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

function toAgeHours(isoValue, nowMs = Date.now()) {
  const safeIso = toIsoOrNull(isoValue);
  if (!safeIso) return null;
  const ts = new Date(safeIso).getTime();
  if (!Number.isFinite(ts)) return null;
  const ageHours = (nowMs - ts) / (60 * 60 * 1000);
  if (!Number.isFinite(ageHours) || ageHours < 0) return null;
  return Number(ageHours.toFixed(4));
}

function incrementCounter(target, key, amount = 1) {
  if (!target || typeof target !== "object") return;
  const safeKey = String(key || "").trim();
  if (!safeKey) return;
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0);
}

function createStageCounters() {
  return {
    requested: 0,
    passed: 0,
    rejected: 0
  };
}

function createSkinportPipelineDiagnostics(requestedCount = 0) {
  return {
    requestedItems: Math.max(Number(requestedCount || 0), 0),
    mappedItems: 0,
    fallbackConfirmed: 0,
    strictConfirmed: 0,
    stageCounters: {
      [SKINPORT_PIPELINE_STAGES.RAW_DATA]: {
        requested: Math.max(Number(requestedCount || 0), 0),
        chunksRequested: 0,
        chunksFetched: 0,
        payloadRows: 0,
        emptyPayloads: 0,
        fetchErrors: 0
      },
      [SKINPORT_PIPELINE_STAGES.PARSING]: createStageCounters(),
      [SKINPORT_PIPELINE_STAGES.NORMALIZATION]: createStageCounters(),
      [SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION]: createStageCounters(),
      [SKINPORT_PIPELINE_STAGES.MAPPING]: createStageCounters()
    },
    rejectReasons: {}
  };
}

function incrementStageCounter(diagnostics, stage, field, amount = 1) {
  if (!diagnostics || typeof diagnostics !== "object") return;
  if (!diagnostics.stageCounters || typeof diagnostics.stageCounters !== "object") return;
  const stageKey = String(stage || "").trim();
  if (!stageKey) return;
  if (!diagnostics.stageCounters[stageKey]) {
    diagnostics.stageCounters[stageKey] = createStageCounters();
  }
  const safeField = String(field || "").trim();
  if (!safeField) return;
  diagnostics.stageCounters[stageKey][safeField] =
    Number(diagnostics.stageCounters[stageKey][safeField] || 0) + Number(amount || 0);
}

function recordRejectReason(diagnostics, stage, reason) {
  if (!diagnostics || typeof diagnostics !== "object") return;
  const stageKey = String(stage || "").trim();
  const reasonKey = String(reason || "").trim();
  const key =
    stageKey && reasonKey ? `${stageKey}.${reasonKey}` : reasonKey || stageKey || "unknown";
  if (!key) return;
  incrementCounter(diagnostics.rejectReasons, key, 1);
}

function markFailureByName(failuresByName, marketHashName, stage, reason) {
  if (!failuresByName || typeof failuresByName !== "object") return;
  const safeName = String(marketHashName || "").trim();
  if (!safeName) return;
  if (String(failuresByName[safeName] || "").trim()) return;
  const stageKey = String(stage || "").trim();
  const reasonKey = String(reason || "").trim();
  const value =
    stageKey && reasonKey
      ? `${stageKey}.${reasonKey}`
      : reasonKey || stageKey || "unknown";
  failuresByName[safeName] = value;
}

function pickPrimaryRejectReason(rejectReasons = {}) {
  return Object.entries(rejectReasons || {})
    .filter(([, count]) => Number(count || 0) > 0)
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))[0]?.[0] || null;
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

function normalizeComparableText(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function urlMatchesExactSkinportSearchItem(itemUrl = "", marketHashName = "") {
  const safeUrl = toSafeHttpUrl(itemUrl);
  const expected = normalizeComparableText(marketHashName);
  if (!safeUrl || !expected) return false;
  try {
    const parsed = new URL(safeUrl);
    const host = String(parsed.hostname || "").toLowerCase();
    if (!host.includes("skinport.com")) return false;
    const searchValue = normalizeComparableText(parsed.searchParams.get("search"));
    if (!searchValue) return false;
    return searchValue === expected;
  } catch (_err) {
    return false;
  }
}

function resolvePriceIntegrityDecision({
  quoteType = "",
  marketHashName = "",
  currency = "",
  itemSlug = null,
  listingId = null,
  itemUrl = ""
} = {}) {
  const safeQuoteType = String(quoteType || "").trim().toLowerCase();
  const safeMarketHashName = String(marketHashName || "").trim();
  const safeCurrency = String(currency || "").trim().toUpperCase();
  const safeItemSlug = String(itemSlug || "").trim();
  const safeListingId = String(listingId || "").trim();

  if (safeQuoteType !== "live_executable") {
    return {
      status: PRICE_INTEGRITY_STATUS_UNCONFIRMED,
      mode: "none",
      reason: "quote_not_live_executable"
    };
  }
  if (!safeMarketHashName) {
    return {
      status: PRICE_INTEGRITY_STATUS_UNCONFIRMED,
      mode: "none",
      reason: "missing_market_hash_name"
    };
  }
  if (!safeCurrency) {
    return {
      status: PRICE_INTEGRITY_STATUS_UNCONFIRMED,
      mode: "none",
      reason: "missing_currency"
    };
  }
  if (safeItemSlug || safeListingId) {
    return {
      status: PRICE_INTEGRITY_STATUS_CONFIRMED,
      mode: "strict_identity",
      reason: "confirmed_identity"
    };
  }
  if (urlMatchesExactSkinportSearchItem(itemUrl, safeMarketHashName)) {
    return {
      status: PRICE_INTEGRITY_STATUS_CONFIRMED,
      mode: "safe_fallback_market_search",
      reason: "confirmed_safe_market_search"
    };
  }
  return {
    status: PRICE_INTEGRITY_STATUS_UNCONFIRMED,
    mode: "none",
    reason: "missing_identity_mapping"
  };
}

function resolvePriceIntegrityStatus({
  quoteType = "",
  marketHashName = "",
  currency = "",
  itemSlug = null,
  listingId = null,
  itemUrl = ""
} = {}) {
  return resolvePriceIntegrityDecision({
    quoteType,
    marketHashName,
    currency,
    itemSlug,
    listingId,
    itemUrl
  }).status;
}

function validateLiveQuoteForFeed(row = {}, options = {}) {
  const maxAgeHours = Math.max(
    Number(options.maxAgeHours || LIVE_QUOTE_STALE_THRESHOLD_HOURS),
    0
  );
  if (String(row?.quoteType || "").trim().toLowerCase() !== "live_executable") {
    return {
      confirmed: false,
      reason: "quote_type_not_live"
    };
  }
  if (
    String(row?.priceIntegrityStatus || "").trim().toLowerCase() !==
    PRICE_INTEGRITY_STATUS_CONFIRMED
  ) {
    return {
      confirmed: false,
      reason: String(row?.priceIntegrityReason || "integrity_unconfirmed")
    };
  }
  const observedAt = toIsoOrNull(row?.observedAt);
  const ageHours = toAgeHours(observedAt);
  if (ageHours == null) {
    return {
      confirmed: false,
      reason: "missing_observed_at"
    };
  }
  if (ageHours > maxAgeHours) {
    return {
      confirmed: false,
      reason: "stale_quote"
    };
  }
  return {
    confirmed: true,
    reason: "confirmed",
    ageHours
  };
}

function isRequestedName(nameSet, value) {
  const safeValue = String(value || "").trim();
  return Boolean(safeValue && nameSet instanceof Set && nameSet.has(safeValue));
}

function selectPreferredRecord(current, next) {
  if (!current) return next;
  const currentPrice = Number(current?.grossPrice);
  const nextPrice = Number(next?.grossPrice);
  if (!Number.isFinite(currentPrice)) return next;
  if (!Number.isFinite(nextPrice)) return current;
  // Prefer lower gross price for executable buy-side parity.
  return nextPrice < currentPrice ? next : current;
}

function mergeFailuresByName(target = {}, updates = {}) {
  for (const [name, reason] of Object.entries(updates || {})) {
    if (!String(name || "").trim()) continue;
    if (String(target[name] || "").trim()) continue;
    target[name] = String(reason || "").trim() || "unknown";
  }
}

function applyChunkFailureToNames(failuresByName, namesChunk = [], stage, reason) {
  for (const name of namesChunk || []) {
    markFailureByName(failuresByName, name, stage, reason);
  }
}

function mergePipelineDiagnostics(target, patch = {}) {
  if (!target || typeof target !== "object") return;
  if (!patch || typeof patch !== "object") return;
  target.requestedItems = Number(target.requestedItems || 0);
  target.mappedItems = Number(target.mappedItems || 0) + Number(patch.mappedItems || 0);
  target.fallbackConfirmed =
    Number(target.fallbackConfirmed || 0) + Number(patch.fallbackConfirmed || 0);
  target.strictConfirmed = Number(target.strictConfirmed || 0) + Number(patch.strictConfirmed || 0);

  for (const [stage, counters] of Object.entries(patch.stageCounters || {})) {
    for (const [field, value] of Object.entries(counters || {})) {
      incrementStageCounter(target, stage, field, Number(value || 0));
    }
  }

  for (const [reason, count] of Object.entries(patch.rejectReasons || {})) {
    incrementCounter(target.rejectReasons, reason, Number(count || 0));
  }
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
    priceIntegrityMode: normalized.priceIntegrityMode || null,
    priceIntegrityReason: normalized.priceIntegrityReason || null,
    rawPayload: row
  };
}

function normalizeItemsPayload(payload, options = {}) {
  const diagnostics =
    options?.diagnostics && typeof options.diagnostics === "object"
      ? options.diagnostics
      : null;
  const failuresByName =
    options?.failuresByName && typeof options.failuresByName === "object"
      ? options.failuresByName
      : null;
  if (!Array.isArray(payload)) {
    recordRejectReason(
      diagnostics,
      SKINPORT_PIPELINE_STAGES.RAW_DATA,
      "invalid_payload_shape"
    );
    return [];
  }
  const observedFallbackIso = toIsoOrNull(options.observedAt) || new Date().toISOString();
  const rows = [];
  for (const row of payload) {
    incrementStageCounter(
      diagnostics,
      SKINPORT_PIPELINE_STAGES.PARSING,
      "requested",
      1
    );
    const marketHashName = String(row?.market_hash_name || row?.marketHashName || "").trim();
    if (!marketHashName) {
      incrementStageCounter(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.PARSING,
        "rejected",
        1
      );
      recordRejectReason(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.PARSING,
        "missing_market_hash_name"
      );
      continue;
    }

    incrementStageCounter(
      diagnostics,
      SKINPORT_PIPELINE_STAGES.PARSING,
      "passed",
      1
    );
    incrementStageCounter(
      diagnostics,
      SKINPORT_PIPELINE_STAGES.NORMALIZATION,
      "requested",
      1
    );
    const liveQuote = extractLiveExecutableQuote(row);
    const historicalQuote = extractHistoricalSummaryQuote(row);
    const selectedQuote = liveQuote || historicalQuote;
    if (selectedQuote?.price == null) {
      incrementStageCounter(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "rejected",
        1
      );
      recordRejectReason(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "missing_price_field"
      );
      markFailureByName(
        failuresByName,
        marketHashName,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "missing_price_field"
      );
      continue;
    }

    const itemUrl =
      String(row?.item_page || row?.itemPage || row?.market_page || row?.marketPage || "").trim() ||
      null;
    const currency = String(row?.currency || "").trim().toUpperCase() || null;
    if (!currency) {
      incrementStageCounter(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "rejected",
        1
      );
      recordRejectReason(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "missing_currency"
      );
      markFailureByName(
        failuresByName,
        marketHashName,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "missing_currency"
      );
      continue;
    }

    const observedAt = resolveObservedAt(row, observedFallbackIso);
    if (!observedAt) {
      incrementStageCounter(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "rejected",
        1
      );
      recordRejectReason(
        diagnostics,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "missing_observed_at"
      );
      markFailureByName(
        failuresByName,
        marketHashName,
        SKINPORT_PIPELINE_STAGES.NORMALIZATION,
        "missing_observed_at"
      );
      continue;
    }

    const listingId = resolveSkinportListingId(row);
    const itemSlug = extractSkinportItemSlug(row, itemUrl || "");
    const quoteType = selectedQuote?.quoteType || "unavailable";
    const priceIntegrityDecision = resolvePriceIntegrityDecision({
      quoteType,
      marketHashName,
      currency,
      itemSlug,
      listingId,
      itemUrl
    });

    incrementStageCounter(
      diagnostics,
      SKINPORT_PIPELINE_STAGES.NORMALIZATION,
      "passed",
      1
    );

    rows.push({
      marketHashName,
      price: selectedQuote.price,
      selectedPriceField: selectedQuote?.selectedPriceField || null,
      quoteType,
      priceIntegrityStatus: priceIntegrityDecision.status,
      priceIntegrityMode: priceIntegrityDecision.mode,
      priceIntegrityReason: priceIntegrityDecision.reason,
      observedAt,
      currency,
      url: itemUrl,
      itemSlug,
      listingId,
      raw: row
    });
  }

  return rows;
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
  const requestedNamesSet = new Set(normalizedNames);
  const failuresByName = {};
  const pipelineDiagnostics = createSkinportPipelineDiagnostics(normalizedNames.length);
  const chunks = splitIntoChunks(normalizedNames);
  const concurrency = Math.max(
    Math.min(Number(options.concurrency || 2), 6),
    1
  );
  const chunkResults = await mapWithConcurrency(
    chunks,
    async (namesChunk) => {
      const chunkDiagnostics = createSkinportPipelineDiagnostics(0);
      incrementStageCounter(
        chunkDiagnostics,
        SKINPORT_PIPELINE_STAGES.RAW_DATA,
        "chunksRequested",
        1
      );
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
        incrementStageCounter(
          chunkDiagnostics,
          SKINPORT_PIPELINE_STAGES.RAW_DATA,
          "chunksFetched",
          1
        );
        if (!Array.isArray(payload)) {
          incrementStageCounter(
            chunkDiagnostics,
            SKINPORT_PIPELINE_STAGES.RAW_DATA,
            "emptyPayloads",
            1
          );
          recordRejectReason(
            chunkDiagnostics,
            SKINPORT_PIPELINE_STAGES.RAW_DATA,
            "invalid_payload_shape"
          );
          applyChunkFailureToNames(
            failuresByName,
            namesChunk,
            SKINPORT_PIPELINE_STAGES.RAW_DATA,
            "invalid_payload_shape"
          );
          return {
            rows: [],
            diagnostics: chunkDiagnostics,
            failuresByName: {}
          };
        }
        if (!payload.length) {
          incrementStageCounter(
            chunkDiagnostics,
            SKINPORT_PIPELINE_STAGES.RAW_DATA,
            "emptyPayloads",
            1
          );
          recordRejectReason(
            chunkDiagnostics,
            SKINPORT_PIPELINE_STAGES.RAW_DATA,
            "empty_payload"
          );
          applyChunkFailureToNames(
            failuresByName,
            namesChunk,
            SKINPORT_PIPELINE_STAGES.RAW_DATA,
            "empty_payload"
          );
        }
        incrementStageCounter(
          chunkDiagnostics,
          SKINPORT_PIPELINE_STAGES.RAW_DATA,
          "payloadRows",
          payload.length
        );
        const chunkFailuresByName = {};
        const rows = normalizeItemsPayload(payload, {
          observedAt: new Date().toISOString(),
          diagnostics: chunkDiagnostics,
          failuresByName: chunkFailuresByName
        });
        return {
          rows,
          diagnostics: chunkDiagnostics,
          failuresByName: chunkFailuresByName
        };
      } catch (_err) {
        incrementStageCounter(
          chunkDiagnostics,
          SKINPORT_PIPELINE_STAGES.RAW_DATA,
          "fetchErrors",
          1
        );
        recordRejectReason(
          chunkDiagnostics,
          SKINPORT_PIPELINE_STAGES.RAW_DATA,
          "fetch_error"
        );
        applyChunkFailureToNames(
          failuresByName,
          namesChunk,
          SKINPORT_PIPELINE_STAGES.RAW_DATA,
          "fetch_error"
        );
        return {
          rows: [],
          diagnostics: chunkDiagnostics,
          failuresByName: {}
        };
      }
    },
    concurrency
  );

  const byName = {};
  for (const chunkResult of chunkResults) {
    const rows = Array.isArray(chunkResult?.rows) ? chunkResult.rows : [];
    mergePipelineDiagnostics(pipelineDiagnostics, chunkResult?.diagnostics || {});
    mergeFailuresByName(failuresByName, chunkResult?.failuresByName || {});

    for (const row of rows) {
      if (!isRequestedName(requestedNamesSet, row.marketHashName)) {
        incrementStageCounter(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.MAPPING,
          "rejected",
          1
        );
        recordRejectReason(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.MAPPING,
          "not_requested_item"
        );
        continue;
      }
      const auditRow = buildQuoteAuditRow(row.raw, row);
      if (options.auditSkinportQuotes !== false) {
        console.info("[skinport-audit]", JSON.stringify(auditRow));
      }
      incrementStageCounter(
        pipelineDiagnostics,
        SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
        "requested",
        1
      );
      if (row.quoteType !== "live_executable") {
        incrementStageCounter(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
          "rejected",
          1
        );
        recordRejectReason(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
          "quote_type_not_live"
        );
        markFailureByName(
          failuresByName,
          row.marketHashName,
          SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
          "quote_type_not_live"
        );
        continue;
      }
      const liveValidation = validateLiveQuoteForFeed(row, {
        maxAgeHours: options.maxQuoteAgeHours
      });
      if (!liveValidation.confirmed) {
        incrementStageCounter(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
          "rejected",
          1
        );
        recordRejectReason(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
          liveValidation.reason
        );
        markFailureByName(
          failuresByName,
          row.marketHashName,
          SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
          liveValidation.reason
        );
        continue;
      }
      incrementStageCounter(
        pipelineDiagnostics,
        SKINPORT_PIPELINE_STAGES.LIVE_QUOTE_VALIDATION,
        "passed",
        1
      );
      if (row.priceIntegrityMode === "strict_identity") {
        pipelineDiagnostics.strictConfirmed = Number(pipelineDiagnostics.strictConfirmed || 0) + 1;
      } else {
        pipelineDiagnostics.fallbackConfirmed =
          Number(pipelineDiagnostics.fallbackConfirmed || 0) + 1;
      }

      incrementStageCounter(
        pipelineDiagnostics,
        SKINPORT_PIPELINE_STAGES.MAPPING,
        "requested",
        1
      );
      const nextRecord = buildMarketPriceRecord({
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
          skinport_price_integrity_status: row.priceIntegrityStatus,
          skinport_price_integrity_mode: row.priceIntegrityMode || null,
          skinport_price_integrity_reason: row.priceIntegrityReason || null
        }
      });
      if (!nextRecord) {
        incrementStageCounter(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.MAPPING,
          "rejected",
          1
        );
        recordRejectReason(
          pipelineDiagnostics,
          SKINPORT_PIPELINE_STAGES.MAPPING,
          "invalid_record"
        );
        markFailureByName(
          failuresByName,
          row.marketHashName,
          SKINPORT_PIPELINE_STAGES.MAPPING,
          "invalid_record"
        );
        continue;
      }
      const selectedRecord = selectPreferredRecord(byName[row.marketHashName], nextRecord);
      byName[row.marketHashName] = selectedRecord;
      incrementStageCounter(
        pipelineDiagnostics,
        SKINPORT_PIPELINE_STAGES.MAPPING,
        "passed",
        1
      );
      delete failuresByName[row.marketHashName];
    }
  }

  for (const name of normalizedNames) {
    if (byName[name]) continue;
    if (!String(failuresByName[name] || "").trim()) {
      failuresByName[name] = `${SKINPORT_PIPELINE_STAGES.MAPPING}.missing_requested_item`;
      recordRejectReason(
        pipelineDiagnostics,
        SKINPORT_PIPELINE_STAGES.MAPPING,
        "missing_requested_item"
      );
      incrementStageCounter(
        pipelineDiagnostics,
        SKINPORT_PIPELINE_STAGES.MAPPING,
        "rejected",
        1
      );
    }
  }
  pipelineDiagnostics.mappedItems = Object.keys(byName).length;
  byName.__meta = {
    failuresByName,
    sourceUnavailableReason:
      Object.keys(byName).length > 0
        ? null
        : pickPrimaryRejectReason(pipelineDiagnostics.rejectReasons) ||
          `${SKINPORT_PIPELINE_STAGES.RAW_DATA}.no_results`,
    pipeline: pipelineDiagnostics
  };

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
    resolvePriceIntegrityStatus,
    resolvePriceIntegrityDecision,
    validateLiveQuoteForFeed,
    urlMatchesExactSkinportSearchItem,
    createSkinportPipelineDiagnostics
  }
};

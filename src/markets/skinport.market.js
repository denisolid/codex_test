const { skinportApiUrl, skinportApiKey } = require("../config/env");
const { fetchJsonWithRetry } = require("./marketHttp");
const { buildMarketPriceRecord, normalizePriceNumber } = require("./marketUtils");
const {
  SOURCE_STATES,
  buildMarketHealthDiagnostics,
  attachMarketHealth,
  normalizeSourceState,
  toIsoOrNull
} = require("./marketSourceDiagnostics");

const SOURCE = "skinport";
const DEFAULT_API_URL = "https://api.skinport.com/v1";
const ITEMS_PATH = "/items";
const ITEMS_CACHE_TTL_MS = 4 * 60 * 1000;
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
const NO_LISTING_MESSAGE = "No Skinport listing found.";
const NO_DATA_MESSAGE = "Skinport returned no usable quote data.";
const PARSING_FAILURE_MESSAGE = "Skinport response could not be parsed.";
const TIMEOUT_MESSAGE = "Skinport request timed out.";
const UNAVAILABLE_MESSAGE = "Skinport is temporarily unavailable.";

const itemsCacheByCurrency = new Map();

function normalizeText(value) {
  return String(value || "").trim();
}

function toApiBaseUrl() {
  return normalizeText(skinportApiUrl || DEFAULT_API_URL).replace(/\/+$/, "");
}

function buildSkinportUrl(path, params = {}) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value == null || value === "") return;
    qs.set(key, String(value));
  });
  const query = qs.toString();
  return `${toApiBaseUrl()}${path}${query ? `?${query}` : ""}`;
}

function buildItemsUrl(currency = "USD") {
  return buildSkinportUrl(ITEMS_PATH, {
    app_id: 730,
    currency: resolveApiCurrency(currency),
    tradable: 1
  });
}

function buildListingUrl(marketHashName) {
  return `https://skinport.com/market?search=${encodeURIComponent(normalizeText(marketHashName))}`;
}

function resolveApiCurrency(input) {
  const candidate = normalizeText(input || "USD").toUpperCase();
  return SUPPORTED_API_CURRENCIES.has(candidate) ? candidate : "USD";
}

function toAgeHours(isoValue, nowMs = Date.now()) {
  const safeIso = toIsoOrNull(isoValue);
  if (!safeIso) return null;
  const ts = new Date(safeIso).getTime();
  if (!Number.isFinite(ts)) return null;
  const ageHours = (nowMs - ts) / (60 * 60 * 1000);
  return Number.isFinite(ageHours) && ageHours >= 0 ? Number(ageHours.toFixed(4)) : null;
}

function createSkinportPipelineDiagnostics(requestedCount = 0) {
  return {
    requestedItems: Math.max(Number(requestedCount || 0), 0),
    mappedItems: 0,
    strictConfirmed: 0,
    fallbackConfirmed: 0,
    stageCounters: {},
    rejectReasons: {}
  };
}

function readPath(obj, path) {
  let current = obj;
  for (const key of Array.isArray(path) ? path : [path]) {
    if (current == null || typeof current !== "object") return null;
    current = current[key];
  }
  return current;
}

function pickPriceFromFields(item = {}, fields = []) {
  for (const field of fields) {
    const parsed = normalizePriceNumber(readPath(item, field.key));
    if (parsed != null) {
      return { price: parsed, selectedPriceField: String(field.label || "") };
    }
  }
  return { price: null, selectedPriceField: null };
}

function extractLiveExecutableQuote(item = {}) {
  const extracted = pickPriceFromFields(item, LIVE_EXECUTABLE_PRICE_FIELDS);
  return extracted.price == null
    ? null
    : { ...extracted, quoteType: "live_executable" };
}

function extractHistoricalSummaryQuote(item = {}) {
  const extracted = pickPriceFromFields(item, HISTORY_SUMMARY_PRICE_FIELDS);
  return extracted.price == null
    ? null
    : { ...extracted, quoteType: "historical_summary" };
}

function extractPrice(item = {}) {
  return extractLiveExecutableQuote(item)?.price ?? null;
}

function toSafeHttpUrl(value) {
  const raw = normalizeText(value);
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    return ["http:", "https:"].includes(parsed.protocol) ? parsed.toString() : null;
  } catch (_err) {
    return null;
  }
}

function extractSkinportItemSlug(row = {}, itemUrl = "") {
  const direct = normalizeText(row?.item_slug || row?.itemSlug || row?.slug);
  if (direct) return direct;
  const safeUrl = toSafeHttpUrl(itemUrl);
  if (!safeUrl) return null;
  try {
    const parsed = new URL(safeUrl);
    const chunks = parsed.pathname.split("/").filter(Boolean);
    const itemIndex = chunks.findIndex((part) => part.toLowerCase() === "item");
    return itemIndex >= 0 && chunks[itemIndex + 1]
      ? decodeURIComponent(chunks[itemIndex + 1])
      : null;
  } catch (_err) {
    return null;
  }
}

function resolveSkinportListingId(row = {}) {
  return (
    [
      row?.listing_id,
      row?.listingId,
      row?.id,
      row?.item_id,
      row?.itemId
    ]
      .map((value) => normalizeText(value))
      .find(Boolean) || null
  );
}

function normalizeComparableText(value) {
  return normalizeText(value).replace(/\s+/g, " ").toLowerCase();
}

function urlMatchesExactSkinportSearchItem(itemUrl = "", marketHashName = "") {
  const safeUrl = toSafeHttpUrl(itemUrl);
  const expected = normalizeComparableText(marketHashName);
  if (!safeUrl || !expected) return false;
  try {
    const parsed = new URL(safeUrl);
    if (!String(parsed.hostname || "").toLowerCase().includes("skinport.com")) return false;
    return normalizeComparableText(parsed.searchParams.get("search")) === expected;
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
  if (normalizeText(quoteType).toLowerCase() !== "live_executable") {
    return { status: PRICE_INTEGRITY_STATUS_UNCONFIRMED, mode: "none", reason: "quote_not_live_executable" };
  }
  if (!normalizeText(marketHashName)) {
    return { status: PRICE_INTEGRITY_STATUS_UNCONFIRMED, mode: "none", reason: "missing_market_hash_name" };
  }
  if (!normalizeText(currency).toUpperCase()) {
    return { status: PRICE_INTEGRITY_STATUS_UNCONFIRMED, mode: "none", reason: "missing_currency" };
  }
  if (normalizeText(itemSlug) || normalizeText(listingId)) {
    return { status: PRICE_INTEGRITY_STATUS_CONFIRMED, mode: "strict_identity", reason: "confirmed_identity" };
  }
  if (urlMatchesExactSkinportSearchItem(itemUrl, marketHashName)) {
    return { status: PRICE_INTEGRITY_STATUS_CONFIRMED, mode: "safe_fallback_market_search", reason: "confirmed_safe_market_search" };
  }
  return { status: PRICE_INTEGRITY_STATUS_UNCONFIRMED, mode: "none", reason: "missing_identity_mapping" };
}

function resolvePriceIntegrityStatus(input = {}) {
  return resolvePriceIntegrityDecision(input).status;
}

function validateLiveQuoteForFeed(row = {}, options = {}) {
  if (normalizeText(row?.quoteType).toLowerCase() !== "live_executable") {
    return { confirmed: false, reason: "quote_type_not_live" };
  }
  if (normalizeText(row?.priceIntegrityStatus).toLowerCase() !== PRICE_INTEGRITY_STATUS_CONFIRMED) {
    return { confirmed: false, reason: normalizeText(row?.priceIntegrityReason) || "integrity_unconfirmed" };
  }
  const ageHours = toAgeHours(row?.observedAt);
  if (ageHours == null) return { confirmed: false, reason: "missing_observed_at" };
  if (ageHours > Math.max(Number(options.maxAgeHours || LIVE_QUOTE_STALE_THRESHOLD_HOURS), 0)) {
    return { confirmed: false, reason: "stale_quote" };
  }
  return { confirmed: true, reason: "confirmed", ageHours };
}

function buildHeaders() {
  return {
    Accept: "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };
}

function classifySkinportError(err) {
  const status = Number(err?.upstreamStatus || err?.statusCode || err?.status || 0);
  const message = normalizeText(err?.message).toLowerCase();
  if (message.includes("timed out")) return { state: SOURCE_STATES.TIMEOUT, reason: TIMEOUT_MESSAGE, responseStatus: status || 504 };
  if (status === 429) return { state: SOURCE_STATES.UNAVAILABLE, reason: "Skinport rate limit reached. Retry shortly.", responseStatus: status };
  return { state: SOURCE_STATES.UNAVAILABLE, reason: UNAVAILABLE_MESSAGE, responseStatus: status || null };
}

function buildDiagnostics({
  requestSent = false,
  responseStatus = null,
  responseParsed = false,
  listingsFound = null,
  buyPricePresent = false,
  freshnessPresent = false,
  listingUrlPresent = false,
  sourceFailureReason = null,
  lastSuccessAt = null,
  lastFailureAt = null
} = {}) {
  return buildMarketHealthDiagnostics({
    marketEnabled: true,
    credentialsPresent: Boolean(skinportApiKey),
    authOk: null,
    requestSent,
    responseStatus,
    responseParsed,
    listingsFound,
    buyPricePresent,
    sellPricePresent: buyPricePresent,
    freshnessPresent,
    listingUrlPresent,
    sourceFailureReason,
    lastSuccessAt,
    lastFailureAt
  });
}

function resolveObservedAt(row = {}, fallbackIso = null) {
  return (
    toIsoOrNull(
      row?.observed_at ||
        row?.observedAt ||
        row?.updated_at ||
        row?.updatedAt ||
        row?.created_at ||
        row?.createdAt ||
        row?.last_update ||
        row?.lastUpdate
    ) ||
    toIsoOrNull(fallbackIso) ||
    new Date().toISOString()
  );
}

function resolveItemUrl(row = {}, marketHashName = "") {
  return (
    toSafeHttpUrl(
      row?.item_page || row?.itemPage || row?.market_page || row?.marketPage || row?.url
    ) || buildListingUrl(marketHashName)
  );
}

function normalizeItemsPayload(payload, options = {}) {
  if (!Array.isArray(payload)) return [];
  const requestedNamesSet =
    options?.requestedNamesSet instanceof Set ? options.requestedNamesSet : null;
  const observedFallbackIso = toIsoOrNull(options.observedAt) || new Date().toISOString();
  const rows = [];

  for (const row of payload) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName);
    if (!marketHashName) continue;
    if (requestedNamesSet && !requestedNamesSet.has(marketHashName)) continue;

    const liveQuote = extractLiveExecutableQuote(row);
    const historicalQuote = extractHistoricalSummaryQuote(row);
    const selectedQuote = liveQuote || historicalQuote;
    if (selectedQuote?.price == null) continue;

    const currency = normalizeText(row?.currency).toUpperCase();
    if (!currency) continue;

    const url = resolveItemUrl(row, marketHashName);
    const observedAt = resolveObservedAt(row, observedFallbackIso);
    const listingId = resolveSkinportListingId(row);
    const itemSlug = extractSkinportItemSlug(row, url);
    const integrity = resolvePriceIntegrityDecision({
      quoteType: selectedQuote.quoteType,
      marketHashName,
      currency,
      itemSlug,
      listingId,
      itemUrl: url
    });

    rows.push({
      marketHashName,
      price: selectedQuote.price,
      selectedPriceField: selectedQuote.selectedPriceField || null,
      quoteType: selectedQuote.quoteType,
      priceIntegrityStatus: integrity.status,
      priceIntegrityMode: integrity.mode,
      priceIntegrityReason: integrity.reason,
      observedAt,
      currency,
      url,
      itemSlug,
      listingId,
      raw: row
    });
  }

  return rows;
}

async function fetchCachedItemsPayload(apiCurrency, options = {}) {
  const cacheKey = resolveApiCurrency(apiCurrency);
  const existing = itemsCacheByCurrency.get(cacheKey);
  const nowMs = Date.now();
  if (
    existing &&
    Array.isArray(existing.payload) &&
    Number.isFinite(existing.fetchedAtMs) &&
    nowMs - existing.fetchedAtMs < ITEMS_CACHE_TTL_MS
  ) {
    return existing.payload;
  }
  if (existing?.pending && typeof existing.pending.then === "function") {
    return existing.pending;
  }

  const pending = fetchJsonWithRetry(buildItemsUrl(cacheKey), {
    timeoutMs: options.timeoutMs,
    maxRetries: options.maxRetries,
    headers: buildHeaders()
  })
    .then((payload) => {
      itemsCacheByCurrency.set(cacheKey, {
        payload,
        fetchedAtMs: Date.now()
      });
      return payload;
    })
    .finally(() => {
      const current = itemsCacheByCurrency.get(cacheKey);
      if (current?.pending === pending) delete current.pending;
    });

  itemsCacheByCurrency.set(cacheKey, {
    payload: Array.isArray(existing?.payload) ? existing.payload : null,
    fetchedAtMs: Number(existing?.fetchedAtMs || 0),
    pending
  });

  return pending;
}

async function searchItemPrice(input = {}) {
  const marketHashName = normalizeText(input.marketHashName);
  if (!marketHashName) return null;
  const rows = await batchGetPrices([{ marketHashName }], input);
  return rows[marketHashName] || null;
}

async function batchGetPrices(items = [], options = {}) {
  const requestedNames = Array.from(
    new Set(
      (Array.isArray(items) ? items : [])
        .map((item) => normalizeText(item?.marketHashName))
        .filter(Boolean)
    )
  );
  if (!requestedNames.length) return {};

  const requestedNamesSet = new Set(requestedNames);
  const apiCurrency = resolveApiCurrency(options.currency || "USD");
  const requestStartedAt = new Date().toISOString();
  const failuresByName = {};
  const stateByName = {};
  const diagnosticsByName = {};
  const pipeline = createSkinportPipelineDiagnostics(requestedNames.length);
  let payload = null;

  try {
    payload = await fetchCachedItemsPayload(apiCurrency, options);
  } catch (err) {
    const failure = classifySkinportError(err);
    for (const name of requestedNames) {
      failuresByName[name] = failure.reason;
      stateByName[name] = failure.state;
      diagnosticsByName[name] = buildDiagnostics({
        requestSent: true,
        responseStatus: failure.responseStatus,
        sourceFailureReason: failure.state,
        lastFailureAt: requestStartedAt
      });
    }
    const failed = {};
    Object.defineProperty(failed, "__meta", {
      value: {
        failuresByName,
        stateByName,
        diagnosticsByName,
        market_enabled: true,
        credentials_present: Boolean(skinportApiKey),
        request_sent: true,
        response_status: failure.responseStatus,
        response_parsed: false,
        listings_found: null,
        buy_price_present: false,
        sell_price_present: false,
        freshness_present: false,
        listing_url_present: false,
        sourceUnavailableReason: failure.reason,
        sourceFailureReason: failure.state,
        source_failure_reason: failure.state,
        last_success_at: null,
        last_failure_at: requestStartedAt,
        pipeline
      },
      enumerable: false
    });
    return failed;
  }

  if (!Array.isArray(payload)) {
    for (const name of requestedNames) {
      failuresByName[name] = PARSING_FAILURE_MESSAGE;
      stateByName[name] = SOURCE_STATES.PARSING_FAILED;
      diagnosticsByName[name] = buildDiagnostics({
        requestSent: true,
        responseStatus: 200,
        sourceFailureReason: SOURCE_STATES.PARSING_FAILED,
        lastFailureAt: requestStartedAt
      });
    }
    const failed = {};
    Object.defineProperty(failed, "__meta", {
      value: {
        failuresByName,
        stateByName,
        diagnosticsByName,
        market_enabled: true,
        credentials_present: Boolean(skinportApiKey),
        request_sent: true,
        response_status: 200,
        response_parsed: false,
        listings_found: null,
        buy_price_present: false,
        sell_price_present: false,
        freshness_present: false,
        listing_url_present: false,
        sourceUnavailableReason: PARSING_FAILURE_MESSAGE,
        sourceFailureReason: SOURCE_STATES.PARSING_FAILED,
        source_failure_reason: SOURCE_STATES.PARSING_FAILED,
        last_success_at: null,
        last_failure_at: requestStartedAt,
        pipeline
      },
      enumerable: false
    });
    return failed;
  }

  const normalizedRows = normalizeItemsPayload(payload, {
    observedAt: requestStartedAt,
    requestedNamesSet
  });
  const rowsByName = {};
  for (const row of normalizedRows) {
    const current = rowsByName[row.marketHashName];
    if (!current || Number(row.price || 0) < Number(current.price || Number.POSITIVE_INFINITY)) {
      rowsByName[row.marketHashName] = row;
    }
  }

  const byName = {};
  for (const marketHashName of requestedNames) {
    const row = rowsByName[marketHashName] || null;
    if (!row) {
      failuresByName[marketHashName] = NO_LISTING_MESSAGE;
      stateByName[marketHashName] = SOURCE_STATES.NO_LISTING;
      diagnosticsByName[marketHashName] = buildDiagnostics({
        requestSent: true,
        responseStatus: 200,
        responseParsed: true,
        listingsFound: false,
        sourceFailureReason: SOURCE_STATES.NO_LISTING,
        lastFailureAt: requestStartedAt
      });
      continue;
    }

    const liveValidation = validateLiveQuoteForFeed(row, {
      maxAgeHours: options.maxQuoteAgeHours
    });
    if (!liveValidation.confirmed) {
      const failureState =
        liveValidation.reason === "stale_quote" ? SOURCE_STATES.STALE : SOURCE_STATES.NO_DATA;
      failuresByName[marketHashName] =
        failureState === SOURCE_STATES.STALE ? "Skinport quote is stale." : NO_DATA_MESSAGE;
      stateByName[marketHashName] = failureState;
      diagnosticsByName[marketHashName] = buildDiagnostics({
        requestSent: true,
        responseStatus: 200,
        responseParsed: true,
        listingsFound: true,
        freshnessPresent: Boolean(row.observedAt),
        listingUrlPresent: Boolean(row.url),
        sourceFailureReason: failureState,
        lastFailureAt: requestStartedAt
      });
      continue;
    }

    const successDiagnostics = buildDiagnostics({
      requestSent: true,
      responseStatus: 200,
      responseParsed: true,
      listingsFound: true,
      buyPricePresent: true,
      freshnessPresent: Boolean(row.observedAt),
      listingUrlPresent: Boolean(row.url),
      lastSuccessAt: requestStartedAt
    });
    const record = buildMarketPriceRecord({
      source: SOURCE,
      marketHashName,
      grossPrice: row.price,
      currency: row.currency || apiCurrency,
      url: row.url || buildListingUrl(marketHashName),
      updatedAt: row.observedAt || requestStartedAt,
      confidence: row.priceIntegrityMode === "strict_identity" ? "high" : "medium",
      raw: attachMarketHealth(
        {
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
        },
        successDiagnostics
      )
    });
    if (!record) {
      failuresByName[marketHashName] = NO_DATA_MESSAGE;
      stateByName[marketHashName] = SOURCE_STATES.NO_DATA;
      diagnosticsByName[marketHashName] = buildDiagnostics({
        requestSent: true,
        responseStatus: 200,
        responseParsed: true,
        listingsFound: true,
        freshnessPresent: Boolean(row.observedAt),
        listingUrlPresent: Boolean(row.url),
        sourceFailureReason: SOURCE_STATES.NO_DATA,
        lastFailureAt: requestStartedAt
      });
      continue;
    }

    byName[marketHashName] = record;
    stateByName[marketHashName] = SOURCE_STATES.OK;
    diagnosticsByName[marketHashName] = successDiagnostics;
    pipeline.mappedItems += 1;
    if (row.priceIntegrityMode === "strict_identity") pipeline.strictConfirmed += 1;
    else pipeline.fallbackConfirmed += 1;
  }

  const failedStates = Array.from(
    new Set(
      Object.values(stateByName)
        .map((value) => normalizeSourceState(value))
        .filter((value) => value && value !== SOURCE_STATES.OK)
    )
  );
  const failureMessages = Array.from(new Set(Object.values(failuresByName).filter(Boolean)));
  Object.defineProperty(byName, "__meta", {
    value: {
      failuresByName,
      stateByName,
      diagnosticsByName,
      market_enabled: true,
      credentials_present: Boolean(skinportApiKey),
      request_sent: true,
      response_status: 200,
      response_parsed: true,
      listings_found:
        Object.values(diagnosticsByName).some((value) => value?.listings_found === true) || null,
      buy_price_present:
        Object.values(diagnosticsByName).some((value) => value?.buy_price_present === true) ||
        null,
      sell_price_present:
        Object.values(diagnosticsByName).some((value) => value?.sell_price_present === true) ||
        null,
      freshness_present:
        Object.values(diagnosticsByName).some((value) => value?.freshness_present === true) ||
        null,
      listing_url_present:
        Object.values(diagnosticsByName).some((value) => value?.listing_url_present === true) ||
        null,
      sourceUnavailableReason:
        failedStates.length === 1 && failureMessages.length === 1 ? failureMessages[0] : null,
      sourceFailureReason: failedStates.length === 1 ? failedStates[0] : null,
      source_failure_reason: failedStates.length === 1 ? failedStates[0] : null,
      last_success_at:
        Object.values(diagnosticsByName).map((value) => value?.last_success_at).filter(Boolean).sort().slice(-1)[0] ||
        null,
      last_failure_at:
        Object.values(diagnosticsByName).map((value) => value?.last_failure_at).filter(Boolean).sort().slice(-1)[0] ||
        null,
      pipeline
    },
    enumerable: false
  });

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
    createSkinportPipelineDiagnostics,
    classifySkinportError,
    buildItemsUrl
  }
};

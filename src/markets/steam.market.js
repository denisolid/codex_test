const steamMarketPriceService = require("../services/steamMarketPriceService");
const { steamMarketCurrency, steamMarketTimeoutMs } = require("../config/env");
const { buildMarketPriceRecord } = require("./marketUtils");
const { mapWithConcurrency } = require("./marketHttp");
const {
  SOURCE_STATES,
  buildMarketHealthDiagnostics,
  attachMarketHealth,
  normalizeSourceState
} = require("./marketSourceDiagnostics");

const SOURCE = "steam";
const NO_LISTING_MESSAGE = "No Steam listing found.";
const NO_DATA_MESSAGE = "Steam returned no usable quote data.";
const PARSING_FAILURE_MESSAGE = "Steam price response could not be parsed.";
const TIMEOUT_MESSAGE = "Steam request timed out.";
const UNAVAILABLE_MESSAGE = "Steam is temporarily unavailable.";
const STEAM_CURRENCY_BY_CODE = Object.freeze({
  USD: 1,
  GBP: 2,
  EUR: 3
});

function buildSteamListingUrl(marketHashName) {
  return `https://steamcommunity.com/market/listings/730/${encodeURIComponent(
    String(marketHashName || "")
  )}`;
}

function resolveSteamCurrencyCode(input) {
  const text = String(input || "")
    .trim()
    .toUpperCase();
  if (text && STEAM_CURRENCY_BY_CODE[text]) {
    return text;
  }
  const numeric = Number(input || steamMarketCurrency || 1);
  if (numeric === 3) return "EUR";
  if (numeric === 2) return "GBP";
  return "USD";
}

function resolveSteamCurrencyId(options = {}) {
  const requestedCode = String(options.currency || "")
    .trim()
    .toUpperCase();
  if (requestedCode && STEAM_CURRENCY_BY_CODE[requestedCode]) {
    return STEAM_CURRENCY_BY_CODE[requestedCode];
  }
  const requestedNumeric = Number(options.steamCurrency);
  if (Number.isFinite(requestedNumeric) && requestedNumeric > 0) {
    return requestedNumeric;
  }
  return Number(steamMarketCurrency || 1);
}

function classifySteamError(err) {
  const status = Number(err?.statusCode || err?.status || err?.upstreamStatus || 0);
  const code = String(err?.code || "").trim().toUpperCase();
  const message = String(err?.message || "").trim().toLowerCase();

  if (code === "STEAM_MARKET_RATE_LIMITED" || status === 429) {
    return {
      state: SOURCE_STATES.UNAVAILABLE,
      reason: "Steam rate limit reached. Retry shortly.",
      responseStatus: 429
    };
  }
  if (code === "ABORT_ERR" || message.includes("timed out")) {
    return {
      state: SOURCE_STATES.TIMEOUT,
      reason: TIMEOUT_MESSAGE,
      responseStatus: status || 504
    };
  }
  if (status >= 500 || message.includes("unsuccessful response")) {
    return {
      state: SOURCE_STATES.UNAVAILABLE,
      reason: UNAVAILABLE_MESSAGE,
      responseStatus: status || 502
    };
  }
  return {
    state: SOURCE_STATES.UNAVAILABLE,
    reason: UNAVAILABLE_MESSAGE,
    responseStatus: status || null
  };
}

function buildDiagnostics({
  requestSent = false,
  responseStatus = null,
  responseParsed = false,
  listingsFound = null,
  buyPricePresent = false,
  listingUrlPresent = false,
  sourceFailureReason = null,
  lastSuccessAt = null,
  lastFailureAt = null
} = {}) {
  return buildMarketHealthDiagnostics({
    marketEnabled: true,
    credentialsPresent: null,
    authOk: null,
    requestSent,
    responseStatus,
    responseParsed,
    listingsFound,
    buyPricePresent,
    sellPricePresent: buyPricePresent,
    freshnessPresent: true,
    listingUrlPresent,
    sourceFailureReason,
    lastSuccessAt,
    lastFailureAt
  });
}

function buildFailureResult(state, reason, diagnostics = {}) {
  return {
    state: normalizeSourceState(state) || SOURCE_STATES.UNAVAILABLE,
    reason: String(reason || "").trim() || null,
    price: null,
    diagnostics: buildDiagnostics(diagnostics)
  };
}

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const requestStartedAt = new Date().toISOString();
  let overview = null;

  try {
    overview = await steamMarketPriceService.getPriceOverview(marketHashName, {
      currency: resolveSteamCurrencyId(input),
      timeoutMs: Number(input.timeoutMs || steamMarketTimeoutMs || 10000),
      maxRetries: input.maxRetries,
      includeRawPayload: true
    });
  } catch (err) {
    const failure = classifySteamError(err);
    return buildFailureResult(failure.state, failure.reason, {
      requestSent: true,
      responseStatus: failure.responseStatus,
      responseParsed: false,
      listingsFound: null,
      buyPricePresent: false,
      listingUrlPresent: false,
      sourceFailureReason: failure.state,
      lastFailureAt: requestStartedAt
    });
  }

  const rawPayload =
    overview?.rawPayload && typeof overview.rawPayload === "object" ? overview.rawPayload : {};
  const hasPriceField = rawPayload.lowest_price != null || rawPayload.median_price != null;
  const grossPrice =
    overview?.lowestPrice != null ? overview.lowestPrice : overview?.medianPrice;
  if (grossPrice == null) {
    if (hasPriceField) {
      return buildFailureResult(SOURCE_STATES.PARSING_FAILED, PARSING_FAILURE_MESSAGE, {
        requestSent: true,
        responseStatus: 200,
        responseParsed: false,
        listingsFound: true,
        buyPricePresent: false,
        listingUrlPresent: false,
        sourceFailureReason: SOURCE_STATES.PARSING_FAILED,
        lastFailureAt: requestStartedAt
      });
    }
    return buildFailureResult(SOURCE_STATES.NO_LISTING, NO_LISTING_MESSAGE, {
      requestSent: true,
      responseStatus: 200,
      responseParsed: true,
      listingsFound: false,
      buyPricePresent: false,
      listingUrlPresent: false,
      sourceFailureReason: SOURCE_STATES.NO_LISTING,
      lastFailureAt: requestStartedAt
    });
  }

  const url = buildSteamListingUrl(marketHashName);
  const successDiagnostics = buildDiagnostics({
    requestSent: true,
    responseStatus: 200,
    responseParsed: true,
    listingsFound: true,
    buyPricePresent: true,
    listingUrlPresent: true,
    sourceFailureReason: null,
    lastSuccessAt: requestStartedAt
  });
  const currencyCode = resolveSteamCurrencyCode(input.currency || input.steamCurrency);
  const record = buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice,
    currency: currencyCode,
    url,
    confidence: overview?.lowestPrice != null ? "high" : "medium",
    raw: attachMarketHealth(
      {
        ...(rawPayload || {}),
        steam_volume_24h: overview?.volume ?? null
      },
      successDiagnostics
    )
  });

  if (!record) {
    return buildFailureResult(SOURCE_STATES.NO_DATA, NO_DATA_MESSAGE, {
      requestSent: true,
      responseStatus: 200,
      responseParsed: true,
      listingsFound: true,
      buyPricePresent: false,
      listingUrlPresent: true,
      sourceFailureReason: SOURCE_STATES.NO_DATA,
      lastFailureAt: requestStartedAt
    });
  }

  return {
    state: SOURCE_STATES.OK,
    reason: null,
    diagnostics: successDiagnostics,
    price: record
  };
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

  const failuresByName = {};
  const stateByName = {};
  const diagnosticsByName = {};
  const stopOnRateLimit = options.stopOnRateLimit === true;
  let stopAfterRateLimit = false;
  const rows = await mapWithConcurrency(
    list,
    async (marketHashName) => {
      if (stopAfterRateLimit) {
        const skippedDiagnostics = buildDiagnostics({
          requestSent: false,
          responseStatus: 429,
          responseParsed: false,
          listingsFound: null,
          buyPricePresent: false,
          listingUrlPresent: false,
          sourceFailureReason: SOURCE_STATES.UNAVAILABLE,
          lastFailureAt: new Date().toISOString()
        });
        stateByName[marketHashName] = SOURCE_STATES.UNAVAILABLE;
        diagnosticsByName[marketHashName] = skippedDiagnostics;
        failuresByName[marketHashName] = "Steam rate limit reached. Retry shortly.";
        return null;
      }

      const result = await searchItemPrice({
        marketHashName,
        steamCurrency: options.steamCurrency,
        currency: options.currency,
        timeoutMs: options.timeoutMs,
        maxRetries: options.maxRetries
      });
      if (result?.state) {
        stateByName[marketHashName] = result.state;
      }
      if (result?.diagnostics) {
        diagnosticsByName[marketHashName] = result.diagnostics;
      }
      if (!result?.price) {
        failuresByName[marketHashName] = result?.reason || NO_DATA_MESSAGE;
        if (
          stopOnRateLimit &&
          Number(result?.diagnostics?.response_status || 0) === 429
        ) {
          stopAfterRateLimit = true;
        }
        return null;
      }
      return {
        marketHashName,
        price: result.price
      };
    },
    options.concurrency
  );

  const byName = {};
  let lastSuccessAt = null;
  let lastFailureAt = null;
  for (const row of rows) {
    if (!row?.marketHashName || !row?.price) continue;
    byName[row.marketHashName] = row.price;
    lastSuccessAt =
      diagnosticsByName[row.marketHashName]?.last_success_at || lastSuccessAt || new Date().toISOString();
  }
  for (const diagnostics of Object.values(diagnosticsByName)) {
    if (!diagnostics?.last_failure_at) continue;
    lastFailureAt = diagnostics.last_failure_at;
  }

  const failedStates = Array.from(
    new Set(
      Object.values(stateByName)
        .map((value) => normalizeSourceState(value))
        .filter((value) => value && value !== SOURCE_STATES.OK)
    )
  );
  Object.defineProperty(byName, "__meta", {
    value: {
      failuresByName,
      stateByName,
      diagnosticsByName,
      market_enabled: true,
      credentials_present: null,
      auth_ok: null,
      request_sent: list.length > 0,
      response_status: null,
      response_parsed:
        Object.values(diagnosticsByName).some((value) => value?.response_parsed === true) || null,
      listings_found:
        Object.values(diagnosticsByName).some((value) => value?.listings_found === true) || null,
      buy_price_present:
        Object.values(diagnosticsByName).some((value) => value?.buy_price_present === true) || null,
      sell_price_present:
        Object.values(diagnosticsByName).some((value) => value?.sell_price_present === true) || null,
      freshness_present: true,
      listing_url_present:
        Object.values(diagnosticsByName).some((value) => value?.listing_url_present === true) ||
        null,
      sourceUnavailableReason:
        failedStates.length === 1
          ? Array.from(new Set(Object.values(failuresByName).filter(Boolean)))[0] || null
          : null,
      sourceFailureReason: failedStates.length === 1 ? failedStates[0] : null,
      source_failure_reason: failedStates.length === 1 ? failedStates[0] : null,
      last_success_at: lastSuccessAt,
      last_failure_at: lastFailureAt
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
    resolveSteamCurrencyCode,
    resolveSteamCurrencyId,
    classifySteamError
  }
};

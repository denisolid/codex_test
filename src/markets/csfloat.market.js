const { csfloatApiUrl, csfloatApiKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const {
  buildMarketPriceRecord,
  normalizePriceFromMinorUnits,
  normalizePriceNumber
} = require("./marketUtils");

const SOURCE = "csfloat";
const DEFAULT_API_URL = "https://csfloat.com/api/v1";
const SOURCE_STATES = Object.freeze({
  AUTH_FAILED: "auth_failed",
  SOURCE_UNAVAILABLE: "source_unavailable",
  NO_LISTING: "no_listing",
  NO_QUOTE_DATA: "no_quote_data",
  OK: "ok"
});
const AUTH_FAILURE_MESSAGE = "CSFloat authentication failed. Check CSFLOAT_API_KEY.";
const SOURCE_UNAVAILABLE_MESSAGE = "CSFloat data unavailable.";
const NO_LISTING_MESSAGE = "No CSFloat listing found.";
const NO_QUOTE_DATA_MESSAGE = "CSFloat returned no usable quote data.";
const AUTH_HEADER_FORMAT = "Authorization: <API-KEY>";

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

function toSafeHttpUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return null;
    }
    return parsed.toString();
  } catch (_err) {
    return null;
  }
}

function sanitizeApiKey(rawValue) {
  let value = String(rawValue || "").trim();
  if (!value) return "";

  function stripWrappedQuotes(input) {
    let current = String(input || "").trim();
    while (current.length >= 2) {
      const first = current[0];
      const last = current[current.length - 1];
      const isQuotedPair =
        (first === "\"" && last === "\"") || (first === "'" && last === "'");
      if (!isQuotedPair) break;
      current = current.slice(1, -1).trim();
    }
    return current;
  }

  value = stripWrappedQuotes(value);
  const knownPrefixes = [
    /^csfloat_api_key\s*=\s*/i,
    /^authorization\s*:\s*/i,
    /^bearer\s+/i
  ];
  let changed = true;
  while (changed && value) {
    changed = false;
    for (const pattern of knownPrefixes) {
      const next = stripWrappedQuotes(value.replace(pattern, "").trim());
      if (next !== value) {
        value = next;
        changed = true;
      }
    }
  }

  // CSFloat keys are single-token credentials; whitespace makes auth fail.
  value = value.replace(/\s+/g, "");
  return value;
}

function buildRequestHeaders(apiKey = sanitizeApiKey(csfloatApiKey)) {
  const safeApiKey = sanitizeApiKey(apiKey);
  const baseHeaders = {
    Accept: "application/json",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };

  if (!safeApiKey) {
    return baseHeaders;
  }

  return {
    ...baseHeaders,
    Authorization: safeApiKey
  };
}

function buildHeaderVariantMap(apiKey = sanitizeApiKey(csfloatApiKey)) {
  const safeApiKey = sanitizeApiKey(apiKey);
  const variants = [];

  if (safeApiKey) {
    variants.push({
      headers: buildRequestHeaders(safeApiKey),
      apiKeyPresent: true,
      authHeaderSent: true
    });
  }

  variants.push({
    headers: buildRequestHeaders(""),
    apiKeyPresent: Boolean(safeApiKey),
    authHeaderSent: false
  });

  return variants;
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

function buildDiagnostics({
  apiKeyPresent = false,
  authHeaderSent = false,
  responseStatus = null,
  sourceFailureReason = null
} = {}) {
  const parsedStatus = Number(responseStatus);
  return {
    api_key_present: Boolean(apiKeyPresent),
    auth_header_sent: Boolean(authHeaderSent),
    response_status: Number.isFinite(parsedStatus) ? parsedStatus : null,
    source_failure_reason: sourceFailureReason
      ? String(sourceFailureReason || "").trim()
      : null
  };
}

function classifyCsfloatFetchError(err) {
  const status = Number(err?.upstreamStatus || err?.statusCode || err?.status || 0);
  if (status === 401 || status === 403) {
    return {
      state: SOURCE_STATES.AUTH_FAILED,
      reason: AUTH_FAILURE_MESSAGE,
      responseStatus: status || null,
      sourceFailureReason: SOURCE_STATES.AUTH_FAILED
    };
  }
  if (status === 429) {
    return {
      state: SOURCE_STATES.SOURCE_UNAVAILABLE,
      reason: "CSFloat rate limit reached. Retry shortly.",
      responseStatus: status,
      sourceFailureReason: SOURCE_STATES.SOURCE_UNAVAILABLE
    };
  }
  if (status >= 500) {
    return {
      state: SOURCE_STATES.SOURCE_UNAVAILABLE,
      reason: "CSFloat is temporarily unavailable.",
      responseStatus: status,
      sourceFailureReason: SOURCE_STATES.SOURCE_UNAVAILABLE
    };
  }
  const message = String(err?.message || "").toLowerCase();
  if (message.includes("timed out")) {
    return {
      state: SOURCE_STATES.SOURCE_UNAVAILABLE,
      reason: "CSFloat request timed out.",
      responseStatus: status || 504,
      sourceFailureReason: SOURCE_STATES.SOURCE_UNAVAILABLE
    };
  }
  return {
    state: SOURCE_STATES.SOURCE_UNAVAILABLE,
    reason: SOURCE_UNAVAILABLE_MESSAGE,
    responseStatus: status || null,
    sourceFailureReason: SOURCE_STATES.SOURCE_UNAVAILABLE
  };
}

function describeCsfloatFetchError(err) {
  return classifyCsfloatFetchError(err).reason;
}

function getPayloadListings(payload) {
  return Array.isArray(payload)
    ? payload
    : Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(payload?.listings)
      ? payload.listings
      : Array.isArray(payload?.results)
        ? payload.results
        : [];
}

function extractBestListing(payload, marketHashName = "") {
  const list = getPayloadListings(payload);

  const first = list[0] || null;
  if (!first) return null;

  const directUrl = toSafeHttpUrl(
    first.url ||
      first.listing_url ||
      first.listingUrl ||
      first.item_url ||
      first.itemUrl ||
      first.market_url ||
      first.marketUrl ||
      first.permalink ||
      first.link ||
      first?.item?.url ||
      first?.item?.item_url ||
      first?.item?.itemUrl
  );

  const directId =
    first.id ||
    first.listing_id ||
    first.listingId ||
    first.item_id ||
    first.itemId ||
    first?.item?.id ||
    first?.item?.item_id ||
    first?.item?.itemId ||
    first?.item?.slug ||
    null;
  const idUrl = directId
    ? `https://csfloat.com/item/${encodeURIComponent(String(directId))}`
    : null;

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
        url: directUrl || idUrl || buildListingUrl(marketHashName),
        raw: first
      };
    }
  }

  return null;
}

function buildFailureResult(state, reason, diagnostics = {}) {
  return {
    state,
    price: null,
    reason: String(reason || "").trim() || null,
    diagnostics: buildDiagnostics(diagnostics)
  };
}

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const apiKey = sanitizeApiKey(csfloatApiKey);
  const url = buildApiUrl(marketHashName);
  const headerVariants = buildHeaderVariantMap(apiKey);
  let payload = null;
  let resolvedVariant = headerVariants[0] || {
    headers: buildRequestHeaders(""),
    apiKeyPresent: false,
    authHeaderSent: false
  };

  for (let index = 0; index < headerVariants.length; index += 1) {
    const variant = headerVariants[index];
    try {
      payload = await fetchJsonWithRetry(url, {
        timeoutMs: input.timeoutMs,
        maxRetries: input.maxRetries,
        headers: variant.headers
      });
      resolvedVariant = variant;
      break;
    } catch (err) {
      const failure = classifyCsfloatFetchError(err);
      const canRetryPublicLookup =
        failure.state === SOURCE_STATES.AUTH_FAILED &&
        variant.authHeaderSent &&
        index < headerVariants.length - 1;
      if (canRetryPublicLookup) {
        continue;
      }
      return buildFailureResult(failure.state, failure.reason, {
        apiKeyPresent: variant.apiKeyPresent,
        authHeaderSent: variant.authHeaderSent,
        responseStatus: failure.responseStatus,
        sourceFailureReason: failure.sourceFailureReason
      });
    }
  }

  const best = extractBestListing(payload, marketHashName);
  if (!best) {
    const listings = getPayloadListings(payload);
    const state = listings.length ? SOURCE_STATES.NO_QUOTE_DATA : SOURCE_STATES.NO_LISTING;
    const reason = state === SOURCE_STATES.NO_QUOTE_DATA
      ? NO_QUOTE_DATA_MESSAGE
      : NO_LISTING_MESSAGE;
    return buildFailureResult(state, reason, {
      apiKeyPresent: resolvedVariant.apiKeyPresent,
      authHeaderSent: resolvedVariant.authHeaderSent,
      responseStatus: 200,
      sourceFailureReason: state
    });
  }

  return {
    state: SOURCE_STATES.OK,
    reason: null,
    diagnostics: buildDiagnostics({
      apiKeyPresent: resolvedVariant.apiKeyPresent,
      authHeaderSent: resolvedVariant.authHeaderSent,
      responseStatus: 200,
      sourceFailureReason: null
    }),
    price: buildMarketPriceRecord({
      source: SOURCE,
      marketHashName,
      grossPrice: best.price,
      currency: best.currency || "USD",
      url: best.url || buildListingUrl(marketHashName),
      confidence: "medium",
      raw: best.raw
    })
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
  const apiKeyPresent = Boolean(sanitizeApiKey(csfloatApiKey));
  const rows = await mapWithConcurrency(
    list,
    async (marketHashName) => {
      try {
        const result = await searchItemPrice({
          marketHashName,
          timeoutMs: options.timeoutMs,
          maxRetries: options.maxRetries
        });
        if (result?.state) {
          stateByName[marketHashName] = result.state;
        }
        if (result?.diagnostics && typeof result.diagnostics === "object") {
          diagnosticsByName[marketHashName] = result.diagnostics;
        }
        if (!result?.price) {
          if (result?.reason) {
            failuresByName[marketHashName] = result.reason;
          }
          return null;
        }
        return result.price
          ? {
              marketHashName,
              price: result.price
            }
          : null;
      } catch (err) {
        const failure = classifyCsfloatFetchError(err);
        stateByName[marketHashName] = failure.state;
        diagnosticsByName[marketHashName] = buildDiagnostics({
          apiKeyPresent,
          authHeaderSent: apiKeyPresent,
          responseStatus: failure.responseStatus,
          sourceFailureReason: failure.sourceFailureReason
        });
        failuresByName[marketHashName] = failure.reason;
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

  const sourceLevelStates = Object.entries(stateByName)
    .filter(([, value]) =>
      [SOURCE_STATES.AUTH_FAILED, SOURCE_STATES.SOURCE_UNAVAILABLE].includes(value)
    )
    .map(([, value]) => value);
  const uniqueSourceLevelStates = Array.from(new Set(sourceLevelStates));
  const uniqueUnavailableReasons = Array.from(
    new Set(
      Object.entries(failuresByName)
        .filter(([marketHashName]) =>
          [SOURCE_STATES.AUTH_FAILED, SOURCE_STATES.SOURCE_UNAVAILABLE].includes(
            stateByName[marketHashName]
          )
        )
        .map(([, value]) => String(value || "").trim())
        .filter(Boolean)
    )
  );
  const uniqueResponseStatuses = Array.from(
    new Set(
      Object.values(diagnosticsByName)
        .map((value) => Number(value?.response_status))
        .filter((value) => Number.isFinite(value))
    )
  );
  Object.defineProperty(byName, "__meta", {
    value: {
      failuresByName,
      stateByName,
      diagnosticsByName,
      sourceUnavailableReason:
        uniqueSourceLevelStates.length === 1 && uniqueUnavailableReasons.length === 1
          ? uniqueUnavailableReasons[0]
          : null,
      sourceFailureReason:
        uniqueSourceLevelStates.length === 1 ? uniqueSourceLevelStates[0] : null,
      api_key_present: apiKeyPresent,
      auth_header_sent: apiKeyPresent,
      response_status: uniqueResponseStatuses.length === 1 ? uniqueResponseStatuses[0] : null,
      source_failure_reason:
        uniqueSourceLevelStates.length === 1 ? uniqueSourceLevelStates[0] : null,
      pipeline: {
        auth_header_format: AUTH_HEADER_FORMAT
      }
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
    extractBestListing,
    toSafeHttpUrl,
    buildHeaderVariantMap,
    describeCsfloatFetchError,
    sanitizeApiKey,
    buildRequestHeaders,
    classifyCsfloatFetchError,
    getPayloadListings,
    buildDiagnostics,
    SOURCE_STATES
  }
};

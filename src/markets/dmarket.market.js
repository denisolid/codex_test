const { dmarketApiUrl, dmarketPublicKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const {
  buildMarketPriceRecord,
  normalizePriceNumber,
  normalizePriceFromMinorUnits
} = require("./marketUtils");
const {
  SOURCE_STATES,
  buildMarketHealthDiagnostics,
  attachMarketHealth,
  normalizeSourceState
} = require("./marketSourceDiagnostics");

const SOURCE = "dmarket";
// This integration is read-only and still relies on exchange/v1 `offers-by-title`.
// If we add authenticated offer create/update/delete flows, those must use Marketplace API v2.
const DEFAULT_API_URL = "https://api.dmarket.com/exchange/v1";
const DEFAULT_GAME_ID = "a8db";
const OFFER_SCAN_LIMIT = 20;
const OFFERS_BY_TITLE_PATH = "/offers-by-title";
const AUTH_FAILURE_MESSAGE = "DMarket authentication failed. Check DMARKET_PUBLIC_KEY.";
const NO_LISTING_MESSAGE = "No DMarket listing found.";
const NO_DATA_MESSAGE = "DMarket returned no usable quote data.";
const PARSING_FAILURE_MESSAGE = "DMarket response could not be parsed.";
const TIMEOUT_MESSAGE = "DMarket request timed out.";
const UNAVAILABLE_MESSAGE = "DMarket is temporarily unavailable.";

function toApiBaseUrl() {
  const raw = String(dmarketApiUrl || DEFAULT_API_URL).trim();
  return raw.replace(/\/+$/, "");
}

function buildApiUrl(marketHashName) {
  const params = new URLSearchParams({
    gameId: DEFAULT_GAME_ID,
    title: String(marketHashName || ""),
    limit: String(OFFER_SCAN_LIMIT)
  });

  return `${toApiBaseUrl()}${OFFERS_BY_TITLE_PATH}?${params.toString()}`;
}

function buildListingUrl(marketHashName) {
  const title = String(marketHashName || "").trim();
  const params = new URLSearchParams();
  if (title) {
    // "title" is used by DMarket's webapp; keep "searchTitle" as a backward-compatible hint.
    params.set("title", title);
    params.set("searchTitle", title);
  }
  const query = params.toString();
  return `https://dmarket.com/ingame-items/item-list/csgo-skins${query ? `?${query}` : ""}`;
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

function resolveOfferUrl(offer = {}, marketHashName = "") {
  const directCandidates = [
    offer.itemPage,
    offer.item_page,
    offer.marketPage,
    offer.market_page,
    offer.url,
    offer.offerUrl,
    offer.offer_url,
    offer.webUrl,
    offer.web_url,
    offer.externalUrl,
    offer.external_url,
    offer.link,
    offer?.item?.itemPage,
    offer?.item?.item_page,
    offer?.item?.marketPage,
    offer?.item?.market_page,
    offer?.item?.url,
    offer?.item?.externalUrl,
    offer?.item?.external_url,
    offer?.links?.itemPage,
    offer?.links?.marketPage,
    offer?.links?.url
  ];

  for (const candidate of directCandidates) {
    const safe = toSafeHttpUrl(candidate);
    if (safe) return safe;
  }

  return buildListingUrl(marketHashName);
}

function buildHeaders(useApiKey = false) {
  const headers = {
    Accept: "application/json",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };

  if (useApiKey && String(dmarketPublicKey || "").trim()) {
    headers["X-Api-Key"] = String(dmarketPublicKey || "").trim();
  }

  return headers;
}

function buildHeaderVariants() {
  const hasPublicKey = Boolean(String(dmarketPublicKey || "").trim());
  const variants = [
    {
      headers: buildHeaders(false),
      credentialsPresent: hasPublicKey,
      authRequested: false
    }
  ];
  if (hasPublicKey) {
    variants.push({
      headers: buildHeaders(true),
      credentialsPresent: true,
      authRequested: true
    });
  }
  return variants;
}

function extractPrice(offer = {}) {
  const priceObj = offer?.price || {};
  const usdCandidates = [priceObj.USD, priceObj.usd, offer.priceUSD, offer.priceUsd, offer.usdPrice];
  for (const candidate of usdCandidates) {
    const text = String(candidate ?? "").trim();
    if (!text) continue;
    const parsed = /^-?\d+$/.test(text)
      ? normalizePriceFromMinorUnits(Number(text))
      : normalizePriceNumber(text);
    if (parsed != null) return parsed;
  }

  const nestedCandidates = [priceObj.amount, priceObj.value, priceObj.price];
  for (const candidate of nestedCandidates) {
    const parsed = normalizePriceNumber(candidate);
    if (parsed != null) return parsed;
  }

  const directCandidates = [offer.price, offer.value];
  for (const candidate of directCandidates) {
    if (typeof candidate !== "number" && typeof candidate !== "string") {
      continue;
    }
    const parsed = normalizePriceNumber(candidate);
    if (parsed != null) return parsed;
  }

  return null;
}

function normalizeTitle(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function getPayloadOffers(payload = {}) {
  return Array.isArray(payload?.objects)
    ? payload.objects
    : Array.isArray(payload?.items)
      ? payload.items
      : Array.isArray(payload?.results)
        ? payload.results
        : Array.isArray(payload?.offers)
          ? payload.offers
          : [];
}

function extractBestOffer(payload = {}, requestedMarketHashName = "") {
  const list = getPayloadOffers(payload);

  if (!list.length) return null;
  const requestedTitle = normalizeTitle(requestedMarketHashName);

  const pricedOffers = list
    .map((offer) => {
      const price = extractPrice(offer);
      if (price == null) return null;
      const offerTitle = normalizeTitle(
        offer?.title || offer?.marketHashName || offer?.item?.title || offer?.item?.marketHashName
      );
      return {
        offer,
        price,
        exactTitleMatch: Boolean(requestedTitle && offerTitle && requestedTitle === offerTitle)
      };
    })
    .filter(Boolean);

  if (!pricedOffers.length) return null;

  const exactMatches = pricedOffers.filter((candidate) => candidate.exactTitleMatch);
  const candidatePool = exactMatches.length ? exactMatches : pricedOffers;
  const selected =
    candidatePool.sort((a, b) => Number(a.price) - Number(b.price))[0] || null;
  if (!selected) return null;
  const offer = selected.offer;

  return {
    price: selected.price,
    currency: String(offer.currency || payload.currency || "USD").trim().toUpperCase(),
    url: resolveOfferUrl(
      offer,
      requestedMarketHashName || offer.title || payload.title || ""
    ),
    raw: offer
  };
}

function classifyDmarketError(err, authRequested = false) {
  const status = Number(err?.upstreamStatus || err?.statusCode || err?.status || 0);
  const message = String(err?.message || "").trim().toLowerCase();

  if ((status === 401 || status === 403) && authRequested) {
    return {
      state: SOURCE_STATES.AUTH_FAILED,
      reason: AUTH_FAILURE_MESSAGE,
      responseStatus: status
    };
  }
  if (message.includes("timed out")) {
    return {
      state: SOURCE_STATES.TIMEOUT,
      reason: TIMEOUT_MESSAGE,
      responseStatus: status || 504
    };
  }
  if (status === 429) {
    return {
      state: SOURCE_STATES.UNAVAILABLE,
      reason: "DMarket rate limit reached. Retry shortly.",
      responseStatus: status
    };
  }
  if (status >= 500) {
    return {
      state: SOURCE_STATES.UNAVAILABLE,
      reason: UNAVAILABLE_MESSAGE,
      responseStatus: status
    };
  }
  return {
    state: SOURCE_STATES.UNAVAILABLE,
    reason: UNAVAILABLE_MESSAGE,
    responseStatus: status || null
  };
}

function buildDiagnostics({
  credentialsPresent = false,
  authOk = null,
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
    credentialsPresent,
    authOk,
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
    price: null,
    reason: String(reason || "").trim() || null,
    diagnostics: buildDiagnostics(diagnostics)
  };
}

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const url = buildApiUrl(marketHashName);
  const requestStartedAt = new Date().toISOString();
  const variants = buildHeaderVariants();
  let payload = null;
  let resolvedVariant = variants[0];

  for (const variant of variants) {
    try {
      payload = await fetchJsonWithRetry(url, {
        timeoutMs: input.timeoutMs,
        maxRetries: input.maxRetries,
        headers: variant.headers
      });
      resolvedVariant = variant;
      break;
    } catch (err) {
      const failure = classifyDmarketError(err, variant.authRequested);
      const canRetryUnauthed =
        failure.state === SOURCE_STATES.AUTH_FAILED &&
        variant.authRequested &&
        variants[0] !== variant;
      if (canRetryUnauthed) {
        continue;
      }
      return buildFailureResult(failure.state, failure.reason, {
        credentialsPresent: variant.credentialsPresent,
        authOk: variant.authRequested ? false : null,
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
  }

  const offers = getPayloadOffers(payload);
  if (!Array.isArray(offers)) {
    return buildFailureResult(SOURCE_STATES.PARSING_FAILED, PARSING_FAILURE_MESSAGE, {
      credentialsPresent: resolvedVariant.credentialsPresent,
      authOk: resolvedVariant.authRequested ? true : null,
      requestSent: true,
      responseStatus: 200,
      responseParsed: false,
      listingsFound: null,
      buyPricePresent: false,
      listingUrlPresent: false,
      sourceFailureReason: SOURCE_STATES.PARSING_FAILED,
      lastFailureAt: requestStartedAt
    });
  }
  if (!offers.length) {
    return buildFailureResult(SOURCE_STATES.NO_LISTING, NO_LISTING_MESSAGE, {
      credentialsPresent: resolvedVariant.credentialsPresent,
      authOk: resolvedVariant.authRequested ? true : null,
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

  const best = extractBestOffer(payload, marketHashName);
  if (!best) {
    return buildFailureResult(SOURCE_STATES.NO_DATA, NO_DATA_MESSAGE, {
      credentialsPresent: resolvedVariant.credentialsPresent,
      authOk: resolvedVariant.authRequested ? true : null,
      requestSent: true,
      responseStatus: 200,
      responseParsed: true,
      listingsFound: true,
      buyPricePresent: false,
      listingUrlPresent: false,
      sourceFailureReason: SOURCE_STATES.NO_DATA,
      lastFailureAt: requestStartedAt
    });
  }

  const diagnostics = buildDiagnostics({
    credentialsPresent: resolvedVariant.credentialsPresent,
    authOk: resolvedVariant.authRequested ? true : null,
    requestSent: true,
    responseStatus: 200,
    responseParsed: true,
    listingsFound: true,
    buyPricePresent: true,
    listingUrlPresent: Boolean(best.url),
    sourceFailureReason: null,
    lastSuccessAt: requestStartedAt
  });
  const price = buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice: best.price,
    currency: best.currency || "USD",
    url: best.url || buildListingUrl(marketHashName),
    confidence: "low",
    raw: attachMarketHealth(best.raw, diagnostics)
  });

  if (!price) {
    return buildFailureResult(SOURCE_STATES.NO_DATA, NO_DATA_MESSAGE, {
      credentialsPresent: resolvedVariant.credentialsPresent,
      authOk: resolvedVariant.authRequested ? true : null,
      requestSent: true,
      responseStatus: 200,
      responseParsed: true,
      listingsFound: true,
      buyPricePresent: false,
      listingUrlPresent: Boolean(best.url),
      sourceFailureReason: SOURCE_STATES.NO_DATA,
      lastFailureAt: requestStartedAt
    });
  }

  return {
    state: SOURCE_STATES.OK,
    reason: null,
    diagnostics,
    price
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
  const rows = await mapWithConcurrency(
    list,
    async (marketHashName) => {
      const result = await searchItemPrice({
        marketHashName,
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
      credentials_present: Boolean(String(dmarketPublicKey || "").trim()),
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
        Object.values(diagnosticsByName).some((value) => value?.sell_price_present === true) ||
        null,
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
    buildApiUrl,
    extractPrice,
    toSafeHttpUrl,
    resolveOfferUrl,
    extractBestOffer,
    buildHeaderVariants,
    classifyDmarketError,
    getPayloadOffers
  }
};

const { dmarketApiUrl, dmarketPublicKey } = require("../config/env");
const { fetchJsonWithRetry, mapWithConcurrency } = require("./marketHttp");
const {
  buildMarketPriceRecord,
  normalizePriceNumber,
  normalizePriceFromMinorUnits
} = require("./marketUtils");

const SOURCE = "dmarket";
// This integration is read-only and still relies on exchange/v1 `offers-by-title`.
// If we add authenticated offer create/update/delete flows, those must use Marketplace API v2.
const DEFAULT_API_URL = "https://api.dmarket.com/exchange/v1";
const DEFAULT_GAME_ID = "a8db";
const OFFER_SCAN_LIMIT = 20;
const OFFERS_BY_TITLE_PATH = "/offers-by-title";

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

function extractBestOffer(payload = {}, requestedMarketHashName = "") {
  const list = Array.isArray(payload?.objects)
    ? payload.objects
    : Array.isArray(payload?.items)
      ? payload.items
      : Array.isArray(payload?.results)
        ? payload.results
        : Array.isArray(payload?.offers)
          ? payload.offers
          : [];

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

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const url = buildApiUrl(marketHashName);
  const payload = await fetchJsonWithRetry(url, {
    timeoutMs: input.timeoutMs,
    maxRetries: input.maxRetries,
    headers: buildHeaders()
  });

  const best = extractBestOffer(payload, marketHashName);
  if (!best) return null;

  return buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice: best.price,
    currency: best.currency || "USD",
    url: best.url || buildListingUrl(marketHashName),
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
  batchGetPrices,
  __testables: {
    buildApiUrl,
    extractPrice,
    toSafeHttpUrl,
    resolveOfferUrl,
    extractBestOffer
  }
};

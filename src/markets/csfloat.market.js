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

function buildHeaderVariantMap() {
  const baseHeaders = {
    Accept: "application/json",
    "User-Agent": "cs2-portfolio-analyzer/1.0"
  };

  const apiKey = sanitizeApiKey(csfloatApiKey);
  if (!apiKey) {
    return [baseHeaders];
  }

  const variants = [
    { ...baseHeaders, Authorization: apiKey },
    { ...baseHeaders, Authorization: `Bearer ${apiKey}` },
    { ...baseHeaders, "X-Api-Key": apiKey },
    { ...baseHeaders, "X-API-Key": apiKey }
  ];

  const seen = new Set();
  const unique = [];
  for (const headers of variants) {
    const key = JSON.stringify(headers);
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(headers);
  }

  return unique;
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

function describeCsfloatFetchError(err) {
  const status = Number(err?.upstreamStatus || err?.statusCode || err?.status || 0);
  if (status === 401 || status === 403) {
    return "CSFloat authentication failed. Check CSFLOAT_API_KEY.";
  }
  if (status === 429) {
    return "CSFloat rate limit reached. Retry shortly.";
  }
  if (status >= 500) {
    return "CSFloat is temporarily unavailable.";
  }
  const message = String(err?.message || "").toLowerCase();
  if (message.includes("timed out")) {
    return "CSFloat request timed out.";
  }
  return "CSFloat data unavailable.";
}

function extractBestListing(payload, marketHashName = "") {
  const list = Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(payload?.listings)
      ? payload.listings
      : Array.isArray(payload?.results)
        ? payload.results
        : [];

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

async function searchItemPrice(input = {}) {
  const marketHashName = String(input.marketHashName || "").trim();
  if (!marketHashName) return null;

  const url = buildApiUrl(marketHashName);
  const headerVariants = buildHeaderVariantMap();
  let lastError = null;
  let payload = null;

  for (const headers of headerVariants) {
    try {
      payload = await fetchJsonWithRetry(url, {
        timeoutMs: input.timeoutMs,
        maxRetries: input.maxRetries,
        headers
      });
      if (payload) break;
    } catch (err) {
      lastError = err;
      const status = Number(err?.upstreamStatus || err?.statusCode || err?.status || 0);
      if (status === 401 || status === 403) {
        continue;
      }
      throw err;
    }
  }

  if (!payload) {
    if (lastError) throw lastError;
    return null;
  }

  const best = extractBestListing(payload, marketHashName);
  if (!best) return null;

  return buildMarketPriceRecord({
    source: SOURCE,
    marketHashName,
    grossPrice: best.price,
    currency: best.currency || "USD",
    url: best.url || buildListingUrl(marketHashName),
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

  const failuresByName = {};
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
      } catch (err) {
        failuresByName[marketHashName] = describeCsfloatFetchError(err);
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

  if (Object.keys(failuresByName).length) {
    const uniqueReasons = Array.from(
      new Set(
        Object.values(failuresByName)
          .map((value) => String(value || "").trim())
          .filter(Boolean)
      )
    );
    const sourceUnavailableReason = uniqueReasons.length === 1 ? uniqueReasons[0] : null;
    Object.defineProperty(byName, "__meta", {
      value: {
        failuresByName,
        sourceUnavailableReason
      },
      enumerable: false
    });
  }

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
    sanitizeApiKey
  }
};

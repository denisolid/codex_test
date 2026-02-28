const AppError = require("../utils/AppError");
const {
  marketPriceRateLimitPerSecond,
  steamMarketMaxRetries,
  steamMarketRetryBaseMs,
  steamMarketPriceStrategy
} = require("../config/env");

let queue = Promise.resolve();
let nextRequestNotBefore = 0;

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function jitter(ms) {
  return Math.floor(Math.random() * Math.max(ms, 1));
}

function isRetryable(err) {
  const status = Number(err?.statusCode || 0);
  return [429, 502, 503, 504].includes(status);
}

function enqueueSteamRequest(task) {
  const minGapMs = Math.max(
    Math.floor(1000 / Math.max(marketPriceRateLimitPerSecond, 1)),
    1
  );

  const run = async () => {
    const now = Date.now();
    const waitMs = Math.max(nextRequestNotBefore - now, 0);
    if (waitMs > 0) {
      await sleep(waitMs);
    }
    nextRequestNotBefore = Date.now() + minGapMs;
    return task();
  };

  const queued = queue.then(run, run);
  queue = queued.catch(() => {});
  return queued;
}

function parsePriceString(value) {
  if (!value || typeof value !== "string") return null;

  const token = value.replace(/\s/g, "").match(/[\d.,]+/);
  if (!token) return null;

  let numeric = token[0];
  const hasComma = numeric.includes(",");
  const hasDot = numeric.includes(".");

  if (hasComma && hasDot) {
    const lastComma = numeric.lastIndexOf(",");
    const lastDot = numeric.lastIndexOf(".");
    if (lastComma > lastDot) {
      numeric = numeric.replace(/\./g, "").replace(",", ".");
    } else {
      numeric = numeric.replace(/,/g, "");
    }
  } else if (hasComma) {
    const parts = numeric.split(",");
    if (parts.length === 2 && parts[1].length <= 2) {
      numeric = numeric.replace(",", ".");
    } else {
      numeric = numeric.replace(/,/g, "");
    }
  } else if (hasDot) {
    const parts = numeric.split(".");
    if (!(parts.length === 2 && parts[1].length <= 2)) {
      numeric = numeric.replace(/\./g, "");
    }
  }

  const n = Number(numeric);
  if (Number.isNaN(n) || n < 0) return null;
  return Number(n.toFixed(2));
}

function parseIntegerString(value) {
  if (value == null) return null;
  const digits = String(value).replace(/[^\d]/g, "");
  if (!digits) return null;
  const n = Number(digits);
  if (!Number.isFinite(n) || n < 0) return null;
  return n;
}

function buildMarketOverviewUrl(marketHashName, currency) {
  const params = new URLSearchParams();
  params.set("appid", "730");
  params.set("currency", String(currency));
  params.set("market_hash_name", marketHashName);
  return `https://steamcommunity.com/market/priceoverview/?${params.toString()}`;
}

async function fetchOverviewWithRetries(url, timeoutMs, maxRetries) {
  let lastErr = null;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await enqueueSteamRequest(() =>
        fetch(url, {
          method: "GET",
          signal: controller.signal,
          headers: {
            "User-Agent": "cs2-portfolio-analyzer/1.0",
            Accept: "application/json"
          }
        })
      );

      if (res.status === 429) {
        const retryAfter = Number(res.headers.get("retry-after") || 0);
        const err = new AppError("Steam market rate limited", 429);
        if (retryAfter > 0) {
          err.retryAfterMs = retryAfter * 1000;
        }
        throw err;
      }

      if (!res.ok) {
        throw new AppError(`Steam market price failed with status ${res.status}`, 502);
      }

      const payload = await res.json();
      if (!payload || payload.success !== true) {
        throw new AppError("Steam market returned unsuccessful response", 502);
      }

      return payload;
    } catch (err) {
      if (err.name === "AbortError") {
        lastErr = new AppError("Steam market price request timed out", 504);
      } else {
        lastErr = err;
      }

      if (attempt < maxRetries && isRetryable(lastErr)) {
        const retryAfterMs = Number(lastErr.retryAfterMs || 0);
        const backoffBase = Math.max(Number(steamMarketRetryBaseMs || 300), 50);
        const backoffMs = Math.max(
          retryAfterMs,
          backoffBase * 2 ** (attempt - 1) + jitter(backoffBase)
        );
        await sleep(backoffMs);
        continue;
      }

      throw lastErr;
    } finally {
      clearTimeout(t);
    }
  }

  throw lastErr || new AppError("Steam market pricing failed", 502);
}

function parseOverview(payload) {
  return {
    lowestPrice: parsePriceString(payload.lowest_price),
    medianPrice: parsePriceString(payload.median_price),
    volume: parseIntegerString(payload.volume)
  };
}

function pickLatestPriceFromOverview(overview, strategyInput) {
  const strategy = String(strategyInput || steamMarketPriceStrategy || "balanced")
    .trim()
    .toLowerCase();
  const lowest = Number(overview?.lowestPrice);
  const median = Number(overview?.medianPrice);
  const volume = Number(overview?.volume);

  const hasLowest = Number.isFinite(lowest) && lowest >= 0;
  const hasMedian = Number.isFinite(median) && median >= 0;

  if (!hasLowest && !hasMedian) {
    return null;
  }

  if (strategy === "lowest") {
    return hasLowest ? lowest : median;
  }

  if (strategy === "median") {
    return hasMedian ? median : lowest;
  }

  // Balanced strategy aims at executable fair value:
  // use median for thin books or obvious low-side outliers.
  if (hasLowest && hasMedian) {
    if (Number.isFinite(volume) && volume < 3) {
      return median;
    }

    const ratio = median > 0 ? lowest / median : 1;
    if (ratio < 0.82) {
      return median;
    }

    return Number((lowest * 0.45 + median * 0.55).toFixed(2));
  }

  return hasMedian ? median : lowest;
}

exports.getPriceOverview = async (marketHashName, options = {}) => {
  const timeoutMs = Number(options.timeoutMs || 10000);
  const currency = Number(options.currency || 1);
  const maxRetries = Math.max(
    Number(options.maxRetries || steamMarketMaxRetries),
    1
  );

  const url = buildMarketOverviewUrl(marketHashName, currency);
  const payload = await fetchOverviewWithRetries(url, timeoutMs, maxRetries);
  return parseOverview(payload);
};

exports.getLatestPrice = async (marketHashName, options = {}) => {
  const overview = await exports.getPriceOverview(marketHashName, options);
  const price = pickLatestPriceFromOverview(overview, options.priceStrategy);

  if (price == null) {
    throw new AppError("Steam market returned no parsable price", 502);
  }

  return price;
};

exports.__testables = {
  parsePriceString,
  parseIntegerString,
  parseOverview,
  pickLatestPriceFromOverview
};

const {
  defaultDisplayCurrency,
  fxRatesUsdJson,
  fxRatesSource,
  fxRatesApiUrl,
  fxRatesRefreshMinutes,
  fxRatesRequestTimeoutMs,
  fxRatesFailureCooldownSeconds
} = require("../config/env");
const AppError = require("../utils/AppError");

const DEFAULT_RATES = {
  USD: 1,
  EUR: 0.92,
  GBP: 0.79,
  UAH: 41.2,
  PLN: 4.02,
  CZK: 23.5
};

function normalizeCode(code) {
  return String(code || "")
    .trim()
    .toUpperCase();
}

function parseEnvRates(jsonText) {
  if (!jsonText) {
    return {};
  }

  try {
    const parsed = JSON.parse(jsonText);
    if (!parsed || typeof parsed !== "object") {
      return {};
    }

    const sanitized = {};
    for (const [key, value] of Object.entries(parsed)) {
      const code = normalizeCode(key);
      const rate = Number(value);
      if (code && Number.isFinite(rate) && rate > 0) {
        sanitized[code] = rate;
      }
    }
    return sanitized;
  } catch (_err) {
    return {};
  }
}

const BASE_RATE_MAP = Object.freeze({
  ...DEFAULT_RATES,
  ...parseEnvRates(fxRatesUsdJson),
  USD: 1
});

const SUPPORTED_CODES = Object.freeze(Object.keys(BASE_RATE_MAP).sort());
const FX_SOURCE = String(fxRatesSource || "static").trim().toLowerCase();
const REFRESH_MS = Math.max(Number(fxRatesRefreshMinutes || 0), 1) * 60 * 1000;
const REQUEST_TIMEOUT_MS = Math.max(Number(fxRatesRequestTimeoutMs || 0), 500);
const FAILURE_COOLDOWN_MS =
  Math.max(Number(fxRatesFailureCooldownSeconds || 0), 5) * 1000;
const FX_API_URL = String(fxRatesApiUrl || "").trim();

let liveRateMap = null;
let lastSuccessAt = 0;
let lastFailureAt = 0;
let lastErrorMessage = "";
let refreshInFlight = null;

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

function getActiveRateMap() {
  return liveRateMap || BASE_RATE_MAP;
}

function resolveCurrency(requestedCode) {
  const configuredFallback = normalizeCode(defaultDisplayCurrency) || "USD";
  const fallback = BASE_RATE_MAP[configuredFallback] ? configuredFallback : "USD";
  const normalized = normalizeCode(requestedCode) || fallback;

  if (!BASE_RATE_MAP[normalized]) {
    throw new AppError(
      `Unsupported currency "${requestedCode}". Supported: ${SUPPORTED_CODES.join(", ")}`,
      400
    );
  }

  return normalized;
}

function convertUsdAmount(amount, currencyCode) {
  if (amount == null) return null;
  const n = Number(amount);
  if (!Number.isFinite(n)) return null;
  const code = resolveCurrency(currencyCode);
  const rate = Number(getActiveRateMap()[code] || BASE_RATE_MAP[code] || 1);
  return round2(n * rate);
}

function getSupportedCurrencies() {
  return SUPPORTED_CODES.slice();
}

function sanitizeLiveRates(payload = {}) {
  const safeRates = payload?.rates;
  if (!safeRates || typeof safeRates !== "object") {
    throw new AppError("FX provider returned invalid payload", 502);
  }

  const merged = { ...BASE_RATE_MAP };
  for (const code of SUPPORTED_CODES) {
    if (code === "USD") {
      merged.USD = 1;
      continue;
    }

    const value = Number(safeRates[code]);
    if (Number.isFinite(value) && value > 0) {
      merged[code] = value;
    }
  }

  return merged;
}

function shouldRefreshNow(force = false) {
  if (FX_SOURCE !== "live") return false;
  if (!FX_API_URL) return false;
  if (force) return true;
  if (refreshInFlight) return false;

  const now = Date.now();
  if (lastSuccessAt && now - lastSuccessAt < REFRESH_MS) {
    return false;
  }
  if (lastFailureAt && now - lastFailureAt < FAILURE_COOLDOWN_MS) {
    return false;
  }

  return true;
}

async function fetchLiveRateMap() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(FX_API_URL, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "User-Agent": "cs2-portfolio-analyzer/1.0"
      }
    });

    if (!response.ok) {
      throw new AppError(`FX provider failed with status ${response.status}`, 502);
    }

    const payload = await response.json();
    if (payload?.result && String(payload.result).toLowerCase() !== "success") {
      throw new AppError("FX provider returned unsuccessful response", 502);
    }

    return sanitizeLiveRates(payload);
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new AppError("FX rates request timed out", 504);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function ensureFreshFxRates(options = {}) {
  const force = Boolean(options.force);
  if (!shouldRefreshNow(force)) {
    return {
      ok: Boolean(liveRateMap),
      source: liveRateMap ? "live-cache" : "fallback-static",
      refreshedAt: lastSuccessAt || null,
      error: lastErrorMessage || null
    };
  }

  if (refreshInFlight && !force) {
    return refreshInFlight;
  }

  refreshInFlight = (async () => {
    try {
      const liveMap = await fetchLiveRateMap();
      liveRateMap = liveMap;
      lastSuccessAt = Date.now();
      lastErrorMessage = "";
      return {
        ok: true,
        source: "live",
        refreshedAt: lastSuccessAt,
        error: null
      };
    } catch (err) {
      lastFailureAt = Date.now();
      lastErrorMessage = String(err?.message || "Unknown FX refresh error");
      return {
        ok: false,
        source: "fallback-static",
        refreshedAt: lastSuccessAt || null,
        error: lastErrorMessage
      };
    } finally {
      refreshInFlight = null;
    }
  })();

  return refreshInFlight;
}

module.exports = {
  resolveCurrency,
  convertUsdAmount,
  ensureFreshFxRates,
  getSupportedCurrencies,
  __testables: {
    parseEnvRates,
    normalizeCode,
    sanitizeLiveRates,
    getActiveRateMap,
    BASE_RATE_MAP
  }
};

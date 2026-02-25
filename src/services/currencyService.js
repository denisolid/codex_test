const { defaultDisplayCurrency, fxRatesUsdJson } = require("../config/env");
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

const RATE_MAP = Object.freeze({
  ...DEFAULT_RATES,
  ...parseEnvRates(fxRatesUsdJson),
  USD: 1
});

const SUPPORTED_CODES = Object.freeze(Object.keys(RATE_MAP).sort());

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

function resolveCurrency(requestedCode) {
  const configuredFallback = normalizeCode(defaultDisplayCurrency) || "USD";
  const fallback = RATE_MAP[configuredFallback] ? configuredFallback : "USD";
  const normalized = normalizeCode(requestedCode) || fallback;

  if (!RATE_MAP[normalized]) {
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
  return round2(n * RATE_MAP[code]);
}

function getSupportedCurrencies() {
  return SUPPORTED_CODES.slice();
}

module.exports = {
  resolveCurrency,
  convertUsdAmount,
  getSupportedCurrencies,
  __testables: {
    parseEnvRates,
    normalizeCode,
    RATE_MAP
  }
};

const {
  marketFeeSteamPercent,
  marketFeeSkinportPercent,
  marketFeeCsfloatPercent,
  marketFeeDmarketPercent
} = require("../config/env");

const FEE_BY_SOURCE = Object.freeze({
  steam: Number(marketFeeSteamPercent || 13),
  skinport: Number(marketFeeSkinportPercent || 12),
  csfloat: Number(marketFeeCsfloatPercent || 2),
  dmarket: Number(marketFeeDmarketPercent || 7)
});

function round2(value) {
  return Number((Number(value || 0)).toFixed(2));
}

function clampPercent(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, 0), 99.99);
}

function normalizeCurrencyCode(value, fallback = "USD") {
  const code = String(value || "")
    .trim()
    .toUpperCase();
  return code || fallback;
}

function isPositiveNumber(value) {
  return Number.isFinite(Number(value)) && Number(value) >= 0;
}

function normalizePriceNumber(value) {
  if (value == null) return null;

  if (typeof value === "number") {
    if (!Number.isFinite(value) || value < 0) return null;
    return round2(value);
  }

  const text = String(value).trim();
  if (!text) return null;
  const token = text.match(/-?[\d.,]+/);
  if (!token) return null;

  let normalized = token[0];
  const hasComma = normalized.includes(",");
  const hasDot = normalized.includes(".");

  if (hasComma && hasDot) {
    if (normalized.lastIndexOf(",") > normalized.lastIndexOf(".")) {
      normalized = normalized.replace(/\./g, "").replace(",", ".");
    } else {
      normalized = normalized.replace(/,/g, "");
    }
  } else if (hasComma) {
    const pieces = normalized.split(",");
    normalized =
      pieces.length === 2 && pieces[1].length <= 2
        ? normalized.replace(",", ".")
        : normalized.replace(/,/g, "");
  }

  const parsed = Number(normalized);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }
  return round2(parsed);
}

function normalizePriceFromMinorUnits(value) {
  if (value == null) return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  return round2(n / 100);
}

function computeNetPrice(grossPrice, feePercent) {
  if (!isPositiveNumber(grossPrice)) return null;
  const fee = clampPercent(feePercent, 0);
  return round2(Number(grossPrice) * (1 - fee / 100));
}

function buildMarketPriceRecord({
  source,
  marketHashName,
  grossPrice,
  currency = "USD",
  url = null,
  updatedAt = null,
  raw = null,
  confidence = "medium",
  feePercent = null
}) {
  const safeSource = String(source || "").trim().toLowerCase();
  const safeGross = normalizePriceNumber(grossPrice);
  if (!safeSource || safeGross == null) {
    return null;
  }

  const effectiveFee =
    feePercent == null ? clampPercent(FEE_BY_SOURCE[safeSource], 0) : clampPercent(feePercent, 0);
  const net = computeNetPrice(safeGross, effectiveFee);
  const safeUpdatedAt = updatedAt ? new Date(updatedAt).toISOString() : new Date().toISOString();

  return {
    source: safeSource,
    marketHashName: String(marketHashName || "").trim(),
    grossPrice: safeGross,
    netPriceAfterFees: net,
    feePercent: round2(effectiveFee),
    currency: normalizeCurrencyCode(currency, "USD"),
    url: url ? String(url) : null,
    updatedAt: safeUpdatedAt,
    confidence: String(confidence || "medium"),
    raw: raw || null
  };
}

function sourceFeePercent(source) {
  return clampPercent(FEE_BY_SOURCE[String(source || "").trim().toLowerCase()], 0);
}

module.exports = {
  FEE_BY_SOURCE,
  round2,
  clampPercent,
  normalizeCurrencyCode,
  normalizePriceNumber,
  normalizePriceFromMinorUnits,
  computeNetPrice,
  buildMarketPriceRecord,
  sourceFeePercent
};

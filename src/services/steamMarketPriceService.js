const AppError = require("../utils/AppError");

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

exports.getLatestPrice = async (marketHashName, options = {}) => {
  const timeoutMs = Number(options.timeoutMs || 10000);
  const currency = Number(options.currency || 1);

  const params = new URLSearchParams();
  params.set("appid", "730");
  params.set("currency", String(currency));
  params.set("market_hash_name", marketHashName);

  const url = `https://steamcommunity.com/market/priceoverview/?${params.toString()}`;

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        "User-Agent": "cs2-portfolio-analyzer/1.0",
        Accept: "application/json"
      }
    });

    if (res.status === 429) {
      throw new AppError("Steam market rate limited", 429);
    }
    if (!res.ok) {
      throw new AppError(`Steam market price failed with status ${res.status}`, 502);
    }

    const payload = await res.json();
    if (!payload || payload.success !== true) {
      throw new AppError("Steam market returned unsuccessful response", 502);
    }

    const price =
      parsePriceString(payload.lowest_price) ||
      parsePriceString(payload.median_price);

    if (price == null) {
      throw new AppError("Steam market returned no parsable price", 502);
    }

    return price;
  } catch (err) {
    if (err.name === "AbortError") {
      throw new AppError("Steam market price request timed out", 504);
    }
    throw err;
  } finally {
    clearTimeout(t);
  }
};

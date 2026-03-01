const AppError = require("../utils/AppError");
const marketPriceRepo = require("../repositories/marketPriceRepository");
const userPricePreferenceRepo = require("../repositories/userPricePreferenceRepository");
const steamMarket = require("../markets/steam.market");
const skinportMarket = require("../markets/skinport.market");
const csfloatMarket = require("../markets/csfloat.market");
const dmarketMarket = require("../markets/dmarket.market");
const {
  round2,
  roundPrice,
  sourceFeePercent,
  buildMarketPriceRecord
} = require("../markets/marketUtils");
const {
  resolveCurrency,
  convertAmount,
  ensureFreshFxRates
} = require("./currencyService");
const {
  marketCompareCacheTtlMinutes,
  marketCompareConcurrency,
  marketCompareTimeoutMs,
  marketCompareMaxRetries,
  csfloatApiKey
} = require("../config/env");

const SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"]);
const ADAPTERS = Object.freeze({
  steam: steamMarket,
  skinport: skinportMarket,
  csfloat: csfloatMarket,
  dmarket: dmarketMarket
});

const PRICING_MODES = Object.freeze(["steam", "best_sell_net", "lowest_buy"]);
const DEFAULT_PRICING_MODE = "lowest_buy";
const CACHE_TTL_MINUTES = Math.max(Number(marketCompareCacheTtlMinutes || 60), 1);

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizePricingMode(value) {
  const mode = String(value || "")
    .trim()
    .toLowerCase();
  return PRICING_MODES.includes(mode) ? mode : DEFAULT_PRICING_MODE;
}

function isFreshTimestamp(isoValue, ttlMinutes) {
  if (!isoValue) return false;
  const ts = new Date(isoValue).getTime();
  if (Number.isNaN(ts)) return false;
  return Date.now() - ts <= Math.max(Number(ttlMinutes || CACHE_TTL_MINUTES), 1) * 60 * 1000;
}

function toUnavailable(source, currency, unavailableReason = null) {
  return {
    source,
    grossPrice: null,
    netPriceAfterFees: null,
    feePercent: sourceFeePercent(source),
    currency,
    url: null,
    updatedAt: null,
    confidence: "low",
    available: false,
    unavailableReason: unavailableReason
      ? String(unavailableReason || "").trim()
      : null
  };
}

function getUnavailableReasonForSource(source) {
  if (source === "csfloat" && !String(csfloatApiKey || "").trim()) {
    return "Missing CSFloat API key";
  }
  return null;
}

function normalizeItems(items = []) {
  const normalized = [];
  const seen = new Set();

  for (const item of Array.isArray(items) ? items : []) {
    const marketHashName = normalizeText(item?.marketHashName || item?.market_hash_name);
    if (!marketHashName) continue;
    if (seen.has(marketHashName)) continue;
    seen.add(marketHashName);

    const quantity = Math.max(Number(item?.quantity || 0), 0);
    normalized.push({
      skinId: Number(item?.skinId || item?.skin_id || 0) || null,
      marketHashName,
      quantity,
      steamPrice: Number(item?.steamPrice || item?.currentPrice || 0),
      steamCurrency: normalizeText(item?.steamCurrency || item?.currency || "USD") || "USD",
      steamRecordedAt: item?.steamRecordedAt || item?.currentPriceRecordedAt || null
    });
  }

  return normalized;
}

function normalizeLiveRecord(record, displayCurrency) {
  if (!record || record.grossPrice == null) return null;

  const fromCurrency = String(record.currency || "USD")
    .trim()
    .toUpperCase();
  const gross =
    fromCurrency === displayCurrency
      ? Number(record.grossPrice)
      : convertAmount(record.grossPrice, fromCurrency, displayCurrency);
  if (!Number.isFinite(gross)) return null;

  return buildMarketPriceRecord({
    source: record.source,
    marketHashName: record.marketHashName,
    grossPrice: gross,
    currency: displayCurrency,
    url: record.url || null,
    updatedAt: record.updatedAt || new Date().toISOString(),
    confidence: record.confidence || "medium",
    raw: record.raw || null,
    feePercent: record.feePercent
  });
}

function fromCachedRow(row, displayCurrency) {
  if (!row) return null;
  const source = String(row.market || row.source || "").trim().toLowerCase();
  const marketHashName = normalizeText(row.market_hash_name || row.marketHashName);
  const rowCurrency = String(row.currency || "USD")
    .trim()
    .toUpperCase();
  const rowGross = Number(row.gross_price ?? row.grossPrice);
  if (!source || !marketHashName || !Number.isFinite(rowGross)) {
    return null;
  }

  const gross =
    rowCurrency === displayCurrency
      ? rowGross
      : convertAmount(rowGross, rowCurrency, displayCurrency);
  if (!Number.isFinite(gross)) {
    return null;
  }

  return buildMarketPriceRecord({
    source,
    marketHashName,
    grossPrice: gross,
    currency: displayCurrency,
    url: row.url || null,
    updatedAt: row.fetched_at || row.updatedAt || null,
    confidence: row?.raw?.confidence || "medium",
    raw: row.raw || null
  });
}

function toUpsertRow(record) {
  if (!record || !record.source || !record.marketHashName) return null;
  return {
    market: record.source,
    market_hash_name: record.marketHashName,
    currency: record.currency || "USD",
    gross_price: Number(record.grossPrice || 0),
    net_price: Number(record.netPriceAfterFees || 0),
    url: record.url || null,
    fetched_at: record.updatedAt || new Date().toISOString(),
    raw: {
      ...(record.raw || {}),
      confidence: record.confidence || "medium"
    }
  };
}

function pickBestBuy(perMarket = []) {
  return perMarket
    .filter((row) => row.available && Number.isFinite(Number(row.grossPrice)))
    .sort((a, b) => Number(a.grossPrice) - Number(b.grossPrice))[0] || null;
}

function pickBestSellNet(perMarket = []) {
  return perMarket
    .filter((row) => row.available && Number.isFinite(Number(row.netPriceAfterFees)))
    .sort((a, b) => Number(b.netPriceAfterFees) - Number(a.netPriceAfterFees))[0] || null;
}

function selectByPricingMode(pricingMode, stream) {
  const steam = stream?.steam || null;
  const bestBuy = stream?.bestBuy || null;
  const bestSellNet = stream?.bestSellNet || null;

  if (pricingMode === "steam") {
    return steam || bestBuy || bestSellNet || null;
  }
  if (pricingMode === "best_sell_net") {
    return bestSellNet || steam || bestBuy || null;
  }
  return bestBuy || steam || bestSellNet || null;
}

function getModeUnitPrice(pricingMode, selected) {
  if (!selected) return 0;
  if (pricingMode === "best_sell_net") {
    return Number(selected.netPriceAfterFees || 0);
  }
  return Number(selected.grossPrice || 0);
}

function buildFeeSummary() {
  return {
    steam: sourceFeePercent("steam"),
    skinport: sourceFeePercent("skinport"),
    csfloat: sourceFeePercent("csfloat"),
    dmarket: sourceFeePercent("dmarket")
  };
}

async function fetchLiveMarketData(itemsBySource = {}, displayCurrency, options = {}) {
  const result = {};
  const rowsToStore = [];
  const fetchCurrency = String(options.fetchCurrency || "USD")
    .trim()
    .toUpperCase();

  for (const source of SOURCE_ORDER) {
    const adapter = ADAPTERS[source];
    if (!adapter?.batchGetPrices) continue;
    const sourceItems = Array.isArray(itemsBySource[source]) ? itemsBySource[source] : [];
    if (!sourceItems.length) {
      result[source] = {};
      continue;
    }

    const byName = await adapter.batchGetPrices(sourceItems, {
      currency: fetchCurrency,
      concurrency: Math.max(Number(options.concurrency || marketCompareConcurrency || 4), 1),
      timeoutMs: Number(options.timeoutMs || marketCompareTimeoutMs || 9000),
      maxRetries: Number(options.maxRetries || marketCompareMaxRetries || 3)
    });

    result[source] = {};
    for (const [marketHashName, rawRecord] of Object.entries(byName || {})) {
      const normalized = normalizeLiveRecord(rawRecord, displayCurrency);
      if (!normalized) continue;
      result[source][marketHashName] = normalized;
      const row = toUpsertRow(rawRecord);
      if (row) {
        rowsToStore.push(row);
      }
    }
  }

  if (rowsToStore.length) {
    await marketPriceRepo.upsertRows(rowsToStore);
  }

  return result;
}

function buildSteamFallback(item, displayCurrency) {
  const steamPrice = Number(item.steamPrice || 0);
  if (!Number.isFinite(steamPrice) || steamPrice <= 0) {
    return null;
  }

  const steamCurrency = String(item.steamCurrency || "USD")
    .trim()
    .toUpperCase();
  const gross =
    steamCurrency === displayCurrency
      ? steamPrice
      : convertAmount(steamPrice, steamCurrency, displayCurrency);
  if (!Number.isFinite(gross) || gross <= 0) {
    return null;
  }

  return buildMarketPriceRecord({
    source: "steam",
    marketHashName: item.marketHashName,
    grossPrice: gross,
    currency: displayCurrency,
    url: `https://steamcommunity.com/market/listings/730/${encodeURIComponent(
      item.marketHashName
    )}`,
    updatedAt: item.steamRecordedAt || new Date().toISOString(),
    confidence: "medium",
    raw: {
      source: "portfolio-price-history-fallback"
    }
  });
}

async function resolvePricingMode(userId, requestedMode, displayCurrency) {
  const safeUserId = normalizeText(userId);
  const requested = normalizePricingMode(requestedMode);
  if (!safeUserId) {
    return {
      pricingMode: requested,
      preferredCurrency: displayCurrency
    };
  }

  const stored = await userPricePreferenceRepo.getByUserId(safeUserId);
  if (!stored) {
    return {
      pricingMode: requested,
      preferredCurrency: displayCurrency
    };
  }

  return {
    pricingMode: requestedMode ? requested : normalizePricingMode(stored.pricing_mode),
    preferredCurrency: resolveCurrency(stored.preferred_currency || displayCurrency)
  };
}

exports.getUserPricePreference = async (userId, options = {}) => {
  const fallbackCurrency = resolveCurrency(options.currency || "USD");
  const safeUserId = normalizeText(userId);
  if (!safeUserId) {
    return {
      pricingMode: DEFAULT_PRICING_MODE,
      preferredCurrency: fallbackCurrency
    };
  }

  const row = await userPricePreferenceRepo.getByUserId(safeUserId);
  if (!row) {
    return {
      pricingMode: DEFAULT_PRICING_MODE,
      preferredCurrency: fallbackCurrency
    };
  }

  return {
    pricingMode: normalizePricingMode(row.pricing_mode),
    preferredCurrency: resolveCurrency(row.preferred_currency || fallbackCurrency)
  };
};

exports.updateUserPricePreference = async (userId, updates = {}) => {
  const safeUserId = normalizeText(userId);
  if (!safeUserId) {
    throw new AppError("Unauthorized", 401, "UNAUTHORIZED");
  }

  const patch = {};
  if (updates.pricingMode != null) {
    const mode = String(updates.pricingMode || "")
      .trim()
      .toLowerCase();
    if (!PRICING_MODES.includes(mode)) {
      throw new AppError(
        `pricingMode must be one of: ${PRICING_MODES.join(", ")}`,
        400,
        "VALIDATION_ERROR"
      );
    }
    patch.pricingMode = mode;
  }

  if (updates.preferredCurrency != null) {
    patch.preferredCurrency = resolveCurrency(updates.preferredCurrency);
  }

  if (!Object.keys(patch).length) {
    return exports.getUserPricePreference(safeUserId);
  }

  const row = await userPricePreferenceRepo.upsertByUserId(safeUserId, patch);
  return {
    pricingMode: normalizePricingMode(row.pricing_mode),
    preferredCurrency: resolveCurrency(row.preferred_currency)
  };
};

exports.compareItems = async (items = [], options = {}) => {
  await ensureFreshFxRates();
  const normalizedItems = normalizeItems(items);
  const displayCurrency = resolveCurrency(options.currency || "USD");
  const modeResolution = await resolvePricingMode(
    options.userId,
    options.pricingMode,
    displayCurrency
  );
  const pricingMode = modeResolution.pricingMode;

  if (!normalizedItems.length) {
    return {
      currency: displayCurrency,
      pricingMode,
      fees: buildFeeSummary(),
      generatedAt: new Date().toISOString(),
      ttlMinutes: CACHE_TTL_MINUTES,
      items: [],
      summary: {
        totalValueSelected: 0,
        totalValueSteam: 0,
        totalValueBestSellNet: 0,
        totalValueLowestBuy: 0,
        pricedItemsCount: 0,
        unavailableItemsCount: 0
      }
    };
  }

  const marketHashNames = normalizedItems.map((item) => item.marketHashName);
  const cachedBySource = await marketPriceRepo.getLatestByMarketHashNames(marketHashNames, {
    sources: SOURCE_ORDER
  });

  const allowLiveFetch = options.allowLiveFetch !== false;
  const forceRefresh = Boolean(options.forceRefresh);
  const staleItemsBySource = {};

  for (const source of SOURCE_ORDER) {
    const sourceRows = cachedBySource[source] || {};
    staleItemsBySource[source] = normalizedItems.filter((item) => {
      const cached = sourceRows[item.marketHashName];
      if (!cached) return true;
      if (forceRefresh) return true;
      return !isFreshTimestamp(cached.fetched_at, options.ttlMinutes || CACHE_TTL_MINUTES);
    });
  }

  let liveBySource = {};
  if (allowLiveFetch) {
    const hasPending = SOURCE_ORDER.some(
      (source) => Array.isArray(staleItemsBySource[source]) && staleItemsBySource[source].length
    );
    if (hasPending) {
      liveBySource = await fetchLiveMarketData(staleItemsBySource, displayCurrency, {
        fetchCurrency: "USD",
        concurrency: options.concurrency,
        timeoutMs: options.timeoutMs,
        maxRetries: options.maxRetries
      });
    }
  }

  const summary = {
    totalValueSelected: 0,
    totalValueSteam: 0,
    totalValueBestSellNet: 0,
    totalValueLowestBuy: 0,
    pricedItemsCount: 0,
    unavailableItemsCount: 0
  };

  const enrichedItems = normalizedItems.map((item) => {
    const perMarket = SOURCE_ORDER.map((source) => {
      const live = liveBySource?.[source]?.[item.marketHashName] || null;
      if (live) {
        return {
          ...live,
          available: true
        };
      }

      const cached = fromCachedRow(
        cachedBySource?.[source]?.[item.marketHashName] || null,
        displayCurrency
      );
      if (cached) {
        return {
          ...cached,
          available: true
        };
      }

      if (source === "steam") {
        const fallback = buildSteamFallback(item, displayCurrency);
        if (fallback) {
          return {
            ...fallback,
            available: true
          };
        }
      }

      return toUnavailable(
        source,
        displayCurrency,
        getUnavailableReasonForSource(source)
      );
    });

    const bestBuy = pickBestBuy(perMarket);
    const bestSellNet = pickBestSellNet(perMarket);
    const steam = perMarket.find((row) => row.source === "steam" && row.available) || null;
    const selected = selectByPricingMode(pricingMode, {
      steam,
      bestBuy,
      bestSellNet
    });
    const quantity = Number(item.quantity || 0);
    const selectedUnitPrice = roundPrice(getModeUnitPrice(pricingMode, selected));
    const selectedLineValue = roundPrice(selectedUnitPrice * quantity);

    const steamUnit = Number(steam?.grossPrice || 0);
    const bestBuyUnit = Number(bestBuy?.grossPrice || 0);
    const bestSellNetUnit = Number(bestSellNet?.netPriceAfterFees || 0);

    summary.totalValueSelected += selectedLineValue;
    summary.totalValueSteam += steamUnit * quantity;
    summary.totalValueBestSellNet += bestSellNetUnit * quantity;
    summary.totalValueLowestBuy += bestBuyUnit * quantity;

    if (selectedUnitPrice > 0) {
      summary.pricedItemsCount += 1;
    } else {
      summary.unavailableItemsCount += 1;
    }

    return {
      skinId: item.skinId,
      marketHashName: item.marketHashName,
      quantity: Number(item.quantity || 0),
      perMarket,
      bestBuy: bestBuy
        ? {
            source: bestBuy.source,
            grossPrice: bestBuy.grossPrice,
            currency: bestBuy.currency,
            url: bestBuy.url
          }
        : null,
      bestSellNet: bestSellNet
        ? {
            source: bestSellNet.source,
            netPriceAfterFees: bestSellNet.netPriceAfterFees,
            currency: bestSellNet.currency,
            url: bestSellNet.url
          }
        : null,
      selectedPricingSource: selected?.source || null,
      selectedUnitPrice,
      selectedLineValue,
      totalsByMode: {
        steam: roundPrice(steamUnit * quantity),
        best_sell_net: roundPrice(bestSellNetUnit * quantity),
        lowest_buy: roundPrice(bestBuyUnit * quantity)
      }
    };
  });

  return {
    currency: displayCurrency,
    pricingMode,
    fees: buildFeeSummary(),
    generatedAt: new Date().toISOString(),
    ttlMinutes: options.ttlMinutes || CACHE_TTL_MINUTES,
    items: enrichedItems,
    summary: {
      ...summary,
      totalValueSelected: round2(summary.totalValueSelected),
      totalValueSteam: round2(summary.totalValueSteam),
      totalValueBestSellNet: round2(summary.totalValueBestSellNet),
      totalValueLowestBuy: round2(summary.totalValueLowestBuy)
    }
  };
};

exports.PRICING_MODES = PRICING_MODES;
exports.DEFAULT_PRICING_MODE = DEFAULT_PRICING_MODE;
exports.__testables = {
  normalizePricingMode,
  pickBestBuy,
  pickBestSellNet,
  selectByPricingMode,
  getModeUnitPrice,
  isFreshTimestamp
};

const AppError = require("../utils/AppError");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const skinRepo = require("../repositories/skinRepository");
const snapshotRepo = require("../repositories/marketSnapshotRepository");
const steamMarketPriceService = require("./steamMarketPriceService");
const { resolveCurrency, convertUsdAmount } = require("./currencyService");
const {
  marketCommissionPercent,
  marketSnapshotTtlMinutes
} = require("../config/env");

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return n;
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, n) => sum + n, 0) / values.length;
}

function stdDev(values) {
  if (values.length <= 1) return 0;
  const mean = average(values);
  const variance =
    values.reduce((sum, n) => sum + (n - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function isSnapshotFresh(snapshot, ttlMinutes) {
  if (!snapshot || !snapshot.captured_at) return false;
  const capturedAt = new Date(snapshot.captured_at).getTime();
  if (Number.isNaN(capturedAt)) return false;
  return Date.now() - capturedAt <= Math.max(ttlMinutes, 1) * 60 * 1000;
}

function deriveStatsFromHistory(historyRows, fallbackPrice) {
  const now = Date.now();
  const oneDayMs = 24 * 60 * 60 * 1000;
  const sevenDayMs = 7 * oneDayMs;

  const rows = (historyRows || [])
    .map((row) => ({
      price: toNumber(row.price, 0),
      ts: new Date(row.recorded_at).getTime()
    }))
    .filter((row) => row.price >= 0 && Number.isFinite(row.ts));

  const prices7d = rows
    .filter((row) => row.ts >= now - sevenDayMs)
    .map((row) => row.price);
  const pricesPool = prices7d.length ? prices7d : rows.map((row) => row.price);

  const avg7d = average(pricesPool);
  const min7d = pricesPool.length ? Math.min(...pricesPool) : fallbackPrice;
  const max7d = pricesPool.length ? Math.max(...pricesPool) : fallbackPrice;
  const vol7d = avg7d > 0 ? (stdDev(pricesPool) / avg7d) * 100 : 0;
  const spread = avg7d > 0 ? ((max7d - min7d) / avg7d) * 100 : 0;
  const volume24h = rows.filter((row) => row.ts >= now - oneDayMs).length;

  return {
    average7dPrice: avg7d > 0 ? avg7d : fallbackPrice,
    volatility7dPercent: Math.max(vol7d, 0),
    spreadPercent: Math.max(spread, 0),
    volume24h
  };
}

function computeLiquidityScore(input) {
  const volume24h = toNumber(input.volume_24h, 0);
  const volatility7dPercent = toNumber(input.volatility_7d_percent, 0);
  const spreadPercent = toNumber(input.spread_percent, 0);

  const volumeScore = clamp((Math.log10(volume24h + 1) / 3) * 100, 0, 100);
  const volatilityScore = 100 - clamp((volatility7dPercent / 25) * 100, 0, 100);
  const spreadScore = 100 - clamp((spreadPercent / 15) * 100, 0, 100);

  const score = clamp(
    volumeScore * 0.55 + volatilityScore * 0.25 + spreadScore * 0.2,
    0,
    100
  );

  let band = "low";
  if (score >= 75) {
    band = "high";
  } else if (score >= 45) {
    band = "medium";
  }

  return {
    score: round2(score),
    band,
    factors: {
      volume24h: round2(volume24h),
      volatility7dPercent: round2(volatility7dPercent),
      spreadPercent: round2(spreadPercent)
    }
  };
}

function buildQuickSellTiers(snapshot, liquidityScore, commissionPercent) {
  const lowest = Math.max(toNumber(snapshot.lowest_listing_price, 0), 0.01);
  const avg7d = Math.max(toNumber(snapshot.average_7d_price, lowest), 0.01);
  const spreadPercent = clamp(toNumber(snapshot.spread_percent, 0), 0, 100);

  const liqFactor = clamp(liquidityScore / 100, 0, 1);
  const undercutFast = clamp(0.01 + (1 - liqFactor) * 0.03 + spreadPercent / 250, 0.01, 0.09);
  const fastSell = Math.max(lowest * (1 - undercutFast), 0.01);

  const blendWeight = clamp(0.65 - liqFactor * 0.2, 0.35, 0.75);
  const balancedBase = lowest * blendWeight + avg7d * (1 - blendWeight);
  const balanced = Math.max(balancedBase * (1 - spreadPercent / 400), fastSell);

  const premium = clamp(0.015 + liqFactor * 0.05 + spreadPercent / 450, 0.02, 0.15);
  const maxProfit = Math.max(Math.max(avg7d, lowest) * (1 + premium), balanced);

  const commissionRate = clamp(toNumber(commissionPercent, 13), 0, 99.99);

  const toTier = (key, listPrice, expectedFill) => ({
    tier: key,
    listPrice: round2(listPrice),
    estimatedNet: round2(listPrice * (1 - commissionRate / 100)),
    expectedFill,
    commissionPercent: round2(commissionRate)
  });

  return [
    toTier("fast_sell", fastSell, "minutes_to_hours"),
    toTier("balanced", balanced, "hours_to_one_day"),
    toTier("max_profit", maxProfit, "one_plus_days")
  ];
}

async function buildAndStoreSnapshot(skin) {
  const latestPrice = await priceRepo.getLatestPriceBySkinId(skin.id);
  const currentPrice = toNumber(latestPrice?.price, 0);
  const history = await priceRepo.getHistoryBySkinId(skin.id, 200);
  const derived = deriveStatsFromHistory(history, currentPrice);

  let lowestListingPrice = currentPrice;
  let volume24h = derived.volume24h;
  let source = "derived-price-history";

  try {
    const overview = await steamMarketPriceService.getPriceOverview(skin.market_hash_name);
    if (overview.lowestPrice != null) {
      lowestListingPrice = overview.lowestPrice;
    } else if (overview.medianPrice != null && lowestListingPrice <= 0) {
      lowestListingPrice = overview.medianPrice;
    }
    if (overview.volume != null) {
      volume24h = overview.volume;
    }
    source = "steam-market-overview+price-history";
  } catch (_err) {
    // Keep derived fallback if live market overview fails.
  }

  const average7dPrice =
    derived.average7dPrice > 0 ? derived.average7dPrice : lowestListingPrice;
  const spreadPercent =
    average7dPrice > 0
      ? Math.abs(average7dPrice - lowestListingPrice) / average7dPrice * 100
      : derived.spreadPercent;

  return snapshotRepo.insertSnapshot({
    skin_id: skin.id,
    lowest_listing_price: round2(Math.max(lowestListingPrice, 0)),
    average_7d_price: round2(Math.max(average7dPrice, 0)),
    volume_24h: Math.max(Math.round(volume24h), 0),
    spread_percent: round2(Math.max(spreadPercent, 0)),
    volatility_7d_percent: round2(Math.max(derived.volatility7dPercent, 0)),
    currency: "USD",
    source
  });
}

async function getOrRefreshSnapshot(skinId) {
  const existing = await snapshotRepo.getLatestBySkinId(skinId);
  if (isSnapshotFresh(existing, marketSnapshotTtlMinutes)) {
    return existing;
  }

  const skin = await skinRepo.getById(skinId);
  if (!skin) {
    throw new AppError("Item not found", 404);
  }

  return buildAndStoreSnapshot(skin);
}

exports.getInventoryValuation = async (userId, options = {}) => {
  const displayCurrency = resolveCurrency(options.currency);
  const holdings = await inventoryRepo.getUserHoldings(userId);
  const commissionPercent = clamp(
    toNumber(options.commissionPercent, marketCommissionPercent),
    0,
    99.99
  );

  if (!holdings.length) {
    return {
      totalValueGross: 0,
      totalValueNet: 0,
      commissionPercent: round2(commissionPercent),
      currency: displayCurrency,
      itemsCount: 0,
      items: []
    };
  }

  const skinIds = holdings.map((h) => h.skin_id);
  const latestRows = await priceRepo.getLatestPriceRowsBySkinIds(skinIds);

  let totalValueGross = 0;
  let totalValueNet = 0;

  const items = holdings.map((holding) => {
    const latest = latestRows[holding.skin_id];
    const unitPrice = toNumber(latest?.price, 0);
    const lineValueGross = holding.quantity * unitPrice;
    const lineValueNet = lineValueGross * (1 - commissionPercent / 100);

    totalValueGross += lineValueGross;
    totalValueNet += lineValueNet;

    return {
      skinId: holding.skin_id,
      marketHashName: holding.skins.market_hash_name,
      quantity: holding.quantity,
      pricePerItem: convertUsdAmount(round2(unitPrice), displayCurrency),
      priceSource: latest?.source || null,
      recordedAt: latest?.recorded_at || null,
      commissionPercent: round2(commissionPercent),
      estimatedNetPerItem: convertUsdAmount(
        round2(unitPrice * (1 - commissionPercent / 100)),
        displayCurrency
      ),
      lineValueGross: convertUsdAmount(round2(lineValueGross), displayCurrency),
      lineValueNet: convertUsdAmount(round2(lineValueNet), displayCurrency),
      currency: displayCurrency
    };
  });

  return {
    totalValueGross: convertUsdAmount(round2(totalValueGross), displayCurrency),
    totalValueNet: convertUsdAmount(round2(totalValueNet), displayCurrency),
    commissionPercent: round2(commissionPercent),
    currency: displayCurrency,
    itemsCount: items.length,
    items
  };
};

exports.getQuickSellSuggestion = async (skinId, options = {}) => {
  const displayCurrency = resolveCurrency(options.currency);
  const normalizedSkinId = Number(skinId);
  if (!Number.isInteger(normalizedSkinId) || normalizedSkinId <= 0) {
    throw new AppError("skinId must be a positive integer", 400);
  }

  const commissionPercent = clamp(
    toNumber(options.commissionPercent, marketCommissionPercent),
    0,
    99.99
  );

  const snapshot = await getOrRefreshSnapshot(normalizedSkinId);
  const liquidity = computeLiquidityScore(snapshot);
  const tiers = buildQuickSellTiers(snapshot, liquidity.score, commissionPercent).map(
    (tier) => ({
      ...tier,
      listPrice: convertUsdAmount(tier.listPrice, displayCurrency),
      estimatedNet: convertUsdAmount(tier.estimatedNet, displayCurrency),
      currency: displayCurrency
    })
  );

  return {
    skinId: normalizedSkinId,
    lowestListingPrice: convertUsdAmount(
      round2(snapshot.lowest_listing_price),
      displayCurrency
    ),
    average7dPrice: convertUsdAmount(round2(snapshot.average_7d_price), displayCurrency),
    volume24h: Number(snapshot.volume_24h || 0),
    spreadPercent: round2(snapshot.spread_percent),
    volatility7dPercent: round2(snapshot.volatility_7d_percent),
    snapshotSource: snapshot.source,
    snapshotCapturedAt: snapshot.captured_at,
    currency: displayCurrency,
    liquidity,
    tiers
  };
};

exports.getLiquidityScore = async (skinId) => {
  const normalizedSkinId = Number(skinId);
  if (!Number.isInteger(normalizedSkinId) || normalizedSkinId <= 0) {
    throw new AppError("skinId must be a positive integer", 400);
  }

  const snapshot = await getOrRefreshSnapshot(normalizedSkinId);
  const liquidity = computeLiquidityScore(snapshot);

  return {
    skinId: normalizedSkinId,
    score: liquidity.score,
    band: liquidity.band,
    factors: liquidity.factors,
    snapshotCapturedAt: snapshot.captured_at,
    snapshotSource: snapshot.source
  };
};

exports.__testables = {
  computeLiquidityScore,
  buildQuickSellTiers,
  deriveStatsFromHistory,
  clamp,
  round2
};

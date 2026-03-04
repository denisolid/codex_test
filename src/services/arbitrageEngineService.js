const { round2, roundPrice } = require("../markets/marketUtils");

const MIN_SPREAD_PERCENT = 3;
const MARKET_RELIABILITY = Object.freeze({
  steam: 100,
  skinport: 90,
  csfloat: 80,
  dmarket: 75
});

function normalizeMarket(source) {
  return String(source || "")
    .trim()
    .toLowerCase();
}

function toFiniteOrNull(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveLiquiditySample(item = {}) {
  const directCandidates = [
    item?.liquiditySales,
    item?.salesCount,
    item?.sales,
    item?.volume24h,
    item?.marketVolume24h,
    item?.marketVolume7d,
    item?.liquidityCount,
    item?.marketInsight?.sellSuggestion?.volume24h
  ];

  for (const candidate of directCandidates) {
    const n = toFiniteOrNull(candidate);
    if (n != null && n >= 0) return n;
  }

  const liquidityScore = toFiniteOrNull(
    item?.liquidityScore ??
      item?.managementClue?.metrics?.liquidityScore ??
      item?.marketComparison?.liquidityScore
  );
  if (liquidityScore != null && liquidityScore >= 0) {
    // Fallback projection when only normalized score (0-100) is known.
    return round2(Math.min(liquidityScore, 100) * 2);
  }

  return null;
}

function resolveSevenDayChange(item = {}) {
  const candidates = [
    item?.sevenDayChangePercent,
    item?.seven_day_change_percent,
    item?.change7dPercent,
    item?.priceChange7dPercent,
    item?.marketInsight?.sellSuggestion?.change7dPercent
  ];
  for (const candidate of candidates) {
    const n = toFiniteOrNull(candidate);
    if (n != null) return n;
  }
  return null;
}

function getSpreadScore(spreadPercent) {
  const spread = toFiniteOrNull(spreadPercent);
  if (spread == null) return 40;
  if (spread > 15) return 100;
  if (spread >= 10) return 80;
  if (spread >= 5) return 60;
  return 40;
}

function getLiquidityScore(liquiditySample) {
  const liquidity = toFiniteOrNull(liquiditySample);
  if (liquidity == null || liquidity < 0) return 30;
  if (liquidity > 200) return 100;
  if (liquidity >= 100) return 80;
  if (liquidity >= 50) return 60;
  return 30;
}

function getStabilityScore(sevenDayChangePercent) {
  const change = toFiniteOrNull(sevenDayChangePercent);
  if (change == null) return 50;
  const absChange = Math.abs(change);
  if (absChange < 5) return 100;
  if (absChange <= 10) return 80;
  if (absChange <= 20) return 50;
  return 20;
}

function getMarketScore(buyMarket, sellMarket) {
  const buyScore = Number(MARKET_RELIABILITY[normalizeMarket(buyMarket)] || 70);
  const sellScore = Number(MARKET_RELIABILITY[normalizeMarket(sellMarket)] || 70);
  return round2((buyScore + sellScore) / 2);
}

function categorizeOpportunityScore(score) {
  const safeScore = toFiniteOrNull(score) ?? 0;
  if (safeScore >= 90) return "Strong opportunity";
  if (safeScore >= 70) return "Good opportunity";
  if (safeScore >= 50) return "Risky";
  return "Weak";
}

function evaluateItemOpportunity(item = {}, options = {}) {
  const minSpreadPercent =
    toFiniteOrNull(options.minSpreadPercent) != null
      ? Number(options.minSpreadPercent)
      : MIN_SPREAD_PERCENT;
  const itemId = Number(item?.skinId || item?.itemId || 0) || null;
  const itemName = String(item?.marketHashName || item?.itemName || "Tracked Item").trim();
  const perMarket = Array.isArray(item?.perMarket) ? item.perMarket : [];

  const availableRows = perMarket.filter((row) => {
    if (!row?.available) return false;
    const buy = Number(row.grossPrice);
    const sell = Number(row.netPriceAfterFees);
    return Number.isFinite(buy) && buy > 0 && Number.isFinite(sell) && sell > 0;
  });

  if (!availableRows.length) {
    const spreadScore = 40;
    const liquidityScore = getLiquidityScore(resolveLiquiditySample(item));
    const stabilityScore = getStabilityScore(resolveSevenDayChange(item));
    const marketScore = 70;
    const opportunityScore = round2(
      spreadScore * 0.35 +
        liquidityScore * 0.35 +
        stabilityScore * 0.2 +
        marketScore * 0.1
    );
    return {
      itemId,
      itemName,
      buyMarket: null,
      buyPrice: null,
      sellMarket: null,
      sellNet: null,
      profit: null,
      spreadPercent: null,
      opportunityScore,
      scoreCategory: categorizeOpportunityScore(opportunityScore),
      isOpportunity: false,
      liquiditySample: resolveLiquiditySample(item),
      sevenDayChangePercent: resolveSevenDayChange(item),
      buyUrl: null,
      sellUrl: null,
      scores: {
        spreadScore,
        liquidityScore,
        stabilityScore,
        marketScore
      }
    };
  }

  const lowestBuyRow = [...availableRows].sort(
    (a, b) => Number(a.grossPrice) - Number(b.grossPrice)
  )[0];
  const highestSellRow = [...availableRows].sort(
    (a, b) => Number(b.netPriceAfterFees) - Number(a.netPriceAfterFees)
  )[0];

  const buyPrice = roundPrice(Number(lowestBuyRow.grossPrice || 0));
  const sellNet = roundPrice(Number(highestSellRow.netPriceAfterFees || 0));
  const rawProfit = Number(sellNet || 0) - Number(buyPrice || 0);
  const profit = roundPrice(rawProfit);
  const spreadPercent =
    Number(buyPrice) > 0 ? round2((Number(rawProfit) / Number(buyPrice)) * 100) : null;

  const liquiditySample = resolveLiquiditySample(item);
  const sevenDayChangePercent = resolveSevenDayChange(item);
  const spreadScore = getSpreadScore(spreadPercent);
  const liquidityScore = getLiquidityScore(liquiditySample);
  const stabilityScore = getStabilityScore(sevenDayChangePercent);
  const marketScore = getMarketScore(lowestBuyRow.source, highestSellRow.source);
  const opportunityScore = round2(
    spreadScore * 0.35 +
      liquidityScore * 0.35 +
      stabilityScore * 0.2 +
      marketScore * 0.1
  );

  const isOpportunity = Number(profit) > 0 && Number(spreadPercent || 0) > Number(minSpreadPercent);

  return {
    itemId,
    itemName,
    buyMarket: normalizeMarket(lowestBuyRow.source),
    buyPrice,
    sellMarket: normalizeMarket(highestSellRow.source),
    sellNet,
    profit,
    spreadPercent,
    opportunityScore,
    scoreCategory: categorizeOpportunityScore(opportunityScore),
    isOpportunity,
    liquiditySample,
    sevenDayChangePercent,
    buyUrl: lowestBuyRow?.url || null,
    sellUrl: highestSellRow?.url || null,
    scores: {
      spreadScore,
      liquidityScore,
      stabilityScore,
      marketScore
    }
  };
}

function normalizeSortBy(sortBy) {
  const safe = String(sortBy || "profit")
    .trim()
    .toLowerCase();
  if (safe === "spread") return "spread";
  if (safe === "score") return "score";
  return "profit";
}

function normalizeMarketSet(marketsInput) {
  if (Array.isArray(marketsInput)) {
    return new Set(
      marketsInput
        .map((value) => normalizeMarket(value))
        .filter(Boolean)
    );
  }
  const raw = String(marketsInput || "").trim();
  if (!raw) return new Set();
  return new Set(
    raw
      .split(",")
      .map((value) => normalizeMarket(value))
      .filter(Boolean)
  );
}

function rankOpportunities(opportunities = [], options = {}) {
  const minProfit = toFiniteOrNull(options.minProfit) ?? 0;
  const minSpreadPercent =
    toFiniteOrNull(options.minSpreadPercent) != null
      ? Number(options.minSpreadPercent)
      : MIN_SPREAD_PERCENT;
  const minScore = toFiniteOrNull(options.minScore) ?? 0;
  const liquidityMin = toFiniteOrNull(options.liquidityMin) ?? 0;
  const limit = Number.isFinite(Number(options.limit)) ? Math.max(Number(options.limit), 0) : 0;
  const sortBy = normalizeSortBy(options.sortBy);
  const marketSet = normalizeMarketSet(options.markets || options.market);

  const filtered = (Array.isArray(opportunities) ? opportunities : [])
    .filter((row) => row && row.isOpportunity)
    .filter((row) => Number(row.profit || 0) >= minProfit)
    .filter((row) => Number(row.spreadPercent || 0) >= minSpreadPercent)
    .filter((row) => Number(row.opportunityScore || 0) >= minScore)
    .filter((row) => Number(row.liquiditySample || 0) >= liquidityMin)
    .filter((row) => {
      if (!marketSet.size) return true;
      return marketSet.has(normalizeMarket(row.buyMarket)) || marketSet.has(normalizeMarket(row.sellMarket));
    });

  filtered.sort((a, b) => {
    if (sortBy === "score") {
      return (
        Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
        Number(b.profit || 0) - Number(a.profit || 0)
      );
    }
    if (sortBy === "spread") {
      return (
        Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0) ||
        Number(b.profit || 0) - Number(a.profit || 0)
      );
    }
    return (
      Number(b.profit || 0) - Number(a.profit || 0) ||
      Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0) ||
      Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0)
    );
  });

  if (limit > 0) {
    return filtered.slice(0, limit);
  }
  return filtered;
}

module.exports = {
  MIN_SPREAD_PERCENT,
  MARKET_RELIABILITY,
  evaluateItemOpportunity,
  rankOpportunities,
  categorizeOpportunityScore,
  __testables: {
    resolveLiquiditySample,
    resolveSevenDayChange,
    getSpreadScore,
    getLiquidityScore,
    getStabilityScore,
    getMarketScore
  }
};

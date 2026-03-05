const { round2, roundPrice } = require("../markets/marketUtils");
const arbitrageRules = require("../config/arbitrageRules");
const {
  normalizeMarket,
  normalizeMarketQuotes,
  resolveVolume7d,
  resolveLiquidityScore,
  resolveSevenDayChangePercent
} = require("./arbitrageQuoteNormalizerService");

const MIN_SPREAD_PERCENT = Number(arbitrageRules.MIN_SPREAD_PERCENT_BASELINE || 5);
const SPREAD_SANITY_MAX_PERCENT = Number(arbitrageRules.SPREAD_SANITY_MAX_PERCENT || 300);
const DEFAULT_SCORE_CUTOFF = Number(arbitrageRules.DEFAULT_SCORE_CUTOFF || 70);
const RISKY_SCORE_CUTOFF = Number(arbitrageRules.RISKY_SCORE_CUTOFF || 50);
const DEFAULT_MIN_PROFIT_ABSOLUTE = Number(arbitrageRules.DEFAULT_MIN_PROFIT_ABSOLUTE || 0.5);
const DEFAULT_MIN_PROFIT_BUY_PERCENT = Number(
  arbitrageRules.DEFAULT_MIN_PROFIT_BUY_PERCENT || 2
);
const ORDERBOOK_OUTLIER_RATIO = Number(arbitrageRules.ORDERBOOK_OUTLIER_RATIO || 3);
const ORDERBOOK_OUTLIER_PRICE_MAX = Number(arbitrageRules.ORDERBOOK_OUTLIER_PRICE_MAX || 1);

const MARKET_RELIABILITY = Object.freeze({
  steam: 100,
  skinport: 90,
  csfloat: 80,
  dmarket: 75
});

const FILTER_REASON_LABELS = Object.freeze({
  insufficient_market_data: "Insufficient market data",
  non_positive_profit: "Non-positive profit",
  spread_below_min: "Spread below baseline threshold",
  low_liquidity: "Low liquidity (no reliable buyers)",
  extreme_spread: "Extreme spread suggests stale or fake pricing"
});

function toFiniteOrNull(value) {
  if (value == null) return null;
  if (typeof value === "string" && !value.trim()) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function resolveLiquiditySample(item = {}, quotes = []) {
  const volume7d = toFiniteOrNull(resolveVolume7d(item));
  if (volume7d != null && volume7d >= 0) return volume7d;
  const quoteVolume = (Array.isArray(quotes) ? quotes : [])
    .map((row) => toFiniteOrNull(row?.volume_7d))
    .filter((row) => row != null && row >= 0)
    .sort((a, b) => b - a)[0];
  if (quoteVolume != null) return quoteVolume;
  const score = toFiniteOrNull(resolveLiquidityScore(item));
  if (score != null && score >= 0) {
    // Preserve backwards-compatible numeric "sample" for existing UI filters.
    return round2(Math.min(score, 100) * 2);
  }
  return null;
}

function resolveSevenDayChange(item = {}) {
  return resolveSevenDayChangePercent(item);
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
  if (liquidity >= 200) return 100;
  if (liquidity >= 100) return 80;
  if (liquidity >= 50) return 60;
  return 30;
}

function getStabilityScore(sevenDayChangePercent) {
  const change = toFiniteOrNull(sevenDayChangePercent);
  if (change == null) return 60;
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
  if (safeScore >= 90) return "Strong";
  if (safeScore >= 70) return "Good";
  if (safeScore >= 50) return "Risky";
  return "Weak";
}

function toNetPrice(grossPrice, feePercent) {
  const gross = toFiniteOrNull(grossPrice);
  if (gross == null || gross <= 0) return null;
  const fee = toFiniteOrNull(feePercent);
  if (fee == null) return roundPrice(gross);
  const clampedFee = Math.min(Math.max(fee, 0), 99.99);
  return roundPrice(gross * (1 - clampedFee / 100));
}

function applyBuyOutlierReplacement(quote = {}) {
  const price = toFiniteOrNull(quote?.best_buy);
  const buyTop1 = toFiniteOrNull(quote?.orderbook?.buy_top1);
  const buyTop2 = toFiniteOrNull(quote?.orderbook?.buy_top2);
  if (price == null || price <= 0 || buyTop1 == null || buyTop2 == null) {
    return {
      price: price != null && price > 0 ? roundPrice(price) : null,
      adjusted: false,
      outlierRatio: null
    };
  }

  const outlierRatio = buyTop2 > 0 && buyTop1 > 0 ? buyTop2 / buyTop1 : null;
  const shouldReplace =
    outlierRatio != null &&
    outlierRatio >= ORDERBOOK_OUTLIER_RATIO &&
    buyTop1 < ORDERBOOK_OUTLIER_PRICE_MAX;

  if (!shouldReplace) {
    return {
      price: roundPrice(price),
      adjusted: false,
      outlierRatio
    };
  }

  // Option B: replace suspicious top1 outlier with top2 and recompute spread/profit.
  return {
    price: roundPrice(buyTop2),
    adjusted: true,
    outlierRatio
  };
}

function applySellOutlierReplacement(quote = {}) {
  const sellNet = toFiniteOrNull(quote?.best_sell_net);
  const sellTop1 = toFiniteOrNull(quote?.orderbook?.sell_top1);
  const sellTop2 = toFiniteOrNull(quote?.orderbook?.sell_top2);
  if (sellNet == null || sellNet <= 0 || sellTop1 == null || sellTop2 == null || sellTop2 <= 0) {
    return {
      net: sellNet != null && sellNet > 0 ? roundPrice(sellNet) : null,
      adjusted: false,
      outlierRatio: null
    };
  }

  const outlierRatio = sellTop1 / sellTop2;
  if (!Number.isFinite(outlierRatio) || outlierRatio < ORDERBOOK_OUTLIER_RATIO) {
    return {
      net: roundPrice(sellNet),
      adjusted: false,
      outlierRatio
    };
  }

  return {
    net: toNetPrice(sellTop2, quote?.fee_percent),
    adjusted: true,
    outlierRatio
  };
}

function pickBestBuyCandidate(quotes = []) {
  const candidates = (Array.isArray(quotes) ? quotes : [])
    .map((quote) => {
      const replacement = applyBuyOutlierReplacement(quote);
      return {
        quote,
        effectiveBuyPrice: replacement.price,
        buyOutlierAdjusted: replacement.adjusted,
        buyOutlierRatio: replacement.outlierRatio
      };
    })
    .filter((row) => Number.isFinite(Number(row.effectiveBuyPrice)) && Number(row.effectiveBuyPrice) > 0)
    .sort((a, b) => Number(a.effectiveBuyPrice) - Number(b.effectiveBuyPrice));

  return candidates[0] || null;
}

function pickBestSellCandidate(quotes = []) {
  const candidates = (Array.isArray(quotes) ? quotes : [])
    .map((quote) => {
      const replacement = applySellOutlierReplacement(quote);
      return {
        quote,
        effectiveSellNet: replacement.net,
        sellOutlierAdjusted: replacement.adjusted,
        sellOutlierRatio: replacement.outlierRatio
      };
    })
    .filter((row) => Number.isFinite(Number(row.effectiveSellNet)) && Number(row.effectiveSellNet) > 0)
    .sort((a, b) => Number(b.effectiveSellNet) - Number(a.effectiveSellNet));

  return candidates[0] || null;
}

function evaluateLiquidityFilter({ buyQuote = null, sellQuote = null, quotes = [] } = {}) {
  const quoteList = Array.isArray(quotes) ? quotes : [];
  const bestVolumeAcrossMarkets = quoteList
    .map((row) => toFiniteOrNull(row?.volume_7d))
    .filter((value) => value != null && value >= 0)
    .sort((a, b) => b - a)[0];
  const bestLiquidityAcrossMarkets = quoteList
    .map((row) => toFiniteOrNull(row?.liquidity_score))
    .filter((value) => value != null && value >= 0)
    .sort((a, b) => b - a)[0];

  const volume7d = [
    toFiniteOrNull(sellQuote?.volume_7d),
    toFiniteOrNull(buyQuote?.volume_7d),
    bestVolumeAcrossMarkets
  ].find((value) => value != null && value >= 0);

  if (volume7d != null) {
    if (volume7d >= Number(arbitrageRules.LIQUIDITY_VOLUME_PASS || 100)) {
      return {
        passed: true,
        medium: false,
        unknown: false,
        signalType: "volume_7d",
        signalValue: volume7d
      };
    }
    if (volume7d >= Number(arbitrageRules.LIQUIDITY_VOLUME_MEDIUM || 50)) {
      return {
        passed: true,
        medium: true,
        unknown: false,
        signalType: "volume_7d",
        signalValue: volume7d
      };
    }
    return {
      passed: false,
      medium: false,
      unknown: false,
      signalType: "volume_7d",
      signalValue: volume7d
    };
  }

  const liquidityScore = [
    toFiniteOrNull(sellQuote?.liquidity_score),
    toFiniteOrNull(buyQuote?.liquidity_score),
    bestLiquidityAcrossMarkets
  ].find((value) => value != null && value >= 0);

  if (liquidityScore != null) {
    if (liquidityScore >= Number(arbitrageRules.LIQUIDITY_SCORE_PASS || 40)) {
      return {
        passed: true,
        medium: false,
        unknown: false,
        signalType: "liquidity_score",
        signalValue: liquidityScore
      };
    }
    if (liquidityScore >= Number(arbitrageRules.LIQUIDITY_SCORE_MEDIUM || 30)) {
      return {
        passed: true,
        medium: true,
        unknown: false,
        signalType: "liquidity_score",
        signalValue: liquidityScore
      };
    }
    return {
      passed: false,
      medium: false,
      unknown: false,
      signalType: "liquidity_score",
      signalValue: liquidityScore
    };
  }

  return {
    passed: true,
    medium: false,
    unknown: true,
    signalType: "unknown",
    signalValue: null
  };
}

function computeLiquidityScoreForRanking(liquidityFilter = {}) {
  if (liquidityFilter.signalType === "volume_7d") {
    const value = toFiniteOrNull(liquidityFilter.signalValue);
    if (value == null || value < 0) return { score: 30, penalty: 0 };
    if (value >= 200) return { score: 100, penalty: 0 };
    if (value >= 100) return { score: 80, penalty: 0 };
    if (value >= 50) return { score: 60, penalty: 0 };
    return { score: 30, penalty: 0 };
  }

  if (liquidityFilter.signalType === "liquidity_score") {
    const value = toFiniteOrNull(liquidityFilter.signalValue);
    if (value == null || value < 0) return { score: 30, penalty: 0 };
    if (value >= 70) return { score: 100, penalty: 0 };
    if (value >= 40) return { score: 80, penalty: 0 };
    if (value >= 30) return { score: 60, penalty: 0 };
    return { score: 30, penalty: 0 };
  }

  return {
    score: Number(arbitrageRules.UNKNOWN_LIQUIDITY_SCORE_BASE || 50),
    penalty: Number(arbitrageRules.UNKNOWN_LIQUIDITY_SCORE_PENALTY || 15)
  };
}

function formatFilterReasons(reasonCodes = []) {
  return (Array.isArray(reasonCodes) ? reasonCodes : []).map(
    (code) => FILTER_REASON_LABELS[String(code || "")] || "Arbitrage filtered by realism checks"
  );
}

function evaluateItemOpportunity(item = {}, options = {}) {
  const minSpreadPercent =
    toFiniteOrNull(options.minSpreadPercent) != null
      ? Number(options.minSpreadPercent)
      : MIN_SPREAD_PERCENT;
  const itemId = Number(item?.skinId || item?.itemId || 0) || null;
  const itemName = String(item?.marketHashName || item?.itemName || "Tracked Item").trim();
  const normalizedQuotes = normalizeMarketQuotes(item);
  const quotes = Array.isArray(normalizedQuotes?.quotes) ? normalizedQuotes.quotes : [];
  const sevenDayChangePercent = resolveSevenDayChange(item);

  const buyCandidate = pickBestBuyCandidate(quotes);
  const sellCandidate = pickBestSellCandidate(quotes);
  const buyQuote = buyCandidate?.quote || null;
  const sellQuote = sellCandidate?.quote || null;
  const buyPrice = Number.isFinite(Number(buyCandidate?.effectiveBuyPrice))
    ? roundPrice(Number(buyCandidate.effectiveBuyPrice))
    : null;
  const sellNet = Number.isFinite(Number(sellCandidate?.effectiveSellNet))
    ? roundPrice(Number(sellCandidate.effectiveSellNet))
    : null;
  const rawProfit =
    Number.isFinite(buyPrice) && Number.isFinite(sellNet)
      ? Number(sellNet) - Number(buyPrice)
      : null;
  const profit = rawProfit != null ? roundPrice(rawProfit) : null;
  const spreadPercent =
    rawProfit != null && Number(buyPrice) > 0
      ? round2((Number(rawProfit) / Number(buyPrice)) * 100)
      : null;

  const liquidityFilter = evaluateLiquidityFilter({
    buyQuote,
    sellQuote,
    quotes
  });
  const liquiditySample = resolveLiquiditySample(item, quotes);

  const reasons = [];
  if (!buyCandidate || !sellCandidate || buyPrice == null || sellNet == null) {
    reasons.push("insufficient_market_data");
  } else {
    if (Number(profit || 0) <= 0) {
      reasons.push("non_positive_profit");
    }
    if (Number(spreadPercent || 0) < Number(minSpreadPercent)) {
      reasons.push("spread_below_min");
    }
    if (Number(spreadPercent || 0) > SPREAD_SANITY_MAX_PERCENT) {
      reasons.push("extreme_spread");
    }
    if (!liquidityFilter.passed) {
      reasons.push("low_liquidity");
    }
  }

  const spreadScore = getSpreadScore(spreadPercent);
  const liquidityScoreBundle = computeLiquidityScoreForRanking(liquidityFilter);
  const liquidityScore = Number(liquidityScoreBundle.score || 0);
  const stabilityScore = getStabilityScore(sevenDayChangePercent);
  const marketScore = getMarketScore(buyQuote?.market, sellQuote?.market);
  const weightedScore = round2(
    spreadScore * 0.35 +
      liquidityScore * 0.35 +
      stabilityScore * 0.2 +
      marketScore * 0.1
  );
  const opportunityScore = round2(
    Math.min(
      Math.max(weightedScore - Number(liquidityScoreBundle.penalty || 0), 0),
      100
    )
  );
  const scoreCategory = categorizeOpportunityScore(opportunityScore);
  const isOpportunity = !reasons.length;

  return {
    itemId,
    itemName,
    buy: {
      market: buyQuote?.market || null,
      price: buyPrice
    },
    sell: {
      market: sellQuote?.market || null,
      net: sellNet
    },
    buyMarket: buyQuote?.market || null,
    buyPrice,
    sellMarket: sellQuote?.market || null,
    sellNet,
    profit,
    spreadPercent,
    spread_pct: spreadPercent,
    opportunityScore,
    scoreCategory,
    isOpportunity,
    liquiditySample,
    sevenDayChangePercent,
    buyUrl: buyQuote?.url || null,
    sellUrl: sellQuote?.url || null,
    antiFake: {
      passed: isOpportunity,
      filteredOut: !isOpportunity,
      reasons,
      reasonLabels: formatFilterReasons(reasons),
      liquidity: liquidityFilter,
      filters: {
        minSpreadPercent,
        spreadSanityMaxPercent: SPREAD_SANITY_MAX_PERCENT
      },
      outlier: {
        buyAdjusted: Boolean(buyCandidate?.buyOutlierAdjusted),
        sellAdjusted: Boolean(sellCandidate?.sellOutlierAdjusted),
        buyOutlierRatio: toFiniteOrNull(buyCandidate?.buyOutlierRatio),
        sellOutlierRatio: toFiniteOrNull(sellCandidate?.sellOutlierRatio)
      }
    },
    scores: {
      spreadScore,
      liquidityScore,
      stabilityScore,
      marketScore,
      liquidityPenalty: Number(liquidityScoreBundle.penalty || 0)
    },
    debug: {
      rawQuotesByMarket: normalizedQuotes?.byMarket || {}
    }
  };
}

function normalizeSortBy(sortBy) {
  const safe = String(sortBy || "score")
    .trim()
    .toLowerCase();
  if (safe === "spread") return "spread";
  if (safe === "profit") return "profit";
  return "score";
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
  if (!raw || raw === "all") return new Set();
  return new Set(
    raw
      .split(",")
      .map((value) => normalizeMarket(value))
      .filter(Boolean)
  );
}

function rankOpportunities(opportunities = [], options = {}) {
  const includeRisky = Boolean(options.includeRisky || options.showRisky);
  const minProfit = toFiniteOrNull(options.minProfit) ?? DEFAULT_MIN_PROFIT_ABSOLUTE;
  const minProfitBuyPercent =
    toFiniteOrNull(options.minProfitBuyPercent) ?? DEFAULT_MIN_PROFIT_BUY_PERCENT;
  const minSpreadPercent =
    toFiniteOrNull(options.minSpreadPercent) != null
      ? Number(options.minSpreadPercent)
      : MIN_SPREAD_PERCENT;
  const minScore =
    toFiniteOrNull(options.minScore) != null
      ? Number(options.minScore)
      : includeRisky
        ? RISKY_SCORE_CUTOFF
        : DEFAULT_SCORE_CUTOFF;
  const liquidityMin = toFiniteOrNull(options.liquidityMin) ?? 0;
  const limit = Number.isFinite(Number(options.limit)) ? Math.max(Number(options.limit), 0) : 0;
  const sortBy = normalizeSortBy(options.sortBy);
  const marketSet = normalizeMarketSet(options.markets || options.market);

  const filtered = (Array.isArray(opportunities) ? opportunities : [])
    .filter((row) => row && row.isOpportunity)
    .filter((row) => {
      const buyPrice = toFiniteOrNull(row?.buyPrice ?? row?.buy?.price);
      const absoluteMin = Number(minProfit || 0);
      const relativeMin =
        buyPrice != null && buyPrice > 0
          ? Number(buyPrice) * (Number(minProfitBuyPercent || 0) / 100)
          : 0;
      const effectiveProfitFloor = Math.max(absoluteMin, relativeMin);
      return Number(row.profit || 0) >= effectiveProfitFloor;
    })
    .filter((row) => Number(row.spreadPercent || 0) >= minSpreadPercent)
    .filter((row) => Number(row.opportunityScore || 0) >= minScore)
    .filter((row) => {
      if (liquidityMin <= 0) return true;
      return Number(row.liquiditySample || 0) >= liquidityMin;
    })
    .filter((row) => {
      if (!marketSet.size) return true;
      return marketSet.has(normalizeMarket(row.buyMarket)) || marketSet.has(normalizeMarket(row.sellMarket));
    });

  filtered.sort((a, b) => {
    if (sortBy === "profit") {
      return (
        Number(b.profit || 0) - Number(a.profit || 0) ||
        Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
        Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0)
      );
    }
    if (sortBy === "spread") {
      return (
        Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0) ||
        Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
        Number(b.profit || 0) - Number(a.profit || 0)
      );
    }
    return (
      Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
      Number(b.profit || 0) - Number(a.profit || 0) ||
      Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0)
    );
  });

  if (limit > 0) {
    return filtered.slice(0, limit);
  }
  return filtered;
}

module.exports = {
  MIN_SPREAD_PERCENT,
  SPREAD_SANITY_MAX_PERCENT,
  DEFAULT_SCORE_CUTOFF,
  RISKY_SCORE_CUTOFF,
  DEFAULT_MIN_PROFIT_ABSOLUTE,
  DEFAULT_MIN_PROFIT_BUY_PERCENT,
  MARKET_RELIABILITY,
  evaluateItemOpportunity,
  rankOpportunities,
  categorizeOpportunityScore,
  FILTER_REASON_LABELS,
  __testables: {
    resolveLiquiditySample,
    resolveSevenDayChange,
    getSpreadScore,
    getLiquidityScore,
    getStabilityScore,
    getMarketScore,
    evaluateLiquidityFilter,
    applyBuyOutlierReplacement,
    applySellOutlierReplacement,
    computeLiquidityScoreForRanking
  }
};

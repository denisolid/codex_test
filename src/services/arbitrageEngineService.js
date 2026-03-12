const { round2, roundPrice } = require("../markets/marketUtils")
const arbitrageRules = require("../config/arbitrageRules")
const {
  normalizeMarket,
  normalizeMarketQuotes,
  resolveVolume7d,
  resolveLiquidityScore,
  resolveSevenDayChangePercent
} = require("./arbitrageQuoteNormalizerService")

const MIN_EXECUTION_PRICE_USD = Number(arbitrageRules.MIN_EXECUTION_PRICE_USD || 3)
const MIN_SPREAD_PERCENT = Number(arbitrageRules.MIN_SPREAD_PERCENT_BASELINE || 5)
const SPREAD_SUSPICIOUS_PENALTY_THRESHOLD = Number(
  arbitrageRules.SPREAD_SUSPICIOUS_PENALTY_THRESHOLD || 120
)
const SPREAD_SANITY_MAX_PERCENT = Number(arbitrageRules.SPREAD_SANITY_MAX_PERCENT || 300)
const REFERENCE_DEVIATION_RATIO_MAX = Number(
  arbitrageRules.REFERENCE_DEVIATION_RATIO_MAX || 3
)
const MIN_MARKET_COVERAGE = Number(arbitrageRules.MIN_MARKET_COVERAGE || 2)
const LIQUIDITY_VOLUME_PASS = Number(arbitrageRules.LIQUIDITY_VOLUME_PASS || 100)
const LIQUIDITY_VOLUME_MEDIUM = Number(arbitrageRules.LIQUIDITY_VOLUME_MEDIUM || 50)
const LIQUIDITY_VOLUME_HIGH = Number(arbitrageRules.LIQUIDITY_VOLUME_HIGH || 200)
const DEFAULT_SCORE_CUTOFF = Number(arbitrageRules.DEFAULT_SCORE_CUTOFF || 75)
const RISKY_SCORE_CUTOFF = Number(arbitrageRules.RISKY_SCORE_CUTOFF || 40)
const DEFAULT_MIN_PROFIT_ABSOLUTE = Number(arbitrageRules.DEFAULT_MIN_PROFIT_ABSOLUTE || 0.5)
const DEFAULT_MIN_PROFIT_BUY_PERCENT = Number(
  arbitrageRules.DEFAULT_MIN_PROFIT_BUY_PERCENT || 2
)
const ORDERBOOK_OUTLIER_RATIO = Number(arbitrageRules.ORDERBOOK_OUTLIER_RATIO || 3)
const PREMIUM_MIN_EXECUTION_PRICE_USD = 20
const PREMIUM_MIN_SPREAD_PERCENT = 3
const PREMIUM_SPREAD_HEAVY_PENALTY_THRESHOLD = 150
const PREMIUM_SPREAD_SANITY_MAX_PERCENT = 250
const PREMIUM_LIQUIDITY_LOW_MIN = 5
const PREMIUM_LIQUIDITY_MEDIUM_MIN = 10
const PREMIUM_LIQUIDITY_HIGH_MIN = 20
const PREMIUM_UNKNOWN_VOLUME_MIN_MARKETS = 3
const PREMIUM_REFERENCE_DEVIATION_PENALTY_RATIO = 1.8
const PREMIUM_REFERENCE_DEVIATION_REJECT_RATIO = 2.5
const PREMIUM_DEPTH_GAP_SUSPICIOUS_RATIO = 1.7
const PREMIUM_DEPTH_GAP_EXTREME_RATIO = 2.4

const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove"
})

const CATEGORY_LIQUIDITY_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    pass: LIQUIDITY_VOLUME_PASS,
    medium: LIQUIDITY_VOLUME_MEDIUM,
    high: LIQUIDITY_VOLUME_HIGH
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    pass: LIQUIDITY_VOLUME_PASS,
    medium: LIQUIDITY_VOLUME_MEDIUM,
    high: LIQUIDITY_VOLUME_HIGH
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    pass: LIQUIDITY_VOLUME_PASS,
    medium: LIQUIDITY_VOLUME_MEDIUM,
    high: LIQUIDITY_VOLUME_HIGH
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    pass: PREMIUM_LIQUIDITY_LOW_MIN,
    medium: PREMIUM_LIQUIDITY_MEDIUM_MIN,
    high: PREMIUM_LIQUIDITY_HIGH_MIN,
    allowMissing: true,
    unknownMinMarkets: PREMIUM_UNKNOWN_VOLUME_MIN_MARKETS
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    pass: PREMIUM_LIQUIDITY_LOW_MIN,
    medium: PREMIUM_LIQUIDITY_MEDIUM_MIN,
    high: PREMIUM_LIQUIDITY_HIGH_MIN,
    allowMissing: true,
    unknownMinMarkets: PREMIUM_UNKNOWN_VOLUME_MIN_MARKETS
  })
})

const REFERENCE_DEVIATION_RATIO_MAX_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: REFERENCE_DEVIATION_RATIO_MAX,
  [ITEM_CATEGORIES.CASE]: REFERENCE_DEVIATION_RATIO_MAX,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: REFERENCE_DEVIATION_RATIO_MAX,
  [ITEM_CATEGORIES.KNIFE]: PREMIUM_REFERENCE_DEVIATION_REJECT_RATIO,
  [ITEM_CATEGORIES.GLOVE]: PREMIUM_REFERENCE_DEVIATION_REJECT_RATIO
})

const CATEGORY_MIN_EXECUTION_PRICE = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: MIN_EXECUTION_PRICE_USD,
  [ITEM_CATEGORIES.CASE]: MIN_EXECUTION_PRICE_USD,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: MIN_EXECUTION_PRICE_USD,
  [ITEM_CATEGORIES.KNIFE]: PREMIUM_MIN_EXECUTION_PRICE_USD,
  [ITEM_CATEGORIES.GLOVE]: PREMIUM_MIN_EXECUTION_PRICE_USD
})

const CATEGORY_SPREAD_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    min: MIN_SPREAD_PERCENT,
    suspiciousPenaltyThreshold: SPREAD_SUSPICIOUS_PENALTY_THRESHOLD,
    max: SPREAD_SANITY_MAX_PERCENT
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    min: MIN_SPREAD_PERCENT,
    suspiciousPenaltyThreshold: SPREAD_SUSPICIOUS_PENALTY_THRESHOLD,
    max: SPREAD_SANITY_MAX_PERCENT
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    min: MIN_SPREAD_PERCENT,
    suspiciousPenaltyThreshold: SPREAD_SUSPICIOUS_PENALTY_THRESHOLD,
    max: SPREAD_SANITY_MAX_PERCENT
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    min: PREMIUM_MIN_SPREAD_PERCENT,
    suspiciousPenaltyThreshold: PREMIUM_SPREAD_HEAVY_PENALTY_THRESHOLD,
    max: PREMIUM_SPREAD_SANITY_MAX_PERCENT
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    min: PREMIUM_MIN_SPREAD_PERCENT,
    suspiciousPenaltyThreshold: PREMIUM_SPREAD_HEAVY_PENALTY_THRESHOLD,
    max: PREMIUM_SPREAD_SANITY_MAX_PERCENT
  })
})

const CATEGORY_SCORE_WEIGHTS = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    spread: 0.25,
    liquidity: 0.3,
    stability: 0.15,
    market: 0.15,
    depth: 0.15
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    spread: 0.25,
    liquidity: 0.3,
    stability: 0.15,
    market: 0.15,
    depth: 0.15
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    spread: 0.25,
    liquidity: 0.3,
    stability: 0.15,
    market: 0.15,
    depth: 0.15
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    spread: 0.2,
    liquidity: 0.2,
    stability: 0.15,
    market: 0.15,
    depth: 0.3
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    spread: 0.2,
    liquidity: 0.2,
    stability: 0.15,
    market: 0.15,
    depth: 0.3
  })
})

const MARKET_RELIABILITY = Object.freeze({
  steam: 1,
  skinport: 0.9,
  csfloat: 0.8,
  dmarket: 0.6
})

const FILTER_REASON_LABELS = Object.freeze({
  insufficient_market_data: "Insufficient market data",
  non_positive_profit: "Non-positive profit",
  spread_below_min: "Spread below baseline threshold",
  ignored_low_price: "Buy price below execution floor",
  ignored_low_liquidity: "Low liquidity",
  ignored_extreme_spread: "Extreme spread suggests stale/fake pricing",
  ignored_reference_deviation: "Price deviates too far from reference",
  ignored_missing_markets: "Missing market coverage",
  ignored_missing_depth: "Insufficient depth confidence"
})

function normalizeItemCategory(value, itemName = "") {
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  if (raw === ITEM_CATEGORIES.KNIFE || raw === "knives") return ITEM_CATEGORIES.KNIFE
  if (raw === ITEM_CATEGORIES.GLOVE || raw === "gloves") return ITEM_CATEGORIES.GLOVE
  if (
    raw === ITEM_CATEGORIES.STICKER_CAPSULE ||
    raw === "sticker capsule" ||
    raw === "capsule"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (raw === ITEM_CATEGORIES.WEAPON_SKIN) return ITEM_CATEGORIES.WEAPON_SKIN
  if (/sticker capsule$/i.test(String(itemName || "").trim())) return ITEM_CATEGORIES.STICKER_CAPSULE
  if (/case$/i.test(String(itemName || "").trim())) return ITEM_CATEGORIES.CASE
  if (/\b(gloves|glove|hand wraps)\b/i.test(String(itemName || "").trim())) return ITEM_CATEGORIES.GLOVE
  if (/\b(knife|bayonet|karambit|daggers)\b/i.test(String(itemName || "").trim())) return ITEM_CATEGORIES.KNIFE
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function getCategoryLiquidityRules(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return CATEGORY_LIQUIDITY_RULES[normalized] || CATEGORY_LIQUIDITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function isPremiumCategory(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return normalized === ITEM_CATEGORIES.KNIFE || normalized === ITEM_CATEGORIES.GLOVE
}

function getCategorySpreadRules(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return CATEGORY_SPREAD_RULES[normalized] || CATEGORY_SPREAD_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function getCategoryMinExecutionPrice(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return CATEGORY_MIN_EXECUTION_PRICE[normalized] || CATEGORY_MIN_EXECUTION_PRICE[ITEM_CATEGORIES.WEAPON_SKIN]
}

function getCategoryScoreWeights(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return CATEGORY_SCORE_WEIGHTS[normalized] || CATEGORY_SCORE_WEIGHTS[ITEM_CATEGORIES.WEAPON_SKIN]
}

function getReferenceDeviationRatioMax(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return (
    REFERENCE_DEVIATION_RATIO_MAX_BY_CATEGORY[normalized] ||
    REFERENCE_DEVIATION_RATIO_MAX_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function toFiniteOrNull(value) {
  if (value == null) return null
  if (typeof value === "string" && !value.trim()) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function clampScore(value) {
  const parsed = toFiniteOrNull(value) ?? 0
  return round2(Math.min(Math.max(parsed, 0), 100))
}

function resolveLiquiditySample(item = {}, quotes = []) {
  const volume7d = toFiniteOrNull(resolveVolume7d(item))
  if (volume7d != null && volume7d >= 0) return volume7d
  const quoteVolume = (Array.isArray(quotes) ? quotes : [])
    .map((row) => toFiniteOrNull(row?.volume_7d))
    .filter((row) => row != null && row >= 0)
    .sort((a, b) => b - a)[0]
  if (quoteVolume != null) return quoteVolume
  const score = toFiniteOrNull(resolveLiquidityScore(item))
  if (score != null && score >= 0) {
    // Backward compatibility for UI filters that use a numeric liquidity sample.
    return round2(Math.min(score, 100) * 2)
  }
  return null
}

function resolveSevenDayChange(item = {}) {
  return resolveSevenDayChangePercent(item)
}

function getSpreadScore(spreadPercent, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const spread = toFiniteOrNull(spreadPercent)
  const spreadRules = getCategorySpreadRules(itemCategory)
  if (spread == null) return 30
  if (spread < Number(spreadRules.min || MIN_SPREAD_PERCENT)) return 20
  if (isPremiumCategory(itemCategory)) {
    if (spread < 10) return 62
    if (spread < 20) return 80
    if (spread <= 40) return 90
    if (spread <= PREMIUM_SPREAD_HEAVY_PENALTY_THRESHOLD) return 78
    if (spread <= Number(spreadRules.max || PREMIUM_SPREAD_SANITY_MAX_PERCENT)) return 42
    return 10
  }
  if (spread < 10) return 60
  if (spread < 20) return 80
  if (spread <= 40) return 90
  return 70
}

function getLiquidityScore(liquiditySample) {
  const liquidity = toFiniteOrNull(liquiditySample)
  if (liquidity == null || liquidity < 0) return 30
  if (liquidity > 500) return 100
  if (liquidity >= 200) return 85
  if (liquidity >= 100) return 70
  return 30
}

function getStabilityScore(sevenDayChangePercent) {
  const change = toFiniteOrNull(sevenDayChangePercent)
  if (change == null) return 60
  const absChange = Math.abs(change)
  if (absChange < 5) return 100
  if (absChange <= 10) return 80
  if (absChange <= 20) return 50
  return 20
}

function getMarketScore(buyMarket, sellMarket) {
  const buyScore = Number(MARKET_RELIABILITY[normalizeMarket(buyMarket)] || 0.7)
  const sellScore = Number(MARKET_RELIABILITY[normalizeMarket(sellMarket)] || 0.7)
  return round2((buyScore * 0.4 + sellScore * 0.6) * 100)
}

function categorizeOpportunityScore(score) {
  const safeScore = toFiniteOrNull(score) ?? 0
  if (safeScore >= 90) return "Strong"
  if (safeScore >= 75) return "Good"
  if (safeScore >= 60) return "Risky"
  return "Weak"
}

function toNetPrice(grossPrice, feePercent) {
  const gross = toFiniteOrNull(grossPrice)
  if (gross == null || gross <= 0) return null
  const fee = toFiniteOrNull(feePercent)
  if (fee == null) return roundPrice(gross)
  const clampedFee = Math.min(Math.max(fee, 0), 99.99)
  return roundPrice(gross * (1 - clampedFee / 100))
}

function applyBuyOutlierReplacement(quote = {}) {
  const price = toPositiveOrNull(quote?.best_buy)
  const buyTop1 = toPositiveOrNull(quote?.orderbook?.buy_top1) ?? price
  const buyTop2 = toPositiveOrNull(quote?.orderbook?.buy_top2)
  const hasDepth = buyTop1 != null && buyTop2 != null
  if (price == null) {
    return {
      price: null,
      adjusted: false,
      outlierRatio: null,
      missingDepth: !hasDepth
    }
  }
  if (!hasDepth) {
    return {
      price: roundPrice(price),
      adjusted: false,
      outlierRatio: null,
      missingDepth: true
    }
  }

  const outlierRatio = buyTop2 / buyTop1
  const shouldReplace = Number.isFinite(outlierRatio) && outlierRatio >= ORDERBOOK_OUTLIER_RATIO

  if (!shouldReplace) {
    return {
      price: roundPrice(price),
      adjusted: false,
      outlierRatio: round2(outlierRatio),
      missingDepth: false
    }
  }

  return {
    price: roundPrice(buyTop2),
    adjusted: true,
    outlierRatio: round2(outlierRatio),
    missingDepth: false
  }
}

function applySellOutlierReplacement(quote = {}) {
  const sellNet = toPositiveOrNull(quote?.best_sell_net)
  const sellTop1 = toPositiveOrNull(quote?.orderbook?.sell_top1)
  const sellTop2 = toPositiveOrNull(quote?.orderbook?.sell_top2)
  const hasDepth = sellTop1 != null && sellTop2 != null
  if (sellNet == null) {
    return {
      net: null,
      adjusted: false,
      outlierRatio: null,
      missingDepth: !hasDepth
    }
  }
  if (!hasDepth || sellTop2 <= 0) {
    return {
      net: roundPrice(sellNet),
      adjusted: false,
      outlierRatio: null,
      missingDepth: true
    }
  }

  const outlierRatio = sellTop1 / sellTop2
  const shouldReplace = Number.isFinite(outlierRatio) && outlierRatio >= ORDERBOOK_OUTLIER_RATIO
  if (!shouldReplace) {
    return {
      net: roundPrice(sellNet),
      adjusted: false,
      outlierRatio: round2(outlierRatio),
      missingDepth: false
    }
  }

  return {
    net: toNetPrice(sellTop2, quote?.fee_percent) ?? roundPrice(sellNet),
    adjusted: true,
    outlierRatio: round2(outlierRatio),
    missingDepth: false
  }
}

function pickBestBuyCandidate(quotes = []) {
  const candidates = (Array.isArray(quotes) ? quotes : [])
    .map((quote) => {
      const replacement = applyBuyOutlierReplacement(quote)
      return {
        quote,
        effectiveBuyPrice: replacement.price,
        buyOutlierAdjusted: replacement.adjusted,
        buyOutlierRatio: replacement.outlierRatio,
        buyDepthMissing: replacement.missingDepth
      }
    })
    .filter((row) => Number.isFinite(Number(row.effectiveBuyPrice)) && Number(row.effectiveBuyPrice) > 0)
    .sort((a, b) => Number(a.effectiveBuyPrice) - Number(b.effectiveBuyPrice))

  return candidates[0] || null
}

function pickBestSellCandidate(quotes = []) {
  const candidates = (Array.isArray(quotes) ? quotes : [])
    .map((quote) => {
      const replacement = applySellOutlierReplacement(quote)
      return {
        quote,
        effectiveSellNet: replacement.net,
        sellOutlierAdjusted: replacement.adjusted,
        sellOutlierRatio: replacement.outlierRatio,
        sellDepthMissing: replacement.missingDepth
      }
    })
    .filter((row) => Number.isFinite(Number(row.effectiveSellNet)) && Number(row.effectiveSellNet) > 0)
    .sort((a, b) => Number(b.effectiveSellNet) - Number(a.effectiveSellNet))

  return candidates[0] || null
}

function evaluateLiquidityFilter({
  buyQuote = null,
  sellQuote = null,
  quotes = [],
  item = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  const rules = getCategoryLiquidityRules(itemCategory)
  const quoteList = Array.isArray(quotes) ? quotes : []
  const bestVolumeAcrossMarkets = quoteList
    .map((row) => toFiniteOrNull(row?.volume_7d))
    .filter((value) => value != null && value >= 0)
    .sort((a, b) => b - a)[0]

  const fallbackVolume = toFiniteOrNull(resolveVolume7d(item))
  const volume7d = [
    toFiniteOrNull(sellQuote?.volume_7d),
    toFiniteOrNull(buyQuote?.volume_7d),
    bestVolumeAcrossMarkets,
    fallbackVolume
  ].find((value) => value != null && value >= 0)

  if (volume7d == null) {
    if (isPremiumCategory(itemCategory) && Boolean(rules.allowMissing)) {
      return {
        passed: true,
        medium: false,
        high: false,
        unknown: true,
        signalType: "volume_7d",
        signalValue: null,
        band: "Unknown"
      }
    }
    return {
      passed: false,
      medium: false,
      high: false,
      unknown: true,
      signalType: "volume_7d",
      signalValue: null,
      band: "Low"
    }
  }

  if (volume7d >= Number(rules.high || LIQUIDITY_VOLUME_HIGH)) {
    return {
      passed: true,
      medium: false,
      high: true,
      unknown: false,
      signalType: "volume_7d",
      signalValue: volume7d,
      band: "High"
    }
  }

  if (volume7d >= Number(rules.medium || rules.pass || LIQUIDITY_VOLUME_PASS)) {
    return {
      passed: true,
      medium: true,
      high: false,
      unknown: false,
      signalType: "volume_7d",
      signalValue: volume7d,
      band: "Medium"
    }
  }

  if (volume7d >= Number(rules.pass || LIQUIDITY_VOLUME_PASS)) {
    return {
      passed: true,
      medium: false,
      high: false,
      unknown: false,
      signalType: "volume_7d",
      signalValue: volume7d,
      band: "Low"
    }
  }

  return {
    passed: false,
    medium: false,
    high: false,
    unknown: false,
    signalType: "volume_7d",
    signalValue: volume7d,
    band: "Low"
  }
}

function computeLiquidityScoreForRanking(
  liquidityFilter = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  if (liquidityFilter.signalType === "volume_7d") {
    const value = toFiniteOrNull(liquidityFilter.signalValue)
    if (isPremiumCategory(itemCategory)) {
      if (value == null) {
        return {
          score: 42,
          penalty: 22
        }
      }
      if (value >= PREMIUM_LIQUIDITY_HIGH_MIN) {
        return {
          score: 92,
          penalty: 0
        }
      }
      if (value >= PREMIUM_LIQUIDITY_MEDIUM_MIN) {
        return {
          score: 74,
          penalty: 6
        }
      }
      if (value >= PREMIUM_LIQUIDITY_LOW_MIN) {
        return {
          score: 56,
          penalty: 14
        }
      }
      return {
        score: 20,
        penalty: 26
      }
    }
    return {
      score: getLiquidityScore(value),
      penalty: value == null ? 20 : 0
    }
  }

  return {
    score: 30,
    penalty: 20
  }
}

function formatFilterReasons(reasonCodes = []) {
  return (Array.isArray(reasonCodes) ? reasonCodes : []).map(
    (code) => FILTER_REASON_LABELS[String(code || "")] || "Arbitrage filtered by realism checks"
  )
}

function countValidMarkets(quotes = []) {
  return (Array.isArray(quotes) ? quotes : []).filter((quote) => {
    const source = normalizeMarket(quote?.market)
    if (!source) return false
    const buy = toPositiveOrNull(quote?.best_buy)
    const sellNet = toPositiveOrNull(quote?.best_sell_net)
    return buy != null || sellNet != null
  }).length
}

function median(values = []) {
  const sorted = (Array.isArray(values) ? values : [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b)
  if (!sorted.length) return null
  const mid = Math.floor(sorted.length / 2)
  if (sorted.length % 2) return sorted[mid]
  return (sorted[mid - 1] + sorted[mid]) / 2
}

function resolveReferencePrice(item = {}, quotes = [], buyPrice = null, sellNet = null) {
  const directCandidates = [
    item?.referencePrice,
    item?.reference_price,
    item?.average7dPrice,
    item?.average_7d_price,
    item?.steamPrice,
    item?.currentPrice,
    item?.marketInsight?.sellSuggestion?.average7dPrice
  ]
    .map((value) => toPositiveOrNull(value))
    .filter((value) => value != null)

  if (directCandidates.length) {
    return roundPrice(directCandidates[0])
  }

  const quoteCandidates = (Array.isArray(quotes) ? quotes : []).flatMap((row) => [
    toPositiveOrNull(row?.best_buy),
    toPositiveOrNull(row?.best_sell_net)
  ])
  if (toPositiveOrNull(buyPrice) != null) quoteCandidates.push(toPositiveOrNull(buyPrice))
  if (toPositiveOrNull(sellNet) != null) quoteCandidates.push(toPositiveOrNull(sellNet))
  const mid = median(quoteCandidates)
  return mid != null ? roundPrice(mid) : null
}

function resolveReferenceDeviationRatio(price, referencePrice) {
  const candidate = toPositiveOrNull(price)
  const reference = toPositiveOrNull(referencePrice)
  if (candidate == null || reference == null) return null
  return round2(Math.max(candidate / reference, reference / candidate))
}

function resolveReferenceDeviationSignal({
  buyPrice = null,
  sellNet = null,
  referencePrice = null,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  const buyRatio = resolveReferenceDeviationRatio(buyPrice, referencePrice)
  const sellRatio = resolveReferenceDeviationRatio(sellNet, referencePrice)
  const maxRatio = [buyRatio, sellRatio].filter((value) => value != null).sort((a, b) => b - a)[0] ?? null

  if (!isPremiumCategory(itemCategory)) {
    return {
      buyRatio,
      sellRatio,
      maxRatio,
      strong: false,
      extreme: maxRatio != null && maxRatio > getReferenceDeviationRatioMax(itemCategory)
    }
  }

  const strong = maxRatio != null && maxRatio > PREMIUM_REFERENCE_DEVIATION_PENALTY_RATIO
  const extreme = maxRatio != null && maxRatio > PREMIUM_REFERENCE_DEVIATION_REJECT_RATIO
  return {
    buyRatio,
    sellRatio,
    maxRatio,
    strong,
    extreme
  }
}

function isReferenceDeviation(price, referencePrice, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const ratio = resolveReferenceDeviationRatio(price, referencePrice)
  if (ratio == null) return false
  return ratio > getReferenceDeviationRatioMax(itemCategory)
}

function getDepthConfidenceScore({
  buyOutlierAdjusted = false,
  sellOutlierAdjusted = false,
  buyDepthMissing = false,
  sellDepthMissing = false,
  buyDepthGapSuspicious = false,
  sellDepthGapSuspicious = false,
  buyDepthGapExtreme = false,
  sellDepthGapExtreme = false,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  if (isPremiumCategory(itemCategory)) {
    if (buyDepthGapExtreme || sellDepthGapExtreme) return 15
    if (buyDepthMissing || sellDepthMissing) return 30
    if (buyOutlierAdjusted || sellOutlierAdjusted) return 42
    if (buyDepthGapSuspicious || sellDepthGapSuspicious) return 48
    return 100
  }
  if (buyOutlierAdjusted || sellOutlierAdjusted) return 40
  if (buyDepthMissing || sellDepthMissing) return 60
  return 100
}

function resolveDepthGapSignals({
  buyOutlierRatio = null,
  sellOutlierRatio = null,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  if (!isPremiumCategory(itemCategory)) {
    return {
      buyDepthGapSuspicious: false,
      sellDepthGapSuspicious: false,
      buyDepthGapExtreme: false,
      sellDepthGapExtreme: false
    }
  }

  const buyRatio = toFiniteOrNull(buyOutlierRatio)
  const sellRatio = toFiniteOrNull(sellOutlierRatio)
  const buyDepthGapExtreme = buyRatio != null && buyRatio >= PREMIUM_DEPTH_GAP_EXTREME_RATIO
  const sellDepthGapExtreme = sellRatio != null && sellRatio >= PREMIUM_DEPTH_GAP_EXTREME_RATIO
  const buyDepthGapSuspicious =
    !buyDepthGapExtreme && buyRatio != null && buyRatio >= PREMIUM_DEPTH_GAP_SUSPICIOUS_RATIO
  const sellDepthGapSuspicious =
    !sellDepthGapExtreme && sellRatio != null && sellRatio >= PREMIUM_DEPTH_GAP_SUSPICIOUS_RATIO

  return {
    buyDepthGapSuspicious,
    sellDepthGapSuspicious,
    buyDepthGapExtreme,
    sellDepthGapExtreme
  }
}

function resolveDepthFlags({
  buyOutlierAdjusted = false,
  sellOutlierAdjusted = false,
  buyDepthMissing = false,
  sellDepthMissing = false,
  buyDepthGapSuspicious = false,
  sellDepthGapSuspicious = false,
  buyDepthGapExtreme = false,
  sellDepthGapExtreme = false
} = {}) {
  const flags = []
  if (buyOutlierAdjusted) flags.push("BUY_OUTLIER_ADJUSTED")
  if (sellOutlierAdjusted) flags.push("SELL_OUTLIER_ADJUSTED")
  if (buyDepthMissing || sellDepthMissing) flags.push("MISSING_DEPTH")
  if (buyDepthGapSuspicious) flags.push("BUY_DEPTH_GAP_SUSPICIOUS")
  if (sellDepthGapSuspicious) flags.push("SELL_DEPTH_GAP_SUSPICIOUS")
  if (buyDepthGapExtreme) flags.push("BUY_DEPTH_GAP_EXTREME")
  if (sellDepthGapExtreme) flags.push("SELL_DEPTH_GAP_EXTREME")
  return flags
}

function resolveExecutionConfidence({
  volume7d = null,
  spreadPercent = null,
  marketScore = 0,
  marketCoverage = 0,
  depthFlags = [],
  quoteAgeMinutes = null,
  snapshotStale = false,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN,
  referenceDeviation = {}
} = {}) {
  const liquidityRules = getCategoryLiquidityRules(itemCategory)
  const spreadRules = getCategorySpreadRules(itemCategory)
  const volume = toFiniteOrNull(volume7d)
  const spread = toFiniteOrNull(spreadPercent)
  const hasOutlierFlag = Array.isArray(depthFlags)
    ? depthFlags.some((flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED")
    : false
  const missingDepth = Array.isArray(depthFlags)
    ? depthFlags.includes("MISSING_DEPTH")
    : false
  const hasDepthGapSuspicious = Array.isArray(depthFlags)
    ? depthFlags.some(
        (flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS"
      )
    : false
  const hasDepthGapExtreme = Array.isArray(depthFlags)
    ? depthFlags.some((flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME")
    : false
  const hasStrongReferenceDeviation = Boolean(referenceDeviation?.strong)
  const hasExtremeReferenceDeviation = Boolean(referenceDeviation?.extreme)
  const stale = (toFiniteOrNull(quoteAgeMinutes) ?? 0) >= 60 || Boolean(snapshotStale)

  if (isPremiumCategory(itemCategory)) {
    if (
      volume != null &&
      volume < Number(liquidityRules.pass || PREMIUM_LIQUIDITY_LOW_MIN)
    ) {
      return "Low"
    }
    if (
      Number(marketCoverage || 0) < Number(MIN_MARKET_COVERAGE || 2) ||
      hasDepthGapExtreme ||
      hasExtremeReferenceDeviation
    ) {
      return "Low"
    }

    const uncertaintyFlags = [
      missingDepth,
      hasOutlierFlag || hasDepthGapSuspicious,
      stale,
      hasStrongReferenceDeviation,
      marketScore < 75,
      spread == null || spread > Number(spreadRules.max || PREMIUM_SPREAD_SANITY_MAX_PERCENT)
    ].filter(Boolean).length

    const depthGood = !missingDepth && !hasOutlierFlag && !hasDepthGapSuspicious && !hasDepthGapExtreme
    if (
      volume != null &&
      volume >= Number(liquidityRules.high || PREMIUM_LIQUIDITY_HIGH_MIN) &&
      Number(marketCoverage || 0) >= 2 &&
      depthGood &&
      !stale &&
      !hasStrongReferenceDeviation &&
      uncertaintyFlags === 0
    ) {
      return "High"
    }
    if (
      volume != null &&
      volume >= Number(liquidityRules.medium || PREMIUM_LIQUIDITY_MEDIUM_MIN) &&
      !hasDepthGapExtreme &&
      !hasExtremeReferenceDeviation &&
      uncertaintyFlags <= 1
    ) {
      return "Medium"
    }
    return "Low"
  }

  if (
    volume != null &&
    volume > Number(liquidityRules.high || LIQUIDITY_VOLUME_HIGH) &&
    spread != null &&
    spread < 40 &&
    !hasOutlierFlag &&
    !missingDepth &&
    marketScore >= 85 &&
    !stale
  ) {
    return "High"
  }

  let uncertainty = 0
  if (hasOutlierFlag) uncertainty += 1
  if (missingDepth) uncertainty += 1
  if (stale) uncertainty += 1
  if (spread == null || spread > 80 || spread < Number(spreadRules.min || MIN_SPREAD_PERCENT)) {
    uncertainty += 1
  }
  if (marketScore < 75) uncertainty += 1

  if (volume != null && volume >= Number(liquidityRules.pass || LIQUIDITY_VOLUME_PASS) && uncertainty <= 1) {
    return "Medium"
  }

  return "Low"
}

function buildReasonBadges({
  liquidityBand = "Low",
  depthFlags = [],
  marketScore = 0,
  executionConfidence = "Low"
} = {}) {
  const badges = []
  if (liquidityBand === "High") badges.push("High liquidity")
  else if (liquidityBand === "Medium") badges.push("Medium liquidity")
  else if (liquidityBand === "Unknown") badges.push("Liquidity uncertain")

  if (Array.isArray(depthFlags) && depthFlags.includes("MISSING_DEPTH")) {
    badges.push("Missing depth")
  } else if (
    Array.isArray(depthFlags) &&
    (depthFlags.includes("BUY_DEPTH_GAP_EXTREME") || depthFlags.includes("SELL_DEPTH_GAP_EXTREME"))
  ) {
    badges.push("Depth anomaly")
  } else if (
    Array.isArray(depthFlags) &&
    (depthFlags.includes("BUY_DEPTH_GAP_SUSPICIOUS") || depthFlags.includes("SELL_DEPTH_GAP_SUSPICIOUS"))
  ) {
    badges.push("Depth gap flagged")
  } else if (
    Array.isArray(depthFlags) &&
    (depthFlags.includes("BUY_OUTLIER_ADJUSTED") ||
      depthFlags.includes("SELL_OUTLIER_ADJUSTED"))
  ) {
    badges.push("Outlier adjusted")
  } else {
    badges.push("Good depth")
  }

  if (marketScore >= 85) badges.push("Reliable markets")
  if (executionConfidence === "Low") badges.push("Risky execution")
  return badges
}

function evaluateItemOpportunity(item = {}, options = {}) {
  const itemId = Number(item?.skinId || item?.itemId || 0) || null
  const itemName = String(item?.marketHashName || item?.itemName || "Tracked Item").trim()
  const itemCategory = normalizeItemCategory(item?.itemCategory || item?.category, itemName)
  const spreadRules = getCategorySpreadRules(itemCategory)
  const minSpreadPercent =
    toFiniteOrNull(options.minSpreadPercent) != null
      ? Number(options.minSpreadPercent)
      : Number(spreadRules.min || MIN_SPREAD_PERCENT)
  const minExecutionPriceUsd = Number(getCategoryMinExecutionPrice(itemCategory) || MIN_EXECUTION_PRICE_USD)
  const premiumCategory = isPremiumCategory(itemCategory)
  const normalizedQuotes = normalizeMarketQuotes(item)
  const quotes = Array.isArray(normalizedQuotes?.quotes) ? normalizedQuotes.quotes : []
  const sevenDayChangePercent = resolveSevenDayChange(item)
  const marketCoverage = countValidMarkets(quotes)

  const buyCandidate = pickBestBuyCandidate(quotes)
  const sellCandidate = pickBestSellCandidate(quotes)
  const buyQuote = buyCandidate?.quote || null
  const sellQuote = sellCandidate?.quote || null
  const buyPrice = Number.isFinite(Number(buyCandidate?.effectiveBuyPrice))
    ? roundPrice(Number(buyCandidate.effectiveBuyPrice))
    : null
  const sellNet = Number.isFinite(Number(sellCandidate?.effectiveSellNet))
    ? roundPrice(Number(sellCandidate.effectiveSellNet))
    : null
  const rawProfit =
    Number.isFinite(buyPrice) && Number.isFinite(sellNet)
      ? Number(sellNet) - Number(buyPrice)
      : null
  const profit = rawProfit != null ? roundPrice(rawProfit) : null
  const spreadPercent =
    rawProfit != null && Number(buyPrice) > 0
      ? round2((Number(rawProfit) / Number(buyPrice)) * 100)
      : null
  const referencePrice = resolveReferencePrice(item, quotes, buyPrice, sellNet)
  const quoteAgeMinutes = toFiniteOrNull(item?.maxQuoteAgeMinutes)

  const liquidityFilter = evaluateLiquidityFilter({
    buyQuote,
    sellQuote,
    quotes,
    item,
    itemCategory
  })
  const liquiditySample = resolveLiquiditySample(item, quotes)
  const depthGapSignals = resolveDepthGapSignals({
    buyOutlierRatio: buyCandidate?.buyOutlierRatio,
    sellOutlierRatio: sellCandidate?.sellOutlierRatio,
    itemCategory
  })
  const referenceDeviation = resolveReferenceDeviationSignal({
    buyPrice,
    sellNet,
    referencePrice,
    itemCategory
  })

  const depthFlags = resolveDepthFlags({
    buyOutlierAdjusted: Boolean(buyCandidate?.buyOutlierAdjusted),
    sellOutlierAdjusted: Boolean(sellCandidate?.sellOutlierAdjusted),
    buyDepthMissing: Boolean(buyCandidate?.buyDepthMissing),
    sellDepthMissing: Boolean(sellCandidate?.sellDepthMissing),
    buyDepthGapSuspicious: depthGapSignals.buyDepthGapSuspicious,
    sellDepthGapSuspicious: depthGapSignals.sellDepthGapSuspicious,
    buyDepthGapExtreme: depthGapSignals.buyDepthGapExtreme,
    sellDepthGapExtreme: depthGapSignals.sellDepthGapExtreme
  })
  const depthConfidenceScore = getDepthConfidenceScore({
    buyOutlierAdjusted: Boolean(buyCandidate?.buyOutlierAdjusted),
    sellOutlierAdjusted: Boolean(sellCandidate?.sellOutlierAdjusted),
    buyDepthMissing: Boolean(buyCandidate?.buyDepthMissing),
    sellDepthMissing: Boolean(sellCandidate?.sellDepthMissing),
    buyDepthGapSuspicious: depthGapSignals.buyDepthGapSuspicious,
    sellDepthGapSuspicious: depthGapSignals.sellDepthGapSuspicious,
    buyDepthGapExtreme: depthGapSignals.buyDepthGapExtreme,
    sellDepthGapExtreme: depthGapSignals.sellDepthGapExtreme,
    itemCategory
  })
  const reasons = new Set()
  if (!buyCandidate || !sellCandidate || buyPrice == null || sellNet == null) {
    reasons.add("insufficient_market_data")
  }
  if (marketCoverage < MIN_MARKET_COVERAGE) {
    reasons.add("ignored_missing_markets")
  }
  if (buyPrice != null && Number(buyPrice) < minExecutionPriceUsd) {
    reasons.add("ignored_low_price")
  }
  if (profit == null || Number(profit) <= 0) {
    reasons.add("non_positive_profit")
  }
  if (spreadPercent == null || Number(spreadPercent) < Number(minSpreadPercent)) {
    reasons.add("spread_below_min")
  }
  if (spreadPercent != null && Number(spreadPercent) > Number(spreadRules.max || SPREAD_SANITY_MAX_PERCENT)) {
    reasons.add("ignored_extreme_spread")
  }
  if (!liquidityFilter.passed) {
    reasons.add("ignored_low_liquidity")
  }
  if (premiumCategory) {
    if (depthGapSignals.buyDepthGapExtreme || depthGapSignals.sellDepthGapExtreme) {
      reasons.add("ignored_missing_depth")
    }
    if (referenceDeviation.extreme) {
      reasons.add("ignored_reference_deviation")
    }
    if (liquidityFilter.unknown) {
      const liquidityRules = getCategoryLiquidityRules(itemCategory)
      if (marketCoverage < Number(liquidityRules.unknownMinMarkets || PREMIUM_UNKNOWN_VOLUME_MIN_MARKETS)) {
        reasons.add("ignored_missing_markets")
      }
      const weakDepthSupport =
        Boolean(buyCandidate?.buyDepthMissing) ||
        Boolean(sellCandidate?.sellDepthMissing) ||
        Boolean(buyCandidate?.buyOutlierAdjusted) ||
        Boolean(sellCandidate?.sellOutlierAdjusted) ||
        Boolean(depthGapSignals.buyDepthGapSuspicious) ||
        Boolean(depthGapSignals.sellDepthGapSuspicious)
      if (weakDepthSupport) {
        reasons.add("ignored_missing_depth")
      }
      if (referenceDeviation.strong) {
        reasons.add("ignored_reference_deviation")
      }
    }
  } else if (
    isReferenceDeviation(buyPrice, referencePrice, itemCategory) ||
    isReferenceDeviation(sellNet, referencePrice, itemCategory)
  ) {
    reasons.add("ignored_reference_deviation")
  }

  const spreadScore = getSpreadScore(spreadPercent, itemCategory)
  const liquidityScoreBundle = computeLiquidityScoreForRanking(liquidityFilter, itemCategory)
  const liquidityScore = Number(liquidityScoreBundle.score || 0)
  const stabilityScore = getStabilityScore(sevenDayChangePercent)
  const marketScore = getMarketScore(buyQuote?.market, sellQuote?.market)
  const scoreWeights = getCategoryScoreWeights(itemCategory)
  const weightedScore = round2(
    spreadScore * Number(scoreWeights.spread || 0) +
      liquidityScore * Number(scoreWeights.liquidity || 0) +
      stabilityScore * Number(scoreWeights.stability || 0) +
      marketScore * Number(scoreWeights.market || 0) +
      depthConfidenceScore * Number(scoreWeights.depth || 0)
  )
  const suspiciousSpreadPenalty =
    spreadPercent != null &&
    spreadPercent > Number(spreadRules.suspiciousPenaltyThreshold || SPREAD_SUSPICIOUS_PENALTY_THRESHOLD) &&
    spreadPercent <= Number(spreadRules.max || SPREAD_SANITY_MAX_PERCENT)
      ? premiumCategory
        ? 22
        : 18
      : 0
  const premiumReferencePenalty =
    premiumCategory && referenceDeviation.strong && !referenceDeviation.extreme ? 18 : 0
  const premiumDepthPenalty =
    premiumCategory &&
    (
      depthGapSignals.buyDepthGapExtreme ||
      depthGapSignals.sellDepthGapExtreme ||
      buyCandidate?.buyDepthMissing ||
      sellCandidate?.sellDepthMissing ||
      buyCandidate?.buyOutlierAdjusted ||
      sellCandidate?.sellOutlierAdjusted ||
      depthGapSignals.buyDepthGapSuspicious ||
      depthGapSignals.sellDepthGapSuspicious
    )
      ? (
          depthGapSignals.buyDepthGapExtreme || depthGapSignals.sellDepthGapExtreme
            ? 30
            : buyCandidate?.buyDepthMissing || sellCandidate?.sellDepthMissing
              ? 26
              : 14
        )
      : 0
  const premiumUnknownLiquidityPenalty = premiumCategory && liquidityFilter.unknown ? 18 : 0
  const opportunityScore = clampScore(
    weightedScore -
      Number(liquidityScoreBundle.penalty || 0) -
      Number(suspiciousSpreadPenalty || 0) -
      Number(premiumReferencePenalty || 0) -
      Number(premiumDepthPenalty || 0) -
      Number(premiumUnknownLiquidityPenalty || 0)
  )
  const scoreCategory = categorizeOpportunityScore(opportunityScore)
  const reasonList = Array.from(reasons)
  const isOpportunity = !reasonList.length
  const executionConfidence = resolveExecutionConfidence({
    volume7d: liquidityFilter.signalValue,
    spreadPercent,
    marketScore,
    marketCoverage,
    depthFlags,
    quoteAgeMinutes,
    snapshotStale: Boolean(item?.snapshotStale),
    itemCategory,
    referenceDeviation
  })
  const reasonBadges = buildReasonBadges({
    liquidityBand: liquidityFilter.band,
    depthFlags,
    marketScore,
    executionConfidence
  })

  const debugFlags = []
  if (buyCandidate?.buyOutlierAdjusted) debugFlags.push("adjusted_buy_outlier")
  if (sellCandidate?.sellOutlierAdjusted) debugFlags.push("adjusted_sell_outlier")

  return {
    itemId,
    itemName,
    itemCategory,
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
    executionConfidence,
    liquidityBand: liquidityFilter.band,
    depthFlags,
    reasonBadges,
    marketCoverage,
    referencePrice,
    isOpportunity,
    liquiditySample,
    sevenDayChangePercent,
    buyUrl: buyQuote?.url || null,
    sellUrl: sellQuote?.url || null,
    antiFake: {
      passed: isOpportunity,
      filteredOut: !isOpportunity,
      reasons: reasonList,
      reasonLabels: formatFilterReasons(reasonList),
      debugReasons: debugFlags,
      liquidity: liquidityFilter,
      referenceDeviation,
      filters: {
        minExecutionPriceUsd,
        minSpreadPercent,
        spreadSuspiciousPenaltyThreshold: Number(
          spreadRules.suspiciousPenaltyThreshold || SPREAD_SUSPICIOUS_PENALTY_THRESHOLD
        ),
        spreadSanityMaxPercent: Number(spreadRules.max || SPREAD_SANITY_MAX_PERCENT),
        minMarketCoverage: MIN_MARKET_COVERAGE,
        referenceDeviationRatioMax: getReferenceDeviationRatioMax(itemCategory)
      },
      depthQuality: {
        buyDepthGapSuspicious: depthGapSignals.buyDepthGapSuspicious,
        sellDepthGapSuspicious: depthGapSignals.sellDepthGapSuspicious,
        buyDepthGapExtreme: depthGapSignals.buyDepthGapExtreme,
        sellDepthGapExtreme: depthGapSignals.sellDepthGapExtreme
      },
      outlier: {
        buyAdjusted: Boolean(buyCandidate?.buyOutlierAdjusted),
        sellAdjusted: Boolean(sellCandidate?.sellOutlierAdjusted),
        buyOutlierRatio: toFiniteOrNull(buyCandidate?.buyOutlierRatio),
        sellOutlierRatio: toFiniteOrNull(sellCandidate?.sellOutlierRatio),
        buyDepthMissing: Boolean(buyCandidate?.buyDepthMissing),
        sellDepthMissing: Boolean(sellCandidate?.sellDepthMissing)
      }
    },
    scores: {
      spreadScore,
      liquidityScore,
      stabilityScore,
      marketScore,
      depthConfidenceScore,
      liquidityPenalty: Number(liquidityScoreBundle.penalty || 0),
      suspiciousSpreadPenalty,
      referenceDeviationPenalty: premiumReferencePenalty,
      depthPenalty: premiumDepthPenalty,
      unknownLiquidityPenalty: premiumUnknownLiquidityPenalty,
      scoreWeights
    },
    debug: {
      rawQuotesByMarket: normalizedQuotes?.byMarket || {}
    }
  }
}

function normalizeSortBy(sortBy) {
  const safe = String(sortBy || "score")
    .trim()
    .toLowerCase()
  if (safe === "spread") return "spread"
  if (safe === "profit") return "profit"
  return "score"
}

function normalizeMarketSet(marketsInput) {
  if (Array.isArray(marketsInput)) {
    return new Set(
      marketsInput
        .map((value) => normalizeMarket(value))
        .filter(Boolean)
    )
  }
  const raw = String(marketsInput || "").trim()
  if (!raw || raw === "all") return new Set()
  return new Set(
    raw
      .split(",")
      .map((value) => normalizeMarket(value))
      .filter(Boolean)
  )
}

function rankOpportunities(opportunities = [], options = {}) {
  const includeRisky = Boolean(options.includeRisky || options.showRisky)
  const minProfit = toFiniteOrNull(options.minProfit) ?? DEFAULT_MIN_PROFIT_ABSOLUTE
  const minProfitBuyPercent =
    toFiniteOrNull(options.minProfitBuyPercent) ?? DEFAULT_MIN_PROFIT_BUY_PERCENT
  const minSpreadPercent =
    toFiniteOrNull(options.minSpreadPercent) != null
      ? Number(options.minSpreadPercent)
      : MIN_SPREAD_PERCENT
  const minScore =
    toFiniteOrNull(options.minScore) != null
      ? Number(options.minScore)
      : includeRisky
        ? RISKY_SCORE_CUTOFF
        : DEFAULT_SCORE_CUTOFF
  const liquidityMin = toFiniteOrNull(options.liquidityMin) ?? 0
  const limit = Number.isFinite(Number(options.limit)) ? Math.max(Number(options.limit), 0) : 0
  const sortBy = normalizeSortBy(options.sortBy)
  const marketSet = normalizeMarketSet(options.markets || options.market)

  const filtered = (Array.isArray(opportunities) ? opportunities : [])
    .filter((row) => row && row.isOpportunity)
    .filter((row) => {
      const buyPrice = toFiniteOrNull(row?.buyPrice ?? row?.buy?.price)
      const absoluteMin = Number(minProfit || 0)
      const relativeMin =
        buyPrice != null && buyPrice > 0
          ? Number(buyPrice) * (Number(minProfitBuyPercent || 0) / 100)
          : 0
      const effectiveProfitFloor = Math.max(absoluteMin, relativeMin)
      return Number(row.profit || 0) >= effectiveProfitFloor
    })
    .filter((row) => Number(row.spreadPercent || 0) >= minSpreadPercent)
    .filter((row) => Number(row.opportunityScore || 0) >= minScore)
    .filter((row) => {
      if (liquidityMin <= 0) return true
      return Number(row.liquiditySample || 0) >= liquidityMin
    })
    .filter((row) => {
      if (!marketSet.size) return true
      return marketSet.has(normalizeMarket(row.buyMarket)) || marketSet.has(normalizeMarket(row.sellMarket))
    })

  filtered.sort((a, b) => {
    if (sortBy === "profit") {
      return (
        Number(b.profit || 0) - Number(a.profit || 0) ||
        Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
        Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0)
      )
    }
    if (sortBy === "spread") {
      return (
        Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0) ||
        Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
        Number(b.profit || 0) - Number(a.profit || 0)
      )
    }
    return (
      Number(b.opportunityScore || 0) - Number(a.opportunityScore || 0) ||
      Number(b.profit || 0) - Number(a.profit || 0) ||
      Number(b.spreadPercent || 0) - Number(a.spreadPercent || 0)
    )
  })

  if (limit > 0) {
    return filtered.slice(0, limit)
  }
  return filtered
}

module.exports = {
  MIN_EXECUTION_PRICE_USD,
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
    computeLiquidityScoreForRanking,
    resolveReferencePrice,
    isReferenceDeviation,
    getDepthConfidenceScore,
    resolveExecutionConfidence
  }
}

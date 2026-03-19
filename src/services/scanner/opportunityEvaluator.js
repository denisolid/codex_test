const { roundPrice, round2 } = require("../../markets/marketUtils")
const {
  CATEGORY_PROFILES,
  ITEM_CATEGORIES,
  OPPORTUNITY_TIERS,
  PENALTY_WEIGHTS
} = require("./config")
const { normalizeCategory } = require("./stateModel")

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function clampScore(value) {
  const numeric = toFiniteOrNull(value)
  if (numeric == null) return 0
  return Math.min(Math.max(round2(numeric), 0), 100)
}

function toArray(value) {
  return Array.isArray(value) ? value : []
}

function confidenceLevel(value) {
  const normalized = normalizeText(value).toLowerCase()
  if (normalized === "high") return 3
  if (normalized === "medium") return 2
  return 1
}

function confidenceLabel(level) {
  if (level >= 3) return "High"
  if (level >= 2) return "Medium"
  return "Low"
}

function resolveCategoryProfile(category = ITEM_CATEGORIES.WEAPON_SKIN) {
  return CATEGORY_PROFILES[category] || CATEGORY_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function countAvailableMarkets(perMarket = []) {
  return toArray(perMarket).filter((row) => Boolean(row?.available)).length
}

function resolveVolume7d(comparedItem = {}, candidate = {}) {
  const fromArbitrage = toFiniteOrNull(comparedItem?.arbitrage?.liquiditySample)
  if (fromArbitrage != null && fromArbitrage >= 0) return fromArbitrage
  const perMarketMax = toArray(comparedItem?.perMarket)
    .map((row) => toFiniteOrNull(row?.volume7d ?? row?.volume_7d))
    .filter((value) => value != null && value >= 0)
    .sort((a, b) => b - a)[0]
  if (perMarketMax != null) return perMarketMax
  const fromCandidate = toFiniteOrNull(candidate?.volume7d)
  if (fromCandidate != null && fromCandidate >= 0) return fromCandidate
  return null
}

function resolveQuoteAgeMinutes(comparedItem = {}, candidate = {}) {
  const timestamps = [
    candidate?.snapshotCapturedAt,
    candidate?.quoteFetchedAt,
    ...toArray(comparedItem?.perMarket).map((row) => row?.updatedAt)
  ]
    .map((value) => normalizeText(value))
    .filter(Boolean)
    .map((value) => new Date(value).getTime())
    .filter((value) => Number.isFinite(value))
  if (!timestamps.length) return null
  const latest = Math.max(...timestamps)
  const ageMinutes = (Date.now() - latest) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return Number(ageMinutes.toFixed(2))
}

function hasExtremeReferenceDeviation({
  buyPrice = null,
  sellNet = null,
  referencePrice = null,
  ratioLimit = 2.7
} = {}) {
  const reference = toFiniteOrNull(referencePrice)
  if (reference == null || reference <= 0) {
    return {
      extreme: false,
      ratio: null
    }
  }
  const candidates = [toFiniteOrNull(buyPrice), toFiniteOrNull(sellNet)].filter(
    (value) => value != null && value > 0
  )
  if (!candidates.length) {
    return {
      extreme: false,
      ratio: null
    }
  }
  const maxRatio = candidates
    .map((value) => Math.max(value / reference, reference / value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => b - a)[0]
  return {
    extreme: Boolean(maxRatio && maxRatio > Number(ratioLimit || 0)),
    ratio: maxRatio ? Number(maxRatio.toFixed(3)) : null
  }
}

function hasFakeOrderbookSignals(comparedItem = {}) {
  const depthFlags = toArray(comparedItem?.arbitrage?.depthFlags).map((value) =>
    normalizeText(value).toUpperCase()
  )
  if (
    depthFlags.includes("BUY_DEPTH_GAP_EXTREME") ||
    depthFlags.includes("SELL_DEPTH_GAP_EXTREME")
  ) {
    return true
  }
  const antiFakeReasons = toArray(comparedItem?.arbitrage?.antiFake?.reasons).map((value) =>
    normalizeText(value).toLowerCase()
  )
  if (antiFakeReasons.includes("ignored_missing_depth")) {
    return true
  }
  return false
}

function buildPenaltySet({
  categoryProfile,
  candidate = {},
  comparedItem = {},
  buyPrice = null,
  profit = null,
  spreadPercent = null,
  marketCoverage = 0,
  referenceDeviation = {},
  volume7d = null,
  quoteAgeMinutes = null
} = {}) {
  const penalties = new Set(toArray(candidate.scanPenaltyFlags))

  if (spreadPercent != null && spreadPercent < Number(categoryProfile.minSpreadPercent || 0)) {
    penalties.add("low_spread")
  }
  if (buyPrice != null && buyPrice < Number(categoryProfile.minPriceUsd || 0)) {
    penalties.add("low_price")
  }
  if (profit != null && profit < Number(categoryProfile.minProfitUsd || 0)) {
    penalties.add("low_profit")
  }
  if (marketCoverage < Number(categoryProfile.minMarketCoverage || 2)) {
    penalties.add("weak_coverage")
  }
  if (volume7d == null || volume7d < Number(categoryProfile.minVolume7d || 0)) {
    penalties.add("missing_liquidity")
  }
  const depthFlags = toArray(comparedItem?.arbitrage?.depthFlags).map((value) =>
    normalizeText(value).toUpperCase()
  )
  if (
    depthFlags.includes("MISSING_DEPTH") ||
    depthFlags.includes("BUY_DEPTH_GAP_SUSPICIOUS") ||
    depthFlags.includes("SELL_DEPTH_GAP_SUSPICIOUS") ||
    depthFlags.includes("BUY_OUTLIER_ADJUSTED") ||
    depthFlags.includes("SELL_OUTLIER_ADJUSTED")
  ) {
    penalties.add("weak_depth")
  }
  if (quoteAgeMinutes == null || quoteAgeMinutes > 90 || candidate.scanFreshness?.state === "stale") {
    penalties.add("aging_data")
  }
  if (referenceDeviation.ratio != null && referenceDeviation.ratio > 1.8) {
    penalties.add("reference_deviation_warning")
  }

  return Array.from(penalties)
}

function computePenaltyScore(penalties = []) {
  return toArray(penalties).reduce((sum, flag) => {
    return sum + Number(PENALTY_WEIGHTS[flag] || 5)
  }, 0)
}

function resolveTier({
  profile = {},
  finalScore = 0,
  confidence = "Low",
  profit = 0,
  hardReject = false
} = {}) {
  if (hardReject) return OPPORTUNITY_TIERS.REJECTED
  const confidenceLvl = confidenceLevel(confidence)
  if (
    finalScore >= Number(profile.strongScoreFloor || 0) &&
    confidenceLvl >= 2 &&
    profit >= Number(profile.minProfitUsd || 0)
  ) {
    return OPPORTUNITY_TIERS.STRONG
  }
  if (finalScore >= Number(profile.riskyScoreFloor || 0) && profit > 0) {
    return OPPORTUNITY_TIERS.RISKY
  }
  if (profit > 0 && finalScore >= Number(profile.speculativeScoreFloor || 0)) {
    return OPPORTUNITY_TIERS.SPECULATIVE
  }
  if (profit > 0) {
    return OPPORTUNITY_TIERS.SPECULATIVE
  }
  return OPPORTUNITY_TIERS.REJECTED
}

function resolveLiquidityBand(volume7d, profile = {}) {
  const value = toFiniteOrNull(volume7d)
  if (value == null) return "Low"
  const minFloor = Number(profile.minVolume7d || 1)
  if (value >= minFloor * 3) return "High"
  if (value >= minFloor * 1.5) return "Medium"
  if (value >= minFloor) return "Low"
  return "Low"
}

function resolveBaseMetrics(comparedItem = {}, candidate = {}) {
  const arbitrage = comparedItem?.arbitrage || {}
  const bestBuy = comparedItem?.bestBuy || {}
  const bestSell = comparedItem?.bestSellNet || {}

  const buyPrice = toFiniteOrNull(arbitrage.buyPrice ?? bestBuy.grossPrice)
  const sellNet = toFiniteOrNull(arbitrage.sellNet ?? bestSell.netPriceAfterFees)
  const profit =
    toFiniteOrNull(arbitrage.profit) ??
    (buyPrice != null && sellNet != null ? roundPrice(sellNet - buyPrice) : null)
  const spreadPercent =
    toFiniteOrNull(arbitrage.spreadPercent ?? arbitrage.spread_pct) ??
    (buyPrice != null && buyPrice > 0 && profit != null
      ? round2((profit / buyPrice) * 100)
      : null)
  const marketCoverage =
    Math.max(
      Number(toFiniteOrNull(arbitrage.marketCoverage) || 0),
      countAvailableMarkets(comparedItem?.perMarket)
    ) || 0
  const referencePrice =
    toFiniteOrNull(arbitrage.referencePrice) ?? toFiniteOrNull(candidate.referencePrice)
  const baseScore =
    toFiniteOrNull(arbitrage.opportunityScore) ??
    (profit != null && spreadPercent != null
      ? clampScore(38 + spreadPercent * 1.2 + profit * 4)
      : 0)
  const executionConfidence = normalizeText(arbitrage.executionConfidence || "Low") || "Low"
  const buyMarket = normalizeText(arbitrage.buyMarket || bestBuy.source).toLowerCase()
  const sellMarket = normalizeText(arbitrage.sellMarket || bestSell.source).toLowerCase()
  const buyUrl = normalizeText(arbitrage.buyUrl || bestBuy.url) || null
  const sellUrl = normalizeText(arbitrage.sellUrl || bestSell.url) || null

  return {
    buyPrice,
    sellNet,
    profit,
    spreadPercent,
    marketCoverage,
    referencePrice,
    baseScore,
    executionConfidence,
    buyMarket,
    sellMarket,
    buyUrl,
    sellUrl,
    antiFakeReasons: toArray(arbitrage?.antiFake?.reasons),
    depthFlags: toArray(arbitrage?.depthFlags)
  }
}

function evaluateCandidateOpportunity(candidate = {}, comparedItem = {}) {
  const category = normalizeCategory(candidate.category, candidate.itemName)
  const profile = resolveCategoryProfile(category)
  const base = resolveBaseMetrics(comparedItem, candidate)
  const volume7d = resolveVolume7d(comparedItem, candidate)
  const quoteAgeMinutes = resolveQuoteAgeMinutes(comparedItem, candidate)
  const referenceDeviation = hasExtremeReferenceDeviation({
    buyPrice: base.buyPrice,
    sellNet: base.sellNet,
    referencePrice: base.referencePrice,
    ratioLimit: profile.referenceRejectRatio
  })

  const hardRejectReasons = []
  if (!base.buyMarket || !base.sellMarket || base.buyPrice == null || base.sellNet == null) {
    hardRejectReasons.push("broken_invalid_data")
  }
  if (base.profit == null || base.profit <= 0) {
    hardRejectReasons.push("non_positive_profit")
  }
  if (
    base.spreadPercent != null &&
    base.spreadPercent > Number(profile.hardSpreadMaxPercent || 9999)
  ) {
    hardRejectReasons.push("absurd_spread")
  }
  if (base.marketCoverage < Number(profile.minMarketCoverage || 2)) {
    hardRejectReasons.push("unusable_market_coverage")
  }
  if (referenceDeviation.extreme) {
    hardRejectReasons.push("extreme_reference_deviation")
  }
  if (hasFakeOrderbookSignals(comparedItem)) {
    hardRejectReasons.push("fake_orderbook_behavior")
  }
  if (
    base.antiFakeReasons.some(
      (reason) =>
        String(reason).toLowerCase() === "insufficient_market_data" ||
        String(reason).toLowerCase() === "ignored_missing_markets"
    )
  ) {
    hardRejectReasons.push("broken_invalid_data")
  }

  const penaltyFlags = buildPenaltySet({
    categoryProfile: profile,
    candidate,
    comparedItem,
    buyPrice: base.buyPrice,
    profit: base.profit,
    spreadPercent: base.spreadPercent,
    marketCoverage: base.marketCoverage,
    referenceDeviation,
    volume7d,
    quoteAgeMinutes
  })
  const penaltyScore = computePenaltyScore(penaltyFlags)
  const finalScore = clampScore(Number(base.baseScore || 0) - penaltyScore)

  let confidence = confidenceLevel(base.executionConfidence)
  if (penaltyFlags.includes("weak_depth")) confidence -= 1
  if (penaltyFlags.includes("missing_liquidity")) confidence -= 1
  if (penaltyFlags.includes("aging_data")) confidence -= 1
  confidence = Math.max(confidence, 1)
  if (hardRejectReasons.length) {
    confidence = 1
  }

  const tier = resolveTier({
    profile,
    finalScore,
    confidence: confidenceLabel(confidence),
    profit: Number(base.profit || 0),
    hardReject: hardRejectReasons.length > 0
  })

  const liquidityBand = resolveLiquidityBand(volume7d, profile)
  const badges = []
  if (tier === OPPORTUNITY_TIERS.STRONG) badges.push("Strong setup")
  if (tier === OPPORTUNITY_TIERS.RISKY) badges.push("Risk-adjusted")
  if (tier === OPPORTUNITY_TIERS.SPECULATIVE) badges.push("Speculative")
  if (penaltyFlags.includes("aging_data")) badges.push("Aging data")
  if (penaltyFlags.includes("weak_depth")) badges.push("Weak depth")
  if (penaltyFlags.includes("missing_liquidity")) badges.push("Missing liquidity")

  return {
    marketHashName: candidate.marketHashName,
    itemName: candidate.itemName,
    itemCategory: category,
    itemSubcategory: candidate.itemSubcategory || null,
    itemId: Number(comparedItem?.skinId || 0) || null,
    tier,
    rejected: tier === OPPORTUNITY_TIERS.REJECTED,
    hardRejectReasons,
    penaltyFlags,
    buyMarket: base.buyMarket || null,
    buyPrice: base.buyPrice == null ? null : roundPrice(base.buyPrice),
    sellMarket: base.sellMarket || null,
    sellNet: base.sellNet == null ? null : roundPrice(base.sellNet),
    profit: base.profit == null ? null : roundPrice(base.profit),
    spread: base.spreadPercent == null ? null : round2(base.spreadPercent),
    score: finalScore,
    executionConfidence: confidenceLabel(confidence),
    liquidity: volume7d == null ? null : round2(volume7d),
    liquidityBand,
    marketCoverage: Number(base.marketCoverage || 0),
    referencePrice: base.referencePrice == null ? null : roundPrice(base.referencePrice),
    buyUrl: base.buyUrl,
    sellUrl: base.sellUrl,
    itemImageUrl: null,
    itemRarity: null,
    itemRarityColor: null,
    scoreCategory:
      tier === OPPORTUNITY_TIERS.STRONG
        ? "Strong"
        : tier === OPPORTUNITY_TIERS.RISKY
          ? "Risky"
          : tier === OPPORTUNITY_TIERS.SPECULATIVE
            ? "Speculative"
            : "Rejected",
    qualityGrade: String(tier || OPPORTUNITY_TIERS.REJECTED).toUpperCase(),
    isHighConfidenceEligible: tier === OPPORTUNITY_TIERS.STRONG,
    isRiskyEligible: tier === OPPORTUNITY_TIERS.STRONG || tier === OPPORTUNITY_TIERS.RISKY,
    flags: Array.from(new Set([...penaltyFlags, ...hardRejectReasons])),
    badges: Array.from(new Set(badges)),
    metadata: {
      opportunity_tier: tier,
      penalty_flags: penaltyFlags,
      hard_reject_reasons: hardRejectReasons,
      penalty_score: penaltyScore,
      base_score: clampScore(base.baseScore),
      quote_age_minutes: quoteAgeMinutes,
      reference_deviation_ratio: referenceDeviation.ratio,
      anti_fake_reasons: base.antiFakeReasons,
      depth_flags: base.depthFlags
    }
  }
}

module.exports = {
  clampScore,
  confidenceLevel,
  confidenceLabel,
  hasExtremeReferenceDeviation,
  evaluateCandidateOpportunity
}

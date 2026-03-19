const { roundPrice, round2 } = require("../../markets/marketUtils")
const {
  CATEGORY_PROFILES,
  ITEM_CATEGORIES,
  OPPORTUNITY_TIERS,
  PENALTY_WEIGHTS
} = require("./config")
const { normalizeCategory } = require("./stateModel")
const MIN_SCAN_COST_USD = 2
const DIAGNOSTIC_FLAGS = Object.freeze({
  SALES_LIQUIDITY: "low_sales_liquidity",
  EXECUTABLE_DEPTH: "thin_executable_depth",
  MARKET_COVERAGE: "limited_market_coverage",
  DATA_FRESHNESS: "stale_market_signal"
})
const FRESHNESS_RULES_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({ freshMaxMinutes: 45, agingMaxMinutes: 120 }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({ freshMaxMinutes: 60, agingMaxMinutes: 180 }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({ freshMaxMinutes: 90, agingMaxMinutes: 240 }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({ freshMaxMinutes: 120, agingMaxMinutes: 300 }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({ freshMaxMinutes: 120, agingMaxMinutes: 300 })
})
const DEPTH_FLAG_SEVERITY = Object.freeze({
  MISSING_DEPTH: 2,
  BUY_DEPTH_GAP_EXTREME: 2,
  SELL_DEPTH_GAP_EXTREME: 2,
  BUY_DEPTH_GAP_SUSPICIOUS: 1,
  SELL_DEPTH_GAP_SUSPICIOUS: 1,
  BUY_OUTLIER_ADJUSTED: 1,
  SELL_OUTLIER_ADJUSTED: 1
})

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

function toIsoOrNull(value) {
  if (value == null || value === "") return null
  if (value instanceof Date) {
    const directTs = value.getTime()
    if (Number.isFinite(directTs)) return new Date(directTs).toISOString()
    return null
  }

  const numeric = toFiniteOrNull(value)
  if (numeric != null) {
    const normalizedTs =
      numeric >= 1e12
        ? Math.round(numeric)
        : numeric >= 1e9
          ? Math.round(numeric * 1000)
          : null
    if (normalizedTs != null) {
      const numericTs = new Date(normalizedTs).getTime()
      if (Number.isFinite(numericTs)) return new Date(numericTs).toISOString()
    }
  }

  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function normalizeSource(value) {
  return normalizeText(value).toLowerCase()
}

function isCaseLikeCategory(category = "") {
  return category === ITEM_CATEGORIES.CASE || category === ITEM_CATEGORIES.STICKER_CAPSULE
}

function isKnifeGloveCategory(category = "") {
  return category === ITEM_CATEGORIES.KNIFE || category === ITEM_CATEGORIES.GLOVE
}

function resolveFreshnessRules(category = ITEM_CATEGORIES.WEAPON_SKIN) {
  return (
    FRESHNESS_RULES_BY_CATEGORY[category] || FRESHNESS_RULES_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function resolveScoreBand(score = 0) {
  const value = Math.max(Number(score || 0), 0)
  if (value >= 80) return "strong"
  if (value >= 60) return "usable"
  if (value >= 40) return "weak"
  return "missing"
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

function resolveLatestIso(values = []) {
  const sorted = (Array.isArray(values) ? values : [])
    .map((value) => toIsoOrNull(value))
    .filter(Boolean)
    .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())
  return sorted[0] || null
}

function resolveReferenceSignalAt(candidate = {}, referencePrice = null, quoteIso = null, snapshotIso = null) {
  if (toFiniteOrNull(referencePrice) == null) return null

  const directReferenceIso = resolveLatestIso([
    candidate?.latestReferencePriceAt,
    candidate?.latest_reference_price_at,
    candidate?.referencePriceAt,
    candidate?.reference_price_at
  ])
  if (directReferenceIso) return directReferenceIso

  const referenceState = normalizeText(candidate?.referenceState || candidate?.reference_state).toLowerCase()
  if (referenceState === "snapshot") return snapshotIso
  if (referenceState === "quote") return quoteIso
  return resolveLatestIso([quoteIso, snapshotIso])
}

function resolveUsedSignalFreshness({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  comparedItem = {},
  candidate = {},
  buyMarket = "",
  sellMarket = "",
  referencePrice = null
} = {}) {
  const rules = resolveFreshnessRules(category)
  const perMarket = toArray(comparedItem?.perMarket)
  const usedSignals = []

  const pushSignal = (source, signalType, timestamp) => {
    const iso = toIsoOrNull(timestamp)
    if (!iso) return
    usedSignals.push({
      source: normalizeSource(source) || null,
      signalType: normalizeText(signalType) || "market_signal",
      updatedAt: iso
    })
  }

  const findMarketRow = (source) => {
    const normalizedSource = normalizeSource(source)
    if (!normalizedSource) return null
    return (
      perMarket.find(
        (row) =>
          normalizeSource(row?.source) === normalizedSource &&
          Boolean(row?.available) &&
          (toFiniteOrNull(row?.grossPrice) != null || toFiniteOrNull(row?.netPriceAfterFees) != null)
      ) || null
    )
  }

  const buyRow = findMarketRow(buyMarket)
  const sellRow = findMarketRow(sellMarket)
  pushSignal(buyMarket, "buy_market_quote", buyRow?.updatedAt || buyRow?.updated_at || buyRow?.fetched_at)
  pushSignal(
    sellMarket,
    "sell_market_quote",
    sellRow?.updatedAt || sellRow?.updated_at || sellRow?.fetched_at
  )

  const latestQuoteAt = resolveLatestIso([
    candidate?.latestQuoteAt,
    candidate?.latest_quote_at,
    candidate?.quoteFetchedAt,
    candidate?.quote_fetched_at,
    candidate?.scanFreshness?.latestQuoteAt,
    candidate?.scanFreshness?.latest_quote_at,
    comparedItem?.latestQuoteAt,
    comparedItem?.latest_quote_at
  ])
  const latestSnapshotAt = resolveLatestIso([
    candidate?.latestSnapshotAt,
    candidate?.latest_snapshot_at,
    candidate?.snapshotCapturedAt,
    candidate?.snapshot_captured_at,
    candidate?.scanFreshness?.latestSnapshotAt,
    candidate?.scanFreshness?.latest_snapshot_at,
    comparedItem?.latestSnapshotAt,
    comparedItem?.latest_snapshot_at
  ])
  const latestReferencePriceAt = resolveReferenceSignalAt(
    {
      ...candidate,
      latestReferencePriceAt:
        candidate?.latestReferencePriceAt ||
        candidate?.latest_reference_price_at ||
        candidate?.scanFreshness?.latestReferencePriceAt ||
        candidate?.scanFreshness?.latest_reference_price_at ||
        comparedItem?.latestReferencePriceAt ||
        comparedItem?.latest_reference_price_at,
      referenceState: candidate?.referenceState || candidate?.reference_state
    },
    referencePrice,
    latestQuoteAt,
    latestSnapshotAt
  )

  pushSignal("catalog", "latest_quote", latestQuoteAt)
  pushSignal("catalog", "latest_snapshot", latestSnapshotAt)
  pushSignal("reference", "latest_reference_price", latestReferencePriceAt)

  if (!usedSignals.length) {
    pushSignal(
      "catalog",
      "catalog_last_market_signal_fallback",
      candidate?.lastMarketSignalAt ||
        candidate?.last_market_signal_at ||
        candidate?.scanFreshness?.latestMarketSignalAt ||
        candidate?.scanFreshness?.latest_market_signal_at ||
        candidate?.latestMarketSignalAt ||
        candidate?.latest_market_signal_at
    )
  }

  const sortedSignals = usedSignals
    .slice()
    .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
  const timestamps = sortedSignals
    .map((signal) => new Date(signal.updatedAt).getTime())
    .filter((value) => Number.isFinite(value))
  if (!timestamps.length) {
    return {
      latestQuoteAt,
      latestSnapshotAt,
      latestReferencePriceAt,
      latestSignalAt: null,
      latestMarketSignalAt: null,
      ageMinutes: null,
      staleThresholdMinutes: Number(rules.agingMaxMinutes || 0),
      staleResult: true,
      staleReasonSource: "no_usable_market_timestamp",
      hasFreshSignalWithinSla: false,
      usedSignals: sortedSignals
    }
  }
  const latestTs = Math.max(...timestamps)
  const ageMinutesRaw = (Date.now() - latestTs) / (60 * 1000)
  const ageMinutes =
    Number.isFinite(ageMinutesRaw) && ageMinutesRaw >= 0
      ? Number(ageMinutesRaw.toFixed(2))
      : null
  const staleThresholdMinutes = Number(rules.agingMaxMinutes || 0)
  const hasFreshSignalWithinSla =
    ageMinutes != null &&
    staleThresholdMinutes > 0 &&
    ageMinutes <= staleThresholdMinutes
  const latestSignal = sortedSignals[0] || null
  return {
    latestQuoteAt,
    latestSnapshotAt,
    latestReferencePriceAt,
    latestSignalAt: new Date(latestTs).toISOString(),
    latestMarketSignalAt: new Date(latestTs).toISOString(),
    ageMinutes,
    staleThresholdMinutes,
    staleResult: !hasFreshSignalWithinSla,
    staleReasonSource: latestSignal
      ? `${normalizeText(latestSignal.signalType)}${latestSignal.source ? `:${latestSignal.source}` : ""}`
      : "no_usable_market_timestamp",
    hasFreshSignalWithinSla,
    usedSignals: sortedSignals
  }
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

function scoreSalesLiquidity({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  categoryProfile = {},
  volume7d = null,
  marketCoverage = 0
} = {}) {
  const value = toFiniteOrNull(volume7d)
  const minVolume = Math.max(Number(categoryProfile.minVolume7d || 0), 1)
  const reasons = []
  let score = 0

  if (value == null || value <= 0) {
    score = isKnifeGloveCategory(category) ? 42 : 28
    reasons.push("missing_recent_sales_signal")
  } else if (isCaseLikeCategory(category)) {
    if (value >= minVolume * 8) score = 96
    else if (value >= minVolume * 4) score = 88
    else if (value >= minVolume * 2) score = 76
    else if (value >= minVolume) score = 62
    else if (value >= Math.max(minVolume * 0.5, 8)) score = 48
    else score = 30
    reasons.push(value >= minVolume * 2 ? "strong_recent_sales_velocity" : "soft_recent_sales_velocity")
  } else if (isKnifeGloveCategory(category)) {
    if (value >= minVolume * 3) score = 88
    else if (value >= minVolume * 1.5) score = 74
    else if (value >= minVolume) score = 64
    else if (value >= Math.max(minVolume * 0.5, 3)) score = 54
    else score = 42
    reasons.push(value >= minVolume ? "sparse_but_usable_sales_signal" : "thin_sales_signal")
  } else {
    if (value >= minVolume * 4) score = 92
    else if (value >= minVolume * 2) score = 80
    else if (value >= minVolume) score = 66
    else if (value >= Math.max(minVolume * 0.6, 10)) score = 48
    else score = 32
    reasons.push(value >= minVolume ? "usable_recent_sales_signal" : "weak_recent_sales_signal")
  }

  if (Number(marketCoverage || 0) >= Number(categoryProfile.minMarketCoverage || 2) + 1) {
    score += 6
    reasons.push("cross_market_sales_supported")
  }

  return {
    score: clampScore(score),
    band: resolveScoreBand(score),
    reasons
  }
}

function scoreMarketCoverage({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  categoryProfile = {},
  marketCoverage = 0
} = {}) {
  const minCoverage = Math.max(Number(categoryProfile.minMarketCoverage || 2), 1)
  const coverage = Math.max(Number(marketCoverage || 0), 0)
  const reasons = []
  let score = 0

  if (coverage >= minCoverage + 2) {
    score = 95
    reasons.push("high_cross_market_presence")
  } else if (coverage >= minCoverage + 1) {
    score = 84
    reasons.push("strong_cross_market_presence")
  } else if (coverage >= minCoverage) {
    score = 70
    reasons.push("meets_cross_market_baseline")
  } else if (coverage >= 1) {
    score = isKnifeGloveCategory(category) ? 58 : 42
    reasons.push("single_market_presence")
  } else {
    score = 20
    reasons.push("no_market_presence")
  }

  return {
    score: clampScore(score),
    band: resolveScoreBand(score),
    reasons
  }
}

function scoreExecutableDepth({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  comparedItem = {},
  marketCoverage = 0,
  salesLiquidityScore = 0
} = {}) {
  const depthFlags = toArray(comparedItem?.arbitrage?.depthFlags).map((value) =>
    normalizeText(value).toUpperCase()
  )
  const reasons = []
  const isCaseLike = isCaseLikeCategory(category)
  const isSparse = isKnifeGloveCategory(category)
  let score = isCaseLike ? 68 : isSparse ? 64 : 76

  if (Number(marketCoverage || 0) >= 4) {
    score += 8
    reasons.push("multi_market_depth_context")
  }
  if (isCaseLike && Number(salesLiquidityScore || 0) >= 80) {
    score += 8
    reasons.push("sales_velocity_offsets_depth_noise")
  }

  for (const flag of depthFlags) {
    const severity = Number(DEPTH_FLAG_SEVERITY[flag] || 0)
    if (!severity) continue
    const penalty =
      severity >= 2
        ? isCaseLike
          ? 18
          : isSparse
            ? 16
            : 26
        : isCaseLike
          ? 10
          : isSparse
            ? 9
            : 14
    score -= penalty
    reasons.push(flag.toLowerCase())
  }

  return {
    score: clampScore(score),
    band: resolveScoreBand(score),
    reasons
  }
}

function scoreDataFreshness({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  freshness = {}
} = {}) {
  const rules = resolveFreshnessRules(category)
  const ageMinutes = toFiniteOrNull(freshness?.ageMinutes)
  const staleThresholdMinutes = Number(
    toFiniteOrNull(freshness?.staleThresholdMinutes) ?? rules.agingMaxMinutes
  )
  const hasFreshSignalWithinSla =
    ageMinutes != null &&
    staleThresholdMinutes > 0 &&
    ageMinutes <= staleThresholdMinutes
  const reasons = []
  let score = 0
  let state = "missing"

  if (ageMinutes == null) {
    score = 24
    state = "missing"
    reasons.push("no_usable_market_timestamp")
  } else if (hasFreshSignalWithinSla && ageMinutes <= Number(rules.freshMaxMinutes || 0)) {
    score = 96
    state = "fresh"
    reasons.push("fresh_usable_market_signal")
  } else if (hasFreshSignalWithinSla) {
    score = 66
    state = "aging"
    reasons.push("aging_usable_market_signal")
  } else if (ageMinutes <= staleThresholdMinutes * 2) {
    score = 42
    state = "stale"
    reasons.push("stale_market_signal")
  } else {
    score = 18
    state = "stale"
    reasons.push("very_stale_market_signal")
  }

  return {
    score: clampScore(score),
    band: resolveScoreBand(score),
    state,
    reasons
  }
}

function buildDiagnosticDimensions({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  categoryProfile = {},
  comparedItem = {},
  volume7d = null,
  marketCoverage = 0,
  usedSignalFreshness = {}
} = {}) {
  const salesLiquidity = scoreSalesLiquidity({
    category,
    categoryProfile,
    volume7d,
    marketCoverage
  })
  const marketCoverageDimension = scoreMarketCoverage({
    category,
    categoryProfile,
    marketCoverage
  })
  const executableDepth = scoreExecutableDepth({
    category,
    comparedItem,
    marketCoverage,
    salesLiquidityScore: salesLiquidity.score
  })
  const dataFreshness = scoreDataFreshness({
    category,
    freshness: usedSignalFreshness
  })
  const freshnessAgeMinutes = toFiniteOrNull(usedSignalFreshness?.ageMinutes)

  return {
    sales_liquidity: salesLiquidity,
    executable_depth: executableDepth,
    market_coverage: marketCoverageDimension,
    data_freshness: {
      ...dataFreshness,
      ageMinutes: toFiniteOrNull(usedSignalFreshness?.ageMinutes),
      latestSignalAt:
        usedSignalFreshness?.latestMarketSignalAt || usedSignalFreshness?.latestSignalAt || null,
      latestMarketSignalAt: usedSignalFreshness?.latestMarketSignalAt || null,
      latestQuoteAt: usedSignalFreshness?.latestQuoteAt || null,
      latestSnapshotAt: usedSignalFreshness?.latestSnapshotAt || null,
      latestReferencePriceAt: usedSignalFreshness?.latestReferencePriceAt || null,
      staleThresholdMinutes:
        toFiniteOrNull(usedSignalFreshness?.staleThresholdMinutes) ??
        Number(resolveFreshnessRules(category).agingMaxMinutes || 0),
      staleResult:
        usedSignalFreshness?.staleResult == null
          ? freshnessAgeMinutes == null
          : Boolean(usedSignalFreshness?.staleResult),
      staleReasonSource: normalizeText(usedSignalFreshness?.staleReasonSource) || "no_usable_market_timestamp",
      hasFreshSignalWithinSla: Boolean(usedSignalFreshness?.hasFreshSignalWithinSla),
      usedSignals: toArray(usedSignalFreshness?.usedSignals)
    }
  }
}

function buildPenaltySet({
  categoryProfile,
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  comparedItem = {},
  buyPrice = null,
  profit = null,
  spreadPercent = null,
  referenceDeviation = {},
  diagnostics = {}
} = {}) {
  const penalties = new Set()

  if (spreadPercent != null && spreadPercent < Number(categoryProfile.minSpreadPercent || 0)) {
    penalties.add("low_spread")
  }
  if (buyPrice != null && buyPrice < Number(categoryProfile.minPriceUsd || 0)) {
    penalties.add("low_price")
  }
  if (profit != null && profit < Number(categoryProfile.minProfitUsd || 0)) {
    penalties.add("low_profit")
  }
  if (referenceDeviation.ratio != null && referenceDeviation.ratio > 1.8) {
    penalties.add("reference_deviation_warning")
  }

  const salesLiquidityScore = Number(diagnostics?.sales_liquidity?.score || 0)
  const executableDepthScore = Number(diagnostics?.executable_depth?.score || 0)
  const marketCoverageScore = Number(diagnostics?.market_coverage?.score || 0)
  const dataFreshnessScore = Number(diagnostics?.data_freshness?.score || 0)
  const hasFreshSignalWithinSla = Boolean(diagnostics?.data_freshness?.hasFreshSignalWithinSla)

  if (salesLiquidityScore < (isKnifeGloveCategory(category) ? 38 : 45)) {
    penalties.add(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
  }
  if (marketCoverageScore < (isKnifeGloveCategory(category) ? 40 : 45)) {
    penalties.add(DIAGNOSTIC_FLAGS.MARKET_COVERAGE)
  }
  if (executableDepthScore < 45) {
    penalties.add(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH)
  }
  if (dataFreshnessScore < 55 && !hasFreshSignalWithinSla) {
    penalties.add(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)
  }

  if (salesLiquidityScore >= 70) penalties.delete(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
  if (marketCoverageScore >= 70) penalties.delete(DIAGNOSTIC_FLAGS.MARKET_COVERAGE)
  if (dataFreshnessScore >= 70) penalties.delete(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)
  if (hasFreshSignalWithinSla) penalties.delete(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)
  if (executableDepthScore >= 65) penalties.delete(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH)

  if (isCaseLikeCategory(category) && salesLiquidityScore >= 70) {
    penalties.delete(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
  }

  return Array.from(penalties)
}

function computePenaltyScore(penalties = []) {
  const localWeights = {
    [DIAGNOSTIC_FLAGS.SALES_LIQUIDITY]: 13,
    [DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH]: 10,
    [DIAGNOSTIC_FLAGS.MARKET_COVERAGE]: 11,
    [DIAGNOSTIC_FLAGS.DATA_FRESHNESS]: 9
  }
  return toArray(penalties).reduce((sum, flag) => {
    return sum + Number(localWeights[flag] ?? PENALTY_WEIGHTS[flag] ?? 5)
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

function resolveCoverageBand(score = 0) {
  const value = Number(score || 0)
  if (value >= 80) return "High"
  if (value >= 60) return "Medium"
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
  const usedSignalFreshness = resolveUsedSignalFreshness({
    category,
    comparedItem,
    candidate,
    buyMarket: base.buyMarket,
    sellMarket: base.sellMarket,
    referencePrice: base.referencePrice
  })
  const diagnostics = buildDiagnosticDimensions({
    category,
    categoryProfile: profile,
    comparedItem,
    volume7d,
    marketCoverage: base.marketCoverage,
    usedSignalFreshness
  })
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
  if (
    (base.buyPrice != null && base.buyPrice < MIN_SCAN_COST_USD) ||
    (base.referencePrice != null && base.referencePrice < MIN_SCAN_COST_USD)
  ) {
    hardRejectReasons.push("below_min_cost_floor")
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
    category,
    comparedItem,
    buyPrice: base.buyPrice,
    profit: base.profit,
    spreadPercent: base.spreadPercent,
    referenceDeviation,
    diagnostics
  })
  const penaltyScore = computePenaltyScore(penaltyFlags)
  const finalScore = clampScore(Number(base.baseScore || 0) - penaltyScore)

  let confidence = confidenceLevel(base.executionConfidence)
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH)) confidence -= 1
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)) confidence -= 1
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)) confidence -= 1
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
  const marketCoverageBand = resolveCoverageBand(diagnostics?.market_coverage?.score)
  const badges = []
  if (tier === OPPORTUNITY_TIERS.STRONG) badges.push("Strong setup")
  if (tier === OPPORTUNITY_TIERS.RISKY) badges.push("Risk-adjusted")
  if (tier === OPPORTUNITY_TIERS.SPECULATIVE) badges.push("Speculative")
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)) badges.push("Stale market signal")
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH)) {
    badges.push("Thin executable depth")
  }
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)) {
    badges.push(
      isCaseLikeCategory(category) ? "Low recent sales velocity" : "Low sales liquidity"
    )
  }
  if (penaltyFlags.includes(DIAGNOSTIC_FLAGS.MARKET_COVERAGE)) {
    badges.push("Limited market coverage")
  }

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
    marketCoverageBand,
    marketCoverageLabel: marketCoverageBand,
    marketCoverage: Number(base.marketCoverage || 0),
    latestMarketSignalAt: usedSignalFreshness?.latestMarketSignalAt || null,
    latestQuoteAt: usedSignalFreshness?.latestQuoteAt || null,
    latestSnapshotAt: usedSignalFreshness?.latestSnapshotAt || null,
    latestReferencePriceAt: usedSignalFreshness?.latestReferencePriceAt || null,
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
      quote_age_minutes: toFiniteOrNull(usedSignalFreshness?.ageMinutes),
      latest_market_signal_at: usedSignalFreshness?.latestMarketSignalAt || null,
      latest_quote_at: usedSignalFreshness?.latestQuoteAt || null,
      latest_snapshot_at: usedSignalFreshness?.latestSnapshotAt || null,
      latest_reference_price_at: usedSignalFreshness?.latestReferencePriceAt || null,
      stale_threshold_used:
        toFiniteOrNull(usedSignalFreshness?.staleThresholdMinutes) ??
        Number(resolveFreshnessRules(category).agingMaxMinutes || 0),
      stale_result:
        usedSignalFreshness?.staleResult == null ? null : Boolean(usedSignalFreshness?.staleResult),
      stale_reason_source: normalizeText(usedSignalFreshness?.staleReasonSource) || null,
      reference_deviation_ratio: referenceDeviation.ratio,
      anti_fake_reasons: base.antiFakeReasons,
      depth_flags: base.depthFlags,
      diagnostics_debug: {
        category,
        latest_quote_at: usedSignalFreshness?.latestQuoteAt || null,
        latest_snapshot_at: usedSignalFreshness?.latestSnapshotAt || null,
        latest_reference_price_at: usedSignalFreshness?.latestReferencePriceAt || null,
        sales_liquidity_score: Number(diagnostics?.sales_liquidity?.score || 0),
        executable_depth_score: Number(diagnostics?.executable_depth?.score || 0),
        market_coverage_score: Number(diagnostics?.market_coverage?.score || 0),
        data_freshness_score: Number(diagnostics?.data_freshness?.score || 0),
        market_coverage_band: marketCoverageBand,
        latest_market_signal_at:
          usedSignalFreshness?.latestMarketSignalAt || usedSignalFreshness?.latestSignalAt || null,
        stale_threshold_used:
          toFiniteOrNull(usedSignalFreshness?.staleThresholdMinutes) ??
          Number(resolveFreshnessRules(category).agingMaxMinutes || 0),
        stale_result:
          usedSignalFreshness?.staleResult == null ? null : Boolean(usedSignalFreshness?.staleResult),
        stale_reason_source:
          normalizeText(usedSignalFreshness?.staleReasonSource) || "no_usable_market_timestamp",
        used_market_signals: toArray(usedSignalFreshness?.usedSignals),
        raw_reasons: {
          sales_liquidity: toArray(diagnostics?.sales_liquidity?.reasons),
          executable_depth: toArray(diagnostics?.executable_depth?.reasons),
          market_coverage: toArray(diagnostics?.market_coverage?.reasons),
          data_freshness: toArray(diagnostics?.data_freshness?.reasons),
          emitted_tags: penaltyFlags
        }
      }
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

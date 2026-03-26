const { OPPORTUNITY_TIERS } = require("./config")
const {
  evaluatePublishValidation,
  buildPublishValidationPreview,
  buildFreshnessContractDiagnostics
} = require("./publishValidation")

const MIN_SCAN_COST_USD = 2
const DIAGNOSTIC_FLAGS = Object.freeze({
  SALES_LIQUIDITY: "low_sales_liquidity",
  EXECUTABLE_DEPTH: "thin_executable_depth",
  MARKET_COVERAGE: "limited_market_coverage",
  DATA_FRESHNESS: "stale_market_signal"
})
const HARD_DEPTH_FLAGS = new Set(["BUY_DEPTH_GAP_EXTREME", "SELL_DEPTH_GAP_EXTREME"])
const SOFT_DEPTH_FLAGS = new Set([
  "MISSING_DEPTH",
  "BUY_DEPTH_GAP_SUSPICIOUS",
  "SELL_DEPTH_GAP_SUSPICIOUS",
  "BUY_OUTLIER_ADJUSTED",
  "SELL_OUTLIER_ADJUSTED"
])

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toArray(value) {
  return Array.isArray(value) ? value : []
}

function toIsoOrNull(value) {
  if (value == null || value === "") return null
  const ts = new Date(value).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function computeAgeMinutes(value) {
  const iso = toIsoOrNull(value)
  if (!iso) return null
  const age = (Date.now() - new Date(iso).getTime()) / (60 * 1000)
  if (!Number.isFinite(age) || age < 0) return null
  return Number(age.toFixed(2))
}

function buildEvaluationDisposition({ tier, hardRejectReasons = [], softRejectReasons = [] } = {}) {
  if (Array.isArray(hardRejectReasons) && hardRejectReasons.length) return "hard_reject"
  if (Array.isArray(softRejectReasons) && softRejectReasons.length) return "soft_skip"
  if (tier === OPPORTUNITY_TIERS.STRONG) return "strong_eligible"
  if (tier === OPPORTUNITY_TIERS.RISKY || tier === OPPORTUNITY_TIERS.SPECULATIVE) {
    return "risky_eligible"
  }
  return "soft_skip"
}

function pushReason(target, value) {
  const safeValue = normalizeText(value)
  if (!safeValue) return
  if (!target.includes(safeValue)) target.push(safeValue)
}

function hasMissingListingState(value = "") {
  return normalizeText(value).toLowerCase().startsWith("missing_")
}

function bandRank(value = "") {
  const normalized = normalizeText(value).toLowerCase()
  if (normalized === "high") return 3
  if (normalized === "medium") return 2
  return 1
}

function minBand(first = "Low", second = "Low") {
  return bandRank(first) <= bandRank(second) ? first : second
}

function resolveWeaponSkinTier({
  profile = {},
  finalScore = 0,
  confidenceLevel = 1,
  profit = 0,
  penaltyFlags = [],
  riskLabels = [],
  hardRejectReasons = [],
  softRejectReasons = []
} = {}) {
  if (
    (Array.isArray(hardRejectReasons) && hardRejectReasons.length) ||
    (Array.isArray(softRejectReasons) && softRejectReasons.length)
  ) {
    return OPPORTUNITY_TIERS.REJECTED
  }

  const safeProfit = Number(profit || 0)
  if (safeProfit <= 0) return OPPORTUNITY_TIERS.REJECTED

  const hasRiskSurface =
    (Array.isArray(riskLabels) && riskLabels.length > 0) ||
    (Array.isArray(penaltyFlags) && penaltyFlags.length > 0)

  if (
    finalScore >= Number(profile.strongScoreFloor || 0) &&
    confidenceLevel >= 2 &&
    safeProfit >= Number(profile.minProfitUsd || 0) &&
    !hasRiskSurface
  ) {
    return OPPORTUNITY_TIERS.STRONG
  }
  if (finalScore >= Number(profile.riskyScoreFloor || 0)) {
    return OPPORTUNITY_TIERS.RISKY
  }
  if (finalScore >= Number(profile.speculativeScoreFloor || 0)) {
    return OPPORTUNITY_TIERS.SPECULATIVE
  }
  return OPPORTUNITY_TIERS.REJECTED
}

function evaluateWeaponSkinOpportunity(options = {}) {
  const {
    candidate = {},
    profile = {},
    base = {},
    routeContext = {},
    diagnostics = {},
    referenceDeviation = {},
    liquiditySignal = {},
    usedSignalFreshness = {},
    initialPenaltyFlags = [],
    computePenaltyScore,
    clampScore,
    confidenceLevel,
    confidenceLabel,
    defaultLiquidityBand = "Low"
  } = options

  const hardRejectReasons = []
  const softRejectReasons = []
  const penaltyFlags = new Set(toArray(initialPenaltyFlags).map((value) => normalizeText(value)).filter(Boolean))
  const riskLabels = new Set()
  const additionalBadges = new Set()

  const antiFakeReasons = toArray(base?.antiFakeReasons).map((value) =>
    normalizeText(value).toLowerCase()
  )
  const depthFlags = toArray(base?.depthFlags).map((value) => normalizeText(value).toUpperCase())
  const marketCoverage = Math.max(Number(base?.marketCoverage || 0), 0)
  const salesLiquidityScore = Number(diagnostics?.sales_liquidity?.score || 0)
  const dataFreshnessState = normalizeText(diagnostics?.data_freshness?.state).toLowerCase()

  const publishValidation = evaluatePublishValidation({
    buyMarket: base?.buyMarket,
    sellMarket: base?.sellMarket,
    buyRouteAvailable: routeContext?.buyRouteAvailable,
    sellRouteAvailable: routeContext?.sellRouteAvailable,
    buyRouteUpdatedAt: routeContext?.buyRouteUpdatedAt,
    sellRouteUpdatedAt: routeContext?.sellRouteUpdatedAt,
    buyListingAvailable: routeContext?.buyListingAvailable,
    sellListingAvailable: routeContext?.sellListingAvailable
  })
  const publishValidationPreview = buildPublishValidationPreview(publishValidation)
  const freshnessContractDiagnostics = buildFreshnessContractDiagnostics(
    routeContext,
    publishValidation
  )

  const catalogStatus = normalizeText(
    candidate?.catalogStatus ?? candidate?.catalog_status ?? candidate?.raw?.catalog_status
  ).toLowerCase()
  if (catalogStatus && catalogStatus !== "scannable") {
    pushReason(hardRejectReasons, "catalog_not_scannable")
  }

  if (!base?.buyMarket || !base?.sellMarket || base?.buyPrice == null || base?.sellNet == null) {
    pushReason(hardRejectReasons, "broken_invalid_data")
  }
  if (
    (base?.buyPrice != null && Number(base.buyPrice) < MIN_SCAN_COST_USD) ||
    (base?.referencePrice != null && Number(base.referencePrice) < MIN_SCAN_COST_USD)
  ) {
    pushReason(hardRejectReasons, "below_min_cost_floor")
  }
  if (base?.profit == null || Number(base.profit) <= 0) {
    pushReason(hardRejectReasons, "non_positive_profit")
  }
  if (
    base?.spreadPercent != null &&
    Number(base.spreadPercent) > Number(profile?.hardSpreadMaxPercent || 9999)
  ) {
    pushReason(hardRejectReasons, "absurd_spread")
  }
  if (marketCoverage <= 0) {
    pushReason(hardRejectReasons, "unusable_market_coverage")
  }
  if (Boolean(referenceDeviation?.extreme) || antiFakeReasons.includes("ignored_reference_deviation")) {
    pushReason(hardRejectReasons, "extreme_reference_deviation")
  }
  if (antiFakeReasons.includes("insufficient_market_data")) {
    pushReason(hardRejectReasons, "broken_invalid_data")
  }
  if (antiFakeReasons.includes("ignored_extreme_spread")) {
    pushReason(hardRejectReasons, "absurd_spread")
  }
  if (depthFlags.some((value) => HARD_DEPTH_FLAGS.has(value))) {
    pushReason(hardRejectReasons, "fake_orderbook_behavior")
  }
  if (routeContext?.requiredRouteState && routeContext.requiredRouteState !== "ready") {
    pushReason(hardRejectReasons, routeContext.requiredRouteState)
  }
  if (!publishValidation.isPublishable) {
    if (publishValidation.requiredRouteState !== "ready") {
      pushReason(hardRejectReasons, publishValidation.requiredRouteState)
    } else if (hasMissingListingState(publishValidation.listingAvailabilityState)) {
      pushReason(hardRejectReasons, publishValidation.listingAvailabilityState)
    } else {
      pushReason(
        hardRejectReasons,
        normalizeText(publishValidation.staleReason) || "publish_freshness_failed"
      )
    }
  }

  const directRouteLiquiditySignals = [
    toFiniteOrNull(liquiditySignal?.sellVolume7d),
    toFiniteOrNull(liquiditySignal?.buyVolume7d)
  ].filter((value) => value != null)
  const fallbackLiquiditySignals = [
    toFiniteOrNull(liquiditySignal?.marketMaxVolume7d),
    toFiniteOrNull(liquiditySignal?.arbitrageSampleVolume7d),
    toFiniteOrNull(liquiditySignal?.candidateVolume7d)
  ].filter((value) => value != null)
  const hasAnyLiquiditySupport =
    directRouteLiquiditySignals.length > 0 || fallbackLiquiditySignals.length > 0
  const fallbackOnlyLiquidity =
    directRouteLiquiditySignals.length === 0 && fallbackLiquiditySignals.length > 0

  if (!hasAnyLiquiditySupport) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
    riskLabels.add("thin_sales_liquidity")
  } else if (fallbackOnlyLiquidity) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
    riskLabels.add("derived_liquidity_support")
  } else if (
    liquiditySignal?.sellVolume7d == null ||
    liquiditySignal?.buyVolume7d == null
  ) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
  } else if (salesLiquidityScore < 45) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)
    riskLabels.add("thin_sales_liquidity")
  }

  if (marketCoverage > 0 && marketCoverage < Number(profile?.minMarketCoverage || 2)) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.MARKET_COVERAGE)
    riskLabels.add("partial_market_coverage")
  } else if (antiFakeReasons.includes("ignored_missing_markets") && marketCoverage > 0) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.MARKET_COVERAGE)
    riskLabels.add("partial_market_coverage")
  }

  const staleThresholdMinutes = Math.max(
    Number(usedSignalFreshness?.staleThresholdMinutes || 0),
    1
  )
  const supportingSignalAges = [
    computeAgeMinutes(usedSignalFreshness?.latestQuoteAt),
    computeAgeMinutes(usedSignalFreshness?.latestSnapshotAt),
    computeAgeMinutes(usedSignalFreshness?.latestReferencePriceAt)
  ]
  const staleSupportingInputs = supportingSignalAges.filter(
    (value) => value != null && value > staleThresholdMinutes
  ).length
  const missingSupportingInputs = supportingSignalAges.filter((value) => value == null).length

  if (Boolean(usedSignalFreshness?.staleResult) && publishValidation.isPublishable) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)
    riskLabels.add("stale_supporting_signal")
  } else if (
    publishValidation.isPublishable &&
    (staleSupportingInputs >= 2 || (staleSupportingInputs >= 1 && missingSupportingInputs >= 1))
  ) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)
    riskLabels.add("stale_supporting_signal")
  } else if (publishValidation.isPublishable && staleSupportingInputs >= 1) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)
  } else if (dataFreshnessState === "aging") {
    riskLabels.add("aging_supporting_signal")
  }

  if (
    antiFakeReasons.includes("ignored_missing_depth") ||
    depthFlags.some((value) => SOFT_DEPTH_FLAGS.has(value))
  ) {
    penaltyFlags.add(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH)
    riskLabels.add("missing_executable_depth")
  }

  const lowValueContext =
    (base?.buyPrice != null && Number(base.buyPrice) < Number(profile?.minPriceUsd || 0)) ||
    (base?.profit != null && Number(base.profit) < Number(profile?.minProfitUsd || 0))
  if (lowValueContext) {
    riskLabels.add("low_value_context")
  }

  const resolvePenaltyScore =
    typeof computePenaltyScore === "function"
      ? computePenaltyScore
      : (penalties = []) => (Array.isArray(penalties) ? penalties.length * 5 : 0)
  const applyClampScore =
    typeof clampScore === "function"
      ? clampScore
      : (value) => Math.min(Math.max(Number(value || 0), 0), 100)
  const resolveConfidenceLevel =
    typeof confidenceLevel === "function"
      ? confidenceLevel
      : (value) => {
          const normalized = normalizeText(value).toLowerCase()
          if (normalized === "high") return 3
          if (normalized === "medium") return 2
          return 1
        }
  const resolveConfidenceLabel =
    typeof confidenceLabel === "function"
      ? confidenceLabel
      : (value) => {
          if (value >= 3) return "High"
          if (value >= 2) return "Medium"
          return "Low"
        }

  const penaltyFlagsArray = Array.from(penaltyFlags)
  const penaltyScore =
    Number(resolvePenaltyScore(penaltyFlagsArray) || 0) +
    (riskLabels.has("aging_supporting_signal") ? 4 : 0)
  const finalScore = applyClampScore(Number(base?.baseScore || 0) - penaltyScore)

  if (
    !hardRejectReasons.length &&
    lowValueContext &&
    (riskLabels.has("partial_market_coverage") ||
      riskLabels.has("thin_sales_liquidity") ||
      riskLabels.has("derived_liquidity_support") ||
      riskLabels.has("stale_supporting_signal") ||
      riskLabels.has("missing_executable_depth")) &&
    finalScore < Math.max(Number(profile?.riskyScoreFloor || 0), 50)
  ) {
    pushReason(softRejectReasons, "low_value_low_support_weapon_skin")
  }
  if (
    !hardRejectReasons.length &&
    !hasAnyLiquiditySupport &&
    marketCoverage <= 1 &&
    finalScore < Math.max(Number(profile?.speculativeScoreFloor || 0), 32)
  ) {
    pushReason(softRejectReasons, "missing_liquidity_low_support_weapon_skin")
  }
  if (
    !hardRejectReasons.length &&
    !softRejectReasons.length &&
    Number(base?.profit || 0) > 0 &&
    finalScore < Number(profile?.speculativeScoreFloor || 0)
  ) {
    pushReason(softRejectReasons, "below_weapon_skin_quality_floor")
  }

  let confidence = resolveConfidenceLevel(base?.executionConfidence || "Low")
  if (penaltyFlags.has(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH)) confidence -= 1
  if (penaltyFlags.has(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)) confidence -= 1
  if (penaltyFlags.has(DIAGNOSTIC_FLAGS.DATA_FRESHNESS)) confidence -= 1
  if (penaltyFlags.has(DIAGNOSTIC_FLAGS.MARKET_COVERAGE)) confidence -= 1
  if (riskLabels.has("low_value_context")) confidence = Math.min(confidence, 2)
  if (riskLabels.size >= 3) confidence = Math.min(confidence, 1)
  confidence = Math.max(confidence, 1)
  if (hardRejectReasons.length || softRejectReasons.length) {
    confidence = 1
  }

  const tier = resolveWeaponSkinTier({
    profile,
    finalScore,
    confidenceLevel: confidence,
    profit: Number(base?.profit || 0),
    penaltyFlags: penaltyFlagsArray,
    riskLabels: Array.from(riskLabels),
    hardRejectReasons,
    softRejectReasons
  })
  const riskLabelsArray = Array.from(riskLabels)
  const outcomeDiagnostics = {
    missing_liquidity_penalty: penaltyFlagsArray.includes(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY),
    partial_market_coverage_penalty:
      penaltyFlagsArray.includes(DIAGNOSTIC_FLAGS.MARKET_COVERAGE) ||
      riskLabels.has("partial_market_coverage"),
    stale_supporting_input_penalty:
      penaltyFlagsArray.includes(DIAGNOSTIC_FLAGS.DATA_FRESHNESS) ||
      riskLabels.has("stale_supporting_signal"),
    thin_executable_depth_penalty:
      penaltyFlagsArray.includes(DIAGNOSTIC_FLAGS.EXECUTABLE_DEPTH) ||
      riskLabels.has("missing_executable_depth"),
    low_value_contextual_penalty: riskLabels.has("low_value_context"),
    soft_skip_reason: softRejectReasons[0] || null,
    hard_reject_reason: hardRejectReasons[0] || null,
    publish_preview_result: publishValidationPreview.result_label,
    final_tier: tier
  }

  if (tier === OPPORTUNITY_TIERS.RISKY) {
    additionalBadges.add("Weapon-skin risk adjusted")
  }
  if (riskLabels.has("derived_liquidity_support")) {
    additionalBadges.add("Derived liquidity support")
  }
  if (riskLabels.has("stale_supporting_signal")) {
    additionalBadges.add("Stale support downgraded")
  }
  if (riskLabels.has("partial_market_coverage")) {
    additionalBadges.add("Partial coverage tolerated")
  }

  let liquidityBand = normalizeText(defaultLiquidityBand) || "Low"
  if (riskLabels.has("derived_liquidity_support")) {
    liquidityBand = minBand(liquidityBand, "Low")
  } else if (penaltyFlags.has(DIAGNOSTIC_FLAGS.SALES_LIQUIDITY)) {
    liquidityBand = minBand(liquidityBand, "Medium")
  }
  const evaluationDisposition = buildEvaluationDisposition({
    tier,
    hardRejectReasons,
    softRejectReasons
  })
  outcomeDiagnostics.outcome = evaluationDisposition

  return {
    hardRejectReasons,
    softRejectReasons,
    penaltyFlags: penaltyFlagsArray,
    riskLabels: riskLabelsArray,
    penaltyScore,
    finalScore,
    confidence: resolveConfidenceLabel(confidence),
    tier,
    rejected: tier === OPPORTUNITY_TIERS.REJECTED,
    liquidityBand,
    additionalBadges: Array.from(additionalBadges),
    evaluationDisposition,
    publishValidationPreview,
    weaponSkinOutcomeDiagnostics: outcomeDiagnostics,
    freshnessContractDiagnostics
  }
}

module.exports = {
  evaluateWeaponSkinOpportunity
}

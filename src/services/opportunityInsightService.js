const AppError = require("../utils/AppError")
const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const { mapFeedRowToApiRow } = require("./scanner/feedPipeline")
const { sourceFeePercent, round2, roundPrice } = require("../markets/marketUtils")
const { CATEGORY_PROFILES, ITEM_CATEGORIES } = require("./scanner/config")

const INSIGHT_CACHE_TTL_MS = 5 * 60 * 1000
const INSIGHT_CACHE_MAX_ITEMS = 3000
const PRECOMPUTE_MAX_BATCH = 200

const insightCache = new Map()

const FRESHNESS_BASELINE_MINUTES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 120,
  [ITEM_CATEGORIES.CASE]: 180,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 240,
  [ITEM_CATEGORIES.KNIFE]: 300,
  [ITEM_CATEGORIES.GLOVE]: 300
})

const CONFIDENCE_BASE = Object.freeze({
  high: 88,
  medium: 72,
  low: 56
})

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function clamp(value, min = 0, max = 100) {
  return Math.min(Math.max(Number(value || 0), min), max)
}

function clampScore(value) {
  return Math.round(clamp(value, 0, 100))
}

function normalizeBoolean(value) {
  if (typeof value === "boolean") return value
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return false
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on"
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function ageMinutesFromIso(iso, nowMs = Date.now()) {
  const safeIso = toIsoOrNull(iso)
  if (!safeIso) return null
  const ts = new Date(safeIso).getTime()
  if (!Number.isFinite(ts)) return null
  const age = (nowMs - ts) / (60 * 1000)
  if (!Number.isFinite(age) || age < 0) return null
  return age
}

function normalizeCategory(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return ITEM_CATEGORIES.WEAPON_SKIN
  if (raw === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (raw === ITEM_CATEGORIES.STICKER_CAPSULE) return ITEM_CATEGORIES.STICKER_CAPSULE
  if (raw === ITEM_CATEGORIES.KNIFE || raw === ITEM_CATEGORIES.FUTURE_KNIFE) return ITEM_CATEGORIES.KNIFE
  if (raw === ITEM_CATEGORIES.GLOVE || raw === ITEM_CATEGORIES.FUTURE_GLOVE) return ITEM_CATEGORIES.GLOVE
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function resolveCategoryProfile(category) {
  const normalized = normalizeCategory(category)
  return CATEGORY_PROFILES[normalized] || CATEGORY_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function uniqueStringList(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value).toLowerCase())
        .filter(Boolean)
    )
  )
}

function resolveConfidenceBase(executionConfidence) {
  const key = normalizeText(executionConfidence).toLowerCase()
  if (key === "high") return CONFIDENCE_BASE.high
  if (key === "medium") return CONFIDENCE_BASE.medium
  return CONFIDENCE_BASE.low
}

function deriveProfitMetrics(opportunity = {}) {
  const buyPrice = toFiniteOrNull(opportunity.buyPrice)
  const sellNet = toFiniteOrNull(opportunity.sellNet)
  const sellFeePct = sourceFeePercent(opportunity.sellMarket)
  const keepRatio = 1 - sellFeePct / 100
  const grossSell =
    sellNet != null && keepRatio > 0 ? roundPrice(sellNet / keepRatio) : null
  const netProfitUsd =
    toFiniteOrNull(opportunity.netProfitAfterFees ?? opportunity.net_profit_after_fees) ??
    toFiniteOrNull(opportunity.profit) ??
    (buyPrice != null && sellNet != null ? roundPrice(sellNet - buyPrice) : null)
  const grossProfitUsd =
    buyPrice != null && grossSell != null ? roundPrice(grossSell - buyPrice) : null
  const netProfitPct =
    buyPrice != null && buyPrice > 0 && netProfitUsd != null
      ? round2((netProfitUsd / buyPrice) * 100)
      : null
  const grossProfitPct =
    buyPrice != null && buyPrice > 0 && grossProfitUsd != null
      ? round2((grossProfitUsd / buyPrice) * 100)
      : null

  return {
    buyPrice,
    sellNet,
    sellFeePct: round2(sellFeePct),
    grossSell,
    grossProfitUsd,
    grossProfitPct,
    netProfitUsd,
    netProfitPct
  }
}

function computeLiquidityScore(opportunity = {}, profile = {}) {
  const volume7d = toFiniteOrNull(opportunity.volume7d ?? opportunity.liquidity)
  const marketCoverage = Math.max(Number(opportunity.marketCoverage || 0), 0)
  const minVolume = Math.max(Number(profile.minVolume7d || 1), 1)
  const minCoverage = Math.max(Number(profile.minMarketCoverage || 2), 1)
  const ratio = volume7d == null ? 0 : volume7d / minVolume

  let volumeScore = 26
  if (ratio >= 4) volumeScore = 96
  else if (ratio >= 2) volumeScore = 84
  else if (ratio >= 1) volumeScore = 70
  else if (ratio >= 0.5) volumeScore = 55
  else if (ratio > 0) volumeScore = 42

  let coverageScore = 24
  if (marketCoverage >= minCoverage + 2) coverageScore = 94
  else if (marketCoverage >= minCoverage + 1) coverageScore = 84
  else if (marketCoverage >= minCoverage) coverageScore = 72
  else if (marketCoverage >= 1) coverageScore = 50

  const score = clampScore(volumeScore * 0.72 + coverageScore * 0.28)
  const summary =
    volume7d == null
      ? "No recent volume signal; liquidity relies on baseline coverage only."
      : `Volume ${round2(volume7d)} (min ${minVolume}) across ${marketCoverage} market(s).`

  return {
    score,
    volume7d,
    marketCoverage,
    minVolume,
    minCoverage,
    summary
  }
}

function computeFreshnessScore(opportunity = {}, category, nowMs = Date.now()) {
  const signalIso =
    toIsoOrNull(opportunity.latestMarketSignalAt) ||
    toIsoOrNull(opportunity.latestQuoteAt) ||
    toIsoOrNull(opportunity.latestSnapshotAt) ||
    toIsoOrNull(opportunity.detectedAt)
  const staleThresholdMinutes =
    toFiniteOrNull(opportunity.staleThresholdUsed) ??
    Number(
      FRESHNESS_BASELINE_MINUTES[normalizeCategory(category)] ||
        FRESHNESS_BASELINE_MINUTES[ITEM_CATEGORIES.WEAPON_SKIN]
    )
  const ageMinutes = ageMinutesFromIso(signalIso, nowMs)
  const staleFlag = opportunity.staleResult == null ? false : Boolean(opportunity.staleResult)
  let score = 22
  let state = "missing"

  if (ageMinutes != null && staleThresholdMinutes > 0) {
    const ratio = ageMinutes / staleThresholdMinutes
    if (ratio <= 0.5) {
      score = 96
      state = "fresh"
    } else if (ratio <= 1) {
      score = 78
      state = "usable"
    } else if (ratio <= 1.5) {
      score = 58
      state = "aging"
    } else if (ratio <= 2) {
      score = 40
      state = "stale"
    } else {
      score = 24
      state = "very_stale"
    }
  }

  if (staleFlag) score -= 12
  score = clampScore(score)

  const summary =
    ageMinutes == null
      ? "No reliable market timestamp found."
      : `Latest signal is ${round2(ageMinutes / 60)}h old (target <= ${round2(
        staleThresholdMinutes / 60
      )}h).`

  return {
    score,
    ageMinutes,
    staleThresholdMinutes,
    state,
    staleFlag,
    signalIso,
    summary
  }
}

function computeConfidenceScore(opportunity = {}, freshness = {}) {
  const base = resolveConfidenceBase(opportunity.executionConfidence)
  const scoreInput = toFiniteOrNull(opportunity.score) ?? 0
  const flags = uniqueStringList(opportunity.flags)
  let penalty = 0
  if (flags.includes("non_positive_profit")) penalty += 28
  if (flags.includes("broken_invalid_data")) penalty += 24
  if (flags.includes("fake_orderbook_behavior")) penalty += 24
  if (flags.includes("thin_executable_depth")) penalty += 11
  if (flags.includes("low_sales_liquidity")) penalty += 10
  if (flags.includes("limited_market_coverage")) penalty += 9
  if (flags.includes("stale_market_signal")) penalty += 8
  if (freshness.staleFlag) penalty += 6

  const score = clampScore(base * 0.45 + scoreInput * 0.4 + Number(freshness.score || 0) * 0.15 - penalty)
  const summary = `Execution ${normalizeText(opportunity.executionConfidence || "Low")} with scanner score ${clampScore(
    scoreInput
  )}/100.`

  return {
    score,
    base,
    scoreInput: clampScore(scoreInput),
    penalty,
    summary
  }
}

function estimateExitEtaHours({
  category,
  liquidityScore,
  confidenceScore,
  freshnessScore,
  volume7d
} = {}) {
  const normalizedCategory = normalizeCategory(category)
  let baseline = 32
  if (normalizedCategory === ITEM_CATEGORIES.CASE) baseline = 16
  if (normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE) baseline = 22
  if (normalizedCategory === ITEM_CATEGORIES.KNIFE || normalizedCategory === ITEM_CATEGORIES.GLOVE) baseline = 84

  let factor = 1
  if (liquidityScore >= 85) factor *= 0.55
  else if (liquidityScore >= 70) factor *= 0.75
  else if (liquidityScore < 35) factor *= 1.9
  else if (liquidityScore < 50) factor *= 1.45

  if (confidenceScore < 60) factor *= 1.2
  if (freshnessScore < 55) factor *= 1.2
  if (toFiniteOrNull(volume7d) != null && Number(volume7d) <= 2) factor *= 1.4

  return round2(clamp(baseline * factor, 2, 240))
}

function recommendPositionSizeUsd({
  category,
  buyPrice,
  confidenceScore,
  liquidityScore,
  freshnessScore,
  volume7d
} = {}) {
  if (buyPrice == null || buyPrice <= 0) return null
  const normalizedCategory = normalizeCategory(category)
  const conviction =
    (Number(confidenceScore || 0) * 0.4 +
      Number(liquidityScore || 0) * 0.35 +
      Number(freshnessScore || 0) * 0.25) /
    100

  let categoryMultiple = 1.4
  if (normalizedCategory === ITEM_CATEGORIES.CASE) categoryMultiple = 1.8
  if (normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE) categoryMultiple = 1.6
  if (normalizedCategory === ITEM_CATEGORIES.KNIFE || normalizedCategory === ITEM_CATEGORIES.GLOVE) {
    categoryMultiple = 0.95
  }

  const baseUsd = buyPrice * Math.max(conviction, 0.2) * categoryMultiple
  const minUsd = buyPrice * 0.8
  const maxUsd = buyPrice * (normalizedCategory === ITEM_CATEGORIES.KNIFE || normalizedCategory === ITEM_CATEGORIES.GLOVE ? 2.5 : 6)
  let resultUsd = clamp(baseUsd, minUsd, maxUsd)

  const volume = toFiniteOrNull(volume7d)
  if (volume != null && volume > 0) {
    const liquidityCapUnits = clamp(volume * 0.03, 1, 12)
    const liquidityCapUsd = buyPrice * liquidityCapUnits
    resultUsd = Math.min(resultUsd, liquidityCapUsd)
  }

  return round2(Math.max(resultUsd, buyPrice * 0.5))
}

function buildRiskFlags({
  opportunity = {},
  metrics = {},
  liquidity = {},
  freshness = {},
  confidence = {},
  profile = {}
} = {}) {
  const flags = new Set(uniqueStringList(opportunity.flags))
  const marketCoverage = Number(liquidity.marketCoverage || 0)
  const minCoverage = Number(profile.minMarketCoverage || 2)

  if ((metrics.netProfitUsd || 0) <= 0) flags.add("non_positive_net_profit")
  if ((metrics.netProfitPct || 0) < 2) flags.add("thin_net_margin")
  if (Number(liquidity.score || 0) < 55) flags.add("low_liquidity")
  if (Number(freshness.score || 0) < 55) flags.add("stale_signal")
  if (marketCoverage < minCoverage) flags.add("limited_market_coverage")
  if (Number(confidence.score || 0) < 60) flags.add("execution_uncertainty")
  if (Number(metrics.sellFeePct || 0) >= 12) flags.add("high_exit_fee_drag")

  return Array.from(flags)
}

function resolveVerdict({
  metrics = {},
  confidenceScore = 0,
  liquidityScore = 0,
  freshnessScore = 0,
  riskFlags = []
} = {}) {
  const flags = new Set(uniqueStringList(riskFlags))
  if (
    (metrics.netProfitUsd || 0) <= 0 ||
    flags.has("non_positive_net_profit") ||
    flags.has("broken_invalid_data") ||
    flags.has("fake_orderbook_behavior")
  ) {
    return "skip"
  }

  if (
    confidenceScore >= 82 &&
    liquidityScore >= 75 &&
    freshnessScore >= 72 &&
    (metrics.netProfitPct || 0) >= 6 &&
    (metrics.netProfitUsd || 0) >= 1
  ) {
    return "strong_buy"
  }

  if (
    confidenceScore >= 70 &&
    liquidityScore >= 60 &&
    freshnessScore >= 55 &&
    (metrics.netProfitPct || 0) >= 3 &&
    (metrics.netProfitUsd || 0) >= 0.5
  ) {
    return "good_small_size"
  }

  if (
    confidenceScore >= 58 &&
    liquidityScore >= 48 &&
    freshnessScore >= 45 &&
    (metrics.netProfitPct || 0) >= 1.5
  ) {
    return "watch"
  }

  if ((metrics.netProfitUsd || 0) > 0) {
    return "risky"
  }

  return "skip"
}

function classifyInsightReasons({
  verdict = "",
  confidenceScore = 0,
  freshnessScore = 0,
  liquidityScore = 0,
  netProfitPct = 0,
  exitEtaHours = null,
  riskFlags = []
} = {}) {
  const flags = new Set(uniqueStringList(riskFlags))
  const confidence = clampScore(confidenceScore)
  const freshness = clampScore(freshnessScore)
  const liquidity = clampScore(liquidityScore)
  const netPct = toFiniteOrNull(netProfitPct) ?? 0
  const eta = toFiniteOrNull(exitEtaHours)
  const safeVerdict = normalizeText(verdict).toLowerCase()

  const reasonState = {
    stale_signal: flags.has("stale_signal") || freshness <= 50,
    low_liquidity: flags.has("low_liquidity") || liquidity <= 52,
    weak_exit:
      (eta != null && eta >= 30) ||
      (liquidity <= 52 && eta != null && eta >= 24),
    execution_uncertainty: flags.has("execution_uncertainty") || confidence < 60,
    fee_drag: flags.has("high_exit_fee_drag") || (netPct > 0 && netPct < 2.5),
    strong_edge: netPct >= 6 && confidence >= 75 && freshness >= 68 && liquidity >= 68,
    healthy_market: freshness >= 72 && liquidity >= 72,
    mixed_setup: false
  }

  let priority = [
    "stale_signal",
    "low_liquidity",
    "weak_exit",
    "execution_uncertainty",
    "fee_drag",
    "mixed_setup"
  ]
  if (safeVerdict === "strong_buy") {
    priority = [
      "strong_edge",
      "healthy_market",
      "execution_uncertainty",
      "stale_signal",
      "low_liquidity",
      "weak_exit",
      "fee_drag",
      "mixed_setup"
    ]
  } else if (safeVerdict === "good_small_size") {
    priority = [
      "strong_edge",
      "healthy_market",
      "stale_signal",
      "low_liquidity",
      "weak_exit",
      "execution_uncertainty",
      "fee_drag",
      "mixed_setup"
    ]
  } else if (safeVerdict === "watch") {
    priority = [
      "stale_signal",
      "low_liquidity",
      "weak_exit",
      "execution_uncertainty",
      "fee_drag",
      "mixed_setup"
    ]
  } else if (safeVerdict === "skip") {
    priority = [
      "stale_signal",
      "execution_uncertainty",
      "low_liquidity",
      "weak_exit",
      "fee_drag",
      "mixed_setup"
    ]
  }

  let primaryReason = priority.find((reason) => reasonState[reason]) || "mixed_setup"

  const staleDominant = reasonState.stale_signal && freshness <= 52
  const liquidityDominant =
    (reasonState.low_liquidity || reasonState.weak_exit) &&
    (liquidity <= 52 || (eta != null && eta >= 30))

  if (staleDominant && safeVerdict !== "strong_buy") {
    primaryReason = "stale_signal"
  } else if (liquidityDominant && safeVerdict !== "strong_buy") {
    primaryReason = reasonState.low_liquidity ? "low_liquidity" : "weak_exit"
  }

  let secondaryReason =
    priority.find((reason) => reason !== primaryReason && reasonState[reason]) || null
  if (!secondaryReason) {
    secondaryReason =
      primaryReason === "strong_edge"
        ? "healthy_market"
        : primaryReason === "healthy_market"
          ? "strong_edge"
          : "mixed_setup"
  }

  return {
    primary_reason: primaryReason,
    secondary_reason: secondaryReason
  }
}

function describeInsightReason(reason, metrics = {}) {
  const confidence = clampScore(metrics.confidenceScore)
  const freshness = clampScore(metrics.freshnessScore)
  const liquidity = clampScore(metrics.liquidityScore)
  const netPct = toFiniteOrNull(metrics.netProfitPct)
  const eta = toFiniteOrNull(metrics.exitEtaHours)

  if (reason === "stale_signal") {
    return `stale signal (${freshness}/100 freshness)`
  }
  if (reason === "low_liquidity") {
    return `thin liquidity (${liquidity}/100 liquidity)`
  }
  if (reason === "weak_exit") {
    return eta == null ? "weak exit depth" : `weak exit depth (~${round2(eta)}h ETA)`
  }
  if (reason === "execution_uncertainty") {
    return `execution uncertainty (${confidence}/100 confidence)`
  }
  if (reason === "fee_drag") {
    return netPct == null ? "fee drag on net edge" : `fee drag on net edge (${round2(netPct)}% net)`
  }
  if (reason === "strong_edge") {
    return netPct == null ? "strong fee-adjusted edge" : `strong edge (${round2(netPct)}% net after fees)`
  }
  if (reason === "healthy_market") {
    return `healthy market (${liquidity}/100 liquidity, ${freshness}/100 freshness)`
  }
  return "mixed setup"
}

function resolveHeadlineStyle(verdict) {
  const safeVerdict = normalizeText(verdict).toLowerCase()
  if (safeVerdict === "strong_buy") return "positive"
  if (safeVerdict === "good_small_size") return "positive-soft"
  if (safeVerdict === "watch") return "warning"
  if (safeVerdict === "risky") return "danger-soft"
  return "danger"
}

function buildInsightHeadline(insight = {}) {
  try {
    const verdict = normalizeText(insight.verdict).toLowerCase()
    const confidenceScore = clampScore(insight.confidence_score)
    const freshnessScore = clampScore(insight.freshness_score)
    const liquidityScore = clampScore(insight.liquidity_score)
    const netProfitPct = toFiniteOrNull(insight.net_profit_pct_after_fees)
    const exitEtaHours = toFiniteOrNull(insight.exit_eta_hours)
    const riskFlags = Array.isArray(insight.risk_flags) ? insight.risk_flags : []

    const reasonClass = classifyInsightReasons({
      verdict,
      confidenceScore,
      freshnessScore,
      liquidityScore,
      netProfitPct,
      exitEtaHours,
      riskFlags
    })
    const primaryReason = reasonClass.primary_reason
    const secondaryReason = reasonClass.secondary_reason
    const primaryText = describeInsightReason(primaryReason, {
      confidenceScore,
      freshnessScore,
      liquidityScore,
      netProfitPct,
      exitEtaHours
    })
    const secondaryText = describeInsightReason(secondaryReason, {
      confidenceScore,
      freshnessScore,
      liquidityScore,
      netProfitPct,
      exitEtaHours
    })

    let headlineText = "Setup needs another validation pass before acting."
    if (verdict === "strong_buy") {
      if (primaryReason === "stale_signal") {
        headlineText = `Strong edge remains, but stale signal needs refresh first. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else if (primaryReason === "low_liquidity" || primaryReason === "weak_exit") {
        headlineText = `Strong edge with thin exit conditions. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else {
        headlineText = `Strong buy setup confirmed. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      }
    } else if (verdict === "good_small_size") {
      if (primaryReason === "stale_signal") {
        headlineText = `Good small-size setup, but stale signal is forming. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else if (primaryReason === "low_liquidity" || primaryReason === "weak_exit") {
        headlineText = `Good only at small size because exit is thin. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else {
        headlineText = `Good small-size setup with controlled risk. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      }
    } else if (verdict === "watch") {
      if (primaryReason === "stale_signal") {
        headlineText = `Watchlist candidate: stale signal is limiting conviction. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else if (primaryReason === "low_liquidity" || primaryReason === "weak_exit") {
        headlineText = `Watchlist candidate: liquidity and exit depth need improvement. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else {
        headlineText = `Watch setup: edge exists but needs cleaner confirmation. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      }
    } else if (verdict === "risky") {
      if (primaryReason === "stale_signal") {
        headlineText = `Risky setup: stale signal is dominant and may invalidate quoted edge. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else if (primaryReason === "low_liquidity" || primaryReason === "weak_exit") {
        headlineText = `Risky setup: thin liquidity points to a weak exit path. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else {
        headlineText = `Risky setup: protection is weak if execution drifts. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      }
    } else if (verdict === "skip") {
      if (primaryReason === "stale_signal") {
        headlineText = `Skip for now: stale signal dominance makes this non-actionable. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else if (primaryReason === "low_liquidity" || primaryReason === "weak_exit") {
        headlineText = `Skip for now: thin liquidity and weak exit quality collapse reliability. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      } else {
        headlineText = `Skip for now: risk-adjusted edge is not reliable. Primary: ${primaryText}. Secondary: ${secondaryText}.`
      }
    } else {
      headlineText = `Decision support is available, but verdict is unresolved. Primary: ${primaryText}. Secondary: ${secondaryText}.`
    }

    return {
      headline_text: headlineText,
      headline_style: resolveHeadlineStyle(verdict),
      primary_reason: primaryReason,
      secondary_reason: secondaryReason
    }
  } catch (err) {
    return {
      headline_text: "Insight headline unavailable due to incomplete deterministic inputs.",
      headline_style: "warning",
      primary_reason: "mixed_setup",
      secondary_reason: "mixed_setup"
    }
  }
}

function buildFailureConditions({
  opportunity = {},
  metrics = {},
  freshness = {},
  liquidity = {},
  profile = {}
} = {}) {
  const buyPrice = toFiniteOrNull(metrics.buyPrice)
  const netProfitUsd = toFiniteOrNull(metrics.netProfitUsd)
  const staleHours = round2(Number(freshness.staleThresholdMinutes || 0) / 60)
  const minCoverage = Number(profile.minMarketCoverage || 2)
  const minVolume = Number(profile.minVolume7d || 1)
  const failureConditions = []

  if (buyPrice != null && netProfitUsd != null) {
    const minNetSell = roundPrice(Math.max(buyPrice + Math.max(netProfitUsd * 0.35, 0.2), buyPrice))
    failureConditions.push(`Skip if expected sell net falls below $${minNetSell}.`)
  } else {
    failureConditions.push("Skip if fee-adjusted sell net no longer covers entry price.")
  }

  if (staleHours > 0) {
    failureConditions.push(`Skip if latest market signal age exceeds ${staleHours}h.`)
  }
  failureConditions.push(`Skip if market coverage drops below ${minCoverage} active market(s).`)
  failureConditions.push(`Reduce size if rolling 7d volume trends below ${minVolume}.`)
  failureConditions.push(
    `Skip if scanner confidence slips from ${normalizeText(opportunity.executionConfidence || "Low")} to Low with added execution warnings.`
  )

  if (Number(liquidity.score || 0) < 50) {
    failureConditions.push("Skip if spread widens but executed fills remain thin (depth mismatch risk).")
  }

  return failureConditions.slice(0, 6)
}

function buildExplanationBlocks({
  opportunity = {},
  metrics = {},
  freshness = {},
  liquidity = {},
  confidence = {},
  verdict = "",
  headline = {}
} = {}) {
  const buyMarket = normalizeText(opportunity.buyMarket || "buy market")
  const sellMarket = normalizeText(opportunity.sellMarket || "sell market")
  const profitUsd = metrics.netProfitUsd == null ? 0 : metrics.netProfitUsd
  const spreadPct = metrics.netProfitPct == null ? 0 : metrics.netProfitPct

  const whyThisTradeExists = `The opportunity exists because ${buyMarket} entry pricing is below ${sellMarket} fee-adjusted exit pricing, leaving about $${round2(
    profitUsd
  )} net (${round2(spreadPct)}%).`

  const whatCanBreakIt = [
    Number(confidence.score || 0) < 60
      ? "Execution confidence is already soft, so one quote shift can erase edge."
      : "",
    Number(liquidity.score || 0) < 55
      ? "Liquidity is thin, so posted spread may not be fully executable."
      : "",
    freshness.staleFlag || Number(freshness.score || 0) < 55
      ? "Signals are aging, so current prices may be stale."
      : ""
  ]
    .filter(Boolean)
    .join(" ")

  const exitEase =
    Number(liquidity.score || 0) >= 75 && Number(confidence.score || 0) >= 70
      ? "Exit should be relatively easy if market conditions remain stable."
      : Number(liquidity.score || 0) >= 55
        ? "Exit is workable but likely needs patient order placement."
        : "Exit may be hard without discounting because recent fill depth is limited."

  const reasonSummary =
    normalizeText(headline.headline_text) ||
    `Verdict ${normalizeText(verdict || "watch").toUpperCase()}.`

  return {
    reasonSummary,
    whyThisTradeExists,
    whatCanBreakIt:
      whatCanBreakIt || "Fast fee-adjusted spread compression and stale quotes can invalidate this trade quickly.",
    whyExitMayBeEasyOrHard: exitEase
  }
}

function buildInsightPayloadFromOpportunity(opportunity = {}, options = {}) {
  const nowMs = Number(options.nowMs || Date.now())
  const category = normalizeCategory(opportunity.itemCategory)
  const profile = resolveCategoryProfile(category)
  const metrics = deriveProfitMetrics(opportunity)
  const liquidity = computeLiquidityScore(opportunity, profile)
  const freshness = computeFreshnessScore(opportunity, category, nowMs)
  const confidence = computeConfidenceScore(opportunity, freshness)
  const riskFlags = buildRiskFlags({
    opportunity,
    metrics,
    liquidity,
    freshness,
    confidence,
    profile
  })
  const verdict = resolveVerdict({
    metrics,
    confidenceScore: confidence.score,
    liquidityScore: liquidity.score,
    freshnessScore: freshness.score,
    riskFlags
  })
  const exitEtaHours = estimateExitEtaHours({
    category,
    liquidityScore: liquidity.score,
    confidenceScore: confidence.score,
    freshnessScore: freshness.score,
    volume7d: liquidity.volume7d
  })
  const recommendedPositionSize = recommendPositionSizeUsd({
    category,
    buyPrice: metrics.buyPrice,
    confidenceScore: confidence.score,
    liquidityScore: liquidity.score,
    freshnessScore: freshness.score,
    volume7d: liquidity.volume7d
  })
  const failureConditions = buildFailureConditions({
    opportunity,
    metrics,
    freshness,
    liquidity,
    profile
  })
  const headline = buildInsightHeadline({
    verdict,
    confidence_score: confidence.score,
    freshness_score: freshness.score,
    liquidity_score: liquidity.score,
    net_profit_pct_after_fees: metrics.netProfitPct,
    exit_eta_hours: exitEtaHours,
    risk_flags: riskFlags
  })
  const explanations = buildExplanationBlocks({
    opportunity,
    metrics,
    freshness,
    liquidity,
    confidence,
    verdict,
    headline
  })

  return {
    gross_profit_pct: metrics.grossProfitPct,
    gross_profit_usd: metrics.grossProfitUsd,
    net_profit_pct_after_fees: metrics.netProfitPct,
    net_profit_usd_after_fees: metrics.netProfitUsd,
    confidence_score: confidence.score,
    liquidity_score: liquidity.score,
    freshness_score: freshness.score,
    exit_eta_hours: exitEtaHours,
    recommended_position_size: recommendedPositionSize,
    risk_flags: riskFlags,
    reason_summary: headline.headline_text,
    headline_text: headline.headline_text,
    headline_style: headline.headline_style,
    primary_reason: headline.primary_reason,
    secondary_reason: headline.secondary_reason,
    failure_conditions: failureConditions,
    verdict,
    why_this_trade_exists: explanations.whyThisTradeExists,
    what_can_break_it: explanations.whatCanBreakIt,
    why_exit_may_be_easy_or_hard: explanations.whyExitMayBeEasyOrHard,
    score_explanations: {
      confidence: confidence.summary,
      liquidity: liquidity.summary,
      freshness: freshness.summary
    },
    score_components: {
      confidence: {
        execution_base: confidence.base,
        scanner_score_component: confidence.scoreInput,
        penalty: confidence.penalty
      },
      liquidity: {
        volume_7d: liquidity.volume7d,
        market_coverage: liquidity.marketCoverage,
        min_volume_7d: liquidity.minVolume,
        min_market_coverage: liquidity.minCoverage
      },
      freshness: {
        latest_signal_at: freshness.signalIso,
        age_minutes: freshness.ageMinutes == null ? null : round2(freshness.ageMinutes),
        stale_threshold_minutes: round2(freshness.staleThresholdMinutes || 0),
        stale_result: freshness.staleFlag
      }
    },
    fee_context: {
      sell_market_fee_pct: metrics.sellFeePct
    }
  }
}

function buildCacheKey(opportunityId) {
  return normalizeText(opportunityId)
}

function getCachedInsight(opportunityId) {
  const key = buildCacheKey(opportunityId)
  if (!key) return null
  const hit = insightCache.get(key)
  if (!hit) return null
  if (Date.now() >= Number(hit.expiresAt || 0)) {
    insightCache.delete(key)
    return null
  }
  return hit.payload
}

function pruneInsightCache() {
  if (insightCache.size <= INSIGHT_CACHE_MAX_ITEMS) return
  const nowMs = Date.now()
  for (const [key, entry] of insightCache.entries()) {
    if (Number(entry?.expiresAt || 0) <= nowMs) {
      insightCache.delete(key)
    }
  }
  if (insightCache.size <= INSIGHT_CACHE_MAX_ITEMS) return
  const overflow = insightCache.size - INSIGHT_CACHE_MAX_ITEMS
  const keys = Array.from(insightCache.keys()).slice(0, overflow)
  for (const key of keys) {
    insightCache.delete(key)
  }
}

function setCachedInsight(opportunityId, payload) {
  const key = buildCacheKey(opportunityId)
  if (!key || !payload) return
  insightCache.set(key, {
    expiresAt: Date.now() + INSIGHT_CACHE_TTL_MS,
    payload
  })
  pruneInsightCache()
}

async function getInsightForFeedId(opportunityId, options = {}) {
  const safeId = normalizeText(opportunityId)
  if (!safeId) {
    throw new AppError("opportunity_id is required", 400, "VALIDATION_ERROR")
  }
  const forceRefresh = normalizeBoolean(options.forceRefresh)
  if (!forceRefresh) {
    const cached = getCachedInsight(safeId)
    if (cached) return cached
  }

  const row = await arbitrageFeedRepo.getById(safeId)
  if (!row) {
    throw new AppError("Opportunity not found", 404, "OPPORTUNITY_NOT_FOUND")
  }
  const opportunity = mapFeedRowToApiRow(row)
  const insight = buildInsightPayloadFromOpportunity(opportunity)
  const payload = {
    opportunity_id: opportunity.feedId || safeId,
    generated_at: new Date().toISOString(),
    cache_ttl_seconds: Math.round(INSIGHT_CACHE_TTL_MS / 1000),
    opportunity: {
      item_name: opportunity.itemName || "Tracked Item",
      item_category: normalizeCategory(opportunity.itemCategory),
      buy_market: opportunity.buyMarket || null,
      buy_price: toFiniteOrNull(opportunity.buyPrice),
      sell_market: opportunity.sellMarket || null,
      sell_net: toFiniteOrNull(opportunity.sellNet),
      spread_pct: toFiniteOrNull(opportunity.spread),
      scanner_score: toFiniteOrNull(opportunity.score),
      execution_confidence: opportunity.executionConfidence || null,
      detected_at: opportunity.detectedAt || null
    },
    ...insight
  }

  setCachedInsight(safeId, payload)
  return payload
}

async function precomputeInsightsForOpportunityIds(opportunityIds = [], options = {}) {
  const uniqueIds = Array.from(
    new Set(
      (Array.isArray(opportunityIds) ? opportunityIds : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  ).slice(0, PRECOMPUTE_MAX_BATCH)

  const results = []
  for (const opportunityId of uniqueIds) {
    try {
      const insight = await getInsightForFeedId(opportunityId, {
        forceRefresh: options.forceRefresh
      })
      results.push({
        opportunity_id: opportunityId,
        status: "ok",
        verdict: insight.verdict
      })
    } catch (err) {
      results.push({
        opportunity_id: opportunityId,
        status: "error",
        error: normalizeText(err?.message) || "insight_compute_failed"
      })
    }
  }

  return {
    generated_at: new Date().toISOString(),
    requested: uniqueIds.length,
    computed: results.filter((row) => row.status === "ok").length,
    results
  }
}

function clearInsightCache() {
  insightCache.clear()
}

exports.INSIGHT_CACHE_TTL_MS = INSIGHT_CACHE_TTL_MS
exports.getOpportunityInsight = getInsightForFeedId
exports.precomputeInsightsForOpportunityIds = precomputeInsightsForOpportunityIds
exports.deriveInsightPayloadFromOpportunity = buildInsightPayloadFromOpportunity

exports.__testables = {
  normalizeCategory,
  deriveProfitMetrics,
  computeLiquidityScore,
  computeFreshnessScore,
  computeConfidenceScore,
  buildRiskFlags,
  resolveVerdict,
  classifyInsightReasons,
  buildInsightHeadline,
  buildFailureConditions,
  buildInsightPayloadFromOpportunity,
  clearInsightCache
}

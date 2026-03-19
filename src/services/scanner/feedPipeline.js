const {
  MIN_CONFIDENCE_CHANGE_LEVELS,
  MIN_LIQUIDITY_CHANGE_PCT,
  MIN_PROFIT_CHANGE_PCT,
  MIN_SCORE_CHANGE,
  MIN_SPREAD_CHANGE_PCT
} = require("./config")

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIsoOrNull(value) {
  if (value == null || value === "") return null
  if (value instanceof Date) {
    const ts = value.getTime()
    if (!Number.isFinite(ts)) return null
    return new Date(ts).toISOString()
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
      const ts = new Date(normalizedTs).getTime()
      if (Number.isFinite(ts)) return new Date(ts).toISOString()
    }
  }
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toBooleanOrNull(value) {
  if (value == null || value === "") return null
  if (typeof value === "boolean") return value
  if (typeof value === "number") {
    if (value === 1) return true
    if (value === 0) return false
    return null
  }
  const text = normalizeText(value).toLowerCase()
  if (!text) return null
  if (text === "true" || text === "1" || text === "yes") return true
  if (text === "false" || text === "0" || text === "no") return false
  return null
}

function clampScore(value) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return 0
  return Math.min(Math.max(Math.round(parsed), 0), 100)
}

function buildDisplayQualityScore(value) {
  const rawScore = toFiniteOrNull(value)
  if (rawScore == null) return null
  const score = clampScore(rawScore)

  // Keep low scores readable in UI while preserving score order.
  if (score <= 70) {
    const normalized = score / 70
    return Number((18 + 52 * Math.pow(normalized, 0.9)).toFixed(1))
  }

  // Avoid showing near-perfect 100/100 by default in visible feeds.
  if (score >= 92) {
    return Number((92 + (score - 92) * 0.6).toFixed(1))
  }

  return Number(score.toFixed(1))
}

function normalizeCategory(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return "weapon_skin"
  if (
    raw === "weapon_skin" ||
    raw === "case" ||
    raw === "sticker_capsule" ||
    raw === "knife" ||
    raw === "glove"
  ) {
    return raw
  }
  if (raw === "skins" || raw === "skin") return "weapon_skin"
  if (raw === "cases") return "case"
  if (raw === "capsules" || raw === "capsule") return "sticker_capsule"
  if (raw === "knives" || raw === "future_knife") return "knife"
  if (raw === "gloves" || raw === "future_glove") return "glove"
  return "weapon_skin"
}

function confidenceLevel(value) {
  const normalized = normalizeText(value).toLowerCase()
  if (normalized === "high") return 3
  if (normalized === "medium") return 2
  return 1
}

function safePercentChange(current, previous) {
  const now = toFiniteOrNull(current)
  const prev = toFiniteOrNull(previous)
  if (now == null || prev == null || prev === 0) return 0
  return Math.abs(((now - prev) / prev) * 100)
}

function normalizeStringSet(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value).toLowerCase())
        .filter(Boolean)
    )
  ).sort()
}

function buildSignature(row = {}) {
  const itemName = normalizeText(row.itemName || row.item_name)
  const buyMarket = normalizeText(row.buyMarket || row.buy_market).toLowerCase()
  const sellMarket = normalizeText(row.sellMarket || row.sell_market).toLowerCase()
  return `${itemName}::${buyMarket}::${sellMarket}`
}

function classifyOpportunityFeedEvent(opportunity = {}, previousRow = null) {
  if (!previousRow) {
    return {
      eventType: "new",
      materiallyChanged: true,
      changeReasons: ["new_signal"]
    }
  }

  if (!Boolean(previousRow?.is_active)) {
    return {
      eventType: "reactivated",
      materiallyChanged: true,
      changeReasons: ["reactivated_signal"]
    }
  }

  const changeReasons = []
  const markChange = (code) => {
    const normalized = normalizeText(code)
    if (!normalized) return
    if (!changeReasons.includes(normalized)) {
      changeReasons.push(normalized)
    }
  }
  const profitChangePct = safePercentChange(opportunity.profit, previousRow.profit)
  if (profitChangePct >= Number(MIN_PROFIT_CHANGE_PCT || 0)) {
    markChange("profit")
  }

  const scoreNow = toFiniteOrNull(opportunity.score)
  const scorePrev = toFiniteOrNull(previousRow.opportunity_score)
  if (scoreNow != null && scorePrev != null && Math.abs(scoreNow - scorePrev) >= Number(MIN_SCORE_CHANGE || 0)) {
    markChange("score")
  }

  const spreadChangePct = safePercentChange(opportunity.spread, previousRow.spread_pct)
  if (spreadChangePct >= Number(MIN_SPREAD_CHANGE_PCT || 0)) {
    markChange("spread")
  }

  const liquidityNow = toFiniteOrNull(opportunity.liquidity)
  const liquidityPrev = toFiniteOrNull(previousRow?.metadata?.liquidity_value)
  const liquidityChangePct = safePercentChange(liquidityNow, liquidityPrev)
  if (liquidityChangePct >= Number(MIN_LIQUIDITY_CHANGE_PCT || 0)) {
    markChange("liquidity")
  }

  const confidenceDelta = Math.abs(
    confidenceLevel(opportunity.executionConfidence) -
      confidenceLevel(previousRow.execution_confidence)
  )
  if (confidenceDelta >= Number(MIN_CONFIDENCE_CHANGE_LEVELS || 0)) {
    markChange("confidence")
  }

  const nowFlags = normalizeStringSet(opportunity.flags)
  const prevFlags = normalizeStringSet(previousRow?.metadata?.flags)
  if (nowFlags.join("|") !== prevFlags.join("|")) {
    markChange("diagnostics")
  }

  const nowBadges = normalizeStringSet(opportunity.badges)
  const prevBadges = normalizeStringSet(previousRow?.metadata?.badges)
  if (nowBadges.join("|") !== prevBadges.join("|")) {
    markChange("diagnostics")
  }

  const nowMetadata = opportunity?.metadata && typeof opportunity.metadata === "object" ? opportunity.metadata : {}
  const prevMetadata =
    previousRow?.metadata && typeof previousRow.metadata === "object" ? previousRow.metadata : {}
  const nowLatestMarketSignalAt =
    toIsoOrNull(
      nowMetadata?.latest_market_signal_at ??
        nowMetadata?.latestMarketSignalAt ??
        opportunity?.latestMarketSignalAt ??
        opportunity?.latest_market_signal_at ??
        nowMetadata?.diagnostics_debug?.latest_market_signal_at
    ) || null
  const prevLatestMarketSignalAt =
    toIsoOrNull(
      prevMetadata?.latest_market_signal_at ??
        prevMetadata?.latestMarketSignalAt ??
        prevMetadata?.diagnostics_debug?.latest_market_signal_at
    ) || null
  if (nowLatestMarketSignalAt !== prevLatestMarketSignalAt) {
    markChange("freshness")
  }

  const nowStaleResult =
    toBooleanOrNull(
      nowMetadata?.stale_result ??
        nowMetadata?.staleResult ??
        opportunity?.staleResult ??
        opportunity?.stale_result ??
        nowMetadata?.diagnostics_debug?.stale_result
    ) ?? null
  const prevStaleResult =
    toBooleanOrNull(
      prevMetadata?.stale_result ??
        prevMetadata?.staleResult ??
        prevMetadata?.diagnostics_debug?.stale_result
    ) ?? null
  if (nowStaleResult !== prevStaleResult) {
    markChange("freshness")
  }

  const nowStaleThreshold =
    toFiniteOrNull(
      nowMetadata?.stale_threshold_used ??
        nowMetadata?.staleThresholdUsed ??
        opportunity?.staleThresholdUsed ??
        opportunity?.stale_threshold_used ??
        nowMetadata?.diagnostics_debug?.stale_threshold_used
    ) ?? null
  const prevStaleThreshold =
    toFiniteOrNull(
      prevMetadata?.stale_threshold_used ??
        prevMetadata?.staleThresholdUsed ??
        prevMetadata?.diagnostics_debug?.stale_threshold_used
    ) ?? null
  if (nowStaleThreshold !== prevStaleThreshold) {
    markChange("freshness")
  }

  const materiallyChanged = changeReasons.length > 0
  return {
    eventType: materiallyChanged ? "updated" : "duplicate",
    materiallyChanged,
    changeReasons: materiallyChanged ? changeReasons : []
  }
}

function isMateriallyNewOpportunity(opportunity = {}, previousRow = null) {
  const classification = classifyOpportunityFeedEvent(opportunity, previousRow)
  return classification.materiallyChanged
}

function buildFeedInsertRow(opportunity = {}, options = {}) {
  const detectedAt = options.detectedAt || new Date().toISOString()
  const scanRunId = normalizeText(options.scanRunId) || null
  const eventMeta = options.eventMeta && typeof options.eventMeta === "object" ? options.eventMeta : {}
  const flags = Array.isArray(opportunity.flags) ? opportunity.flags : []
  const badges = Array.isArray(opportunity.badges) ? opportunity.badges : []
  const rawQualityScore = clampScore(opportunity.score)
  const qualityScoreDisplay = buildDisplayQualityScore(rawQualityScore)

  return {
    item_name: normalizeText(opportunity.itemName || opportunity.marketHashName || "Tracked Item"),
    market_hash_name: normalizeText(opportunity.marketHashName || opportunity.itemName || "Tracked Item"),
    category: normalizeCategory(opportunity.itemCategory || opportunity.category),
    buy_market: normalizeText(opportunity.buyMarket).toLowerCase(),
    buy_price: Number(Number(opportunity.buyPrice || 0).toFixed(4)),
    sell_market: normalizeText(opportunity.sellMarket).toLowerCase(),
    sell_net: Number(Number(opportunity.sellNet || 0).toFixed(4)),
    profit: Number(Number(opportunity.profit || 0).toFixed(4)),
    spread_pct: Number(Number(opportunity.spread || 0).toFixed(4)),
    opportunity_score: rawQualityScore,
    execution_confidence: normalizeText(opportunity.executionConfidence || "Low") || "Low",
    quality_grade: normalizeText(opportunity.qualityGrade || "SPECULATIVE") || "SPECULATIVE",
    liquidity_label: normalizeText(opportunity.liquidityBand || "Low") || "Low",
    detected_at: detectedAt,
    scan_run_id: scanRunId,
    is_active: true,
    is_duplicate: Boolean(eventMeta?.eventType === "duplicate"),
    metadata: {
      item_id: opportunity.itemId || null,
      item_subcategory: opportunity.itemSubcategory || null,
      item_rarity: opportunity.itemRarity || null,
      item_rarity_color: opportunity.itemRarityColor || null,
      item_image_url: opportunity.itemImageUrl || null,
      volume_7d: toFiniteOrNull(opportunity.liquidity),
      liquidity_value: toFiniteOrNull(opportunity.liquidity),
      market_coverage: Number(opportunity.marketCoverage || 0),
      reference_price: toFiniteOrNull(opportunity.referencePrice),
      flags,
      badges,
      latest_market_signal_at:
        toIsoOrNull(
          opportunity?.metadata?.latest_market_signal_at ??
            opportunity?.metadata?.latestMarketSignalAt ??
            opportunity?.latestMarketSignalAt ??
            opportunity?.latest_market_signal_at
        ) || null,
      latest_quote_at:
        toIsoOrNull(
          opportunity?.metadata?.latest_quote_at ??
            opportunity?.metadata?.latestQuoteAt ??
            opportunity?.latestQuoteAt ??
            opportunity?.latest_quote_at
        ) || null,
      latest_snapshot_at:
        toIsoOrNull(
          opportunity?.metadata?.latest_snapshot_at ??
            opportunity?.metadata?.latestSnapshotAt ??
            opportunity?.latestSnapshotAt ??
            opportunity?.latest_snapshot_at
        ) || null,
      latest_reference_price_at:
        toIsoOrNull(
          opportunity?.metadata?.latest_reference_price_at ??
            opportunity?.metadata?.latestReferencePriceAt ??
            opportunity?.latestReferencePriceAt ??
            opportunity?.latest_reference_price_at
        ) || null,
      stale_threshold_used:
        toFiniteOrNull(
          opportunity?.metadata?.stale_threshold_used ??
            opportunity?.metadata?.staleThresholdUsed ??
            opportunity?.staleThresholdUsed ??
            opportunity?.stale_threshold_used
        ) ?? null,
      stale_result:
        toBooleanOrNull(
          opportunity?.metadata?.stale_result ??
            opportunity?.metadata?.staleResult ??
            opportunity?.staleResult ??
            opportunity?.stale_result
        ) ?? null,
      stale_reason_source:
        normalizeText(
          opportunity?.metadata?.stale_reason_source ??
            opportunity?.metadata?.staleReasonSource ??
            opportunity?.staleReasonSource ??
            opportunity?.stale_reason_source
        ) || null,
      buy_url: opportunity.buyUrl || null,
      sell_url: opportunity.sellUrl || null,
      opportunity_tier: normalizeText(opportunity.tier || "").toLowerCase() || "speculative",
      is_high_confidence_eligible: Boolean(opportunity.isHighConfidenceEligible),
      is_risky_eligible: Boolean(opportunity.isRiskyEligible),
      score_category: opportunity.scoreCategory || null,
      ...eventMeta,
      ...(opportunity.metadata && typeof opportunity.metadata === "object" ? opportunity.metadata : {}),
      quality_score_display: qualityScoreDisplay
    }
  }
}

function mapFeedRowToApiRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === "object" ? row.metadata : {}
  const detectedAt = row?.detected_at || null
  const tier = normalizeText(metadata?.opportunity_tier).toLowerCase()
  const rawScore = toFiniteOrNull(row?.opportunity_score)
  const qualityScoreDisplay =
    toFiniteOrNull(metadata?.quality_score_display ?? metadata?.qualityScoreDisplay) ??
    buildDisplayQualityScore(rawScore)
  return {
    feedId: row?.id || null,
    detectedAt,
    scanRunId: row?.scan_run_id || null,
    isActive: row?.is_active == null ? true : Boolean(row.is_active),
    isDuplicate: Boolean(row?.is_duplicate),
    itemId: metadata?.item_id || null,
    itemName: normalizeText(row?.item_name || row?.market_hash_name || "Tracked Item"),
    marketHashName: normalizeText(row?.market_hash_name || row?.item_name || "Tracked Item"),
    itemCategory: normalizeCategory(row?.category),
    itemSubcategory: metadata?.item_subcategory || null,
    itemRarity: metadata?.item_rarity || null,
    itemRarityColor: metadata?.item_rarity_color || null,
    itemImageUrl: metadata?.item_image_url || null,
    buyMarket: normalizeText(row?.buy_market).toLowerCase() || null,
    buyPrice: toFiniteOrNull(row?.buy_price),
    sellMarket: normalizeText(row?.sell_market).toLowerCase() || null,
    sellNet: toFiniteOrNull(row?.sell_net),
    profit: toFiniteOrNull(row?.profit),
    spread: toFiniteOrNull(row?.spread_pct),
    score: rawScore,
    qualityScoreDisplay,
    quality_score_display: qualityScoreDisplay,
    scoreCategory:
      metadata?.score_category ||
      (tier === "strong"
        ? "Strong"
        : tier === "risky"
          ? "Risky"
          : tier === "speculative"
            ? "Speculative"
            : "Rejected"),
    executionConfidence: normalizeText(row?.execution_confidence || "Low") || "Low",
    qualityGrade: normalizeText(row?.quality_grade || "").toUpperCase() || "SPECULATIVE",
    liquidity: toFiniteOrNull(metadata?.volume_7d ?? metadata?.liquidity_value),
    liquidityBand: normalizeText(row?.liquidity_label || "Low") || "Low",
    liquidityLabel: normalizeText(row?.liquidity_label || "Low") || "Low",
    volume7d: toFiniteOrNull(metadata?.volume_7d ?? metadata?.liquidity_value),
    marketCoverage: Number(metadata?.market_coverage || 0),
    referencePrice: toFiniteOrNull(metadata?.reference_price),
    latestMarketSignalAt:
      toIsoOrNull(
        metadata?.latest_market_signal_at ??
          metadata?.latestMarketSignalAt ??
          metadata?.diagnostics_debug?.latest_market_signal_at
      ) || null,
    latest_market_signal_at:
      toIsoOrNull(
        metadata?.latest_market_signal_at ??
          metadata?.latestMarketSignalAt ??
          metadata?.diagnostics_debug?.latest_market_signal_at
      ) || null,
    latestQuoteAt:
      toIsoOrNull(
        metadata?.latest_quote_at ?? metadata?.latestQuoteAt ?? metadata?.diagnostics_debug?.latest_quote_at
      ) || null,
    latest_quote_at:
      toIsoOrNull(
        metadata?.latest_quote_at ?? metadata?.latestQuoteAt ?? metadata?.diagnostics_debug?.latest_quote_at
      ) || null,
    latestSnapshotAt:
      toIsoOrNull(
        metadata?.latest_snapshot_at ??
          metadata?.latestSnapshotAt ??
          metadata?.diagnostics_debug?.latest_snapshot_at
      ) || null,
    latest_snapshot_at:
      toIsoOrNull(
        metadata?.latest_snapshot_at ??
          metadata?.latestSnapshotAt ??
          metadata?.diagnostics_debug?.latest_snapshot_at
      ) || null,
    latestReferencePriceAt:
      toIsoOrNull(
        metadata?.latest_reference_price_at ??
          metadata?.latestReferencePriceAt ??
          metadata?.diagnostics_debug?.latest_reference_price_at
      ) || null,
    latest_reference_price_at:
      toIsoOrNull(
        metadata?.latest_reference_price_at ??
          metadata?.latestReferencePriceAt ??
          metadata?.diagnostics_debug?.latest_reference_price_at
      ) || null,
    staleThresholdUsed:
      toFiniteOrNull(
        metadata?.stale_threshold_used ??
          metadata?.staleThresholdUsed ??
          metadata?.diagnostics_debug?.stale_threshold_used
      ) ?? null,
    stale_threshold_used:
      toFiniteOrNull(
        metadata?.stale_threshold_used ??
          metadata?.staleThresholdUsed ??
          metadata?.diagnostics_debug?.stale_threshold_used
      ) ?? null,
    staleResult:
      toBooleanOrNull(
        metadata?.stale_result ?? metadata?.staleResult ?? metadata?.diagnostics_debug?.stale_result
      ) ?? null,
    stale_result:
      toBooleanOrNull(
        metadata?.stale_result ?? metadata?.staleResult ?? metadata?.diagnostics_debug?.stale_result
      ) ?? null,
    staleReasonSource:
      normalizeText(
        metadata?.stale_reason_source ??
          metadata?.staleReasonSource ??
          metadata?.diagnostics_debug?.stale_reason_source
      ) || null,
    stale_reason_source:
      normalizeText(
        metadata?.stale_reason_source ??
          metadata?.staleReasonSource ??
          metadata?.diagnostics_debug?.stale_reason_source
      ) || null,
    flags: Array.isArray(metadata?.flags) ? metadata.flags : [],
    badges: Array.isArray(metadata?.badges) ? metadata.badges : [],
    diagnosticsDebug:
      metadata?.diagnostics_debug && typeof metadata.diagnostics_debug === "object"
        ? metadata.diagnostics_debug
        : null,
    isHighConfidenceEligible: Boolean(metadata?.is_high_confidence_eligible),
    isRiskyEligible: Boolean(metadata?.is_risky_eligible),
    buyUrl: metadata?.buy_url || null,
    sellUrl: metadata?.sell_url || null
  }
}

module.exports = {
  buildSignature,
  classifyOpportunityFeedEvent,
  isMateriallyNewOpportunity,
  buildFeedInsertRow,
  mapFeedRowToApiRow
}

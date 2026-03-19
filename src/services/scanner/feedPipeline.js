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
  const profitChangePct = safePercentChange(opportunity.profit, previousRow.profit)
  if (profitChangePct >= Number(MIN_PROFIT_CHANGE_PCT || 0)) {
    changeReasons.push("profit")
  }

  const scoreNow = toFiniteOrNull(opportunity.score)
  const scorePrev = toFiniteOrNull(previousRow.opportunity_score)
  if (scoreNow != null && scorePrev != null && Math.abs(scoreNow - scorePrev) >= Number(MIN_SCORE_CHANGE || 0)) {
    changeReasons.push("score")
  }

  const spreadChangePct = safePercentChange(opportunity.spread, previousRow.spread_pct)
  if (spreadChangePct >= Number(MIN_SPREAD_CHANGE_PCT || 0)) {
    changeReasons.push("spread")
  }

  const liquidityNow = toFiniteOrNull(opportunity.liquidity)
  const liquidityPrev = toFiniteOrNull(previousRow?.metadata?.liquidity_value)
  const liquidityChangePct = safePercentChange(liquidityNow, liquidityPrev)
  if (liquidityChangePct >= Number(MIN_LIQUIDITY_CHANGE_PCT || 0)) {
    changeReasons.push("liquidity")
  }

  const confidenceDelta = Math.abs(
    confidenceLevel(opportunity.executionConfidence) -
      confidenceLevel(previousRow.execution_confidence)
  )
  if (confidenceDelta >= Number(MIN_CONFIDENCE_CHANGE_LEVELS || 0)) {
    changeReasons.push("confidence")
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
    opportunity_score: Math.min(Math.max(Math.round(Number(opportunity.score || 0)), 0), 100),
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
      buy_url: opportunity.buyUrl || null,
      sell_url: opportunity.sellUrl || null,
      opportunity_tier: normalizeText(opportunity.tier || "").toLowerCase() || "speculative",
      is_high_confidence_eligible: Boolean(opportunity.isHighConfidenceEligible),
      is_risky_eligible: Boolean(opportunity.isRiskyEligible),
      score_category: opportunity.scoreCategory || null,
      ...eventMeta,
      ...(opportunity.metadata && typeof opportunity.metadata === "object" ? opportunity.metadata : {})
    }
  }
}

function mapFeedRowToApiRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === "object" ? row.metadata : {}
  const detectedAt = row?.detected_at || null
  const tier = normalizeText(metadata?.opportunity_tier).toLowerCase()
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
    score: toFiniteOrNull(row?.opportunity_score),
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
    flags: Array.isArray(metadata?.flags) ? metadata.flags : [],
    badges: Array.isArray(metadata?.badges) ? metadata.badges : [],
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

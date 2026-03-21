const { createHash } = require("crypto")
const {
  MIN_CONFIDENCE_CHANGE_LEVELS,
  MIN_LIQUIDITY_CHANGE_PCT,
  MIN_PROFIT_CHANGE_PCT,
  MIN_SCORE_CHANGE,
  MIN_SPREAD_CHANGE_PCT
} = require("./config")

const MIN_BUY_PRICE_CHANGE_PCT = 2
const MIN_SELL_PRICE_CHANGE_PCT = 2
const FINGERPRINT_PRICE_BANDS = Object.freeze([
  { max: 5, step: 0.1 },
  { max: 20, step: 0.25 },
  { max: 100, step: 0.5 },
  { max: 300, step: 1 },
  { max: Infinity, step: 2 }
])
const MATERIAL_PRICE_BANDS = Object.freeze([
  { max: 5, step: 0.05 },
  { max: 20, step: 0.1 },
  { max: 100, step: 0.25 },
  { max: 300, step: 0.5 },
  { max: Infinity, step: 1 }
])
const MATERIAL_PROFIT_BANDS = Object.freeze([
  { max: 2.5, step: 0.05 },
  { max: 10, step: 0.1 },
  { max: 50, step: 0.25 },
  { max: Infinity, step: 0.5 }
])

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

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function toIntegerOrNull(value, fallback = null, min = 1) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), min)
}

function normalizeUrlIdentity(value) {
  const raw = normalizeText(value)
  if (!raw) return ""
  try {
    const parsed = new URL(raw)
    return `${parsed.host}${parsed.pathname}`.toLowerCase()
  } catch (_err) {
    return raw.toLowerCase()
  }
}

function firstNonEmptyText(values = []) {
  for (const value of Array.isArray(values) ? values : []) {
    const normalized = normalizeText(value)
    if (normalized) return normalized
  }
  return ""
}

function normalizeBandValue(value, bands = FINGERPRINT_PRICE_BANDS) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return "na"
  const absolute = Math.abs(parsed)
  const profile = (Array.isArray(bands) ? bands : []).find(
    (row) => absolute <= Number(row?.max || 0)
  )
  const step = Number(profile?.step || 0.25)
  if (!Number.isFinite(step) || step <= 0) return "na"
  const bucket = Math.round(parsed / step)
  const snapped = Number((bucket * step).toFixed(4))
  return `${snapped.toFixed(4)}@${step.toFixed(4)}`
}

function hashCanonical(parts = []) {
  const payload = (Array.isArray(parts) ? parts : [])
    .map((part) => normalizeText(part))
    .join("|")
  return createHash("sha1").update(payload).digest("hex")
}

function resolveListingIdentity(row = {}, metadata = {}) {
  return (
    firstNonEmptyText([
      row?.skinport_listing_id,
      row?.skinportListingId,
      row?.buy_listing_id,
      row?.buyListingId,
      row?.sell_listing_id,
      row?.sellListingId,
      row?.listing_id,
      row?.listingId,
      metadata?.skinport_listing_id,
      metadata?.buy_listing_id,
      metadata?.sell_listing_id,
      metadata?.listing_id,
      metadata?.listingId
    ]).toLowerCase() || "na"
  )
}

function resolveVerdict(row = {}, metadata = {}) {
  return (
    firstNonEmptyText([
      row?.verdict,
      metadata?.publish_refresh?.verdict,
      metadata?.verdict
    ]).toLowerCase() || "na"
  )
}

function buildOpportunityFingerprint(row = {}) {
  const metadata = toJsonObject(row?.metadata)
  const itemIdentity =
    firstNonEmptyText([
      row?.item_id,
      row?.itemId,
      metadata?.item_id,
      metadata?.itemId,
      row?.market_hash_name,
      row?.marketHashName,
      row?.item_name,
      row?.itemName
    ]).toLowerCase() || "na"
  const category = normalizeCategory(row?.category || row?.itemCategory)
  const buyMarket = normalizeText(row?.buy_market || row?.buyMarket).toLowerCase() || "na"
  const sellMarket = normalizeText(row?.sell_market || row?.sellMarket).toLowerCase() || "na"
  const variantToken = firstNonEmptyText([
    row?.item_subcategory,
    row?.itemSubcategory,
    metadata?.item_subcategory,
    metadata?.itemSubcategory,
    row?.item_rarity,
    row?.itemRarity,
    metadata?.item_rarity,
    metadata?.itemRarity,
    metadata?.item_variant,
    metadata?.wear,
    metadata?.phase
  ])
    .toLowerCase()
    .replace(/\s+/g, "_") || "na"
  const listingIdentity = resolveListingIdentity(row, metadata)
  const quoteTypeIdentity =
    firstNonEmptyText([
      row?.skinport_quote_type,
      row?.skinportQuoteType,
      metadata?.skinport_quote_type,
      metadata?.quote_type
    ]).toLowerCase() || "na"
  const buyQuoteIdentity =
    firstNonEmptyText([
      row?.buy_quote_identity,
      metadata?.buy_quote_identity,
      normalizeUrlIdentity(row?.buy_url || row?.buyUrl || metadata?.buy_url),
      normalizeBandValue(row?.buy_price ?? row?.buyPrice, FINGERPRINT_PRICE_BANDS)
    ]).toLowerCase() || "na"
  const sellQuoteIdentity =
    firstNonEmptyText([
      row?.sell_quote_identity,
      metadata?.sell_quote_identity,
      normalizeUrlIdentity(row?.sell_url || row?.sellUrl || metadata?.sell_url),
      normalizeBandValue(row?.sell_net ?? row?.sellNet, FINGERPRINT_PRICE_BANDS)
    ]).toLowerCase() || "na"

  return `ofp_${hashCanonical([
    `item:${itemIdentity}`,
    `category:${category}`,
    `buy:${buyMarket}`,
    `sell:${sellMarket}`,
    `variant:${variantToken}`,
    `listing:${listingIdentity}`,
    `buy_quote:${buyQuoteIdentity}`,
    `sell_quote:${sellQuoteIdentity}`,
    `quote_type:${quoteTypeIdentity}`,
    `buy_band:${normalizeBandValue(row?.buy_price ?? row?.buyPrice, FINGERPRINT_PRICE_BANDS)}`,
    `sell_band:${normalizeBandValue(row?.sell_net ?? row?.sellNet, FINGERPRINT_PRICE_BANDS)}`
  ])}`
}

function buildMaterialChangeHash(row = {}) {
  const metadata = toJsonObject(row?.metadata)
  const buyMarket = normalizeText(row?.buy_market || row?.buyMarket).toLowerCase() || "na"
  const sellMarket = normalizeText(row?.sell_market || row?.sellMarket).toLowerCase() || "na"
  const qualityGrade =
    normalizeText(row?.quality_grade || row?.qualityGrade || "SPECULATIVE").toUpperCase() ||
    "SPECULATIVE"
  const executionConfidence =
    normalizeText(row?.execution_confidence || row?.executionConfidence || "low").toLowerCase() ||
    "low"
  return `mch_${hashCanonical([
    `buy:${buyMarket}`,
    `sell:${sellMarket}`,
    `listing:${resolveListingIdentity(row, metadata)}`,
    `buy_band:${normalizeBandValue(row?.buy_price ?? row?.buyPrice, MATERIAL_PRICE_BANDS)}`,
    `sell_band:${normalizeBandValue(row?.sell_net ?? row?.sellNet, MATERIAL_PRICE_BANDS)}`,
    `profit_band:${normalizeBandValue(row?.profit, MATERIAL_PROFIT_BANDS)}`,
    `quality:${qualityGrade}`,
    `confidence:${executionConfidence}`,
    `verdict:${resolveVerdict(row, metadata)}`,
    `buy_quote:${normalizeUrlIdentity(row?.buy_url || row?.buyUrl || metadata?.buy_url) || "na"}`,
    `sell_quote:${normalizeUrlIdentity(row?.sell_url || row?.sellUrl || metadata?.sell_url) || "na"}`
  ])}`
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
  const buyMarketNow = normalizeText(opportunity.buyMarket || opportunity.buy_market).toLowerCase()
  const buyMarketPrev = normalizeText(previousRow?.buy_market || previousRow?.buyMarket).toLowerCase()
  const sellMarketNow = normalizeText(opportunity.sellMarket || opportunity.sell_market).toLowerCase()
  const sellMarketPrev = normalizeText(previousRow?.sell_market || previousRow?.sellMarket).toLowerCase()
  if (
    buyMarketPrev &&
    sellMarketPrev &&
    (buyMarketNow !== buyMarketPrev || sellMarketNow !== sellMarketPrev)
  ) {
    markChange("market_path")
  }

  const buyPriceChangePct = safePercentChange(
    opportunity.buyPrice ?? opportunity.buy_price,
    previousRow?.buy_price ?? previousRow?.buyPrice
  )
  if (buyPriceChangePct >= MIN_BUY_PRICE_CHANGE_PCT) {
    markChange("buy_price")
  }

  const sellNetChangePct = safePercentChange(
    opportunity.sellNet ?? opportunity.sell_net,
    previousRow?.sell_net ?? previousRow?.sellNet
  )
  if (sellNetChangePct >= MIN_SELL_PRICE_CHANGE_PCT) {
    markChange("sell_net")
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
  const listingNow = resolveListingIdentity(opportunity, nowMetadata)
  const listingPrev = resolveListingIdentity(previousRow, prevMetadata)
  if (listingNow !== listingPrev) {
    markChange("listing")
  }

  const qualityBucketNow = normalizeText(
    opportunity.qualityGrade || opportunity.quality_grade
  ).toUpperCase()
  const qualityBucketPrev = normalizeText(
    previousRow?.quality_grade || previousRow?.qualityGrade
  ).toUpperCase()
  if (qualityBucketNow !== qualityBucketPrev) {
    markChange("quality_bucket")
  }

  const verdictNow = resolveVerdict(opportunity, nowMetadata)
  const verdictPrev = resolveVerdict(previousRow, prevMetadata)
  if (verdictNow !== verdictPrev) {
    markChange("verdict")
  }

  const materialHashNow = buildMaterialChangeHash(opportunity)
  const materialHashPrev =
    normalizeText(previousRow?.material_change_hash || previousRow?.materialChangeHash) ||
    normalizeText(prevMetadata?.material_change_hash || prevMetadata?.materialChangeHash)
  if (materialHashPrev && materialHashNow !== materialHashPrev) {
    markChange("material_hash")
  }

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
  const firstSeenAt = toIsoOrNull(options.firstSeenAt || detectedAt) || detectedAt
  const lastSeenAt = toIsoOrNull(options.lastSeenAt || detectedAt) || detectedAt
  const lastPublishedAt = toIsoOrNull(options.lastPublishedAt || detectedAt) || detectedAt
  const timesSeen = toIntegerOrNull(options.timesSeen, 1, 1) || 1
  const scanRunId = normalizeText(options.scanRunId) || null
  const eventMeta = options.eventMeta && typeof options.eventMeta === "object" ? options.eventMeta : {}
  const flags = Array.isArray(opportunity.flags) ? opportunity.flags : []
  const badges = Array.isArray(opportunity.badges) ? opportunity.badges : []
  const rawQualityScore = clampScore(opportunity.score)
  const qualityScoreDisplay = buildDisplayQualityScore(rawQualityScore)
  const opportunityFingerprint = buildOpportunityFingerprint(opportunity)
  const materialChangeHash = buildMaterialChangeHash(opportunity)

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
    first_seen_at: firstSeenAt,
    last_seen_at: lastSeenAt,
    last_published_at: lastPublishedAt,
    times_seen: timesSeen,
    opportunity_fingerprint: opportunityFingerprint,
    material_change_hash: materialChangeHash,
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
      opportunity_fingerprint: opportunityFingerprint,
      material_change_hash: materialChangeHash,
      first_seen_at: firstSeenAt,
      last_seen_at: lastSeenAt,
      last_published_at: lastPublishedAt,
      times_seen: timesSeen,
      quality_score_display: qualityScoreDisplay
    }
  }
}

function mapFeedRowToApiRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === "object" ? row.metadata : {}
  const detectedAt = row?.detected_at || null
  const discoveredAt = row?.discovered_at || detectedAt || null
  const tier = normalizeText(metadata?.opportunity_tier).toLowerCase()
  const rawScore = toFiniteOrNull(row?.opportunity_score)
  const qualityScoreDisplay =
    toFiniteOrNull(metadata?.quality_score_display ?? metadata?.qualityScoreDisplay) ??
    buildDisplayQualityScore(rawScore)
  const skinportQuotePrice =
    toFiniteOrNull(row?.skinport_quote_price ?? metadata?.skinport_quote_price) ?? null
  const skinportQuoteCurrency =
    normalizeText(row?.skinport_quote_currency ?? metadata?.skinport_quote_currency).toUpperCase() ||
    null
  const skinportQuoteObservedAt =
    toIsoOrNull(row?.skinport_quote_observed_at ?? metadata?.skinport_quote_observed_at) || null
  const skinportQuoteType =
    normalizeText(row?.skinport_quote_type ?? metadata?.skinport_quote_type).toLowerCase() || null
  const skinportItemSlug =
    normalizeText(row?.skinport_item_slug ?? metadata?.skinport_item_slug) || null
  const skinportListingId =
    normalizeText(row?.skinport_listing_id ?? metadata?.skinport_listing_id) || null
  const skinportPriceIntegrityStatus =
    normalizeText(
      row?.skinport_price_integrity_status ?? metadata?.skinport_price_integrity_status
    ).toLowerCase() || null
  const opportunityFingerprint =
    normalizeText(
      row?.opportunity_fingerprint ??
        row?.opportunityFingerprint ??
        metadata?.opportunity_fingerprint ??
        metadata?.opportunityFingerprint
    ) || null
  const firstSeenAt =
    toIsoOrNull(
      row?.first_seen_at ?? row?.firstSeenAt ?? metadata?.first_seen_at ?? metadata?.firstSeenAt
    ) ||
    discoveredAt ||
    null
  const lastSeenAt =
    toIsoOrNull(
      row?.last_seen_at ?? row?.lastSeenAt ?? metadata?.last_seen_at ?? metadata?.lastSeenAt
    ) ||
    detectedAt ||
    null
  const lastPublishedAt =
    toIsoOrNull(
      row?.last_published_at ??
        row?.lastPublishedAt ??
        metadata?.last_published_at ??
        metadata?.lastPublishedAt
    ) ||
    toIsoOrNull(row?.feed_published_at) ||
    detectedAt ||
    null
  const timesSeen =
    toIntegerOrNull(
      row?.times_seen ?? row?.timesSeen ?? metadata?.times_seen ?? metadata?.timesSeen,
      1,
      1
    ) || 1
  const materialChangeHash =
    normalizeText(
      row?.material_change_hash ??
        row?.materialChangeHash ??
        metadata?.material_change_hash ??
        metadata?.materialChangeHash
    ) || null
  const resolvedVolume7d =
    toFiniteOrNull(
      row?.volume_7d ??
        row?.volume7d ??
        metadata?.volume_7d ??
        metadata?.volume7d ??
        metadata?.liquidity_value ??
        metadata?.liquidityValue
    ) ?? null
  const resolvedMarketCoverage =
    toFiniteOrNull(
      row?.market_coverage ??
        row?.marketCoverage ??
        metadata?.market_coverage ??
        metadata?.marketCoverage
    ) ?? null
  const resolvedReferencePrice =
    toFiniteOrNull(
      row?.reference_price ??
        row?.referencePrice ??
        metadata?.reference_price ??
        metadata?.referencePrice
    ) ?? null
  return {
    feedId: row?.id || null,
    detectedAt,
    discoveredAt,
    discovered_at: discoveredAt,
    firstSeenAt,
    first_seen_at: firstSeenAt,
    lastSeenAt,
    last_seen_at: lastSeenAt,
    lastPublishedAt,
    last_published_at: lastPublishedAt,
    timesSeen,
    times_seen: timesSeen,
    opportunityFingerprint,
    opportunity_fingerprint: opportunityFingerprint,
    materialChangeHash,
    material_change_hash: materialChangeHash,
    scanRunId: row?.scan_run_id || null,
    isActive: row?.is_active == null ? true : Boolean(row.is_active),
    isDuplicate: Boolean(row?.is_duplicate),
    itemId:
      normalizeText(row?.item_id ?? row?.itemId ?? metadata?.item_id ?? metadata?.itemId) || null,
    itemName: normalizeText(row?.item_name || row?.market_hash_name || "Tracked Item"),
    marketHashName: normalizeText(row?.market_hash_name || row?.item_name || "Tracked Item"),
    itemCategory: normalizeCategory(row?.category),
    itemSubcategory:
      normalizeText(
        row?.item_subcategory ??
          row?.itemSubcategory ??
          metadata?.item_subcategory ??
          metadata?.itemSubcategory
      ) || null,
    itemRarity:
      normalizeText(
        row?.item_rarity ??
          row?.itemRarity ??
          row?.rarity ??
          metadata?.item_rarity ??
          metadata?.itemRarity ??
          metadata?.rarity
      ) || null,
    itemRarityColor:
      normalizeText(
        row?.item_rarity_color ??
          row?.itemRarityColor ??
          row?.rarity_color ??
          row?.rarityColor ??
          metadata?.item_rarity_color ??
          metadata?.itemRarityColor ??
          metadata?.rarity_color ??
          metadata?.rarityColor
      ) || null,
    itemImageUrl:
      normalizeText(
        row?.item_image_url ??
          row?.itemImageUrl ??
          metadata?.item_image_url ??
          metadata?.itemImageUrl
      ) || null,
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
    liquidity: resolvedVolume7d,
    liquidityBand: normalizeText(row?.liquidity_label || "Low") || "Low",
    liquidityLabel: normalizeText(row?.liquidity_label || "Low") || "Low",
    volume7d: resolvedVolume7d,
    marketCoverage: resolvedMarketCoverage == null ? 0 : resolvedMarketCoverage,
    referencePrice: resolvedReferencePrice,
    latestMarketSignalAt:
      toIsoOrNull(
        row?.market_signal_observed_at ??
        metadata?.latest_market_signal_at ??
          metadata?.latestMarketSignalAt ??
          metadata?.diagnostics_debug?.latest_market_signal_at
      ) || null,
    latest_market_signal_at:
      toIsoOrNull(
        row?.market_signal_observed_at ??
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
    marketSignalObservedAt:
      toIsoOrNull(
        row?.market_signal_observed_at ??
          metadata?.latest_market_signal_at ??
          metadata?.latestMarketSignalAt
      ) || null,
    market_signal_observed_at:
      toIsoOrNull(
        row?.market_signal_observed_at ??
          metadata?.latest_market_signal_at ??
          metadata?.latestMarketSignalAt
      ) || null,
    feedPublishedAt: toIsoOrNull(row?.feed_published_at) || null,
    feed_published_at: toIsoOrNull(row?.feed_published_at) || null,
    insightRefreshedAt: toIsoOrNull(row?.insight_refreshed_at) || null,
    insight_refreshed_at: toIsoOrNull(row?.insight_refreshed_at) || null,
    lastRefreshAttemptAt: toIsoOrNull(row?.last_refresh_attempt_at) || null,
    last_refresh_attempt_at: toIsoOrNull(row?.last_refresh_attempt_at) || null,
    latestSignalAgeHours:
      toFiniteOrNull(row?.latest_signal_age_hours ?? metadata?.publish_refresh?.latest_signal_age_hours) ??
      null,
    latest_signal_age_hours:
      toFiniteOrNull(row?.latest_signal_age_hours ?? metadata?.publish_refresh?.latest_signal_age_hours) ??
      null,
    netProfitAfterFees:
      toFiniteOrNull(row?.net_profit_after_fees) ?? toFiniteOrNull(row?.profit),
    net_profit_after_fees:
      toFiniteOrNull(row?.net_profit_after_fees) ?? toFiniteOrNull(row?.profit),
    confidenceScore:
      toFiniteOrNull(row?.confidence_score) != null
        ? clampScore(row?.confidence_score)
        : null,
    confidence_score:
      toFiniteOrNull(row?.confidence_score) != null
        ? clampScore(row?.confidence_score)
        : null,
    freshnessScore:
      toFiniteOrNull(row?.freshness_score) != null
        ? clampScore(row?.freshness_score)
        : null,
    freshness_score:
      toFiniteOrNull(row?.freshness_score) != null
        ? clampScore(row?.freshness_score)
        : null,
    verdict:
      normalizeText(row?.verdict || metadata?.publish_refresh?.verdict).toLowerCase() || null,
    refreshStatus:
      normalizeText(row?.refresh_status || metadata?.publish_refresh?.refresh_status).toLowerCase() ||
      "pending",
    refresh_status:
      normalizeText(row?.refresh_status || metadata?.publish_refresh?.refresh_status).toLowerCase() ||
      "pending",
    liveStatus:
      normalizeText(row?.live_status || metadata?.publish_refresh?.live_status).toLowerCase() ||
      "degraded",
    live_status:
      normalizeText(row?.live_status || metadata?.publish_refresh?.live_status).toLowerCase() ||
      "degraded",
    skinportQuotePrice,
    skinport_quote_price: skinportQuotePrice,
    skinportQuoteCurrency,
    skinport_quote_currency: skinportQuoteCurrency,
    skinportQuoteObservedAt,
    skinport_quote_observed_at: skinportQuoteObservedAt,
    skinportQuoteType,
    skinport_quote_type: skinportQuoteType,
    skinportItemSlug,
    skinport_item_slug: skinportItemSlug,
    skinportListingId,
    skinport_listing_id: skinportListingId,
    skinportPriceIntegrityStatus,
    skinport_price_integrity_status: skinportPriceIntegrityStatus,
    flags: Array.isArray(metadata?.flags) ? metadata.flags : [],
    badges: Array.isArray(metadata?.badges) ? metadata.badges : [],
    diagnosticsDebug:
      metadata?.diagnostics_debug && typeof metadata.diagnostics_debug === "object"
        ? metadata.diagnostics_debug
        : null,
    isHighConfidenceEligible: Boolean(metadata?.is_high_confidence_eligible),
    isRiskyEligible: Boolean(metadata?.is_risky_eligible),
    buyUrl: normalizeText(row?.buy_url || row?.buyUrl || metadata?.buy_url) || null,
    sellUrl: normalizeText(row?.sell_url || row?.sellUrl || metadata?.sell_url) || null
  }
}

module.exports = {
  buildSignature,
  buildOpportunityFingerprint,
  buildMaterialChangeHash,
  classifyOpportunityFeedEvent,
  isMateriallyNewOpportunity,
  buildFeedInsertRow,
  mapFeedRowToApiRow
}

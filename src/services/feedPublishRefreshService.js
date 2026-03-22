const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const { mapFeedRowToApiRow } = require("./scanner/feedPipeline")
const {
  evaluatePublishValidation,
  DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS
} = require("./scanner/publishValidation")
const { CATEGORY_PROFILES, ITEM_CATEGORIES } = require("./scanner/config")
const { sourceFeePercent, round2, roundPrice } = require("../markets/marketUtils")
const { deriveInsightPayloadFromOpportunity } = require("./opportunityInsightService")

const LIVE_MAX_SIGNAL_AGE_HOURS = Number(
  (DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS / (60 * 60 * 1000)).toFixed(3)
)
const ANALYZABLE_MAX_SIGNAL_AGE_HOURS = 24
const QUOTE_LOOKBACK_HOURS = 72
const SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE = "live_executable"
const SKINPORT_PRICE_INTEGRITY_CONFIRMED = "confirmed"
const SKINPORT_VALIDATION_TIER_STRICT = "strict"
const SKINPORT_VALIDATION_TIER_FALLBACK = "fallback"
const SKINPORT_PUBLISH_GATE_STAGE = "publish_gate"

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function normalizeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return fallback
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on"
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function incrementCounter(target, key, amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

function createSkinportPublishDiagnostics() {
  return {
    candidates: 0,
    admitted: 0,
    rejected: 0,
    strictValidated: 0,
    fallbackValidated: 0,
    rejectReasons: {},
    stageCounters: {
      [SKINPORT_PUBLISH_GATE_STAGE]: {
        requested: 0,
        passed: 0,
        rejected: 0
      }
    }
  }
}

function resolveLatestIso(values = []) {
  const sorted = (Array.isArray(values) ? values : [])
    .map((value) => toIsoOrNull(value))
    .filter(Boolean)
    .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())
  return sorted[0] || null
}

function computeAgeHours(iso, nowMs = Date.now()) {
  const safeIso = toIsoOrNull(iso)
  if (!safeIso) return null
  const ts = new Date(safeIso).getTime()
  if (!Number.isFinite(ts)) return null
  const ageHours = (nowMs - ts) / (60 * 60 * 1000)
  if (!Number.isFinite(ageHours) || ageHours < 0) return null
  return Number(ageHours.toFixed(3))
}

function normalizeStatus(value, fallback = "degraded") {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return fallback
  return raw
}

function toBooleanOrNull(value) {
  if (value === true) return true
  if (value === false) return false
  if (value == null || value === "") return null
  const parsed = Number(value)
  if (Number.isFinite(parsed)) {
    if (parsed === 1) return true
    if (parsed === 0) return false
  }
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return null
  if (raw === "true" || raw === "yes" || raw === "on") return true
  if (raw === "false" || raw === "no" || raw === "off") return false
  return null
}

function buildPublishValidationMetadata(validation = {}) {
  const signalAgeMs = toFiniteOrNull(validation?.signalAgeMs)
  const publishValidatedAt = toIsoOrNull(validation?.publishValidatedAt)
  const publishFreshnessState = normalizeText(validation?.publishFreshnessState) || "missing"
  const requiredRouteState = normalizeText(validation?.requiredRouteState) || "missing_buy_and_sell_route"
  const listingAvailabilityState = normalizeText(validation?.listingAvailabilityState) || "unknown"
  const staleReason = normalizeText(validation?.staleReason) || null
  const routeSignalObservedAt = toIsoOrNull(validation?.routeSignalObservedAt)
  return {
    signal_age_ms: signalAgeMs,
    signalAgeMs: signalAgeMs,
    publish_validated_at: publishValidatedAt,
    publishValidatedAt: publishValidatedAt,
    publish_freshness_state: publishFreshnessState,
    publishFreshnessState: publishFreshnessState,
    required_route_state: requiredRouteState,
    requiredRouteState: requiredRouteState,
    listing_availability_state: listingAvailabilityState,
    listingAvailabilityState: listingAvailabilityState,
    stale_reason: staleReason,
    staleReason: staleReason,
    route_signal_observed_at: routeSignalObservedAt,
    routeSignalObservedAt: routeSignalObservedAt,
    publish_validation: {
      is_publishable: Boolean(validation?.isPublishable),
      signal_age_ms: signalAgeMs,
      publish_validated_at: publishValidatedAt,
      publish_freshness_state: publishFreshnessState,
      required_route_state: requiredRouteState,
      listing_availability_state: listingAvailabilityState,
      stale_reason: staleReason,
      route_signal_observed_at: routeSignalObservedAt
    }
  }
}

function normalizeItemCategory(value) {
  const raw = normalizeText(value).toLowerCase()
  if (
    raw === ITEM_CATEGORIES.WEAPON_SKIN ||
    raw === ITEM_CATEGORIES.CASE ||
    raw === ITEM_CATEGORIES.STICKER_CAPSULE ||
    raw === ITEM_CATEGORIES.KNIFE ||
    raw === ITEM_CATEGORIES.GLOVE ||
    raw === ITEM_CATEGORIES.FUTURE_KNIFE ||
    raw === ITEM_CATEGORIES.FUTURE_GLOVE
  ) {
    return raw
  }
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function resolveLiquidityLabel(volume7d, category) {
  const value = toPositiveOrNull(volume7d)
  if (value == null) return "Low"
  const normalizedCategory = normalizeItemCategory(category)
  const profile = CATEGORY_PROFILES[normalizedCategory] || CATEGORY_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
  const minVolume = Math.max(Number(profile?.minVolume7d || 1), 1)
  if (value >= minVolume * 3) return "High"
  if (value >= minVolume * 1.5) return "Medium"
  return "Low"
}

function resolveNetSellFromQuote(quoteRow = {}, sellMarket = "", fallback = null) {
  const direct = toFiniteOrNull(quoteRow?.best_sell_net)
  if (direct != null && direct > 0) return roundPrice(direct)

  const bestSell = toFiniteOrNull(quoteRow?.best_sell)
  if (bestSell != null && bestSell > 0) {
    const fee = sourceFeePercent(sellMarket)
    return roundPrice(bestSell * (1 - fee / 100))
  }
  return toFiniteOrNull(fallback)
}

function readSkinportQuoteField(quoteRow = {}, keys = []) {
  const qualityFlags = toJsonObject(quoteRow?.quality_flags || quoteRow?.qualityFlags)
  for (const key of Array.isArray(keys) ? keys : []) {
    const fromQualityFlags = normalizeText(qualityFlags?.[key])
    if (fromQualityFlags) return fromQualityFlags
    const fromRow = normalizeText(quoteRow?.[key])
    if (fromRow) return fromRow
  }
  return ""
}

function deriveSkinportItemSlug(quoteRow = {}) {
  const direct = readSkinportQuoteField(quoteRow, [
    "skinport_item_slug",
    "item_slug",
    "itemSlug",
    "slug"
  ])
  if (direct) return direct

  const url = readSkinportQuoteField(quoteRow, [
    "skinport_item_url",
    "item_page",
    "itemPage",
    "listing_url",
    "url"
  ])
  if (!url) return null

  try {
    const parsed = new URL(url)
    const chunks = parsed.pathname.split("/").filter(Boolean)
    const itemIdx = chunks.findIndex((part) => String(part).toLowerCase() === "item")
    if (itemIdx >= 0 && chunks[itemIdx + 1]) {
      return decodeURIComponent(chunks[itemIdx + 1])
    }
  } catch (_err) {
    return null
  }
  return null
}

function normalizeComparableText(value) {
  return normalizeText(value)
    .replace(/\s+/g, " ")
    .toLowerCase()
}

function resolveSkinportQuoteItemName(quoteRow = {}) {
  return (
    readSkinportQuoteField(quoteRow, [
      "market_hash_name",
      "marketHashName",
      "item_name",
      "itemName"
    ]) || null
  )
}

function resolveMappedItemName(mapped = {}) {
  return normalizeText(mapped.marketHashName || mapped.itemName) || null
}

function isSkinportItemNameMatch(mapped = {}, quoteRow = {}) {
  const expected = normalizeComparableText(resolveMappedItemName(mapped))
  const actual = normalizeComparableText(resolveSkinportQuoteItemName(quoteRow))
  return Boolean(expected && actual && expected === actual)
}

function buildSkinportValidation(mapped = {}, buyQuote = null, sellQuote = null) {
  const buyMarket = normalizeText(mapped.buyMarket).toLowerCase()
  const sellMarket = normalizeText(mapped.sellMarket).toLowerCase()
  const side = buyMarket === "skinport" ? "buy" : sellMarket === "skinport" ? "sell" : null
  if (!side) {
    return {
      applicable: false,
      confirmed: true,
      quotePrice: null,
      quoteCurrency: null,
      quoteObservedAt: null,
      quoteType: null,
      itemSlug: null,
      listingId: null,
      priceIntegrityStatus: null,
      validationTier: null,
      fallbackApplied: false,
      quoteTypeFromFallback: false,
      integrityFromFallback: false,
      rejectReason: null
    }
  }

  const quoteRow = side === "buy" ? buyQuote : sellQuote
  const quotePrice =
    side === "buy"
      ? toFiniteOrNull(quoteRow?.best_buy)
      : toFiniteOrNull(quoteRow?.best_sell_net ?? quoteRow?.best_sell)
  const quoteCurrency =
    readSkinportQuoteField(quoteRow, [
      "skinport_quote_currency",
      "quote_currency",
      "currency"
    ]).toUpperCase() || null
  const quoteObservedAt = toIsoOrNull(
    readSkinportQuoteField(quoteRow, [
      "skinport_quote_observed_at",
      "quote_observed_at",
      "quoteObservedAt"
    ]) ||
      quoteRow?.fetched_at ||
      quoteRow?.fetchedAt
  )
  const rawQuoteType =
    normalizeText(
      readSkinportQuoteField(quoteRow, [
        "skinport_quote_type",
        "quote_type",
        "quoteType"
      ])
    ).toLowerCase() || null
  const listingId =
    readSkinportQuoteField(quoteRow, [
      "skinport_listing_id",
      "listing_id",
      "listingId",
      "id"
    ]) || null
  const itemSlug = deriveSkinportItemSlug(quoteRow)
  const rawPriceIntegrityStatus =
    normalizeText(
      readSkinportQuoteField(quoteRow, [
        "skinport_price_integrity_status",
        "price_integrity_status",
        "priceIntegrityStatus"
      ])
    ).toLowerCase() || "unconfirmed"
  const hasExecutablePrice = quotePrice != null && quotePrice > 0
  const quoteRowMarket = normalizeText(quoteRow?.market).toLowerCase()
  const itemNameMatch = isSkinportItemNameMatch(mapped, quoteRow)

  let quoteType = rawQuoteType
  let quoteTypeFromFallback = false
  if (!quoteType && hasExecutablePrice && quoteObservedAt && quoteRowMarket === "skinport") {
    quoteType = SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE
    quoteTypeFromFallback = true
  }

  let priceIntegrityStatus = rawPriceIntegrityStatus
  let integrityFromFallback = false
  if (
    priceIntegrityStatus !== SKINPORT_PRICE_INTEGRITY_CONFIRMED &&
    quoteType === SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE &&
    hasExecutablePrice &&
    quoteObservedAt &&
    itemNameMatch
  ) {
    priceIntegrityStatus = SKINPORT_PRICE_INTEGRITY_CONFIRMED
    integrityFromFallback = true
  }

  let rejectReason = null
  if (!hasExecutablePrice) {
    rejectReason = "missing_executable_price"
  } else if (!quoteObservedAt) {
    rejectReason = "missing_quote_observed_at"
  } else if (quoteType !== SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE) {
    rejectReason = "quote_type_not_live_executable"
  } else if (priceIntegrityStatus !== SKINPORT_PRICE_INTEGRITY_CONFIRMED) {
    rejectReason = "integrity_unconfirmed"
  }

  const confirmed = rejectReason == null
  const fallbackApplied = confirmed && (quoteTypeFromFallback || integrityFromFallback)
  const validationTier = confirmed
    ? fallbackApplied
      ? SKINPORT_VALIDATION_TIER_FALLBACK
      : SKINPORT_VALIDATION_TIER_STRICT
    : null

  return {
    applicable: true,
    side,
    confirmed,
    quotePrice: quotePrice == null ? null : roundPrice(quotePrice),
    quoteCurrency,
    quoteObservedAt,
    quoteType,
    itemSlug,
    listingId,
    priceIntegrityStatus,
    validationTier,
    fallbackApplied,
    quoteTypeFromFallback,
    integrityFromFallback,
    rejectReason
  }
}

function resolveRefreshOutcome({
  ageHours,
  netProfitAfterFees,
  verdict
} = {}) {
  if (ageHours == null) {
    return {
      refreshStatus: "failed",
      liveStatus: "degraded",
      admission: "suppressed"
    }
  }

  if ((netProfitAfterFees || 0) <= 0 || normalizeText(verdict).toLowerCase() === "skip") {
    return {
      refreshStatus: "degraded",
      liveStatus: "degraded",
      admission: "suppressed"
    }
  }

  if (ageHours <= LIVE_MAX_SIGNAL_AGE_HOURS) {
    return {
      refreshStatus: "ok",
      liveStatus: "live",
      admission: "live"
    }
  }

  if (ageHours <= ANALYZABLE_MAX_SIGNAL_AGE_HOURS) {
    return {
      refreshStatus: "stale",
      liveStatus: "stale",
      admission: "risky"
    }
  }

  return {
    refreshStatus: "stale",
    liveStatus: "degraded",
    admission: "suppressed"
  }
}

function buildRefreshedOpportunityRow(rawRow = {}, quoteRowsByItem = {}, options = {}) {
  const nowIso = options.nowIso || new Date().toISOString()
  const nowMs = options.nowMs || Date.now()
  const mapped = mapFeedRowToApiRow(rawRow)
  const metadata =
    rawRow?.metadata && typeof rawRow.metadata === "object" && !Array.isArray(rawRow.metadata)
      ? rawRow.metadata
      : {}
  const itemName = normalizeText(mapped.marketHashName || mapped.itemName)
  const itemQuotes = quoteRowsByItem?.[itemName] || {}
  const buyMarket = normalizeText(mapped.buyMarket).toLowerCase()
  const sellMarket = normalizeText(mapped.sellMarket).toLowerCase()
  const buyQuote = itemQuotes?.[buyMarket] || null
  const sellQuote = itemQuotes?.[sellMarket] || null
  const skinportValidation = buildSkinportValidation(mapped, buyQuote, sellQuote)

  const buyPriceFromQuote =
    toFiniteOrNull(buyQuote?.best_buy) != null && Number(buyQuote.best_buy) > 0
      ? roundPrice(Number(buyQuote.best_buy))
      : null
  const buyPriceRefreshed =
    buyMarket === "skinport"
      ? skinportValidation.confirmed
        ? buyPriceFromQuote
        : null
      : buyPriceFromQuote != null
        ? buyPriceFromQuote
        : toFiniteOrNull(mapped.buyPrice)

  const sellNetFromQuote = resolveNetSellFromQuote(sellQuote, sellMarket, null)
  const sellNetRefreshed =
    sellMarket === "skinport"
      ? skinportValidation.confirmed
        ? sellNetFromQuote
        : null
      : sellNetFromQuote != null
        ? sellNetFromQuote
        : toFiniteOrNull(mapped.sellNet)

  let netProfitAfterFees =
    buyPriceRefreshed != null && sellNetRefreshed != null
      ? roundPrice(sellNetRefreshed - buyPriceRefreshed)
      : toFiniteOrNull(mapped.profit)
  if (skinportValidation.applicable && !skinportValidation.confirmed) {
    netProfitAfterFees = null
  }

  let spreadPct =
    buyPriceRefreshed != null &&
    buyPriceRefreshed > 0 &&
    netProfitAfterFees != null
      ? round2((netProfitAfterFees / buyPriceRefreshed) * 100)
      : toFiniteOrNull(mapped.spread)
  if (skinportValidation.applicable && !skinportValidation.confirmed) {
    spreadPct = null
  }

  const buyRouteUpdatedAt = toIsoOrNull(
    buyQuote?.fetched_at || buyQuote?.updated_at || buyQuote?.updatedAt
  )
  const sellRouteUpdatedAt = toIsoOrNull(
    sellQuote?.fetched_at || sellQuote?.updated_at || sellQuote?.updatedAt
  )
  const buyRouteAvailable = buyPriceFromQuote != null && Number(buyPriceFromQuote) > 0
  const sellRouteAvailable = sellNetFromQuote != null && Number(sellNetFromQuote) > 0
  const buyListingAvailable =
    buyMarket === "skinport"
      ? Boolean(skinportValidation.confirmed)
      : toBooleanOrNull(
          mapped?.buyListingAvailable ??
            mapped?.buy_listing_available ??
            metadata?.buy_listing_available ??
            metadata?.buyListingAvailable
        )
  const sellListingAvailable =
    sellMarket === "skinport"
      ? Boolean(skinportValidation.confirmed)
      : toBooleanOrNull(
          mapped?.sellListingAvailable ??
            mapped?.sell_listing_available ??
            metadata?.sell_listing_available ??
            metadata?.sellListingAvailable
        )
  const publishValidation = evaluatePublishValidation({
    nowIso,
    nowMs,
    maxSignalAgeMs: LIVE_MAX_SIGNAL_AGE_HOURS * 60 * 60 * 1000,
    buyMarket,
    sellMarket,
    buyRouteAvailable,
    sellRouteAvailable,
    buyRouteUpdatedAt,
    sellRouteUpdatedAt,
    buyListingAvailable,
    sellListingAvailable
  })
  const publishValidationMetadata = buildPublishValidationMetadata(publishValidation)
  const marketSignalObservedAt = publishValidation.routeSignalObservedAt || null
  const latestSignalAgeHours =
    publishValidation.signalAgeMs == null
      ? null
      : Number((Number(publishValidation.signalAgeMs) / (60 * 60 * 1000)).toFixed(3))
  const sellVolume7d =
    toPositiveOrNull(sellQuote?.volume_7d) ??
    toPositiveOrNull(mapped.sellVolume7d ?? mapped.sell_volume_7d) ??
    null
  const buyVolume7d =
    toPositiveOrNull(buyQuote?.volume_7d) ??
    toPositiveOrNull(mapped.buyVolume7d ?? mapped.buy_volume_7d) ??
    null
  const marketMaxVolume7d = Object.values(itemQuotes || {})
    .map((row) => toPositiveOrNull(row?.volume_7d))
    .filter((value) => value != null)
    .sort((a, b) => b - a)[0]
  const mappedVolume7d = toPositiveOrNull(mapped.volume7d)
  const selectedLiquiditySignal = [
    { source: "sell_quote", value: sellVolume7d },
    { source: "market_max_quote", value: marketMaxVolume7d },
    { source: "buy_quote", value: buyVolume7d },
    { source: "mapped_volume", value: mappedVolume7d }
  ].find((entry) => entry.value != null)
  const volume7d = selectedLiquiditySignal?.value ?? null
  const liquiditySource = selectedLiquiditySignal?.source || null
  const liquidityLabel = resolveLiquidityLabel(volume7d, mapped.itemCategory || rawRow?.category)

  const insight = deriveInsightPayloadFromOpportunity({
    ...mapped,
    buyPrice: buyPriceRefreshed,
    sellNet: sellNetRefreshed,
    profit: netProfitAfterFees,
    spread: spreadPct,
    volume7d,
    liquidity: volume7d,
    latestMarketSignalAt: marketSignalObservedAt,
    latestQuoteAt: marketSignalObservedAt,
    staleResult: publishValidation.publishFreshnessState !== "fresh"
  })

  const baseOutcome = resolveRefreshOutcome({
    ageHours: latestSignalAgeHours,
    netProfitAfterFees,
    verdict: insight?.verdict
  })
  const publishBlocked = !publishValidation.isPublishable
  const outcome = publishBlocked
    ? {
        refreshStatus:
          publishValidation.publishFreshnessState === "stale" ? "stale" : "degraded",
        liveStatus:
          publishValidation.publishFreshnessState === "stale" ? "stale" : "degraded",
        admission: "suppressed"
      }
    : baseOutcome

  const discoveredAt =
    toIsoOrNull(rawRow?.discovered_at) ||
    toIsoOrNull(mapped.detectedAt) ||
    nowIso

  const mergedMetadata = {
    ...metadata,
    latest_market_signal_at: marketSignalObservedAt || null,
    latest_quote_at: marketSignalObservedAt || null,
    buy_route_updated_at: buyRouteUpdatedAt || null,
    sell_route_updated_at: sellRouteUpdatedAt || null,
    buy_route_available: Boolean(buyRouteAvailable),
    sell_route_available: Boolean(sellRouteAvailable),
    buy_listing_available: buyListingAvailable == null ? null : Boolean(buyListingAvailable),
    sell_listing_available: sellListingAvailable == null ? null : Boolean(sellListingAvailable),
    volume_7d: volume7d == null ? metadata?.volume_7d || null : round2(volume7d),
    liquidity_value: volume7d == null ? metadata?.liquidity_value || null : round2(volume7d),
    sell_volume_7d:
      sellVolume7d == null ? metadata?.sell_volume_7d || metadata?.sellVolume7d || null : round2(sellVolume7d),
    sellVolume7d:
      sellVolume7d == null ? metadata?.sellVolume7d || metadata?.sell_volume_7d || null : round2(sellVolume7d),
    buy_volume_7d:
      buyVolume7d == null ? metadata?.buy_volume_7d || metadata?.buyVolume7d || null : round2(buyVolume7d),
    buyVolume7d:
      buyVolume7d == null ? metadata?.buyVolume7d || metadata?.buy_volume_7d || null : round2(buyVolume7d),
    market_max_volume_7d:
      marketMaxVolume7d == null
        ? metadata?.market_max_volume_7d || metadata?.marketMaxVolume7d || null
        : round2(marketMaxVolume7d),
    marketMaxVolume7d:
      marketMaxVolume7d == null
        ? metadata?.marketMaxVolume7d || metadata?.market_max_volume_7d || null
        : round2(marketMaxVolume7d),
    liquidity_source: normalizeText(liquiditySource) || metadata?.liquidity_source || null,
    liquiditySource: normalizeText(liquiditySource) || metadata?.liquiditySource || null,
    skinport_quote_price: skinportValidation.quotePrice,
    skinport_quote_currency: skinportValidation.quoteCurrency,
    skinport_quote_observed_at: skinportValidation.quoteObservedAt,
    skinport_quote_type: skinportValidation.quoteType,
    skinport_item_slug: skinportValidation.itemSlug,
    skinport_listing_id: skinportValidation.listingId,
    skinport_price_integrity_status: skinportValidation.priceIntegrityStatus,
    skinport_validation_tier: skinportValidation.validationTier,
    skinport_validation_reject_reason: skinportValidation.rejectReason,
    skinport_fallback_applied: Boolean(skinportValidation.fallbackApplied),
    ...publishValidationMetadata,
    publish_refresh: {
      refreshed_at: nowIso,
      refresh_status: outcome.refreshStatus,
      live_status: outcome.liveStatus,
      latest_signal_age_hours: latestSignalAgeHours,
      admission: outcome.admission,
      verdict: normalizeText(insight?.verdict).toLowerCase() || null,
      publish_freshness_state: publishValidation.publishFreshnessState,
      required_route_state: publishValidation.requiredRouteState,
      listing_availability_state: publishValidation.listingAvailabilityState,
      stale_reason: publishValidation.staleReason || null
    }
  }

  const refreshedRaw = {
    ...rawRow,
    buy_price: buyPriceRefreshed == null ? rawRow?.buy_price : buyPriceRefreshed,
    sell_net: sellNetRefreshed == null ? rawRow?.sell_net : sellNetRefreshed,
    profit: netProfitAfterFees == null ? rawRow?.profit : netProfitAfterFees,
    spread_pct: spreadPct == null ? rawRow?.spread_pct : spreadPct,
    discovered_at: discoveredAt,
    market_signal_observed_at: marketSignalObservedAt,
    feed_published_at: nowIso,
    insight_refreshed_at: nowIso,
    last_refresh_attempt_at: nowIso,
    latest_signal_age_hours: latestSignalAgeHours,
    net_profit_after_fees:
      netProfitAfterFees == null ? rawRow?.net_profit_after_fees : netProfitAfterFees,
    confidence_score: insight?.confidence_score ?? null,
    freshness_score: insight?.freshness_score ?? null,
    verdict: normalizeText(insight?.verdict).toLowerCase() || null,
    liquidity_label: liquidityLabel,
    refresh_status: outcome.refreshStatus,
    live_status: outcome.liveStatus,
    metadata: mergedMetadata
  }

  const refreshedApi = {
    ...mapFeedRowToApiRow(refreshedRaw),
    discoveredAt: discoveredAt,
    marketSignalObservedAt: marketSignalObservedAt,
    feedPublishedAt: nowIso,
    insightRefreshedAt: nowIso,
    lastRefreshAttemptAt: nowIso,
    latestSignalAgeHours,
    netProfitAfterFees,
    confidenceScore: insight?.confidence_score ?? null,
    freshnessScore: insight?.freshness_score ?? null,
    verdict: normalizeText(insight?.verdict).toLowerCase() || null,
    refreshStatus: outcome.refreshStatus,
    liveStatus: outcome.liveStatus,
    admission: outcome.admission,
    signalAgeMs:
      publishValidation.signalAgeMs == null ? null : Number(publishValidation.signalAgeMs),
    signal_age_ms:
      publishValidation.signalAgeMs == null ? null : Number(publishValidation.signalAgeMs),
    publishValidatedAt: publishValidation.publishValidatedAt || null,
    publish_validated_at: publishValidation.publishValidatedAt || null,
    publishFreshnessState: publishValidation.publishFreshnessState || "missing",
    publish_freshness_state: publishValidation.publishFreshnessState || "missing",
    requiredRouteState: publishValidation.requiredRouteState || "missing_buy_and_sell_route",
    required_route_state: publishValidation.requiredRouteState || "missing_buy_and_sell_route",
    listingAvailabilityState: publishValidation.listingAvailabilityState || "unknown",
    listing_availability_state: publishValidation.listingAvailabilityState || "unknown",
    staleReason: publishValidation.staleReason || null,
    stale_reason: publishValidation.staleReason || null,
    requiredRoutePublishable: Boolean(publishValidation.isPublishable),
    required_route_publishable: Boolean(publishValidation.isPublishable),
    skinportQuotePrice: skinportValidation.quotePrice,
    skinport_quote_price: skinportValidation.quotePrice,
    skinportQuoteCurrency: skinportValidation.quoteCurrency,
    skinport_quote_currency: skinportValidation.quoteCurrency,
    skinportQuoteObservedAt: skinportValidation.quoteObservedAt,
    skinport_quote_observed_at: skinportValidation.quoteObservedAt,
    skinportQuoteType: skinportValidation.quoteType,
    skinport_quote_type: skinportValidation.quoteType,
    skinportItemSlug: skinportValidation.itemSlug,
    skinport_item_slug: skinportValidation.itemSlug,
    skinportListingId: skinportValidation.listingId,
    skinport_listing_id: skinportValidation.listingId,
    skinportPriceIntegrityStatus: skinportValidation.priceIntegrityStatus,
    skinport_price_integrity_status: skinportValidation.priceIntegrityStatus,
    skinportValidationTier: skinportValidation.validationTier,
    skinport_validation_tier: skinportValidation.validationTier,
    skinportValidationRejectReason: skinportValidation.rejectReason,
    skinport_validation_reject_reason: skinportValidation.rejectReason,
    skinportFallbackApplied: Boolean(skinportValidation.fallbackApplied),
    skinport_fallback_applied: Boolean(skinportValidation.fallbackApplied),
    reasonSummary: insight?.reason_summary || null,
    whyThisTradeExists: insight?.why_this_trade_exists || null,
    whatCanBreakIt: insight?.what_can_break_it || null,
    whyExitMayBeEasyOrHard: insight?.why_exit_may_be_easy_or_hard || null
  }

  return {
    raw: refreshedRaw,
    api: refreshedApi,
    patch: {
      buy_price: refreshedRaw.buy_price,
      sell_net: refreshedRaw.sell_net,
      profit: refreshedRaw.profit,
      spread_pct: refreshedRaw.spread_pct,
      discovered_at: refreshedRaw.discovered_at,
      market_signal_observed_at: refreshedRaw.market_signal_observed_at,
      feed_published_at: refreshedRaw.feed_published_at,
      insight_refreshed_at: refreshedRaw.insight_refreshed_at,
      last_refresh_attempt_at: refreshedRaw.last_refresh_attempt_at,
      latest_signal_age_hours: refreshedRaw.latest_signal_age_hours,
      net_profit_after_fees: refreshedRaw.net_profit_after_fees,
      confidence_score: refreshedRaw.confidence_score,
      freshness_score: refreshedRaw.freshness_score,
      verdict: refreshedRaw.verdict,
      liquidity_label: refreshedRaw.liquidity_label,
      refresh_status: refreshedRaw.refresh_status,
      live_status: refreshedRaw.live_status,
      metadata: refreshedRaw.metadata
    }
  }
}

function evaluateFeedAdmission(row = {}, options = {}) {
  const includeRisky = normalizeBoolean(options.includeRisky, false)
  const refreshStatus = normalizeStatus(row.refreshStatus || row.refresh_status, "degraded")
  const liveStatus = normalizeStatus(row.liveStatus || row.live_status, "degraded")
  const buyMarket = normalizeText(row.buyMarket || row.buy_market).toLowerCase()
  const sellMarket = normalizeText(row.sellMarket || row.sell_market).toLowerCase()
  const skinportApplicable = buyMarket === "skinport" || sellMarket === "skinport"
  const skinportQuoteType = normalizeText(
    row.skinportQuoteType ?? row.skinport_quote_type
  ).toLowerCase()
  const skinportIntegrityStatus = normalizeText(
    row.skinportPriceIntegrityStatus ?? row.skinport_price_integrity_status
  ).toLowerCase()
  const ageHours =
    toFiniteOrNull(row.latestSignalAgeHours ?? row.latest_signal_age_hours) ?? null
  const analyzable =
    (toFiniteOrNull(row.netProfitAfterFees ?? row.net_profit_after_fees ?? row.profit) || 0) > 0 &&
    ageHours != null &&
    ageHours <= ANALYZABLE_MAX_SIGNAL_AGE_HOURS

  if (skinportApplicable) {
    const validationRejectReason = normalizeText(
      row.skinportValidationRejectReason ?? row.skinport_validation_reject_reason
    ).toLowerCase()
    const skinportQuoteObservedAt = toIsoOrNull(
      row.skinportQuoteObservedAt ?? row.skinport_quote_observed_at
    )
    const skinportQuoteAgeHours = computeAgeHours(skinportQuoteObservedAt)
    const isLiveSkinport =
      liveStatus === "live" &&
      refreshStatus === "ok" &&
      ageHours != null &&
      ageHours <= LIVE_MAX_SIGNAL_AGE_HOURS &&
      skinportQuoteAgeHours != null &&
      skinportQuoteAgeHours <= LIVE_MAX_SIGNAL_AGE_HOURS
    if (skinportQuoteType !== SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE) {
      return {
        admit: false,
        stage: SKINPORT_PUBLISH_GATE_STAGE,
        reason: validationRejectReason || "quote_type_not_live_executable",
        skinportApplicable: true
      }
    }
    if (skinportIntegrityStatus !== SKINPORT_PRICE_INTEGRITY_CONFIRMED) {
      return {
        admit: false,
        stage: SKINPORT_PUBLISH_GATE_STAGE,
        reason: validationRejectReason || "integrity_unconfirmed",
        skinportApplicable: true
      }
    }
    if (!isLiveSkinport) {
      const reason =
        liveStatus !== "live" || refreshStatus !== "ok"
          ? "not_live_refresh_status"
          : skinportQuoteAgeHours == null || skinportQuoteAgeHours > LIVE_MAX_SIGNAL_AGE_HOURS
            ? "stale_skinport_quote"
            : "stale_market_signal"
      return {
        admit: false,
        stage: SKINPORT_PUBLISH_GATE_STAGE,
        reason,
        skinportApplicable: true
      }
    }
    return {
      admit: true,
      stage: SKINPORT_PUBLISH_GATE_STAGE,
      reason: "confirmed_live_skinport_quote",
      skinportApplicable: true
    }
  }

  if (
    liveStatus === "live" &&
    refreshStatus === "ok" &&
    ageHours != null &&
    ageHours <= LIVE_MAX_SIGNAL_AGE_HOURS
  ) {
    return {
      admit: true,
      stage: "general",
      reason: "live_signal_ok",
      skinportApplicable: false
    }
  }
  if (includeRisky && liveStatus === "stale" && analyzable) {
    return {
      admit: true,
      stage: "general",
      reason: "stale_but_analyzable",
      skinportApplicable: false
    }
  }
  return {
    admit: false,
    stage: "general",
    reason: "not_admissible",
    skinportApplicable: false
  }
}

function shouldAdmitToFeed(row = {}, options = {}) {
  return Boolean(evaluateFeedAdmission(row, options).admit)
}

async function refreshForFeedPublish(feedRows = [], options = {}) {
  const rows = Array.isArray(feedRows) ? feedRows : []
  const nowIso = new Date().toISOString()
  const nowMs = Date.now()
  if (!rows.length) {
    return {
      rows: [],
      diagnostics: {
        attempted: 0,
        refreshed: 0,
        admitted: 0,
        live: 0,
        stale: 0,
        degraded: 0,
        suppressed: 0,
        quoteRowsFound: 0,
        skinportPipeline: createSkinportPublishDiagnostics()
      }
    }
  }

  const mapped = rows.map((row) => mapFeedRowToApiRow(row))
  const itemNames = Array.from(
    new Set(
      mapped
        .map((row) => normalizeText(row.marketHashName || row.itemName))
        .filter(Boolean)
    )
  )
  const skinportItemNames = Array.from(
    new Set(
      mapped
        .filter((row) => {
          const buyMarket = normalizeText(row.buyMarket || row.buy_market).toLowerCase()
          const sellMarket = normalizeText(row.sellMarket || row.sell_market).toLowerCase()
          return buyMarket === "skinport" || sellMarket === "skinport"
        })
        .map((row) => normalizeText(row.marketHashName || row.itemName))
        .filter(Boolean)
    )
  )

  let quoteRowsByItem = {}
  try {
    quoteRowsByItem = await marketQuoteRepo.getLatestRowsByItemNames(itemNames, {
      lookbackHours: Number(options.lookbackHours || QUOTE_LOOKBACK_HOURS)
    })
  } catch (_err) {
    quoteRowsByItem = {}
  }
  if (skinportItemNames.length) {
    try {
      const skinportRowsByItem = await marketQuoteRepo.getLatestRowsByItemNames(
        skinportItemNames,
        {
          lookbackHours: Number(options.lookbackHours || QUOTE_LOOKBACK_HOURS),
          markets: ["skinport"],
          includeQualityFlags: true,
          useRpc: false
        }
      )
      for (const [itemName, marketRows] of Object.entries(skinportRowsByItem || {})) {
        if (!quoteRowsByItem[itemName]) {
          quoteRowsByItem[itemName] = {}
        }
        if (marketRows?.skinport) {
          quoteRowsByItem[itemName].skinport = marketRows.skinport
        }
      }
    } catch (_err) {
      // Fall back to the base quote map when quality-flag refresh fails.
    }
  }

  const refreshedRows = rows.map((row) =>
    buildRefreshedOpportunityRow(row, quoteRowsByItem, { nowIso, nowMs })
  )

  const skinportPipeline = createSkinportPublishDiagnostics()
  const publishValidationDiagnostics = {
    blocked: 0,
    deactivated: 0,
    reasons: {}
  }
  const admittedRows = []
  const patchRows = []
  for (const refreshed of refreshedRows) {
    const row = refreshed?.api || {}
    const decision = evaluateFeedAdmission(row, { includeRisky: options.includeRisky })
    const publishBlocked = !Boolean(
      row?.requiredRoutePublishable ?? row?.required_route_publishable
    )
    const publishReason =
      normalizeText(row?.staleReason ?? row?.stale_reason) || "publish_validation_failed"
    const finalAdmit = decision.admit && !publishBlocked

    if (publishBlocked) {
      publishValidationDiagnostics.blocked += 1
      incrementCounter(publishValidationDiagnostics.reasons, publishReason)
      if (Boolean(refreshed?.raw?.is_active)) {
        publishValidationDiagnostics.deactivated += 1
      }
    }

    if (decision.skinportApplicable) {
      skinportPipeline.candidates += 1
      skinportPipeline.stageCounters[SKINPORT_PUBLISH_GATE_STAGE].requested += 1
      const validationTier = normalizeText(
        row.skinportValidationTier ?? row.skinport_validation_tier
      ).toLowerCase()
      if (validationTier === SKINPORT_VALIDATION_TIER_STRICT) {
        skinportPipeline.strictValidated += 1
      } else if (validationTier === SKINPORT_VALIDATION_TIER_FALLBACK) {
        skinportPipeline.fallbackValidated += 1
      }
      if (finalAdmit) {
        skinportPipeline.admitted += 1
        skinportPipeline.stageCounters[SKINPORT_PUBLISH_GATE_STAGE].passed += 1
      } else {
        skinportPipeline.rejected += 1
        skinportPipeline.stageCounters[SKINPORT_PUBLISH_GATE_STAGE].rejected += 1
        incrementCounter(
          skinportPipeline.rejectReasons,
          publishBlocked ? publishReason : decision.reason || "rejected"
        )
      }
    }
    if (finalAdmit) {
      admittedRows.push(row)
    }

    if (normalizeText(refreshed?.raw?.id)) {
      const patch = {
        ...refreshed.patch
      }
      if (publishBlocked) {
        patch.is_active = false
        patch.refresh_status = normalizeStatus(row.refreshStatus || row.refresh_status, "degraded")
        patch.live_status = normalizeStatus(row.liveStatus || row.live_status, "degraded")
      }
      patchRows.push({
        id: refreshed.raw.id,
        patch
      })
    }
  }

  if (options.persist !== false && patchRows.length) {
    await arbitrageFeedRepo.updatePublishRefreshState(patchRows).catch(() => 0)
  }

  const liveCount = refreshedRows.filter(
    (row) => normalizeStatus(row.api.liveStatus, "degraded") === "live"
  ).length
  const staleCount = refreshedRows.filter(
    (row) => normalizeStatus(row.api.liveStatus, "degraded") === "stale"
  ).length
  const degradedCount = refreshedRows.filter(
    (row) => normalizeStatus(row.api.liveStatus, "degraded") === "degraded"
  ).length
  const quoteRowsFound = Object.values(quoteRowsByItem || {}).reduce((sum, itemMarkets) => {
    return sum + Object.keys(itemMarkets || {}).length
  }, 0)

  return {
    rows: admittedRows,
    diagnostics: {
      attempted: rows.length,
      refreshed: refreshedRows.length,
      admitted: admittedRows.length,
      live: liveCount,
      stale: staleCount,
      degraded: degradedCount,
      suppressed: Math.max(refreshedRows.length - admittedRows.length, 0),
      quoteRowsFound,
      publishValidation: publishValidationDiagnostics,
      skinportPipeline
    }
  }
}

exports.LIVE_MAX_SIGNAL_AGE_HOURS = LIVE_MAX_SIGNAL_AGE_HOURS
exports.ANALYZABLE_MAX_SIGNAL_AGE_HOURS = ANALYZABLE_MAX_SIGNAL_AGE_HOURS
exports.refreshForFeedPublish = refreshForFeedPublish
exports.runFeedPublishRefreshJob = refreshForFeedPublish

exports.__testables = {
  computeAgeHours,
  resolveRefreshOutcome,
  buildSkinportValidation,
  evaluateFeedAdmission,
  shouldAdmitToFeed,
  buildRefreshedOpportunityRow
}

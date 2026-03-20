const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const { mapFeedRowToApiRow } = require("./scanner/feedPipeline")
const { sourceFeePercent, round2, roundPrice } = require("../markets/marketUtils")
const { deriveInsightPayloadFromOpportunity } = require("./opportunityInsightService")

const LIVE_MAX_SIGNAL_AGE_HOURS = 2
const ANALYZABLE_MAX_SIGNAL_AGE_HOURS = 24
const QUOTE_LOOKBACK_HOURS = 72
const SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE = "live_executable"
const SKINPORT_PRICE_INTEGRITY_CONFIRMED = "confirmed"

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
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
      priceIntegrityStatus: null
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
  const quoteType =
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
  const priceIntegrityStatus =
    normalizeText(
      readSkinportQuoteField(quoteRow, [
        "skinport_price_integrity_status",
        "price_integrity_status",
        "priceIntegrityStatus"
      ])
    ).toLowerCase() || "unconfirmed"
  const hasExecutablePrice = quotePrice != null && quotePrice > 0
  const confirmed =
    hasExecutablePrice &&
    quoteObservedAt != null &&
    quoteType === SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE &&
    priceIntegrityStatus === SKINPORT_PRICE_INTEGRITY_CONFIRMED

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
    priceIntegrityStatus
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

  const marketSignalObservedAt = resolveLatestIso([
    buyQuote?.fetched_at,
    sellQuote?.fetched_at,
    rawRow?.market_signal_observed_at,
    metadata?.latest_market_signal_at,
    mapped.latestMarketSignalAt,
    mapped.detectedAt
  ])
  const latestSignalAgeHours = computeAgeHours(marketSignalObservedAt, nowMs)
  const volume7d = [
    toFiniteOrNull(sellQuote?.volume_7d),
    toFiniteOrNull(buyQuote?.volume_7d),
    toFiniteOrNull(mapped.volume7d)
  ].find((value) => value != null && value >= 0)

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
    staleResult:
      latestSignalAgeHours == null ? true : latestSignalAgeHours > LIVE_MAX_SIGNAL_AGE_HOURS
  })

  const outcome = resolveRefreshOutcome({
    ageHours: latestSignalAgeHours,
    netProfitAfterFees,
    verdict: insight?.verdict
  })

  const discoveredAt =
    toIsoOrNull(rawRow?.discovered_at) ||
    toIsoOrNull(mapped.detectedAt) ||
    nowIso

  const mergedMetadata = {
    ...metadata,
    latest_market_signal_at: marketSignalObservedAt || metadata?.latest_market_signal_at || null,
    latest_quote_at: marketSignalObservedAt || metadata?.latest_quote_at || null,
    volume_7d: volume7d == null ? metadata?.volume_7d || null : round2(volume7d),
    skinport_quote_price: skinportValidation.quotePrice,
    skinport_quote_currency: skinportValidation.quoteCurrency,
    skinport_quote_observed_at: skinportValidation.quoteObservedAt,
    skinport_quote_type: skinportValidation.quoteType,
    skinport_item_slug: skinportValidation.itemSlug,
    skinport_listing_id: skinportValidation.listingId,
    skinport_price_integrity_status: skinportValidation.priceIntegrityStatus,
    publish_refresh: {
      refreshed_at: nowIso,
      refresh_status: outcome.refreshStatus,
      live_status: outcome.liveStatus,
      latest_signal_age_hours: latestSignalAgeHours,
      admission: outcome.admission,
      verdict: normalizeText(insight?.verdict).toLowerCase() || null
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
      refresh_status: refreshedRaw.refresh_status,
      live_status: refreshedRaw.live_status,
      metadata: refreshedRaw.metadata
    }
  }
}

function shouldAdmitToFeed(row = {}, options = {}) {
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
    if (
      skinportQuoteType !== SKINPORT_QUOTE_TYPE_LIVE_EXECUTABLE ||
      skinportIntegrityStatus !== SKINPORT_PRICE_INTEGRITY_CONFIRMED ||
      !isLiveSkinport
    ) {
      return false
    }
    return true
  }

  if (
    liveStatus === "live" &&
    refreshStatus === "ok" &&
    ageHours != null &&
    ageHours <= LIVE_MAX_SIGNAL_AGE_HOURS
  ) {
    return true
  }
  if (includeRisky && liveStatus === "stale" && analyzable) {
    return true
  }
  return false
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
        quoteRowsFound: 0
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

  if (options.persist !== false) {
    const patchRows = refreshedRows
      .filter((row) => normalizeText(row?.raw?.id))
      .map((row) => ({
        id: row.raw.id,
        patch: row.patch
      }))
    if (patchRows.length) {
      await arbitrageFeedRepo.updatePublishRefreshState(patchRows).catch(() => 0)
    }
  }

  const admittedRows = refreshedRows
    .map((row) => row.api)
    .filter((row) => shouldAdmitToFeed(row, { includeRisky: options.includeRisky }))

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
      quoteRowsFound
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
  shouldAdmitToFeed,
  buildRefreshedOpportunityRow
}

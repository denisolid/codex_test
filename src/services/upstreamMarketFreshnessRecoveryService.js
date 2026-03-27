const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const marketSnapshotRepo = require("../repositories/marketSnapshotRepository")
const marketPriceRepo = require("../repositories/marketPriceRepository")
const skinRepo = require("../repositories/skinRepository")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const marketService = require("./marketService")
const steamMarket = require("../markets/steam.market")
const skinportMarket = require("../markets/skinport.market")
const csfloatMarket = require("../markets/csfloat.market")
const dmarketMarket = require("../markets/dmarket.market")
const {
  arbitrageDefaultUniverseLimit,
  arbitrageMaxConcurrentMarketRequests,
  marketCompareTimeoutMs,
  marketCompareMaxRetries
} = require("../config/env")

const RECOVERY_CATEGORIES = Object.freeze(["weapon_skin", "case", "sticker_capsule"])
const RECOVERY_SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"])
const RECOVERY_SOURCES = Object.freeze({
  steam: steamMarket,
  skinport: skinportMarket,
  csfloat: csfloatMarket,
  dmarket: dmarketMarket
})
const DEFAULT_TARGET_LIMIT = Math.max(
  Math.min(Number(arbitrageDefaultUniverseLimit || 3000), 900),
  150
)
const DEFAULT_QUOTE_BATCH_SIZE = 80
const DEFAULT_SNAPSHOT_BATCH_SIZE = 60
const DEFAULT_HEALTH_WINDOW_HOURS = 2
const DEFAULT_QUOTE_LOOKBACK_HOURS = 24 * 14
const DEFAULT_REFRESH_CONCURRENCY = Math.max(Number(arbitrageMaxConcurrentMarketRequests || 4), 1)
const CATEGORY_REFRESH_TARGETS = Object.freeze({
  weapon_skin: Object.freeze({ share: 0.8, min: 120 }),
  case: Object.freeze({ share: 0.1, min: 25 }),
  sticker_capsule: Object.freeze({ share: 0.1, min: 25 })
})
const CATEGORY_HEALTH_REQUIREMENTS = Object.freeze({
  weapon_skin: Object.freeze({
    minFreshCoverageRows: 40,
    minFreshCoverageRate: 0.05,
    requireSnapshots: true,
    minFreshSnapshots: 25,
    minFreshSnapshotRate: 0.05
  }),
  case: Object.freeze({
    minFreshCoverageRows: 5,
    minFreshCoverageRate: 0.05,
    requireSnapshots: false,
    minFreshSnapshots: 0,
    minFreshSnapshotRate: 0
  }),
  sticker_capsule: Object.freeze({
    minFreshCoverageRows: 5,
    minFreshCoverageRate: 0.05,
    requireSnapshots: false,
    minFreshSnapshots: 0,
    minFreshSnapshotRate: 0
  })
})

function normalizeText(value) {
  return String(value || "").trim()
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIntegerOrNull(value, min = 0) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return Math.max(Math.round(parsed), min)
}

function chunkArray(values = [], chunkSize = 100) {
  const rows = Array.isArray(values) ? values : []
  const size = Math.max(Number(chunkSize || 0), 1)
  const chunks = []
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size))
  }
  return chunks
}

function isFreshWithinHours(isoValue, hours = DEFAULT_HEALTH_WINDOW_HOURS, nowMs = Date.now()) {
  const iso = toIsoOrNull(isoValue)
  if (!iso) return false
  const ageMs = nowMs - new Date(iso).getTime()
  if (!Number.isFinite(ageMs) || ageMs < 0) return false
  return ageMs <= Math.max(Number(hours || 0), 1) * 60 * 60 * 1000
}

function pickLatestIso(...values) {
  let latest = null
  let latestTs = Number.NaN
  for (const value of values) {
    const iso = toIsoOrNull(value)
    if (!iso) continue
    const ts = new Date(iso).getTime()
    if (!Number.isFinite(ts)) continue
    if (!Number.isFinite(latestTs) || ts > latestTs) {
      latest = iso
      latestTs = ts
    }
  }
  return latest
}

function updateFreshnessRange(target = {}, isoValue = null) {
  const iso = toIsoOrNull(isoValue)
  if (!iso) return
  const ts = new Date(iso).getTime()
  if (!Number.isFinite(ts)) return
  const freshestKey = "freshestAt"
  const oldestKey = "oldestAt"
  const currentFreshestTs = target[freshestKey] ? new Date(target[freshestKey]).getTime() : Number.NaN
  const currentOldestTs = target[oldestKey] ? new Date(target[oldestKey]).getTime() : Number.NaN
  if (!Number.isFinite(currentFreshestTs) || ts > currentFreshestTs) {
    target[freshestKey] = iso
  }
  if (!Number.isFinite(currentOldestTs) || ts < currentOldestTs) {
    target[oldestKey] = iso
  }
}

function emptyCategoryNumberMap(initialValue = 0) {
  return Object.fromEntries(
    RECOVERY_CATEGORIES.map((category) => [category, Number(initialValue || 0)])
  )
}

function createCategorySummary() {
  return {
    totalRows: 0,
    quote: {
      attempted: 0,
      refreshed: 0,
      fresh: 0,
      coverageReady: 0,
      stale: 0,
      missing: 0,
      freshestAt: null,
      oldestAt: null,
      refreshSuccessRate: 0
    },
    snapshot: {
      attempted: 0,
      refreshed: 0,
      fresh: 0,
      stale: 0,
      missing: 0,
      missingSkin: 0,
      derivedOnly: 0,
      freshestAt: null,
      oldestAt: null,
      refreshSuccessRate: 0
    },
    rowsStillStale: 0,
    upstreamNewerThanCatalog: 0
  }
}

function createFreshnessSummary() {
  return {
    generatedAt: new Date().toISOString(),
    healthWindowHours: DEFAULT_HEALTH_WINDOW_HOURS,
    quoteLookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS,
    totalRows: 0,
    quote: {
      attemptedRows: 0,
      refreshedRows: 0,
      freshRows: 0,
      coverageReadyRows: 0,
      staleRows: 0,
      missingRows: 0
    },
    snapshot: {
      attemptedRows: 0,
      refreshedRows: 0,
      freshRows: 0,
      staleRows: 0,
      missingRows: 0,
      missingSkinRows: 0,
      derivedOnlyRows: 0
    },
    rowsStillStale: 0,
    upstreamNewerThanCatalog: 0,
    byCategory: Object.fromEntries(
      RECOVERY_CATEGORIES.map((category) => [category, createCategorySummary()])
    )
  }
}

function buildCategoryTargets(totalLimit = DEFAULT_TARGET_LIMIT, categories = RECOVERY_CATEGORIES) {
  const safeCategories = RECOVERY_CATEGORIES.filter((category) => categories.includes(category))
  const safeTotal = Math.max(Math.round(Number(totalLimit || DEFAULT_TARGET_LIMIT)), safeCategories.length)
  const quotas = {}
  let allocated = 0

  for (const [index, category] of safeCategories.entries()) {
    const target = CATEGORY_REFRESH_TARGETS[category] || { share: 0, min: 1 }
    const rawTarget = Math.round(safeTotal * Number(target.share || 0))
    const minimum = Math.min(Number(target.min || 1), safeTotal)
    const base = Math.max(rawTarget, minimum)
    quotas[category] =
      index === safeCategories.length - 1 ? Math.max(safeTotal - allocated, 1) : base
    allocated += quotas[category]
  }

  while (allocated > safeTotal) {
    const reducible = safeCategories.find((category) => quotas[category] > 1)
    if (!reducible) break
    quotas[reducible] -= 1
    allocated -= 1
  }

  while (allocated < safeTotal) {
    const category = safeCategories[allocated % safeCategories.length]
    quotas[category] = Number(quotas[category] || 0) + 1
    allocated += 1
  }

  return quotas
}

function dedupeRowsByMarketHashName(rows = []) {
  const deduped = []
  const seen = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName || seen.has(marketHashName)) continue
    seen.add(marketHashName)
    deduped.push(row)
  }
  return deduped
}

async function listRecoveryRows(options = {}) {
  const categories = Array.isArray(options.categories) ? options.categories : RECOVERY_CATEGORIES
  const targets = buildCategoryTargets(options.limit, categories)
  const results = await Promise.all(
    Object.entries(targets).map(async ([category, limit]) => [
      category,
      await marketSourceCatalogRepo.listActiveTradable({
        limit,
        categories: [category]
      })
    ])
  )

  return {
    targets,
    rows: dedupeRowsByMarketHashName(
      results.flatMap(([, rows]) => (Array.isArray(rows) ? rows : []))
    )
  }
}

function extractVolume7dFallback(row = {}, record = {}) {
  const direct = toIntegerOrNull(row?.volume_7d ?? row?.volume7d, 0)
  if (direct != null) return direct
  const raw = record?.raw && typeof record.raw === "object" ? record.raw : {}
  return (
    toIntegerOrNull(raw?.volume, 0) ??
    toIntegerOrNull(raw?.volume_7d ?? raw?.volume7d, 0) ??
    toIntegerOrNull(raw?.sales, 0) ??
    null
  )
}

function buildQualityFlags(record = {}, row = {}, fetchedAtIso = null) {
  const raw = record?.raw && typeof record.raw === "object" ? record.raw : {}
  return {
    recovery_refresh: true,
    recovery_category: normalizeText(row?.category || row?.itemCategory).toLowerCase() || null,
    confidence: normalizeText(record?.confidence).toLowerCase() || null,
    route_available: Boolean(record?.url),
    listing_available:
      raw?.listing_available == null ? Boolean(record?.url) : Boolean(raw.listing_available),
    source_updated_at: toIsoOrNull(record?.updatedAt || raw?.updated_at || raw?.updatedAt),
    fetched_at: toIsoOrNull(fetchedAtIso),
    url: normalizeText(record?.url) || null
  }
}

function buildQuoteInsertRow(record = {}, row = {}, fetchedAtIso = null) {
  const source = normalizeText(record?.source).toLowerCase()
  const marketHashName = normalizeText(record?.marketHashName || row?.market_hash_name || row?.marketHashName)
  const grossPrice = toFiniteOrNull(record?.grossPrice)
  if (!source || !marketHashName || grossPrice == null || grossPrice <= 0) {
    return null
  }

  return {
    item_name: marketHashName,
    market: source,
    best_buy: Number(grossPrice.toFixed(4)),
    best_sell: Number(grossPrice.toFixed(4)),
    best_sell_net:
      toFiniteOrNull(record?.netPriceAfterFees) == null
        ? null
        : Number(Number(record.netPriceAfterFees).toFixed(4)),
    volume_7d: extractVolume7dFallback(row, record),
    liquidity_score: toIntegerOrNull(row?.liquidity_rank ?? row?.liquidityRank, 0),
    fetched_at: toIsoOrNull(fetchedAtIso) || new Date().toISOString(),
    quality_flags: buildQualityFlags(record, row, fetchedAtIso)
  }
}

function buildMarketPriceUpsertRow(record = {}, fetchedAtIso = null) {
  const source = normalizeText(record?.source).toLowerCase()
  const marketHashName = normalizeText(record?.marketHashName)
  const grossPrice = toFiniteOrNull(record?.grossPrice)
  const netPrice = toFiniteOrNull(record?.netPriceAfterFees)
  if (!source || !marketHashName || grossPrice == null || netPrice == null) {
    return null
  }

  return {
    market: source,
    market_hash_name: marketHashName,
    currency: normalizeText(record?.currency).toUpperCase() || "USD",
    gross_price: Number(grossPrice.toFixed(4)),
    net_price: Number(netPrice.toFixed(4)),
    url: normalizeText(record?.url) || null,
    fetched_at: toIsoOrNull(fetchedAtIso) || new Date().toISOString(),
    raw: {
      ...(record?.raw && typeof record.raw === "object" ? record.raw : {}),
      confidence: normalizeText(record?.confidence) || null,
      source_updated_at: toIsoOrNull(record?.updatedAt)
    }
  }
}

function buildItemsForSource(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    marketHashName: normalizeText(row?.market_hash_name || row?.marketHashName),
    itemCategory: normalizeText(row?.category || row?.itemCategory).toLowerCase() || null,
    volume7d: toFiniteOrNull(row?.volume_7d ?? row?.volume7d),
    marketCoverageCount: toFiniteOrNull(
      row?.market_coverage_count ?? row?.marketCoverageCount
    )
  }))
}

function createAttemptCounter() {
  return {
    quoteAttemptedByCategory: emptyCategoryNumberMap(0),
    quoteRefreshedNamesByCategory: Object.fromEntries(
      RECOVERY_CATEGORIES.map((category) => [category, new Set()])
    ),
    snapshotAttemptedByCategory: emptyCategoryNumberMap(0),
    snapshotRefreshedNamesByCategory: Object.fromEntries(
      RECOVERY_CATEGORIES.map((category) => [category, new Set()])
    ),
    quoteRowsInserted: 0,
    marketPriceRowsUpserted: 0,
    quoteSourceDiagnostics: Object.fromEntries(
      RECOVERY_SOURCE_ORDER.map((source) => [source, { refreshed: 0, failed: 0, error: null }])
    )
  }
}

async function refreshQuotes(rows = [], options = {}) {
  const quoteBatchSize = Math.max(Number(options.quoteBatchSize || DEFAULT_QUOTE_BATCH_SIZE), 1)
  const concurrency = Math.max(Number(options.concurrency || DEFAULT_REFRESH_CONCURRENCY), 1)
  const timeoutMs = Math.max(Number(options.timeoutMs || marketCompareTimeoutMs || 9000), 500)
  const maxRetries = Math.max(Number(options.maxRetries || marketCompareMaxRetries || 3), 1)
  const counters = createAttemptCounter()
  const rowsByName = Object.fromEntries(
    (Array.isArray(rows) ? rows : []).map((row) => [
      normalizeText(row?.market_hash_name || row?.marketHashName),
      row
    ])
  )

  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
    if (counters.quoteAttemptedByCategory[category] != null) {
      counters.quoteAttemptedByCategory[category] += 1
    }
  }

  for (const chunk of chunkArray(rows, quoteBatchSize)) {
    const sourceItems = buildItemsForSource(chunk)
    if (!sourceItems.length) continue

    const chunkMarketPriceRows = []
    const chunkQuoteRows = []
    const chunkFetchedAt = new Date().toISOString()

    for (const source of RECOVERY_SOURCE_ORDER) {
      const adapter = RECOVERY_SOURCES[source]
      if (!adapter?.batchGetPrices) continue

      try {
        const sourceResult = await adapter.batchGetPrices(sourceItems, {
          currency: "USD",
          concurrency,
          timeoutMs,
          maxRetries
        })
        const meta =
          sourceResult && typeof sourceResult === "object" && sourceResult.__meta
            ? sourceResult.__meta
            : {}
        const failuresByName =
          meta && typeof meta.failuresByName === "object" ? meta.failuresByName : {}

        for (const row of chunk) {
          const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
          if (!marketHashName) continue
          if (failuresByName[marketHashName]) {
            counters.quoteSourceDiagnostics[source].failed += 1
          }
        }

        for (const [marketHashName, record] of Object.entries(sourceResult || {})) {
          if (marketHashName === "__meta") continue
          if (!record || typeof record !== "object") continue
          const row = rowsByName[marketHashName] || {}
          const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
          chunkMarketPriceRows.push(buildMarketPriceUpsertRow(record, chunkFetchedAt))
          chunkQuoteRows.push(buildQuoteInsertRow(record, row, chunkFetchedAt))
          if (counters.quoteRefreshedNamesByCategory[category] instanceof Set) {
            counters.quoteRefreshedNamesByCategory[category].add(marketHashName)
          }
          counters.quoteSourceDiagnostics[source].refreshed += 1
        }

        const sourceUnavailableReason = normalizeText(meta?.sourceUnavailableReason)
        if (sourceUnavailableReason) {
          counters.quoteSourceDiagnostics[source].error = sourceUnavailableReason
        }
      } catch (err) {
        counters.quoteSourceDiagnostics[source].error =
          normalizeText(err?.message) || "quote_refresh_failed"
        counters.quoteSourceDiagnostics[source].failed += sourceItems.length
      }
    }

    const marketPriceRows = chunkMarketPriceRows.filter(Boolean)
    const quoteRows = chunkQuoteRows.filter(Boolean)
    if (marketPriceRows.length) {
      counters.marketPriceRowsUpserted += await marketPriceRepo.upsertRows(marketPriceRows)
    }
    if (quoteRows.length) {
      counters.quoteRowsInserted += await marketQuoteRepo.insertRows(quoteRows)
    }
  }

  return counters
}

function isStrictSnapshotUsable(snapshot = {}) {
  const capturedAt = toIsoOrNull(snapshot?.captured_at || snapshot?.capturedAt)
  if (!capturedAt) return false
  const source = normalizeText(snapshot?.source).toLowerCase()
  if (source === "derived-price-history") return false
  return true
}

async function refreshSnapshots(rows = [], skinsByName = {}, options = {}) {
  const snapshotBatchSize = Math.max(
    Number(options.snapshotBatchSize || DEFAULT_SNAPSHOT_BATCH_SIZE),
    1
  )
  const concurrency = Math.max(Number(options.concurrency || DEFAULT_REFRESH_CONCURRENCY), 1)
  const counters = createAttemptCounter()
  const targetSkins = []

  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
    const skin = skinsByName[marketHashName] || null
    if (!skin) continue
    targetSkins.push(skin)
    if (counters.snapshotAttemptedByCategory[category] != null) {
      counters.snapshotAttemptedByCategory[category] += 1
    }
  }

  for (const chunk of chunkArray(targetSkins, snapshotBatchSize)) {
    const refreshed = await marketService.refreshSnapshotsForSkins(chunk, {
      concurrency,
      refreshStaleOnly: true,
      requireLiveOverview: true
    })
    for (const result of Array.isArray(refreshed) ? refreshed : []) {
      if (!result?.refreshed) continue
      const marketHashName = normalizeText(result?.marketHashName)
      const category = normalizeText(skinsByName[marketHashName]?.category).toLowerCase()
      if (counters.snapshotRefreshedNamesByCategory[category] instanceof Set) {
        counters.snapshotRefreshedNamesByCategory[category].add(marketHashName)
      }
    }
  }

  return counters
}

function buildSkinMaps(rows = [], skins = []) {
  const categoryByName = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue
    categoryByName[marketHashName] =
      normalizeText(row?.category || row?.itemCategory).toLowerCase() || "weapon_skin"
  }

  const skinsByName = {}
  const skinIds = []
  for (const skin of Array.isArray(skins) ? skins : []) {
    const marketHashName = normalizeText(skin?.market_hash_name || skin?.marketHashName)
    if (!marketHashName) continue
    const category = categoryByName[marketHashName] || "weapon_skin"
    skinsByName[marketHashName] = {
      ...skin,
      category
    }
    const skinId = Number(skin?.id || 0)
    if (Number.isInteger(skinId) && skinId > 0) {
      skinIds.push(skinId)
    }
  }

  return {
    skinsByName,
    skinIds: Array.from(new Set(skinIds))
  }
}

function buildFreshnessSummary(rows = [], quoteCoverageByItem = {}, snapshotsBySkinId = {}, skinsByName = {}) {
  const summary = createFreshnessSummary()
  const nowMs = Date.now()

  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
    if (!RECOVERY_CATEGORIES.includes(category)) continue

    const bucket = summary.byCategory[category]
    const skin = skinsByName[marketHashName] || null
    const skinId = Number(skin?.id || 0)
    const snapshot = Number.isInteger(skinId) && skinId > 0 ? snapshotsBySkinId?.[skinId] || null : null
    const quoteCoverage = quoteCoverageByItem?.[marketHashName] || {}
    const latestQuoteAt = toIsoOrNull(quoteCoverage?.latestFetchedAt)
    const latestSnapshotAt = isStrictSnapshotUsable(snapshot)
      ? toIsoOrNull(snapshot?.captured_at || snapshot?.capturedAt)
      : null
    const latestAnySnapshotAt = toIsoOrNull(snapshot?.captured_at || snapshot?.capturedAt)
    const latestUpstreamSignalAt = pickLatestIso(latestQuoteAt, latestSnapshotAt)
    const latestCatalogSignalAt = toIsoOrNull(row?.last_market_signal_at || row?.lastMarketSignalAt)
    const freshQuote = isFreshWithinHours(latestQuoteAt, DEFAULT_HEALTH_WINDOW_HOURS, nowMs)
    const freshSnapshot = isFreshWithinHours(latestSnapshotAt, DEFAULT_HEALTH_WINDOW_HOURS, nowMs)
    const coverageReady = freshQuote && Number(quoteCoverage?.marketCoverageCount || 0) >= 2

    summary.totalRows += 1
    bucket.totalRows += 1

    if (!latestQuoteAt) {
      summary.quote.missingRows += 1
      bucket.quote.missing += 1
    } else if (freshQuote) {
      summary.quote.freshRows += 1
      bucket.quote.fresh += 1
    } else {
      summary.quote.staleRows += 1
      bucket.quote.stale += 1
    }
    if (coverageReady) {
      summary.quote.coverageReadyRows += 1
      bucket.quote.coverageReady += 1
    }
    updateFreshnessRange(bucket.quote, latestQuoteAt)

    if (!skin || !Number.isInteger(skinId) || skinId <= 0) {
      summary.snapshot.missingSkinRows += 1
      bucket.snapshot.missingSkin += 1
    } else if (!latestAnySnapshotAt) {
      summary.snapshot.missingRows += 1
      bucket.snapshot.missing += 1
    } else if (!isStrictSnapshotUsable(snapshot)) {
      summary.snapshot.derivedOnlyRows += 1
      summary.snapshot.staleRows += 1
      bucket.snapshot.derivedOnly += 1
      bucket.snapshot.stale += 1
    } else if (freshSnapshot) {
      summary.snapshot.freshRows += 1
      bucket.snapshot.fresh += 1
    } else {
      summary.snapshot.staleRows += 1
      bucket.snapshot.stale += 1
    }
    updateFreshnessRange(bucket.snapshot, latestAnySnapshotAt)

    if (!freshQuote && !freshSnapshot) {
      summary.rowsStillStale += 1
      bucket.rowsStillStale += 1
    }

    const upstreamTs = latestUpstreamSignalAt ? new Date(latestUpstreamSignalAt).getTime() : Number.NaN
    const catalogTs = latestCatalogSignalAt ? new Date(latestCatalogSignalAt).getTime() : Number.NaN
    if (Number.isFinite(upstreamTs) && (!Number.isFinite(catalogTs) || upstreamTs > catalogTs)) {
      summary.upstreamNewerThanCatalog += 1
      bucket.upstreamNewerThanCatalog += 1
    }
  }

  return summary
}

function applyRefreshCounters(summary = {}, quoteRefresh = {}, snapshotRefresh = {}) {
  const next = {
    ...summary,
    quote: {
      ...(summary.quote || {})
    },
    snapshot: {
      ...(summary.snapshot || {})
    },
    byCategory: Object.fromEntries(
      RECOVERY_CATEGORIES.map((category) => [
        category,
        {
          ...createCategorySummary(),
          ...(summary.byCategory?.[category] || {}),
          quote: {
            ...createCategorySummary().quote,
            ...(summary.byCategory?.[category]?.quote || {})
          },
          snapshot: {
            ...createCategorySummary().snapshot,
            ...(summary.byCategory?.[category]?.snapshot || {})
          }
        }
      ])
    )
  }

  next.quote.attemptedRows = Object.values(quoteRefresh.quoteAttemptedByCategory || {}).reduce(
    (sum, value) => sum + Number(value || 0),
    0
  )
  next.snapshot.attemptedRows = Object.values(
    snapshotRefresh.snapshotAttemptedByCategory || {}
  ).reduce((sum, value) => sum + Number(value || 0), 0)

  for (const category of RECOVERY_CATEGORIES) {
    const quoteAttempted = Number(quoteRefresh.quoteAttemptedByCategory?.[category] || 0)
    const quoteRefreshed = Number(
      quoteRefresh.quoteRefreshedNamesByCategory?.[category] instanceof Set
        ? quoteRefresh.quoteRefreshedNamesByCategory[category].size
        : 0
    )
    const snapshotAttempted = Number(snapshotRefresh.snapshotAttemptedByCategory?.[category] || 0)
    const snapshotRefreshed = Number(
      snapshotRefresh.snapshotRefreshedNamesByCategory?.[category] instanceof Set
        ? snapshotRefresh.snapshotRefreshedNamesByCategory[category].size
        : 0
    )

    next.byCategory[category].quote.attempted = quoteAttempted
    next.byCategory[category].quote.refreshed = quoteRefreshed
    next.byCategory[category].quote.refreshSuccessRate =
      quoteAttempted > 0 ? Number((quoteRefreshed / quoteAttempted).toFixed(4)) : 0

    next.byCategory[category].snapshot.attempted = snapshotAttempted
    next.byCategory[category].snapshot.refreshed = snapshotRefreshed
    next.byCategory[category].snapshot.refreshSuccessRate =
      snapshotAttempted > 0 ? Number((snapshotRefreshed / snapshotAttempted).toFixed(4)) : 0

    next.quote.refreshedRows += quoteRefreshed
    next.snapshot.refreshedRows += snapshotRefreshed
  }

  return next
}

function evaluateRecoveryHealth(summary = {}) {
  const healthGate = {
    healthyEnough: true,
    evaluatedCategories: [],
    reasons: [],
    byCategory: {}
  }

  for (const category of RECOVERY_CATEGORIES) {
    const bucket = summary?.byCategory?.[category] || createCategorySummary()
    if (Number(bucket.totalRows || 0) <= 0) continue

    const requirements = CATEGORY_HEALTH_REQUIREMENTS[category] || {}
    const freshCoverageRows = Number(bucket?.quote?.coverageReady || 0)
    const freshCoverageRate =
      Number(bucket.totalRows || 0) > 0 ? freshCoverageRows / Number(bucket.totalRows || 1) : 0
    const freshSnapshotRows = Number(bucket?.snapshot?.fresh || 0)
    const snapshotPopulation = Math.max(
      Number(bucket.totalRows || 0) - Number(bucket?.snapshot?.missingSkin || 0),
      0
    )
    const freshSnapshotRate =
      snapshotPopulation > 0 ? freshSnapshotRows / snapshotPopulation : 0

    const quoteHealthy =
      freshCoverageRows >= Number(requirements.minFreshCoverageRows || 0) ||
      freshCoverageRate >= Number(requirements.minFreshCoverageRate || 0)
    const snapshotHealthy =
      !requirements.requireSnapshots ||
      snapshotPopulation <= 0 ||
      freshSnapshotRows >= Number(requirements.minFreshSnapshots || 0) ||
      freshSnapshotRate >= Number(requirements.minFreshSnapshotRate || 0)

    const categoryReasons = []
    if (!quoteHealthy) {
      categoryReasons.push("insufficient_fresh_quote_coverage")
    }
    if (!snapshotHealthy) {
      categoryReasons.push("insufficient_fresh_snapshots")
    }

    const categoryHealthy = quoteHealthy && snapshotHealthy
    healthGate.evaluatedCategories.push(category)
    healthGate.byCategory[category] = {
      totalRows: Number(bucket.totalRows || 0),
      freshCoverageRows,
      freshCoverageRate: Number(freshCoverageRate.toFixed(4)),
      freshSnapshotRows,
      freshSnapshotRate: Number(freshSnapshotRate.toFixed(4)),
      quoteHealthy,
      snapshotHealthy,
      healthyEnough: categoryHealthy,
      reasons: categoryReasons
    }

    if (!categoryHealthy) {
      healthGate.healthyEnough = false
      for (const reason of categoryReasons) {
        healthGate.reasons.push(`${category}:${reason}`)
      }
    }
  }

  return healthGate
}

function summarizeCatalogRecompute(diagnostics = {}) {
  const sourceCatalog = diagnostics?.sourceCatalog || diagnostics || {}
  return {
    executed: true,
    generatedAt: diagnostics?.generatedAt || null,
    scannableRows: Number(sourceCatalog?.scannable || 0),
    shadowRows: Number(sourceCatalog?.shadow || 0),
    blockedRows: Number(sourceCatalog?.blocked || 0),
    eligibleTradableRows: Number(sourceCatalog?.eligibleTradableRows || 0),
    nearEligibleRows: Number(sourceCatalog?.nearEligibleRows || 0),
    scanEligibleRows: Number(sourceCatalog?.eligibleRows || 0),
    scannerSourceSize: Number(
      sourceCatalog?.scanner_source_size || sourceCatalog?.scannerSourceSize || 0
    )
  }
}

async function runFreshnessRecovery(options = {}) {
  const categories = Array.isArray(options.categories) ? options.categories : RECOVERY_CATEGORIES
  const targetLimit = Math.max(Number(options.limit || DEFAULT_TARGET_LIMIT), 1)
  const { rows, targets } = await listRecoveryRows({
    categories,
    limit: targetLimit
  })
  const marketHashNames = rows
    .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
    .filter(Boolean)
  const skins = marketHashNames.length ? await skinRepo.getByMarketHashNames(marketHashNames) : []
  const { skinsByName, skinIds } = buildSkinMaps(rows, skins)

  const quoteCoverageMap = await marketQuoteRepo.getLatestCoverageByItemNames(marketHashNames, {
    lookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS
  })
  const snapshotMap = skinIds.length ? await marketSnapshotRepo.getLatestBySkinIds(skinIds) : {}
  const preRefresh = buildFreshnessSummary(rows, quoteCoverageMap, snapshotMap, skinsByName)

  const quoteRowsNeedingRefresh = rows.filter((row) => {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const coverage = quoteCoverageMap?.[marketHashName] || {}
    return (
      !isFreshWithinHours(coverage?.latestFetchedAt, DEFAULT_HEALTH_WINDOW_HOURS) ||
      Number(coverage?.marketCoverageCount || 0) < 2
    )
  })
  const snapshotRowsNeedingRefresh = rows.filter((row) => {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const skin = skinsByName[marketHashName] || null
    const skinId = Number(skin?.id || 0)
    if (!Number.isInteger(skinId) || skinId <= 0) return false
    const snapshot = snapshotMap?.[skinId] || null
    if (!snapshot) return true
    if (!isStrictSnapshotUsable(snapshot)) return true
    return !isFreshWithinHours(
      snapshot?.captured_at || snapshot?.capturedAt,
      DEFAULT_HEALTH_WINDOW_HOURS
    )
  })

  const quoteRefresh = await refreshQuotes(quoteRowsNeedingRefresh, options)
  const snapshotRefresh = await refreshSnapshots(snapshotRowsNeedingRefresh, skinsByName, options)

  const postQuoteCoverageMap = await marketQuoteRepo.getLatestCoverageByItemNames(marketHashNames, {
    lookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS
  })
  const postSnapshotMap = skinIds.length ? await marketSnapshotRepo.getLatestBySkinIds(skinIds) : {}
  const postRefreshBase = buildFreshnessSummary(
    rows,
    postQuoteCoverageMap,
    postSnapshotMap,
    skinsByName
  )
  const postRefresh = applyRefreshCounters(postRefreshBase, quoteRefresh, snapshotRefresh)
  const healthGate = evaluateRecoveryHealth(postRefresh)

  let catalogRecompute = {
    executed: false,
    skippedReason: healthGate.healthyEnough ? null : "upstream_not_healthy_enough"
  }
  if (healthGate.healthyEnough && options.recompute !== false) {
    const diagnostics = await marketSourceCatalogService.prepareSourceCatalog({
      forceRefresh: true,
      targetUniverseSize: Number(options.targetUniverseSize || arbitrageDefaultUniverseLimit || 3000)
    })
    catalogRecompute = summarizeCatalogRecompute(diagnostics)
  }

  return {
    generatedAt: new Date().toISOString(),
    categories,
    targets,
    targetLimit,
    preRefresh,
    postRefresh,
    healthGate,
    quoteRefresh: {
      rowsAttempted: Number(postRefresh?.quote?.attemptedRows || 0),
      rowsRefreshed: Number(postRefresh?.quote?.refreshedRows || 0),
      rowsStillStale: Number(postRefresh?.quote?.staleRows || 0),
      rowsMissing: Number(postRefresh?.quote?.missingRows || 0),
      quoteRowsInserted: Number(quoteRefresh.quoteRowsInserted || 0),
      marketPriceRowsUpserted: Number(quoteRefresh.marketPriceRowsUpserted || 0),
      bySource: Object.fromEntries(
        Object.entries(quoteRefresh.quoteSourceDiagnostics || {}).map(([source, diag]) => [
          source,
          {
            refreshed: Number(diag?.refreshed || 0),
            failed: Number(diag?.failed || 0),
            error: diag?.error || null
          }
        ])
      )
    },
    snapshotRefresh: {
      rowsAttempted: Number(postRefresh?.snapshot?.attemptedRows || 0),
      rowsRefreshed: Number(postRefresh?.snapshot?.refreshedRows || 0),
      rowsStillStale: Number(postRefresh?.snapshot?.staleRows || 0),
      rowsMissing: Number(postRefresh?.snapshot?.missingRows || 0),
      rowsMissingSkin: Number(postRefresh?.snapshot?.missingSkinRows || 0)
    },
    catalogRecompute
  }
}

module.exports = {
  runFreshnessRecovery,
  __testables: {
    buildCategoryTargets,
    buildFreshnessSummary,
    applyRefreshCounters,
    evaluateRecoveryHealth,
    isStrictSnapshotUsable,
    buildQuoteInsertRow,
    buildMarketPriceUpsertRow
  }
}

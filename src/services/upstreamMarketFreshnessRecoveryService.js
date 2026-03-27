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
  Math.min(Number(arbitrageDefaultUniverseLimit || 3000), 180),
  60
)
const DEFAULT_SELECTION_BATCH_SIZE = 30
const DEFAULT_QUOTE_BATCH_SIZE = 20
const DEFAULT_SNAPSHOT_BATCH_SIZE = 10
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

function createStageLogger(logProgress) {
  return typeof logProgress === "function" ? logProgress : () => {}
}

function emitProgress(logProgress, payload = {}) {
  try {
    createStageLogger(logProgress)({
      ts: new Date().toISOString(),
      ...payload
    })
  } catch (_err) {
    // Progress logging must never fail the recovery path.
  }
}

function isStatementTimeoutError(error) {
  const code = normalizeText(error?.code).toUpperCase()
  const message = normalizeText(error?.message).toLowerCase()
  return (
    code === "57014" ||
    message.includes("statement timeout") ||
    message.includes("canceling statement due to statement timeout")
  )
}

function buildStageError(stage = "", error = null, meta = {}) {
  return {
    stage: normalizeText(stage) || "unknown_stage",
    message: normalizeText(error?.message) || "unknown_error",
    code: normalizeText(error?.code) || null,
    timedOut: isStatementTimeoutError(error),
    ...meta
  }
}

async function runLoggedStage(stage = "", options = {}, fn) {
  const safeStage = normalizeText(stage) || "unknown_stage"
  const logProgress = options?.logProgress
  const meta = options?.meta && typeof options.meta === "object" ? options.meta : {}
  const startedAt = Date.now()
  emitProgress(logProgress, {
    type: "stage_start",
    stage: safeStage,
    ...meta
  })
  try {
    const value = await fn()
    emitProgress(logProgress, {
      type: "stage_done",
      stage: safeStage,
      durationMs: Date.now() - startedAt,
      ...meta
    })
    return value
  } catch (error) {
    const stageError = buildStageError(safeStage, error, {
      durationMs: Date.now() - startedAt,
      ...meta
    })
    emitProgress(logProgress, {
      type: "stage_error",
      ...stageError
    })
    throw Object.assign(error || new Error("unknown_error"), {
      recoveryStageError: stageError
    })
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

function normalizeRecoveryCategories(values = []) {
  const normalized = Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value).toLowerCase())
        .filter((value) => RECOVERY_CATEGORIES.includes(value))
    )
  )
  return normalized.length ? normalized : RECOVERY_CATEGORIES.slice()
}

function resolveRecoveryCursor(categories = [], options = {}) {
  const safeCategories = normalizeRecoveryCategories(categories)
  const requestedStartCategory = normalizeText(options.startCategory).toLowerCase()
  const startCategory = safeCategories.includes(requestedStartCategory)
    ? requestedStartCategory
    : safeCategories[0] || null
  const startOffset = Math.max(Number(options.startOffset || 0), 0)
  return {
    categories: safeCategories,
    startCategory,
    startOffset
  }
}

function mergeStringSet(target, source) {
  if (!(target instanceof Set) || !(source instanceof Set)) return
  for (const value of source) {
    const normalized = normalizeText(value)
    if (normalized) {
      target.add(normalized)
    }
  }
}

function mergeAttemptCounters(target = createAttemptCounter(), source = {}) {
  const next = target
  for (const category of RECOVERY_CATEGORIES) {
    next.quoteAttemptedByCategory[category] =
      Number(next.quoteAttemptedByCategory?.[category] || 0) +
      Number(source.quoteAttemptedByCategory?.[category] || 0)
    next.snapshotAttemptedByCategory[category] =
      Number(next.snapshotAttemptedByCategory?.[category] || 0) +
      Number(source.snapshotAttemptedByCategory?.[category] || 0)
    mergeStringSet(
      next.quoteRefreshedNamesByCategory?.[category],
      source.quoteRefreshedNamesByCategory?.[category]
    )
    mergeStringSet(
      next.snapshotRefreshedNamesByCategory?.[category],
      source.snapshotRefreshedNamesByCategory?.[category]
    )
    for (const reason of SNAPSHOT_REASON_KEYS) {
      next.snapshotReasonCountsByCategory[category][reason] =
        Number(next.snapshotReasonCountsByCategory?.[category]?.[reason] || 0) +
        Number(source.snapshotReasonCountsByCategory?.[category]?.[reason] || 0)
    }
  }

  for (const reason of SNAPSHOT_REASON_KEYS) {
    next.snapshotReasonCounts[reason] =
      Number(next.snapshotReasonCounts?.[reason] || 0) +
      Number(source.snapshotReasonCounts?.[reason] || 0)
  }

  if (Array.isArray(source.snapshotBatchDiagnostics) && source.snapshotBatchDiagnostics.length) {
    next.snapshotBatchDiagnostics.push(...source.snapshotBatchDiagnostics)
  }

  next.quoteRowsInserted =
    Number(next.quoteRowsInserted || 0) + Number(source.quoteRowsInserted || 0)
  next.marketPriceRowsUpserted =
    Number(next.marketPriceRowsUpserted || 0) + Number(source.marketPriceRowsUpserted || 0)

  for (const sourceName of RECOVERY_SOURCE_ORDER) {
    const existing = next.quoteSourceDiagnostics?.[sourceName] || {
      refreshed: 0,
      failed: 0,
      error: null
    }
    const incoming = source.quoteSourceDiagnostics?.[sourceName] || {}
    existing.refreshed = Number(existing.refreshed || 0) + Number(incoming.refreshed || 0)
    existing.failed = Number(existing.failed || 0) + Number(incoming.failed || 0)
    existing.error = incoming.error || existing.error || null
    next.quoteSourceDiagnostics[sourceName] = existing
  }

  return next
}

function mergeCategoryFreshnessSummary(target = createCategorySummary(), source = {}) {
  target.totalRows += Number(source.totalRows || 0)
  target.rowsStillStale += Number(source.rowsStillStale || 0)
  target.upstreamNewerThanCatalog += Number(source.upstreamNewerThanCatalog || 0)

  for (const field of ["attempted", "refreshed", "fresh", "coverageReady", "stale", "missing"]) {
    target.quote[field] = Number(target.quote?.[field] || 0) + Number(source.quote?.[field] || 0)
  }
  updateFreshnessRange(target.quote, source.quote?.freshestAt)
  updateFreshnessRange(target.quote, source.quote?.oldestAt)

  for (const field of [
    "attempted",
    "refreshed",
    "fresh",
    "stale",
    "missing",
    "missingSkin",
    "derivedOnly"
  ]) {
    target.snapshot[field] =
      Number(target.snapshot?.[field] || 0) + Number(source.snapshot?.[field] || 0)
  }
  updateFreshnessRange(target.snapshot, source.snapshot?.freshestAt)
  updateFreshnessRange(target.snapshot, source.snapshot?.oldestAt)

  return target
}

function mergeFreshnessSummary(target = createFreshnessSummary(), source = {}) {
  target.totalRows += Number(source.totalRows || 0)
  target.rowsStillStale += Number(source.rowsStillStale || 0)
  target.upstreamNewerThanCatalog += Number(source.upstreamNewerThanCatalog || 0)

  for (const field of [
    "attemptedRows",
    "refreshedRows",
    "freshRows",
    "coverageReadyRows",
    "staleRows",
    "missingRows"
  ]) {
    target.quote[field] = Number(target.quote?.[field] || 0) + Number(source.quote?.[field] || 0)
  }

  for (const field of [
    "attemptedRows",
    "refreshedRows",
    "freshRows",
    "staleRows",
    "missingRows",
    "missingSkinRows",
    "derivedOnlyRows"
  ]) {
    target.snapshot[field] =
      Number(target.snapshot?.[field] || 0) + Number(source.snapshot?.[field] || 0)
  }

  for (const category of RECOVERY_CATEGORIES) {
    mergeCategoryFreshnessSummary(
      target.byCategory[category],
      source.byCategory?.[category] || createCategorySummary()
    )
  }

  return target
}

function buildCheckpointState({
  categories = RECOVERY_CATEGORIES,
  nextCategory = null,
  nextOffset = 0,
  completedBatches = 0,
  processedRows = 0,
  processedRowsByCategory = emptyCategoryNumberMap(0),
  done = false
} = {}) {
  return {
    categories: Array.isArray(categories) ? categories.slice() : RECOVERY_CATEGORIES.slice(),
    nextCategory: done ? null : nextCategory || null,
    nextOffset: done ? 0 : Math.max(Number(nextOffset || 0), 0),
    completedBatches: Math.max(Number(completedBatches || 0), 0),
    processedRows: Math.max(Number(processedRows || 0), 0),
    processedRowsByCategory: {
      ...emptyCategoryNumberMap(0),
      ...(processedRowsByCategory && typeof processedRowsByCategory === "object"
        ? Object.fromEntries(
            Object.entries(processedRowsByCategory).map(([category, value]) => [
              category,
              Math.max(Number(value || 0), 0)
            ])
          )
        : {})
    },
    resumeArgs:
      done || !nextCategory
        ? []
        : [
            `--start-category=${nextCategory}`,
            `--start-offset=${Math.max(Number(nextOffset || 0), 0)}`
          ]
  }
}

async function selectRecoveryRowBatch(options = {}) {
  const category = normalizeText(options.category).toLowerCase() || "weapon_skin"
  const limit = Math.max(Number(options.limit || DEFAULT_SELECTION_BATCH_SIZE), 1)
  const offset = Math.max(Number(options.offset || 0), 0)
  const batchIndex = Math.max(Number(options.batchIndex || 0), 0)
  const logProgress = options.logProgress

  const rows = await runLoggedStage(
    "universe_selection",
    {
      logProgress,
      meta: {
        category,
        batchIndex,
        offset,
        limit,
        categoryTarget: Number(options.categoryTarget || 0)
      }
    },
    async () =>
      marketSourceCatalogRepo.listActiveTradable({
        limit,
        offset,
        categories: [category]
      })
  )

  return dedupeRowsByMarketHashName(rows)
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

const SNAPSHOT_REASON_KEYS = Object.freeze([
  "snapshot_source_request_failed",
  "snapshot_http_error",
  "snapshot_rate_limited",
  "snapshot_empty_payload",
  "snapshot_parse_failed",
  "snapshot_missing_skin_mapping",
  "snapshot_live_overview_missing",
  "snapshot_derived_only_rejected",
  "snapshot_write_skipped",
  "snapshot_write_succeeded"
])

const SNAPSHOT_TRANSIENT_REASON_SET = new Set([
  "snapshot_source_request_failed",
  "snapshot_http_error",
  "snapshot_rate_limited"
])

function createSnapshotReasonMap() {
  return Object.fromEntries(SNAPSHOT_REASON_KEYS.map((reason) => [reason, 0]))
}

function incrementSnapshotReasonCounter(counters = {}, category = "", reason = "", amount = 1) {
  const safeCategory = normalizeText(category).toLowerCase()
  const safeReason = SNAPSHOT_REASON_KEYS.includes(reason) ? reason : "snapshot_source_request_failed"
  const safeAmount = Math.max(Number(amount || 0), 0)
  if (!safeAmount) return

  if (counters.snapshotReasonCounts?.[safeReason] != null) {
    counters.snapshotReasonCounts[safeReason] += safeAmount
  }
  if (
    counters.snapshotReasonCountsByCategory?.[safeCategory] &&
    counters.snapshotReasonCountsByCategory[safeCategory][safeReason] != null
  ) {
    counters.snapshotReasonCountsByCategory[safeCategory][safeReason] += safeAmount
  }
}

function summarizeSnapshotReasonCounts(reasonCounts = {}) {
  const totalFailures = SNAPSHOT_REASON_KEYS.filter((reason) => reason !== "snapshot_write_succeeded")
    .filter((reason) => reason !== "snapshot_write_skipped")
    .reduce((sum, reason) => sum + Number(reasonCounts?.[reason] || 0), 0)
  const dominantFailureReason = SNAPSHOT_REASON_KEYS.filter((reason) => {
    if (reason === "snapshot_write_succeeded" || reason === "snapshot_write_skipped") return false
    return Number(reasonCounts?.[reason] || 0) > 0
  }).sort((left, right) => Number(reasonCounts?.[right] || 0) - Number(reasonCounts?.[left] || 0))[0] || null
  const transientFailureCount = SNAPSHOT_REASON_KEYS.filter((reason) =>
    SNAPSHOT_TRANSIENT_REASON_SET.has(reason)
  ).reduce((sum, reason) => sum + Number(reasonCounts?.[reason] || 0), 0)
  const classification =
    totalFailures <= 0
      ? "none"
      : transientFailureCount === totalFailures
        ? "upstream"
        : transientFailureCount === 0
          ? "internal_or_strict"
          : "mixed"

  return {
    totalFailures,
    dominantFailureReason,
    failureClassification: classification,
    retryLikelyHelpful:
      totalFailures > 0 &&
      transientFailureCount > 0 &&
      transientFailureCount >= Math.ceil(totalFailures / 2)
  }
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
    snapshotReasonCounts: createSnapshotReasonMap(),
    snapshotReasonCountsByCategory: Object.fromEntries(
      RECOVERY_CATEGORIES.map((category) => [category, createSnapshotReasonMap()])
    ),
    snapshotBatchDiagnostics: [],
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
  const logProgress = options.logProgress
  const batchMeta = options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}
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

  const chunks = chunkArray(rows, quoteBatchSize)
  for (const [chunkIndex, chunk] of chunks.entries()) {
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
      counters.marketPriceRowsUpserted += await runLoggedStage(
        "market_prices_upserts",
        {
          logProgress,
          meta: {
            ...batchMeta,
            quoteChunkIndex: chunkIndex,
            quoteChunkSize: chunk.length,
            rowCount: marketPriceRows.length
          }
        },
        async () => marketPriceRepo.upsertRows(marketPriceRows)
      )
    }
    if (quoteRows.length) {
      counters.quoteRowsInserted += await runLoggedStage(
        "quote_writes",
        {
          logProgress,
          meta: {
            ...batchMeta,
            quoteChunkIndex: chunkIndex,
            quoteChunkSize: chunk.length,
            rowCount: quoteRows.length
          }
        },
        async () => marketQuoteRepo.insertRows(quoteRows)
      )
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
  const logProgress = options.logProgress
  const batchMeta = options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}
  const counters = createAttemptCounter()
  const targetSkins = []
  const batchReasonCounts = createSnapshotReasonMap()

  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
    const skin = skinsByName[marketHashName] || null
    if (!skin) {
      incrementSnapshotReasonCounter(
        counters,
        category,
        "snapshot_missing_skin_mapping"
      )
      batchReasonCounts.snapshot_missing_skin_mapping += 1
      continue
    }
    targetSkins.push(skin)
    if (counters.snapshotAttemptedByCategory[category] != null) {
      counters.snapshotAttemptedByCategory[category] += 1
    }
  }

  const chunks = chunkArray(targetSkins, snapshotBatchSize)
  for (const [chunkIndex, chunk] of chunks.entries()) {
    const refreshed = await runLoggedStage(
      "snapshot_writes",
      {
        logProgress,
        meta: {
          ...batchMeta,
          snapshotChunkIndex: chunkIndex,
          snapshotChunkSize: chunk.length,
          rowCount: chunk.length
        }
      },
      async () =>
        marketService.refreshSnapshotsForSkins(chunk, {
          concurrency,
          refreshStaleOnly: true,
          requireLiveOverview: true
        })
    )
    for (const result of Array.isArray(refreshed) ? refreshed : []) {
      const marketHashName = normalizeText(result?.marketHashName)
      const category = normalizeText(skinsByName[marketHashName]?.category).toLowerCase()
      const reason =
        SNAPSHOT_REASON_KEYS.includes(result?.refreshReason) &&
        normalizeText(result?.refreshReason)
          ? result.refreshReason
          : result?.refreshed
            ? "snapshot_write_succeeded"
            : result?.skippedFresh
              ? "snapshot_write_skipped"
              : "snapshot_source_request_failed"
      incrementSnapshotReasonCounter(counters, category, reason)
      if (batchReasonCounts[reason] != null) {
        batchReasonCounts[reason] += 1
      }
      if (
        reason === "snapshot_write_succeeded" &&
        counters.snapshotRefreshedNamesByCategory[category] instanceof Set
      ) {
        counters.snapshotRefreshedNamesByCategory[category].add(marketHashName)
      }
    }
  }

  const batchDiagnostics = {
    ...batchMeta,
    reasonCounts: batchReasonCounts,
    ...summarizeSnapshotReasonCounts(batchReasonCounts)
  }
  counters.snapshotBatchDiagnostics.push(batchDiagnostics)
  emitProgress(logProgress, {
    type: "snapshot_batch_summary",
    stage: "snapshot_writes",
    ...batchDiagnostics
  })

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

async function processRecoveryBatch(rows = [], options = {}) {
  const logProgress = options.logProgress
  const batchMeta = options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}
  const marketHashNames = rows
    .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
    .filter(Boolean)
  const skins = marketHashNames.length
    ? await runLoggedStage(
        "skin_lookup",
        {
          logProgress,
          meta: {
            ...batchMeta,
            rowCount: marketHashNames.length
          }
        },
        async () => skinRepo.getByMarketHashNames(marketHashNames)
      )
    : []
  const { skinsByName, skinIds } = buildSkinMaps(rows, skins)
  const quoteCoverageMap = await runLoggedStage(
    "quote_refresh_selection",
    {
      logProgress,
      meta: {
        ...batchMeta,
        phase: "pre",
        rowCount: marketHashNames.length
      }
    },
    async () =>
      marketQuoteRepo.getLatestCoverageByItemNames(marketHashNames, {
        lookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS
      })
  )
  const snapshotMap = await runLoggedStage(
    "snapshot_refresh_selection",
    {
      logProgress,
      meta: {
        ...batchMeta,
        phase: "pre",
        rowCount: skinIds.length
      }
    },
    async () => (skinIds.length ? marketSnapshotRepo.getLatestBySkinIds(skinIds) : {})
  )
  const preRefresh = buildFreshnessSummary(rows, quoteCoverageMap, snapshotMap, skinsByName)

  const quoteRowsNeedingRefresh = rows.filter((row) => {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const coverage = quoteCoverageMap?.[marketHashName] || {}
    return (
      !isFreshWithinHours(coverage?.latestFetchedAt, DEFAULT_HEALTH_WINDOW_HOURS) ||
      Number(coverage?.marketCoverageCount || 0) < 2
    )
  })
  emitProgress(logProgress, {
    type: "selection_summary",
    stage: "quote_refresh_selection",
    ...batchMeta,
    rowCount: rows.length,
    selectedRows: quoteRowsNeedingRefresh.length
  })

  const snapshotRowsNeedingRefresh = rows.filter((row) => {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const skin = skinsByName[marketHashName] || null
    const skinId = Number(skin?.id || 0)
    if (!Number.isInteger(skinId) || skinId <= 0) return true
    const snapshot = snapshotMap?.[skinId] || null
    if (!snapshot) return true
    if (!isStrictSnapshotUsable(snapshot)) return true
    return !isFreshWithinHours(
      snapshot?.captured_at || snapshot?.capturedAt,
      DEFAULT_HEALTH_WINDOW_HOURS
    )
  })
  emitProgress(logProgress, {
    type: "selection_summary",
    stage: "snapshot_refresh_selection",
    ...batchMeta,
    rowCount: rows.length,
    selectedRows: snapshotRowsNeedingRefresh.length
  })

  const quoteRefresh = await refreshQuotes(quoteRowsNeedingRefresh, {
    ...options,
    logProgress,
    batchMeta
  })
  const snapshotRefresh = await refreshSnapshots(snapshotRowsNeedingRefresh, skinsByName, {
    ...options,
    logProgress,
    batchMeta
  })

  const postQuoteCoverageMap = await runLoggedStage(
    "quote_refresh_selection",
    {
      logProgress,
      meta: {
        ...batchMeta,
        phase: "post",
        rowCount: marketHashNames.length
      }
    },
    async () =>
      marketQuoteRepo.getLatestCoverageByItemNames(marketHashNames, {
        lookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS
      })
  )
  const postSnapshotMap = await runLoggedStage(
    "snapshot_refresh_selection",
    {
      logProgress,
      meta: {
        ...batchMeta,
        phase: "post",
        rowCount: skinIds.length
      }
    },
    async () => (skinIds.length ? marketSnapshotRepo.getLatestBySkinIds(skinIds) : {})
  )
  const postRefreshBase = buildFreshnessSummary(
    rows,
    postQuoteCoverageMap,
    postSnapshotMap,
    skinsByName
  )

  return {
    preRefresh,
    postRefreshBase,
    quoteRefresh,
    snapshotRefresh,
    rowCount: rows.length
  }
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
  const logProgress = options.logProgress
  const cursor = resolveRecoveryCursor(options.categories, options)
  const categories = cursor.categories
  const targetLimit = Math.max(Number(options.limit || DEFAULT_TARGET_LIMIT), 1)
  const selectionBatchSize = Math.max(
    Number(options.selectionBatchSize || DEFAULT_SELECTION_BATCH_SIZE),
    1
  )
  const maxBatches =
    Number.isFinite(Number(options.maxBatches)) && Number(options.maxBatches) > 0
      ? Math.max(Math.round(Number(options.maxBatches)), 1)
      : null
  const targets = buildCategoryTargets(targetLimit, categories)
  const preRefresh = createFreshnessSummary()
  const postRefreshBase = createFreshnessSummary()
  const aggregateQuoteRefresh = createAttemptCounter()
  const aggregateSnapshotRefresh = createAttemptCounter()
  const processedRowsByCategory = emptyCategoryNumberMap(0)
  let processedRows = 0
  let completedBatches = 0
  let paused = false
  let timedOut = false
  let failedStage = null
  let errorSummary = null
  let nextCategory = cursor.startCategory
  let nextOffset = cursor.startOffset

  const startIndex = categories.indexOf(cursor.startCategory)
  const orderedCategories =
    startIndex >= 0 ? categories.slice(startIndex) : categories.slice()

  emitProgress(logProgress, {
    type: "recovery_plan",
    categories,
    targets,
    targetLimit,
    selectionBatchSize,
    maxBatches,
    startCategory: cursor.startCategory,
    startOffset: cursor.startOffset
  })

  batchLoop: for (const category of orderedCategories) {
    let categoryOffset = category === cursor.startCategory ? cursor.startOffset : 0
    let categoryProcessed = category === cursor.startCategory ? cursor.startOffset : 0
    const categoryTarget = Math.max(Number(targets?.[category] || 0), 0)

    while (categoryProcessed < categoryTarget) {
      if (maxBatches != null && completedBatches >= maxBatches) {
        paused = true
        nextCategory = category
        nextOffset = categoryOffset
        break batchLoop
      }

      const batchLimit = Math.max(
        Math.min(selectionBatchSize, categoryTarget - categoryProcessed),
        1
      )
      const batchMeta = {
        batchIndex: completedBatches,
        category,
        offset: categoryOffset,
        limit: batchLimit,
        categoryTarget
      }

      emitProgress(logProgress, {
        type: "batch_start",
        ...batchMeta,
        processedRows,
        processedRowsByCategory: {
          ...processedRowsByCategory
        }
      })

      try {
        const rows = await selectRecoveryRowBatch({
          category,
          limit: batchLimit,
          offset: categoryOffset,
          batchIndex: completedBatches,
          categoryTarget,
          logProgress
        })

        if (!rows.length) {
          emitProgress(logProgress, {
            type: "batch_empty",
            ...batchMeta
          })
          break
        }

        const batchResult = await processRecoveryBatch(rows, {
          ...options,
          logProgress,
          batchMeta
        })

        mergeFreshnessSummary(preRefresh, batchResult.preRefresh)
        mergeFreshnessSummary(postRefreshBase, batchResult.postRefreshBase)
        mergeAttemptCounters(aggregateQuoteRefresh, batchResult.quoteRefresh)
        mergeAttemptCounters(aggregateSnapshotRefresh, batchResult.snapshotRefresh)

        const rowCount = Number(batchResult.rowCount || rows.length || 0)
        processedRows += rowCount
        processedRowsByCategory[category] =
          Number(processedRowsByCategory?.[category] || 0) + rowCount
        categoryProcessed += rowCount
        categoryOffset += rowCount
        completedBatches += 1
        nextCategory = category
        nextOffset = categoryOffset

        emitProgress(logProgress, {
          type: "batch_done",
          ...batchMeta,
          rowCount,
          processedRows,
          processedRowsByCategory: {
            ...processedRowsByCategory
          }
        })
      } catch (error) {
        const stageError = error?.recoveryStageError || buildStageError("unknown_stage", error)
        failedStage = stageError.stage
        timedOut = Boolean(stageError.timedOut)
        errorSummary = {
          stage: stageError.stage,
          message: stageError.message,
          code: stageError.code,
          timedOut: Boolean(stageError.timedOut)
        }
        nextCategory = category
        nextOffset = categoryOffset
        break batchLoop
      }
    }
  }

  const postRefresh = applyRefreshCounters(
    postRefreshBase,
    aggregateQuoteRefresh,
    aggregateSnapshotRefresh
  )

  const healthGate = await runLoggedStage(
    "health_gate_evaluation",
    {
      logProgress,
      meta: {
        completedBatches,
        processedRows
      }
    },
    async () => evaluateRecoveryHealth(postRefresh)
  )
  healthGate.recoveryComplete = !paused && !failedStage
  if (!healthGate.recoveryComplete) {
    healthGate.healthyEnough = false
    if (paused) {
      healthGate.reasons.unshift("recovery_paused")
    }
    if (failedStage) {
      healthGate.reasons.unshift(`recovery_failed:${failedStage}`)
    } else {
      healthGate.reasons.unshift("recovery_incomplete")
    }
  }

  let catalogRecompute = {
    executed: false,
    skippedReason: paused
      ? "recovery_paused"
      : failedStage
        ? "recovery_failed"
        : healthGate.healthyEnough
          ? options.recompute === false
            ? "skip_requested"
            : null
          : "upstream_not_healthy_enough"
  }

  if (!paused && !failedStage && healthGate.healthyEnough && options.recompute !== false) {
    try {
      const diagnostics = await runLoggedStage(
        "force_recompute",
        {
          logProgress,
          meta: {
            targetUniverseSize: Number(
              options.targetUniverseSize || arbitrageDefaultUniverseLimit || 3000
            )
          }
        },
        async () =>
          marketSourceCatalogService.prepareSourceCatalog({
            forceRefresh: true,
            targetUniverseSize: Number(
              options.targetUniverseSize || arbitrageDefaultUniverseLimit || 3000
            )
          })
      )
      catalogRecompute = summarizeCatalogRecompute(diagnostics)
    } catch (error) {
      const stageError = error?.recoveryStageError || buildStageError("force_recompute", error)
      failedStage = stageError.stage
      timedOut = Boolean(stageError.timedOut)
      errorSummary = {
        stage: stageError.stage,
        message: stageError.message,
        code: stageError.code,
        timedOut: Boolean(stageError.timedOut)
      }
      healthGate.healthyEnough = false
      healthGate.recoveryComplete = false
      healthGate.reasons.unshift(`recovery_failed:${stageError.stage}`)
      catalogRecompute = {
        executed: false,
        skippedReason: "force_recompute_failed"
      }
    }
  }

  const completed = !paused && !failedStage
  const checkpoint = buildCheckpointState({
    categories,
    nextCategory,
    nextOffset,
    completedBatches,
    processedRows,
    processedRowsByCategory,
    done: completed
  })

  return {
    generatedAt: new Date().toISOString(),
    categories,
    targets,
    targetLimit,
    selectionBatchSize,
    completed,
    paused,
    timedOut,
    completedBatches,
    processedRows,
    processedRowsByCategory,
    failedStage,
    error: errorSummary,
    checkpoint,
    preRefresh,
    postRefresh,
    healthGate,
    quoteRefresh: {
      rowsAttempted: Number(postRefresh?.quote?.attemptedRows || 0),
      rowsRefreshed: Number(postRefresh?.quote?.refreshedRows || 0),
      rowsStillStale: Number(postRefresh?.quote?.staleRows || 0),
      rowsMissing: Number(postRefresh?.quote?.missingRows || 0),
      quoteRowsInserted: Number(aggregateQuoteRefresh.quoteRowsInserted || 0),
      marketPriceRowsUpserted: Number(aggregateQuoteRefresh.marketPriceRowsUpserted || 0),
      bySource: Object.fromEntries(
        Object.entries(aggregateQuoteRefresh.quoteSourceDiagnostics || {}).map(([source, diag]) => [
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
      rowsMissingSkin: Number(postRefresh?.snapshot?.missingSkinRows || 0),
      failureReasons: {
        ...(aggregateSnapshotRefresh.snapshotReasonCounts || createSnapshotReasonMap())
      },
      byCategory: Object.fromEntries(
        RECOVERY_CATEGORIES.map((category) => [
          category,
          {
            reasons: {
              ...(aggregateSnapshotRefresh.snapshotReasonCountsByCategory?.[category] ||
                createSnapshotReasonMap())
            },
            ...summarizeSnapshotReasonCounts(
              aggregateSnapshotRefresh.snapshotReasonCountsByCategory?.[category] ||
                createSnapshotReasonMap()
            )
          }
        ])
      ),
      batches: Array.isArray(aggregateSnapshotRefresh.snapshotBatchDiagnostics)
        ? aggregateSnapshotRefresh.snapshotBatchDiagnostics
        : [],
      ...summarizeSnapshotReasonCounts(
        aggregateSnapshotRefresh.snapshotReasonCounts || createSnapshotReasonMap()
      )
    },
    catalogRecompute
  }
}

module.exports = {
  runFreshnessRecovery,
  __testables: {
    buildCategoryTargets,
    resolveRecoveryCursor,
    mergeAttemptCounters,
    mergeFreshnessSummary,
    buildFreshnessSummary,
    applyRefreshCounters,
    evaluateRecoveryHealth,
    isStrictSnapshotUsable,
    buildQuoteInsertRow,
    buildMarketPriceUpsertRow
  }
}

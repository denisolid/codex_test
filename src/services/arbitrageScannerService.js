const AppError = require("../utils/AppError")
const skinRepo = require("../repositories/skinRepository")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const marketComparisonService = require("./marketComparisonService")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const marketImageService = require("./marketImageService")
const feedPublishRefreshService = require("./feedPublishRefreshService")
const planService = require("./planService")
const premiumCategoryAccessService = require("./premiumCategoryAccessService")
const {
  SCANNER_TYPES,
  ITEM_CATEGORIES,
  ROUND_ROBIN_CATEGORY_ORDER,
  SCAN_STATE,
  OPPORTUNITY_TIERS,
  DEFAULT_UNIVERSE_LIMIT,
  UNIVERSE_DB_LIMIT,
  OPPORTUNITY_BATCH_TARGET,
  OPPORTUNITY_SAFE_BATCH_SIZE,
  OPPORTUNITY_HOT_TARGET,
  OPPORTUNITY_BATCH_RUNTIME_TARGET,
  ENRICHMENT_BATCH_TARGET,
  SCAN_CHUNK_SIZE,
  ENRICHMENT_INTERVAL_MINUTES,
  OPPORTUNITY_SCAN_INTERVAL_MINUTES,
  ENRICHMENT_INTERVAL_MS,
  OPPORTUNITY_SCAN_INTERVAL_MS,
  ENRICHMENT_JOB_TIMEOUT_MS,
  OPPORTUNITY_HARD_TIMEOUT_MS,
  OPPORTUNITY_SCAN_ALLOW_LIVE_FETCH,
  SCAN_TIMEOUT_PER_BATCH_MS,
  DUPLICATE_WINDOW_HOURS,
  ALLOW_CROSS_JOB_PARALLELISM,
  RECORD_SKIPPED_ALREADY_RUNNING
} = require("./scanner/config")
const { classifyCatalogState } = require("./scanner/stateModel")
const { selectScanCandidates, buildRoundRobinPool } = require("./scanner/candidateSelector")
const { evaluateCandidateOpportunity, clampScore } = require("./scanner/opportunityEvaluator")
const {
  buildSignature,
  buildOpportunityFingerprint,
  buildMaterialChangeHash,
  classifyOpportunityFeedEvent,
  isMateriallyNewOpportunity,
  buildFeedInsertRow,
  mapFeedRowToApiRow
} = require("./scanner/feedPipeline")

const MAX_API_LIMIT = 200
const FEED_PAGE_SIZE = 100
const DEFAULT_API_LIMIT = FEED_PAGE_SIZE
const MAX_FEED_LIMIT = FEED_PAGE_SIZE
const FEED_WINDOW_HOURS = 24
const DEFAULT_HISTORY_WINDOW_HOURS = 24
const MAX_HISTORY_WINDOW_HOURS = 168
const MANUAL_REFRESH_TRACKER_MAX = 4000
const LEGACY_SCANNER_TYPE = "global_arbitrage"
const SCANNER_RUN_RETENTION_HOURS = 24
const CATALOG_SCAN_CATEGORIES = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE,
  ITEM_CATEGORIES.KNIFE,
  ITEM_CATEGORIES.GLOVE
])

const scannerState = {
  timer: null,
  nextScheduledAt: null,
  inFlight: null,
  currentRunId: null,
  currentRunStartedAt: null
}

const enrichmentState = {
  timer: null,
  nextScheduledAt: null,
  inFlight: null,
  currentRunId: null,
  currentRunStartedAt: null
}

const rotationState = {
  cursor: 0,
  lastScannedAtByName: new Map()
}

const manualRefreshTracker = new Map()
const scannerEntitlements = planService.getEntitlements("alpha_access")

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

function isStatementTimeoutError(err) {
  const code = normalizeText(err?.code)
  const message = normalizeText(err?.message).toLowerCase()
  return (
    code === "57014" ||
    message.includes("statement timeout") ||
    message.includes("canceling statement due to statement timeout")
  )
}

function normalizeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return fallback
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on"
}

function normalizeLimit(value, fallback = DEFAULT_API_LIMIT, max = MAX_API_LIMIT) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), max)
}

function normalizePage(value, fallback = 1) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), 1)
}

function normalizeHistoryHours(value, fallback = DEFAULT_HISTORY_WINDOW_HOURS) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_HISTORY_WINDOW_HOURS)
}

function buildSinceIso(hours, nowMs = Date.now()) {
  const safeHours = normalizeHistoryHours(hours, DEFAULT_HISTORY_WINDOW_HOURS)
  return new Date(nowMs - safeHours * 60 * 60 * 1000).toISOString()
}

async function countFeedSafely(options = {}) {
  try {
    return {
      count: await arbitrageFeedRepo.countFeed(options),
      timedOut: false
    }
  } catch (err) {
    if (isStatementTimeoutError(err)) {
      return {
        count: null,
        timedOut: true
      }
    }
    throw err
  }
}

function normalizeCategoryFilter(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw || raw === "all") return "all"
  if (raw === "skins" || raw === "skin" || raw === ITEM_CATEGORIES.WEAPON_SKIN) return ITEM_CATEGORIES.WEAPON_SKIN
  if (raw === "cases" || raw === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (raw === "capsules" || raw === "capsule" || raw === ITEM_CATEGORIES.STICKER_CAPSULE) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === "knives" || raw === "knife" || raw === "future_knife") return ITEM_CATEGORIES.KNIFE
  if (raw === "gloves" || raw === "glove" || raw === "future_glove") return ITEM_CATEGORIES.GLOVE
  return "all"
}

function chunkArray(values = [], chunkSize = 25) {
  const rows = Array.isArray(values) ? values : []
  const size = Math.max(Number(chunkSize || 0), 1)
  const chunks = []
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size))
  }
  return chunks
}

function withTimeout(promise, timeoutMs, code = "SCANNER_TIMEOUT") {
  const safeTimeoutMs = Math.max(Math.round(Number(timeoutMs || 0)), 1)
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new AppError("Scanner timeout", 504, code)), safeTimeoutMs)
    )
  ])
}

function updateNextScheduledAt(scannerType) {
  const now = Date.now()
  if (scannerType === SCANNER_TYPES.ENRICHMENT) {
    enrichmentState.nextScheduledAt = new Date(now + ENRICHMENT_INTERVAL_MS).toISOString()
  } else {
    scannerState.nextScheduledAt = new Date(now + OPPORTUNITY_SCAN_INTERVAL_MS).toISOString()
  }
}

function trimRotationMap(maxSize = 15000) {
  if (rotationState.lastScannedAtByName.size <= maxSize) return
  const oldest = Array.from(rotationState.lastScannedAtByName.entries())
    .sort((a, b) => Number(a[1] || 0) - Number(b[1] || 0))
    .slice(0, rotationState.lastScannedAtByName.size - maxSize)
  for (const [name] of oldest) {
    rotationState.lastScannedAtByName.delete(name)
  }
}

function mapScannerCategory(category) {
  const normalized = normalizeCategoryFilter(category)
  if (normalized === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (normalized === ITEM_CATEGORIES.STICKER_CAPSULE) return ITEM_CATEGORIES.STICKER_CAPSULE
  if (normalized === ITEM_CATEGORIES.KNIFE) return ITEM_CATEGORIES.KNIFE
  if (normalized === ITEM_CATEGORIES.GLOVE) return ITEM_CATEGORIES.GLOVE
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function emptyCategoryMap() {
  return Object.fromEntries(ROUND_ROBIN_CATEGORY_ORDER.map((category) => [category, 0]))
}

function incrementCounter(target, key) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + 1
}

function confidenceLevel(value) {
  const normalized = normalizeText(value).toLowerCase()
  if (normalized === "high") return 3
  if (normalized === "medium") return 2
  return 1
}

function isScannerRunOverdue(status = {}, nowMs = Date.now()) {
  const latestRun = status?.latestRun || null
  const latestCompleted = status?.latestCompletedRun || null
  if (normalizeText(latestRun?.status).toLowerCase() === "running") return false
  const completedIso =
    toIsoOrNull(latestCompleted?.completed_at) ||
    toIsoOrNull(latestRun?.completed_at) ||
    toIsoOrNull(latestRun?.started_at)
  if (!completedIso) return true
  const elapsed = nowMs - new Date(completedIso).getTime()
  const threshold = OPPORTUNITY_SCAN_INTERVAL_MS + Math.max(Math.round(OPPORTUNITY_SCAN_INTERVAL_MS * 0.2), 15000)
  return elapsed > threshold
}

async function resolvePlanContext(options = {}) {
  if (options?.entitlements && typeof options.entitlements === "object") {
    const planTier = planService.normalizePlanTier(
      options.planTier || options.entitlements.planTier
    )
    return {
      userId: normalizeText(options.userId) || null,
      planTier,
      entitlements: options.entitlements
    }
  }
  const userId = normalizeText(options.userId)
  if (userId) {
    const profile = await planService.getUserPlanProfile(userId)
    return {
      userId,
      planTier: profile?.planTier || "free",
      entitlements: profile?.entitlements || planService.getEntitlements(profile?.planTier || "free")
    }
  }
  const planTier = planService.normalizePlanTier(options.planTier || "free")
  return {
    userId: null,
    planTier,
    entitlements: planService.getEntitlements(planTier)
  }
}

function pruneManualRefreshTracker(nowMs = Date.now()) {
  if (manualRefreshTracker.size <= MANUAL_REFRESH_TRACKER_MAX) return
  const staleCutoffMs = nowMs - 7 * 24 * 60 * 60 * 1000
  for (const [userId, entry] of manualRefreshTracker.entries()) {
    if (Number(entry?.lastTriggeredAtMs || 0) < staleCutoffMs) {
      manualRefreshTracker.delete(userId)
    }
    if (manualRefreshTracker.size <= MANUAL_REFRESH_TRACKER_MAX) break
  }
}

function formatRetryWindow(remainingMs) {
  const minutes = Math.max(Math.ceil(Number(remainingMs || 0) / 60000), 1)
  if (minutes >= 60) return `${Math.ceil(minutes / 60)} hour(s)`
  return `${minutes} minute(s)`
}

function enforceManualRefreshCooldown(userId, entitlements = {}, nowMs = Date.now()) {
  const safeUserId = normalizeText(userId)
  if (!safeUserId) return
  const previous = manualRefreshTracker.get(safeUserId)
  const lastTriggeredAtMs = Number(previous?.lastTriggeredAtMs || 0)
  const policy = planService.canRefreshScanner(entitlements, lastTriggeredAtMs, { nowMs })
  const intervalMinutes = Number(policy?.intervalMinutes || 0)
  if (!policy.allowed) {
    const retryAfterMs = Number(policy?.retryAfterMs || 0)
    const err = new AppError(
      `Manual scanner refresh is available every ${intervalMinutes} minute(s) on your plan. Try again in ${formatRetryWindow(
        retryAfterMs
      )}.`,
      429,
      "SCANNER_REFRESH_COOLDOWN"
    )
    err.retryAfterMs = retryAfterMs
    throw err
  }
  manualRefreshTracker.set(safeUserId, { lastTriggeredAtMs: nowMs, intervalMinutes })
  pruneManualRefreshTracker(nowMs)
}

async function enrichRowsWithSkinMetadata(rows = []) {
  const safeRows = Array.isArray(rows) ? rows : []
  if (!safeRows.length) return safeRows
  const names = Array.from(
    new Set(safeRows.map((row) => normalizeText(row.marketHashName || row.itemName)).filter(Boolean))
  )
  if (!names.length) return safeRows
  let skinRows = []
  try {
    skinRows = await skinRepo.getByMarketHashNames(names)
  } catch (_err) {
    skinRows = []
  }
  const byName = new Map((skinRows || []).map((row) => [normalizeText(row.market_hash_name), row]))
  return safeRows.map((row) => {
    const skin = byName.get(normalizeText(row.marketHashName || row.itemName))
    if (!skin) return row
    return {
      ...row,
      itemId: row.itemId || skin.id || null,
      itemImageUrl: row.itemImageUrl || skin.image_url || null,
      itemRarity: row.itemRarity || skin.rarity || null,
      itemRarityColor: row.itemRarityColor || skin.rarity_color || null
    }
  })
}

function applyFeedPlanRestrictions(rows = [], entitlements = {}) {
  const nowMs = Date.now()
  const delayedSignals = Boolean(entitlements?.delayedSignals)
  const signalDelayMinutes = Math.max(Number(entitlements?.signalDelayMinutes || 0), 0)
  const visibleFeedLimit = Math.max(Number(entitlements?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
  let filtered = Array.isArray(rows) ? [...rows] : []
  let delayedCount = 0
  if (delayedSignals && signalDelayMinutes > 0) {
    const cutoffMs = nowMs - signalDelayMinutes * 60 * 1000
    filtered = filtered.filter((row) => {
      const detectedAt = toIsoOrNull(row.detectedAt)
      if (!detectedAt) return false
      const visible = new Date(detectedAt).getTime() <= cutoffMs
      if (!visible) delayedCount += 1
      return visible
    })
  }
  const hiddenByLimit = 0
  const premiumLock = premiumCategoryAccessService.applyPremiumPreviewLock(filtered, entitlements)
  return {
    rows: premiumLock.rows,
    planLimits: {
      delayedSignals,
      signalDelayMinutes,
      feedTruncatedByDelay: delayedCount,
      hiddenByLimit,
      visibleFeedLimit,
      lockedPremiumPreviewRows: Number(premiumLock.lockedCount || 0)
    }
  }
}

async function loadScannerSourceRows() {
  const attempts = Array.from(
    new Set([
      Math.max(UNIVERSE_DB_LIMIT, OPPORTUNITY_BATCH_RUNTIME_TARGET),
      Math.max(Math.min(UNIVERSE_DB_LIMIT, 3000), OPPORTUNITY_BATCH_RUNTIME_TARGET),
      Math.max(Math.min(UNIVERSE_DB_LIMIT, 1500), OPPORTUNITY_BATCH_RUNTIME_TARGET),
      Math.max(OPPORTUNITY_BATCH_RUNTIME_TARGET * 4, 200)
    ])
  )
  const diagnostics = {
    attemptedLimits: [],
    selectedLimit: null,
    fallbackUsed: false,
    statementTimeoutFallbacks: 0,
    sourceMode: "catalog_status_scannable"
  }
  let lastError = null
  for (let index = 0; index < attempts.length; index += 1) {
    const limit = attempts[index]
    diagnostics.attemptedLimits.push(limit)
    try {
      const rows = await marketSourceCatalogRepo.listScannerSource({
        limit,
        categories: CATALOG_SCAN_CATEGORIES
      })
      diagnostics.selectedLimit = limit
      return { rows, diagnostics }
    } catch (err) {
      lastError = err
      if (!isStatementTimeoutError(err) || index >= attempts.length - 1) {
        throw err
      }
      diagnostics.fallbackUsed = true
      diagnostics.statementTimeoutFallbacks += 1
    }
  }
  throw lastError
}

function buildCompareInput(candidate = {}) {
  const referencePrice = Number(candidate.referencePrice || 0)
  const snapshotTs =
    candidate.latestMarketSignalAt ||
    candidate.latest_market_signal_at ||
    candidate.lastMarketSignalAt ||
    candidate.last_market_signal_at ||
    candidate.quoteFetchedAt ||
    candidate.snapshotCapturedAt ||
    null
  return {
    marketHashName: candidate.marketHashName,
    itemCategory: candidate.category,
    itemSubcategory: candidate.itemSubcategory || null,
    quantity: 1,
    steamPrice: Number.isFinite(referencePrice) && referencePrice > 0 ? referencePrice : 0,
    steamCurrency: "USD",
    steamRecordedAt: snapshotTs,
    volume7d: candidate.volume7d,
    marketCoverageCount: candidate.marketCoverageCount,
    marketVolume7d: candidate.volume7d,
    referencePrice: candidate.referencePrice
  }
}

async function compareCandidates(candidates = [], forceRefresh = false) {
  const byName = {}
  const allowLiveFetch = OPPORTUNITY_SCAN_ALLOW_LIVE_FETCH && Boolean(forceRefresh)
  const diagnostics = {
    batchesAttempted: 0,
    batchesCompleted: 0,
    batchesTimedOut: 0,
    batchesFailed: 0,
    chunkSize: SCAN_CHUNK_SIZE,
    allowLiveFetch,
    forceRefresh: allowLiveFetch
  }
  for (const chunk of chunkArray(candidates, SCAN_CHUNK_SIZE)) {
    diagnostics.batchesAttempted += 1
    try {
      const compared = await withTimeout(
        marketComparisonService.compareItems(chunk.map((row) => buildCompareInput(row)), {
          planTier: "alpha_access",
          entitlements: scannerEntitlements,
          allowLiveFetch,
          forceRefresh: allowLiveFetch
        }),
        SCAN_TIMEOUT_PER_BATCH_MS,
        "SCANNER_BATCH_TIMEOUT"
      )
      const rows = Array.isArray(compared?.items) ? compared.items : []
      for (const row of rows) {
        const name = normalizeText(row?.marketHashName)
        if (name) byName[name] = row
      }
      diagnostics.batchesCompleted += 1
    } catch (err) {
      if (String(err?.code || "").trim() === "SCANNER_BATCH_TIMEOUT") {
        diagnostics.batchesTimedOut += 1
      } else {
        diagnostics.batchesFailed += 1
      }
    }
  }
  return { byName, diagnostics }
}

function summarizeEvaluations(rows = []) {
  const summary = {
    scannedItems: rows.length,
    strongFound: 0,
    riskyFound: 0,
    speculativeFound: 0,
    rejectedFound: 0,
    opportunitiesByCategory: emptyCategoryMap(),
    rejectedByCategory: emptyCategoryMap(),
    rejectedByReason: {}
  }
  for (const row of rows) {
    const category = mapScannerCategory(row.itemCategory)
    if (row.tier === OPPORTUNITY_TIERS.STRONG) {
      summary.strongFound += 1
      summary.opportunitiesByCategory[category] += 1
    } else if (row.tier === OPPORTUNITY_TIERS.RISKY) {
      summary.riskyFound += 1
      summary.opportunitiesByCategory[category] += 1
    } else if (row.tier === OPPORTUNITY_TIERS.SPECULATIVE) {
      summary.speculativeFound += 1
      summary.opportunitiesByCategory[category] += 1
    } else {
      summary.rejectedFound += 1
      summary.rejectedByCategory[category] += 1
      for (const reason of row.hardRejectReasons || []) {
        incrementCounter(summary.rejectedByReason, reason)
      }
    }
  }
  return summary
}

function buildFeedKey(row = {}) {
  return buildSignature({
    itemName: row.itemName || row.item_name,
    buyMarket: row.buyMarket || row.buy_market,
    sellMarket: row.sellMarket || row.sell_market
  })
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function toSafeInteger(value, fallback = 1, min = 1) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), min)
}

function buildFeedUpdatePatch({
  previousRow = {},
  insertRow = {},
  nowIso,
  scanRunId,
  event
} = {}) {
  const previousMetadata = toJsonObject(previousRow?.metadata)
  const insertMetadata = toJsonObject(insertRow?.metadata)
  const firstSeenAt =
    toIsoOrNull(previousRow?.first_seen_at || previousRow?.firstSeenAt) ||
    toIsoOrNull(insertRow?.first_seen_at || insertRow?.firstSeenAt) ||
    toIsoOrNull(previousRow?.discovered_at || previousRow?.detected_at) ||
    nowIso
  const timesSeen = toSafeInteger(previousRow?.times_seen ?? previousRow?.timesSeen, 1, 1) + 1
  const eventType = normalizeText(event?.eventType || "updated").toLowerCase() || "updated"
  const materiallyChanged = Boolean(event?.materiallyChanged)
  const changeReasons = Array.isArray(event?.changeReasons) ? event.changeReasons : []

  const mergedMetadata = {
    ...previousMetadata,
    ...insertMetadata,
    feed_event: eventType,
    feed_event_reasons: changeReasons,
    feed_event_materially_changed: materiallyChanged,
    opportunity_fingerprint:
      normalizeText(insertRow?.opportunity_fingerprint) ||
      normalizeText(previousRow?.opportunity_fingerprint) ||
      null,
    material_change_hash:
      normalizeText(insertRow?.material_change_hash) ||
      normalizeText(previousRow?.material_change_hash) ||
      null,
    first_seen_at: firstSeenAt,
    last_seen_at: nowIso,
    last_published_at: nowIso,
    times_seen: timesSeen
  }

  return {
    item_name: insertRow.item_name,
    market_hash_name: insertRow.market_hash_name,
    category: insertRow.category,
    buy_market: insertRow.buy_market,
    buy_price: insertRow.buy_price,
    sell_market: insertRow.sell_market,
    sell_net: insertRow.sell_net,
    profit: insertRow.profit,
    spread_pct: insertRow.spread_pct,
    opportunity_score: insertRow.opportunity_score,
    execution_confidence: insertRow.execution_confidence,
    quality_grade: insertRow.quality_grade,
    liquidity_label: insertRow.liquidity_label,
    detected_at: nowIso,
    discovered_at: firstSeenAt,
    first_seen_at: firstSeenAt,
    last_seen_at: nowIso,
    last_published_at: nowIso,
    times_seen: timesSeen,
    opportunity_fingerprint:
      normalizeText(insertRow?.opportunity_fingerprint) ||
      normalizeText(previousRow?.opportunity_fingerprint) ||
      null,
    material_change_hash:
      normalizeText(insertRow?.material_change_hash) ||
      normalizeText(previousRow?.material_change_hash) ||
      null,
    market_signal_observed_at:
      insertRow?.market_signal_observed_at ?? previousRow?.market_signal_observed_at ?? null,
    feed_published_at: nowIso,
    scan_run_id: normalizeText(scanRunId) || null,
    is_active: true,
    is_duplicate: !materiallyChanged,
    metadata: mergedMetadata
  }
}

function attachEventMetaToInsertRow(insertRow = {}, event = {}) {
  const safeInsertRow = insertRow && typeof insertRow === "object" ? insertRow : {}
  const safeMetadata = toJsonObject(safeInsertRow.metadata)
  const eventType = normalizeText(event?.eventType || "new").toLowerCase() || "new"
  const changeReasons = Array.isArray(event?.changeReasons) ? event.changeReasons : []
  const materiallyChanged = Boolean(event?.materiallyChanged)
  return {
    ...safeInsertRow,
    is_duplicate: eventType === "duplicate",
    metadata: {
      ...safeMetadata,
      feed_event: eventType,
      feed_event_reasons: changeReasons,
      feed_event_materially_changed: materiallyChanged
    }
  }
}

async function persistFeedRows(opportunities = [], scanRunId = null) {
  const counters = {
    insertedCount: 0,
    newCount: 0,
    updatedCount: 0,
    reactivatedCount: 0,
    duplicateCount: 0,
    skippedUnchanged: 0,
    cleanup: {
      olderMarkedInactive: 0,
      beyondLimitMarkedInactive: 0,
      hadTimeout: false,
      errors: []
    }
  }
  const rows = (Array.isArray(opportunities) ? opportunities : []).filter((row) => row.rejected !== true)
  if (!rows.length) return counters
  const sinceIso = new Date(Date.now() - DUPLICATE_WINDOW_HOURS * 60 * 60 * 1000).toISOString()
  const names = Array.from(new Set(rows.map((row) => normalizeText(row.marketHashName || row.itemName)).filter(Boolean)))
  const nowIso = new Date().toISOString()
  const preparedRows = rows.map((opportunity) => {
    const insertRow = buildFeedInsertRow(opportunity, {
      scanRunId,
      detectedAt: nowIso,
      firstSeenAt: nowIso,
      lastSeenAt: nowIso,
      lastPublishedAt: nowIso,
      timesSeen: 1
    })
    const fingerprint =
      normalizeText(insertRow?.opportunity_fingerprint) ||
      buildOpportunityFingerprint(opportunity)
    const materialChangeHash =
      normalizeText(insertRow?.material_change_hash) ||
      buildMaterialChangeHash(opportunity)
    insertRow.opportunity_fingerprint = fingerprint
    insertRow.material_change_hash = materialChangeHash
    insertRow.metadata = {
      ...toJsonObject(insertRow.metadata),
      opportunity_fingerprint: fingerprint || null,
      material_change_hash: materialChangeHash || null
    }
    return {
      opportunity,
      insertRow,
      key: buildFeedKey(insertRow) || buildFeedKey(opportunity),
      fingerprint
    }
  })
  let previousRows = []
  try {
    previousRows = await arbitrageFeedRepo.getRecentRowsByItems({
      itemNames: names,
      sinceIso,
      limit: 1200
    })
  } catch (err) {
    if (isStatementTimeoutError(err)) {
      counters.cleanup.hadTimeout = true
      counters.cleanup.errors.push("recent_feed_lookup_timeout")
      previousRows = []
    } else {
      throw err
    }
  }
  let activeFingerprintRows = []
  const fingerprints = Array.from(
    new Set(preparedRows.map((row) => normalizeText(row.fingerprint)).filter(Boolean))
  )
  if (fingerprints.length) {
    try {
      activeFingerprintRows = await arbitrageFeedRepo.getActiveRowsByFingerprints({
        fingerprints,
        sinceIso,
        limit: Math.max(1200, fingerprints.length * 2)
      })
    } catch (err) {
      if (isStatementTimeoutError(err)) {
        counters.cleanup.hadTimeout = true
        counters.cleanup.errors.push("active_fingerprint_lookup_timeout")
        activeFingerprintRows = []
      } else {
        throw err
      }
    }
  }

  const latestByKey = {}
  for (const previous of previousRows || []) {
    const key = buildFeedKey(previous)
    if (key && !latestByKey[key]) latestByKey[key] = previous
  }
  const activeByFingerprint = {}
  for (const previous of activeFingerprintRows || []) {
    const fingerprint = normalizeText(
      previous?.opportunity_fingerprint ||
        previous?.opportunityFingerprint ||
        previous?.metadata?.opportunity_fingerprint
    )
    if (fingerprint && !activeByFingerprint[fingerprint]) {
      activeByFingerprint[fingerprint] = previous
    }
  }

  const insertRows = []
  const updateRows = []
  const pendingInsertFingerprints = new Set()

  for (const prepared of preparedRows) {
    const opportunity = prepared.opportunity
    const key = prepared.key
    const fingerprint = normalizeText(prepared.fingerprint)
    let previous = fingerprint ? activeByFingerprint[fingerprint] || null : null
    let matchedBy = previous ? "fingerprint" : null
    if (!previous && key) {
      previous = latestByKey[key] || null
      matchedBy = previous ? "signature" : null
    }

    const baseEvent = classifyOpportunityFeedEvent(opportunity, previous)
    const previousMaterialHash = normalizeText(
      previous?.material_change_hash ||
        previous?.materialChangeHash ||
        previous?.metadata?.material_change_hash
    )
    const nextMaterialHash = normalizeText(prepared.insertRow?.material_change_hash)
    const fingerprintShifted =
      Boolean(previous) &&
      Boolean(fingerprint) &&
      normalizeText(previous?.opportunity_fingerprint) &&
      normalizeText(previous?.opportunity_fingerprint) !== fingerprint
    const materialHashShifted =
      Boolean(previous) &&
      Boolean(previousMaterialHash) &&
      Boolean(nextMaterialHash) &&
      previousMaterialHash !== nextMaterialHash
    const materiallyChanged =
      Boolean(baseEvent?.materiallyChanged) ||
      fingerprintShifted ||
      materialHashShifted
    const extraReasons = []
    if (fingerprintShifted) extraReasons.push("quote_identity")
    if (materialHashShifted) extraReasons.push("material_hash")
    if (matchedBy === "signature" && fingerprintShifted) extraReasons.push("fingerprint_shift")
    const normalizedEventType = !previous
      ? "new"
      : !Boolean(previous?.is_active)
        ? "reactivated"
        : materiallyChanged
          ? "updated"
          : "duplicate"
    const normalizedEvent = {
      eventType: normalizedEventType,
      materiallyChanged,
      changeReasons: Array.from(
        new Set([...(Array.isArray(baseEvent?.changeReasons) ? baseEvent.changeReasons : []), ...extraReasons])
      )
    }
    const insertRowWithEvent = attachEventMetaToInsertRow(prepared.insertRow, normalizedEvent)

    if (!previous) {
      if (fingerprint && pendingInsertFingerprints.has(fingerprint)) {
        counters.duplicateCount += 1
        counters.skippedUnchanged += 1
        continue
      }
      counters.newCount += 1
      if (fingerprint) pendingInsertFingerprints.add(fingerprint)
      insertRows.push(insertRowWithEvent)
      continue
    }

    if (normalizedEvent.eventType === "updated") {
      counters.updatedCount += 1
    } else if (normalizedEvent.eventType === "reactivated") {
      counters.reactivatedCount += 1
    } else {
      counters.duplicateCount += 1
    }

    const patch = buildFeedUpdatePatch({
      previousRow: previous,
      insertRow: insertRowWithEvent,
      nowIso,
      scanRunId,
      event: normalizedEvent
    })
    updateRows.push({
      id: previous.id,
      patch
    })

    const cachedUpdatedRow = {
      ...previous,
      ...patch,
      metadata: toJsonObject(patch.metadata),
      is_active: true,
      id: previous.id
    }
    if (key) latestByKey[key] = cachedUpdatedRow
    if (fingerprint) activeByFingerprint[fingerprint] = cachedUpdatedRow
  }

  if (updateRows.length) {
    await arbitrageFeedRepo.updateRowsById(updateRows)
  }
  if (insertRows.length) {
    const inserted = await arbitrageFeedRepo.insertRows(insertRows)
    counters.insertedCount = Array.isArray(inserted) ? inserted.length : 0
  }
  const cutoffIso = new Date(Date.now() - FEED_WINDOW_HOURS * 60 * 60 * 1000).toISOString()
  const cleanupResults = await Promise.allSettled([
    arbitrageFeedRepo.markInactiveOlderThan(cutoffIso, { batchSize: 120, maxRows: 600 })
  ])
  const olderResult = cleanupResults[0]
  if (olderResult?.status === "fulfilled") {
    counters.cleanup.olderMarkedInactive = Number(olderResult.value || 0)
  } else if (olderResult?.status === "rejected") {
    const reason = olderResult.reason
    counters.cleanup.hadTimeout ||= isStatementTimeoutError(reason)
    counters.cleanup.errors.push(normalizeText(reason?.message) || "older_cleanup_failed")
  }
  return counters
}

function mergeDiagnostics({
  selection = {},
  compare = {},
  evaluations = {},
  persisted = {},
  sourceCatalog = {},
  timing = {},
  runtimeConfig = {}
} = {}) {
  const selectionDiag = selection.diagnostics || {}
  return {
    scanStateCounts: {
      scanable: Number(selectionDiag.scanable || 0),
      scanableWithPenalties: Number(selectionDiag.scanableWithPenalties || 0),
      hardReject: Number(selectionDiag.hardReject || 0)
    },
    scanStateByCategory: selectionDiag.stateByCategory || {},
    candidatePoolSize: Number(selection.poolSize || 0),
    requestedBatchSize: Number(selection.attemptedBatchSize || 0),
    selectedBatchSize: Number(selection.selected?.length || 0),
    scannedItems: Number(evaluations.scannedItems || 0),
    strongFound: Number(evaluations.strongFound || 0),
    riskyFound: Number(evaluations.riskyFound || 0),
    speculativeFound: Number(evaluations.speculativeFound || 0),
    rejectedFound: Number(evaluations.rejectedFound || 0),
    opportunitiesByCategory: evaluations.opportunitiesByCategory || {},
    rejectedByCategory: evaluations.rejectedByCategory || {},
    rejectedByReason: evaluations.rejectedByReason || {},
    persisted: {
      insertedCount: Number(persisted.insertedCount || 0),
      newCount: Number(persisted.newCount || 0),
      updatedCount: Number(persisted.updatedCount || 0),
      reactivatedCount: Number(persisted.reactivatedCount || 0),
      duplicateCount: Number(persisted.duplicateCount || 0),
      skippedUnchanged: Number(persisted.skippedUnchanged || 0),
      cleanup: persisted.cleanup || {}
    },
    sourceCatalog,
    batchScan: compare,
    timing: {
      selectionMs: Number(timing.selectionMs || 0),
      dbQueryMs: Number(timing.dbQueryMs || 0),
      computeMs: Number(timing.computeMs || 0),
      writeMs: Number(timing.writeMs || 0),
      totalRunMs: Number(timing.totalRunMs || 0)
    },
    runtimeConfig: {
      configuredBatchTarget: Number(runtimeConfig.configuredBatchTarget || 0),
      hotTarget: Number(runtimeConfig.hotTarget || 0),
      safeBatchSize: Number(runtimeConfig.safeBatchSize || 0),
      runtimeBatchTarget: Number(runtimeConfig.runtimeBatchTarget || 0),
      scanChunkSize: Number(runtimeConfig.scanChunkSize || 0),
      scanAllowLiveFetch: Boolean(runtimeConfig.scanAllowLiveFetch),
      hardTimeoutMs: Number(runtimeConfig.hardTimeoutMs || 0)
    },
    scanProgress: {
      cursorBefore: Number(selection.cursorBefore || 0),
      cursorAfter: Number(selection.nextCursor || 0),
      rotationPoolSize: Number(selection.poolSize || 0)
    }
  }
}

function noOpportunitiesReason(summary = {}, status = {}, rows = []) {
  if (Array.isArray(rows) && rows.length) return null
  if (normalizeText(status?.currentStatus).toLowerCase() === "running") {
    return {
      code: "scan_in_progress",
      message: "Scanner run is in progress. Feed will update after completion."
    }
  }
  if (!Number(summary?.scannedItems || 0)) {
    return {
      code: "no_items_scanned",
      message: "No catalog rows were scannable in the latest run."
    }
  }
  const reasons = summary?.discardedReasons || {}
  const topReason = Object.entries(reasons)
    .filter(([, count]) => Number(count || 0) > 0)
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))[0]
  if (!topReason) {
    return {
      code: "no_matching_feed_rows",
      message: "Scanner completed but no opportunities were qualified."
    }
  }
  return {
    code: topReason[0],
    count: Number(topReason[1] || 0),
    message: "Most scanned rows were rejected by anti-fake hard-reject rules."
  }
}

async function runEnrichmentJob({ forceRefresh = false } = {}) {
  const sourceCatalogDiagnostics = await marketSourceCatalogService.prepareSourceCatalog({
    forceRefresh: Boolean(forceRefresh),
    targetUniverseSize: DEFAULT_UNIVERSE_LIMIT
  }).catch((err) => ({
    error: normalizeText(err?.message) || "source_catalog_prepare_failed"
  }))

  const catalogRows = await marketSourceCatalogRepo.listActiveTradable({
    limit: ENRICHMENT_BATCH_TARGET,
    categories: CATALOG_SCAN_CATEGORIES
  }).catch(() => [])
  const names = Array.from(
    new Set((catalogRows || []).map((row) => normalizeText(row?.market_hash_name)).filter(Boolean))
  ).slice(0, ENRICHMENT_BATCH_TARGET)

  let imageUpdated = 0
  if (names.length) {
    try {
      const byName = await withTimeout(
        marketImageService.fetchSteamSearchMetadataBatch(names, {
          timeoutMs: 7000,
          maxRetries: 2,
          concurrency: 2
        }),
        20000,
        "SCANNER_IMAGE_ENRICH_TIMEOUT"
      )
      const upserts = Object.entries(byName || {}).map(([name, metadata]) => ({
        market_hash_name: name,
        image_url: metadata?.imageUrl || null,
        rarity: metadata?.rarity || null,
        rarity_color: metadata?.rarityColor || null
      }))
      if (upserts.length) {
        await skinRepo.upsertSkins(upserts)
      }
      imageUpdated = upserts.length
    } catch (_err) {
      imageUpdated = 0
    }
  }

  return {
    selectedCount: names.length,
    opportunitiesFound: 0,
    newOpportunitiesAdded: 0,
    diagnostics: {
      sourceCatalog: sourceCatalogDiagnostics,
      imageEnrichment: {
        attempted: names.length,
        updated: imageUpdated
      }
    }
  }
}

async function runOpportunityJob({ forceRefresh = false } = {}) {
  const runStartedAtMs = Date.now()
  const dbQueryStartedAtMs = Date.now()
  const catalogLoad = await loadScannerSourceRows()
  const catalogRows = Array.isArray(catalogLoad?.rows) ? catalogLoad.rows : []
  const dbQueryMs = Date.now() - dbQueryStartedAtMs

  const selectionStartedAtMs = Date.now()
  const selection = selectScanCandidates({
    catalogRows,
    batchSize: OPPORTUNITY_BATCH_RUNTIME_TARGET,
    cursor: rotationState.cursor,
    lastScannedAtByName: rotationState.lastScannedAtByName
  })
  selection.cursorBefore = rotationState.cursor
  rotationState.cursor = selection.nextCursor
  trimRotationMap()
  const selectionMs = Date.now() - selectionStartedAtMs

  const computeStartedAtMs = Date.now()
  const compared = await compareCandidates(selection.selected || [], forceRefresh)
  const evaluated = (selection.selected || []).map((candidate) =>
    evaluateCandidateOpportunity(candidate, compared.byName?.[candidate.marketHashName] || {})
  )
  const evaluationSummary = summarizeEvaluations(evaluated)
  const opportunities = evaluated.filter((row) => row.rejected !== true)
  const computeMs = Date.now() - computeStartedAtMs

  const writeStartedAtMs = Date.now()
  const persisted = await persistFeedRows(opportunities)
  const writeMs = Date.now() - writeStartedAtMs
  const totalRunMs = Date.now() - runStartedAtMs

  return {
    selectedCount: Number(selection.selected?.length || 0),
    opportunitiesFound: opportunities.length,
    newOpportunitiesAdded: Number(persisted.newCount || 0),
    diagnostics: mergeDiagnostics({
      selection,
      compare: compared.diagnostics,
      evaluations: evaluationSummary,
      persisted,
      sourceCatalog: {
        mode: "catalog_status_scannable",
        scannerSourceSize: catalogRows.length,
        catalogLoad: catalogLoad?.diagnostics || {}
      },
      timing: {
        selectionMs,
        dbQueryMs,
        computeMs,
        writeMs,
        totalRunMs
      },
      runtimeConfig: {
        configuredBatchTarget: OPPORTUNITY_BATCH_TARGET,
        hotTarget: OPPORTUNITY_HOT_TARGET,
        safeBatchSize: OPPORTUNITY_SAFE_BATCH_SIZE,
        runtimeBatchTarget: OPPORTUNITY_BATCH_RUNTIME_TARGET,
        scanChunkSize: SCAN_CHUNK_SIZE,
        scanAllowLiveFetch: OPPORTUNITY_SCAN_ALLOW_LIVE_FETCH,
        hardTimeoutMs: OPPORTUNITY_HARD_TIMEOUT_MS
      }
    })
  }
}

function formatRunResult(input = {}) {
  return {
    scanRunId: input.scanRunId || null,
    status: input.status || "started",
    alreadyRunning: Boolean(input.alreadyRunning),
    startedAt: input.startedAt || new Date().toISOString(),
    elapsedMs: input.elapsedMs == null ? null : Number(input.elapsedMs),
    existingRunId: input.existingRunId || null,
    existingRunStartedAt: input.existingRunStartedAt || null,
    blockedByCrossJob: Boolean(input.blockedByCrossJob),
    blockingScannerType: input.blockingScannerType || null,
    blockingRunId: input.blockingRunId || null,
    blockingRunStartedAt: input.blockingRunStartedAt || null,
    blockingElapsedMs: input.blockingElapsedMs == null ? null : Number(input.blockingElapsedMs)
  }
}

async function runJobWithLock({
  scannerType,
  state,
  peerState,
  peerScannerType,
  timeoutMs,
  hardTimeoutMs = null,
  trigger,
  forceRefresh,
  worker
}) {
  const safeScannerType = normalizeText(scannerType) || LEGACY_SCANNER_TYPE
  const safeTimeoutMs = Math.max(Math.round(Number(timeoutMs || 0)), 1000)
  const safeHardTimeoutMs = Math.max(Math.round(Number(hardTimeoutMs || 0)), 0)
  if (state.inFlight) {
    return formatRunResult({
      scanRunId: state.currentRunId || null,
      status: "already_running",
      alreadyRunning: true,
      startedAt: state.currentRunStartedAt || null,
      elapsedMs:
        state.currentRunStartedAt && toIsoOrNull(state.currentRunStartedAt)
          ? Date.now() - new Date(state.currentRunStartedAt).getTime()
          : null
    })
  }
  if (!ALLOW_CROSS_JOB_PARALLELISM && peerState.inFlight) {
    return formatRunResult({
      status: "blocked_by_cross_job",
      blockedByCrossJob: true,
      blockingScannerType: peerScannerType,
      blockingRunId: peerState.currentRunId || null,
      blockingRunStartedAt: peerState.currentRunStartedAt || null,
      blockingElapsedMs:
        peerState.currentRunStartedAt && toIsoOrNull(peerState.currentRunStartedAt)
          ? Date.now() - new Date(peerState.currentRunStartedAt).getTime()
          : null
      })
  }

  if (safeHardTimeoutMs > 0) {
    const nowIso = new Date().toISOString()
    const staleCutoffIso = new Date(Date.now() - safeHardTimeoutMs).toISOString()
    await scannerRunRepo
      .timeoutStaleRunningRuns(safeScannerType, {
        cutoffIso: staleCutoffIso,
        nowIso,
        failureReason: `${safeScannerType}_hard_timeout_recovery`,
        diagnosticsSummary: {
          trigger,
          hardTimeoutMs: safeHardTimeoutMs,
          recoveredFromStaleLock: true
        }
      })
      .catch(() => 0)
  }

  const startedAt = new Date().toISOString()
  let runInsert = await scannerRunRepo.tryCreateRunningRun({
    scannerType: safeScannerType,
    startedAt
  })
  if (runInsert.alreadyRunning && safeHardTimeoutMs > 0) {
    const existingStartedAt = toIsoOrNull(runInsert?.existingRun?.started_at)
    const existingElapsedMs = existingStartedAt ? Date.now() - new Date(existingStartedAt).getTime() : null
    if (Number(existingElapsedMs || 0) > safeHardTimeoutMs) {
      const nowIso = new Date().toISOString()
      const staleCutoffIso = new Date(Date.now() - safeHardTimeoutMs).toISOString()
      await scannerRunRepo
        .timeoutStaleRunningRuns(safeScannerType, {
          cutoffIso: staleCutoffIso,
          nowIso,
          failureReason: `${safeScannerType}_stale_lock_recovered`,
          diagnosticsSummary: {
            trigger,
            hardTimeoutMs: safeHardTimeoutMs,
            recoveredFromStaleLock: true
          }
        })
        .catch(() => 0)
      runInsert = await scannerRunRepo.tryCreateRunningRun({
        scannerType: safeScannerType,
        startedAt
      })
    }
  }
  if (runInsert.alreadyRunning) {
    if (RECORD_SKIPPED_ALREADY_RUNNING) {
      await scannerRunRepo.createRun({
        scannerType,
        startedAt,
        completedAt: startedAt,
        status: "skipped_already_running",
        diagnosticsSummary: { trigger, reason: "already_running" }
      }).catch(() => null)
    }
    return formatRunResult({
      status: "already_running",
      alreadyRunning: true,
      existingRunId: runInsert.existingRun?.id || null,
      existingRunStartedAt: runInsert.existingRun?.started_at || null
    })
  }

  const runId = runInsert?.run?.id || null
  state.currentRunId = runId
  state.currentRunStartedAt = startedAt
  state.inFlight = (async () => {
    const startedAtMs = Date.now()
    try {
      const result = await withTimeout(worker({ forceRefresh }), safeTimeoutMs, "SCANNER_JOB_TIMEOUT")
      await scannerRunRepo.markCompleted(runId, {
        completedAt: new Date().toISOString(),
        itemsScanned: Number(result?.selectedCount || 0),
        opportunitiesFound: Number(result?.opportunitiesFound || 0),
        newOpportunitiesAdded: Number(result?.newOpportunitiesAdded || 0),
        durationMs: Date.now() - startedAtMs,
        diagnosticsSummary: {
          trigger,
          timeoutMs: safeTimeoutMs,
          hardTimeoutMs: safeHardTimeoutMs || null,
          ...(result?.diagnostics || {})
        }
      })
    } catch (err) {
      const timedOut = String(err?.code || "").trim() === "SCANNER_JOB_TIMEOUT"
      const dbStatementTimedOut = isStatementTimeoutError(err)
      const elapsedMs = Date.now() - startedAtMs
      await scannerRunRepo.markFailed(runId, {
        completedAt: new Date().toISOString(),
        status: timedOut || dbStatementTimedOut ? "timed_out" : "failed",
        durationMs: elapsedMs,
        failureReason: timedOut
          ? `${safeScannerType}_timeout_after_${safeTimeoutMs}ms`
          : dbStatementTimedOut
            ? `${safeScannerType}_db_statement_timeout`
          : normalizeText(err?.message) || "scanner_job_failed",
        diagnosticsSummary: {
          trigger,
          timedOut,
          dbStatementTimedOut,
          errorCode: normalizeText(err?.code) || null,
          timeoutMs: safeTimeoutMs,
          hardTimeoutMs: safeHardTimeoutMs || null,
          elapsedMs
        }
      })
    } finally {
      const cutoffIso = new Date(
        Date.now() - SCANNER_RUN_RETENTION_HOURS * 60 * 60 * 1000
      ).toISOString()
      scannerRunRepo
        .deleteOlderThan(cutoffIso, { excludeRunning: true })
        .catch(() => null)
      state.inFlight = null
      state.currentRunId = null
      state.currentRunStartedAt = null
    }
  })()
  state.inFlight.catch(() => null)

  return formatRunResult({
    scanRunId: runId,
    status: "started",
    alreadyRunning: false,
    startedAt
  })
}

async function enqueueScan(options = {}) {
  return runJobWithLock({
    scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    state: scannerState,
    peerState: enrichmentState,
    peerScannerType: SCANNER_TYPES.ENRICHMENT,
    timeoutMs: OPPORTUNITY_HARD_TIMEOUT_MS,
    hardTimeoutMs: OPPORTUNITY_HARD_TIMEOUT_MS,
    trigger: normalizeText(options.trigger || "system"),
    forceRefresh: Boolean(options.forceRefresh),
    worker: runOpportunityJob
  })
}

async function enqueueEnrichment(options = {}) {
  return runJobWithLock({
    scannerType: SCANNER_TYPES.ENRICHMENT,
    state: enrichmentState,
    peerState: scannerState,
    peerScannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    timeoutMs: ENRICHMENT_JOB_TIMEOUT_MS,
    trigger: normalizeText(options.trigger || "system"),
    forceRefresh: Boolean(options.forceRefresh),
    worker: runEnrichmentJob
  })
}

async function getScannerStatusInternal() {
  const [latestRun, latestCompletedRun, latestEnrichmentRun, latestEnrichmentCompletedRun, activeOpportunities] =
    await Promise.all([
      scannerRunRepo.getLatestRun(SCANNER_TYPES.OPPORTUNITY_SCAN),
      scannerRunRepo.getLatestCompletedRun(SCANNER_TYPES.OPPORTUNITY_SCAN),
      scannerRunRepo.getLatestRun(SCANNER_TYPES.ENRICHMENT),
      scannerRunRepo.getLatestCompletedRun(SCANNER_TYPES.ENRICHMENT),
      arbitrageFeedRepo.countFeed({ includeInactive: false }).catch(() => 0)
    ])

  const currentStatus =
    scannerState.inFlight || normalizeText(latestRun?.status).toLowerCase() === "running"
      ? "running"
      : "idle"

  return {
    schedulerRunning: Boolean(scannerState.timer || enrichmentState.timer),
    currentStatus,
    currentRunId: scannerState.currentRunId || null,
    currentRunStartedAt: scannerState.currentRunStartedAt || null,
    currentRunElapsedMs:
      scannerState.currentRunStartedAt && toIsoOrNull(scannerState.currentRunStartedAt)
        ? Date.now() - new Date(scannerState.currentRunStartedAt).getTime()
        : null,
    nextScheduledAt: scannerState.nextScheduledAt,
    activeOpportunities: Number(activeOpportunities || 0),
    latestRun: latestRun || null,
    latestCompletedRun: latestCompletedRun || null,
    coordination: {
      allowCrossJobParallelism: ALLOW_CROSS_JOB_PARALLELISM,
      overdue: isScannerRunOverdue({ latestRun, latestCompletedRun })
    },
    jobs: {
      [SCANNER_TYPES.OPPORTUNITY_SCAN]: {
        scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
        intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
        nextScheduledAt: scannerState.nextScheduledAt,
        status: currentStatus,
        latestRun: latestRun || null,
        latestCompletedRun: latestCompletedRun || null
      },
      [SCANNER_TYPES.ENRICHMENT]: {
        scannerType: SCANNER_TYPES.ENRICHMENT,
        intervalMinutes: ENRICHMENT_INTERVAL_MINUTES,
        nextScheduledAt: enrichmentState.nextScheduledAt,
        status:
          enrichmentState.inFlight || normalizeText(latestEnrichmentRun?.status).toLowerCase() === "running"
            ? "running"
            : "idle",
        latestRun: latestEnrichmentRun || null,
        latestCompletedRun: latestEnrichmentCompletedRun || null
      }
    }
  }
}

exports.getFeed = async (options = {}) => {
  const planContext = await resolvePlanContext(options)
  const entitlements = planContext?.entitlements || planService.getEntitlements(planContext?.planTier)
  const advancedFiltersEnabled = planService.canUseAdvancedFilters(entitlements)

  const requestedLimit = normalizeLimit(options.limit, DEFAULT_API_LIMIT, MAX_API_LIMIT)
  const limit = FEED_PAGE_SIZE
  const requestedPage = normalizePage(options.page, 1)
  const requestedHistoryHours = normalizeHistoryHours(options.historyHours, FEED_WINDOW_HOURS)
  const historyWindowHours = FEED_WINDOW_HOURS
  const historySinceIso = buildSinceIso(historyWindowHours)

  const requestedShowRisky =
    options.showRisky == null ? true : normalizeBoolean(options.showRisky)
  const requestedIncludeOlder = normalizeBoolean(options.includeOlder || options.showOlder)
  const requestedCategory = normalizeCategoryFilter(options.category)
  const showRisky = advancedFiltersEnabled ? requestedShowRisky : true
  const includeOlder = advancedFiltersEnabled ? requestedIncludeOlder : false
  const categoryFilter = advancedFiltersEnabled ? requestedCategory : "all"

  const totalCountPromise = countFeedSafely({
    includeInactive: includeOlder,
    category: categoryFilter === "all" ? "" : categoryFilter,
    minScore: 0,
    excludeLowConfidence: false,
    highConfidenceOnly: !showRisky,
    sinceIso: historySinceIso
  })
  const activeCountPromise = includeOlder
    ? countFeedSafely({
        includeInactive: false,
        category: categoryFilter === "all" ? "" : categoryFilter,
        minScore: 0,
        excludeLowConfidence: false,
        highConfidenceOnly: !showRisky,
        sinceIso: historySinceIso
      })
    : totalCountPromise

  const [status, totalCountResult, activeCountResult] = await Promise.all([
    getScannerStatusInternal(),
    totalCountPromise,
    activeCountPromise
  ])

  const knownTotalCount = Number.isFinite(Number(totalCountResult?.count))
    ? Math.max(Number(totalCountResult.count), 0)
    : null
  const knownActiveCount = Number.isFinite(Number(activeCountResult?.count))
    ? Math.max(Number(activeCountResult.count), 0)
    : null
  const knownTotalPages =
    knownTotalCount == null ? null : Math.max(Math.ceil(knownTotalCount / limit), 1)
  const page = knownTotalPages == null ? requestedPage : Math.min(requestedPage, knownTotalPages)
  const offset = (page - 1) * limit

  const rawFeedRows = await arbitrageFeedRepo.listFeed({
    limit: knownTotalCount == null ? limit + 1 : limit,
    offset,
    includeInactive: includeOlder,
    category: categoryFilter === "all" ? "" : categoryFilter,
    minScore: 0,
    excludeLowConfidence: false,
    highConfidenceOnly: !showRisky,
    sinceIso: historySinceIso
  })
  const hasExtraRow = knownTotalCount == null && rawFeedRows.length > limit
  const feedRows = hasExtraRow ? rawFeedRows.slice(0, limit) : rawFeedRows

  const derivedTotalCount =
    knownTotalCount == null
      ? Math.max(offset + feedRows.length + (hasExtraRow ? 1 : 0), 0)
      : knownTotalCount
  const totalPages =
    knownTotalPages == null ? Math.max(page + (hasExtraRow ? 1 : 0), 1) : knownTotalPages
  const hasNextPage = knownTotalPages == null ? hasExtraRow : page < totalPages
  const activeCount =
    includeOlder
      ? knownActiveCount == null
        ? Number(status?.activeOpportunities || 0)
        : knownActiveCount
      : derivedTotalCount

  const countDiagnostics = {
    totalCountTimedOut: Boolean(totalCountResult?.timedOut),
    activeCountTimedOut: Boolean(activeCountResult?.timedOut)
  }

  let publishRefreshDiagnostics = {
    attempted: 0,
    refreshed: 0,
    admitted: 0,
    live: 0,
    stale: 0,
    degraded: 0,
    suppressed: 0,
    quoteRowsFound: 0
  }
  let mappedRows = []
  try {
    const refreshed = await feedPublishRefreshService.refreshForFeedPublish(feedRows, {
      includeRisky: showRisky,
      persist: true
    })
    publishRefreshDiagnostics =
      refreshed?.diagnostics && typeof refreshed.diagnostics === "object"
        ? refreshed.diagnostics
        : publishRefreshDiagnostics
    mappedRows = Array.isArray(refreshed?.rows) ? refreshed.rows : []
  } catch (_err) {
    mappedRows = (feedRows || []).map((row) => ({
      ...mapFeedRowToApiRow(row),
      refreshStatus: "failed",
      liveStatus: "degraded"
    }))
  }

  mappedRows = await enrichRowsWithSkinMetadata(mappedRows)
  const restricted = applyFeedPlanRestrictions(mappedRows, entitlements)
  mappedRows = restricted.rows

  const latestCompleted = status?.latestCompletedRun || null
  const diagnostics = latestCompleted?.diagnostics_summary || {}
  const summary = {
    scannedItems: Number(diagnostics.scannedItems || latestCompleted?.items_scanned || 0),
    opportunities: mappedRows.length,
    totalDetected: Number(derivedTotalCount || 0),
    activeOpportunities: Number(activeCount || 0),
    candidateItems: Number(diagnostics.candidatePoolSize || 0),
    discardedReasons: diagnostics.rejectedByReason || {},
    discardedReasonsByCategory: diagnostics.rejectedByCategory || {},
    rejectedByCategory: diagnostics.rejectedByCategory || {},
    opportunitiesByCategory: diagnostics.opportunitiesByCategory || {},
    sourceCatalog: diagnostics.sourceCatalog || {},
    scanStateCounts: diagnostics.scanStateCounts || {},
    scanStateByCategory: diagnostics.scanStateByCategory || {},
    scanProgress: diagnostics.scanProgress || {},
    batchScan: diagnostics.batchScan || {},
    timing: diagnostics.timing || {},
    runtimeConfig: diagnostics.runtimeConfig || {},
    highConfidence: Number(diagnostics.strongFound || 0),
    riskyEligible: Number(diagnostics.riskyFound || 0),
    speculativeEligible: Number(diagnostics.speculativeFound || 0),
    newOpportunitiesAdded: Number(latestCompleted?.new_opportunities_added || diagnostics?.persisted?.newCount || 0),
    updatedOpportunities: Number(diagnostics?.persisted?.updatedCount || 0),
    reactivatedOpportunities: Number(diagnostics?.persisted?.reactivatedCount || 0),
    signalEventsAdded: Number(diagnostics?.persisted?.insertedCount || 0),
    feedRetentionHours: FEED_WINDOW_HOURS,
    historyWindowHours,
    countDiagnostics,
    publishRefresh: publishRefreshDiagnostics,
    plan: {
      planTier: planContext?.planTier || "free",
      requestedLimit,
      appliedLimit: limit,
      requestedPage,
      appliedPage: page,
      requestedHistoryHours,
      appliedHistoryHours: historyWindowHours,
      requestedShowRisky,
      appliedShowRisky: showRisky,
      requestedIncludeOlder,
      appliedIncludeOlder: includeOlder,
      requestedCategory,
      appliedCategory: categoryFilter,
      liveSignalMaxAgeHours: Number(feedPublishRefreshService.LIVE_MAX_SIGNAL_AGE_HOURS || 2),
      ...restricted.planLimits
    }
  }
  summary.noOpportunitiesReason = noOpportunitiesReason(summary, status, mappedRows)
  const pagination = {
    page,
    pageSize: limit,
    totalCount: Number(derivedTotalCount || 0),
    totalPages,
    hasPrevPage: page > 1,
    hasNextPage,
    historyHours: historyWindowHours
  }

  return {
    generatedAt: latestCompleted?.completed_at || latestCompleted?.started_at || null,
    ttlSeconds: Math.round(OPPORTUNITY_SCAN_INTERVAL_MS / 1000),
    currency: "USD",
    summary,
    pagination,
    opportunities: mappedRows,
    plan: summary.plan,
    status: {
      scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
      intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
      schedulerRunning: Boolean(status?.schedulerRunning),
      currentStatus: status?.currentStatus || "idle",
      currentRunId: status?.currentRunId || null,
      nextScheduledAt: status?.nextScheduledAt || null,
      activeOpportunities: Number(status?.activeOpportunities || 0),
      latestRun: status?.latestRun || null,
      latestCompletedRun: latestCompleted,
      jobs: status?.jobs || {}
    }
  }
}

exports.getTopOpportunities = async (options = {}) => exports.getFeed(options)

exports.triggerRefresh = async (options = {}) => {
  const planContext = await resolvePlanContext(options)
  enforceManualRefreshCooldown(planContext?.userId, planContext?.entitlements, Date.now())
  const forceRefresh = options.forceRefresh == null ? true : normalizeBoolean(options.forceRefresh)
  const requestedJobType = normalizeText(options.jobType).toLowerCase()
  const trigger = normalizeText(options.trigger || "manual")
  const runOpportunity = !requestedJobType || requestedJobType === SCANNER_TYPES.OPPORTUNITY_SCAN
  const runEnrichment = !requestedJobType || requestedJobType === SCANNER_TYPES.ENRICHMENT

  const [opportunity, enrichment] = await Promise.all([
    runOpportunity ? enqueueScan({ forceRefresh, trigger }) : Promise.resolve(null),
    runEnrichment ? enqueueEnrichment({ forceRefresh, trigger }) : Promise.resolve(null)
  ])

  return {
    scanRunId: opportunity?.scanRunId || enrichment?.scanRunId || null,
    alreadyRunning: Boolean(
      (runOpportunity ? opportunity?.alreadyRunning : true) &&
        (runEnrichment ? enrichment?.alreadyRunning : true)
    ),
    startedAt: new Date().toISOString(),
    jobs: {
      [SCANNER_TYPES.OPPORTUNITY_SCAN]: runOpportunity ? opportunity : null,
      [SCANNER_TYPES.ENRICHMENT]: runEnrichment ? enrichment : null
    },
    plan: {
      planTier: planContext?.planTier || "free",
      scannerRefreshIntervalMinutes: Number(
        planService.getPlanConfig(planContext?.entitlements || planContext?.planTier)
          .scannerRefreshIntervalMinutes || OPPORTUNITY_SCAN_INTERVAL_MINUTES
      ),
      allowCrossJobParallelism: ALLOW_CROSS_JOB_PARALLELISM
    }
  }
}

exports.getStatus = async () => {
  const status = await getScannerStatusInternal()
  return {
    scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
    schedulerRunning: Boolean(status?.schedulerRunning),
    currentStatus: status?.currentStatus || "idle",
    currentRunId: status?.currentRunId || null,
    currentRunStartedAt: status?.currentRunStartedAt || null,
    currentRunElapsedMs: status?.currentRunElapsedMs ?? null,
    nextScheduledAt: status?.nextScheduledAt || null,
    activeOpportunities: Number(status?.activeOpportunities || 0),
    latestRun: status?.latestRun || null,
    latestCompletedRun: status?.latestCompletedRun || null,
    coordination: status?.coordination || {},
    jobs: status?.jobs || {}
  }
}

exports.startScheduler = () => {
  if (!enrichmentState.timer) {
    enqueueEnrichment({ forceRefresh: false, trigger: "startup_enrichment" }).catch((err) => {
      console.error("[arbitrage-scanner] Initial enrichment enqueue failed", err.message)
    })
    updateNextScheduledAt(SCANNER_TYPES.ENRICHMENT)
    enrichmentState.timer = setInterval(() => {
      enqueueEnrichment({ forceRefresh: false, trigger: "scheduled_enrichment" }).catch((err) => {
        console.error("[arbitrage-scanner] Scheduled enrichment enqueue failed", err.message)
      })
      updateNextScheduledAt(SCANNER_TYPES.ENRICHMENT)
    }, ENRICHMENT_INTERVAL_MS)
    enrichmentState.timer.unref?.()
  }

  if (!scannerState.timer) {
    enqueueScan({ forceRefresh: false, trigger: "startup_opportunity_scan" }).catch((err) => {
      console.error("[arbitrage-scanner] Initial opportunity scan enqueue failed", err.message)
    })
    updateNextScheduledAt(SCANNER_TYPES.OPPORTUNITY_SCAN)
    scannerState.timer = setInterval(() => {
      enqueueScan({ forceRefresh: false, trigger: "scheduled_opportunity_scan" }).catch((err) => {
        console.error("[arbitrage-scanner] Scheduled opportunity scan enqueue failed", err.message)
      })
      updateNextScheduledAt(SCANNER_TYPES.OPPORTUNITY_SCAN)
    }, OPPORTUNITY_SCAN_INTERVAL_MS)
    scannerState.timer.unref?.()
  }

  console.log(
    `[arbitrage-scanner] Scheduler started (enrichment=${ENRICHMENT_INTERVAL_MINUTES}m, opportunity_scan=${OPPORTUNITY_SCAN_INTERVAL_MINUTES}m)`
  )
}

exports.stopScheduler = () => {
  if (scannerState.timer) {
    clearInterval(scannerState.timer)
    scannerState.timer = null
    scannerState.nextScheduledAt = null
  }
  if (enrichmentState.timer) {
    clearInterval(enrichmentState.timer)
    enrichmentState.timer = null
    enrichmentState.nextScheduledAt = null
  }
}

exports.forceRefresh = async () =>
  enqueueScan({ forceRefresh: true, trigger: "manual" })

exports.__testables = {
  normalizeCategoryFilter,
  classifyCatalogState,
  buildRoundRobinPool,
  selectScanCandidates,
  evaluateCandidateOpportunity,
  buildOpportunityFingerprint,
  buildMaterialChangeHash,
  classifyOpportunityFeedEvent,
  isMateriallyNewOpportunity,
  buildFeedInsertRow,
  mapFeedRowToApiRow,
  persistFeedRows,
  buildFeedUpdatePatch,
  confidenceLevel,
  clampScore,
  isScannerRunOverdue,
  DEFAULT_UNIVERSE_LIMIT,
  OPPORTUNITY_BATCH_TARGET,
  SCAN_CHUNK_SIZE,
  SCAN_TIMEOUT_PER_BATCH_MS,
  SCAN_STATE
}


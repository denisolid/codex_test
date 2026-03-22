const AppError = require("../utils/AppError")
const skinRepo = require("../repositories/skinRepository")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const marketComparisonService = require("./marketComparisonService")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const marketImageService = require("./marketImageService")
const planService = require("./planService")
const premiumCategoryAccessService = require("./premiumCategoryAccessService")
const {
  resolveCanonicalRarity,
  canonicalRarityToDisplay,
  getCanonicalRarityColor,
  buildUnknownRarityDiagnostics
} = require("../utils/rarityResolver")
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
const FEED_PAGE_SIZE = 200
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
const FEED_FIRST_PAGE_CACHE_TTL_MS = 20 * 1000
const FEED_FIRST_PAGE_CACHE_MAX = 24
const FEED_CURSOR_DELIMITER = "|"
const FEED_CACHEABLE_CATEGORY_FILTERS = Object.freeze(
  new Set([
    "all",
    ITEM_CATEGORIES.WEAPON_SKIN,
    ITEM_CATEGORIES.CASE,
    ITEM_CATEGORIES.STICKER_CAPSULE,
    ITEM_CATEGORIES.KNIFE
  ])
)

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
const feedFirstPageCache = new Map()
const UNKNOWN_RARITY_TEXT_SET = new Set([
  "unknown",
  "default",
  "none",
  "n/a",
  "na",
  "null",
  "-",
  "?"
])
const TRADEMARK_SYMBOL_REGEX = /[\u2122\u00ae]/g
const TRADEMARK_MOJIBAKE_REGEX = /\u00e2\u201e\u00a2|\u00c2\u00ae/g
const TRADEMARK_TEXT_REGEX = /\bTM\b/gi
const PLACEHOLDER_IMAGE_MARKERS = Object.freeze([
  "/skin-placeholder.svg",
  "/case-placeholder.svg",
  "/public/images/apps/730/header.jpg",
  "apps/730/header.jpg"
])

function normalizeText(value) {
  return String(value || "").trim()
}

function stripTrademarkArtifacts(value) {
  return normalizeText(value)
    .replace(TRADEMARK_SYMBOL_REGEX, "")
    .replace(TRADEMARK_MOJIBAKE_REGEX, "")
    .replace(TRADEMARK_TEXT_REGEX, "")
    .replace(/\s+/g, " ")
    .trim()
}

function canonicalSkinLookupName(value) {
  return stripTrademarkArtifacts(normalizeText(value).normalize("NFKC"))
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
}

function buildSkinLookupNameVariants(value) {
  const raw = normalizeText(value).replace(/\s+/g, " ").trim()
  if (!raw) return []
  const nfc = raw.normalize("NFC").replace(/\s+/g, " ").trim()
  const nfkc = raw.normalize("NFKC").replace(/\s+/g, " ").trim()
  const withoutMarksRaw = stripTrademarkArtifacts(raw)
  const withoutMarksNfkc = stripTrademarkArtifacts(nfkc)
  return Array.from(
    new Set(
      [raw, nfc, nfkc, withoutMarksRaw, withoutMarksNfkc]
        .map((entry) => normalizeText(entry))
        .filter(Boolean)
    )
  )
}

function pickPreferredRarityValue(...values) {
  for (const candidate of values) {
    const text = normalizeText(candidate)
    if (!text) continue
    if (UNKNOWN_RARITY_TEXT_SET.has(text.toLowerCase())) continue
    return text
  }
  return null
}

function isHttpImageUrl(value) {
  return /^https?:\/\//i.test(normalizeText(value))
}

function isPlaceholderImageUrl(value) {
  const text = normalizeText(value).toLowerCase()
  if (!text) return true
  return PLACEHOLDER_IMAGE_MARKERS.some((marker) => text.includes(marker))
}

function pickPreferredImageUrl(...values) {
  for (const candidate of values) {
    const text = normalizeText(candidate)
    if (!text) continue
    if (!isHttpImageUrl(text)) continue
    if (isPlaceholderImageUrl(text)) continue
    return text
  }
  return null
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

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function normalizeCursorPayload(value) {
  const raw = normalizeText(value)
  if (!raw) return null
  try {
    const decoded = Buffer.from(raw, "base64url").toString("utf8")
    const [createdAtRaw, idRaw] = decoded.split(FEED_CURSOR_DELIMITER)
    const createdAt = toIsoOrNull(createdAtRaw)
    const id = normalizeText(idRaw)
    if (!createdAt || !id) return null
    return {
      createdAt,
      id
    }
  } catch (_err) {
    return null
  }
}

function encodeCursorPayload(createdAt, id) {
  const safeCreatedAt = toIsoOrNull(createdAt)
  const safeId = normalizeText(id)
  if (!safeCreatedAt || !safeId) return null
  const payload = `${safeCreatedAt}${FEED_CURSOR_DELIMITER}${safeId}`
  return Buffer.from(payload, "utf8").toString("base64url")
}

function buildFeedPageCacheKey(options = {}) {
  const category = normalizeCategoryFilter(options.category)
  return [
    options.includeInactive ? "1" : "0",
    options.highConfidenceOnly ? "1" : "0",
    category || "all",
    FEED_WINDOW_HOURS
  ].join("::")
}

function shouldCacheFirstPage(options = {}) {
  if (options.cursorCreatedAt || options.cursorId) return false
  if (Boolean(options.includeInactive)) return false
  const category = normalizeCategoryFilter(options.category)
  return FEED_CACHEABLE_CATEGORY_FILTERS.has(category)
}

function pruneFeedFirstPageCache(nowMs = Date.now()) {
  for (const [key, entry] of feedFirstPageCache.entries()) {
    if (Number(entry?.expiresAt || 0) <= nowMs) {
      feedFirstPageCache.delete(key)
    }
  }

  if (feedFirstPageCache.size <= FEED_FIRST_PAGE_CACHE_MAX) return
  const overflow = feedFirstPageCache.size - FEED_FIRST_PAGE_CACHE_MAX
  const oldestKeys = Array.from(feedFirstPageCache.entries())
    .sort((a, b) => Number(a?.[1]?.createdAtMs || 0) - Number(b?.[1]?.createdAtMs || 0))
    .slice(0, overflow)
    .map(([key]) => key)
  for (const key of oldestKeys) {
    feedFirstPageCache.delete(key)
  }
}

function getFeedFirstPageCache(options = {}) {
  if (!shouldCacheFirstPage(options)) return null
  const key = buildFeedPageCacheKey(options)
  const cached = feedFirstPageCache.get(key)
  if (!cached) return null
  if (Date.now() >= Number(cached.expiresAt || 0)) {
    feedFirstPageCache.delete(key)
    return null
  }
  return {
    rows: Array.isArray(cached.rows) ? cached.rows.map((row) => ({ ...row })) : [],
    hasNextPage: Boolean(cached.hasNextPage),
    nextCursor: normalizeText(cached.nextCursor) || null
  }
}

function setFeedFirstPageCache(options = {}, payload = {}) {
  if (!shouldCacheFirstPage(options)) return
  const key = buildFeedPageCacheKey(options)
  const rows = Array.isArray(payload?.rows) ? payload.rows.map((row) => ({ ...row })) : []
  feedFirstPageCache.set(key, {
    createdAtMs: Date.now(),
    expiresAt: Date.now() + FEED_FIRST_PAGE_CACHE_TTL_MS,
    rows,
    hasNextPage: Boolean(payload?.hasNextPage),
    nextCursor: normalizeText(payload?.nextCursor) || null
  })
  pruneFeedFirstPageCache(Date.now())
}

function clearFeedFirstPageCache() {
  feedFirstPageCache.clear()
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

function countRowsByScannerCategory(rows = []) {
  const counts = emptyCategoryMap()
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = mapScannerCategory(
      row?.category || row?.itemCategory || row?.raw?.category || ""
    )
    if (counts[category] == null) continue
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

function listMissingScannerCategories(counts = {}) {
  return ROUND_ROBIN_CATEGORY_ORDER.filter((category) => Number(counts?.[category] || 0) <= 0)
}

function mergeUniqueScannerSourceRows(baseRows = [], extraRows = []) {
  const merged = []
  const seen = new Set()
  const appendRows = (rows = []) => {
    for (const row of Array.isArray(rows) ? rows : []) {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName || seen.has(marketHashName)) continue
      seen.add(marketHashName)
      merged.push(row)
    }
  }
  appendRows(baseRows)
  appendRows(extraRows)
  return merged
}

function incrementCounter(target, key) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + 1
}

function incrementCounterBy(target, key, amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

function mergeCounterMap(target = {}, patch = {}) {
  for (const [key, value] of Object.entries(patch || {})) {
    incrementCounterBy(target, key, Number(value || 0))
  }
}

function createSkinportPipelineSummary() {
  return {
    enabled: false,
    chunksWithDiagnostics: 0,
    requestedItems: 0,
    mappedItems: 0,
    strictConfirmed: 0,
    fallbackConfirmed: 0,
    stageCounters: {},
    rejectReasons: {},
    sourceUnavailableReasonCounts: {},
    failureReasonCounts: {}
  }
}

function mergeSkinportPipelineSummary(target = {}, sourceDiagnostics = null) {
  if (!sourceDiagnostics || typeof sourceDiagnostics !== "object") return
  target.chunksWithDiagnostics = Number(target.chunksWithDiagnostics || 0) + 1
  const sourceUnavailableReason = normalizeText(sourceDiagnostics?.sourceUnavailableReason)
  if (sourceUnavailableReason) {
    incrementCounter(target.sourceUnavailableReasonCounts, sourceUnavailableReason)
  }
  for (const reason of Object.values(sourceDiagnostics?.failuresByName || {})) {
    incrementCounter(target.failureReasonCounts, reason)
  }
  const pipeline =
    sourceDiagnostics?.pipeline && typeof sourceDiagnostics.pipeline === "object"
      ? sourceDiagnostics.pipeline
      : null
  if (!pipeline) return

  target.requestedItems = Number(target.requestedItems || 0) + Number(pipeline.requestedItems || 0)
  target.mappedItems = Number(target.mappedItems || 0) + Number(pipeline.mappedItems || 0)
  target.strictConfirmed =
    Number(target.strictConfirmed || 0) + Number(pipeline.strictConfirmed || 0)
  target.fallbackConfirmed =
    Number(target.fallbackConfirmed || 0) + Number(pipeline.fallbackConfirmed || 0)
  for (const [stage, counters] of Object.entries(pipeline.stageCounters || {})) {
    if (!target.stageCounters[stage]) target.stageCounters[stage] = {}
    mergeCounterMap(target.stageCounters[stage], counters || {})
  }
  mergeCounterMap(target.rejectReasons, pipeline.rejectReasons || {})
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
  const names = Array.from(new Set(
    safeRows
      .flatMap((row) =>
        buildSkinLookupNameVariants(
          row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name
        )
      )
      .filter(Boolean)
  ))
  if (!names.length) return safeRows
  let skinRows = []
  try {
    skinRows = await skinRepo.getByMarketHashNames(names)
  } catch (_err) {
    skinRows = []
  }
  const byName = new Map((skinRows || []).map((row) => [normalizeText(row.market_hash_name), row]))
  const byCanonicalName = new Map(
    (skinRows || []).map((row) => [canonicalSkinLookupName(row.market_hash_name), row])
  )
  return safeRows.map((row) => {
    const rowName = normalizeText(
      row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name
    )
    const nameVariants = buildSkinLookupNameVariants(rowName)
    const skin =
      nameVariants.map((variant) => byName.get(variant)).find(Boolean) ||
      nameVariants.map((variant) => byCanonicalName.get(canonicalSkinLookupName(variant))).find(Boolean) ||
      null
    if (!skin) return row
    const itemId = row.itemId || row.item_id || skin.id || null
    const itemImageUrl = pickPreferredImageUrl(
      row.itemImageUrl,
      row.item_image_url,
      skin.image_url_large,
      skin.image_url
    )
    const rarityResolution = resolveCanonicalRarity({
      catalogRarity: pickPreferredRarityValue(
        row.itemCanonicalRarity,
        row.item_canonical_rarity,
        skin.canonical_rarity,
        skin.rarity
      ),
      sourceRarity: pickPreferredRarityValue(row.itemRarity, row.item_rarity, skin.rarity),
      category: row.itemCategory || row.category || null,
      marketHashName: rowName,
      weapon: null
    })
    const itemCanonicalRarity = rarityResolution.canonicalRarity
    const itemRarity = canonicalRarityToDisplay(itemCanonicalRarity)
    const itemRarityColor = getCanonicalRarityColor(itemCanonicalRarity)
    const unknownRarityDiagnostics = buildUnknownRarityDiagnostics(rarityResolution, {
      category: row.itemCategory || row.category || null,
      marketHashName: rowName,
      weapon: null,
      catalogRarity: pickPreferredRarityValue(
        row.itemCanonicalRarity,
        row.item_canonical_rarity,
        skin.canonical_rarity,
        skin.rarity
      ),
      sourceRarity: pickPreferredRarityValue(row.itemRarity, row.item_rarity, skin.rarity)
    })
    return {
      ...row,
      itemId,
      item_id: itemId,
      itemImageUrl,
      item_image_url: itemImageUrl,
      itemCanonicalRarity,
      item_canonical_rarity: itemCanonicalRarity,
      itemRarity,
      item_rarity: itemRarity,
      itemRarityColor,
      item_rarity_color: itemRarityColor,
      itemRarityDiagnostics: unknownRarityDiagnostics,
      item_rarity_diagnostics: unknownRarityDiagnostics,
      itemRarityUnknownReason: unknownRarityDiagnostics?.reason || null,
      item_rarity_unknown_reason: unknownRarityDiagnostics?.reason || null
    }
  })
}

function mapFeedRowToCard(rawRow = {}) {
  const row = mapFeedRowToApiRow(rawRow)
  const qualityScoreDisplay =
    toFiniteOrNull(row?.qualityScoreDisplay ?? row?.quality_score_display) ??
    toFiniteOrNull(row?.score)
  const detectedAt = row?.detectedAt || rawRow?.detected_at || rawRow?.created_at || null
  const discoveredAt = row?.discoveredAt || row?.discovered_at || rawRow?.discovered_at || detectedAt || null
  const firstSeenAt = row?.firstSeenAt || row?.first_seen_at || discoveredAt || detectedAt || null
  const lastSeenAt = row?.lastSeenAt || row?.last_seen_at || detectedAt || null
  const lastPublishedAt =
    row?.lastPublishedAt ||
    row?.last_published_at ||
    row?.feedPublishedAt ||
    row?.feed_published_at ||
    detectedAt ||
    null
  const timesSeen = toSafeInteger(row?.timesSeen ?? row?.times_seen, 1, 1)

  return {
    feedId: row?.feedId || normalizeText(rawRow?.id) || null,
    detectedAt,
    marketHashName: row?.marketHashName || row?.itemName || rawRow?.market_hash_name || null,
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
    scanRunId: row?.scanRunId || null,
    isActive: row?.isActive == null ? true : Boolean(row.isActive),
    isDuplicate: Boolean(row?.isDuplicate),
    itemId: row?.itemId || null,
    itemName: row?.itemName || row?.marketHashName || "Tracked Item",
    itemCategory: row?.itemCategory || "weapon_skin",
    itemSubcategory: row?.itemSubcategory || null,
    itemCanonicalRarity: row?.itemCanonicalRarity || null,
    item_canonical_rarity: row?.itemCanonicalRarity || null,
    itemRarity: row?.itemRarity || null,
    item_rarity: row?.itemRarity || null,
    itemRarityColor: row?.itemRarityColor || null,
    item_rarity_color: row?.itemRarityColor || null,
    itemRarityDiagnostics: row?.itemRarityDiagnostics || null,
    item_rarity_diagnostics: row?.itemRarityDiagnostics || null,
    itemRarityUnknownReason: row?.itemRarityUnknownReason || null,
    item_rarity_unknown_reason: row?.itemRarityUnknownReason || null,
    itemImageUrl: row?.itemImageUrl || null,
    item_image_url: row?.itemImageUrl || null,
    buyMarket: row?.buyMarket || null,
    buyPrice: toFiniteOrNull(row?.buyPrice),
    sellMarket: row?.sellMarket || null,
    sellNet: toFiniteOrNull(row?.sellNet),
    profit: toFiniteOrNull(row?.profit),
    spread: toFiniteOrNull(row?.spread),
    score: toFiniteOrNull(row?.score),
    qualityScoreDisplay,
    quality_score_display: qualityScoreDisplay,
    scoreCategory: row?.scoreCategory || null,
    executionConfidence: row?.executionConfidence || null,
    qualityGrade: row?.qualityGrade || null,
    liquidity: toFiniteOrNull(row?.liquidity),
    liquidityBand: row?.liquidityBand || row?.liquidityLabel || null,
    liquidityLabel: row?.liquidityLabel || row?.liquidityBand || null,
    volume7d: toFiniteOrNull(row?.volume7d ?? row?.liquidity),
    marketCoverage: toFiniteOrNull(row?.marketCoverage),
    referencePrice: toFiniteOrNull(row?.referencePrice),
    latestSignalAgeHours:
      toFiniteOrNull(row?.latestSignalAgeHours ?? row?.latest_signal_age_hours) ?? null,
    latest_signal_age_hours:
      toFiniteOrNull(row?.latestSignalAgeHours ?? row?.latest_signal_age_hours) ?? null,
    refreshStatus: row?.refreshStatus || row?.refresh_status || "pending",
    refresh_status: row?.refreshStatus || row?.refresh_status || "pending",
    liveStatus: row?.liveStatus || row?.live_status || "degraded",
    live_status: row?.liveStatus || row?.live_status || "degraded",
    verdict: row?.verdict || null,
    buyUrl: row?.buyUrl || null,
    sellUrl: row?.sellUrl || null,
    flags: Array.isArray(row?.flags) ? row.flags : [],
    badges: Array.isArray(row?.badges) ? row.badges : []
  }
}

function toStatusRunSnapshot(run = null) {
  if (!run || typeof run !== "object") return null
  const itemsScanned = toFiniteOrNull(run?.items_scanned)
  const opportunitiesFound = toFiniteOrNull(run?.opportunities_found)
  const newOpportunitiesAdded = toFiniteOrNull(run?.new_opportunities_added)
  return {
    id: run?.id || null,
    status: run?.status || null,
    started_at: run?.started_at || null,
    completed_at: run?.completed_at || null,
    items_scanned: itemsScanned == null ? null : Math.max(Math.round(itemsScanned), 0),
    opportunities_found:
      opportunitiesFound == null ? null : Math.max(Math.round(opportunitiesFound), 0),
    new_opportunities_added:
      newOpportunitiesAdded == null ? null : Math.max(Math.round(newOpportunitiesAdded), 0)
  }
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
  const minSourcePoolTarget = Math.max(OPPORTUNITY_BATCH_RUNTIME_TARGET * 3, 300)
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
    sourceMode: "catalog_status_scannable",
    topup: {
      candidatePoolAttempted: false,
      candidatePoolFailed: false,
      candidatePoolRowsAdded: 0,
      activeTradableAttempted: false,
      activeTradableFailed: false,
      activeTradableRowsAdded: 0
    },
    categoryCountsBeforeTopup: emptyCategoryMap(),
    categoryCountsAfterTopup: emptyCategoryMap(),
    missingCategoriesBeforeTopup: [],
    missingCategoriesAfterTopup: []
  }
  let lastError = null
  for (let index = 0; index < attempts.length; index += 1) {
    const limit = attempts[index]
    diagnostics.attemptedLimits.push(limit)
    try {
      let rows = await marketSourceCatalogRepo.listScannerSource({
        limit,
        categories: CATALOG_SCAN_CATEGORIES
      })
      rows = Array.isArray(rows) ? rows : []

      let categoryCounts = countRowsByScannerCategory(rows)
      let missingCategories = listMissingScannerCategories(categoryCounts)
      diagnostics.categoryCountsBeforeTopup = categoryCounts
      diagnostics.missingCategoriesBeforeTopup = missingCategories

      if (rows.length < minSourcePoolTarget || missingCategories.length > 0) {
        diagnostics.topup.candidatePoolAttempted = true
        const candidateTopupCategories = missingCategories.length
          ? missingCategories
          : CATALOG_SCAN_CATEGORIES
        try {
          const candidateRows = await marketSourceCatalogRepo.listCandidatePool({
            limit: Math.max(limit, minSourcePoolTarget * 2),
            categories: candidateTopupCategories,
            candidateStatuses: ["near_eligible", "enriching", "candidate"]
          })
          const mergedRows = mergeUniqueScannerSourceRows(rows, candidateRows)
          diagnostics.topup.candidatePoolRowsAdded = Math.max(mergedRows.length - rows.length, 0)
          rows = mergedRows
        } catch (_err) {
          diagnostics.topup.candidatePoolFailed = true
        }
      }

      categoryCounts = countRowsByScannerCategory(rows)
      missingCategories = listMissingScannerCategories(categoryCounts)
      if (missingCategories.length > 0) {
        diagnostics.topup.activeTradableAttempted = true
        try {
          const activeRows = await marketSourceCatalogRepo.listActiveTradable({
            limit: Math.max(limit, minSourcePoolTarget * 2),
            categories: missingCategories
          })
          const mergedRows = mergeUniqueScannerSourceRows(rows, activeRows)
          diagnostics.topup.activeTradableRowsAdded = Math.max(mergedRows.length - rows.length, 0)
          rows = mergedRows
        } catch (_err) {
          diagnostics.topup.activeTradableFailed = true
        }
      }

      categoryCounts = countRowsByScannerCategory(rows)
      diagnostics.selectedLimit = limit
      diagnostics.categoryCountsAfterTopup = categoryCounts
      diagnostics.missingCategoriesAfterTopup = listMissingScannerCategories(categoryCounts)
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
  const skinportPipeline = createSkinportPipelineSummary()
  skinportPipeline.enabled = allowLiveFetch
  const diagnostics = {
    batchesAttempted: 0,
    batchesCompleted: 0,
    batchesTimedOut: 0,
    batchesFailed: 0,
    chunkSize: SCAN_CHUNK_SIZE,
    allowLiveFetch,
    forceRefresh: allowLiveFetch,
    skinportPipeline
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
      const skinportSourceDiagnostics = compared?.diagnostics?.liveFetch?.bySource?.skinport
      if (skinportSourceDiagnostics && typeof skinportSourceDiagnostics === "object") {
        mergeSkinportPipelineSummary(skinportPipeline, skinportSourceDiagnostics)
      }
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

function resolveFeedRowFingerprint(row = {}) {
  const metadata = toJsonObject(row?.metadata)
  return (
    normalizeText(
      row?.opportunity_fingerprint ??
        row?.opportunityFingerprint ??
        metadata?.opportunity_fingerprint ??
        metadata?.opportunityFingerprint
    ).toLowerCase() || ""
  )
}

function resolveFeedRowMaterialHash(row = {}) {
  const metadata = toJsonObject(row?.metadata)
  return (
    normalizeText(
      row?.material_change_hash ??
        row?.materialChangeHash ??
        metadata?.material_change_hash ??
        metadata?.materialChangeHash
    ).toLowerCase() || ""
  )
}

function buildFeedDedupIdentity(row = {}) {
  const fingerprint = resolveFeedRowFingerprint(row)
  if (fingerprint) {
    return `fp:${fingerprint}`
  }
  const signature = normalizeText(buildFeedKey(row)).toLowerCase()
  if (!signature) return ""
  const materialHash = resolveFeedRowMaterialHash(row) || "na"
  return `sig:${signature}::material:${materialHash}`
}

function resolveFeedRowRecencyMs(row = {}) {
  const candidates = [
    row?.created_at,
    row?.createdAt,
    row?.last_seen_at,
    row?.lastSeenAt,
    row?.last_published_at,
    row?.lastPublishedAt,
    row?.detected_at,
    row?.detectedAt,
    row?.discovered_at,
    row?.discoveredAt
  ]
  for (const value of candidates) {
    const iso = toIsoOrNull(value)
    if (!iso) continue
    const ts = new Date(iso).getTime()
    if (Number.isFinite(ts)) return ts
  }
  return 0
}

function collectOlderActiveDuplicateIds(rows = []) {
  const uniqueRows = []
  const seenIds = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!Boolean(row?.is_active)) continue
    const id = normalizeText(row?.id)
    if (!id) continue
    if (seenIds.has(id)) continue
    seenIds.add(id)
    uniqueRows.push(row)
  }

  uniqueRows.sort((left, right) => {
    const delta = resolveFeedRowRecencyMs(right) - resolveFeedRowRecencyMs(left)
    if (delta !== 0) return delta
    return normalizeText(right?.id).localeCompare(normalizeText(left?.id))
  })

  const seen = new Set()
  const duplicateIds = []
  for (const row of uniqueRows) {
    const id = normalizeText(row?.id)
    const dedupIdentity = buildFeedDedupIdentity(row)
    if (!dedupIdentity) continue
    if (seen.has(dedupIdentity)) {
      duplicateIds.push(id)
      continue
    }
    seen.add(dedupIdentity)
  }
  return Array.from(new Set(duplicateIds))
}

function pickLatestIso(first, second) {
  const firstIso = toIsoOrNull(first)
  const secondIso = toIsoOrNull(second)
  if (!firstIso) return secondIso
  if (!secondIso) return firstIso
  return new Date(firstIso).getTime() >= new Date(secondIso).getTime() ? firstIso : secondIso
}

function pickEarliestIso(first, second) {
  const firstIso = toIsoOrNull(first)
  const secondIso = toIsoOrNull(second)
  if (!firstIso) return secondIso
  if (!secondIso) return firstIso
  return new Date(firstIso).getTime() <= new Date(secondIso).getTime() ? firstIso : secondIso
}

function mergeDuplicateFeedCards(primary = {}, candidate = {}) {
  const merged = { ...primary }
  const primaryTimesSeen = toSafeInteger(primary?.timesSeen ?? primary?.times_seen, 1, 1)
  const candidateTimesSeen = toSafeInteger(candidate?.timesSeen ?? candidate?.times_seen, 1, 1)
  const timesSeen = Math.max(primaryTimesSeen, candidateTimesSeen)
  merged.timesSeen = timesSeen
  merged.times_seen = timesSeen

  const mergedFirstSeenAt = pickEarliestIso(
    primary?.firstSeenAt || primary?.first_seen_at || primary?.discoveredAt || primary?.discovered_at || primary?.detectedAt || primary?.detected_at,
    candidate?.firstSeenAt || candidate?.first_seen_at || candidate?.discoveredAt || candidate?.discovered_at || candidate?.detectedAt || candidate?.detected_at
  )
  if (mergedFirstSeenAt) {
    merged.firstSeenAt = mergedFirstSeenAt
    merged.first_seen_at = mergedFirstSeenAt
    merged.discoveredAt = mergedFirstSeenAt
    merged.discovered_at = mergedFirstSeenAt
  }

  const mergedLastSeenAt = pickLatestIso(
    primary?.lastSeenAt || primary?.last_seen_at || primary?.detectedAt || primary?.detected_at,
    candidate?.lastSeenAt || candidate?.last_seen_at || candidate?.detectedAt || candidate?.detected_at
  )
  if (mergedLastSeenAt) {
    merged.lastSeenAt = mergedLastSeenAt
    merged.last_seen_at = mergedLastSeenAt
  }

  const mergedLastPublishedAt = pickLatestIso(
    primary?.lastPublishedAt || primary?.last_published_at || primary?.detectedAt || primary?.detected_at,
    candidate?.lastPublishedAt || candidate?.last_published_at || candidate?.detectedAt || candidate?.detected_at
  )
  if (mergedLastPublishedAt) {
    merged.lastPublishedAt = mergedLastPublishedAt
    merged.last_published_at = mergedLastPublishedAt
  }

  return merged
}

function dedupeFeedCards(rows = []) {
  const deduped = []
  const byIdentity = new Map()
  for (const row of Array.isArray(rows) ? rows : []) {
    const dedupIdentity =
      buildFeedDedupIdentity(row) ||
      `row:${normalizeText(row?.feedId || row?.id || deduped.length)}`
    const existingIndex = byIdentity.get(dedupIdentity)
    if (existingIndex == null) {
      byIdentity.set(dedupIdentity, deduped.length)
      deduped.push({ ...row })
      continue
    }
    deduped[existingIndex] = mergeDuplicateFeedCards(deduped[existingIndex], row)
  }
  return deduped
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

function preferNonEmptyMetadataValue(nextValue, previousValue) {
  const next = normalizeText(nextValue)
  if (next) return next
  const previous = normalizeText(previousValue)
  return previous || null
}

function normalizeRarityCandidate(value) {
  const raw = normalizeText(value)
  if (!raw) return ""
  if (UNKNOWN_RARITY_TEXT_SET.has(raw.toLowerCase())) return ""
  return raw
}

function preferNonEmptyRarityValue(nextValue, previousValue) {
  const next = normalizeRarityCandidate(nextValue)
  if (next) return next
  const previous = normalizeRarityCandidate(previousValue)
  return previous || null
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
  const resolvedItemId = preferNonEmptyMetadataValue(
    insertMetadata?.item_id ?? insertMetadata?.itemId,
    previousMetadata?.item_id ?? previousMetadata?.itemId
  )
  const resolvedItemSubcategory = preferNonEmptyMetadataValue(
    insertMetadata?.item_subcategory ?? insertMetadata?.itemSubcategory,
    previousMetadata?.item_subcategory ?? previousMetadata?.itemSubcategory
  )
  const resolvedItemCanonicalRarityRaw = preferNonEmptyRarityValue(
    insertMetadata?.item_canonical_rarity ?? insertMetadata?.itemCanonicalRarity,
    previousMetadata?.item_canonical_rarity ?? previousMetadata?.itemCanonicalRarity
  )
  const resolvedItemRarityRaw = preferNonEmptyRarityValue(
    insertMetadata?.item_rarity ?? insertMetadata?.itemRarity ?? insertMetadata?.rarity,
    previousMetadata?.item_rarity ?? previousMetadata?.itemRarity ?? previousMetadata?.rarity
  )
  const rarityResolution = resolveCanonicalRarity({
    catalogRarity: resolvedItemCanonicalRarityRaw,
    sourceRarity: resolvedItemRarityRaw,
    category: insertRow?.category || previousRow?.category || null,
    marketHashName:
      insertRow?.market_hash_name ||
      previousRow?.market_hash_name ||
      insertRow?.item_name ||
      previousRow?.item_name ||
      null,
    weapon: insertMetadata?.weapon || previousMetadata?.weapon || null
  })
  const resolvedItemCanonicalRarity = rarityResolution.canonicalRarity
  const resolvedItemRarity = canonicalRarityToDisplay(resolvedItemCanonicalRarity)
  const resolvedItemRarityColor = getCanonicalRarityColor(resolvedItemCanonicalRarity)
  const unknownRarityDiagnostics = buildUnknownRarityDiagnostics(rarityResolution, {
    category: insertRow?.category || previousRow?.category || null,
    marketHashName:
      insertRow?.market_hash_name ||
      previousRow?.market_hash_name ||
      insertRow?.item_name ||
      previousRow?.item_name ||
      null,
    weapon: insertMetadata?.weapon || previousMetadata?.weapon || null,
    catalogRarity: resolvedItemCanonicalRarityRaw,
    sourceRarity: resolvedItemRarityRaw
  })
  const resolvedItemImageUrl = preferNonEmptyMetadataValue(
    insertMetadata?.item_image_url ??
      insertMetadata?.itemImageUrl ??
      insertMetadata?.image_url ??
      insertMetadata?.imageUrl,
    previousMetadata?.item_image_url ??
      previousMetadata?.itemImageUrl ??
      previousMetadata?.image_url ??
      previousMetadata?.imageUrl
  )

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
    item_id: resolvedItemId,
    itemId: resolvedItemId,
    item_subcategory: resolvedItemSubcategory,
    itemSubcategory: resolvedItemSubcategory,
    item_canonical_rarity: resolvedItemCanonicalRarity,
    itemCanonicalRarity: resolvedItemCanonicalRarity,
    item_rarity: resolvedItemRarity,
    itemRarity: resolvedItemRarity,
    item_rarity_color: resolvedItemRarityColor,
    itemRarityColor: resolvedItemRarityColor,
    item_rarity_resolution_source: rarityResolution.source || null,
    itemRarityResolutionSource: rarityResolution.source || null,
    item_rarity_unknown_reason: unknownRarityDiagnostics?.reason || null,
    itemRarityUnknownReason: unknownRarityDiagnostics?.reason || null,
    item_rarity_diagnostics: unknownRarityDiagnostics || null,
    itemRarityDiagnostics: unknownRarityDiagnostics || null,
    item_image_url: resolvedItemImageUrl,
    itemImageUrl: resolvedItemImageUrl,
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
      duplicateActivesMarkedInactive: 0,
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
      includeInactive: false,
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

  const duplicateActiveIds = collectOlderActiveDuplicateIds([
    ...(previousRows || []),
    ...(activeFingerprintRows || [])
  ])
  if (duplicateActiveIds.length) {
    try {
      counters.cleanup.duplicateActivesMarkedInactive = await arbitrageFeedRepo.markRowsInactiveByIds(
        duplicateActiveIds
      )
    } catch (err) {
      if (isStatementTimeoutError(err)) {
        counters.cleanup.hadTimeout = true
        counters.cleanup.errors.push("active_duplicate_cleanup_timeout")
      } else {
        counters.cleanup.errors.push(normalizeText(err?.message) || "active_duplicate_cleanup_failed")
      }
    }
    const deactivatedIds = new Set(duplicateActiveIds.map((value) => normalizeText(value)))
    previousRows = (previousRows || []).filter((row) => !deactivatedIds.has(normalizeText(row?.id)))
    activeFingerprintRows = (activeFingerprintRows || []).filter(
      (row) => !deactivatedIds.has(normalizeText(row?.id))
    )
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
      const upserts = Object.entries(byName || {}).map(([name, metadata]) => {
        const rarityResolution = resolveCanonicalRarity({
          catalogRarity: metadata?.canonicalRarity || metadata?.rarity || null,
          sourceRarity: metadata?.rarity || null,
          marketHashName: name
        })
        const canonicalRarity = rarityResolution.canonicalRarity
        return {
          market_hash_name: name,
          image_url: metadata?.imageUrl || null,
          rarity: canonicalRarityToDisplay(canonicalRarity),
          canonical_rarity: canonicalRarity,
          rarity_color: getCanonicalRarityColor(canonicalRarity)
        }
      })
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

async function getScannerStatusInternal(options = {}) {
  const includeActiveCount = options.includeActiveCount !== false
  const [latestRun, latestCompletedRun, latestEnrichmentRun, latestEnrichmentCompletedRun, activeOpportunities] =
    await Promise.all([
      scannerRunRepo.getLatestRun(SCANNER_TYPES.OPPORTUNITY_SCAN),
      scannerRunRepo.getLatestCompletedRun(SCANNER_TYPES.OPPORTUNITY_SCAN),
      scannerRunRepo.getLatestRun(SCANNER_TYPES.ENRICHMENT),
      scannerRunRepo.getLatestCompletedRun(SCANNER_TYPES.ENRICHMENT),
      includeActiveCount
        ? arbitrageFeedRepo.countFeed({ includeInactive: false }).catch(() => 0)
        : Promise.resolve(null)
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
    activeOpportunities:
      activeOpportunities == null ? null : Number(activeOpportunities || 0),
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
  const requestedHistoryHours = normalizeHistoryHours(options.historyHours, FEED_WINDOW_HOURS)
  const historyWindowHours = FEED_WINDOW_HOURS
  const historySinceIso = buildSinceIso(historyWindowHours)
  const requestedCursor = normalizeCursorPayload(options.cursor)
  const includeCount = normalizeBoolean(options.includeCount, false)

  const requestedShowRisky =
    options.showRisky == null ? true : normalizeBoolean(options.showRisky)
  const requestedIncludeOlder = normalizeBoolean(options.includeOlder || options.showOlder)
  const requestedCategory = normalizeCategoryFilter(options.category)
  const showRisky = advancedFiltersEnabled ? requestedShowRisky : true
  const includeOlder = advancedFiltersEnabled ? requestedIncludeOlder : false
  const categoryFilter = advancedFiltersEnabled ? requestedCategory : "all"
  const canonicalCategory = categoryFilter === "all" ? "" : categoryFilter

  const statusPromise = getScannerStatusInternal({ includeActiveCount: false })
  const countPromise = includeCount
    ? countFeedSafely({
        includeInactive: includeOlder,
        category: canonicalCategory,
        minScore: 0,
        excludeLowConfidence: false,
        highConfidenceOnly: !showRisky,
        sinceIso: historySinceIso
      })
    : Promise.resolve({
        count: null,
        timedOut: false,
        skipped: true
      })

  const feedQuery = {
    limit: limit + 1,
    cursorCreatedAt: requestedCursor?.createdAt || "",
    cursorId: requestedCursor?.id || "",
    includeInactive: includeOlder,
    category: canonicalCategory,
    minScore: 0,
    excludeLowConfidence: false,
    highConfidenceOnly: !showRisky,
    sinceIso: historySinceIso
  }

  let pagePayload = getFeedFirstPageCache(feedQuery)
  if (!pagePayload) {
    const rawRows = await arbitrageFeedRepo.listFeedByCursor(feedQuery)
    const hasExtraRow = rawRows.length > limit
    const rows = hasExtraRow ? rawRows.slice(0, limit) : rawRows
    const lastRow = rows.length ? rows[rows.length - 1] : null
    pagePayload = {
      rows,
      hasNextPage: hasExtraRow,
      nextCursor: hasExtraRow
        ? encodeCursorPayload(lastRow?.created_at || lastRow?.detected_at, lastRow?.id)
        : null
    }
    setFeedFirstPageCache(feedQuery, pagePayload)
  }

  const [status, countResult] = await Promise.all([statusPromise, countPromise])

  let mappedRows = (Array.isArray(pagePayload?.rows) ? pagePayload.rows : []).map((row) =>
    mapFeedRowToCard(row)
  )
  mappedRows = await enrichRowsWithSkinMetadata(mappedRows)
  mappedRows = dedupeFeedCards(mappedRows)
  const restricted = applyFeedPlanRestrictions(mappedRows, entitlements)
  mappedRows = restricted.rows

  const latestCompleted = status?.latestCompletedRun || null
  const totalCount =
    includeCount && Number.isFinite(Number(countResult?.count))
      ? Math.max(Number(countResult.count), 0)
      : null
  const requestedPage = normalizePage(options.page, requestedCursor ? 2 : 1)
  const currentCursor = requestedCursor
    ? encodeCursorPayload(requestedCursor.createdAt, requestedCursor.id)
    : null

  const pagination = {
    page: requestedPage,
    pageSize: limit,
    cursor: currentCursor,
    nextCursor: normalizeText(pagePayload?.nextCursor) || null,
    hasPrevPage: Boolean(currentCursor),
    hasNextPage: Boolean(pagePayload?.hasNextPage),
    historyHours: historyWindowHours,
    returnedCount: mappedRows.length,
    totalCount,
    countMode: includeCount ? "exact" : "skipped",
    totalCountTimedOut: Boolean(includeCount && countResult?.timedOut)
  }

  const noRowsReason = noOpportunitiesReason(
    {
      scannedItems: Number(latestCompleted?.items_scanned || 0),
      discardedReasons: {}
    },
    status,
    mappedRows
  )

  const summary = {
    scannedItems: Number(latestCompleted?.items_scanned || 0),
    opportunities: mappedRows.length,
    totalDetected: totalCount,
    activeOpportunities: Number(
      status?.activeOpportunities ?? totalCount ?? mappedRows.length
    ),
    feedRetentionHours: FEED_WINDOW_HOURS,
    historyWindowHours,
    noOpportunitiesReason: noRowsReason,
    countDiagnostics: {
      totalCountTimedOut: Boolean(includeCount && countResult?.timedOut),
      totalCountSkipped: !includeCount
    },
    plan: {
      planTier: planContext?.planTier || "free",
      requestedLimit,
      appliedLimit: limit,
      requestedHistoryHours,
      appliedHistoryHours: historyWindowHours,
      requestedShowRisky,
      appliedShowRisky: showRisky,
      requestedIncludeOlder,
      appliedIncludeOlder: includeOlder,
      requestedCategory,
      appliedCategory: categoryFilter,
      requestedCursor: normalizeText(options.cursor) || null,
      appliedCursor: currentCursor,
      ...restricted.planLimits
    }
  }

  return {
    generatedAt:
      latestCompleted?.completed_at ||
      latestCompleted?.started_at ||
      status?.latestRun?.started_at ||
      null,
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
      activeOpportunities: Number(
        status?.activeOpportunities ?? totalCount ?? mappedRows.length
      ),
      latestRun: toStatusRunSnapshot(status?.latestRun),
      latestCompletedRun: toStatusRunSnapshot(latestCompleted)
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

  clearFeedFirstPageCache()

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
  mapFeedRowToCard,
  dedupeFeedCards,
  loadScannerSourceRows,
  persistFeedRows,
  buildFeedUpdatePatch,
  normalizeCursorPayload,
  encodeCursorPayload,
  buildFeedPageCacheKey,
  clearFeedFirstPageCache,
  confidenceLevel,
  clampScore,
  isScannerRunOverdue,
  DEFAULT_UNIVERSE_LIMIT,
  OPPORTUNITY_BATCH_TARGET,
  SCAN_CHUNK_SIZE,
  SCAN_TIMEOUT_PER_BATCH_MS,
  SCAN_STATE
}


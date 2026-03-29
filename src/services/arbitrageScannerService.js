const AppError = require("../utils/AppError")
const skinRepo = require("../repositories/skinRepository")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../repositories/marketUniverseRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const globalOpportunityLifecycleLogRepo = require("../repositories/globalOpportunityLifecycleLogRepository")
const marketComparisonService = require("./marketComparisonService")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const marketImageService = require("./marketImageService")
const candidateProgressionService = require("./scanner/candidateProgressionService")
const scanSourceCohortService = require("./scanner/scanSourceCohortService")
const globalFeedPublisher = require("./feed/globalFeedPublisher")
const {
  LIFECYCLE_STATUS,
  resolveLifecycleStatusFromState,
  buildLifecycleRow
} = require("./feed/opportunityLifecyclePolicy")
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
  SCAN_COHORT_CATEGORIES,
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
  SCAN_COHORT_DEGRADED_FALLBACK_SHARE,
  SCAN_COHORT_DEGRADED_PRIMARY_SHARE,
  ENRICHMENT_JOB_TIMEOUT_MS,
  OPPORTUNITY_HARD_TIMEOUT_MS,
  SCAN_TIMEOUT_PER_BATCH_MS,
  DUPLICATE_WINDOW_HOURS,
  RECORD_SKIPPED_ALREADY_RUNNING,
  SCANNER_V2_TUNING_SURFACE
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
const {
  resolvePublishValidationContextForOpportunity: resolveSharedPublishValidationContextForOpportunity
} = require("./scanner/publishValidation")
const {
  revalidateOpportunitiesForEmit,
  buildEmitRevalidationMetadata
} = require("./scanner/emitRevalidationService")

const MAX_API_LIMIT = 200
const FEED_PAGE_SIZE = 200
const DEFAULT_API_LIMIT = FEED_PAGE_SIZE
const MAX_FEED_LIMIT = FEED_PAGE_SIZE
const FEED_FAMILY_STREAK_CAP = 2
const FEED_WINDOW_HOURS = 24
const DEFAULT_HISTORY_WINDOW_HOURS = 24
const MAX_HISTORY_WINDOW_HOURS = 168
const MANUAL_REFRESH_TRACKER_MAX = 4000
const DEFAULT_RUNTIME_SCANNER_TYPE = SCANNER_TYPES.OPPORTUNITY_SCAN
const SCANNER_RUN_RETENTION_HOURS = 24
const CATALOG_SCAN_CATEGORIES = Object.freeze([
  ...SCAN_COHORT_CATEGORIES
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

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
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

function countScannableRowsByScannerCategory(rows = []) {
  const counts = emptyCategoryMap()
  for (const row of Array.isArray(rows) ? rows : []) {
    let classification = null
    try {
      classification = classifyCatalogState({
        ...row,
        marketHashName:
          row?.marketHashName ||
          row?.market_hash_name ||
          row?.itemName ||
          row?.item_name ||
          "",
        itemName:
          row?.itemName ||
          row?.item_name ||
          row?.marketHashName ||
          row?.market_hash_name ||
          "",
        category: row?.category || row?.itemCategory || row?.raw?.category || ""
      })
    } catch (_err) {
      classification = null
    }
    if (!classification || classification.state === SCAN_STATE.REJECTED) {
      continue
    }
    const category = mapScannerCategory(
      classification.category || row?.category || row?.itemCategory || row?.raw?.category || ""
    )
    if (counts[category] == null) continue
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

function sumCategoryCounts(counts = {}) {
  return Object.values(counts || {}).reduce((sum, value) => sum + Number(value || 0), 0)
}

function listMissingScannerCategories(counts = {}) {
  return ROUND_ROBIN_CATEGORY_ORDER.filter((category) => Number(counts?.[category] || 0) <= 0)
}

function normalizeSelectionFamilyName(value = "") {
  return normalizeText(value)
    .replace(/^stattrak(?:\u2122|â„¢)?\s*/i, "")
    .replace(/^souvenir\s+/i, "")
    .replace(/\((factory new|minimal wear|field-tested|well-worn|battle-scarred)\)\s*$/i, "")
    .replace(/^\u2605\s*/u, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
}

function resolveScannerFamilyKey(row = {}) {
  const category = mapScannerCategory(row?.category || row?.itemCategory || row?.raw?.category || "")
  const subcategory = normalizeText(
    row?.itemSubcategory || row?.subcategory || row?.raw?.subcategory
  ).toLowerCase()
  const marketHashName = normalizeText(
    row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name
  )

  let family = subcategory
  if (
    !family &&
    (category === ITEM_CATEGORIES.WEAPON_SKIN ||
      category === ITEM_CATEGORIES.KNIFE ||
      category === ITEM_CATEGORIES.GLOVE)
  ) {
    const [prefix] = marketHashName.split("|")
    family = normalizeSelectionFamilyName(prefix || marketHashName)
  }
  if (!family) family = category

  return `${category}:${family || "unknown"}`
}

function rebalanceSelectionForFeedDiversity(rows = [], options = {}) {
  const safeRows = Array.isArray(rows) ? rows.filter(Boolean) : []
  if (!safeRows.length) return []
  const maxConsecutiveFamily = Math.max(
    Math.round(Number(options.maxConsecutiveFamily || FEED_FAMILY_STREAK_CAP)),
    1
  )
  if (safeRows.length <= maxConsecutiveFamily) {
    return safeRows.slice()
  }

  const queue = safeRows.slice()
  const reordered = []
  let activeFamily = ""
  let activeFamilyStreak = 0

  while (queue.length) {
    let pickIndex = 0
    const firstFamily = resolveScannerFamilyKey(queue[0])
    if (activeFamilyStreak >= maxConsecutiveFamily && firstFamily === activeFamily) {
      const alternativeIndex = queue.findIndex(
        (candidate) => resolveScannerFamilyKey(candidate) !== activeFamily
      )
      if (alternativeIndex >= 0) {
        pickIndex = alternativeIndex
      }
    }

    const [picked] = queue.splice(pickIndex, 1)
    reordered.push(picked)
    const pickedFamily = resolveScannerFamilyKey(picked)
    if (pickedFamily === activeFamily) {
      activeFamilyStreak += 1
    } else {
      activeFamily = pickedFamily
      activeFamilyStreak = 1
    }
  }

  return reordered
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
  const sellVolume7d = toPositiveOrNull(row?.sellVolume7d ?? row?.sell_volume_7d)
  const buyVolume7d = toPositiveOrNull(row?.buyVolume7d ?? row?.buy_volume_7d)
  const marketMaxVolume7d = toPositiveOrNull(row?.marketMaxVolume7d ?? row?.market_max_volume_7d)
  const resolvedVolume7d =
    sellVolume7d ??
    marketMaxVolume7d ??
    buyVolume7d ??
    toPositiveOrNull(row?.volume7d ?? row?.liquidity)

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
    liquidity: resolvedVolume7d,
    liquidityBand: row?.liquidityBand || row?.liquidityLabel || null,
    liquidityLabel: row?.liquidityLabel || row?.liquidityBand || null,
    volume7d: resolvedVolume7d,
    sellVolume7d,
    buyVolume7d,
    marketMaxVolume7d,
    liquiditySource: normalizeText(row?.liquiditySource || row?.liquidity_source) || null,
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

function summarizeSelectedSourceRows(rows = [], batchSize = OPPORTUNITY_BATCH_RUNTIME_TARGET) {
  const safeRows = Array.isArray(rows) ? rows : []
  const selectedByCohort = {
    hot: 0,
    warm: 0,
    cold: 0,
    fallback: 0
  }
  const fallbackRowsSelectedBySource = {
    candidatePool: 0,
    activeTradable: 0
  }

  for (const row of safeRows) {
    const cohort = normalizeText(row?.scanCohort || row?.scan_cohort).toLowerCase()
    if (selectedByCohort[cohort] != null) {
      selectedByCohort[cohort] = Number(selectedByCohort[cohort] || 0) + 1
    }
    if (cohort !== "fallback") continue
    const source = normalizeText(row?.fallbackSource || row?.fallback_source).toLowerCase()
    if (source === "candidatepool") {
      fallbackRowsSelectedBySource.candidatePool =
        Number(fallbackRowsSelectedBySource.candidatePool || 0) + 1
    } else if (source === "activetradable") {
      fallbackRowsSelectedBySource.activeTradable =
        Number(fallbackRowsSelectedBySource.activeTradable || 0) + 1
    }
  }

  const selectedCount = Math.max(safeRows.length, 0)
  const fallbackSelectedCount = Number(selectedByCohort.fallback || 0)
  const hotWarmSelectedCount =
    Number(selectedByCohort.hot || 0) + Number(selectedByCohort.warm || 0)
  const fallbackSelectedShare =
    selectedCount > 0 ? Number((fallbackSelectedCount / selectedCount).toFixed(4)) : 0
  const selectedFromHotShare =
    selectedCount > 0 ? Number((Number(selectedByCohort.hot || 0) / selectedCount).toFixed(4)) : 0
  const selectedFromWarmShare =
    selectedCount > 0 ? Number((Number(selectedByCohort.warm || 0) / selectedCount).toFixed(4)) : 0
  const selectedFromColdShare =
    selectedCount > 0 ? Number((Number(selectedByCohort.cold || 0) / selectedCount).toFixed(4)) : 0
  const primarySelectedShare =
    selectedCount > 0 ? Number((hotWarmSelectedCount / selectedCount).toFixed(4)) : 0

  return {
    scanner_selected_count: selectedCount,
    selectedByCohort,
    fallbackRowsSelectedBySource,
    fallbackSelectedShare,
    fallback_selected_share: fallbackSelectedShare,
    selectedFromHotShare,
    selected_from_hot_share: selectedFromHotShare,
    selectedFromWarmShare,
    selected_from_warm_share: selectedFromWarmShare,
    selectedFromColdShare,
    selected_from_cold_share: selectedFromColdShare,
    degradedScannerHealth:
      Number(fallbackRowsSelectedBySource.activeTradable || 0) > 0 ||
      fallbackSelectedShare > SCAN_COHORT_DEGRADED_FALLBACK_SHARE ||
      primarySelectedShare < SCAN_COHORT_DEGRADED_PRIMARY_SHARE,
    batchSize: Math.max(Math.round(Number(batchSize || 0)), 0)
  }
}

function sumCounterMap(values = {}) {
  if (!values || typeof values !== "object" || Array.isArray(values)) {
    return 0
  }
  return Object.values(values).reduce((total, value) => {
    const numeric = Number(value)
    if (!Number.isFinite(numeric)) return total
    return total + Math.max(numeric, 0)
  }, 0)
}

function buildJobExecutionDiagnostics({
  jobType,
  selectedRows = 0,
  skippedRows = 0,
  enrichedRows = 0,
  eligibleRows = 0,
  emittedRows = 0,
  blockedRows = 0
} = {}) {
  const normalizedJobType = normalizeText(jobType) || DEFAULT_RUNTIME_SCANNER_TYPE
  return {
    job_type: normalizedJobType,
    selected_rows: Math.max(Math.round(Number(selectedRows || 0)), 0),
    skipped_rows: Math.max(Math.round(Number(skippedRows || 0)), 0),
    enriched_rows: Math.max(Math.round(Number(enrichedRows || 0)), 0),
    eligible_rows: Math.max(Math.round(Number(eligibleRows || 0)), 0),
    emitted_rows: Math.max(Math.round(Number(emittedRows || 0)), 0),
    blocked_rows: Math.max(Math.round(Number(blockedRows || 0)), 0)
  }
}

function buildEnrichmentJobExecutionDiagnostics({
  forceRefresh = false,
  sourceCatalogDiagnostics = {},
  selectedRows = 0,
  enrichedRows = 0
} = {}) {
  const sourceCatalog =
    sourceCatalogDiagnostics?.sourceCatalog &&
    typeof sourceCatalogDiagnostics.sourceCatalog === "object" &&
    !Array.isArray(sourceCatalogDiagnostics.sourceCatalog)
      ? sourceCatalogDiagnostics.sourceCatalog
      : {}
  const dueBacklogRowsByState =
    sourceCatalogDiagnostics?.due_backlog_rows_by_state &&
    typeof sourceCatalogDiagnostics.due_backlog_rows_by_state === "object" &&
    !Array.isArray(sourceCatalogDiagnostics.due_backlog_rows_by_state)
      ? sourceCatalogDiagnostics.due_backlog_rows_by_state
      : {}
  const processedRows = Number(
    sourceCatalogDiagnostics?.progression_rows_processed_total || enrichedRows || selectedRows || 0
  )
  const effectiveSelectedRows = forceRefresh
    ? Number(
        sourceCatalog.incrementalRecomputeRows ||
          sourceCatalog.fullRebuildRows ||
          sourceCatalog.totalRows ||
          processedRows ||
          selectedRows ||
          0
      )
    : Number(selectedRows || processedRows || 0)
  const effectiveEnrichedRows = forceRefresh
    ? Number(
        sourceCatalog.incrementalRecomputeRows ||
          sourceCatalog.fullRebuildRows ||
          sourceCatalog.totalRows ||
          processedRows ||
          enrichedRows ||
          0
      )
    : Number(enrichedRows || processedRows || 0)
  const skippedRows = forceRefresh
    ? Number(sourceCatalog.incrementalSkippedRows || 0)
    : Math.max(sumCounterMap(dueBacklogRowsByState) - effectiveEnrichedRows, 0)
  const eligibleRows = forceRefresh
    ? Number(sourceCatalog.eligibleTradableRows || sourceCatalog.eligibleRows || 0)
    : Number(sourceCatalogDiagnostics?.eligible_tradable_rows || 0)

  return buildJobExecutionDiagnostics({
    jobType: SCANNER_TYPES.ENRICHMENT,
    selectedRows: effectiveSelectedRows,
    skippedRows,
    enrichedRows: effectiveEnrichedRows,
    eligibleRows,
    emittedRows: 0,
    blockedRows: 0
  })
}

function buildOpportunityJobExecutionDiagnostics({
  selection = {},
  evaluations = {},
  eligibleRows = 0,
  persisted = {}
} = {}) {
  const selectedRows = Number(selection?.selected?.length || 0)
  const effectiveEligibleRows = Math.max(
    Math.round(Number(eligibleRows || evaluations?.scannedItems || 0)),
    0
  )
  return buildJobExecutionDiagnostics({
    jobType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    selectedRows,
    skippedRows: Math.max(selectedRows - effectiveEligibleRows, 0),
    enrichedRows: 0,
    eligibleRows: effectiveEligibleRows,
    emittedRows: Number(
      persisted?.emittedCount ||
        persisted?.activeRowsWritten ||
        persisted?.insertedCount ||
        0
    ),
    blockedRows: Number(
      persisted?.emitRevalidation?.blocked_on_emit_total ||
        persisted?.publishValidation?.blocked ||
        0
    )
  })
}

function normalizeRecoveryCandidateStatus(value = "") {
  const status = normalizeText(value).toLowerCase()
  if (
    status === "candidate" ||
    status === "enriching" ||
    status === "near_eligible" ||
    status === "eligible" ||
    status === "rejected"
  ) {
    return status
  }
  return ""
}

function normalizeRecoveryCatalogStatus(value = "", fallback = "shadow") {
  const status = normalizeText(value).toLowerCase()
  if (status === "scannable" || status === "shadow" || status === "blocked") return status
  return normalizeText(fallback).toLowerCase() || "shadow"
}

function applyRecoveryCatalogCompatibility(row = {}) {
  const compatible = marketSourceCatalogService.resolveCompatibleCatalogStatusFields(row)
  return {
    ...row,
    catalog_status:
      compatible?.catalogStatus || row?.catalog_status || row?.catalogStatus || "shadow",
    catalog_block_reason:
      compatible?.catalogBlockReason || row?.catalog_block_reason || row?.catalogBlockReason || null,
    catalog_quality_score:
      compatible?.catalogQualityScore ?? row?.catalog_quality_score ?? row?.catalogQualityScore ?? 0,
    last_market_signal_at:
      compatible?.lastMarketSignalAt || row?.last_market_signal_at || row?.lastMarketSignalAt || null
  }
}

function isRecoveryBaseRow(row = {}) {
  const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
  if (!CATALOG_SCAN_CATEGORIES.includes(category)) return false
  if (row?.is_active === false || row?.isActive === false) return false
  if (row?.tradable === false) return false
  const compatibleRow = applyRecoveryCatalogCompatibility(row)
  return (
    normalizeRecoveryCatalogStatus(compatibleRow?.catalog_status || compatibleRow?.catalogStatus) ===
    "scannable"
  )
}

function decorateRecoveryPrimaryRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .filter((row) => isRecoveryBaseRow(row))
    .map((row) => {
      const scanEligible =
        row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible)
      const candidateStatus = normalizeRecoveryCandidateStatus(
        row?.candidate_status ?? row?.candidateStatus
      )

      let scanCohort = ""
      if (scanEligible) {
        scanCohort = "hot"
      } else if (candidateStatus === "near_eligible") {
        scanCohort = "warm"
      } else if (
        (candidateStatus === "candidate" || candidateStatus === "enriching") &&
        marketSourceCatalogService.isUniverseBackfillReadyRow(row)
      ) {
        scanCohort = "cold"
      }

      if (!scanCohort) return null
      return {
        ...applyRecoveryCatalogCompatibility(row),
        scanCohort,
        sourceOrigin: "recovery_primary",
        fallbackSource: null
      }
    })
    .filter(Boolean)
}

function decorateRecoveryFallbackRows(rows = [], fallbackSource = "") {
  const safeFallbackSource = normalizeText(fallbackSource)
  return (Array.isArray(rows) ? rows : [])
    .filter((row) => isRecoveryBaseRow(row))
    .map((row) => ({
      ...applyRecoveryCatalogCompatibility(row),
      scanCohort: "fallback",
      sourceOrigin: "recovery_fallback",
      fallbackSource: safeFallbackSource
    }))
}

async function loadScannerSourceRowsRecovery() {
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
    sourceMode: "active_generation_universe_recovery",
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
    scannablePoolBeforeTopup: 0,
    scannablePoolAfterTopup: 0,
    missingCategoriesBeforeTopup: [],
    missingCategoriesAfterTopup: [],
    universeRowsLoaded: 0,
    catalogRowsResolved: 0,
    universeRowsMissingCatalog: 0
  }
  let lastError = null
  for (let index = 0; index < attempts.length; index += 1) {
    const limit = attempts[index]
    diagnostics.attemptedLimits.push(limit)
    try {
      const universeRows = await marketUniverseRepo.listActiveByLiquidityRank({
        limit,
        categories: CATALOG_SCAN_CATEGORIES,
        requireOpportunityScanEnabled: true
      })
      diagnostics.universeRowsLoaded = universeRows.length
      const universeByName = new Map(
        universeRows.map((row) => [
          normalizeText(row?.market_hash_name || row?.marketHashName),
          row
        ])
      )
      const universeNames = universeRows
        .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
        .filter(Boolean)
      const catalogRows = universeNames.length
        ? await marketSourceCatalogRepo.listByMarketHashNames(universeNames, {
          limit,
          categories: CATALOG_SCAN_CATEGORIES,
          requireOpportunityScanEnabled: true
        })
        : []
      const matchedCatalogRows = catalogRows.filter((row) =>
        universeByName.has(normalizeText(row?.market_hash_name || row?.marketHashName))
      )
      diagnostics.catalogRowsResolved = matchedCatalogRows.length
      diagnostics.universeRowsMissingCatalog = Math.max(
        universeNames.length - matchedCatalogRows.length,
        0
      )

      let rows = decorateRecoveryPrimaryRows(
        [...matchedCatalogRows].sort((left, right) => {
          const leftUniverse = universeByName.get(
            normalizeText(left?.market_hash_name || left?.marketHashName)
          )
          const rightUniverse = universeByName.get(
            normalizeText(right?.market_hash_name || right?.marketHashName)
          )
          const leftRank = Number(
            leftUniverse?.liquidity_rank || leftUniverse?.liquidityRank || Infinity
          )
          const rightRank = Number(
            rightUniverse?.liquidity_rank || rightUniverse?.liquidityRank || Infinity
          )
          if (leftRank !== rightRank) return leftRank - rightRank
          return normalizeText(left?.market_hash_name || left?.marketHashName).localeCompare(
            normalizeText(right?.market_hash_name || right?.marketHashName)
          )
        })
      )

      let categoryCounts = countRowsByScannerCategory(rows)
      let scannablePoolSize = sumCategoryCounts(categoryCounts)
      let missingCategories = listMissingScannerCategories(categoryCounts)
      diagnostics.categoryCountsBeforeTopup = categoryCounts
      diagnostics.scannablePoolBeforeTopup = scannablePoolSize
      diagnostics.missingCategoriesBeforeTopup = missingCategories

      categoryCounts = countRowsByScannerCategory(rows)
      scannablePoolSize = sumCategoryCounts(categoryCounts)
      diagnostics.selectedLimit = limit
      diagnostics.categoryCountsAfterTopup = categoryCounts
      diagnostics.scannablePoolAfterTopup = scannablePoolSize
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

async function loadScannerSourceRows() {
  try {
    return await scanSourceCohortService.loadScanSource({
      batchSize: OPPORTUNITY_BATCH_RUNTIME_TARGET,
      categories: CATALOG_SCAN_CATEGORIES
    })
  } catch (_err) {
    const recovery = await loadScannerSourceRowsRecovery()
    const diagnostics =
      recovery?.diagnostics && typeof recovery.diagnostics === "object"
        ? recovery.diagnostics
        : {}
    diagnostics.sourceMode = "recovery_universe_source"
    diagnostics.fallbackUsed = true
    diagnostics.degradedScannerHealth = true
    diagnostics.fallbackReasons = Array.from(
      new Set([
        ...(Array.isArray(diagnostics.fallbackReasons) ? diagnostics.fallbackReasons : []),
        "cohort_loader_failed"
      ])
    )
    diagnostics.cohortQueryFailures = diagnostics.cohortQueryFailures || {
      universe: true,
      catalog: false,
      hot: true,
      warm: true,
      cold: true,
      candidatePool: Boolean(diagnostics?.topup?.candidatePoolFailed),
      activeTradable: Boolean(diagnostics?.topup?.activeTradableFailed)
    }
    diagnostics.primaryCohortCounts = diagnostics.primaryCohortCounts || {
      hot: 0,
      warm: 0,
      cold: 0
    }
    diagnostics.fallbackRowsLoadedBySource = diagnostics.fallbackRowsLoadedBySource || {
      candidatePool: Number(diagnostics?.topup?.candidatePoolRowsAdded || 0),
      activeTradable: Number(diagnostics?.topup?.activeTradableRowsAdded || 0)
    }
    diagnostics.fallbackRowsSelectedBySource = diagnostics.fallbackRowsSelectedBySource || {
      candidatePool: 0,
      activeTradable: 0
    }
    diagnostics.fallbackSelectedShare = Number(diagnostics.fallbackSelectedShare || 0)
    return {
      ...recovery,
      diagnostics
    }
  }
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

async function compareCandidates(candidates = []) {
  const byName = {}
  const allowLiveFetch = false
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
    eligibleFound: 0,
    nearEligibleFound: 0,
    candidateFound: 0,
    rejectedFound: 0,
    opportunitiesByCategory: emptyCategoryMap(),
    rejectedByCategory: emptyCategoryMap(),
    rejectedByReason: {},
    weaponSkinEvaluator: {
      outcome: {
        rejected: 0,
        candidate: 0,
        near_eligible: 0,
        eligible: 0
      },
      missing_liquidity_penalty: 0,
      partial_market_coverage_penalty: 0,
      stale_supporting_input_penalty: 0,
      thin_executable_depth_penalty: 0,
      low_value_contextual_penalty: 0,
      rejected_reason: {},
      publish_preview_result: {},
      freshness_contract: {
        missing_buy_route_timestamp: 0,
        missing_sell_route_timestamp: 0,
        buy_route_stale: 0,
        sell_route_stale: 0,
        buy_and_sell_route_stale: 0,
        buy_route_unavailable: 0,
        sell_route_unavailable: 0,
        missing_listing_availability: 0,
        freshness_contract_incomplete: 0
      },
      final_tier: {
        eligible: 0,
        near_eligible: 0,
        candidate: 0,
        rejected: 0
      }
    }
  }
  for (const row of rows) {
    const category = mapScannerCategory(row.itemCategory)
    if (row.tier === OPPORTUNITY_TIERS.ELIGIBLE) {
      summary.eligibleFound += 1
      summary.opportunitiesByCategory[category] += 1
    } else if (row.tier === OPPORTUNITY_TIERS.NEAR_ELIGIBLE) {
      summary.nearEligibleFound += 1
      summary.opportunitiesByCategory[category] += 1
    } else if (row.tier === OPPORTUNITY_TIERS.CANDIDATE) {
      summary.candidateFound += 1
      summary.opportunitiesByCategory[category] += 1
    } else {
      summary.rejectedFound += 1
      summary.rejectedByCategory[category] += 1
      for (const reason of row.hardRejectReasons || []) {
        incrementCounter(summary.rejectedByReason, reason)
      }
    }

    if (category !== ITEM_CATEGORIES.WEAPON_SKIN) {
      continue
    }

    const metadata =
      row?.metadata && typeof row.metadata === "object" && !Array.isArray(row.metadata)
        ? row.metadata
        : {}
    const weaponSkinDiagnostics =
      metadata?.weapon_skin_evaluator_diagnostics &&
      typeof metadata.weapon_skin_evaluator_diagnostics === "object" &&
      !Array.isArray(metadata.weapon_skin_evaluator_diagnostics)
        ? metadata.weapon_skin_evaluator_diagnostics
        : {}
    const freshnessContractDiagnostics =
      metadata?.freshness_contract_diagnostics &&
      typeof metadata.freshness_contract_diagnostics === "object" &&
      !Array.isArray(metadata.freshness_contract_diagnostics)
        ? metadata.freshness_contract_diagnostics
        : {}
    const evaluationDisposition = normalizeText(
      row?.evaluationDisposition ||
        metadata?.evaluation_disposition ||
        weaponSkinDiagnostics?.outcome
    ).toLowerCase()
    if (summary.weaponSkinEvaluator.outcome[evaluationDisposition] != null) {
      summary.weaponSkinEvaluator.outcome[evaluationDisposition] =
        Number(summary.weaponSkinEvaluator.outcome[evaluationDisposition] || 0) + 1
    }

    const finalTier = normalizeText(
      weaponSkinDiagnostics?.final_tier || row?.finalTier || row?.tier || metadata?.final_tier
    ).toLowerCase()
    if (summary.weaponSkinEvaluator.final_tier[finalTier] != null) {
      summary.weaponSkinEvaluator.final_tier[finalTier] =
        Number(summary.weaponSkinEvaluator.final_tier[finalTier] || 0) + 1
    }

    if (Boolean(weaponSkinDiagnostics?.missing_liquidity_penalty)) {
      summary.weaponSkinEvaluator.missing_liquidity_penalty += 1
    }
    if (Boolean(weaponSkinDiagnostics?.partial_market_coverage_penalty)) {
      summary.weaponSkinEvaluator.partial_market_coverage_penalty += 1
    }
    if (Boolean(weaponSkinDiagnostics?.stale_supporting_input_penalty)) {
      summary.weaponSkinEvaluator.stale_supporting_input_penalty += 1
    }
    if (Boolean(weaponSkinDiagnostics?.thin_executable_depth_penalty)) {
      summary.weaponSkinEvaluator.thin_executable_depth_penalty += 1
    }
    if (Boolean(weaponSkinDiagnostics?.low_value_contextual_penalty)) {
      summary.weaponSkinEvaluator.low_value_contextual_penalty += 1
    }

    incrementCounter(summary.weaponSkinEvaluator.rejected_reason, weaponSkinDiagnostics?.rejected_reason)
    incrementCounter(
      summary.weaponSkinEvaluator.publish_preview_result,
      weaponSkinDiagnostics?.publish_preview_result || metadata?.publish_preview_result
    )
    for (const key of Object.keys(summary.weaponSkinEvaluator.freshness_contract)) {
      if (Boolean(freshnessContractDiagnostics?.[key])) {
        summary.weaponSkinEvaluator.freshness_contract[key] += 1
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

function buildPublishValidationMetadata(validation = {}) {
  const signalAgeMs = toFiniteOrNull(validation?.signalAgeMs)
  const publishValidatedAt = toIsoOrNull(validation?.publishValidatedAt)
  const publishFreshnessState = normalizeText(validation?.publishFreshnessState) || "missing"
  const requiredRouteState = normalizeText(validation?.requiredRouteState) || "missing_buy_and_sell_route"
  const listingAvailabilityState = normalizeText(validation?.listingAvailabilityState) || "unknown"
  const staleReason = normalizeText(validation?.staleReason) || null
  const routeSignalObservedAt = toIsoOrNull(validation?.routeSignalObservedAt)
  const routeFreshnessContract =
    validation?.routeFreshnessContract &&
    typeof validation.routeFreshnessContract === "object" &&
    !Array.isArray(validation.routeFreshnessContract)
      ? validation.routeFreshnessContract
      : null
  const freshnessContractDiagnostics =
    validation?.freshnessContractDiagnostics &&
    typeof validation.freshnessContractDiagnostics === "object" &&
    !Array.isArray(validation.freshnessContractDiagnostics)
      ? validation.freshnessContractDiagnostics
      : null
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
    route_freshness_contract: routeFreshnessContract,
    routeFreshnessContract: routeFreshnessContract,
    freshness_contract_diagnostics: freshnessContractDiagnostics,
    publish_validation: {
      is_publishable: Boolean(validation?.isPublishable),
      signal_age_ms: signalAgeMs,
      publish_validated_at: publishValidatedAt,
      publish_freshness_state: publishFreshnessState,
      required_route_state: requiredRouteState,
      listing_availability_state: listingAvailabilityState,
      stale_reason: staleReason,
      route_signal_observed_at: routeSignalObservedAt,
      route_freshness_contract: routeFreshnessContract,
      freshness_contract_diagnostics: freshnessContractDiagnostics
    }
  }
}

function resolvePublishValidationContextForOpportunity(opportunity = {}, nowMs = Date.now(), nowIso = null) {
  return resolveSharedPublishValidationContextForOpportunity(opportunity, nowMs, nowIso)
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

async function persistFeedRows(opportunities = [], scanRunId = null, options = {}) {
  const scannedCount = Math.max(Math.round(Number(options?.scannedCount || 0)), 0)
  const result = await globalFeedPublisher.publishBatch({
    scanRunId,
    opportunities,
    nowIso: new Date().toISOString(),
    trigger: "opportunity_scan",
    scannedCount
  })

  return {
    insertedCount: Number(result?.activeRowsWritten || 0),
    newCount: Math.max(
      Number(result?.publishedCount || 0) -
        Number(result?.updatedCount || 0) -
        Number(result?.reactivatedCount || 0),
      0
    ),
    updatedCount: Number(result?.updatedCount || 0),
    reactivatedCount: Number(result?.reactivatedCount || 0),
    duplicateCount: 0,
    skippedUnchanged: Number(result?.unchangedCount || 0),
    publishValidation: {
      blocked: Number(result?.blockedCount || 0),
      deactivated: 0,
      reasons: result?.validationReasons || {}
    },
    emitRevalidation: result?.emitRevalidation || {
      emit_revalidation_checked: 0,
      emitted_after_revalidation: Number(result?.publishedCount || 0),
      blocked_on_emit_total: Number(result?.blockedCount || 0),
      blocked_on_emit_by_reason: {}
    },
    lifecycle: result?.lifecycle || {
      detected_total: 0,
      published_total: Number(result?.publishedCount || 0),
      expired_total: 0,
      invalidated_total: 0,
      blocked_on_emit_total: Number(result?.blockedCount || 0)
    },
    emittedCount: Number(result?.emittedCount ?? result?.publishedCount ?? 0),
    publisherMetrics: result?.publisherMetrics || null,
    cleanup: {
      olderMarkedInactive: 0,
      beyondLimitMarkedInactive: 0,
      duplicateActivesMarkedInactive: 0,
      hadTimeout: false,
      errors: []
    },
    activeRowsWritten: Number(result?.activeRowsWritten || 0),
    historyRowsWritten: Number(result?.historyRowsWritten || 0)
  }
}

function roundDiagnosticRatio(numerator, denominator, digits = 4) {
  const safeNumerator = Number(numerator || 0)
  const safeDenominator = Number(denominator || 0)
  if (!Number.isFinite(safeNumerator) || !Number.isFinite(safeDenominator) || safeDenominator <= 0) {
    return 0
  }
  return Number((safeNumerator / safeDenominator).toFixed(digits))
}

function buildTopCountList(counter = {}, options = {}) {
  const safeCounter = toJsonObject(counter)
  const limit = Math.max(Math.round(Number(options.limit || 0)), 1)
  const labelKey = normalizeText(options.labelKey || "reason") || "reason"
  return Object.entries(safeCounter)
    .map(([key, value]) => ({
      [labelKey]: normalizeText(key),
      count: Number(value || 0)
    }))
    .filter((entry) => entry[labelKey] && entry.count > 0)
    .sort((left, right) => {
      if (right.count !== left.count) return right.count - left.count
      return String(left[labelKey]).localeCompare(String(right[labelKey]))
    })
    .slice(0, limit)
}

function buildTopCountListByCategory(counterByCategory = {}, options = {}) {
  const safeCounterByCategory = toJsonObject(counterByCategory)
  const limit = Math.max(Math.round(Number(options.limit || 0)), 1)
  const summary = {}
  for (const [category, reasons] of Object.entries(safeCounterByCategory)) {
    const safeCategory = normalizeText(category).toLowerCase()
    const topReasons = buildTopCountList(reasons, {
      limit,
      labelKey: options.labelKey || "reason"
    })
    if (!safeCategory || !topReasons.length) continue
    summary[safeCategory] = topReasons
  }
  return summary
}

function buildConsolidatedDiagnosticsSummary({
  evaluations = {},
  persisted = {},
  sourceCatalog = {}
} = {}) {
  const catalogLoad = toJsonObject(sourceCatalog?.catalogLoad)
  const publisherMetrics = toJsonObject(persisted?.publisherMetrics)
  const lifecycle = toJsonObject(persisted?.lifecycle)
  const emitRevalidation = toJsonObject(persisted?.emitRevalidation)
  const detailedEmitBlockReasons = toJsonObject(persisted?.publishValidation?.reasons)
  const topRejectReasonsByCategory = buildTopCountListByCategory(
    catalogLoad?.top_reject_reasons_by_category || catalogLoad?.topRejectReasonsByCategory || {},
    {
      limit: 3,
      labelKey: "reason"
    }
  )
  if (
    !Object.keys(topRejectReasonsByCategory).length &&
    Object.keys(toJsonObject(evaluations?.rejectedByReason)).length
  ) {
    topRejectReasonsByCategory.all = buildTopCountList(evaluations.rejectedByReason, {
      limit: 5,
      labelKey: "reason"
    })
  }

  const scannedCount = Number(publisherMetrics?.scannedCount || evaluations?.scannedItems || 0)
  const eligibleCount = Number(
    publisherMetrics?.eligibleCount ||
      Number(evaluations?.eligibleFound || 0) +
        Number(evaluations?.nearEligibleFound || 0) +
        Number(evaluations?.candidateFound || 0)
  )
  const emittedCount = Number(
    publisherMetrics?.emittedCount ||
      persisted?.emittedCount ||
      persisted?.activeRowsWritten ||
      persisted?.insertedCount ||
      0
  )
  const blockedOnEmitCount = Number(
    publisherMetrics?.blockedOnEmitCount ||
      emitRevalidation?.blocked_on_emit_total ||
      persisted?.publishValidation?.blocked ||
      0
  )

  return {
    lifecycleDistribution: {
      detected: Number(lifecycle?.detected_total || 0),
      published: Number(lifecycle?.published_total || 0),
      expired: Number(lifecycle?.expired_total || 0),
      invalidated: Number(lifecycle?.invalidated_total || 0),
      blockedOnEmit: Number(lifecycle?.blocked_on_emit_total || 0)
    },
    repairOutcomes: {
      repairCandidatesSelected: Number(catalogLoad?.repair_candidates_selected || 0),
      repairedRows: Number(catalogLoad?.repaired_rows || 0),
      repairedToNearEligible: Number(catalogLoad?.repaired_to_near_eligible || 0),
      repairedToEligible: Number(catalogLoad?.repaired_to_eligible || 0),
      cooldown: Number(catalogLoad?.cooldown_after_failed_repair || 0),
      rejected: Number(catalogLoad?.rejected_after_failed_repair || 0),
      topFailedRepairReasons: buildTopCountList(catalogLoad?.top_failed_repair_reasons || {}, {
        limit: 5,
        labelKey: "reason"
      })
    },
    hotUniverseComposition: {
      sourceMode: normalizeText(sourceCatalog?.mode || catalogLoad?.sourceMode) || null,
      selectionLayer: normalizeText(catalogLoad?.selection_layer) || "alpha_hot_universe",
      hotUniverseSize: Number(catalogLoad?.hot_universe_size || 0),
      byCategory: toJsonObject(catalogLoad?.hot_universe_by_category),
      byState: toJsonObject(catalogLoad?.hot_universe_by_state),
      intakeByCategory: toJsonObject(catalogLoad?.intake_by_category),
      nearEligibleAllowed: Boolean(catalogLoad?.near_eligible_allowed),
      nearEligibleCap: Number(catalogLoad?.near_eligible_cap || 0),
      categoryQuotas: toJsonObject(catalogLoad?.category_quotas),
      quotaHitsByCategory: toJsonObject(catalogLoad?.quota_hits_by_category),
      quotaSkipsByCategory: toJsonObject(catalogLoad?.quota_skips_by_category)
    },
    emittedVsBlockedOnEmit: {
      scannedCount,
      eligibleCount,
      emittedCount,
      blockedOnEmitCount,
      staleOnEmitCount: Number(
        publisherMetrics?.staleOnEmitCount || emitRevalidation?.stale_on_emit_count || 0
      ),
      emittedScannedRatio: Number(
        publisherMetrics?.emittedScannedRatio ??
          roundDiagnosticRatio(emittedCount, scannedCount)
      )
    },
    topRejectReasonsByCategory,
    topBlockReasonsOnEmit: buildTopCountList(
      Object.keys(detailedEmitBlockReasons).length
        ? detailedEmitBlockReasons
        : emitRevalidation?.blocked_on_emit_by_reason || {},
      {
        limit: 5,
        labelKey: "reason"
      }
    )
  }
}

function mergeDiagnostics({
  job = {},
  selection = {},
  compare = {},
  evaluations = {},
  persisted = {},
  sourceCatalog = {},
  timing = {},
  runtimeConfig = {}
} = {}) {
  const selectionDiag = selection.diagnostics || {}
  const catalogLoad = toJsonObject(sourceCatalog?.catalogLoad)
  const loadedScannerSourceRows = Number(
    sourceCatalog?.loaded_scanner_source_rows ||
      sourceCatalog?.loadedScannerSourceRows ||
      sourceCatalog?.scannerSourceSize ||
      0
  )
  const rowsDroppedBeforeAlpha = Number(
    catalogLoad?.rows_dropped_before_alpha || catalogLoad?.universeRowsDroppedBeforeAlpha || 0
  )
  const finalItemsScanned = Number(
    evaluations.scannedItems || selection.selected?.length || job?.selected_rows || 0
  )
  return {
    ...job,
    loaded_scanner_source_rows: loadedScannerSourceRows,
    rows_dropped_before_alpha: rowsDroppedBeforeAlpha,
    final_items_scanned: finalItemsScanned,
    scanStateCounts: {
      eligible: Number(selectionDiag.eligible || 0),
      nearEligible: Number(selectionDiag.nearEligible || 0),
      rejected: Number(selectionDiag.rejected || 0)
    },
    scanStateByCategory: selectionDiag.stateByCategory || {},
    candidatePoolSize: Number(selection.poolSize || 0),
    requestedBatchSize: Number(selection.attemptedBatchSize || 0),
    selectedBatchSize: Number(selection.selected?.length || 0),
    scannedItems: Number(evaluations.scannedItems || 0),
    eligibleFound: Number(evaluations.eligibleFound || 0),
    nearEligibleFound: Number(evaluations.nearEligibleFound || 0),
    candidateFound: Number(evaluations.candidateFound || 0),
    rejectedFound: Number(evaluations.rejectedFound || 0),
    opportunitiesByCategory: evaluations.opportunitiesByCategory || {},
    rejectedByCategory: evaluations.rejectedByCategory || {},
    rejectedByReason: evaluations.rejectedByReason || {},
    weaponSkinEvaluator: evaluations.weaponSkinEvaluator || {},
    consolidatedSummary: buildConsolidatedDiagnosticsSummary({
      evaluations,
      persisted,
      sourceCatalog
    }),
    tuningSurface: SCANNER_V2_TUNING_SURFACE,
    persisted: {
      insertedCount: Number(persisted.insertedCount || 0),
      newCount: Number(persisted.newCount || 0),
      updatedCount: Number(persisted.updatedCount || 0),
      reactivatedCount: Number(persisted.reactivatedCount || 0),
      duplicateCount: Number(persisted.duplicateCount || 0),
      skippedUnchanged: Number(persisted.skippedUnchanged || 0),
      publishValidation: {
        blocked: Number(persisted?.publishValidation?.blocked || 0),
        deactivated: Number(persisted?.publishValidation?.deactivated || 0),
        reasons: persisted?.publishValidation?.reasons || {},
        freshnessContract: persisted?.publishValidation?.freshnessContract || {}
      },
      emitRevalidation:
        persisted?.emitRevalidation &&
        typeof persisted.emitRevalidation === "object" &&
        !Array.isArray(persisted.emitRevalidation)
          ? persisted.emitRevalidation
          : {},
      lifecycle:
        persisted?.lifecycle &&
        typeof persisted.lifecycle === "object" &&
        !Array.isArray(persisted.lifecycle)
          ? persisted.lifecycle
          : {},
      cleanup: persisted.cleanup || {},
      emittedCount: Number(
        persisted?.emittedCount ||
          persisted?.activeRowsWritten ||
          persisted?.insertedCount ||
          0
      )
    },
    sourceCatalog,
    sourceCatalogSummary: {
      loaded_scanner_source_rows: loadedScannerSourceRows,
      rows_dropped_before_alpha: rowsDroppedBeforeAlpha
    },
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
  const sourceCatalogDiagnostics = forceRefresh
    ? await marketSourceCatalogService
        .prepareSourceCatalog({
          forceRefresh: true,
          targetUniverseSize: DEFAULT_UNIVERSE_LIMIT
        })
        .catch((err) => ({
          error: normalizeText(err?.message) || "source_catalog_prepare_failed"
        }))
    : await candidateProgressionService
        .runProgressionBatch({
          batchSize: ENRICHMENT_BATCH_TARGET
        })
        .then((result) => ({
          mode: "candidate_progression",
          ...result.diagnostics,
          processedMarketHashNames: result.processedMarketHashNames || []
        }))
        .catch((err) => ({
          mode: "candidate_progression",
          error: normalizeText(err?.message) || "candidate_progression_failed",
          processedMarketHashNames: []
        }))

  const names = Array.from(
    new Set(
      (Array.isArray(sourceCatalogDiagnostics?.processedMarketHashNames)
        ? sourceCatalogDiagnostics.processedMarketHashNames
        : []
      )
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
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

  const enrichedRows = Number(
    forceRefresh
      ? sourceCatalogDiagnostics?.sourceCatalog?.incrementalRecomputeRows ||
          sourceCatalogDiagnostics?.sourceCatalog?.fullRebuildRows ||
          sourceCatalogDiagnostics?.sourceCatalog?.totalRows ||
          0
      : sourceCatalogDiagnostics?.progression_rows_processed_total || names.length
  )

  return {
    selectedCount: enrichedRows,
    opportunitiesFound: 0,
    newOpportunitiesAdded: 0,
    diagnostics: {
      ...buildEnrichmentJobExecutionDiagnostics({
        forceRefresh,
        sourceCatalogDiagnostics,
        selectedRows: enrichedRows,
        enrichedRows
      }),
      sourceCatalog: sourceCatalogDiagnostics,
      imageEnrichment: {
        attempted: names.length,
        updated: imageUpdated
      }
    }
  }
}

async function runOpportunityJob({ scanRunId = null } = {}) {
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
  const preDiversifiedSelection = Array.isArray(selection?.selected) ? selection.selected : []
  selection.selected = rebalanceSelectionForFeedDiversity(preDiversifiedSelection, {
    maxConsecutiveFamily: FEED_FAMILY_STREAK_CAP
  })
  if (selection?.diagnostics && typeof selection.diagnostics === "object") {
    const beforeKeys = preDiversifiedSelection.map(
      (row) =>
        normalizeText(
          row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name
        ) || "unknown"
    )
    const afterKeys = (selection.selected || []).map(
      (row) =>
        normalizeText(
          row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name
        ) || "unknown"
    )
    selection.diagnostics.diversityStreakCap = FEED_FAMILY_STREAK_CAP
    selection.diagnostics.diversityReordered = beforeKeys.join("|") !== afterKeys.join("|")
  }
  const sourceSelectionDiagnostics = summarizeSelectedSourceRows(
    selection.selected || [],
    OPPORTUNITY_BATCH_RUNTIME_TARGET
  )
  if (catalogLoad?.diagnostics && typeof catalogLoad.diagnostics === "object") {
    catalogLoad.diagnostics.fallbackRowsSelectedBySource =
      sourceSelectionDiagnostics.fallbackRowsSelectedBySource
    catalogLoad.diagnostics.fallbackSelectedShare =
      sourceSelectionDiagnostics.fallbackSelectedShare
    catalogLoad.diagnostics.degradedScannerHealth = Boolean(
      catalogLoad.diagnostics.degradedScannerHealth || sourceSelectionDiagnostics.degradedScannerHealth
    )
  }
  selection.cursorBefore = rotationState.cursor
  rotationState.cursor = selection.nextCursor
  trimRotationMap()
  const selectionMs = Date.now() - selectionStartedAtMs

  const computeStartedAtMs = Date.now()
  const compared = await compareCandidates(selection.selected || [])
  const evaluated = (selection.selected || []).map((candidate) =>
    evaluateCandidateOpportunity(candidate, compared.byName?.[candidate.marketHashName] || {})
  )
  const evaluationSummary = summarizeEvaluations(evaluated)
  const opportunities = rebalanceSelectionForFeedDiversity(
    evaluated.filter((row) => row.rejected !== true),
    {
      maxConsecutiveFamily: FEED_FAMILY_STREAK_CAP
    }
  )
  const computeMs = Date.now() - computeStartedAtMs

  const writeStartedAtMs = Date.now()
  const persisted = await persistFeedRows(opportunities, scanRunId, {
    scannedCount: Number(selection.selected?.length || 0)
  })
  const writeMs = Date.now() - writeStartedAtMs
  const totalRunMs = Date.now() - runStartedAtMs

  return {
    selectedCount: Number(selection.selected?.length || 0),
    opportunitiesFound: opportunities.length,
    newOpportunitiesAdded: Number(persisted.newCount || 0),
    diagnostics: mergeDiagnostics({
      job: buildOpportunityJobExecutionDiagnostics({
        selection,
        evaluations: evaluationSummary,
        eligibleRows: opportunities.length,
        persisted
      }),
      selection,
      compare: compared.diagnostics,
      evaluations: evaluationSummary,
      persisted,
      sourceCatalog: {
        mode: catalogLoad?.diagnostics?.sourceMode || "persisted_cohorts",
        scannerSourceSize: catalogRows.length,
        catalogLoad: {
          ...(catalogLoad?.diagnostics || {}),
          ...sourceSelectionDiagnostics
        }
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
        scanAllowLiveFetch: false,
        hardTimeoutMs: OPPORTUNITY_HARD_TIMEOUT_MS
      }
    })
  }
}

function formatRunResult(input = {}) {
  return {
    jobType: input.jobType || input.job_type || null,
    job_type: input.job_type || input.jobType || null,
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
  timeoutMs,
  hardTimeoutMs = null,
  trigger,
  forceRefresh,
  worker
}) {
  const safeScannerType = normalizeText(scannerType) || DEFAULT_RUNTIME_SCANNER_TYPE
  const safeTimeoutMs = Math.max(Math.round(Number(timeoutMs || 0)), 1000)
  const safeHardTimeoutMs = Math.max(Math.round(Number(hardTimeoutMs || 0)), 0)
  if (state.inFlight) {
    return formatRunResult({
      jobType: safeScannerType,
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
    startedAt,
    diagnosticsSummary: buildJobExecutionDiagnostics({
      jobType: safeScannerType
    })
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
        startedAt,
        diagnosticsSummary: buildJobExecutionDiagnostics({
          jobType: safeScannerType
        })
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
        diagnosticsSummary: {
          trigger,
          reason: "already_running",
          ...buildJobExecutionDiagnostics({
            jobType: safeScannerType
          })
        }
      }).catch(() => null)
    }
    return formatRunResult({
      jobType: safeScannerType,
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
      const result = await withTimeout(
        worker({ forceRefresh, scanRunId: runId }),
        safeTimeoutMs,
        "SCANNER_JOB_TIMEOUT"
      )
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
          ...buildJobExecutionDiagnostics({
            jobType: safeScannerType
          }),
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
          elapsedMs,
          ...buildJobExecutionDiagnostics({
            jobType: safeScannerType
          })
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
    jobType: safeScannerType,
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
    timeoutMs: OPPORTUNITY_HARD_TIMEOUT_MS,
    hardTimeoutMs: OPPORTUNITY_HARD_TIMEOUT_MS,
    trigger: normalizeText(options.trigger || "system"),
    forceRefresh: false,
    worker: runOpportunityJob
  })
}

async function enqueueEnrichment(options = {}) {
  return runJobWithLock({
    scannerType: SCANNER_TYPES.ENRICHMENT,
    state: enrichmentState,
    timeoutMs: ENRICHMENT_JOB_TIMEOUT_MS,
    hardTimeoutMs: ENRICHMENT_JOB_TIMEOUT_MS,
    trigger: normalizeText(options.trigger || "system"),
    forceRefresh: Boolean(options.forceRefresh),
    worker: runEnrichmentJob
  })
}

exports.__runtime = {
  enqueueScan,
  enqueueEnrichment
}

exports.__testables = {
  normalizeCategoryFilter,
  classifyCatalogState,
  compareCandidates,
  buildRoundRobinPool,
  buildJobExecutionDiagnostics,
  buildEnrichmentJobExecutionDiagnostics,
  buildOpportunityJobExecutionDiagnostics,
  selectScanCandidates,
  evaluateCandidateOpportunity,
  summarizeEvaluations,
  buildOpportunityFingerprint,
  buildMaterialChangeHash,
  classifyOpportunityFeedEvent,
  isMateriallyNewOpportunity,
  buildFeedInsertRow,
  mapFeedRowToApiRow,
  mapFeedRowToCard,
  dedupeFeedCards,
  countRowsByScannerCategory,
  countScannableRowsByScannerCategory,
  resolveScannerFamilyKey,
  rebalanceSelectionForFeedDiversity,
  loadScannerSourceRows,
  persistFeedRows,
  mergeDiagnostics,
  buildConsolidatedDiagnosticsSummary,
  buildFeedUpdatePatch,
  normalizeCursorPayload,
  encodeCursorPayload,
  buildFeedPageCacheKey,
  clearFeedFirstPageCache,
  confidenceLevel,
  clampScore,
  formatRunResult,
  isScannerRunOverdue,
  runJobWithLock,
  DEFAULT_UNIVERSE_LIMIT,
  OPPORTUNITY_BATCH_TARGET,
  SCAN_CHUNK_SIZE,
  SCAN_TIMEOUT_PER_BATCH_MS,
  SCAN_STATE
}


const AppError = require("../utils/AppError")
const catalogGenerationRepo = require("../repositories/catalogGenerationRepository")
const globalActiveOpportunityRepo = require("../repositories/globalActiveOpportunityRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const scannerRuntimeService = require("./arbitrageScannerService")
const catalogGenerationService = require("./catalogGenerationService")
const planService = require("./planService")
const premiumCategoryAccessService = require("./premiumCategoryAccessService")
const opportunityInsightService = require("./opportunityInsightService")
const feedRevalidationService = require("./feed/feedRevalidationService")
const {
  SCANNER_TYPES,
  ITEM_CATEGORIES,
  ENRICHMENT_INTERVAL_MS,
  ENRICHMENT_INTERVAL_MINUTES,
  OPPORTUNITY_SCAN_INTERVAL_MS,
  OPPORTUNITY_SCAN_INTERVAL_MINUTES
} = require("./scanner/config")
const { mapFeedRowToApiRow } = require("./scanner/feedPipeline")

const FEED_PAGE_SIZE = 200
const DEFAULT_API_LIMIT = FEED_PAGE_SIZE
const MAX_API_LIMIT = 200
const FEED_WINDOW_HOURS = 24
const MAX_HISTORY_WINDOW_HOURS = 168
const FEED_CURSOR_DELIMITER = "|"
const MANUAL_REFRESH_TRACKER_MAX = 4000

const schedulerState = {
  opportunityTimer: null,
  enrichmentTimer: null,
  feedRevalidationStarted: false,
  nextOpportunityScheduledAt: null,
  nextEnrichmentScheduledAt: null
}

const manualRefreshTracker = new Map()

function normalizeText(value) {
  return String(value || "").trim()
}

function buildCatalogGenerationSnapshot(generation = {}) {
  if (!generation || typeof generation !== "object") return null
  const id = normalizeText(generation?.id)
  if (!id) return null
  return {
    id,
    generationKey: normalizeText(generation?.generation_key || generation?.generationKey) || null,
    status: normalizeText(generation?.status).toLowerCase() || null,
    isActive: Boolean(generation?.is_active ?? generation?.isActive),
    opportunityScanEnabled: Boolean(
      generation?.opportunity_scan_enabled ?? generation?.opportunityScanEnabled
    ),
    activatedAt: toIsoOrNull(generation?.activated_at || generation?.activatedAt),
    archivedAt: toIsoOrNull(generation?.archived_at || generation?.archivedAt),
    sourceGenerationId:
      normalizeText(generation?.source_generation_id || generation?.sourceGenerationId) || null
  }
}

function buildOpportunityScanBlockedResult(generation = null, gate = null) {
  const diagnostics = gate?.diagnostics && typeof gate.diagnostics === "object" ? gate.diagnostics : {}
  return {
    jobType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    scanRunId: null,
    status: "blocked_generation_not_ready",
    alreadyRunning: false,
    startedAt: new Date().toISOString(),
    catalogGeneration: buildCatalogGenerationSnapshot(generation),
    reason: normalizeText(gate?.reason) || "catalog_generation_scan_disabled",
    blocked_by_generation_flag: Boolean(diagnostics.blocked_by_generation_flag),
    blocked_by_readiness_gate: Boolean(diagnostics.blocked_by_readiness_gate),
    blocked_by_empty_scanner_source: Boolean(diagnostics.blocked_by_empty_scanner_source),
    readinessSource: normalizeText(diagnostics.readiness_source) || "not_ready",
    weaponSkinReadinessSource:
      normalizeText(diagnostics.weapon_skin_readiness_source) || "not_ready",
    autoEnabledGenerationFlag: Boolean(gate?.autoEnabled),
    readiness: gate?.readiness || null
  }
}

async function enqueueOpportunityScanIfReady(runtime, trigger = "system") {
  const gate = await catalogGenerationService.ensureOpportunityScanEnabledForActiveGeneration({
    autoEnable: true
  })
  if (!gate?.allowed) {
    return buildOpportunityScanBlockedResult(gate?.catalogGeneration, gate)
  }
  const enqueueResult = await runtime.enqueueScan({ trigger })
  return {
    ...(enqueueResult && typeof enqueueResult === "object" ? enqueueResult : {}),
    catalogGeneration: buildCatalogGenerationSnapshot(gate?.catalogGeneration),
    blocked_by_generation_flag: false,
    blocked_by_readiness_gate: false,
    blocked_by_empty_scanner_source: false,
    readinessSource: normalizeText(gate?.diagnostics?.readiness_source) || "generation_flag",
    weaponSkinReadinessSource:
      normalizeText(gate?.diagnostics?.weapon_skin_readiness_source) || "generation_flag",
    autoEnabledGenerationFlag: Boolean(gate?.autoEnabled),
    readiness: gate?.readiness || null
  }
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

function normalizeHistoryHours(value, fallback = FEED_WINDOW_HOURS) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_HISTORY_WINDOW_HOURS)
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

function isStatementTimeoutError(err) {
  const code = normalizeText(err?.code)
  const message = normalizeText(err?.message).toLowerCase()
  return (
    code === "57014" ||
    message.includes("statement timeout") ||
    message.includes("canceling statement due to statement timeout")
  )
}

function buildSinceIso(hours, nowMs = Date.now()) {
  const safeHours = normalizeHistoryHours(hours, FEED_WINDOW_HOURS)
  return new Date(nowMs - safeHours * 60 * 60 * 1000).toISOString()
}

function normalizeCategoryFilter(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw || raw === "all") return "all"
  if (raw === "skins" || raw === "skin" || raw === ITEM_CATEGORIES.WEAPON_SKIN) {
    return ITEM_CATEGORIES.WEAPON_SKIN
  }
  if (raw === "cases" || raw === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (raw === "capsules" || raw === "capsule" || raw === ITEM_CATEGORIES.STICKER_CAPSULE) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === "knives" || raw === "knife" || raw === "future_knife") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === "gloves" || raw === "glove" || raw === "future_glove") {
    return ITEM_CATEGORIES.GLOVE
  }
  return "all"
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

async function countFeedSafely(options = {}) {
  try {
    return {
      count: await globalActiveOpportunityRepo.countFeed(options),
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

function applyFeedPlanRestrictions(rows = [], entitlements = {}) {
  const nowMs = Date.now()
  const delayedSignals = Boolean(entitlements?.delayedSignals)
  const signalDelayMinutes = Math.max(Number(entitlements?.signalDelayMinutes || 0), 0)
  const visibleFeedLimit = Math.max(Number(entitlements?.visibleFeedLimit || MAX_API_LIMIT), 1)
  let filtered = Array.isArray(rows) ? [...rows] : []
  let delayedCount = 0

  if (delayedSignals && signalDelayMinutes > 0) {
    const cutoffMs = nowMs - signalDelayMinutes * 60 * 1000
    filtered = filtered.filter((row) => {
      const detectedAt = toIsoOrNull(row?.detectedAt)
      if (!detectedAt) return false
      const visible = new Date(detectedAt).getTime() <= cutoffMs
      if (!visible) delayedCount += 1
      return visible
    })
  }

  const premiumLock = premiumCategoryAccessService.applyPremiumPreviewLock(filtered, entitlements)
  return {
    rows: premiumLock.rows,
    planLimits: {
      delayedSignals,
      signalDelayMinutes,
      feedTruncatedByDelay: delayedCount,
      hiddenByLimit: 0,
      visibleFeedLimit,
      lockedPremiumPreviewRows: Number(premiumLock.lockedCount || 0)
    }
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
  const threshold =
    OPPORTUNITY_SCAN_INTERVAL_MS +
    Math.max(Math.round(OPPORTUNITY_SCAN_INTERVAL_MS * 0.2), 15000)
  return elapsed > threshold
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

function updateNextScheduledAt(scannerType) {
  const nowMs = Date.now()
  if (scannerType === SCANNER_TYPES.ENRICHMENT) {
    schedulerState.nextEnrichmentScheduledAt = new Date(nowMs + ENRICHMENT_INTERVAL_MS).toISOString()
    return
  }
  schedulerState.nextOpportunityScheduledAt = new Date(
    nowMs + OPPORTUNITY_SCAN_INTERVAL_MS
  ).toISOString()
}

function getRuntimeBridge() {
  const runtime = scannerRuntimeService.__runtime
  if (
    !runtime ||
    typeof runtime.enqueueScan !== "function" ||
    typeof runtime.enqueueEnrichment !== "function"
  ) {
    const err = new Error("scanner_v2_runtime_bridge_unavailable")
    err.code = "SCANNER_V2_ERROR"
    throw err
  }
  return runtime
}

async function getStatusInternal(options = {}) {
  const includeActiveCount = options.includeActiveCount !== false
  const [
    currentGeneration,
    latestRun,
    latestCompletedRun,
    latestEnrichmentRun,
    latestEnrichmentCompletedRun,
    activeOpportunities
  ] = await Promise.all([
    catalogGenerationRepo.getCurrentGeneration().catch(() => null),
    scannerRunRepo.getLatestRun(SCANNER_TYPES.OPPORTUNITY_SCAN),
    scannerRunRepo.getLatestCompletedRun(SCANNER_TYPES.OPPORTUNITY_SCAN),
    scannerRunRepo.getLatestRun(SCANNER_TYPES.ENRICHMENT),
    scannerRunRepo.getLatestCompletedRun(SCANNER_TYPES.ENRICHMENT),
    includeActiveCount
      ? globalActiveOpportunityRepo.countFeed({ includeInactive: false }).catch(() => 0)
      : Promise.resolve(null)
  ])

  const opportunityGate =
    currentGeneration?.opportunity_scan_enabled ?? currentGeneration?.opportunityScanEnabled
      ? {
          allowed: true,
          autoEnabled: false,
          catalogGeneration: currentGeneration,
          readiness: null,
          diagnostics: {
            blocked_by_generation_flag: false,
            blocked_by_readiness_gate: false,
            blocked_by_empty_scanner_source: false,
            readiness_source: "generation_flag",
            weapon_skin_readiness_source: "generation_flag",
            auto_enabled: false
          }
        }
      : await catalogGenerationService.ensureOpportunityScanEnabledForActiveGeneration({
          autoEnable: false,
          generation: currentGeneration
        })

  const opportunityRunning = normalizeText(latestRun?.status).toLowerCase() === "running"
  const enrichmentRunning =
    normalizeText(latestEnrichmentRun?.status).toLowerCase() === "running"
  const catalogGeneration = buildCatalogGenerationSnapshot(currentGeneration)
  const opportunityStatus = opportunityRunning
    ? "running"
    : catalogGeneration?.opportunityScanEnabled
      ? "idle"
      : "blocked_generation_not_ready"

  return {
    schedulerRunning: Boolean(
      schedulerState.opportunityTimer ||
        schedulerState.enrichmentTimer ||
        schedulerState.feedRevalidationStarted
    ),
    currentStatus: opportunityRunning
      ? "running"
      : enrichmentRunning && !catalogGeneration?.opportunityScanEnabled
        ? "enrichment_only"
        : opportunityStatus === "blocked_generation_not_ready"
          ? "enrichment_only"
          : "idle",
    currentRunId: opportunityRunning ? latestRun?.id || null : null,
    currentRunStartedAt: opportunityRunning ? latestRun?.started_at || null : null,
    currentRunElapsedMs:
      opportunityRunning && toIsoOrNull(latestRun?.started_at)
        ? Date.now() - new Date(latestRun.started_at).getTime()
        : null,
    nextScheduledAt: schedulerState.nextOpportunityScheduledAt,
    activeOpportunities:
      activeOpportunities == null ? null : Number(activeOpportunities || 0),
    catalogGeneration,
    latestRun: latestRun || null,
    latestCompletedRun: latestCompletedRun || null,
    coordination: {
      allowCrossJobParallelism: true,
      singleActiveRunPerJobType: true,
      overdue: isScannerRunOverdue({ latestRun, latestCompletedRun })
    },
    jobs: {
      [SCANNER_TYPES.OPPORTUNITY_SCAN]: {
        scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
        intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
        nextScheduledAt: schedulerState.nextOpportunityScheduledAt,
        status: opportunityStatus,
        catalogGeneration,
        blocked_by_generation_flag: Boolean(
          opportunityGate?.diagnostics?.blocked_by_generation_flag
        ),
        blocked_by_readiness_gate: Boolean(
          opportunityGate?.diagnostics?.blocked_by_readiness_gate
        ),
        blocked_by_empty_scanner_source: Boolean(
          opportunityGate?.diagnostics?.blocked_by_empty_scanner_source
        ),
        readinessSource:
          normalizeText(opportunityGate?.diagnostics?.readiness_source) || "generation_flag",
        weaponSkinReadinessSource:
          normalizeText(opportunityGate?.diagnostics?.weapon_skin_readiness_source) ||
          "generation_flag",
        autoEnabledGenerationFlag: Boolean(opportunityGate?.autoEnabled),
        readiness: opportunityGate?.readiness || null,
        latestRun: latestRun || null,
        latestCompletedRun: latestCompletedRun || null
      },
      [SCANNER_TYPES.ENRICHMENT]: {
        scannerType: SCANNER_TYPES.ENRICHMENT,
        intervalMinutes: ENRICHMENT_INTERVAL_MINUTES,
        nextScheduledAt: schedulerState.nextEnrichmentScheduledAt,
        status: enrichmentRunning ? "running" : "idle",
        latestRun: latestEnrichmentRun || null,
        latestCompletedRun: latestEnrichmentCompletedRun || null
      }
    }
  }
}

async function getFeed(options = {}) {
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

  const statusPromise = getStatusInternal({ includeActiveCount: false })
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

  const rawRows = await globalActiveOpportunityRepo.listFeedByCursor(feedQuery)
  const hasExtraRow = rawRows.length > limit
  const rows = hasExtraRow ? rawRows.slice(0, limit) : rawRows
  const lastRow = rows.length ? rows[rows.length - 1] : null

  const [status, countResult] = await Promise.all([statusPromise, countPromise])

  const mappedRows = rows.map((row) => mapFeedRowToApiRow(row))
  const restricted = applyFeedPlanRestrictions(mappedRows, entitlements)
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
    nextCursor: hasExtraRow
      ? encodeCursorPayload(lastRow?.last_published_at || lastRow?.last_seen_at, lastRow?.id)
      : null,
    hasPrevPage: Boolean(currentCursor),
    hasNextPage: Boolean(hasExtraRow),
    historyHours: historyWindowHours,
    returnedCount: restricted.rows.length,
    totalCount,
    countMode: includeCount ? "exact" : "skipped",
    totalCountTimedOut: Boolean(includeCount && countResult?.timedOut)
  }

  const summary = {
    scannedItems: Number(latestCompleted?.items_scanned || 0),
    opportunities: restricted.rows.length,
    totalDetected: totalCount,
    activeOpportunities: Number(
      status?.activeOpportunities ?? totalCount ?? restricted.rows.length
    ),
    feedRetentionHours: FEED_WINDOW_HOURS,
    historyWindowHours,
    noOpportunitiesReason: null,
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
    opportunities: restricted.rows,
    plan: summary.plan,
    status: {
      scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
      intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
      schedulerRunning: Boolean(status?.schedulerRunning),
      currentStatus: status?.currentStatus || "idle",
      currentRunId: status?.currentRunId || null,
      nextScheduledAt: status?.nextScheduledAt || null,
      activeOpportunities: Number(
        status?.activeOpportunities ?? totalCount ?? restricted.rows.length
      ),
      latestRun: toStatusRunSnapshot(status?.latestRun),
      latestCompletedRun: toStatusRunSnapshot(latestCompleted),
      catalogGeneration: status?.catalogGeneration || null
    }
  }
}

async function getTopOpportunities(options = {}) {
  return getFeed(options)
}

async function getOpportunityInsight(opportunityId, options = {}) {
  return opportunityInsightService.getOpportunityInsight(opportunityId, options)
}

async function getStatus() {
  const status = await getStatusInternal()
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
    catalogGeneration: status?.catalogGeneration || null,
    latestRun: status?.latestRun || null,
    latestCompletedRun: status?.latestCompletedRun || null,
    coordination: status?.coordination || {},
    jobs: status?.jobs || {}
  }
}

async function triggerRefresh(options = {}) {
  const planContext = await resolvePlanContext(options)
  enforceManualRefreshCooldown(planContext?.userId, planContext?.entitlements, Date.now())

  const runtime = getRuntimeBridge()
  const forceRefresh = options.forceRefresh == null ? true : normalizeBoolean(options.forceRefresh)
  const requestedJobType = normalizeText(options.jobType).toLowerCase()
  const trigger = normalizeText(options.trigger || "manual")
  const runOpportunity = !requestedJobType || requestedJobType === SCANNER_TYPES.OPPORTUNITY_SCAN
  const runEnrichment = !requestedJobType || requestedJobType === SCANNER_TYPES.ENRICHMENT

  const [opportunity, enrichment] = await Promise.all([
    runOpportunity ? enqueueOpportunityScanIfReady(runtime, trigger) : Promise.resolve(null),
    runEnrichment
      ? runtime.enqueueEnrichment({ forceRefresh, trigger })
      : Promise.resolve(null)
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
      allowCrossJobParallelism: true,
      singleActiveRunPerJobType: true
    }
  }
}

function startScheduler() {
  const runtime = getRuntimeBridge()

  if (!schedulerState.enrichmentTimer) {
    runtime.enqueueEnrichment({ forceRefresh: false, trigger: "startup_enrichment" }).catch((err) => {
      console.error("[scanner-v2] Initial enrichment enqueue failed", err.message)
    })
    updateNextScheduledAt(SCANNER_TYPES.ENRICHMENT)
    schedulerState.enrichmentTimer = setInterval(() => {
      runtime.enqueueEnrichment({ forceRefresh: false, trigger: "scheduled_enrichment" }).catch((err) => {
        console.error("[scanner-v2] Scheduled enrichment enqueue failed", err.message)
      })
      updateNextScheduledAt(SCANNER_TYPES.ENRICHMENT)
    }, ENRICHMENT_INTERVAL_MS)
    schedulerState.enrichmentTimer.unref?.()
  }

  if (!schedulerState.opportunityTimer) {
    enqueueOpportunityScanIfReady(runtime, "startup_opportunity_scan").catch((err) => {
      console.error("[scanner-v2] Initial opportunity scan enqueue failed", err.message)
    })
    updateNextScheduledAt(SCANNER_TYPES.OPPORTUNITY_SCAN)
    schedulerState.opportunityTimer = setInterval(() => {
      enqueueOpportunityScanIfReady(runtime, "scheduled_opportunity_scan").catch((err) => {
        console.error("[scanner-v2] Scheduled opportunity scan enqueue failed", err.message)
      })
      updateNextScheduledAt(SCANNER_TYPES.OPPORTUNITY_SCAN)
    }, OPPORTUNITY_SCAN_INTERVAL_MS)
    schedulerState.opportunityTimer.unref?.()
  }

  feedRevalidationService.startScheduler()
  schedulerState.feedRevalidationStarted = true

  return {
    engine: "scanner_v2",
    feedRevalidationStarted: schedulerState.feedRevalidationStarted
  }
}

function stopScheduler() {
  if (schedulerState.opportunityTimer) {
    clearInterval(schedulerState.opportunityTimer)
    schedulerState.opportunityTimer = null
    schedulerState.nextOpportunityScheduledAt = null
  }
  if (schedulerState.enrichmentTimer) {
    clearInterval(schedulerState.enrichmentTimer)
    schedulerState.enrichmentTimer = null
    schedulerState.nextEnrichmentScheduledAt = null
  }

  feedRevalidationService.stopScheduler()
  schedulerState.feedRevalidationStarted = false

  return {
    engine: "scanner_v2",
    feedRevalidationStarted: schedulerState.feedRevalidationStarted
  }
}

module.exports = {
  getFeed,
  getTopOpportunities,
  getOpportunityInsight,
  getStatus,
  triggerRefresh,
  startScheduler,
  stopScheduler,
  __testables: {
    normalizeCursorPayload,
    encodeCursorPayload,
    normalizeCategoryFilter,
    resolvePlanContext,
    applyFeedPlanRestrictions,
    toStatusRunSnapshot,
    isScannerRunOverdue,
    buildCatalogGenerationSnapshot,
    buildOpportunityScanBlockedResult,
    enqueueOpportunityScanIfReady
  }
}

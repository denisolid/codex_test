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
  SOURCE_STATES,
  readMarketHealth,
  buildMarketHealthDiagnostics,
  normalizeSourceState
} = require("../markets/marketSourceDiagnostics")
const {
  arbitrageDefaultUniverseLimit,
  arbitrageMaxConcurrentMarketRequests,
  marketCompareCacheTtlMinutes,
  marketCompareTimeoutMs,
  marketCompareMaxRetries,
  marketPriceRateLimitPerSecond
} = require("../config/env")
const {
  buildScannerMarketPolicyDiagnostics,
  getScannerCoverageMarkets,
  getScannerMarketPolicy,
  isScannerMarketDisabled,
  shouldUseFreshCacheOnRateLimit
} = require("./scanner/marketReliabilityPolicy")

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
const DEFAULT_WEAPON_SKIN_VERIFICATION_LIMIT = 12
const DEFAULT_WEAPON_SKIN_VERIFICATION_POOL_MULTIPLIER = 8
const DEFAULT_WEAPON_SKIN_VERIFICATION_MIN_POOL = 90
const DEFAULT_WEAPON_SKIN_FALLBACK_PROBE_LIMIT = 2
const DEFAULT_WEAPON_SKIN_VERIFICATION_RETRY_BUDGET = 2
const DEFAULT_WEAPON_SKIN_VERIFICATION_COOLDOWN_MS = 30 * 60 * 1000
const DEFAULT_HEALTH_WINDOW_HOURS = 2
const DEFAULT_QUOTE_LOOKBACK_HOURS = 24 * 14
const DEFAULT_REFRESH_CONCURRENCY = Math.max(Number(arbitrageMaxConcurrentMarketRequests || 4), 1)
const DEFAULT_STEAM_SCANNER_LIVE_REQUEST_BUDGET = Math.max(
  Math.min(Number(marketPriceRateLimitPerSecond || 2), 2),
  1
)
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
const WEAPON_SKIN_PRIORITY_TIER_WEIGHTS = Object.freeze({
  tier_a: 20,
  tier_b: 12
})
const WEAPON_SKIN_VERIFICATION_PRIORITY_BUCKETS = Object.freeze({
  highest: 135,
  high: 110,
  medium: 85
})
const STEAM_SCANNER_STATUS = Object.freeze({
  RATE_LIMITED: "steam_rate_limited",
  CACHED_FALLBACK: "steam_cached_fallback",
  UNAVAILABLE: "steam_unavailable"
})
const STEAM_SCANNER_BUDGET_EXHAUSTED_REASON =
  "Steam scanner live budget exhausted. Fresh cache unavailable."
const WEAPON_SKIN_STRUCTURAL_BLOCK_REASON_PATTERN =
  /(invalid|unusable|anti[_\s-]?fake|below[_\s-]?min|impossible|unsupported|not[_\s-]?tradable|structural|economics)/i

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

function isFreshMarketPriceCacheRow(row = {}, ttlMinutes = marketCompareCacheTtlMinutes, nowMs = Date.now()) {
  const fetchedAt = toIsoOrNull(row?.fetched_at || row?.fetchedAt)
  if (!fetchedAt) return false
  const fetchedAtMs = new Date(fetchedAt).getTime()
  if (!Number.isFinite(fetchedAtMs)) return false
  const ageMs = Number(nowMs || Date.now()) - fetchedAtMs
  if (!Number.isFinite(ageMs) || ageMs < 0) return false
  return ageMs <= Math.max(Number(ttlMinutes || 0), 1) * 60 * 1000
}

function isRateLimitReason(value = "") {
  const text = normalizeText(value).toLowerCase()
  if (!text) return false
  return text.includes("rate limit") || text.includes("too many requests") || text.includes("429")
}

function incrementFailureReasonCount(target = {}, source = "", reason = "", amount = 1) {
  const safeSource = normalizeText(source).toLowerCase()
  const safeReason = normalizeText(reason)
  const safeAmount = Math.max(Number(amount || 0), 0)
  if (!safeSource || !safeReason || !safeAmount) return
  const key = `${safeSource}:${safeReason}`
  target[key] = Number(target[key] || 0) + safeAmount
}

function incrementQuoteSourceStateCount(sourceDiagnostics = {}, state = "", amount = 1) {
  const safeState = normalizeSourceState(state) || normalizeText(state).toLowerCase()
  const safeAmount = Math.max(Number(amount || 0), 0)
  if (!safeState || !safeAmount) return
  sourceDiagnostics.stateCounts = sourceDiagnostics.stateCounts || {}
  sourceDiagnostics.stateCounts[safeState] =
    Number(sourceDiagnostics.stateCounts?.[safeState] || 0) + safeAmount
}

function incrementQuoteSourceScannerStatusCount(sourceDiagnostics = {}, status = "", amount = 1) {
  const safeStatus = normalizeText(status)
  const safeAmount = Math.max(Number(amount || 0), 0)
  if (!safeStatus || !safeAmount) return
  sourceDiagnostics.scanner_status_counts = sourceDiagnostics.scanner_status_counts || {}
  sourceDiagnostics.scanner_status_counts[safeStatus] =
    Number(sourceDiagnostics.scanner_status_counts?.[safeStatus] || 0) + safeAmount
}

function recordQuoteRowOutcome(counters = {}, outcome = {}) {
  if (!counters?.quoteRowOutcomesByKey || typeof counters.quoteRowOutcomesByKey !== "object") {
    return
  }
  const source = normalizeText(outcome?.source).toLowerCase()
  const marketHashName = normalizeText(outcome?.marketHashName)
  if (!source || !marketHashName) return
  const key = `${source}::${marketHashName}`
  counters.quoteRowOutcomesByKey[key] = {
    source,
    marketHashName,
    available: Boolean(outcome?.available),
    sourceState:
      normalizeSourceState(outcome?.sourceState) ||
      (Boolean(outcome?.available) ? SOURCE_STATES.OK : SOURCE_STATES.NO_DATA),
    reason: normalizeText(outcome?.reason) || null,
    scannerStatus: normalizeText(outcome?.scannerStatus) || null,
    usedFreshCache: Boolean(outcome?.usedFreshCache),
    requestSent:
      outcome?.requestSent == null ? null : Boolean(outcome.requestSent)
  }
}

function buildRecordFromCachedMarketPriceRow(row = {}, source = "", fetchedAtIso = null) {
  const market = normalizeText(source || row?.market || row?.source).toLowerCase()
  const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
  const grossPrice = toFiniteOrNull(row?.gross_price ?? row?.grossPrice)
  const netPriceAfterFees = toFiniteOrNull(row?.net_price ?? row?.netPriceAfterFees)
  if (!market || !marketHashName || grossPrice == null || netPriceAfterFees == null) {
    return null
  }
  const raw = row?.raw && typeof row.raw === "object" ? row.raw : {}
  const cachedFallbackRaw = {
    ...raw,
    scanner_cached_fallback_used: true
  }
  if (market === "steam") {
    cachedFallbackRaw.steam_scanner_status = STEAM_SCANNER_STATUS.CACHED_FALLBACK
  }
  return {
    source: market,
    marketHashName,
    grossPrice,
    netPriceAfterFees,
    currency: normalizeText(row?.currency).toUpperCase() || "USD",
    url: normalizeText(row?.url) || null,
    updatedAt:
      toIsoOrNull(raw?.source_updated_at || raw?.updated_at || raw?.updatedAt) ||
      toIsoOrNull(row?.fetched_at || row?.fetchedAt) ||
      toIsoOrNull(fetchedAtIso) ||
      new Date().toISOString(),
    confidence: normalizeText(raw?.confidence).toLowerCase() || "medium",
    raw: cachedFallbackRaw
  }
}

function withSourceSpecificScannerStatus(record = {}, source = "", scannerStatus = null) {
  const safeSource = normalizeText(source || record?.source).toLowerCase()
  const safeStatus = normalizeText(scannerStatus)
  if (!record || typeof record !== "object" || !safeSource || !safeStatus) {
    return record
  }
  return {
    ...record,
    raw: {
      ...(record?.raw && typeof record.raw === "object" ? record.raw : {}),
      [`${safeSource}_scanner_status`]: safeStatus
    }
  }
}

function getSteamScannerLiveRequestBudget(options = {}) {
  const explicitBudget = toIntegerOrNull(
    options?.steamLiveRequestBudget ?? options?.scannerSteamLiveRequestBudget,
    1
  )
  if (explicitBudget != null) {
    return explicitBudget
  }
  return DEFAULT_STEAM_SCANNER_LIVE_REQUEST_BUDGET
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

function createPriorityBucketMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return {
    highest: initial,
    high: initial,
    medium: initial,
    low: initial
  }
}

function createQueuedRowsByStateMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return {
    eligible: initial,
    near_eligible: initial,
    enriching: initial,
    candidate: initial
  }
}

function normalizeWeaponSkinCandidateStatus(value = "") {
  const status = normalizeText(value).toLowerCase()
  if (["eligible", "near_eligible", "enriching", "candidate"].includes(status)) {
    return status
  }
  return "candidate"
}

function createWeaponSkinVerificationState(state = {}) {
  return {
    byMarketHashName:
      state?.byMarketHashName && typeof state.byMarketHashName === "object"
        ? { ...state.byMarketHashName }
        : {}
  }
}

function getWeaponSkinVerificationDefaults(options = {}) {
  return {
    verificationLimit: Math.max(
      Number(options.weaponSkinVerificationLimit || DEFAULT_WEAPON_SKIN_VERIFICATION_LIMIT),
      1
    ),
    fallbackProbeLimit: Math.max(
      Math.min(
        Number(
          options.weaponSkinFallbackProbeLimit || DEFAULT_WEAPON_SKIN_FALLBACK_PROBE_LIMIT
        ),
        3
      ),
      1
    ),
    retryBudget: Math.max(
      Number(options.weaponSkinVerificationRetryBudget || DEFAULT_WEAPON_SKIN_VERIFICATION_RETRY_BUDGET),
      1
    ),
    cooldownMs: Math.max(
      Number(options.weaponSkinVerificationCooldownMs || DEFAULT_WEAPON_SKIN_VERIFICATION_COOLDOWN_MS),
      1000
    ),
    candidatePoolLimit: Math.max(
      Number(
        options.weaponSkinVerificationCandidatePoolLimit ||
          Math.max(
            DEFAULT_WEAPON_SKIN_VERIFICATION_MIN_POOL,
            Number(options.limit || DEFAULT_SELECTION_BATCH_SIZE) *
              DEFAULT_WEAPON_SKIN_VERIFICATION_POOL_MULTIPLIER
          )
      ),
      1
    )
  }
}

function resolveWeaponSkinVerificationCatalogGate(row = {}) {
  const catalogStatus = normalizeText(
    row?.catalog_status ?? row?.catalogStatus
  ).toLowerCase()
  const catalogBlockReason = normalizeText(
    row?.catalog_block_reason ?? row?.catalogBlockReason
  ).toLowerCase()
  const candidateStatus = normalizeWeaponSkinCandidateStatus(
    row?.candidate_status ?? row?.candidateStatus
  )
  const structurallyInvalid =
    candidateStatus === "rejected" ||
    catalogStatus === "blocked" ||
    (catalogStatus === "shadow" &&
      catalogBlockReason &&
      WEAPON_SKIN_STRUCTURAL_BLOCK_REASON_PATTERN.test(catalogBlockReason))

  return {
    catalogStatus,
    catalogBlockReason: catalogBlockReason || null,
    structurallyInvalid
  }
}

function getWeaponSkinVerificationRowState(
  weaponSkinVerificationState = {},
  marketHashName = "",
  options = {}
) {
  if (
    !weaponSkinVerificationState.byMarketHashName ||
    typeof weaponSkinVerificationState.byMarketHashName !== "object"
  ) {
    weaponSkinVerificationState.byMarketHashName = {}
  }
  const name = normalizeText(marketHashName)
  if (!name) {
    return {
      retriesRemaining: getWeaponSkinVerificationDefaults(options).retryBudget,
      retryBudget: getWeaponSkinVerificationDefaults(options).retryBudget,
      retryBudgetExhausted: false,
      cooldownUntil: null,
      verifiedFresh: false,
      recomputedAt: null,
      lastAttemptAt: null,
      lastResult: null,
      lastPriorityScore: 0
    }
  }
  const defaults = getWeaponSkinVerificationDefaults(options)
  if (!weaponSkinVerificationState.byMarketHashName[name]) {
    weaponSkinVerificationState.byMarketHashName[name] = {
      retriesRemaining: defaults.retryBudget,
      retryBudget: defaults.retryBudget,
      retryBudgetExhausted: false,
      cooldownUntil: null,
      verifiedFresh: false,
      recomputedAt: null,
      lastAttemptAt: null,
      lastResult: null,
      lastPriorityScore: 0
    }
  }
  const entry = weaponSkinVerificationState.byMarketHashName[name]
  entry.retryBudget = Math.max(Number(entry.retryBudget || defaults.retryBudget), 1)
  if (!Number.isFinite(Number(entry.retriesRemaining))) {
    entry.retriesRemaining = entry.retryBudget
  }
  entry.retriesRemaining = Math.max(Math.round(Number(entry.retriesRemaining || 0)), 0)
  entry.retryBudgetExhausted =
    Boolean(entry.retryBudgetExhausted) || entry.retriesRemaining <= 0
  entry.cooldownUntil = toIsoOrNull(entry.cooldownUntil) || null
  entry.lastAttemptAt = toIsoOrNull(entry.lastAttemptAt) || null
  entry.recomputedAt = toIsoOrNull(entry.recomputedAt) || null
  entry.lastResult = normalizeText(entry.lastResult) || null
  entry.lastPriorityScore = Number(entry.lastPriorityScore || 0)
  entry.verifiedFresh = Boolean(entry.verifiedFresh)
  return entry
}

function isWeaponSkinVerificationCooldownActive(rowState = {}, nowMs = Date.now()) {
  const cooldownUntil = toIsoOrNull(rowState?.cooldownUntil)
  if (!cooldownUntil) return false
  return new Date(cooldownUntil).getTime() > Number(nowMs || Date.now())
}

function resolveWeaponSkinVerificationPriorityBucket(score = 0) {
  const safeScore = Number(score || 0)
  if (safeScore >= WEAPON_SKIN_VERIFICATION_PRIORITY_BUCKETS.highest) return "highest"
  if (safeScore >= WEAPON_SKIN_VERIFICATION_PRIORITY_BUCKETS.high) return "high"
  if (safeScore >= WEAPON_SKIN_VERIFICATION_PRIORITY_BUCKETS.medium) return "medium"
  return "low"
}

function computeWeaponSkinVerificationPriority(row = {}, nowMs = Date.now()) {
  const candidateStatus = normalizeWeaponSkinCandidateStatus(
    row?.candidate_status ?? row?.candidateStatus
  )
  const marketCoverageCount = Math.max(
    Number((row?.market_coverage_count ?? row?.marketCoverageCount) || 0),
    0
  )
  const liquidityRank = Math.max(Number((row?.liquidity_rank ?? row?.liquidityRank) || 0), 0)
  const priorityTier = normalizeText(row?.priority_tier || row?.priorityTier).toLowerCase() || null
  const priorityBoost = Math.max(Number((row?.priority_boost ?? row?.priorityBoost) || 0), 0)
  const quoteFresh = isFreshWithinHours(row?.quote_fetched_at || row?.quoteFetchedAt, 2, nowMs)
  const signalFresh = isFreshWithinHours(
    row?.last_market_signal_at || row?.lastMarketSignalAt,
    6,
    nowMs
  )
  const opportunityPotential = Math.max(
    Number(row?.opportunity_score ?? row?.opportunityScore ?? row?.catalog_quality_score ?? row?.catalogQualityScore ?? 0),
    0
  )
  const candidateWeight =
    candidateStatus === "eligible"
      ? 90
      : candidateStatus === "near_eligible"
        ? 72
        : candidateStatus === "enriching"
          ? 38
          : 24
  const quoteWeight = quoteFresh ? 28 : 0
  const coverageWeight = Math.min(marketCoverageCount * 8, 24)
  const liquidityWeight = Math.min(liquidityRank * 0.3, 20)
  const tierWeight = WEAPON_SKIN_PRIORITY_TIER_WEIGHTS[priorityTier] || 0
  const boostWeight = Math.min(priorityBoost * 0.25, 22)
  const signalWeight = signalFresh ? 10 : 0
  const opportunityWeight = Math.min(opportunityPotential * 0.12, 12)
  const score =
    candidateWeight +
    quoteWeight +
    coverageWeight +
    liquidityWeight +
    tierWeight +
    boostWeight +
    signalWeight +
    opportunityWeight
  const strongQuoteSupport = quoteFresh && marketCoverageCount >= 2
  const minimumQuoteSupport = quoteFresh && marketCoverageCount >= 1
  const highPriorityCandidate =
    candidateStatus === "eligible" ||
    candidateStatus === "near_eligible" ||
    liquidityRank >= 55 ||
    priorityBoost >= 12 ||
    priorityTier === "tier_a" ||
    marketCoverageCount >= 3
  const fallbackQualityCandidate =
    liquidityRank >= 35 ||
    priorityBoost >= 8 ||
    ["tier_a", "tier_b"].includes(priorityTier) ||
    opportunityPotential >= 45
  const coldCandidate = ["candidate", "enriching"].includes(candidateStatus)
  const lowPriorityCold =
    coldCandidate &&
    !(
      liquidityRank >= 55 ||
      priorityBoost >= 12 ||
      priorityTier === "tier_a" ||
      marketCoverageCount >= 2 ||
      opportunityPotential >= 70
    )
  const primaryEligible =
    strongQuoteSupport &&
    (["eligible", "near_eligible"].includes(candidateStatus) || highPriorityCandidate)
  const fallbackEligible =
    minimumQuoteSupport && fallbackQualityCandidate && !lowPriorityCold

  return {
    candidateStatus,
    quoteFresh,
    marketCoverageCount,
    liquidityRank,
    priorityTier,
    priorityBoost,
    opportunityPotential,
    minimumQuoteSupport,
    strongQuoteSupport,
    highPriorityCandidate,
    fallbackQualityCandidate,
    coldCandidate,
    lowPriorityCold,
    primaryEligible,
    fallbackEligible,
    priorityScore: Number(score.toFixed(2)),
    priorityBucket: resolveWeaponSkinVerificationPriorityBucket(score)
  }
}

function createWeaponSkinVerificationAggregate() {
  return {
    verificationQueueSize: 0,
    topQueuedRowsCount: 0,
    primaryQueueSize: 0,
    fallbackProbeQueueSize: 0,
    queuedRowsByState: createQueuedRowsByStateMap(),
    attemptedVerificationRows: 0,
    successfulVerificationRows: 0,
    fallbackProbeRowsAttempted: 0,
    fallbackProbeRowsSuccessful: 0,
    cooledDownRows: 0,
    retryBudgetBlockedRows: 0,
    skippedLowPriorityRows: 0,
    skippedPrimaryTooStrictCount: 0,
    skippedFallbackBelowMinimumQualityCount: 0,
    verificationPriorityBuckets: createPriorityBucketMap(),
    recomputedVerifiedRows: 0,
    fallbackProbeRowsRecomputed: 0,
    verifiedScannableRows: 0,
    fallbackProbeVerifiedScannableRows: 0,
    weaponSkinScannerSourceIncreased: false,
    fallbackProbeWeaponSkinScannerSourceIncreased: false,
    fallbackLaneActivated: false,
    queueEmptyReason: null
  }
}

function mergeWeaponSkinVerificationAggregate(target = createWeaponSkinVerificationAggregate(), source = {}) {
  const next = target
  next.verificationQueueSize += Number(source.verificationQueueSize || 0)
  next.topQueuedRowsCount += Number(source.topQueuedRowsCount || 0)
  next.primaryQueueSize += Number(source.primaryQueueSize || 0)
  next.fallbackProbeQueueSize += Number(source.fallbackProbeQueueSize || 0)
  next.attemptedVerificationRows += Number(source.attemptedVerificationRows || 0)
  next.successfulVerificationRows += Number(source.successfulVerificationRows || 0)
  next.fallbackProbeRowsAttempted += Number(source.fallbackProbeRowsAttempted || 0)
  next.fallbackProbeRowsSuccessful += Number(source.fallbackProbeRowsSuccessful || 0)
  next.cooledDownRows += Number(source.cooledDownRows || 0)
  next.retryBudgetBlockedRows += Number(source.retryBudgetBlockedRows || 0)
  next.skippedLowPriorityRows += Number(source.skippedLowPriorityRows || 0)
  next.skippedPrimaryTooStrictCount += Number(source.skippedPrimaryTooStrictCount || 0)
  next.skippedFallbackBelowMinimumQualityCount += Number(
    source.skippedFallbackBelowMinimumQualityCount || 0
  )
  next.recomputedVerifiedRows += Number(source.recomputedVerifiedRows || 0)
  next.fallbackProbeRowsRecomputed += Number(source.fallbackProbeRowsRecomputed || 0)
  next.verifiedScannableRows += Number(source.verifiedScannableRows || 0)
  next.fallbackProbeVerifiedScannableRows += Number(
    source.fallbackProbeVerifiedScannableRows || 0
  )
  next.weaponSkinScannerSourceIncreased =
    Boolean(next.weaponSkinScannerSourceIncreased) ||
    Boolean(source.weaponSkinScannerSourceIncreased)
  next.fallbackProbeWeaponSkinScannerSourceIncreased =
    Boolean(next.fallbackProbeWeaponSkinScannerSourceIncreased) ||
    Boolean(source.fallbackProbeWeaponSkinScannerSourceIncreased)
  next.fallbackLaneActivated =
    Boolean(next.fallbackLaneActivated) || Boolean(source.fallbackLaneActivated)
  if (!next.queueEmptyReason && normalizeText(source.queueEmptyReason)) {
    next.queueEmptyReason = normalizeText(source.queueEmptyReason)
  }

  for (const state of Object.keys(next.queuedRowsByState)) {
    next.queuedRowsByState[state] += Number(source.queuedRowsByState?.[state] || 0)
  }
  for (const bucket of Object.keys(next.verificationPriorityBuckets)) {
    next.verificationPriorityBuckets[bucket] += Number(
      source.verificationPriorityBuckets?.[bucket] || 0
    )
  }

  return next
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

function createCategoryProgressState(categories = RECOVERY_CATEGORIES, targets = {}, options = {}) {
  const safeCategories = normalizeRecoveryCategories(categories)
  const resumeState =
    options.resumeState && typeof options.resumeState === "object" ? options.resumeState : {}
  const resumeProgress =
    resumeState.categoryProgressState && typeof resumeState.categoryProgressState === "object"
      ? resumeState.categoryProgressState
      : {}
  const cursor = resolveRecoveryCursor(safeCategories, options)
  const startedFromCursor = !Object.keys(resumeProgress).length

  return Object.fromEntries(
    safeCategories.map((category) => {
      const target = Math.max(Number(targets?.[category] || 0), 0)
      const resumeEntry =
        resumeProgress?.[category] && typeof resumeProgress[category] === "object"
          ? resumeProgress[category]
          : null
      if (resumeEntry) {
        const nextOffset = Math.min(
          Math.max(Number(resumeEntry.nextOffset || 0), 0),
          target
        )
        return [
          category,
          {
            nextOffset,
            done:
              resumeEntry.done == null ? nextOffset >= target : Boolean(resumeEntry.done),
            blockedReason: normalizeText(resumeEntry.blockedReason) || null
          }
        ]
      }

      if (!startedFromCursor) {
        return [
          category,
          {
            nextOffset: 0,
            done: false,
            blockedReason: null
          }
        ]
      }

      const categoryIndex = safeCategories.indexOf(category)
      const startIndex = safeCategories.indexOf(cursor.startCategory)
      if (startIndex >= 0 && categoryIndex < startIndex) {
        return [
          category,
          {
            nextOffset: target,
            done: true,
            blockedReason: null
          }
        ]
      }
      if (category === cursor.startCategory) {
        const nextOffset = Math.min(cursor.startOffset, target)
        return [
          category,
          {
            nextOffset,
            done: nextOffset >= target,
            blockedReason: null
          }
        ]
      }
      return [
        category,
        {
          nextOffset: 0,
          done: false,
          blockedReason: null
        }
      ]
    })
  )
}

function buildNextRecoveryCursor(categories = RECOVERY_CATEGORIES, categoryProgressState = {}, targets = {}) {
  const safeCategories = normalizeRecoveryCategories(categories)
  for (const category of safeCategories) {
    const target = Math.max(Number(targets?.[category] || 0), 0)
    const entry = categoryProgressState?.[category] || {}
    const nextOffset = Math.max(Number(entry.nextOffset || 0), 0)
    const done = Boolean(entry.done) || nextOffset >= target
    if (!done) {
      return {
        nextCategory: category,
        nextOffset
      }
    }
  }
  return {
    nextCategory: null,
    nextOffset: 0
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

  next.snapshotCooldownAppliedCount =
    Number(next.snapshotCooldownAppliedCount || 0) +
    Number(source.snapshotCooldownAppliedCount || 0)
  next.snapshotBatchesSkippedDueToCooldown =
    Number(next.snapshotBatchesSkippedDueToCooldown || 0) +
    Number(source.snapshotBatchesSkippedDueToCooldown || 0)
  next.snapshotRetryBudgetExhaustedCount =
    Number(next.snapshotRetryBudgetExhaustedCount || 0) +
    Number(source.snapshotRetryBudgetExhaustedCount || 0)

  for (const category of RECOVERY_CATEGORIES) {
    next.snapshotCooldownAppliedByCategory[category] =
      Number(next.snapshotCooldownAppliedByCategory?.[category] || 0) +
      Number(source.snapshotCooldownAppliedByCategory?.[category] || 0)
    next.snapshotBatchesSkippedDueToCooldownByCategory[category] =
      Number(next.snapshotBatchesSkippedDueToCooldownByCategory?.[category] || 0) +
      Number(source.snapshotBatchesSkippedDueToCooldownByCategory?.[category] || 0)
    next.snapshotRetryBudgetExhaustedByCategory[category] =
      Number(next.snapshotRetryBudgetExhaustedByCategory?.[category] || 0) +
      Number(source.snapshotRetryBudgetExhaustedByCategory?.[category] || 0)
  }

  next.quoteRowsInserted =
    Number(next.quoteRowsInserted || 0) + Number(source.quoteRowsInserted || 0)
  next.marketPriceRowsUpserted =
    Number(next.marketPriceRowsUpserted || 0) + Number(source.marketPriceRowsUpserted || 0)
  next.steam_rate_limited_count =
    Number(next.steam_rate_limited_count || 0) + Number(source.steam_rate_limited_count || 0)
  next.steam_cached_fallback_count =
    Number(next.steam_cached_fallback_count || 0) +
    Number(source.steam_cached_fallback_count || 0)
  next.steam_unavailable_count =
    Number(next.steam_unavailable_count || 0) + Number(source.steam_unavailable_count || 0)
  for (const [reasonKey, count] of Object.entries(source.market_failure_reason_counts || {})) {
    next.market_failure_reason_counts[reasonKey] =
      Number(next.market_failure_reason_counts?.[reasonKey] || 0) + Number(count || 0)
  }
  if (
    next.quoteRowOutcomesByKey &&
    typeof next.quoteRowOutcomesByKey === "object" &&
    source.quoteRowOutcomesByKey &&
    typeof source.quoteRowOutcomesByKey === "object"
  ) {
    Object.assign(next.quoteRowOutcomesByKey, source.quoteRowOutcomesByKey)
  }

  for (const sourceName of RECOVERY_SOURCE_ORDER) {
    const existing = next.quoteSourceDiagnostics?.[sourceName] || {
      scanner_market_mode: getScannerMarketPolicy(sourceName)?.mode || null,
      refreshed: 0,
      failed: 0,
      error: null,
      stateCounts: {},
      scanner_status_counts: {},
      live_request_budget: null,
      live_request_attempted: 0,
      live_request_skipped_due_to_budget: 0
    }
    const incoming = source.quoteSourceDiagnostics?.[sourceName] || {}
    existing.scanner_market_mode =
      incoming.scanner_market_mode || existing.scanner_market_mode || null
    existing.refreshed = Number(existing.refreshed || 0) + Number(incoming.refreshed || 0)
    existing.failed = Number(existing.failed || 0) + Number(incoming.failed || 0)
    existing.error = incoming.error || existing.error || null
    for (const [state, count] of Object.entries(incoming.stateCounts || {})) {
      existing.stateCounts[state] = Number(existing.stateCounts?.[state] || 0) + Number(count || 0)
    }
    for (const [status, count] of Object.entries(incoming.scanner_status_counts || {})) {
      existing.scanner_status_counts[status] =
        Number(existing.scanner_status_counts?.[status] || 0) + Number(count || 0)
    }
    for (const key of [
      "live_request_budget",
      "live_request_attempted",
      "live_request_skipped_due_to_budget",
      "market_enabled",
      "credentials_present",
      "auth_ok",
      "request_sent",
      "response_status",
      "response_parsed",
      "listings_found",
      "buy_price_present",
      "sell_price_present",
      "freshness_present",
      "listing_url_present",
      "source_failure_reason",
      "last_success_at",
      "last_failure_at"
    ]) {
      if (incoming[key] != null) {
        existing[key] = incoming[key]
      }
    }
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
  targets = {},
  categoryProgressState = {},
  snapshotPacingState = {},
  weaponSkinVerificationState = {},
  completedBatches = 0,
  processedRows = 0,
  processedRowsByCategory = emptyCategoryNumberMap(0),
  pauseReason = null,
  done = false
} = {}) {
  const cursor = buildNextRecoveryCursor(categories, categoryProgressState, targets)
  const resumeState = {
    categories: Array.isArray(categories) ? categories.slice() : RECOVERY_CATEGORIES.slice(),
    categoryProgressState,
    snapshotPacingState,
    weaponSkinVerificationState
  }
  const resumeStateToken = done ? null : encodeResumeStateToken(resumeState)
  return {
    categories: Array.isArray(categories) ? categories.slice() : RECOVERY_CATEGORIES.slice(),
    nextCategory: done ? null : cursor.nextCategory || null,
    nextOffset: done ? 0 : Math.max(Number(cursor.nextOffset || 0), 0),
    completedBatches: Math.max(Number(completedBatches || 0), 0),
    processedRows: Math.max(Number(processedRows || 0), 0),
    pauseReason: normalizeText(pauseReason) || null,
    categoryProgressState,
    snapshotPacingState,
    weaponSkinVerificationState,
    resumeState: done ? null : resumeState,
    resumeStateToken,
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
      done || !cursor.nextCategory
        ? []
        : [
            `--start-category=${cursor.nextCategory}`,
            `--start-offset=${Math.max(Number(cursor.nextOffset || 0), 0)}`,
            `--resume-state=${resumeStateToken}`
          ]
  }
}

function buildWeaponSkinVerificationQueue(rows = [], options = {}) {
  const nowMs = Number(options.nowMs || Date.now())
  const verificationState = createWeaponSkinVerificationState(
    options.weaponSkinVerificationState || {}
  )
  const defaults = getWeaponSkinVerificationDefaults(options)
  const primaryQueueable = []
  const fallbackQueueable = []
  let cooldownBlockedRows = 0
  let retryBudgetBlockedRows = 0
  let skippedLowPriorityRows = 0
  let skippedPrimaryTooStrictCount = 0
  let skippedFallbackBelowMinimumQualityCount = 0
  let verificationNotNeededRows = 0

  for (const row of dedupeRowsByMarketHashName(rows)) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue

    const rowState = getWeaponSkinVerificationRowState(verificationState, marketHashName, options)
    if (rowState.verifiedFresh) {
      continue
    }
    if (rowState.retryBudgetExhausted) {
      retryBudgetBlockedRows += 1
      continue
    }
    if (isWeaponSkinVerificationCooldownActive(rowState, nowMs)) {
      cooldownBlockedRows += 1
      continue
    }

    const snapshotFresh = isFreshWithinHours(
      row?.snapshot_captured_at || row?.snapshotCapturedAt,
      DEFAULT_HEALTH_WINDOW_HOURS,
      nowMs
    )
    const snapshotUsable =
      snapshotFresh &&
      normalizeText(row?.snapshot_state || row?.snapshotState).toLowerCase() !== "derived_only_snapshot"
    if (snapshotUsable) {
      verificationNotNeededRows += 1
      continue
    }

    const priority = computeWeaponSkinVerificationPriority(row, nowMs)
    if (priority.primaryEligible) {
      rowState.lastPriorityScore = priority.priorityScore
      primaryQueueable.push({
        row,
        marketHashName,
        candidateStatus: priority.candidateStatus,
        priorityScore: priority.priorityScore,
        priorityBucket: priority.priorityBucket
      })
      continue
    }

    const catalogGate = resolveWeaponSkinVerificationCatalogGate(row)
    if (catalogGate.structurallyInvalid) {
      skippedLowPriorityRows += 1
      skippedFallbackBelowMinimumQualityCount += 1
      continue
    }
    if (priority.fallbackEligible) {
      skippedPrimaryTooStrictCount += 1
      fallbackQueueable.push({
        row,
        marketHashName,
        candidateStatus: priority.candidateStatus,
        priorityScore: priority.priorityScore,
        priorityBucket: priority.priorityBucket
      })
      continue
    }

    if (!priority.minimumQuoteSupport || !priority.fallbackQualityCandidate || priority.lowPriorityCold) {
      skippedLowPriorityRows += 1
      skippedFallbackBelowMinimumQualityCount += 1
      continue
    }
  }

  const sortQueueEntries = (entries = []) =>
    entries.sort(
      (left, right) =>
        Number(right.priorityScore || 0) - Number(left.priorityScore || 0) ||
        Number(right.row?.priority_boost || 0) - Number(left.row?.priority_boost || 0) ||
        Number(right.row?.liquidity_rank || 0) - Number(left.row?.liquidity_rank || 0) ||
        String(left.marketHashName || "").localeCompare(String(right.marketHashName || ""))
    )

  sortQueueEntries(primaryQueueable)
  sortQueueEntries(fallbackQueueable)

  const selectedPrimary = primaryQueueable.slice(0, Math.max(defaults.verificationLimit, 1))
  const fallbackLaneActivated = selectedPrimary.length === 0 && fallbackQueueable.length > 0
  const selectedFallback = fallbackLaneActivated
    ? fallbackQueueable.slice(0, Math.max(defaults.fallbackProbeLimit, 1))
    : []
  const activeQueue = fallbackLaneActivated ? fallbackQueueable : primaryQueueable
  const selected = fallbackLaneActivated ? selectedFallback : selectedPrimary
  const activeQueuedRowsByState = createQueuedRowsByStateMap()
  const activePriorityBuckets = createPriorityBucketMap()

  for (const entry of activeQueue) {
    activeQueuedRowsByState[entry.candidateStatus] += 1
    activePriorityBuckets[entry.priorityBucket] += 1
  }

  let queueEmptyReason = null
  if (!selected.length) {
    if (retryBudgetBlockedRows > 0) {
      queueEmptyReason = "retry_budget_blocked"
    } else if (cooldownBlockedRows > 0) {
      queueEmptyReason = "cooldown_active"
    } else if (verificationNotNeededRows > 0 && rows.length === verificationNotNeededRows) {
      queueEmptyReason = "no_snapshot_verification_needed"
    } else if (skippedPrimaryTooStrictCount > 0) {
      queueEmptyReason = "primary_too_strict"
    } else if (skippedFallbackBelowMinimumQualityCount > 0) {
      queueEmptyReason = "fallback_below_minimum_quality"
    } else if (!rows.length) {
      queueEmptyReason = "no_rows_loaded"
    } else {
      queueEmptyReason = "no_quote_supported_candidates"
    }
  } else if (fallbackLaneActivated) {
    queueEmptyReason = "primary_queue_empty_activated_fallback_probe"
  }

  return {
    rows: selected.map((entry) => entry.row),
    diagnostics: {
      verificationQueueSize: activeQueue.length,
      topQueuedRowsCount: selected.length,
      primaryQueueSize: primaryQueueable.length,
      fallbackProbeQueueSize: fallbackQueueable.length,
      queuedRowsByState: activeQueuedRowsByState,
      attemptedVerificationRows: 0,
      successfulVerificationRows: 0,
      fallbackProbeRowsAttempted: 0,
      fallbackProbeRowsSuccessful: 0,
      cooledDownRows: cooldownBlockedRows,
      retryBudgetBlockedRows,
      skippedLowPriorityRows,
      skippedPrimaryTooStrictCount,
      skippedFallbackBelowMinimumQualityCount,
      verificationPriorityBuckets: activePriorityBuckets,
      recomputedVerifiedRows: 0,
      fallbackProbeRowsRecomputed: 0,
      verifiedScannableRows: 0,
      fallbackProbeVerifiedScannableRows: 0,
      weaponSkinScannerSourceIncreased: false,
      fallbackProbeWeaponSkinScannerSourceIncreased: false,
      fallbackLaneActivated,
      queueEmptyReason,
      selectedPrimaryNames: selectedPrimary.map((entry) => entry.marketHashName),
      selectedFallbackNames: selectedFallback.map((entry) => entry.marketHashName)
    }
  }
}

async function selectWeaponSkinVerificationBatch(options = {}) {
  const limit = Math.max(Number(options.limit || DEFAULT_SELECTION_BATCH_SIZE), 1)
  const candidatePoolLimit = Math.max(
    Number(
      options.weaponSkinVerificationCandidatePoolLimit ||
        Math.max(
          DEFAULT_WEAPON_SKIN_VERIFICATION_MIN_POOL,
          limit * DEFAULT_WEAPON_SKIN_VERIFICATION_POOL_MULTIPLIER
        )
    ),
    limit
  )
  const logProgress = options.logProgress
  const rows = await runLoggedStage(
    "weapon_skin_verification_queue",
    {
      logProgress,
      meta: {
        category: "weapon_skin",
        batchIndex: Math.max(Number(options.batchIndex || 0), 0),
        offset: Math.max(Number(options.offset || 0), 0),
        limit,
        candidatePoolLimit,
        categoryTarget: Number(options.categoryTarget || 0)
      }
    },
    async () =>
      marketSourceCatalogRepo.listActiveTradable({
        limit: candidatePoolLimit,
        offset: 0,
        categories: ["weapon_skin"]
      })
  )
  const queue = buildWeaponSkinVerificationQueue(rows, options)
  emitProgress(logProgress, {
    type: "weapon_skin_verification_queue",
    stage: "weapon_skin_verification_queue",
    batchIndex: Math.max(Number(options.batchIndex || 0), 0),
    verificationQueueSize: queue.diagnostics.verificationQueueSize,
    topQueuedRowsCount: queue.diagnostics.topQueuedRowsCount,
    primaryQueueSize: queue.diagnostics.primaryQueueSize,
    fallbackProbeQueueSize: queue.diagnostics.fallbackProbeQueueSize,
    queuedRowsByState: queue.diagnostics.queuedRowsByState,
    cooledDownRows: queue.diagnostics.cooledDownRows,
    retryBudgetBlockedRows: queue.diagnostics.retryBudgetBlockedRows,
    skippedLowPriorityRows: queue.diagnostics.skippedLowPriorityRows,
    skippedPrimaryTooStrictCount: queue.diagnostics.skippedPrimaryTooStrictCount,
    skippedFallbackBelowMinimumQualityCount:
      queue.diagnostics.skippedFallbackBelowMinimumQualityCount,
    fallbackLaneActivated: queue.diagnostics.fallbackLaneActivated,
    queueEmptyReason: queue.diagnostics.queueEmptyReason,
    verificationPriorityBuckets: queue.diagnostics.verificationPriorityBuckets
  })
  return {
    rows: queue.rows,
    selectionDiagnostics: {
      weaponSkinVerification: queue.diagnostics,
      completeCategoryAfterBatch: true,
      blockedReason:
        queue.rows.length > 0
          ? null
          : queue.diagnostics.retryBudgetBlockedRows > 0
            ? "retry_budget_exhausted"
            : queue.diagnostics.cooledDownRows > 0
              ? "active_cooldown_retry_later"
              : null,
      shouldRetryLater:
        queue.rows.length === 0 &&
        queue.diagnostics.retryBudgetBlockedRows <= 0 &&
        queue.diagnostics.cooledDownRows > 0,
      retryBudgetExhausted:
        queue.rows.length === 0 && queue.diagnostics.retryBudgetBlockedRows > 0
    }
  }
}

async function selectRecoveryRowBatch(options = {}) {
  const category = normalizeText(options.category).toLowerCase() || "weapon_skin"
  const limit = Math.max(Number(options.limit || DEFAULT_SELECTION_BATCH_SIZE), 1)
  const offset = Math.max(Number(options.offset || 0), 0)
  const batchIndex = Math.max(Number(options.batchIndex || 0), 0)
  const logProgress = options.logProgress

  if (category === "weapon_skin") {
    return selectWeaponSkinVerificationBatch({
      ...options,
      category,
      limit,
      offset,
      batchIndex,
      logProgress
    })
  }

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

  return {
    rows: dedupeRowsByMarketHashName(rows),
    selectionDiagnostics: null
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

function pickSourceSpecificRawFields(source = "", raw = {}) {
  const safeSource = normalizeText(source).toLowerCase()
  if (!safeSource || !raw || typeof raw !== "object") return {}
  return Object.fromEntries(
    Object.entries(raw).filter(([key, value]) => {
      if (!normalizeText(key).toLowerCase().startsWith(`${safeSource}_`)) return false
      return (
        value == null ||
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      )
    })
  )
}

function buildQualityFlags(record = {}, row = {}, fetchedAtIso = null) {
  const raw = record?.raw && typeof record.raw === "object" ? record.raw : {}
  const source = normalizeText(record?.source).toLowerCase()
  const scannerPolicy = getScannerMarketPolicy(source)
  const marketHealth =
    readMarketHealth(raw) ||
    buildMarketHealthDiagnostics({
      marketEnabled: true,
      requestSent: false,
      responseParsed: true,
      listingsFound: Boolean(record?.url),
      buyPricePresent: toFiniteOrNull(record?.grossPrice) != null,
      sellPricePresent: toFiniteOrNull(record?.netPriceAfterFees) != null,
      freshnessPresent: Boolean(record?.updatedAt),
      listingUrlPresent: Boolean(record?.url),
      sourceFailureReason: normalizeSourceState(raw?.source_failure_reason),
      lastSuccessAt: record?.updatedAt
    })
  return {
    recovery_refresh: true,
    recovery_category: normalizeText(row?.category || row?.itemCategory).toLowerCase() || null,
    confidence: normalizeText(record?.confidence).toLowerCase() || null,
    route_available: Boolean(record?.url),
    listing_available:
      raw?.listing_available == null ? Boolean(record?.url) : Boolean(raw.listing_available),
    source_updated_at: toIsoOrNull(record?.updatedAt || raw?.updated_at || raw?.updatedAt),
    fetched_at: toIsoOrNull(fetchedAtIso),
    url: normalizeText(record?.url) || null,
    source_state: normalizeSourceState(marketHealth?.source_failure_reason) || "ok",
    scanner_market_mode: scannerPolicy?.mode || null,
    scanner_market_primary: Boolean(scannerPolicy?.primary),
    market_enabled: marketHealth?.market_enabled ?? true,
    credentials_present: marketHealth?.credentials_present ?? null,
    auth_ok: marketHealth?.auth_ok ?? null,
    request_sent: marketHealth?.request_sent ?? null,
    response_status: marketHealth?.response_status ?? null,
    response_parsed: marketHealth?.response_parsed ?? null,
    listings_found: marketHealth?.listings_found ?? null,
    buy_price_present: marketHealth?.buy_price_present ?? null,
    sell_price_present: marketHealth?.sell_price_present ?? null,
    freshness_present: marketHealth?.freshness_present ?? null,
    listing_url_present: marketHealth?.listing_url_present ?? null,
    source_failure_reason: marketHealth?.source_failure_reason ?? null,
    last_success_at: marketHealth?.last_success_at ?? null,
    last_failure_at: marketHealth?.last_failure_at ?? null,
    ...pickSourceSpecificRawFields(source, raw)
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
const SNAPSHOT_SOURCE_KEY = "steam_market_overview"
const SNAPSHOT_PACING_DEFAULTS = Object.freeze({
  weapon_skin: Object.freeze({
    preferredBatchSize: 3,
    cooldownMs: 15 * 60 * 1000,
    maxCooldownMs: 60 * 60 * 1000,
    retryBudget: 2
  }),
  case: Object.freeze({
    preferredBatchSize: 2,
    cooldownMs: 5 * 60 * 1000,
    maxCooldownMs: 20 * 60 * 1000,
    retryBudget: 2
  }),
  sticker_capsule: Object.freeze({
    preferredBatchSize: 2,
    cooldownMs: 5 * 60 * 1000,
    maxCooldownMs: 20 * 60 * 1000,
    retryBudget: 2
  })
})

function getSnapshotPacingDefaults(category = "weapon_skin", overrides = {}) {
  const safeCategory = normalizeText(category).toLowerCase() || "weapon_skin"
  const baseDefaults = SNAPSHOT_PACING_DEFAULTS[safeCategory] || SNAPSHOT_PACING_DEFAULTS.weapon_skin
  const categoryOverrides =
    overrides && typeof overrides === "object" ? overrides[safeCategory] || {} : {}
  return {
    preferredBatchSize: Math.max(
      Number(categoryOverrides.preferredBatchSize || baseDefaults.preferredBatchSize || 1),
      1
    ),
    cooldownMs: Math.max(
      Number(categoryOverrides.cooldownMs || baseDefaults.cooldownMs || 60000),
      1000
    ),
    maxCooldownMs: Math.max(
      Number(categoryOverrides.maxCooldownMs || baseDefaults.maxCooldownMs || 60000),
      Number(categoryOverrides.cooldownMs || baseDefaults.cooldownMs || 60000)
    ),
    retryBudget: Math.max(
      Number(categoryOverrides.retryBudget ?? baseDefaults.retryBudget ?? 0),
      0
    )
  }
}

function toIsoFromMsOrNull(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) return null
  return new Date(numeric).toISOString()
}

function toMsFromIsoOrNull(value) {
  const iso = toIsoOrNull(value)
  if (!iso) return null
  const ts = new Date(iso).getTime()
  return Number.isFinite(ts) ? ts : null
}

function createSnapshotSourcePacingState(category = "weapon_skin", source = SNAPSHOT_SOURCE_KEY, seed = {}, overrides = {}) {
  const defaults = getSnapshotPacingDefaults(category, overrides)
  const retriesRemaining = Math.max(
    Number(
      seed?.retriesRemaining == null ? defaults.retryBudget : seed.retriesRemaining
    ),
    0
  )
  return {
    source,
    preferredBatchSize: defaults.preferredBatchSize,
    cooldownMsApplied: Math.max(Number(seed?.cooldownMsApplied || 0), 0),
    nextSafeRetryAt: toIsoOrNull(seed?.nextSafeRetryAt) || null,
    retriesRemaining,
    retryBudget: defaults.retryBudget,
    retryBudgetExhausted:
      seed?.retryBudgetExhausted == null
        ? retriesRemaining <= 0
        : Boolean(seed.retryBudgetExhausted),
    rateLimitHits: Math.max(Number(seed?.rateLimitHits || 0), 0),
    batchesSkippedDueToCooldown: Math.max(Number(seed?.batchesSkippedDueToCooldown || 0), 0),
    cooldownAppliedCount: Math.max(Number(seed?.cooldownAppliedCount || 0), 0),
    lastRateLimitedAt: toIsoOrNull(seed?.lastRateLimitedAt) || null,
    lastBlockedReason: normalizeText(seed?.lastBlockedReason) || null
  }
}

function createSnapshotPacingState(seed = {}, categories = RECOVERY_CATEGORIES, overrides = {}) {
  const safeCategories = normalizeRecoveryCategories(categories)
  const byCategorySeed = seed?.byCategory && typeof seed.byCategory === "object" ? seed.byCategory : {}
  return {
    byCategory: Object.fromEntries(
      safeCategories.map((category) => {
        const categorySeed =
          byCategorySeed?.[category]?.bySource?.[SNAPSHOT_SOURCE_KEY] ||
          byCategorySeed?.[category] ||
          {}
        return [
          category,
          {
            bySource: {
              [SNAPSHOT_SOURCE_KEY]: createSnapshotSourcePacingState(
                category,
                SNAPSHOT_SOURCE_KEY,
                categorySeed,
                overrides
              )
            }
          }
        ]
      })
    )
  }
}

function getSnapshotSourcePacingState(snapshotPacingState = {}, category = "weapon_skin", overrides = {}) {
  if (!snapshotPacingState.byCategory || typeof snapshotPacingState.byCategory !== "object") {
    snapshotPacingState.byCategory = {}
  }
  if (!snapshotPacingState.byCategory[category]) {
    snapshotPacingState.byCategory[category] = { bySource: {} }
  }
  if (
    !snapshotPacingState.byCategory[category].bySource ||
    typeof snapshotPacingState.byCategory[category].bySource !== "object"
  ) {
    snapshotPacingState.byCategory[category].bySource = {}
  }
  if (!snapshotPacingState.byCategory[category].bySource[SNAPSHOT_SOURCE_KEY]) {
    snapshotPacingState.byCategory[category].bySource[SNAPSHOT_SOURCE_KEY] =
      createSnapshotSourcePacingState(category, SNAPSHOT_SOURCE_KEY, {}, overrides)
  }
  return snapshotPacingState.byCategory[category].bySource[SNAPSHOT_SOURCE_KEY]
}

function isSnapshotCooldownActive(sourceState = {}, nowMs = Date.now()) {
  const nextSafeRetryMs = toMsFromIsoOrNull(sourceState?.nextSafeRetryAt)
  return Number.isFinite(nextSafeRetryMs) && nextSafeRetryMs > nowMs
}

function getSnapshotRemainingCooldownMs(sourceState = {}, nowMs = Date.now()) {
  const nextSafeRetryMs = toMsFromIsoOrNull(sourceState?.nextSafeRetryAt)
  if (!Number.isFinite(nextSafeRetryMs)) return 0
  return Math.max(nextSafeRetryMs - nowMs, 0)
}

function applySnapshotRateLimitState(
  snapshotPacingState = {},
  category = "weapon_skin",
  options = {}
) {
  const overrides = options.snapshotPacingOverrides || {}
  const nowMs = Number(options.nowMs || Date.now())
  const sourceState = getSnapshotSourcePacingState(snapshotPacingState, category, overrides)
  const defaults = getSnapshotPacingDefaults(category, overrides)
  sourceState.rateLimitHits = Math.max(Number(sourceState.rateLimitHits || 0), 0) + 1
  sourceState.cooldownAppliedCount = Math.max(Number(sourceState.cooldownAppliedCount || 0), 0) + 1
  if (Number(sourceState.retriesRemaining || 0) > 0) {
    sourceState.retriesRemaining -= 1
  }
  sourceState.retryBudgetExhausted = Number(sourceState.retriesRemaining || 0) <= 0
  const cooldownMs = Math.min(
    defaults.cooldownMs * 2 ** Math.max(sourceState.rateLimitHits - 1, 0),
    defaults.maxCooldownMs
  )
  sourceState.cooldownMsApplied = cooldownMs
  sourceState.lastRateLimitedAt = toIsoFromMsOrNull(nowMs)
  sourceState.nextSafeRetryAt = toIsoFromMsOrNull(nowMs + cooldownMs)
  sourceState.lastBlockedReason = sourceState.retryBudgetExhausted
    ? "retry_budget_exhausted"
    : "active_cooldown_retry_later"
  return {
    cooldownMsApplied: cooldownMs,
    nextSafeRetryAt: sourceState.nextSafeRetryAt,
    retriesRemaining: Number(sourceState.retriesRemaining || 0),
    retryBudgetExhausted: Boolean(sourceState.retryBudgetExhausted)
  }
}

function summarizeSnapshotPacingState(snapshotPacingState = {}, categories = RECOVERY_CATEGORIES, nowMs = Date.now(), overrides = {}) {
  const safeCategories = normalizeRecoveryCategories(categories)
  return Object.fromEntries(
    safeCategories.map((category) => {
      const state = getSnapshotSourcePacingState(snapshotPacingState, category, overrides)
      const cooldownActive = isSnapshotCooldownActive(state, nowMs)
      return [
        category,
        {
          source: SNAPSHOT_SOURCE_KEY,
          preferredBatchSize: Number(state.preferredBatchSize || getSnapshotPacingDefaults(category, overrides).preferredBatchSize || 1),
          cooldownActive,
          cooldownMsApplied: Number(state.cooldownMsApplied || 0),
          remainingCooldownMs: getSnapshotRemainingCooldownMs(state, nowMs),
          nextSafeRetryAt: state.nextSafeRetryAt || null,
          retriesRemaining: Number(state.retriesRemaining || 0),
          retryBudget: Number(state.retryBudget || getSnapshotPacingDefaults(category, overrides).retryBudget || 0),
          retryBudgetExhausted: Boolean(state.retryBudgetExhausted),
          batchesSkippedDueToCooldown: Number(state.batchesSkippedDueToCooldown || 0),
          cooldownAppliedCount: Number(state.cooldownAppliedCount || 0),
          rateLimitHits: Number(state.rateLimitHits || 0),
          lastRateLimitedAt: state.lastRateLimitedAt || null,
          lastBlockedReason: state.lastBlockedReason || null,
          retryCurrentlyUseful:
            !cooldownActive && !Boolean(state.retryBudgetExhausted),
          retryTemporarilyBlocked:
            cooldownActive && !Boolean(state.retryBudgetExhausted)
        }
      ]
    })
  )
}

function encodeResumeStateToken(value = {}) {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url")
}

function decodeResumeStateToken(value = "") {
  const text = normalizeText(value)
  if (!text) return {}
  try {
    return JSON.parse(Buffer.from(text, "base64url").toString("utf8"))
  } catch (_error) {
    return {}
  }
}

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

function createAttemptCounter(options = {}) {
  const captureQuoteRowOutcomes = options.captureQuoteRowOutcomes === true
  return {
    ...buildScannerMarketPolicyDiagnostics(),
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
    snapshotCooldownAppliedCount: 0,
    snapshotBatchesSkippedDueToCooldown: 0,
    snapshotRetryBudgetExhaustedCount: 0,
    snapshotCooldownAppliedByCategory: emptyCategoryNumberMap(0),
    snapshotBatchesSkippedDueToCooldownByCategory: emptyCategoryNumberMap(0),
    snapshotRetryBudgetExhaustedByCategory: emptyCategoryNumberMap(0),
    quoteRowsInserted: 0,
    marketPriceRowsUpserted: 0,
    market_failure_reason_counts: {},
    steam_rate_limited_count: 0,
    steam_cached_fallback_count: 0,
    steam_unavailable_count: 0,
    quoteRowOutcomesByKey: captureQuoteRowOutcomes ? {} : null,
    quoteSourceDiagnostics: Object.fromEntries(
      RECOVERY_SOURCE_ORDER.map((source) => [
        source,
        {
          scanner_market_mode: getScannerMarketPolicy(source)?.mode || null,
          refreshed: 0,
          failed: 0,
          error: null,
          stateCounts: {},
          scanner_status_counts: {},
          live_request_budget: null,
          live_request_attempted: 0,
          live_request_skipped_due_to_budget: 0,
          market_enabled: true,
          credentials_present: null,
          auth_ok: null,
          request_sent: false,
          response_status: null,
          response_parsed: null,
          listings_found: null,
          buy_price_present: null,
          sell_price_present: null,
          freshness_present: null,
          listing_url_present: null,
          source_failure_reason: null,
          last_success_at: null,
          last_failure_at: null
        }
      ])
    )
  }
}

async function refreshQuotes(rows = [], options = {}) {
  const quoteBatchSize = Math.max(Number(options.quoteBatchSize || DEFAULT_QUOTE_BATCH_SIZE), 1)
  const concurrency = Math.max(Number(options.concurrency || DEFAULT_REFRESH_CONCURRENCY), 1)
  const timeoutMs = Math.max(Number(options.timeoutMs || marketCompareTimeoutMs || 9000), 500)
  const maxRetries = Math.max(Number(options.maxRetries || marketCompareMaxRetries || 3), 1)
  const cacheTtlMinutes = Math.max(
    Number(options.cacheTtlMinutes || marketCompareCacheTtlMinutes || 60),
    1
  )
  const logProgress = options.logProgress
  const batchMeta = options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}
  const counters = createAttemptCounter({
    captureQuoteRowOutcomes: options.collectQuoteRowOutcomes === true
  })
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
    const sourceItemNames = sourceItems
      .map((item) => normalizeText(item?.marketHashName))
      .filter(Boolean)
    let cachedRowsBySource = {}
    if (sourceItemNames.length) {
      cachedRowsBySource = await marketPriceRepo
        .getLatestByMarketHashNames(sourceItemNames, {
          sources: RECOVERY_SOURCE_ORDER.filter((source) => shouldUseFreshCacheOnRateLimit(source))
        })
        .catch(() => ({}))
    }

    const appendCachedFallbackRecords = (source, marketHashNames = [], scannerStatus = null) => {
      if (!shouldUseFreshCacheOnRateLimit(source)) return 0
      const cachedRows = cachedRowsBySource?.[source] || {}
      let recoveredCount = 0
      for (const marketHashName of Array.from(
        new Set(marketHashNames.map((value) => normalizeText(value)).filter(Boolean))
      )) {
        const cachedRow = cachedRows?.[marketHashName]
        if (!cachedRow || !isFreshMarketPriceCacheRow(cachedRow, cacheTtlMinutes)) {
          continue
        }
        const cachedRecord = withSourceSpecificScannerStatus(
          buildRecordFromCachedMarketPriceRow(cachedRow, source, chunkFetchedAt),
          source,
          scannerStatus
        )
        if (!cachedRecord) continue
        const row = rowsByName[marketHashName] || {}
        const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
        const quoteRow = buildQuoteInsertRow(cachedRecord, row, chunkFetchedAt)
        if (!quoteRow) continue
        chunkQuoteRows.push(quoteRow)
        if (counters.quoteRefreshedNamesByCategory[category] instanceof Set) {
          counters.quoteRefreshedNamesByCategory[category].add(marketHashName)
        }
        counters.quoteSourceDiagnostics[source].refreshed += 1
        incrementQuoteSourceStateCount(
          counters.quoteSourceDiagnostics[source],
          SOURCE_STATES.OK,
          1
        )
        if (scannerStatus) {
          incrementQuoteSourceScannerStatusCount(
            counters.quoteSourceDiagnostics[source],
            scannerStatus,
            1
          )
        }
        if (source === "steam" && scannerStatus === STEAM_SCANNER_STATUS.CACHED_FALLBACK) {
          counters.steam_cached_fallback_count += 1
        }
        recordQuoteRowOutcome(counters, {
          source,
          marketHashName,
          available: true,
          sourceState: SOURCE_STATES.OK,
          scannerStatus,
          requestSent: false,
          usedFreshCache: true
        })
        recoveredCount += 1
      }
      return recoveredCount
    }

    for (const source of RECOVERY_SOURCE_ORDER) {
      const adapter = RECOVERY_SOURCES[source]
      if (!adapter?.batchGetPrices) continue
      const sourceDiagnostics = counters.quoteSourceDiagnostics[source]
      const scannerPolicy = getScannerMarketPolicy(source)
      sourceDiagnostics.scanner_market_mode = scannerPolicy?.mode || null
      if (isScannerMarketDisabled(source)) {
        sourceDiagnostics.error = "scanner_policy_disabled"
        sourceDiagnostics.request_sent = false
        sourceDiagnostics.source_failure_reason = "disabled"
        incrementQuoteSourceStateCount(sourceDiagnostics, SOURCE_STATES.DISABLED, sourceItems.length)
        continue
      }

      let requestedSourceItems = sourceItems
      let requestedItemNames = sourceItemNames

      if (source === "steam" && shouldUseFreshCacheOnRateLimit(source)) {
        const steamLiveRequestBudget = getSteamScannerLiveRequestBudget(options)
        sourceDiagnostics.live_request_budget = steamLiveRequestBudget
        const freshCachedNames = []
        const liveEligibleItems = []

        for (const item of sourceItems) {
          const marketHashName = normalizeText(item?.marketHashName)
          const cachedRow = cachedRowsBySource?.[source]?.[marketHashName]
          if (cachedRow && isFreshMarketPriceCacheRow(cachedRow, cacheTtlMinutes)) {
            freshCachedNames.push(marketHashName)
          } else {
            liveEligibleItems.push(item)
          }
        }

        if (freshCachedNames.length) {
          appendCachedFallbackRecords(
            source,
            freshCachedNames,
            STEAM_SCANNER_STATUS.CACHED_FALLBACK
          )
        }

        requestedSourceItems = liveEligibleItems.slice(0, steamLiveRequestBudget)
        requestedItemNames = requestedSourceItems
          .map((item) => normalizeText(item?.marketHashName))
          .filter(Boolean)
        const skippedItems = liveEligibleItems.slice(requestedSourceItems.length)
        sourceDiagnostics.live_request_attempted += requestedSourceItems.length
        sourceDiagnostics.live_request_skipped_due_to_budget += skippedItems.length

        if (skippedItems.length) {
          sourceDiagnostics.failed += skippedItems.length
          sourceDiagnostics.error = sourceDiagnostics.error || STEAM_SCANNER_BUDGET_EXHAUSTED_REASON
          incrementFailureReasonCount(
            counters.market_failure_reason_counts,
            source,
            STEAM_SCANNER_BUDGET_EXHAUSTED_REASON,
            skippedItems.length
          )
          incrementQuoteSourceStateCount(sourceDiagnostics, SOURCE_STATES.UNAVAILABLE, skippedItems.length)
          incrementQuoteSourceScannerStatusCount(
            sourceDiagnostics,
            STEAM_SCANNER_STATUS.UNAVAILABLE,
            skippedItems.length
          )
          counters.steam_unavailable_count += skippedItems.length
          for (const skippedItem of skippedItems) {
            recordQuoteRowOutcome(counters, {
              source,
              marketHashName: skippedItem.marketHashName,
              available: false,
              sourceState: SOURCE_STATES.UNAVAILABLE,
              reason: STEAM_SCANNER_BUDGET_EXHAUSTED_REASON,
              scannerStatus: STEAM_SCANNER_STATUS.UNAVAILABLE,
              requestSent: false,
              usedFreshCache: false
            })
          }
        }

        if (!requestedSourceItems.length) {
          sourceDiagnostics.request_sent = false
          continue
        }
      }

      try {
        const sourceResult = await adapter.batchGetPrices(requestedSourceItems, {
          currency: "USD",
          concurrency: source === "steam" ? 1 : concurrency,
          timeoutMs,
          maxRetries,
          stopOnRateLimit: source === "steam"
        })
        const meta =
          sourceResult && typeof sourceResult === "object" && sourceResult.__meta
            ? sourceResult.__meta
            : {}
        const failuresByName =
          meta && typeof meta.failuresByName === "object" ? meta.failuresByName : {}
        const stateByName = meta && typeof meta.stateByName === "object" ? meta.stateByName : {}
        const diagnosticsByName =
          meta && typeof meta.diagnosticsByName === "object" ? meta.diagnosticsByName : {}
        const rateLimitedNames = []
        const failedNames = new Set()
        const successfulNames = new Set()
        for (const [state, count] of Object.entries(
          Object.values(stateByName).reduce((acc, state) => {
            const key = normalizeSourceState(state) || normalizeText(state).toLowerCase() || "unknown"
            acc[key] = Number(acc[key] || 0) + 1
            return acc
          }, {})
        )) {
          incrementQuoteSourceStateCount(sourceDiagnostics, state, count)
        }
        for (const key of [
          "market_enabled",
          "credentials_present",
          "auth_ok",
          "request_sent",
          "response_status",
          "response_parsed",
          "listings_found",
          "buy_price_present",
          "sell_price_present",
          "freshness_present",
          "listing_url_present",
          "source_failure_reason",
          "last_success_at",
          "last_failure_at"
        ]) {
          if (meta[key] != null) {
            counters.quoteSourceDiagnostics[source][key] = meta[key]
          }
        }

        for (const item of requestedSourceItems) {
          const marketHashName = normalizeText(item?.marketHashName)
          if (!marketHashName) continue
          const failureReason = normalizeText(failuresByName[marketHashName])
          if (failureReason) {
            sourceDiagnostics.failed += 1
            failedNames.add(marketHashName)
            incrementFailureReasonCount(
              counters.market_failure_reason_counts,
              source,
              failureReason
            )
            const isRateLimitedFailure = source === "steam" && isRateLimitReason(failureReason)
            if (isRateLimitedFailure) {
              counters.steam_rate_limited_count += 1
              rateLimitedNames.push(marketHashName)
              incrementQuoteSourceScannerStatusCount(
                sourceDiagnostics,
                STEAM_SCANNER_STATUS.RATE_LIMITED,
                1
              )
            } else if (source === "steam") {
              counters.steam_unavailable_count += 1
              incrementQuoteSourceScannerStatusCount(
                sourceDiagnostics,
                STEAM_SCANNER_STATUS.UNAVAILABLE,
                1
              )
            }
            recordQuoteRowOutcome(counters, {
              source,
              marketHashName,
              available: false,
              sourceState:
                normalizeSourceState(stateByName[marketHashName]) || SOURCE_STATES.UNAVAILABLE,
              reason: failureReason,
              scannerStatus:
                source === "steam"
                  ? isRateLimitedFailure
                    ? STEAM_SCANNER_STATUS.RATE_LIMITED
                    : STEAM_SCANNER_STATUS.UNAVAILABLE
                  : null,
              requestSent:
                diagnosticsByName?.[marketHashName]?.request_sent == null
                  ? true
                  : Boolean(diagnosticsByName[marketHashName].request_sent),
              usedFreshCache: false
            })
          }
        }

        for (const [marketHashName, record] of Object.entries(sourceResult || {})) {
          if (marketHashName === "__meta") continue
          if (!record || typeof record !== "object") continue
          successfulNames.add(marketHashName)
          const row = rowsByName[marketHashName] || {}
          const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
          chunkMarketPriceRows.push(buildMarketPriceUpsertRow(record, chunkFetchedAt))
          chunkQuoteRows.push(buildQuoteInsertRow(record, row, chunkFetchedAt))
          if (counters.quoteRefreshedNamesByCategory[category] instanceof Set) {
            counters.quoteRefreshedNamesByCategory[category].add(marketHashName)
          }
          sourceDiagnostics.refreshed += 1
          recordQuoteRowOutcome(counters, {
            source,
            marketHashName,
            available: true,
            sourceState: SOURCE_STATES.OK,
            scannerStatus:
              source === "steam"
                ? normalizeText(record?.raw?.steam_scanner_status) || null
                : null,
            requestSent: true,
            usedFreshCache: false
          })
        }

        const sourceUnavailableReason = normalizeText(meta?.sourceUnavailableReason)
        if (sourceUnavailableReason) {
          sourceDiagnostics.error = sourceUnavailableReason
          if (!Object.keys(failuresByName).length) {
            incrementFailureReasonCount(
              counters.market_failure_reason_counts,
              source,
              sourceUnavailableReason,
              requestedSourceItems.length
            )
            if (source === "steam" && isRateLimitReason(sourceUnavailableReason)) {
              counters.steam_rate_limited_count += requestedSourceItems.length
              incrementQuoteSourceScannerStatusCount(
                sourceDiagnostics,
                STEAM_SCANNER_STATUS.RATE_LIMITED,
                requestedSourceItems.length
              )
            } else if (source === "steam") {
              counters.steam_unavailable_count += requestedSourceItems.length
              incrementQuoteSourceScannerStatusCount(
                sourceDiagnostics,
                STEAM_SCANNER_STATUS.UNAVAILABLE,
                requestedSourceItems.length
              )
            }
          }
        }

        if (sourceUnavailableReason) {
          for (const item of requestedSourceItems) {
            const marketHashName = normalizeText(item?.marketHashName)
            if (!marketHashName || successfulNames.has(marketHashName) || failedNames.has(marketHashName)) {
              continue
            }
            recordQuoteRowOutcome(counters, {
              source,
              marketHashName,
              available: false,
              sourceState:
                normalizeSourceState(meta?.sourceFailureReason || meta?.source_failure_reason) ||
                SOURCE_STATES.UNAVAILABLE,
              reason: sourceUnavailableReason,
              scannerStatus:
                source === "steam"
                  ? isRateLimitReason(sourceUnavailableReason)
                    ? STEAM_SCANNER_STATUS.RATE_LIMITED
                    : STEAM_SCANNER_STATUS.UNAVAILABLE
                  : null,
              requestSent: true,
              usedFreshCache: false
            })
          }
        }

        if (rateLimitedNames.length) {
          appendCachedFallbackRecords(
            source,
            rateLimitedNames,
            source === "steam" ? STEAM_SCANNER_STATUS.CACHED_FALLBACK : null
          )
        } else if (
          sourceUnavailableReason &&
          isRateLimitReason(sourceUnavailableReason)
        ) {
          appendCachedFallbackRecords(
            source,
            requestedItemNames,
            source === "steam" ? STEAM_SCANNER_STATUS.CACHED_FALLBACK : null
          )
        }
      } catch (err) {
        const errorMessage = normalizeText(err?.message) || "quote_refresh_failed"
        sourceDiagnostics.error = errorMessage
        sourceDiagnostics.failed += requestedSourceItems.length
        incrementFailureReasonCount(
          counters.market_failure_reason_counts,
          source,
          errorMessage,
          requestedSourceItems.length
        )
        if (source === "steam" && isRateLimitReason(errorMessage)) {
          counters.steam_rate_limited_count += requestedSourceItems.length
          incrementQuoteSourceScannerStatusCount(
            sourceDiagnostics,
            STEAM_SCANNER_STATUS.RATE_LIMITED,
            requestedSourceItems.length
          )
        } else if (source === "steam") {
          counters.steam_unavailable_count += requestedSourceItems.length
          incrementQuoteSourceScannerStatusCount(
            sourceDiagnostics,
            STEAM_SCANNER_STATUS.UNAVAILABLE,
            requestedSourceItems.length
          )
        }
        for (const item of requestedSourceItems) {
          recordQuoteRowOutcome(counters, {
            source,
            marketHashName: item.marketHashName,
            available: false,
            sourceState: SOURCE_STATES.UNAVAILABLE,
            reason: errorMessage,
            scannerStatus:
              source === "steam"
                ? isRateLimitReason(errorMessage)
                  ? STEAM_SCANNER_STATUS.RATE_LIMITED
                  : STEAM_SCANNER_STATUS.UNAVAILABLE
                : null,
            requestSent: true,
            usedFreshCache: false
          })
        }
        if (isRateLimitReason(errorMessage)) {
          appendCachedFallbackRecords(
            source,
            requestedItemNames,
            source === "steam" ? STEAM_SCANNER_STATUS.CACHED_FALLBACK : null
          )
        }
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
  const category =
    normalizeText(options?.batchMeta?.category).toLowerCase() ||
    normalizeText(rows?.[0]?.category || rows?.[0]?.itemCategory).toLowerCase() ||
    "weapon_skin"
  const snapshotPacingOverrides =
    options.snapshotPacingOverrides && typeof options.snapshotPacingOverrides === "object"
      ? options.snapshotPacingOverrides
      : {}
  const snapshotPacingState =
    options.snapshotPacingState && typeof options.snapshotPacingState === "object"
      ? options.snapshotPacingState
      : createSnapshotPacingState({}, [category], snapshotPacingOverrides)
  const sourceState = getSnapshotSourcePacingState(
    snapshotPacingState,
    category,
    snapshotPacingOverrides
  )
  const pacingDefaults = getSnapshotPacingDefaults(category, snapshotPacingOverrides)
  const nowMs = Number(options.nowMs || Date.now())
  const snapshotBatchSize = Math.max(
    Math.min(
      Number(options.snapshotBatchSize || DEFAULT_SNAPSHOT_BATCH_SIZE),
      Number(sourceState.preferredBatchSize || pacingDefaults.preferredBatchSize || 1)
    ),
    1
  )
  const concurrency = Math.max(Number(options.concurrency || DEFAULT_REFRESH_CONCURRENCY), 1)
  const logProgress = options.logProgress
  const batchMeta = options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}
  const counters = createAttemptCounter()
  const targetSkins = []
  const batchReasonCounts = createSnapshotReasonMap()
  const rowOutcomes = []
  const currentPacingState = () =>
    summarizeSnapshotPacingState(
      snapshotPacingState,
      [category],
      nowMs,
      snapshotPacingOverrides
    )[category]

  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    const rowCategory = normalizeText(row?.category || row?.itemCategory).toLowerCase()
    const skin = skinsByName[marketHashName] || null
    if (!skin) {
      incrementSnapshotReasonCounter(
        counters,
        rowCategory,
        "snapshot_missing_skin_mapping"
      )
      batchReasonCounts.snapshot_missing_skin_mapping += 1
      rowOutcomes.push({
        marketHashName,
        category: rowCategory,
        reason: "snapshot_missing_skin_mapping",
        refreshed: false
      })
      continue
    }
    targetSkins.push(skin)
    if (counters.snapshotAttemptedByCategory[rowCategory] != null) {
      counters.snapshotAttemptedByCategory[rowCategory] += 1
    }
  }

  if (sourceState.retryBudgetExhausted) {
    counters.snapshotRetryBudgetExhaustedCount += 1
    counters.snapshotRetryBudgetExhaustedByCategory[category] += 1
    const pacing = currentPacingState()
    const batchDiagnostics = {
      ...batchMeta,
      reasonCounts: batchReasonCounts,
      blockedReason: "retry_budget_exhausted",
      pacing
    }
    counters.snapshotBatchDiagnostics.push(batchDiagnostics)
    emitProgress(logProgress, {
      type: "snapshot_retry_budget_exhausted",
      stage: "snapshot_writes",
      ...batchDiagnostics
    })
    return {
      ...counters,
      blocked: true,
      blockedReason: "retry_budget_exhausted",
      shouldRetryLater: false,
      retryBudgetExhausted: true,
      pacing
    }
  }

  if (isSnapshotCooldownActive(sourceState, nowMs)) {
    sourceState.batchesSkippedDueToCooldown = Math.max(
      Number(sourceState.batchesSkippedDueToCooldown || 0),
      0
    ) + 1
    counters.snapshotBatchesSkippedDueToCooldown += 1
    counters.snapshotBatchesSkippedDueToCooldownByCategory[category] += 1
    const pacing = currentPacingState()
    const batchDiagnostics = {
      ...batchMeta,
      reasonCounts: batchReasonCounts,
      blockedReason: "active_cooldown_retry_later",
      pacing
    }
    counters.snapshotBatchDiagnostics.push(batchDiagnostics)
    emitProgress(logProgress, {
      type: "snapshot_cooldown_skip",
      stage: "snapshot_writes",
      ...batchDiagnostics
    })
    return {
      ...counters,
      blocked: true,
      blockedReason: "active_cooldown_retry_later",
      shouldRetryLater: true,
      retryBudgetExhausted: false,
      pacing
    }
  }

  const chunks = chunkArray(targetSkins, snapshotBatchSize)
  let blocked = false
  let blockedReason = null
  let shouldRetryLater = false
  let retryBudgetExhausted = false
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
      rowOutcomes.push({
        marketHashName,
        category,
        reason,
        refreshed: reason === "snapshot_write_succeeded"
      })
      if (
        reason === "snapshot_write_succeeded" &&
        counters.snapshotRefreshedNamesByCategory[category] instanceof Set
      ) {
        counters.snapshotRefreshedNamesByCategory[category].add(marketHashName)
      }
    }

    if (batchReasonCounts.snapshot_rate_limited > 0) {
      const pacingUpdate = applySnapshotRateLimitState(snapshotPacingState, category, {
        nowMs,
        snapshotPacingOverrides
      })
      counters.snapshotCooldownAppliedCount += 1
      counters.snapshotCooldownAppliedByCategory[category] += 1
      if (pacingUpdate.retryBudgetExhausted) {
        counters.snapshotRetryBudgetExhaustedCount += 1
        counters.snapshotRetryBudgetExhaustedByCategory[category] += 1
      }
      blocked = true
      retryBudgetExhausted = Boolean(pacingUpdate.retryBudgetExhausted)
      blockedReason = retryBudgetExhausted
        ? "retry_budget_exhausted"
        : "active_cooldown_retry_later"
      shouldRetryLater = !retryBudgetExhausted
      emitProgress(logProgress, {
        type: "snapshot_cooldown_applied",
        stage: "snapshot_writes",
        ...batchMeta,
        snapshotChunkIndex: chunkIndex,
        snapshotChunkSize: chunk.length,
        cooldownMsApplied: pacingUpdate.cooldownMsApplied,
        nextSafeRetryAt: pacingUpdate.nextSafeRetryAt,
        retriesRemaining: pacingUpdate.retriesRemaining,
        retryBudgetExhausted
      })
      break
    }
  }

  const pacing = currentPacingState()
  const batchDiagnostics = {
    ...batchMeta,
    reasonCounts: batchReasonCounts,
    ...summarizeSnapshotReasonCounts(batchReasonCounts),
    blockedReason,
    pacing
  }
  counters.snapshotBatchDiagnostics.push(batchDiagnostics)
  emitProgress(logProgress, {
    type: "snapshot_batch_summary",
    stage: "snapshot_writes",
    ...batchDiagnostics
  })

  return {
    ...counters,
    blocked,
    blockedReason,
    shouldRetryLater,
    retryBudgetExhausted,
    pacing,
    rowOutcomes
  }
}

function needsQuoteRepairRefresh(row = {}) {
  const marketCoverageCount = Math.max(
    Number(row?.market_coverage_count ?? row?.marketCoverageCount ?? 0),
    0
  )
  const referencePrice = toFiniteOrNull(row?.reference_price ?? row?.referencePrice)
  const quoteFetchedAt = toIsoOrNull(row?.quote_fetched_at || row?.quoteFetchedAt)
  const lastSignalAt = toIsoOrNull(row?.last_market_signal_at || row?.lastMarketSignalAt)
  const latestQuoteSignal = pickLatestIso(quoteFetchedAt, lastSignalAt)
  return (
    marketCoverageCount <= 0 ||
    referencePrice == null ||
    !quoteFetchedAt ||
    !isFreshWithinHours(latestQuoteSignal, DEFAULT_HEALTH_WINDOW_HOURS)
  )
}

function needsSnapshotRepairRefresh(row = {}) {
  const snapshotCapturedAt = toIsoOrNull(row?.snapshot_captured_at || row?.snapshotCapturedAt)
  const snapshotStale = row?.snapshot_stale == null ? Boolean(row?.snapshotStale) : Boolean(row.snapshot_stale)
  const referencePrice = toFiniteOrNull(row?.reference_price ?? row?.referencePrice)
  return (
    !snapshotCapturedAt ||
    snapshotStale ||
    referencePrice == null ||
    !isFreshWithinHours(snapshotCapturedAt, DEFAULT_HEALTH_WINDOW_HOURS)
  )
}

async function repairCatalogRows(rows = [], options = {}) {
  const repairRows = dedupeRowsByMarketHashName(rows).filter((row) => {
    const category = normalizeText(row?.category || row?.itemCategory).toLowerCase()
    return RECOVERY_CATEGORIES.includes(category)
  })
  if (!repairRows.length) {
    return {
      attemptedRows: 0,
      quoteRowsSelected: 0,
      snapshotRowsSelected: 0,
      quoteRefresh: createAttemptCounter({
        captureQuoteRowOutcomes: options.collectQuoteRowOutcomes === true
      }),
      snapshotRefresh: {
        ...createAttemptCounter(),
        blocked: false,
        blockedReason: null,
        shouldRetryLater: false,
        retryBudgetExhausted: false,
        pacing: {}
      },
      processedMarketHashNames: []
    }
  }

  const marketHashNames = repairRows
    .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
    .filter(Boolean)
  const skins = await skinRepo.getByMarketHashNames(marketHashNames).catch(() => [])
  const skinsByName = Object.fromEntries(
    (Array.isArray(skins) ? skins : [])
      .map((skin) => [normalizeText(skin?.market_hash_name), skin])
      .filter(([marketHashName]) => Boolean(marketHashName))
  )
  const quoteRows = repairRows.filter((row) => needsQuoteRepairRefresh(row))
  const snapshotRows = repairRows.filter((row) => needsSnapshotRepairRefresh(row))
  const quoteRefresh =
    quoteRows.length > 0
      ? await refreshQuotes(quoteRows, {
          ...options,
          batchMeta: {
            lane: "enrichment_repair",
            ...((options?.batchMeta && typeof options.batchMeta === "object") ? options.batchMeta : {})
          }
        })
      : createAttemptCounter({
          captureQuoteRowOutcomes: options.collectQuoteRowOutcomes === true
        })

  let snapshotRefresh = {
    ...createAttemptCounter(),
    blocked: false,
    blockedReason: null,
    shouldRetryLater: false,
    retryBudgetExhausted: false,
    pacing: {},
    rowOutcomes: []
  }

  for (const category of RECOVERY_CATEGORIES) {
    const categoryRows = snapshotRows.filter(
      (row) => normalizeText(row?.category || row?.itemCategory).toLowerCase() === category
    )
    if (!categoryRows.length) continue
    const categoryRefresh = await refreshSnapshots(categoryRows, skinsByName, {
      ...options,
      batchMeta: {
        category,
        lane: "enrichment_repair",
        ...((options?.batchMeta && typeof options.batchMeta === "object") ? options.batchMeta : {})
      }
    })
    snapshotRefresh = {
      ...snapshotRefresh,
      ...mergeAttemptCounters(snapshotRefresh, categoryRefresh),
      blocked: Boolean(snapshotRefresh.blocked || categoryRefresh.blocked),
      blockedReason: categoryRefresh.blockedReason || snapshotRefresh.blockedReason || null,
      shouldRetryLater: Boolean(snapshotRefresh.shouldRetryLater || categoryRefresh.shouldRetryLater),
      retryBudgetExhausted: Boolean(
        snapshotRefresh.retryBudgetExhausted || categoryRefresh.retryBudgetExhausted
      ),
      pacing:
        categoryRefresh.pacing && typeof categoryRefresh.pacing === "object"
          ? { ...(snapshotRefresh.pacing || {}), [category]: categoryRefresh.pacing }
          : snapshotRefresh.pacing,
      rowOutcomes: [
        ...(Array.isArray(snapshotRefresh.rowOutcomes) ? snapshotRefresh.rowOutcomes : []),
        ...(Array.isArray(categoryRefresh.rowOutcomes) ? categoryRefresh.rowOutcomes : [])
      ]
    }
  }

  return {
    attemptedRows: repairRows.length,
    quoteRowsSelected: quoteRows.length,
    snapshotRowsSelected: snapshotRows.length,
    quoteRefresh,
    snapshotRefresh,
    processedMarketHashNames: marketHashNames
  }
}

function applyWeaponSkinVerificationOutcomes(
  rowOutcomes = [],
  weaponSkinVerificationState = {},
  options = {}
) {
  const nowMs = Number(options.nowMs || Date.now())
  const defaults = getWeaponSkinVerificationDefaults(options)
  const cooldownMs = Math.max(Number(defaults.cooldownMs || DEFAULT_WEAPON_SKIN_VERIFICATION_COOLDOWN_MS), 1000)
  const rateLimitRetryAt = toIsoOrNull(options.retryAfterIso)
  const selectedFallbackNames = new Set(
    (Array.isArray(options.selectedFallbackNames) ? options.selectedFallbackNames : [])
      .map((value) => normalizeText(value))
      .filter(Boolean)
  )
  const diagnostics = {
    attemptedVerificationRows: 0,
    successfulVerificationRows: 0,
    fallbackProbeRowsAttempted: 0,
    fallbackProbeRowsSuccessful: 0,
    cooledDownRows: 0,
    retryBudgetBlockedRows: 0
  }

  for (const outcome of Array.isArray(rowOutcomes) ? rowOutcomes : []) {
    const marketHashName = normalizeText(outcome?.marketHashName)
    const category = normalizeText(outcome?.category).toLowerCase()
    if (category !== "weapon_skin" || !marketHashName) continue

    const rowState = getWeaponSkinVerificationRowState(
      weaponSkinVerificationState,
      marketHashName,
      options
    )
    const reason = normalizeText(outcome?.reason) || "snapshot_source_request_failed"
    const fallbackProbeRow = selectedFallbackNames.has(marketHashName)
    rowState.lastResult = reason
    if (reason !== "snapshot_missing_skin_mapping") {
      rowState.lastAttemptAt = new Date(nowMs).toISOString()
      diagnostics.attemptedVerificationRows += 1
      if (fallbackProbeRow) {
        diagnostics.fallbackProbeRowsAttempted += 1
      }
    }

    if (reason === "snapshot_write_succeeded") {
      rowState.verifiedFresh = true
      rowState.cooldownUntil = null
      diagnostics.successfulVerificationRows += 1
      if (fallbackProbeRow) {
        diagnostics.fallbackProbeRowsSuccessful += 1
      }
      continue
    }

    if (reason === "snapshot_write_skipped" || reason === "snapshot_missing_skin_mapping") {
      continue
    }

    rowState.verifiedFresh = false
    rowState.retriesRemaining = Math.max(Number(rowState.retriesRemaining || defaults.retryBudget) - 1, 0)
    rowState.retryBudgetExhausted = rowState.retriesRemaining <= 0
    rowState.cooldownUntil =
      rateLimitRetryAt && reason === "snapshot_rate_limited"
        ? rateLimitRetryAt
        : new Date(nowMs + cooldownMs).toISOString()
    diagnostics.cooledDownRows += 1
    if (rowState.retryBudgetExhausted) {
      diagnostics.retryBudgetBlockedRows += 1
    }
  }

  return diagnostics
}

async function recomputeVerifiedWeaponSkinRows(rows = [], rowOutcomes = [], options = {}) {
  const selectedFallbackNames = new Set(
    (Array.isArray(options.selectedFallbackNames) ? options.selectedFallbackNames : [])
      .map((value) => normalizeText(value))
      .filter(Boolean)
  )
  const verifiedNames = Array.from(
    new Set(
      (Array.isArray(rowOutcomes) ? rowOutcomes : [])
        .filter((outcome) => normalizeText(outcome?.reason) === "snapshot_write_succeeded")
        .map((outcome) => normalizeText(outcome?.marketHashName))
        .filter(Boolean)
    )
  )
  if (!verifiedNames.length) {
    return {
      recomputedVerifiedRows: 0,
      verifiedScannableRows: 0,
      weaponSkinScannerSourceIncreased: false
    }
  }

  const verifiedRows = (Array.isArray(rows) ? rows : []).filter((row) =>
    verifiedNames.includes(normalizeText(row?.market_hash_name || row?.marketHashName))
  )
  if (!verifiedRows.length) {
    return {
      recomputedVerifiedRows: 0,
      verifiedScannableRows: 0,
      weaponSkinScannerSourceIncreased: false
    }
  }

  await runLoggedStage(
    "verified_row_recompute",
    {
      logProgress: options.logProgress,
      meta: {
        ...(options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}),
        rowCount: verifiedRows.length,
        marketHashNames: verifiedNames
      }
    },
    async () =>
      marketSourceCatalogService.recomputeCandidateReadinessRows(verifiedRows, {
        categories: ["weapon_skin"]
      })
  )

  const updatedRows = await runLoggedStage(
    "verified_row_readback",
    {
      logProgress: options.logProgress,
      meta: {
        ...(options.batchMeta && typeof options.batchMeta === "object" ? options.batchMeta : {}),
        rowCount: verifiedNames.length
      }
    },
    async () =>
      marketSourceCatalogService.getCatalogRowsByMarketHashNames(verifiedNames, {
        categories: ["weapon_skin"],
        activeOnly: true,
        tradableOnly: true
      })
  )

  const verifiedScannableRows = (Array.isArray(updatedRows) ? updatedRows : []).filter((row) => {
    const compatible = marketSourceCatalogService.resolveCompatibleCatalogStatusFields(row)
    return normalizeText(compatible?.catalogStatus || row?.catalog_status).toLowerCase() === "scannable"
  }).length
  const fallbackVerifiedNames = verifiedNames.filter((marketHashName) =>
    selectedFallbackNames.has(marketHashName)
  )
  const fallbackProbeVerifiedScannableRows = (Array.isArray(updatedRows) ? updatedRows : []).filter((row) => {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!selectedFallbackNames.has(marketHashName)) return false
    const compatible = marketSourceCatalogService.resolveCompatibleCatalogStatusFields(row)
    return normalizeText(compatible?.catalogStatus || row?.catalog_status).toLowerCase() === "scannable"
  }).length

  if (
    options.weaponSkinVerificationState &&
    typeof options.weaponSkinVerificationState === "object"
  ) {
    for (const marketHashName of verifiedNames) {
      const rowState = getWeaponSkinVerificationRowState(
        options.weaponSkinVerificationState,
        marketHashName,
        options
      )
      rowState.recomputedAt = new Date().toISOString()
    }
  }

  return {
    recomputedVerifiedRows: verifiedRows.length,
    fallbackProbeRowsRecomputed: fallbackVerifiedNames.length,
    verifiedScannableRows,
    fallbackProbeVerifiedScannableRows,
    weaponSkinScannerSourceIncreased: verifiedScannableRows > 0,
    fallbackProbeWeaponSkinScannerSourceIncreased: fallbackProbeVerifiedScannableRows > 0
  }
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
  const category =
    normalizeText(batchMeta?.category).toLowerCase() ||
    normalizeText(rows?.[0]?.category || rows?.[0]?.itemCategory).toLowerCase() ||
    "weapon_skin"
  const selectionDiagnostics =
    options.selectionDiagnostics && typeof options.selectionDiagnostics === "object"
      ? options.selectionDiagnostics
      : {}
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
        lookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS,
        markets: getScannerCoverageMarkets()
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
  const weaponSkinVerification = createWeaponSkinVerificationAggregate()
  if (category === "weapon_skin") {
    mergeWeaponSkinVerificationAggregate(
      weaponSkinVerification,
      selectionDiagnostics?.weaponSkinVerification || {}
    )
    mergeWeaponSkinVerificationAggregate(
      weaponSkinVerification,
      applyWeaponSkinVerificationOutcomes(
        snapshotRefresh?.rowOutcomes || [],
        options.weaponSkinVerificationState,
        {
          ...options,
          retryAfterIso: snapshotRefresh?.pacing?.nextSafeRetryAt || null,
          selectedFallbackNames:
            selectionDiagnostics?.weaponSkinVerification?.selectedFallbackNames || []
        }
      )
    )
    mergeWeaponSkinVerificationAggregate(
      weaponSkinVerification,
      await recomputeVerifiedWeaponSkinRows(rows, snapshotRefresh?.rowOutcomes || [], {
        ...options,
        logProgress,
        batchMeta,
        selectedFallbackNames:
          selectionDiagnostics?.weaponSkinVerification?.selectedFallbackNames || []
      })
    )
  }

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
        lookbackHours: DEFAULT_QUOTE_LOOKBACK_HOURS,
        markets: getScannerCoverageMarkets()
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
    weaponSkinVerification: category === "weapon_skin" ? weaponSkinVerification : null,
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

function buildCategoryHealthGate({
  categories = RECOVERY_CATEGORIES,
  healthGate = {},
  snapshotPacingSummary = {},
  categoryProgressState = {}
} = {}) {
  const scopedCategories = normalizeRecoveryCategories(categories)
  const scopedCategorySet = new Set(scopedCategories)
  const byCategory = {}
  const healthyCategories = []
  const blockedCategories = []

  for (const category of RECOVERY_CATEGORIES) {
    const bucket = healthGate?.byCategory?.[category] || {}
    const pacing = snapshotPacingSummary?.[category] || {}
    const progress = categoryProgressState?.[category] || {}
    const reasons = Array.isArray(bucket?.reasons) ? [...bucket.reasons] : []
    const progressBlockedReason = normalizeText(progress?.blockedReason)
    if (progressBlockedReason && !reasons.includes(progressBlockedReason)) {
      reasons.push(progressBlockedReason)
    }

    const inScope = scopedCategorySet.has(category)
    const totalRows = Number(bucket?.totalRows || 0)
    const categoryHealthy = inScope && Boolean(bucket?.healthyEnough)
    const blocked = inScope && !categoryHealthy

    byCategory[category] = {
      inScope,
      evaluated: inScope && totalRows > 0,
      healthyEnough: categoryHealthy,
      blocked,
      totalRows,
      freshCoverageRows: Number(bucket?.freshCoverageRows || 0),
      freshCoverageRate: Number(bucket?.freshCoverageRate || 0),
      freshSnapshotRows: Number(bucket?.freshSnapshotRows || 0),
      freshSnapshotRate: Number(bucket?.freshSnapshotRate || 0),
      quoteHealthy: Boolean(bucket?.quoteHealthy),
      snapshotHealthy: Boolean(bucket?.snapshotHealthy),
      reasons,
      cooldownActive: Boolean(pacing?.cooldownActive),
      nextSafeRetryAt: pacing?.nextSafeRetryAt || null,
      retriesRemaining: Number(pacing?.retriesRemaining || 0)
    }

    if (!inScope) continue
    if (categoryHealthy) {
      healthyCategories.push(category)
    } else {
      blockedCategories.push(category)
    }
  }

  return {
    healthyEnough: blockedCategories.length === 0 && healthyCategories.length > 0,
    evaluatedCategories: scopedCategories,
    healthyCategories,
    blockedCategories,
    byCategory
  }
}

function buildRecomputePlan({
  categories = RECOVERY_CATEGORIES,
  categoryHealthGate = {},
  pauseReason = null,
  failedStage = null,
  recomputeEnabled = true
} = {}) {
  const scopedCategories = normalizeRecoveryCategories(categories)
  const healthyCategories = Array.isArray(categoryHealthGate?.healthyCategories)
    ? [...categoryHealthGate.healthyCategories]
    : []
  const blockedCategories = Array.isArray(categoryHealthGate?.blockedCategories)
    ? [...categoryHealthGate.blockedCategories]
    : []
  const plan = {
    recomputeMode: "none",
    recomputedCategories: [],
    blockedCategories,
    opportunityScanSafeToResume: false,
    opportunityScanResumeCategories: healthyCategories,
    skippedReason: null
  }

  if (!recomputeEnabled) {
    plan.skippedReason = "skip_requested"
    return plan
  }
  if (failedStage) {
    plan.skippedReason = "recovery_failed"
    return plan
  }
  if (pauseReason === "manual_max_batches_pause") {
    plan.skippedReason = "manual_max_batches_pause"
    return plan
  }
  if (!healthyCategories.length) {
    plan.skippedReason = "upstream_not_healthy_enough"
    return plan
  }

  plan.recomputedCategories = healthyCategories
  plan.recomputeMode =
    healthyCategories.length === scopedCategories.length &&
    scopedCategories.every((category) => healthyCategories.includes(category))
      ? "full"
      : "partial"
  plan.opportunityScanSafeToResume = healthyCategories.length > 0
  return plan
}

function summarizeCatalogRecompute(diagnostics = {}, plan = {}) {
  const sourceCatalog = diagnostics?.sourceCatalog || diagnostics || {}
  const byCategory = sourceCatalog?.byCategory && typeof sourceCatalog.byCategory === "object"
    ? sourceCatalog.byCategory
    : {}
  const scannableRowsByCategory = emptyCategoryNumberMap(0)
  const eligibleTradableRowsByCategory = emptyCategoryNumberMap(0)
  const nearEligibleRowsByCategory = emptyCategoryNumberMap(0)
  const scannerSourceSizeByCategory = emptyCategoryNumberMap(0)

  for (const category of RECOVERY_CATEGORIES) {
    const bucket = byCategory?.[category] || {}
    scannableRowsByCategory[category] = Number(
      sourceCatalog?.scannableRowsByCategory?.[category] || bucket?.scannable || 0
    )
    eligibleTradableRowsByCategory[category] = Number(
      sourceCatalog?.eligibleRowsByCategory?.[category] || bucket?.eligible || 0
    )
    nearEligibleRowsByCategory[category] = Number(
      sourceCatalog?.nearEligibleRowsByCategory?.[category] || bucket?.nearEligible || 0
    )
    scannerSourceSizeByCategory[category] = Number(
      sourceCatalog?.scannerSourceSizeByCategory?.[category] ||
        sourceCatalog?.scanner_source_size_by_category?.[category] ||
        bucket?.scannable ||
        0
    )
  }

  const hasExplicitCategoryScannerSource = Object.values(scannerSourceSizeByCategory).some(
    (value) => Number(value || 0) > 0
  )
  const opportunityScanResumeCategories = hasExplicitCategoryScannerSource
    ? (Array.isArray(plan?.opportunityScanResumeCategories)
        ? plan.opportunityScanResumeCategories
        : []
      ).filter((category) => Number(scannerSourceSizeByCategory?.[category] || 0) > 0)
    : Array.isArray(plan?.opportunityScanResumeCategories)
      ? [...plan.opportunityScanResumeCategories]
      : []

  return {
    executed: true,
    recomputeMode: plan?.recomputeMode || "full",
    recomputedCategories: Array.isArray(plan?.recomputedCategories) ? plan.recomputedCategories : [],
    blockedCategories: Array.isArray(plan?.blockedCategories) ? plan.blockedCategories : [],
    generatedAt: diagnostics?.generatedAt || null,
    scannableRows: Number(sourceCatalog?.scannable || 0),
    shadowRows: Number(sourceCatalog?.shadow || 0),
    blockedRows: Number(sourceCatalog?.blocked || 0),
    eligibleTradableRows: Number(sourceCatalog?.eligibleTradableRows || 0),
    nearEligibleRows: Number(sourceCatalog?.nearEligibleRows || 0),
    scanEligibleRows: Number(sourceCatalog?.eligibleRows || 0),
    scannerSourceSize: Number(
      sourceCatalog?.scanner_source_size || sourceCatalog?.scannerSourceSize || 0
    ),
    scannableRowsByCategory,
    eligibleTradableRowsByCategory,
    nearEligibleRowsByCategory,
    scannerSourceSizeByCategory,
    opportunityScanSafeToResume: opportunityScanResumeCategories.length > 0,
    opportunityScanResumeCategories
  }
}

async function runFreshnessRecovery(options = {}) {
  const logProgress = options.logProgress
  const resumeState =
    typeof options.resumeState === "string"
      ? decodeResumeStateToken(options.resumeState)
      : options.resumeState && typeof options.resumeState === "object"
        ? options.resumeState
        : {}
  const categories = normalizeRecoveryCategories(
    Array.isArray(options.categories) && options.categories.length
      ? options.categories
      : resumeState?.categories
  )
  const targetLimit = Math.max(Number(options.limit || DEFAULT_TARGET_LIMIT), 1)
  const selectionBatchSize = Math.max(
    Number(options.selectionBatchSize || DEFAULT_SELECTION_BATCH_SIZE),
    1
  )
  const nowMs = Number(options.nowMs || Date.now())
  const maxBatches =
    Number.isFinite(Number(options.maxBatches)) && Number(options.maxBatches) > 0
      ? Math.max(Math.round(Number(options.maxBatches)), 1)
      : null
  const targets = buildCategoryTargets(targetLimit, categories)
  const categoryProgressState = createCategoryProgressState(categories, targets, {
    ...options,
    resumeState
  })
  const snapshotPacingState = createSnapshotPacingState(
    resumeState?.snapshotPacingState || options.snapshotPacingState || {},
    categories,
    options.snapshotPacingOverrides
  )
  const weaponSkinVerificationState = createWeaponSkinVerificationState(
    resumeState?.weaponSkinVerificationState || options.weaponSkinVerificationState || {}
  )
  const preRefresh = createFreshnessSummary()
  const postRefreshBase = createFreshnessSummary()
  const aggregateQuoteRefresh = createAttemptCounter()
  const aggregateSnapshotRefresh = createAttemptCounter()
  const aggregateWeaponSkinVerification = createWeaponSkinVerificationAggregate()
  const processedRowsByCategory = emptyCategoryNumberMap(0)
  let processedRows = 0
  let completedBatches = 0
  let paused = false
  let pauseReason = null
  let timedOut = false
  let failedStage = null
  let errorSummary = null
  const cooledDownCategories = new Set()
  const exhaustedRetryBudgetCategories = new Set()

  emitProgress(logProgress, {
    type: "recovery_plan",
    categories,
    targets,
    targetLimit,
    selectionBatchSize,
    maxBatches,
    startCategory: buildNextRecoveryCursor(categories, categoryProgressState, targets).nextCategory,
    startOffset: buildNextRecoveryCursor(categories, categoryProgressState, targets).nextOffset,
    resumeStatePresent: Boolean(Object.keys(resumeState || {}).length),
    snapshotPacingState: summarizeSnapshotPacingState(
      snapshotPacingState,
      categories,
      nowMs,
      options.snapshotPacingOverrides
    )
  })

  batchLoop: for (const category of categories) {
    const progressEntry = categoryProgressState[category] || {
      nextOffset: 0,
      done: false,
      blockedReason: null
    }
    let categoryOffset = Math.max(Number(progressEntry.nextOffset || 0), 0)
    const categoryTarget = Math.max(Number(targets?.[category] || 0), 0)

    while (categoryOffset < categoryTarget) {
      if (maxBatches != null && completedBatches >= maxBatches) {
        paused = true
        pauseReason = "manual_max_batches_pause"
        progressEntry.nextOffset = categoryOffset
        progressEntry.done = false
        break batchLoop
      }

      const batchLimit = Math.max(
        Math.min(selectionBatchSize, categoryTarget - categoryOffset),
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
        const { rows, selectionDiagnostics } = await selectRecoveryRowBatch({
          category,
          limit: batchLimit,
          offset: categoryOffset,
          batchIndex: completedBatches,
          categoryTarget,
          logProgress,
          nowMs,
          weaponSkinVerificationState,
          weaponSkinVerificationLimit: options.weaponSkinVerificationLimit,
          weaponSkinVerificationCandidatePoolLimit:
            options.weaponSkinVerificationCandidatePoolLimit,
          weaponSkinVerificationRetryBudget: options.weaponSkinVerificationRetryBudget,
          weaponSkinVerificationCooldownMs: options.weaponSkinVerificationCooldownMs
        })

        if (!rows.length) {
          mergeWeaponSkinVerificationAggregate(
            aggregateWeaponSkinVerification,
            selectionDiagnostics?.weaponSkinVerification || {}
          )
          const selectionBlockedReason = normalizeText(selectionDiagnostics?.blockedReason) || null
          if (selectionBlockedReason) {
            progressEntry.nextOffset = categoryOffset
            progressEntry.done = false
            progressEntry.blockedReason = selectionBlockedReason
            if (selectionDiagnostics?.shouldRetryLater) {
              cooledDownCategories.add(category)
            }
            if (selectionDiagnostics?.retryBudgetExhausted) {
              exhaustedRetryBudgetCategories.add(category)
            }
            emitProgress(logProgress, {
              type: "category_snapshot_blocked",
              ...batchMeta,
              rowCount: 0,
              blockedReason: selectionBlockedReason,
              nextSafeRetryAt:
                snapshotPacingState?.byCategory?.[category]?.bySource?.steam_market_overview
                  ?.nextSafeRetryAt || null,
              retriesRemaining: Number(
                snapshotPacingState?.byCategory?.[category]?.bySource?.steam_market_overview
                  ?.retriesRemaining || 0
              )
            })
            break
          }
          emitProgress(logProgress, {
            type: "batch_empty",
            ...batchMeta
          })
          progressEntry.nextOffset = categoryTarget
          progressEntry.done = true
          progressEntry.blockedReason = null
          break
        }

        const batchResult = await processRecoveryBatch(rows, {
          ...options,
          resumeState,
          nowMs,
          snapshotPacingState,
          weaponSkinVerificationState,
          logProgress,
          batchMeta,
          selectionDiagnostics
        })

        mergeFreshnessSummary(preRefresh, batchResult.preRefresh)
        mergeFreshnessSummary(postRefreshBase, batchResult.postRefreshBase)
        mergeAttemptCounters(aggregateQuoteRefresh, batchResult.quoteRefresh)
        mergeAttemptCounters(aggregateSnapshotRefresh, batchResult.snapshotRefresh)
        mergeWeaponSkinVerificationAggregate(
          aggregateWeaponSkinVerification,
          batchResult.weaponSkinVerification || {}
        )

        const rowCount = Number(batchResult.rowCount || rows.length || 0)
        processedRows += rowCount
        processedRowsByCategory[category] =
          Number(processedRowsByCategory?.[category] || 0) + rowCount
        completedBatches += 1

        const snapshotBlocked = Boolean(batchResult.snapshotRefresh?.blocked)
        if (snapshotBlocked) {
          progressEntry.nextOffset = categoryOffset
          progressEntry.done = false
          progressEntry.blockedReason =
            normalizeText(batchResult.snapshotRefresh?.blockedReason) || null
          if (batchResult.snapshotRefresh?.shouldRetryLater) {
            cooledDownCategories.add(category)
          }
          if (batchResult.snapshotRefresh?.retryBudgetExhausted) {
            exhaustedRetryBudgetCategories.add(category)
          }
          emitProgress(logProgress, {
            type: "category_snapshot_blocked",
            ...batchMeta,
            rowCount,
            blockedReason: batchResult.snapshotRefresh?.blockedReason || null,
            nextSafeRetryAt: batchResult.snapshotRefresh?.pacing?.nextSafeRetryAt || null,
            retriesRemaining: Number(batchResult.snapshotRefresh?.pacing?.retriesRemaining || 0)
          })
          break
        }

        categoryOffset += rowCount
        if (selectionDiagnostics?.completeCategoryAfterBatch) {
          categoryOffset = categoryTarget
        }
        progressEntry.nextOffset = categoryOffset
        progressEntry.done = categoryOffset >= categoryTarget
        progressEntry.blockedReason = null

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
        progressEntry.nextOffset = categoryOffset
        progressEntry.done = false
        progressEntry.blockedReason = "hard_failure"
        break batchLoop
      }
    }
  }

  const nextCursor = buildNextRecoveryCursor(categories, categoryProgressState, targets)
  const hasRemainingWork = Boolean(nextCursor.nextCategory)

  if (failedStage) {
    paused = true
    pauseReason = "hard_failure"
  } else if (!pauseReason && exhaustedRetryBudgetCategories.size) {
    paused = true
    pauseReason = "retry_budget_exhausted"
  } else if (!pauseReason && cooledDownCategories.size) {
    paused = true
    pauseReason = "active_cooldown_retry_later"
  } else if (!pauseReason && hasRemainingWork) {
    paused = true
    pauseReason = "active_cooldown_retry_later"
  } else if (!pauseReason) {
    pauseReason = "work_completed"
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
  const snapshotPacingSummary = summarizeSnapshotPacingState(
    snapshotPacingState,
    categories,
    nowMs,
    options.snapshotPacingOverrides
  )
  const completed = !paused && !failedStage && !hasRemainingWork
  healthGate.recoveryComplete = completed
  if (!healthGate.recoveryComplete) {
    healthGate.healthyEnough = false
    if (pauseReason) {
      healthGate.reasons.unshift(`recovery_paused:${pauseReason}`)
    }
    if (failedStage) {
      healthGate.reasons.unshift(`recovery_failed:${failedStage}`)
    } else if (!pauseReason) {
      healthGate.reasons.unshift("recovery_incomplete")
    }
  }
  const categoryHealthGate = buildCategoryHealthGate({
    categories,
    healthGate,
    snapshotPacingSummary,
    categoryProgressState
  })
  const recomputePlan = buildRecomputePlan({
    categories,
    categoryHealthGate,
    pauseReason,
    failedStage,
    recomputeEnabled: options.recompute !== false
  })

  let catalogRecompute = {
    executed: false,
    recomputeMode: recomputePlan.recomputeMode,
    recomputedCategories: recomputePlan.recomputedCategories,
    blockedCategories: recomputePlan.blockedCategories,
    opportunityScanSafeToResume: false,
    opportunityScanResumeCategories: [],
    skippedReason: recomputePlan.skippedReason
  }

  if (recomputePlan.recomputedCategories.length) {
    try {
      const diagnostics = await runLoggedStage(
        "force_recompute",
        {
          logProgress,
          meta: {
            recomputeMode: recomputePlan.recomputeMode,
            recomputedCategories: recomputePlan.recomputedCategories,
            blockedCategories: recomputePlan.blockedCategories,
            targetUniverseSize: Number(
              options.targetUniverseSize || arbitrageDefaultUniverseLimit || 3000
            )
          }
        },
        async () =>
          marketSourceCatalogService.prepareSourceCatalog({
            forceRefresh: true,
            categories: recomputePlan.recomputedCategories,
            targetUniverseSize: Number(
              options.targetUniverseSize || arbitrageDefaultUniverseLimit || 3000
            )
          })
      )
      catalogRecompute = summarizeCatalogRecompute(diagnostics, recomputePlan)
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
        recomputeMode: recomputePlan.recomputeMode,
        recomputedCategories: recomputePlan.recomputedCategories,
        blockedCategories: recomputePlan.blockedCategories,
        opportunityScanSafeToResume: false,
        opportunityScanResumeCategories: [],
        skippedReason: "force_recompute_failed"
      }
    }
  }

  const checkpoint = buildCheckpointState({
    categories,
    targets,
    categoryProgressState,
    snapshotPacingState,
    weaponSkinVerificationState,
    completedBatches,
    processedRows,
    processedRowsByCategory,
    pauseReason,
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
    pauseReason,
    timedOut,
    completedBatches,
    processedRows,
    processedRowsByCategory,
    failedStage,
    error: errorSummary,
    checkpoint,
    preRefresh,
    postRefresh,
    snapshotPacing: snapshotPacingSummary,
    healthGate,
    categoryHealthGate,
    quoteRefresh: {
      rowsAttempted: Number(postRefresh?.quote?.attemptedRows || 0),
      rowsRefreshed: Number(postRefresh?.quote?.refreshedRows || 0),
      rowsStillStale: Number(postRefresh?.quote?.staleRows || 0),
      rowsMissing: Number(postRefresh?.quote?.missingRows || 0),
      quoteRowsInserted: Number(aggregateQuoteRefresh.quoteRowsInserted || 0),
      marketPriceRowsUpserted: Number(aggregateQuoteRefresh.marketPriceRowsUpserted || 0),
      markets_enabled_for_scanner: Array.isArray(
        aggregateQuoteRefresh.markets_enabled_for_scanner
      )
        ? aggregateQuoteRefresh.markets_enabled_for_scanner
        : [],
      markets_degraded_for_scanner: Array.isArray(
        aggregateQuoteRefresh.markets_degraded_for_scanner
      )
        ? aggregateQuoteRefresh.markets_degraded_for_scanner
        : [],
      markets_disabled_for_scanner: Array.isArray(
        aggregateQuoteRefresh.markets_disabled_for_scanner
      )
        ? aggregateQuoteRefresh.markets_disabled_for_scanner
        : [],
      market_failure_reason_counts:
        aggregateQuoteRefresh.market_failure_reason_counts || {},
      steam_rate_limited_count: Number(aggregateQuoteRefresh.steam_rate_limited_count || 0),
      steam_cached_fallback_count: Number(
        aggregateQuoteRefresh.steam_cached_fallback_count || 0
      ),
      steam_unavailable_count: Number(aggregateQuoteRefresh.steam_unavailable_count || 0),
      bySource: Object.fromEntries(
        Object.entries(aggregateQuoteRefresh.quoteSourceDiagnostics || {}).map(([source, diag]) => [
          source,
          {
            scanner_market_mode: diag?.scanner_market_mode || null,
            refreshed: Number(diag?.refreshed || 0),
            failed: Number(diag?.failed || 0),
            error: diag?.error || null,
            stateCounts: diag?.stateCounts || {},
            scanner_status_counts: diag?.scanner_status_counts || {},
            live_request_budget: diag?.live_request_budget ?? null,
            live_request_attempted: Number(diag?.live_request_attempted || 0),
            live_request_skipped_due_to_budget: Number(
              diag?.live_request_skipped_due_to_budget || 0
            ),
            market_enabled:
              diag?.market_enabled == null ? true : Boolean(diag?.market_enabled),
            credentials_present: diag?.credentials_present ?? null,
            auth_ok: diag?.auth_ok ?? null,
            request_sent: diag?.request_sent ?? null,
            response_status: diag?.response_status ?? null,
            response_parsed: diag?.response_parsed ?? null,
            listings_found: diag?.listings_found ?? null,
            buy_price_present: diag?.buy_price_present ?? null,
            sell_price_present: diag?.sell_price_present ?? null,
            freshness_present: diag?.freshness_present ?? null,
            listing_url_present: diag?.listing_url_present ?? null,
            source_failure_reason: diag?.source_failure_reason ?? null,
            last_success_at: diag?.last_success_at ?? null,
            last_failure_at: diag?.last_failure_at ?? null
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
      cooldownAppliedCount: Number(aggregateSnapshotRefresh.snapshotCooldownAppliedCount || 0),
      batchesSkippedDueToCooldown: Number(
        aggregateSnapshotRefresh.snapshotBatchesSkippedDueToCooldown || 0
      ),
      retryBudgetExhaustedCount: Number(
        aggregateSnapshotRefresh.snapshotRetryBudgetExhaustedCount || 0
      ),
      failureReasons: {
        ...(aggregateSnapshotRefresh.snapshotReasonCounts || createSnapshotReasonMap())
      },
      byCategory: Object.fromEntries(
        categories.map((category) => [
          category,
          {
            reasons: {
              ...(aggregateSnapshotRefresh.snapshotReasonCountsByCategory?.[category] ||
                createSnapshotReasonMap())
            },
            pacing: snapshotPacingSummary?.[category] || null,
            cooldownAppliedCount: Number(
              aggregateSnapshotRefresh.snapshotCooldownAppliedByCategory?.[category] || 0
            ),
            batchesSkippedDueToCooldown: Number(
              aggregateSnapshotRefresh.snapshotBatchesSkippedDueToCooldownByCategory?.[category] || 0
            ),
            retryBudgetExhaustedCount: Number(
              aggregateSnapshotRefresh.snapshotRetryBudgetExhaustedByCategory?.[category] || 0
            ),
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
      nextSafeRetryAt: Object.values(snapshotPacingSummary)
        .map((state) => state?.nextSafeRetryAt || null)
        .filter(Boolean)
        .sort()[0] || null,
      retryCurrentlyUseful: Object.values(snapshotPacingSummary).some(
        (state) => Boolean(state?.retryCurrentlyUseful)
      ),
      retryTemporarilyBlocked: Object.values(snapshotPacingSummary).some(
        (state) => Boolean(state?.retryTemporarilyBlocked)
      ),
      ...summarizeSnapshotReasonCounts(
        aggregateSnapshotRefresh.snapshotReasonCounts || createSnapshotReasonMap()
      )
    },
    weaponSkinVerification: {
      ...aggregateWeaponSkinVerification
    },
    catalogRecompute
  }
}

module.exports = {
  runFreshnessRecovery,
  repairCatalogRows,
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

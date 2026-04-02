const AppError = require("../utils/AppError")
const referenceCatalogRules = require("../config/referenceCatalogRules")
const catalogGenerationRepo = require("../repositories/catalogGenerationRepository")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../repositories/marketUniverseRepository")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const {
  DEFAULT_UNIVERSE_LIMIT,
  OPPORTUNITY_BATCH_RUNTIME_TARGET,
  SCAN_COHORT_CATEGORIES
} = require("./scanner/config")

const CATALOG_SCOPE_CATEGORIES = Object.freeze(
  Array.isArray(referenceCatalogRules?.PRIMARY_GENERATION_CATEGORIES)
    ? referenceCatalogRules.PRIMARY_GENERATION_CATEGORIES.slice()
    : ["weapon_skin", "case", "sticker_capsule"]
)
const ACTIVE_GENERATION_TARGET_RULES =
  referenceCatalogRules?.ACTIVE_GENERATION_TARGET || {}
const HEALTHY_OUTPUT_TARGET_RULES =
  referenceCatalogRules?.HEALTHY_OUTPUT_TARGET || {}
const CANDIDATE_STATUS_SET = new Set([
  "candidate",
  "enriching",
  "near_eligible",
  "eligible",
  "rejected"
])
const CATALOG_STATUS_SET = new Set(["scannable", "shadow", "blocked"])

function normalizeText(value) {
  return String(value || "").trim()
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const timestamp = new Date(text).getTime()
  if (!Number.isFinite(timestamp)) return null
  return new Date(timestamp).toISOString()
}

function normalizeBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value
  const text = normalizeText(value).toLowerCase()
  if (!text) return fallback
  if (["1", "true", "yes", "on", "auto"].includes(text)) return true
  if (["0", "false", "no", "off"].includes(text)) return false
  return fallback
}

function normalizeInteger(value, fallback, min = 0) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), min)
}

function normalizeCategories(values = []) {
  const requested = Array.isArray(values) ? values : [values]
  const normalized = Array.from(
    new Set(
      requested
        .map((value) => normalizeText(value).toLowerCase())
        .filter((value) => CATALOG_SCOPE_CATEGORIES.includes(value))
    )
  )
  return normalized.length ? normalized : CATALOG_SCOPE_CATEGORIES.slice()
}

function normalizeCandidateStatus(value) {
  const text = normalizeText(value).toLowerCase()
  return CANDIDATE_STATUS_SET.has(text) ? text : "candidate"
}

function normalizeCatalogStatus(value) {
  const text = normalizeText(value).toLowerCase()
  return CATALOG_STATUS_SET.has(text) ? text : "shadow"
}

function buildGenerationSnapshot(generation = {}) {
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
      normalizeText(generation?.source_generation_id || generation?.sourceGenerationId) || null,
    diagnosticsSummary:
      generation?.diagnostics_summary && typeof generation.diagnostics_summary === "object"
        ? generation.diagnostics_summary
        : generation?.diagnosticsSummary && typeof generation.diagnosticsSummary === "object"
          ? generation.diagnosticsSummary
          : {}
  }
}

function buildCategorySummaryMap() {
  return Object.fromEntries(
    CATALOG_SCOPE_CATEGORIES.map((category) => [
      category,
      {
        totalRows: 0,
        tradableRows: 0,
        scannerSourceSize: 0,
        eligibleRows: 0,
        nearEligibleRows: 0,
        enrichingRows: 0,
        candidateRows: 0,
        rejectedRows: 0,
        blockedRows: 0,
        shadowRows: 0
      }
    ])
  )
}

function buildStatusSummaryMap(keys = []) {
  return Object.fromEntries((Array.isArray(keys) ? keys : []).map((key) => [key, 0]))
}

function buildTopCounts(counter = {}, limit = 5) {
  return Object.entries(counter || {})
    .filter(([, count]) => Number(count || 0) > 0)
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
    .slice(0, Math.max(Number(limit || 0), 0))
    .map(([key, count]) => ({
      key,
      count: Number(count || 0)
    }))
}

function summarizeCoverageRows(rows = [], generation = null, options = {}) {
  const scopedCategories = normalizeCategories(options.categories)
  const byCategory = buildCategorySummaryMap()
  const candidateStatusCounts = buildStatusSummaryMap([
    "candidate",
    "enriching",
    "near_eligible",
    "eligible",
    "rejected"
  ])
  const catalogStatusCounts = buildStatusSummaryMap(["scannable", "shadow", "blocked"])
  const blockReasonCounts = {}

  let totalRows = 0
  let tradableRows = 0
  let scannerSourceSize = 0
  let eligibleTradableRows = 0
  let nearEligibleRows = 0

  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeText(row?.category).toLowerCase()
    if (!scopedCategories.includes(category)) continue

    const candidateStatus = normalizeCandidateStatus(
      row?.candidate_status ?? row?.candidateStatus
    )
    const catalogStatus = normalizeCatalogStatus(row?.catalog_status ?? row?.catalogStatus)
    const tradable = row?.tradable == null ? true : Boolean(row?.tradable)
    const scanEligible =
      row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row?.scan_eligible)

    totalRows += 1
    if (tradable) tradableRows += 1
    if (candidateStatusCounts[candidateStatus] != null) {
      candidateStatusCounts[candidateStatus] += 1
    }
    if (catalogStatusCounts[catalogStatus] != null) {
      catalogStatusCounts[catalogStatus] += 1
    }

    const categorySummary = byCategory[category]
    if (categorySummary) {
      categorySummary.totalRows += 1
      if (tradable) categorySummary.tradableRows += 1
      if (catalogStatus === "scannable") categorySummary.scannerSourceSize += 1
      if (candidateStatus === "eligible") categorySummary.eligibleRows += 1
      if (candidateStatus === "near_eligible") categorySummary.nearEligibleRows += 1
      if (candidateStatus === "enriching") categorySummary.enrichingRows += 1
      if (candidateStatus === "candidate") categorySummary.candidateRows += 1
      if (candidateStatus === "rejected") categorySummary.rejectedRows += 1
      if (catalogStatus === "blocked") categorySummary.blockedRows += 1
      if (catalogStatus === "shadow") categorySummary.shadowRows += 1
    }

    if (catalogStatus === "scannable") {
      scannerSourceSize += 1
    }
    if (catalogStatus === "scannable" && tradable && scanEligible) {
      eligibleTradableRows += 1
    }
    if (candidateStatus === "near_eligible" && catalogStatus === "scannable") {
      nearEligibleRows += 1
    }

    if (catalogStatus !== "scannable") {
      const reason =
        normalizeText(row?.catalog_block_reason || row?.catalogBlockReason) || "unclassified"
      blockReasonCounts[reason] = Number(blockReasonCounts[reason] || 0) + 1
    }
  }

  const readyCategories = scopedCategories.filter(
    (category) => Number(byCategory?.[category]?.scannerSourceSize || 0) > 0
  )

  return {
    generation: buildGenerationSnapshot(generation),
    categories: scopedCategories,
    totalRows,
    tradableRows,
    scannerSourceSize,
    eligibleTradableRows,
    nearEligibleRows,
    readyCategoryCount: readyCategories.length,
    readyCategories,
    candidateStatusCounts,
    catalogStatusCounts,
    byCategory,
    topBlockReasons: buildTopCounts(blockReasonCounts)
  }
}

function summarizeUniverseRows(rows = [], generation = null, options = {}) {
  const scopedCategories = normalizeCategories(options.categories)
  const byCategory = Object.fromEntries(
    scopedCategories.map((category) => [
      category,
      {
        totalRows: 0
      }
    ])
  )

  let totalRows = 0
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeText(row?.category).toLowerCase()
    if (!scopedCategories.includes(category)) continue
    totalRows += 1
    if (byCategory[category]) {
      byCategory[category].totalRows += 1
    }
  }

  return {
    generation: buildGenerationSnapshot(generation),
    categories: scopedCategories,
    totalRows,
    byCategory
  }
}

function buildCatalogLivenessSummary(summary = {}, options = {}) {
  const activeGenerationTargets =
    options?.activeGenerationTargets && typeof options.activeGenerationTargets === "object"
      ? options.activeGenerationTargets
      : ACTIVE_GENERATION_TARGET_RULES
  const healthyOutputTargets =
    options?.healthyOutputTargets && typeof options.healthyOutputTargets === "object"
      ? options.healthyOutputTargets
      : HEALTHY_OUTPUT_TARGET_RULES

  const activeRows = Number(summary?.totalRows || 0)
  const scannableRows = Number(summary?.scannerSourceSize || 0)
  const hotUniverseRows = Number(summary?.eligibleTradableRows || 0)
  const withinTarget = {
    active_generation:
      activeRows >= Number(activeGenerationTargets?.min || 600) &&
      activeRows <= Number(activeGenerationTargets?.max || 900),
    scannable:
      scannableRows >= Number(healthyOutputTargets?.scannable?.min || 250) &&
      scannableRows <= Number(healthyOutputTargets?.scannable?.max || 450),
    hot_universe:
      hotUniverseRows >= Number(healthyOutputTargets?.hot_universe?.min || 120) &&
      hotUniverseRows <= Number(healthyOutputTargets?.hot_universe?.max || 250)
  }

  let status = "degraded"
  if (activeRows <= 0 || scannableRows <= 0) {
    status = "dead"
  } else if (Object.values(withinTarget).every(Boolean)) {
    status = "healthy"
  } else if (
    activeRows >= Number(activeGenerationTargets?.min || 600) &&
    scannableRows >= Number(healthyOutputTargets?.scannable?.min || 250) &&
    hotUniverseRows >= Number(healthyOutputTargets?.hot_universe?.min || 120)
  ) {
    status = "recovering"
  }

  return {
    status,
    actuals: {
      activeRows,
      scannableRows,
      hotUniverseRows
    },
    targets: {
      active_generation: activeGenerationTargets,
      scannable: healthyOutputTargets?.scannable || {},
      hot_universe: healthyOutputTargets?.hot_universe || {}
    },
    withinTarget
  }
}

function buildReadinessSummary(summary = {}, options = {}) {
  const universeSummary =
    options?.universeSummary && typeof options.universeSummary === "object"
      ? options.universeSummary
      : {}
  const liveness = buildCatalogLivenessSummary(summary, options)
  const enforceWeaponSkinReadiness = normalizeBoolean(
    options.enforceWeaponSkinReadiness,
    false
  )
  const thresholds = {
    minScannerSourceSize: Math.max(
      normalizeInteger(
        options.minScannerSourceSize,
        Number(HEALTHY_OUTPUT_TARGET_RULES?.scannable?.min || 250),
        1
      ),
      1
    ),
    minEligibleRows: Math.max(
      normalizeInteger(
        options.minEligibleRows ?? options.minEligibleTradableRows,
        Number(HEALTHY_OUTPUT_TARGET_RULES?.hot_universe?.min || 120),
        1
      ),
      1
    ),
    minNearEligibleRows: Math.max(
      normalizeInteger(
        options.minNearEligibleRows,
        1,
        1
      ),
      1
    ),
    minReadyCategories: Math.max(
      normalizeInteger(
        options.minReadyCategories,
        2,
        1
      ),
      1
    ),
    minActiveUniverseRows: Math.max(
      normalizeInteger(
        options.minActiveUniverseRows,
        Number(ACTIVE_GENERATION_TARGET_RULES?.min || 600),
        1
      ),
      1
    ),
    minWeaponSkinEligibleRows: Math.max(
      normalizeInteger(options.minWeaponSkinEligibleRows, 1, 1),
      1
    ),
    minWeaponSkinNearEligibleRows: Math.max(
      normalizeInteger(options.minWeaponSkinNearEligibleRows, 1, 1),
      1
    )
  }

  const weaponSkinSummary =
    summary?.byCategory && typeof summary.byCategory === "object"
      ? summary.byCategory.weapon_skin || {}
      : {}
  const actuals = {
    scannerSourceSize: Number(summary?.scannerSourceSize || 0),
    eligibleRows: Number(summary?.eligibleTradableRows || 0),
    eligibleTradableRows: Number(summary?.eligibleTradableRows || 0),
    nearEligibleRows: Number(summary?.nearEligibleRows || 0),
    readyCategoryCount: Number(summary?.readyCategoryCount || 0),
    activeUniverseRows: Number(universeSummary?.totalRows || 0),
    weaponSkinEligibleRows: Number(weaponSkinSummary?.eligibleRows || 0),
    weaponSkinNearEligibleRows: Number(weaponSkinSummary?.nearEligibleRows || 0)
  }
  const eligibleSupplyReady =
    actuals.scannerSourceSize > 0 && actuals.eligibleRows >= thresholds.minEligibleRows
  const nearEligibleSupplyReady =
    actuals.scannerSourceSize > 0 &&
    actuals.nearEligibleRows >= thresholds.minNearEligibleRows
  const weaponSkinEligibleSupplyReady =
    actuals.weaponSkinEligibleRows >= thresholds.minWeaponSkinEligibleRows
  const weaponSkinNearEligibleSupplyReady =
    actuals.weaponSkinNearEligibleRows >= thresholds.minWeaponSkinNearEligibleRows

  const signals = {
    scannerSourceNonZero: actuals.scannerSourceSize > 0,
    scannerSourceSizeReady: actuals.scannerSourceSize >= thresholds.minScannerSourceSize,
    eligibleRowsReady: actuals.eligibleRows >= thresholds.minEligibleRows,
    nearEligibleReady: actuals.nearEligibleRows >= thresholds.minNearEligibleRows,
    eligibleSupplyReady,
    nearEligibleSupplyReady,
    supplyReady: eligibleSupplyReady || nearEligibleSupplyReady,
    categoryCoverageReady: actuals.readyCategoryCount >= thresholds.minReadyCategories,
    activeUniverseReady: actuals.activeUniverseRows >= thresholds.minActiveUniverseRows,
    weaponSkinEligibleReady: weaponSkinEligibleSupplyReady,
    weaponSkinNearEligibleReady: weaponSkinNearEligibleSupplyReady,
    weaponSkinEligibleSupplyReady,
    weaponSkinNearEligibleSupplyReady,
    weaponSkinSupplyReady:
      weaponSkinEligibleSupplyReady || weaponSkinNearEligibleSupplyReady
  }

  const readinessSource = eligibleSupplyReady
    ? "eligible_supply"
    : nearEligibleSupplyReady
      ? "near_eligible_supply"
      : "not_ready"
  const weaponSkinReadinessSource = weaponSkinEligibleSupplyReady
    ? "eligible_supply"
    : weaponSkinNearEligibleSupplyReady
      ? "near_eligible_supply"
      : "not_ready"

  const requiredSignalKeys = [
    "scannerSourceNonZero",
    "scannerSourceSizeReady",
    "supplyReady",
    "categoryCoverageReady",
    "activeUniverseReady"
  ]
  if (enforceWeaponSkinReadiness) {
    requiredSignalKeys.push("weaponSkinSupplyReady")
  }

  return {
    thresholds,
    actuals,
    liveness,
    signals,
    readinessSource,
    weaponSkinReadinessSource,
    requiredSignalKeys,
    optionalSignalKeys: [
      "eligibleRowsReady",
      "nearEligibleReady",
      "weaponSkinEligibleReady",
      "weaponSkinNearEligibleReady"
    ],
    readyForOpportunityScan: requiredSignalKeys.every((key) => Boolean(signals[key]))
  }
}

function buildCategoryFocusComparison(
  previousSummary = null,
  nextSummary = null,
  previousUniverseSummary = null,
  nextUniverseSummary = null,
  category = "weapon_skin"
) {
  const safeCategory = normalizeText(category).toLowerCase() || "weapon_skin"
  const previousSource = previousSummary?.byCategory?.[safeCategory] || {}
  const nextSource = nextSummary?.byCategory?.[safeCategory] || {}
  const previousUniverse = previousUniverseSummary?.byCategory?.[safeCategory] || {}
  const nextUniverse = nextUniverseSummary?.byCategory?.[safeCategory] || {}

  return {
    category: safeCategory,
    previous: {
      sourceCatalog: previousSource,
      universe: previousUniverse
    },
    next: {
      sourceCatalog: nextSource,
      universe: nextUniverse
    },
    delta: {
      totalRows: Number(nextSource.totalRows || 0) - Number(previousSource.totalRows || 0),
      scannerSourceSize:
        Number(nextSource.scannerSourceSize || 0) - Number(previousSource.scannerSourceSize || 0),
      eligibleRows:
        Number(nextSource.eligibleRows || 0) - Number(previousSource.eligibleRows || 0),
      nearEligibleRows:
        Number(nextSource.nearEligibleRows || 0) - Number(previousSource.nearEligibleRows || 0),
      enrichingRows:
        Number(nextSource.enrichingRows || 0) - Number(previousSource.enrichingRows || 0),
      rejectedRows:
        Number(nextSource.rejectedRows || 0) - Number(previousSource.rejectedRows || 0),
      blockedRows:
        Number(nextSource.blockedRows || 0) - Number(previousSource.blockedRows || 0),
      activeUniverseRows:
        Number(nextUniverse.totalRows || 0) - Number(previousUniverse.totalRows || 0)
    }
  }
}

function compareGenerationSummaries(previousSummary = null, nextSummary = null) {
  const previous = previousSummary && typeof previousSummary === "object" ? previousSummary : null
  const next = nextSummary && typeof nextSummary === "object" ? nextSummary : null
  if (!next) {
    return {
      previousGeneration: previous?.generation || null,
      nextGeneration: null,
      delta: {}
    }
  }

  const deltaByCategory = Object.fromEntries(
    next.categories.map((category) => {
      const previousCategory = previous?.byCategory?.[category] || {}
      const nextCategory = next?.byCategory?.[category] || {}
      return [
        category,
        {
          totalRows: Number(nextCategory.totalRows || 0) - Number(previousCategory.totalRows || 0),
          scannerSourceSize:
            Number(nextCategory.scannerSourceSize || 0) -
            Number(previousCategory.scannerSourceSize || 0),
          eligibleRows:
            Number(nextCategory.eligibleRows || 0) - Number(previousCategory.eligibleRows || 0),
          nearEligibleRows:
            Number(nextCategory.nearEligibleRows || 0) -
            Number(previousCategory.nearEligibleRows || 0),
          rejectedRows:
            Number(nextCategory.rejectedRows || 0) - Number(previousCategory.rejectedRows || 0),
          blockedRows:
            Number(nextCategory.blockedRows || 0) - Number(previousCategory.blockedRows || 0)
        }
      ]
    })
  )

  return {
    previousGeneration: previous?.generation || null,
    nextGeneration: next?.generation || null,
    delta: {
      totalRows: Number(next.totalRows || 0) - Number(previous?.totalRows || 0),
      tradableRows: Number(next.tradableRows || 0) - Number(previous?.tradableRows || 0),
      scannerSourceSize:
        Number(next.scannerSourceSize || 0) - Number(previous?.scannerSourceSize || 0),
      eligibleTradableRows:
        Number(next.eligibleTradableRows || 0) -
        Number(previous?.eligibleTradableRows || 0),
      nearEligibleRows:
        Number(next.nearEligibleRows || 0) - Number(previous?.nearEligibleRows || 0),
      readyCategoryCount:
        Number(next.readyCategoryCount || 0) - Number(previous?.readyCategoryCount || 0),
      byCategory: deltaByCategory
    }
  }
}

function compareUniverseSummaries(previousSummary = null, nextSummary = null) {
  const previous = previousSummary && typeof previousSummary === "object" ? previousSummary : null
  const next = nextSummary && typeof nextSummary === "object" ? nextSummary : null
  if (!next) {
    return {
      previousGeneration: previous?.generation || null,
      nextGeneration: null,
      delta: {}
    }
  }

  const categories = Array.isArray(next.categories) ? next.categories : CATALOG_SCOPE_CATEGORIES
  const deltaByCategory = Object.fromEntries(
    categories.map((category) => {
      const previousCategory = previous?.byCategory?.[category] || {}
      const nextCategory = next?.byCategory?.[category] || {}
      return [
        category,
        {
          totalRows: Number(nextCategory.totalRows || 0) - Number(previousCategory.totalRows || 0)
        }
      ]
    })
  )

  return {
    previousGeneration: previous?.generation || null,
    nextGeneration: next?.generation || null,
    delta: {
      totalRows: Number(next.totalRows || 0) - Number(previous?.totalRows || 0),
      byCategory: deltaByCategory
    }
  }
}

function buildGenerationKey() {
  return `catalog-reset-${new Date().toISOString().replace(/[:.]/g, "-")}`
}

async function summarizeGeneration(generationOrId, options = {}) {
  const generation =
    typeof generationOrId === "string"
      ? await catalogGenerationRepo.getById(generationOrId)
      : generationOrId
  const snapshot = buildGenerationSnapshot(generation)
  if (!snapshot?.id) return null

  const rows = await marketSourceCatalogRepo.listCoverageSummary({
    generationId: snapshot.id,
    categories: normalizeCategories(options.categories),
    limit: normalizeInteger(options.limit, 12000, 1)
  })

  return summarizeCoverageRows(rows, generation, options)
}

async function summarizeUniverseGeneration(generationOrId, options = {}) {
  const generation =
    typeof generationOrId === "string"
      ? await catalogGenerationRepo.getById(generationOrId)
      : generationOrId
  const snapshot = buildGenerationSnapshot(generation)
  if (!snapshot?.id) return null

  const rows = await marketUniverseRepo.listActiveByLiquidityRank({
    generationId: snapshot.id,
    categories: normalizeCategories(options.categories),
    limit: normalizeInteger(options.limit, 5000, 1)
  })

  return summarizeUniverseRows(rows, generation, options)
}

function buildOpportunityScanGateDiagnostics(readiness = null, options = {}) {
  const autoEnabled = Boolean(options.autoEnabled)
  const generationFlagEnabled = Boolean(options.generationFlagEnabled)
  return {
    blocked_by_generation_flag: !generationFlagEnabled && !autoEnabled,
    blocked_by_readiness_gate: readiness ? !Boolean(readiness.readyForOpportunityScan) : false,
    blocked_by_empty_scanner_source: readiness
      ? !Boolean(readiness?.signals?.scannerSourceNonZero)
      : false,
    readiness_source: readiness?.readinessSource || (generationFlagEnabled ? "generation_flag" : "not_ready"),
    weapon_skin_readiness_source:
      readiness?.weaponSkinReadinessSource ||
      (generationFlagEnabled ? "generation_flag" : "not_ready"),
    auto_enabled: autoEnabled
  }
}

async function ensureOpportunityScanEnabledForActiveGeneration(options = {}) {
  const categories = normalizeCategories(options.categories || SCAN_COHORT_CATEGORIES)
  const currentGeneration =
    options.generation && typeof options.generation === "object"
      ? options.generation
      : await catalogGenerationRepo.getCurrentGeneration().catch(() => null)
  const snapshot = buildGenerationSnapshot(currentGeneration)

  if (!snapshot?.id) {
    return {
      allowed: false,
      autoEnabled: false,
      catalogGeneration: null,
      readiness: null,
      diagnostics: {
        blocked_by_generation_flag: true,
        blocked_by_readiness_gate: true,
        blocked_by_empty_scanner_source: true,
        readiness_source: "not_ready",
        weapon_skin_readiness_source: "not_ready",
        auto_enabled: false
      },
      reason: "active_catalog_generation_missing",
      canAutoEnable: false
    }
  }

  if (snapshot.opportunityScanEnabled) {
    return {
      allowed: true,
      autoEnabled: false,
      catalogGeneration: snapshot,
      readiness: null,
      diagnostics: buildOpportunityScanGateDiagnostics(null, {
        generationFlagEnabled: true,
        autoEnabled: false
      }),
      reason: null,
      canAutoEnable: false
    }
  }

  const summary = await summarizeGeneration(currentGeneration, { categories })
  const universeSummary = await summarizeUniverseGeneration(currentGeneration, { categories })
  const readiness = buildReadinessSummary(summary, {
    ...options,
    categories,
    universeSummary
  })
  const diagnostics = buildOpportunityScanGateDiagnostics(readiness, {
    generationFlagEnabled: false,
    autoEnabled: false
  })

  if (!readiness.readyForOpportunityScan || options.autoEnable === false) {
    return {
      allowed: false,
      autoEnabled: false,
      catalogGeneration: snapshot,
      readiness,
      diagnostics,
      reason: diagnostics.blocked_by_empty_scanner_source
        ? "catalog_generation_scanner_source_empty"
        : diagnostics.blocked_by_readiness_gate
          ? "catalog_generation_readiness_gate_failed"
          : "catalog_generation_scan_disabled",
      canAutoEnable: Boolean(readiness.readyForOpportunityScan)
    }
  }

  const enabledAt = new Date().toISOString()
  const diagnosticsSummary = {
    ...(snapshot.diagnosticsSummary && typeof snapshot.diagnosticsSummary === "object"
      ? snapshot.diagnosticsSummary
      : {}),
    readiness,
    opportunityScanUnlock: {
      autoEnabled: true,
      enabledAt,
      readinessSource: readiness.readinessSource,
      weaponSkinReadinessSource: readiness.weaponSkinReadinessSource,
      blocked_by_generation_flag: false,
      blocked_by_readiness_gate: false,
      blocked_by_empty_scanner_source: false
    }
  }
  const enabledGeneration = await catalogGenerationRepo.enableOpportunityScan(snapshot.id, {
    opportunityScanEnabledAt: enabledAt,
    diagnosticsSummary
  })

  return {
    allowed: true,
    autoEnabled: true,
    catalogGeneration: buildGenerationSnapshot(enabledGeneration),
    readiness,
    diagnostics: buildOpportunityScanGateDiagnostics(readiness, {
      generationFlagEnabled: true,
      autoEnabled: true
    }),
    reason: null,
    canAutoEnable: false
  }
}

async function runCatalogGenerationReset(options = {}) {
  const targetUniverseSize = Math.max(
    normalizeInteger(options.targetUniverseSize, DEFAULT_UNIVERSE_LIMIT, 1),
    1
  )
  const categories = normalizeCategories(options.categories)
  const autoEnableOpportunityScan = options.autoEnableOpportunityScan == null
    ? true
    : normalizeBoolean(options.autoEnableOpportunityScan, true)
  const forceEnableOpportunityScan = normalizeBoolean(options.forceEnableOpportunityScan, false)
  const startedAt = new Date().toISOString()

  const previousGeneration = await catalogGenerationRepo.getCurrentGeneration()
  const previousSummary = previousGeneration
    ? await summarizeGeneration(previousGeneration, { categories }).catch(() => null)
    : null
  const previousUniverseSummary = previousGeneration
    ? await summarizeUniverseGeneration(previousGeneration, { categories }).catch(() => null)
    : null

  const stagedGeneration = await catalogGenerationRepo.createGeneration({
    generationKey: normalizeText(options.generationKey) || buildGenerationKey(),
    status: "archived",
    isActive: false,
    opportunityScanEnabled: false,
    sourceGenerationId: previousGeneration?.id || null,
    diagnosticsSummary: {
      phase: "created",
      startedAt,
      categories,
      targetUniverseSize
    }
  })

  if (!stagedGeneration?.id) {
    throw new AppError("catalog_generation_create_failed", 500)
  }

  if (previousGeneration?.id) {
    await catalogGenerationRepo.archiveGeneration(previousGeneration.id, {
      archivedAt: startedAt,
      diagnosticsSummary: {
        ...(previousGeneration?.diagnostics_summary || {}),
        archivedForCatalogReset: true,
        archivedForCatalogResetAt: startedAt,
        replacedByGenerationId: stagedGeneration.id
      }
    })
  }

  await catalogGenerationRepo.activateGeneration(stagedGeneration.id, {
    activatedAt: startedAt,
    opportunityScanEnabled: false,
    diagnosticsSummary: {
      phase: "enrichment_only",
      startedAt,
      categories,
      targetUniverseSize,
      sourceGenerationId: previousGeneration?.id || null
    }
  })

  try {
    const pipelineDiagnostics = await marketSourceCatalogService.prepareSourceCatalog({
      forceRefresh: true,
      targetUniverseSize,
      categories,
      catalogGeneration: stagedGeneration,
      generationId: stagedGeneration.id,
      previousGeneration
    })
    const nextSummary = await summarizeGeneration(stagedGeneration.id, { categories })
    const nextUniverseSummary = await summarizeUniverseGeneration(stagedGeneration.id, {
      categories
    })
    const catalogComparison = compareGenerationSummaries(previousSummary, nextSummary)
    const universeComparison = compareUniverseSummaries(
      previousUniverseSummary,
      nextUniverseSummary
    )
    const comparison = {
      ...catalogComparison,
      catalog: catalogComparison,
      universe: universeComparison,
      weaponSkin: buildCategoryFocusComparison(
        previousSummary,
        nextSummary,
        previousUniverseSummary,
        nextUniverseSummary,
        "weapon_skin"
      )
    }
    const readiness = buildReadinessSummary(nextSummary, {
      ...options,
      universeSummary: nextUniverseSummary
    })
    const shouldEnableOpportunityScan =
      forceEnableOpportunityScan || (autoEnableOpportunityScan && readiness.readyForOpportunityScan)
    const completedAt = new Date().toISOString()
    const diagnosticsSummary = {
      phase: shouldEnableOpportunityScan ? "opportunity_scan_enabled" : "enrichment_only",
      startedAt,
      completedAt,
      categories,
      targetUniverseSize,
      rebuildFlow: {
        archivedPreviousGeneration: Boolean(previousGeneration?.id),
        createdGenerationId: stagedGeneration.id,
        activatedGenerationId: stagedGeneration.id,
        opportunityScanEnabled: shouldEnableOpportunityScan
      },
      previousUniverseSummary,
      nextUniverseSummary,
      comparison,
      readiness,
      sourceCatalogDiagnostics:
        pipelineDiagnostics && typeof pipelineDiagnostics === "object"
          ? pipelineDiagnostics
          : {}
    }

    const updatedGeneration = shouldEnableOpportunityScan
      ? await catalogGenerationRepo.enableOpportunityScan(stagedGeneration.id, {
          opportunityScanEnabledAt: completedAt,
          diagnosticsSummary
        })
      : await catalogGenerationRepo.updateGeneration(stagedGeneration.id, {
          diagnosticsSummary
        })

    return {
      startedAt,
      completedAt,
      targetUniverseSize,
      categories,
      previousGeneration: buildGenerationSnapshot(previousGeneration),
      activeGeneration: buildGenerationSnapshot(updatedGeneration),
      diagnostics: {
        previousSummary,
        nextSummary,
        previousUniverseSummary,
        nextUniverseSummary,
        comparison,
        readiness
      }
    }
  } catch (error) {
    const failedAt = new Date().toISOString()
    await catalogGenerationRepo.updateGeneration(stagedGeneration.id, {
      diagnosticsSummary: {
        phase: "rebuild_failed",
        startedAt,
        failedAt,
        categories,
        targetUniverseSize,
        error: normalizeText(error?.message) || "catalog_generation_rebuild_failed"
      }
    }).catch(() => null)
    throw error
  }
}

module.exports = {
  runCatalogGenerationReset,
  summarizeGeneration,
  summarizeUniverseGeneration,
  ensureOpportunityScanEnabledForActiveGeneration,
  buildGenerationSnapshot,
  __testables: {
    summarizeCoverageRows,
    summarizeUniverseRows,
    buildReadinessSummary,
    buildCatalogLivenessSummary,
    compareGenerationSummaries,
    compareUniverseSummaries,
    buildCategoryFocusComparison,
    normalizeCategories,
    buildOpportunityScanGateDiagnostics
  }
}

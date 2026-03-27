const marketSourceCatalogRepo = require("../../repositories/marketSourceCatalogRepository")
const marketSourceCatalogService = require("../marketSourceCatalogService")
const alphaHotUniverseService = require("./alphaHotUniverseService")
const {
  OPPORTUNITY_BATCH_RUNTIME_TARGET,
  SCAN_COHORT_CATEGORIES,
  SCAN_COHORT_PRIMARY_POOL_MULTIPLIER
} = require("./config")

const {
  isUniverseBackfillReadyRow,
  normalizeCandidateStatus,
  resolveCompatibleCatalogStatusFields
} = marketSourceCatalogService

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeCategory(value) {
  const category = normalizeText(value).toLowerCase()
  return SCAN_COHORT_CATEGORIES.includes(category) ? category : ""
}

function normalizeCatalogStatus(value) {
  const status = normalizeText(value).toLowerCase()
  if (status === "scannable" || status === "shadow" || status === "blocked") {
    return status
  }
  return "shadow"
}

function applyCatalogStatusCompatibility(row = {}) {
  const compatible = resolveCompatibleCatalogStatusFields(row)
  return {
    ...row,
    catalog_status: compatible?.catalogStatus || row?.catalog_status || row?.catalogStatus || "shadow",
    catalog_block_reason:
      compatible?.catalogBlockReason || row?.catalog_block_reason || row?.catalogBlockReason || null,
    catalog_quality_score:
      compatible?.catalogQualityScore ?? row?.catalog_quality_score ?? row?.catalogQualityScore ?? 0,
    last_market_signal_at:
      compatible?.lastMarketSignalAt || row?.last_market_signal_at || row?.lastMarketSignalAt || null
  }
}

function resolveScanEligible(row = {}) {
  return row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible)
}

function isBaseScanCohortRow(row = {}) {
  const category = normalizeCategory(row?.category || row?.itemCategory)
  if (!category) return false
  if (row?.is_active === false || row?.isActive === false) return false
  if (row?.tradable === false) return false
  const compatibleRow = applyCatalogStatusCompatibility(row)
  return normalizeCatalogStatus(compatibleRow?.catalog_status || compatibleRow?.catalogStatus) === "scannable"
}

function isHotCohortRow(row = {}) {
  return (
    isBaseScanCohortRow(row) &&
    normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus) === "eligible" &&
    resolveScanEligible(row)
  )
}

function isWarmCohortRow(row = {}) {
  return (
    isBaseScanCohortRow(row) &&
    normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus) === "near_eligible"
  )
}

function isColdProbeRow(row = {}) {
  const candidateStatus = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
  return (
    isBaseScanCohortRow(row) &&
    (candidateStatus === "enriching" || candidateStatus === "candidate") &&
    isUniverseBackfillReadyRow(row)
  )
}

function emptyCategoryMap() {
  return Object.fromEntries(SCAN_COHORT_CATEGORIES.map((category) => [category, 0]))
}

function countRowsByCategory(rows = []) {
  const counts = emptyCategoryMap()
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeCategory(row?.category || row?.itemCategory)
    if (!category) continue
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

function listMissingCategories(counts = {}) {
  return SCAN_COHORT_CATEGORIES.filter((category) => Number(counts?.[category] || 0) <= 0)
}

function decorateRows(rows = [], patch = {}) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    ...applyCatalogStatusCompatibility(row),
    ...patch
  }))
}

function dedupeRows(rows = []) {
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

function mergeRows(baseRows = [], nextRows = []) {
  return dedupeRows([...(Array.isArray(baseRows) ? baseRows : []), ...(Array.isArray(nextRows) ? nextRows : [])])
}

function filterScannableFallbackRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).filter((row) => isBaseScanCohortRow(row))
}

function resolveSettledRows(result = {}) {
  if (result?.status !== "fulfilled" || !Array.isArray(result.value)) return []
  return result.value
}

async function loadScanSource(options = {}) {
  const batchSize = Math.max(
    Math.round(Number(options.batchSize || OPPORTUNITY_BATCH_RUNTIME_TARGET)),
    1
  )
  const categories = Array.isArray(options.categories) ? options.categories : SCAN_COHORT_CATEGORIES
  const cohortLimit = Math.max(batchSize * 4, batchSize * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER, 24)
  const diagnostics = {
    sourceMode: "persisted_cohorts",
    fallbackUsed: false,
    fallbackReasons: [],
    primaryCohortCounts: {
      hot: 0,
      warm: 0,
      cold: 0
    },
    primaryCategoryCounts: emptyCategoryMap(),
    missingCategoriesAfterPrimary: [],
    cohortQueryFailures: {
      hot: false,
      warm: false,
      cold: false,
      candidatePool: false,
      activeTradable: false
    },
    fallbackRowsLoadedBySource: {
      candidatePool: 0,
      activeTradable: 0
    },
    fallbackRowsSelectedBySource: {
      candidatePool: 0,
      activeTradable: 0
    },
    fallbackSelectedShare: 0,
    degradedScannerHealth: false
  }

  const [hotResult, warmResult, coldResult] = await Promise.allSettled([
    marketSourceCatalogRepo.listHotScanCohort({
      limit: cohortLimit,
      categories
    }),
    marketSourceCatalogRepo.listWarmScanCohort({
      limit: cohortLimit,
      categories
    }),
    marketSourceCatalogRepo.listColdScanCohort({
      limit: cohortLimit,
      categories
    })
  ])

  if (hotResult.status === "rejected") diagnostics.cohortQueryFailures.hot = true
  if (warmResult.status === "rejected") diagnostics.cohortQueryFailures.warm = true
  if (coldResult.status === "rejected") diagnostics.cohortQueryFailures.cold = true

  const hotRows = decorateRows(
    resolveSettledRows(hotResult).filter((row) => isHotCohortRow(row)),
    { scanCohort: "hot", sourceOrigin: "hot" }
  )
  const warmRows = decorateRows(
    resolveSettledRows(warmResult).filter((row) => isWarmCohortRow(row)),
    { scanCohort: "warm", sourceOrigin: "warm" }
  )
  const coldRows = decorateRows(
    resolveSettledRows(coldResult).filter((row) => isColdProbeRow(row)),
    { scanCohort: "cold", sourceOrigin: "cold" }
  )

  diagnostics.primaryCohortCounts.hot = hotRows.length
  diagnostics.primaryCohortCounts.warm = warmRows.length
  diagnostics.primaryCohortCounts.cold = coldRows.length

  let rows = mergeRows(hotRows, warmRows)
  rows = mergeRows(rows, coldRows)

  diagnostics.primaryCategoryCounts = countRowsByCategory(rows)
  diagnostics.missingCategoriesAfterPrimary = listMissingCategories(diagnostics.primaryCategoryCounts)

  if (diagnostics.cohortQueryFailures.hot) diagnostics.fallbackReasons.push("hot_query_failed")
  if (diagnostics.cohortQueryFailures.warm) diagnostics.fallbackReasons.push("warm_query_failed")
  if (diagnostics.cohortQueryFailures.cold) diagnostics.fallbackReasons.push("cold_query_failed")
  if (rows.length < batchSize * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER) {
    diagnostics.fallbackReasons.push("primary_pool_under_target")
  }
  if (diagnostics.missingCategoriesAfterPrimary.length) {
    diagnostics.fallbackReasons.push("missing_category_coverage")
  }

  if (diagnostics.fallbackReasons.length) {
    diagnostics.fallbackUsed = true
    const fallbackCategories = diagnostics.missingCategoriesAfterPrimary.length
      ? diagnostics.missingCategoriesAfterPrimary
      : categories
    try {
      const candidatePoolRows = filterScannableFallbackRows(
        await marketSourceCatalogRepo.listCandidatePool({
          limit: Math.max(batchSize, 20),
          categories: fallbackCategories,
          candidateStatuses: ["near_eligible", "enriching", "candidate"],
          catalogStatuses: ["scannable"]
        })
      )
      const mergedRows = mergeRows(
        rows,
        decorateRows(candidatePoolRows, {
          scanCohort: "fallback",
          sourceOrigin: "fallback",
          fallbackSource: "candidatePool"
        })
      )
      diagnostics.fallbackRowsLoadedBySource.candidatePool = Math.max(
        mergedRows.length - rows.length,
        0
      )
      rows = mergedRows
    } catch (_err) {
      diagnostics.cohortQueryFailures.candidatePool = true
      diagnostics.fallbackReasons.push("candidate_pool_query_failed")
    }

    const categoryCountsAfterCandidatePool = countRowsByCategory(rows)
    const missingAfterCandidatePool = listMissingCategories(categoryCountsAfterCandidatePool)
    const needsActiveTradableFallback =
      diagnostics.cohortQueryFailures.candidatePool ||
      rows.length < batchSize ||
      missingAfterCandidatePool.length > 0

    if (needsActiveTradableFallback) {
      try {
        const activeTradableRows = filterScannableFallbackRows(
          await marketSourceCatalogRepo.listActiveTradable({
            limit: Math.max(batchSize, 20),
            categories: missingAfterCandidatePool.length ? missingAfterCandidatePool : fallbackCategories,
            catalogStatuses: ["scannable"]
          })
        )
        const mergedRows = mergeRows(
          rows,
          decorateRows(activeTradableRows, {
            scanCohort: "fallback",
            sourceOrigin: "fallback",
            fallbackSource: "activeTradable"
          })
        )
        diagnostics.fallbackRowsLoadedBySource.activeTradable = Math.max(
          mergedRows.length - rows.length,
          0
        )
        rows = mergedRows
      } catch (_err) {
        diagnostics.cohortQueryFailures.activeTradable = true
        diagnostics.fallbackReasons.push("active_tradable_query_failed")
      }
    }
  }

  diagnostics.fallbackReasons = Array.from(new Set(diagnostics.fallbackReasons))
  diagnostics.degradedScannerHealth =
    diagnostics.cohortQueryFailures.hot ||
    diagnostics.cohortQueryFailures.warm ||
    diagnostics.cohortQueryFailures.cold

  const alphaHotUniverse = alphaHotUniverseService.buildAlphaHotUniverse({
    rows,
    batchSize,
    allowNearEligible: true
  })
  const alphaDiagnostics =
    alphaHotUniverse?.diagnostics && typeof alphaHotUniverse.diagnostics === "object"
      ? alphaHotUniverse.diagnostics
      : {}

  return {
    rows: Array.isArray(alphaHotUniverse?.rows) ? alphaHotUniverse.rows : [],
    diagnostics: {
      ...diagnostics,
      ...alphaDiagnostics
    }
  }
}

module.exports = {
  loadScanSource,
  __testables: {
    countRowsByCategory,
    listMissingCategories,
    filterScannableFallbackRows,
    isBaseScanCohortRow,
    isHotCohortRow,
    isWarmCohortRow,
    isColdProbeRow
  }
}

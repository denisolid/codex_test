const marketSourceCatalogRepo = require("../../repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../../repositories/marketUniverseRepository")
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

function buildUniverseRowMap(rows = []) {
  const map = new Map()
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue
    map.set(marketHashName, row)
  }
  return map
}

function sortRowsByUniverseRank(rows = [], universeRowsByName = new Map()) {
  return [...(Array.isArray(rows) ? rows : [])].sort((left, right) => {
    const leftUniverse = universeRowsByName.get(
      normalizeText(left?.market_hash_name || left?.marketHashName)
    )
    const rightUniverse = universeRowsByName.get(
      normalizeText(right?.market_hash_name || right?.marketHashName)
    )
    const leftRank = Number(leftUniverse?.liquidity_rank || leftUniverse?.liquidityRank || Infinity)
    const rightRank = Number(
      rightUniverse?.liquidity_rank || rightUniverse?.liquidityRank || Infinity
    )
    if (leftRank !== rightRank) return leftRank - rightRank
    return normalizeText(left?.market_hash_name || left?.marketHashName).localeCompare(
      normalizeText(right?.market_hash_name || right?.marketHashName)
    )
  })
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
    sourceMode: "active_generation_universe",
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
      universe: false,
      catalog: false,
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
    degradedScannerHealth: false,
    universeRowsLoaded: 0,
    catalogRowsResolved: 0,
    universeRowsMissingCatalog: 0,
    universeRowsDroppedBeforeAlpha: 0
  }

  let universeRows = []
  try {
    universeRows = await marketUniverseRepo.listActiveByLiquidityRank({
      limit: cohortLimit,
      categories,
      requireOpportunityScanEnabled: true
    })
  } catch (_err) {
    diagnostics.cohortQueryFailures.universe = true
    throw _err
  }

  diagnostics.universeRowsLoaded = universeRows.length
  if (!universeRows.length) {
    diagnostics.fallbackReasons.push("active_generation_universe_empty")
    diagnostics.missingCategoriesAfterPrimary = [...categories]
    return {
      rows: [],
      diagnostics
    }
  }

  const universeRowsByName = buildUniverseRowMap(universeRows)
  const universeMarketHashNames = universeRows
    .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
    .filter(Boolean)

  let catalogRows = []
  try {
    catalogRows = await marketSourceCatalogRepo.listByMarketHashNames(universeMarketHashNames, {
      categories,
      requireOpportunityScanEnabled: true
    })
  } catch (_err) {
    diagnostics.cohortQueryFailures.catalog = true
    throw _err
  }

  const matchedCatalogRows = catalogRows.filter((row) =>
    universeRowsByName.has(normalizeText(row?.market_hash_name || row?.marketHashName))
  )
  diagnostics.catalogRowsResolved = matchedCatalogRows.length
  diagnostics.universeRowsMissingCatalog = Math.max(
    universeMarketHashNames.length - matchedCatalogRows.length,
    0
  )

  const hydratedRows = sortRowsByUniverseRank(
    matchedCatalogRows.map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      const universeRow = universeRowsByName.get(marketHashName)
      return {
        ...applyCatalogStatusCompatibility(row),
        liquidity_rank:
          universeRow?.liquidity_rank ??
          universeRow?.liquidityRank ??
          row?.liquidity_rank ??
          row?.liquidityRank ??
          null,
        universe_liquidity_rank:
          universeRow?.liquidity_rank ?? universeRow?.liquidityRank ?? null,
        sourceOrigin: "active_universe"
      }
    }),
    universeRowsByName
  )

  const hotRows = decorateRows(hydratedRows.filter((row) => isHotCohortRow(row)), {
    scanCohort: "hot",
    sourceOrigin: "active_universe"
  })
  const warmRows = decorateRows(hydratedRows.filter((row) => isWarmCohortRow(row)), {
    scanCohort: "warm",
    sourceOrigin: "active_universe"
  })
  const coldRows = decorateRows(hydratedRows.filter((row) => isColdProbeRow(row)), {
    scanCohort: "cold",
    sourceOrigin: "active_universe"
  })

  diagnostics.primaryCohortCounts.hot = hotRows.length
  diagnostics.primaryCohortCounts.warm = warmRows.length
  diagnostics.primaryCohortCounts.cold = coldRows.length

  let rows = mergeRows(hotRows, warmRows)
  rows = mergeRows(rows, coldRows)

  diagnostics.universeRowsDroppedBeforeAlpha = Math.max(hydratedRows.length - rows.length, 0)
  diagnostics.primaryCategoryCounts = countRowsByCategory(rows)
  diagnostics.missingCategoriesAfterPrimary = listMissingCategories(diagnostics.primaryCategoryCounts)

  if (rows.length < batchSize * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER) {
    diagnostics.fallbackReasons.push("active_generation_universe_under_target")
  }
  if (diagnostics.missingCategoriesAfterPrimary.length) {
    diagnostics.fallbackReasons.push("missing_category_coverage")
  }
  diagnostics.fallbackReasons = Array.from(new Set(diagnostics.fallbackReasons))

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

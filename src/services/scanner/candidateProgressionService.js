const marketSourceCatalogRepo = require("../../repositories/marketSourceCatalogRepository")
const marketSourceCatalogService = require("../marketSourceCatalogService")
const catalogPriorityCoverageService = require("../catalogPriorityCoverageService")
const {
  ENRICHMENT_BATCH_TARGET,
  ENRICHMENT_INTERVAL_MS,
  PROGRESSION_RETRY_INTERVAL_MULTIPLIERS,
  SCAN_COHORT_CATEGORIES,
  UNIVERSE_DB_LIMIT
} = require("./config")

const {
  isUniverseBackfillReadyRow,
  normalizeCandidateStatus,
  resolveCompatibleCatalogStatusFields
} = marketSourceCatalogService

const PROGRESSION_STATE_ORDER = Object.freeze([
  "near_eligible",
  "eligible",
  "enriching",
  "candidate"
])
const progressionHealthState = {
  previousNearEligibleDueBacklog: null,
  consecutiveNearEligibleGrowth: 0
}

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

function computeAgeMinutes(isoValue, nowMs = Date.now()) {
  const iso = toIsoOrNull(isoValue)
  if (!iso) return null
  const ageMinutes = (nowMs - new Date(iso).getTime()) / 60000
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return Number(ageMinutes.toFixed(2))
}

function isRowDueForProgression(row = {}, nowMs = Date.now()) {
  const candidateStatus = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
  const multiplier = Number(PROGRESSION_RETRY_INTERVAL_MULTIPLIERS[candidateStatus] || 0)
  if (multiplier <= 0) return false
  const lastEnrichedAt = toIsoOrNull(row?.last_enriched_at || row?.lastEnrichedAt)
  if (!lastEnrichedAt) return true
  const elapsedMs = nowMs - new Date(lastEnrichedAt).getTime()
  if (!Number.isFinite(elapsedMs) || elapsedMs < 0) return true
  return elapsedMs >= ENRICHMENT_INTERVAL_MS * multiplier
}

function buildStateNumberMap(initialValue = 0) {
  return Object.fromEntries(
    PROGRESSION_STATE_ORDER.map((state) => [state, Number(initialValue || 0)])
  )
}

function buildDueBeforeIso(candidateStatus = "", nowMs = Date.now()) {
  const normalizedStatus = normalizeCandidateStatus(candidateStatus)
  const multiplier = Number(PROGRESSION_RETRY_INTERVAL_MULTIPLIERS[normalizedStatus] || 0)
  if (multiplier <= 0) return null
  return new Date(nowMs - ENRICHMENT_INTERVAL_MS * multiplier).toISOString()
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

function summarizeDueBacklog(rowsByState = {}, nowMs = Date.now()) {
  const dueBacklogRowsByState = buildStateNumberMap(0)
  const dueBacklogOldestAgeMinutesByState = buildStateNumberMap(0)

  for (const state of PROGRESSION_STATE_ORDER) {
    const rows = Array.isArray(rowsByState?.[state]) ? rowsByState[state] : []
    let oldestAge = 0
    for (const row of rows) {
      const ageMinutes = computeAgeMinutes(row?.last_enriched_at || row?.lastEnrichedAt, nowMs)
      if (ageMinutes != null) {
        oldestAge = Math.max(oldestAge, Number(ageMinutes || 0))
      }
    }
    dueBacklogRowsByState[state] = rows.length
    dueBacklogOldestAgeMinutesByState[state] = Number(oldestAge.toFixed(2))
  }

  return {
    dueBacklogRowsByState,
    dueBacklogOldestAgeMinutesByState,
    nearEligibleDueBacklog: Number(dueBacklogRowsByState.near_eligible || 0),
    eligibleRecheckDueBacklog: Number(dueBacklogRowsByState.eligible || 0)
  }
}

function buildCoverageMetrics(rows = []) {
  const metrics = {
    eligibleTradableRows: 0,
    hotCohortSize: 0,
    warmCohortSize: 0,
    coldProbeSize: 0
  }

  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeText(row?.category).toLowerCase()
    if (!SCAN_COHORT_CATEGORIES.includes(category)) continue
    if (row?.is_active === false || row?.isActive === false) continue
    if (row?.tradable === false) continue
    if (
      normalizeText(resolveCompatibleCatalogStatusFields(row)?.catalogStatus).toLowerCase() !==
      "scannable"
    ) {
      continue
    }

    const candidateStatus = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
    const scanEligible =
      row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible)

    if (candidateStatus === "eligible" && scanEligible) {
      metrics.eligibleTradableRows += 1
      metrics.hotCohortSize += 1
      continue
    }
    if (candidateStatus === "near_eligible") {
      metrics.warmCohortSize += 1
      continue
    }
    if (
      (candidateStatus === "enriching" || candidateStatus === "candidate") &&
      isUniverseBackfillReadyRow(row)
    ) {
      metrics.coldProbeSize += 1
    }
  }

  return metrics
}

function updateNearEligibleGrowthState(nearEligibleDueBacklog = 0) {
  const safeBacklog = Math.max(Number(nearEligibleDueBacklog || 0), 0)
  if (
    progressionHealthState.previousNearEligibleDueBacklog != null &&
    safeBacklog > progressionHealthState.previousNearEligibleDueBacklog
  ) {
    progressionHealthState.consecutiveNearEligibleGrowth += 1
  } else {
    progressionHealthState.consecutiveNearEligibleGrowth = 0
  }
  progressionHealthState.previousNearEligibleDueBacklog = safeBacklog
  return progressionHealthState.consecutiveNearEligibleGrowth
}

async function listRowsByState(state = "", limit = UNIVERSE_DB_LIMIT, nowMs = Date.now()) {
  return marketSourceCatalogRepo.listDueProgressionRows({
    limit,
    categories: SCAN_COHORT_CATEGORIES,
    candidateStatuses: [state],
    dueBeforeIso: buildDueBeforeIso(state, nowMs)
  })
}

function buildProcessedRowsByState(rows = []) {
  const counts = buildStateNumberMap(0)
  for (const row of Array.isArray(rows) ? rows : []) {
    const state = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
    if (counts[state] == null) continue
    counts[state] = Number(counts[state] || 0) + 1
  }
  return counts
}

function buildTransitionMetrics(previousRows = [], currentRows = []) {
  const previousByName = new Map()
  for (const row of Array.isArray(previousRows) ? previousRows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue
    previousByName.set(
      marketHashName,
      normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
    )
  }

  let demotedFromEligibleTotal = 0
  let demotedFromNearEligibleTotal = 0
  for (const row of Array.isArray(currentRows) ? currentRows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue
    const previousStatus = previousByName.get(marketHashName)
    const currentStatus = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
    if (previousStatus === "eligible" && currentStatus !== "eligible") {
      demotedFromEligibleTotal += 1
    }
    if (
      previousStatus === "near_eligible" &&
      currentStatus !== "near_eligible" &&
      currentStatus !== "eligible"
    ) {
      demotedFromNearEligibleTotal += 1
    }
  }

  return {
    demotedFromEligibleTotal,
    demotedFromNearEligibleTotal
  }
}

async function runProgressionBatch(options = {}) {
  const batchSize = Math.max(Math.round(Number(options.batchSize || ENRICHMENT_BATCH_TARGET)), 1)
  const nowMs = Number(options.nowMs || Date.now())
  const metricsSampleLimit = Math.max(UNIVERSE_DB_LIMIT, batchSize * 4)
  const rowsByState = Object.fromEntries(
    await Promise.all(
      PROGRESSION_STATE_ORDER.map(async (state) => [
        state,
        await listRowsByState(state, metricsSampleLimit, nowMs).catch(() => [])
      ])
    )
  )

  const dueSummary = summarizeDueBacklog(rowsByState, nowMs)
  const selectedRows = dedupeRowsByMarketHashName(
    PROGRESSION_STATE_ORDER.flatMap((state) =>
      Array.isArray(rowsByState?.[state]) ? rowsByState[state] : []
    )
  ).slice(0, batchSize)

  let priorityCoverage = {
    totalPriorityItemsConfigured: 0,
    matchedExistingCatalogItems: 0,
    insertedMissingCatalogItems: 0,
    unmatchedPriorityItems: [],
    entries: [],
    byKey: new Map(),
    policyHintsByTier: {},
    error: null
  }
  try {
    priorityCoverage = await catalogPriorityCoverageService.syncPriorityCoverageSet({
      allowCatalogInsert: false
    })
  } catch (err) {
    priorityCoverage = {
      ...priorityCoverage,
      error: normalizeText(err?.message) || "priority_coverage_sync_failed"
    }
  }

  const progressionDiagnostics =
    selectedRows.length > 0
      ? await marketSourceCatalogService.recomputeCandidateReadinessRows(selectedRows, {
          priorityCoverage
        })
      : {
          promotedToNearEligible: 0,
          promotedToEligible: 0,
          processedMarketHashNames: []
        }

  const reloadedRows =
    selectedRows.length > 0
      ? await marketSourceCatalogRepo.listByMarketHashNames(
          selectedRows.map((row) => row?.market_hash_name || row?.marketHashName),
          {
            categories: SCAN_COHORT_CATEGORIES
          }
        )
      : []
  const transitionMetrics = buildTransitionMetrics(selectedRows, reloadedRows)
  const coverageRows = await marketSourceCatalogRepo.listCoverageSummary({
    limit: metricsSampleLimit,
    categories: SCAN_COHORT_CATEGORIES
  })
  const coverageMetrics = buildCoverageMetrics(coverageRows)
  const consecutiveNearEligibleGrowth = updateNearEligibleGrowthState(
    dueSummary.nearEligibleDueBacklog
  )
  const degradedScannerHealth =
    consecutiveNearEligibleGrowth >= 3 ||
    (Number(progressionDiagnostics?.promotedToEligible || 0) === 0 &&
      Number(dueSummary.nearEligibleDueBacklog || 0) > 0)

  return {
    processedCount: selectedRows.length,
    processedRowsByState: buildProcessedRowsByState(selectedRows),
    processedMarketHashNames:
      progressionDiagnostics?.processedMarketHashNames || selectedRows.map((row) => row.market_hash_name),
    diagnostics: {
      progression_rows_processed_total: selectedRows.length,
      progression_rows_processed_by_state: buildProcessedRowsByState(selectedRows),
      due_backlog_rows_by_state: dueSummary.dueBacklogRowsByState,
      due_backlog_oldest_age_minutes_by_state: dueSummary.dueBacklogOldestAgeMinutesByState,
      near_eligible_due_backlog: dueSummary.nearEligibleDueBacklog,
      eligible_recheck_due_backlog: dueSummary.eligibleRecheckDueBacklog,
      promoted_to_near_eligible_total: Number(progressionDiagnostics?.promotedToNearEligible || 0),
      promoted_to_eligible_total: Number(progressionDiagnostics?.promotedToEligible || 0),
      demoted_from_eligible_total: Number(transitionMetrics.demotedFromEligibleTotal || 0),
      demoted_from_near_eligible_total: Number(transitionMetrics.demotedFromNearEligibleTotal || 0),
      eligible_tradable_rows: Number(coverageMetrics.eligibleTradableRows || 0),
      hot_cohort_size: Number(coverageMetrics.hotCohortSize || 0),
      warm_cohort_size: Number(coverageMetrics.warmCohortSize || 0),
      cold_probe_size: Number(coverageMetrics.coldProbeSize || 0),
      consecutive_near_eligible_growth_runs: consecutiveNearEligibleGrowth,
      degradedScannerHealth,
      priorityCoverage: {
        totalPriorityItemsConfigured: Number(priorityCoverage?.totalPriorityItemsConfigured || 0),
        matchedExistingCatalogItems: Number(priorityCoverage?.matchedExistingCatalogItems || 0),
        insertedMissingCatalogItems: Number(priorityCoverage?.insertedMissingCatalogItems || 0),
        error: normalizeText(priorityCoverage?.error) || null
      }
    }
  }
}

module.exports = {
  runProgressionBatch,
  __testables: {
    isRowDueForProgression,
    summarizeDueBacklog,
    buildCoverageMetrics,
    buildTransitionMetrics
  }
}

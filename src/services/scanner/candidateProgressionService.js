const marketSourceCatalogRepo = require("../../repositories/marketSourceCatalogRepository")
const marketSourceCatalogService = require("../marketSourceCatalogService")
const catalogPriorityCoverageService = require("../catalogPriorityCoverageService")
const upstreamMarketFreshnessRecoveryService = require("../upstreamMarketFreshnessRecoveryService")
const enrichmentRepairService = require("./enrichmentRepairService")
const {
  ENRICHMENT_BATCH_TARGET,
  ENRICHMENT_INTERVAL_MS,
  PROGRESSION_RETRY_INTERVAL_MULTIPLIERS,
  SCAN_COHORT_CATEGORIES,
  DEFAULT_UNIVERSE_LIMIT,
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

function sortProgressionRows(rows = [], nowMs = Date.now()) {
  return [...(Array.isArray(rows) ? rows : [])].sort((left, right) => {
    const leftNeedsRepair = enrichmentRepairService.getRepairNeeds(left, nowMs).needsRepair
    const rightNeedsRepair = enrichmentRepairService.getRepairNeeds(right, nowMs).needsRepair
    if (leftNeedsRepair !== rightNeedsRepair) {
      return Number(rightNeedsRepair) - Number(leftNeedsRepair)
    }
    if (leftNeedsRepair && rightNeedsRepair) {
      const repairPriorityDelta =
        enrichmentRepairService.buildRepairPriorityScore(right, nowMs) -
        enrichmentRepairService.buildRepairPriorityScore(left, nowMs)
      if (repairPriorityDelta !== 0) return repairPriorityDelta
    }
    const enrichmentPriorityDelta =
      Number(right?.enrichment_priority ?? right?.enrichmentPriority ?? 0) -
      Number(left?.enrichment_priority ?? left?.enrichmentPriority ?? 0)
    if (enrichmentPriorityDelta !== 0) return enrichmentPriorityDelta
    const priorityBoostDelta =
      Number(right?.priority_boost ?? right?.priorityBoost ?? 0) -
      Number(left?.priority_boost ?? left?.priorityBoost ?? 0)
    if (priorityBoostDelta !== 0) return priorityBoostDelta
    const liquidityDelta =
      Number(right?.liquidity_rank ?? right?.liquidityRank ?? 0) -
      Number(left?.liquidity_rank ?? left?.liquidityRank ?? 0)
    if (liquidityDelta !== 0) return liquidityDelta
    const lastSignalDelta =
      new Date(toIsoOrNull(right?.last_market_signal_at || right?.lastMarketSignalAt) || 0).getTime() -
      new Date(toIsoOrNull(left?.last_market_signal_at || left?.lastMarketSignalAt) || 0).getTime()
    if (lastSignalDelta !== 0) return lastSignalDelta
    return normalizeText(left?.market_hash_name || left?.marketHashName).localeCompare(
      normalizeText(right?.market_hash_name || right?.marketHashName)
    )
  })
}

function mergeUniqueRows(primaryRows = [], secondaryRows = []) {
  return dedupeRowsByMarketHashName([
    ...(Array.isArray(primaryRows) ? primaryRows : []),
    ...(Array.isArray(secondaryRows) ? secondaryRows : [])
  ])
}

function buildRepairSummaryFromRefresh(repairRefresh = {}) {
  const snapshotOutcomes = Array.isArray(repairRefresh?.snapshotRefresh?.rowOutcomes)
    ? repairRefresh.snapshotRefresh.rowOutcomes
    : []
  return {
    attemptedRows: Number(repairRefresh?.attemptedRows || 0),
    quoteRowsSelected: Number(repairRefresh?.quoteRowsSelected || 0),
    snapshotRowsSelected: Number(repairRefresh?.snapshotRowsSelected || 0),
    snapshotBlocked: Boolean(repairRefresh?.snapshotRefresh?.blocked),
    snapshotBlockedReason: normalizeText(repairRefresh?.snapshotRefresh?.blockedReason) || null,
    snapshotOutcomesByReason: snapshotOutcomes.reduce((acc, outcome) => {
      const reason = normalizeText(outcome?.reason)
      if (!reason) return acc
      acc[reason] = Number(acc[reason] || 0) + 1
      return acc
    }, {})
  }
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
  const dueRows = sortProgressionRows(
    dedupeRowsByMarketHashName(
      PROGRESSION_STATE_ORDER.flatMap((state) =>
        Array.isArray(rowsByState?.[state]) ? rowsByState[state] : []
      )
    ).filter((row) => !enrichmentRepairService.isRepairCooldownActive(row, nowMs)),
    nowMs
  )
  const repairSelection = enrichmentRepairService.selectRepairCandidates(dueRows, {
    limit: batchSize,
    nowMs
  })
  const repairSelectedNames = new Set(
    (repairSelection.rows || [])
      .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
      .filter(Boolean)
  )
  const selectedRows = mergeUniqueRows(
    repairSelection.rows,
    dueRows.filter((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      return marketHashName && !repairSelectedNames.has(marketHashName)
    })
  )
    .filter((row) => !enrichmentRepairService.isRepairCooldownActive(row, nowMs))
    .slice(0, batchSize)

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

  let repairRefresh = {
    attemptedRows: 0,
    quoteRowsSelected: 0,
    snapshotRowsSelected: 0,
    quoteRefresh: {},
    snapshotRefresh: {
      blocked: false,
      blockedReason: null,
      rowOutcomes: []
    },
    processedMarketHashNames: []
  }
  if (repairSelection.rows.length > 0) {
    try {
      repairRefresh = await upstreamMarketFreshnessRecoveryService.repairCatalogRows(
        repairSelection.rows,
        {
          quoteBatchSize: Math.max(Math.min(repairSelection.rows.length, 8), 1),
          snapshotBatchSize: Math.max(Math.min(repairSelection.rows.length, 4), 1),
          batchMeta: {
            lane: "enrichment_repair",
            rowCount: repairSelection.rows.length
          }
        }
      )
    } catch (err) {
      repairRefresh = {
        ...repairRefresh,
        error: normalizeText(err?.message) || "repair_refresh_failed"
      }
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
  const reloadedRowsByName = new Map(
    (Array.isArray(reloadedRows) ? reloadedRows : []).map((row) => [
      normalizeText(row?.market_hash_name || row?.marketHashName),
      row
    ])
  )
  const repairDecisions = repairSelection.rows
    .map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName) return null
      return enrichmentRepairService.buildRepairDecision({
        previousRow: row,
        currentRow: reloadedRowsByName.get(marketHashName) || row,
        nowMs
      })
    })
    .filter(Boolean)
  const repairPatchRows = repairDecisions
    .map((decision) => (decision?.attempted ? decision.patch : null))
    .filter(Boolean)
  let finalRows = Array.isArray(reloadedRows) ? reloadedRows.slice() : []
  if (repairPatchRows.length > 0) {
    await marketSourceCatalogRepo.upsertRows(repairPatchRows)
    const finalRowsByName = new Map(
      finalRows.map((row) => [normalizeText(row?.market_hash_name || row?.marketHashName), row])
    )
    for (const patchRow of repairPatchRows) {
      finalRowsByName.set(
        normalizeText(patchRow?.market_hash_name || patchRow?.marketHashName),
        patchRow
      )
    }
    finalRows = Array.from(finalRowsByName.values())
  }
  let universeRefresh = {
    triggered: false,
    skipped: true,
    targetUniverseSize: DEFAULT_UNIVERSE_LIMIT,
    universeRowsBeforeRefresh: 0,
    universeRowsAfterRefresh: 0,
    universeRowsDroppedAsStale: 0,
    universeRowsAdded: 0,
    activeUniverseBuilt: 0,
    error: null
  }
  if (selectedRows.length > 0) {
    try {
      const refreshResult = await marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog({
        targetUniverseSize: DEFAULT_UNIVERSE_LIMIT,
        categories: SCAN_COHORT_CATEGORIES
      })
      universeRefresh = {
        triggered: true,
        skipped: Boolean(refreshResult?.persisted?.skipped ?? refreshResult?.skipped),
        targetUniverseSize: Number(
          refreshResult?.targetUniverseSize || DEFAULT_UNIVERSE_LIMIT
        ),
        universeRowsBeforeRefresh: Number(refreshResult?.universeRowsBeforeRefresh || 0),
        universeRowsAfterRefresh: Number(refreshResult?.universeRowsAfterRefresh || 0),
        universeRowsDroppedAsStale: Number(refreshResult?.universeRowsDroppedAsStale || 0),
        universeRowsAdded: Number(refreshResult?.universeRowsAdded || 0),
        activeUniverseBuilt: Number(refreshResult?.activeUniverseBuilt || 0),
        error: normalizeText(refreshResult?.error) || null
      }
    } catch (err) {
      universeRefresh = {
        ...universeRefresh,
        triggered: true,
        skipped: true,
        error: normalizeText(err?.message) || "active_universe_refresh_failed"
      }
    }
  }
  const repairSummary = enrichmentRepairService.summarizeRepairDecisions(repairDecisions)
  const repairRefreshSummary = buildRepairSummaryFromRefresh(repairRefresh)
  const transitionMetrics = buildTransitionMetrics(selectedRows, finalRows)
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
      universeRowsBeforeRefresh: Number(universeRefresh.universeRowsBeforeRefresh || 0),
      universeRowsAfterRefresh: Number(universeRefresh.universeRowsAfterRefresh || 0),
      universeRowsDroppedAsStale: Number(universeRefresh.universeRowsDroppedAsStale || 0),
      universeRowsAdded: Number(universeRefresh.universeRowsAdded || 0),
      demoted_from_eligible_total: Number(transitionMetrics.demotedFromEligibleTotal || 0),
      demoted_from_near_eligible_total: Number(transitionMetrics.demotedFromNearEligibleTotal || 0),
      eligible_tradable_rows: Number(coverageMetrics.eligibleTradableRows || 0),
      hot_cohort_size: Number(coverageMetrics.hotCohortSize || 0),
      warm_cohort_size: Number(coverageMetrics.warmCohortSize || 0),
      cold_probe_size: Number(coverageMetrics.coldProbeSize || 0),
      repair_candidates_selected: Number(repairSelection?.diagnostics?.repair_candidates_selected || 0),
      repaired_rows: Number(repairSummary.repaired_rows || 0),
      repaired_to_near_eligible: Number(repairSummary.repaired_to_near_eligible || 0),
      repaired_to_eligible: Number(repairSummary.repaired_to_eligible || 0),
      cooldown_after_failed_repair: Number(repairSummary.cooldown_after_failed_repair || 0),
      rejected_after_failed_repair: Number(repairSummary.rejected_after_failed_repair || 0),
      top_failed_repair_reasons: repairSummary.top_failed_repair_reasons || {},
      hard_reject_to_penalty_conversions_by_category:
        progressionDiagnostics?.hard_reject_to_penalty_conversions_by_category ||
        progressionDiagnostics?.hardRejectToPenaltyConversionsByCategory ||
        {},
      near_eligible_by_category:
        progressionDiagnostics?.near_eligible_by_category ||
        progressionDiagnostics?.nearEligibleByCategory ||
        {},
      eligible_by_category:
        progressionDiagnostics?.eligible_by_category ||
        progressionDiagnostics?.eligibleByCategory ||
        {},
      top_reject_reasons_by_category:
        progressionDiagnostics?.top_reject_reasons_by_category ||
        progressionDiagnostics?.topRejectReasonsByCategory ||
        {},
      weapon_skin_recovery_paths:
        progressionDiagnostics?.weapon_skin_recovery_paths ||
        progressionDiagnostics?.weaponSkinRecoveryPaths ||
        {},
      repairLane: {
        selectedRows: Number(repairSelection?.diagnostics?.repair_candidates_selected || 0),
        skippedCooldownRows: Number(repairSelection?.diagnostics?.skippedCooldownRows || 0),
        skippedNonRepairRows: Number(repairSelection?.diagnostics?.skippedNonRepairRows || 0),
        quoteRowsSelected: Number(repairRefreshSummary.quoteRowsSelected || 0),
        snapshotRowsSelected: Number(repairRefreshSummary.snapshotRowsSelected || 0),
        snapshotBlocked: Boolean(repairRefreshSummary.snapshotBlocked),
        snapshotBlockedReason: repairRefreshSummary.snapshotBlockedReason,
        snapshotOutcomesByReason: repairRefreshSummary.snapshotOutcomesByReason || {},
        error: normalizeText(repairRefresh?.error) || null
      },
      consecutive_near_eligible_growth_runs: consecutiveNearEligibleGrowth,
      degradedScannerHealth,
      priorityCoverage: {
        totalPriorityItemsConfigured: Number(priorityCoverage?.totalPriorityItemsConfigured || 0),
        matchedExistingCatalogItems: Number(priorityCoverage?.matchedExistingCatalogItems || 0),
        insertedMissingCatalogItems: Number(priorityCoverage?.insertedMissingCatalogItems || 0),
        error: normalizeText(priorityCoverage?.error) || null
      },
      activeUniverseRefresh: universeRefresh
    }
  }
}

module.exports = {
  runProgressionBatch,
  __testables: {
    isRowDueForProgression,
    summarizeDueBacklog,
    buildCoverageMetrics,
    buildTransitionMetrics,
    sortProgressionRows
  }
}

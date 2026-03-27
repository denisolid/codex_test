const { ENRICHMENT_INTERVAL_MS } = require("./config")

const CANDIDATE_STATUS = Object.freeze({
  CANDIDATE: "candidate",
  ENRICHING: "enriching",
  NEAR_ELIGIBLE: "near_eligible",
  ELIGIBLE: "eligible",
  REJECTED: "rejected"
})

const FRESHNESS_RULES_MINUTES = Object.freeze({
  weapon_skin: 120,
  case: 180,
  sticker_capsule: 240,
  knife: 240,
  glove: 240
})

const REPAIR_REASON = Object.freeze({
  REPAIRED_MARKET_COVERAGE: "repaired_market_coverage",
  REPAIRED_REFERENCE_PRICE: "repaired_reference_price",
  REPAIRED_FRESHNESS: "repaired_freshness",
  STILL_UNUSABLE_MARKET_COVERAGE: "still_unusable_market_coverage",
  STILL_MISSING_REFERENCE_PRICE: "still_missing_reference_price",
  STILL_STALE_AFTER_REPAIR: "still_stale_after_repair",
  COOLDOWN_AFTER_FAILED_REPAIR: "cooldown_after_failed_repair",
  REJECTED_AFTER_FAILED_REPAIR: "rejected_after_failed_repair"
})

const MAX_REPAIR_REJECT_ATTEMPTS = 3
const REPAIR_BACKOFF_MULTIPLIERS = Object.freeze([1, 2, 4, 8])
const REPAIR_ATTEMPTS_PREFIX = "repair_attempts:"
const REPAIR_COOLDOWN_UNTIL_PREFIX = "repair_cooldown_until:"

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

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function normalizeCandidateStatus(value, fallback = CANDIDATE_STATUS.CANDIDATE) {
  const normalized = normalizeText(value).toLowerCase()
  if (Object.values(CANDIDATE_STATUS).includes(normalized)) {
    return normalized
  }
  return fallback
}

function resolveLatestSignalIso(row = {}) {
  const timestamps = [
    row?.last_market_signal_at,
    row?.lastMarketSignalAt,
    row?.quote_fetched_at,
    row?.quoteFetchedAt,
    row?.snapshot_captured_at,
    row?.snapshotCapturedAt
  ]
  let latestIso = null
  let latestTs = Number.NaN
  for (const value of timestamps) {
    const iso = toIsoOrNull(value)
    if (!iso) continue
    const ts = new Date(iso).getTime()
    if (!Number.isFinite(ts)) continue
    if (!Number.isFinite(latestTs) || ts > latestTs) {
      latestIso = iso
      latestTs = ts
    }
  }
  return latestIso
}

function resolveFreshnessMaxMinutes(category = "") {
  const normalizedCategory = normalizeText(category).toLowerCase()
  return Number(FRESHNESS_RULES_MINUTES[normalizedCategory] || FRESHNESS_RULES_MINUTES.weapon_skin)
}

function isFreshnessUsable(row = {}, nowMs = Date.now()) {
  if (row?.snapshot_stale === true || row?.snapshotStale === true) {
    return false
  }
  const latestIso = resolveLatestSignalIso(row)
  if (!latestIso) return false
  const ageMs = nowMs - new Date(latestIso).getTime()
  if (!Number.isFinite(ageMs) || ageMs < 0) return false
  return ageMs <= resolveFreshnessMaxMinutes(row?.category || row?.itemCategory) * 60 * 1000
}

function getRepairNeeds(row = {}, nowMs = Date.now()) {
  const marketCoverageCount = Math.max(
    Number(row?.market_coverage_count ?? row?.marketCoverageCount ?? 0),
    0
  )
  const referencePrice = toPositiveOrNull(row?.reference_price ?? row?.referencePrice)
  const quoteFetchedAt = toIsoOrNull(row?.quote_fetched_at || row?.quoteFetchedAt)
  const snapshotCapturedAt = toIsoOrNull(row?.snapshot_captured_at || row?.snapshotCapturedAt)
  const missingReference =
    row?.missing_reference == null ? referencePrice == null : Boolean(row.missing_reference)
  const missingCoverage =
    row?.missing_market_coverage == null
      ? marketCoverageCount <= 0
      : Boolean(row.missing_market_coverage) || marketCoverageCount <= 0
  const missingTimestamps = !quoteFetchedAt || !snapshotCapturedAt
  const staleFreshness = !isFreshnessUsable(row, nowMs)
  const needsFreshnessRepair = missingTimestamps || staleFreshness
  const usableSignalCount =
    Number(referencePrice != null) +
    Number(marketCoverageCount > 0) +
    Number(Boolean(quoteFetchedAt)) +
    Number(Boolean(snapshotCapturedAt)) +
    Number(isFreshnessUsable(row, nowMs))

  return {
    missingReference,
    missingCoverage,
    missingTimestamps,
    staleFreshness,
    needsFreshnessRepair,
    needsRepair: missingReference || missingCoverage || needsFreshnessRepair,
    usableSignalCount,
    emptySignals: usableSignalCount === 0
  }
}

function parseRepairAttemptCount(row = {}) {
  const blockers = Array.isArray(row?.progression_blockers)
    ? row.progression_blockers
    : Array.isArray(row?.progressionBlockers)
      ? row.progressionBlockers
      : []
  for (const value of blockers) {
    const text = normalizeText(value)
    if (!text.toLowerCase().startsWith(REPAIR_ATTEMPTS_PREFIX)) continue
    const parsed = Number(text.slice(REPAIR_ATTEMPTS_PREFIX.length))
    if (Number.isFinite(parsed) && parsed >= 0) {
      return Math.max(Math.round(parsed), 0)
    }
  }
  return 0
}

function parseRepairCooldownUntil(row = {}) {
  const blockers = Array.isArray(row?.progression_blockers)
    ? row.progression_blockers
    : Array.isArray(row?.progressionBlockers)
      ? row.progressionBlockers
      : []
  for (const value of blockers) {
    const text = normalizeText(value)
    if (!text.toLowerCase().startsWith(REPAIR_COOLDOWN_UNTIL_PREFIX)) continue
    return toIsoOrNull(text.slice(REPAIR_COOLDOWN_UNTIL_PREFIX.length))
  }
  return null
}

function isRepairCooldownActive(row = {}, nowMs = Date.now()) {
  const cooldownUntil = parseRepairCooldownUntil(row)
  if (!cooldownUntil) return false
  return new Date(cooldownUntil).getTime() > nowMs
}

function getRepairBackoffMs(attemptCount = 0) {
  const safeAttempt = Math.max(Math.round(Number(attemptCount || 0)), 1)
  const multiplier =
    REPAIR_BACKOFF_MULTIPLIERS[Math.min(safeAttempt - 1, REPAIR_BACKOFF_MULTIPLIERS.length - 1)] || 1
  return ENRICHMENT_INTERVAL_MS * multiplier
}

function buildRepairPriorityScore(row = {}, nowMs = Date.now()) {
  const needs = getRepairNeeds(row, nowMs)
  const status = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
  const statusBoost =
    status === CANDIDATE_STATUS.ELIGIBLE
      ? 46
      : status === CANDIDATE_STATUS.NEAR_ELIGIBLE
        ? 38
        : status === CANDIDATE_STATUS.ENRICHING
          ? 27
          : 16
  const liquidityRank = Math.max(Number(row?.liquidity_rank ?? row?.liquidityRank ?? 0), 0)
  const priorityBoost = Math.max(Number(row?.priority_boost ?? row?.priorityBoost ?? 0), 0)
  const coverageCount = Math.max(Number(row?.market_coverage_count ?? row?.marketCoverageCount ?? 0), 0)
  const volume7d = Math.max(Number(row?.volume_7d ?? row?.volume7d ?? 0), 0)
  const usableSignalsBoost = needs.usableSignalCount * 18
  const partialReadinessBoost =
    Number(!needs.missingReference) * 18 +
    Number(!needs.missingCoverage) * 16 +
    Number(!needs.needsFreshnessRepair) * 14
  const emptyPenalty = needs.emptySignals ? 42 : 0
  const stalePenalty = needs.staleFreshness ? 10 : 0
  return Number(
    (
      statusBoost +
      usableSignalsBoost +
      partialReadinessBoost +
      Math.min(liquidityRank * 0.22, 20) +
      Math.min(priorityBoost * 0.06, 24) +
      Math.min(coverageCount * 8, 16) +
      Math.min(volume7d / 15, 18) -
      emptyPenalty -
      stalePenalty
    ).toFixed(2)
  )
}

function sortRepairRows(rows = [], nowMs = Date.now()) {
  return [...rows].sort((left, right) => {
    const scoreDelta = buildRepairPriorityScore(right, nowMs) - buildRepairPriorityScore(left, nowMs)
    if (scoreDelta !== 0) return scoreDelta
    const priorityDelta =
      Number(right?.enrichment_priority ?? right?.enrichmentPriority ?? 0) -
      Number(left?.enrichment_priority ?? left?.enrichmentPriority ?? 0)
    if (priorityDelta !== 0) return priorityDelta
    const liquidityDelta =
      Number(right?.liquidity_rank ?? right?.liquidityRank ?? 0) -
      Number(left?.liquidity_rank ?? left?.liquidityRank ?? 0)
    if (liquidityDelta !== 0) return liquidityDelta
    return normalizeText(left?.market_hash_name || left?.marketHashName).localeCompare(
      normalizeText(right?.market_hash_name || right?.marketHashName)
    )
  })
}

function selectRepairCandidates(rows = [], options = {}) {
  const nowMs = Number(options.nowMs || Date.now())
  const limit = Math.max(Math.round(Number(options.limit || rows.length || 0)), 0)
  const selected = []
  const skippedCooldownRows = []
  const skippedNonRepairRows = []
  const seen = new Set()
  const repairableRows = []

  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName || seen.has(marketHashName)) continue
    seen.add(marketHashName)
    const status = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
    if (status === CANDIDATE_STATUS.REJECTED) continue
    const needs = getRepairNeeds(row, nowMs)
    if (!needs.needsRepair) {
      skippedNonRepairRows.push(row)
      continue
    }
    if (isRepairCooldownActive(row, nowMs)) {
      skippedCooldownRows.push(row)
      continue
    }
    repairableRows.push(row)
  }

  for (const row of sortRepairRows(repairableRows, nowMs)) {
    if (selected.length >= limit) break
    selected.push(row)
  }

  return {
    rows: selected,
    diagnostics: {
      repair_candidates_selected: selected.length,
      skippedCooldownRows: skippedCooldownRows.length,
      skippedNonRepairRows: skippedNonRepairRows.length
    },
    skippedCooldownRows,
    skippedNonRepairRows
  }
}

function isRepairRecoveryUnlikely(row = {}, failureReasons = [], attemptCount = 0, nowMs = Date.now()) {
  if (attemptCount < MAX_REPAIR_REJECT_ATTEMPTS) return false
  const marketCoverageCount = Math.max(
    Number(row?.market_coverage_count ?? row?.marketCoverageCount ?? 0),
    0
  )
  const referencePrice = toPositiveOrNull(row?.reference_price ?? row?.referencePrice)
  const quoteFetchedAt = toIsoOrNull(row?.quote_fetched_at || row?.quoteFetchedAt)
  const snapshotCapturedAt = toIsoOrNull(row?.snapshot_captured_at || row?.snapshotCapturedAt)
  const invalidReason = normalizeText(row?.invalid_reason || row?.invalidReason || row?.eligibility_reason || row?.eligibilityReason)
  const structuralReason = /\brejected|anti[_\s-]?fake|unsupported|outofscope|namepattern|not[_\s-]?tradable\b/i.test(
    invalidReason
  )
  if (structuralReason) return true
  return (
    marketCoverageCount <= 0 &&
    referencePrice == null &&
    !quoteFetchedAt &&
    !snapshotCapturedAt &&
    !isFreshnessUsable(row, nowMs) &&
    failureReasons.length >= 2
  )
}

function buildRepairDecision({ previousRow = {}, currentRow = {}, nowMs = Date.now() } = {}) {
  const previousNeeds = getRepairNeeds(previousRow, nowMs)
  if (!previousNeeds.needsRepair) {
    return {
      attempted: false,
      patch: null,
      repairedReasons: [],
      failedReasons: [],
      repairedToNearEligible: false,
      repairedToEligible: false,
      cooldownApplied: false,
      rejectedApplied: false,
      primaryFailedReason: null
    }
  }

  const currentNeeds = getRepairNeeds(currentRow, nowMs)
  const repairedReasons = []
  const failedReasons = []

  if (previousNeeds.missingCoverage) {
    if (!currentNeeds.missingCoverage) {
      repairedReasons.push(REPAIR_REASON.REPAIRED_MARKET_COVERAGE)
    } else {
      failedReasons.push(REPAIR_REASON.STILL_UNUSABLE_MARKET_COVERAGE)
    }
  }
  if (previousNeeds.missingReference) {
    if (!currentNeeds.missingReference) {
      repairedReasons.push(REPAIR_REASON.REPAIRED_REFERENCE_PRICE)
    } else {
      failedReasons.push(REPAIR_REASON.STILL_MISSING_REFERENCE_PRICE)
    }
  }
  if (previousNeeds.needsFreshnessRepair) {
    if (!currentNeeds.needsFreshnessRepair) {
      repairedReasons.push(REPAIR_REASON.REPAIRED_FRESHNESS)
    } else {
      failedReasons.push(REPAIR_REASON.STILL_STALE_AFTER_REPAIR)
    }
  }

  const currentStatus = normalizeCandidateStatus(
    currentRow?.candidate_status ?? currentRow?.candidateStatus,
    CANDIDATE_STATUS.CANDIDATE
  )
  const scanEligible =
    currentRow?.scan_eligible == null ? Boolean(currentRow?.scanEligible) : Boolean(currentRow.scan_eligible)
  const becameUsable =
    currentStatus === CANDIDATE_STATUS.NEAR_ELIGIBLE ||
    (currentStatus === CANDIDATE_STATUS.ELIGIBLE && scanEligible)
  const previousAttempts = parseRepairAttemptCount(previousRow)
  const nextAttemptCount = previousAttempts + 1
  const nowIso = new Date(nowMs).toISOString()

  if (repairedReasons.length && becameUsable) {
    return {
      attempted: true,
      patch: {
        ...currentRow,
        progression_status: repairedReasons[0],
        progression_blockers: repairedReasons.slice(),
        last_enriched_at: nowIso
      },
      repairedReasons,
      failedReasons,
      repairedToNearEligible: currentStatus === CANDIDATE_STATUS.NEAR_ELIGIBLE,
      repairedToEligible: currentStatus === CANDIDATE_STATUS.ELIGIBLE && scanEligible,
      cooldownApplied: false,
      rejectedApplied: false,
      primaryFailedReason: null
    }
  }

  const primaryFailedReason = failedReasons[0] || REPAIR_REASON.STILL_STALE_AFTER_REPAIR
  if (isRepairRecoveryUnlikely(currentRow, failedReasons, nextAttemptCount, nowMs)) {
    return {
      attempted: true,
      patch: {
        ...currentRow,
        scan_eligible: false,
        candidate_status: CANDIDATE_STATUS.REJECTED,
        maturity_state: "cold",
        scan_layer: "cold",
        progression_status: REPAIR_REASON.REJECTED_AFTER_FAILED_REPAIR,
        progression_blockers: [
          ...repairedReasons,
          ...failedReasons,
          `${REPAIR_ATTEMPTS_PREFIX}${nextAttemptCount}`
        ],
        eligibility_reason: primaryFailedReason,
        invalid_reason: normalizeText(currentRow?.invalid_reason || currentRow?.invalidReason) || primaryFailedReason,
        last_enriched_at: nowIso
      },
      repairedReasons,
      failedReasons,
      repairedToNearEligible: false,
      repairedToEligible: false,
      cooldownApplied: false,
      rejectedApplied: true,
      primaryFailedReason
    }
  }

  const cooldownUntilIso = new Date(nowMs + getRepairBackoffMs(nextAttemptCount)).toISOString()
  return {
    attempted: true,
      patch: {
        ...currentRow,
        scan_eligible: false,
        progression_status: REPAIR_REASON.COOLDOWN_AFTER_FAILED_REPAIR,
        progression_blockers: [
          ...repairedReasons,
          ...failedReasons,
          `${REPAIR_ATTEMPTS_PREFIX}${nextAttemptCount}`,
          `${REPAIR_COOLDOWN_UNTIL_PREFIX}${cooldownUntilIso}`
        ],
      eligibility_reason:
        normalizeText(currentRow?.eligibility_reason || currentRow?.eligibilityReason) || primaryFailedReason,
      last_enriched_at: nowIso
    },
    repairedReasons,
    failedReasons,
    repairedToNearEligible: false,
    repairedToEligible: false,
    cooldownApplied: true,
    rejectedApplied: false,
    primaryFailedReason
  }
}

function summarizeRepairDecisions(decisions = []) {
  const topFailedRepairReasons = {}
  const summary = {
    repaired_rows: 0,
    repaired_to_near_eligible: 0,
    repaired_to_eligible: 0,
    cooldown_after_failed_repair: 0,
    rejected_after_failed_repair: 0,
    top_failed_repair_reasons: topFailedRepairReasons
  }

  for (const decision of Array.isArray(decisions) ? decisions : []) {
    if (!decision?.attempted) continue
    if (decision.repairedToNearEligible || decision.repairedToEligible) {
      summary.repaired_rows += 1
    }
    if (decision.repairedToNearEligible) {
      summary.repaired_to_near_eligible += 1
    }
    if (decision.repairedToEligible) {
      summary.repaired_to_eligible += 1
    }
    if (decision.cooldownApplied) {
      summary.cooldown_after_failed_repair += 1
    }
    if (decision.rejectedApplied) {
      summary.rejected_after_failed_repair += 1
    }
    if (decision.primaryFailedReason) {
      topFailedRepairReasons[decision.primaryFailedReason] =
        Number(topFailedRepairReasons[decision.primaryFailedReason] || 0) + 1
    }
  }

  return summary
}

module.exports = {
  REPAIR_REASON,
  selectRepairCandidates,
  buildRepairDecision,
  summarizeRepairDecisions,
  isRepairCooldownActive,
  parseRepairAttemptCount,
  getRepairNeeds,
  buildRepairPriorityScore,
  __testables: {
    normalizeCandidateStatus,
    isFreshnessUsable,
    getRepairBackoffMs,
    parseRepairCooldownUntil
  }
}

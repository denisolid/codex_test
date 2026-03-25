const scannerRunRepo = require("../repositories/scannerRunRepository")

function normalizeText(value) {
  return String(value || "").trim()
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function resolveCounter(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 0
  return Math.max(Math.round(parsed), 0)
}

async function acquire({ jobType, trigger, startedAt, timeoutMs } = {}) {
  const safeStartedAt = startedAt || new Date().toISOString()
  const result = await scannerRunRepo.tryCreateRunningRun({
    scannerType: normalizeText(jobType),
    startedAt: safeStartedAt,
    diagnosticsSummary: {
      trigger: normalizeText(trigger) || null,
      timeoutMs: Number(timeoutMs || 0) || null
    }
  })

  return {
    acquired: !Boolean(result?.alreadyRunning),
    leaseId: result?.run?.id || null,
    existingRunId: result?.existingRun?.id || null,
    startedAt: safeStartedAt
  }
}

async function recoverExpired({ jobType, cutoffIso, failureReason } = {}) {
  return scannerRunRepo.timeoutStaleRunningRuns(normalizeText(jobType), {
    cutoffIso,
    nowIso: new Date().toISOString(),
    failureReason:
      normalizeText(failureReason) || `${normalizeText(jobType)}_hard_timeout_recovery`,
    diagnosticsSummary: {
      recoveredFromStaleLock: true
    }
  })
}

async function complete({ leaseId, completedAt, counters, diagnostics } = {}) {
  if (!normalizeText(leaseId)) return
  const safeCounters = toJsonObject(counters)
  await scannerRunRepo.markCompleted(leaseId, {
    completedAt: completedAt || new Date().toISOString(),
    itemsScanned: resolveCounter(
      safeCounters.scannedCount ?? safeCounters.selectedCount ?? safeCounters.attemptedCount
    ),
    opportunitiesFound: resolveCounter(
      safeCounters.publishedCount ??
        safeCounters.refreshedLiveCount ??
        safeCounters.updatedCount ??
        safeCounters.activeRowsWritten
    ),
    newOpportunitiesAdded: resolveCounter(
      safeCounters.newCount ?? safeCounters.reactivatedCount ?? safeCounters.historyRowsWritten
    ),
    diagnosticsSummary: toJsonObject(diagnostics)
  })
}

async function fail({ leaseId, completedAt, error, diagnostics, status = "failed" } = {}) {
  if (!normalizeText(leaseId)) return
  await scannerRunRepo.markFailed(leaseId, {
    completedAt: completedAt || new Date().toISOString(),
    status,
    error: normalizeText(error) || null,
    failureReason: normalizeText(error) || null,
    diagnosticsSummary: toJsonObject(diagnostics)
  })
}

module.exports = {
  acquire,
  recoverExpired,
  complete,
  fail
}

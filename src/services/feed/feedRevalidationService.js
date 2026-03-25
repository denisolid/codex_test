const env = require("../../config/env")
const globalActiveOpportunityRepo = require("../../repositories/globalActiveOpportunityRepository")
const globalOpportunityHistoryRepo = require("../../repositories/globalOpportunityHistoryRepository")
const diagnosticsWriter = require("../diagnosticsWriter")
const marketStateReadService = require("../marketStateReadService")
const scannerRunLeaseService = require("../scannerRunLeaseService")
const feedCompatibilityProjector = require("./feedCompatibilityProjector")
const { buildHistoryRow, resolveExitEventType } = require("./feedHistoryPolicy")
const {
  FEED_REVALIDATION_INTERVAL_MS,
  FEED_REVALIDATION_INTERVAL_MINUTES,
  SCANNER_TYPES
} = require("../scanner/config")
const {
  buildRefreshedOpportunityRow,
  QUOTE_LOOKBACK_HOURS
} = require("../feedPublishRefreshService")

const schedulerState = {
  timer: null,
  inFlight: null
}

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function buildActiveRevalidationPatch(refreshed = {}, nowIso) {
  const patch = refreshed?.patch || {}
  return {
    buy_price: patch.buy_price,
    sell_net: patch.sell_net,
    profit: patch.profit,
    spread_pct: patch.spread_pct,
    liquidity_label: patch.liquidity_label,
    market_signal_observed_at: patch.market_signal_observed_at,
    refresh_status: patch.refresh_status,
    live_status: patch.live_status,
    latest_signal_age_hours: patch.latest_signal_age_hours,
    last_revalidation_attempt_at: toIsoOrNull(nowIso) || new Date().toISOString(),
    metadata: toJsonObject(patch.metadata)
  }
}

function resolveExpireReason(refreshed = {}) {
  return (
    normalizeText(
      refreshed?.patch?.metadata?.publish_refresh?.stale_reason ||
        refreshed?.patch?.metadata?.stale_reason ||
        refreshed?.api?.staleReason ||
        refreshed?.api?.stale_reason
    ) || "revalidation_expired"
  )
}

function isKeepaliveUnchanged(row = {}, patch = {}) {
  return (
    toFiniteOrNull(row.buy_price) === toFiniteOrNull(patch.buy_price) &&
    toFiniteOrNull(row.sell_net) === toFiniteOrNull(patch.sell_net) &&
    toFiniteOrNull(row.profit) === toFiniteOrNull(patch.profit) &&
    toFiniteOrNull(row.spread_pct) === toFiniteOrNull(patch.spread_pct) &&
    normalizeText(row.liquidity_label) === normalizeText(patch.liquidity_label) &&
    normalizeText(row.live_status) === normalizeText(patch.live_status) &&
    normalizeText(row.refresh_status) === normalizeText(patch.refresh_status) &&
    toIsoOrNull(row.market_signal_observed_at) === toIsoOrNull(patch.market_signal_observed_at)
  )
}

function buildRowsByFingerprint(rows = []) {
  const byFingerprint = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const fingerprint = normalizeText(row?.opportunity_fingerprint).toLowerCase()
    if (fingerprint && !byFingerprint[fingerprint]) {
      byFingerprint[fingerprint] = row
    }
  }
  return byFingerprint
}

async function runSweepForRows(rows = [], { nowIso, trigger, runId, heartbeat } = {}) {
  const safeNowIso = toIsoOrNull(nowIso) || new Date().toISOString()
  const safeRows = Array.isArray(rows) ? rows : []
  if (!safeRows.length) {
    return {
      scannedCount: 0,
      refreshedLiveCount: 0,
      staleExpiredCount: 0,
      degradedCount: 0,
      unchangedCount: 0,
      historyRowsWritten: 0,
      compatibilityRowsWritten: 0
    }
  }

  const itemNames = Array.from(
    new Set(safeRows.map((row) => normalizeText(row.item_name || row.market_hash_name)).filter(Boolean))
  )
  const fingerprints = Array.from(
    new Set(
      safeRows
        .map((row) => normalizeText(row.opportunity_fingerprint).toLowerCase())
        .filter(Boolean)
    )
  )
  const quoteRowsByItem = await marketStateReadService.getLatestQuotesByItemNames({
    itemNames,
    lookbackHours: QUOTE_LOOKBACK_HOURS,
    includeQualityFlags: true
  })

  const activeUpdates = []
  const historyPlans = []
  const projectionContextByFingerprint = {}
  let refreshedLiveCount = 0
  let staleExpiredCount = 0
  let degradedCount = 0
  let unchangedCount = 0

  for (let index = 0; index < safeRows.length; index += 1) {
    const row = safeRows[index]
    if (heartbeat && (index === 0 || index % 25 === 0)) {
      await heartbeat({ processedCount: index })
    }
    const refreshed = buildRefreshedOpportunityRow(row, quoteRowsByItem, {
      nowIso: safeNowIso,
      nowMs: Date.parse(safeNowIso)
    })
    const activePatch = buildActiveRevalidationPatch(refreshed, safeNowIso)
    const fingerprint = normalizeText(row.opportunity_fingerprint).toLowerCase()
    projectionContextByFingerprint[fingerprint] = {
      net_profit_after_fees:
        toFiniteOrNull(refreshed?.raw?.net_profit_after_fees) ??
        toFiniteOrNull(activePatch.profit),
      confidence_score: toFiniteOrNull(refreshed?.raw?.confidence_score),
      freshness_score: toFiniteOrNull(refreshed?.raw?.freshness_score),
      verdict: normalizeText(refreshed?.raw?.verdict).toLowerCase() || null
    }

    activeUpdates.push({
      id: row.id,
      patch: activePatch
    })

    if (normalizeText(activePatch.live_status).toLowerCase() === "live") {
      if (isKeepaliveUnchanged(row, activePatch)) {
        unchangedCount += 1
      } else {
        refreshedLiveCount += 1
      }
      continue
    }

    const reason = resolveExpireReason(refreshed)
    const exitEventType = resolveExitEventType({
      liveStatus: activePatch.live_status,
      refreshStatus: activePatch.refresh_status
    })
    if (exitEventType === "expired") {
      staleExpiredCount += 1
    } else {
      degradedCount += 1
    }
    historyPlans.push({
      fingerprint,
      eventType: exitEventType,
      reason
    })
  }

  await globalActiveOpportunityRepo.updateRowsById(activeUpdates)
  if (heartbeat) {
    await heartbeat({ processedCount: safeRows.length, stage: "active_updates_applied" })
  }

  const persistedActiveRows = fingerprints.length
    ? await globalActiveOpportunityRepo.getRowsByFingerprints({
        fingerprints,
        includeExpired: true,
        limit: Math.max(250, fingerprints.length * 2)
      })
    : []
  const persistedActiveByFingerprint = buildRowsByFingerprint(persistedActiveRows)

  const compatibilityResult = persistedActiveRows.length
    ? await feedCompatibilityProjector.syncRows({
        activeRows: Object.values(persistedActiveByFingerprint),
        stage: "revalidate",
        nowIso: safeNowIso,
        projectionContextByFingerprint
      })
    : { rowsWritten: 0 }

  const historyRows = historyPlans
    .map((plan) =>
      buildHistoryRow({
        writerStage: "revalidate",
        scanRunId: runId,
        eventType: plan.eventType,
        activeRow: persistedActiveByFingerprint[normalizeText(plan.fingerprint).toLowerCase()],
        eventAt: safeNowIso,
        reason: plan.reason,
        materiallyChanged: true
      })
    )
    .filter(Boolean)
  const insertedHistory = historyRows.length
    ? await globalOpportunityHistoryRepo.insertRows(historyRows)
    : []

  const result = {
    scannedCount: safeRows.length,
    refreshedLiveCount,
    staleExpiredCount,
    degradedCount,
    unchangedCount,
    historyRowsWritten: Array.isArray(insertedHistory) ? insertedHistory.length : 0,
    compatibilityRowsWritten: Number(compatibilityResult?.rowsWritten || 0)
  }

  await diagnosticsWriter.writeRevalidationBatch({
    runId,
    counters: result,
    trigger,
    timings: {
      ranAt: safeNowIso
    }
  })
  return result
}

async function withLease(rowLoader, { nowIso, limit, trigger } = {}) {
  const safeNowIso = toIsoOrNull(nowIso) || new Date().toISOString()
  const hardTimeoutMs = Math.max(FEED_REVALIDATION_INTERVAL_MS * 2, 10 * 60 * 1000)
  await scannerRunLeaseService.recoverExpired({
    jobType: SCANNER_TYPES.FEED_REVALIDATION,
    cutoffIso: new Date(Date.parse(safeNowIso) - hardTimeoutMs).toISOString(),
    failureReason: "feed_revalidation_hard_timeout_recovery"
  })

  const lease = await scannerRunLeaseService.acquire({
    jobType: SCANNER_TYPES.FEED_REVALIDATION,
    trigger,
    startedAt: safeNowIso,
    timeoutMs: hardTimeoutMs
  })
  if (!lease.acquired) {
    return {
      scannedCount: 0,
      refreshedLiveCount: 0,
      staleExpiredCount: 0,
      degradedCount: 0,
      unchangedCount: 0,
      historyRowsWritten: 0,
      compatibilityRowsWritten: 0
    }
  }

  try {
    const rows = await rowLoader(limit)
    await scannerRunLeaseService.heartbeat({
      leaseId: lease.leaseId,
      heartbeatAt: new Date().toISOString(),
      diagnostics: {
        trigger,
        stage: "rows_loaded",
        selectedCount: Array.isArray(rows) ? rows.length : 0
      }
    })
    const result = await runSweepForRows(rows, {
      nowIso: safeNowIso,
      trigger,
      runId: lease.leaseId,
      heartbeat: ({ processedCount, stage }) =>
        scannerRunLeaseService.heartbeat({
          leaseId: lease.leaseId,
          heartbeatAt: new Date().toISOString(),
          diagnostics: {
            trigger,
            stage: normalizeText(stage) || "revalidation_progress",
            processedCount: Number(processedCount || 0)
          }
        })
    })
    await scannerRunLeaseService.complete({
      leaseId: lease.leaseId,
      completedAt: new Date().toISOString(),
      counters: result,
      diagnostics: {
        trigger
      }
    })
    return result
  } catch (err) {
    await scannerRunLeaseService.fail({
      leaseId: lease.leaseId,
      completedAt: new Date().toISOString(),
      error: err?.message || "feed_revalidation_failed",
      diagnostics: {
        trigger
      }
    })
    throw err
  }
}

async function runScheduledSweep({ nowIso, limit = 200, trigger = "scheduled_feed_revalidation" } = {}) {
  return withLease(
    (safeLimit) =>
      globalActiveOpportunityRepo.listRowsForRevalidation({
        limit: safeLimit
      }),
    { nowIso, limit, trigger }
  )
}

async function revalidateByItemNames({
  itemNames = [],
  nowIso,
  trigger = "targeted_feed_revalidation"
} = {}) {
  const normalizedNames = Array.from(
    new Set((Array.isArray(itemNames) ? itemNames : []).map((value) => normalizeText(value)).filter(Boolean))
  )
  return withLease(
    () =>
      globalActiveOpportunityRepo.getRecentRowsByItems({
        itemNames: normalizedNames,
        includeExpired: false,
        limit: Math.max(normalizedNames.length * 20, 50)
      }),
    { nowIso, limit: normalizedNames.length, trigger }
  )
}

function startScheduler() {
  if (schedulerState.timer) return
  if (!env.globalFeedV2Enabled) return
  if (FEED_REVALIDATION_INTERVAL_MINUTES <= 0 || FEED_REVALIDATION_INTERVAL_MS <= 0) return

  schedulerState.timer = setInterval(() => {
    if (schedulerState.inFlight) return
    schedulerState.inFlight = runScheduledSweep().finally(() => {
      schedulerState.inFlight = null
    })
    schedulerState.inFlight.catch(() => null)
  }, FEED_REVALIDATION_INTERVAL_MS)

  if (typeof schedulerState.timer?.unref === "function") {
    schedulerState.timer.unref()
  }
}

function stopScheduler() {
  if (schedulerState.timer) {
    clearInterval(schedulerState.timer)
    schedulerState.timer = null
  }
}

module.exports = {
  runScheduledSweep,
  revalidateByItemNames,
  startScheduler,
  stopScheduler
}

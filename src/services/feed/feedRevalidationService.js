const env = require("../../config/env")
const arbitrageFeedRepo = require("../../repositories/arbitrageFeedRepository")
const globalActiveOpportunityRepo = require("../../repositories/globalActiveOpportunityRepository")
const globalOpportunityHistoryRepo = require("../../repositories/globalOpportunityHistoryRepository")
const diagnosticsWriter = require("../diagnosticsWriter")
const marketStateReadService = require("../marketStateReadService")
const scannerRunLeaseService = require("../scannerRunLeaseService")
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

function buildHistorySourceEventKey({
  runId,
  eventType,
  fingerprint,
  materialChangeHash,
  liveStatus,
  refreshStatus,
  reason
} = {}) {
  const { createHash } = require("crypto")
  const payload = [
    "revalidate",
    normalizeText(runId).toLowerCase() || "na",
    normalizeText(eventType).toLowerCase() || "na",
    normalizeText(fingerprint).toLowerCase() || "na",
    normalizeText(materialChangeHash).toLowerCase() || "na",
    normalizeText(liveStatus).toLowerCase() || "na",
    normalizeText(refreshStatus).toLowerCase() || "na",
    normalizeText(reason).toLowerCase() || "na"
  ].join("|")
  return `goh_${createHash("sha1").update(payload).digest("hex")}`
}

function buildActiveRevalidationPatch(refreshed = {}) {
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
    metadata: toJsonObject(patch.metadata)
  }
}

function buildLegacyRevalidationPatch(previousLegacy = {}, refreshed = {}, nowIso) {
  const patch = refreshed?.patch || {}
  const raw = refreshed?.raw || {}
  return {
    buy_price: patch.buy_price,
    sell_net: patch.sell_net,
    profit: patch.profit,
    spread_pct: patch.spread_pct,
    market_signal_observed_at: patch.market_signal_observed_at,
    insight_refreshed_at: nowIso,
    last_refresh_attempt_at: nowIso,
    latest_signal_age_hours: patch.latest_signal_age_hours,
    net_profit_after_fees:
      toFiniteOrNull(raw.net_profit_after_fees) ?? toFiniteOrNull(patch.profit) ?? null,
    confidence_score: toFiniteOrNull(raw.confidence_score) ?? null,
    freshness_score: toFiniteOrNull(raw.freshness_score) ?? null,
    verdict: normalizeText(raw.verdict).toLowerCase() || null,
    liquidity_label: patch.liquidity_label,
    refresh_status: patch.refresh_status,
    live_status: patch.live_status,
    is_active: normalizeText(patch.live_status).toLowerCase() === "live",
    metadata: toJsonObject(patch.metadata)
  }
}

function buildHistorySnapshot(row = {}, options = {}) {
  return {
    item_name: row.item_name || row.market_hash_name || null,
    market_hash_name: row.market_hash_name || row.item_name || null,
    category: row.category || null,
    buy_market: row.buy_market || null,
    buy_price: row.buy_price ?? null,
    sell_market: row.sell_market || null,
    sell_net: row.sell_net ?? null,
    profit: row.profit ?? null,
    spread_pct: row.spread_pct ?? null,
    opportunity_score: row.opportunity_score ?? null,
    execution_confidence: row.execution_confidence || null,
    quality_grade: row.quality_grade || null,
    liquidity_label: row.liquidity_label || null,
    market_signal_observed_at: row.market_signal_observed_at || null,
    refresh_status: row.refresh_status || null,
    live_status: row.live_status || null,
    material_change_hash: row.material_change_hash || null,
    metadata: toJsonObject(row.metadata),
    reason: options.reason || null
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

async function runSweepForRows(rows = [], { nowIso, trigger, runId } = {}) {
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
  const legacyRows = fingerprints.length
    ? await arbitrageFeedRepo.getActiveRowsByFingerprints({
        fingerprints,
        limit: Math.max(250, fingerprints.length * 2)
      })
    : []
  const legacyByFingerprint = {}
  for (const row of legacyRows || []) {
    const fingerprint = normalizeText(
      row?.opportunity_fingerprint || row?.metadata?.opportunity_fingerprint
    ).toLowerCase()
    if (fingerprint && !legacyByFingerprint[fingerprint]) {
      legacyByFingerprint[fingerprint] = row
    }
  }

  const activeUpdates = []
  const legacyUpdates = []
  const historyRows = []
  let refreshedLiveCount = 0
  let staleExpiredCount = 0
  let degradedCount = 0
  let unchangedCount = 0

  for (const row of safeRows) {
    const refreshed = buildRefreshedOpportunityRow(row, quoteRowsByItem, {
      nowIso: safeNowIso,
      nowMs: Date.parse(safeNowIso)
    })
    const activePatch = buildActiveRevalidationPatch(refreshed)
    const legacyPatch = buildLegacyRevalidationPatch(
      legacyByFingerprint[normalizeText(row.opportunity_fingerprint).toLowerCase()] || null,
      refreshed,
      safeNowIso
    )

    activeUpdates.push({
      id: row.id,
      patch: activePatch
    })

    const fingerprint = normalizeText(row.opportunity_fingerprint).toLowerCase()
    const legacyRow = legacyByFingerprint[fingerprint]
    if (legacyRow && normalizeText(legacyRow.id)) {
      legacyUpdates.push({
        id: legacyRow.id,
        patch: legacyPatch
      })
    }

    if (normalizeText(activePatch.live_status).toLowerCase() === "live") {
      if (isKeepaliveUnchanged(row, activePatch)) {
        unchangedCount += 1
      } else {
        refreshedLiveCount += 1
      }
      continue
    }

    const reason = resolveExpireReason(refreshed)
    if (normalizeText(activePatch.live_status).toLowerCase() === "stale") {
      staleExpiredCount += 1
    } else {
      degradedCount += 1
    }
    historyRows.push({
      source_event_key: buildHistorySourceEventKey({
        runId,
        eventType: "expired",
        fingerprint,
        materialChangeHash: normalizeText(row.material_change_hash),
        liveStatus: activePatch.live_status,
        refreshStatus: activePatch.refresh_status,
        reason
      }),
      active_opportunity_id: row.id,
      opportunity_fingerprint: fingerprint,
      scan_run_id: normalizeText(runId) || null,
      event_type: "expired",
      event_at: safeNowIso,
      refresh_status: activePatch.refresh_status,
      live_status: activePatch.live_status,
      reason,
      snapshot: buildHistorySnapshot(
        {
          ...row,
          ...activePatch,
          metadata: activePatch.metadata
        },
        { reason }
      )
    })
  }

  await globalActiveOpportunityRepo.updateRowsById(activeUpdates)
  const insertedHistory = historyRows.length
    ? await globalOpportunityHistoryRepo.insertRows(historyRows)
    : []
  const compatibilityRowsWritten = legacyUpdates.length
    ? await arbitrageFeedRepo.updateRowsById(legacyUpdates)
    : 0

  const result = {
    scannedCount: safeRows.length,
    refreshedLiveCount,
    staleExpiredCount,
    degradedCount,
    unchangedCount,
    historyRowsWritten: Array.isArray(insertedHistory) ? insertedHistory.length : 0,
    compatibilityRowsWritten
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
    const result = await runSweepForRows(rows, {
      nowIso: safeNowIso,
      trigger,
      runId: lease.leaseId
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

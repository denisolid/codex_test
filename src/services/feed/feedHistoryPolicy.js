const { createHash } = require("crypto")

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

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function buildHistorySourceEventKey({
  writerStage,
  scanRunId,
  eventType,
  fingerprint,
  materialChangeHash,
  liveStatus,
  refreshStatus,
  reason
} = {}) {
  const payload = [
    normalizeText(writerStage).toLowerCase() || "publish",
    normalizeText(scanRunId).toLowerCase() || "na",
    normalizeText(eventType).toLowerCase() || "na",
    normalizeText(fingerprint).toLowerCase() || "na",
    normalizeText(materialChangeHash).toLowerCase() || "na",
    normalizeText(liveStatus).toLowerCase() || "na",
    normalizeText(refreshStatus).toLowerCase() || "na",
    normalizeText(reason).toLowerCase() || "na"
  ].join("|")
  return `goh_${createHash("sha1").update(payload).digest("hex")}`
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
    first_seen_at: row.first_seen_at || null,
    last_seen_at: row.last_seen_at || null,
    last_published_at: row.last_published_at || null,
    last_revalidation_attempt_at: row.last_revalidation_attempt_at || null,
    refresh_status: row.refresh_status || null,
    live_status: row.live_status || null,
    material_change_hash: row.material_change_hash || null,
    metadata: toJsonObject(row.metadata),
    reason: options.reason || null
  }
}

function resolveExitEventType({ liveStatus, refreshStatus } = {}) {
  const normalizedLiveStatus = normalizeText(liveStatus).toLowerCase()
  const normalizedRefreshStatus = normalizeText(refreshStatus).toLowerCase()

  if (normalizedLiveStatus === "stale" && normalizedRefreshStatus === "stale") {
    return "expired"
  }
  if (normalizedLiveStatus === "degraded") {
    return "degraded"
  }
  return null
}

function shouldWriteHistoryEvent({ eventType, materiallyChanged = true } = {}) {
  const normalized = normalizeText(eventType).toLowerCase()
  if (
    normalized === "new" ||
    normalized === "reactivated" ||
    normalized === "expired" ||
    normalized === "degraded"
  ) {
    return true
  }
  if (normalized === "updated") {
    return Boolean(materiallyChanged)
  }
  return false
}

function buildHistoryRow({
  writerStage,
  scanRunId,
  eventType,
  activeRow,
  eventAt,
  reason = null,
  materiallyChanged = true
} = {}) {
  if (!shouldWriteHistoryEvent({ eventType, materiallyChanged })) {
    return null
  }

  const safeRow =
    activeRow && typeof activeRow === "object" && !Array.isArray(activeRow) ? activeRow : null
  if (!safeRow) return null

  const safeEventAt = toIsoOrNull(eventAt) || new Date().toISOString()
  const fingerprint = normalizeText(safeRow.opportunity_fingerprint).toLowerCase()
  if (!fingerprint) return null

  const safeEventType = normalizeText(eventType).toLowerCase()
  const safeReason = normalizeText(reason) || null

  return {
    source_event_key: buildHistorySourceEventKey({
      writerStage,
      scanRunId,
      eventType: safeEventType,
      fingerprint,
      materialChangeHash: normalizeText(safeRow.material_change_hash) || null,
      liveStatus: safeRow.live_status,
      refreshStatus: safeRow.refresh_status,
      reason: safeReason
    }),
    active_opportunity_id: normalizeText(safeRow.id) || null,
    opportunity_fingerprint: fingerprint,
    scan_run_id: normalizeText(scanRunId) || null,
    event_type: safeEventType,
    event_at: safeEventAt,
    refresh_status: normalizeText(safeRow.refresh_status).toLowerCase() || null,
    live_status: normalizeText(safeRow.live_status).toLowerCase() || null,
    reason: safeReason,
    snapshot: buildHistorySnapshot(safeRow, { reason: safeReason })
  }
}

module.exports = {
  buildHistorySourceEventKey,
  buildHistorySnapshot,
  resolveExitEventType,
  shouldWriteHistoryEvent,
  buildHistoryRow
}

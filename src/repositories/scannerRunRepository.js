const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "scanner_runs"
const RUNNING_LOCK_INDEX = "idx_scanner_runs_single_active_per_type"

function normalizeScannerType(value) {
  const text = String(value || "global_arbitrage").trim()
  return text || "global_arbitrage"
}

function normalizeStatus(value, fallback = "running") {
  const text = String(value || fallback)
    .trim()
    .toLowerCase()
  if (!text) return fallback
  if (
    text === "queued" ||
    text === "running" ||
    text === "completed" ||
    text === "failed" ||
    text === "timed_out" ||
    text === "skipped_already_running"
  ) {
    return text
  }
  return fallback
}

function toInteger(value, fallback = 0) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), 0)
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function toDurationMs(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 0
  return Math.max(Math.round(parsed), 0)
}

function normalizeFailureReason(value) {
  const text = String(value || "").trim()
  return text || null
}

function isUniqueViolation(error) {
  return String(error?.code || "").trim() === "23505"
}

function isRunningLockConflict(error) {
  if (!isUniqueViolation(error)) return false
  const message = String(error?.message || "").toLowerCase()
  const details = String(error?.details || "").toLowerCase()
  return (
    message.includes(RUNNING_LOCK_INDEX) ||
    details.includes(RUNNING_LOCK_INDEX) ||
    message.includes("scanner_type") ||
    details.includes("scanner_type")
  )
}

exports.createRun = async (payload = {}) => {
  const status = normalizeStatus(payload.status, "running")
  const row = {
    scanner_type: normalizeScannerType(payload.scannerType),
    started_at: payload.startedAt || new Date().toISOString(),
    completed_at: payload.completedAt || null,
    status,
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
    diagnostics_summary: toJsonObject(payload.diagnosticsSummary),
    duration_ms: toDurationMs(payload.durationMs),
    failure_reason: normalizeFailureReason(payload.failureReason)
  }

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .insert(row)
    .select("*")
    .single()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data
}

async function getLatestRunningRunInternal(scannerType = "global_arbitrage") {
  const type = normalizeScannerType(scannerType)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("scanner_type", type)
    .eq("status", "running")
    .order("started_at", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.getLatestRunningRun = async (scannerType = "global_arbitrage") =>
  getLatestRunningRunInternal(scannerType)

exports.tryCreateRunningRun = async (payload = {}) => {
  const row = {
    scanner_type: normalizeScannerType(payload.scannerType),
    started_at: payload.startedAt || new Date().toISOString(),
    status: "running",
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
    diagnostics_summary: toJsonObject(payload.diagnosticsSummary),
    duration_ms: toDurationMs(payload.durationMs),
    failure_reason: normalizeFailureReason(payload.failureReason)
  }

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .insert(row)
    .select("*")
    .single()

  if (!error) {
    return {
      run: data,
      alreadyRunning: false,
      conflictReason: null,
      existingRun: null
    }
  }

  if (isRunningLockConflict(error)) {
    const existingRun = await getLatestRunningRunInternal(row.scanner_type).catch(() => null)
    return {
      run: null,
      alreadyRunning: true,
      conflictReason: "already_running",
      existingRun
    }
  }

  throw new AppError(error.message, 500)
}

exports.markCompleted = async (runId, payload = {}) => {
  const id = String(runId || "").trim()
  if (!id) return null

  const patch = {
    completed_at: payload.completedAt || new Date().toISOString(),
    status: "completed",
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
    diagnostics_summary: toJsonObject(payload.diagnosticsSummary),
    duration_ms: toDurationMs(payload.durationMs),
    failure_reason: null
  }

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .update(patch)
    .eq("id", id)
    .select("*")
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.markFailed = async (runId, payload = {}) => {
  const id = String(runId || "").trim()
  if (!id) return null

  const patch = {
    completed_at: payload.completedAt || new Date().toISOString(),
    status: normalizeStatus(payload.status, "failed"),
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
    duration_ms: toDurationMs(payload.durationMs),
    failure_reason: normalizeFailureReason(payload.failureReason || payload.error),
    diagnostics_summary: {
      ...toJsonObject(payload.diagnosticsSummary),
      error: String(payload.error || "").trim() || undefined
    }
  }

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .update(patch)
    .eq("id", id)
    .select("*")
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.getLatestRun = async (scannerType = "global_arbitrage") => {
  const type = normalizeScannerType(scannerType)

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("scanner_type", type)
    .order("started_at", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.getLatestCompletedRun = async (scannerType = "global_arbitrage") => {
  const type = normalizeScannerType(scannerType)

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("scanner_type", type)
    .eq("status", "completed")
    .order("completed_at", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.getById = async (runId) => {
  const id = String(runId || "").trim()
  if (!id) return null

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("id", id)
    .limit(1)
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.listRunningRuns = async (scannerType = "global_arbitrage", options = {}) => {
  const type = normalizeScannerType(scannerType)
  const limit = Math.max(Math.min(toInteger(options.limit, 100), 500), 1)

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("scanner_type", type)
    .eq("status", "running")
    .order("started_at", { ascending: false })
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }

  return Array.isArray(data) ? data : []
}

exports.deleteOlderThan = async (cutoffIso, options = {}) => {
  const cutoff = String(cutoffIso || "").trim()
  if (!cutoff) return 0

  const excludeRunning = options.excludeRunning !== false
  let countQuery = supabaseAdmin
    .from(TABLE)
    .select("id", { count: "exact", head: true })
    .lt("started_at", cutoff)

  if (excludeRunning) {
    countQuery = countQuery.neq("status", "running")
  }

  const { count, error: countError } = await countQuery
  if (countError) {
    throw new AppError(countError.message, 500)
  }

  const estimated = Number(count || 0)
  if (!estimated) return 0

  let deleteQuery = supabaseAdmin
    .from(TABLE)
    .delete()
    .lt("started_at", cutoff)

  if (excludeRunning) {
    deleteQuery = deleteQuery.neq("status", "running")
  }

  const { error } = await deleteQuery
  if (error) {
    throw new AppError(error.message, 500)
  }

  return estimated
}

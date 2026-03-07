const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "scanner_runs"

function normalizeScannerType(value) {
  const text = String(value || "global_arbitrage").trim()
  return text || "global_arbitrage"
}

function normalizeStatus(value, fallback = "running") {
  const text = String(value || fallback)
    .trim()
    .toLowerCase()
  if (!text) return fallback
  if (text === "running" || text === "completed" || text === "failed") {
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

exports.createRun = async (payload = {}) => {
  const row = {
    scanner_type: normalizeScannerType(payload.scannerType),
    started_at: payload.startedAt || new Date().toISOString(),
    status: normalizeStatus(payload.status, "running"),
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
    diagnostics_summary: toJsonObject(payload.diagnosticsSummary)
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

exports.markCompleted = async (runId, payload = {}) => {
  const id = String(runId || "").trim()
  if (!id) return null

  const patch = {
    completed_at: payload.completedAt || new Date().toISOString(),
    status: "completed",
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
    diagnostics_summary: toJsonObject(payload.diagnosticsSummary)
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
    status: "failed",
    items_scanned: toInteger(payload.itemsScanned, 0),
    opportunities_found: toInteger(payload.opportunitiesFound, 0),
    new_opportunities_added: toInteger(payload.newOpportunitiesAdded, 0),
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

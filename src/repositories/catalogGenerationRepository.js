const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "catalog_generations"

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeStatus(value, fallback = "active") {
  const text = normalizeText(value).toLowerCase()
  if (text === "active" || text === "archived") return text
  return fallback
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

function normalizeGenerationKey(value, fallback = "") {
  const text = normalizeText(value)
  return text || fallback || null
}

function buildGenerationRow(payload = {}) {
  const status = normalizeStatus(payload.status, "active")
  const isActive = payload.isActive == null ? status === "active" : Boolean(payload.isActive)
  return {
    generation_key:
      normalizeGenerationKey(
        payload.generationKey,
        `catalog-${new Date().toISOString().replace(/[:.]/g, "-")}`
      ) || `catalog-${Date.now()}`,
    status,
    is_active: isActive,
    opportunity_scan_enabled: Boolean(payload.opportunityScanEnabled),
    source_generation_id: normalizeText(payload.sourceGenerationId) || null,
    activated_at:
      toIsoOrNull(payload.activatedAt) ||
      (isActive ? new Date().toISOString() : null),
    archived_at: toIsoOrNull(payload.archivedAt),
    opportunity_scan_enabled_at:
      toIsoOrNull(payload.opportunityScanEnabledAt) ||
      (Boolean(payload.opportunityScanEnabled) ? new Date().toISOString() : null),
    diagnostics_summary: toJsonObject(payload.diagnosticsSummary)
  }
}

async function selectSingle(query, fallbackMessage) {
  const { data, error } = await query
  if (error) {
    throw new AppError(error.message || fallbackMessage, 500)
  }
  return data || null
}

exports.createGeneration = async (payload = {}) => {
  const row = buildGenerationRow(payload)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .insert(row)
    .select("*")
    .single()

  if (error) {
    throw new AppError(error.message || "catalog_generation_create_failed", 500)
  }

  return data || null
}

exports.getActiveGeneration = async (options = {}) =>
  selectSingle(
    (() => {
      let query = supabaseAdmin
        .from(TABLE)
        .select("*")
        .eq("is_active", true)
        .order("activated_at", { ascending: false, nullsFirst: false })
        .order("created_at", { ascending: false })
        .limit(1)

      if (options.requireOpportunityScanEnabled === true) {
        query = query.eq("opportunity_scan_enabled", true)
      }

      return query.maybeSingle()
    })(),
    "catalog_generation_active_read_failed"
  )

exports.getCurrentGeneration = async () =>
  selectSingle(
    supabaseAdmin
      .from(TABLE)
      .select("*")
      .eq("is_active", true)
      .order("activated_at", { ascending: false, nullsFirst: false })
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle(),
    "catalog_generation_current_read_failed"
  )

exports.getById = async (id) => {
  const safeId = normalizeText(id)
  if (!safeId) return null
  return selectSingle(
    supabaseAdmin.from(TABLE).select("*").eq("id", safeId).maybeSingle(),
    "catalog_generation_read_failed"
  )
}

exports.listRecent = async (options = {}) => {
  const limit = Math.max(Math.round(Number(options.limit || 10)), 1)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .order("created_at", { ascending: false })
    .limit(limit)

  if (error) {
    throw new AppError(error.message || "catalog_generation_list_failed", 500)
  }

  return Array.isArray(data) ? data : []
}

exports.updateGeneration = async (id, payload = {}) => {
  const safeId = normalizeText(id)
  if (!safeId) return null

  const patch = {}
  if (payload.status !== undefined) {
    patch.status = normalizeStatus(payload.status)
  }
  if (payload.isActive !== undefined) {
    patch.is_active = Boolean(payload.isActive)
  }
  if (payload.opportunityScanEnabled !== undefined) {
    patch.opportunity_scan_enabled = Boolean(payload.opportunityScanEnabled)
    patch.opportunity_scan_enabled_at = Boolean(payload.opportunityScanEnabled)
      ? toIsoOrNull(payload.opportunityScanEnabledAt) || new Date().toISOString()
      : null
  }
  if (payload.activatedAt !== undefined) {
    patch.activated_at = toIsoOrNull(payload.activatedAt)
  }
  if (payload.archivedAt !== undefined) {
    patch.archived_at = toIsoOrNull(payload.archivedAt)
  }
  if (payload.diagnosticsSummary !== undefined) {
    patch.diagnostics_summary = toJsonObject(payload.diagnosticsSummary)
  }

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .update(patch)
    .eq("id", safeId)
    .select("*")
    .maybeSingle()

  if (error) {
    throw new AppError(error.message || "catalog_generation_update_failed", 500)
  }

  return data || null
}

exports.archiveGeneration = async (id, payload = {}) =>
  exports.updateGeneration(id, {
    status: "archived",
    isActive: false,
    archivedAt: payload.archivedAt || new Date().toISOString(),
    opportunityScanEnabled: false,
    diagnosticsSummary: payload.diagnosticsSummary
  })

exports.activateGeneration = async (id, payload = {}) =>
  exports.updateGeneration(id, {
    status: "active",
    isActive: true,
    activatedAt: payload.activatedAt || new Date().toISOString(),
    opportunityScanEnabled: Boolean(payload.opportunityScanEnabled),
    opportunityScanEnabledAt: payload.opportunityScanEnabledAt || null,
    diagnosticsSummary: payload.diagnosticsSummary
  })

exports.enableOpportunityScan = async (id, payload = {}) =>
  exports.updateGeneration(id, {
    opportunityScanEnabled: true,
    opportunityScanEnabledAt: payload.opportunityScanEnabledAt || new Date().toISOString(),
    diagnosticsSummary: payload.diagnosticsSummary
  })

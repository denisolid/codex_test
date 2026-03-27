const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "global_opportunity_lifecycle_log"
const INSERT_BATCH_SIZE = 200

function normalizeText(value) {
  return String(value || "").trim()
}

function toIsoOrNull(value) {
  const raw = normalizeText(value)
  if (!raw) return null
  const ts = new Date(raw).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const lifecycleEventKey = normalizeText(
        row?.lifecycle_event_key || row?.lifecycleEventKey
      )
      const opportunityFingerprint = normalizeText(
        row?.opportunity_fingerprint || row?.opportunityFingerprint
      ).toLowerCase()
      const lifecycleStatus = normalizeText(
        row?.lifecycle_status || row?.lifecycleStatus
      ).toLowerCase()
      if (!lifecycleEventKey || !opportunityFingerprint || !lifecycleStatus) {
        return null
      }
      return {
        lifecycle_event_key: lifecycleEventKey,
        active_opportunity_id:
          normalizeText(row?.active_opportunity_id || row?.activeOpportunityId) || null,
        opportunity_fingerprint: opportunityFingerprint,
        scan_run_id: normalizeText(row?.scan_run_id || row?.scanRunId) || null,
        lifecycle_status: lifecycleStatus,
        event_at: toIsoOrNull(row?.event_at || row?.eventAt) || new Date().toISOString(),
        category: normalizeText(row?.category).toLowerCase() || null,
        market_hash_name:
          normalizeText(row?.market_hash_name || row?.marketHashName || row?.item_name) || null,
        item_name:
          normalizeText(row?.item_name || row?.itemName || row?.market_hash_name) || null,
        reason: normalizeText(row?.reason) || null,
        snapshot: toJsonObject(row?.snapshot)
      }
    })
    .filter(Boolean)
}

exports.insertRows = async (rows = []) => {
  const payload = normalizeRows(rows)
  if (!payload.length) return []

  const insertedRows = []
  for (let index = 0; index < payload.length; index += INSERT_BATCH_SIZE) {
    const chunk = payload.slice(index, index + INSERT_BATCH_SIZE)
    const { data, error } = await supabaseAdmin
      .from(TABLE)
      .upsert(chunk, { onConflict: "lifecycle_event_key", ignoreDuplicates: true })
      .select("id, lifecycle_event_key")

    if (error) {
      throw new AppError(error.message, 500)
    }

    if (Array.isArray(data) && data.length) {
      insertedRows.push(...data)
    }
  }

  return insertedRows
}

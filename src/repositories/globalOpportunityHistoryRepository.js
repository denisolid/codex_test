const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "global_opportunity_history"
const INSERT_BATCH_SIZE = 200

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

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const sourceEventKey = normalizeText(row?.source_event_key || row?.sourceEventKey)
      if (!sourceEventKey) return null
      return {
        source_event_key: sourceEventKey,
        active_opportunity_id:
          normalizeText(row?.active_opportunity_id || row?.activeOpportunityId) || null,
        opportunity_fingerprint:
          normalizeText(row?.opportunity_fingerprint || row?.opportunityFingerprint).toLowerCase() ||
          null,
        scan_run_id: normalizeText(row?.scan_run_id || row?.scanRunId) || null,
        event_type: normalizeText(row?.event_type || row?.eventType).toLowerCase() || null,
        event_at: toIsoOrNull(row?.event_at || row?.eventAt) || new Date().toISOString(),
        refresh_status:
          normalizeText(row?.refresh_status || row?.refreshStatus).toLowerCase() || null,
        live_status: normalizeText(row?.live_status || row?.liveStatus).toLowerCase() || null,
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
      .upsert(chunk, { onConflict: "source_event_key", ignoreDuplicates: true })
      .select("id, source_event_key")

    if (error) {
      throw new AppError(error.message, 500)
    }

    if (Array.isArray(data) && data.length) {
      insertedRows.push(...data)
    }
  }

  return insertedRows
}

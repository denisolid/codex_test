const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "global_active_opportunities"
const INSERT_BATCH_SIZE = 200
const MAX_LIMIT = 1000
const FEED_TIME_COLUMN = "last_published_at"

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

function normalizeCategory(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return "weapon_skin"
  if (
    raw === "weapon_skin" ||
    raw === "case" ||
    raw === "sticker_capsule" ||
    raw === "knife" ||
    raw === "glove"
  ) {
    return raw
  }
  if (raw === "skin" || raw === "skins") return "weapon_skin"
  if (raw === "cases") return "case"
  if (raw === "capsule" || raw === "capsules") return "sticker_capsule"
  if (raw === "knives") return "knife"
  if (raw === "gloves") return "glove"
  return "weapon_skin"
}

function normalizeStatus(value, fallback) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return fallback
  return raw
}

function normalizeLimit(value, fallback = 100) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

function applyFeedFilters(query, options = {}) {
  const includeInactive = Boolean(options.includeInactive)
  const sinceIso = toIsoOrNull(options.sinceIso)
  const category = normalizeCategory(options.category)
  const minScore = toFiniteOrNull(options.minScore)
  const excludeLowConfidence = Boolean(options.excludeLowConfidence)
  const highConfidenceOnly = Boolean(options.highConfidenceOnly)

  let scoped = query
  if (!includeInactive) {
    scoped = scoped.eq("live_status", "live")
  }
  if (sinceIso) {
    scoped = scoped.gte(FEED_TIME_COLUMN, sinceIso)
  }
  if (category) {
    scoped = scoped.eq("category", category)
  }
  if (minScore != null) {
    scoped = scoped.gte("opportunity_score", minScore)
  }
  if (excludeLowConfidence) {
    scoped = scoped.neq("execution_confidence", "Low")
  }
  if (highConfidenceOnly) {
    scoped = scoped.contains("metadata", { is_high_confidence_eligible: true })
  }
  return scoped
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const fingerprint = normalizeText(
        row?.opportunity_fingerprint || row?.opportunityFingerprint
      ).toLowerCase()
      if (!fingerprint) return null
      return {
        opportunity_fingerprint: fingerprint,
        material_change_hash:
          normalizeText(row?.material_change_hash || row?.materialChangeHash) || null,
        scan_run_id: normalizeText(row?.scan_run_id || row?.scanRunId) || null,
        market_hash_name:
          normalizeText(row?.market_hash_name || row?.marketHashName || row?.item_name) || null,
        item_name:
          normalizeText(row?.item_name || row?.itemName || row?.market_hash_name) || null,
        category: normalizeCategory(row?.category),
        buy_market: normalizeText(row?.buy_market || row?.buyMarket).toLowerCase(),
        buy_price: toFiniteOrNull(row?.buy_price ?? row?.buyPrice),
        sell_market: normalizeText(row?.sell_market || row?.sellMarket).toLowerCase(),
        sell_net: toFiniteOrNull(row?.sell_net ?? row?.sellNet),
        profit: toFiniteOrNull(row?.profit),
        spread_pct: toFiniteOrNull(row?.spread_pct ?? row?.spread),
        opportunity_score:
          toFiniteOrNull(row?.opportunity_score ?? row?.opportunityScore) == null
            ? null
            : Math.min(
                Math.max(Math.round(Number(row?.opportunity_score ?? row?.opportunityScore)), 0),
                100
              ),
        execution_confidence:
          normalizeText(row?.execution_confidence || row?.executionConfidence) || null,
        quality_grade: normalizeText(row?.quality_grade || row?.qualityGrade) || null,
        liquidity_label: normalizeText(row?.liquidity_label || row?.liquidityLabel) || null,
        market_signal_observed_at:
          toIsoOrNull(row?.market_signal_observed_at || row?.marketSignalObservedAt) || null,
        first_seen_at: toIsoOrNull(row?.first_seen_at || row?.firstSeenAt) || null,
        last_seen_at: toIsoOrNull(row?.last_seen_at || row?.lastSeenAt) || null,
        last_published_at:
          toIsoOrNull(row?.last_published_at || row?.lastPublishedAt) || null,
        last_revalidation_attempt_at:
          toIsoOrNull(
            row?.last_revalidation_attempt_at || row?.lastRevalidationAttemptAt
          ) || null,
        refresh_status: normalizeStatus(
          row?.refresh_status || row?.refreshStatus,
          "pending"
        ),
        live_status: normalizeStatus(row?.live_status || row?.liveStatus, "live"),
        latest_signal_age_hours:
          toFiniteOrNull(row?.latest_signal_age_hours ?? row?.latestSignalAgeHours) ?? null,
        metadata: toJsonObject(row?.metadata)
      }
    })
    .filter(Boolean)
}

function normalizeFingerprints(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value).toLowerCase())
        .filter(Boolean)
    )
  )
}

function normalizeItemNames(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )
}

exports.upsertRows = async (rows = []) => {
  const payload = normalizeRows(rows)
  if (!payload.length) return []

  const insertedRows = []
  for (let index = 0; index < payload.length; index += INSERT_BATCH_SIZE) {
    const chunk = payload.slice(index, index + INSERT_BATCH_SIZE)
    const { data, error } = await supabaseAdmin
      .from(TABLE)
      .upsert(chunk, { onConflict: "opportunity_fingerprint" })
      .select("id, opportunity_fingerprint")

    if (error) {
      throw new AppError(error.message, 500)
    }

    if (Array.isArray(data) && data.length) {
      insertedRows.push(...data)
    }
  }

  return insertedRows
}

exports.listFeedByCursor = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 100)
  const cursorCreatedAt = toIsoOrNull(options.cursorCreatedAt)
  const cursorId = normalizeText(options.cursorId)
  const useCursor = Boolean(cursorCreatedAt && cursorId)

  const buildBaseQuery = () =>
    applyFeedFilters(supabaseAdmin.from(TABLE).select("*"), options)

  if (!useCursor) {
    const { data, error } = await buildBaseQuery()
      .order(FEED_TIME_COLUMN, { ascending: false })
      .order("id", { ascending: false })
      .limit(limit)

    if (error) {
      throw new AppError(error.message, 500)
    }

    return Array.isArray(data) ? data : []
  }

  const sameTimestamp = await buildBaseQuery()
    .eq(FEED_TIME_COLUMN, cursorCreatedAt)
    .lt("id", cursorId)
    .order("id", { ascending: false })
    .limit(limit)

  if (sameTimestamp.error) {
    throw new AppError(sameTimestamp.error.message, 500)
  }

  const sameRows = Array.isArray(sameTimestamp.data) ? sameTimestamp.data : []
  if (sameRows.length >= limit) {
    return sameRows.slice(0, limit)
  }

  const remaining = Math.max(limit - sameRows.length, 0)
  if (remaining <= 0) {
    return sameRows
  }

  const olderRows = await buildBaseQuery()
    .lt(FEED_TIME_COLUMN, cursorCreatedAt)
    .order(FEED_TIME_COLUMN, { ascending: false })
    .order("id", { ascending: false })
    .limit(remaining)

  if (olderRows.error) {
    throw new AppError(olderRows.error.message, 500)
  }

  return [...sameRows, ...(Array.isArray(olderRows.data) ? olderRows.data : [])]
}

exports.countFeed = async (options = {}) => {
  const { count, error } = await applyFeedFilters(
    supabaseAdmin.from(TABLE).select("id", { count: "exact", head: true }),
    options
  )

  if (error) {
    throw new AppError(error.message, 500)
  }

  return Number(count || 0)
}

exports.getById = async (id) => {
  const safeId = normalizeText(id)
  if (!safeId) return null

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("id", safeId)
    .maybeSingle()

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.updateRowsById = async (rows = []) => {
  const payload = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      id: normalizeText(row?.id),
      patch: row?.patch && typeof row.patch === "object" && !Array.isArray(row.patch)
        ? row.patch
        : null
    }))
    .filter((row) => row.id && row.patch)

  if (!payload.length) return 0

  let updatedCount = 0
  for (const row of payload) {
    const { error } = await supabaseAdmin
      .from(TABLE)
      .update(row.patch)
      .eq("id", row.id)

    if (error) {
      throw new AppError(error.message, 500)
    }
    updatedCount += 1
  }

  return updatedCount
}

exports.getRowsByFingerprints = async (options = {}) => {
  const fingerprints = normalizeFingerprints(options.fingerprints)
  if (!fingerprints.length) return []

  const includeExpired = options.includeExpired !== false
  const limit = Math.max(Math.round(Number(options.limit || 2500)), 1)
  let query = supabaseAdmin
    .from(TABLE)
    .select("*")
    .in("opportunity_fingerprint", fingerprints)
    .order("last_seen_at", { ascending: false })
    .order("id", { ascending: false })
    .limit(limit)

  if (!includeExpired) {
    query = query.eq("live_status", "live")
  }

  const { data, error } = await query
  if (error) {
    throw new AppError(error.message, 500)
  }

  return Array.isArray(data) ? data : []
}

exports.getRecentRowsByItems = async (options = {}) => {
  const itemNames = normalizeItemNames(options.itemNames)
  if (!itemNames.length) return []

  const includeExpired = options.includeExpired !== false
  const limit = Math.max(Math.round(Number(options.limit || 2500)), 1)
  let query = supabaseAdmin
    .from(TABLE)
    .select("*")
    .in("item_name", itemNames)
    .order("last_seen_at", { ascending: false })
    .order("id", { ascending: false })
    .limit(limit)

  if (!includeExpired) {
    query = query.eq("live_status", "live")
  }

  const { data, error } = await query
  if (error) {
    throw new AppError(error.message, 500)
  }

  return Array.isArray(data) ? data : []
}

exports.listRowsForRevalidation = async (options = {}) => {
  const limit = Math.max(Math.round(Number(options.limit || 200)), 1)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("*")
    .eq("live_status", "live")
    .order("last_revalidation_attempt_at", { ascending: true, nullsFirst: true })
    .order("last_published_at", { ascending: true })
    .order("id", { ascending: true })
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }

  return Array.isArray(data) ? data : []
}

const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "arbitrage_feed"
const MAX_LIMIT = 1000
const INSERT_BATCH_SIZE = 200
const FEED_SELECT_FIELDS_BASE =
  "id, item_name, market_hash_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, detected_at, discovered_at, market_signal_observed_at, feed_published_at, insight_refreshed_at, last_refresh_attempt_at, latest_signal_age_hours, net_profit_after_fees, confidence_score, freshness_score, verdict, refresh_status, live_status, scan_run_id, is_active, is_duplicate, metadata"
const FEED_SELECT_FIELDS_LEGACY =
  "id, item_name, market_hash_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, detected_at, scan_run_id, is_active, is_duplicate, metadata"

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeCategory(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return ""
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
  return ""
}

function normalizeLimit(value, fallback = 100) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

function normalizeOffset(value, fallback = 0) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 0), 500000)
}

function normalizeIso(value) {
  const text = normalizeText(value)
  if (!text) return ""
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return ""
  return new Date(ts).toISOString()
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function normalizeMarket(value) {
  const text = normalizeText(value).toLowerCase()
  if (!text) return ""
  if (text === "steam" || text === "skinport" || text === "csfloat" || text === "dmarket") {
    return text
  }
  return text
}

function normalizeId(value) {
  return normalizeText(value)
}

function normalizeStatus(value, fallback = "") {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return fallback
  return raw
}

function isMissingColumnError(error) {
  const message = normalizeText(error?.message).toLowerCase()
  const code = normalizeText(error?.code).toUpperCase()
  return (
    code === "42703" ||
    message.includes("column") && message.includes("does not exist")
  )
}

async function runFeedSelectWithFallback(queryBuilder) {
  const first = await queryBuilder(FEED_SELECT_FIELDS_BASE)
  if (!first?.error) {
    return first
  }
  if (isMissingColumnError(first.error)) {
    return queryBuilder(FEED_SELECT_FIELDS_LEGACY)
  }
  return first
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const itemName = normalizeText(row?.item_name || row?.itemName)
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName || itemName)
      const buyMarket = normalizeMarket(row?.buy_market || row?.buyMarket)
      const sellMarket = normalizeMarket(row?.sell_market || row?.sellMarket)
      const buyPrice = toFiniteOrNull(row?.buy_price ?? row?.buyPrice)
      const sellNet = toFiniteOrNull(row?.sell_net ?? row?.sellNet)
      const profit = toFiniteOrNull(row?.profit)
      const spread = toFiniteOrNull(row?.spread_pct ?? row?.spread)
      const score = toFiniteOrNull(row?.opportunity_score ?? row?.opportunityScore)
      if (
        !itemName ||
        !marketHashName ||
        !buyMarket ||
        !sellMarket ||
        buyPrice == null ||
        sellNet == null ||
        profit == null ||
        spread == null ||
        score == null
      ) {
        return null
      }

      return {
        item_name: itemName,
        market_hash_name: marketHashName,
        category: normalizeCategory(row?.category) || "weapon_skin",
        buy_market: buyMarket,
        buy_price: Number(buyPrice.toFixed(4)),
        sell_market: sellMarket,
        sell_net: Number(sellNet.toFixed(4)),
        profit: Number(profit.toFixed(4)),
        spread_pct: Number(spread.toFixed(4)),
        opportunity_score: Math.min(Math.max(Math.round(score), 0), 100),
        execution_confidence: normalizeText(row?.execution_confidence || row?.executionConfidence || "Low") || "Low",
        quality_grade: normalizeText(row?.quality_grade || row?.qualityGrade || "RISKY") || "RISKY",
        liquidity_label: normalizeText(row?.liquidity_label || row?.liquidityLabel || "Low") || "Low",
        detected_at: row?.detected_at || row?.detectedAt || new Date().toISOString(),
        discovered_at:
          row?.discovered_at ||
          row?.discoveredAt ||
          row?.detected_at ||
          row?.detectedAt ||
          new Date().toISOString(),
        market_signal_observed_at:
          row?.market_signal_observed_at ||
          row?.marketSignalObservedAt ||
          null,
        feed_published_at: row?.feed_published_at || row?.feedPublishedAt || null,
        insight_refreshed_at:
          row?.insight_refreshed_at || row?.insightRefreshedAt || null,
        last_refresh_attempt_at:
          row?.last_refresh_attempt_at || row?.lastRefreshAttemptAt || null,
        latest_signal_age_hours:
          toFiniteOrNull(row?.latest_signal_age_hours ?? row?.latestSignalAgeHours) ??
          null,
        net_profit_after_fees:
          toFiniteOrNull(row?.net_profit_after_fees ?? row?.netProfitAfterFees ?? profit) ??
          null,
        confidence_score:
          toFiniteOrNull(row?.confidence_score ?? row?.confidenceScore) == null
            ? null
            : Math.min(
                Math.max(
                  Math.round(
                    Number(
                      toFiniteOrNull(row?.confidence_score ?? row?.confidenceScore)
                    )
                  ),
                  0
                ),
                100
              ),
        freshness_score:
          toFiniteOrNull(row?.freshness_score ?? row?.freshnessScore) == null
            ? null
            : Math.min(
                Math.max(
                  Math.round(
                    Number(
                      toFiniteOrNull(row?.freshness_score ?? row?.freshnessScore)
                    )
                  ),
                  0
                ),
                100
              ),
        verdict: normalizeStatus(row?.verdict, null),
        refresh_status: normalizeStatus(
          row?.refresh_status ?? row?.refreshStatus,
          "pending"
        ),
        live_status: normalizeStatus(row?.live_status ?? row?.liveStatus, "degraded"),
        scan_run_id: normalizeText(row?.scan_run_id || row?.scanRunId) || null,
        is_active: row?.is_active == null ? true : Boolean(row.is_active),
        is_duplicate: row?.is_duplicate == null ? false : Boolean(row.is_duplicate),
        metadata: toJsonObject(row?.metadata)
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
      .insert(chunk)
      .select("id")

    if (error) {
      throw new AppError(error.message, 500)
    }

    if (Array.isArray(data) && data.length) {
      insertedRows.push(...data)
    }
  }

  return insertedRows
}

exports.listFeed = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 100)
  const offset = normalizeOffset(options.offset, 0)
  const sinceIso = normalizeIso(options.sinceIso)
  const includeInactive = Boolean(options.includeInactive)
  const category = normalizeCategory(options.category)
  const minScore = toFiniteOrNull(options.minScore)
  const excludeLowConfidence = Boolean(options.excludeLowConfidence)
  const highConfidenceOnly = Boolean(options.highConfidenceOnly)

  const { data, error } = await runFeedSelectWithFallback((selectFields) => {
    let query = supabaseAdmin
      .from(TABLE)
      .select(selectFields)
      .order("detected_at", { ascending: false })
      .order("id", { ascending: false })
      .range(offset, offset + limit - 1)

    if (!includeInactive) {
      query = query.eq("is_active", true)
    }
    if (sinceIso) {
      query = query.gte("detected_at", sinceIso)
    }
    if (category) {
      query = query.eq("category", category)
    }
    if (minScore != null) {
      query = query.gte("opportunity_score", minScore)
    }
    if (excludeLowConfidence) {
      query = query.neq("execution_confidence", "Low")
    }
    if (highConfidenceOnly) {
      query = query.contains("metadata", { is_high_confidence_eligible: true })
    }
    return query
  })

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || []
}

exports.getById = async (id) => {
  const safeId = normalizeId(id)
  if (!safeId) return null

  const { data, error } = await runFeedSelectWithFallback((selectFields) =>
    supabaseAdmin
      .from(TABLE)
      .select(selectFields)
      .eq("id", safeId)
      .maybeSingle()
  )

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || null
}

exports.countFeed = async (options = {}) => {
  const sinceIso = normalizeIso(options.sinceIso)
  const includeInactive = Boolean(options.includeInactive)
  const category = normalizeCategory(options.category)
  const minScore = toFiniteOrNull(options.minScore)
  const excludeLowConfidence = Boolean(options.excludeLowConfidence)
  const highConfidenceOnly = Boolean(options.highConfidenceOnly)

  let query = supabaseAdmin.from(TABLE).select("id", { count: "exact", head: true })
  if (!includeInactive) {
    query = query.eq("is_active", true)
  }
  if (sinceIso) {
    query = query.gte("detected_at", sinceIso)
  }
  if (category) {
    query = query.eq("category", category)
  }
  if (minScore != null) {
    query = query.gte("opportunity_score", minScore)
  }
  if (excludeLowConfidence) {
    query = query.neq("execution_confidence", "Low")
  }
  if (highConfidenceOnly) {
    query = query.contains("metadata", { is_high_confidence_eligible: true })
  }

  const { count, error } = await query

  if (error) {
    throw new AppError(error.message, 500)
  }

  return Number(count || 0)
}

exports.getRecentRowsByItems = async (options = {}) => {
  const itemNames = Array.from(
    new Set(
      (Array.isArray(options.itemNames) ? options.itemNames : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )
  if (!itemNames.length) return []

  const sinceIso = normalizeText(options.sinceIso)
  const limit = normalizeLimit(options.limit, 2500)

  let query = supabaseAdmin
    .from(TABLE)
    .select(
      "id, item_name, category, buy_market, sell_market, profit, spread_pct, opportunity_score, execution_confidence, liquidity_label, detected_at, is_active, is_duplicate, metadata"
    )
    .in("item_name", itemNames)
    .order("detected_at", { ascending: false })
    .order("id", { ascending: false })
    .limit(limit)

  if (sinceIso) {
    query = query.gte("detected_at", sinceIso)
  }

  const { data, error } = await query
  if (error) {
    throw new AppError(error.message, 500)
  }
  return data || []
}

exports.markRowsInactiveByIds = async (ids = []) => {
  const normalizedIds = Array.from(
    new Set(
      (Array.isArray(ids) ? ids : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )
  if (!normalizedIds.length) return 0

  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .update({ is_active: false })
    .in("id", normalizedIds)
    .eq("is_active", true)
    .select("id")

  if (error) {
    throw new AppError(error.message, 500)
  }

  return Array.isArray(data) ? data.length : 0
}

exports.markInactiveOlderThan = async (cutoffIso, options = {}) => {
  const cutoff = normalizeText(cutoffIso)
  if (!cutoff) return 0

  const batchSize = Math.max(Math.round(Number(options.batchSize || 200)), 1)
  const maxRows = Math.max(Math.round(Number(options.maxRows || 1000)), batchSize)
  let totalMarked = 0

  while (totalMarked < maxRows) {
    const remaining = maxRows - totalMarked
    const selectSize = Math.min(batchSize, remaining)
    const { data: idRows, error: selectError } = await supabaseAdmin
      .from(TABLE)
      .select("id")
      .eq("is_active", true)
      .lt("detected_at", cutoff)
      .order("detected_at", { ascending: true })
      .limit(selectSize)

    if (selectError) {
      throw new AppError(selectError.message, 500)
    }

    const ids = (Array.isArray(idRows) ? idRows : [])
      .map((row) => normalizeText(row?.id))
      .filter(Boolean)
    if (!ids.length) {
      break
    }

    const { data: updatedRows, error: updateError } = await supabaseAdmin
      .from(TABLE)
      .update({ is_active: false })
      .in("id", ids)
      .eq("is_active", true)
      .select("id")

    if (updateError) {
      throw new AppError(updateError.message, 500)
    }

    const affected = Array.isArray(updatedRows) ? updatedRows.length : 0
    totalMarked += affected
    if (affected < selectSize) {
      break
    }
  }

  return totalMarked
}

exports.markInactiveBeyondLimit = async (activeLimit = 500, options = {}) => {
  const limit = Math.max(Math.round(Number(activeLimit || 0)), 1)
  const batchSize = Math.max(Math.round(Number(options.batchSize || 200)), 1)
  const maxBatches = Math.max(Math.round(Number(options.maxBatches || 4)), 1)
  let totalMarked = 0
  let batches = 0

  while (true) {
    if (batches >= maxBatches) {
      break
    }
    const { data, error } = await supabaseAdmin
      .from(TABLE)
      .select("id")
      .eq("is_active", true)
      .order("detected_at", { ascending: false })
      .order("id", { ascending: false })
      .range(limit, limit + batchSize - 1)

    if (error) {
      throw new AppError(error.message, 500)
    }

    const ids = (Array.isArray(data) ? data : [])
      .map((row) => normalizeText(row?.id))
      .filter(Boolean)
    if (!ids.length) {
      break
    }

    const { data: updatedRows, error: updateError } = await supabaseAdmin
      .from(TABLE)
      .update({ is_active: false })
      .in("id", ids)
      .select("id")

    if (updateError) {
      throw new AppError(updateError.message, 500)
    }

    totalMarked += Array.isArray(updatedRows) ? updatedRows.length : 0
    batches += 1
  }

  return totalMarked
}

exports.updatePublishRefreshState = async (rows = []) => {
  const payload = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      id: normalizeText(row?.id),
      patch:
        row?.patch && typeof row.patch === "object" && !Array.isArray(row.patch)
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

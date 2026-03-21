const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "arbitrage_feed"
const MAX_LIMIT = 1000
const INSERT_BATCH_SIZE = 200
const FEED_TIME_COLUMNS = Object.freeze([
  "created_at",
  "last_seen_at",
  "last_published_at",
  "detected_at"
])
const FEED_SELECT_FIELDS_BASE =
  "id, item_name, market_hash_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, created_at, detected_at, discovered_at, opportunity_fingerprint, first_seen_at, last_seen_at, last_published_at, times_seen, material_change_hash, market_signal_observed_at, feed_published_at, insight_refreshed_at, last_refresh_attempt_at, latest_signal_age_hours, net_profit_after_fees, confidence_score, freshness_score, verdict, refresh_status, live_status, scan_run_id, is_active, is_duplicate, metadata"
const FEED_SELECT_FIELDS_LEGACY =
  "id, item_name, market_hash_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, detected_at, scan_run_id, is_active, is_duplicate, metadata"
const RECENT_ROWS_FIELDS_BASE =
  "id, item_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, detected_at, discovered_at, opportunity_fingerprint, first_seen_at, last_seen_at, last_published_at, times_seen, material_change_hash, verdict, is_active, is_duplicate, metadata"
const RECENT_ROWS_FIELDS_LEGACY =
  "id, item_name, category, buy_market, sell_market, profit, spread_pct, opportunity_score, execution_confidence, liquidity_label, detected_at, is_active, is_duplicate, metadata"
const FEED_CARD_SELECT_FIELDS_BASE =
  "id, item_name, market_hash_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, created_at, detected_at, scan_run_id, is_active, is_duplicate, refresh_status, live_status, latest_signal_age_hours, verdict, item_id:metadata->>item_id, item_subcategory:metadata->>item_subcategory, item_rarity:metadata->>item_rarity, item_rarity_color:metadata->>item_rarity_color, item_image_url:metadata->>item_image_url, volume_7d:metadata->>volume_7d, market_coverage:metadata->>market_coverage, reference_price:metadata->>reference_price, buy_url:metadata->>buy_url, sell_url:metadata->>sell_url, score_category:metadata->>score_category, quality_score_display:metadata->>quality_score_display, flags:metadata->flags, badges:metadata->badges"
const FEED_CARD_SELECT_FIELDS_FALLBACK =
  "id, item_name, market_hash_name, category, buy_market, buy_price, sell_market, sell_net, profit, spread_pct, opportunity_score, execution_confidence, quality_grade, liquidity_label, created_at, detected_at, scan_run_id, is_active, is_duplicate, refresh_status, live_status, latest_signal_age_hours, verdict, metadata"

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

function isSelectSyntaxError(error) {
  const message = normalizeText(error?.message).toLowerCase()
  const code = normalizeText(error?.code).toUpperCase()
  return (
    code === "PGRST100" ||
    message.includes("failed to parse select parameter") ||
    message.includes("syntax error")
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

async function runFeedCardSelectWithFallback(queryBuilder) {
  const primary = await queryBuilder(FEED_CARD_SELECT_FIELDS_BASE)
  if (!primary?.error) {
    return primary
  }
  if (isMissingColumnError(primary.error) || isSelectSyntaxError(primary.error)) {
    const secondary = await queryBuilder(FEED_CARD_SELECT_FIELDS_FALLBACK)
    if (!secondary?.error) {
      return secondary
    }
    if (isMissingColumnError(secondary?.error)) {
      return queryBuilder(FEED_SELECT_FIELDS_LEGACY)
    }
    return secondary
  }
  return primary
}

async function runFeedQueryWithTimeColumn(queryBuilder) {
  let latestResponse = null
  for (const timeColumn of FEED_TIME_COLUMNS) {
    const result = await queryBuilder(timeColumn)
    latestResponse = result
    if (!result?.error) {
      return result
    }
    if (!isMissingColumnError(result?.error)) {
      return result
    }
  }
  return latestResponse
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const metadata = toJsonObject(row?.metadata)
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
      const detectedAt = row?.detected_at || row?.detectedAt || new Date().toISOString()
      const discoveredAt =
        row?.discovered_at ||
        row?.discoveredAt ||
        row?.detected_at ||
        row?.detectedAt ||
        new Date().toISOString()
      const firstSeenAt =
        row?.first_seen_at ||
        row?.firstSeenAt ||
        discoveredAt
      const lastSeenAt =
        row?.last_seen_at ||
        row?.lastSeenAt ||
        detectedAt
      const lastPublishedAt =
        row?.last_published_at ||
        row?.lastPublishedAt ||
        row?.feed_published_at ||
        row?.feedPublishedAt ||
        detectedAt
      const fingerprintFallback = `${marketHashName}::${buyMarket}::${sellMarket}`.toLowerCase()
      const opportunityFingerprint =
        normalizeText(
          row?.opportunity_fingerprint ||
            row?.opportunityFingerprint ||
            metadata?.opportunity_fingerprint ||
            metadata?.opportunityFingerprint
        ).toLowerCase() || fingerprintFallback
      const timesSeenRaw =
        toFiniteOrNull(
          row?.times_seen ??
            row?.timesSeen ??
            metadata?.times_seen ??
            metadata?.timesSeen
        ) ?? 1
      const timesSeen = Math.max(Math.round(timesSeenRaw), 1)
      const materialChangeHash =
        normalizeText(
          row?.material_change_hash ||
            row?.materialChangeHash ||
            metadata?.material_change_hash ||
            metadata?.materialChangeHash
        ) || null

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
        detected_at: detectedAt,
        discovered_at: discoveredAt,
        first_seen_at: firstSeenAt,
        last_seen_at: lastSeenAt,
        last_published_at: lastPublishedAt,
        times_seen: timesSeen,
        opportunity_fingerprint: opportunityFingerprint,
        material_change_hash: materialChangeHash,
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
        metadata: metadata
      }
    })
    .filter(Boolean)
}

function applyFeedFilters(query, options = {}, timeColumn = "created_at") {
  const includeInactive = Boolean(options.includeInactive)
  const sinceIso = normalizeIso(options.sinceIso)
  const category = normalizeCategory(options.category)
  const minScore = toFiniteOrNull(options.minScore)
  const excludeLowConfidence = Boolean(options.excludeLowConfidence)
  const highConfidenceOnly = Boolean(options.highConfidenceOnly)

  let scoped = query
  if (!includeInactive) {
    scoped = scoped.eq("is_active", true)
  }
  if (sinceIso) {
    scoped = scoped.gte(timeColumn, sinceIso)
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

  const { data, error } = await runFeedQueryWithTimeColumn((timeColumn) =>
    runFeedSelectWithFallback((selectFields) => {
      const query = supabaseAdmin
        .from(TABLE)
        .select(selectFields)
        .order(timeColumn, { ascending: false })
        .order("id", { ascending: false })
        .range(offset, offset + limit - 1)
      return applyFeedFilters(query, options, timeColumn)
    })
  )

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || []
}

exports.listFeedByCursor = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 100)
  const cursorCreatedAt = normalizeIso(options.cursorCreatedAt)
  const cursorId = normalizeId(options.cursorId)
  const useCursor = Boolean(cursorCreatedAt && cursorId)

  const { data, error } = await runFeedQueryWithTimeColumn((timeColumn) =>
    runFeedCardSelectWithFallback((selectFields) => {
      const buildBaseQuery = () =>
        applyFeedFilters(
          supabaseAdmin.from(TABLE).select(selectFields),
          options,
          timeColumn
        )

      if (!useCursor) {
        return buildBaseQuery()
          .order(timeColumn, { ascending: false })
          .order("id", { ascending: false })
          .limit(limit)
      }

      return (async () => {
        const sameTimestamp = await buildBaseQuery()
          .eq(timeColumn, cursorCreatedAt)
          .lt("id", cursorId)
          .order("id", { ascending: false })
          .limit(limit)

        if (sameTimestamp?.error) {
          return sameTimestamp
        }
        const sameRows = Array.isArray(sameTimestamp?.data) ? sameTimestamp.data : []
        if (sameRows.length >= limit) {
          return { data: sameRows.slice(0, limit), error: null }
        }

        const remaining = Math.max(limit - sameRows.length, 0)
        if (remaining <= 0) {
          return { data: sameRows, error: null }
        }

        const olderRows = await buildBaseQuery()
          .lt(timeColumn, cursorCreatedAt)
          .order(timeColumn, { ascending: false })
          .order("id", { ascending: false })
          .limit(remaining)

        if (olderRows?.error) {
          return olderRows
        }

        return {
          data: [...sameRows, ...(Array.isArray(olderRows?.data) ? olderRows.data : [])],
          error: null
        }
      })()
    })
  )

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

  const { count, error } = await runFeedQueryWithTimeColumn((timeColumn) => {
    let query = supabaseAdmin.from(TABLE).select("id", { count: "exact", head: true })
    if (!includeInactive) {
      query = query.eq("is_active", true)
    }
    if (sinceIso) {
      query = query.gte(timeColumn, sinceIso)
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

  const { data, error } = await runFeedQueryWithTimeColumn((timeColumn) => {
    const runQuery = (selectFields) => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(selectFields)
        .in("item_name", itemNames)
        .order(timeColumn, { ascending: false })
        .order("id", { ascending: false })
        .limit(limit)

      if (sinceIso) {
        query = query.gte(timeColumn, sinceIso)
      }
      return query
    }

    return (async () => {
      const primary = await runQuery(RECENT_ROWS_FIELDS_BASE)
      if (!primary?.error) return primary
      if (isMissingColumnError(primary.error)) {
        return runQuery(RECENT_ROWS_FIELDS_LEGACY)
      }
      return primary
    })()
  })
  if (error) {
    throw new AppError(error.message, 500)
  }
  return data || []
}

exports.getActiveRowsByFingerprints = async (options = {}) => {
  const fingerprints = Array.from(
    new Set(
      (Array.isArray(options.fingerprints) ? options.fingerprints : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )
  if (!fingerprints.length) return []

  const sinceIso = normalizeText(options.sinceIso)
  const limit = normalizeLimit(options.limit, 2500)

  const { data, error } = await runFeedQueryWithTimeColumn((timeColumn) => {
    const runQuery = (selectFields) => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(selectFields)
        .eq("is_active", true)
        .in("opportunity_fingerprint", fingerprints)
        .order(timeColumn, { ascending: false })
        .order("id", { ascending: false })
        .limit(limit)
      if (sinceIso) {
        query = query.gte(timeColumn, sinceIso)
      }
      return query
    }

    return (async () => {
      const primary = await runQuery(RECENT_ROWS_FIELDS_BASE)
      if (!primary?.error) return primary
      if (isMissingColumnError(primary.error)) {
        // Schema does not expose fingerprint columns yet.
        return { data: [], error: null }
      }
      return primary
    })()
  })

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
    const { data: idRows, error: selectError } = await runFeedQueryWithTimeColumn(
      (timeColumn) =>
        supabaseAdmin
          .from(TABLE)
          .select("id")
          .eq("is_active", true)
          .lt(timeColumn, cutoff)
          .order(timeColumn, { ascending: true })
          .limit(selectSize)
    )

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
    const { data, error } = await runFeedQueryWithTimeColumn((timeColumn) =>
      supabaseAdmin
        .from(TABLE)
        .select("id")
        .eq("is_active", true)
        .order(timeColumn, { ascending: false })
        .order("id", { ascending: false })
        .range(limit, limit + batchSize - 1)
    )

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

async function updateRowsById(rows = []) {
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

exports.updateRowsById = updateRowsById

exports.updatePublishRefreshState = async (rows = []) => updateRowsById(rows)

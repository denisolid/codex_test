const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_source_catalog"
const INSERT_BATCH_SIZE = 400
const MAX_LIMIT = 5000
const CATEGORY_SET = new Set(["weapon_skin", "case", "sticker_capsule"])

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIntegerOrNull(value, min = 0) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return Math.max(Math.round(parsed), min)
}

function toIntegerOrDefault(value, defaultValue = 0, min = 0) {
  const parsed = toIntegerOrNull(value, min)
  if (parsed == null) {
    return Math.max(Math.round(Number(defaultValue || 0)), min)
  }
  return parsed
}

function normalizeCategory(value) {
  const text = normalizeText(value).toLowerCase()
  return CATEGORY_SET.has(text) ? text : "weapon_skin"
}

function normalizeLimit(value, fallback = 1000) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName) return null

      const itemName =
        normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName
      const category = normalizeCategory(row?.category)

      return {
        market_hash_name: marketHashName,
        item_name: itemName,
        category,
        subcategory: normalizeText(row?.subcategory) || null,
        tradable: row?.tradable == null ? true : Boolean(row.tradable),
        scan_eligible: row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible),
        reference_price: toFiniteOrNull(row?.reference_price ?? row?.referencePrice),
        market_coverage_count: toIntegerOrDefault(
          row?.market_coverage_count ?? row?.marketCoverageCount,
          0,
          0
        ),
        liquidity_rank: toFiniteOrNull(row?.liquidity_rank ?? row?.liquidityRank),
        volume_7d: toIntegerOrNull(row?.volume_7d ?? row?.volume7d, 0),
        snapshot_stale: row?.snapshot_stale == null ? Boolean(row?.snapshotStale) : Boolean(row.snapshot_stale),
        snapshot_captured_at: row?.snapshot_captured_at || row?.snapshotCapturedAt || null,
        invalid_reason: normalizeText(row?.invalid_reason || row?.invalidReason) || null,
        source_tag: normalizeText(row?.source_tag || row?.sourceTag) || "curated_seed",
        is_active: row?.is_active == null ? (row?.isActive == null ? true : Boolean(row.isActive)) : Boolean(row.is_active),
        last_enriched_at: row?.last_enriched_at || row?.lastEnrichedAt || null
      }
    })
    .filter(Boolean)
}

async function upsertInChunks(rows = []) {
  const payload = normalizeRows(rows)
  if (!payload.length) return 0

  let total = 0
  for (let index = 0; index < payload.length; index += INSERT_BATCH_SIZE) {
    const chunk = payload.slice(index, index + INSERT_BATCH_SIZE)
    const { error } = await supabaseAdmin
      .from(TABLE)
      .upsert(chunk, { onConflict: "market_hash_name" })

    if (error) {
      throw new AppError(error.message, 500)
    }
    total += chunk.length
  }

  return total
}

exports.upsertRows = async (rows = []) => upsertInChunks(rows)

exports.listActiveTradable = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 1500)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select(
      "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,invalid_reason,source_tag,is_active,last_enriched_at"
    )
    .eq("is_active", true)
    .eq("tradable", true)
    .order("liquidity_rank", { ascending: false, nullsFirst: false })
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }
  return data || []
}

exports.listScanEligible = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 2000)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select(
      "market_hash_name,item_name,category,subcategory,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at"
    )
    .eq("is_active", true)
    .eq("tradable", true)
    .eq("scan_eligible", true)
    .order("liquidity_rank", { ascending: false, nullsFirst: false })
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }
  return data || []
}

exports.listCoverageSummary = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 3000)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select(
      "category,tradable,scan_eligible,is_active,reference_price,volume_7d,market_coverage_count,snapshot_stale,invalid_reason"
    )
    .eq("is_active", true)
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || []
}

exports.__testables = {
  normalizeRows,
  toIntegerOrNull,
  toIntegerOrDefault
}

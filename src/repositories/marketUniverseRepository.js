const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_universe"
const MAX_LIMIT = 2000
const INSERT_BATCH_SIZE = 400
const CATEGORY_SET = new Set(["weapon_skin", "case", "sticker_capsule", "knife", "glove"])

function normalizeLimit(value, fallback = 300) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

function normalizeCategory(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
  return CATEGORY_SET.has(normalized) ? normalized : "weapon_skin"
}

exports.listActiveByLiquidityRank = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 300)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("market_hash_name, item_name, category, subcategory, liquidity_rank, is_active, updated_at")
    .eq("is_active", true)
    .order("liquidity_rank", { ascending: true })
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || []
}

exports.replaceActiveUniverse = async (rows = []) => {
  const payload = Array.isArray(rows)
    ? rows
        .map((row, index) => {
          const marketHashName = String(row?.market_hash_name || row?.marketHashName || "").trim()
          if (!marketHashName) return null
          const itemName =
            String(row?.item_name || row?.itemName || marketHashName).trim() || marketHashName
          const liquidityRank = Number(row?.liquidity_rank || row?.liquidityRank || index + 1)
          return {
            market_hash_name: marketHashName,
            item_name: itemName,
            category: normalizeCategory(row?.category),
            subcategory: String(row?.subcategory || "").trim() || null,
            liquidity_rank: Number.isFinite(liquidityRank) ? Math.max(Math.round(liquidityRank), 1) : index + 1,
            is_active: true
          }
        })
        .filter(Boolean)
    : []

  const deactivateRes = await supabaseAdmin.from(TABLE).update({ is_active: false }).eq("is_active", true)
  if (deactivateRes.error) {
    throw new AppError(deactivateRes.error.message, 500)
  }

  if (!payload.length) {
    return {
      deactivated: Number(deactivateRes.count || 0),
      upserted: 0
    }
  }

  let upserted = 0
  for (let index = 0; index < payload.length; index += INSERT_BATCH_SIZE) {
    const chunk = payload.slice(index, index + INSERT_BATCH_SIZE)
    const { error } = await supabaseAdmin
      .from(TABLE)
      .upsert(chunk, { onConflict: "market_hash_name" })
    if (error) {
      throw new AppError(error.message, 500)
    }
    upserted += chunk.length
  }

  return {
    deactivated: Number(deactivateRes.count || 0),
    upserted
  }
}

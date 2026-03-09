const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_universe"
const MAX_LIMIT = 2000

function normalizeLimit(value, fallback = 300) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

exports.listActiveByLiquidityRank = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 300)
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("market_hash_name, item_name, liquidity_rank, is_active, updated_at")
    .eq("is_active", true)
    .order("liquidity_rank", { ascending: true })
    .limit(limit)

  if (error) {
    throw new AppError(error.message, 500)
  }

  return data || []
}

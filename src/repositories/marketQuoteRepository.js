const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_quotes"
const SOURCES = new Set(["steam", "skinport", "csfloat", "dmarket"])
const INSERT_BATCH_SIZE = 400

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeSource(value) {
  const source = normalizeText(value).toLowerCase()
  return SOURCES.has(source) ? source : ""
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIntegerOrNull(value, options = {}) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  const min = Number.isFinite(Number(options.min)) ? Number(options.min) : -Infinity
  const max = Number.isFinite(Number(options.max)) ? Number(options.max) : Infinity
  return Math.min(Math.max(Math.round(parsed), min), max)
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
      const itemName = normalizeText(row?.item_name || row?.itemName)
      const market = normalizeSource(row?.market || row?.source)
      if (!itemName || !market) return null

      const bestBuy = toFiniteOrNull(row?.best_buy ?? row?.bestBuy)
      const bestSell = toFiniteOrNull(row?.best_sell ?? row?.bestSell)
      const bestSellNet = toFiniteOrNull(row?.best_sell_net ?? row?.bestSellNet)
      const volume7d = toIntegerOrNull(row?.volume_7d ?? row?.volume7d, { min: 0 })
      const liquidityScore = toIntegerOrNull(row?.liquidity_score ?? row?.liquidityScore, {
        min: 0,
        max: 100
      })

      return {
        item_name: itemName,
        market,
        best_buy: bestBuy == null ? null : Number(bestBuy.toFixed(4)),
        best_sell: bestSell == null ? null : Number(bestSell.toFixed(4)),
        best_sell_net: bestSellNet == null ? null : Number(bestSellNet.toFixed(4)),
        volume_7d: volume7d,
        liquidity_score: liquidityScore,
        fetched_at: row?.fetched_at || row?.fetchedAt || new Date().toISOString(),
        quality_flags: toJsonObject(row?.quality_flags || row?.qualityFlags)
      }
    })
    .filter(Boolean)
}

exports.insertRows = async (rows = []) => {
  const payload = normalizeRows(rows)
  if (!payload.length) return 0

  let insertedCount = 0
  for (let index = 0; index < payload.length; index += INSERT_BATCH_SIZE) {
    const chunk = payload.slice(index, index + INSERT_BATCH_SIZE)
    const { error } = await supabaseAdmin.from(TABLE).insert(chunk)
    if (error) {
      throw new AppError(error.message, 500)
    }
    insertedCount += chunk.length
  }

  return insertedCount
}

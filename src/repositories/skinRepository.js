const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const UPSERT_BATCH_SIZE = 250
const MARKET_HASH_QUERY_BATCH_SIZE = 60
const ID_QUERY_BATCH_SIZE = 400

function normalizeMarketHashNames(names = []) {
  return Array.from(
    new Set(
      (Array.isArray(names) ? names : [])
        .map((name) => String(name || "").trim())
        .filter(Boolean)
    )
  )
}

function normalizeIds(ids = []) {
  return Array.from(
    new Set(
      (Array.isArray(ids) ? ids : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0)
    )
  )
}

function chunkArray(values = [], chunkSize = 100) {
  const rows = Array.isArray(values) ? values : []
  const size = Math.max(Number(chunkSize || 0), 1)
  const chunks = []
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size))
  }
  return chunks
}

function formatSupabaseError(error, fallbackMessage = "database_error") {
  const message = String(error?.message || "").trim() || fallbackMessage
  const details = String(error?.details || "").trim()
  const hint = String(error?.hint || "").trim()
  const code = String(error?.code || "").trim()

  const parts = [message]
  if (details) parts.push(`details: ${details}`)
  if (hint) parts.push(`hint: ${hint}`)
  if (code) parts.push(`code: ${code}`)
  return parts.join(" | ")
}

function isMissingColumnError(error, columnName = "") {
  const message = String(error?.message || "").toLowerCase()
  const code = String(error?.code || "").toUpperCase()
  return (
    code === "42703" ||
    (message.includes("does not exist") &&
      (!columnName || message.includes(String(columnName || "").toLowerCase())))
  )
}

exports.upsertSkins = async (rows = []) => {
  const payload = (Array.isArray(rows) ? rows : []).filter(Boolean)
  if (!payload.length) return []

  const mergedRows = []
  for (const chunk of chunkArray(payload, UPSERT_BATCH_SIZE)) {
    let { data, error } = await supabaseAdmin
      .from("skins")
      .upsert(chunk, { onConflict: "market_hash_name" })
      .select("*")
    if (error && isMissingColumnError(error, "canonical_rarity")) {
      const compatibilityChunk = chunk.map((row) => {
        const clone = { ...row }
        delete clone.canonical_rarity
        return clone
      })
      const retried = await supabaseAdmin
        .from("skins")
        .upsert(compatibilityChunk, { onConflict: "market_hash_name" })
        .select("*")
      data = retried.data
      error = retried.error
    }

    if (error) {
      throw new AppError(formatSupabaseError(error, "skins_upsert_failed"), 500)
    }

    if (Array.isArray(data) && data.length) {
      mergedRows.push(...data)
    }
  }

  return mergedRows
}

exports.getByMarketHashNames = async (names = []) => {
  const safeNames = normalizeMarketHashNames(names)
  if (!safeNames.length) return []

  const rows = []
  for (const chunk of chunkArray(safeNames, MARKET_HASH_QUERY_BATCH_SIZE)) {
    const { data, error } = await supabaseAdmin
      .from("skins")
      .select("*")
      .in("market_hash_name", chunk)

    if (error) {
      throw new AppError(formatSupabaseError(error, "skins_lookup_failed"), 500)
    }

    if (Array.isArray(data) && data.length) {
      rows.push(...data)
    }
  }

  return rows
}

exports.getByIds = async (ids = []) => {
  const safeIds = normalizeIds(ids)
  if (!safeIds.length) return []

  const rows = []
  for (const chunk of chunkArray(safeIds, ID_QUERY_BATCH_SIZE)) {
    const { data, error } = await supabaseAdmin
      .from("skins")
      .select("*")
      .in("id", chunk)

    if (error) {
      throw new AppError(formatSupabaseError(error, "skins_lookup_by_ids_failed"), 500)
    }

    if (Array.isArray(data) && data.length) {
      rows.push(...data)
    }
  }

  return rows
}

exports.getById = async (id) => {
  const { data, error } = await supabaseAdmin
    .from("skins")
    .select("*")
    .eq("id", id)
    .single()

  if (error && error.code !== "PGRST116") {
    throw new AppError(formatSupabaseError(error, "skin_lookup_failed"), 500)
  }
  return data || null
}

exports.listAll = async () => {
  const { data, error } = await supabaseAdmin
    .from("skins")
    .select("id, market_hash_name")
    .order("id", { ascending: true })

  if (error) {
    throw new AppError(formatSupabaseError(error, "skins_list_failed"), 500)
  }
  return data || []
}

exports.__testables = {
  chunkArray,
  normalizeMarketHashNames,
  normalizeIds,
  formatSupabaseError
}

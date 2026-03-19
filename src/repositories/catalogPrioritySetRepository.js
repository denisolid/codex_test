const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const SET_TABLE = "catalog_priority_sets"
const ITEM_TABLE = "catalog_priority_set_items"
const UPSERT_BATCH_SIZE = 200
const PRIORITY_TIERS = new Set(["tier_a", "tier_b"])
const PRIORITY_CATEGORIES = new Set(["weapon_skin", "case", "knife", "glove"])

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeTier(value, fallback = "tier_b") {
  const tier = normalizeText(value).toLowerCase()
  if (PRIORITY_TIERS.has(tier)) return tier
  return PRIORITY_TIERS.has(String(fallback || "").toLowerCase())
    ? String(fallback || "").toLowerCase()
    : "tier_b"
}

function normalizeCategory(value, fallback = "weapon_skin") {
  const category = normalizeText(value).toLowerCase()
  if (PRIORITY_CATEGORIES.has(category)) return category
  return PRIORITY_CATEGORIES.has(String(fallback || "").toLowerCase())
    ? String(fallback || "").toLowerCase()
    : "weapon_skin"
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function formatError(error, fallback = "catalog_priority_set_error") {
  const message = normalizeText(error?.message) || fallback
  const details = normalizeText(error?.details)
  const hint = normalizeText(error?.hint)
  const code = normalizeText(error?.code)
  const parts = [message]
  if (details) parts.push(`details: ${details}`)
  if (hint) parts.push(`hint: ${hint}`)
  if (code) parts.push(`code: ${code}`)
  return parts.join(" | ")
}

function stableJson(value) {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableJson(entry)).join(",")}]`
  }
  if (value && typeof value === "object") {
    const keys = Object.keys(value).sort()
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`
  }
  return JSON.stringify(value)
}

function normalizeSetRow(input = {}) {
  const setName = normalizeText(input.set_name || input.setName)
  if (!setName) return null
  const version = Math.max(Math.round(Number(input.version || 1)), 1)
  const description = normalizeText(input.description) || null
  const policyHints =
    input.policy_hints && typeof input.policy_hints === "object" ? input.policy_hints : {}
  const rawPayload =
    input.raw_payload && typeof input.raw_payload === "object"
      ? input.raw_payload
      : input.rawPayload && typeof input.rawPayload === "object"
        ? input.rawPayload
        : {}

  return {
    set_name: setName,
    version,
    description,
    policy_hints: policyHints,
    raw_payload: rawPayload,
    is_active: input.is_active == null ? true : Boolean(input.is_active)
  }
}

function normalizeItemRows(setName = "", rows = []) {
  const safeSetName = normalizeText(setName)
  if (!safeSetName) return []
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const canonicalItemName = normalizeText(row?.canonical_item_name || row?.canonicalItemName)
      const itemName = normalizeText(row?.item_name || row?.itemName || canonicalItemName)
      if (!canonicalItemName || !itemName) return null
      const canonicalCategory = normalizeCategory(
        row?.canonical_category || row?.canonicalCategory || row?.category
      )
      const priorityTier = normalizeTier(row?.priority_tier || row?.priorityTier)
      const priorityRank = Math.max(Math.round(Number(row?.priority_rank || row?.priorityRank || 1)), 1)
      const priorityBoost =
        toFiniteOrNull(row?.priority_boost ?? row?.priorityBoost) == null
          ? 0
          : Number(toFiniteOrNull(row?.priority_boost ?? row?.priorityBoost).toFixed(2))
      const policyHints =
        row?.policy_hints && typeof row.policy_hints === "object"
          ? row.policy_hints
          : row?.policyHints && typeof row.policyHints === "object"
            ? row.policyHints
            : {}

      return {
        set_name: safeSetName,
        canonical_category: canonicalCategory,
        item_name: itemName,
        canonical_item_name: canonicalItemName,
        priority_tier: priorityTier,
        priority_rank: priorityRank,
        priority_boost: priorityBoost,
        policy_hints: policyHints,
        is_active: row?.is_active == null ? true : Boolean(row.is_active)
      }
    })
    .filter(Boolean)
}

exports.upsertSet = async (row = {}) => {
  const payload = normalizeSetRow(row)
  if (!payload) {
    throw new AppError("catalog_priority_set_invalid", 400)
  }
  const { data, error } = await supabaseAdmin
    .from(SET_TABLE)
    .upsert(payload, { onConflict: "set_name" })
    .select("set_name,version,description,policy_hints,raw_payload,is_active")
    .maybeSingle()

  if (error) {
    throw new AppError(formatError(error, "catalog_priority_set_upsert_failed"), 500)
  }
  return data || payload
}

exports.replaceItems = async (setName = "", rows = []) => {
  const safeSetName = normalizeText(setName)
  if (!safeSetName) return { deactivated: 0, upserted: 0, activeItems: 0 }
  const payload = normalizeItemRows(safeSetName, rows)
  const payloadByKey = new Map(
    payload.map((row) => [`${row.canonical_category}::${row.canonical_item_name}`, row])
  )

  const { data: existingRows, error: existingError } = await supabaseAdmin
    .from(ITEM_TABLE)
    .select(
      "id,set_name,canonical_category,item_name,canonical_item_name,priority_tier,priority_rank,priority_boost,policy_hints,is_active"
    )
    .eq("set_name", safeSetName)
    .eq("is_active", true)
  if (existingError) {
    throw new AppError(formatError(existingError, "catalog_priority_items_list_existing_failed"), 500)
  }
  const existing = Array.isArray(existingRows) ? existingRows : []
  const existingByKey = new Map(
    existing.map((row) => [
      `${normalizeCategory(row?.canonical_category)}::${normalizeText(row?.canonical_item_name)}`,
      row
    ])
  )

  const rowsToUpsert = []
  for (const row of payload) {
    const key = `${row.canonical_category}::${row.canonical_item_name}`
    const previous = existingByKey.get(key)
    if (!previous) {
      rowsToUpsert.push(row)
      continue
    }
    const sameItemName = normalizeText(previous.item_name) === row.item_name
    const sameTier = normalizeTier(previous.priority_tier) === row.priority_tier
    const sameRank = Number(previous.priority_rank || 0) === Number(row.priority_rank || 0)
    const sameBoost =
      Number(toFiniteOrNull(previous.priority_boost) || 0).toFixed(2) ===
      Number(toFiniteOrNull(row.priority_boost) || 0).toFixed(2)
    const sameHints = stableJson(previous.policy_hints || {}) === stableJson(row.policy_hints || {})
    if (!(sameItemName && sameTier && sameRank && sameBoost && sameHints)) {
      rowsToUpsert.push(row)
    }
  }

  const deactivateIds = existing
    .filter((row) => !payloadByKey.has(`${normalizeCategory(row?.canonical_category)}::${normalizeText(row?.canonical_item_name)}`))
    .map((row) => normalizeText(row?.id))
    .filter(Boolean)

  let deactivated = 0
  for (let index = 0; index < deactivateIds.length; index += UPSERT_BATCH_SIZE) {
    const chunk = deactivateIds.slice(index, index + UPSERT_BATCH_SIZE)
    const { error: deactivateError } = await supabaseAdmin
      .from(ITEM_TABLE)
      .update({ is_active: false })
      .in("id", chunk)
    if (deactivateError) {
      throw new AppError(formatError(deactivateError, "catalog_priority_items_deactivate_failed"), 500)
    }
    deactivated += chunk.length
  }

  let upserted = 0
  for (let index = 0; index < rowsToUpsert.length; index += UPSERT_BATCH_SIZE) {
    const chunk = rowsToUpsert.slice(index, index + UPSERT_BATCH_SIZE)
    const { error } = await supabaseAdmin
      .from(ITEM_TABLE)
      .upsert(chunk, { onConflict: "set_name,canonical_category,canonical_item_name" })
    if (error) {
      throw new AppError(formatError(error, "catalog_priority_items_upsert_failed"), 500)
    }
    upserted += chunk.length
  }

  const { count, error: countError } = await supabaseAdmin
    .from(ITEM_TABLE)
    .select("id", { head: true, count: "exact" })
    .eq("set_name", safeSetName)
    .eq("is_active", true)

  if (countError) {
    throw new AppError(formatError(countError, "catalog_priority_items_count_failed"), 500)
  }

  return {
    deactivated,
    upserted,
    activeItems: Number(count || 0)
  }
}

exports.listActiveItems = async (setName = "") => {
  const safeSetName = normalizeText(setName)
  if (!safeSetName) return []
  const { data, error } = await supabaseAdmin
    .from(ITEM_TABLE)
    .select(
      "set_name,canonical_category,item_name,canonical_item_name,priority_tier,priority_rank,priority_boost,policy_hints,is_active"
    )
    .eq("set_name", safeSetName)
    .eq("is_active", true)
    .order("priority_boost", { ascending: false, nullsFirst: false })
    .order("priority_rank", { ascending: true, nullsFirst: false })

  if (error) {
    throw new AppError(formatError(error, "catalog_priority_items_list_failed"), 500)
  }
  return Array.isArray(data) ? data : []
}

exports.__testables = {
  normalizeSetRow,
  normalizeItemRows,
  normalizeCategory,
  normalizeTier
}

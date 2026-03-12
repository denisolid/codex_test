const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_source_catalog"
const INSERT_BATCH_SIZE = 200
const MAX_LIMIT = 12000
const SELECT_PAGE_SIZE = 1000
const CATEGORY_SET = new Set(["weapon_skin", "case", "sticker_capsule", "knife", "glove"])
const CANDIDATE_STATUS_SET = new Set(["candidate", "enriching", "eligible", "rejected"])
const CANDIDATE_STATE_COLUMNS = Object.freeze([
  "candidate_status",
  "missing_snapshot",
  "missing_reference",
  "missing_market_coverage",
  "enrichment_priority",
  "eligibility_reason"
])

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

function normalizeCandidateStatus(value, fallback = "candidate") {
  const text = normalizeText(value).toLowerCase()
  if (CANDIDATE_STATUS_SET.has(text)) return text
  return CANDIDATE_STATUS_SET.has(String(fallback || "").toLowerCase())
    ? String(fallback || "").toLowerCase()
    : "candidate"
}

function normalizeCandidateStatuses(values = [], fallback = ["eligible", "enriching", "candidate"]) {
  const fallbackList = Array.isArray(fallback) ? fallback : [fallback]
  const normalized = Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeCandidateStatus(value, ""))
        .filter((value) => CANDIDATE_STATUS_SET.has(value))
    )
  )
  if (normalized.length) return normalized
  return Array.from(
    new Set(
      fallbackList
        .map((value) => normalizeCandidateStatus(value, ""))
        .filter((value) => CANDIDATE_STATUS_SET.has(value))
    )
  )
}

function normalizeCategories(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value).toLowerCase())
        .filter((value) => CATEGORY_SET.has(value))
    )
  )
}

function normalizeLimit(value, fallback = 1000) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

function formatSupabaseError(error, fallbackMessage = "database_error") {
  const message = normalizeText(error?.message) || fallbackMessage
  const details = normalizeText(error?.details)
  const hint = normalizeText(error?.hint)
  const code = normalizeText(error?.code)

  const chunks = [message]
  if (details) chunks.push(`details: ${details}`)
  if (hint) chunks.push(`hint: ${hint}`)
  if (code) chunks.push(`code: ${code}`)
  return chunks.join(" | ")
}

function isTransientNetworkError(error) {
  const message = normalizeText(error?.message).toLowerCase()
  return (
    message.includes("fetch failed") ||
    message.includes("connect timeout") ||
    message.includes("etimedout") ||
    message.includes("ecconnreset")
  )
}

function isMissingCandidateColumnError(error) {
  const message = normalizeText(error?.message).toLowerCase()
  if (!message) return false
  const hasMissingColumnSignal =
    message.includes("pgrst204") ||
    message.includes("42703") ||
    message.includes("schema cache") ||
    message.includes("does not exist")
  if (!hasMissingColumnSignal) return false
  return CANDIDATE_STATE_COLUMNS.some((column) => message.includes(String(column).toLowerCase()))
}

function stripCandidateStateColumns(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => {
    if (!row || typeof row !== "object") return row
    const clone = { ...row }
    for (const column of CANDIDATE_STATE_COLUMNS) {
      delete clone[column]
    }
    return clone
  })
}

async function upsertChunkWithRetry(chunk = [], maxAttempts = 2) {
  let lastError = null
  for (let attempt = 1; attempt <= Math.max(Number(maxAttempts || 0), 1); attempt += 1) {
    const { error } = await supabaseAdmin
      .from(TABLE)
      .upsert(chunk, { onConflict: "market_hash_name" })
    if (!error) {
      return
    }
    if (isMissingCandidateColumnError(error)) {
      const { error: compatibilityError } = await supabaseAdmin
        .from(TABLE)
        .upsert(stripCandidateStateColumns(chunk), { onConflict: "market_hash_name" })
      if (!compatibilityError) {
        return
      }
      lastError = compatibilityError
      break
    }
    lastError = error
    if (!isTransientNetworkError(error) || attempt >= maxAttempts) {
      break
    }
  }

  throw new AppError(
    formatSupabaseError(lastError, "market_source_catalog_upsert_failed"),
    500
  )
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName) return null

      const itemName =
        normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName
      const category = normalizeCategory(row?.category)
      const scanEligible =
        row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible)
      const candidateStatus = normalizeCandidateStatus(
        row?.candidate_status ?? row?.candidateStatus,
        scanEligible ? "eligible" : "candidate"
      )

      return {
        market_hash_name: marketHashName,
        item_name: itemName,
        category,
        subcategory: normalizeText(row?.subcategory) || null,
        tradable: row?.tradable == null ? true : Boolean(row.tradable),
        scan_eligible: scanEligible,
        candidate_status: candidateStatus,
        missing_snapshot:
          row?.missing_snapshot == null
            ? Boolean(row?.missingSnapshot)
            : Boolean(row.missing_snapshot),
        missing_reference:
          row?.missing_reference == null
            ? Boolean(row?.missingReference)
            : Boolean(row.missing_reference),
        missing_market_coverage:
          row?.missing_market_coverage == null
            ? Boolean(row?.missingMarketCoverage)
            : Boolean(row.missing_market_coverage),
        enrichment_priority: toFiniteOrNull(
          row?.enrichment_priority ?? row?.enrichmentPriority
        ),
        eligibility_reason:
          normalizeText(row?.eligibility_reason || row?.eligibilityReason) || null,
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
    await upsertChunkWithRetry(chunk, 2)
    total += chunk.length
  }

  return total
}

async function selectWithPagination(buildQuery, options = {}) {
  const limit = normalizeLimit(options.limit, 1000)
  const fallbackMessage = normalizeText(options.fallbackMessage) || "market_source_catalog_select_failed"
  const rows = []
  let offset = 0

  while (rows.length < limit) {
    const remaining = limit - rows.length
    const pageSize = Math.min(SELECT_PAGE_SIZE, remaining)
    const from = offset
    const to = offset + pageSize - 1

    const { data, error } = await buildQuery().range(from, to)
    if (error) {
      throw new AppError(formatSupabaseError(error, fallbackMessage), 500)
    }

    const chunk = Array.isArray(data) ? data : []
    if (!chunk.length) break

    rows.push(...chunk)
    if (chunk.length < pageSize) break
    offset += chunk.length
  }

  return rows
}

async function selectWithCompatibilityFallback({
  buildPrimaryQuery,
  buildFallbackQuery,
  limit = 1000,
  fallbackMessage = "market_source_catalog_select_failed"
} = {}) {
  try {
    return await selectWithPagination(buildPrimaryQuery, { limit, fallbackMessage })
  } catch (error) {
    if (!buildFallbackQuery || !isMissingCandidateColumnError(error)) {
      throw error
    }
    return selectWithPagination(buildFallbackQuery, { limit, fallbackMessage })
  }
}

exports.upsertRows = async (rows = []) => upsertInChunks(rows)

exports.listActiveTradable = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 1500)
  const categories = normalizeCategories(options.categories)
  return selectWithCompatibilityFallback({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,enrichment_priority,eligibility_reason,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,invalid_reason,source_tag,is_active,last_enriched_at"
        )
        .eq("is_active", true)
        .eq("tradable", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,invalid_reason,source_tag,is_active,last_enriched_at"
        )
        .eq("is_active", true)
        .eq("tradable", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    limit,
    fallbackMessage: "market_source_catalog_list_active_failed"
  })
}

exports.listScanEligible = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 2000)
  const categories = normalizeCategories(options.categories)
  return selectWithCompatibilityFallback({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "market_hash_name,item_name,category,subcategory,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,enrichment_priority,eligibility_reason,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at"
        )
        .eq("is_active", true)
        .eq("tradable", true)
        .eq("scan_eligible", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "market_hash_name,item_name,category,subcategory,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at"
        )
        .eq("is_active", true)
        .eq("tradable", true)
        .eq("scan_eligible", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    limit,
    fallbackMessage: "market_source_catalog_list_eligible_failed"
  })
}

exports.listCoverageSummary = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 3000)
  const categories = normalizeCategories(options.categories)
  return selectWithCompatibilityFallback({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "category,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,is_active,reference_price,volume_7d,market_coverage_count,snapshot_stale,invalid_reason,eligibility_reason"
        )
        .eq("is_active", true)

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "category,tradable,scan_eligible,is_active,reference_price,volume_7d,market_coverage_count,snapshot_stale,invalid_reason"
        )
        .eq("is_active", true)

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    limit,
    fallbackMessage: "market_source_catalog_coverage_failed"
  })
}

exports.listCandidatePool = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 4000)
  const categories = normalizeCategories(options.categories)
  const candidateStatuses = normalizeCandidateStatuses(options.candidateStatuses)

  return selectWithCompatibilityFallback({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,enrichment_priority,eligibility_reason,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,invalid_reason,source_tag,is_active,last_enriched_at"
        )
        .eq("is_active", true)
        .eq("tradable", true)
        .in("candidate_status", candidateStatuses)
        .order("enrichment_priority", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(
          "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,invalid_reason,source_tag,is_active,last_enriched_at"
        )
        .eq("is_active", true)
        .eq("tradable", true)
        .eq("scan_eligible", false)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    limit,
    fallbackMessage: "market_source_catalog_list_candidate_pool_failed"
  })
}

exports.__testables = {
  normalizeRows,
  normalizeCandidateStatus,
  normalizeCandidateStatuses,
  toIntegerOrNull,
  toIntegerOrDefault,
  formatSupabaseError
}

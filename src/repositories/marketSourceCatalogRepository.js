const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_source_catalog"
const INSERT_BATCH_SIZE = 200
const MAX_LIMIT = 12000
const SELECT_PAGE_SIZE = 1000
const QUERY_BATCH_SIZE = 120
const CATEGORY_SET = new Set(["weapon_skin", "case", "sticker_capsule", "knife", "glove"])
const CANDIDATE_STATUS_SET = new Set([
  "candidate",
  "enriching",
  "near_eligible",
  "eligible",
  "rejected"
])
const MATURITY_STATE_SET = new Set(["cold", "enriching", "near_eligible", "eligible"])
const SCAN_LAYER_SET = new Set(["hot", "warm", "cold"])
const CATALOG_STATUS_SET = new Set(["scannable", "shadow", "blocked"])
const PRIORITY_TIER_SET = new Set(["tier_a", "tier_b"])
const CANDIDATE_STATE_COLUMNS = Object.freeze([
  "candidate_status",
  "missing_snapshot",
  "missing_reference",
  "missing_market_coverage",
  "enrichment_priority",
  "eligibility_reason",
  "maturity_state",
  "maturity_score",
  "scan_layer",
  "quote_fetched_at",
  "snapshot_state",
  "reference_state",
  "liquidity_state",
  "coverage_state",
  "progression_status",
  "progression_blockers",
  "catalog_status",
  "catalog_block_reason",
  "catalog_quality_score",
  "last_market_signal_at",
  "priority_set_name",
  "priority_tier",
  "priority_rank",
  "priority_boost",
  "is_priority_item"
])
const PRIMARY_SELECT_COLUMNS =
  "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,enrichment_priority,eligibility_reason,maturity_state,maturity_score,scan_layer,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,quote_fetched_at,snapshot_state,reference_state,liquidity_state,coverage_state,progression_status,progression_blockers,catalog_status,catalog_block_reason,catalog_quality_score,last_market_signal_at,priority_set_name,priority_tier,priority_rank,priority_boost,is_priority_item,invalid_reason,source_tag,is_active,last_enriched_at"
const COMPATIBILITY_SELECT_COLUMNS =
  "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,enrichment_priority,eligibility_reason,maturity_state,maturity_score,scan_layer,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,quote_fetched_at,snapshot_state,reference_state,liquidity_state,coverage_state,progression_status,progression_blockers,invalid_reason,source_tag,is_active,last_enriched_at"
const LEGACY_FALLBACK_SELECT_COLUMNS =
  "market_hash_name,item_name,category,subcategory,tradable,scan_eligible,reference_price,market_coverage_count,liquidity_rank,volume_7d,snapshot_stale,snapshot_captured_at,invalid_reason,source_tag,is_active,last_enriched_at"
const COVERAGE_SUMMARY_PRIMARY_SELECT_COLUMNS =
  "category,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,maturity_state,is_active,reference_price,volume_7d,market_coverage_count,liquidity_rank,snapshot_stale,quote_fetched_at,snapshot_captured_at,invalid_reason,eligibility_reason,snapshot_state,reference_state,liquidity_state,coverage_state,progression_status,progression_blockers,catalog_status,catalog_block_reason,catalog_quality_score,last_market_signal_at,priority_set_name,priority_tier,priority_rank,priority_boost,is_priority_item"
const COVERAGE_SUMMARY_COMPATIBILITY_SELECT_COLUMNS =
  "category,tradable,scan_eligible,candidate_status,missing_snapshot,missing_reference,missing_market_coverage,maturity_state,is_active,reference_price,volume_7d,market_coverage_count,liquidity_rank,snapshot_stale,quote_fetched_at,snapshot_captured_at,invalid_reason,eligibility_reason,snapshot_state,reference_state,liquidity_state,coverage_state,progression_status,progression_blockers"
const COVERAGE_SUMMARY_LEGACY_FALLBACK_SELECT_COLUMNS =
  "category,tradable,scan_eligible,is_active,reference_price,volume_7d,market_coverage_count,snapshot_stale,invalid_reason"

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

function normalizeMaturityState(value, fallback = "cold") {
  const text = normalizeText(value).toLowerCase()
  if (MATURITY_STATE_SET.has(text)) return text
  const fallbackValue = normalizeText(fallback).toLowerCase()
  return MATURITY_STATE_SET.has(fallbackValue) ? fallbackValue : "cold"
}

function normalizeScanLayer(value, fallback = "cold") {
  const text = normalizeText(value).toLowerCase()
  if (SCAN_LAYER_SET.has(text)) return text
  const fallbackValue = normalizeText(fallback).toLowerCase()
  return SCAN_LAYER_SET.has(fallbackValue) ? fallbackValue : "cold"
}

function normalizeCatalogStatus(value, fallback = "shadow") {
  const text = normalizeText(value).toLowerCase()
  if (CATALOG_STATUS_SET.has(text)) return text
  const fallbackValue = normalizeText(fallback).toLowerCase()
  return CATALOG_STATUS_SET.has(fallbackValue) ? fallbackValue : "shadow"
}

function normalizePriorityTier(value, fallback = null) {
  const text = normalizeText(value).toLowerCase()
  if (PRIORITY_TIER_SET.has(text)) return text
  const fallbackValue = normalizeText(fallback).toLowerCase()
  return PRIORITY_TIER_SET.has(fallbackValue) ? fallbackValue : null
}

function deriveScanLayerFromMaturityState(state = "cold") {
  const maturityState = normalizeMaturityState(state, "cold")
  if (maturityState === "eligible") return "hot"
  if (maturityState === "near_eligible" || maturityState === "enriching") return "warm"
  return "cold"
}

function normalizeCandidateStatuses(
  values = [],
  fallback = ["eligible", "near_eligible", "enriching", "candidate"]
) {
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

function normalizeCatalogStatuses(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeCatalogStatus(value, ""))
        .filter((value) => CATALOG_STATUS_SET.has(value))
    )
  )
}

function normalizeMarketHashNames(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )
}

function normalizeLimit(value, fallback = 1000) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_LIMIT)
}

function normalizeOffset(value, fallback = 0) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), 0)
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

let resolveCompatibleCatalogStatusFieldsRef = null

function getResolveCompatibleCatalogStatusFields() {
  if (typeof resolveCompatibleCatalogStatusFieldsRef === "function") {
    return resolveCompatibleCatalogStatusFieldsRef
  }
  const marketSourceCatalogService = require("../services/marketSourceCatalogService")
  resolveCompatibleCatalogStatusFieldsRef =
    marketSourceCatalogService?.resolveCompatibleCatalogStatusFields
  return resolveCompatibleCatalogStatusFieldsRef
}

function applyCatalogStatusCompatibility(rows = []) {
  const resolveCompatibleCatalogStatusFields = getResolveCompatibleCatalogStatusFields()
  if (typeof resolveCompatibleCatalogStatusFields !== "function") {
    return Array.isArray(rows) ? rows : []
  }

  return (Array.isArray(rows) ? rows : []).map((row) => {
    if (!row || typeof row !== "object") return row
    const compatible = resolveCompatibleCatalogStatusFields(row)
    return {
      ...row,
      catalog_status: normalizeCatalogStatus(
        row?.catalog_status ?? row?.catalogStatus,
        compatible?.catalogStatus || "shadow"
      ),
      catalog_block_reason:
        normalizeText(row?.catalog_block_reason || row?.catalogBlockReason) ||
        compatible?.catalogBlockReason ||
        null,
      catalog_quality_score:
        toFiniteOrNull(row?.catalog_quality_score ?? row?.catalogQualityScore) ??
        toFiniteOrNull(compatible?.catalogQualityScore) ??
        0,
      last_market_signal_at:
        row?.last_market_signal_at ||
        row?.lastMarketSignalAt ||
        compatible?.lastMarketSignalAt ||
        null
    }
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
      const maturityState = normalizeMaturityState(
        row?.maturity_state ?? row?.maturityState,
        scanEligible
          ? "eligible"
          : candidateStatus === "near_eligible"
            ? "near_eligible"
            : candidateStatus === "enriching"
              ? "enriching"
              : "cold"
      )
      const scanLayer = normalizeScanLayer(
        row?.scan_layer ?? row?.scanLayer,
        deriveScanLayerFromMaturityState(maturityState)
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
        enrichment_priority:
          toFiniteOrNull(row?.enrichment_priority ?? row?.enrichmentPriority) ?? 0,
        eligibility_reason:
          normalizeText(row?.eligibility_reason || row?.eligibilityReason) || null,
        maturity_state: maturityState,
        maturity_score: toFiniteOrNull(row?.maturity_score ?? row?.maturityScore) ?? 0,
        scan_layer: scanLayer,
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
        quote_fetched_at: row?.quote_fetched_at || row?.quoteFetchedAt || null,
        snapshot_state: normalizeText(row?.snapshot_state || row?.snapshotState) || null,
        reference_state: normalizeText(row?.reference_state || row?.referenceState) || null,
        liquidity_state: normalizeText(row?.liquidity_state || row?.liquidityState) || null,
        coverage_state: normalizeText(row?.coverage_state || row?.coverageState) || null,
        progression_status:
          normalizeText(row?.progression_status || row?.progressionStatus) || null,
        progression_blockers: Array.isArray(row?.progression_blockers)
          ? row.progression_blockers.map((value) => normalizeText(value)).filter(Boolean)
          : Array.isArray(row?.progressionBlockers)
            ? row.progressionBlockers.map((value) => normalizeText(value)).filter(Boolean)
            : [],
        catalog_status: normalizeCatalogStatus(
          row?.catalog_status ?? row?.catalogStatus,
          scanEligible ? "scannable" : "shadow"
        ),
        catalog_block_reason: normalizeText(row?.catalog_block_reason || row?.catalogBlockReason) || null,
        catalog_quality_score:
          toFiniteOrNull(row?.catalog_quality_score ?? row?.catalogQualityScore) ?? 0,
        last_market_signal_at: row?.last_market_signal_at || row?.lastMarketSignalAt || null,
        priority_set_name: normalizeText(row?.priority_set_name || row?.prioritySetName) || null,
        priority_tier: normalizePriorityTier(row?.priority_tier || row?.priorityTier, null),
        priority_rank: toIntegerOrNull(row?.priority_rank ?? row?.priorityRank, 1),
        priority_boost: toFiniteOrNull(row?.priority_boost ?? row?.priorityBoost) ?? 0,
        is_priority_item:
          row?.is_priority_item == null
            ? normalizePriorityTier(row?.priority_tier || row?.priorityTier, null) != null
            : Boolean(row.is_priority_item),
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
  const offsetStart = normalizeOffset(options.offset, 0)
  const fallbackMessage = normalizeText(options.fallbackMessage) || "market_source_catalog_select_failed"
  const rows = []
  let offset = offsetStart

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
  buildCompatibilityQuery,
  buildFallbackQuery,
  limit = 1000,
  offset = 0,
  fallbackMessage = "market_source_catalog_select_failed"
} = {}) {
  const queryBuilders = [
    buildPrimaryQuery,
    buildCompatibilityQuery,
    buildFallbackQuery
  ].filter((builder) => typeof builder === "function")
  let lastError = null

  for (let index = 0; index < queryBuilders.length; index += 1) {
    try {
      return await selectWithPagination(queryBuilders[index], { limit, offset, fallbackMessage })
    } catch (error) {
      lastError = error
      const hasNextAttempt = index < queryBuilders.length - 1
      if (!hasNextAttempt || !isMissingCandidateColumnError(error)) {
        throw error
      }
    }
  }

  throw lastError || new AppError(fallbackMessage, 500)
}

function applyCatalogStatusFilter(query, catalogStatuses = []) {
  const statuses = normalizeCatalogStatuses(catalogStatuses)
  if (!statuses.length) return query
  if (!statuses.includes("scannable")) {
    return query.in("catalog_status", statuses)
  }
  if (statuses.length === 1) {
    return query.or("catalog_status.eq.scannable,catalog_status.is.null")
  }
  return query.or(`catalog_status.in.(${statuses.join(",")}),catalog_status.is.null`)
}

async function selectCatalogRowsWithCompatibility(options = {}) {
  const rows = await selectWithCompatibilityFallback(options)
  return applyCatalogStatusCompatibility(rows)
}

function filterRowsByCatalogStatuses(rows = [], catalogStatuses = []) {
  const statuses = normalizeCatalogStatuses(catalogStatuses)
  if (!statuses.length) return Array.isArray(rows) ? rows : []
  return (Array.isArray(rows) ? rows : []).filter((row) =>
    statuses.includes(normalizeCatalogStatus(row?.catalog_status ?? row?.catalogStatus))
  )
}

async function selectDueProgressionSegment({
  categories = [],
  candidateStatuses = [],
  dueBeforeIso = null,
  limit = 1000,
  nullOnly = false
} = {}) {
  const applyDueFilter = (query) =>
    nullOnly ? query.is("last_enriched_at", null) : query.lte("last_enriched_at", dueBeforeIso)

  return selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () =>
      applyDueFilter(
        buildPrimaryProgressionQuery({
          categories,
          candidateStatuses
        })
      ),
    buildCompatibilityQuery: () =>
      applyDueFilter(
        buildCompatibilityProgressionQuery({
          categories,
          candidateStatuses
        })
      ),
    buildFallbackQuery: () =>
      applyDueFilter(
        buildFallbackProgressionQuery({
          categories
        })
      ),
    limit,
    fallbackMessage: "market_source_catalog_list_due_progression_failed"
  })
}

function toIsoStringOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function dedupeByMarketHashName(rows = []) {
  const deduped = []
  const seen = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName || seen.has(marketHashName)) continue
    seen.add(marketHashName)
    deduped.push(row)
  }
  return deduped
}

function buildPrimaryProgressionQuery({
  categories = [],
  candidateStatuses = []
} = {}) {
  let query = supabaseAdmin
    .from(TABLE)
    .select(PRIMARY_SELECT_COLUMNS)
    .eq("is_active", true)
    .eq("tradable", true)
    .in("candidate_status", candidateStatuses)
    .order("last_enriched_at", { ascending: true, nullsFirst: true })
    .order("priority_boost", { ascending: false, nullsFirst: false })
    .order("liquidity_rank", { ascending: false, nullsFirst: false })

  if (categories.length) {
    query = query.in("category", categories)
  }
  return query
}

function buildCompatibilityProgressionQuery({
  categories = [],
  candidateStatuses = []
} = {}) {
  let query = supabaseAdmin
    .from(TABLE)
    .select(COMPATIBILITY_SELECT_COLUMNS)
    .eq("is_active", true)
    .eq("tradable", true)
    .in("candidate_status", candidateStatuses)
    .order("last_enriched_at", { ascending: true, nullsFirst: true })
    .order("liquidity_rank", { ascending: false, nullsFirst: false })

  if (categories.length) {
    query = query.in("category", categories)
  }
  return query
}

function buildFallbackProgressionQuery({
  categories = []
} = {}) {
  let query = supabaseAdmin
    .from(TABLE)
    .select(LEGACY_FALLBACK_SELECT_COLUMNS)
    .eq("is_active", true)
    .eq("tradable", true)
    .order("last_enriched_at", { ascending: true, nullsFirst: true })
    .order("liquidity_rank", { ascending: false, nullsFirst: false })

  if (categories.length) {
    query = query.in("category", categories)
  }
  return query
}

exports.upsertRows = async (rows = []) => upsertInChunks(rows)

exports.listActiveTradable = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 1500)
  const offset = normalizeOffset(options.offset, 0)
  const categories = normalizeCategories(options.categories)
  const catalogStatuses = normalizeCatalogStatuses(options.catalogStatuses)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .order("priority_boost", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      query = applyCatalogStatusFilter(query, catalogStatuses)
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
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
        .select(LEGACY_FALLBACK_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    limit,
    offset,
    fallbackMessage: "market_source_catalog_list_active_failed"
  })
  return catalogStatuses.length ? filterRowsByCatalogStatuses(rows, catalogStatuses) : rows
}

exports.listScannerSource = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 1500)
  const categories = normalizeCategories(options.categories)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .or("catalog_status.eq.scannable,catalog_status.is.null")
        .order("priority_tier", { ascending: true, nullsFirst: false })
        .order("catalog_quality_score", { ascending: false, nullsFirst: false })
        .order("last_market_signal_at", { ascending: false, nullsFirst: false })
        .order("priority_boost", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
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
        .select(LEGACY_FALLBACK_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    limit,
    fallbackMessage: "market_source_catalog_list_scanner_source_failed"
  })
  return filterRowsByCatalogStatuses(rows, ["scannable"])
}

exports.listScanEligible = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 2000)
  const categories = normalizeCategories(options.categories)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .eq("scan_eligible", true)
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
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
  return filterRowsByCatalogStatuses(rows, ["scannable"])
}

exports.listCoverageSummary = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 3000)
  const categories = normalizeCategories(options.categories)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COVERAGE_SUMMARY_PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COVERAGE_SUMMARY_COMPATIBILITY_SELECT_COLUMNS)
        .eq("is_active", true)

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COVERAGE_SUMMARY_LEGACY_FALLBACK_SELECT_COLUMNS)
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
  const catalogStatuses = normalizeCatalogStatuses(options.catalogStatuses)

  return selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .in("candidate_status", candidateStatuses)
        .order("enrichment_priority", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      query = applyCatalogStatusFilter(query, catalogStatuses)
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
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
        .select(LEGACY_FALLBACK_SELECT_COLUMNS)
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
  return catalogStatuses.length ? filterRowsByCatalogStatuses(rows, catalogStatuses) : rows
}

exports.listProgressionRows = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 1200)
  const categories = normalizeCategories(options.categories)
  const candidateStatuses = normalizeCandidateStatuses(options.candidateStatuses, [
    "near_eligible",
    "eligible",
    "enriching",
    "candidate"
  ])
  return selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      return buildPrimaryProgressionQuery({
        categories,
        candidateStatuses
      })
    },
    buildCompatibilityQuery: () => {
      return buildCompatibilityProgressionQuery({
        categories,
        candidateStatuses
      })
    },
    buildFallbackQuery: () => {
      return buildFallbackProgressionQuery({
        categories
      })
    },
    limit,
    fallbackMessage: "market_source_catalog_list_progression_failed"
  })
}

exports.listDueProgressionRows = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 1200)
  const categories = normalizeCategories(options.categories)
  const candidateStatuses = normalizeCandidateStatuses(options.candidateStatuses, [
    "near_eligible",
    "eligible",
    "enriching",
    "candidate"
  ])
  const dueBeforeIso = toIsoStringOrNull(options.dueBeforeIso)

  if (!dueBeforeIso) {
    throw new AppError("market_source_catalog_due_progression_requires_due_before_iso", 500)
  }

  const nullDueRows = await selectDueProgressionSegment({
    categories,
    candidateStatuses,
    dueBeforeIso,
    limit,
    nullOnly: true
  })

  const remaining = Math.max(limit - nullDueRows.length, 0)
  if (remaining <= 0) {
    return dedupeByMarketHashName(nullDueRows).slice(0, limit)
  }

  const staleDueRows = await selectDueProgressionSegment({
    categories,
    candidateStatuses,
    dueBeforeIso,
    limit: remaining,
    nullOnly: false
  })

  return dedupeByMarketHashName([...nullDueRows, ...staleDueRows]).slice(0, limit)
}

exports.listHotScanCohort = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 600)
  const categories = normalizeCategories(options.categories)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .or("catalog_status.eq.scannable,catalog_status.is.null")
        .eq("candidate_status", "eligible")
        .eq("scan_eligible", true)
        .order("priority_tier", { ascending: true, nullsFirst: false })
        .order("priority_boost", { ascending: false, nullsFirst: false })
        .order("last_market_signal_at", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .eq("candidate_status", "eligible")
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
        .select(LEGACY_FALLBACK_SELECT_COLUMNS)
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
    fallbackMessage: "market_source_catalog_list_hot_cohort_failed"
  })
  return filterRowsByCatalogStatuses(rows, ["scannable"])
}

exports.listWarmScanCohort = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 400)
  const categories = normalizeCategories(options.categories)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .or("catalog_status.eq.scannable,catalog_status.is.null")
        .eq("candidate_status", "near_eligible")
        .order("priority_tier", { ascending: true, nullsFirst: false })
        .order("priority_boost", { ascending: false, nullsFirst: false })
        .order("last_market_signal_at", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .eq("candidate_status", "near_eligible")
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(LEGACY_FALLBACK_SELECT_COLUMNS)
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
    fallbackMessage: "market_source_catalog_list_warm_cohort_failed"
  })
  return filterRowsByCatalogStatuses(rows, ["scannable"])
}

exports.listColdScanCohort = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 300)
  const categories = normalizeCategories(options.categories)
  const rows = await selectCatalogRowsWithCompatibility({
    buildPrimaryQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(PRIMARY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .or("catalog_status.eq.scannable,catalog_status.is.null")
        .in("candidate_status", ["enriching", "candidate"])
        .order("priority_tier", { ascending: true, nullsFirst: false })
        .order("priority_boost", { ascending: false, nullsFirst: false })
        .order("last_market_signal_at", { ascending: false, nullsFirst: false })
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildCompatibilityQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(COMPATIBILITY_SELECT_COLUMNS)
        .eq("is_active", true)
        .eq("tradable", true)
        .in("candidate_status", ["enriching", "candidate"])
        .order("liquidity_rank", { ascending: false, nullsFirst: false })

      if (categories.length) {
        query = query.in("category", categories)
      }
      return query
    },
    buildFallbackQuery: () => {
      let query = supabaseAdmin
        .from(TABLE)
        .select(LEGACY_FALLBACK_SELECT_COLUMNS)
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
    fallbackMessage: "market_source_catalog_list_cold_cohort_failed"
  })
  return filterRowsByCatalogStatuses(rows, ["scannable"])
}

exports.listByMarketHashNames = async (marketHashNames = [], options = {}) => {
  const names = normalizeMarketHashNames(marketHashNames)
  if (!names.length) return []

  const categories = normalizeCategories(options.categories)
  const activeOnly = options.activeOnly !== false
  const tradableOnly = options.tradableOnly !== false
  const rows = []

  for (let index = 0; index < names.length; index += QUERY_BATCH_SIZE) {
    const chunk = names.slice(index, index + QUERY_BATCH_SIZE)
    const chunkRows = await selectCatalogRowsWithCompatibility({
      buildPrimaryQuery: () => {
        let query = supabaseAdmin
          .from(TABLE)
          .select(PRIMARY_SELECT_COLUMNS)
          .in("market_hash_name", chunk)

        if (activeOnly) {
          query = query.eq("is_active", true)
        }
        if (tradableOnly) {
          query = query.eq("tradable", true)
        }
        if (categories.length) {
          query = query.in("category", categories)
        }
        return query.order("liquidity_rank", { ascending: false, nullsFirst: false })
      },
      buildCompatibilityQuery: () => {
        let query = supabaseAdmin
          .from(TABLE)
          .select(COMPATIBILITY_SELECT_COLUMNS)
          .in("market_hash_name", chunk)

        if (activeOnly) {
          query = query.eq("is_active", true)
        }
        if (tradableOnly) {
          query = query.eq("tradable", true)
        }
        if (categories.length) {
          query = query.in("category", categories)
        }
        return query.order("liquidity_rank", { ascending: false, nullsFirst: false })
      },
      buildFallbackQuery: () => {
        let query = supabaseAdmin
          .from(TABLE)
          .select(LEGACY_FALLBACK_SELECT_COLUMNS)
          .in("market_hash_name", chunk)

        if (activeOnly) {
          query = query.eq("is_active", true)
        }
        if (tradableOnly) {
          query = query.eq("tradable", true)
        }
        if (categories.length) {
          query = query.in("category", categories)
        }
        return query.order("liquidity_rank", { ascending: false, nullsFirst: false })
      },
      limit: chunk.length,
      fallbackMessage: "market_source_catalog_list_by_names_failed"
    })
    rows.push(...chunkRows)
  }

  return rows
}

exports.__testables = {
  normalizeRows,
  normalizeCandidateStatus,
  normalizeCandidateStatuses,
  normalizeCatalogStatuses,
  applyCatalogStatusCompatibility,
  toIntegerOrNull,
  toIntegerOrDefault,
  formatSupabaseError
}

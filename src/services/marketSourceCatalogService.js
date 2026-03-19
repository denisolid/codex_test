const {
  arbitrageDefaultUniverseLimit,
  arbitrageScannerUniverseTargetSize,
  arbitrageSourceCatalogLimit,
  arbitrageSourceCatalogRefreshMinutes,
  marketSnapshotTtlMinutes
} = require("../config/env")
const sourceCatalogSeed = require("../config/marketSourceCatalogSeed")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../repositories/marketUniverseRepository")
const marketSnapshotRepo = require("../repositories/marketSnapshotRepository")
const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const skinRepo = require("../repositories/skinRepository")
const catalogPriorityCoverageService = require("./catalogPriorityCoverageService")

const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove"
})

const SCANNER_SCOPE_CATEGORIES = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE,
  ITEM_CATEGORIES.KNIFE,
  ITEM_CATEGORIES.GLOVE
])
const SCANNER_SCOPE_CATEGORY_SET = new Set(SCANNER_SCOPE_CATEGORIES)
const CATALOG_CANDIDATE_STATUS = Object.freeze({
  CANDIDATE: "candidate",
  ENRICHING: "enriching",
  NEAR_ELIGIBLE: "near_eligible",
  ELIGIBLE: "eligible",
  REJECTED: "rejected"
})
const CATALOG_MATURITY_STATE = Object.freeze({
  COLD: "cold",
  ENRICHING: "enriching",
  NEAR_ELIGIBLE: "near_eligible",
  ELIGIBLE: "eligible"
})
const ACTIVE_CANDIDATE_STATUSES = Object.freeze([
  CATALOG_CANDIDATE_STATUS.ELIGIBLE,
  CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE,
  CATALOG_CANDIDATE_STATUS.ENRICHING,
  CATALOG_CANDIDATE_STATUS.CANDIDATE
])
const CATALOG_CANDIDATE_STATUS_SET = new Set(
  Object.values(CATALOG_CANDIDATE_STATUS)
)
const CATALOG_MATURITY_STATE_SET = new Set(Object.values(CATALOG_MATURITY_STATE))

const DEFAULT_UNIVERSE_LIMIT = 3000
const DEFAULT_SOURCE_CATALOG_TARGET = 5000
const DEFAULT_UNIVERSE_TARGET = Math.max(
  Number(
    arbitrageScannerUniverseTargetSize ||
      arbitrageDefaultUniverseLimit ||
      DEFAULT_UNIVERSE_LIMIT
  ),
  100
)
const SOURCE_CATALOG_LIMIT = Math.max(
  Number(arbitrageSourceCatalogLimit || DEFAULT_SOURCE_CATALOG_TARGET),
  DEFAULT_SOURCE_CATALOG_TARGET
)
const SOURCE_CATALOG_REFRESH_MS =
  Math.max(Number(arbitrageSourceCatalogRefreshMinutes || 60), 5) * 60 * 1000
const SOURCE_CATALOG_ERROR_RETRY_MS = Math.max(
  Math.min(SOURCE_CATALOG_REFRESH_MS, 15 * 60 * 1000),
  5 * 60 * 1000
)
const SNAPSHOT_TTL_MS = Math.max(Number(marketSnapshotTtlMinutes || 30), 5) * 60 * 1000
const MAJOR_CAPSULE_EVENT_PATTERN = /\b(katowice|cologne|atlanta|krakow|boston|london|berlin|stockholm|antwerp|rio|paris|copenhagen|major|rmr)\b/i
const CAPSULE_EVENT_SIGNAL_PATTERN = /\b(esl|blast|pgl|dreamhack|iem|cluj|funspark|faceit|challengers|legends|contenders|champions|team)\b/i
const CAPSULE_YEAR_PATTERN = /\b20(1[3-9]|2[0-9])\b/
const WEAR_PATTERN = /\((factory new|minimal wear|field-tested|well-worn|battle-scarred)\)$/i
const WEAPON_PREFIX_ALLOWLIST = Object.freeze(
  new Set([
    "AK-47",
    "M4A1-S",
    "M4A4",
    "AWP",
    "USP-S",
    "Glock-18",
    "Desert Eagle",
    "P250",
    "Five-SeveN",
    "Tec-9",
    "CZ75-Auto",
    "Dual Berettas",
    "R8 Revolver",
    "MP9",
    "MP7",
    "MP5-SD",
    "MAC-10",
    "UMP-45",
    "P90",
    "PP-Bizon",
    "FAMAS",
    "Galil AR",
    "SG 553",
    "AUG",
    "SSG 08",
    "SCAR-20",
    "G3SG1",
    "XM1014",
    "Nova",
    "MAG-7",
    "Sawed-Off",
    "M249",
    "Negev"
  ])
)
const HIGH_LIQUIDITY_WEAPON_PREFIXES = Object.freeze(
  new Set([
    "AK-47",
    "AWP",
    "M4A1-S",
    "M4A4",
    "USP-S",
    "Glock-18",
    "Desert Eagle",
    "P250",
    "Five-SeveN",
    "MP9",
    "MAC-10",
    "FAMAS",
    "Galil AR",
    "AUG",
    "SG 553",
    "P90"
  ])
)
const LOW_VALUE_WEAPON_PATTERNS = Object.freeze([
  /\|\s*Sand Dune/i,
  /\|\s*Safari Mesh/i,
  /\|\s*Boreal Forest/i,
  /\|\s*Urban DDPAT/i,
  /\|\s*Forest DDPAT/i,
  /\|\s*Scorched/i,
  /\|\s*Contractor/i,
  /\|\s*Army Sheen/i,
  /\|\s*Groundwater/i
])
const EXCLUDED_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i,
  /^patch\s*\|/i,
  /^music kit\s*\|/i,
  /^name tag$/i,
  / pass$/i,
  /\bviewer pass\b/i,
  /\bx-ray p250 package\b/i
])
const LIQUID_WEAPON_KEYWORDS = Object.freeze([
  "asiimov",
  "printstream",
  "fade",
  "doppler",
  "gamma",
  "vulcan",
  "redline",
  "neo-noir",
  "bloodsport",
  "case hardened",
  "tiger tooth",
  "slaughter",
  "marble fade"
])
const SOURCE_CATALOG_QUOTA_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    min: 3900,
    target: 4400,
    max: 8400
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    min: 260,
    target: 350,
    max: 1000
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    min: 180,
    target: 250,
    max: 800
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    min: 0,
    target: 0,
    max: 0
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    min: 0,
    target: 0,
    max: 0
  })
})

const SOURCE_QUALITY_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    minReferencePrice: 2,
    minVolume7d: 35,
    minMarketCoverage: 2
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    minReferencePrice: 2,
    minVolume7d: 70,
    minMarketCoverage: 2
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    minReferencePrice: 2,
    minVolume7d: 35,
    minMarketCoverage: 2
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    minReferencePrice: 20,
    minVolume7d: 6,
    minMarketCoverage: 2
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    minReferencePrice: 20,
    minVolume7d: 6,
    minMarketCoverage: 2
  })
})
const SOURCE_CANDIDATE_HARD_FLOOR = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 0.45,
  [ITEM_CATEGORIES.CASE]: 0.3,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 0.3,
  [ITEM_CATEGORIES.KNIFE]: 2,
  [ITEM_CATEGORIES.GLOVE]: 2
})

const CATEGORY_QUOTA_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    min: 2200,
    target: 2400,
    max: 2800
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    min: 250,
    target: 350,
    max: 650
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    min: 180,
    target: 250,
    max: 450
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    min: 0,
    target: 0,
    max: 0
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    min: 0,
    target: 0,
    max: 0
  })
})

const CATEGORY_PRIORITY = SCANNER_SCOPE_CATEGORIES
const CATEGORY_QUOTA_BASE_TOTAL = Object.values(CATEGORY_QUOTA_RULES).reduce(
  (sum, value) => sum + Number(value?.target || 0),
  0
)
const SOURCE_CATALOG_QUOTA_BASE_TOTAL = Object.values(SOURCE_CATALOG_QUOTA_RULES).reduce(
  (sum, value) => sum + Number(value?.target || 0),
  0
)

const CATEGORY_DEFAULT_COUNTER = Object.freeze({
  total: 0,
  cold: 0,
  candidate: 0,
  enriching: 0,
  nearEligible: 0,
  eligible: 0,
  rejected: 0,
  excludedLowValueItems: 0,
  excludedLowLiquidityItems: 0,
  excludedWeakMarketCoverageItems: 0,
  excludedStaleItems: 0,
  excludedMissingReferenceItems: 0,
  missingSnapshot: 0,
  missingReference: 0,
  missingMarketCoverage: 0
})

const BASE_EXCLUDED_REASON_COUNTER = Object.freeze({
  excludedLowValueItems: 0,
  excludedLowLiquidityItems: 0,
  excludedWeakMarketCoverageItems: 0,
  excludedStaleItems: 0,
  excludedMissingReferenceItems: 0
})
const CATALOG_PROMOTION_REASON_KEYS = Object.freeze([
  "missing_reference",
  "freshness_not_usable",
  "partial_market_support_missing",
  "market_coverage_insufficient",
  "missing_snapshot",
  "missing_liquidity_context",
  "low_volume_context",
  "structural_reason",
  "candidate_not_ready"
])
const CATALOG_FRESHNESS_STATES = Object.freeze({
  FRESH: "fresh",
  AGING: "aging",
  STALE: "stale",
  MISSING: "missing"
})
const CATALOG_FRESHNESS_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    freshMaxMinutes: 45,
    agingMaxMinutes: 120
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    freshMaxMinutes: 60,
    agingMaxMinutes: 180
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    freshMaxMinutes: 90,
    agingMaxMinutes: 240
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240
  })
})
const SNAPSHOT_DIAGNOSTIC_STATES = Object.freeze({
  MISSING: "missing_snapshot",
  PARTIAL: "partial_snapshot",
  STALE: "stale_snapshot",
  READY: "snapshot_ready"
})
const REFERENCE_DIAGNOSTIC_STATES = Object.freeze({
  MISSING: "missing_reference",
  SNAPSHOT: "snapshot_reference",
  QUOTE: "quote_reference"
})
const LIQUIDITY_DIAGNOSTIC_STATES = Object.freeze({
  MISSING: "missing_liquidity",
  PARTIAL: "partial_liquidity",
  READY: "liquidity_ready"
})
const COVERAGE_DIAGNOSTIC_STATES = Object.freeze({
  MISSING: "missing_coverage",
  INSUFFICIENT: "insufficient_coverage",
  READY: "coverage_ready"
})
const PROGRESSION_DIAGNOSTIC_STATUS = Object.freeze({
  ELIGIBLE: "eligible",
  BLOCKED_ELIGIBLE: "blocked_from_eligible",
  BLOCKED_NEAR_ELIGIBLE: "blocked_from_near_eligible",
  REJECTED: "rejected"
})
const CATALOG_STATUS = Object.freeze({
  SCANNABLE: "scannable",
  SHADOW: "shadow",
  BLOCKED: "blocked"
})
const CATALOG_STATUS_SET = new Set(Object.values(CATALOG_STATUS))
const CATALOG_BLOCK_REASONS = Object.freeze({
  INVALID_CATALOG_REASON: "invalid_catalog_reason",
  BELOW_MIN_COST_FLOOR: "below_min_cost_floor",
  UNUSABLE_MARKET_COVERAGE: "unusable_market_coverage"
})
const CATALOG_SHADOW_REASONS = Object.freeze({
  STALE_ONLY_SIGNALS: "stale_only_signals",
  WEAK_MARKET_COVERAGE: "weak_market_coverage",
  INCOMPLETE_REFERENCE_PRICING: "incomplete_reference_pricing"
})
const PRIORITY_TIERS = Object.freeze({
  TIER_A: "tier_a",
  TIER_B: "tier_b"
})
const PRIORITY_TIER_SET = new Set(Object.values(PRIORITY_TIERS))
const MIN_SCAN_COST_USD = 2

const BASE_INGEST_EXCLUDED_REASON_COUNTER = Object.freeze({
  excludedDuplicate: 0,
  excludedOutOfScopeCategory: 0,
  excludedNamePattern: 0,
  excludedLowValueName: 0,
  excludedUnsupportedWeaponPrefix: 0,
  excludedMissingWear: 0,
  excludedWeakCaseCandidate: 0,
  excludedWeakCapsuleCandidate: 0
})

const sourceCatalogState = {
  inFlight: null,
  lastPreparedAt: 0,
  lastDiagnostics: null,
  lastSuccessfulDiagnostics: null
}

function isScannerScopeCategory(category = "") {
  return SCANNER_SCOPE_CATEGORY_SET.has(normalizeText(category).toLowerCase())
}

function buildEmptyCategoryCounter() {
  return Object.fromEntries(
    SCANNER_SCOPE_CATEGORIES.map((category) => [category, { ...CATEGORY_DEFAULT_COUNTER }])
  )
}

function buildCategoryNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(SCANNER_SCOPE_CATEGORIES.map((category) => [category, initial]))
}

function buildStatusNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(
    Object.values(CATALOG_CANDIDATE_STATUS).map((status) => [status, initial])
  )
}

function buildCatalogStatusNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(Object.values(CATALOG_STATUS).map((status) => [status, initial]))
}

function buildCatalogReasonMap(initialValue = 0, reasons = {}) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(
    Object.values(reasons).map((reason) => [reason, initial])
  )
}

function buildMaturityNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(
    Object.values(CATALOG_MATURITY_STATE).map((state) => [state, initial])
  )
}

function buildMaturityByCategoryMap(initialValue = 0) {
  return Object.fromEntries(
    SCANNER_SCOPE_CATEGORIES.map((category) => [category, buildMaturityNumberMap(initialValue)])
  )
}

function buildPromotionReasonMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(CATALOG_PROMOTION_REASON_KEYS.map((reason) => [reason, initial]))
}

function buildPromotionReasonByCategoryMap(initialValue = 0) {
  return Object.fromEntries(
    SCANNER_SCOPE_CATEGORIES.map((category) => [category, buildPromotionReasonMap(initialValue)])
  )
}

function buildFreshnessNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(
    Object.values(CATALOG_FRESHNESS_STATES).map((state) => [state, initial])
  )
}

function buildFreshnessByCategoryMap(initialValue = 0) {
  return Object.fromEntries(
    SCANNER_SCOPE_CATEGORIES.map((category) => [category, buildFreshnessNumberMap(initialValue)])
  )
}

function normalizeCandidateStatus(value, fallback = CATALOG_CANDIDATE_STATUS.CANDIDATE) {
  const text = normalizeText(value).toLowerCase()
  if (CATALOG_CANDIDATE_STATUS_SET.has(text)) return text
  const fallbackText = normalizeText(fallback).toLowerCase()
  return CATALOG_CANDIDATE_STATUS_SET.has(fallbackText)
    ? fallbackText
    : CATALOG_CANDIDATE_STATUS.CANDIDATE
}

function normalizeMaturityState(value, fallback = CATALOG_MATURITY_STATE.COLD) {
  const text = normalizeText(value).toLowerCase()
  if (CATALOG_MATURITY_STATE_SET.has(text)) return text
  const fallbackText = normalizeText(fallback).toLowerCase()
  return CATALOG_MATURITY_STATE_SET.has(fallbackText)
    ? fallbackText
    : CATALOG_MATURITY_STATE.COLD
}

function normalizeCatalogStatus(value, fallback = CATALOG_STATUS.SHADOW) {
  const text = normalizeText(value).toLowerCase()
  if (CATALOG_STATUS_SET.has(text)) return text
  const fallbackText = normalizeText(fallback).toLowerCase()
  return CATALOG_STATUS_SET.has(fallbackText) ? fallbackText : CATALOG_STATUS.SHADOW
}

function normalizePriorityTier(value, fallback = null) {
  const text = normalizeText(value).toLowerCase()
  if (PRIORITY_TIER_SET.has(text)) return text
  const fallbackText = normalizeText(fallback).toLowerCase()
  return PRIORITY_TIER_SET.has(fallbackText) ? fallbackText : null
}

function resolveScanLayerForMaturityState(maturityState = CATALOG_MATURITY_STATE.COLD) {
  const normalized = normalizeMaturityState(maturityState)
  if (normalized === CATALOG_MATURITY_STATE.ELIGIBLE) return "hot"
  if (
    normalized === CATALOG_MATURITY_STATE.NEAR_ELIGIBLE ||
    normalized === CATALOG_MATURITY_STATE.ENRICHING
  ) {
    return "warm"
  }
  return "cold"
}

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return parsed > 0 ? parsed : null
}

function countTrueValues(values = []) {
  return (Array.isArray(values) ? values : []).reduce(
    (sum, value) => sum + Number(Boolean(value)),
    0
  )
}

function uniqueTextList(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )
}

function toIsoStringOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const parsed = new Date(text).getTime()
  if (!Number.isFinite(parsed)) return null
  return new Date(parsed).toISOString()
}

function normalizeIntegerOrNull(value, min = 0) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return Math.max(Math.round(parsed), min)
}

function normalizeIntegerOrDefault(value, defaultValue = 0, min = 0) {
  const parsed = normalizeIntegerOrNull(value, min)
  if (parsed == null) return Math.max(Math.round(Number(defaultValue || 0)), min)
  return parsed
}

function normalizeNumberForCompare(value, decimals = 4) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return Number(parsed.toFixed(decimals))
}

function normalizeTextListForCompare(values = []) {
  return uniqueTextList(values).sort((a, b) => a.localeCompare(b))
}

function buildCatalogComparableState(row = {}) {
  return {
    market_hash_name: normalizeText(row?.market_hash_name || row?.marketHashName) || null,
    item_name: normalizeText(row?.item_name || row?.itemName) || null,
    category: normalizeCategory(row?.category, row?.market_hash_name || row?.marketHashName),
    subcategory: normalizeText(row?.subcategory) || null,
    tradable: row?.tradable == null ? true : Boolean(row.tradable),
    scan_eligible: row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible),
    candidate_status: normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus),
    missing_snapshot:
      row?.missing_snapshot == null ? Boolean(row?.missingSnapshot) : Boolean(row.missing_snapshot),
    missing_reference:
      row?.missing_reference == null ? Boolean(row?.missingReference) : Boolean(row.missing_reference),
    missing_market_coverage:
      row?.missing_market_coverage == null
        ? Boolean(row?.missingMarketCoverage)
        : Boolean(row.missing_market_coverage),
    enrichment_priority:
      normalizeNumberForCompare(row?.enrichment_priority ?? row?.enrichmentPriority, 2) ?? 0,
    eligibility_reason: normalizeText(row?.eligibility_reason || row?.eligibilityReason) || null,
    maturity_state: normalizeMaturityState(row?.maturity_state ?? row?.maturityState),
    maturity_score: normalizeNumberForCompare(row?.maturity_score ?? row?.maturityScore, 2) ?? 0,
    scan_layer: normalizeText(row?.scan_layer || row?.scanLayer) || null,
    reference_price: normalizeNumberForCompare(row?.reference_price ?? row?.referencePrice, 4),
    market_coverage_count: normalizeIntegerOrDefault(
      row?.market_coverage_count ?? row?.marketCoverageCount,
      0,
      0
    ),
    liquidity_rank: normalizeNumberForCompare(row?.liquidity_rank ?? row?.liquidityRank, 2),
    volume_7d: normalizeIntegerOrNull(row?.volume_7d ?? row?.volume7d, 0),
    snapshot_stale: row?.snapshot_stale == null ? Boolean(row?.snapshotStale) : Boolean(row.snapshot_stale),
    snapshot_captured_at: toIsoStringOrNull(row?.snapshot_captured_at || row?.snapshotCapturedAt),
    quote_fetched_at: toIsoStringOrNull(row?.quote_fetched_at || row?.quoteFetchedAt),
    snapshot_state: normalizeText(row?.snapshot_state || row?.snapshotState) || null,
    reference_state: normalizeText(row?.reference_state || row?.referenceState) || null,
    liquidity_state: normalizeText(row?.liquidity_state || row?.liquidityState) || null,
    coverage_state: normalizeText(row?.coverage_state || row?.coverageState) || null,
    progression_status: normalizeText(row?.progression_status || row?.progressionStatus) || null,
    progression_blockers: normalizeTextListForCompare(
      Array.isArray(row?.progression_blockers)
        ? row.progression_blockers
        : Array.isArray(row?.progressionBlockers)
          ? row.progressionBlockers
          : []
    ),
    catalog_status: normalizeCatalogStatus(row?.catalog_status ?? row?.catalogStatus),
    catalog_block_reason: normalizeText(row?.catalog_block_reason || row?.catalogBlockReason) || null,
    catalog_quality_score:
      normalizeNumberForCompare(row?.catalog_quality_score ?? row?.catalogQualityScore, 2) ?? 0,
    last_market_signal_at: toIsoStringOrNull(row?.last_market_signal_at || row?.lastMarketSignalAt),
    priority_set_name: normalizeText(row?.priority_set_name || row?.prioritySetName) || null,
    priority_tier: normalizePriorityTier(row?.priority_tier || row?.priorityTier, null),
    priority_rank: normalizeIntegerOrNull(row?.priority_rank ?? row?.priorityRank, 1),
    priority_boost: normalizeNumberForCompare(row?.priority_boost ?? row?.priorityBoost, 2) ?? 0,
    is_priority_item: row?.is_priority_item == null ? Boolean(row?.isPriorityItem) : Boolean(row.is_priority_item),
    invalid_reason: normalizeText(row?.invalid_reason || row?.invalidReason) || null,
    source_tag: normalizeText(row?.source_tag || row?.sourceTag) || "curated_seed",
    is_active: row?.is_active == null ? (row?.isActive == null ? true : Boolean(row.isActive)) : Boolean(row.is_active)
  }
}

function hasCatalogRowChanges(previousRow = {}, nextRow = {}) {
  const previous = buildCatalogComparableState(previousRow)
  const next = buildCatalogComparableState(nextRow)
  const fields = [
    "market_hash_name",
    "item_name",
    "category",
    "subcategory",
    "tradable",
    "scan_eligible",
    "candidate_status",
    "missing_snapshot",
    "missing_reference",
    "missing_market_coverage",
    "enrichment_priority",
    "eligibility_reason",
    "maturity_state",
    "maturity_score",
    "scan_layer",
    "reference_price",
    "market_coverage_count",
    "liquidity_rank",
    "volume_7d",
    "snapshot_stale",
    "snapshot_captured_at",
    "quote_fetched_at",
    "snapshot_state",
    "reference_state",
    "liquidity_state",
    "coverage_state",
    "progression_status",
    "catalog_status",
    "catalog_block_reason",
    "catalog_quality_score",
    "last_market_signal_at",
    "priority_set_name",
    "priority_tier",
    "priority_rank",
    "priority_boost",
    "is_priority_item",
    "invalid_reason",
    "source_tag",
    "is_active"
  ]

  for (const field of fields) {
    if (previous[field] !== next[field]) return true
  }

  const previousBlockers = Array.isArray(previous.progression_blockers) ? previous.progression_blockers : []
  const nextBlockers = Array.isArray(next.progression_blockers) ? next.progression_blockers : []
  if (previousBlockers.length !== nextBlockers.length) return true
  for (let index = 0; index < previousBlockers.length; index += 1) {
    if (previousBlockers[index] !== nextBlockers[index]) return true
  }

  return false
}

function normalizeUniverseRowForCompare(row = {}, fallbackRank = 1) {
  const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
  return {
    market_hash_name: marketHashName || null,
    item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || null,
    category: normalizeCategory(row?.category, marketHashName),
    subcategory: normalizeText(row?.subcategory) || null,
    liquidity_rank: normalizeIntegerOrDefault(row?.liquidity_rank ?? row?.liquidityRank, fallbackRank, 1),
    is_active: row?.is_active == null ? true : Boolean(row.is_active)
  }
}

function isSameUniverseRows(existingRows = [], nextRows = []) {
  const existing = (Array.isArray(existingRows) ? existingRows : [])
    .map((row, index) => normalizeUniverseRowForCompare(row, index + 1))
    .sort(
      (a, b) =>
        Number(a.liquidity_rank || 0) - Number(b.liquidity_rank || 0) ||
        String(a.market_hash_name || "").localeCompare(String(b.market_hash_name || ""))
    )
  const next = (Array.isArray(nextRows) ? nextRows : [])
    .map((row, index) => normalizeUniverseRowForCompare(row, index + 1))
    .sort(
      (a, b) =>
        Number(a.liquidity_rank || 0) - Number(b.liquidity_rank || 0) ||
        String(a.market_hash_name || "").localeCompare(String(b.market_hash_name || ""))
    )

  if (existing.length !== next.length) return false
  for (let index = 0; index < existing.length; index += 1) {
    const prev = existing[index]
    const curr = next[index]
    if (
      prev.market_hash_name !== curr.market_hash_name ||
      prev.item_name !== curr.item_name ||
      prev.category !== curr.category ||
      prev.subcategory !== curr.subcategory ||
      prev.liquidity_rank !== curr.liquidity_rank ||
      prev.is_active !== curr.is_active
    ) {
      return false
    }
  }
  return true
}

function resolveAgeMinutes(value) {
  const iso = toIsoStringOrNull(value)
  if (!iso) return null
  const ageMinutes = (Date.now() - new Date(iso).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return Number(ageMinutes.toFixed(2))
}

function getCatalogFreshnessRules(category = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeCategory(category)
  return (
    CATALOG_FRESHNESS_RULES[normalized] ||
    CATALOG_FRESHNESS_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function resolveCatalogFreshnessState(ageMinutes = null, rules = {}) {
  const safeAge = toFiniteOrNull(ageMinutes)
  if (safeAge == null || safeAge < 0) return CATALOG_FRESHNESS_STATES.MISSING
  if (safeAge <= Number(rules.freshMaxMinutes || 0)) return CATALOG_FRESHNESS_STATES.FRESH
  if (safeAge <= Number(rules.agingMaxMinutes || 0)) return CATALOG_FRESHNESS_STATES.AGING
  return CATALOG_FRESHNESS_STATES.STALE
}

function resolveCatalogFreshnessContext({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  snapshotCapturedAt = null,
  quoteFetchedAt = null,
  snapshotStale = true,
  hasSnapshot = null
} = {}) {
  const rules = getCatalogFreshnessRules(category)
  const snapshotAgeMinutes = resolveAgeMinutes(snapshotCapturedAt)
  const quoteAgeMinutes = resolveAgeMinutes(quoteFetchedAt)
  const hasSnapshotData =
    hasSnapshot == null ? Boolean(snapshotCapturedAt) : Boolean(hasSnapshot)
  const snapshotState =
    snapshotAgeMinutes == null
      ? hasSnapshotData
        ? snapshotStale
          ? CATALOG_FRESHNESS_STATES.STALE
          : CATALOG_FRESHNESS_STATES.FRESH
        : CATALOG_FRESHNESS_STATES.MISSING
      : resolveCatalogFreshnessState(snapshotAgeMinutes, rules)
  const quoteState = resolveCatalogFreshnessState(quoteAgeMinutes, rules)
  const state =
    snapshotState === CATALOG_FRESHNESS_STATES.FRESH || quoteState === CATALOG_FRESHNESS_STATES.FRESH
      ? CATALOG_FRESHNESS_STATES.FRESH
      : snapshotState === CATALOG_FRESHNESS_STATES.AGING ||
          quoteState === CATALOG_FRESHNESS_STATES.AGING
        ? CATALOG_FRESHNESS_STATES.AGING
        : snapshotState === CATALOG_FRESHNESS_STATES.STALE ||
            quoteState === CATALOG_FRESHNESS_STATES.STALE
          ? CATALOG_FRESHNESS_STATES.STALE
          : CATALOG_FRESHNESS_STATES.MISSING

  return {
    state,
    snapshotState,
    quoteState,
    snapshotAgeMinutes,
    quoteAgeMinutes,
    hasSnapshotData,
    hasQuoteFreshness: quoteAgeMinutes != null,
    usable:
      snapshotState === CATALOG_FRESHNESS_STATES.FRESH ||
      snapshotState === CATALOG_FRESHNESS_STATES.AGING ||
      quoteState === CATALOG_FRESHNESS_STATES.FRESH ||
      quoteState === CATALOG_FRESHNESS_STATES.AGING,
    strong:
      snapshotState === CATALOG_FRESHNESS_STATES.FRESH ||
      quoteState === CATALOG_FRESHNESS_STATES.FRESH
  }
}

function resolveCoverageDiagnosticState(coverageCount = 0, minCoverage = 2) {
  const normalizedCoverage = Math.max(Number(coverageCount || 0), 0)
  if (normalizedCoverage <= 0) return COVERAGE_DIAGNOSTIC_STATES.MISSING
  if (normalizedCoverage < Math.max(Number(minCoverage || 0), 1)) {
    return COVERAGE_DIAGNOSTIC_STATES.INSUFFICIENT
  }
  return COVERAGE_DIAGNOSTIC_STATES.READY
}

function resolveReferenceDiagnosticState({
  referencePrice = null,
  snapshotHasPriceSignal = false
} = {}) {
  if (toPositiveOrNull(referencePrice) == null) {
    return REFERENCE_DIAGNOSTIC_STATES.MISSING
  }
  return snapshotHasPriceSignal
    ? REFERENCE_DIAGNOSTIC_STATES.SNAPSHOT
    : REFERENCE_DIAGNOSTIC_STATES.QUOTE
}

function resolveLiquidityDiagnosticState({
  hasLiquidityContext = false,
  sufficientVolume = false,
  reasonableVolume = false,
  hasUtilitySignal = false,
  liquidityRank = 0
} = {}) {
  if (sufficientVolume) return LIQUIDITY_DIAGNOSTIC_STATES.READY
  if (hasLiquidityContext || reasonableVolume || hasUtilitySignal || Number(liquidityRank || 0) > 0) {
    return LIQUIDITY_DIAGNOSTIC_STATES.PARTIAL
  }
  return LIQUIDITY_DIAGNOSTIC_STATES.MISSING
}

function resolveSnapshotDiagnosticState({
  snapshotCapturedAt = null,
  snapshotStale = false,
  snapshotComplete = false
} = {}) {
  if (!normalizeText(snapshotCapturedAt)) return SNAPSHOT_DIAGNOSTIC_STATES.MISSING
  if (snapshotStale) return SNAPSHOT_DIAGNOSTIC_STATES.STALE
  if (!snapshotComplete) return SNAPSHOT_DIAGNOSTIC_STATES.PARTIAL
  return SNAPSHOT_DIAGNOSTIC_STATES.READY
}

function buildProgressionBlockers({
  candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE,
  rejectedReason = "",
  progress = {},
  snapshotState = SNAPSHOT_DIAGNOSTIC_STATES.MISSING,
  referenceState = REFERENCE_DIAGNOSTIC_STATES.MISSING,
  liquidityState = LIQUIDITY_DIAGNOSTIC_STATES.MISSING,
  coverageState = COVERAGE_DIAGNOSTIC_STATES.MISSING,
  snapshotIncomplete = false
} = {}) {
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE) return []

  const blockers = []
  if (rejectedReason || progress.hasStructuralReason) {
    blockers.push("anti_fake_guard")
  }
  if (snapshotState === SNAPSHOT_DIAGNOSTIC_STATES.MISSING) {
    blockers.push(SNAPSHOT_DIAGNOSTIC_STATES.MISSING)
  } else if (snapshotState === SNAPSHOT_DIAGNOSTIC_STATES.STALE) {
    blockers.push(SNAPSHOT_DIAGNOSTIC_STATES.STALE)
  }
  if (snapshotIncomplete) {
    blockers.push(SNAPSHOT_DIAGNOSTIC_STATES.PARTIAL)
  }
  if (referenceState === REFERENCE_DIAGNOSTIC_STATES.MISSING) {
    blockers.push(REFERENCE_DIAGNOSTIC_STATES.MISSING)
  }
  if (liquidityState === LIQUIDITY_DIAGNOSTIC_STATES.MISSING) {
    blockers.push(LIQUIDITY_DIAGNOSTIC_STATES.MISSING)
  } else if (liquidityState === LIQUIDITY_DIAGNOSTIC_STATES.PARTIAL) {
    blockers.push(LIQUIDITY_DIAGNOSTIC_STATES.PARTIAL)
  }
  if (coverageState !== COVERAGE_DIAGNOSTIC_STATES.READY) {
    blockers.push(COVERAGE_DIAGNOSTIC_STATES.INSUFFICIENT)
  }

  const targetBlockers =
    candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
      ? progress.eligibleBlockers
      : progress.nearEligibleBlockers
  if (Array.isArray(targetBlockers) && targetBlockers.includes("candidate_not_ready")) {
    blockers.push("candidate_not_ready")
  }

  return uniqueTextList(blockers)
}

function computeCatalogProgressContext({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  snapshotCapturedAt = null,
  quoteFetchedAt = null,
  snapshotStale = true,
  hasSnapshot = null,
  liquidityRank = 0,
  eligibilityReason = "",
  snapshotHasPriceSignal = false,
  snapshotHasLiquiditySignal = false
} = {}) {
  const normalizedCategory = normalizeCategory(category)
  const rules =
    SOURCE_QUALITY_RULES[normalizedCategory] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const minCoverage = Math.max(Number(rules.minMarketCoverage || 2), 1)
  const minVolume = Math.max(Number(rules.minVolume7d || 1), 1)
  const safeReferencePrice = toPositiveOrNull(referencePrice)
  const safeVolume7d = toPositiveOrNull(volume7d)
  const coverageCount = Math.max(Number(marketCoverageCount || 0), 0)
  const inferredSnapshotPresence =
    hasSnapshot == null
      ? Boolean(snapshotCapturedAt) || (safeReferencePrice != null && snapshotStale === false)
      : Boolean(hasSnapshot)
  const freshness = resolveCatalogFreshnessContext({
    category: normalizedCategory,
    snapshotCapturedAt,
    quoteFetchedAt,
    snapshotStale,
    hasSnapshot: inferredSnapshotPresence
  })
  const hasReference = safeReferencePrice != null
  const isWeaponSkin = normalizedCategory === ITEM_CATEGORIES.WEAPON_SKIN
  const hasAnyCoverage = coverageCount >= 1
  const sufficientCoverage = coverageCount >= minCoverage
  const partialCoverage = coverageCount >= Math.max(1, Math.min(minCoverage - 1, minCoverage))
  const hasLiquidityContext = safeVolume7d != null
  const sufficientVolume = hasLiquidityContext && safeVolume7d >= minVolume
  const reasonableVolume = hasLiquidityContext && safeVolume7d >= Math.max(minVolume * 0.55, 18)
  const meaningfulReference =
    hasReference &&
    safeReferencePrice >= Math.max(Number(rules.minReferencePrice || 1) * 0.9, 0.75)
  const hasUtilitySignal = Number(liquidityRank || 0) >= 28
  const partialMarketSupport = isWeaponSkin ? hasAnyCoverage : partialCoverage || reasonableVolume
  const hasStructuralReason = /\brejected|hard|outofscope|namepattern|unsupported\b/i.test(
    String(eligibilityReason || "")
  )
  const weaponSkinCoverageLedNearEligible =
    isWeaponSkin &&
    !hasReference &&
    !hasStructuralReason &&
    hasAnyCoverage &&
    freshness.usable &&
    (coverageCount >= minCoverage || hasLiquidityContext || hasUtilitySignal)
  const nearEligibleSupportCount = countTrueValues([
    hasReference,
    freshness.usable,
    partialMarketSupport,
    inferredSnapshotPresence || hasAnyCoverage || freshness.hasQuoteFreshness,
    hasLiquidityContext || meaningfulReference || hasUtilitySignal
  ])
  const eligibleSupportCount = countTrueValues([
    hasReference,
    freshness.usable,
    sufficientCoverage,
    sufficientVolume,
    inferredSnapshotPresence
  ])
  const nearEligibleBlockers = []
  const eligibleBlockers = []

  if (hasStructuralReason) {
    nearEligibleBlockers.push("structural_reason")
    eligibleBlockers.push("structural_reason")
  }
  if (!hasReference && !weaponSkinCoverageLedNearEligible) {
    nearEligibleBlockers.push("missing_reference")
  }
  if (!hasReference) {
    eligibleBlockers.push("missing_reference")
  }
  if (!freshness.usable) {
    nearEligibleBlockers.push("freshness_not_usable")
    eligibleBlockers.push("freshness_not_usable")
  }
  if (!partialMarketSupport) {
    nearEligibleBlockers.push("partial_market_support_missing")
  }
  if (isWeaponSkin && !hasAnyCoverage) {
    nearEligibleBlockers.push("market_coverage_insufficient")
  }
  if (!sufficientCoverage) {
    eligibleBlockers.push("market_coverage_insufficient")
  }
  if (!inferredSnapshotPresence && coverageCount === 0 && !freshness.hasQuoteFreshness) {
    nearEligibleBlockers.push("missing_snapshot")
  }
  if (!inferredSnapshotPresence) {
    eligibleBlockers.push("missing_snapshot")
  }
  if (!hasLiquidityContext) {
    if (coverageCount === 0 && !freshness.hasQuoteFreshness) {
      nearEligibleBlockers.push("missing_liquidity_context")
    }
    eligibleBlockers.push("missing_liquidity_context")
  } else if (!sufficientVolume) {
    eligibleBlockers.push("low_volume_context")
  }
  if (!nearEligibleBlockers.length && nearEligibleSupportCount < 3) {
    nearEligibleBlockers.push("candidate_not_ready")
  }
  if (!eligibleBlockers.length && eligibleSupportCount < 4) {
    eligibleBlockers.push("candidate_not_ready")
  }

  return {
    freshness,
    inferredSnapshotPresence,
    hasReference,
    sufficientCoverage,
    partialCoverage,
    hasLiquidityContext,
    sufficientVolume,
    reasonableVolume,
    partialMarketSupport,
    meaningfulReference,
    hasUtilitySignal,
    weaponSkinCoverageLedNearEligible,
    nearEligibleSupportCount,
    eligibleSupportCount,
    nearEligibleBlockers,
    eligibleBlockers,
    canReachNearEligible:
      !nearEligibleBlockers.length &&
      freshness.usable &&
      partialMarketSupport &&
      (!isWeaponSkin || hasAnyCoverage) &&
      (hasReference || weaponSkinCoverageLedNearEligible) &&
      nearEligibleSupportCount >= 3,
    canReachEligible:
      !eligibleBlockers.length &&
      hasReference &&
      freshness.usable &&
      sufficientCoverage &&
      sufficientVolume &&
      inferredSnapshotPresence &&
      eligibleSupportCount >= 4,
    hasMeaningfulProgress:
      countTrueValues([
        hasReference,
        inferredSnapshotPresence,
        freshness.usable,
        partialCoverage,
        reasonableVolume,
        coverageCount > 0,
        hasUtilitySignal
      ]) >= 2,
    hasStructuralReason,
    coverageState: resolveCoverageDiagnosticState(coverageCount, minCoverage),
    referenceState: resolveReferenceDiagnosticState({
      referencePrice: safeReferencePrice,
      snapshotHasPriceSignal
    }),
    liquidityState: resolveLiquidityDiagnosticState({
      hasLiquidityContext,
      sufficientVolume,
      reasonableVolume,
      hasUtilitySignal,
      liquidityRank
    }),
    snapshotState: resolveSnapshotDiagnosticState({
      snapshotCapturedAt,
      snapshotStale: Boolean(snapshotStale) && Boolean(snapshotCapturedAt),
      snapshotComplete:
        Boolean(snapshotCapturedAt) &&
        Boolean(snapshotHasPriceSignal) &&
        Boolean(snapshotHasLiquiditySignal)
    }),
    snapshotIncomplete:
      Boolean(snapshotCapturedAt) &&
      (!Boolean(snapshotHasPriceSignal) || !Boolean(snapshotHasLiquiditySignal))
  }
}

function isUniverseBackfillReadyRow(row = {}) {
  const candidateStatus = normalizeCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
  if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
    candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
  ) {
    return true
  }
  if (
    candidateStatus !== CATALOG_CANDIDATE_STATUS.ENRICHING &&
    candidateStatus !== CATALOG_CANDIDATE_STATUS.CANDIDATE
  ) {
    return false
  }

  const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
  const category = normalizeCategory(row?.category, marketHashName)
  const maturityState = normalizeMaturityState(
    row?.maturity_state ?? row?.maturityState,
    CATALOG_MATURITY_STATE.ENRICHING
  )
  if (!isScannerScopeCategory(category) || maturityState === CATALOG_MATURITY_STATE.COLD) {
    return false
  }

  const referencePrice = toPositiveOrNull(row?.reference_price ?? row?.referencePrice)
  const marketCoverageCount = Math.max(
    Number((row?.market_coverage_count ?? row?.marketCoverageCount) || 0),
    0
  )
  const volume7d = toPositiveOrNull(row?.volume_7d ?? row?.volume7d)
  const snapshotCapturedAt = normalizeText(
    row?.snapshot_captured_at || row?.snapshotCapturedAt
  ) || null
  const quoteFetchedAt = normalizeText(row?.quote_fetched_at || row?.quoteFetchedAt) || null
  const missingSnapshot =
    row?.missing_snapshot == null ? !snapshotCapturedAt : Boolean(row.missing_snapshot)
  const snapshotStale =
    row?.snapshot_stale == null ? false : Boolean(row.snapshot_stale)
  const progress = computeCatalogProgressContext({
    category,
    referencePrice,
    volume7d,
    marketCoverageCount,
    snapshotCapturedAt,
    quoteFetchedAt,
    snapshotStale,
    hasSnapshot: !missingSnapshot,
    liquidityRank: toFiniteOrNull(row?.liquidity_rank ?? row?.liquidityRank) ?? 0,
    eligibilityReason: row?.eligibility_reason ?? row?.eligibilityReason
  })

  if (progress.hasStructuralReason) {
    return false
  }
  const hasCoverageForComparison = marketCoverageCount >= 1
  if (!hasCoverageForComparison) {
    return false
  }
  const hasSnapshotSignal =
    Boolean(progress.inferredSnapshotPresence) || Boolean(progress.freshness?.hasQuoteFreshness)
  if (!hasSnapshotSignal) {
    return false
  }
  const hasReferenceOrSafeProxy =
    (Boolean(progress.hasReference) &&
      Boolean(progress.meaningfulReference || progress.freshness?.usable || progress.hasUtilitySignal)) ||
    (hasCoverageForComparison &&
      hasSnapshotSignal &&
      Boolean(progress.hasLiquidityContext || progress.reasonableVolume || progress.hasUtilitySignal))
  if (!hasReferenceOrSafeProxy) {
    return false
  }
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE && !progress.hasMeaningfulProgress) {
    return false
  }

  return progress.hasMeaningfulProgress
}

function incrementPromotionReasons(counter = {}, reasons = [], category = "", byCategory = null) {
  const safeReasons = Array.isArray(reasons) ? reasons : []
  const normalizedCategory = normalizeCategory(category)
  for (const reason of safeReasons) {
    if (!Object.prototype.hasOwnProperty.call(counter, reason)) continue
    counter[reason] = Number(counter[reason] || 0) + 1
    if (
      byCategory &&
      isScannerScopeCategory(normalizedCategory) &&
      byCategory[normalizedCategory] &&
      Object.prototype.hasOwnProperty.call(byCategory[normalizedCategory], reason)
    ) {
      byCategory[normalizedCategory][reason] =
        Number(byCategory[normalizedCategory][reason] || 0) + 1
    }
  }
}

function hasExcludedNamePattern(name = "") {
  const text = normalizeText(name)
  return EXCLUDED_NAME_PATTERNS.some((pattern) => pattern.test(text))
}

function isOutOfScopePremiumName(name = "") {
  const text = normalizeText(name)
  if (/ case$/i.test(text) || /\bweapon case\b/i.test(text) || /\bsouvenir package\b/i.test(text)) {
    return false
  }
  if (/\bcapsule\b/i.test(text)) return false
  return /\b(gloves|glove|hand wraps|knife|bayonet|karambit|daggers)\b/i.test(text)
}

function extractWeaponPrefix(name = "") {
  const text = normalizeText(name)
  if (!text.includes("|")) return ""
  const withoutPrefix = text
    .replace(/^stattrak[™\u2122]?\s*/i, "")
    .replace(/^souvenir\s+/i, "")
    .trim()
  return normalizeText(withoutPrefix.split("|")[0])
}

function isLowValueWeaponName(name = "") {
  return LOW_VALUE_WEAPON_PATTERNS.some((pattern) => pattern.test(name))
}

function isEligibleWeaponSkinName(name = "") {
  const text = normalizeText(name)
  if (!text.includes("|")) return false
  if (!WEAR_PATTERN.test(text)) return false
  if (isOutOfScopePremiumName(text)) return false
  const weaponPrefix = extractWeaponPrefix(text)
  if (!WEAPON_PREFIX_ALLOWLIST.has(weaponPrefix)) return false
  if (isLowValueWeaponName(text)) return false
  return true
}

function isEligibleCaseName(name = "") {
  const text = normalizeText(name)
  if (!text) return false
  if (/\bkey\b/i.test(text)) return false
  if (/\bcapsule\b/i.test(text)) return false
  return / case$/i.test(text) || /\bweapon case\b/i.test(text) || /\bsouvenir package\b/i.test(text)
}

function isEligibleStickerCapsuleName(name = "") {
  const text = normalizeText(name)
  if (!text || /\bgraffiti\b/i.test(text)) return false
  const hasCapsule = /\bcapsule\b/i.test(text)
  const hasStickerOrAuto = /\b(sticker|autograph)\b/i.test(text)
  if (!hasCapsule || !hasStickerOrAuto) return false
  const hasMajorSignals =
    MAJOR_CAPSULE_EVENT_PATTERN.test(text) ||
    CAPSULE_EVENT_SIGNAL_PATTERN.test(text) ||
    CAPSULE_YEAR_PATTERN.test(text)
  return hasMajorSignals
}

function normalizeCategory(value, marketHashName = "") {
  const text = normalizeText(value).toLowerCase()
  if (isScannerScopeCategory(text)) {
    return text
  }

  const name = normalizeText(marketHashName)
  if (!name || hasExcludedNamePattern(name) || isOutOfScopePremiumName(name)) return ""
  if (isEligibleCaseName(name)) return ITEM_CATEGORIES.CASE
  if (isEligibleStickerCapsuleName(name)) return ITEM_CATEGORIES.STICKER_CAPSULE
  if (isEligibleWeaponSkinName(name)) return ITEM_CATEGORIES.WEAPON_SKIN
  return ""
}

function inferSubcategory(name = "", category = "") {
  const text = normalizeText(name)
  if (category === ITEM_CATEGORIES.CASE) {
    return /souvenir package/i.test(text) ? "souvenir_package" : "weapon_case"
  }
  if (category === ITEM_CATEGORIES.STICKER_CAPSULE) {
    if (/autograph/i.test(text)) return "major_team_autograph_capsule"
    return "major_sticker_capsule"
  }

  const prefix = extractWeaponPrefix(text)
  if (
    ["AK-47", "M4A1-S", "M4A4", "FAMAS", "Galil AR", "SG 553", "AUG"].includes(prefix)
  ) {
    return "rifle"
  }
  if (["AWP", "SSG 08", "SCAR-20", "G3SG1"].includes(prefix)) {
    return "sniper"
  }
  if (
    [
      "USP-S",
      "Glock-18",
      "Desert Eagle",
      "P250",
      "Five-SeveN",
      "Tec-9",
      "CZ75-Auto",
      "Dual Berettas",
      "R8 Revolver"
    ].includes(prefix)
  ) {
    return "pistol"
  }
  if (["MP9", "MP7", "MP5-SD", "MAC-10", "UMP-45", "P90", "PP-Bizon"].includes(prefix)) {
    return "smg"
  }
  if (["XM1014", "Nova", "MAG-7", "Sawed-Off"].includes(prefix)) {
    return "shotgun"
  }
  if (["Negev", "M249"].includes(prefix)) {
    return "machine_gun"
  }
  return "weapon_skin"
}

function isSnapshotStale(snapshot = {}) {
  const capturedAt = normalizeText(snapshot?.captured_at)
  if (!capturedAt) return true
  const ts = new Date(capturedAt).getTime()
  if (!Number.isFinite(ts)) return true
  return Date.now() - ts > SNAPSHOT_TTL_MS
}

function computeSourceLiquidityScore({
  referencePrice = null,
  volume7d = null,
  marketCoverage = 0,
  snapshotStale = false,
  category = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  const rules = SOURCE_QUALITY_RULES[category] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const normalizedPrice = toPositiveOrNull(referencePrice) || 0
  const normalizedVolume = toPositiveOrNull(volume7d) || 0
  const normalizedCoverage = Math.max(Number(marketCoverage || 0), 0)

  const priceScore = Math.min((normalizedPrice / Math.max(Number(rules.minReferencePrice || 1), 1)) * 22, 22)
  const volumeScore = Math.min((normalizedVolume / Math.max(Number(rules.minVolume7d || 1), 1)) * 56, 56)
  const coverageScore = Math.min(normalizedCoverage * 11, 22)
  const stalePenalty = snapshotStale ? 14 : 0

  const score = Math.max(priceScore + volumeScore + coverageScore - stalePenalty, 0)
  return Number(score.toFixed(2))
}

function scaleQuotaValue(baseValue, targetSize, quotaBaseTotal) {
  if (!Number(quotaBaseTotal || 0)) return 0
  return Math.max(
    Math.round((Number(baseValue || 0) * Number(targetSize || 0)) / Number(quotaBaseTotal || 0)),
    0
  )
}

function allocateCategorySlots(quotas = {}, remaining = 0, buckets = [], field = "targetScaled") {
  let slots = Math.max(Number(remaining || 0), 0)
  while (slots > 0) {
    const candidates = buckets
      .filter((bucket) => Number(quotas[bucket.category] || 0) < Number(bucket[field] || 0))
      .sort(
        (a, b) =>
          Number(b[field] || 0) - Number(quotas[b.category] || 0) -
            (Number(a[field] || 0) - Number(quotas[a.category] || 0)) ||
          Number(b.targetScaled || 0) - Number(a.targetScaled || 0)
      )
    if (!candidates.length) break
    quotas[candidates[0].category] += 1
    slots -= 1
  }
  return slots
}

function buildScaledQuotas(targetSize, categoryRules = {}, categories = [], baseTotal = 0) {
  const safeCategories = Array.isArray(categories) ? categories : []
  const safeTarget = Math.max(Math.round(Number(targetSize || 0)), 1)
  const quotas = Object.fromEntries(safeCategories.map((category) => [category, 0]))

  if (!Number(baseTotal || 0) || !safeCategories.length) {
    if (safeCategories.length) {
      quotas[safeCategories[0]] = safeTarget
    }
    return quotas
  }

  const bucketPlan = safeCategories.map((category) => {
    const rule = categoryRules[category] || {}
    const minScaled = scaleQuotaValue(rule.min, safeTarget, baseTotal)
    const targetScaled = scaleQuotaValue(rule.target, safeTarget, baseTotal)
    const maxScaled = Math.max(scaleQuotaValue(rule.max, safeTarget, baseTotal), minScaled)
    return {
      category,
      minScaled,
      targetScaled: Math.max(targetScaled, minScaled),
      maxScaled
    }
  })

  const minTotal = bucketPlan.reduce((sum, bucket) => sum + Number(bucket.minScaled || 0), 0)
  if (minTotal > safeTarget) {
    const targetBuckets = bucketPlan
      .map((bucket) => ({
        ...bucket,
        exactShare:
          (Number(bucket.targetScaled || 0) / Math.max(
            bucketPlan.reduce((sum, row) => sum + Number(row.targetScaled || 0), 0),
            1
          )) * safeTarget
      }))
      .sort((a, b) => Number(b.exactShare || 0) - Number(a.exactShare || 0))

    for (const bucket of targetBuckets) {
      quotas[bucket.category] = Math.floor(Number(bucket.exactShare || 0))
    }
    let remainder = safeTarget - Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
    let index = 0
    while (remainder > 0 && targetBuckets.length) {
      quotas[targetBuckets[index % targetBuckets.length].category] += 1
      remainder -= 1
      index += 1
    }
    return quotas
  }

  for (const bucket of bucketPlan) {
    quotas[bucket.category] = Number(bucket.minScaled || 0)
  }

  let remaining = safeTarget - minTotal
  remaining = allocateCategorySlots(quotas, remaining, bucketPlan, "targetScaled")
  remaining = allocateCategorySlots(quotas, remaining, bucketPlan, "maxScaled")

  if (remaining > 0) {
    const byPriority = bucketPlan.slice().sort(
      (a, b) => Number(b.targetScaled || 0) - Number(a.targetScaled || 0)
    )
    let index = 0
    while (remaining > 0 && byPriority.length) {
      quotas[byPriority[index % byPriority.length].category] += 1
      remaining -= 1
      index += 1
    }
  }

  return quotas
}

function buildCategoryQuotas(targetSize) {
  return buildScaledQuotas(targetSize, CATEGORY_QUOTA_RULES, CATEGORY_PRIORITY, CATEGORY_QUOTA_BASE_TOTAL)
}

function buildSourceCatalogQuotas(targetSize) {
  return buildScaledQuotas(
    targetSize,
    SOURCE_CATALOG_QUOTA_RULES,
    CATEGORY_PRIORITY,
    SOURCE_CATALOG_QUOTA_BASE_TOTAL
  )
}

function buildBaseDiagnostics() {
  return {
    generatedAt: new Date().toISOString(),
    targetUniverseSize: DEFAULT_UNIVERSE_TARGET,
    sourceCatalog: {
      targetRows: SOURCE_CATALOG_LIMIT,
      totalRows: 0,
      totalCatalog: 0,
      total_catalog: 0,
      seededRows: 0,
      sourceCandidateRows: 0,
      selectedSeedRowsByCategory: buildCategoryNumberMap(),
      sourceCandidateRowsByCategory: buildCategoryNumberMap(),
      sourceExcludedRowsByReason: { ...BASE_INGEST_EXCLUDED_REASON_COUNTER },
      sourceCatalogQuotaTargetByCategory: buildSourceCatalogQuotas(SOURCE_CATALOG_LIMIT),
      sourceCatalogQuotaStageByCategory: buildCategoryNumberMap(),
      sourceCatalogQuotaShortfallByCategory: buildCategoryNumberMap(),
      sourceCatalogQuotaReallocationByCategory: buildCategoryNumberMap(),
      missingRowsToTarget: 0,
      missingRowsToTargetByCategory: buildCategoryNumberMap(),
      activeCatalogRows: 0,
      activeTradable: 0,
      active_tradable: 0,
      scannable: 0,
      shadow: 0,
      blocked: 0,
      blockedByReason: buildCatalogReasonMap(0, CATALOG_BLOCK_REASONS),
      shadowByReason: buildCatalogReasonMap(0, CATALOG_SHADOW_REASONS),
      blocked_by_reason: buildCatalogReasonMap(0, CATALOG_BLOCK_REASONS),
      shadow_by_reason: buildCatalogReasonMap(0, CATALOG_SHADOW_REASONS),
      catalogStatusCounts: buildCatalogStatusNumberMap(),
      scannerSourceSize: 0,
      scanner_source_size: 0,
      tradableRows: 0,
      candidateRows: 0,
      enrichingRows: 0,
      nearEligibleRows: 0,
      coldRows: 0,
      eligibleRows: 0,
      rejectedRows: 0,
      eligibleTradableRows: 0,
      promotedToNearEligible: 0,
      promotedToEligible: 0,
      demotedToEnriching: 0,
      promotedToNearEligibleByCategory: buildCategoryNumberMap(),
      promotedToEligibleByCategory: buildCategoryNumberMap(),
      demotedToEnrichingByCategory: buildCategoryNumberMap(),
      stuckInEnrichingByReason: buildPromotionReasonMap(),
      stuckInEnrichingByReasonByCategory: buildPromotionReasonByCategoryMap(),
      blockedFromPromotionByReason: buildPromotionReasonMap(),
      blockedFromPromotionByReasonByCategory: buildPromotionReasonByCategoryMap(),
      enrichingFreshnessByState: buildFreshnessNumberMap(),
      enrichingFreshnessByStateByCategory: buildFreshnessByCategoryMap(),
      nearEligibleFreshnessByState: buildFreshnessNumberMap(),
      nearEligibleFreshnessByStateByCategory: buildFreshnessByCategoryMap(),
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0,
      excludedRowsByReason: { ...BASE_EXCLUDED_REASON_COUNTER },
      candidateFunnel: buildStatusNumberMap(),
      maturityFunnel: buildMaturityNumberMap(),
      maturityFunnelByCategory: buildMaturityByCategoryMap(),
      candidateFunnelByCategory: buildEmptyCategoryCounter(),
      eligibleRowsByCategory: buildCategoryNumberMap(),
      nearEligibleRowsByCategory: buildCategoryNumberMap(),
      candidateRowsByCategory: buildCategoryNumberMap(),
      enrichingRowsByCategory: buildCategoryNumberMap(),
      priorityCoverage: {
        totalPriorityItemsConfigured: 0,
        matchedExistingCatalogItems: 0,
        insertedMissingCatalogItems: 0,
        unmatchedPriorityItems: [],
        scannablePriorityItemsByTier: buildPriorityTierNumberMap(),
        shadowPriorityItemsByTier: buildPriorityTierNumberMap(),
        blockedPriorityItemsByTier: buildPriorityTierNumberMap(),
        scannerSourceCountsByTier: buildPriorityTierNumberMap()
      },
      fullRebuildRows: 0,
      incrementalRecomputeRows: 0,
      incrementalSkippedRows: 0,
      byCategory: buildEmptyCategoryCounter()
    },
    universeBuild: {
      activeUniverseBuilt: 0,
      missingToTarget: 0,
      quotas: buildCategoryQuotas(DEFAULT_UNIVERSE_TARGET),
      quotaTargetByCategory: buildCategoryQuotas(DEFAULT_UNIVERSE_TARGET),
      selectedByCategory: buildCategoryNumberMap(),
      selectedByCategoryQuotaStage: buildCategoryNumberMap(),
      quotaShortfallByCategory: buildCategoryNumberMap(),
      quotaOverflowByCategory: buildCategoryNumberMap(),
      quotaReallocationByCategory: buildCategoryNumberMap(),
      reallocatedSlots: 0,
      eligibleRows: 0,
      nearEligibleRows: 0,
      candidateRows: 0,
      enrichingRows: 0,
      eligibleRowsByCategory: buildCategoryNumberMap(),
      nearEligibleRowsByCategory: buildCategoryNumberMap(),
      candidateRowsByCategory: buildCategoryNumberMap(),
      enrichingRowsByCategory: buildCategoryNumberMap(),
      selectedFromEligible: 0,
      selectedFromNearEligible: 0,
      selectedFromEnriching: 0,
      selectedFromCandidate: 0,
      backfillReadyRows: 0,
      backfillBlockedRows: 0,
      backfillBlockedRowsByCategory: buildCategoryNumberMap(),
      candidateBackfillUsed: false,
      seedPromotionActive: false,
      fallbackToMaxEligible: false
    },
    refreshed: false,
    skipped: false,
    refreshIntervalMs: SOURCE_CATALOG_REFRESH_MS
  }
}

function mergeCategoryCounter(counter = {}, category = "", field = "") {
  const key = normalizeCategory(category)
  if (!key) return
  if (!counter[key]) counter[key] = { ...CATEGORY_DEFAULT_COUNTER }
  if (field && Object.prototype.hasOwnProperty.call(counter[key], field)) {
    counter[key][field] += 1
  }
}

function toByNameMap(rows = [], key = "market_hash_name") {
  const map = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const name = normalizeText(row?.[key])
    if (!name) continue
    map[name] = row
  }
  return map
}

async function ensureSkinsForCatalogNames(marketNames = []) {
  const names = Array.from(new Set((Array.isArray(marketNames) ? marketNames : []).map(normalizeText).filter(Boolean)))
  if (!names.length) {
    return []
  }

  let existing = []
  try {
    existing = await skinRepo.getByMarketHashNames(names)
  } catch (err) {
    console.error("[source-catalog] Failed to fetch skins by market hash names", err.message)
    existing = names.map((marketHashName) => ({ market_hash_name: marketHashName }))
  }
  const existingByName = toByNameMap(existing, "market_hash_name")
  const missingNames = names.filter((name) => !existingByName[name])

  if (missingNames.length) {
    try {
      await skinRepo.upsertSkins(
        missingNames.map((marketHashName) => ({ market_hash_name: marketHashName }))
      )
    } catch (err) {
      console.error("[source-catalog] Failed to auto-seed missing skins", err.message)
    }
  }

  try {
    return await skinRepo.getByMarketHashNames(names)
  } catch (err) {
    console.error("[source-catalog] Failed to refetch skins after auto-seed", err.message)
    return existing
  }
}

function classifyCatalogCandidate(marketHashName = "", categoryHint = "") {
  const name = normalizeText(marketHashName)
  if (!name) {
    return {
      category: "",
      exclusionReason: "excludedNamePattern"
    }
  }
  if (hasExcludedNamePattern(name)) {
    return {
      category: "",
      exclusionReason: "excludedNamePattern"
    }
  }

  const normalizedHint = normalizeText(categoryHint).toLowerCase()
  if (normalizedHint && !isScannerScopeCategory(normalizedHint)) {
    return {
      category: "",
      exclusionReason: "excludedOutOfScopeCategory"
    }
  }

  if (isOutOfScopePremiumName(name)) {
    return {
      category: "",
      exclusionReason: "excludedOutOfScopeCategory"
    }
  }

  const category = normalizeCategory(normalizedHint || "", name)
  if (category) {
    return {
      category,
      exclusionReason: ""
    }
  }

  if (name.includes("|")) {
    if (!WEAR_PATTERN.test(name)) {
      return {
        category: "",
        exclusionReason: "excludedMissingWear"
      }
    }
    const prefix = extractWeaponPrefix(name)
    if (!WEAPON_PREFIX_ALLOWLIST.has(prefix)) {
      return {
        category: "",
        exclusionReason: "excludedUnsupportedWeaponPrefix"
      }
    }
    if (isLowValueWeaponName(name)) {
      return {
        category: "",
        exclusionReason: "excludedLowValueName"
      }
    }
  }

  if (/case|package/i.test(name)) {
    return {
      category: "",
      exclusionReason: "excludedWeakCaseCandidate"
    }
  }
  if (/capsule/i.test(name)) {
    return {
      category: "",
      exclusionReason: "excludedWeakCapsuleCandidate"
    }
  }

  return {
    category: "",
    exclusionReason: "excludedOutOfScopeCategory"
  }
}

function scoreSourceCatalogCandidate(row = {}, sourceRank = 0) {
  const marketHashName = normalizeText(row?.marketHashName || row?.market_hash_name)
  const category = normalizeCategory(row?.category, marketHashName)
  const lowered = marketHashName.toLowerCase()
  let score = Number(sourceRank || 0)

  if (category === ITEM_CATEGORIES.WEAPON_SKIN) {
    const prefix = extractWeaponPrefix(marketHashName)
    score += 50
    if (/^stattrak/i.test(marketHashName)) score += 12
    if (/^souvenir/i.test(marketHashName)) score += 8
    if (HIGH_LIQUIDITY_WEAPON_PREFIXES.has(prefix)) score += 10
    if (LIQUID_WEAPON_KEYWORDS.some((keyword) => lowered.includes(keyword))) score += 8
    if (/\((factory new|minimal wear)\)$/i.test(marketHashName)) score += 2
  } else if (category === ITEM_CATEGORIES.CASE) {
    score += 40
    if (/operation|kilowatt|gallery|dreams & nightmares|revolution|recoil|fracture|snakebite/i.test(marketHashName)) {
      score += 10
    }
    if (/souvenir package/i.test(marketHashName)) score += 5
  } else if (category === ITEM_CATEGORIES.STICKER_CAPSULE) {
    score += 40
    if (MAJOR_CAPSULE_EVENT_PATTERN.test(marketHashName)) score += 10
    if (/\b(legends|challengers|contenders|champions)\b/i.test(marketHashName)) score += 8
    if (/autograph/i.test(marketHashName)) score += 6
  }

  return Number(score.toFixed(2))
}

function toSourceCatalogSeedRows(rows = [], sourceTag = "curated_seed", sourceRank = 0, counts = null) {
  const output = []
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name)
    const classification = classifyCatalogCandidate(marketHashName, row?.category)
    if (!classification.category) {
      if (counts && counts[classification.exclusionReason] != null) {
        counts[classification.exclusionReason] += 1
      }
      continue
    }

    output.push({
      marketHashName,
      itemName: normalizeText(row?.itemName || row?.item_name || marketHashName) || marketHashName,
      category: classification.category,
      subcategory: normalizeText(row?.subcategory) || inferSubcategory(marketHashName, classification.category),
      tradable: true,
      scanEligible: Boolean(row?.scanEligible ?? row?.scan_eligible ?? false),
      isActive: Boolean(row?.isActive ?? row?.is_active ?? true),
      sourceTag,
      sourceRank,
      candidateScore: scoreSourceCatalogCandidate(
        {
          marketHashName,
          category: classification.category
        },
        sourceRank
      )
    })
  }
  return output
}

function pickSourceCatalogRowsByQuota(candidates = [], limit = SOURCE_CATALOG_LIMIT) {
  const safeLimit = Math.max(Math.round(Number(limit || SOURCE_CATALOG_LIMIT)), 1)
  const deduped = []
  const seen = new Set()
  const excludedByReason = { ...BASE_INGEST_EXCLUDED_REASON_COUNTER }
  const candidateByCategory = buildCategoryNumberMap()

  for (const row of Array.isArray(candidates) ? candidates : []) {
    const marketHashName = normalizeText(row?.marketHashName || row?.market_hash_name)
    if (!marketHashName) continue
    const key = marketHashName.toLowerCase()
    if (seen.has(key)) {
      excludedByReason.excludedDuplicate += 1
      continue
    }
    const category = normalizeCategory(row?.category, marketHashName)
    if (!isScannerScopeCategory(category)) {
      excludedByReason.excludedOutOfScopeCategory += 1
      continue
    }

    seen.add(key)
    candidateByCategory[category] += 1
    deduped.push({
      ...row,
      marketHashName,
      category,
      candidateScore:
        toFiniteOrNull(row?.candidateScore) ??
        scoreSourceCatalogCandidate({ marketHashName, category }, Number(row?.sourceRank || 0))
    })
  }

  const quotas = buildSourceCatalogQuotas(safeLimit)
  const buckets = Object.fromEntries(CATEGORY_PRIORITY.map((category) => [category, []]))
  for (const row of deduped) {
    buckets[row.category].push(row)
  }
  for (const category of CATEGORY_PRIORITY) {
    buckets[category].sort(
      (a, b) =>
        Number(b.candidateScore || 0) - Number(a.candidateScore || 0) ||
        Number(b.sourceRank || 0) - Number(a.sourceRank || 0) ||
        String(a.marketHashName || "").localeCompare(String(b.marketHashName || ""))
    )
  }

  const selected = []
  const selectedByCategory = buildCategoryNumberMap()
  const selectedByQuotaStage = buildCategoryNumberMap()
  const leftovers = []

  for (const category of CATEGORY_PRIORITY) {
    const quota = Math.max(Number(quotas[category] || 0), 0)
    const rows = buckets[category]
    const stageRows = rows.slice(0, quota)
    selected.push(...stageRows)
    selectedByCategory[category] += stageRows.length
    selectedByQuotaStage[category] = stageRows.length
    leftovers.push(...rows.slice(quota))
  }

  leftovers.sort(
    (a, b) =>
      Number(b.candidateScore || 0) - Number(a.candidateScore || 0) ||
      Number(b.sourceRank || 0) - Number(a.sourceRank || 0)
  )

  for (const row of leftovers) {
    if (selected.length >= safeLimit) break
    selected.push(row)
    selectedByCategory[row.category] += 1
  }

  const quotaShortfallByCategory = buildCategoryNumberMap()
  const quotaReallocationByCategory = buildCategoryNumberMap()
  const missingRowsToTargetByCategory = buildCategoryNumberMap()
  for (const category of CATEGORY_PRIORITY) {
    const quota = Math.max(Number(quotas[category] || 0), 0)
    const stageSelected = Number(selectedByQuotaStage[category] || 0)
    const finalSelected = Number(selectedByCategory[category] || 0)
    quotaShortfallByCategory[category] = Math.max(quota - stageSelected, 0)
    quotaReallocationByCategory[category] = finalSelected - quota
    missingRowsToTargetByCategory[category] = Math.max(quota - finalSelected, 0)
  }

  const missingRowsToTarget = Math.max(safeLimit - Math.min(selected.length, safeLimit), 0)

  return {
    rows: selected.slice(0, safeLimit),
    selectedByCategory,
    candidateByCategory,
    quotas,
    selectedByQuotaStage,
    quotaShortfallByCategory,
    quotaReallocationByCategory,
    missingRowsToTarget,
    missingRowsToTargetByCategory,
    excludedByReason
  }
}

function resolveSeedBuilder(limit = SOURCE_CATALOG_LIMIT) {
  if (typeof sourceCatalogSeed?.buildSourceCatalogSeed === "function") {
    return sourceCatalogSeed.buildSourceCatalogSeed(limit)
  }
  return Array.isArray(sourceCatalogSeed) ? sourceCatalogSeed.slice(0, limit) : []
}

async function ingestSourceCatalogSeeds() {
  const ingestExclusions = { ...BASE_INGEST_EXCLUDED_REASON_COUNTER }
  const curatedSeedRows = toSourceCatalogSeedRows(
    resolveSeedBuilder(Math.max(SOURCE_CATALOG_LIMIT * 2, 1000)),
    "curated_seed",
    20,
    ingestExclusions
  )

  let skinIndexRows = []
  try {
    const allSkins = await skinRepo.listAll()
    skinIndexRows = toSourceCatalogSeedRows(
      allSkins.map((row) => ({
        marketHashName: row?.market_hash_name || row?.marketHashName
      })),
      "skin_index_curated",
      10,
      ingestExclusions
    )
  } catch (err) {
    console.error("[source-catalog] Failed to read skin index for source expansion", err.message)
  }

  const selection = pickSourceCatalogRowsByQuota(
    [...curatedSeedRows, ...skinIndexRows],
    SOURCE_CATALOG_LIMIT
  )
  for (const [reason, count] of Object.entries(selection.excludedByReason || {})) {
    if (ingestExclusions[reason] == null) continue
    ingestExclusions[reason] += Number(count || 0)
  }

  const seededRows = await marketSourceCatalogRepo.upsertRows(selection.rows)
  const skins = await ensureSkinsForCatalogNames(selection.rows.map((row) => row.marketHashName))

  return {
    seedRows: curatedSeedRows.length,
    sourceCandidateRows: curatedSeedRows.length + skinIndexRows.length,
    seededRows,
    seededSkins: Array.isArray(skins) ? skins.length : 0,
    selectedSeedRowsByCategory: selection.selectedByCategory,
    sourceCandidateRowsByCategory: selection.candidateByCategory,
    sourceExcludedRowsByReason: ingestExclusions,
    sourceCatalogQuotaTargetByCategory: selection.quotas,
    sourceCatalogQuotaStageByCategory: selection.selectedByQuotaStage,
    sourceCatalogQuotaShortfallByCategory: selection.quotaShortfallByCategory,
    sourceCatalogQuotaReallocationByCategory: selection.quotaReallocationByCategory,
    missingRowsToTarget: Number(selection.missingRowsToTarget || 0),
    missingRowsToTargetByCategory: selection.missingRowsToTargetByCategory
  }
}

function resolveVolume7d(snapshot = null, quoteCoverage = {}) {
  const snapshotVolume24h = toPositiveOrNull(snapshot?.volume_24h)
  const snapshotVolume7d = snapshotVolume24h == null ? null : Math.max(Math.round(snapshotVolume24h * 7), 0)
  const quoteVolume = toPositiveOrNull(quoteCoverage?.volume7dMax)

  if (snapshotVolume7d == null && quoteVolume == null) return null
  if (snapshotVolume7d == null) return Math.round(quoteVolume)
  if (quoteVolume == null) return Math.round(snapshotVolume7d)
  return Math.round(Math.max(snapshotVolume7d, quoteVolume))
}

function computeEnrichmentPriority({
  candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE,
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  liquidityRank = 0,
  priorityBoost = 0,
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  missingSnapshot = false,
  missingReference = false,
  missingMarketCoverage = false,
  snapshotStale = false
} = {}) {
  const status = normalizeCandidateStatus(candidateStatus)
  if (status === CATALOG_CANDIDATE_STATUS.REJECTED) return 0

  const statusBoost =
    status === CATALOG_CANDIDATE_STATUS.ELIGIBLE
      ? 38
      : status === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
        ? 32
      : status === CATALOG_CANDIDATE_STATUS.ENRICHING
        ? 26
        : 18
  const categoryBoost =
    category === ITEM_CATEGORIES.CASE
      ? 5
      : category === ITEM_CATEGORIES.STICKER_CAPSULE
        ? 6
        : 0
  const referenceBoost = referencePrice == null ? 0 : Math.min(Number(referencePrice || 0) * 2, 18)
  const volumeBoost = volume7d == null ? 0 : Math.min(Number(volume7d || 0) / 12, 26)
  const coverageBoost = Math.min(Math.max(Number(marketCoverageCount || 0), 0) * 4, 16)
  const readinessBoost =
    (missingReference ? 0 : 6) +
    (missingMarketCoverage ? 0 : 6) +
    (missingSnapshot ? 0 : 5)
  const stalePenalty = snapshotStale ? 8 : 0
  const missingPenalty =
    (missingReference ? 8 : 0) +
    (missingMarketCoverage ? 7 : 0) +
    (missingSnapshot ? 7 : 0)

  const score =
    Number(liquidityRank || 0) +
    Math.min(Math.max(Number(priorityBoost || 0), 0) * 0.12, 60) +
    statusBoost +
    categoryBoost +
    referenceBoost +
    volumeBoost +
    coverageBoost +
    readinessBoost -
    stalePenalty -
    missingPenalty

  return Number(Math.max(score, 0).toFixed(2))
}

function computeCatalogMaturity({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE,
  missingSnapshot = false,
  missingReference = false,
  missingMarketCoverage = false,
  missingLiquidityContext = false,
  snapshotStale = false,
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  liquidityRank = 0,
  eligibilityReason = "",
  snapshotCapturedAt = null,
  quoteFetchedAt = null
} = {}) {
  const normalizedCategory = normalizeCategory(category)
  const normalizedStatus = normalizeCandidateStatus(candidateStatus)
  const missingSignals =
    Number(Boolean(missingSnapshot)) +
    Number(Boolean(missingReference)) +
    Number(Boolean(missingMarketCoverage)) +
    Number(Boolean(missingLiquidityContext))
  const progress = computeCatalogProgressContext({
    category: normalizedCategory,
    referencePrice,
    volume7d,
    marketCoverageCount,
    snapshotCapturedAt,
    quoteFetchedAt,
    snapshotStale,
    hasSnapshot: !missingSnapshot,
    liquidityRank,
    eligibilityReason
  })

  let maturityState = CATALOG_MATURITY_STATE.COLD
  if (normalizedStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE && progress.canReachEligible) {
    maturityState = CATALOG_MATURITY_STATE.ELIGIBLE
  } else if (
    (normalizedStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
      normalizedStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE ||
      normalizedStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) &&
    progress.canReachNearEligible
  ) {
    maturityState = CATALOG_MATURITY_STATE.NEAR_ELIGIBLE
  } else if (
    normalizedStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE ||
    normalizedStatus === CATALOG_CANDIDATE_STATUS.ENRICHING ||
    normalizedStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE
  ) {
    maturityState =
      missingSignals >= 3 && !progress.hasMeaningfulProgress
        ? CATALOG_MATURITY_STATE.COLD
        : CATALOG_MATURITY_STATE.ENRICHING
  }

  const baseScore =
    maturityState === CATALOG_MATURITY_STATE.ELIGIBLE
      ? 84
      : maturityState === CATALOG_MATURITY_STATE.NEAR_ELIGIBLE
        ? 66
        : maturityState === CATALOG_MATURITY_STATE.ENRICHING
          ? 46
          : 24
  const categoryBoost =
    normalizedCategory === ITEM_CATEGORIES.CASE
      ? 5
      : normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE
        ? 6
        : 0
  const freshnessBoost =
    progress.freshness.state === CATALOG_FRESHNESS_STATES.FRESH
      ? 8
      : progress.freshness.state === CATALOG_FRESHNESS_STATES.AGING
        ? 3
        : progress.freshness.state === CATALOG_FRESHNESS_STATES.STALE
          ? -6
          : -10
  const referenceBoost = progress.hasReference ? Math.min(Number(referencePrice || 0), 12) : -8
  const coverageBoost = Math.min(Math.max(Number(marketCoverageCount || 0), 0) * 3, 15)
  const volumeBoost =
    volume7d == null
      ? -6
      : Math.min(
          (Number(volume7d || 0) /
            Math.max(
              Number(
                (
                  SOURCE_QUALITY_RULES[normalizedCategory] ||
                  SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
                ).minVolume7d || 1
              ),
              1
            )) *
            12,
          16
        )
  const liquidityBoost = Math.min(Number(liquidityRank || 0) * 0.1, 10)
  const missingPenalty = missingSignals * 6
  const structuralPenalty = progress.hasStructuralReason ? 12 : 0
  const score = Math.max(
    Math.min(
      baseScore +
        categoryBoost +
        freshnessBoost +
        referenceBoost +
        coverageBoost +
        volumeBoost +
        liquidityBoost -
        missingPenalty -
        structuralPenalty,
      100
    ),
    0
  )

  return {
    maturityState: normalizeMaturityState(maturityState),
    maturityScore: Number(score.toFixed(2)),
    missingSignals,
    hasStructuralReason: progress.hasStructuralReason,
    freshnessState: progress.freshness.state
  }
}

function evaluateCandidateState({
  marketHashName = "",
  category = "",
  tradable = true,
  eligibility = {},
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  snapshot = null,
  snapshotStale = true,
  liquidityRank = 0,
  quoteFetchedAt = null,
  priorityBoost = 0
} = {}) {
  const normalizedCategory = normalizeCategory(category, marketHashName)
  const hardFloor = Number(
    SOURCE_CANDIDATE_HARD_FLOOR[normalizedCategory] ??
      SOURCE_CANDIDATE_HARD_FLOOR[ITEM_CATEGORIES.WEAPON_SKIN]
  )
  const snapshotCapturedAt = normalizeText(snapshot?.captured_at) || null
  const snapshotHasPriceSignal =
    toPositiveOrNull(snapshot?.average_7d_price) != null ||
    toPositiveOrNull(snapshot?.lowest_listing_price) != null
  const snapshotHasLiquiditySignal = toPositiveOrNull(snapshot?.volume_24h) != null
  const hasSnapshot = Boolean(snapshot) && Boolean(snapshotCapturedAt)
  const missingSnapshot = !hasSnapshot
  const missingReference = referencePrice == null
  const rules =
    SOURCE_QUALITY_RULES[normalizedCategory] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const missingMarketCoverage =
    Number(marketCoverageCount || 0) < Number(rules.minMarketCoverage || 0)
  const missingLiquidityContext = volume7d == null
  const effectiveSnapshotStale = Boolean(snapshotCapturedAt) && Boolean(snapshotStale)
  const progress = computeCatalogProgressContext({
    category: normalizedCategory,
    referencePrice,
    volume7d,
    marketCoverageCount,
    snapshotCapturedAt,
    quoteFetchedAt,
    snapshotStale: effectiveSnapshotStale,
    hasSnapshot,
    liquidityRank,
    eligibilityReason: eligibility?.reason,
    snapshotHasPriceSignal,
    snapshotHasLiquiditySignal
  })

  let rejectedReason = ""
  if (!tradable) {
    rejectedReason = "rejectedNotTradable"
  } else if (!isScannerScopeCategory(normalizedCategory)) {
    rejectedReason = "rejectedOutOfScopeCategory"
  } else if (hasExcludedNamePattern(marketHashName)) {
    rejectedReason = "rejectedNamePattern"
  } else if (referencePrice != null && referencePrice < hardFloor) {
    rejectedReason = "rejectedHardValueFloor"
  }

  const strictEligible = Boolean(eligibility?.eligible)
  const strictReason = normalizeText(eligibility?.reason)
  let candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE
  if (rejectedReason) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.REJECTED
  } else if (strictEligible) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.ELIGIBLE
  } else if (progress.canReachNearEligible) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
  } else if (progress.hasMeaningfulProgress || missingReference || missingMarketCoverage || missingLiquidityContext) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.ENRICHING
  }

  const eligibilityReason =
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
      ? ""
      : rejectedReason ||
        strictReason ||
        (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
          ? progress.eligibleBlockers[0] || "near_eligible"
          : progress.nearEligibleBlockers[0] || "candidate_not_ready")
  const progressionStatus =
    candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED
      ? PROGRESSION_DIAGNOSTIC_STATUS.REJECTED
      : candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
        ? PROGRESSION_DIAGNOSTIC_STATUS.ELIGIBLE
        : candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
          ? PROGRESSION_DIAGNOSTIC_STATUS.BLOCKED_ELIGIBLE
          : PROGRESSION_DIAGNOSTIC_STATUS.BLOCKED_NEAR_ELIGIBLE
  const progressionBlockers = buildProgressionBlockers({
    candidateStatus,
    rejectedReason,
    progress,
    snapshotState: progress.snapshotState,
    referenceState: progress.referenceState,
    liquidityState: progress.liquidityState,
    coverageState: progress.coverageState,
    snapshotIncomplete: progress.snapshotIncomplete
  })

  const enrichmentPriority = computeEnrichmentPriority({
    candidateStatus,
    category: normalizedCategory,
    liquidityRank,
    priorityBoost,
    referencePrice,
    volume7d,
    marketCoverageCount,
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    snapshotStale: effectiveSnapshotStale
  })
  const maturity = computeCatalogMaturity({
    category: normalizedCategory,
    candidateStatus,
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    missingLiquidityContext,
    snapshotStale: effectiveSnapshotStale,
    referencePrice,
    volume7d,
    marketCoverageCount,
    liquidityRank,
    eligibilityReason,
    snapshotCapturedAt,
    quoteFetchedAt
  })

  return {
    candidateStatus,
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    missingLiquidityContext,
    eligibilityReason,
    strictEligible,
    strictReason,
    enrichmentPriority,
    maturityState: maturity.maturityState,
    maturityScore: maturity.maturityScore,
    missingSignals: maturity.missingSignals,
    freshnessState: progress.freshness.state,
    nearEligibleBlockers: progress.nearEligibleBlockers,
    eligibleBlockers: progress.eligibleBlockers,
    snapshotState: progress.snapshotState,
    referenceState: progress.referenceState,
    liquidityState: progress.liquidityState,
    coverageState: progress.coverageState,
    progressionStatus,
    progressionBlockers,
    antiFakeBlocked: Boolean(rejectedReason) || progress.hasStructuralReason
  }
}

function hasStructuralCatalogReason(reason = "") {
  return /\binvalid|rejected|anti[_\s-]?fake|not[_\s-]?tradable|broken|outofscope|namepattern|unsupported\b/i.test(
    String(reason || "")
  )
}

function resolveLatestMarketSignalAt(snapshotCapturedAt = null, quoteFetchedAt = null) {
  const snapshotIso = toIsoStringOrNull(snapshotCapturedAt)
  const quoteIso = toIsoStringOrNull(quoteFetchedAt)
  if (!snapshotIso && !quoteIso) return null
  if (!snapshotIso) return quoteIso
  if (!quoteIso) return snapshotIso
  return new Date(snapshotIso).getTime() >= new Date(quoteIso).getTime() ? snapshotIso : quoteIso
}

function computeCatalogQualityScore({
  catalogStatus = CATALOG_STATUS.SHADOW,
  maturityScore = 0,
  liquidityRank = 0,
  priorityBoost = 0,
  marketCoverageCount = 0,
  referencePrice = null,
  freshnessState = CATALOG_FRESHNESS_STATES.MISSING
} = {}) {
  const maturityComponent = Math.min(Math.max(Number(maturityScore || 0), 0), 100) * 0.58
  const liquidityComponent = Math.min(Math.max(Number(liquidityRank || 0), 0), 100) * 0.32
  const coverageComponent = Math.min(Math.max(Number(marketCoverageCount || 0), 0) * 4, 12)
  const referenceComponent =
    toPositiveOrNull(referencePrice) == null ? 0 : Math.min(Number(referencePrice || 0) * 1.5, 10)
  const priorityComponent = Math.min(Math.max(Number(priorityBoost || 0), 0) * 0.08, 24)
  const freshnessComponent =
    freshnessState === CATALOG_FRESHNESS_STATES.FRESH
      ? 8
      : freshnessState === CATALOG_FRESHNESS_STATES.AGING
        ? 4
        : freshnessState === CATALOG_FRESHNESS_STATES.STALE
          ? -6
          : -10

  let score =
    maturityComponent +
    liquidityComponent +
    priorityComponent +
    coverageComponent +
    referenceComponent +
    freshnessComponent
  if (catalogStatus === CATALOG_STATUS.SHADOW) {
    score = Math.max(score - 22, 5)
  } else if (catalogStatus === CATALOG_STATUS.BLOCKED) {
    score = 0
  }
  return Number(Math.max(Math.min(score, 100), 0).toFixed(2))
}

function classifyCatalogStatus({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  referencePrice = null,
  marketCoverageCount = 0,
  snapshotCapturedAt = null,
  quoteFetchedAt = null,
  snapshotStale = false,
  liquidityRank = 0,
  priorityBoost = 0,
  invalidReason = "",
  candidateState = {}
} = {}) {
  const normalizedCategory = normalizeCategory(category)
  const rules =
    SOURCE_QUALITY_RULES[normalizedCategory] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const minCoverage = Math.max(Number(rules.minMarketCoverage || 2), 1)
  const safeReferencePrice = toPositiveOrNull(referencePrice)
  const normalizedCoverage = Math.max(Number(marketCoverageCount || 0), 0)
  const snapshotIso = toIsoStringOrNull(snapshotCapturedAt)
  const quoteIso = toIsoStringOrNull(quoteFetchedAt)
  const lastMarketSignalAt = resolveLatestMarketSignalAt(snapshotIso, quoteIso)
  const freshness = resolveCatalogFreshnessContext({
    category: normalizedCategory,
    snapshotCapturedAt: snapshotIso,
    quoteFetchedAt: quoteIso,
    snapshotStale,
    hasSnapshot: Boolean(snapshotIso)
  })
  const hasAnySnapshotSignal = Boolean(snapshotIso)
  const hasAnyQuoteSignal = Boolean(quoteIso)
  const hasAnySignal = hasAnySnapshotSignal || hasAnyQuoteSignal
  const hasRecentSnapshot =
    freshness.snapshotState === CATALOG_FRESHNESS_STATES.FRESH ||
    freshness.snapshotState === CATALOG_FRESHNESS_STATES.AGING
  const hasRecentQuote =
    freshness.quoteState === CATALOG_FRESHNESS_STATES.FRESH ||
    freshness.quoteState === CATALOG_FRESHNESS_STATES.AGING
  const hasRecentSignal = hasRecentSnapshot || hasRecentQuote
  const antiFakeBlocked =
    Boolean(candidateState?.antiFakeBlocked) || hasStructuralCatalogReason(invalidReason)
  const belowMinCostFloor = safeReferencePrice != null && safeReferencePrice < MIN_SCAN_COST_USD
  const noUsableMarketBasis =
    normalizedCoverage <= 0 && safeReferencePrice == null && !hasAnySnapshotSignal && !hasAnyQuoteSignal
  const staleOnlySignals = !hasRecentSignal && hasAnySignal
  const weakCoverage = normalizedCoverage < minCoverage
  const incompleteReferencePricing = safeReferencePrice == null

  let catalogStatus = CATALOG_STATUS.SCANNABLE
  let catalogBlockReason = null
  if (antiFakeBlocked) {
    catalogStatus = CATALOG_STATUS.BLOCKED
    catalogBlockReason = CATALOG_BLOCK_REASONS.INVALID_CATALOG_REASON
  } else if (belowMinCostFloor) {
    catalogStatus = CATALOG_STATUS.BLOCKED
    catalogBlockReason = CATALOG_BLOCK_REASONS.BELOW_MIN_COST_FLOOR
  } else if (noUsableMarketBasis) {
    catalogStatus = CATALOG_STATUS.BLOCKED
    catalogBlockReason = CATALOG_BLOCK_REASONS.UNUSABLE_MARKET_COVERAGE
  } else if (staleOnlySignals) {
    catalogStatus = CATALOG_STATUS.SHADOW
    catalogBlockReason = CATALOG_SHADOW_REASONS.STALE_ONLY_SIGNALS
  } else if (weakCoverage) {
    catalogStatus = CATALOG_STATUS.SHADOW
    catalogBlockReason = CATALOG_SHADOW_REASONS.WEAK_MARKET_COVERAGE
  } else if (incompleteReferencePricing) {
    catalogStatus = CATALOG_STATUS.SHADOW
    catalogBlockReason = CATALOG_SHADOW_REASONS.INCOMPLETE_REFERENCE_PRICING
  } else if (!hasRecentSignal) {
    catalogStatus = CATALOG_STATUS.SHADOW
    catalogBlockReason = CATALOG_SHADOW_REASONS.STALE_ONLY_SIGNALS
  }

  return {
    catalogStatus,
    catalogBlockReason,
    catalogQualityScore: computeCatalogQualityScore({
      catalogStatus,
      maturityScore: candidateState?.maturityScore,
      liquidityRank,
      priorityBoost,
      marketCoverageCount: normalizedCoverage,
      referencePrice: safeReferencePrice,
      freshnessState: freshness.state
    }),
    lastMarketSignalAt,
    freshnessState: freshness.state
  }
}

function evaluateEligibility({
  category,
  referencePrice,
  volume7d,
  marketCoverageCount,
  snapshotStale,
  snapshotCapturedAt = null,
  quoteFetchedAt = null
}) {
  const rules = SOURCE_QUALITY_RULES[category] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const freshness = resolveCatalogFreshnessContext({
    category,
    snapshotCapturedAt,
    quoteFetchedAt,
    snapshotStale,
    hasSnapshot: Boolean(snapshotCapturedAt) || (referencePrice != null && snapshotStale === false)
  })

  if (!isScannerScopeCategory(category)) {
    return { eligible: false, reason: "excludedOutOfScopeCategory" }
  }
  if (referencePrice == null) return { eligible: false, reason: "excludedMissingReferenceItems" }
  if (referencePrice < Number(rules.minReferencePrice || 0)) {
    return { eligible: false, reason: "excludedLowValueItems" }
  }
  if (volume7d == null || volume7d < Number(rules.minVolume7d || 0)) {
    return { eligible: false, reason: "excludedLowLiquidityItems" }
  }
  if (Number(marketCoverageCount || 0) < Number(rules.minMarketCoverage || 0)) {
    return { eligible: false, reason: "excludedWeakMarketCoverageItems" }
  }
  if (!freshness.usable) {
    return { eligible: false, reason: "excludedStaleItems" }
  }

  return { eligible: true, reason: "" }
}

function buildPriorityTierNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return {
    tier_a: initial,
    tier_b: initial
  }
}

function resolvePriorityEntryForCatalogRow(row = {}, priorityByKey = new Map()) {
  if (!(priorityByKey instanceof Map) || !priorityByKey.size) return null
  const category = normalizeCategory(row?.category, row?.market_hash_name || row?.marketHashName)
  if (!category) return null
  const itemName = normalizeText(row?.item_name || row?.itemName || row?.market_hash_name || row?.marketHashName)
  const key = catalogPriorityCoverageService.buildPriorityKey(category, itemName)
  if (!key) return null
  return priorityByKey.get(key) || null
}

async function enrichSourceCatalog(options = {}) {
  const priorityCoverage =
    options?.priorityCoverage && typeof options.priorityCoverage === "object"
      ? options.priorityCoverage
      : {}
  const priorityByKey = priorityCoverage?.byKey instanceof Map ? priorityCoverage.byKey : new Map()
  const rows = await marketSourceCatalogRepo.listActiveTradable({
    limit: 12000,
    categories: CATEGORY_PRIORITY
  })
  if (!rows.length) {
    return {
      totalRows: 0,
      totalCatalog: 0,
      total_catalog: 0,
      activeCatalogRows: 0,
      activeTradable: 0,
      active_tradable: 0,
      scannable: 0,
      shadow: 0,
      blocked: 0,
      blockedByReason: buildCatalogReasonMap(0, CATALOG_BLOCK_REASONS),
      shadowByReason: buildCatalogReasonMap(0, CATALOG_SHADOW_REASONS),
      blocked_by_reason: buildCatalogReasonMap(0, CATALOG_BLOCK_REASONS),
      shadow_by_reason: buildCatalogReasonMap(0, CATALOG_SHADOW_REASONS),
      catalogStatusCounts: buildCatalogStatusNumberMap(),
      scannerSourceSize: 0,
      scanner_source_size: 0,
      priorityCoverage: {
        totalPriorityItemsConfigured: Number(priorityCoverage?.totalPriorityItemsConfigured || 0),
        matchedExistingCatalogItems: Number(priorityCoverage?.matchedExistingCatalogItems || 0),
        insertedMissingCatalogItems: Number(priorityCoverage?.insertedMissingCatalogItems || 0),
        unmatchedPriorityItems: Array.isArray(priorityCoverage?.unmatchedPriorityItems)
          ? priorityCoverage.unmatchedPriorityItems
          : [],
        error: normalizeText(priorityCoverage?.error) || null,
        scannablePriorityItemsByTier: buildPriorityTierNumberMap(),
        shadowPriorityItemsByTier: buildPriorityTierNumberMap(),
        blockedPriorityItemsByTier: buildPriorityTierNumberMap(),
        scannerSourceCountsByTier: buildPriorityTierNumberMap()
      },
      tradableRows: 0,
      candidateRows: 0,
      enrichingRows: 0,
      nearEligibleRows: 0,
      coldRows: 0,
      eligibleRows: 0,
      rejectedRows: 0,
      eligibleTradableRows: 0,
      promotedToNearEligible: 0,
      promotedToEligible: 0,
      demotedToEnriching: 0,
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0,
      excludedRowsByReason: { ...BASE_EXCLUDED_REASON_COUNTER },
      candidateFunnel: buildStatusNumberMap(),
      maturityFunnel: buildMaturityNumberMap(),
      maturityFunnelByCategory: buildMaturityByCategoryMap(),
      promotedToNearEligibleByCategory: buildCategoryNumberMap(),
      promotedToEligibleByCategory: buildCategoryNumberMap(),
      demotedToEnrichingByCategory: buildCategoryNumberMap(),
      stuckInEnrichingByReason: buildPromotionReasonMap(),
      stuckInEnrichingByReasonByCategory: buildPromotionReasonByCategoryMap(),
      blockedFromPromotionByReason: buildPromotionReasonMap(),
      blockedFromPromotionByReasonByCategory: buildPromotionReasonByCategoryMap(),
      enrichingFreshnessByState: buildFreshnessNumberMap(),
      enrichingFreshnessByStateByCategory: buildFreshnessByCategoryMap(),
      nearEligibleFreshnessByState: buildFreshnessNumberMap(),
      nearEligibleFreshnessByStateByCategory: buildFreshnessByCategoryMap(),
      candidateFunnelByCategory: buildEmptyCategoryCounter(),
      byCategory: buildEmptyCategoryCounter(),
      eligibleRowsByCategory: buildCategoryNumberMap(),
      nearEligibleRowsByCategory: buildCategoryNumberMap(),
      candidateRowsByCategory: buildCategoryNumberMap(),
      enrichingRowsByCategory: buildCategoryNumberMap(),
      fullRebuildRows: 0,
      incrementalRecomputeRows: 0,
      incrementalSkippedRows: 0
    }
  }

  const marketNames = rows.map((row) => normalizeText(row?.market_hash_name || row?.marketHashName)).filter(Boolean)
  const [skinsResult, quoteCoverageResult] = await Promise.allSettled([
    ensureSkinsForCatalogNames(marketNames),
    marketQuoteRepo.getLatestCoverageByItemNames(marketNames)
  ])

  const skins =
    skinsResult.status === "fulfilled" && Array.isArray(skinsResult.value)
      ? skinsResult.value
      : []
  const quoteCoverageByItem =
    quoteCoverageResult.status === "fulfilled" && quoteCoverageResult.value
      ? quoteCoverageResult.value
      : {}

  if (skinsResult.status === "rejected") {
    console.error("[source-catalog] Failed to load skins for enrichment", skinsResult.reason?.message || skinsResult.reason)
  }
  if (quoteCoverageResult.status === "rejected") {
    console.error(
      "[source-catalog] Failed to load quote coverage for enrichment",
      quoteCoverageResult.reason?.message || quoteCoverageResult.reason
    )
  }

  const skinsByName = toByNameMap(
    (Array.isArray(skins) ? skins : []).map((row) => ({
      ...row,
      market_hash_name: normalizeText(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const skinIds = (Array.isArray(skins) ? skins : [])
    .map((row) => Number(row?.id || 0))
    .filter((value) => Number.isInteger(value) && value > 0)
  const snapshotsBySkinId = skinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(skinIds)
    : {}

  const byCategory = buildEmptyCategoryCounter()
  const candidateFunnel = buildStatusNumberMap()
  const maturityFunnel = buildMaturityNumberMap()
  const maturityFunnelByCategory = buildMaturityByCategoryMap()
  const stuckInEnrichingByReason = buildPromotionReasonMap()
  const stuckInEnrichingByReasonByCategory = buildPromotionReasonByCategoryMap()
  const blockedFromPromotionByReason = buildPromotionReasonMap()
  const blockedFromPromotionByReasonByCategory = buildPromotionReasonByCategoryMap()
  const enrichingFreshnessByState = buildFreshnessNumberMap()
  const enrichingFreshnessByStateByCategory = buildFreshnessByCategoryMap()
  const nearEligibleFreshnessByState = buildFreshnessNumberMap()
  const nearEligibleFreshnessByStateByCategory = buildFreshnessByCategoryMap()
  const updates = []
  const nowIso = new Date().toISOString()
  let skippedUnchangedRows = 0
  const catalogStatusCounts = buildCatalogStatusNumberMap()
  const blockedByReason = buildCatalogReasonMap(0, CATALOG_BLOCK_REASONS)
  const shadowByReason = buildCatalogReasonMap(0, CATALOG_SHADOW_REASONS)
  const scannablePriorityItemsByTier = buildPriorityTierNumberMap()
  const shadowPriorityItemsByTier = buildPriorityTierNumberMap()
  const blockedPriorityItemsByTier = buildPriorityTierNumberMap()
  const scannerSourceCountsByTier = buildPriorityTierNumberMap()
  const counts = {
    totalRows: rows.length,
    totalCatalog: rows.length,
    activeCatalogRows: rows.length,
    activeTradable: 0,
    scannable: 0,
    shadow: 0,
    blocked: 0,
    scannerSourceSize: 0,
    tradableRows: 0,
    candidateRows: 0,
    enrichingRows: 0,
    nearEligibleRows: 0,
    coldRows: 0,
    eligibleRows: 0,
    rejectedRows: 0,
    eligibleTradableRows: 0,
    promotedToNearEligible: 0,
    promotedToEligible: 0,
    demotedToEnriching: 0,
    excludedLowValueItems: 0,
    excludedLowLiquidityItems: 0,
    excludedWeakMarketCoverageItems: 0,
    excludedStaleItems: 0,
    excludedMissingReferenceItems: 0
  }

  const eligibleRowsByCategory = buildCategoryNumberMap()
  const nearEligibleRowsByCategory = buildCategoryNumberMap()
  const candidateRowsByCategory = buildCategoryNumberMap()
  const enrichingRowsByCategory = buildCategoryNumberMap()
  const promotedToNearEligibleByCategory = buildCategoryNumberMap()
  const promotedToEligibleByCategory = buildCategoryNumberMap()
  const demotedToEnrichingByCategory = buildCategoryNumberMap()

  for (const row of rows) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue

    const category = normalizeCategory(row?.category, marketHashName)
    if (!isScannerScopeCategory(category)) {
      continue
    }
    const priorityEntry = resolvePriorityEntryForCatalogRow(
      {
        ...row,
        category,
        market_hash_name: marketHashName
      },
      priorityByKey
    )
    const priorityTier = normalizePriorityTier(
      priorityEntry?.tier || row?.priority_tier || row?.priorityTier,
      null
    )
    const priorityRankRaw = Number(
      priorityEntry?.rank ?? row?.priority_rank ?? row?.priorityRank ?? 0
    )
    const priorityRank =
      Number.isFinite(priorityRankRaw) && priorityRankRaw > 0 ? Math.round(priorityRankRaw) : null
    const priorityBoost =
      toFiniteOrNull(priorityEntry?.priorityBoost ?? row?.priority_boost ?? row?.priorityBoost) ?? 0
    const prioritySetName =
      normalizeText(
        priorityEntry?.setName || row?.priority_set_name || row?.prioritySetName
      ) || null
    const isPriorityItem = Boolean(priorityTier && prioritySetName)
    mergeCategoryCounter(byCategory, category)
    byCategory[category].total += 1

    const tradable = row?.tradable == null ? true : Boolean(row.tradable)
    if (tradable) {
      counts.tradableRows += 1
      counts.activeTradable += 1
    }

    const skinId = Number(skinsByName[marketHashName]?.id || 0)
    const snapshot = skinId > 0 ? snapshotsBySkinId[skinId] || null : null
    const quoteCoverage = quoteCoverageByItem[marketHashName] || {}
    const categoryRules =
      SOURCE_QUALITY_RULES[category] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
    const snapshotCapturedAt = normalizeText(snapshot?.captured_at) || null
    const snapshotReferencePrice = toPositiveOrNull(
      toFiniteOrNull(snapshot?.average_7d_price) ?? toFiniteOrNull(snapshot?.lowest_listing_price)
    )
    const marketCoverageCount = Math.max(Number(quoteCoverage?.marketCoverageCount || 0), 0)
    const quoteReferencePrice =
      marketCoverageCount >= Math.max(Number(categoryRules.minMarketCoverage || 2), 2) &&
      Number(quoteCoverage?.referencePriceCandidateCount || 0) >= 2
        ? toPositiveOrNull(quoteCoverage?.referencePriceMedian)
        : null
    const referencePrice = snapshotReferencePrice ?? quoteReferencePrice
    const volume7d = resolveVolume7d(snapshot, quoteCoverage)
    const snapshotStale = snapshotCapturedAt ? isSnapshotStale(snapshot) : false
    const quoteFetchedAt = normalizeText(quoteCoverage?.latestFetchedAt) || null

    const eligibility = evaluateEligibility({
      category,
      referencePrice,
      volume7d,
      marketCoverageCount,
      snapshotStale,
      snapshotCapturedAt,
      quoteFetchedAt
    })

    const liquidityRank = computeSourceLiquidityScore({
      category,
      referencePrice,
      volume7d,
      marketCoverage: marketCoverageCount,
      snapshotStale
    })
    const candidateState = evaluateCandidateState({
      marketHashName,
      category,
      tradable,
      eligibility,
      referencePrice,
      volume7d,
      marketCoverageCount,
      snapshot,
      snapshotStale,
      liquidityRank,
      quoteFetchedAt,
      priorityBoost
    })
    const previousCandidateStatus = normalizeCandidateStatus(
      row?.candidate_status ?? row?.candidateStatus,
      row?.scan_eligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
    )
    const candidateStatus = normalizeCandidateStatus(candidateState.candidateStatus)
    const scanEligible =
      tradable &&
      candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
      candidateState.strictEligible
    const maturityState = normalizeMaturityState(candidateState.maturityState)
    candidateFunnel[candidateStatus] = Number(candidateFunnel[candidateStatus] || 0) + 1
    maturityFunnel[maturityState] = Number(maturityFunnel[maturityState] || 0) + 1
    if (!maturityFunnelByCategory[category]) {
      maturityFunnelByCategory[category] = buildMaturityNumberMap()
    }
    maturityFunnelByCategory[category][maturityState] =
      Number(maturityFunnelByCategory[category][maturityState] || 0) + 1

    if (scanEligible) {
      counts.eligibleTradableRows += 1
      counts.eligibleRows += 1
      byCategory[category].eligible += 1
      eligibleRowsByCategory[category] += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) {
      counts.enrichingRows += 1
      byCategory[category].enriching += 1
      enrichingRowsByCategory[category] += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE) {
      counts.candidateRows += 1
      byCategory[category].candidate += 1
      candidateRowsByCategory[category] += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED) {
      counts.rejectedRows += 1
      byCategory[category].rejected += 1
    }
    if (
      previousCandidateStatus !== CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE &&
      candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
    ) {
      counts.promotedToNearEligible += 1
      promotedToNearEligibleByCategory[category] += 1
    }
    if (
      previousCandidateStatus !== CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
      candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
    ) {
      counts.promotedToEligible += 1
      promotedToEligibleByCategory[category] += 1
    }
    if (
      previousCandidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
      candidateStatus !== CATALOG_CANDIDATE_STATUS.ELIGIBLE
    ) {
      counts.demotedToEnriching += 1
      demotedToEnrichingByCategory[category] += 1
    }
    if (maturityState === CATALOG_MATURITY_STATE.NEAR_ELIGIBLE) {
      counts.nearEligibleRows += 1
      byCategory[category].nearEligible += 1
      nearEligibleRowsByCategory[category] += 1
    }
    if (maturityState === CATALOG_MATURITY_STATE.COLD) {
      counts.coldRows += 1
      byCategory[category].cold += 1
    }
    if (candidateState.missingSnapshot) {
      byCategory[category].missingSnapshot += 1
    }
    if (candidateState.missingReference) {
      byCategory[category].missingReference += 1
    }
    if (candidateState.missingMarketCoverage) {
      byCategory[category].missingMarketCoverage += 1
    }
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) {
      incrementPromotionReasons(
        stuckInEnrichingByReason,
        candidateState.nearEligibleBlockers,
        category,
        stuckInEnrichingByReasonByCategory
      )
      enrichingFreshnessByState[candidateState.freshnessState] =
        Number(enrichingFreshnessByState[candidateState.freshnessState] || 0) + 1
      enrichingFreshnessByStateByCategory[category][candidateState.freshnessState] =
        Number(enrichingFreshnessByStateByCategory[category][candidateState.freshnessState] || 0) + 1
    }
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) {
      nearEligibleFreshnessByState[candidateState.freshnessState] =
        Number(nearEligibleFreshnessByState[candidateState.freshnessState] || 0) + 1
      nearEligibleFreshnessByStateByCategory[category][candidateState.freshnessState] =
        Number(nearEligibleFreshnessByStateByCategory[category][candidateState.freshnessState] || 0) + 1
    }
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) {
      incrementPromotionReasons(
        blockedFromPromotionByReason,
        candidateState.eligibleBlockers,
        category,
        blockedFromPromotionByReasonByCategory
      )
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) {
      incrementPromotionReasons(
        blockedFromPromotionByReason,
        candidateState.nearEligibleBlockers,
        category,
        blockedFromPromotionByReasonByCategory
      )
    }
    if (eligibility.reason) {
      if (Object.prototype.hasOwnProperty.call(counts, eligibility.reason)) {
        counts[eligibility.reason] += 1
      }
      mergeCategoryCounter(byCategory, category, eligibility.reason)
    }

    const invalidReason = scanEligible
      ? null
      : normalizeText(
          candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED
            ? candidateState.eligibilityReason
            : eligibility.reason || candidateState.eligibilityReason || "candidate_not_ready"
        ) || "candidate_not_ready"
    const catalogClassification = classifyCatalogStatus({
      category,
      referencePrice,
      marketCoverageCount,
      snapshotCapturedAt,
      quoteFetchedAt,
      snapshotStale,
      liquidityRank,
      priorityBoost,
      invalidReason,
      candidateState
    })
    const catalogStatus = normalizeCatalogStatus(catalogClassification.catalogStatus)
    const catalogBlockReason = normalizeText(catalogClassification.catalogBlockReason) || null
    if (catalogStatus === CATALOG_STATUS.SCANNABLE) {
      counts.scannable += 1
      counts.scannerSourceSize += 1
      if (isPriorityItem && priorityTier && scannerSourceCountsByTier[priorityTier] != null) {
        scannerSourceCountsByTier[priorityTier] += 1
      }
    } else if (catalogStatus === CATALOG_STATUS.SHADOW) {
      counts.shadow += 1
      if (catalogBlockReason && Object.prototype.hasOwnProperty.call(shadowByReason, catalogBlockReason)) {
        shadowByReason[catalogBlockReason] = Number(shadowByReason[catalogBlockReason] || 0) + 1
      }
    } else {
      counts.blocked += 1
      if (catalogBlockReason && Object.prototype.hasOwnProperty.call(blockedByReason, catalogBlockReason)) {
        blockedByReason[catalogBlockReason] = Number(blockedByReason[catalogBlockReason] || 0) + 1
      }
    }
    catalogStatusCounts[catalogStatus] = Number(catalogStatusCounts[catalogStatus] || 0) + 1
    if (isPriorityItem && priorityTier && scannablePriorityItemsByTier[priorityTier] != null) {
      if (catalogStatus === CATALOG_STATUS.SCANNABLE) {
        scannablePriorityItemsByTier[priorityTier] += 1
      } else if (catalogStatus === CATALOG_STATUS.SHADOW) {
        shadowPriorityItemsByTier[priorityTier] += 1
      } else {
        blockedPriorityItemsByTier[priorityTier] += 1
      }
    }
    const scanLayer = resolveScanLayerForMaturityState(maturityState)

    const nextRow = {
      market_hash_name: marketHashName,
      item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
      category,
      subcategory: normalizeText(row?.subcategory) || inferSubcategory(marketHashName, category),
      tradable,
      scan_eligible: scanEligible,
      candidate_status: candidateStatus,
      missing_snapshot: Boolean(candidateState.missingSnapshot),
      missing_reference: Boolean(candidateState.missingReference),
      missing_market_coverage: Boolean(candidateState.missingMarketCoverage),
      enrichment_priority: candidateState.enrichmentPriority,
      eligibility_reason: scanEligible ? null : normalizeText(candidateState.eligibilityReason) || null,
      maturity_state: maturityState,
      maturity_score: Number(candidateState.maturityScore || 0),
      scan_layer: scanLayer,
      reference_price: referencePrice,
      market_coverage_count: marketCoverageCount,
      liquidity_rank: liquidityRank,
      volume_7d: volume7d,
      snapshot_stale: snapshotStale,
      snapshot_captured_at: snapshotCapturedAt,
      quote_fetched_at: quoteFetchedAt,
      snapshot_state: candidateState.snapshotState,
      reference_state: candidateState.referenceState,
      liquidity_state: candidateState.liquidityState,
      coverage_state: candidateState.coverageState,
      progression_status: candidateState.progressionStatus,
      progression_blockers: scanEligible ? [] : candidateState.progressionBlockers,
      catalog_status: catalogStatus,
      catalog_block_reason: catalogBlockReason,
      catalog_quality_score: Number(catalogClassification.catalogQualityScore || 0),
      last_market_signal_at: catalogClassification.lastMarketSignalAt,
      priority_set_name: isPriorityItem ? prioritySetName : null,
      priority_tier: isPriorityItem ? priorityTier : null,
      priority_rank: isPriorityItem ? priorityRank : null,
      priority_boost: isPriorityItem ? Number(priorityBoost || 0) : 0,
      is_priority_item: isPriorityItem,
      invalid_reason: invalidReason,
      source_tag: normalizeText(row?.source_tag || row?.sourceTag) || "curated_seed",
      is_active: row?.is_active == null ? true : Boolean(row.is_active)
    }

    if (hasCatalogRowChanges(row, nextRow)) {
      updates.push({
        ...nextRow,
        last_enriched_at: nowIso
      })
    } else {
      skippedUnchangedRows += 1
    }
  }

  if (updates.length) {
    await marketSourceCatalogRepo.upsertRows(updates)
  }

  const excludedRowsByReason = {
    excludedLowValueItems: Number(counts.excludedLowValueItems || 0),
    excludedLowLiquidityItems: Number(counts.excludedLowLiquidityItems || 0),
    excludedWeakMarketCoverageItems: Number(counts.excludedWeakMarketCoverageItems || 0),
    excludedStaleItems: Number(counts.excludedStaleItems || 0),
    excludedMissingReferenceItems: Number(counts.excludedMissingReferenceItems || 0)
  }

  return {
    ...counts,
    catalogStatusCounts,
    blockedByReason,
    shadowByReason,
    total_catalog: Number(counts.totalCatalog || counts.totalRows || 0),
    active_tradable: Number(counts.activeTradable || counts.tradableRows || 0),
    blocked_by_reason: blockedByReason,
    shadow_by_reason: shadowByReason,
    scanner_source_size: Number(counts.scannerSourceSize || counts.scannable || 0),
    excludedRowsByReason,
    candidateFunnel,
    maturityFunnel,
    maturityFunnelByCategory,
    promotedToNearEligibleByCategory,
    promotedToEligibleByCategory,
    demotedToEnrichingByCategory,
    stuckInEnrichingByReason,
    stuckInEnrichingByReasonByCategory,
    blockedFromPromotionByReason,
    blockedFromPromotionByReasonByCategory,
    enrichingFreshnessByState,
    enrichingFreshnessByStateByCategory,
    nearEligibleFreshnessByState,
    nearEligibleFreshnessByStateByCategory,
    candidateFunnelByCategory: byCategory,
    byCategory,
    eligibleRowsByCategory,
    nearEligibleRowsByCategory,
    candidateRowsByCategory,
    enrichingRowsByCategory,
    priorityCoverage: {
      totalPriorityItemsConfigured: Number(priorityCoverage?.totalPriorityItemsConfigured || 0),
      matchedExistingCatalogItems: Number(priorityCoverage?.matchedExistingCatalogItems || 0),
      insertedMissingCatalogItems: Number(priorityCoverage?.insertedMissingCatalogItems || 0),
      unmatchedPriorityItems: Array.isArray(priorityCoverage?.unmatchedPriorityItems)
        ? priorityCoverage.unmatchedPriorityItems
        : [],
      error: normalizeText(priorityCoverage?.error) || null,
      scannablePriorityItemsByTier,
      shadowPriorityItemsByTier,
      blockedPriorityItemsByTier,
      scannerSourceCountsByTier
    },
    fullRebuildRows: rows.length,
    incrementalRecomputeRows: updates.length,
    incrementalSkippedRows: skippedUnchangedRows,
    persistedUpdateRows: updates.length,
    skippedUnchangedRows
  }
}

function takeTopByCategory(rows = [], quotas = {}) {
  const byCategory = Object.fromEntries(CATEGORY_PRIORITY.map((category) => [category, []]))

  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeCategory(row?.category, row?.market_hash_name)
    if (!isScannerScopeCategory(category)) continue
    byCategory[category].push(row)
  }

  const selected = []
  const used = new Set()
  const selectedByCategory = buildCategoryNumberMap()

  for (const category of CATEGORY_PRIORITY) {
    const bucket = byCategory[category]
    const quota = Math.max(Number(quotas[category] || 0), 0)
    for (const row of bucket.slice(0, quota)) {
      const name = normalizeText(row?.market_hash_name)
      if (!name || used.has(name)) continue
      used.add(name)
      selected.push(row)
      selectedByCategory[category] += 1
    }
  }

  const leftovers = rows.filter((row) => {
    const name = normalizeText(row?.market_hash_name)
    return name && !used.has(name)
  })

  return {
    selected,
    leftovers,
    selectedByCategory
  }
}

function normalizeCatalogCandidateRows(rows = [], selectionTier = "") {
  const forcedTier = normalizeText(selectionTier)
  function resolveSelectionTier(candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE) {
    if (forcedTier) return forcedTier
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE) return "strict_eligible"
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) return "candidate_near_eligible"
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) return "candidate_enriching"
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE) return "candidate_backfill"
    return "candidate_backfill"
  }
  function resolveTierRank(tier = "") {
    const normalizedTier = normalizeText(tier).toLowerCase()
    if (normalizedTier === "strict_eligible") return 3
    if (normalizedTier === "candidate_near_eligible") return 2.5
    if (normalizedTier === "candidate_enriching") return 2
    return 1
  }

  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName) return null
      const category = normalizeCategory(row?.category, marketHashName)
      if (!isScannerScopeCategory(category)) return null
      const scanEligible =
        row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible)
      const hasExplicitCandidateStatus = Boolean(
        normalizeText(row?.candidate_status ?? row?.candidateStatus)
      )
      const referencePrice = toFiniteOrNull(row?.reference_price ?? row?.referencePrice)
      const marketCoverageCount = Math.max(
        Number((row?.market_coverage_count ?? row?.marketCoverageCount) || 0),
        0
      )
      const snapshotStale =
        row?.snapshot_stale == null ? Boolean(row?.snapshotStale) : Boolean(row.snapshot_stale)
      const snapshotCapturedAt = normalizeText(
        row?.snapshot_captured_at || row?.snapshotCapturedAt
      )
      const missingSnapshot =
        row?.missing_snapshot == null
          ? !snapshotCapturedAt
          : Boolean(row.missing_snapshot)
      const missingReference =
        row?.missing_reference == null
          ? referencePrice == null
          : Boolean(row.missing_reference)
      const missingMarketCoverage =
        row?.missing_market_coverage == null
          ? marketCoverageCount <= 0
          : Boolean(row.missing_market_coverage)

      let candidateStatus = normalizeCandidateStatus(
        row?.candidate_status ?? row?.candidateStatus,
        scanEligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
      )
      if (
        !hasExplicitCandidateStatus &&
        !scanEligible
      ) {
        candidateStatus =
          missingSnapshot || missingReference || missingMarketCoverage
            ? CATALOG_CANDIDATE_STATUS.ENRICHING
            : CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
      }
      if (candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED) return null
      const maturityState = normalizeMaturityState(
        row?.maturity_state ?? row?.maturityState,
        candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
          ? CATALOG_MATURITY_STATE.ELIGIBLE
          : candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
            ? CATALOG_MATURITY_STATE.NEAR_ELIGIBLE
            : candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING
              ? CATALOG_MATURITY_STATE.ENRICHING
              : CATALOG_MATURITY_STATE.COLD
      )
      const tier = resolveSelectionTier(candidateStatus)
      return {
        ...row,
        market_hash_name: marketHashName,
        item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
        category,
        candidate_status: candidateStatus,
        scan_eligible: scanEligible,
        missing_snapshot: missingSnapshot,
        missing_reference: missingReference,
        missing_market_coverage: missingMarketCoverage,
        enrichment_priority: toFiniteOrNull(row?.enrichment_priority ?? row?.enrichmentPriority) ?? 0,
        maturity_state: maturityState,
        maturity_score: toFiniteOrNull(row?.maturity_score ?? row?.maturityScore) ?? 0,
        scan_layer: normalizeText(row?.scan_layer || row?.scanLayer) || resolveScanLayerForMaturityState(maturityState),
        liquidity_rank: toFiniteOrNull(row?.liquidity_rank ?? row?.liquidityRank) ?? 0,
        market_coverage_count: marketCoverageCount,
        volume_7d: toFiniteOrNull(row?.volume_7d ?? row?.volume7d),
        reference_price: referencePrice,
        snapshot_stale: snapshotStale,
        snapshot_captured_at: snapshotCapturedAt || null,
        quote_fetched_at: normalizeText(row?.quote_fetched_at || row?.quoteFetchedAt) || null,
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
        priority_set_name: normalizeText(row?.priority_set_name || row?.prioritySetName) || null,
        priority_tier: normalizePriorityTier(row?.priority_tier || row?.priorityTier, null),
        priority_rank: normalizeIntegerOrNull(row?.priority_rank ?? row?.priorityRank, 1),
        priority_boost: toFiniteOrNull(row?.priority_boost ?? row?.priorityBoost) ?? 0,
        is_priority_item:
          row?.is_priority_item == null
            ? normalizePriorityTier(row?.priority_tier || row?.priorityTier, null) != null
            : Boolean(row.is_priority_item),
        selectionTier: tier,
        selectionTierRank: resolveTierRank(tier)
      }
    })
    .filter(Boolean)
}

function dedupeByMarketHashName(rows = []) {
  const deduped = []
  const seen = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    const name = normalizeText(row?.market_hash_name)
    if (!name || seen.has(name)) continue
    seen.add(name)
    deduped.push(row)
  }
  return deduped
}

function countCatalogRowsByCategory(rows = []) {
  const counts = buildCategoryNumberMap()
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeCategory(
      row?.category || row?.itemCategory,
      row?.market_hash_name || row?.marketHashName || row?.item_name || row?.itemName
    )
    if (!isScannerScopeCategory(category)) continue
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

async function rebuildUniverseFromCatalog(targetSize = DEFAULT_UNIVERSE_TARGET) {
  const safeTarget = Math.max(Math.round(Number(targetSize || DEFAULT_UNIVERSE_TARGET)), 1)
  const strictEligibleRows = normalizeCatalogCandidateRows(
    await marketSourceCatalogRepo.listScanEligible({
      limit: Math.max(SOURCE_CATALOG_LIMIT, safeTarget * 3),
      categories: CATEGORY_PRIORITY
    }),
    "strict_eligible"
  )
  const candidatePoolRows = normalizeCatalogCandidateRows(
    await marketSourceCatalogRepo.listCandidatePool({
      limit: Math.max(SOURCE_CATALOG_LIMIT, safeTarget * 3),
      categories: CATEGORY_PRIORITY,
      candidateStatuses: [
        CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE,
        CATALOG_CANDIDATE_STATUS.ENRICHING,
        CATALOG_CANDIDATE_STATUS.CANDIDATE
      ]
    })
  )
  const backfillReadyCandidatePoolRows = candidatePoolRows.filter(isUniverseBackfillReadyRow)
  const backfillBlockedRows = candidatePoolRows.filter((row) => !isUniverseBackfillReadyRow(row))
  const nearEligibleRows = backfillReadyCandidatePoolRows.filter(
    (row) => normalizeCandidateStatus(row?.candidate_status) === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
  )
  const enrichingRows = backfillReadyCandidatePoolRows.filter(
    (row) => normalizeCandidateStatus(row?.candidate_status) === CATALOG_CANDIDATE_STATUS.ENRICHING
  )
  const candidateRows = backfillReadyCandidatePoolRows.filter(
    (row) => normalizeCandidateStatus(row?.candidate_status) === CATALOG_CANDIDATE_STATUS.CANDIDATE
  )

  const rankedRows = dedupeByMarketHashName([
    ...strictEligibleRows,
    ...nearEligibleRows,
    ...enrichingRows,
    ...candidateRows
  ])
    .sort(
      (a, b) =>
        Number(b.selectionTierRank || 0) - Number(a.selectionTierRank || 0) ||
        Number(b.enrichment_priority || 0) - Number(a.enrichment_priority || 0) ||
        Number(b.liquidity_rank || 0) - Number(a.liquidity_rank || 0) ||
        Number(b.market_coverage_count || 0) - Number(a.market_coverage_count || 0) ||
        Number(b.volume_7d || 0) - Number(a.volume_7d || 0) ||
        Number(b.reference_price || 0) - Number(a.reference_price || 0)
    )

  const quotas = buildCategoryQuotas(safeTarget)
  const { selected, leftovers, selectedByCategory } = takeTopByCategory(rankedRows, quotas)
  const selectedByCategoryQuotaStage = buildCategoryNumberMap()
  for (const category of CATEGORY_PRIORITY) {
    selectedByCategoryQuotaStage[category] = Number(selectedByCategory[category] || 0)
  }

  const finalRows = [...selected]
  for (const row of leftovers) {
    if (finalRows.length >= safeTarget) break
    finalRows.push(row)
    const category = normalizeCategory(row?.category, row?.market_hash_name)
    if (isScannerScopeCategory(category)) {
      selectedByCategory[category] = Number(selectedByCategory[category] || 0) + 1
    }
  }

  const selectedFromStrict = finalRows.filter((row) => row.selectionTier === "strict_eligible").length
  const selectedFromNearEligible = finalRows.filter(
    (row) => row.selectionTier === "candidate_near_eligible"
  ).length
  const selectedFromEnriching = finalRows.filter(
    (row) => row.selectionTier === "candidate_enriching"
  ).length
  const selectedFromCandidate = finalRows.filter(
    (row) => row.selectionTier === "candidate_backfill"
  ).length
  const selectedFromFallback = selectedFromNearEligible + selectedFromEnriching + selectedFromCandidate
  const candidateBackfillUsed = selectedFromFallback > 0

  const normalizedUniverseRows = finalRows.slice(0, safeTarget).map((row, index) => ({
    marketHashName: row.market_hash_name,
    itemName: row.item_name || row.market_hash_name,
    category: normalizeCategory(row?.category, row?.market_hash_name),
    subcategory: normalizeText(row?.subcategory) || null,
    liquidityRank: index + 1
  }))

  const universeComparisonLimit = Math.min(Math.max(safeTarget * 3, safeTarget + 250), 10000)
  let existingUniverseRows = []
  try {
    existingUniverseRows = await marketUniverseRepo.listActiveByLiquidityRank({
      limit: universeComparisonLimit
    })
  } catch (_err) {
    existingUniverseRows = []
  }

  const unchangedUniverse =
    existingUniverseRows.length > 0 && isSameUniverseRows(existingUniverseRows, normalizedUniverseRows)
  const persist = unchangedUniverse
    ? { deactivated: 0, upserted: 0, skipped: true }
    : {
        ...(await marketUniverseRepo.replaceActiveUniverse(normalizedUniverseRows)),
        skipped: false
      }
  const quotaShortfallByCategory = buildCategoryNumberMap()
  const quotaOverflowByCategory = buildCategoryNumberMap()
  const quotaReallocationByCategory = buildCategoryNumberMap()
  for (const category of CATEGORY_PRIORITY) {
    const quota = Number(quotas[category] || 0)
    const selectedQuotaStage = Number(selectedByCategoryQuotaStage[category] || 0)
    const selectedFinal = Number(selectedByCategory[category] || 0)
    const shortfall = Math.max(quota - selectedQuotaStage, 0)
    const overflow = Math.max(selectedFinal - quota, 0)
    quotaShortfallByCategory[category] = shortfall
    quotaOverflowByCategory[category] = overflow
    quotaReallocationByCategory[category] = selectedFinal - quota
  }

  return {
    targetUniverseSize: safeTarget,
    eligibleRows: strictEligibleRows.length,
    nearEligibleRows: nearEligibleRows.length,
    candidateRows: candidateRows.length,
    enrichingRows: enrichingRows.length,
    strictEligibleRows: strictEligibleRows.length,
    fallbackTradableRows: backfillReadyCandidatePoolRows.length,
    selectedFromStrict,
    selectedFromEligible: selectedFromStrict,
    selectedFromNearEligible,
    selectedFromEnriching,
    selectedFromCandidate,
    selectedFromFallback,
    backfillReadyRows: backfillReadyCandidatePoolRows.length,
    backfillBlockedRows: backfillBlockedRows.length,
    backfillBlockedRowsByCategory: countCatalogRowsByCategory(backfillBlockedRows),
    activeUniverseBuilt: normalizedUniverseRows.length,
    missingToTarget: Math.max(safeTarget - normalizedUniverseRows.length, 0),
    quotaTargetByCategory: quotas,
    selectedByCategory,
    selectedByCategoryQuotaStage,
    quotaShortfallByCategory,
    quotaOverflowByCategory,
    quotaReallocationByCategory,
    reallocatedSlots: Math.max(finalRows.length - selected.length, 0),
    eligibleRowsByCategory: countCatalogRowsByCategory(strictEligibleRows),
    nearEligibleRowsByCategory: countCatalogRowsByCategory(nearEligibleRows),
    candidateRowsByCategory: countCatalogRowsByCategory(candidateRows),
    enrichingRowsByCategory: countCatalogRowsByCategory(enrichingRows),
    candidateBackfillUsed,
    seedPromotionActive: candidateBackfillUsed,
    quotas,
    fallbackToMaxEligible: candidateBackfillUsed,
    persisted: persist
  }
}

async function runPipeline(options = {}) {
  const startedAt = Date.now()
  const targetUniverseSize = Math.max(
    Math.round(Number(options.targetUniverseSize || DEFAULT_UNIVERSE_TARGET)),
    1
  )
  const base = buildBaseDiagnostics()
  base.targetUniverseSize = targetUniverseSize

  const ingest = await ingestSourceCatalogSeeds()
  const priorityCoverage = await catalogPriorityCoverageService
    .syncPriorityCoverageSet()
    .catch((err) => ({
      setName: null,
      version: 1,
      description: null,
      totalPriorityItemsConfigured: 0,
      matchedExistingCatalogItems: 0,
      insertedMissingCatalogItems: 0,
      unmatchedPriorityItems: [],
      entries: [],
      byKey: new Map(),
      policyHintsByTier: {},
      error: normalizeText(err?.message) || "priority_coverage_sync_failed"
    }))
  const sourceCoverage = await enrichSourceCatalog({ priorityCoverage })
  const universeBuild = await rebuildUniverseFromCatalog(targetUniverseSize)

  return {
    ...base,
    generatedAt: new Date().toISOString(),
    refreshed: true,
    skipped: false,
    elapsedMs: Date.now() - startedAt,
    sourceCatalog: {
      ...base.sourceCatalog,
      targetRows: SOURCE_CATALOG_LIMIT,
      totalRows: Number(sourceCoverage?.totalRows || 0),
      totalCatalog: Number(sourceCoverage?.totalCatalog || sourceCoverage?.totalRows || 0),
      total_catalog: Number(
        sourceCoverage?.total_catalog || sourceCoverage?.totalCatalog || sourceCoverage?.totalRows || 0
      ),
      seededRows: Number(ingest?.seededRows || 0),
      sourceCandidateRows: Number(ingest?.sourceCandidateRows || 0),
      selectedSeedRowsByCategory:
        ingest?.selectedSeedRowsByCategory || base.sourceCatalog.selectedSeedRowsByCategory,
      sourceCandidateRowsByCategory:
        ingest?.sourceCandidateRowsByCategory || base.sourceCatalog.sourceCandidateRowsByCategory,
      sourceExcludedRowsByReason:
        ingest?.sourceExcludedRowsByReason || base.sourceCatalog.sourceExcludedRowsByReason,
      sourceCatalogQuotaTargetByCategory:
        ingest?.sourceCatalogQuotaTargetByCategory || base.sourceCatalog.sourceCatalogQuotaTargetByCategory,
      sourceCatalogQuotaStageByCategory:
        ingest?.sourceCatalogQuotaStageByCategory || base.sourceCatalog.sourceCatalogQuotaStageByCategory,
      sourceCatalogQuotaShortfallByCategory:
        ingest?.sourceCatalogQuotaShortfallByCategory || base.sourceCatalog.sourceCatalogQuotaShortfallByCategory,
      sourceCatalogQuotaReallocationByCategory:
        ingest?.sourceCatalogQuotaReallocationByCategory || base.sourceCatalog.sourceCatalogQuotaReallocationByCategory,
      missingRowsToTarget: Number(ingest?.missingRowsToTarget || 0),
      missingRowsToTargetByCategory:
        ingest?.missingRowsToTargetByCategory || base.sourceCatalog.missingRowsToTargetByCategory,
      activeCatalogRows: Number(sourceCoverage?.activeCatalogRows || 0),
      activeTradable: Number(sourceCoverage?.activeTradable || sourceCoverage?.tradableRows || 0),
      active_tradable: Number(
        sourceCoverage?.active_tradable || sourceCoverage?.activeTradable || sourceCoverage?.tradableRows || 0
      ),
      scannable: Number(sourceCoverage?.scannable || 0),
      shadow: Number(sourceCoverage?.shadow || 0),
      blocked: Number(sourceCoverage?.blocked || 0),
      blockedByReason:
        sourceCoverage?.blockedByReason || base.sourceCatalog.blockedByReason,
      shadowByReason:
        sourceCoverage?.shadowByReason || base.sourceCatalog.shadowByReason,
      blocked_by_reason:
        sourceCoverage?.blocked_by_reason || sourceCoverage?.blockedByReason || base.sourceCatalog.blocked_by_reason,
      shadow_by_reason:
        sourceCoverage?.shadow_by_reason || sourceCoverage?.shadowByReason || base.sourceCatalog.shadow_by_reason,
      catalogStatusCounts:
        sourceCoverage?.catalogStatusCounts || base.sourceCatalog.catalogStatusCounts,
      scannerSourceSize: Number(sourceCoverage?.scannerSourceSize || sourceCoverage?.scannable || 0),
      scanner_source_size: Number(
        sourceCoverage?.scanner_source_size ||
          sourceCoverage?.scannerSourceSize ||
          sourceCoverage?.scannable ||
          0
      ),
      tradableRows: Number(sourceCoverage?.tradableRows || 0),
      candidateRows: Number(sourceCoverage?.candidateRows || 0),
      enrichingRows: Number(sourceCoverage?.enrichingRows || 0),
      nearEligibleRows: Number(sourceCoverage?.nearEligibleRows || 0),
      coldRows: Number(sourceCoverage?.coldRows || 0),
      eligibleRows: Number(sourceCoverage?.eligibleRows || 0),
      rejectedRows: Number(sourceCoverage?.rejectedRows || 0),
      eligibleTradableRows: Number(sourceCoverage?.eligibleTradableRows || 0),
      excludedLowValueItems: Number(sourceCoverage?.excludedLowValueItems || 0),
      excludedLowLiquidityItems: Number(sourceCoverage?.excludedLowLiquidityItems || 0),
      excludedWeakMarketCoverageItems: Number(sourceCoverage?.excludedWeakMarketCoverageItems || 0),
      excludedStaleItems: Number(sourceCoverage?.excludedStaleItems || 0),
      excludedMissingReferenceItems: Number(sourceCoverage?.excludedMissingReferenceItems || 0),
      excludedRowsByReason:
        sourceCoverage?.excludedRowsByReason || base.sourceCatalog.excludedRowsByReason,
      candidateFunnel:
        sourceCoverage?.candidateFunnel || base.sourceCatalog.candidateFunnel,
      maturityFunnel: sourceCoverage?.maturityFunnel || base.sourceCatalog.maturityFunnel,
      maturityFunnelByCategory:
        sourceCoverage?.maturityFunnelByCategory || base.sourceCatalog.maturityFunnelByCategory,
      promotedToNearEligible: Number(
        sourceCoverage?.promotedToNearEligible || base.sourceCatalog.promotedToNearEligible || 0
      ),
      promotedToEligible: Number(
        sourceCoverage?.promotedToEligible || base.sourceCatalog.promotedToEligible || 0
      ),
      demotedToEnriching: Number(
        sourceCoverage?.demotedToEnriching || base.sourceCatalog.demotedToEnriching || 0
      ),
      promotedToNearEligibleByCategory:
        sourceCoverage?.promotedToNearEligibleByCategory ||
        base.sourceCatalog.promotedToNearEligibleByCategory,
      promotedToEligibleByCategory:
        sourceCoverage?.promotedToEligibleByCategory ||
        base.sourceCatalog.promotedToEligibleByCategory,
      demotedToEnrichingByCategory:
        sourceCoverage?.demotedToEnrichingByCategory ||
        base.sourceCatalog.demotedToEnrichingByCategory,
      stuckInEnrichingByReason:
        sourceCoverage?.stuckInEnrichingByReason || base.sourceCatalog.stuckInEnrichingByReason,
      stuckInEnrichingByReasonByCategory:
        sourceCoverage?.stuckInEnrichingByReasonByCategory ||
        base.sourceCatalog.stuckInEnrichingByReasonByCategory,
      blockedFromPromotionByReason:
        sourceCoverage?.blockedFromPromotionByReason ||
        base.sourceCatalog.blockedFromPromotionByReason,
      blockedFromPromotionByReasonByCategory:
        sourceCoverage?.blockedFromPromotionByReasonByCategory ||
        base.sourceCatalog.blockedFromPromotionByReasonByCategory,
      enrichingFreshnessByState:
        sourceCoverage?.enrichingFreshnessByState || base.sourceCatalog.enrichingFreshnessByState,
      enrichingFreshnessByStateByCategory:
        sourceCoverage?.enrichingFreshnessByStateByCategory ||
        base.sourceCatalog.enrichingFreshnessByStateByCategory,
      nearEligibleFreshnessByState:
        sourceCoverage?.nearEligibleFreshnessByState ||
        base.sourceCatalog.nearEligibleFreshnessByState,
      nearEligibleFreshnessByStateByCategory:
        sourceCoverage?.nearEligibleFreshnessByStateByCategory ||
        base.sourceCatalog.nearEligibleFreshnessByStateByCategory,
      candidateFunnelByCategory:
        sourceCoverage?.candidateFunnelByCategory || base.sourceCatalog.candidateFunnelByCategory,
      eligibleRowsByCategory:
        sourceCoverage?.eligibleRowsByCategory || base.sourceCatalog.eligibleRowsByCategory,
      nearEligibleRowsByCategory:
        sourceCoverage?.nearEligibleRowsByCategory || base.sourceCatalog.nearEligibleRowsByCategory,
      candidateRowsByCategory:
        sourceCoverage?.candidateRowsByCategory || base.sourceCatalog.candidateRowsByCategory,
      enrichingRowsByCategory:
        sourceCoverage?.enrichingRowsByCategory || base.sourceCatalog.enrichingRowsByCategory,
      priorityCoverage:
        sourceCoverage?.priorityCoverage || base.sourceCatalog.priorityCoverage,
      fullRebuildRows: Number(sourceCoverage?.fullRebuildRows || sourceCoverage?.totalRows || 0),
      incrementalRecomputeRows: Number(
        sourceCoverage?.incrementalRecomputeRows || sourceCoverage?.persistedUpdateRows || 0
      ),
      incrementalSkippedRows: Number(
        sourceCoverage?.incrementalSkippedRows || sourceCoverage?.skippedUnchangedRows || 0
      ),
      byCategory: sourceCoverage?.byCategory || base.sourceCatalog.byCategory
    },
    universeBuild
  }
}

function shouldRefresh(force = false) {
  if (force) return true
  if (!sourceCatalogState.lastPreparedAt) return true
  if (sourceCatalogState.lastDiagnostics?.error) {
    return Date.now() - sourceCatalogState.lastPreparedAt >= SOURCE_CATALOG_ERROR_RETRY_MS
  }
  return Date.now() - sourceCatalogState.lastPreparedAt >= SOURCE_CATALOG_REFRESH_MS
}

function shouldBypassSkipForRecovery(diagnostics = {}, targetUniverseSize = DEFAULT_UNIVERSE_TARGET) {
  const sourceCatalog =
    diagnostics?.sourceCatalog && typeof diagnostics.sourceCatalog === "object"
      ? diagnostics.sourceCatalog
      : {}
  const universeBuild =
    diagnostics?.universeBuild && typeof diagnostics.universeBuild === "object"
      ? diagnostics.universeBuild
      : {}

  const activeCatalogRows = Math.max(
    Number(sourceCatalog?.activeCatalogRows || sourceCatalog?.totalRows || 0),
    0
  )
  const eligibleRows = Math.max(
    Number(sourceCatalog?.eligibleTradableRows || sourceCatalog?.eligibleRows || 0),
    0
  )
  const candidateRows = Math.max(Number(sourceCatalog?.candidateRows || 0), 0)
  const enrichingRows = Math.max(Number(sourceCatalog?.enrichingRows || 0), 0)
  const nearEligibleRows = Math.max(Number(sourceCatalog?.nearEligibleRows || 0), 0)
  const rejectedRows = Math.max(Number(sourceCatalog?.rejectedRows || 0), 0)
  const activeUniverseBuilt = Math.max(Number(universeBuild?.activeUniverseBuilt || 0), 0)
  const targetSize = Math.max(
    Math.round(
      Number(
        universeBuild?.targetUniverseSize || diagnostics?.targetUniverseSize || targetUniverseSize
      )
    ),
    1
  )
  const missingToTarget = Math.max(
    Number(universeBuild?.missingToTarget ?? Math.max(targetSize - activeUniverseBuilt, 0)),
    0
  )
  const hasCollapsedFunnel =
    activeCatalogRows > 250 &&
    eligibleRows > 0 &&
    candidateRows === 0 &&
    enrichingRows === 0 &&
    nearEligibleRows === 0 &&
    rejectedRows === 0
  const hasCollapsedUniverse =
    activeUniverseBuilt > 0 &&
    activeUniverseBuilt <= Math.max(eligibleRows, 1) &&
    missingToTarget > Math.max(Math.round(targetSize * 0.5), 500)

  return hasCollapsedFunnel || hasCollapsedUniverse
}

async function prepareSourceCatalog(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const targetUniverseSize = Number(options.targetUniverseSize || DEFAULT_UNIVERSE_TARGET)

  if (
    !shouldRefresh(forceRefresh) &&
    sourceCatalogState.lastDiagnostics &&
    !shouldBypassSkipForRecovery(sourceCatalogState.lastDiagnostics, targetUniverseSize)
  ) {
    return {
      ...sourceCatalogState.lastDiagnostics,
      refreshed: false,
      skipped: true
    }
  }

  if (sourceCatalogState.inFlight) {
    return sourceCatalogState.inFlight
  }

  sourceCatalogState.inFlight = runPipeline({ targetUniverseSize })
    .then((diagnostics) => {
      sourceCatalogState.lastPreparedAt = Date.now()
      sourceCatalogState.lastDiagnostics = diagnostics
      sourceCatalogState.lastSuccessfulDiagnostics = diagnostics
      return diagnostics
    })
    .catch((err) => {
      const safeTarget = Math.max(
        Math.round(Number(targetUniverseSize || DEFAULT_UNIVERSE_TARGET)),
        1
      )
      const errorMessage = String(err?.message || "source_catalog_pipeline_failed")
      const lastSuccessful = sourceCatalogState.lastSuccessfulDiagnostics
      if (lastSuccessful && typeof lastSuccessful === "object") {
        const degraded = {
          ...lastSuccessful,
          generatedAt: new Date().toISOString(),
          refreshed: false,
          skipped: true,
          error: errorMessage,
          staleDiagnosticsRetained: true
        }
        sourceCatalogState.lastPreparedAt = Date.now()
        sourceCatalogState.lastDiagnostics = degraded
        return degraded
      }

      const fallback = {
        ...buildBaseDiagnostics(),
        generatedAt: new Date().toISOString(),
        targetUniverseSize: safeTarget,
        refreshed: false,
        skipped: false,
        error: errorMessage
      }
      fallback.universeBuild = {
        ...fallback.universeBuild,
        targetUniverseSize: safeTarget,
        activeUniverseBuilt: 0,
        missingToTarget: safeTarget,
        quotas: buildCategoryQuotas(safeTarget),
        quotaTargetByCategory: buildCategoryQuotas(safeTarget),
        fallbackToMaxEligible: false
      }
      sourceCatalogState.lastPreparedAt = Date.now()
      sourceCatalogState.lastDiagnostics = fallback
      return fallback
    })
    .finally(() => {
      sourceCatalogState.inFlight = null
    })

  return sourceCatalogState.inFlight
}

function getLastDiagnostics() {
  return sourceCatalogState.lastDiagnostics || buildBaseDiagnostics()
}

async function getCatalogRowsByMarketHashNames(marketHashNames = [], options = {}) {
  return marketSourceCatalogRepo.listByMarketHashNames(marketHashNames, {
    categories: Array.isArray(options.categories) ? options.categories : CATEGORY_PRIORITY,
    activeOnly: options.activeOnly !== false,
    tradableOnly: options.tradableOnly !== false
  })
}

module.exports = {
  prepareSourceCatalog,
  getLastDiagnostics,
  getCatalogRowsByMarketHashNames,
  __testables: {
    normalizeCategory,
    normalizeCandidateStatus,
    normalizeMaturityState,
    computeSourceLiquidityScore,
    computeEnrichmentPriority,
    computeCatalogMaturity,
    computeCatalogQualityScore,
    classifyCatalogStatus,
    evaluateCandidateState,
    evaluateEligibility,
    isUniverseBackfillReadyRow,
    buildCategoryQuotas,
    buildSourceCatalogQuotas,
    resolveVolume7d,
    shouldBypassSkipForRecovery
  }
}

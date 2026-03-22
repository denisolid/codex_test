const premiumCategoryAccessService = require("../premiumCategoryAccessService")
const {
  CATEGORY_PROFILES,
  ITEM_CATEGORIES,
  SCAN_STATE,
  SUPPORTED_SCAN_CATEGORIES
} = require("./config")

const LOW_VALUE_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i,
  /\|\s*(Sand Spray|Sand Dune|Grey Smoke|Coolant|Mudder|Gator Mesh|Orange Peel|Mandrel)\b/i
])

const STRUCTURAL_INVALID_REASON_PATTERNS = Object.freeze([
  /invalid/i,
  /rejected/i,
  /anti[_\s-]?fake/i,
  /not[_\s-]?tradable/i,
  /broken/i
])

const FRESHNESS_RULES_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({ freshMaxMinutes: 45, agingMaxMinutes: 120 }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({ freshMaxMinutes: 60, agingMaxMinutes: 180 }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    freshMaxMinutes: 90,
    agingMaxMinutes: 240
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({ freshMaxMinutes: 120, agingMaxMinutes: 240 }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({ freshMaxMinutes: 120, agingMaxMinutes: 240 }),
  [ITEM_CATEGORIES.FUTURE_KNIFE]: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240
  }),
  [ITEM_CATEGORIES.FUTURE_GLOVE]: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240
  })
})
const MIN_SCAN_COST_USD = 2

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
  return parsed != null && parsed > 0 ? parsed : null
}

function resolveSeedVolume7d(seed = {}) {
  return (
    toPositiveOrNull(
      seed?.sellVolume7d ??
        seed?.sell_volume_7d ??
        seed?.sellRouteVolume7d ??
        seed?.sell_route_volume_7d
    ) ??
    toPositiveOrNull(seed?.marketMaxVolume7d ?? seed?.market_max_volume_7d) ??
    toPositiveOrNull(seed?.buyVolume7d ?? seed?.buy_volume_7d) ??
    toPositiveOrNull(seed?.volume7d ?? seed?.volume_7d)
  )
}

function toIsoOrNull(value) {
  if (value == null || value === "") return null
  if (value instanceof Date) {
    const directTs = value.getTime()
    if (Number.isFinite(directTs)) return new Date(directTs).toISOString()
    return null
  }

  const numeric = toFiniteOrNull(value)
  if (numeric != null) {
    const normalizedTs =
      numeric >= 1e12
        ? Math.round(numeric)
        : numeric >= 1e9
          ? Math.round(numeric * 1000)
          : null
    if (normalizedTs != null) {
      const numericTs = new Date(normalizedTs).getTime()
      if (Number.isFinite(numericTs)) return new Date(numericTs).toISOString()
    }
  }

  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function resolveLatestIso(values = []) {
  const sorted = (Array.isArray(values) ? values : [])
    .map((value) => toIsoOrNull(value))
    .filter(Boolean)
    .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())
  return sorted[0] || null
}

function resolveReferenceSignalAt(seed = {}, quoteIso = null, snapshotIso = null) {
  if (toFiniteOrNull(seed?.referencePrice ?? seed?.reference_price) == null) return null
  const directReferenceIso = resolveLatestIso([
    seed?.latestReferencePriceAt,
    seed?.latest_reference_price_at,
    seed?.referencePriceAt,
    seed?.reference_price_at
  ])
  if (directReferenceIso) return directReferenceIso

  const referenceState = normalizeText(seed?.referenceState || seed?.reference_state).toLowerCase()
  if (referenceState === "snapshot") return snapshotIso
  if (referenceState === "quote") return quoteIso
  return resolveLatestIso([quoteIso, snapshotIso])
}

function resolveLatestMarketSignalAt(seed = {}) {
  const snapshotIso = resolveLatestIso([seed?.snapshotCapturedAt, seed?.snapshot_captured_at])
  const quoteIso = resolveLatestIso([seed?.quoteFetchedAt, seed?.quote_fetched_at])
  const referenceIso = resolveReferenceSignalAt(seed, quoteIso, snapshotIso)
  const directLatestSignalIso = resolveLatestIso([
    seed?.latestMarketSignalAt,
    seed?.latest_market_signal_at,
    seed?.lastMarketSignalAt,
    seed?.last_market_signal_at
  ])
  return (
    resolveLatestIso([snapshotIso, quoteIso, referenceIso]) ||
    directLatestSignalIso ||
    null
  )
}

function normalizeCategory(value, fallbackName = "") {
  const raw = normalizeText(value).toLowerCase()
  if (raw === ITEM_CATEGORIES.FUTURE_KNIFE) return ITEM_CATEGORIES.FUTURE_KNIFE
  if (raw === ITEM_CATEGORIES.FUTURE_GLOVE) return ITEM_CATEGORIES.FUTURE_GLOVE
  return premiumCategoryAccessService.normalizeItemCategory(raw, fallbackName)
}

function normalizeToSupportedCategory(value, fallbackName = "") {
  const normalized = normalizeCategory(value, fallbackName)
  if (!SUPPORTED_SCAN_CATEGORIES.includes(normalized)) {
    return ITEM_CATEGORIES.WEAPON_SKIN
  }
  if (normalized === ITEM_CATEGORIES.FUTURE_KNIFE) return ITEM_CATEGORIES.KNIFE
  if (normalized === ITEM_CATEGORIES.FUTURE_GLOVE) return ITEM_CATEGORIES.GLOVE
  return normalized
}

function getCategoryProfile(category = ITEM_CATEGORIES.WEAPON_SKIN) {
  return CATEGORY_PROFILES[category] || CATEGORY_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function resolveFreshnessAgeMinutes(seed = {}) {
  const newestIso = resolveLatestMarketSignalAt(seed)
  if (!newestIso) return null
  const ageMinutes = (Date.now() - new Date(newestIso).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return Number(ageMinutes.toFixed(2))
}

function resolveFreshnessState(seed = {}, category = ITEM_CATEGORIES.WEAPON_SKIN) {
  const rules = FRESHNESS_RULES_BY_CATEGORY[category] || FRESHNESS_RULES_BY_CATEGORY.weapon_skin
  const ageMinutes = resolveFreshnessAgeMinutes(seed)
  const latestMarketSignalAt = resolveLatestMarketSignalAt(seed)
  if (ageMinutes == null) {
    return {
      state: "missing",
      ageMinutes: null,
      latestMarketSignalAt: null,
      staleThresholdMinutes: Number(rules.agingMaxMinutes || 0)
    }
  }
  if (ageMinutes <= Number(rules.freshMaxMinutes || 0)) {
    return {
      state: "fresh",
      ageMinutes,
      latestMarketSignalAt,
      staleThresholdMinutes: Number(rules.agingMaxMinutes || 0)
    }
  }
  if (ageMinutes <= Number(rules.agingMaxMinutes || 0)) {
    return {
      state: "aging",
      ageMinutes,
      latestMarketSignalAt,
      staleThresholdMinutes: Number(rules.agingMaxMinutes || 0)
    }
  }
  return {
    state: "stale",
    ageMinutes,
    latestMarketSignalAt,
    staleThresholdMinutes: Number(rules.agingMaxMinutes || 0)
  }
}

function isLowValueFlagged(seed = {}, category = ITEM_CATEGORIES.WEAPON_SKIN) {
  if (category !== ITEM_CATEGORIES.WEAPON_SKIN) return false
  const marketHashName = normalizeText(seed.marketHashName || seed.market_hash_name || seed.itemName)
  if (!marketHashName) return false
  const profile = getCategoryProfile(category)
  const referencePrice = toFiniteOrNull(seed.referencePrice ?? seed.reference_price)
  const hasLowValuePattern = LOW_VALUE_NAME_PATTERNS.some((pattern) => pattern.test(marketHashName))
  if (!hasLowValuePattern) return false
  if (referencePrice == null) return true
  return referencePrice < Number(profile.minPriceUsd || 0) * 1.4
}

function hasStructuralInvalidReason(seed = {}) {
  const reason = normalizeText(seed.invalidReason || seed.invalid_reason)
  if (!reason) return false
  return STRUCTURAL_INVALID_REASON_PATTERNS.some((pattern) => pattern.test(reason))
}

function buildPenaltyFlags(seed = {}, category = ITEM_CATEGORIES.WEAPON_SKIN) {
  const flags = []
  const profile = getCategoryProfile(category)
  const volume7d = resolveSeedVolume7d(seed)
  const referencePrice = toFiniteOrNull(seed.referencePrice ?? seed.reference_price)
  const marketCoverageCount = Math.max(
    Number(toFiniteOrNull(seed.marketCoverageCount ?? seed.market_coverage_count) || 0),
    0
  )
  const freshness = resolveFreshnessState(seed, category)

  if (volume7d == null || volume7d <= 0) {
    flags.push("missing_liquidity")
  }
  if (marketCoverageCount < Number(profile.minMarketCoverage || 2)) {
    flags.push("weak_coverage")
  }
  if (freshness.state === "aging" || freshness.state === "stale" || freshness.state === "missing") {
    flags.push("aging_data")
  }
  if (referencePrice == null || referencePrice <= 0) {
    flags.push("weak_coverage")
  } else if (referencePrice < Number(profile.minPriceUsd || 0)) {
    flags.push("low_price")
  }
  if (isLowValueFlagged(seed, category)) {
    flags.push("low_value_flag")
  }

  return Array.from(new Set(flags))
}

function classifyCatalogState(seed = {}) {
  const marketHashName = normalizeText(seed.marketHashName || seed.market_hash_name)
  const itemName = normalizeText(seed.itemName || seed.item_name || marketHashName)
  const category = normalizeToSupportedCategory(seed.category || seed.itemCategory, itemName)
  const hardRejectReasons = []

  if (!marketHashName) {
    hardRejectReasons.push("invalid_row")
  }
  if (!SUPPORTED_SCAN_CATEGORIES.includes(normalizeCategory(category, itemName))) {
    hardRejectReasons.push("unsupported_category")
  }
  if (seed.isActive === false || seed.is_active === false || seed.tradable === false) {
    hardRejectReasons.push("inactive_or_untradable")
  }
  if (hasStructuralInvalidReason(seed)) {
    hardRejectReasons.push("invalid_catalog_reason")
  }

  const referencePrice = toFiniteOrNull(seed.referencePrice ?? seed.reference_price)
  if (referencePrice != null && referencePrice < MIN_SCAN_COST_USD) {
    hardRejectReasons.push("below_min_cost_floor")
  }
  const marketCoverageCount = Math.max(
    Number(toFiniteOrNull(seed.marketCoverageCount ?? seed.market_coverage_count) || 0),
    0
  )
  const hasFreshnessSignal = Boolean(resolveLatestMarketSignalAt(seed))
  if (marketCoverageCount <= 0 && referencePrice == null && !hasFreshnessSignal) {
    hardRejectReasons.push("unusable_market_coverage")
  }

  const penaltyFlags = buildPenaltyFlags(seed, category)
  const state = hardRejectReasons.length
    ? SCAN_STATE.HARD_REJECT
    : penaltyFlags.length
      ? SCAN_STATE.SCANABLE_WITH_PENALTIES
      : SCAN_STATE.SCANABLE

  return {
    state,
    category,
    marketHashName,
    itemName: itemName || marketHashName,
    hardRejectReasons,
    penaltyFlags,
    freshness: resolveFreshnessState(seed, category),
    profile: getCategoryProfile(category)
  }
}

module.exports = {
  normalizeCategory: normalizeToSupportedCategory,
  getCategoryProfile,
  resolveFreshnessState,
  isLowValueFlagged,
  buildPenaltyFlags,
  classifyCatalogState,
  SCAN_STATE
}

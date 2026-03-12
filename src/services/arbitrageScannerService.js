const { round2, roundPrice } = require("../markets/marketUtils")
const {
  arbitrageScannerIntervalMinutes,
  arbitrageDefaultUniverseLimit,
  arbitrageScannerUniverseTargetSize,
  arbitrageScanBatchSize,
  arbitrageMaxConcurrentMarketRequests,
  arbitrageScanTimeoutPerBatchMs,
  arbitrageImageEnrichBatchSize,
  arbitrageImageEnrichConcurrency,
  arbitrageImageEnrichTimeoutMs,
  arbitrageQuoteRefreshBatchSize,
  arbitrageQuoteComputeBatchSize,
  arbitrageUniverseDbLimit,
  marketSnapshotTtlMinutes,
  arbitrageFeedRetentionHours,
  arbitrageFeedActiveLimit,
  arbitrageDuplicateWindowHours,
  arbitrageMinProfitChangePct,
  arbitrageMinScoreChange,
  arbitrageInsertDuplicates
} = require("../config/env")
const marketUniverseTop100 = require("../config/marketUniverseTop100.json")
const skinRepo = require("../repositories/skinRepository")
const marketSnapshotRepo = require("../repositories/marketSnapshotRepository")
const marketUniverseRepo = require("../repositories/marketUniverseRepository")
const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const marketComparisonService = require("./marketComparisonService")
const arbitrageEngine = require("./arbitrageEngineService")
const marketService = require("./marketService")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const marketImageService = require("./marketImageService")
const planService = require("./planService")
const premiumCategoryAccessService = require("./premiumCategoryAccessService")
const AppError = require("../utils/AppError")

const SCANNER_TYPE = "global_arbitrage"
const SCANNER_INTERVAL_MINUTES = Math.max(Number(arbitrageScannerIntervalMinutes || 30), 1)
const SCANNER_INTERVAL_MS = SCANNER_INTERVAL_MINUTES * 60 * 1000
const CACHE_TTL_MS = SCANNER_INTERVAL_MS
const SCANNER_OVERDUE_GRACE_MS = Math.max(Math.round(SCANNER_INTERVAL_MS * 0.2), 15 * 1000)
const HIGH_CONFIDENCE_MIN_PRICE_USD = 5
const HIGH_CONFIDENCE_MIN_SPREAD_PERCENT = 5
const HIGH_CONFIDENCE_MAX_SPREAD_PERCENT = 120
const HIGH_CONFIDENCE_MIN_VOLUME_7D = 100
const HIGH_CONFIDENCE_MIN_SCORE = 75
const RISKY_MIN_PRICE_USD = 3
const RISKY_MIN_SPREAD_PERCENT = 3
const RISKY_MAX_SPREAD_PERCENT = 250
const RISKY_MIN_VOLUME_7D = 20
const RISKY_MIN_SCORE = 50
const PREMIUM_MIN_PRICE_USD = 20
const PREMIUM_MIN_SPREAD_PERCENT = 3
const PREMIUM_SPREAD_HEAVY_PENALTY_PERCENT = 150
const PREMIUM_MAX_SPREAD_PERCENT = 250
const PREMIUM_MIN_VOLUME_REJECT = 5
const PREMIUM_MIN_VOLUME_MEDIUM = 10
const PREMIUM_MIN_VOLUME_HIGH = 20
const PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE = 3
const PREMIUM_REFERENCE_PENALTY_RATIO = 1.8
const PREMIUM_REFERENCE_REJECT_RATIO = 2.5
const MIN_SPREAD_PERCENT = HIGH_CONFIDENCE_MIN_SPREAD_PERCENT
const MAX_SPREAD_PERCENT = RISKY_MAX_SPREAD_PERCENT
const MIN_VOLUME_7D = HIGH_CONFIDENCE_MIN_VOLUME_7D
const MIN_EXECUTION_PRICE_USD = HIGH_CONFIDENCE_MIN_PRICE_USD
const MIN_MARKET_COVERAGE = 2
const DEFAULT_SCORE_CUTOFF = HIGH_CONFIDENCE_MIN_SCORE
const RISKY_SCORE_CUTOFF = RISKY_MIN_SCORE
const FEED_RISKY_MIN_SCORE = 48
const MAX_API_LIMIT = 200
const DEFAULT_API_LIMIT = 100
const MAX_FEED_LIMIT = 500
const FEED_METADATA_ENRICH_LIMIT = 60
const FEED_RETENTION_HOURS = Math.max(Number(arbitrageFeedRetentionHours || 24), 1)
const FEED_ACTIVE_LIMIT = Math.max(Number(arbitrageFeedActiveLimit || 500), 50)
const DUPLICATE_WINDOW_HOURS = Math.max(Number(arbitrageDuplicateWindowHours || 4), 1)
const MIN_PROFIT_CHANGE_PCT = Math.max(Number(arbitrageMinProfitChangePct || 10), 0)
const MIN_SCORE_CHANGE = Math.max(Number(arbitrageMinScoreChange || 8), 0)
const INSERT_DUPLICATES = Boolean(arbitrageInsertDuplicates)
const MANUAL_REFRESH_TRACKER_MAX = 4000
const manualRefreshTracker = new Map()

const FRESHNESS_STATES = Object.freeze({
  FRESH: "fresh",
  AGING: "aging",
  STALE: "stale"
})

const CATEGORY_STALE_RULES = Object.freeze({
  weapon_skin: Object.freeze({
    freshMaxMinutes: 30,
    agingMaxMinutes: 60,
    agingPenalty: 8,
    stalePenalty: 20
  }),
  case: Object.freeze({
    freshMaxMinutes: 60,
    agingMaxMinutes: 120,
    agingPenalty: 6,
    stalePenalty: 16
  }),
  sticker_capsule: Object.freeze({
    freshMaxMinutes: 90,
    agingMaxMinutes: 180,
    agingPenalty: 5,
    stalePenalty: 14
  }),
  knife: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240,
    agingPenalty: 4,
    stalePenalty: 12
  }),
  glove: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240,
    agingPenalty: 4,
    stalePenalty: 12
  })
})

const SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"])
const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove"
})
const SCANNER_AUDIT_CATEGORIES = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE
])
const PERFORMANCE_STAGE_KEYS = Object.freeze([
  "sourceCatalogPreparationMs",
  "inputHydrationMs",
  "quoteFetchingMs",
  "normalizationMs",
  "opportunityComputationMs",
  "dbWritesMs",
  "diagnosticsAggregationMs"
])
const FALLBACK_UNIVERSE = Object.freeze(normalizeUniverseEntries(marketUniverseTop100))
const DEFAULT_UNIVERSE_LIMIT_BASELINE = 3000
const DEFAULT_UNIVERSE_LIMIT = Math.max(
  Number(arbitrageDefaultUniverseLimit || DEFAULT_UNIVERSE_LIMIT_BASELINE),
  100
)
const UNIVERSE_TARGET_SIZE = Math.max(
  Number(arbitrageScannerUniverseTargetSize || DEFAULT_UNIVERSE_LIMIT),
  20
)
const PRE_COMPARE_UNIVERSE_LIMIT = Math.max(
  UNIVERSE_TARGET_SIZE * 2,
  DEFAULT_UNIVERSE_LIMIT
)
const UNIVERSE_DB_LIMIT = Math.max(
  Number(arbitrageUniverseDbLimit || DEFAULT_UNIVERSE_LIMIT * 2),
  PRE_COMPARE_UNIVERSE_LIMIT
)
const SCAN_BATCH_SIZE = Math.max(
  Math.min(
    Number(
      arbitrageScanBatchSize ||
        arbitrageQuoteRefreshBatchSize ||
        arbitrageQuoteComputeBatchSize ||
        40
    ),
    100
  ),
  10
)
const MAX_CONCURRENT_MARKET_REQUESTS = Math.max(
  Number(arbitrageMaxConcurrentMarketRequests || 6),
  1
)
const SCAN_TIMEOUT_PER_BATCH = Math.max(
  Number(arbitrageScanTimeoutPerBatchMs || 30000),
  1000
)
const IMAGE_ENRICH_BATCH_SIZE = Math.max(
  Math.min(Number(arbitrageImageEnrichBatchSize || 30), 120),
  0
)
const IMAGE_ENRICH_CONCURRENCY = Math.max(
  Math.min(Number(arbitrageImageEnrichConcurrency || 2), 6),
  1
)
const IMAGE_ENRICH_TIMEOUT_MS = Math.max(
  Number(arbitrageImageEnrichTimeoutMs || 9000),
  1000
)
const SNAPSHOT_WARMUP_MAX_ITEMS = Math.max(Math.min(SCAN_BATCH_SIZE, 30), 10)
const SNAPSHOT_WARMUP_CONCURRENCY = Math.max(
  Math.min(MAX_CONCURRENT_MARKET_REQUESTS, 3),
  1
)
const SNAPSHOT_WARMUP_TRIGGER_FRESH_MIN = Math.max(SCAN_BATCH_SIZE, 20)
const MIN_STRICT_SEED_COVERAGE = Math.max(Math.round(SCAN_BATCH_SIZE / 2), 10)
const STRICT_SEED_COVERAGE_RATIO_TARGET = 0.6
const UNIVERSE_MIN_PRICE_FLOOR_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 2,
  [ITEM_CATEGORIES.CASE]: 1,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 1,
  [ITEM_CATEGORIES.KNIFE]: 20,
  [ITEM_CATEGORIES.GLOVE]: 20
})
const UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 20,
  [ITEM_CATEGORIES.CASE]: 20,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 20,
  [ITEM_CATEGORIES.KNIFE]: PREMIUM_MIN_VOLUME_REJECT,
  [ITEM_CATEGORIES.GLOVE]: PREMIUM_MIN_VOLUME_REJECT
})
const LOW_VALUE_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i
])
const KNOWN_BAD_IMAGE_HOSTS = Object.freeze(
  new Set(["example.com", "www.example.com"])
)
const DIAGNOSTIC_REASON_KEYS = Object.freeze([
  "ignored_execution_floor",
  "ignored_low_value_universe",
  "ignored_low_liquidity",
  "risky_low_price",
  "risky_low_profit",
  "risky_low_score",
  "risky_low_confidence",
  "risky_missing_depth",
  "spread_below_min",
  "non_positive_profit",
  "ignored_extreme_spread",
  "ignored_reference_deviation",
  "ignored_missing_markets",
  "ignored_missing_liquidity_data",
  "ignored_missing_depth",
  "ignored_low_score",
  "ignored_stale_data"
])
const DIAGNOSTIC_REASON_ALIAS = Object.freeze({
  ignored_low_price: "ignored_execution_floor",
  insufficient_market_data: "ignored_missing_markets"
})

const HARD_REJECTION_REASONS = Object.freeze(
  new Set([
    "insufficient_market_data",
    "non_positive_profit",
    "ignored_reference_deviation",
    "ignored_extreme_spread",
    "ignored_missing_markets"
  ])
)

const STRICT_SCAN_PROFILE = Object.freeze({
  name: "strict",
  minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
  minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
  minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
  allowMissingLiquidity: false,
  requireFreshData: true,
  maxQuoteAgeMinutes: 60,
  minScore: HIGH_CONFIDENCE_MIN_SCORE
})

const RISKY_SCAN_PROFILE = Object.freeze({
  name: "risky",
  minPriceUsd: RISKY_MIN_PRICE_USD,
  minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
  minVolume7d: RISKY_MIN_VOLUME_7D,
  allowMissingLiquidity: true,
  requireFreshData: false,
  maxQuoteAgeMinutes: Infinity,
  minScore: RISKY_MIN_SCORE
})

const CATEGORY_SCAN_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
      minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
      maxSpreadPercent: HIGH_CONFIDENCE_MAX_SPREAD_PERCENT,
      minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: 40,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 55
    })
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
      minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
      maxSpreadPercent: HIGH_CONFIDENCE_MAX_SPREAD_PERCENT,
      minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 1,
      minProfitUsd: 0.4,
      minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: RISKY_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 50
    })
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
      minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
      maxSpreadPercent: HIGH_CONFIDENCE_MAX_SPREAD_PERCENT,
      minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 1,
      minProfitUsd: 0.5,
      minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: RISKY_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 50
    })
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: PREMIUM_MIN_PRICE_USD,
      minSpreadPercent: PREMIUM_MIN_SPREAD_PERCENT,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_MEDIUM,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 40,
      minProfitUsd: 3,
      minSpreadPercent: 2.5,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 48,
      allowMissingDepthWithPenalty: true
    })
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: PREMIUM_MIN_PRICE_USD,
      minSpreadPercent: PREMIUM_MIN_SPREAD_PERCENT,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_MEDIUM,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 40,
      minProfitUsd: 3,
      minSpreadPercent: 2.5,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 48,
      allowMissingDepthWithPenalty: true
    })
  })
})

const PREMIUM_ITEM_CATEGORIES = Object.freeze(
  new Set([ITEM_CATEGORIES.KNIFE, ITEM_CATEGORIES.GLOVE])
)

const CATEGORY_RISKY_MODE_PROFILES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    name: "risky_weapon_skin",
    minPriceUsd: 3,
    minProfitUsd: 0.75,
    minSpreadPercent: 4,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 40,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 55,
    allowMissingLiquidity: false,
    allowMissingDepthWithPenalty: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    name: "risky_case",
    minPriceUsd: 1,
    minProfitUsd: 0.4,
    minSpreadPercent: 3,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 20,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 50,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    name: "risky_sticker_capsule",
    minPriceUsd: 1,
    minProfitUsd: 0.5,
    minSpreadPercent: 3,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 20,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 50,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    name: "risky_knife",
    minPriceUsd: 40,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
    minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 48,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: true,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    name: "risky_glove",
    minPriceUsd: 40,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
    minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 48,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: true,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  })
})

const RISKY_QUALITY_FLOOR_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({ minScore: 55, minProfitUsd: 0.75 }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({ minScore: 50, minProfitUsd: 0.4 }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({ minScore: 50, minProfitUsd: 0.5 }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({ minScore: 48, minProfitUsd: 3 }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({ minScore: 48, minProfitUsd: 3 })
})

function normalizeMarketHashName(value) {
  return String(value || "").trim()
}

function sanitizeImageUrl(value) {
  const raw = String(value || "").trim()
  if (!raw) return null
  try {
    const parsed = new URL(raw)
    if (!["http:", "https:"].includes(parsed.protocol)) return null
    if (KNOWN_BAD_IMAGE_HOSTS.has(String(parsed.hostname || "").trim().toLowerCase())) {
      return null
    }
    return parsed.toString()
  } catch (_err) {
    return null
  }
}

function normalizeItemCategory(value, marketHashName = "") {
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  if (raw === ITEM_CATEGORIES.KNIFE || raw === "knives") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === ITEM_CATEGORIES.GLOVE || raw === "gloves") {
    return ITEM_CATEGORIES.GLOVE
  }
  if (
    raw === ITEM_CATEGORIES.STICKER_CAPSULE ||
    raw === "sticker capsule" ||
    raw === "capsule" ||
    raw === "sticker_capsules"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (raw === ITEM_CATEGORIES.WEAPON_SKIN) return ITEM_CATEGORIES.WEAPON_SKIN
  if (/sticker capsule$/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (/case$/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.CASE
  }
  if (/\b(gloves|glove|hand wraps)\b/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.GLOVE
  }
  if (/\b(knife|bayonet|karambit|daggers)\b/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.KNIFE
  }
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function normalizeUniverseEntries(items = []) {
  const seen = new Set()
  const normalized = []
  for (const rawEntry of Array.isArray(items) ? items : []) {
    const entry =
      rawEntry && typeof rawEntry === "object"
        ? rawEntry
        : { marketHashName: rawEntry, category: ITEM_CATEGORIES.WEAPON_SKIN, scan_enabled: true }
    const marketHashName = normalizeMarketHashName(
      entry?.marketHashName || entry?.market_hash_name || entry?.name
    )
    if (!marketHashName) continue
    const key = marketHashName.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    const category = normalizeItemCategory(entry?.category, marketHashName)
    const subcategory = String(entry?.subcategory || "").trim() || null
    const scanEnabled = entry?.scan_enabled == null ? true : Boolean(entry.scan_enabled)
    if (!scanEnabled) continue
    normalized.push({
      marketHashName,
      category,
      subcategory,
      scan_enabled: true
    })
  }
  return normalized
}

function toFiniteOrNull(value) {
  if (value == null) return null
  if (typeof value === "string" && !value.trim()) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return parsed > 0 ? parsed : null
}

function toIsoStringOrNull(value) {
  const text = String(value || "").trim()
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function clampScore(value) {
  const parsed = toFiniteOrNull(value) ?? 0
  return round2(Math.min(Math.max(parsed, 0), 100))
}

function toTitle(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\b\w/g, (token) => token.toUpperCase())
}

function normalizeBoolean(value) {
  if (typeof value === "boolean") return value
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on"
}

function normalizeCategoryFilter(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  if (!raw || raw === "all") return "all"
  if (raw === "skin" || raw === "skins" || raw === "weapon_skin") {
    return ITEM_CATEGORIES.WEAPON_SKIN
  }
  if (raw === "case" || raw === "cases") {
    return ITEM_CATEGORIES.CASE
  }
  if (raw === "knife" || raw === "knives") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === "glove" || raw === "gloves") {
    return ITEM_CATEGORIES.GLOVE
  }
  if (
    raw === "capsule" ||
    raw === "capsules" ||
    raw === "sticker_capsule" ||
    raw === "sticker_capsules"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  return "all"
}

function getCategoryStaleRules(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return CATEGORY_STALE_RULES[normalized] || CATEGORY_STALE_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function resolveFreshnessState(ageMinutes = null, rules = {}) {
  const age = toFiniteOrNull(ageMinutes)
  if (age == null || age < 0) return FRESHNESS_STATES.STALE
  if (age <= Number(rules.freshMaxMinutes || 0)) return FRESHNESS_STATES.FRESH
  if (age <= Number(rules.agingMaxMinutes || 0)) return FRESHNESS_STATES.AGING
  return FRESHNESS_STATES.STALE
}

function computeLiquidityScoreFromSnapshot(snapshot = {}) {
  const volume24h = Math.max(toFiniteOrNull(snapshot?.volume_24h) ?? 0, 0)
  const volatility7dPercent = Math.max(toFiniteOrNull(snapshot?.volatility_7d_percent) ?? 0, 0)
  const spreadPercent = Math.max(toFiniteOrNull(snapshot?.spread_percent) ?? 0, 0)

  const volumeScore = Math.min(Math.max((Math.log10(volume24h + 1) / 3) * 100, 0), 100)
  const volatilityScore = 100 - Math.min(Math.max((volatility7dPercent / 25) * 100, 0), 100)
  const spreadScore = 100 - Math.min(Math.max((spreadPercent / 15) * 100, 0), 100)

  return round2(
    Math.min(Math.max(volumeScore * 0.55 + volatilityScore * 0.25 + spreadScore * 0.2, 0), 100)
  )
}

function resolveSevenDayChangePercent(snapshot = {}) {
  const average7d = toFiniteOrNull(snapshot?.average_7d_price)
  const lowestListing = toFiniteOrNull(snapshot?.lowest_listing_price)
  if (average7d == null || average7d <= 0 || lowestListing == null) {
    return null
  }
  return round2(((lowestListing - average7d) / average7d) * 100)
}

function resolveVolume7d(snapshot = {}) {
  const volume24h = toFiniteOrNull(snapshot?.volume_24h)
  if (volume24h == null || volume24h < 0) return null
  return round2(volume24h * 7)
}

function isSnapshotStale(snapshot = {}) {
  const capturedAt = toIsoStringOrNull(snapshot?.captured_at)
  if (!capturedAt) return true
  const ageMs = Date.now() - new Date(capturedAt).getTime()
  return ageMs > Math.max(Number(marketSnapshotTtlMinutes || 30), 1) * 60 * 1000
}

function toByNameMap(rows = [], keyField) {
  const map = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const key = String(row?.[keyField] || "").trim()
    if (!key) continue
    map[key] = row
  }
  return map
}

function toBySourceMap(rows = []) {
  const map = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const source = String(row?.source || row?.market || "")
      .trim()
      .toLowerCase()
    if (!source) continue
    map[source] = row
  }
  return map
}

function chunkArray(items = [], chunkSize = 50) {
  const safeChunkSize = Math.max(Number(chunkSize || 0), 1)
  const rows = Array.isArray(items) ? items : []
  const chunks = []
  for (let index = 0; index < rows.length; index += safeChunkSize) {
    chunks.push(rows.slice(index, index + safeChunkSize))
  }
  return chunks
}

function toNameKey(value) {
  return normalizeMarketHashName(value).toLowerCase()
}

function sourceRank(value) {
  const source = String(value || "")
    .trim()
    .toLowerCase()
  if (source === "curated_db") return 4
  if (source === "dynamic_snapshot") return 3
  if (source === "fallback_curated") return 2
  if (source === "fallback_mvp") return 2
  return 1
}

function computeUniverseSeedRank(seed = {}) {
  const volume7d = toFiniteOrNull(seed?.marketVolume7d) ?? 0
  const liquidityScore = toFiniteOrNull(seed?.liquidityScore) ?? 0
  const referencePrice = toFiniteOrNull(seed?.referencePrice) ?? 0
  const snapshotFreshness = seed?.hasSnapshotData ? (seed?.snapshotStale ? 0 : 1) : 0
  return (
    sourceRank(seed?.universeSource) * 100000 +
    snapshotFreshness * 50000 +
    Math.min(volume7d, 6000) * 8 +
    Math.min(liquidityScore, 100) * 55 +
    Math.min(referencePrice, 1000)
  )
}

function mergeUniverseSeeds(seedSets = []) {
  const byName = {}
  for (const set of Array.isArray(seedSets) ? seedSets : []) {
    for (const seed of Array.isArray(set) ? set : []) {
      const marketHashName = normalizeMarketHashName(seed?.marketHashName)
      if (!marketHashName) continue
      const key = toNameKey(marketHashName)
      const nextSeed = {
        ...seed,
        marketHashName
      }
      const existing = byName[key]
      if (!existing) {
        byName[key] = nextSeed
        continue
      }
      if (computeUniverseSeedRank(nextSeed) > computeUniverseSeedRank(existing)) {
        byName[key] = nextSeed
      }
    }
  }
  return Object.values(byName)
}

function resolveQuoteAgeMinutes(quote = {}) {
  const updatedAt = toIsoStringOrNull(
    quote?.updatedAt || quote?.fetched_at || quote?.fetchedAt || quote?.recorded_at
  )
  if (!updatedAt) return null
  const ageMinutes = (Date.now() - new Date(updatedAt).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return round2(ageMinutes)
}

function resolveSnapshotAgeMinutes(inputItem = {}) {
  const capturedAt = toIsoStringOrNull(inputItem?.snapshotCapturedAt || inputItem?.steamRecordedAt)
  if (!capturedAt) return null
  const ageMinutes = (Date.now() - new Date(capturedAt).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return round2(ageMinutes)
}

function resolveSnapshotFreshnessState(inputItem = {}, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const rules = getCategoryStaleRules(itemCategory)
  const ageMinutes = resolveSnapshotAgeMinutes(inputItem)
  const hasSnapshotData = Boolean(inputItem?.hasSnapshotData || inputItem?.snapshotCapturedAt)
  if (!hasSnapshotData || ageMinutes == null) {
    return {
      state: FRESHNESS_STATES.STALE,
      ageMinutes: ageMinutes,
      hasSnapshotData
    }
  }
  return {
    state: resolveFreshnessState(ageMinutes, rules),
    ageMinutes,
    hasSnapshotData
  }
}

function hasUsableQuotePrice(quote = {}) {
  const hasGross = Number.isFinite(Number(quote?.grossPrice)) && Number(quote.grossPrice) > 0
  const hasNet =
    Number.isFinite(Number(quote?.netPriceAfterFees)) && Number(quote.netPriceAfterFees) > 0
  return Boolean(quote?.available) && (hasGross || hasNet)
}

function resolveQuoteFreshnessEntry(quote = {}, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const source = normalizeMarketLabel(quote?.source || quote?.market)
  const ageMinutes = resolveQuoteAgeMinutes(quote)
  const rules = getCategoryStaleRules(itemCategory)
  const state = resolveFreshnessState(ageMinutes, rules)
  const hasUsablePrice = hasUsableQuotePrice(quote)
  return {
    source,
    ageMinutes,
    state,
    hasUsablePrice,
    usable: hasUsablePrice && state !== FRESHNESS_STATES.STALE
  }
}

function resolveStaleDataPenalty(
  perMarket = [],
  opportunity = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const rules = getCategoryStaleRules(itemCategory)
  const bySource = {}
  for (const row of Array.isArray(perMarket) ? perMarket : []) {
    const entry = resolveQuoteFreshnessEntry(row, itemCategory)
    if (!entry.source || !SOURCE_ORDER.includes(entry.source)) continue
    const existing = bySource[entry.source]
    if (!existing) {
      bySource[entry.source] = entry
      continue
    }
    const existingAge = toFiniteOrNull(existing.ageMinutes)
    const nextAge = toFiniteOrNull(entry.ageMinutes)
    if (existingAge == null && nextAge != null) {
      bySource[entry.source] = entry
      continue
    }
    if (existingAge != null && nextAge != null && nextAge < existingAge) {
      bySource[entry.source] = entry
    }
  }

  const buySource = normalizeMarketLabel(opportunity?.buyMarket)
  const sellSource = normalizeMarketLabel(opportunity?.sellMarket)
  const buyEntry = bySource[buySource] || null
  const sellEntry = bySource[sellSource] || null
  const buyAgeMinutes = toFiniteOrNull(buyEntry?.ageMinutes)
  const sellAgeMinutes = toFiniteOrNull(sellEntry?.ageMinutes)
  const maxAgeMinutes = [buyAgeMinutes, sellAgeMinutes].filter((value) => value != null).sort((a, b) => b - a)[0] ?? null

  let freshMarkets = 0
  let agingMarkets = 0
  let staleMarkets = 0
  let usableMarkets = 0
  for (const source of SOURCE_ORDER) {
    const entry = bySource[source]
    if (!entry) continue
    if (entry.state === FRESHNESS_STATES.FRESH) freshMarkets += 1
    else if (entry.state === FRESHNESS_STATES.AGING) agingMarkets += 1
    else staleMarkets += 1
    if (entry.usable) usableMarkets += 1
  }

  const selectedState =
    !buyEntry ||
    !sellEntry ||
    buyEntry.state === FRESHNESS_STATES.STALE ||
    sellEntry.state === FRESHNESS_STATES.STALE
      ? FRESHNESS_STATES.STALE
      : buyEntry.state === FRESHNESS_STATES.AGING || sellEntry.state === FRESHNESS_STATES.AGING
        ? FRESHNESS_STATES.AGING
        : FRESHNESS_STATES.FRESH

  let penalty = 0
  if (selectedState === FRESHNESS_STATES.AGING) {
    penalty += Number(rules.agingPenalty || 0)
  } else if (selectedState === FRESHNESS_STATES.STALE) {
    penalty += Number(rules.stalePenalty || 0)
  }
  if (usableMarkets === MIN_MARKET_COVERAGE && selectedState === FRESHNESS_STATES.AGING) {
    penalty += 3
  }

  return {
    penalty: round2(Math.max(penalty, 0)),
    maxAgeMinutes: maxAgeMinutes != null ? round2(maxAgeMinutes) : null,
    selectedState,
    selectedBuyState: String(buyEntry?.state || FRESHNESS_STATES.STALE),
    selectedSellState: String(sellEntry?.state || FRESHNESS_STATES.STALE),
    buyAgeMinutes,
    sellAgeMinutes,
    usableMarkets,
    freshMarkets,
    agingMarkets,
    staleMarkets,
    hasInsufficientUsableMarkets: usableMarkets < MIN_MARKET_COVERAGE,
    bySource,
    rules
  }
}

function buildFreshnessStateCounter() {
  return {
    [FRESHNESS_STATES.FRESH]: 0,
    [FRESHNESS_STATES.AGING]: 0,
    [FRESHNESS_STATES.STALE]: 0
  }
}

function normalizeFreshnessState(value = "") {
  const safe = String(value || "")
    .trim()
    .toLowerCase()
  if (safe === FRESHNESS_STATES.FRESH) return FRESHNESS_STATES.FRESH
  if (safe === FRESHNESS_STATES.AGING) return FRESHNESS_STATES.AGING
  return FRESHNESS_STATES.STALE
}

function incrementFreshnessCounter(counter = {}, state = FRESHNESS_STATES.STALE, count = 1) {
  const normalizedState = normalizeFreshnessState(state)
  counter[normalizedState] = Number(counter?.[normalizedState] || 0) + Number(count || 0)
}

function buildStaleDiagnosticsAccumulator() {
  const byMarket = Object.fromEntries(
    SOURCE_ORDER.map((source) => [source, buildFreshnessStateCounter()])
  )
  const byCategory = Object.fromEntries(
    Object.values(ITEM_CATEGORIES).map((category) => [category, buildFreshnessStateCounter()])
  )
  return {
    byMarket,
    byCategory
  }
}

function trackStaleDiagnostics(
  accumulator = null,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN,
  stale = {}
) {
  if (!accumulator || typeof accumulator !== "object") return
  if (!accumulator.byMarket || typeof accumulator.byMarket !== "object") {
    accumulator.byMarket = Object.fromEntries(
      SOURCE_ORDER.map((source) => [source, buildFreshnessStateCounter()])
    )
  }
  if (!accumulator.byCategory || typeof accumulator.byCategory !== "object") {
    accumulator.byCategory = Object.fromEntries(
      Object.values(ITEM_CATEGORIES).map((category) => [category, buildFreshnessStateCounter()])
    )
  }

  const normalizedCategory = normalizeItemCategory(itemCategory)
  if (!accumulator.byCategory[normalizedCategory]) {
    accumulator.byCategory[normalizedCategory] = buildFreshnessStateCounter()
  }
  incrementFreshnessCounter(accumulator.byCategory[normalizedCategory], stale?.selectedState, 1)

  const bySource = stale?.bySource && typeof stale.bySource === "object" ? stale.bySource : {}
  for (const source of SOURCE_ORDER) {
    const entry = bySource[source]
    if (!entry) continue
    if (!accumulator.byMarket[source]) {
      accumulator.byMarket[source] = buildFreshnessStateCounter()
    }
    incrementFreshnessCounter(accumulator.byMarket[source], entry?.state, 1)
  }
}

function toStaleDiagnosticsSummary(accumulator = {}) {
  const byMarket = {}
  const staleByMarket = {}
  const agingByMarket = {}
  for (const source of SOURCE_ORDER) {
    const bucket = accumulator?.byMarket?.[source] || {}
    const fresh = Number(bucket?.[FRESHNESS_STATES.FRESH] || 0)
    const aging = Number(bucket?.[FRESHNESS_STATES.AGING] || 0)
    const stale = Number(bucket?.[FRESHNESS_STATES.STALE] || 0)
    byMarket[source] = {
      fresh,
      aging,
      stale
    }
    staleByMarket[source] = stale
    agingByMarket[source] = aging
  }

  const byCategory = {}
  const staleByCategory = {}
  const agingByCategory = {}
  for (const category of Object.values(ITEM_CATEGORIES)) {
    const bucket = accumulator?.byCategory?.[category] || {}
    const fresh = Number(bucket?.[FRESHNESS_STATES.FRESH] || 0)
    const aging = Number(bucket?.[FRESHNESS_STATES.AGING] || 0)
    const stale = Number(bucket?.[FRESHNESS_STATES.STALE] || 0)
    byCategory[category] = {
      fresh,
      aging,
      stale
    }
    staleByCategory[category] = stale
    agingByCategory[category] = aging
  }

  return {
    quoteStateByMarket: byMarket,
    selectedStateByCategory: byCategory,
    staleByMarket,
    agingByMarket,
    staleByCategory,
    agingByCategory,
    totals: {
      staleQuotes: Object.values(staleByMarket).reduce((sum, value) => sum + Number(value || 0), 0),
      agingQuotes: Object.values(agingByMarket).reduce((sum, value) => sum + Number(value || 0), 0),
      staleItems: Object.values(staleByCategory).reduce((sum, value) => sum + Number(value || 0), 0),
      agingItems: Object.values(agingByCategory).reduce((sum, value) => sum + Number(value || 0), 0)
    }
  }
}

function normalizeMarketLabel(value) {
  const text = String(value || "").trim().toLowerCase()
  if (!text) return ""
  if (SOURCE_ORDER.includes(text)) return text
  return text
}

function countAvailableMarkets(perMarket = []) {
  const available = new Set()
  for (const row of Array.isArray(perMarket) ? perMarket : []) {
    const source = normalizeMarketLabel(row?.source || row?.market)
    if (!source || !SOURCE_ORDER.includes(source)) continue
    const hasGross = Number.isFinite(Number(row?.grossPrice)) && Number(row.grossPrice) > 0
    const hasNet =
      Number.isFinite(Number(row?.netPriceAfterFees)) && Number(row.netPriceAfterFees) > 0
    if (Boolean(row?.available) && (hasGross || hasNet)) {
      available.add(source)
    }
  }
  return available.size
}

function isLowValueJunkName(marketHashName = "") {
  const name = String(marketHashName || "").trim()
  if (!name) return true
  return LOW_VALUE_NAME_PATTERNS.some((pattern) => pattern.test(name))
}

function computeVolumeScore(volume7d, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalizedCategory = normalizeItemCategory(itemCategory)
  const volume = toFiniteOrNull(volume7d)
  if (volume == null || volume <= 0) return 0
  if (PREMIUM_ITEM_CATEGORIES.has(normalizedCategory)) {
    if (volume >= 80) return 100
    if (volume >= 40) return 90
    if (volume >= PREMIUM_MIN_VOLUME_HIGH) return 82
    if (volume >= PREMIUM_MIN_VOLUME_MEDIUM) return 70
    if (volume >= PREMIUM_MIN_VOLUME_REJECT) return 56
    return 20
  }
  if (volume >= 1000) return 100
  if (volume >= 500) return 92
  if (volume >= 200) return 80
  if (volume >= 100) return 65
  return 35
}

function computeMarketCoverageScore(coverageCount) {
  const value = Number(coverageCount || 0)
  if (value >= 4) return 100
  if (value === 3) return 85
  if (value === 2) return 70
  if (value === 1) return 35
  return 0
}

function computePriceStabilityScore(sevenDayChangePercent) {
  const change = toFiniteOrNull(sevenDayChangePercent)
  if (change == null) return 55
  const absChange = Math.abs(change)
  if (absChange < 5) return 100
  if (absChange <= 10) return 80
  if (absChange <= 20) return 50
  return 20
}

function computeReferencePriceScore(referencePrice, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalizedCategory = normalizeItemCategory(itemCategory)
  const price = toFiniteOrNull(referencePrice)
  if (PREMIUM_ITEM_CATEGORIES.has(normalizedCategory)) {
    if (price == null || price < 20) return 0
    if (price >= 250) return 100
    if (price >= 120) return 92
    if (price >= 60) return 82
    return 68
  }
  if (normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE) {
    if (price == null || price < 0.75) return 0
    if (price >= 8) return 100
    if (price >= 3) return 85
    return 65
  }
  if (normalizedCategory === ITEM_CATEGORIES.CASE) {
    if (price == null || price < 0.5) return 0
    if (price >= 5) return 100
    if (price >= 2) return 85
    return 65
  }
  if (price == null || price < MIN_EXECUTION_PRICE_USD) return 0
  if (price >= 30) return 100
  if (price >= 10) return 85
  return 65
}

function computeLiquidityRank({
  volume7d = null,
  marketCoverage = 0,
  sevenDayChangePercent = null,
  referencePrice = null,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  const volumeScore = computeVolumeScore(volume7d, itemCategory)
  const marketCoverageScore = computeMarketCoverageScore(marketCoverage)
  const priceStabilityScore = computePriceStabilityScore(sevenDayChangePercent)
  const referencePriceScore = computeReferencePriceScore(referencePrice, itemCategory)
  const liquidityRank = round2(
    volumeScore * 0.5 +
      marketCoverageScore * 0.2 +
      priceStabilityScore * 0.15 +
      referencePriceScore * 0.15
  )

  return {
    liquidityRank,
    volumeScore,
    marketCoverageScore,
    priceStabilityScore,
    referencePriceScore
  }
}

function normalizeDiagnosticReason(reason = "") {
  const raw = String(reason || "").trim()
  if (!raw) return ""
  return DIAGNOSTIC_REASON_ALIAS[raw] || raw
}

function incrementReasonCounter(counter = {}, reason, category = "") {
  const key = normalizeDiagnosticReason(reason)
  if (!key) return
  counter[key] = Number(counter[key] || 0) + 1

  const normalizedCategory = String(category || "").trim()
    ? normalizeItemCategory(category)
    : ""
  if (!normalizedCategory) return

  if (!counter.__byCategory || typeof counter.__byCategory !== "object") {
    counter.__byCategory = {}
  }
  if (!counter.__byCategory[normalizedCategory]) {
    counter.__byCategory[normalizedCategory] = {}
  }

  const categoryBucket = counter.__byCategory[normalizedCategory]
  categoryBucket[key] = Number(categoryBucket[key] || 0) + 1
}

function incrementItemReasonCounter(rejectionsByItem = {}, itemName, reason, category = "") {
  const normalizedItem = String(itemName || "").trim() || "Unknown item"
  const normalizedReason = normalizeDiagnosticReason(reason) || "unknown"
  const normalizedCategory = normalizeItemCategory(category)
  if (!rejectionsByItem[normalizedItem]) {
    rejectionsByItem[normalizedItem] = {
      total: 0,
      category: normalizedCategory,
      reasons: {}
    }
  }
  const bucket = rejectionsByItem[normalizedItem]
  if (!bucket.category) {
    bucket.category = normalizedCategory
  }
  bucket.total += 1
  bucket.reasons[normalizedReason] = Number(bucket.reasons[normalizedReason] || 0) + 1
}

function toTopRejectedItems(rejectionsByItem = {}, limit = 8) {
  return Object.entries(rejectionsByItem)
    .map(([itemName, payload]) => {
      const reasonEntries = Object.entries(payload?.reasons || {}).sort(
        (a, b) => Number(b[1] || 0) - Number(a[1] || 0)
      )
      const [mainReason, mainReasonCount] = reasonEntries[0] || ["unknown", 0]
      return {
        itemName,
        category: normalizeItemCategory(payload?.category),
        rejectedCount: Number(payload?.total || 0),
        mainReason,
        mainReasonCount: Number(mainReasonCount || 0),
        reasons: payload?.reasons || {}
      }
    })
    .sort(
      (a, b) =>
        Number(b.rejectedCount || 0) - Number(a.rejectedCount || 0) ||
        Number(b.mainReasonCount || 0) - Number(a.mainReasonCount || 0)
    )
    .slice(0, Math.max(Number(limit || 0), 0))
}

function toRejectionReasonsByItem(rejectionsByItem = {}, limit = 20) {
  return Object.entries(rejectionsByItem)
    .map(([itemName, payload]) => ({
      itemName,
      category: normalizeItemCategory(payload?.category),
      rejectedCount: Number(payload?.total || 0),
      reasons: payload?.reasons || {}
    }))
    .sort((a, b) => Number(b.rejectedCount || 0) - Number(a.rejectedCount || 0))
    .slice(0, Math.max(Number(limit || 0), 0))
}

function normalizeDiscardStats(counter = {}) {
  const normalized = {}
  for (const key of DIAGNOSTIC_REASON_KEYS) {
    normalized[key] = Number(counter?.[key] || 0)
  }
  for (const [reason, count] of Object.entries(counter || {})) {
    if (String(reason || "").startsWith("__")) continue
    if (DIAGNOSTIC_REASON_KEYS.includes(reason)) continue
    normalized[reason] = Number(count || 0)
  }
  return normalized
}

function normalizeDiscardStatsByCategory(counter = {}) {
  const byCategory =
    counter?.__byCategory && typeof counter.__byCategory === "object"
      ? counter.__byCategory
      : {}

  const normalized = {}
  for (const [category, reasonMap] of Object.entries(byCategory)) {
    const normalizedCategory = normalizeItemCategory(category)
    const reasonStats = normalizeDiscardStats(reasonMap || {})
    normalized[normalizedCategory] = {
      totalRejected: Object.values(reasonStats).reduce(
        (sum, value) => sum + Number(value || 0),
        0
      ),
      reasons: reasonStats
    }
  }

  return normalized
}

function buildInputItemFromSkinAndSnapshot({
  skin = null,
  snapshot = null,
  marketHashName = "",
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  subcategory = null,
  universeSource = "fallback_curated",
  liquidityRank = null
} = {}) {
  const normalizedName = normalizeMarketHashName(
    marketHashName || skin?.market_hash_name || skin?.marketHashName
  )
  if (!normalizedName) return null

  const referencePrice = toPositiveOrNull(
    toFiniteOrNull(snapshot?.average_7d_price) ?? toFiniteOrNull(snapshot?.lowest_listing_price)
  )
  const volume7d = toPositiveOrNull(resolveVolume7d(snapshot || {}))
  const hasUsableSnapshotLiquidity = referencePrice != null || volume7d != null
  const liquidityScore =
    snapshot && hasUsableSnapshotLiquidity ? computeLiquidityScoreFromSnapshot(snapshot) : null
  const sevenDayChangePercent =
    snapshot && hasUsableSnapshotLiquidity ? resolveSevenDayChangePercent(snapshot) : null
  const imageUrl = sanitizeImageUrl(skin?.image_url || skin?.imageUrl)
  const imageUrlLarge =
    sanitizeImageUrl(skin?.image_url_large || skin?.imageUrlLarge) || imageUrl || null

  return {
    skinId: Number(skin?.id || 0) || null,
    marketHashName: normalizedName,
    itemCategory: normalizeItemCategory(category, normalizedName),
    itemSubcategory: String(subcategory || "").trim() || null,
    itemRarity: String(skin?.rarity || "").trim() || null,
    itemRarityColor:
      String(skin?.rarity_color || skin?.rarityColor || "").trim() || null,
    itemImageUrl: imageUrl,
    itemImageUrlLarge: imageUrlLarge,
    quantity: 1,
    marketVolume7d: volume7d,
    liquidityScore,
    sevenDayChangePercent,
    referencePrice,
    hasSnapshotData: Boolean(snapshot) && hasUsableSnapshotLiquidity,
    snapshotCapturedAt: snapshot?.captured_at || null,
    snapshotStale: snapshot ? isSnapshotStale(snapshot) : false,
    universeSource: String(universeSource || "fallback_curated").trim() || "fallback_curated",
    liquidityRank: toFiniteOrNull(liquidityRank)
  }
}

async function ensureSkinsForMarketNames(marketNames = []) {
  const uniqueNames = Array.from(
    new Set(
      (Array.isArray(marketNames) ? marketNames : [])
        .map((name) => normalizeMarketHashName(name))
        .filter(Boolean)
    )
  )
  if (!uniqueNames.length) {
    return []
  }

  let existing = []
  try {
    existing = await skinRepo.getByMarketHashNames(uniqueNames)
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to fetch skins by market hash name", err.message)
    existing = uniqueNames.map((marketHashName) => ({ market_hash_name: marketHashName }))
  }
  const existingByName = toByNameMap(
    (Array.isArray(existing) ? existing : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const missingNames = uniqueNames.filter((name) => !existingByName[name])
  if (!missingNames.length) {
    return Array.isArray(existing) ? existing : []
  }

  try {
    await skinRepo.upsertSkins(
      missingNames.map((marketHashName) => ({
        market_hash_name: marketHashName
      }))
    )
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to auto-seed missing skins", err.message)
  }

  try {
    return await skinRepo.getByMarketHashNames(uniqueNames)
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to refetch skins after auto-seed", err.message)
    return Array.isArray(existing) ? existing : []
  }
}

function computeStrictCoverageThreshold(totalSeeds = 0) {
  const seedCount = Math.max(Math.round(Number(totalSeeds || 0)), 0)
  if (!seedCount) return MIN_STRICT_SEED_COVERAGE
  const ratioThreshold = Math.round(seedCount * STRICT_SEED_COVERAGE_RATIO_TARGET)
  return Math.max(Math.min(ratioThreshold, seedCount), MIN_STRICT_SEED_COVERAGE)
}

async function loadCuratedUniverseSeeds() {
  let curatedRows = []
  try {
    curatedRows = await marketUniverseRepo.listActiveByLiquidityRank({
      limit: UNIVERSE_DB_LIMIT,
      categories: [
        ITEM_CATEGORIES.WEAPON_SKIN,
        ITEM_CATEGORIES.CASE,
        ITEM_CATEGORIES.STICKER_CAPSULE
      ]
    })
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to load curated market universe", err.message)
    return []
  }

  if (!Array.isArray(curatedRows) || !curatedRows.length) {
    return []
  }

  const normalizedRows = curatedRows
    .map((row) => {
      const marketHashName = normalizeMarketHashName(
        row?.market_hash_name || row?.marketHashName || row?.item_name || row?.itemName
      )
      if (!marketHashName) return null
      return {
        marketHashName,
        category: normalizeItemCategory(row?.category, marketHashName),
        subcategory: String(row?.subcategory || "").trim() || null,
        liquidityRank: toFiniteOrNull(row?.liquidity_rank || row?.liquidityRank)
      }
    })
    .filter(Boolean)

  if (!normalizedRows.length) {
    return []
  }

  const skins = await ensureSkinsForMarketNames(normalizedRows.map((row) => row.marketHashName))
  const skinsByName = toByNameMap(
    (Array.isArray(skins) ? skins : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const skinIds = (Array.isArray(skins) ? skins : [])
    .map((row) => Number(row?.id || 0))
    .filter((value) => Number.isInteger(value) && value > 0)
  const snapshotsBySkinId = skinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(skinIds)
    : {}

  return normalizedRows
    .map((row) =>
      buildInputItemFromSkinAndSnapshot({
        skin: skinsByName[row.marketHashName] || null,
        snapshot:
          Number(skinsByName[row.marketHashName]?.id || 0) > 0
            ? snapshotsBySkinId[Number(skinsByName[row.marketHashName].id)] || null
            : null,
        marketHashName: row.marketHashName,
        category: row.category,
        subcategory: row.subcategory,
        universeSource: "curated_db",
        liquidityRank: row.liquidityRank
      })
    )
    .filter(Boolean)
}

async function loadFallbackUniverseSeeds() {
  const marketNames = FALLBACK_UNIVERSE.map((entry) => entry.marketHashName)
  const skins = await ensureSkinsForMarketNames(marketNames)
  const skinsByName = toByNameMap(
    (Array.isArray(skins) ? skins : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const skinIds = (Array.isArray(skins) ? skins : [])
    .map((row) => Number(row?.id || 0))
    .filter((value) => Number.isInteger(value) && value > 0)
  const snapshotsBySkinId = skinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(skinIds)
    : {}

  return FALLBACK_UNIVERSE.map((entry, index) =>
    buildInputItemFromSkinAndSnapshot({
      skin: skinsByName[entry.marketHashName] || null,
      snapshot:
        Number(skinsByName[entry.marketHashName]?.id || 0) > 0
          ? snapshotsBySkinId[Number(skinsByName[entry.marketHashName].id)] || null
          : null,
      marketHashName: entry.marketHashName,
      category: entry.category,
      subcategory: entry.subcategory,
      universeSource: "fallback_curated",
      liquidityRank: index + 1
    })
  ).filter(Boolean)
}

function passesUniverseSeedFilters(
  inputItem = {},
  discardStats = {},
  rejectedByItem = null,
  options = {}
) {
  const allowMissingSnapshotData = Boolean(options?.allowMissingSnapshotData)
  const marketHashName = String(inputItem?.marketHashName || "").trim()
  const itemCategory = normalizeItemCategory(inputItem?.itemCategory, marketHashName)
  if (!marketHashName) return false
  if (isLowValueJunkName(marketHashName)) {
    incrementReasonCounter(discardStats, "ignored_low_value_universe", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        marketHashName,
        "ignored_low_value_universe",
        itemCategory
      )
    }
    return false
  }

  if (!Boolean(inputItem?.hasSnapshotData)) {
    if (!allowMissingSnapshotData) {
      incrementReasonCounter(discardStats, "ignored_missing_liquidity_data", itemCategory)
      if (rejectedByItem) {
        incrementItemReasonCounter(
          rejectedByItem,
          marketHashName,
          "ignored_missing_liquidity_data",
          itemCategory
        )
      }
      return false
    }
  }

  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, itemCategory)
  if (
    snapshotFreshness.state === FRESHNESS_STATES.STALE &&
    snapshotFreshness.ageMinutes != null &&
    snapshotFreshness.ageMinutes > Math.max(Number(marketSnapshotTtlMinutes || 30), 1) * 12
  ) {
    incrementReasonCounter(discardStats, "ignored_stale_data", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(rejectedByItem, marketHashName, "ignored_stale_data", itemCategory)
    }
    return false
  }

  const referencePrice = toFiniteOrNull(inputItem?.referencePrice)
  const universePriceFloor = Number(
    UNIVERSE_MIN_PRICE_FLOOR_BY_CATEGORY[itemCategory] ?? UNIVERSE_MIN_PRICE_FLOOR_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
  if (referencePrice != null && referencePrice < universePriceFloor) {
    incrementReasonCounter(discardStats, "ignored_low_value_universe", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        marketHashName,
        "ignored_low_value_universe",
        itemCategory
      )
    }
    return false
  }

  const volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
  const universeVolumeFloor = Number(
    UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY[itemCategory] ??
      UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
  if (volume7d != null && volume7d < universeVolumeFloor) {
    incrementReasonCounter(discardStats, "ignored_low_liquidity", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(rejectedByItem, marketHashName, "ignored_low_liquidity", itemCategory)
    }
    return false
  }

  return true
}

function resolveLiquidityMetrics(opportunity = {}, inputItem = {}) {
  const liquiditySignal = opportunity?.antiFake?.liquidity || {}
  let volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
  let liquidityScore = toFiniteOrNull(inputItem?.liquidityScore)
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const outlier = opportunity?.antiFake?.outlier && typeof opportunity.antiFake.outlier === "object"
    ? opportunity.antiFake.outlier
    : {}
  const referenceDeviation =
    opportunity?.antiFake?.referenceDeviation && typeof opportunity.antiFake.referenceDeviation === "object"
      ? opportunity.antiFake.referenceDeviation
      : {}

  if (liquiditySignal?.signalType === "volume_7d") {
    volume7d = toFiniteOrNull(liquiditySignal?.signalValue)
  }
  if (liquiditySignal?.signalType === "liquidity_score") {
    liquidityScore = toFiniteOrNull(liquiditySignal?.signalValue)
  }

  return {
    volume7d,
    liquidityScore,
    hasMissingDepth: depthFlags.includes("MISSING_DEPTH"),
    hasOutlierAdjusted: depthFlags.some(
      (flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED"
    ),
    hasSuspiciousDepthGap: depthFlags.some(
      (flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS"
    ),
    hasExtremeDepthGap: depthFlags.some(
      (flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME"
    ),
    buyDepthMissing: Boolean(outlier?.buyDepthMissing),
    sellDepthMissing: Boolean(outlier?.sellDepthMissing),
    referenceDeviationRatio: toFiniteOrNull(referenceDeviation?.maxRatio),
    hasStrongReferenceDeviation:
      Boolean(referenceDeviation?.strong) ||
      (toFiniteOrNull(referenceDeviation?.maxRatio) ?? 0) > PREMIUM_REFERENCE_PENALTY_RATIO,
    hasExtremeReferenceDeviation:
      Boolean(referenceDeviation?.extreme) ||
      (toFiniteOrNull(referenceDeviation?.maxRatio) ?? 0) > PREMIUM_REFERENCE_REJECT_RATIO
  }
}

function passesScannerGuards(opportunity = {}, liquidity = {}) {
  const itemCategory = normalizeItemCategory(
    opportunity?.itemCategory || opportunity?.category,
    opportunity?.itemName || opportunity?.marketHashName
  )
  const rules = getCategoryScanRules(itemCategory, "strict")
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const hasMissingDepth = Boolean(liquidity?.hasMissingDepth)
  const hasBothDepthMissing = Boolean(liquidity?.buyDepthMissing) && Boolean(liquidity?.sellDepthMissing)
  const hasSuspiciousDepthGap = Boolean(liquidity?.hasSuspiciousDepthGap)
  const hasExtremeDepthGap = Boolean(liquidity?.hasExtremeDepthGap)
  const referenceSignals = resolveReferenceSignals(opportunity, liquidity)
  const isPremiumCategory = PREMIUM_ITEM_CATEGORIES.has(itemCategory)

  if (!opportunity?.isOpportunity) return false
  if (profit == null || profit <= 0) return false
  if (buyPrice == null || buyPrice < Number(rules.minPriceUsd || MIN_EXECUTION_PRICE_USD)) return false
  if (
    spread == null ||
    spread < Number(rules.minSpreadPercent || MIN_SPREAD_PERCENT) ||
    spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)
  ) {
    return false
  }
  if (!isPremiumCategory && (volume7d == null || volume7d < Number(rules.minVolume7d || MIN_VOLUME_7D))) {
    return false
  }
  if (isPremiumCategory) {
    if (volume7d != null && volume7d < PREMIUM_MIN_VOLUME_REJECT) return false
    if (volume7d == null && marketCoverage < PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) return false
    if (referenceSignals.hasExtremeReferenceDeviation) return false
    if (hasExtremeDepthGap) return false
  }
  if (marketCoverage < Number(rules.minMarketCoverage || MIN_MARKET_COVERAGE)) return false
  if (isPremiumCategory && (hasBothDepthMissing || hasMissingDepth || hasSuspiciousDepthGap)) return false

  return true
}

function getPrimaryHardRejectionReason(opportunity = {}) {
  const reasons = Array.isArray(opportunity?.antiFake?.reasons) ? opportunity.antiFake.reasons : []
  for (const reason of reasons) {
    if (HARD_REJECTION_REASONS.has(String(reason || "").trim())) {
      return String(reason || "").trim()
    }
  }
  return ""
}

function getCategoryScanRules(itemCategory, profileName = "strict") {
  const category = normalizeItemCategory(itemCategory)
  const categoryRules = CATEGORY_SCAN_RULES[category] || CATEGORY_SCAN_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  if (profileName === "risky") return categoryRules.risky
  return categoryRules.strict
}

function getCategoryRiskyProfile(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    CATEGORY_RISKY_MODE_PROFILES[category] ||
    CATEGORY_RISKY_MODE_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function getRiskyQualityFloor(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    RISKY_QUALITY_FLOOR_BY_CATEGORY[category] ||
    RISKY_QUALITY_FLOOR_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function resolveDepthSignals(opportunity = {}, liquidity = {}) {
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const outlier = opportunity?.antiFake?.outlier && typeof opportunity.antiFake.outlier === "object"
    ? opportunity.antiFake.outlier
    : {}
  const depthQuality =
    opportunity?.antiFake?.depthQuality && typeof opportunity.antiFake.depthQuality === "object"
      ? opportunity.antiFake.depthQuality
      : {}
  const buyDepthMissing = Boolean(liquidity?.buyDepthMissing ?? outlier?.buyDepthMissing)
  const sellDepthMissing = Boolean(liquidity?.sellDepthMissing ?? outlier?.sellDepthMissing)
  const hasMissingDepth = Boolean(liquidity?.hasMissingDepth) || depthFlags.includes("MISSING_DEPTH")
  const hasOutlierAdjusted =
    Boolean(liquidity?.hasOutlierAdjusted) ||
    depthFlags.some((flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED")
  const hasSuspiciousDepthGap =
    Boolean(liquidity?.hasSuspiciousDepthGap) ||
    Boolean(depthQuality?.buyDepthGapSuspicious) ||
    Boolean(depthQuality?.sellDepthGapSuspicious) ||
    depthFlags.some((flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS")
  const hasExtremeDepthGap =
    Boolean(liquidity?.hasExtremeDepthGap) ||
    Boolean(depthQuality?.buyDepthGapExtreme) ||
    Boolean(depthQuality?.sellDepthGapExtreme) ||
    depthFlags.some((flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME")

  return {
    buyDepthMissing,
    sellDepthMissing,
    hasMissingDepth,
    hasOutlierAdjusted,
    hasSuspiciousDepthGap,
    hasExtremeDepthGap,
    hasBothDepthMissing: buyDepthMissing && sellDepthMissing
  }
}

function resolveReferenceSignals(opportunity = {}, liquidity = {}) {
  const referenceDeviation =
    opportunity?.antiFake?.referenceDeviation && typeof opportunity.antiFake.referenceDeviation === "object"
      ? opportunity.antiFake.referenceDeviation
      : {}
  const maxRatio =
    toFiniteOrNull(liquidity?.referenceDeviationRatio) ??
    toFiniteOrNull(referenceDeviation?.maxRatio)
  const hasStrongReferenceDeviation =
    Boolean(liquidity?.hasStrongReferenceDeviation) ||
    Boolean(referenceDeviation?.strong) ||
    (maxRatio ?? 0) > PREMIUM_REFERENCE_PENALTY_RATIO
  const hasExtremeReferenceDeviation =
    Boolean(liquidity?.hasExtremeReferenceDeviation) ||
    Boolean(referenceDeviation?.extreme) ||
    (maxRatio ?? 0) > PREMIUM_REFERENCE_REJECT_RATIO

  return {
    ratio: maxRatio,
    hasStrongReferenceDeviation,
    hasExtremeReferenceDeviation
  }
}

function computeRiskAdjustments({
  opportunity = {},
  liquidity = {},
  stale = {},
  inputItem = {},
  profile = RISKY_SCAN_PROFILE
} = {}) {
  const itemCategory = normalizeItemCategory(
    inputItem?.itemCategory || opportunity?.itemCategory,
    opportunity?.itemName || inputItem?.marketHashName
  )
  const rules = getCategoryScanRules(itemCategory, profile?.name || "strict")
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const staleMinutes = toFiniteOrNull(stale?.maxAgeMinutes)
  const quoteFreshnessState = String(
    stale?.selectedState || stale?.state || FRESHNESS_STATES.FRESH
  )
    .trim()
    .toLowerCase()
  const usableMarketsAfterFreshness = Math.max(
    Number(stale?.usableMarkets || marketCoverage || 0),
    0
  )
  const hasInsufficientUsableMarkets = Boolean(stale?.hasInsufficientUsableMarkets)
  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, itemCategory)
  const snapshotState = String(snapshotFreshness?.state || FRESHNESS_STATES.STALE)
    .trim()
    .toLowerCase()
  const hasSnapshotStale = snapshotState === FRESHNESS_STATES.STALE
  const hasSnapshotAging = snapshotState === FRESHNESS_STATES.AGING
  const isPremiumCategory = PREMIUM_ITEM_CATEGORIES.has(itemCategory)
  const isRiskyProfile = String(profile?.name || "").trim().toLowerCase().startsWith("risky")
  const minProfitUsd = Number(rules.minProfitUsd ?? profile?.minProfitUsd ?? 0)
  const allowMissingDepthWithPenalty = Boolean(
    rules.allowMissingDepthWithPenalty ?? profile?.allowMissingDepthWithPenalty
  )
  const depthSignals = resolveDepthSignals(opportunity, liquidity)
  const referenceSignals = resolveReferenceSignals(opportunity, liquidity)

  if (profit == null || profit <= 0) {
    return { passed: false, primaryReason: "non_positive_profit", penalty: 0 }
  }
  if (isRiskyProfile && profit < minProfitUsd) {
    return { passed: false, primaryReason: "risky_low_profit", penalty: 0 }
  }
  if (buyPrice == null || buyPrice < Number(rules.minPriceUsd || profile.minPriceUsd || 0)) {
    return {
      passed: false,
      primaryReason: isRiskyProfile ? "risky_low_price" : "ignored_execution_floor",
      penalty: 0
    }
  }
  if (
    spread == null ||
    spread < Number(rules.minSpreadPercent || profile.minSpreadPercent || 0) ||
    spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)
  ) {
    return {
      passed: false,
      primaryReason:
        spread != null && spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)
          ? "ignored_extreme_spread"
          : "spread_below_min",
      penalty: 0
    }
  }
  if (hasInsufficientUsableMarkets || usableMarketsAfterFreshness < MIN_MARKET_COVERAGE) {
    return { passed: false, primaryReason: "ignored_stale_data", penalty: 0 }
  }
  if (marketCoverage < Number(rules.minMarketCoverage || MIN_MARKET_COVERAGE)) {
    return { passed: false, primaryReason: "ignored_missing_markets", penalty: 0 }
  }

  if (isPremiumCategory) {
    if (referenceSignals.hasExtremeReferenceDeviation) {
      return { passed: false, primaryReason: "ignored_reference_deviation", penalty: 0 }
    }
    if (depthSignals.hasExtremeDepthGap) {
      return { passed: false, primaryReason: "ignored_missing_depth", penalty: 0 }
    }
    if (volume7d != null && volume7d < PREMIUM_MIN_VOLUME_REJECT) {
      return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
    }
    if (volume7d == null) {
      if (profile.name === "strict" || !profile.allowMissingLiquidity) {
        return { passed: false, primaryReason: "ignored_missing_liquidity_data", penalty: 0 }
      }
      if (marketCoverage < PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) {
        return { passed: false, primaryReason: "ignored_missing_markets", penalty: 0 }
      }
      if (
        depthSignals.hasMissingDepth ||
        depthSignals.hasOutlierAdjusted ||
        depthSignals.hasSuspiciousDepthGap
      ) {
        return {
          passed: false,
          primaryReason: isRiskyProfile ? "risky_missing_depth" : "ignored_missing_depth",
          penalty: 0
        }
      }
      if (referenceSignals.hasStrongReferenceDeviation) {
        return { passed: false, primaryReason: "ignored_reference_deviation", penalty: 0 }
      }
    } else if (volume7d < Number(rules.minVolume7d || profile.minVolume7d || PREMIUM_MIN_VOLUME_REJECT)) {
      return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
    }
  } else if (volume7d == null) {
    if (!profile.allowMissingLiquidity) {
      return { passed: false, primaryReason: "ignored_missing_liquidity_data", penalty: 0 }
    }
  } else if (volume7d < Number(rules.minVolume7d || profile.minVolume7d || 0)) {
    return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
  }

  if (
    !isPremiumCategory &&
    isRiskyProfile &&
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
    depthSignals.hasMissingDepth &&
    marketCoverage <= MIN_MARKET_COVERAGE
  ) {
    return { passed: false, primaryReason: "risky_missing_depth", penalty: 0 }
  }

  const weakPremiumConfidence =
    hasSnapshotStale ||
    quoteFreshnessState === FRESHNESS_STATES.STALE ||
    (staleMinutes != null && staleMinutes >= Number(getCategoryStaleRules(itemCategory).agingMaxMinutes || 0)) ||
    referenceSignals.hasStrongReferenceDeviation ||
    depthSignals.hasOutlierAdjusted ||
    depthSignals.hasSuspiciousDepthGap
  if (isPremiumCategory && depthSignals.hasBothDepthMissing) {
    if (!isRiskyProfile || !allowMissingDepthWithPenalty || weakPremiumConfidence) {
      return {
        passed: false,
        primaryReason: isRiskyProfile ? "risky_missing_depth" : "ignored_missing_depth",
        penalty: 0
      }
    }
  }
  if (isPremiumCategory && depthSignals.hasMissingDepth && !depthSignals.hasBothDepthMissing) {
    if (!isRiskyProfile || !allowMissingDepthWithPenalty) {
      return { passed: false, primaryReason: "ignored_missing_depth", penalty: 0 }
    }
    if (weakPremiumConfidence && marketCoverage <= MIN_MARKET_COVERAGE) {
      return { passed: false, primaryReason: "risky_missing_depth", penalty: 0 }
    }
  }

  if (isPremiumCategory && isRiskyProfile && quoteFreshnessState === FRESHNESS_STATES.STALE) {
    if (
      usableMarketsAfterFreshness <= MIN_MARKET_COVERAGE ||
      referenceSignals.hasStrongReferenceDeviation
    ) {
      return { passed: false, primaryReason: "ignored_stale_data", penalty: 0 }
    }
  }

  let penalty = 0
  if (isPremiumCategory) {
    if (volume7d == null) {
      penalty += 18
      if (marketCoverage === PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) {
        penalty += 8
      }
    } else if (volume7d < PREMIUM_MIN_VOLUME_MEDIUM) {
      penalty += 14
    } else if (volume7d < PREMIUM_MIN_VOLUME_HIGH) {
      penalty += 7
    }

    if (spread != null && spread > PREMIUM_SPREAD_HEAVY_PENALTY_PERCENT) {
      penalty += 22
    }
    if (depthSignals.hasBothDepthMissing) {
      penalty += 28
    } else if (depthSignals.hasMissingDepth) {
      penalty += 22
    } else if (depthSignals.hasOutlierAdjusted || depthSignals.hasSuspiciousDepthGap) {
      penalty += 16
    }
    if (referenceSignals.hasStrongReferenceDeviation) {
      penalty += 18
    }
    if (quoteFreshnessState === FRESHNESS_STATES.STALE) {
      penalty += Number(getCategoryStaleRules(itemCategory).stalePenalty || 12) + 8
    } else if (quoteFreshnessState === FRESHNESS_STATES.AGING) {
      penalty += Number(getCategoryStaleRules(itemCategory).agingPenalty || 4)
    }
    if (hasSnapshotStale) {
      penalty += 8
    } else if (hasSnapshotAging) {
      penalty += 4
    }
  } else {
    if (volume7d == null) {
      penalty += profile.allowMissingLiquidity ? 16 : 0
    } else if (volume7d < Number(rules.minVolume7d || MIN_VOLUME_7D)) {
      penalty += volume7d < 60 ? 14 : 8
    }

    if (buyPrice < Number(rules.minPriceUsd || MIN_EXECUTION_PRICE_USD)) {
      penalty += 7
    }
    if (spread < Number(rules.minSpreadPercent || MIN_SPREAD_PERCENT)) {
      penalty += 10
    }
    if (quoteFreshnessState === FRESHNESS_STATES.STALE) {
      penalty += Number(getCategoryStaleRules(itemCategory).stalePenalty || 20)
    } else if (quoteFreshnessState === FRESHNESS_STATES.AGING) {
      penalty += Number(getCategoryStaleRules(itemCategory).agingPenalty || 8)
    }
    if (hasSnapshotStale) {
      penalty += 10
    } else if (hasSnapshotAging) {
      penalty += 5
    }
    if (
      isRiskyProfile &&
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      (depthSignals.hasMissingDepth || depthSignals.hasOutlierAdjusted)
    ) {
      penalty += marketCoverage <= MIN_MARKET_COVERAGE ? 14 : 7
    }
    if (isRiskyProfile && itemCategory === ITEM_CATEGORIES.WEAPON_SKIN && profit < 1.2) {
      penalty += 6
    }
  }

  return {
    passed: true,
    primaryReason: "",
    penalty: round2(Math.max(penalty, 0))
  }
}

function normalizeConfidence(value) {
  const safe = String(value || "").trim().toLowerCase()
  if (safe === "high") return "High"
  if (safe === "medium") return "Medium"
  return "Low"
}

function downgradeConfidenceForStale(
  baseConfidence,
  stale = {},
  snapshotFreshnessState = FRESHNESS_STATES.FRESH
) {
  const confidence = normalizeConfidence(baseConfidence)
  const quoteState = String(stale?.selectedState || stale?.state || FRESHNESS_STATES.FRESH)
    .trim()
    .toLowerCase()
  const snapshotState = String(snapshotFreshnessState || FRESHNESS_STATES.FRESH)
    .trim()
    .toLowerCase()
  if (quoteState === FRESHNESS_STATES.STALE) return "Low"
  if (quoteState === FRESHNESS_STATES.FRESH && snapshotState === FRESHNESS_STATES.FRESH) {
    return confidence
  }
  if (snapshotState === FRESHNESS_STATES.STALE && confidence === "High") return "Low"
  if (confidence === "High") return "Medium"
  if (confidence === "Medium") return "Low"
  return "Low"
}

function normalizeBadges(rawBadges = []) {
  const unique = new Set()
  for (const badge of Array.isArray(rawBadges) ? rawBadges : []) {
    const text = String(badge || "").trim()
    if (!text) continue
    unique.add(text)
  }
  return Array.from(unique)
}

function buildApiOpportunityRow({
  opportunity = {},
  inputItem = {},
  liquidity = {},
  stale = {},
  perMarket = [],
  extraPenalty = 0,
  isHighConfidenceEligible = false,
  isRiskyEligible = false
}) {
  const bySource = toBySourceMap(perMarket)
  const buySource = normalizeMarketLabel(opportunity?.buyMarket)
  const sellSource = normalizeMarketLabel(opportunity?.sellMarket)
  const buyQuote = bySource[buySource] || null
  const sellQuote = bySource[sellSource] || null
  const itemCategory = normalizeItemCategory(
    opportunity?.itemCategory || inputItem?.itemCategory,
    opportunity?.itemName || inputItem?.marketHashName
  )
  const baseScore = toFiniteOrNull(opportunity?.opportunityScore) ?? 0
  const stalePenalty = toFiniteOrNull(stale?.penalty) ?? 0
  const softPenalty = toFiniteOrNull(extraPenalty) ?? 0
  const score = clampScore(baseScore - stalePenalty - softPenalty)
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const hasOutlierAdjusted = depthFlags.some(
    (flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED"
  )
  const hasMissingDepth = depthFlags.includes("MISSING_DEPTH")
  const hasDepthGapSuspicious = depthFlags.some(
    (flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS"
  )
  const hasDepthGapExtreme = depthFlags.some(
    (flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME"
  )
  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, itemCategory)
  const snapshotFreshnessState = normalizeFreshnessState(snapshotFreshness?.state)
  const snapshotStale = snapshotFreshnessState === FRESHNESS_STATES.STALE
  const quoteFreshnessState = normalizeFreshnessState(stale?.selectedState || stale?.state)
  const executionConfidence = downgradeConfidenceForStale(
    opportunity?.executionConfidence,
    stale,
    snapshotFreshnessState
  )
  const buyPrice = roundPrice(opportunity?.buyPrice || 0)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const premiumRiskyBadges =
    isRiskyEligible && PREMIUM_ITEM_CATEGORIES.has(itemCategory)
      ? [
          "Premium category",
          buyPrice >= 100 ? "High-ticket opportunity" : "",
          volume7d == null || volume7d < PREMIUM_MIN_VOLUME_HIGH ? "Low-frequency market" : "",
          hasMissingDepth ? "Missing depth tolerated" : ""
        ].filter(Boolean)
      : []

  const badges = normalizeBadges([
    ...(Array.isArray(opportunity?.reasonBadges) ? opportunity.reasonBadges : []),
    ...premiumRiskyBadges,
    quoteFreshnessState === FRESHNESS_STATES.STALE ? ["Stale market data"] : [],
    quoteFreshnessState === FRESHNESS_STATES.AGING ? ["Aging market data"] : [],
    snapshotFreshnessState === FRESHNESS_STATES.STALE ? ["Stale snapshot"] : [],
    hasDepthGapExtreme ? ["Depth anomaly"] : [],
    !hasDepthGapExtreme && hasDepthGapSuspicious ? ["Depth gap flagged"] : [],
    !hasDepthGapExtreme && !hasDepthGapSuspicious && hasMissingDepth ? ["Missing depth"] : [],
    hasOutlierAdjusted ? ["Outlier adjusted"] : [],
    !hasOutlierAdjusted && !hasMissingDepth && !hasDepthGapSuspicious && !hasDepthGapExtreme
      ? ["Good depth"]
      : []
  ])

  return {
    itemId: Number(opportunity?.itemId || inputItem?.skinId || 0) || null,
    itemName: String(opportunity?.itemName || inputItem?.marketHashName || "Tracked Item"),
    itemCategory,
    itemSubcategory: String(inputItem?.itemSubcategory || "").trim() || null,
    itemRarity:
      String(
        opportunity?.itemRarity || inputItem?.itemRarity || inputItem?.rarity || ""
      ).trim() || null,
    itemRarityColor:
      String(
        opportunity?.itemRarityColor ||
          inputItem?.itemRarityColor ||
          inputItem?.rarityColor ||
          ""
      ).trim() || null,
    itemImageUrl:
      sanitizeImageUrl(inputItem?.itemImageUrlLarge || inputItem?.itemImageUrl) || null,
    buyMarket: buySource || null,
    buyPrice,
    sellMarket: sellSource || null,
    sellNet: roundPrice(opportunity?.sellNet || 0),
    profit: roundPrice(opportunity?.profit || 0),
    spread: round2(opportunity?.spreadPercent || opportunity?.spread_pct || 0),
    score,
    scoreCategory: String(opportunity?.scoreCategory || arbitrageEngine.categorizeOpportunityScore(score)),
    executionConfidence,
    liquidityBand: String(opportunity?.liquidityBand || "Low"),
    liquidity:
      toFiniteOrNull(liquidity?.volume7d) ??
      toFiniteOrNull(liquidity?.liquidityScore) ??
      toFiniteOrNull(opportunity?.liquiditySample) ??
      null,
    liquidityScore: toFiniteOrNull(liquidity?.liquidityScore),
    volume7d,
    marketCoverage: Number(opportunity?.marketCoverage || 0),
    referencePrice: toFiniteOrNull(opportunity?.referencePrice ?? inputItem?.referencePrice),
    stalePenalty,
    softPenalty,
    maxQuoteAgeMinutes: toFiniteOrNull(stale?.maxAgeMinutes),
    quoteFreshnessState,
    snapshotFreshnessState,
    staleMarkets: Number(stale?.staleMarkets || 0),
    agingMarkets: Number(stale?.agingMarkets || 0),
    usableMarketsAfterFreshness: Number(stale?.usableMarkets || 0),
    buyUrl: buyQuote?.url || opportunity?.buyUrl || null,
    sellUrl: sellQuote?.url || opportunity?.sellUrl || null,
    snapshotStale,
    flags: depthFlags,
    badges,
    spreadScore: toFiniteOrNull(opportunity?.scores?.spreadScore),
    liquidityScoreComponent: toFiniteOrNull(opportunity?.scores?.liquidityScore),
    stabilityScore: toFiniteOrNull(opportunity?.scores?.stabilityScore),
    marketReliabilityScore: toFiniteOrNull(opportunity?.scores?.marketScore),
    depthConfidenceScore: toFiniteOrNull(opportunity?.scores?.depthConfidenceScore),
    rawOpportunityScore: baseScore,
    isHighConfidenceEligible: Boolean(isHighConfidenceEligible),
    isRiskyEligible: Boolean(isRiskyEligible)
  }
}

function confidenceRank(value) {
  const safe = normalizeConfidence(value)
  if (safe === "High") return 3
  if (safe === "Medium") return 2
  return 1
}

function toGradeFromScore(score, row = {}) {
  const safeScore = clampScore(score)
  const executionConfidence = normalizeConfidence(row?.executionConfidence)
  const isHighConfidence = Boolean(row?.isHighConfidenceEligible)
  if (isHighConfidence && safeScore >= 90) return "A"
  if (isHighConfidence && safeScore >= 78) return "B"
  if (isHighConfidence) return "C"
  if (safeScore >= DEFAULT_SCORE_CUTOFF && executionConfidence !== "Low") return "C"
  return "RISKY"
}

function toMetadataObject(row = {}) {
  const rawQuoteState = String(row?.quoteFreshnessState || "").trim()
  const rawSnapshotState = String(row?.snapshotFreshnessState || "").trim()
  return {
    item_id: Number(row?.itemId || 0) || null,
    item_subcategory: String(row?.itemSubcategory || "").trim() || null,
    item_rarity: String(row?.itemRarity || "").trim() || null,
    item_rarity_color: String(row?.itemRarityColor || "").trim() || null,
    item_image_url: sanitizeImageUrl(row?.itemImageUrl) || null,
    score_category: String(row?.scoreCategory || "").trim() || null,
    liquidity_value: toFiniteOrNull(row?.liquidity),
    liquidity_score: toFiniteOrNull(row?.liquidityScore),
    volume_7d: toFiniteOrNull(row?.volume7d),
    market_coverage: Number(row?.marketCoverage || 0),
    reference_price: toFiniteOrNull(row?.referencePrice),
    stale_penalty: toFiniteOrNull(row?.stalePenalty),
    quote_freshness_state: rawQuoteState
      ? normalizeFreshnessState(rawQuoteState)
      : FRESHNESS_STATES.FRESH,
    snapshot_freshness_state: rawSnapshotState
      ? normalizeFreshnessState(rawSnapshotState)
      : Boolean(row?.snapshotStale)
        ? FRESHNESS_STATES.STALE
        : FRESHNESS_STATES.FRESH,
    stale_markets: Number(row?.staleMarkets || 0),
    aging_markets: Number(row?.agingMarkets || 0),
    usable_markets_after_freshness: Number(row?.usableMarketsAfterFreshness || 0),
    soft_penalty: toFiniteOrNull(row?.softPenalty),
    buy_url: String(row?.buyUrl || "").trim() || null,
    sell_url: String(row?.sellUrl || "").trim() || null,
    flags: Array.isArray(row?.flags) ? row.flags : [],
    badges: Array.isArray(row?.badges) ? row.badges : [],
    spread_score: toFiniteOrNull(row?.spreadScore),
    liquidity_score_component: toFiniteOrNull(row?.liquidityScoreComponent),
    stability_score: toFiniteOrNull(row?.stabilityScore),
    market_reliability_score: toFiniteOrNull(row?.marketReliabilityScore),
    depth_confidence_score: toFiniteOrNull(row?.depthConfidenceScore),
    raw_opportunity_score: toFiniteOrNull(row?.rawOpportunityScore),
    is_high_confidence_eligible: Boolean(row?.isHighConfidenceEligible),
    is_risky_eligible: Boolean(row?.isRiskyEligible),
    snapshot_stale: Boolean(row?.snapshotStale)
  }
}

function buildFeedInsertRow(row = {}, scanRunId = "", options = {}) {
  const detectedAt = options.detectedAt || new Date().toISOString()
  const isDuplicate = Boolean(options.isDuplicate)
  const score = clampScore(row?.score)
  const executionConfidence = normalizeConfidence(row?.executionConfidence)
  return {
    item_name: String(row?.itemName || "Tracked Item").trim(),
    market_hash_name: String(row?.itemName || "Tracked Item").trim(),
    category: normalizeItemCategory(row?.itemCategory, row?.itemName),
    buy_market: normalizeMarketLabel(row?.buyMarket),
    buy_price: roundPrice(row?.buyPrice || 0),
    sell_market: normalizeMarketLabel(row?.sellMarket),
    sell_net: roundPrice(row?.sellNet || 0),
    profit: roundPrice(row?.profit || 0),
    spread_pct: round2(row?.spread || 0),
    opportunity_score: Math.round(score),
    execution_confidence: executionConfidence,
    quality_grade: toGradeFromScore(score, row),
    liquidity_label: String(row?.liquidityBand || "Low").trim() || "Low",
    detected_at: detectedAt,
    scan_run_id: String(scanRunId || "").trim() || null,
    is_active: true,
    is_duplicate: isDuplicate,
    metadata: toMetadataObject(row)
  }
}

function buildDedupeSignature(itemName = "", buyMarket = "", sellMarket = "") {
  return `${String(itemName || "").trim().toLowerCase()}::${String(buyMarket || "")
    .trim()
    .toLowerCase()}::${String(sellMarket || "")
    .trim()
    .toLowerCase()}`
}

function toProfitDeltaPercent(currentProfit, previousProfit) {
  const current = toFiniteOrNull(currentProfit)
  const previous = toFiniteOrNull(previousProfit)
  if (current == null || previous == null) return Infinity
  const denominator = Math.max(Math.abs(previous), 0.01)
  return Math.abs(current - previous) / denominator * 100
}

function isMateriallyNewOpportunity(current = {}, previous = {}) {
  const profitDeltaPercent = toProfitDeltaPercent(current?.profit, previous?.profit)
  const scoreDelta = Math.abs(
    (toFiniteOrNull(current?.score) ?? 0) - (toFiniteOrNull(previous?.opportunity_score) ?? 0)
  )
  return profitDeltaPercent >= MIN_PROFIT_CHANGE_PCT || scoreDelta >= MIN_SCORE_CHANGE
}

function mapFeedRowToApiRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === "object" ? row.metadata : {}
  const score = clampScore(row?.opportunity_score)
  const executionConfidence = normalizeConfidence(row?.execution_confidence)
  const itemName = String(row?.item_name || "Tracked Item").trim() || "Tracked Item"
  const rawQuoteState = String(metadata?.quote_freshness_state || "").trim()
  const rawSnapshotState = String(metadata?.snapshot_freshness_state || "").trim()
  return {
    feedId: String(row?.id || "").trim() || null,
    detectedAt: row?.detected_at || null,
    scanRunId: String(row?.scan_run_id || "").trim() || null,
    isActive: Boolean(row?.is_active),
    isDuplicate: Boolean(row?.is_duplicate),
    itemId: Number(metadata?.item_id || 0) || null,
    itemName,
    itemCategory: normalizeItemCategory(row?.category, itemName),
    itemSubcategory: String(metadata?.item_subcategory || "").trim() || null,
    itemRarity: String(metadata?.item_rarity || "").trim() || null,
    itemRarityColor: String(metadata?.item_rarity_color || "").trim() || null,
    itemImageUrl: sanitizeImageUrl(metadata?.item_image_url) || null,
    buyMarket: normalizeMarketLabel(row?.buy_market),
    buyPrice: roundPrice(row?.buy_price || 0),
    sellMarket: normalizeMarketLabel(row?.sell_market),
    sellNet: roundPrice(row?.sell_net || 0),
    profit: roundPrice(row?.profit || 0),
    spread: round2(row?.spread_pct || 0),
    score,
    scoreCategory:
      String(metadata?.score_category || "").trim() || arbitrageEngine.categorizeOpportunityScore(score),
    executionConfidence,
    qualityGrade: String(row?.quality_grade || "").trim() || null,
    liquidityBand: String(row?.liquidity_label || "Low").trim() || "Low",
    liquidityLabel: String(row?.liquidity_label || "Low").trim() || "Low",
    liquidity: toFiniteOrNull(metadata?.liquidity_value),
    liquidityScore: toFiniteOrNull(metadata?.liquidity_score),
    volume7d: toFiniteOrNull(metadata?.volume_7d),
    marketCoverage: Number(metadata?.market_coverage || 0),
    referencePrice: toFiniteOrNull(metadata?.reference_price),
    stalePenalty: toFiniteOrNull(metadata?.stale_penalty),
    quoteFreshnessState: rawQuoteState
      ? normalizeFreshnessState(rawQuoteState)
      : FRESHNESS_STATES.FRESH,
    snapshotFreshnessState: rawSnapshotState
      ? normalizeFreshnessState(rawSnapshotState)
      : Boolean(metadata?.snapshot_stale)
        ? FRESHNESS_STATES.STALE
        : FRESHNESS_STATES.FRESH,
    staleMarkets: Number(metadata?.stale_markets || 0),
    agingMarkets: Number(metadata?.aging_markets || 0),
    usableMarketsAfterFreshness: Number(metadata?.usable_markets_after_freshness || 0),
    softPenalty: toFiniteOrNull(metadata?.soft_penalty),
    buyUrl: String(metadata?.buy_url || "").trim() || null,
    sellUrl: String(metadata?.sell_url || "").trim() || null,
    snapshotStale: Boolean(metadata?.snapshot_stale),
    flags: Array.isArray(metadata?.flags) ? metadata.flags : [],
    badges: Array.isArray(metadata?.badges) ? metadata.badges : [],
    spreadScore: toFiniteOrNull(metadata?.spread_score),
    liquidityScoreComponent: toFiniteOrNull(metadata?.liquidity_score_component),
    stabilityScore: toFiniteOrNull(metadata?.stability_score),
    marketReliabilityScore: toFiniteOrNull(metadata?.market_reliability_score),
    depthConfidenceScore: toFiniteOrNull(metadata?.depth_confidence_score),
    rawOpportunityScore: toFiniteOrNull(metadata?.raw_opportunity_score),
    isHighConfidenceEligible: Boolean(metadata?.is_high_confidence_eligible),
    isRiskyEligible: Boolean(metadata?.is_risky_eligible)
  }
}

async function enrichFeedRowsWithSkinMetadata(rows = []) {
  const items = Array.isArray(rows) ? rows : []
  if (!items.length) return []

  const skinIds = Array.from(
    new Set(
      items
        .map((row) => Number(row?.itemId || 0))
        .filter((value) => Number.isInteger(value) && value > 0)
    )
  )
  const marketHashNames = Array.from(
    new Set(
      items
        .map((row) => normalizeMarketHashName(row?.itemName))
        .filter(Boolean)
    )
  )
  if (!marketHashNames.length && !skinIds.length) return items

  let skinsByNameRows = []
  let skinsByIdRows = []
  try {
    const [byName, byId] = await Promise.all([
      marketHashNames.length ? skinRepo.getByMarketHashNames(marketHashNames) : Promise.resolve([]),
      skinIds.length ? skinRepo.getByIds(skinIds) : Promise.resolve([])
    ])
    skinsByNameRows = Array.isArray(byName) ? byName : []
    skinsByIdRows = Array.isArray(byId) ? byId : []
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to enrich feed rows with skin metadata", err.message)
    return items
  }

  const skinsById = {}
  for (const row of skinsByIdRows) {
    const id = Number(row?.id || 0)
    if (!Number.isInteger(id) || id <= 0) continue
    skinsById[id] = row
  }
  const skinsByName = toByNameMap(
    skinsByNameRows.map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const merged = items.map((row) => {
    const key = normalizeMarketHashName(row?.itemName)
    const skinId = Number(row?.itemId || 0)
    const skin =
      (Number.isInteger(skinId) && skinId > 0 ? skinsById[skinId] || null : null) ||
      (key ? skinsByName[key] || null : null)
    if (!skin) return row
    const currentImage = sanitizeImageUrl(row?.itemImageUrl)
    const skinImage = sanitizeImageUrl(skin?.image_url_large || skin?.image_url)
    return {
      ...row,
      itemRarity:
        String(row?.itemRarity || "").trim() || String(skin?.rarity || "").trim() || null,
      itemRarityColor:
        String(row?.itemRarityColor || "").trim() ||
        String(skin?.rarity_color || skin?.rarityColor || "").trim() ||
        null,
      itemImageUrl:
        currentImage || skinImage || null
    }
  })

  const missingMetadataNames = Array.from(
    new Set(
      merged
        .filter((row) => {
          const itemName = normalizeMarketHashName(row?.itemName)
          if (!itemName) return false
          const rarity = String(row?.itemRarity || "")
            .trim()
            .toLowerCase()
          const missingImage = !sanitizeImageUrl(row?.itemImageUrl)
          const missingRarity = !rarity
          const weakRarity = rarity === "consumer grade"
          return missingImage || missingRarity || weakRarity
        })
        .map((row) => normalizeMarketHashName(row?.itemName))
        .filter(Boolean)
    )
  ).slice(0, FEED_METADATA_ENRICH_LIMIT)

  if (!missingMetadataNames.length) {
    return merged
  }

  let steamMetadataByName = {}
  try {
    steamMetadataByName = await marketImageService.fetchSteamSearchMetadataBatch(
      missingMetadataNames,
      {
        timeoutMs: 8000,
        maxRetries: 1,
        concurrency: 2,
        count: 50
      }
    )
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to fetch steam metadata for feed rows", err.message)
    steamMetadataByName = {}
  }

  const steamKeys = Object.keys(steamMetadataByName || {})
  if (!steamKeys.length) {
    return merged
  }

  try {
    const upsertRows = steamKeys
      .map((marketHashName) => {
        const metadata = steamMetadataByName[marketHashName] || {}
        const imageUrl = String(metadata?.imageUrl || "").trim() || null
        const imageUrlLarge = String(metadata?.imageUrlLarge || "").trim() || imageUrl
        const rarity = String(metadata?.rarity || "").trim() || null
        const rarityColor = String(metadata?.rarityColor || "").trim() || null
        if (!imageUrl && !imageUrlLarge && !rarity && !rarityColor) {
          return null
        }
        return {
          market_hash_name: marketHashName,
          rarity,
          rarity_color: rarityColor,
          image_url: imageUrl,
          image_url_large: imageUrlLarge
        }
      })
      .filter(Boolean)
    if (upsertRows.length) {
      await skinRepo.upsertSkins(upsertRows)
    }
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to persist steam metadata enrichment", err.message)
  }

  return merged.map((row) => {
    const key = normalizeMarketHashName(row?.itemName)
    const metadata = key ? steamMetadataByName[key] || null : null
    if (!metadata) return row

    const currentRarity = String(row?.itemRarity || "").trim()
    const currentRarityColor = String(row?.itemRarityColor || "").trim()
    const shouldReplaceRarity = !currentRarity || currentRarity.toLowerCase() === "consumer grade"
    const shouldReplaceRarityColor =
      shouldReplaceRarity ||
      !currentRarityColor ||
      currentRarityColor.toLowerCase() === "#b0c3d9" ||
      currentRarityColor.toLowerCase() === "#7f8ba5"
    const currentImage = sanitizeImageUrl(row?.itemImageUrl)
    const metadataImage = sanitizeImageUrl(metadata?.imageUrlLarge || metadata?.imageUrl)

    return {
      ...row,
      itemRarity:
        shouldReplaceRarity && metadata?.rarity
          ? String(metadata.rarity).trim() || currentRarity || null
          : currentRarity || null,
      itemRarityColor:
        shouldReplaceRarityColor && metadata?.rarityColor
          ? String(metadata.rarityColor).trim() || currentRarityColor || null
          : currentRarityColor || null,
      itemImageUrl:
        currentImage || metadataImage || null
    }
  })
}

function computeRiskyRankScore(row = {}) {
  const baseScore = Number(row?.score || 0)
  const category = normalizeItemCategory(row?.itemCategory, row?.itemName)
  const price = toFiniteOrNull(row?.buyPrice) ?? 0
  const profit = toFiniteOrNull(row?.profit) ?? 0
  const volume7d = toFiniteOrNull(row?.volume7d)
  const marketCoverage = Number(row?.marketCoverage || 0)
  const confidence = normalizeConfidence(row?.executionConfidence)
  const hasMissingDepth = Array.isArray(row?.flags) && row.flags.includes("MISSING_DEPTH")
  const isHighConfidence = Boolean(row?.isHighConfidenceEligible)
  let rankScore = baseScore

  if (isHighConfidence) {
    return rankScore + 20
  }

  if (PREMIUM_ITEM_CATEGORIES.has(category)) {
    rankScore += 6
    rankScore += Math.min(price / 120, 10)
    rankScore += Math.min(profit / 4, 8)
    rankScore += Math.min(marketCoverage * 2, 8)
    if (confidence !== "Low") rankScore += 2
    if (volume7d != null && volume7d < PREMIUM_MIN_VOLUME_HIGH) rankScore -= 2
    if (hasMissingDepth) rankScore -= 3
    return round2(rankScore)
  }

  if (category === ITEM_CATEGORIES.WEAPON_SKIN) {
    if (price < 6) rankScore -= 8
    if (profit < 1) rankScore -= 7
    if ((volume7d ?? 0) < 60) rankScore -= 5
    if (confidence === "Low") rankScore -= 6
  }
  return round2(rankScore)
}

function sortOpportunities(rows = []) {
  return [...rows].sort((a, b) => {
    const riskyRankDelta = computeRiskyRankScore(b) - computeRiskyRankScore(a)
    if (riskyRankDelta) return riskyRankDelta
    return (
      Number(b?.score || 0) - Number(a?.score || 0) ||
      confidenceRank(b?.executionConfidence) - confidenceRank(a?.executionConfidence) ||
      Number(b?.profit || 0) - Number(a?.profit || 0) ||
      Number(b?.spread || 0) - Number(a?.spread || 0)
    )
  })
}

function buildInputItemForComparison(item = {}) {
  return {
    skinId: Number(item?.skinId || 0) || null,
    marketHashName: String(item?.marketHashName || "").trim(),
    itemCategory: normalizeItemCategory(item?.itemCategory, item?.marketHashName),
    itemSubcategory: String(item?.itemSubcategory || "").trim() || null,
    itemRarity: String(item?.itemRarity || item?.rarity || "").trim() || null,
    itemRarityColor:
      String(item?.itemRarityColor || item?.rarityColor || "").trim() || null,
    imageUrl: sanitizeImageUrl(item?.itemImageUrl) || null,
    imageUrlLarge:
      sanitizeImageUrl(item?.itemImageUrlLarge) || sanitizeImageUrl(item?.itemImageUrl) || null,
    quantity: 1,
    steamPrice:
      toFiniteOrNull(item?.referencePrice) ??
      toFiniteOrNull(item?.steamPrice) ??
      0,
    steamCurrency: "USD",
    steamRecordedAt: item?.snapshotCapturedAt || null,
    sevenDayChangePercent: toFiniteOrNull(item?.sevenDayChangePercent),
    liquidityScore: toFiniteOrNull(item?.liquidityScore),
    marketVolume7d: toFiniteOrNull(item?.marketVolume7d),
    referencePrice: toFiniteOrNull(item?.referencePrice),
    snapshotStale: Boolean(item?.snapshotStale),
    universeSource: String(item?.universeSource || "").trim() || "fallback_curated",
    liquidityRank: toFiniteOrNull(item?.liquidityRank)
  }
}

function collectDiscardReasonsFromOpportunity(
  opportunity = {},
  discardStats = {},
  rejectedByItem = null,
  itemName = "",
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const reasons = Array.isArray(opportunity?.antiFake?.reasons) ? opportunity.antiFake.reasons : []
  for (const reason of reasons) {
    incrementReasonCounter(discardStats, reason, itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        itemName || opportunity?.itemName,
        reason,
        itemCategory
      )
    }
  }

  const debugReasons = Array.isArray(opportunity?.antiFake?.debugReasons)
    ? opportunity.antiFake.debugReasons
    : []
  for (const reason of debugReasons) {
    incrementReasonCounter(discardStats, reason, itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        itemName || opportunity?.itemName,
        reason,
        itemCategory
      )
    }
  }
}

function applyGuardFallbackReason(
  opportunity = {},
  liquidity = {},
  discardStats = {},
  rejectedByItem = null,
  itemName = "",
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const rules = getCategoryScanRules(itemCategory, "risky")
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const hasMissingDepth = Boolean(liquidity?.hasMissingDepth)
  const hasBothDepthMissing = Boolean(liquidity?.buyDepthMissing) && Boolean(liquidity?.sellDepthMissing)
  const hasSuspiciousDepthGap = Boolean(liquidity?.hasSuspiciousDepthGap)
  const hasExtremeDepthGap = Boolean(liquidity?.hasExtremeDepthGap)
  const referenceSignals = resolveReferenceSignals(opportunity, liquidity)
  const isPremiumCategory = PREMIUM_ITEM_CATEGORIES.has(normalizeItemCategory(itemCategory))
  const targetItemName = itemName || opportunity?.itemName
  const record = (reason) => {
    incrementReasonCounter(discardStats, reason, itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(rejectedByItem, targetItemName, reason, itemCategory)
    }
  }
  if (buyPrice != null && buyPrice < Number(rules.minPriceUsd || MIN_EXECUTION_PRICE_USD)) {
    record("ignored_execution_floor")
  }
  if (
    (!isPremiumCategory && (volume7d == null || volume7d < Number(rules.minVolume7d || MIN_VOLUME_7D))) ||
    (isPremiumCategory && volume7d != null && volume7d < PREMIUM_MIN_VOLUME_REJECT)
  ) {
    record("ignored_low_liquidity")
  }
  if (isPremiumCategory && volume7d == null && marketCoverage < PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) {
    record("ignored_missing_markets")
  }
  if (spread != null && spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)) {
    record("ignored_extreme_spread")
  }
  if (marketCoverage < Number(rules.minMarketCoverage || MIN_MARKET_COVERAGE)) {
    record("ignored_missing_markets")
  }
  if (isPremiumCategory && referenceSignals.hasExtremeReferenceDeviation) {
    record("ignored_reference_deviation")
  }
  if (
    isPremiumCategory &&
    (hasBothDepthMissing || hasMissingDepth || hasSuspiciousDepthGap || hasExtremeDepthGap)
  ) {
    record("ignored_missing_depth")
  }
}

const scannerState = {
  latest: null,
  inFlight: null,
  inFlightRunId: null,
  timer: null,
  lastError: null,
  nextScheduledAt: null,
  lastStartedAt: null,
  lastCompletedAt: null,
  lastPersistSummary: null
}

function parseTimestampMs(value) {
  const text = String(value || "").trim()
  if (!text) return null
  const parsed = new Date(text).getTime()
  return Number.isFinite(parsed) ? parsed : null
}

function updateNextScheduledAt() {
  scannerState.nextScheduledAt = new Date(Date.now() + SCANNER_INTERVAL_MS).toISOString()
}

function isScannerRunOverdue(status = {}, nowMs = Date.now()) {
  const latestStartedMs = parseTimestampMs(status?.latestRun?.started_at || scannerState.lastStartedAt)
  const latestRunStatus = String(status?.latestRun?.status || "")
    .trim()
    .toLowerCase()
  if (latestRunStatus === "running") {
    if (scannerState.inFlight) return false
    if (!latestStartedMs) return true
    return nowMs - latestStartedMs >= SCANNER_INTERVAL_MS + SCANNER_OVERDUE_GRACE_MS
  }

  const latestCompletedMs = parseTimestampMs(
    status?.latestCompletedRun?.completed_at ||
      status?.latestCompletedRun?.started_at ||
      scannerState.lastCompletedAt
  )
  const lastActivityMs = Math.max(latestCompletedMs || 0, latestStartedMs || 0)
  if (!lastActivityMs) return true
  return nowMs - lastActivityMs >= SCANNER_INTERVAL_MS + SCANNER_OVERDUE_GRACE_MS
}

function mergeDiscardStats(target = {}, source = {}) {
  for (const [reason, count] of Object.entries(source || {})) {
    if (reason === "__byCategory") continue
    target[reason] = Number(target[reason] || 0) + Number(count || 0)
  }

  const sourceByCategory =
    source?.__byCategory && typeof source.__byCategory === "object" ? source.__byCategory : {}
  if (!Object.keys(sourceByCategory).length) return

  if (!target.__byCategory || typeof target.__byCategory !== "object") {
    target.__byCategory = {}
  }

  for (const [category, reasonStats] of Object.entries(sourceByCategory)) {
    const normalizedCategory = normalizeItemCategory(category)
    if (!target.__byCategory[normalizedCategory]) {
      target.__byCategory[normalizedCategory] = {}
    }
    const bucket = target.__byCategory[normalizedCategory]
    for (const [reason, count] of Object.entries(reasonStats || {})) {
      const normalizedReason = normalizeDiagnosticReason(reason)
      if (!normalizedReason) continue
      bucket[normalizedReason] = Number(bucket[normalizedReason] || 0) + Number(count || 0)
    }
  }
}

function mergeRejectedByItem(target = {}, source = {}) {
  for (const [itemName, payload] of Object.entries(source || {})) {
    const normalizedItem = String(itemName || "").trim() || "Unknown item"
    if (!target[normalizedItem]) {
      target[normalizedItem] = {
        total: 0,
        category: normalizeItemCategory(payload?.category, normalizedItem),
        reasons: {}
      }
    }
    const bucket = target[normalizedItem]
    if (!bucket.category) {
      bucket.category = normalizeItemCategory(payload?.category, normalizedItem)
    }
    bucket.total += Number(payload?.total || 0)
    for (const [reason, count] of Object.entries(payload?.reasons || {})) {
      const normalizedReason = normalizeDiagnosticReason(reason) || "unknown"
      bucket.reasons[normalizedReason] = Number(bucket.reasons[normalizedReason] || 0) + Number(count || 0)
    }
  }
}

function filterUniverseSeedsForScan(seeds = [], options = {}) {
  const seedList = Array.isArray(seeds) ? seeds : []
  const discardStats = {}
  const rejectedByItem = {}
  const selectedSeeds = seedList.filter((row) =>
    passesUniverseSeedFilters(row, discardStats, rejectedByItem, options)
  )

  return {
    selectedSeeds,
    discardStats,
    rejectedByItem
  }
}

async function loadScannerInputs(discardStats = {}, rejectedByItem = {}) {
  const [curatedSeeds, fallbackSeeds] = await Promise.all([
    loadCuratedUniverseSeeds().catch((err) => {
      console.error("[arbitrage-scanner] Curated universe load failed", err.message)
      return []
    }),
    loadFallbackUniverseSeeds().catch((err) => {
      console.error("[arbitrage-scanner] Fallback universe load failed", err.message)
      return []
    })
  ])

  const mergedSeeds = mergeUniverseSeeds([curatedSeeds, fallbackSeeds])
  const { seeds: hydratedSeeds, snapshotWarmup } = await refreshSeedSnapshotsIfNeeded(mergedSeeds)
  const strictFilterResult = filterUniverseSeedsForScan(hydratedSeeds)
  const strictCoverageThreshold = computeStrictCoverageThreshold(hydratedSeeds.length)
  let selectedFilterResult = strictFilterResult
  let seedFilterMode = "strict"
  if (strictFilterResult.selectedSeeds.length < strictCoverageThreshold) {
    const relaxedFilterResult = filterUniverseSeedsForScan(hydratedSeeds, {
      allowMissingSnapshotData: true
    })
    if (relaxedFilterResult.selectedSeeds.length > strictFilterResult.selectedSeeds.length) {
      selectedFilterResult = relaxedFilterResult
      seedFilterMode =
        strictFilterResult.selectedSeeds.length > 0
          ? "strict_plus_missing_snapshot_data"
          : "allow_missing_snapshot_data"
    }
  }

  mergeDiscardStats(discardStats, selectedFilterResult.discardStats)
  mergeRejectedByItem(rejectedByItem, selectedFilterResult.rejectedByItem)

  const ranked = selectedFilterResult.selectedSeeds
    .map((row) => ({
      ...row,
      ...computeLiquidityRank({
        volume7d: row.marketVolume7d,
        marketCoverage: 2,
        sevenDayChangePercent: row.sevenDayChangePercent,
        referencePrice: row.referencePrice,
        itemCategory: row.itemCategory
      })
    }))
    .sort(
      (a, b) =>
        Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
        sourceRank(b.universeSource) - sourceRank(a.universeSource) ||
        Number(b.marketVolume7d || 0) - Number(a.marketVolume7d || 0)
    )
    .slice(0, PRE_COMPARE_UNIVERSE_LIMIT)

  return {
    seeds: ranked,
    snapshotWarmup: toSnapshotWarmupSummary({
      ...snapshotWarmup,
      seedFilterMode,
      strictCoverageThreshold,
      strictEligibleSeeds: strictFilterResult.selectedSeeds.length,
      selectedEligibleSeeds: selectedFilterResult.selectedSeeds.length
    })
  }
}

function toSnapshotWarmupSummary(overrides = {}) {
  return {
    triggered: false,
    reason: "",
    freshSeedsBefore: 0,
    warmupCandidates: 0,
    attemptedItems: 0,
    refreshedItems: 0,
    failedItems: 0,
    batchSize: SNAPSHOT_WARMUP_MAX_ITEMS,
    concurrency: SNAPSHOT_WARMUP_CONCURRENCY,
    seedFilterMode: "strict",
    strictCoverageThreshold: MIN_STRICT_SEED_COVERAGE,
    strictEligibleSeeds: 0,
    selectedEligibleSeeds: 0,
    errors: [],
    ...overrides
  }
}

async function mapWithConcurrency(items = [], concurrencyLimit = 1, mapper = async () => null) {
  const source = Array.isArray(items) ? items : []
  if (!source.length) return []

  const limit = Math.max(Number(concurrencyLimit || 0), 1)
  const results = new Array(source.length)
  let index = 0

  async function worker() {
    while (true) {
      const current = index
      index += 1
      if (current >= source.length) return
      results[current] = await mapper(source[current], current)
    }
  }

  const workers = Array.from({ length: Math.min(limit, source.length) }, () => worker())
  await Promise.all(workers)
  return results
}

function hasUsableItemImage(inputItem = {}) {
  return Boolean(
    sanitizeImageUrl(inputItem?.itemImageUrlLarge) || sanitizeImageUrl(inputItem?.itemImageUrl)
  )
}

function applyImageToInputItem(inputItem = {}, image = {}) {
  if (!inputItem || typeof inputItem !== "object") return false
  const imageUrl = sanitizeImageUrl(image?.imageUrl) || null
  const imageUrlLarge = sanitizeImageUrl(image?.imageUrlLarge) || imageUrl || null
  const currentImage = sanitizeImageUrl(inputItem?.itemImageUrl)
  const currentImageLarge = sanitizeImageUrl(inputItem?.itemImageUrlLarge) || currentImage || null

  let changed = false
  if (!currentImage && imageUrl) {
    inputItem.itemImageUrl = imageUrl
    changed = true
  }
  if (!currentImageLarge && imageUrlLarge) {
    inputItem.itemImageUrlLarge = imageUrlLarge
    changed = true
  }
  return changed
}

function toSkinImageUpsertRows(imageByName = {}) {
  const rows = []
  for (const [marketHashName, image] of Object.entries(imageByName || {})) {
    const normalizedName = normalizeMarketHashName(marketHashName)
    if (!normalizedName) continue
    const imageUrl = sanitizeImageUrl(image?.imageUrl) || null
    const imageUrlLarge = sanitizeImageUrl(image?.imageUrlLarge) || imageUrl || null
    if (!imageUrl && !imageUrlLarge) continue
    rows.push({
      market_hash_name: normalizedName,
      image_url: imageUrl || imageUrlLarge,
      image_url_large: imageUrlLarge || imageUrl
    })
  }
  return rows
}

function buildDefaultImageEnrichmentSummary() {
  return {
    enabled: IMAGE_ENRICH_BATCH_SIZE > 0,
    missingBefore: 0,
    missingAfter: 0,
    fromMarketPayload: 0,
    steamSearchAttempted: 0,
    fromSteamSearch: 0,
    persistedRows: 0,
    errors: []
  }
}

async function hydrateUniverseImages(inputByName = {}, comparisonItems = []) {
  const summary = buildDefaultImageEnrichmentSummary()
  const inputItems = Object.values(inputByName || {})
  if (!inputItems.length) return summary

  summary.missingBefore = inputItems.filter((item) => !hasUsableItemImage(item)).length
  if (!summary.missingBefore) return summary

  const imageUpdatesByName = {}
  for (const comparisonItem of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const marketHashName = normalizeMarketHashName(comparisonItem?.marketHashName)
    if (!marketHashName) continue
    const inputItem = inputByName[marketHashName] || null
    if (!inputItem || hasUsableItemImage(inputItem)) continue

    for (const quote of Array.isArray(comparisonItem?.perMarket) ? comparisonItem.perMarket : []) {
      const image = marketImageService.pickImageFromMarketRow(quote)
      if (!image?.imageUrl && !image?.imageUrlLarge) continue
      const changed = applyImageToInputItem(inputItem, image)
      if (!changed) continue
      imageUpdatesByName[marketHashName] = {
        imageUrl: String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() || null,
        imageUrlLarge:
          String(inputItem?.itemImageUrlLarge || image?.imageUrlLarge || "").trim() ||
          String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() ||
          null
      }
      summary.fromMarketPayload += 1
      break
    }
  }

  const missingAfterPayload = inputItems.filter((item) => !hasUsableItemImage(item))
  if (IMAGE_ENRICH_BATCH_SIZE > 0 && missingAfterPayload.length) {
    const targetNames = missingAfterPayload
      .slice()
      .sort(
        (a, b) =>
          Number(b?.marketVolume7d || 0) - Number(a?.marketVolume7d || 0) ||
          Number(b?.referencePrice || 0) - Number(a?.referencePrice || 0)
      )
      .slice(0, IMAGE_ENRICH_BATCH_SIZE)
      .map((row) => normalizeMarketHashName(row?.marketHashName))
      .filter(Boolean)

    summary.steamSearchAttempted = targetNames.length
    if (targetNames.length) {
      try {
        const steamImagesByName = await marketImageService.fetchSteamSearchImagesBatch(targetNames, {
          concurrency: IMAGE_ENRICH_CONCURRENCY,
          timeoutMs: IMAGE_ENRICH_TIMEOUT_MS,
          maxRetries: 2
        })
        for (const [marketHashName, image] of Object.entries(steamImagesByName || {})) {
          const inputItem = inputByName[marketHashName] || null
          if (!inputItem) continue
          const changed = applyImageToInputItem(inputItem, image)
          if (!changed) continue
          imageUpdatesByName[marketHashName] = {
            imageUrl: String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() || null,
            imageUrlLarge:
              String(inputItem?.itemImageUrlLarge || image?.imageUrlLarge || "").trim() ||
              String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() ||
              null
          }
          summary.fromSteamSearch += 1
        }
      } catch (err) {
        summary.errors.push(String(err?.message || "steam_image_search_failed"))
      }
    }
  }

  const upsertRows = toSkinImageUpsertRows(imageUpdatesByName)
  if (upsertRows.length) {
    try {
      const persisted = await skinRepo.upsertSkins(upsertRows)
      summary.persistedRows = Array.isArray(persisted) ? persisted.length : upsertRows.length
    } catch (err) {
      summary.errors.push(String(err?.message || "skin_image_upsert_failed"))
    }
  }

  summary.missingAfter = inputItems.filter((item) => !hasUsableItemImage(item)).length
  summary.errors = summary.errors.slice(0, 5)
  return summary
}

function mergeSeedWithSnapshot(seed = {}, snapshot = null) {
  if (!snapshot || typeof snapshot !== "object") return seed
  const referencePrice = toPositiveOrNull(
    toFiniteOrNull(snapshot?.average_7d_price) ??
      toFiniteOrNull(snapshot?.lowest_listing_price) ??
      toFiniteOrNull(seed?.referencePrice)
  )
  const marketVolume7d = toPositiveOrNull(resolveVolume7d(snapshot))
  const hasUsableSnapshotLiquidity = referencePrice != null || marketVolume7d != null

  return {
    ...seed,
    marketVolume7d,
    liquidityScore:
      hasUsableSnapshotLiquidity ? computeLiquidityScoreFromSnapshot(snapshot) : null,
    sevenDayChangePercent:
      hasUsableSnapshotLiquidity ? resolveSevenDayChangePercent(snapshot) : null,
    referencePrice,
    hasSnapshotData: hasUsableSnapshotLiquidity,
    snapshotCapturedAt: snapshot?.captured_at || seed?.snapshotCapturedAt || null,
    snapshotStale: isSnapshotStale(snapshot)
  }
}

async function refreshSeedSnapshotsIfNeeded(seeds = []) {
  const rows = Array.isArray(seeds) ? seeds : []
  if (!rows.length) {
    return {
      seeds: rows,
      snapshotWarmup: toSnapshotWarmupSummary()
    }
  }

  const freshSeedsBefore = rows.filter(
    (seed) => Boolean(seed?.hasSnapshotData) && !Boolean(seed?.snapshotStale)
  ).length
  const warmupCandidates = rows
    .filter((seed) => {
      const skinId = Number(seed?.skinId || 0)
      return Number.isInteger(skinId) && skinId > 0 && (!seed?.hasSnapshotData || seed?.snapshotStale)
    })
    .sort(
      (a, b) =>
        sourceRank(b?.universeSource) - sourceRank(a?.universeSource) ||
        Number(a?.liquidityRank || Number.MAX_SAFE_INTEGER) -
          Number(b?.liquidityRank || Number.MAX_SAFE_INTEGER)
    )

  if (!warmupCandidates.length) {
    return {
      seeds: rows,
      snapshotWarmup: toSnapshotWarmupSummary({
        freshSeedsBefore,
        warmupCandidates: warmupCandidates.length
      })
    }
  }

  const warmupLimit =
    freshSeedsBefore < SNAPSHOT_WARMUP_TRIGGER_FRESH_MIN
      ? SNAPSHOT_WARMUP_MAX_ITEMS
      : Math.max(Math.round(SNAPSHOT_WARMUP_MAX_ITEMS / 2), 5)
  const selected = warmupCandidates.slice(0, warmupLimit)
  const errors = []
  const refreshedSkinIds = []
  const results = await mapWithConcurrency(
    selected,
    SNAPSHOT_WARMUP_CONCURRENCY,
    async (seed) => {
      const skinId = Number(seed?.skinId || 0)
      try {
        await marketService.getLiquidityScore(skinId)
        refreshedSkinIds.push(skinId)
        return true
      } catch (err) {
        errors.push(String(err?.message || `snapshot_refresh_failed:${skinId}`))
        return false
      }
    }
  )

  const refreshedCount = results.filter((ok) => Boolean(ok)).length
  const snapshotsBySkinId = refreshedSkinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(refreshedSkinIds)
    : {}

  const refreshedBySkinId = {}
  for (const skinId of refreshedSkinIds) {
    if (snapshotsBySkinId[skinId]) {
      refreshedBySkinId[skinId] = snapshotsBySkinId[skinId]
    }
  }

  const hydratedSeeds = rows.map((seed) => {
    const skinId = Number(seed?.skinId || 0)
    return mergeSeedWithSnapshot(seed, refreshedBySkinId[skinId] || null)
  })

  return {
    seeds: hydratedSeeds,
    snapshotWarmup: toSnapshotWarmupSummary({
      triggered: true,
      reason: "insufficient_fresh_snapshot_coverage",
      freshSeedsBefore,
      warmupCandidates: warmupCandidates.length,
      attemptedItems: selected.length,
      refreshedItems: refreshedCount,
      failedItems: Math.max(selected.length - refreshedCount, 0),
      errors: errors.slice(0, 8)
    })
  }
}

function countAvailableQuotes(items = []) {
  let total = 0
  for (const item of Array.isArray(items) ? items : []) {
    for (const row of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const source = normalizeMarketLabel(row?.source || row?.market)
      if (!source) continue
      const hasGross = Number.isFinite(Number(row?.grossPrice)) && Number(row.grossPrice) > 0
      const hasNet =
        Number.isFinite(Number(row?.netPriceAfterFees)) && Number(row.netPriceAfterFees) > 0
      if (Boolean(row?.available) && (hasGross || hasNet)) {
        total += 1
      }
    }
  }
  return total
}

function isBatchTimeoutError(err = {}) {
  return String(err?.code || "")
    .trim()
    .toLowerCase() === "scan_batch_timeout"
}

async function compareBatchItems(batch = [], options = {}) {
  const allowLiveFetch = options.allowLiveFetch !== false
  const compareTimeoutMs = allowLiveFetch
    ? Math.max(SCAN_TIMEOUT_PER_BATCH + 5000, SCAN_TIMEOUT_PER_BATCH)
    : SCAN_TIMEOUT_PER_BATCH
  const comparePromise = marketComparisonService.compareItems(batch, {
    currency: "USD",
    pricingMode: "lowest_buy",
    allowLiveFetch,
    forceRefresh: Boolean(options.forceRefresh),
    userId: null,
    concurrency: MAX_CONCURRENT_MARKET_REQUESTS,
    timeoutMs: SCAN_TIMEOUT_PER_BATCH
  })

  let timeoutId = null
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      const timeoutErr = new Error(`scan_batch_timeout_${compareTimeoutMs}ms`)
      timeoutErr.code = "scan_batch_timeout"
      reject(timeoutErr)
    }, compareTimeoutMs)
  })

  try {
    return await Promise.race([comparePromise, timeoutPromise])
  } finally {
    clearTimeout(timeoutId)
  }
}

async function refreshQuotesInBatches(inputItems = [], options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const batches = chunkArray(inputItems, SCAN_BATCH_SIZE)
  let itemsCompared = 0
  let availableQuotes = 0
  let completedBatches = 0
  let timedOutBatches = 0
  let totalBatchMs = 0
  let slowestBatchMs = 0
  const errors = []

  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index]
    const startedAt = Date.now()
    try {
      const comparison = await compareBatchItems(batch, {
        allowLiveFetch: true,
        forceRefresh
      })
      const comparedItems = Array.isArray(comparison?.items) ? comparison.items : []
      itemsCompared += comparedItems.length
      availableQuotes += countAvailableQuotes(comparedItems)
      completedBatches += 1
    } catch (err) {
      if (isBatchTimeoutError(err)) {
        timedOutBatches += 1
      }
      errors.push(`[batch ${index + 1}] ${String(err?.message || "quote_batch_failed")}`)
    } finally {
      const batchMs = Math.max(Date.now() - startedAt, 0)
      totalBatchMs += batchMs
      slowestBatchMs = Math.max(slowestBatchMs, batchMs)
    }
  }

  return {
    batchSize: SCAN_BATCH_SIZE,
    timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
    maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
    requestedItems: inputItems.length,
    totalBatches: batches.length,
    completedBatches,
    failedBatches: errors.length,
    timedOutBatches,
    itemsCompared,
    availableQuotes,
    averageBatchMs: round2(totalBatchMs / Math.max(batches.length, 1)),
    slowestBatchMs,
    errors: errors.slice(0, 5)
  }
}

async function compareFromSavedQuotes(inputItems = []) {
  const batches = chunkArray(inputItems, SCAN_BATCH_SIZE)
  const items = []
  let itemsCompared = 0
  let availableQuotes = 0
  let completedBatches = 0
  let timedOutBatches = 0
  let totalBatchMs = 0
  let slowestBatchMs = 0
  const errors = []
  let currency = "USD"

  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index]
    const startedAt = Date.now()
    try {
      const comparison = await compareBatchItems(batch, {
        allowLiveFetch: false,
        forceRefresh: false
      })
      const comparedItems = Array.isArray(comparison?.items) ? comparison.items : []
      items.push(...comparedItems)
      itemsCompared += comparedItems.length
      availableQuotes += countAvailableQuotes(comparedItems)
      completedBatches += 1
      currency = String(comparison?.currency || currency)
        .trim()
        .toUpperCase()
    } catch (err) {
      if (isBatchTimeoutError(err)) {
        timedOutBatches += 1
      }
      errors.push(`[batch ${index + 1}] ${String(err?.message || "compare_saved_quotes_failed")}`)
    } finally {
      const batchMs = Math.max(Date.now() - startedAt, 0)
      totalBatchMs += batchMs
      slowestBatchMs = Math.max(slowestBatchMs, batchMs)
    }
  }

  return {
    currency,
    items,
    diagnostics: {
      batchSize: SCAN_BATCH_SIZE,
      timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
      maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
      requestedItems: inputItems.length,
      totalBatches: batches.length,
      completedBatches,
      failedBatches: errors.length,
      timedOutBatches,
      itemsCompared,
      availableQuotes,
      averageBatchMs: round2(totalBatchMs / Math.max(batches.length, 1)),
      slowestBatchMs,
      errors: errors.slice(0, 5)
    }
  }
}

function buildQuoteSnapshotRows(comparisonItems = [], inputByName = {}) {
  const rows = []
  const nowIso = new Date().toISOString()

  for (const item of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const marketHashName = normalizeMarketHashName(item?.marketHashName)
    if (!marketHashName) continue
    const inputItem = inputByName[marketHashName] || null
    const volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
    const liquidityScore = toFiniteOrNull(inputItem?.liquidityScore)

    for (const quote of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const market = normalizeMarketLabel(quote?.source || quote?.market)
      if (!market || !SOURCE_ORDER.includes(market)) continue
      const gross = toFiniteOrNull(quote?.grossPrice)
      const sellNet = toFiniteOrNull(quote?.netPriceAfterFees)
      rows.push({
        item_name: marketHashName,
        market,
        best_buy: gross,
        best_sell: gross,
        best_sell_net: sellNet,
        volume_7d: volume7d,
        liquidity_score: liquidityScore,
        fetched_at: quote?.updatedAt || nowIso,
        quality_flags: {
          available: Boolean(quote?.available),
          confidence: String(quote?.confidence || "low").trim() || "low",
          unavailable_reason: String(quote?.unavailableReason || "").trim() || null,
          quote_age_minutes: resolveQuoteAgeMinutes(quote),
          snapshot_stale: Boolean(inputItem?.snapshotStale),
          universe_source: String(inputItem?.universeSource || "").trim() || "fallback_curated"
        }
      })
    }
  }

  return rows
}

async function persistQuoteSnapshot(comparisonItems = [], inputByName = {}) {
  const rows = buildQuoteSnapshotRows(comparisonItems, inputByName)
  if (!rows.length) {
    return {
      rowsPrepared: 0,
      rowsInserted: 0,
      persisted: true
    }
  }

  try {
    const rowsInserted = await marketQuoteRepo.insertRows(rows)
    return {
      rowsPrepared: rows.length,
      rowsInserted,
      persisted: true
    }
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to persist scanner quote snapshot", err.message)
    return {
      rowsPrepared: rows.length,
      rowsInserted: 0,
      persisted: false,
      error: String(err?.message || "quote_snapshot_persist_failed")
    }
  }
}

function selectTopUniverseItems(
  comparisonItems = [],
  inputByName = {},
  discardStats = {},
  rejectedByItem = {}
) {
  const ranked = []
  for (const comparisonItem of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const name = normalizeMarketHashName(comparisonItem?.marketHashName)
    const inputItem = inputByName[name] || null
    if (!inputItem) continue
    const itemCategory = normalizeItemCategory(inputItem?.itemCategory, name)
    const marketCoverage = countAvailableMarkets(comparisonItem?.perMarket)
    if (marketCoverage < MIN_MARKET_COVERAGE) {
      incrementReasonCounter(discardStats, "ignored_missing_markets", itemCategory)
      incrementItemReasonCounter(
        rejectedByItem,
        name || inputItem?.marketHashName,
        "ignored_missing_markets",
        itemCategory
      )
      continue
    }
    const rank = computeLiquidityRank({
      volume7d: inputItem.marketVolume7d,
      marketCoverage,
      sevenDayChangePercent: inputItem.sevenDayChangePercent,
      referencePrice: inputItem.referencePrice,
      itemCategory: inputItem.itemCategory
    })
    ranked.push({
      inputItem,
      comparisonItem,
      marketCoverage,
      ...rank
    })
  }

  return ranked
    .sort(
      (a, b) => {
        const curatedRankA = toFiniteOrNull(a.inputItem?.liquidityRank)
        const curatedRankB = toFiniteOrNull(b.inputItem?.liquidityRank)
        const curatedRankDelta =
          curatedRankA == null || curatedRankB == null ? 0 : curatedRankA - curatedRankB
        return (
        Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
        sourceRank(b.inputItem?.universeSource) - sourceRank(a.inputItem?.universeSource) ||
        curatedRankDelta ||
        Number(b.inputItem?.marketVolume7d || 0) - Number(a.inputItem?.marketVolume7d || 0)
        )
      }
    )
    .slice(0, UNIVERSE_TARGET_SIZE)
}

function countRowsByCategory(rows = []) {
  const counts = Object.fromEntries(
    Object.values(ITEM_CATEGORIES).map((category) => [category, 0])
  )
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeItemCategory(row?.itemCategory, row?.itemName)
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

function toRejectedCountByCategory(discardedReasonsByCategory = {}) {
  const counts = Object.fromEntries(
    Object.values(ITEM_CATEGORIES).map((category) => [category, 0])
  )
  for (const [category, payload] of Object.entries(discardedReasonsByCategory || {})) {
    const normalizedCategory = normalizeItemCategory(category)
    counts[normalizedCategory] = Number(payload?.totalRejected || 0)
  }
  return counts
}

function toKnifeGloveRejectionSummary(discardedReasonsByCategory = {}) {
  const reasonMapByCategory = {
    [ITEM_CATEGORIES.KNIFE]: discardedReasonsByCategory?.[ITEM_CATEGORIES.KNIFE]?.reasons || {},
    [ITEM_CATEGORIES.GLOVE]: discardedReasonsByCategory?.[ITEM_CATEGORIES.GLOVE]?.reasons || {}
  }

  const result = {}
  for (const [category, reasons] of Object.entries(reasonMapByCategory)) {
    result[category] = {
      low_liquidity: Number(reasons.ignored_low_liquidity || 0),
      extreme_spread: Number(reasons.ignored_extreme_spread || 0),
      stale_market_data: Number(reasons.ignored_stale_data || 0),
      missing_depth: Number(reasons.ignored_missing_depth || 0) + Number(reasons.risky_missing_depth || 0),
      reference_deviation: Number(reasons.ignored_reference_deviation || 0),
      weak_market_coverage: Number(reasons.ignored_missing_markets || 0)
    }
  }
  return result
}

function buildRiskyProfileDiagnostics() {
  const diagnostics = {}
  for (const category of Object.values(ITEM_CATEGORIES)) {
    const profile = getCategoryRiskyProfile(category)
    diagnostics[category] = {
      profile: {
        minPriceUsd: Number(profile.minPriceUsd || 0),
        minProfitUsd: Number(profile.minProfitUsd || 0),
        minSpreadPercent: Number(profile.minSpreadPercent || 0),
        minVolume7d: Number(profile.minVolume7d || 0),
        minMarketCoverage: Number(profile.minMarketCoverage || 0),
        minScore: Number(profile.minScore || 0),
        allowMissingDepthWithPenalty: Boolean(profile.allowMissingDepthWithPenalty)
      },
      attempted: 0,
      accepted: 0,
      rejected: 0,
      acceptedReasons: {},
      rejectedReasons: {}
    }
  }
  return diagnostics
}

function trackRiskyDecision(
  diagnostics = {},
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  status = "attempted",
  reason = ""
) {
  const normalizedCategory = normalizeItemCategory(category)
  if (!diagnostics[normalizedCategory]) {
    diagnostics[normalizedCategory] = {
      profile: getCategoryRiskyProfile(normalizedCategory),
      attempted: 0,
      accepted: 0,
      rejected: 0,
      acceptedReasons: {},
      rejectedReasons: {}
    }
  }
  const bucket = diagnostics[normalizedCategory]
  if (status === "attempted") {
    bucket.attempted += 1
    return
  }
  if (status === "accepted") {
    bucket.accepted += 1
    if (reason) {
      bucket.acceptedReasons[reason] = Number(bucket.acceptedReasons[reason] || 0) + 1
    }
    return
  }
  bucket.rejected += 1
  if (reason) {
    bucket.rejectedReasons[reason] = Number(bucket.rejectedReasons[reason] || 0) + 1
  }
}

function buildScanProgressStats({
  universeTarget = 0,
  candidateItems = 0,
  scannedItems = 0,
  quoteRefresh = {},
  computeFromSavedQuotes = {}
} = {}) {
  const refreshBatches = Number(quoteRefresh?.totalBatches || 0)
  const computeBatches = Number(computeFromSavedQuotes?.totalBatches || 0)
  const totalBatches = refreshBatches + computeBatches
  const completedBatches =
    Number(quoteRefresh?.completedBatches || 0) +
    Number(computeFromSavedQuotes?.completedBatches || 0)
  const failedBatches =
    Number(quoteRefresh?.failedBatches || 0) +
    Number(computeFromSavedQuotes?.failedBatches || 0)
  const timedOutBatches =
    Number(quoteRefresh?.timedOutBatches || 0) +
    Number(computeFromSavedQuotes?.timedOutBatches || 0)

  return {
    universeTarget: Number(universeTarget || 0),
    candidateItems: Number(candidateItems || 0),
    scannedItems: Number(scannedItems || 0),
    batchSize: SCAN_BATCH_SIZE,
    maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
    timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
    totalBatches,
    completedBatches,
    failedBatches,
    timedOutBatches,
    completionPercent:
      totalBatches > 0 ? round2((completedBatches / Math.max(totalBatches, 1)) * 100) : 100
  }
}

function toAuditDurationMs(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 0
  return Math.max(Math.round(parsed), 0)
}

function buildScannerAuditCategoryCounter(initialValue = 0) {
  const initial = Math.max(Number(initialValue || 0), 0)
  return Object.fromEntries(SCANNER_AUDIT_CATEGORIES.map((category) => [category, initial]))
}

function toScannerAuditCategoryCounts(counter = {}, nestedField = "") {
  const counts = buildScannerAuditCategoryCounter(0)
  const source = counter && typeof counter === "object" ? counter : {}
  for (const category of SCANNER_AUDIT_CATEGORIES) {
    const value = source?.[category]
    if (nestedField && value && typeof value === "object") {
      counts[category] = Math.max(Number(value?.[nestedField] || 0), 0)
      continue
    }
    counts[category] = Math.max(Number(value || 0), 0)
  }
  return counts
}

function resolveSourceCatalogTotalRows(sourceCatalogDiagnostics = {}) {
  const sourceCatalog =
    sourceCatalogDiagnostics?.sourceCatalog && typeof sourceCatalogDiagnostics.sourceCatalog === "object"
      ? sourceCatalogDiagnostics.sourceCatalog
      : {}
  const totalRows = Number(sourceCatalog?.totalRows || 0)
  if (totalRows > 0) return totalRows
  return Math.max(Number(sourceCatalog?.activeCatalogRows || 0), 0)
}

function resolveSourceCatalogEligibleByCategory(sourceCatalogDiagnostics = {}) {
  const sourceCatalog =
    sourceCatalogDiagnostics?.sourceCatalog && typeof sourceCatalogDiagnostics.sourceCatalog === "object"
      ? sourceCatalogDiagnostics.sourceCatalog
      : {}
  const direct = sourceCatalog?.eligibleRowsByCategory
  if (direct && typeof direct === "object") {
    const directCounts = toScannerAuditCategoryCounts(direct)
    const hasDirect = Object.values(directCounts).some((value) => Number(value || 0) > 0)
    if (hasDirect) return directCounts
  }

  return toScannerAuditCategoryCounts(sourceCatalog?.byCategory || {}, "eligible")
}

function deriveSourceCatalogDiagnosticsFromScan({
  sourceCatalogDiagnostics = {},
  candidateItems = 0,
  scannedItems = 0,
  candidateByCategory = {},
  selectedByCategory = {},
  universeTarget = UNIVERSE_TARGET_SIZE
} = {}) {
  const base =
    sourceCatalogDiagnostics && typeof sourceCatalogDiagnostics === "object"
      ? sourceCatalogDiagnostics
      : {}
  const sourceCatalog =
    base?.sourceCatalog && typeof base.sourceCatalog === "object" ? base.sourceCatalog : {}
  const universeBuild =
    base?.universeBuild && typeof base.universeBuild === "object" ? base.universeBuild : {}
  const hasUniverseCoverage = Number(universeBuild?.activeUniverseBuilt || 0) > 0
  if (hasUniverseCoverage) {
    return base
  }

  const hasScannerCoverage = Number(scannedItems || 0) > 0 || Number(candidateItems || 0) > 0
  if (!hasScannerCoverage) {
    return base
  }

  const derivedCandidateByCategory = toScannerAuditCategoryCounts(candidateByCategory)
  const derivedSelectedByCategory = toScannerAuditCategoryCounts(selectedByCategory)
  const normalizedTarget = Math.max(Number(universeTarget || UNIVERSE_TARGET_SIZE), 0)
  const normalizedScanned = Math.max(Number(scannedItems || 0), 0)
  const normalizedCandidates = Math.max(Number(candidateItems || 0), 0)

  return {
    ...base,
    degradedFromScannerInputs: true,
    sourceCatalog: {
      ...sourceCatalog,
      totalRows: Math.max(Number(sourceCatalog?.totalRows || 0), normalizedCandidates),
      activeCatalogRows: Math.max(Number(sourceCatalog?.activeCatalogRows || 0), normalizedCandidates),
      eligibleTradableRows: Math.max(
        Number(sourceCatalog?.eligibleTradableRows || 0),
        normalizedCandidates
      ),
      eligibleRowsByCategory: derivedCandidateByCategory
    },
    universeBuild: {
      ...universeBuild,
      targetUniverseSize: normalizedTarget,
      activeUniverseBuilt: Math.max(Number(universeBuild?.activeUniverseBuilt || 0), normalizedScanned),
      missingToTarget: Math.max(normalizedTarget - normalizedScanned, 0),
      selectedByCategory: derivedSelectedByCategory
    }
  }
}

function buildTopReasonSummary(reasonCounter = {}, limit = 8) {
  return Object.entries(reasonCounter || {})
    .filter(([, count]) => Number(count || 0) > 0)
    .map(([reason, count]) => ({
      reason: String(reason || "").trim() || "unknown",
      count: Number(count || 0)
    }))
    .sort((a, b) => Number(b.count || 0) - Number(a.count || 0))
    .slice(0, Math.max(Number(limit || 0), 0))
}

function buildTopReasonSummaryByCategory(reasonsByCategory = {}, limit = 5) {
  const output = {}
  const source = reasonsByCategory && typeof reasonsByCategory === "object" ? reasonsByCategory : {}
  for (const category of SCANNER_AUDIT_CATEGORIES) {
    output[category] = buildTopReasonSummary(source?.[category]?.reasons || source?.[category] || {}, limit)
  }
  return output
}

function buildOpportunitiesByConfidenceTier(rows = []) {
  const counts = {
    high: 0,
    medium: 0,
    low: 0,
    total: 0,
    highConfidenceEligible: 0,
    riskyEligible: 0
  }
  for (const row of Array.isArray(rows) ? rows : []) {
    const confidence = normalizeConfidence(row?.executionConfidence)
    if (confidence === "High") counts.high += 1
    else if (confidence === "Medium") counts.medium += 1
    else counts.low += 1

    if (Boolean(row?.isHighConfidenceEligible)) counts.highConfidenceEligible += 1
    if (Boolean(row?.isRiskyEligible)) counts.riskyEligible += 1
    counts.total += 1
  }
  return counts
}

function buildQuoteCoverageByMarket(comparisonItems = [], universeSize = 0) {
  const byMarket = Object.fromEntries(
    SOURCE_ORDER.map((source) => [
      source,
      {
        itemsSeen: 0,
        itemsWithAvailableQuote: 0,
        availableQuoteRows: 0,
        unavailableQuoteRows: 0,
        coveragePct: 0
      }
    ])
  )

  for (const item of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const seenInItem = {}
    const availableInItem = {}
    for (const quote of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const source = normalizeMarketLabel(quote?.source || quote?.market)
      if (!source || !SOURCE_ORDER.includes(source)) continue
      seenInItem[source] = true
      if (hasUsableQuotePrice(quote)) {
        availableInItem[source] = true
        byMarket[source].availableQuoteRows += 1
      } else {
        byMarket[source].unavailableQuoteRows += 1
      }
    }

    for (const source of Object.keys(seenInItem)) {
      byMarket[source].itemsSeen += 1
      if (availableInItem[source]) {
        byMarket[source].itemsWithAvailableQuote += 1
      }
    }
  }

  const safeUniverseSize = Math.max(Number(universeSize || 0), 0)
  for (const source of SOURCE_ORDER) {
    const coverageBase = Math.max(safeUniverseSize, 1)
    byMarket[source].coveragePct =
      safeUniverseSize > 0
        ? round2((Number(byMarket[source].itemsWithAvailableQuote || 0) / coverageBase) * 100)
        : 0
  }

  return {
    totalUniverseItems: safeUniverseSize,
    byMarket
  }
}

function buildDefaultPerformanceStageDurations() {
  return {
    sourceCatalogPreparationMs: 0,
    inputHydrationMs: 0,
    quoteFetchingMs: 0,
    normalizationMs: 0,
    opportunityComputationMs: 0,
    dbWritesMs: 0,
    diagnosticsAggregationMs: 0
  }
}

function normalizePerformanceStageDurations(stageDurations = {}) {
  const defaults = buildDefaultPerformanceStageDurations()
  const normalized = {}
  for (const key of PERFORMANCE_STAGE_KEYS) {
    normalized[key] = toAuditDurationMs(stageDurations?.[key] ?? defaults[key])
  }
  return normalized
}

function toPerformanceStageSharePercent(stageDurations = {}, totalDurationMs = 0) {
  const total = Math.max(Number(totalDurationMs || 0), 0)
  const denominator =
    total > 0
      ? total
      : Math.max(
          Object.values(stageDurations || {}).reduce((sum, value) => sum + Number(value || 0), 0),
          1
        )
  const share = {}
  for (const [stage, duration] of Object.entries(stageDurations || {})) {
    share[stage] = round2((Math.max(Number(duration || 0), 0) / denominator) * 100)
  }
  return share
}

function resolveSlowestPerformanceStage(stageDurations = {}) {
  const entries = Object.entries(stageDurations || {}).sort(
    (a, b) => Number(b[1] || 0) - Number(a[1] || 0)
  )
  const [stage, durationMs] = entries[0] || ["", 0]
  return {
    stage: String(stage || "").trim(),
    durationMs: toAuditDurationMs(durationMs)
  }
}

function evaluateScannerPerformanceSafety({
  scanDurationMs = 0,
  stageDurationsMs = {},
  batching = {}
} = {}) {
  const totalDurationMs = toAuditDurationMs(scanDurationMs)
  const stageDurations = normalizePerformanceStageDurations(stageDurationsMs)
  const refresh = batching?.quoteRefresh || {}
  const compute = batching?.computeFromSavedQuotes || {}

  const maxTotalDurationMs = Math.max(Math.round(SCANNER_INTERVAL_MS * 0.85), 120000)
  const maxQuoteFetchingMs = Math.max(Math.round(maxTotalDurationMs * 0.75), 90000)
  const maxNormalizationMs = Math.max(Math.round(maxTotalDurationMs * 0.18), 15000)
  const maxOpportunityComputationMs = Math.max(Math.round(maxTotalDurationMs * 0.24), 25000)
  const maxDbWritesMs = Math.max(Math.round(maxTotalDurationMs * 0.1), 15000)
  const maxDiagnosticsAggregationMs = 12000
  const maxAverageBatchMs = Math.max(Math.round(SCAN_TIMEOUT_PER_BATCH * 0.8), 1500)

  const checks = [
    {
      name: "scan_duration",
      withinBound: totalDurationMs <= maxTotalDurationMs,
      actual: totalDurationMs,
      bound: maxTotalDurationMs,
      unit: "ms"
    },
    {
      name: "quote_fetching_stage",
      withinBound: Number(stageDurations.quoteFetchingMs || 0) <= maxQuoteFetchingMs,
      actual: Number(stageDurations.quoteFetchingMs || 0),
      bound: maxQuoteFetchingMs,
      unit: "ms"
    },
    {
      name: "normalization_stage",
      withinBound: Number(stageDurations.normalizationMs || 0) <= maxNormalizationMs,
      actual: Number(stageDurations.normalizationMs || 0),
      bound: maxNormalizationMs,
      unit: "ms"
    },
    {
      name: "opportunity_computation_stage",
      withinBound:
        Number(stageDurations.opportunityComputationMs || 0) <= maxOpportunityComputationMs,
      actual: Number(stageDurations.opportunityComputationMs || 0),
      bound: maxOpportunityComputationMs,
      unit: "ms"
    },
    {
      name: "db_writes_stage",
      withinBound: Number(stageDurations.dbWritesMs || 0) <= maxDbWritesMs,
      actual: Number(stageDurations.dbWritesMs || 0),
      bound: maxDbWritesMs,
      unit: "ms"
    },
    {
      name: "diagnostics_aggregation_stage",
      withinBound:
        Number(stageDurations.diagnosticsAggregationMs || 0) <= maxDiagnosticsAggregationMs,
      actual: Number(stageDurations.diagnosticsAggregationMs || 0),
      bound: maxDiagnosticsAggregationMs,
      unit: "ms"
    },
    {
      name: "quote_refresh_batch_timeout",
      withinBound: Number(refresh?.timedOutBatches || 0) === 0,
      actual: Number(refresh?.timedOutBatches || 0),
      bound: 0,
      unit: "count"
    },
    {
      name: "saved_quote_compute_batch_timeout",
      withinBound: Number(compute?.timedOutBatches || 0) === 0,
      actual: Number(compute?.timedOutBatches || 0),
      bound: 0,
      unit: "count"
    },
    {
      name: "quote_refresh_average_batch_ms",
      withinBound:
        Number(refresh?.totalBatches || 0) === 0 ||
        Number(refresh?.averageBatchMs || 0) <= maxAverageBatchMs,
      actual: Number(refresh?.averageBatchMs || 0),
      bound: maxAverageBatchMs,
      unit: "ms"
    },
    {
      name: "saved_quote_compute_average_batch_ms",
      withinBound:
        Number(compute?.totalBatches || 0) === 0 ||
        Number(compute?.averageBatchMs || 0) <= maxAverageBatchMs,
      actual: Number(compute?.averageBatchMs || 0),
      bound: maxAverageBatchMs,
      unit: "ms"
    }
  ]

  const breachedChecks = checks.filter((check) => !check.withinBound)
  return {
    withinSafeBounds: !breachedChecks.length,
    breachedChecks,
    checks,
    bounds: {
      maxTotalDurationMs,
      maxQuoteFetchingMs,
      maxNormalizationMs,
      maxOpportunityComputationMs,
      maxDbWritesMs,
      maxDiagnosticsAggregationMs,
      maxAverageBatchMs,
      maxTimedOutBatches: 0
    }
  }
}

function buildScannerPerformanceAudit({
  sourceCatalogDiagnostics = {},
  selectedUniverseByCategory = {},
  staleDiagnostics = {},
  discardedReasons = {},
  discardedReasonsByCategory = {},
  sortedRows = [],
  selectedUniverseRows = [],
  quoteRefreshSummary = {},
  computeFromSavedSummary = {},
  stageDurationsMs = {},
  scanDurationMs = 0
} = {}) {
  const stageDurations = normalizePerformanceStageDurations(stageDurationsMs)
  const safeScanDurationMs = toAuditDurationMs(scanDurationMs)
  const sourceCatalogTotalRows = resolveSourceCatalogTotalRows(sourceCatalogDiagnostics)
  const sourceCatalogEligibleByCategory = resolveSourceCatalogEligibleByCategory(sourceCatalogDiagnostics)
  const activeUniverseByCategory = toScannerAuditCategoryCounts(selectedUniverseByCategory)
  const staleDataByCategory = toScannerAuditCategoryCounts(staleDiagnostics?.staleByCategory || {}, "")
  const opportunitiesByConfidenceTier = buildOpportunitiesByConfidenceTier(sortedRows)
  const quoteCoverageByMarket = buildQuoteCoverageByMarket(
    selectedUniverseRows.map((row) => row?.comparisonItem || null).filter(Boolean),
    selectedUniverseRows.length
  )
  const topRejectedReasons = buildTopReasonSummary(discardedReasons, 10)
  const topRejectedReasonsByCategory = buildTopReasonSummaryByCategory(
    discardedReasonsByCategory,
    5
  )

  const refreshBatches = Number(quoteRefreshSummary?.totalBatches || 0)
  const computeBatches = Number(computeFromSavedSummary?.totalBatches || 0)
  const totalBatches = refreshBatches + computeBatches
  const weightedAverageBatchMs =
    totalBatches > 0
      ? round2(
          ((Number(quoteRefreshSummary?.averageBatchMs || 0) * refreshBatches +
            Number(computeFromSavedSummary?.averageBatchMs || 0) * computeBatches) /
            totalBatches)
        )
      : 0
  const batching = {
    totalBatches,
    completedBatches:
      Number(quoteRefreshSummary?.completedBatches || 0) +
      Number(computeFromSavedSummary?.completedBatches || 0),
    failedBatches:
      Number(quoteRefreshSummary?.failedBatches || 0) +
      Number(computeFromSavedSummary?.failedBatches || 0),
    timedOutBatches:
      Number(quoteRefreshSummary?.timedOutBatches || 0) +
      Number(computeFromSavedSummary?.timedOutBatches || 0),
    slowestBatchMs: Math.max(
      Number(quoteRefreshSummary?.slowestBatchMs || 0),
      Number(computeFromSavedSummary?.slowestBatchMs || 0)
    ),
    averageBatchMs: weightedAverageBatchMs,
    quoteRefresh: quoteRefreshSummary || {},
    computeFromSavedQuotes: computeFromSavedSummary || {}
  }
  const stageSharePercent = toPerformanceStageSharePercent(stageDurations, safeScanDurationMs)
  const slowestStage = resolveSlowestPerformanceStage(stageDurations)
  const safeBounds = evaluateScannerPerformanceSafety({
    scanDurationMs: safeScanDurationMs,
    stageDurationsMs: stageDurations,
    batching
  })

  return {
    categories: SCANNER_AUDIT_CATEGORIES,
    sourceCatalog: {
      totalRows: sourceCatalogTotalRows,
      eligibleRowsByCategory: sourceCatalogEligibleByCategory
    },
    activeUniverse: {
      totalRows: Number(selectedUniverseRows.length || 0),
      byCategory: activeUniverseByCategory
    },
    scanDurationMs: {
      core: safeScanDurationMs,
      endToEnd: safeScanDurationMs
    },
    stageDurationsMs: stageDurations,
    stageSharePercent,
    slowestStage,
    batching,
    staleDataByCategory,
    topRejectedReasons,
    topRejectedReasonsByCategory,
    opportunitiesByConfidenceTier,
    quoteCoverageByMarket,
    safeBounds
  }
}

function extendPerformanceAudit(
  performanceAudit = {},
  options = {}
) {
  if (!performanceAudit || typeof performanceAudit !== "object") {
    return performanceAudit
  }
  const additionalDbWriteDurationMs = toAuditDurationMs(options?.additionalDbWriteDurationMs)
  const additionalDiagnosticsAggregationMs = toAuditDurationMs(
    options?.additionalDiagnosticsAggregationMs
  )
  const endToEndScanDurationMs = options?.endToEndScanDurationMs

  const stageDurations = normalizePerformanceStageDurations(performanceAudit?.stageDurationsMs || {})
  stageDurations.dbWritesMs += additionalDbWriteDurationMs
  stageDurations.diagnosticsAggregationMs += additionalDiagnosticsAggregationMs

  const coreDurationMs = toAuditDurationMs(performanceAudit?.scanDurationMs?.core)
  const endToEndDurationMs = toAuditDurationMs(
    endToEndScanDurationMs == null
      ? performanceAudit?.scanDurationMs?.endToEnd ?? coreDurationMs
      : endToEndScanDurationMs
  )
  const totalDurationMs = Math.max(endToEndDurationMs, coreDurationMs)
  const stageSharePercent = toPerformanceStageSharePercent(stageDurations, totalDurationMs)
  const slowestStage = resolveSlowestPerformanceStage(stageDurations)
  const batching = performanceAudit?.batching || {}
  const safeBounds = evaluateScannerPerformanceSafety({
    scanDurationMs: totalDurationMs,
    stageDurationsMs: stageDurations,
    batching
  })

  return {
    ...performanceAudit,
    scanDurationMs: {
      core: coreDurationMs,
      endToEnd: totalDurationMs
    },
    stageDurationsMs: stageDurations,
    stageSharePercent,
    slowestStage,
    safeBounds
  }
}

async function runScanInternal(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const scanStartedAt = Date.now()
  const stageDurationsMs = buildDefaultPerformanceStageDurations()
  let imageEnrichmentSummary = buildDefaultImageEnrichmentSummary()
  const sourceCatalogStartedAt = Date.now()
  const sourceCatalogDiagnostics = await marketSourceCatalogService
    .prepareSourceCatalog({
      targetUniverseSize: UNIVERSE_TARGET_SIZE,
      forceRefresh
    })
    .catch((err) => {
      console.error("[arbitrage-scanner] Source catalog refresh failed", err.message)
      return {
        ...marketSourceCatalogService.getLastDiagnostics(),
        error: String(err?.message || "source_catalog_refresh_failed")
      }
    })
  stageDurationsMs.sourceCatalogPreparationMs = Date.now() - sourceCatalogStartedAt
  const discardStats = {}
  const rejectedByItem = {}
  const staleDiagnosticsAccumulator = buildStaleDiagnosticsAccumulator()
  const inputHydrationStartedAt = Date.now()
  const scannerInputs = await loadScannerInputs(discardStats, rejectedByItem)
  stageDurationsMs.inputHydrationMs = Date.now() - inputHydrationStartedAt
  const universeSeeds = Array.isArray(scannerInputs?.seeds) ? scannerInputs.seeds : []
  const snapshotWarmupSummary = scannerInputs?.snapshotWarmup || toSnapshotWarmupSummary()
  if (!universeSeeds.length) {
    const diagnosticsAggregationStartedAt = Date.now()
    const generatedTs = Date.now()
    const emptyByCategory = Object.fromEntries(
      Object.values(ITEM_CATEGORIES).map((category) => [category, 0])
    )
    const discardedReasons = normalizeDiscardStats(discardStats)
    const discardedReasonsByCategory = normalizeDiscardStatsByCategory(discardStats)
    const staleDiagnostics = toStaleDiagnosticsSummary(staleDiagnosticsAccumulator)
    const emptyPayload = {
      generatedAt: new Date(generatedTs).toISOString(),
      expiresAt: generatedTs + CACHE_TTL_MS,
      ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
      currency: "USD",
      summary: {
        scannedItems: 0,
        opportunities: 0,
        totalDetected: 0,
        universeSize: 0,
        universeTarget: UNIVERSE_TARGET_SIZE,
        candidateItems: 0,
        discardedReasons,
        discardedReasonsByCategory,
        rejectedByCategory: { ...emptyByCategory },
        topRejectedItems: toTopRejectedItems(rejectedByItem),
        rejectionReasonsByItem: toRejectionReasonsByItem(rejectedByItem),
        selectedUniverseByCategory: { ...emptyByCategory },
        opportunitiesByCategory: { ...emptyByCategory },
        staleDiagnostics,
        knifeGloveRejections: toKnifeGloveRejectionSummary({}),
        riskyProfileDiagnostics: buildRiskyProfileDiagnostics(),
        snapshotWarmup: snapshotWarmupSummary,
        imageEnrichment: imageEnrichmentSummary,
        sourceCatalog: sourceCatalogDiagnostics,
        scanProgress: buildScanProgressStats({
          universeTarget: UNIVERSE_TARGET_SIZE,
          candidateItems: 0,
          scannedItems: 0
        }),
        highConfidence: 0,
        riskyEligible: 0
      },
      opportunities: [],
      pipeline: {
        sequence: [
          "fetch_quotes",
          "save_market_prices",
          "compute_from_saved_quotes",
          "apply_freshness_scoring",
          "persist_diagnostics"
        ],
        config: {
          defaultUniverseLimit: DEFAULT_UNIVERSE_LIMIT,
          universeTargetSize: UNIVERSE_TARGET_SIZE,
          preCompareUniverseLimit: PRE_COMPARE_UNIVERSE_LIMIT,
          universeDbLimit: UNIVERSE_DB_LIMIT,
          scanBatchSize: SCAN_BATCH_SIZE,
          maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
          scanTimeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
          imageEnrichBatchSize: IMAGE_ENRICH_BATCH_SIZE,
          imageEnrichConcurrency: IMAGE_ENRICH_CONCURRENCY,
          imageEnrichTimeoutMs: IMAGE_ENRICH_TIMEOUT_MS
        },
        quoteRefresh: {
          batchSize: SCAN_BATCH_SIZE,
          timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
          maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
          requestedItems: 0,
          totalBatches: 0,
          completedBatches: 0,
          failedBatches: 0,
          timedOutBatches: 0,
          itemsCompared: 0,
          availableQuotes: 0,
          averageBatchMs: 0,
          slowestBatchMs: 0,
          errors: []
        },
        computeFromSavedQuotes: {
          batchSize: SCAN_BATCH_SIZE,
          timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
          maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
          requestedItems: 0,
          totalBatches: 0,
          completedBatches: 0,
          failedBatches: 0,
          timedOutBatches: 0,
          itemsCompared: 0,
          availableQuotes: 0,
          averageBatchMs: 0,
          slowestBatchMs: 0,
          errors: []
        },
        quoteSnapshot: {
          rowsPrepared: 0,
          rowsInserted: 0,
          persisted: true
        },
        snapshotWarmup: snapshotWarmupSummary,
        imageEnrichment: imageEnrichmentSummary,
        sourceCatalog: sourceCatalogDiagnostics
      }
    }

    stageDurationsMs.diagnosticsAggregationMs = Date.now() - diagnosticsAggregationStartedAt
    const auditBuildStartedAt = Date.now()
    let performanceAudit = buildScannerPerformanceAudit({
      sourceCatalogDiagnostics,
      selectedUniverseByCategory: emptyByCategory,
      staleDiagnostics,
      discardedReasons,
      discardedReasonsByCategory,
      sortedRows: [],
      selectedUniverseRows: [],
      quoteRefreshSummary: emptyPayload.pipeline.quoteRefresh,
      computeFromSavedSummary: emptyPayload.pipeline.computeFromSavedQuotes,
      stageDurationsMs,
      scanDurationMs: Date.now() - scanStartedAt
    })
    const auditBuildDurationMs = Date.now() - auditBuildStartedAt
    performanceAudit = extendPerformanceAudit(performanceAudit, {
      additionalDiagnosticsAggregationMs: auditBuildDurationMs,
      endToEndScanDurationMs: Date.now() - scanStartedAt
    })
    emptyPayload.summary.performanceAudit = performanceAudit
    emptyPayload.pipeline.performanceAudit = performanceAudit

    scannerState.latest = emptyPayload
    return emptyPayload
  }

  const normalizationStartedAt = Date.now()
  const comparisonInputItems = universeSeeds.map((row) => buildInputItemForComparison(row))
  const inputByName = toByNameMap(
    universeSeeds.map((row) => ({
      ...row,
      marketHashName: normalizeMarketHashName(row?.marketHashName)
    })),
    "marketHashName"
  )
  stageDurationsMs.normalizationMs += Date.now() - normalizationStartedAt

  const quoteFetchingStartedAt = Date.now()
  const quoteRefreshSummary = await refreshQuotesInBatches(comparisonInputItems, {
    forceRefresh
  })
  const comparisonFromSaved = await compareFromSavedQuotes(comparisonInputItems)
  stageDurationsMs.quoteFetchingMs = Date.now() - quoteFetchingStartedAt

  const normalizationEnrichmentStartedAt = Date.now()
  imageEnrichmentSummary = await hydrateUniverseImages(inputByName, comparisonFromSaved.items)
  stageDurationsMs.normalizationMs += Date.now() - normalizationEnrichmentStartedAt

  const quoteSnapshotWriteStartedAt = Date.now()
  const quoteSnapshotSummary = await persistQuoteSnapshot(comparisonFromSaved.items, inputByName)
  stageDurationsMs.dbWritesMs += Date.now() - quoteSnapshotWriteStartedAt

  const opportunityComputationStartedAt = Date.now()
  const selectedUniverse = selectTopUniverseItems(
    comparisonFromSaved?.items,
    inputByName,
    discardStats,
    rejectedByItem
  )
  const rows = []
  const riskyProfileDiagnostics = buildRiskyProfileDiagnostics()
  for (const selected of selectedUniverse) {
    const item = selected?.comparisonItem || null
    const inputItem = selected?.inputItem || null
    const itemName = String(inputItem?.marketHashName || item?.marketHashName || "").trim()
    const itemCategory = normalizeItemCategory(inputItem?.itemCategory, itemName)
    if (!item || !inputItem) continue
    const opportunity = item?.arbitrage || null
    if (!opportunity) {
      incrementReasonCounter(discardStats, "insufficient_market_data", itemCategory)
      incrementItemReasonCounter(rejectedByItem, itemName, "insufficient_market_data", itemCategory)
      continue
    }

    const enrichedOpportunity = {
      ...opportunity,
      itemCategory,
      marketCoverage: selected.marketCoverage
    }
    const liquidity = resolveLiquidityMetrics(enrichedOpportunity, inputItem)
    const stale = resolveStaleDataPenalty(item?.perMarket, enrichedOpportunity, itemCategory)
    trackStaleDiagnostics(staleDiagnosticsAccumulator, itemCategory, stale)
    trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "attempted")

    const hardRejectionReason = getPrimaryHardRejectionReason(enrichedOpportunity)
    if (hardRejectionReason) {
      collectDiscardReasonsFromOpportunity(
        enrichedOpportunity,
        discardStats,
        rejectedByItem,
        itemName,
        itemCategory
      )
      if (!Array.isArray(enrichedOpportunity?.antiFake?.reasons) || !enrichedOpportunity.antiFake.reasons.length) {
        incrementReasonCounter(discardStats, hardRejectionReason, itemCategory)
        incrementItemReasonCounter(rejectedByItem, itemName, hardRejectionReason, itemCategory)
      }
      trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "rejected", hardRejectionReason)
      continue
    }

    const riskyProfile = getCategoryRiskyProfile(itemCategory)
    const riskyEvaluation = computeRiskAdjustments({
      opportunity: enrichedOpportunity,
      liquidity,
      stale,
      inputItem,
      profile: riskyProfile
    })
    if (!riskyEvaluation.passed) {
      incrementReasonCounter(discardStats, riskyEvaluation.primaryReason, itemCategory)
      incrementItemReasonCounter(
        rejectedByItem,
        itemName,
        riskyEvaluation.primaryReason,
        itemCategory
      )
      trackRiskyDecision(
        riskyProfileDiagnostics,
        itemCategory,
        "rejected",
        riskyEvaluation.primaryReason
      )
      continue
    }

    const strictEvaluation = computeRiskAdjustments({
      opportunity: enrichedOpportunity,
      liquidity,
      stale,
      inputItem,
      profile: STRICT_SCAN_PROFILE
    })

    const apiRow = buildApiOpportunityRow({
      opportunity: enrichedOpportunity,
      inputItem,
      liquidity,
      stale,
      perMarket: item?.perMarket,
      extraPenalty: riskyEvaluation.penalty,
      isRiskyEligible: true,
      isHighConfidenceEligible: strictEvaluation.passed
    })

    const riskyFloor = getRiskyQualityFloor(itemCategory)
    if (Number(apiRow?.profit || 0) < Number(riskyFloor.minProfitUsd || 0)) {
      incrementReasonCounter(discardStats, "risky_low_profit", itemCategory)
      incrementItemReasonCounter(rejectedByItem, itemName, "risky_low_profit", itemCategory)
      trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "rejected", "risky_low_profit")
      continue
    }
    if (Number(apiRow?.score || 0) < Number(riskyFloor.minScore || RISKY_MIN_SCORE)) {
      incrementReasonCounter(discardStats, "risky_low_score", itemCategory)
      incrementItemReasonCounter(rejectedByItem, itemName, "risky_low_score", itemCategory)
      trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "rejected", "risky_low_score")
      continue
    }
    if (
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      normalizeConfidence(apiRow?.executionConfidence) === "Low"
    ) {
      incrementReasonCounter(discardStats, "risky_low_confidence", itemCategory)
      incrementItemReasonCounter(rejectedByItem, itemName, "risky_low_confidence", itemCategory)
      trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "rejected", "risky_low_confidence")
      continue
    }

    const highConfidenceEligible =
      Boolean(strictEvaluation?.passed) &&
      Number(apiRow?.score || 0) >= HIGH_CONFIDENCE_MIN_SCORE &&
      String(apiRow?.executionConfidence || "")
        .trim()
        .toLowerCase() !== "low"
    rows.push({
      ...apiRow,
      isHighConfidenceEligible: highConfidenceEligible
    })
    const premiumAcceptedWithMissingDepth =
      PREMIUM_ITEM_CATEGORIES.has(itemCategory) &&
      (
        Boolean(liquidity?.hasMissingDepth) ||
        (Boolean(liquidity?.buyDepthMissing) && Boolean(liquidity?.sellDepthMissing))
      )
    trackRiskyDecision(
      riskyProfileDiagnostics,
      itemCategory,
      "accepted",
      premiumAcceptedWithMissingDepth
        ? "accepted_premium_missing_depth_tolerated"
        : PREMIUM_ITEM_CATEGORIES.has(itemCategory)
          ? "accepted_premium_profile"
          : "accepted_standard_profile"
    )
  }

  const sortedRows = sortOpportunities(rows)
  stageDurationsMs.opportunityComputationMs = Date.now() - opportunityComputationStartedAt
  const diagnosticsAggregationStartedAt = Date.now()
  const highConfidenceCount = sortedRows.filter(
    (row) =>
      Boolean(row?.isHighConfidenceEligible) &&
      Number(row?.score || 0) >= DEFAULT_SCORE_CUTOFF &&
      String(row?.executionConfidence || "")
        .trim()
        .toLowerCase() !== "low"
  ).length
  const riskyEligibleCount = sortedRows.filter((row) => Boolean(row?.isRiskyEligible)).length
  const selectedUniverseByCategory = countRowsByCategory(
    selectedUniverse.map((row) => ({
      itemCategory: row?.inputItem?.itemCategory,
      itemName: row?.inputItem?.marketHashName
    }))
  )
  const candidateUniverseByCategory = countRowsByCategory(
    universeSeeds.map((row) => ({
      itemCategory: row?.itemCategory,
      itemName: row?.marketHashName
    }))
  )
  const effectiveSourceCatalogDiagnostics = deriveSourceCatalogDiagnosticsFromScan({
    sourceCatalogDiagnostics,
    candidateItems: universeSeeds.length,
    scannedItems: selectedUniverse.length,
    candidateByCategory: candidateUniverseByCategory,
    selectedByCategory: selectedUniverseByCategory,
    universeTarget: UNIVERSE_TARGET_SIZE
  })
  const opportunitiesByCategory = countRowsByCategory(sortedRows)
  const discardedReasons = normalizeDiscardStats(discardStats)
  const discardedReasonsByCategory = normalizeDiscardStatsByCategory(discardStats)
  const rejectedByCategory = toRejectedCountByCategory(discardedReasonsByCategory)
  const staleDiagnostics = toStaleDiagnosticsSummary(staleDiagnosticsAccumulator)
  const knifeGloveRejections = toKnifeGloveRejectionSummary(discardedReasonsByCategory)
  const scanProgress = buildScanProgressStats({
    universeTarget: UNIVERSE_TARGET_SIZE,
    candidateItems: universeSeeds.length,
    scannedItems: selectedUniverse.length,
    quoteRefresh: quoteRefreshSummary,
    computeFromSavedQuotes: comparisonFromSaved?.diagnostics || {}
  })
  const generatedTs = Date.now()
  const payload = {
    generatedAt: new Date(generatedTs).toISOString(),
    expiresAt: generatedTs + CACHE_TTL_MS,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: String(comparisonFromSaved?.currency || "USD")
      .trim()
      .toUpperCase(),
    summary: {
      scannedItems: selectedUniverse.length,
      opportunities: highConfidenceCount,
      totalDetected: sortedRows.length,
      universeSize: selectedUniverse.length,
      universeTarget: UNIVERSE_TARGET_SIZE,
      candidateItems: universeSeeds.length,
      discardedReasons,
      discardedReasonsByCategory,
      rejectedByCategory,
      topRejectedItems: toTopRejectedItems(rejectedByItem),
      rejectionReasonsByItem: toRejectionReasonsByItem(rejectedByItem),
      selectedUniverseByCategory,
      opportunitiesByCategory,
      staleDiagnostics,
      knifeGloveRejections,
      riskyProfileDiagnostics,
      snapshotWarmup: snapshotWarmupSummary,
      imageEnrichment: imageEnrichmentSummary,
      sourceCatalog: effectiveSourceCatalogDiagnostics,
      scanProgress,
      highConfidence: highConfidenceCount,
      riskyEligible: riskyEligibleCount
    },
    opportunities: sortedRows,
    pipeline: {
      sequence: [
        "fetch_quotes",
        "save_market_prices",
        "compute_from_saved_quotes",
        "apply_freshness_scoring",
        "persist_diagnostics"
      ],
      config: {
        defaultUniverseLimit: DEFAULT_UNIVERSE_LIMIT,
        universeTargetSize: UNIVERSE_TARGET_SIZE,
        preCompareUniverseLimit: PRE_COMPARE_UNIVERSE_LIMIT,
        universeDbLimit: UNIVERSE_DB_LIMIT,
        scanBatchSize: SCAN_BATCH_SIZE,
        maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
        scanTimeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
        imageEnrichBatchSize: IMAGE_ENRICH_BATCH_SIZE,
        imageEnrichConcurrency: IMAGE_ENRICH_CONCURRENCY,
        imageEnrichTimeoutMs: IMAGE_ENRICH_TIMEOUT_MS
      },
      quoteRefresh: quoteRefreshSummary,
      computeFromSavedQuotes: comparisonFromSaved?.diagnostics || null,
      quoteSnapshot: quoteSnapshotSummary,
      snapshotWarmup: snapshotWarmupSummary,
      imageEnrichment: imageEnrichmentSummary,
      sourceCatalog: effectiveSourceCatalogDiagnostics
    }
  }

  stageDurationsMs.diagnosticsAggregationMs = Date.now() - diagnosticsAggregationStartedAt
  const auditBuildStartedAt = Date.now()
  let performanceAudit = buildScannerPerformanceAudit({
    sourceCatalogDiagnostics: effectiveSourceCatalogDiagnostics,
    selectedUniverseByCategory,
    staleDiagnostics,
    discardedReasons,
    discardedReasonsByCategory,
    sortedRows,
    selectedUniverseRows: selectedUniverse,
    quoteRefreshSummary,
    computeFromSavedSummary: comparisonFromSaved?.diagnostics || {},
    stageDurationsMs,
    scanDurationMs: Date.now() - scanStartedAt
  })
  const auditBuildDurationMs = Date.now() - auditBuildStartedAt
  performanceAudit = extendPerformanceAudit(performanceAudit, {
    additionalDiagnosticsAggregationMs: auditBuildDurationMs,
    endToEndScanDurationMs: Date.now() - scanStartedAt
  })
  payload.summary.performanceAudit = performanceAudit
  payload.pipeline.performanceAudit = performanceAudit

  scannerState.latest = payload
  scannerState.lastError = null
  return payload
}

async function runScan(options = {}) {
  const enqueue = await enqueueScan(options)
  if (enqueue.alreadyRunning && scannerState.inFlight) {
    return scannerState.inFlight
  }
  return scannerState.inFlight || scannerState.latest
}

function normalizeLimit(value, fallback = DEFAULT_API_LIMIT, maxLimit = MAX_API_LIMIT) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), maxLimit)
}

function toScanDiagnosticsSummary(scanPayload = {}, persistSummary = {}, trigger = "manual") {
  return {
    trigger: String(trigger || "manual"),
    generatedAt: scanPayload?.generatedAt || null,
    scannedItems: Number(scanPayload?.summary?.scannedItems || 0),
    opportunities: Number(scanPayload?.summary?.opportunities || 0),
    totalDetected: Number(scanPayload?.summary?.totalDetected || 0),
    universeSize: Number(scanPayload?.summary?.universeSize || 0),
    universeTarget: Number(scanPayload?.summary?.universeTarget || UNIVERSE_TARGET_SIZE),
    candidateItems: Number(scanPayload?.summary?.candidateItems || 0),
    discardedReasons: scanPayload?.summary?.discardedReasons || {},
    discardedReasonsByCategory: scanPayload?.summary?.discardedReasonsByCategory || {},
    rejectedByCategory: scanPayload?.summary?.rejectedByCategory || {},
    topRejectedItems: scanPayload?.summary?.topRejectedItems || [],
    rejectionReasonsByItem: scanPayload?.summary?.rejectionReasonsByItem || [],
    selectedUniverseByCategory: scanPayload?.summary?.selectedUniverseByCategory || {},
    opportunitiesByCategory: scanPayload?.summary?.opportunitiesByCategory || {},
    staleDiagnostics: scanPayload?.summary?.staleDiagnostics || {},
    knifeGloveRejections: scanPayload?.summary?.knifeGloveRejections || {},
    riskyProfileDiagnostics: scanPayload?.summary?.riskyProfileDiagnostics || {},
    snapshotWarmup:
      scanPayload?.summary?.snapshotWarmup || scanPayload?.pipeline?.snapshotWarmup || {},
    imageEnrichment:
      scanPayload?.summary?.imageEnrichment || scanPayload?.pipeline?.imageEnrichment || {},
    sourceCatalog:
      scanPayload?.summary?.sourceCatalog || scanPayload?.pipeline?.sourceCatalog || {},
    scanProgress: scanPayload?.summary?.scanProgress || {},
    performanceAudit:
      scanPayload?.summary?.performanceAudit || scanPayload?.pipeline?.performanceAudit || {},
    highConfidence: Number(scanPayload?.summary?.highConfidence || 0),
    riskyEligible: Number(scanPayload?.summary?.riskyEligible || 0),
    pipeline: scanPayload?.pipeline || {},
    persisted: persistSummary || {}
  }
}

async function applyFeedRetention() {
  const cutoffIso = new Date(Date.now() - FEED_RETENTION_HOURS * 60 * 60 * 1000).toISOString()
  const byAge = await arbitrageFeedRepo.markInactiveOlderThan(cutoffIso)
  const byLimit = await arbitrageFeedRepo.markInactiveBeyondLimit(FEED_ACTIVE_LIMIT)
  return {
    retentionCutoffIso: cutoffIso,
    deactivatedByAge: byAge,
    deactivatedByLimit: byLimit
  }
}

async function appendOpportunitiesToFeed(rows = [], scanRunId = "") {
  const candidates = (Array.isArray(rows) ? rows : [])
    .filter((row) => {
      const marketHashName = String(row?.itemName || "").trim()
      if (!marketHashName) return false
      if (!Boolean(row?.isRiskyEligible) && !Boolean(row?.isHighConfidenceEligible)) return false
      const itemCategory = normalizeItemCategory(row?.itemCategory, marketHashName)
      const floor = getRiskyQualityFloor(itemCategory)
      const score = Number(row?.score || 0)
      const minScore = Boolean(row?.isHighConfidenceEligible)
        ? DEFAULT_SCORE_CUTOFF
        : Number(floor?.minScore || FEED_RISKY_MIN_SCORE)
      const minProfit = Boolean(row?.isHighConfidenceEligible)
        ? 0
        : Number(floor?.minProfitUsd || 0)
      return score >= minScore && Number(row?.profit || 0) >= minProfit
    })
    .map((row) => ({
      ...row,
      itemName: String(row?.itemName || "").trim(),
      buyMarket: normalizeMarketLabel(row?.buyMarket),
      sellMarket: normalizeMarketLabel(row?.sellMarket)
    }))

  if (!candidates.length) {
    const retention = await applyFeedRetention()
    return {
      candidates: 0,
      insertedCount: 0,
      duplicateSkipped: 0,
      duplicateInserted: 0,
      ...retention
    }
  }

  const dedupeCutoffIso = new Date(Date.now() - DUPLICATE_WINDOW_HOURS * 60 * 60 * 1000).toISOString()
  const recentRows = await arbitrageFeedRepo.getRecentRowsByItems({
    itemNames: candidates.map((row) => row.itemName),
    sinceIso: dedupeCutoffIso,
    limit: 5000
  })

  const latestBySignature = {}
  for (const row of recentRows) {
    const signature = buildDedupeSignature(row?.item_name, row?.buy_market, row?.sell_market)
    if (!signature || latestBySignature[signature]) continue
    latestBySignature[signature] = row
  }

  const detectedAt = new Date().toISOString()
  const toInsert = []
  let duplicateSkipped = 0
  let duplicateInserted = 0
  for (const row of candidates) {
    const signature = buildDedupeSignature(row?.itemName, row?.buyMarket, row?.sellMarket)
    const previous = latestBySignature[signature] || null
    const materiallyNew = !previous || isMateriallyNewOpportunity(row, previous)
    if (!materiallyNew && !INSERT_DUPLICATES) {
      duplicateSkipped += 1
      continue
    }

    const isDuplicate = !materiallyNew
    if (isDuplicate) {
      duplicateInserted += 1
    }
    toInsert.push(
      buildFeedInsertRow(row, scanRunId, {
        detectedAt,
        isDuplicate
      })
    )

    latestBySignature[signature] = {
      profit: row?.profit,
      opportunity_score: row?.score
    }
  }

  const insertedRows = toInsert.length ? await arbitrageFeedRepo.insertRows(toInsert) : []
  const retention = await applyFeedRetention()

  return {
    candidates: candidates.length,
    insertedCount: insertedRows.length,
    duplicateSkipped,
    duplicateInserted,
    ...retention
  }
}

async function runScanWithRunRecord(runRecord = {}, options = {}) {
  const trigger = String(options.trigger || "manual")
  const forceRefresh = Boolean(options.forceRefresh)
  const runStartedAt = Date.now()
  try {
    const scanPayload = await runScanInternal({ forceRefresh })
    const feedPersistStartedAt = Date.now()
    const persistSummary = await appendOpportunitiesToFeed(scanPayload?.opportunities || [], runRecord?.id)
    const feedPersistDurationMs = Date.now() - feedPersistStartedAt
    const performanceAuditBeforePersist =
      scanPayload?.summary?.performanceAudit || scanPayload?.pipeline?.performanceAudit || null
    if (performanceAuditBeforePersist && typeof performanceAuditBeforePersist === "object") {
      const extendedPerformanceAudit = extendPerformanceAudit(performanceAuditBeforePersist, {
        additionalDbWriteDurationMs: feedPersistDurationMs,
        endToEndScanDurationMs: Date.now() - runStartedAt
      })
      scanPayload.summary.performanceAudit = extendedPerformanceAudit
      if (scanPayload.pipeline && typeof scanPayload.pipeline === "object") {
        scanPayload.pipeline.performanceAudit = extendedPerformanceAudit
      }
    }

    const diagnosticsAggregationStartedAt = Date.now()
    const diagnosticsSummary = toScanDiagnosticsSummary(scanPayload, persistSummary, trigger)
    const diagnosticsAggregationDurationMs = Date.now() - diagnosticsAggregationStartedAt
    if (scanPayload?.summary?.performanceAudit && typeof scanPayload.summary.performanceAudit === "object") {
      const extendedPerformanceAudit = extendPerformanceAudit(scanPayload.summary.performanceAudit, {
        additionalDiagnosticsAggregationMs: diagnosticsAggregationDurationMs,
        endToEndScanDurationMs: Date.now() - runStartedAt
      })
      scanPayload.summary.performanceAudit = extendedPerformanceAudit
      if (scanPayload.pipeline && typeof scanPayload.pipeline === "object") {
        scanPayload.pipeline.performanceAudit = extendedPerformanceAudit
      }
      diagnosticsSummary.performanceAudit = extendedPerformanceAudit
    }

    const completedRun = await scannerRunRepo.markCompleted(runRecord?.id, {
      itemsScanned: Number(scanPayload?.summary?.scannedItems || 0),
      opportunitiesFound: Number(scanPayload?.opportunities?.length || 0),
      newOpportunitiesAdded: Number(persistSummary?.insertedCount || 0),
      diagnosticsSummary
    })

    scannerState.lastPersistSummary = persistSummary
    scannerState.lastCompletedAt = completedRun?.completed_at || scanPayload?.generatedAt || null
    return {
      run: completedRun || runRecord,
      payload: scanPayload,
      persistSummary
    }
  } catch (err) {
    scannerState.lastError = err
    await scannerRunRepo
      .markFailed(runRecord?.id, {
        diagnosticsSummary: {
          trigger,
          error: String(err?.message || "scan_failed")
        },
        error: err?.message
      })
      .catch((persistErr) => {
        console.error("[arbitrage-scanner] Failed to mark run as failed", persistErr.message)
      })
    throw err
  } finally {
    scannerState.inFlight = null
    scannerState.inFlightRunId = null
  }
}

async function enqueueScan(options = {}) {
  if (scannerState.inFlight) {
    return {
      scanRunId: scannerState.inFlightRunId,
      alreadyRunning: true
    }
  }

  const trigger = String(options.trigger || "manual")
  const runRecord = await scannerRunRepo.createRun({
    scannerType: SCANNER_TYPE,
    status: "running",
    diagnosticsSummary: {
      trigger
    }
  })

  scannerState.inFlightRunId = runRecord?.id || null
  scannerState.lastStartedAt = runRecord?.started_at || new Date().toISOString()
  scannerState.inFlight = runScanWithRunRecord(runRecord, options).catch((err) => {
    if (scannerState.latest) {
      return {
        run: runRecord,
        payload: scannerState.latest,
        persistSummary: scannerState.lastPersistSummary || null,
        error: err
      }
    }
    throw err
  })

  return {
    scanRunId: runRecord?.id || null,
    alreadyRunning: false
  }
}

async function getScannerStatusInternal() {
  const [latestRun, latestCompletedRun, activeCount] = await Promise.all([
    scannerRunRepo.getLatestRun(SCANNER_TYPE),
    scannerRunRepo.getLatestCompletedRun(SCANNER_TYPE),
    arbitrageFeedRepo.countFeed({ includeInactive: false })
  ])
  const latestRunStatus = String(latestRun?.status || "")
    .trim()
    .toLowerCase()
  const currentStatus = scannerState.inFlight || latestRunStatus === "running" ? "running" : "idle"
  const currentRunId = scannerState.inFlightRunId || (currentStatus === "running" ? latestRun?.id || null : null)

  return {
    scannerType: SCANNER_TYPE,
    intervalMinutes: SCANNER_INTERVAL_MINUTES,
    schedulerRunning: Boolean(scannerState.timer),
    currentStatus,
    currentRunId,
    nextScheduledAt: scannerState.nextScheduledAt,
    activeOpportunities: Number(activeCount || 0),
    latestRun,
    latestCompletedRun
  }
}

async function ensureScheduledScanHeartbeat(statusHint = null, trigger = "watchdog") {
  if (scannerState.inFlight) return false

  const status = statusHint || (await getScannerStatusInternal())
  if (!isScannerRunOverdue(status)) {
    return false
  }

  const enqueue = await enqueueScan({
    forceRefresh: false,
    trigger
  })
  if (!enqueue?.alreadyRunning) {
    updateNextScheduledAt()
  }
  return Boolean(enqueue?.scanRunId)
}

function parseIsoTimestampMs(value) {
  const text = String(value || "").trim()
  if (!text) return null
  const ts = new Date(text).getTime()
  return Number.isFinite(ts) ? ts : null
}

async function resolvePlanContext(options = {}) {
  if (options?.entitlements && typeof options.entitlements === "object") {
    return {
      userId: String(options.userId || "").trim(),
      planTier: planService.normalizePlanTier(
        options.planTier || options.entitlements.planTier
      ),
      entitlements: options.entitlements
    }
  }

  const userId = String(options.userId || "").trim()
  if (!userId) {
    const planTier = planService.normalizePlanTier(options.planTier || "full_access")
    return {
      userId: "",
      planTier,
      entitlements: planService.getEntitlements(planTier)
    }
  }

  const { planTier, entitlements } = await planService.getUserPlanProfile(userId)
  return { userId, planTier, entitlements }
}

function applyFeedPlanRestrictions(rows = [], entitlements = {}) {
  const safeRows = Array.isArray(rows) ? rows : []
  const planConfig = planService.getPlanConfig(entitlements?.planTier || entitlements)
  const visibleFeedLimit = Math.max(Number(planConfig?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
  const limitedRows = safeRows.filter((row, index) =>
    Boolean(
      planService.canViewOpportunity(
        planConfig,
        { position: index },
        row?.itemCategory || "weapon_skin"
      )?.visibleAllowed
    )
  )

  const delayedSignals = planService.hasFeatureAccess(planConfig, "delayed_signals")
  const signalDelayMinutes = delayedSignals
    ? Math.max(Number(planConfig?.signalDelayMinutes || 0), 0)
    : 0
  const cutoffTs = signalDelayMinutes > 0 ? Date.now() - signalDelayMinutes * 60 * 1000 : null
  const delayFilteredRows =
    cutoffTs == null
      ? limitedRows
      : limitedRows.filter((row) => {
          const detectedAtTs = parseIsoTimestampMs(row?.detectedAt || row?.detected_at)
          if (detectedAtTs == null) return true
          return detectedAtTs <= cutoffTs
        })
  const premiumPreviewResult = premiumCategoryAccessService.applyPremiumPreviewLock(
    delayFilteredRows,
    entitlements
  )
  const premiumCategoryAccess = premiumCategoryAccessService.hasPremiumCategoryAccess(entitlements)

  return {
    rows: premiumPreviewResult.rows,
    planLimits: {
      visibleFeedLimit,
      delayedSignals,
      signalDelayMinutes,
      advancedFilters: planService.canUseAdvancedFilters(planConfig),
      fullGlobalScanner: planService.hasFeatureAccess(planConfig, "full_global_scanner"),
      fullOpportunitiesFeed: planService.hasFeatureAccess(
        planConfig,
        "full_opportunities_feed"
      ),
      premiumCategoryAccess,
      knivesGlovesAccess: premiumCategoryAccess,
      lockedPremiumPreviewRows: Number(premiumPreviewResult.lockedCount || 0),
      feedTruncatedByLimit: Math.max(safeRows.length - limitedRows.length, 0),
      feedTruncatedByDelay: Math.max(limitedRows.length - delayFilteredRows.length, 0),
      feedTruncatedByPremiumLock: 0
    }
  }
}

function pruneManualRefreshTracker(nowMs) {
  if (manualRefreshTracker.size <= MANUAL_REFRESH_TRACKER_MAX) {
    return
  }

  const staleCutoffMs = nowMs - 7 * 24 * 60 * 60 * 1000
  for (const [userId, entry] of manualRefreshTracker.entries()) {
    if (Number(entry?.lastTriggeredAtMs || 0) < staleCutoffMs) {
      manualRefreshTracker.delete(userId)
    }
    if (manualRefreshTracker.size <= MANUAL_REFRESH_TRACKER_MAX) {
      break
    }
  }
}

function formatRetryWindow(remainingMs) {
  const remainingMinutes = Math.max(Math.ceil(Number(remainingMs || 0) / 60000), 1)
  if (remainingMinutes >= 60) {
    const hours = Math.max(Math.ceil(remainingMinutes / 60), 1)
    return `${hours} hour(s)`
  }
  return `${remainingMinutes} minute(s)`
}

function enforceManualRefreshCooldown(userId, entitlements = {}, nowMs = Date.now()) {
  const safeUserId = String(userId || "").trim()
  if (!safeUserId) return

  const previous = manualRefreshTracker.get(safeUserId)
  const lastTriggeredAtMs = Number(previous?.lastTriggeredAtMs || 0)
  const refreshPolicy = planService.canRefreshScanner(entitlements, lastTriggeredAtMs, {
    nowMs
  })
  const intervalMinutes = Math.max(Number(refreshPolicy?.intervalMinutes || SCANNER_INTERVAL_MINUTES), 1)

  if (!refreshPolicy.allowed) {
    const retryAfterMs = Math.max(Number(refreshPolicy?.retryAfterMs || 0), 0)
    const err = new AppError(
      `Manual scanner refresh is available every ${intervalMinutes} minute(s) on your plan. Try again in ${formatRetryWindow(
        retryAfterMs
      )}.`,
      429,
      "SCANNER_REFRESH_COOLDOWN"
    )
    err.retryAfterMs = retryAfterMs
    throw err
  }

  manualRefreshTracker.set(safeUserId, {
    lastTriggeredAtMs: nowMs,
    intervalMinutes
  })
  pruneManualRefreshTracker(nowMs)
}

function resolveNoOpportunitiesReason(summary = {}, status = {}, opportunities = []) {
  if (Array.isArray(opportunities) && opportunities.length) {
    return null
  }

  const currentStatus = String(status?.currentStatus || "")
    .trim()
    .toLowerCase()
  if (currentStatus === "running") {
    return {
      code: "scan_in_progress",
      message: "Scanner run is in progress. Feed will update after completion."
    }
  }

  const scannedItems = Number(summary?.scannedItems || 0)
  if (!scannedItems) {
    const sourceCatalog = summary?.sourceCatalog || {}
    const missingToTarget = Number(sourceCatalog?.universeBuild?.missingToTarget || 0)
    if (missingToTarget > 0) {
      return {
        code: "insufficient_catalog_coverage",
        count: missingToTarget,
        message: `Universe build is short by ${missingToTarget} item(s) due to insufficient eligible source catalog coverage.`
      }
    }
    return {
      code: "no_items_scanned",
      message: "No universe items were eligible for scan in the latest run."
    }
  }

  const discardedReasons = summary?.discardedReasons && typeof summary.discardedReasons === "object"
    ? summary.discardedReasons
    : {}
  const topReasonEntry = Object.entries(discardedReasons)
    .filter(([, count]) => Number(count || 0) > 0)
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))[0]
  if (!topReasonEntry) {
    return {
      code: "no_matching_feed_rows",
      message: "Scanner completed but no opportunities passed current filters."
    }
  }

  const [code, count] = topReasonEntry
  const reasonMessageMap = {
    ignored_execution_floor: "Most candidates failed execution floor checks.",
    ignored_low_price: "Most candidates failed execution floor checks.",
    ignored_low_value_universe: "Most candidates were filtered as low-value universe items.",
    ignored_low_liquidity: "Most candidates were rejected for low liquidity.",
    risky_low_price: "Most risky candidates were below category price floors.",
    risky_low_profit: "Most risky candidates had insufficient profit for their category.",
    risky_low_score: "Most risky candidates scored below category quality floors.",
    risky_low_confidence: "Most risky candidates had low execution confidence.",
    risky_missing_depth: "Most risky candidates failed depth checks under risky mode.",
    spread_below_min: "Most candidates were below spread baseline.",
    non_positive_profit: "Most candidates had non-positive projected profit.",
    ignored_extreme_spread: "Most candidates were rejected for extreme spread values.",
    ignored_reference_deviation: "Most candidates failed reference deviation checks.",
    ignored_missing_markets: "Most candidates were missing enough market coverage.",
    ignored_missing_liquidity_data: "Most candidates were missing liquidity data.",
    ignored_missing_depth: "Most candidates were rejected due to missing orderbook depth.",
    ignored_low_score: "Most candidates scored below profile thresholds.",
    ignored_stale_data: "Most candidates were rejected due to stale data."
  }
  return {
    code,
    count: Number(count || 0),
    message:
      reasonMessageMap[code] || "Most candidates were rejected by scanner quality filters."
  }
}

exports.getFeed = async (options = {}) => {
  const planContext = await resolvePlanContext(options)
  const entitlements =
    planContext?.entitlements || planService.getEntitlements(planContext?.planTier)
  const planConfig = planService.getPlanConfig(planContext?.planTier || entitlements?.planTier || "free")
  const advancedFiltersEnabled = planService.canUseAdvancedFilters(entitlements)
  const requestedLimit = normalizeLimit(options.limit, DEFAULT_API_LIMIT, MAX_FEED_LIMIT)
  const planVisibleLimit = Math.max(Number(planConfig?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
  const limit = Math.min(requestedLimit, planVisibleLimit)
  const requestedShowRisky = normalizeBoolean(options.showRisky)
  const requestedIncludeOlder = normalizeBoolean(options.includeOlder || options.showOlder)
  const requestedCategory = normalizeCategoryFilter(options.category)
  const showRisky = advancedFiltersEnabled ? requestedShowRisky : false
  const includeOlder = advancedFiltersEnabled ? requestedIncludeOlder : false
  const categoryFilter = advancedFiltersEnabled ? requestedCategory : "all"

  const minScore = showRisky ? FEED_RISKY_MIN_SCORE : DEFAULT_SCORE_CUTOFF
  const excludeLowConfidence = !showRisky
  const highConfidenceOnly = !showRisky

  const [feedRows, totalCount, activeCount, status] = await Promise.all([
    arbitrageFeedRepo.listFeed({
      limit,
      includeInactive: includeOlder,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence,
      highConfidenceOnly
    }),
    arbitrageFeedRepo.countFeed({
      includeInactive: includeOlder,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence,
      highConfidenceOnly
    }),
    arbitrageFeedRepo.countFeed({
      includeInactive: false,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence,
      highConfidenceOnly
    }),
    getScannerStatusInternal()
  ])

  let mappedRows = (Array.isArray(feedRows) ? feedRows : []).map((row) => mapFeedRowToApiRow(row))
  mappedRows = await enrichFeedRowsWithSkinMetadata(mappedRows)
  const feedRestrictionResult = applyFeedPlanRestrictions(mappedRows, entitlements)
  mappedRows = feedRestrictionResult.rows
  const feedPlanLimits = feedRestrictionResult.planLimits
  const latestCompleted = status?.latestCompletedRun || null
  const diagnosticsSummary =
    latestCompleted?.diagnostics_summary && typeof latestCompleted.diagnostics_summary === "object"
      ? latestCompleted.diagnostics_summary
      : {}
  const summary = {
    scannedItems:
      Number(diagnosticsSummary?.scannedItems || 0) || Number(latestCompleted?.items_scanned || 0),
    opportunities: mappedRows.length,
    totalDetected: Number(totalCount || mappedRows.length),
    activeOpportunities: Number(activeCount || 0),
    universeSize: Number(diagnosticsSummary?.universeSize || 0),
    universeTarget: Number(diagnosticsSummary?.universeTarget || UNIVERSE_TARGET_SIZE),
    candidateItems: Number(diagnosticsSummary?.candidateItems || 0),
    discardedReasons: diagnosticsSummary?.discardedReasons || {},
    discardedReasonsByCategory: diagnosticsSummary?.discardedReasonsByCategory || {},
    rejectedByCategory: diagnosticsSummary?.rejectedByCategory || {},
    topRejectedItems: diagnosticsSummary?.topRejectedItems || [],
    rejectionReasonsByItem: diagnosticsSummary?.rejectionReasonsByItem || [],
    selectedUniverseByCategory: diagnosticsSummary?.selectedUniverseByCategory || {},
    opportunitiesByCategory: diagnosticsSummary?.opportunitiesByCategory || {},
    staleDiagnostics: diagnosticsSummary?.staleDiagnostics || {},
    knifeGloveRejections: diagnosticsSummary?.knifeGloveRejections || {},
    riskyProfileDiagnostics: diagnosticsSummary?.riskyProfileDiagnostics || {},
    snapshotWarmup:
      diagnosticsSummary?.snapshotWarmup || diagnosticsSummary?.pipeline?.snapshotWarmup || {},
    imageEnrichment:
      diagnosticsSummary?.imageEnrichment || diagnosticsSummary?.pipeline?.imageEnrichment || {},
    sourceCatalog:
      diagnosticsSummary?.sourceCatalog || diagnosticsSummary?.pipeline?.sourceCatalog || {},
    scanProgress: diagnosticsSummary?.scanProgress || {},
    performanceAudit:
      diagnosticsSummary?.performanceAudit || diagnosticsSummary?.pipeline?.performanceAudit || {},
    highConfidence: Number(diagnosticsSummary?.highConfidence || 0),
    riskyEligible: Number(diagnosticsSummary?.riskyEligible || 0),
    newOpportunitiesAdded:
      Number(latestCompleted?.new_opportunities_added || diagnosticsSummary?.persisted?.insertedCount || 0),
    feedRetentionHours: FEED_RETENTION_HOURS,
    feedActiveLimit: FEED_ACTIVE_LIMIT,
    plan: {
      planTier: planContext?.planTier || "free",
      requestedLimit,
      appliedLimit: limit,
      requestedShowRisky,
      appliedShowRisky: showRisky,
      requestedIncludeOlder,
      appliedIncludeOlder: includeOlder,
      requestedCategory,
      appliedCategory: categoryFilter,
      ...feedPlanLimits
    }
  }
  if (!mappedRows.length && Number(feedPlanLimits?.feedTruncatedByDelay || 0) > 0) {
    summary.noOpportunitiesReason = {
      code: "signals_delayed_by_plan",
      message: `Signals are delayed by ${Number(feedPlanLimits?.signalDelayMinutes || 0)} minute(s) on your current plan.`
    }
  } else {
    summary.noOpportunitiesReason = resolveNoOpportunitiesReason(summary, status, mappedRows)
  }

  return {
    generatedAt: latestCompleted?.completed_at || latestCompleted?.started_at || null,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: "USD",
    summary,
    opportunities: mappedRows,
    plan: summary.plan,
    status: {
      scannerType: SCANNER_TYPE,
      intervalMinutes: SCANNER_INTERVAL_MINUTES,
      schedulerRunning: Boolean(status?.schedulerRunning),
      currentStatus: status?.currentStatus || "idle",
      currentRunId: status?.currentRunId || null,
      nextScheduledAt: status?.nextScheduledAt || null,
      activeOpportunities: Number(status?.activeOpportunities || 0),
      latestRun: status?.latestRun || null,
      latestCompletedRun: latestCompleted
    }
  }
}

exports.getTopOpportunities = async (options = {}) => exports.getFeed(options)

exports.triggerRefresh = async (options = {}) => {
  const planContext = await resolvePlanContext(options)
  enforceManualRefreshCooldown(planContext?.userId, planContext?.entitlements, Date.now())
  const forceRefresh = options.forceRefresh == null ? true : normalizeBoolean(options.forceRefresh)
  const enqueue = await enqueueScan({
    forceRefresh,
    trigger: String(options.trigger || "manual")
  })
  return {
    scanRunId: enqueue.scanRunId || null,
    alreadyRunning: Boolean(enqueue.alreadyRunning),
    startedAt: new Date().toISOString(),
    plan: {
      planTier: planContext?.planTier || "free",
      scannerRefreshIntervalMinutes: Number(
        planService.getPlanConfig(planContext?.entitlements || planContext?.planTier)
          .scannerRefreshIntervalMinutes || SCANNER_INTERVAL_MINUTES
      ),
    }
  }
}

exports.getStatus = async () => {
  const status = await getScannerStatusInternal()
  return {
    scannerType: SCANNER_TYPE,
    intervalMinutes: SCANNER_INTERVAL_MINUTES,
    schedulerRunning: Boolean(status?.schedulerRunning),
    currentStatus: status?.currentStatus || "idle",
    currentRunId: status?.currentRunId || null,
    nextScheduledAt: status?.nextScheduledAt || null,
    activeOpportunities: Number(status?.activeOpportunities || 0),
    latestRun: status?.latestRun || null,
    latestCompletedRun: status?.latestCompletedRun || null
  }
}

exports.startScheduler = () => {
  if (scannerState.timer) {
    return
  }

  enqueueScan({ forceRefresh: false, trigger: "startup" }).catch((err) => {
    console.error("[arbitrage-scanner] Initial scan enqueue failed", err.message)
  })
  updateNextScheduledAt()

  scannerState.timer = setInterval(() => {
    enqueueScan({ forceRefresh: false, trigger: "scheduled" }).catch((err) => {
      console.error("[arbitrage-scanner] Scheduled scan enqueue failed", err.message)
    })
    updateNextScheduledAt()
  }, SCANNER_INTERVAL_MS)
  scannerState.timer.unref?.()

  console.log(`[arbitrage-scanner] Scheduler started (every ${SCANNER_INTERVAL_MINUTES} minute(s))`)
}

exports.stopScheduler = () => {
  if (!scannerState.timer) return
  clearInterval(scannerState.timer)
  scannerState.timer = null
  scannerState.nextScheduledAt = null
}

exports.forceRefresh = async () => runScan({ forceRefresh: true, trigger: "manual" })

exports.__testables = {
  normalizeUniverseEntries,
  normalizeItemCategory,
  normalizeCategoryFilter,
  computeLiquidityScoreFromSnapshot,
  resolveVolume7d,
  resolveStaleDataPenalty,
  resolveLiquidityMetrics,
  passesUniverseSeedFilters,
  passesScannerGuards,
  buildApiOpportunityRow,
  buildFeedInsertRow,
  mapFeedRowToApiRow,
  isMateriallyNewOpportunity,
  isScannerRunOverdue,
  clampScore,
  computeLiquidityRank,
  countAvailableMarkets,
  isLowValueJunkName,
  computeStrictCoverageThreshold,
  DEFAULT_UNIVERSE_LIMIT,
  SCAN_BATCH_SIZE,
  MAX_CONCURRENT_MARKET_REQUESTS,
  SCAN_TIMEOUT_PER_BATCH
}

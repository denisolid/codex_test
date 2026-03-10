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
const RISKY_MIN_PRICE_USD = 2
const RISKY_MIN_SPREAD_PERCENT = 3
const RISKY_MAX_SPREAD_PERCENT = 250
const RISKY_MIN_VOLUME_7D = 20
const RISKY_MIN_SCORE = 45
const MIN_SPREAD_PERCENT = HIGH_CONFIDENCE_MIN_SPREAD_PERCENT
const MAX_SPREAD_PERCENT = RISKY_MAX_SPREAD_PERCENT
const MIN_VOLUME_7D = HIGH_CONFIDENCE_MIN_VOLUME_7D
const MIN_EXECUTION_PRICE_USD = HIGH_CONFIDENCE_MIN_PRICE_USD
const MIN_MARKET_COVERAGE = 2
const DEFAULT_SCORE_CUTOFF = HIGH_CONFIDENCE_MIN_SCORE
const RISKY_SCORE_CUTOFF = RISKY_MIN_SCORE
const FEED_RISKY_MIN_SCORE = RISKY_MIN_SCORE
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

const STALE_PENALTY_RULES = Object.freeze([
  { minMinutes: 180, penalty: 25 },
  { minMinutes: 60, penalty: 15 },
  { minMinutes: 15, penalty: 8 }
])

const SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"])
const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule"
})
const FALLBACK_UNIVERSE = Object.freeze(normalizeUniverseEntries(marketUniverseTop100))
const DEFAULT_UNIVERSE_LIMIT = Math.max(
  Number(arbitrageDefaultUniverseLimit || 500),
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
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 1
})
const UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 20,
  [ITEM_CATEGORIES.CASE]: 20,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 20
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
  "spread_below_min",
  "non_positive_profit",
  "ignored_extreme_spread",
  "ignored_reference_deviation",
  "ignored_missing_markets",
  "ignored_missing_liquidity_data",
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
      minPriceUsd: RISKY_MIN_PRICE_USD,
      minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: RISKY_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
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
      minPriceUsd: RISKY_MIN_PRICE_USD,
      minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: RISKY_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
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
      minPriceUsd: RISKY_MIN_PRICE_USD,
      minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: RISKY_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    })
  })
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
  const updatedAt = toIsoStringOrNull(quote?.updatedAt || quote?.fetched_at || quote?.recorded_at)
  if (!updatedAt) return null
  const ageMinutes = (Date.now() - new Date(updatedAt).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return ageMinutes
}

function resolveStaleDataPenalty(perMarket = [], opportunity = {}) {
  const bySource = toBySourceMap(perMarket)
  const buySource = String(opportunity?.buyMarket || "").trim().toLowerCase()
  const sellSource = String(opportunity?.sellMarket || "").trim().toLowerCase()
  const buyAgeMinutes = resolveQuoteAgeMinutes(bySource[buySource])
  const sellAgeMinutes = resolveQuoteAgeMinutes(bySource[sellSource])
  const maxAgeMinutes = Math.max(
    toFiniteOrNull(buyAgeMinutes) ?? 0,
    toFiniteOrNull(sellAgeMinutes) ?? 0
  )

  if (!Number.isFinite(maxAgeMinutes) || maxAgeMinutes <= 0) {
    return {
      penalty: 0,
      maxAgeMinutes: null
    }
  }

  for (const rule of STALE_PENALTY_RULES) {
    if (maxAgeMinutes >= rule.minMinutes) {
      return {
        penalty: rule.penalty,
        maxAgeMinutes: round2(maxAgeMinutes)
      }
    }
  }

  return {
    penalty: 0,
    maxAgeMinutes: round2(maxAgeMinutes)
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

function computeVolumeScore(volume7d) {
  const volume = toFiniteOrNull(volume7d)
  if (volume == null || volume <= 0) return 0
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
  const volumeScore = computeVolumeScore(volume7d)
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
      limit: UNIVERSE_DB_LIMIT
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
        category: normalizeItemCategory("", row.marketHashName),
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

  if (Boolean(inputItem?.snapshotStale)) {
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

  if (liquiditySignal?.signalType === "volume_7d") {
    volume7d = toFiniteOrNull(liquiditySignal?.signalValue)
  }
  if (liquiditySignal?.signalType === "liquidity_score") {
    liquidityScore = toFiniteOrNull(liquiditySignal?.signalValue)
  }

  return {
    volume7d,
    liquidityScore
  }
}

function passesScannerGuards(opportunity = {}, liquidity = {}) {
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)

  if (!opportunity?.isOpportunity) return false
  if (profit == null || profit <= 0) return false
  if (buyPrice == null || buyPrice < MIN_EXECUTION_PRICE_USD) return false
  if (spread == null || spread < MIN_SPREAD_PERCENT || spread > MAX_SPREAD_PERCENT) return false
  if (volume7d == null || volume7d < MIN_VOLUME_7D) return false
  if (marketCoverage < MIN_MARKET_COVERAGE) return false

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
  const hasSnapshotStale = Boolean(inputItem?.snapshotStale)

  if (profit == null || profit <= 0) {
    return { passed: false, primaryReason: "non_positive_profit", penalty: 0 }
  }
  if (buyPrice == null || buyPrice < Number(rules.minPriceUsd || profile.minPriceUsd || 0)) {
    return { passed: false, primaryReason: "ignored_execution_floor", penalty: 0 }
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
  if (marketCoverage < Number(rules.minMarketCoverage || MIN_MARKET_COVERAGE)) {
    return { passed: false, primaryReason: "ignored_missing_markets", penalty: 0 }
  }

  if (volume7d == null) {
    if (!profile.allowMissingLiquidity) {
      return { passed: false, primaryReason: "ignored_missing_liquidity_data", penalty: 0 }
    }
  } else if (volume7d < Number(rules.minVolume7d || profile.minVolume7d || 0)) {
    return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
  }

  if (profile.requireFreshData) {
    if (hasSnapshotStale) {
      return { passed: false, primaryReason: "ignored_stale_data", penalty: 0 }
    }
    if (staleMinutes != null && staleMinutes >= Number(profile.maxQuoteAgeMinutes || 0)) {
      return { passed: false, primaryReason: "ignored_stale_data", penalty: 0 }
    }
  }

  let penalty = 0
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
  if (hasSnapshotStale) {
    penalty += 12
  }
  if (staleMinutes != null && staleMinutes >= 180) {
    penalty += 18
  } else if (staleMinutes != null && staleMinutes >= 60) {
    penalty += 10
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

function downgradeConfidenceForStale(baseConfidence, stale = {}, snapshotStale = false) {
  const confidence = normalizeConfidence(baseConfidence)
  const staleMinutes = toFiniteOrNull(stale?.maxAgeMinutes) ?? 0
  if (staleMinutes >= 180) return "Low"
  if (!snapshotStale && staleMinutes < 60) return confidence
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
  const baseScore = toFiniteOrNull(opportunity?.opportunityScore) ?? 0
  const stalePenalty = toFiniteOrNull(stale?.penalty) ?? 0
  const softPenalty = toFiniteOrNull(extraPenalty) ?? 0
  const score = clampScore(baseScore - stalePenalty - softPenalty)
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const hasOutlierAdjusted = depthFlags.some(
    (flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED"
  )
  const hasMissingDepth = depthFlags.includes("MISSING_DEPTH")
  const snapshotStale = Boolean(inputItem?.snapshotStale)
  const executionConfidence = downgradeConfidenceForStale(
    opportunity?.executionConfidence,
    stale,
    snapshotStale
  )

  const badges = normalizeBadges([
    ...(Array.isArray(opportunity?.reasonBadges) ? opportunity.reasonBadges : []),
    ...(toFiniteOrNull(stale?.maxAgeMinutes) ?? 0) >= 60 ? ["Stale market data"] : [],
    hasOutlierAdjusted ? ["Outlier adjusted"] : [],
    !hasOutlierAdjusted && !hasMissingDepth ? ["Good depth"] : []
  ])

  return {
    itemId: Number(opportunity?.itemId || inputItem?.skinId || 0) || null,
    itemName: String(opportunity?.itemName || inputItem?.marketHashName || "Tracked Item"),
    itemCategory: normalizeItemCategory(
      opportunity?.itemCategory || inputItem?.itemCategory,
      opportunity?.itemName || inputItem?.marketHashName
    ),
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
    buyPrice: roundPrice(opportunity?.buyPrice || 0),
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
    volume7d: toFiniteOrNull(liquidity?.volume7d),
    marketCoverage: Number(opportunity?.marketCoverage || 0),
    referencePrice: toFiniteOrNull(opportunity?.referencePrice ?? inputItem?.referencePrice),
    stalePenalty,
    softPenalty,
    maxQuoteAgeMinutes: toFiniteOrNull(stale?.maxAgeMinutes),
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

function sortOpportunities(rows = []) {
  return [...rows].sort(
    (a, b) =>
      Number(b?.score || 0) - Number(a?.score || 0) ||
      confidenceRank(b?.executionConfidence) - confidenceRank(a?.executionConfidence) ||
      Number(b?.profit || 0) - Number(a?.profit || 0) ||
      Number(b?.spread || 0) - Number(a?.spread || 0)
  )
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
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const targetItemName = itemName || opportunity?.itemName
  const record = (reason) => {
    incrementReasonCounter(discardStats, reason, itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(rejectedByItem, targetItemName, reason, itemCategory)
    }
  }
  if (buyPrice != null && buyPrice < MIN_EXECUTION_PRICE_USD) {
    record("ignored_execution_floor")
  }
  if (volume7d == null || volume7d < MIN_VOLUME_7D) {
    record("ignored_low_liquidity")
  }
  if (spread != null && spread > MAX_SPREAD_PERCENT) {
    record("ignored_extreme_spread")
  }
  if (marketCoverage < MIN_MARKET_COVERAGE) {
    record("ignored_missing_markets")
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
  const comparePromise = marketComparisonService.compareItems(batch, {
    currency: "USD",
    pricingMode: "lowest_buy",
    allowLiveFetch: options.allowLiveFetch !== false,
    forceRefresh: Boolean(options.forceRefresh),
    userId: null,
    concurrency: MAX_CONCURRENT_MARKET_REQUESTS,
    timeoutMs: SCAN_TIMEOUT_PER_BATCH
  })

  let timeoutId = null
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      const timeoutErr = new Error(`scan_batch_timeout_${SCAN_TIMEOUT_PER_BATCH}ms`)
      timeoutErr.code = "scan_batch_timeout"
      reject(timeoutErr)
    }, SCAN_TIMEOUT_PER_BATCH)
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
  const counts = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeItemCategory(row?.itemCategory, row?.itemName)
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
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

async function runScanInternal(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  let imageEnrichmentSummary = buildDefaultImageEnrichmentSummary()
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
  const discardStats = {}
  const rejectedByItem = {}
  const scannerInputs = await loadScannerInputs(discardStats, rejectedByItem)
  const universeSeeds = Array.isArray(scannerInputs?.seeds) ? scannerInputs.seeds : []
  const snapshotWarmupSummary = scannerInputs?.snapshotWarmup || toSnapshotWarmupSummary()
  if (!universeSeeds.length) {
    const generatedTs = Date.now()
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
        discardedReasons: normalizeDiscardStats(discardStats),
        discardedReasonsByCategory: normalizeDiscardStatsByCategory(discardStats),
        topRejectedItems: toTopRejectedItems(rejectedByItem),
        rejectionReasonsByItem: toRejectionReasonsByItem(rejectedByItem),
        opportunitiesByCategory: {},
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
    scannerState.latest = emptyPayload
    return emptyPayload
  }

  const comparisonInputItems = universeSeeds.map((row) => buildInputItemForComparison(row))
  const inputByName = toByNameMap(
    universeSeeds.map((row) => ({
      ...row,
      marketHashName: normalizeMarketHashName(row?.marketHashName)
    })),
    "marketHashName"
  )

  const quoteRefreshSummary = await refreshQuotesInBatches(comparisonInputItems, {
    forceRefresh
  })
  const comparisonFromSaved = await compareFromSavedQuotes(comparisonInputItems)
  imageEnrichmentSummary = await hydrateUniverseImages(inputByName, comparisonFromSaved.items)
  const quoteSnapshotSummary = await persistQuoteSnapshot(comparisonFromSaved.items, inputByName)

  const selectedUniverse = selectTopUniverseItems(
    comparisonFromSaved?.items,
    inputByName,
    discardStats,
    rejectedByItem
  )
  const rows = []
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
    const stale = resolveStaleDataPenalty(item?.perMarket, enrichedOpportunity)

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
      continue
    }

    const riskyEvaluation = computeRiskAdjustments({
      opportunity: enrichedOpportunity,
      liquidity,
      stale,
      inputItem,
      profile: RISKY_SCAN_PROFILE
    })
    if (!riskyEvaluation.passed) {
      incrementReasonCounter(discardStats, riskyEvaluation.primaryReason, itemCategory)
      incrementItemReasonCounter(
        rejectedByItem,
        itemName,
        riskyEvaluation.primaryReason,
        itemCategory
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

    if (Number(apiRow?.score || 0) < RISKY_MIN_SCORE) {
      incrementReasonCounter(discardStats, "ignored_low_score", itemCategory)
      incrementItemReasonCounter(rejectedByItem, itemName, "ignored_low_score", itemCategory)
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
  }

  const sortedRows = sortOpportunities(rows)
  const highConfidenceCount = sortedRows.filter(
    (row) =>
      Boolean(row?.isHighConfidenceEligible) &&
      Number(row?.score || 0) >= DEFAULT_SCORE_CUTOFF &&
      String(row?.executionConfidence || "")
        .trim()
        .toLowerCase() !== "low"
  ).length
  const riskyEligibleCount = sortedRows.filter((row) => Boolean(row?.isRiskyEligible)).length
  const opportunitiesByCategory = countRowsByCategory(sortedRows)
  const discardedReasons = normalizeDiscardStats(discardStats)
  const discardedReasonsByCategory = normalizeDiscardStatsByCategory(discardStats)
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
      topRejectedItems: toTopRejectedItems(rejectedByItem),
      rejectionReasonsByItem: toRejectionReasonsByItem(rejectedByItem),
      opportunitiesByCategory,
      snapshotWarmup: snapshotWarmupSummary,
      imageEnrichment: imageEnrichmentSummary,
      sourceCatalog: sourceCatalogDiagnostics,
      scanProgress,
      highConfidence: highConfidenceCount,
      riskyEligible: riskyEligibleCount
    },
    opportunities: sortedRows,
    pipeline: {
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
      sourceCatalog: sourceCatalogDiagnostics
    }
  }

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
    topRejectedItems: scanPayload?.summary?.topRejectedItems || [],
    rejectionReasonsByItem: scanPayload?.summary?.rejectionReasonsByItem || [],
    opportunitiesByCategory: scanPayload?.summary?.opportunitiesByCategory || {},
    snapshotWarmup:
      scanPayload?.summary?.snapshotWarmup || scanPayload?.pipeline?.snapshotWarmup || {},
    imageEnrichment:
      scanPayload?.summary?.imageEnrichment || scanPayload?.pipeline?.imageEnrichment || {},
    sourceCatalog:
      scanPayload?.summary?.sourceCatalog || scanPayload?.pipeline?.sourceCatalog || {},
    scanProgress: scanPayload?.summary?.scanProgress || {},
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
      const score = Number(row?.score || 0)
      return score >= FEED_RISKY_MIN_SCORE
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
  try {
    const scanPayload = await runScanInternal({ forceRefresh })
    const persistSummary = await appendOpportunitiesToFeed(scanPayload?.opportunities || [], runRecord?.id)
    const diagnosticsSummary = toScanDiagnosticsSummary(scanPayload, persistSummary, trigger)
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
  const visibleFeedLimit = Math.max(Number(entitlements?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
  const limitedRows = safeRows.slice(0, visibleFeedLimit)

  const delayedSignals = Boolean(entitlements?.delayedSignals)
  const signalDelayMinutes = delayedSignals
    ? Math.max(Number(entitlements?.signalDelayMinutes || 0), 0)
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

  return {
    rows: delayFilteredRows,
    planLimits: {
      visibleFeedLimit,
      delayedSignals,
      signalDelayMinutes,
      advancedFilters: Boolean(entitlements?.advancedFilters),
      fullGlobalScanner: Boolean(entitlements?.fullGlobalScanner),
      fullOpportunitiesFeed: Boolean(entitlements?.fullOpportunitiesFeed),
      feedTruncatedByLimit: Math.max(safeRows.length - limitedRows.length, 0),
      feedTruncatedByDelay: Math.max(limitedRows.length - delayFilteredRows.length, 0)
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

  const intervalMinutes = Math.max(
    Number(entitlements?.scannerRefreshIntervalMinutes || SCANNER_INTERVAL_MINUTES),
    1
  )
  const cooldownMs = intervalMinutes * 60 * 1000
  const previous = manualRefreshTracker.get(safeUserId)
  const lastTriggeredAtMs = Number(previous?.lastTriggeredAtMs || 0)

  if (lastTriggeredAtMs > 0 && nowMs - lastTriggeredAtMs < cooldownMs) {
    const retryAfterMs = cooldownMs - (nowMs - lastTriggeredAtMs)
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
    spread_below_min: "Most candidates were below spread baseline.",
    non_positive_profit: "Most candidates had non-positive projected profit.",
    ignored_extreme_spread: "Most candidates were rejected for extreme spread values.",
    ignored_reference_deviation: "Most candidates failed reference deviation checks.",
    ignored_missing_markets: "Most candidates were missing enough market coverage.",
    ignored_missing_liquidity_data: "Most candidates were missing liquidity data.",
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
  const entitlements = planContext?.entitlements || planService.getEntitlements(planContext?.planTier)
  const advancedFiltersEnabled = Boolean(entitlements?.advancedFilters)
  const requestedLimit = normalizeLimit(options.limit, DEFAULT_API_LIMIT, MAX_FEED_LIMIT)
  const planVisibleLimit = Math.max(Number(entitlements?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
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
    topRejectedItems: diagnosticsSummary?.topRejectedItems || [],
    rejectionReasonsByItem: diagnosticsSummary?.rejectionReasonsByItem || [],
    opportunitiesByCategory: diagnosticsSummary?.opportunitiesByCategory || {},
    snapshotWarmup:
      diagnosticsSummary?.snapshotWarmup || diagnosticsSummary?.pipeline?.snapshotWarmup || {},
    imageEnrichment:
      diagnosticsSummary?.imageEnrichment || diagnosticsSummary?.pipeline?.imageEnrichment || {},
    sourceCatalog:
      diagnosticsSummary?.sourceCatalog || diagnosticsSummary?.pipeline?.sourceCatalog || {},
    scanProgress: diagnosticsSummary?.scanProgress || {},
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
        planContext?.entitlements?.scannerRefreshIntervalMinutes || SCANNER_INTERVAL_MINUTES
      )
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

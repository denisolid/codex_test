const { round2, roundPrice } = require("../markets/marketUtils")
const {
  arbitrageScannerIntervalMinutes,
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
const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const marketComparisonService = require("./marketComparisonService")
const arbitrageEngine = require("./arbitrageEngineService")

const SCANNER_TYPE = "global_arbitrage"
const SCANNER_INTERVAL_MINUTES = Math.max(Number(arbitrageScannerIntervalMinutes || 30), 1)
const SCANNER_INTERVAL_MS = SCANNER_INTERVAL_MINUTES * 60 * 1000
const CACHE_TTL_MS = SCANNER_INTERVAL_MS
const SCANNER_OVERDUE_GRACE_MS = Math.max(Math.round(SCANNER_INTERVAL_MS * 0.2), 15 * 1000)
const MIN_SPREAD_PERCENT = Number(arbitrageEngine.MIN_SPREAD_PERCENT || 5)
const MAX_SPREAD_PERCENT = Number(arbitrageEngine.SPREAD_SANITY_MAX_PERCENT || 300)
const MIN_VOLUME_7D = 100
const MIN_EXECUTION_PRICE_USD = Number(arbitrageEngine.MIN_EXECUTION_PRICE_USD || 3)
const MIN_MARKET_COVERAGE = 2
const DEFAULT_SCORE_CUTOFF = Number(arbitrageEngine.DEFAULT_SCORE_CUTOFF || 75)
const RISKY_SCORE_CUTOFF = Number(arbitrageEngine.RISKY_SCORE_CUTOFF || 60)
const MAX_API_LIMIT = 200
const DEFAULT_API_LIMIT = 100
const MAX_FEED_LIMIT = 500
const RISKY_MIN_PRICE_USD = 2
const RISKY_MIN_SPREAD_PERCENT = 3
const RISKY_MIN_VOLUME_7D = 30
const RECENT_SNAPSHOT_FETCH_LIMIT = 25000
const FEED_RETENTION_HOURS = Math.max(Number(arbitrageFeedRetentionHours || 24), 1)
const FEED_ACTIVE_LIMIT = Math.max(Number(arbitrageFeedActiveLimit || 500), 50)
const DUPLICATE_WINDOW_HOURS = Math.max(Number(arbitrageDuplicateWindowHours || 4), 1)
const MIN_PROFIT_CHANGE_PCT = Math.max(Number(arbitrageMinProfitChangePct || 10), 0)
const MIN_SCORE_CHANGE = Math.max(Number(arbitrageMinScoreChange || 8), 0)
const INSERT_DUPLICATES = Boolean(arbitrageInsertDuplicates)

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
const UNIVERSE_TARGET_SIZE = Math.max(Math.min(FALLBACK_UNIVERSE.length || 100, 100), 20)
const PRE_COMPARE_UNIVERSE_LIMIT = Math.max(UNIVERSE_TARGET_SIZE * 2, 120)
const LOW_VALUE_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i
])

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
  minPriceUsd: MIN_EXECUTION_PRICE_USD,
  minSpreadPercent: MIN_SPREAD_PERCENT,
  minVolume7d: MIN_VOLUME_7D,
  allowMissingLiquidity: false,
  requireFreshData: true,
  maxQuoteAgeMinutes: 60
})

const RISKY_SCAN_PROFILE = Object.freeze({
  name: "risky",
  minPriceUsd: RISKY_MIN_PRICE_USD,
  minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
  minVolume7d: RISKY_MIN_VOLUME_7D,
  allowMissingLiquidity: true,
  requireFreshData: false,
  maxQuoteAgeMinutes: Infinity
})

const CATEGORY_SCAN_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: MIN_EXECUTION_PRICE_USD,
      minSpreadPercent: MIN_SPREAD_PERCENT,
      maxSpreadPercent: MAX_SPREAD_PERCENT,
      minVolume7d: MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: RISKY_MIN_PRICE_USD,
      minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
      maxSpreadPercent: MAX_SPREAD_PERCENT,
      minVolume7d: RISKY_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    })
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: 0.5,
      minSpreadPercent: 3,
      maxSpreadPercent: 150,
      minVolume7d: 50,
      minMarketCoverage: 2
    }),
    risky: Object.freeze({
      minPriceUsd: 0.5,
      minSpreadPercent: 3,
      maxSpreadPercent: 150,
      minVolume7d: 50,
      minMarketCoverage: 2
    })
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: 0.75,
      minSpreadPercent: 3,
      maxSpreadPercent: 150,
      minVolume7d: 40,
      minMarketCoverage: 2
    }),
    risky: Object.freeze({
      minPriceUsd: 0.75,
      minSpreadPercent: 3,
      maxSpreadPercent: 150,
      minVolume7d: 40,
      minMarketCoverage: 2
    })
  })
})

function normalizeMarketHashName(value) {
  return String(value || "").trim()
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

function incrementReasonCounter(counter = {}, reason, category = "") {
  const key = String(reason || "").trim()
  if (!key) return
  counter[key] = Number(counter[key] || 0) + 1
  const normalizedCategory = normalizeItemCategory(category)
  const scopedKey = `${key}__${normalizedCategory}`
  counter[scopedKey] = Number(counter[scopedKey] || 0) + 1
}

function incrementItemReasonCounter(rejectionsByItem = {}, itemName, reason, category = "") {
  const normalizedItem = String(itemName || "").trim() || "Unknown item"
  const normalizedReason = String(reason || "").trim() || "unknown"
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
        mainReasonCount: Number(mainReasonCount || 0)
      }
    })
    .sort(
      (a, b) =>
        Number(b.rejectedCount || 0) - Number(a.rejectedCount || 0) ||
        Number(b.mainReasonCount || 0) - Number(a.mainReasonCount || 0)
    )
    .slice(0, Math.max(Number(limit || 0), 0))
}

function buildInputItemFromSkinAndSnapshot({
  skin = null,
  snapshot = null,
  marketHashName = "",
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  subcategory = null
} = {}) {
  const normalizedName = normalizeMarketHashName(
    marketHashName || skin?.market_hash_name || skin?.marketHashName
  )
  if (!normalizedName) return null

  const referencePrice =
    toFiniteOrNull(snapshot?.average_7d_price) ??
    toFiniteOrNull(snapshot?.lowest_listing_price) ??
    null
  const volume7d = resolveVolume7d(snapshot || {})
  const liquidityScore = snapshot ? computeLiquidityScoreFromSnapshot(snapshot) : null
  const sevenDayChangePercent = snapshot ? resolveSevenDayChangePercent(snapshot) : null

  return {
    skinId: Number(skin?.id || 0) || null,
    marketHashName: normalizedName,
    itemCategory: normalizeItemCategory(category, normalizedName),
    itemSubcategory: String(subcategory || "").trim() || null,
    itemImageUrl: String(skin?.image_url || skin?.imageUrl || "").trim() || null,
    itemImageUrlLarge:
      String(skin?.image_url_large || skin?.imageUrlLarge || "").trim() || null,
    quantity: 1,
    marketVolume7d: volume7d,
    liquidityScore,
    sevenDayChangePercent,
    referencePrice,
    hasSnapshotData: Boolean(snapshot),
    snapshotCapturedAt: snapshot?.captured_at || null,
    snapshotStale: snapshot ? isSnapshotStale(snapshot) : false
  }
}

async function loadDynamicUniverseSeeds() {
  const recentSnapshots = await marketSnapshotRepo.getRecentSnapshots({
    limit: RECENT_SNAPSHOT_FETCH_LIMIT
  })
  const latestBySkinId = {}
  for (const row of Array.isArray(recentSnapshots) ? recentSnapshots : []) {
    const skinId = Number(row?.skin_id || 0)
    if (!Number.isInteger(skinId) || skinId <= 0) continue
    if (!latestBySkinId[skinId]) {
      latestBySkinId[skinId] = row
    }
  }

  const skinIds = Object.keys(latestBySkinId).map((value) => Number(value))
  if (!skinIds.length) return []
  const skins = await skinRepo.getByIds(skinIds)
  const skinsById = {}
  for (const row of Array.isArray(skins) ? skins : []) {
    const skinId = Number(row?.id || 0)
    if (!Number.isInteger(skinId) || skinId <= 0) continue
    skinsById[skinId] = row
  }

  const seeds = []
  for (const skinId of skinIds) {
    const skin = skinsById[skinId]
    if (!skin) continue
    const input = buildInputItemFromSkinAndSnapshot({
      skin,
      snapshot: latestBySkinId[skinId]
    })
    if (input) seeds.push(input)
  }
  return seeds
}

async function loadFallbackUniverseSeeds() {
  const marketNames = FALLBACK_UNIVERSE.map((entry) => entry.marketHashName)
  const skins = await skinRepo.getByMarketHashNames(marketNames)
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

  return FALLBACK_UNIVERSE.map((entry) =>
    buildInputItemFromSkinAndSnapshot({
      skin: skinsByName[entry.marketHashName] || null,
      snapshot:
        Number(skinsByName[entry.marketHashName]?.id || 0) > 0
          ? snapshotsBySkinId[Number(skinsByName[entry.marketHashName].id)] || null
          : null,
      marketHashName: entry.marketHashName,
      category: entry.category,
      subcategory: entry.subcategory
    })
  ).filter(Boolean)
}

function passesUniverseSeedFilters(inputItem = {}, discardStats = {}, rejectedByItem = null) {
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
  const referencePrice = toFiniteOrNull(inputItem?.referencePrice)
  const seedRules = getCategoryScanRules(itemCategory, "risky")
  if (referencePrice != null && referencePrice < Number(seedRules.minPriceUsd || RISKY_MIN_PRICE_USD)) {
    incrementReasonCounter(discardStats, "ignored_low_price", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(rejectedByItem, marketHashName, "ignored_low_price", itemCategory)
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
    return { passed: false, primaryReason: "ignored_low_price", penalty: 0 }
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
      return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
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
    itemImageUrl:
      String(inputItem?.itemImageUrlLarge || inputItem?.itemImageUrl || "").trim() || null,
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

function toGradeFromScore(score) {
  const safeScore = clampScore(score)
  if (safeScore >= 90) return "A"
  if (safeScore >= 75) return "B"
  if (safeScore >= 60) return "C"
  return "RISKY"
}

function toMetadataObject(row = {}) {
  return {
    item_id: Number(row?.itemId || 0) || null,
    item_subcategory: String(row?.itemSubcategory || "").trim() || null,
    item_image_url: String(row?.itemImageUrl || "").trim() || null,
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
    quality_grade: toGradeFromScore(score),
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
    itemImageUrl: String(metadata?.item_image_url || "").trim() || null,
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
    liquidityBand: String(row?.liquidity_label || "Low").trim() || "Low",
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
    imageUrl: String(item?.itemImageUrl || "").trim() || null,
    imageUrlLarge: String(item?.itemImageUrlLarge || "").trim() || null,
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
    snapshotStale: Boolean(item?.snapshotStale)
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
    record("ignored_low_price")
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

async function loadScannerInputs(discardStats = {}, rejectedByItem = {}) {
  const fallbackSeeds = await loadFallbackUniverseSeeds()
  const fallbackFiltered = fallbackSeeds.filter((row) =>
    passesUniverseSeedFilters(row, discardStats, rejectedByItem)
  )

  const ranked = fallbackFiltered
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
        Number(b.marketVolume7d || 0) - Number(a.marketVolume7d || 0)
    )
    .slice(0, PRE_COMPARE_UNIVERSE_LIMIT)

  return ranked
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
      (a, b) =>
        Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
        Number(b.inputItem?.marketVolume7d || 0) - Number(a.inputItem?.marketVolume7d || 0)
    )
    .slice(0, UNIVERSE_TARGET_SIZE)
}

async function runScanInternal(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const discardStats = {}
  const rejectedByItem = {}
  const universeSeeds = await loadScannerInputs(discardStats, rejectedByItem)
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
        candidateItems: 0,
        discardedReasons: discardStats,
        topRejectedItems: toTopRejectedItems(rejectedByItem)
      },
      opportunities: []
    }
    scannerState.latest = emptyPayload
    return emptyPayload
  }

  const comparisonInputItems = universeSeeds.map((row) => buildInputItemForComparison(row))
  const comparison = await marketComparisonService.compareItems(comparisonInputItems, {
    currency: "USD",
    pricingMode: "lowest_buy",
    allowLiveFetch: true,
    forceRefresh,
    userId: null
  })

  const inputByName = toByNameMap(
    universeSeeds.map((row) => ({
      ...row,
      marketHashName: normalizeMarketHashName(row?.marketHashName)
    })),
    "marketHashName"
  )

  const selectedUniverse = selectTopUniverseItems(
    comparison?.items,
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

    rows.push(
      buildApiOpportunityRow({
        opportunity: enrichedOpportunity,
        inputItem,
        liquidity,
        stale,
        perMarket: item?.perMarket,
        extraPenalty: riskyEvaluation.penalty,
        isRiskyEligible: true,
        isHighConfidenceEligible: strictEvaluation.passed
      })
    )
  }

  const sortedRows = sortOpportunities(rows)
  const generatedTs = Date.now()
  const payload = {
    generatedAt: new Date(generatedTs).toISOString(),
    expiresAt: generatedTs + CACHE_TTL_MS,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: String(comparison?.currency || "USD")
      .trim()
      .toUpperCase(),
    summary: {
      scannedItems: selectedUniverse.length,
      opportunities: sortedRows.filter(
        (row) =>
          Boolean(row?.isHighConfidenceEligible) &&
          Number(row?.score || 0) >= DEFAULT_SCORE_CUTOFF &&
          String(row?.executionConfidence || "")
            .trim()
            .toLowerCase() !== "low"
      ).length,
      totalDetected: sortedRows.length,
      universeSize: selectedUniverse.length,
      candidateItems: universeSeeds.length,
      discardedReasons: discardStats,
      topRejectedItems: toTopRejectedItems(rejectedByItem)
    },
    opportunities: sortedRows
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
    candidateItems: Number(scanPayload?.summary?.candidateItems || 0),
    discardedReasons: scanPayload?.summary?.discardedReasons || {},
    topRejectedItems: scanPayload?.summary?.topRejectedItems || [],
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
      if (!Boolean(row?.isRiskyEligible)) return false
      const score = Number(row?.score || 0)
      return score >= RISKY_SCORE_CUTOFF
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

  return {
    scannerType: SCANNER_TYPE,
    intervalMinutes: SCANNER_INTERVAL_MINUTES,
    schedulerRunning: Boolean(scannerState.timer),
    currentStatus: scannerState.inFlight ? "running" : "idle",
    currentRunId: scannerState.inFlightRunId,
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

exports.getFeed = async (options = {}) => {
  const limit = normalizeLimit(options.limit, DEFAULT_API_LIMIT, MAX_FEED_LIMIT)
  const showRisky = normalizeBoolean(options.showRisky)
  const includeOlder = normalizeBoolean(options.includeOlder || options.showOlder)
  const forceRefresh = normalizeBoolean(options.forceRefresh || options.force)
  const categoryFilter = normalizeCategoryFilter(options.category)

  if (forceRefresh) {
    await enqueueScan({ forceRefresh: true, trigger: "manual" }).catch((err) => {
      console.error("[arbitrage-scanner] Failed to enqueue manual refresh", err.message)
    })
  } else {
    await ensureScheduledScanHeartbeat(null, "feed_watchdog").catch((err) => {
      console.error("[arbitrage-scanner] Failed to enqueue watchdog scan", err.message)
    })
  }

  const minScore = showRisky ? RISKY_SCORE_CUTOFF : DEFAULT_SCORE_CUTOFF
  const excludeLowConfidence = !showRisky

  const [feedRows, totalCount, activeCount, status] = await Promise.all([
    arbitrageFeedRepo.listFeed({
      limit,
      includeInactive: includeOlder,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence
    }),
    arbitrageFeedRepo.countFeed({
      includeInactive: includeOlder,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence
    }),
    arbitrageFeedRepo.countFeed({
      includeInactive: false,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence
    }),
    getScannerStatusInternal()
  ])

  const mappedRows = (Array.isArray(feedRows) ? feedRows : []).map((row) => mapFeedRowToApiRow(row))
  const latestCompleted = status?.latestCompletedRun || null
  const diagnosticsSummary =
    latestCompleted?.diagnostics_summary && typeof latestCompleted.diagnostics_summary === "object"
      ? latestCompleted.diagnostics_summary
      : {}

  return {
    generatedAt: latestCompleted?.completed_at || latestCompleted?.started_at || null,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: "USD",
    summary: {
      scannedItems:
        Number(diagnosticsSummary?.scannedItems || 0) || Number(latestCompleted?.items_scanned || 0),
      opportunities: mappedRows.length,
      totalDetected: Number(totalCount || mappedRows.length),
      activeOpportunities: Number(activeCount || 0),
      universeSize: Number(diagnosticsSummary?.universeSize || 0),
      candidateItems: Number(diagnosticsSummary?.candidateItems || 0),
      discardedReasons: diagnosticsSummary?.discardedReasons || {},
      topRejectedItems: diagnosticsSummary?.topRejectedItems || [],
      newOpportunitiesAdded:
        Number(latestCompleted?.new_opportunities_added || diagnosticsSummary?.persisted?.insertedCount || 0),
      feedRetentionHours: FEED_RETENTION_HOURS,
      feedActiveLimit: FEED_ACTIVE_LIMIT
    },
    opportunities: mappedRows,
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
  const forceRefresh = options.forceRefresh == null ? true : normalizeBoolean(options.forceRefresh)
  const enqueue = await enqueueScan({
    forceRefresh,
    trigger: String(options.trigger || "manual")
  })
  return {
    scanRunId: enqueue.scanRunId || null,
    alreadyRunning: Boolean(enqueue.alreadyRunning),
    startedAt: new Date().toISOString()
  }
}

exports.getStatus = async () => {
  let status = await getScannerStatusInternal()
  await ensureScheduledScanHeartbeat(status, "status_watchdog").catch((err) => {
    console.error("[arbitrage-scanner] Failed to enqueue status watchdog scan", err.message)
  })
  status = await getScannerStatusInternal()
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
  isLowValueJunkName
}

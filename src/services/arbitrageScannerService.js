const { round2, roundPrice } = require("../markets/marketUtils")
const {
  arbitrageScannerIntervalMinutes,
  marketSnapshotTtlMinutes
} = require("../config/env")
const marketUniverseTop100 = require("../config/marketUniverseTop100.json")
const skinRepo = require("../repositories/skinRepository")
const marketSnapshotRepo = require("../repositories/marketSnapshotRepository")
const marketComparisonService = require("./marketComparisonService")
const arbitrageEngine = require("./arbitrageEngineService")

const SCANNER_INTERVAL_MINUTES = Math.max(Number(arbitrageScannerIntervalMinutes || 5), 1)
const CACHE_TTL_MS = SCANNER_INTERVAL_MINUTES * 60 * 1000
const MIN_SPREAD_PERCENT = Number(arbitrageEngine.MIN_SPREAD_PERCENT || 5)
const MAX_SPREAD_PERCENT = Number(arbitrageEngine.SPREAD_SANITY_MAX_PERCENT || 300)
const MIN_VOLUME_7D = 100
const MIN_EXECUTION_PRICE_USD = Number(arbitrageEngine.MIN_EXECUTION_PRICE_USD || 3)
const MIN_MARKET_COVERAGE = 2
const DEFAULT_SCORE_CUTOFF = Number(arbitrageEngine.DEFAULT_SCORE_CUTOFF || 75)
const RISKY_SCORE_CUTOFF = Number(arbitrageEngine.RISKY_SCORE_CUTOFF || 60)
const MAX_API_LIMIT = 200
const DEFAULT_API_LIMIT = 100
const UNIVERSE_TARGET_SIZE = 100
const PRE_COMPARE_UNIVERSE_LIMIT = 350
const RECENT_SNAPSHOT_FETCH_LIMIT = 25000

const STALE_PENALTY_RULES = Object.freeze([
  { minMinutes: 180, penalty: 25 },
  { minMinutes: 60, penalty: 15 },
  { minMinutes: 15, penalty: 8 }
])

const SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"])
const FALLBACK_UNIVERSE = Object.freeze(normalizeUniverseNames(marketUniverseTop100))
const LOW_VALUE_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i
])

function normalizeMarketHashName(value) {
  return String(value || "").trim()
}

function normalizeUniverseNames(items = []) {
  return Array.from(
    new Set((Array.isArray(items) ? items : []).map(normalizeMarketHashName).filter(Boolean))
  )
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

function computeReferencePriceScore(referencePrice) {
  const price = toFiniteOrNull(referencePrice)
  if (price == null || price < MIN_EXECUTION_PRICE_USD) return 0
  if (price >= 30) return 100
  if (price >= 10) return 85
  return 65
}

function computeLiquidityRank({
  volume7d = null,
  marketCoverage = 0,
  sevenDayChangePercent = null,
  referencePrice = null
} = {}) {
  const volumeScore = computeVolumeScore(volume7d)
  const marketCoverageScore = computeMarketCoverageScore(marketCoverage)
  const priceStabilityScore = computePriceStabilityScore(sevenDayChangePercent)
  const referencePriceScore = computeReferencePriceScore(referencePrice)
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

function incrementReasonCounter(counter = {}, reason) {
  const key = String(reason || "").trim()
  if (!key) return
  counter[key] = Number(counter[key] || 0) + 1
}

function buildInputItemFromSkinAndSnapshot({
  skin = null,
  snapshot = null,
  marketHashName = ""
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
    quantity: 1,
    marketVolume7d: volume7d,
    liquidityScore,
    sevenDayChangePercent,
    referencePrice,
    snapshotCapturedAt: snapshot?.captured_at || null,
    snapshotStale: snapshot ? isSnapshotStale(snapshot) : true
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
  const skins = await skinRepo.getByMarketHashNames(FALLBACK_UNIVERSE)
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

  return FALLBACK_UNIVERSE.map((marketHashName) =>
    buildInputItemFromSkinAndSnapshot({
      skin: skinsByName[marketHashName] || null,
      snapshot:
        Number(skinsByName[marketHashName]?.id || 0) > 0
          ? snapshotsBySkinId[Number(skinsByName[marketHashName].id)] || null
          : null,
      marketHashName
    })
  ).filter(Boolean)
}

function passesUniverseSeedFilters(inputItem = {}, discardStats = {}) {
  const marketHashName = String(inputItem?.marketHashName || "").trim()
  if (!marketHashName) return false
  if (isLowValueJunkName(marketHashName)) {
    incrementReasonCounter(discardStats, "ignored_low_value_universe")
    return false
  }
  if (inputItem?.snapshotStale) {
    incrementReasonCounter(discardStats, "ignored_stale_data")
    return false
  }
  const referencePrice = toFiniteOrNull(inputItem?.referencePrice)
  if (referencePrice != null && referencePrice < MIN_EXECUTION_PRICE_USD) {
    incrementReasonCounter(discardStats, "ignored_low_price")
    return false
  }
  const volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
  if (volume7d != null && volume7d < MIN_VOLUME_7D) {
    incrementReasonCounter(discardStats, "ignored_low_liquidity")
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
  perMarket = []
}) {
  const bySource = toBySourceMap(perMarket)
  const buySource = normalizeMarketLabel(opportunity?.buyMarket)
  const sellSource = normalizeMarketLabel(opportunity?.sellMarket)
  const buyQuote = bySource[buySource] || null
  const sellQuote = bySource[sellSource] || null
  const baseScore = toFiniteOrNull(opportunity?.opportunityScore) ?? 0
  const stalePenalty = toFiniteOrNull(stale?.penalty) ?? 0
  const score = clampScore(baseScore - stalePenalty)
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
    rawOpportunityScore: baseScore
  }
}

function confidenceRank(value) {
  const safe = normalizeConfidence(value)
  if (safe === "High") return 3
  if (safe === "Medium") return 2
  return 1
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

function collectDiscardReasonsFromOpportunity(opportunity = {}, discardStats = {}) {
  const reasons = Array.isArray(opportunity?.antiFake?.reasons) ? opportunity.antiFake.reasons : []
  for (const reason of reasons) {
    incrementReasonCounter(discardStats, reason)
  }

  const debugReasons = Array.isArray(opportunity?.antiFake?.debugReasons)
    ? opportunity.antiFake.debugReasons
    : []
  for (const reason of debugReasons) {
    incrementReasonCounter(discardStats, reason)
  }
}

function applyGuardFallbackReason(opportunity = {}, liquidity = {}, discardStats = {}) {
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  if (buyPrice != null && buyPrice < MIN_EXECUTION_PRICE_USD) {
    incrementReasonCounter(discardStats, "ignored_low_price")
  }
  if (volume7d == null || volume7d < MIN_VOLUME_7D) {
    incrementReasonCounter(discardStats, "ignored_low_liquidity")
  }
  if (spread != null && spread > MAX_SPREAD_PERCENT) {
    incrementReasonCounter(discardStats, "ignored_extreme_spread")
  }
  if (marketCoverage < MIN_MARKET_COVERAGE) {
    incrementReasonCounter(discardStats, "ignored_missing_markets")
  }
}

const scannerState = {
  latest: null,
  inFlight: null,
  timer: null,
  lastError: null
}

async function loadScannerInputs(discardStats = {}) {
  const dynamicSeeds = await loadDynamicUniverseSeeds()
  const baseSeeds = dynamicSeeds.length ? dynamicSeeds : await loadFallbackUniverseSeeds()

  const filtered = baseSeeds.filter((row) => passesUniverseSeedFilters(row, discardStats))
  const ranked = filtered
    .map((row) => ({
      ...row,
      ...computeLiquidityRank({
        volume7d: row.marketVolume7d,
        marketCoverage: 2,
        sevenDayChangePercent: row.sevenDayChangePercent,
        referencePrice: row.referencePrice
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

function selectTopUniverseItems(comparisonItems = [], inputByName = {}, discardStats = {}) {
  const ranked = []
  for (const comparisonItem of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const name = normalizeMarketHashName(comparisonItem?.marketHashName)
    const inputItem = inputByName[name] || null
    if (!inputItem) continue
    const marketCoverage = countAvailableMarkets(comparisonItem?.perMarket)
    if (marketCoverage < MIN_MARKET_COVERAGE) {
      incrementReasonCounter(discardStats, "ignored_missing_markets")
      continue
    }
    const rank = computeLiquidityRank({
      volume7d: inputItem.marketVolume7d,
      marketCoverage,
      sevenDayChangePercent: inputItem.sevenDayChangePercent,
      referencePrice: inputItem.referencePrice
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
  const universeSeeds = await loadScannerInputs(discardStats)
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
        discardedReasons: discardStats
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

  const selectedUniverse = selectTopUniverseItems(comparison?.items, inputByName, discardStats)
  const rows = []
  for (const selected of selectedUniverse) {
    const item = selected?.comparisonItem || null
    const inputItem = selected?.inputItem || null
    if (!item || !inputItem) continue
    const opportunity = item?.arbitrage || null
    if (!opportunity) {
      incrementReasonCounter(discardStats, "insufficient_market_data")
      continue
    }

    const enrichedOpportunity = {
      ...opportunity,
      marketCoverage: selected.marketCoverage
    }
    collectDiscardReasonsFromOpportunity(enrichedOpportunity, discardStats)
    const liquidity = resolveLiquidityMetrics(enrichedOpportunity, inputItem)
    if (!passesScannerGuards(enrichedOpportunity, liquidity)) {
      applyGuardFallbackReason(enrichedOpportunity, liquidity, discardStats)
      continue
    }
    const stale = resolveStaleDataPenalty(item?.perMarket, enrichedOpportunity)
    rows.push(
      buildApiOpportunityRow({
        opportunity: enrichedOpportunity,
        inputItem,
        liquidity,
        stale,
        perMarket: item?.perMarket
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
      opportunities: sortedRows.filter((row) => Number(row?.score || 0) >= DEFAULT_SCORE_CUTOFF).length,
      totalDetected: sortedRows.length,
      universeSize: selectedUniverse.length,
      candidateItems: universeSeeds.length,
      discardedReasons: discardStats
    },
    opportunities: sortedRows
  }

  scannerState.latest = payload
  scannerState.lastError = null
  return payload
}

async function runScan(options = {}) {
  if (scannerState.inFlight) {
    return scannerState.inFlight
  }

  scannerState.inFlight = runScanInternal(options)
    .catch((err) => {
      scannerState.lastError = err
      if (scannerState.latest) {
        return scannerState.latest
      }
      throw err
    })
    .finally(() => {
      scannerState.inFlight = null
    })

  return scannerState.inFlight
}

function normalizeLimit(value, fallback = DEFAULT_API_LIMIT) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), MAX_API_LIMIT)
}

async function ensureFreshCache() {
  if (!scannerState.latest) {
    return runScan({ forceRefresh: true })
  }

  const expired = Date.now() >= Number(scannerState.latest?.expiresAt || 0)
  if (expired && !scannerState.inFlight) {
    runScan({ forceRefresh: true }).catch((err) => {
      console.error("[arbitrage-scanner] Background refresh failed", err.message)
    })
  }

  return scannerState.latest
}

exports.getTopOpportunities = async (options = {}) => {
  const limit = normalizeLimit(options.limit, 50)
  const showRisky = normalizeBoolean(options.showRisky)
  const minScore = showRisky ? RISKY_SCORE_CUTOFF : DEFAULT_SCORE_CUTOFF
  const cache = await ensureFreshCache()
  const safeCache = cache || {
    generatedAt: null,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: "USD",
    summary: {
      scannedItems: 0,
      opportunities: 0,
      totalDetected: 0,
      universeSize: 0,
      candidateItems: 0,
      discardedReasons: {}
    },
    opportunities: []
  }

  const allRows = Array.isArray(safeCache.opportunities) ? safeCache.opportunities : []
  const filtered = allRows.filter((row) => {
    if (showRisky) return true
    return (
      Number(row?.score || 0) >= minScore &&
      String(row?.executionConfidence || "")
        .trim()
        .toLowerCase() !== "low"
    )
  })

  return {
    generatedAt: safeCache.generatedAt,
    ttlSeconds: safeCache.ttlSeconds,
    currency: safeCache.currency,
    summary: {
      ...(safeCache.summary || {}),
      opportunities: filtered.length,
      totalDetected:
        Number(safeCache.summary?.totalDetected || 0) || allRows.length
    },
    opportunities: filtered.slice(0, limit)
  }
}

exports.startScheduler = () => {
  if (scannerState.timer) {
    return
  }

  runScan({ forceRefresh: true }).catch((err) => {
    console.error("[arbitrage-scanner] Initial scan failed", err.message)
  })

  const intervalMs = SCANNER_INTERVAL_MINUTES * 60 * 1000
  scannerState.timer = setInterval(() => {
    runScan({ forceRefresh: true }).catch((err) => {
      console.error("[arbitrage-scanner] Scheduled scan failed", err.message)
    })
  }, intervalMs)
  scannerState.timer.unref?.()

  console.log(`[arbitrage-scanner] Scheduler started (every ${SCANNER_INTERVAL_MINUTES} minute(s))`)
}

exports.stopScheduler = () => {
  if (!scannerState.timer) return
  clearInterval(scannerState.timer)
  scannerState.timer = null
}

exports.forceRefresh = async () => runScan({ forceRefresh: true })

exports.__testables = {
  normalizeUniverseNames,
  computeLiquidityScoreFromSnapshot,
  resolveVolume7d,
  resolveStaleDataPenalty,
  resolveLiquidityMetrics,
  passesScannerGuards,
  buildApiOpportunityRow,
  clampScore,
  computeLiquidityRank,
  countAvailableMarkets,
  isLowValueJunkName
}

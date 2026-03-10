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

const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule"
})

const DEFAULT_UNIVERSE_TARGET = Math.max(
  Number(arbitrageScannerUniverseTargetSize || arbitrageDefaultUniverseLimit || 500),
  100
)
const SOURCE_CATALOG_LIMIT = Math.max(Number(arbitrageSourceCatalogLimit || 1000), 600)
const SOURCE_CATALOG_REFRESH_MS =
  Math.max(Number(arbitrageSourceCatalogRefreshMinutes || 60), 5) * 60 * 1000
const SNAPSHOT_TTL_MS = Math.max(Number(marketSnapshotTtlMinutes || 30), 5) * 60 * 1000

const SOURCE_QUALITY_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    minReferencePrice: 2,
    minVolume7d: 45,
    minMarketCoverage: 2
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    minReferencePrice: 1,
    minVolume7d: 70,
    minMarketCoverage: 2
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    minReferencePrice: 1,
    minVolume7d: 35,
    minMarketCoverage: 2
  })
})

const CATEGORY_QUOTA_TARGETS = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 320,
  [ITEM_CATEGORIES.CASE]: 120,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 60
})

const CATEGORY_PRIORITY = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE
])

const CATEGORY_QUOTA_BASE_TOTAL = Object.values(CATEGORY_QUOTA_TARGETS).reduce(
  (sum, value) => sum + Number(value || 0),
  0
)

const sourceCatalogState = {
  inFlight: null,
  lastPreparedAt: 0,
  lastDiagnostics: null
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

function normalizeCategory(value, marketHashName = "") {
  const text = normalizeText(value).toLowerCase()
  if (text === ITEM_CATEGORIES.WEAPON_SKIN || text === ITEM_CATEGORIES.CASE || text === ITEM_CATEGORIES.STICKER_CAPSULE) {
    return text
  }

  const name = normalizeText(marketHashName).toLowerCase()
  if (name.endsWith(" case")) return ITEM_CATEGORIES.CASE
  if (name.includes("sticker capsule")) return ITEM_CATEGORIES.STICKER_CAPSULE
  return ITEM_CATEGORIES.WEAPON_SKIN
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

function buildCategoryQuotas(targetSize) {
  const safeTarget = Math.max(Math.round(Number(targetSize || DEFAULT_UNIVERSE_TARGET)), 1)
  const quotas = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: 0,
    [ITEM_CATEGORIES.CASE]: 0,
    [ITEM_CATEGORIES.STICKER_CAPSULE]: 0
  }

  if (!CATEGORY_QUOTA_BASE_TOTAL) {
    quotas[ITEM_CATEGORIES.WEAPON_SKIN] = safeTarget
    return quotas
  }

  const bucketStats = CATEGORY_PRIORITY.map((category) => {
    const baseTarget = Number(CATEGORY_QUOTA_TARGETS[category] || 0)
    const exact = (baseTarget * safeTarget) / CATEGORY_QUOTA_BASE_TOTAL
    const floor = Math.max(Math.floor(exact), 0)
    quotas[category] = floor
    return {
      category,
      baseTarget,
      remainder: exact - floor
    }
  })

  let remainderSlots = safeTarget - Object.values(quotas).reduce((sum, value) => sum + value, 0)
  if (remainderSlots > 0) {
    const byRemainder = bucketStats
      .slice()
      .sort((a, b) => b.remainder - a.remainder || b.baseTarget - a.baseTarget)
    let index = 0
    while (remainderSlots > 0) {
      const bucket = byRemainder[index % byRemainder.length]
      quotas[bucket.category] += 1
      remainderSlots -= 1
      index += 1
    }
  }

  if (safeTarget >= CATEGORY_PRIORITY.length) {
    for (const category of CATEGORY_PRIORITY) {
      if (quotas[category] > 0) continue
      const donor = CATEGORY_PRIORITY.slice()
        .sort((a, b) => quotas[b] - quotas[a])
        .find((candidate) => quotas[candidate] > 1)
      if (!donor) break
      quotas[donor] -= 1
      quotas[category] += 1
    }
  }

  return {
    [ITEM_CATEGORIES.WEAPON_SKIN]: Number(quotas[ITEM_CATEGORIES.WEAPON_SKIN] || 0),
    [ITEM_CATEGORIES.CASE]: Number(quotas[ITEM_CATEGORIES.CASE] || 0),
    [ITEM_CATEGORIES.STICKER_CAPSULE]: Number(quotas[ITEM_CATEGORIES.STICKER_CAPSULE] || 0)
  }
}

function buildBaseDiagnostics() {
  return {
    generatedAt: new Date().toISOString(),
    targetUniverseSize: DEFAULT_UNIVERSE_TARGET,
    sourceCatalog: {
      seededRows: 0,
      activeCatalogRows: 0,
      tradableRows: 0,
      eligibleTradableRows: 0,
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0,
      byCategory: {}
    },
    universeBuild: {
      activeUniverseBuilt: 0,
      missingToTarget: 0,
      quotas: buildCategoryQuotas(DEFAULT_UNIVERSE_TARGET),
      quotaTargetByCategory: buildCategoryQuotas(DEFAULT_UNIVERSE_TARGET),
      selectedByCategory: {},
      selectedByCategoryQuotaStage: {},
      quotaShortfallByCategory: {},
      reallocatedSlots: 0,
      eligibleRows: 0,
      fallbackToMaxEligible: true
    },
    refreshed: false,
    skipped: false,
    refreshIntervalMs: SOURCE_CATALOG_REFRESH_MS
  }
}

function mergeCategoryCounter(counter = {}, category = "", field = "") {
  const key = normalizeCategory(category)
  if (!counter[key]) {
    counter[key] = {
      total: 0,
      eligible: 0,
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0
    }
  }
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

async function ingestSourceCatalogSeeds() {
  const seedRows = Array.isArray(sourceCatalogSeed) ? sourceCatalogSeed.slice(0, SOURCE_CATALOG_LIMIT) : []
  if (!seedRows.length) {
    return {
      seedRows: 0,
      seededRows: 0,
      seededSkins: 0
    }
  }

  const seededRows = await marketSourceCatalogRepo.upsertRows(seedRows)
  const skins = await ensureSkinsForCatalogNames(seedRows.map((row) => row.marketHashName))

  return {
    seedRows: seedRows.length,
    seededRows,
    seededSkins: Array.isArray(skins) ? skins.length : 0
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

function evaluateEligibility({ category, referencePrice, volume7d, marketCoverageCount, snapshotStale }) {
  const rules = SOURCE_QUALITY_RULES[category] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]

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
  if (snapshotStale) {
    return { eligible: false, reason: "excludedStaleItems" }
  }

  return { eligible: true, reason: "" }
}

async function enrichSourceCatalog() {
  const rows = await marketSourceCatalogRepo.listActiveTradable({ limit: SOURCE_CATALOG_LIMIT })
  if (!rows.length) {
    return {
      activeCatalogRows: 0,
      tradableRows: 0,
      eligibleTradableRows: 0,
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0,
      byCategory: {}
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

  const byCategory = {}
  const updates = []
  const counts = {
    activeCatalogRows: rows.length,
    tradableRows: 0,
    eligibleTradableRows: 0,
    excludedLowValueItems: 0,
    excludedLowLiquidityItems: 0,
    excludedWeakMarketCoverageItems: 0,
    excludedStaleItems: 0,
    excludedMissingReferenceItems: 0
  }

  for (const row of rows) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue

    const category = normalizeCategory(row?.category, marketHashName)
    mergeCategoryCounter(byCategory, category)
    byCategory[category].total += 1

    const tradable = row?.tradable == null ? true : Boolean(row.tradable)
    if (tradable) {
      counts.tradableRows += 1
    }

    const skinId = Number(skinsByName[marketHashName]?.id || 0)
    const snapshot = skinId > 0 ? snapshotsBySkinId[skinId] || null : null
    const quoteCoverage = quoteCoverageByItem[marketHashName] || {}
    const referencePrice = toPositiveOrNull(
      toFiniteOrNull(snapshot?.average_7d_price) ?? toFiniteOrNull(snapshot?.lowest_listing_price)
    )
    const volume7d = resolveVolume7d(snapshot, quoteCoverage)
    const marketCoverageCount = Math.max(Number(quoteCoverage?.marketCoverageCount || 0), 0)
    const snapshotStale = snapshot ? isSnapshotStale(snapshot) : true

    const eligibility = evaluateEligibility({
      category,
      referencePrice,
      volume7d,
      marketCoverageCount,
      snapshotStale
    })

    const liquidityRank = computeSourceLiquidityScore({
      category,
      referencePrice,
      volume7d,
      marketCoverage: marketCoverageCount,
      snapshotStale
    })

    const scanEligible = tradable && eligibility.eligible
    if (scanEligible) {
      counts.eligibleTradableRows += 1
      byCategory[category].eligible += 1
    } else if (eligibility.reason) {
      counts[eligibility.reason] += 1
      mergeCategoryCounter(byCategory, category, eligibility.reason)
    }

    updates.push({
      market_hash_name: marketHashName,
      item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
      category,
      subcategory: normalizeText(row?.subcategory) || null,
      tradable,
      scan_eligible: scanEligible,
      reference_price: referencePrice,
      market_coverage_count: marketCoverageCount,
      liquidity_rank: liquidityRank,
      volume_7d: volume7d,
      snapshot_stale: snapshotStale,
      snapshot_captured_at: snapshot?.captured_at || null,
      invalid_reason: scanEligible ? null : eligibility.reason || "not_tradable",
      source_tag: normalizeText(row?.source_tag || row?.sourceTag) || "curated_seed",
      is_active: row?.is_active == null ? true : Boolean(row.is_active),
      last_enriched_at: new Date().toISOString()
    })
  }

  await marketSourceCatalogRepo.upsertRows(updates)

  return {
    ...counts,
    byCategory
  }
}

function takeTopByCategory(rows = [], quotas = {}) {
  const byCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: [],
    [ITEM_CATEGORIES.CASE]: [],
    [ITEM_CATEGORIES.STICKER_CAPSULE]: []
  }

  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeCategory(row?.category, row?.market_hash_name)
    byCategory[category].push(row)
  }

  const selected = []
  const used = new Set()
  const selectedByCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: 0,
    [ITEM_CATEGORIES.CASE]: 0,
    [ITEM_CATEGORIES.STICKER_CAPSULE]: 0
  }

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

function normalizeCatalogCandidateRows(rows = [], selectionTier = "strict_eligible") {
  const tier = normalizeText(selectionTier) || "strict_eligible"
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName) return null
      return {
        ...row,
        market_hash_name: marketHashName,
        item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
        category: normalizeCategory(row?.category, marketHashName),
        liquidity_rank: toFiniteOrNull(row?.liquidity_rank) ?? 0,
        market_coverage_count: Math.max(Number(row?.market_coverage_count || 0), 0),
        volume_7d: Math.max(Number(row?.volume_7d || 0), 0),
        reference_price: toFiniteOrNull(row?.reference_price) ?? 0,
        selectionTier: tier,
        selectionTierRank: tier === "strict_eligible" ? 2 : 1
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

async function rebuildUniverseFromCatalog(targetSize = DEFAULT_UNIVERSE_TARGET) {
  const safeTarget = Math.max(Math.round(Number(targetSize || DEFAULT_UNIVERSE_TARGET)), 1)
  const strictEligibleRows = normalizeCatalogCandidateRows(
    await marketSourceCatalogRepo.listScanEligible({
      limit: Math.max(SOURCE_CATALOG_LIMIT, safeTarget * 3)
    }),
    "strict_eligible"
  )

  let fallbackTradableRows = []
  if (strictEligibleRows.length < safeTarget) {
    const strictNames = new Set(strictEligibleRows.map((row) => row.market_hash_name))
    fallbackTradableRows = normalizeCatalogCandidateRows(
      await marketSourceCatalogRepo.listActiveTradable({
        limit: Math.max(SOURCE_CATALOG_LIMIT, safeTarget * 4)
      }),
      "fallback_tradable"
    ).filter((row) => !strictNames.has(row.market_hash_name))
  }

  const rankedRows = dedupeByMarketHashName([...strictEligibleRows, ...fallbackTradableRows])
    .sort(
      (a, b) =>
        Number(b.selectionTierRank || 0) - Number(a.selectionTierRank || 0) ||
        Number(b.liquidity_rank || 0) - Number(a.liquidity_rank || 0) ||
        Number(b.market_coverage_count || 0) - Number(a.market_coverage_count || 0) ||
        Number(b.volume_7d || 0) - Number(a.volume_7d || 0) ||
        Number(b.reference_price || 0) - Number(a.reference_price || 0)
    )

  const quotas = buildCategoryQuotas(safeTarget)
  const { selected, leftovers, selectedByCategory } = takeTopByCategory(rankedRows, quotas)
  const selectedByCategoryQuotaStage = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: Number(selectedByCategory[ITEM_CATEGORIES.WEAPON_SKIN] || 0),
    [ITEM_CATEGORIES.CASE]: Number(selectedByCategory[ITEM_CATEGORIES.CASE] || 0),
    [ITEM_CATEGORIES.STICKER_CAPSULE]: Number(selectedByCategory[ITEM_CATEGORIES.STICKER_CAPSULE] || 0)
  }

  const finalRows = [...selected]
  for (const row of leftovers) {
    if (finalRows.length >= safeTarget) break
    finalRows.push(row)
    const category = normalizeCategory(row?.category, row?.market_hash_name)
    selectedByCategory[category] = Number(selectedByCategory[category] || 0) + 1
  }

  const selectedFromStrict = finalRows.filter((row) => row.selectionTier === "strict_eligible").length
  const selectedFromFallback = Math.max(finalRows.length - selectedFromStrict, 0)

  const normalizedUniverseRows = finalRows.slice(0, safeTarget).map((row, index) => ({
    marketHashName: row.market_hash_name,
    itemName: row.item_name || row.market_hash_name,
    liquidityRank: index + 1
  }))

  const persist = await marketUniverseRepo.replaceActiveUniverse(normalizedUniverseRows)
  const quotaShortfallByCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: Math.max(
      Number(quotas[ITEM_CATEGORIES.WEAPON_SKIN] || 0) -
        Number(selectedByCategoryQuotaStage[ITEM_CATEGORIES.WEAPON_SKIN] || 0),
      0
    ),
    [ITEM_CATEGORIES.CASE]: Math.max(
      Number(quotas[ITEM_CATEGORIES.CASE] || 0) -
        Number(selectedByCategoryQuotaStage[ITEM_CATEGORIES.CASE] || 0),
      0
    ),
    [ITEM_CATEGORIES.STICKER_CAPSULE]: Math.max(
      Number(quotas[ITEM_CATEGORIES.STICKER_CAPSULE] || 0) -
        Number(selectedByCategoryQuotaStage[ITEM_CATEGORIES.STICKER_CAPSULE] || 0),
      0
    )
  }

  return {
    targetUniverseSize: safeTarget,
    eligibleRows: strictEligibleRows.length,
    strictEligibleRows: strictEligibleRows.length,
    fallbackTradableRows: fallbackTradableRows.length,
    selectedFromStrict,
    selectedFromFallback,
    activeUniverseBuilt: normalizedUniverseRows.length,
    missingToTarget: Math.max(safeTarget - normalizedUniverseRows.length, 0),
    quotaTargetByCategory: quotas,
    selectedByCategory,
    selectedByCategoryQuotaStage,
    quotaShortfallByCategory,
    reallocatedSlots: Math.max(finalRows.length - selected.length, 0),
    quotas,
    fallbackToMaxEligible: true,
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
  const sourceCoverage = await enrichSourceCatalog()
  const universeBuild = await rebuildUniverseFromCatalog(targetUniverseSize)

  return {
    ...base,
    generatedAt: new Date().toISOString(),
    refreshed: true,
    skipped: false,
    elapsedMs: Date.now() - startedAt,
    sourceCatalog: {
      ...base.sourceCatalog,
      seededRows: Number(ingest?.seededRows || 0),
      activeCatalogRows: Number(sourceCoverage?.activeCatalogRows || 0),
      tradableRows: Number(sourceCoverage?.tradableRows || 0),
      eligibleTradableRows: Number(sourceCoverage?.eligibleTradableRows || 0),
      excludedLowValueItems: Number(sourceCoverage?.excludedLowValueItems || 0),
      excludedLowLiquidityItems: Number(sourceCoverage?.excludedLowLiquidityItems || 0),
      excludedWeakMarketCoverageItems: Number(sourceCoverage?.excludedWeakMarketCoverageItems || 0),
      excludedStaleItems: Number(sourceCoverage?.excludedStaleItems || 0),
      excludedMissingReferenceItems: Number(sourceCoverage?.excludedMissingReferenceItems || 0),
      byCategory: sourceCoverage?.byCategory || {}
    },
    universeBuild
  }
}

function shouldRefresh(force = false) {
  if (force) return true
  if (sourceCatalogState.lastDiagnostics?.error) return true
  if (!sourceCatalogState.lastPreparedAt) return true
  return Date.now() - sourceCatalogState.lastPreparedAt >= SOURCE_CATALOG_REFRESH_MS
}

async function prepareSourceCatalog(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const targetUniverseSize = Number(options.targetUniverseSize || DEFAULT_UNIVERSE_TARGET)

  if (!shouldRefresh(forceRefresh) && sourceCatalogState.lastDiagnostics) {
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
      return diagnostics
    })
    .catch((err) => {
      const safeTarget = Math.max(
        Math.round(Number(targetUniverseSize || DEFAULT_UNIVERSE_TARGET)),
        1
      )
      const fallback = {
        ...buildBaseDiagnostics(),
        generatedAt: new Date().toISOString(),
        targetUniverseSize: safeTarget,
        refreshed: false,
        skipped: false,
        error: String(err?.message || "source_catalog_pipeline_failed")
      }
      fallback.universeBuild = {
        ...fallback.universeBuild,
        targetUniverseSize: safeTarget,
        activeUniverseBuilt: 0,
        missingToTarget: safeTarget,
        fallbackToMaxEligible: true
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

module.exports = {
  prepareSourceCatalog,
  getLastDiagnostics,
  __testables: {
    normalizeCategory,
    computeSourceLiquidityScore,
    evaluateEligibility,
    buildCategoryQuotas,
    resolveVolume7d
  }
}

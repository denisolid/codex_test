const {
  ITEM_CATEGORIES,
  ROUND_ROBIN_CATEGORY_ORDER,
  SCAN_STATE,
  SUPPORTED_SCAN_CATEGORIES
} = require("./config")
const { classifyCatalogState, normalizeCategory } = require("./stateModel")

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function normalizePriorityTier(value) {
  const tier = normalizeText(value).toLowerCase()
  if (tier === "tier_a" || tier === "tier_b") return tier
  return null
}

function resolvePriorityTierRank(value) {
  const tier = normalizePriorityTier(value)
  if (tier === "tier_a") return 2
  if (tier === "tier_b") return 1
  return 0
}

function resolvePriorityTierBucket(row = {}) {
  const tier = normalizePriorityTier(row.priorityTier || row.priority_tier)
  if (tier === "tier_a") return "tier_a"
  if (tier === "tier_b") return "tier_b"
  return "non_priority"
}

function normalizeCatalogRow(row = {}) {
  const marketHashName = normalizeText(row.market_hash_name || row.marketHashName)
  if (!marketHashName) return null
  const itemName = normalizeText(row.item_name || row.itemName || marketHashName) || marketHashName
  const category = normalizeCategory(row.category || row.itemCategory, itemName)
  return {
    marketHashName,
    itemName,
    category,
    itemSubcategory: normalizeText(row.subcategory || row.itemSubcategory) || null,
    tradable: row.tradable == null ? true : Boolean(row.tradable),
    isActive: row.is_active == null ? (row.isActive == null ? true : Boolean(row.isActive)) : Boolean(row.is_active),
    referencePrice: toFiniteOrNull(row.reference_price ?? row.referencePrice),
    marketCoverageCount: Math.max(
      Number(toFiniteOrNull(row.market_coverage_count ?? row.marketCoverageCount) || 0),
      0
    ),
    volume7d: toFiniteOrNull(row.volume_7d ?? row.volume7d),
    liquidityRank: toFiniteOrNull(row.liquidity_rank ?? row.liquidityRank),
    snapshotStale: row.snapshot_stale == null ? Boolean(row.snapshotStale) : Boolean(row.snapshot_stale),
    snapshotCapturedAt: row.snapshot_captured_at || row.snapshotCapturedAt || null,
    quoteFetchedAt: row.quote_fetched_at || row.quoteFetchedAt || null,
    priorityTier: normalizePriorityTier(row.priority_tier || row.priorityTier),
    priorityBoost: toFiniteOrNull(row.priority_boost ?? row.priorityBoost) ?? 0,
    isPriorityItem:
      row.is_priority_item == null
        ? normalizePriorityTier(row.priority_tier || row.priorityTier) != null
        : Boolean(row.is_priority_item),
    invalidReason: normalizeText(row.invalid_reason || row.invalidReason) || null,
    sourceTag: normalizeText(row.source_tag || row.sourceTag) || null,
    raw: row
  }
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function hasStrongScanEvidence(row = {}) {
  const marketCoverageCount = Math.max(Number(toFiniteOrNull(row.marketCoverageCount) || 0), 0)
  const referencePrice = toFiniteOrNull(row.referencePrice)
  const volume7d = toFiniteOrNull(row.volume7d)
  const snapshotCapturedAt = toIsoOrNull(row.snapshotCapturedAt)
  const quoteFetchedAt = toIsoOrNull(row.quoteFetchedAt)
  const hasFreshnessSignal = Boolean(snapshotCapturedAt || quoteFetchedAt)

  if (marketCoverageCount < 2) return false
  if (referencePrice == null || referencePrice <= 0) return false
  if (volume7d != null && volume7d > 0) return true
  return hasFreshnessSignal
}

function buildCategoryStateCounter() {
  return Object.fromEntries(
    ROUND_ROBIN_CATEGORY_ORDER.map((category) => [
      category,
      {
        total: 0,
        scanable: 0,
        scanableWithPenalties: 0,
        hardReject: 0
      }
    ])
  )
}

function normalizeRows(rows = []) {
  const deduped = []
  const seen = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    const normalized = normalizeCatalogRow(row)
    if (!normalized) continue
    if (!SUPPORTED_SCAN_CATEGORIES.includes(normalized.category)) continue
    if (seen.has(normalized.marketHashName)) continue
    seen.add(normalized.marketHashName)
    deduped.push(normalized)
  }
  return deduped
}

function incrementCounter(target, key, amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

function buildRoundRobinPool(scannableRows = [], options = {}) {
  const lastScannedAtByName = options.lastScannedAtByName || new Map()
  const byCategory = Object.fromEntries(
    ROUND_ROBIN_CATEGORY_ORDER.map((category) => [
      category,
      { preferred: [], fallback: [] }
    ])
  )

  for (const row of Array.isArray(scannableRows) ? scannableRows : []) {
    const category = normalizeCategory(row.category || row.itemCategory, row.itemName)
    const bucket = byCategory[category]
    if (!bucket) continue
    if (hasStrongScanEvidence(row)) {
      bucket.preferred.push(row)
    } else {
      bucket.fallback.push(row)
    }
  }

  const sortRows = (rows = []) => {
    rows.sort((a, b) => {
      const aSeenAt = Number(lastScannedAtByName.get(a.marketHashName) || 0)
      const bSeenAt = Number(lastScannedAtByName.get(b.marketHashName) || 0)
      const aTier = resolvePriorityTierRank(a.priorityTier)
      const bTier = resolvePriorityTierRank(b.priorityTier)
      if (aTier !== bTier) return bTier - aTier
      const aPriorityBoost = Number(a.priorityBoost || 0)
      const bPriorityBoost = Number(b.priorityBoost || 0)
      if (aPriorityBoost !== bPriorityBoost) return bPriorityBoost - aPriorityBoost
      if (aSeenAt !== bSeenAt) return aSeenAt - bSeenAt
      const aCoverage = Number(a.marketCoverageCount || 0)
      const bCoverage = Number(b.marketCoverageCount || 0)
      if (aCoverage !== bCoverage) return bCoverage - aCoverage
      const aVolume = Number(a.volume7d || 0)
      const bVolume = Number(b.volume7d || 0)
      if (aVolume !== bVolume) return bVolume - aVolume
      const aRank = Number(a.liquidityRank || 0)
      const bRank = Number(b.liquidityRank || 0)
      if (aRank !== bRank) return bRank - aRank
      return String(a.marketHashName).localeCompare(String(b.marketHashName))
    })
  }

  for (const category of Object.keys(byCategory)) {
    sortRows(byCategory[category].preferred)
    sortRows(byCategory[category].fallback)
  }

  const roundRobinPool = []
  const pullRow = (bucket = {}) => {
    if (bucket.preferred && bucket.preferred.length) return bucket.preferred.shift()
    if (bucket.fallback && bucket.fallback.length) return bucket.fallback.shift()
    return null
  }

  while (true) {
    let addedInPass = false
    for (const category of ROUND_ROBIN_CATEGORY_ORDER) {
      const bucket = byCategory[category]
      if (!bucket) continue
      const picked = pullRow(bucket)
      if (!picked) continue
      roundRobinPool.push(picked)
      addedInPass = true
    }
    if (!addedInPass) break
  }

  return roundRobinPool
}

function selectScanCandidates(options = {}) {
  const rawRows = normalizeRows(options.catalogRows)
  const batchSize = Math.max(Math.round(Number(options.batchSize || 0)), 1)
  const lastScannedAtByName = options.lastScannedAtByName || new Map()
  const previousCursor = Math.max(Math.round(Number(options.cursor || 0)), 0)
  const nowMs = Number(options.nowMs || Date.now())

  const diagnostics = {
    totalCatalogRows: rawRows.length,
    scanable: 0,
    scanableWithPenalties: 0,
    hardReject: 0,
    poolByPriorityTier: {
      tier_a: 0,
      tier_b: 0,
      non_priority: 0
    },
    selectedByPriorityTier: {
      tier_a: 0,
      tier_b: 0,
      non_priority: 0
    },
    stateByCategory: buildCategoryStateCounter(),
    hardRejectReasons: {},
    selectedByCategory: Object.fromEntries(
      ROUND_ROBIN_CATEGORY_ORDER.map((category) => [category, 0])
    )
  }

  const classifiedRows = []
  for (const row of rawRows) {
    const classification = classifyCatalogState(row)
    const category = normalizeCategory(classification.category, row.itemName)
    const categoryCounter = diagnostics.stateByCategory[category] || diagnostics.stateByCategory.weapon_skin
    categoryCounter.total += 1

    if (classification.state === SCAN_STATE.SCANABLE) {
      diagnostics.scanable += 1
      categoryCounter.scanable += 1
    } else if (classification.state === SCAN_STATE.SCANABLE_WITH_PENALTIES) {
      diagnostics.scanableWithPenalties += 1
      categoryCounter.scanableWithPenalties += 1
    } else {
      diagnostics.hardReject += 1
      categoryCounter.hardReject += 1
      for (const reason of classification.hardRejectReasons) {
        incrementCounter(diagnostics.hardRejectReasons, reason, 1)
      }
    }

    classifiedRows.push({
      ...row,
      scanState: classification.state,
      scanPenaltyFlags: classification.penaltyFlags,
      scanHardRejectReasons: classification.hardRejectReasons,
      scanFreshness: classification.freshness
    })
  }

  const scannableRows = classifiedRows.filter(
    (row) => row.scanState !== SCAN_STATE.HARD_REJECT
  )
  for (const row of scannableRows) {
    const bucket = resolvePriorityTierBucket(row)
    diagnostics.poolByPriorityTier[bucket] = Number(diagnostics.poolByPriorityTier[bucket] || 0) + 1
  }
  const pool = buildRoundRobinPool(scannableRows, { lastScannedAtByName })
  if (!pool.length) {
    return {
      selected: [],
      poolSize: 0,
      attemptedBatchSize: batchSize,
      nextCursor: 0,
      diagnostics
    }
  }

  const selected = []
  const hasScanHistory = lastScannedAtByName instanceof Map && lastScannedAtByName.size > 0
  const startCursor = hasScanHistory ? 0 : previousCursor % pool.length
  const maxSelection = Math.min(batchSize, pool.length)
  for (let offset = 0; offset < maxSelection; offset += 1) {
    const index = (startCursor + offset) % pool.length
    const selectedRow = pool[index]
    if (!selectedRow) continue
    selected.push(selectedRow)
    diagnostics.selectedByCategory[selectedRow.category] =
      Number(diagnostics.selectedByCategory[selectedRow.category] || 0) + 1
    const priorityBucket = resolvePriorityTierBucket(selectedRow)
    diagnostics.selectedByPriorityTier[priorityBucket] =
      Number(diagnostics.selectedByPriorityTier[priorityBucket] || 0) + 1
    lastScannedAtByName.set(selectedRow.marketHashName, nowMs)
  }

  return {
    selected,
    poolSize: pool.length,
    attemptedBatchSize: batchSize,
    nextCursor: hasScanHistory ? 0 : (startCursor + selected.length) % Math.max(pool.length, 1),
    diagnostics
  }
}

module.exports = {
  normalizeCatalogRow,
  normalizeRows,
  buildRoundRobinPool,
  selectScanCandidates
}

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
    invalidReason: normalizeText(row.invalid_reason || row.invalidReason) || null,
    sourceTag: normalizeText(row.source_tag || row.sourceTag) || null,
    raw: row
  }
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
    ROUND_ROBIN_CATEGORY_ORDER.map((category) => [category, []])
  )

  for (const row of Array.isArray(scannableRows) ? scannableRows : []) {
    const category = normalizeCategory(row.category || row.itemCategory, row.itemName)
    const bucket = byCategory[category]
    if (!bucket) continue
    bucket.push(row)
  }

  for (const [category, bucket] of Object.entries(byCategory)) {
    bucket.sort((a, b) => {
      const aSeenAt = Number(lastScannedAtByName.get(a.marketHashName) || 0)
      const bSeenAt = Number(lastScannedAtByName.get(b.marketHashName) || 0)
      if (aSeenAt !== bSeenAt) return aSeenAt - bSeenAt
      const aRank = Number(a.liquidityRank || 0)
      const bRank = Number(b.liquidityRank || 0)
      if (aRank !== bRank) return bRank - aRank
      return String(a.marketHashName).localeCompare(String(b.marketHashName))
    })
    byCategory[category] = bucket
  }

  const roundRobinPool = []
  let hasRows = true
  while (hasRows) {
    hasRows = false
    for (const category of ROUND_ROBIN_CATEGORY_ORDER) {
      const bucket = byCategory[category]
      if (!bucket || !bucket.length) continue
      hasRows = true
      roundRobinPool.push(bucket.shift())
    }
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
  const startCursor = previousCursor % pool.length
  const maxSelection = Math.min(batchSize, pool.length)
  for (let offset = 0; offset < maxSelection; offset += 1) {
    const index = (startCursor + offset) % pool.length
    const selectedRow = pool[index]
    if (!selectedRow) continue
    selected.push(selectedRow)
    diagnostics.selectedByCategory[selectedRow.category] =
      Number(diagnostics.selectedByCategory[selectedRow.category] || 0) + 1
    lastScannedAtByName.set(selectedRow.marketHashName, nowMs)
  }

  return {
    selected,
    poolSize: pool.length,
    attemptedBatchSize: batchSize,
    nextCursor: (startCursor + selected.length) % Math.max(pool.length, 1),
    diagnostics
  }
}

module.exports = {
  normalizeCatalogRow,
  normalizeRows,
  buildRoundRobinPool,
  selectScanCandidates
}

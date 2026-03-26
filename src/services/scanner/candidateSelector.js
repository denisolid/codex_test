const {
  ITEM_CATEGORIES,
  ROUND_ROBIN_CATEGORY_ORDER,
  SCAN_STATE,
  SUPPORTED_SCAN_CATEGORIES,
  SCAN_COHORT_CATEGORIES,
  SCAN_COHORT_HOT_SHARE,
  SCAN_COHORT_WARM_SHARE,
  SCAN_COHORT_COLD_SHARE,
  SCAN_COHORT_COLD_MIN,
  SCAN_COHORT_COLD_MAX,
  SCAN_COHORT_FALLBACK_CANDIDATE_POOL_SHARE,
  SCAN_COHORT_FALLBACK_CANDIDATE_POOL_MAX,
  SCAN_COHORT_FALLBACK_ACTIVE_TRADABLE_SHARE,
  SCAN_COHORT_FALLBACK_ACTIVE_TRADABLE_MAX,
  SCAN_COHORT_FALLBACK_COMBINED_SHARE,
  SCAN_COHORT_FALLBACK_COMBINED_MAX
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

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function resolveVolume7d(row = {}) {
  return (
    toPositiveOrNull(
      row.sell_volume_7d ??
        row.sellVolume7d ??
        row.sell_route_volume_7d ??
        row.sellRouteVolume7d
    ) ??
    toPositiveOrNull(row.market_max_volume_7d ?? row.marketMaxVolume7d) ??
    toPositiveOrNull(row.buy_volume_7d ?? row.buyVolume7d) ??
    toPositiveOrNull(row.volume_7d ?? row.volume7d)
  )
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

function normalizeCandidateStatus(value) {
  const status = normalizeText(value).toLowerCase()
  if (
    status === "candidate" ||
    status === "enriching" ||
    status === "near_eligible" ||
    status === "eligible" ||
    status === "rejected"
  ) {
    return status
  }
  return "candidate"
}

function normalizeCatalogStatus(value) {
  const status = normalizeText(value).toLowerCase()
  if (status === "scannable" || status === "shadow" || status === "blocked") return status
  return "shadow"
}

function normalizeScanCohort(value) {
  const cohort = normalizeText(value).toLowerCase()
  if (cohort === "hot" || cohort === "warm" || cohort === "cold" || cohort === "fallback") {
    return cohort
  }
  return ""
}

function resolvePersistedScanCohort(row = {}) {
  const explicit = normalizeScanCohort(row.scanCohort || row.scan_cohort)
  if (explicit) return explicit
  if (normalizeText(row.fallbackSource || row.fallback_source)) return "fallback"
  return ""
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
    volume7d: resolveVolume7d(row),
    sellVolume7d: toPositiveOrNull(
      row.sell_volume_7d ??
        row.sellVolume7d ??
        row.sell_route_volume_7d ??
        row.sellRouteVolume7d
    ),
    buyVolume7d: toPositiveOrNull(row.buy_volume_7d ?? row.buyVolume7d),
    marketMaxVolume7d: toPositiveOrNull(row.market_max_volume_7d ?? row.marketMaxVolume7d),
    liquiditySource: normalizeText(row.liquidity_source || row.liquiditySource) || null,
    liquidityRank: toFiniteOrNull(row.liquidity_rank ?? row.liquidityRank),
    snapshotStale: row.snapshot_stale == null ? Boolean(row.snapshotStale) : Boolean(row.snapshot_stale),
    snapshotCapturedAt: row.snapshot_captured_at || row.snapshotCapturedAt || null,
    quoteFetchedAt: row.quote_fetched_at || row.quoteFetchedAt || null,
    referenceState: normalizeText(row.reference_state || row.referenceState) || null,
    lastMarketSignalAt:
      row.last_market_signal_at ||
      row.lastMarketSignalAt ||
      row.latest_market_signal_at ||
      row.latestMarketSignalAt ||
      null,
    latestReferencePriceAt:
      row.latest_reference_price_at ||
      row.latestReferencePriceAt ||
      row.reference_price_at ||
      row.referencePriceAt ||
      null,
    priorityTier: normalizePriorityTier(row.priority_tier || row.priorityTier),
    priorityBoost: toFiniteOrNull(row.priority_boost ?? row.priorityBoost) ?? 0,
    isPriorityItem:
      row.is_priority_item == null
        ? normalizePriorityTier(row.priority_tier || row.priorityTier) != null
        : Boolean(row.is_priority_item),
    candidateStatus: normalizeCandidateStatus(row.candidate_status ?? row.candidateStatus),
    scanEligible:
      row.scan_eligible == null ? Boolean(row.scanEligible) : Boolean(row.scan_eligible),
    catalogStatus: normalizeCatalogStatus(row.catalog_status ?? row.catalogStatus),
    scanCohort: resolvePersistedScanCohort(row),
    fallbackSource: normalizeText(row.fallbackSource || row.fallback_source) || null,
    sourceOrigin: normalizeText(row.sourceOrigin || row.source_origin) || null,
    invalidReason: normalizeText(row.invalid_reason || row.invalidReason) || null,
    sourceTag: normalizeText(row.source_tag || row.sourceTag) || null,
    raw: row
  }
}

function toIsoOrNull(value) {
  if (value == null || value === "") return null
  if (value instanceof Date) {
    const dateTs = value.getTime()
    if (!Number.isFinite(dateTs)) return null
    return new Date(dateTs).toISOString()
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
      const ts = new Date(normalizedTs).getTime()
      if (Number.isFinite(ts)) return new Date(ts).toISOString()
    }
  }

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
  const lastMarketSignalAt = toIsoOrNull(row.lastMarketSignalAt)
  const hasFreshnessSignal = Boolean(snapshotCapturedAt || quoteFetchedAt || lastMarketSignalAt)

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

function buildCohortCounter(initialValue = 0) {
  return {
    hot: Number(initialValue || 0),
    warm: Number(initialValue || 0),
    cold: Number(initialValue || 0),
    fallback: Number(initialValue || 0)
  }
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

function calculateCohortBudgetCaps(batchSize = 0) {
  const safeBatchSize = Math.max(Math.round(Number(batchSize || 0)), 1)
  const hotCap = Math.min(
    Math.ceil(safeBatchSize * SCAN_COHORT_HOT_SHARE),
    safeBatchSize
  )
  const warmCap = Math.min(
    Math.ceil(safeBatchSize * SCAN_COHORT_WARM_SHARE),
    safeBatchSize
  )
  const coldCap = Math.min(
    Math.max(Math.floor(safeBatchSize * SCAN_COHORT_COLD_SHARE), SCAN_COHORT_COLD_MIN),
    SCAN_COHORT_COLD_MAX
  )
  const candidatePoolFallbackCap = Math.min(
    Math.ceil(safeBatchSize * SCAN_COHORT_FALLBACK_CANDIDATE_POOL_SHARE),
    SCAN_COHORT_FALLBACK_CANDIDATE_POOL_MAX
  )
  const activeTradableFallbackCap = Math.min(
    Math.ceil(safeBatchSize * SCAN_COHORT_FALLBACK_ACTIVE_TRADABLE_SHARE),
    SCAN_COHORT_FALLBACK_ACTIVE_TRADABLE_MAX
  )
  const combinedFallbackCap = Math.min(
    Math.ceil(safeBatchSize * SCAN_COHORT_FALLBACK_COMBINED_SHARE),
    SCAN_COHORT_FALLBACK_COMBINED_MAX
  )
  return {
    hotCap,
    warmCap,
    coldCap,
    candidatePoolFallbackCap,
    activeTradableFallbackCap,
    combinedFallbackCap
  }
}

function incrementCounter(target, key, amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

const PREMIUM_RESERVE_CATEGORY_ORDER = Object.freeze([
  ITEM_CATEGORIES.KNIFE,
  ITEM_CATEGORIES.GLOVE
])
const PREMIUM_RESERVE_MIN_REFERENCE_PRICE = 20

function hasPremiumReserveQuality(row = {}) {
  const category = normalizeCategory(row.category || row.itemCategory, row.itemName)
  if (!PREMIUM_RESERVE_CATEGORY_ORDER.includes(category)) return false
  if (row.scanState !== SCAN_STATE.SCANABLE) return false

  const referencePrice = toFiniteOrNull(row.referencePrice)
  const marketCoverageCount = Math.max(Number(toFiniteOrNull(row.marketCoverageCount) || 0), 0)
  const freshnessState = normalizeText(row?.scanFreshness?.state).toLowerCase()

  if (referencePrice == null || referencePrice < PREMIUM_RESERVE_MIN_REFERENCE_PRICE) return false
  if (marketCoverageCount < 2) return false
  if (freshnessState && freshnessState !== "fresh" && freshnessState !== "aging") return false

  return hasStrongScanEvidence(row)
}

function pickPremiumReserveRows(pool = [], maxCount = 0) {
  const safePool = Array.isArray(pool) ? pool : []
  const safeMaxCount = Math.max(Math.round(Number(maxCount || 0)), 0)
  if (!safePool.length || safeMaxCount <= 0) return []

  const selected = []
  const selectedNames = new Set()
  for (const category of PREMIUM_RESERVE_CATEGORY_ORDER) {
    if (selected.length >= safeMaxCount) break
    const match = safePool.find((row) => {
      const marketHashName = normalizeText(row?.marketHashName)
      if (!marketHashName || selectedNames.has(marketHashName)) return false
      if (normalizeCategory(row?.category || row?.itemCategory, row?.itemName) !== category) {
        return false
      }
      return hasPremiumReserveQuality(row)
    })
    if (!match) continue
    selected.push(match)
    selectedNames.add(match.marketHashName)
  }
  return selected
}

function buildRoundRobinPool(scannableRows = [], options = {}) {
  const byCategory = Object.fromEntries(
    ROUND_ROBIN_CATEGORY_ORDER.map((category) => [category, []])
  )

  for (const row of Array.isArray(scannableRows) ? scannableRows : []) {
    const category = normalizeCategory(row.category || row.itemCategory, row.itemName)
    const bucket = byCategory[category]
    if (!bucket) continue
    bucket.push(row)
  }

  const sortRows = (rows = []) => {
    rows.sort((a, b) => {
      const aTier = resolvePriorityTierRank(a.priorityTier)
      const bTier = resolvePriorityTierRank(b.priorityTier)
      if (aTier !== bTier) return bTier - aTier
      const aPriorityBoost = Number(a.priorityBoost || 0)
      const bPriorityBoost = Number(b.priorityBoost || 0)
      if (aPriorityBoost !== bPriorityBoost) return bPriorityBoost - aPriorityBoost
      const aLastSignalAt = Number(new Date(toIsoOrNull(a.lastMarketSignalAt) || 0).getTime() || 0)
      const bLastSignalAt = Number(new Date(toIsoOrNull(b.lastMarketSignalAt) || 0).getTime() || 0)
      if (aLastSignalAt !== bLastSignalAt) return bLastSignalAt - aLastSignalAt
      const aRank = Number(a.liquidityRank || 0)
      const bRank = Number(b.liquidityRank || 0)
      if (aRank !== bRank) return bRank - aRank
      return String(a.marketHashName).localeCompare(String(b.marketHashName))
    })
  }

  for (const category of Object.keys(byCategory)) {
    sortRows(byCategory[category])
  }

  const roundRobinPool = []

  while (true) {
    let addedInPass = false
    for (const category of ROUND_ROBIN_CATEGORY_ORDER) {
      const bucket = byCategory[category]
      if (!bucket || !bucket.length) continue
      const picked = bucket.shift()
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
      poolByCohort: buildCohortCounter(),
      selectedByCohort: buildCohortCounter(),
      fallbackSelectedBySource: {
        candidatePool: 0,
        activeTradable: 0
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
        scanFreshness: classification.freshness,
        scanCohort: row.scanCohort || resolvePersistedScanCohort(row)
      })
    }

  const cohortRows = classifiedRows.filter((row) => Boolean(normalizeScanCohort(row.scanCohort)))
  for (const row of cohortRows) {
    const bucket = resolvePriorityTierBucket(row)
    diagnostics.poolByPriorityTier[bucket] = Number(diagnostics.poolByPriorityTier[bucket] || 0) + 1
    const cohort = normalizeScanCohort(row.scanCohort)
    if (cohort && diagnostics.poolByCohort[cohort] != null) {
      diagnostics.poolByCohort[cohort] = Number(diagnostics.poolByCohort[cohort] || 0) + 1
    }
  }
  const hotPool = buildRoundRobinPool(
    cohortRows.filter((row) => normalizeScanCohort(row.scanCohort) === "hot"),
    { lastScannedAtByName }
  )
  const warmPool = buildRoundRobinPool(
    cohortRows.filter((row) => normalizeScanCohort(row.scanCohort) === "warm"),
    { lastScannedAtByName }
  )
  const coldPool = buildRoundRobinPool(
    cohortRows.filter((row) => normalizeScanCohort(row.scanCohort) === "cold"),
    { lastScannedAtByName }
  )
  const candidatePoolFallback = buildRoundRobinPool(
    cohortRows.filter(
      (row) =>
        normalizeScanCohort(row.scanCohort) === "fallback" &&
        normalizeText(row.fallbackSource).toLowerCase() === "candidatepool"
    ),
    { lastScannedAtByName }
  )
  const activeTradableFallback = buildRoundRobinPool(
    cohortRows.filter(
      (row) =>
        normalizeScanCohort(row.scanCohort) === "fallback" &&
        normalizeText(row.fallbackSource).toLowerCase() === "activetradable"
    ),
    { lastScannedAtByName }
  )
  const poolSize =
    hotPool.length +
    warmPool.length +
    coldPool.length +
    candidatePoolFallback.length +
    activeTradableFallback.length

  if (!poolSize) {
    return {
      selected: [],
      poolSize: 0,
      attemptedBatchSize: batchSize,
      nextCursor: 0,
      diagnostics
    }
  }

  const selected = []
  const maxSelection = Math.min(batchSize, poolSize)
  const cohortCaps = calculateCohortBudgetCaps(maxSelection)

  diagnostics.reservedPremiumByCategory = Object.fromEntries(
    PREMIUM_RESERVE_CATEGORY_ORDER.map((category) => [category, 0])
  )

  const selectedNames = new Set()
  let hotSelected = 0
  let warmSelected = 0
  let coldSelected = 0
  let fallbackSelected = 0
  let candidatePoolFallbackSelected = 0
  let activeTradableFallbackSelected = 0
  const recordSelectedRow = (selectedRow, { reserved = false } = {}) => {
    if (!selectedRow) return
    const marketHashName = normalizeText(selectedRow.marketHashName)
    if (!marketHashName || selectedNames.has(marketHashName)) return
    selected.push(selectedRow)
    selectedNames.add(marketHashName)
    diagnostics.selectedByCategory[selectedRow.category] =
      Number(diagnostics.selectedByCategory[selectedRow.category] || 0) + 1
    const priorityBucket = resolvePriorityTierBucket(selectedRow)
    diagnostics.selectedByPriorityTier[priorityBucket] =
      Number(diagnostics.selectedByPriorityTier[priorityBucket] || 0) + 1
    if (reserved && diagnostics.reservedPremiumByCategory[selectedRow.category] != null) {
      diagnostics.reservedPremiumByCategory[selectedRow.category] =
        Number(diagnostics.reservedPremiumByCategory[selectedRow.category] || 0) + 1
    }
    const cohort = normalizeScanCohort(selectedRow.scanCohort)
    if (cohort && diagnostics.selectedByCohort[cohort] != null) {
      diagnostics.selectedByCohort[cohort] = Number(diagnostics.selectedByCohort[cohort] || 0) + 1
    }
    if (cohort === "fallback") {
      fallbackSelected += 1
      const fallbackSource = normalizeText(selectedRow.fallbackSource).toLowerCase()
      if (fallbackSource === "candidatepool") {
        candidatePoolFallbackSelected += 1
        diagnostics.fallbackSelectedBySource.candidatePool = candidatePoolFallbackSelected
      } else if (fallbackSource === "activetradable") {
        activeTradableFallbackSelected += 1
        diagnostics.fallbackSelectedBySource.activeTradable = activeTradableFallbackSelected
      }
    } else if (cohort === "hot") {
      hotSelected += 1
    } else if (cohort === "warm") {
      warmSelected += 1
    } else if (cohort === "cold") {
      coldSelected += 1
    }
    lastScannedAtByName.set(selectedRow.marketHashName, nowMs)
  }

  const reservedPremiumRows = pickPremiumReserveRows(
    hotPool,
    Math.min(maxSelection, PREMIUM_RESERVE_CATEGORY_ORDER.length)
  )
  for (const row of reservedPremiumRows) {
    if (selected.length >= maxSelection) break
    recordSelectedRow(row, { reserved: true })
  }

  const drainPool = (rows = [], options = {}) => {
    const safeRows = Array.isArray(rows) ? rows : []
    const cohort = normalizeScanCohort(options.cohort)
    const isFallback = cohort === "fallback"
    const fallbackSource = normalizeText(options.fallbackSource).toLowerCase()
    for (const selectedRow of safeRows) {
      if (selected.length >= maxSelection) break
      if (!selectedRow) continue
      if (selectedNames.has(selectedRow.marketHashName)) continue
      if (selectedRow.scanState === SCAN_STATE.HARD_REJECT) continue
      if (cohort === "hot" && hotSelected >= cohortCaps.hotCap) break
      if (cohort === "warm" && warmSelected >= cohortCaps.warmCap) break
      if (cohort === "cold" && coldSelected >= cohortCaps.coldCap) break
      if (isFallback) {
        if (fallbackSelected >= cohortCaps.combinedFallbackCap) break
        if (
          fallbackSource === "candidatepool" &&
          candidatePoolFallbackSelected >= cohortCaps.candidatePoolFallbackCap
        ) {
          break
        }
        if (
          fallbackSource === "activetradable" &&
          activeTradableFallbackSelected >= cohortCaps.activeTradableFallbackCap
        ) {
          break
        }
      }
      recordSelectedRow(selectedRow)
    }
  }

  drainPool(hotPool, { cohort: "hot" })
  if (selected.length < maxSelection) {
    drainPool(warmPool, { cohort: "warm" })
  }
  if (selected.length < maxSelection) {
    drainPool(coldPool, { cohort: "cold" })
  }
  if (selected.length < maxSelection) {
    drainPool(candidatePoolFallback, { cohort: "fallback", fallbackSource: "candidatePool" })
  }
  if (selected.length < maxSelection) {
    drainPool(activeTradableFallback, { cohort: "fallback", fallbackSource: "activeTradable" })
  }

  return {
    selected,
    poolSize,
    attemptedBatchSize: batchSize,
    nextCursor: previousCursor,
    diagnostics
  }
}

module.exports = {
  normalizeCatalogRow,
  normalizeRows,
  resolvePersistedScanCohort,
  calculateCohortBudgetCaps,
  buildRoundRobinPool,
  selectScanCandidates
}

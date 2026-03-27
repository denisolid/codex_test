const {
  ITEM_CATEGORIES,
  SCAN_COHORT_CATEGORIES,
  OPPORTUNITY_BATCH_RUNTIME_TARGET,
  ALPHA_HOT_UNIVERSE_MIN_SIZE,
  ALPHA_HOT_UNIVERSE_NEAR_ELIGIBLE_SHARE,
  ALPHA_HOT_UNIVERSE_NEAR_ELIGIBLE_MAX,
  ALPHA_HOT_UNIVERSE_WEAPON_SKIN_SHARE,
  ALPHA_HOT_UNIVERSE_CASE_SHARE,
  ALPHA_HOT_UNIVERSE_CASE_MAX,
  ALPHA_HOT_UNIVERSE_STICKER_CAPSULE_SHARE,
  ALPHA_HOT_UNIVERSE_STICKER_CAPSULE_MAX,
  ALPHA_HOT_UNIVERSE_MIN_PER_AVAILABLE_CATEGORY,
  ALPHA_HOT_UNIVERSE_WEAPON_SEGMENT_SHARE_CAP,
  ALPHA_HOT_UNIVERSE_WEAPON_SEGMENT_MIN,
  ALPHA_HOT_UNIVERSE_WEAPON_FAMILY_SHARE_CAP,
  ALPHA_HOT_UNIVERSE_WEAPON_FAMILY_MIN,
  ALPHA_HOT_UNIVERSE_PREMIUM_SEGMENT_SHARE_CAP,
  ALPHA_HOT_UNIVERSE_PREMIUM_SEGMENT_MIN,
  ALPHA_HOT_UNIVERSE_MAX_CONSECUTIVE_CATEGORY,
  ALPHA_HOT_UNIVERSE_MAX_CONSECUTIVE_SUBTYPE,
  ALPHA_HOT_UNIVERSE_MAX_CONSECUTIVE_FAMILY,
  ALPHA_HOT_UNIVERSE_MIN_LIQUIDITY_RANK,
  ALPHA_HOT_UNIVERSE_MIN_VOLUME_7D,
  ALPHA_HOT_UNIVERSE_MIN_MATURITY_SCORE
} = require("./config")
const { normalizeCatalogRow } = require("./candidateSelector")
const { classifyCatalogState } = require("./stateModel")

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function emptyCategoryMap() {
  return Object.fromEntries(SCAN_COHORT_CATEGORIES.map((category) => [category, 0]))
}

function emptyStateMap() {
  return {
    eligible: 0,
    near_eligible: 0
  }
}

function emptySubtypeMap() {
  return {}
}

function incrementCounter(target = {}, key = "", amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

function normalizePriorityTierRank(value = null) {
  const tier = normalizeText(value).toLowerCase()
  if (tier === "tier_a") return 2
  if (tier === "tier_b") return 1
  return 0
}

function resolveFreshnessRank(state = "") {
  const normalized = normalizeText(state).toLowerCase()
  if (normalized === "fresh") return 2
  if (normalized === "aging") return 1
  return 0
}

function normalizeSelectionFamilyName(value = "") {
  return normalizeText(value)
    .replace(/^stattrak(?:\u2122|Ã¢â€žÂ¢)?\s*/i, "")
    .replace(/^souvenir\s+/i, "")
    .replace(/\((factory new|minimal wear|field-tested|well-worn|battle-scarred)\)\s*$/i, "")
    .replace(/^\u2605\s*/u, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
}

function inferWeaponSegment(row = {}) {
  const explicit = normalizeText(row.itemSubcategory || row.subcategory).toLowerCase()
  if (explicit) return explicit
  const marketHashName = normalizeText(row.marketHashName || row.market_hash_name || row.itemName || row.item_name)
  const [prefix] = marketHashName.split("|")
  const normalizedPrefix = normalizeText(prefix)
  if (
    ["AK-47", "M4A1-S", "M4A4", "FAMAS", "Galil AR", "SG 553", "AUG"].includes(normalizedPrefix)
  ) {
    return "rifle"
  }
  if (["AWP", "SSG 08", "SCAR-20", "G3SG1"].includes(normalizedPrefix)) {
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
    ].includes(normalizedPrefix)
  ) {
    return "pistol"
  }
  if (["MP9", "MP7", "MP5-SD", "MAC-10", "UMP-45", "P90", "PP-Bizon"].includes(normalizedPrefix)) {
    return "smg"
  }
  if (["XM1014", "Nova", "MAG-7", "Sawed-Off"].includes(normalizedPrefix)) {
    return "shotgun"
  }
  if (["Negev", "M249"].includes(normalizedPrefix)) {
    return "machine_gun"
  }
  return "weapon_skin"
}

function resolveItemSubtype(row = {}) {
  const category = normalizeText(row.category).toLowerCase()
  if (category === ITEM_CATEGORIES.WEAPON_SKIN) {
    return inferWeaponSegment(row)
  }
  return normalizeText(row.itemSubcategory || row.subcategory).toLowerCase() || category
}

function resolveWeaponFamily(row = {}) {
  const marketHashName = normalizeText(
    row.marketHashName || row.market_hash_name || row.itemName || row.item_name
  )
  const [prefix] = marketHashName.split("|")
  return normalizeSelectionFamilyName(prefix || marketHashName) || "weapon_skin"
}

function resolveStatTrakBucket(row = {}) {
  const marketHashName = normalizeText(
    row.marketHashName || row.market_hash_name || row.itemName || row.item_name
  )
  return /stattrak/i.test(marketHashName) ? "stattrak" : "standard"
}

function resolvePremiumSegment(row = {}) {
  const marketHashName = normalizeText(
    row.marketHashName || row.market_hash_name || row.itemName || row.item_name
  )
  if (/stattrak/i.test(marketHashName)) return "stattrak"
  if (/souvenir/i.test(marketHashName)) return "souvenir"
  const category = normalizeText(row.category).toLowerCase()
  if (category === ITEM_CATEGORIES.KNIFE || category === ITEM_CATEGORIES.GLOVE) {
    return "premium"
  }
  return "standard"
}

function computeAlphaPriorityScore(row = {}, meta = {}) {
  const candidateStatus = normalizeText(row.candidateStatus).toLowerCase()
  const freshnessState = normalizeText(meta?.classification?.freshness?.state).toLowerCase()
  const marketCoverageCount = Math.max(Number(row.marketCoverageCount || 0), 0)
  const referencePrice = Number(row.referencePrice || 0)
  const liquidityRank = Number(row.liquidityRank || 0)
  const priorityTierRank = normalizePriorityTierRank(row.priorityTier)
  const priorityBoost = Number(row.priorityBoost || 0)
  const maturityScore = Math.max(Number(toFiniteOrNull(row.raw?.maturity_score ?? row.raw?.maturityScore) || 0), 0)
  const volume7d = Math.max(Number(row.volume7d || 0), 0)
  const opportunityScore = Math.max(
    Number(
      toFiniteOrNull(
        row.raw?.opportunity_score ??
          row.raw?.opportunityScore ??
          row.raw?.score
      ) || 0
    ),
    0
  )
  const lastSignalTs = Number(new Date(toIsoOrNull(row.lastMarketSignalAt) || 0).getTime() || 0)
  const nowMs = Number(meta?.nowMs || Date.now())
  const recencyMinutes = lastSignalTs > 0 ? Math.max((nowMs - lastSignalTs) / (60 * 1000), 0) : null

  let score = 0
  score += candidateStatus === "eligible" ? 120 : candidateStatus === "near_eligible" ? 78 : 0
  score += resolveFreshnessRank(freshnessState) * 18
  score += Math.min(marketCoverageCount * 6, 30)
  score += referencePrice > 0 ? Math.min(referencePrice / 5, 12) : 0
  score += Math.min(liquidityRank, 100) / 6
  score += priorityTierRank * 16
  score += Math.min(priorityBoost, 50) / 2
  score += Math.min(maturityScore / 8, 10)
  score += Math.min(volume7d / 40, 8)
  score += Math.min(opportunityScore / 12, 8)
  if (recencyMinutes != null) {
    if (recencyMinutes <= 30) score += 12
    else if (recencyMinutes <= 120) score += 6
  }
  return Number(score.toFixed(2))
}

function buildCategoryQuotas(targetSize = 0, availableCounts = {}) {
  const safeTargetSize = Math.max(Math.round(Number(targetSize || 0)), 1)
  const quotas = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: 0,
    [ITEM_CATEGORIES.CASE]: 0,
    [ITEM_CATEGORIES.STICKER_CAPSULE]: 0
  }
  const availableCategories = SCAN_COHORT_CATEGORIES.filter(
    (category) => Number(availableCounts?.[category] || 0) > 0
  )
  if (!availableCategories.length) {
    return quotas
  }

  const weightMap = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: ALPHA_HOT_UNIVERSE_WEAPON_SKIN_SHARE,
    [ITEM_CATEGORIES.CASE]: ALPHA_HOT_UNIVERSE_CASE_SHARE,
    [ITEM_CATEGORIES.STICKER_CAPSULE]: ALPHA_HOT_UNIVERSE_STICKER_CAPSULE_SHARE
  }
  const maxMap = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: safeTargetSize,
    [ITEM_CATEGORIES.CASE]: ALPHA_HOT_UNIVERSE_CASE_MAX,
    [ITEM_CATEGORIES.STICKER_CAPSULE]: ALPHA_HOT_UNIVERSE_STICKER_CAPSULE_MAX
  }

  let remaining = safeTargetSize
  if (safeTargetSize >= availableCategories.length) {
    for (const category of availableCategories) {
      const baseline = Math.min(
        ALPHA_HOT_UNIVERSE_MIN_PER_AVAILABLE_CATEGORY,
        Number(availableCounts?.[category] || 0),
        Number(maxMap?.[category] || 0)
      )
      quotas[category] = baseline
      remaining -= baseline
    }
  }

  const totalWeight = availableCategories.reduce(
    (sum, category) => sum + Number(weightMap?.[category] || 0),
    0
  )
  const remainders = []
  for (const category of availableCategories) {
    const weight = Number(weightMap?.[category] || 0)
    const exact = remaining > 0 ? (remaining * weight) / Math.max(totalWeight, 1) : 0
    const cap = Math.min(
      Number(maxMap?.[category] || 0),
      Number(availableCounts?.[category] || 0)
    )
    const allocation = Math.min(Math.floor(exact), Math.max(cap - Number(quotas[category] || 0), 0))
    quotas[category] += allocation
    remainders.push({
      category,
      remainder: exact - Math.floor(exact)
    })
  }

  let assigned = Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
  for (const entry of remainders.sort((a, b) => b.remainder - a.remainder)) {
    if (assigned >= safeTargetSize) break
    const category = entry.category
    const cap = Math.min(
      Number(maxMap?.[category] || 0),
      Number(availableCounts?.[category] || 0)
    )
    if (Number(quotas?.[category] || 0) >= cap) continue
    quotas[category] += 1
    assigned += 1
  }

  while (assigned < safeTargetSize) {
    const spillCategory = availableCategories
      .slice()
      .sort((left, right) => Number(weightMap?.[right] || 0) - Number(weightMap?.[left] || 0))
      .find((category) => {
        const cap = Math.min(
          Number(maxMap?.[category] || 0),
          Number(availableCounts?.[category] || 0)
        )
        return Number(quotas?.[category] || 0) < cap
      })
    if (!spillCategory) break
    quotas[spillCategory] += 1
    assigned += 1
  }

  return quotas
}

function buildNearEligibleCategoryQuotas(totalCap = 0, categoryQuotas = {}, availableCounts = {}) {
  const safeTotalCap = Math.max(Math.round(Number(totalCap || 0)), 0)
  const categories = SCAN_COHORT_CATEGORIES.filter((category) => Number(availableCounts?.[category] || 0) > 0)
  const allocations = emptyCategoryMap()
  if (!safeTotalCap || !categories.length) {
    return allocations
  }

  const totalQuotaWeight = categories.reduce(
    (sum, category) => sum + Math.max(Number(categoryQuotas?.[category] || 0), 1),
    0
  )
  const remainders = []
  let assigned = 0
  for (const category of categories) {
    const weight = Math.max(Number(categoryQuotas?.[category] || 0), 1)
    const exact = (safeTotalCap * weight) / Math.max(totalQuotaWeight, 1)
    const base = Math.min(Math.floor(exact), Number(availableCounts?.[category] || 0))
    allocations[category] = base
    assigned += base
    remainders.push({
      category,
      remainder: exact - Math.floor(exact)
    })
  }

  for (const category of categories) {
    if (assigned >= safeTotalCap) break
    if (allocations[category] > 0) continue
    allocations[category] = 1
    assigned += 1
  }

  for (const entry of remainders.sort((a, b) => b.remainder - a.remainder)) {
    if (assigned >= safeTotalCap) break
    const category = entry.category
    const maxAvailable = Number(availableCounts?.[category] || 0)
    if (allocations[category] >= maxAvailable) continue
    allocations[category] += 1
    assigned += 1
  }

  return allocations
}

function buildSelectionMeta(row = {}, classification = {}, nowMs = Date.now()) {
  const category = normalizeText(row.category).toLowerCase()
  const freshnessState = normalizeText(classification?.freshness?.state).toLowerCase() || "missing"
  const segment = resolveItemSubtype(row)
  const family =
    category === ITEM_CATEGORIES.WEAPON_SKIN
      ? resolveWeaponFamily(row)
      : normalizeSelectionFamilyName(row.itemName || row.marketHashName) || category
  const stattrakBucket = category === ITEM_CATEGORIES.WEAPON_SKIN ? resolveStatTrakBucket(row) : "not_applicable"
  const premiumSegment = resolvePremiumSegment(row)
  const priorityScore = computeAlphaPriorityScore(row, {
    classification,
    nowMs
  })
  return {
    freshnessState,
    segment,
    family,
    stattrakBucket,
    premiumSegment,
    priorityScore
  }
}

function sortCandidates(a, b) {
  const aScore = Number(a?.alphaMeta?.priorityScore || 0)
  const bScore = Number(b?.alphaMeta?.priorityScore || 0)
  if (aScore !== bScore) return bScore - aScore
  const aSignal = Number(new Date(toIsoOrNull(a?.lastMarketSignalAt) || 0).getTime() || 0)
  const bSignal = Number(new Date(toIsoOrNull(b?.lastMarketSignalAt) || 0).getTime() || 0)
  if (aSignal !== bSignal) return bSignal - aSignal
  const aBoost = Number(a?.priorityBoost || 0)
  const bBoost = Number(b?.priorityBoost || 0)
  if (aBoost !== bBoost) return bBoost - aBoost
  const aLiquidity = Number(a?.liquidityRank || 0)
  const bLiquidity = Number(b?.liquidityRank || 0)
  if (aLiquidity !== bLiquidity) return bLiquidity - aLiquidity
  return String(a?.marketHashName || "").localeCompare(String(b?.marketHashName || ""))
}

function shouldExcludeForLowMaturity(row = {}) {
  const candidateStatus = normalizeText(row.candidateStatus).toLowerCase()
  const scanCohort = normalizeText(row.scanCohort).toLowerCase()
  return !(
    (candidateStatus === "eligible" && scanCohort === "hot") ||
    (candidateStatus === "near_eligible" && scanCohort === "warm")
  )
}

function isLowQualityCandidate(row = {}, classification = {}) {
  const referencePrice = Number(row.referencePrice || 0)
  const marketCoverageCount = Math.max(Number(row.marketCoverageCount || 0), 0)
  const liquidityRank = Math.max(Number(row.liquidityRank || 0), 0)
  const volume7d = Math.max(Number(row.volume7d || 0), 0)
  const rawMaturityScore = toFiniteOrNull(
    row.raw?.maturity_score ?? row.raw?.maturityScore
  )
  const maturityScore = Math.max(Number(rawMaturityScore || 0), 0)
  const candidateStatus = normalizeText(row.candidateStatus).toLowerCase()
  const freshnessState = normalizeText(classification?.freshness?.state).toLowerCase()

  if (referencePrice <= 0 || marketCoverageCount < 2) return true
  if (freshnessState !== "fresh" && freshnessState !== "aging") return true
  if (liquidityRank < ALPHA_HOT_UNIVERSE_MIN_LIQUIDITY_RANK && volume7d < ALPHA_HOT_UNIVERSE_MIN_VOLUME_7D) {
    return true
  }
  if (
    candidateStatus !== "eligible" &&
    rawMaturityScore != null &&
    maturityScore < ALPHA_HOT_UNIVERSE_MIN_MATURITY_SCORE
  ) {
    return true
  }
  return false
}

function selectCategoryRows(candidates = [], quota = 0, options = {}) {
  const safeQuota = Math.max(Math.round(Number(quota || 0)), 0)
  if (!safeQuota) {
    return {
      rows: [],
      diversityApplied: false
    }
  }

  const safeCandidates = Array.isArray(candidates) ? candidates.slice().sort(sortCandidates) : []
  const selected = []
  const seedRows = Array.isArray(options.seedRows) ? options.seedRows : []
  const familyCounts = {}
  const segmentCounts = {}
  const premiumCounts = {}
  let diversityApplied = false
  const category = normalizeText(safeCandidates[0]?.category || seedRows[0]?.category).toLowerCase()
  const capBase = Math.max(safeQuota + seedRows.length, safeQuota)
  const familyCap =
    category === ITEM_CATEGORIES.WEAPON_SKIN
      ? Math.max(
          ALPHA_HOT_UNIVERSE_WEAPON_FAMILY_MIN,
          Math.min(3, Math.ceil(capBase * ALPHA_HOT_UNIVERSE_WEAPON_FAMILY_SHARE_CAP))
        )
      : Math.max(1, Math.ceil(capBase * 0.5))
  const segmentCap =
    category === ITEM_CATEGORIES.WEAPON_SKIN
      ? Math.max(
          ALPHA_HOT_UNIVERSE_WEAPON_SEGMENT_MIN,
          Math.min(5, Math.ceil(capBase * ALPHA_HOT_UNIVERSE_WEAPON_SEGMENT_SHARE_CAP))
        )
      : capBase
  const stattrakCap =
    category === ITEM_CATEGORIES.WEAPON_SKIN
      ? Math.max(
          ALPHA_HOT_UNIVERSE_PREMIUM_SEGMENT_MIN,
          Math.min(4, Math.ceil(capBase * ALPHA_HOT_UNIVERSE_PREMIUM_SEGMENT_SHARE_CAP))
        )
      : capBase

  for (const row of seedRows) {
    const family = normalizeText(row?.alphaMeta?.family).toLowerCase() || "unknown"
    const segment = normalizeText(row?.alphaMeta?.segment).toLowerCase() || "unknown"
    const premiumSegment =
      normalizeText(row?.alphaMeta?.premiumSegment).toLowerCase() || "standard"
    familyCounts[family] = Number(familyCounts[family] || 0) + 1
    segmentCounts[segment] = Number(segmentCounts[segment] || 0) + 1
    if (premiumSegment !== "standard") {
      premiumCounts[premiumSegment] = Number(premiumCounts[premiumSegment] || 0) + 1
    }
  }

  const primaryPass = []
  const deferred = []
  for (const row of safeCandidates) {
    const family = normalizeText(row?.alphaMeta?.family).toLowerCase() || "unknown"
    const segment = normalizeText(row?.alphaMeta?.segment).toLowerCase() || "unknown"
    const premiumSegment =
      normalizeText(row?.alphaMeta?.premiumSegment).toLowerCase() || "standard"
    const nextFamilyCount = Number(familyCounts[family] || 0) + 1
    const nextSegmentCount = Number(segmentCounts[segment] || 0) + 1
    const nextPremiumCount =
      premiumSegment === "standard"
        ? 0
        : Number(premiumCounts[premiumSegment] || 0) + 1

    if (
      nextFamilyCount > familyCap ||
      nextSegmentCount > segmentCap ||
      (premiumSegment !== "standard" && nextPremiumCount > stattrakCap)
    ) {
      deferred.push(row)
      diversityApplied = true
      continue
    }

    primaryPass.push(row)
    familyCounts[family] = nextFamilyCount
    segmentCounts[segment] = nextSegmentCount
    if (premiumSegment !== "standard") {
      premiumCounts[premiumSegment] = nextPremiumCount
    }
    if (primaryPass.length >= safeQuota) break
  }

  selected.push(...primaryPass.slice(0, safeQuota))
  if (selected.length >= safeQuota) {
    return {
      rows: selected,
      diversityApplied
    }
  }

  for (const row of deferred) {
    if (selected.length >= safeQuota) break
    selected.push(row)
  }
  return {
    rows: selected,
    diversityApplied
  }
}

function rebalanceUniverseRows(rows = [], options = {}) {
  const safeRows = Array.isArray(rows) ? rows.slice() : []
  if (safeRows.length <= 2) {
    return {
      rows: safeRows,
      diversityApplied: false
    }
  }

  const queue = safeRows.slice().sort(sortCandidates)
  const selected = []
  let previousCategory = ""
  let previousSubtype = ""
  let previousFamily = ""
  let categoryStreak = 0
  let subtypeStreak = 0
  let familyStreak = 0
  let diversityApplied = false

  while (queue.length) {
    let pickIndex = 0
    if (selected.length) {
      const alternativeIndex = queue.findIndex((row) => {
        const category = normalizeText(row?.category).toLowerCase()
        const subtype = normalizeText(row?.alphaMeta?.segment).toLowerCase()
        const family = normalizeText(row?.alphaMeta?.family).toLowerCase()
        const nextCategoryStreak =
          category && category === previousCategory ? categoryStreak + 1 : 1
        const nextSubtypeStreak =
          subtype && subtype === previousSubtype ? subtypeStreak + 1 : 1
        const nextFamilyStreak =
          family && family === previousFamily ? familyStreak + 1 : 1
        return (
          nextCategoryStreak <= ALPHA_HOT_UNIVERSE_MAX_CONSECUTIVE_CATEGORY &&
          nextSubtypeStreak <= ALPHA_HOT_UNIVERSE_MAX_CONSECUTIVE_SUBTYPE &&
          nextFamilyStreak <= ALPHA_HOT_UNIVERSE_MAX_CONSECUTIVE_FAMILY
        )
      })
      if (alternativeIndex > 0) {
        pickIndex = alternativeIndex
        diversityApplied = true
      }
    }

    const [picked] = queue.splice(pickIndex, 1)
    if (!picked) continue
    selected.push(picked)
    const category = normalizeText(picked?.category).toLowerCase()
    const subtype = normalizeText(picked?.alphaMeta?.segment).toLowerCase()
    const family = normalizeText(picked?.alphaMeta?.family).toLowerCase()
    categoryStreak = category && category === previousCategory ? categoryStreak + 1 : 1
    subtypeStreak = subtype && subtype === previousSubtype ? subtypeStreak + 1 : 1
    familyStreak = family && family === previousFamily ? familyStreak + 1 : 1
    previousCategory = category
    previousSubtype = subtype
    previousFamily = family
  }

  return {
    rows: selected,
    diversityApplied
  }
}

function patchSelectedRow(row = {}, index = 0) {
  return {
    ...row.raw,
    scanCohort: row.scanCohort,
    fallbackSource: row.fallbackSource || null,
    sourceOrigin: row.sourceOrigin || row.raw?.sourceOrigin || null,
    alpha_hot_universe_source: "alpha_hot_universe",
    alpha_hot_priority_score: Number(row.alphaMeta?.priorityScore || 0),
    alpha_hot_rank: index + 1,
    alpha_hot_state_rank: normalizeText(row.candidateStatus).toLowerCase() === "eligible" ? 2 : 1,
    alpha_hot_freshness_state: row.alphaMeta?.freshnessState || null,
    alpha_hot_diversity_bucket:
      row.category === ITEM_CATEGORIES.WEAPON_SKIN
        ? `${row.alphaMeta?.segment || "weapon_skin"}:${row.alphaMeta?.family || "unknown"}:${row.alphaMeta?.premiumSegment || row.alphaMeta?.stattrakBucket || "standard"}`
        : `${row.category}:${row.alphaMeta?.family || row.category}`,
    alpha_hot_category_quota_bucket: row.category
  }
}

function buildAlphaHotUniverse(options = {}) {
  const batchSize = Math.max(
    Math.round(Number(options.batchSize || OPPORTUNITY_BATCH_RUNTIME_TARGET)),
    1
  )
  const allowNearEligible = options.allowNearEligible !== false
  const defaultTargetSize = batchSize + Math.min(batchSize, 60)
  const totalTarget = Math.max(
    Math.round(Number(options.targetSize || defaultTargetSize)),
    ALPHA_HOT_UNIVERSE_MIN_SIZE
  )
  const nearEligibleCap = allowNearEligible
    ? Math.min(
        Math.max(Math.ceil(totalTarget * ALPHA_HOT_UNIVERSE_NEAR_ELIGIBLE_SHARE), 1),
        ALPHA_HOT_UNIVERSE_NEAR_ELIGIBLE_MAX
      )
    : 0

  const diagnostics = {
    selection_layer: "alpha_hot_universe",
    hot_universe_size: 0,
    hot_universe_by_category: emptyCategoryMap(),
    hot_universe_by_state: emptyStateMap(),
    intake_by_category: emptyCategoryMap(),
    intake_by_subtype: emptySubtypeMap(),
    rows_excluded_for_staleness: 0,
    rows_excluded_for_missing_coverage: 0,
    rows_excluded_for_low_maturity: 0,
    excluded_for_low_quality: 0,
    diversity_rebalancing_applied: false,
    near_eligible_allowed: allowNearEligible,
    near_eligible_cap: nearEligibleCap,
    category_quotas: buildCategoryQuotas(totalTarget, {}),
    quota_hits_by_category: emptyCategoryMap(),
    quota_skips_by_category: emptyCategoryMap()
  }

  const seen = new Set()
  const candidatesByCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: {
      eligible: [],
      near_eligible: []
    },
    [ITEM_CATEGORIES.CASE]: {
      eligible: [],
      near_eligible: []
    },
    [ITEM_CATEGORIES.STICKER_CAPSULE]: {
      eligible: [],
      near_eligible: []
    }
  }

  for (const input of Array.isArray(options.rows) ? options.rows : []) {
    const row = normalizeCatalogRow(input)
    if (!row) continue
    if (!SCAN_COHORT_CATEGORIES.includes(row.category)) continue
    if (seen.has(row.marketHashName)) continue
    seen.add(row.marketHashName)

    const classification = classifyCatalogState({
      ...input,
      ...row
    })

    if (classification.state === "hard_reject") {
      diagnostics.rows_excluded_for_low_maturity += 1
      continue
    }
    if (normalizeText(row.catalogStatus).toLowerCase() !== "scannable") {
      diagnostics.rows_excluded_for_low_maturity += 1
      continue
    }
    if (shouldExcludeForLowMaturity(row)) {
      diagnostics.rows_excluded_for_low_maturity += 1
      continue
    }

    const referencePrice = Number(row.referencePrice || 0)
    const marketCoverageCount = Math.max(Number(row.marketCoverageCount || 0), 0)
    if (referencePrice <= 0 || marketCoverageCount < 2) {
      diagnostics.rows_excluded_for_missing_coverage += 1
      continue
    }

    const freshnessState = normalizeText(classification?.freshness?.state).toLowerCase()
    if (freshnessState !== "fresh" && freshnessState !== "aging") {
      diagnostics.rows_excluded_for_staleness += 1
      continue
    }

    const candidateStatus = normalizeText(row.candidateStatus).toLowerCase()
    if (candidateStatus === "near_eligible" && !allowNearEligible) {
      diagnostics.rows_excluded_for_low_maturity += 1
      continue
    }
    if (isLowQualityCandidate(row, classification)) {
      diagnostics.excluded_for_low_quality += 1
      continue
    }

    const alphaMeta = buildSelectionMeta(row, classification, options.nowMs)
    const bucket = candidatesByCategory[row.category]
    if (!bucket) {
      diagnostics.rows_excluded_for_low_maturity += 1
      continue
    }
    bucket[candidateStatus].push({
      ...row,
      alphaMeta
    })
  }

  const availableCounts = Object.fromEntries(
    SCAN_COHORT_CATEGORIES.map((category) => [
      category,
      Number(candidatesByCategory?.[category]?.eligible?.length || 0) +
        Number(candidatesByCategory?.[category]?.near_eligible?.length || 0)
    ])
  )
  diagnostics.category_quotas = buildCategoryQuotas(totalTarget, availableCounts)

  const selectedRows = []
  const nearEligibleAvailableCounts = Object.fromEntries(
    SCAN_COHORT_CATEGORIES.map((category) => [
      category,
      Number(candidatesByCategory?.[category]?.near_eligible?.length || 0)
    ])
  )
  const nearEligibleCategoryQuotas = buildNearEligibleCategoryQuotas(
    nearEligibleCap,
    diagnostics.category_quotas,
    nearEligibleAvailableCounts
  )
  diagnostics.near_eligible_category_quotas = nearEligibleCategoryQuotas
  const selectedByCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: [],
    [ITEM_CATEGORIES.CASE]: [],
    [ITEM_CATEGORIES.STICKER_CAPSULE]: []
  }
  for (const category of SCAN_COHORT_CATEGORIES) {
    const quota = Number(diagnostics.category_quotas?.[category] || 0)
    const categoryBucket = candidatesByCategory[category]
    if (!categoryBucket || quota <= 0) continue

    const eligibleSelection = selectCategoryRows(categoryBucket.eligible, quota, {
      category
    })
    selectedRows.push(...eligibleSelection.rows)
    selectedByCategory[category].push(...eligibleSelection.rows)
    diagnostics.diversity_rebalancing_applied =
      diagnostics.diversity_rebalancing_applied || eligibleSelection.diversityApplied

    const remainingQuota = Math.max(quota - eligibleSelection.rows.length, 0)
    const nearQuota = Math.min(
      remainingQuota,
      Number(nearEligibleCategoryQuotas?.[category] || 0)
    )
    if (nearQuota > 0) {
      const nearSelection = selectCategoryRows(categoryBucket.near_eligible, nearQuota, {
        category,
        seedRows: selectedByCategory[category]
      })
      selectedRows.push(...nearSelection.rows)
      selectedByCategory[category].push(...nearSelection.rows)
      diagnostics.diversity_rebalancing_applied =
        diagnostics.diversity_rebalancing_applied || nearSelection.diversityApplied
    }
    const skippedForQuota = Math.max(
      Number(categoryBucket.eligible.length + categoryBucket.near_eligible.length) -
        Number(selectedByCategory[category].length),
      0
    )
    if (
      quota > 0 &&
      (Number(selectedByCategory[category].length) >= quota || skippedForQuota > 0)
    ) {
      diagnostics.quota_hits_by_category[category] =
        Number(diagnostics.quota_hits_by_category[category] || 0) + 1
    }
    diagnostics.quota_skips_by_category[category] = skippedForQuota
  }

  const selectedNames = new Set(selectedRows.map((row) => row.marketHashName))
  const spilloverEligible = []
  const spilloverNear = []
  for (const category of SCAN_COHORT_CATEGORIES) {
    const categoryBucket = candidatesByCategory[category]
    if (!categoryBucket) continue
    for (const row of categoryBucket.eligible) {
      if (!selectedNames.has(row.marketHashName)) spilloverEligible.push(row)
    }
    for (const row of categoryBucket.near_eligible) {
      if (!selectedNames.has(row.marketHashName)) spilloverNear.push(row)
    }
  }

  const spilloverEligibleByCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: spilloverEligible.filter((row) => row.category === ITEM_CATEGORIES.WEAPON_SKIN),
    [ITEM_CATEGORIES.CASE]: spilloverEligible.filter((row) => row.category === ITEM_CATEGORIES.CASE),
    [ITEM_CATEGORIES.STICKER_CAPSULE]: spilloverEligible.filter((row) => row.category === ITEM_CATEGORIES.STICKER_CAPSULE)
  }
  const spilloverNearByCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: spilloverNear.filter((row) => row.category === ITEM_CATEGORIES.WEAPON_SKIN),
    [ITEM_CATEGORIES.CASE]: spilloverNear.filter((row) => row.category === ITEM_CATEGORIES.CASE),
    [ITEM_CATEGORIES.STICKER_CAPSULE]: spilloverNear.filter((row) => row.category === ITEM_CATEGORIES.STICKER_CAPSULE)
  }

  for (const category of SCAN_COHORT_CATEGORIES) {
    if (selectedRows.length >= totalTarget) break
    const remainingSlots = Math.max(totalTarget - selectedRows.length, 0)
    if (!remainingSlots) break
    const spilloverSelection = selectCategoryRows(spilloverEligibleByCategory[category], remainingSlots, {
      category,
      seedRows: selectedByCategory[category]
    })
    for (const row of spilloverSelection.rows) {
      if (!row || selectedNames.has(row.marketHashName)) continue
      selectedRows.push(row)
      selectedNames.add(row.marketHashName)
      selectedByCategory[category].push(row)
    }
    diagnostics.diversity_rebalancing_applied =
      diagnostics.diversity_rebalancing_applied || spilloverSelection.diversityApplied
  }

  for (const category of SCAN_COHORT_CATEGORIES) {
    if (selectedRows.length >= totalTarget) break
    const remainingNearQuota = Math.max(
      Number(nearEligibleCategoryQuotas?.[category] || 0) - selectedByCategory[category].filter(
        (row) => normalizeText(row?.candidateStatus).toLowerCase() === "near_eligible"
      ).length,
      0
    )
    if (!remainingNearQuota) continue
    const remainingSlots = Math.max(Math.min(totalTarget - selectedRows.length, remainingNearQuota), 0)
    if (!remainingSlots) break
    const spilloverSelection = selectCategoryRows(spilloverNearByCategory[category], remainingSlots, {
      category,
      seedRows: selectedByCategory[category]
    })
    for (const row of spilloverSelection.rows) {
      if (!row || selectedNames.has(row.marketHashName)) continue
      selectedRows.push(row)
      selectedNames.add(row.marketHashName)
      selectedByCategory[category].push(row)
    }
    diagnostics.diversity_rebalancing_applied =
      diagnostics.diversity_rebalancing_applied || spilloverSelection.diversityApplied
  }

  const finalSelection = rebalanceUniverseRows(selectedRows, options)
  diagnostics.diversity_rebalancing_applied =
    diagnostics.diversity_rebalancing_applied || Boolean(finalSelection.diversityApplied)
  const patchedRows = finalSelection.rows.map((row, index) => patchSelectedRow(row, index))
  diagnostics.hot_universe_size = patchedRows.length
  for (const row of finalSelection.rows) {
    incrementCounter(diagnostics.hot_universe_by_category, row.category, 1)
    incrementCounter(diagnostics.intake_by_category, row.category, 1)
    incrementCounter(
      diagnostics.hot_universe_by_state,
      normalizeText(row.candidateStatus).toLowerCase() === "eligible" ? "eligible" : "near_eligible",
      1
    )
    incrementCounter(diagnostics.intake_by_subtype, row.alphaMeta?.segment || row.category, 1)
  }

  return {
    rows: patchedRows,
    diagnostics
  }
}

module.exports = {
  buildAlphaHotUniverse,
  __testables: {
    buildCategoryQuotas,
    buildNearEligibleCategoryQuotas,
    inferWeaponSegment,
    resolveItemSubtype,
    resolveWeaponFamily,
    resolvePremiumSegment,
    computeAlphaPriorityScore,
    selectCategoryRows,
    rebalanceUniverseRows,
    isLowQualityCandidate
  }
}

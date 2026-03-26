const prioritySetConfig = require("../config/catalogPriorityCoverageSet")
const catalogPrioritySetRepo = require("../repositories/catalogPrioritySetRepository")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")

const CATEGORY_MAP = Object.freeze({
  skin: "weapon_skin",
  weapon_skin: "weapon_skin",
  case: "case",
  knife: "knife",
  glove: "glove"
})
const TIER_ORDER = Object.freeze({
  tier_a: 2,
  tier_b: 1
})
const SNAPSHOT_PRIORITY_WEIGHTS = Object.freeze({
  highest: 1.15,
  high: 1.08,
  normal: 1
})
const SCAN_BUDGET_WEIGHTS = Object.freeze({
  highest: 1.18,
  high: 1.08,
  medium: 1,
  normal: 0.95
})
const FRESHNESS_TARGET_WEIGHTS = Object.freeze({
  strongest: 1.12,
  strong: 1.08,
  normal: 1
})
const COVERAGE_TARGET_WEIGHTS = Object.freeze({
  strongest: 1.12,
  strong: 1.08,
  normal: 1
})
const WEAR_SUFFIX_PATTERN =
  /\((factory new|minimal wear|field-tested|well-worn|battle-scarred)\)\s*$/i

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizePriorityCategory(value = "") {
  const raw = normalizeText(value).toLowerCase()
  return CATEGORY_MAP[raw] || ""
}

function normalizePriorityTier(value = "") {
  const tier = normalizeText(value).toLowerCase()
  return tier === "tier_a" || tier === "tier_b" ? tier : ""
}

function foldTextForKey(value = "") {
  return normalizeText(value)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .toLowerCase()
}

function canonicalizeItemName(value = "", category = "weapon_skin") {
  let name = normalizeText(value)
  if (!name) return ""
  name = name
    .replace(/^★\s*/u, "")
    .replace(/^stattrak(?:™|\u2122)?\s*/i, "")
    .replace(/^souvenir\s+/i, "")
    .replace(/\s+/g, " ")
    .trim()
  if (category === "weapon_skin" || category === "knife" || category === "glove") {
    name = name.replace(WEAR_SUFFIX_PATTERN, "").trim()
  }
  return name
}

function buildPriorityKey(category = "", canonicalItemName = "") {
  const safeCategory = normalizePriorityCategory(category)
  const safeName = canonicalizeItemName(canonicalItemName, safeCategory)
  if (!safeCategory || !safeName) return ""
  return `${safeCategory}::${foldTextForKey(safeName)}`
}

function computePriorityBoost(tier = "", rank = 1, policyHints = {}) {
  const safeTier = normalizePriorityTier(tier)
  const safeRank = Math.max(Math.round(Number(rank || 1)), 1)
  const snapshotPriority = normalizeText(policyHints?.snapshot_priority).toLowerCase() || "normal"
  const scanBudget = normalizeText(policyHints?.scan_budget).toLowerCase() || "normal"
  const freshnessTarget = normalizeText(policyHints?.freshness_target).toLowerCase() || "normal"
  const coverageTarget = normalizeText(policyHints?.coverage_target).toLowerCase() || "normal"
  const recoveryWeight = Math.max(Number(policyHints?.recovery_weight || 1), 0.2)
  const snapshotWeight = Number(SNAPSHOT_PRIORITY_WEIGHTS[snapshotPriority] || 1)
  const scanBudgetWeight = Number(SCAN_BUDGET_WEIGHTS[scanBudget] || 1)
  const freshnessWeight = Number(FRESHNESS_TARGET_WEIGHTS[freshnessTarget] || 1)
  const coverageWeight = Number(COVERAGE_TARGET_WEIGHTS[coverageTarget] || 1)
  const hintsWeight = Math.min(
    Math.max(snapshotWeight * scanBudgetWeight * freshnessWeight * coverageWeight, 0.7),
    1.9
  )
  const base = safeTier === "tier_a" ? 420 : 260
  const floor = safeTier === "tier_a" ? 260 : 120
  const rankAdjusted = Math.max(base - safeRank * 1.5, floor)
  return Number((rankAdjusted * recoveryWeight * hintsWeight).toFixed(2))
}

function buildPriorityEntries(config = prioritySetConfig) {
  const safeConfig = config && typeof config === "object" ? config : {}
  const version = Math.max(Math.round(Number(safeConfig.version || 1)), 1)
  const setName = normalizeText(safeConfig.name)
  const description = normalizeText(safeConfig.description) || null
  const tiers = safeConfig.tiers && typeof safeConfig.tiers === "object" ? safeConfig.tiers : {}
  const orderedTierNames = Object.keys(TIER_ORDER).sort((a, b) => Number(TIER_ORDER[b]) - Number(TIER_ORDER[a]))
  const entriesByKey = new Map()

  for (const tierName of orderedTierNames) {
    const tier = tiers[tierName]
    if (!tier || typeof tier !== "object") continue
    const normalizedTier = normalizePriorityTier(tierName)
    if (!normalizedTier) continue
    const policyHints = tier.policy_hints && typeof tier.policy_hints === "object" ? tier.policy_hints : {}
    const items = Array.isArray(tier.items) ? tier.items : []
    let rank = 1
    for (const rawItem of items) {
      const category = normalizePriorityCategory(rawItem?.category)
      const itemName = normalizeText(rawItem?.item_name || rawItem?.itemName)
      const canonicalItemName = canonicalizeItemName(itemName, category)
      const key = buildPriorityKey(category, canonicalItemName)
      if (!key) continue
      const priorityBoost = computePriorityBoost(normalizedTier, rank, policyHints)
      const next = {
        setName,
        version,
        description,
        tier: normalizedTier,
        rank,
        category,
        itemName,
        canonicalItemName,
        key,
        priorityBoost,
        policyHints
      }
      const previous = entriesByKey.get(key)
      if (!previous) {
        entriesByKey.set(key, next)
      } else {
        const previousTierOrder = Number(TIER_ORDER[previous.tier] || 0)
        const nextTierOrder = Number(TIER_ORDER[next.tier] || 0)
        if (nextTierOrder > previousTierOrder || (nextTierOrder === previousTierOrder && next.rank < previous.rank)) {
          entriesByKey.set(key, next)
        }
      }
      rank += 1
    }
  }

  return {
    setName,
    version,
    description,
    tiers,
    entries: Array.from(entriesByKey.values())
  }
}

function buildCatalogCanonicalIndex(rows = []) {
  const index = new Map()
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue
    const category = normalizePriorityCategory(row?.category)
    const canonicalItemName = canonicalizeItemName(
      normalizeText(row?.item_name || row?.itemName || marketHashName),
      category
    )
    const key = buildPriorityKey(category, canonicalItemName)
    if (!key) continue
    const bucket = index.get(key) || []
    bucket.push({
      marketHashName,
      itemName: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
      category
    })
    index.set(key, bucket)
  }
  return index
}

function buildPriorityCatalogRow(entry = {}) {
  return {
    marketHashName: entry.itemName,
    itemName: entry.itemName,
    category: entry.category,
    subcategory: entry.category === "knife" ? "knife" : entry.category === "glove" ? "glove" : null,
    tradable: true,
    isActive: true,
    sourceTag: "priority_coverage_set",
    scanEligible: false,
    prioritySetName: entry.setName,
    priorityTier: entry.tier,
    priorityRank: entry.rank,
    priorityBoost: entry.priorityBoost,
    isPriorityItem: true
  }
}

async function syncPriorityCoverageSet(options = {}) {
  const sourceConfig = options.config && typeof options.config === "object" ? options.config : prioritySetConfig
  const allowCatalogInsert = Boolean(options.allowCatalogInsert)
  const built = buildPriorityEntries(sourceConfig)
  const setName = built.setName
  if (!setName || !built.entries.length) {
    return {
      setName: setName || null,
      version: built.version || 1,
      description: built.description || null,
      totalPriorityItemsConfigured: 0,
      matchedExistingCatalogItems: 0,
      insertedMissingCatalogItems: 0,
      unmatchedPriorityItems: [],
      entries: [],
      byKey: new Map(),
      policyHintsByTier: {}
    }
  }

  const policyHintsByTier = Object.fromEntries(
    Object.keys(TIER_ORDER).map((tierName) => [
      tierName,
      built.tiers?.[tierName]?.policy_hints && typeof built.tiers[tierName].policy_hints === "object"
        ? built.tiers[tierName].policy_hints
        : {}
    ])
  )

  await catalogPrioritySetRepo.upsertSet({
    set_name: setName,
    version: built.version,
    description: built.description,
    policy_hints: policyHintsByTier,
    raw_payload: sourceConfig,
    is_active: true
  })

  await catalogPrioritySetRepo.replaceItems(
    setName,
    built.entries.map((entry) => ({
      canonical_category: entry.category,
      item_name: entry.itemName,
      canonical_item_name: entry.canonicalItemName,
      priority_tier: entry.tier,
      priority_rank: entry.rank,
      priority_boost: entry.priorityBoost,
      policy_hints: entry.policyHints,
      is_active: true
    }))
  )

  const coverageCategories = ["weapon_skin", "case", "knife", "glove"]
  const catalogRows = await marketSourceCatalogRepo.listActiveTradable({
    limit: 12000,
    categories: coverageCategories
  })
  const byKey = new Map(built.entries.map((entry) => [entry.key, entry]))
  const catalogIndex = buildCatalogCanonicalIndex(catalogRows)
  const missingEntries = []
  let matchedExistingCatalogItems = 0

  for (const entry of built.entries) {
    const matches = catalogIndex.get(entry.key) || []
    if (matches.length) {
      matchedExistingCatalogItems += 1
    } else {
      missingEntries.push(entry)
    }
  }

  let insertedMissingCatalogItems = 0
  if (allowCatalogInsert && missingEntries.length) {
    insertedMissingCatalogItems = await marketSourceCatalogRepo.upsertRows(
      missingEntries.map((entry) => buildPriorityCatalogRow(entry))
    )
  }

  const insertedRows = allowCatalogInsert && missingEntries.length
    ? await marketSourceCatalogRepo.listByMarketHashNames(
        missingEntries.map((entry) => entry.itemName),
        {
          categories: coverageCategories,
          activeOnly: true,
          tradableOnly: true
        }
      )
    : []
  const insertedRowNameSet = new Set(
    (Array.isArray(insertedRows) ? insertedRows : [])
      .map((row) => normalizeText(row?.market_hash_name || row?.marketHashName))
      .filter(Boolean)
  )
  const unmatchedPriorityItems = missingEntries
    .filter((entry) => !insertedRowNameSet.has(entry.itemName))
    .map((entry) => ({
      category: entry.category,
      itemName: entry.itemName,
      tier: entry.tier
    }))

  return {
    setName,
    version: built.version,
    description: built.description,
    totalPriorityItemsConfigured: built.entries.length,
    matchedExistingCatalogItems,
    insertedMissingCatalogItems: Number(insertedMissingCatalogItems || 0),
    catalogInsertApplied: allowCatalogInsert,
    unmatchedPriorityItems,
    entries: built.entries,
    byKey,
    policyHintsByTier
  }
}

module.exports = {
  syncPriorityCoverageSet,
  buildPriorityEntries,
  canonicalizeItemName,
  buildPriorityKey,
  normalizePriorityCategory,
  normalizePriorityTier
}

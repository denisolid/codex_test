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

const SCANNER_SCOPE_CATEGORIES = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE
])
const SCANNER_SCOPE_CATEGORY_SET = new Set(SCANNER_SCOPE_CATEGORIES)
const CATALOG_CANDIDATE_STATUS = Object.freeze({
  CANDIDATE: "candidate",
  ENRICHING: "enriching",
  ELIGIBLE: "eligible",
  REJECTED: "rejected"
})
const CATALOG_MATURITY_STATE = Object.freeze({
  COLD: "cold",
  ENRICHING: "enriching",
  NEAR_ELIGIBLE: "near_eligible",
  ELIGIBLE: "eligible"
})
const ACTIVE_CANDIDATE_STATUSES = Object.freeze([
  CATALOG_CANDIDATE_STATUS.ELIGIBLE,
  CATALOG_CANDIDATE_STATUS.ENRICHING,
  CATALOG_CANDIDATE_STATUS.CANDIDATE
])
const CATALOG_CANDIDATE_STATUS_SET = new Set(
  Object.values(CATALOG_CANDIDATE_STATUS)
)
const CATALOG_MATURITY_STATE_SET = new Set(Object.values(CATALOG_MATURITY_STATE))

const DEFAULT_UNIVERSE_LIMIT = 3000
const DEFAULT_SOURCE_CATALOG_TARGET = 5000
const DEFAULT_UNIVERSE_TARGET = Math.max(
  Number(
    arbitrageScannerUniverseTargetSize ||
      arbitrageDefaultUniverseLimit ||
      DEFAULT_UNIVERSE_LIMIT
  ),
  100
)
const SOURCE_CATALOG_LIMIT = Math.max(
  Number(arbitrageSourceCatalogLimit || DEFAULT_SOURCE_CATALOG_TARGET),
  DEFAULT_SOURCE_CATALOG_TARGET
)
const SOURCE_CATALOG_REFRESH_MS =
  Math.max(Number(arbitrageSourceCatalogRefreshMinutes || 60), 5) * 60 * 1000
const SNAPSHOT_TTL_MS = Math.max(Number(marketSnapshotTtlMinutes || 30), 5) * 60 * 1000
const MAJOR_CAPSULE_EVENT_PATTERN = /\b(katowice|cologne|atlanta|krakow|boston|london|berlin|stockholm|antwerp|rio|paris|copenhagen|major|rmr)\b/i
const CAPSULE_EVENT_SIGNAL_PATTERN = /\b(esl|blast|pgl|dreamhack|iem|cluj|funspark|faceit|challengers|legends|contenders|champions|team)\b/i
const CAPSULE_YEAR_PATTERN = /\b20(1[3-9]|2[0-9])\b/
const WEAR_PATTERN = /\((factory new|minimal wear|field-tested|well-worn|battle-scarred)\)$/i
const WEAPON_PREFIX_ALLOWLIST = Object.freeze(
  new Set([
    "AK-47",
    "M4A1-S",
    "M4A4",
    "AWP",
    "USP-S",
    "Glock-18",
    "Desert Eagle",
    "P250",
    "Five-SeveN",
    "Tec-9",
    "CZ75-Auto",
    "Dual Berettas",
    "R8 Revolver",
    "MP9",
    "MP7",
    "MP5-SD",
    "MAC-10",
    "UMP-45",
    "P90",
    "PP-Bizon",
    "FAMAS",
    "Galil AR",
    "SG 553",
    "AUG",
    "SSG 08",
    "SCAR-20",
    "G3SG1",
    "XM1014",
    "Nova",
    "MAG-7",
    "Sawed-Off",
    "M249",
    "Negev"
  ])
)
const HIGH_LIQUIDITY_WEAPON_PREFIXES = Object.freeze(
  new Set([
    "AK-47",
    "AWP",
    "M4A1-S",
    "M4A4",
    "USP-S",
    "Glock-18",
    "Desert Eagle",
    "P250",
    "Five-SeveN",
    "MP9",
    "MAC-10",
    "FAMAS",
    "Galil AR",
    "AUG",
    "SG 553",
    "P90"
  ])
)
const LOW_VALUE_WEAPON_PATTERNS = Object.freeze([
  /\|\s*Sand Dune/i,
  /\|\s*Safari Mesh/i,
  /\|\s*Boreal Forest/i,
  /\|\s*Urban DDPAT/i,
  /\|\s*Forest DDPAT/i,
  /\|\s*Scorched/i,
  /\|\s*Contractor/i,
  /\|\s*Army Sheen/i,
  /\|\s*Groundwater/i
])
const EXCLUDED_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i,
  /^patch\s*\|/i,
  /^music kit\s*\|/i,
  /^name tag$/i,
  / pass$/i,
  /\bviewer pass\b/i,
  /\bx-ray p250 package\b/i
])
const LIQUID_WEAPON_KEYWORDS = Object.freeze([
  "asiimov",
  "printstream",
  "fade",
  "doppler",
  "gamma",
  "vulcan",
  "redline",
  "neo-noir",
  "bloodsport",
  "case hardened",
  "tiger tooth",
  "slaughter",
  "marble fade"
])
const SOURCE_CATALOG_QUOTA_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    min: 3900,
    target: 4400,
    max: 8400
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    min: 260,
    target: 350,
    max: 1000
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    min: 180,
    target: 250,
    max: 800
  })
})

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
const SOURCE_CANDIDATE_HARD_FLOOR = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 0.45,
  [ITEM_CATEGORIES.CASE]: 0.3,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 0.3
})

const CATEGORY_QUOTA_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    min: 2200,
    target: 2400,
    max: 2800
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    min: 250,
    target: 350,
    max: 650
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    min: 180,
    target: 250,
    max: 450
  })
})

const CATEGORY_PRIORITY = SCANNER_SCOPE_CATEGORIES
const CATEGORY_QUOTA_BASE_TOTAL = Object.values(CATEGORY_QUOTA_RULES).reduce(
  (sum, value) => sum + Number(value?.target || 0),
  0
)
const SOURCE_CATALOG_QUOTA_BASE_TOTAL = Object.values(SOURCE_CATALOG_QUOTA_RULES).reduce(
  (sum, value) => sum + Number(value?.target || 0),
  0
)

const CATEGORY_DEFAULT_COUNTER = Object.freeze({
  total: 0,
  cold: 0,
  candidate: 0,
  enriching: 0,
  nearEligible: 0,
  eligible: 0,
  rejected: 0,
  excludedLowValueItems: 0,
  excludedLowLiquidityItems: 0,
  excludedWeakMarketCoverageItems: 0,
  excludedStaleItems: 0,
  excludedMissingReferenceItems: 0,
  missingSnapshot: 0,
  missingReference: 0,
  missingMarketCoverage: 0
})

const BASE_EXCLUDED_REASON_COUNTER = Object.freeze({
  excludedLowValueItems: 0,
  excludedLowLiquidityItems: 0,
  excludedWeakMarketCoverageItems: 0,
  excludedStaleItems: 0,
  excludedMissingReferenceItems: 0
})

const BASE_INGEST_EXCLUDED_REASON_COUNTER = Object.freeze({
  excludedDuplicate: 0,
  excludedOutOfScopeCategory: 0,
  excludedNamePattern: 0,
  excludedLowValueName: 0,
  excludedUnsupportedWeaponPrefix: 0,
  excludedMissingWear: 0,
  excludedWeakCaseCandidate: 0,
  excludedWeakCapsuleCandidate: 0
})

const sourceCatalogState = {
  inFlight: null,
  lastPreparedAt: 0,
  lastDiagnostics: null,
  lastSuccessfulDiagnostics: null
}

function isScannerScopeCategory(category = "") {
  return SCANNER_SCOPE_CATEGORY_SET.has(normalizeText(category).toLowerCase())
}

function buildEmptyCategoryCounter() {
  return Object.fromEntries(
    SCANNER_SCOPE_CATEGORIES.map((category) => [category, { ...CATEGORY_DEFAULT_COUNTER }])
  )
}

function buildCategoryNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(SCANNER_SCOPE_CATEGORIES.map((category) => [category, initial]))
}

function buildStatusNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(
    Object.values(CATALOG_CANDIDATE_STATUS).map((status) => [status, initial])
  )
}

function buildMaturityNumberMap(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(
    Object.values(CATALOG_MATURITY_STATE).map((state) => [state, initial])
  )
}

function buildMaturityByCategoryMap(initialValue = 0) {
  return Object.fromEntries(
    SCANNER_SCOPE_CATEGORIES.map((category) => [category, buildMaturityNumberMap(initialValue)])
  )
}

function normalizeCandidateStatus(value, fallback = CATALOG_CANDIDATE_STATUS.CANDIDATE) {
  const text = normalizeText(value).toLowerCase()
  if (CATALOG_CANDIDATE_STATUS_SET.has(text)) return text
  const fallbackText = normalizeText(fallback).toLowerCase()
  return CATALOG_CANDIDATE_STATUS_SET.has(fallbackText)
    ? fallbackText
    : CATALOG_CANDIDATE_STATUS.CANDIDATE
}

function normalizeMaturityState(value, fallback = CATALOG_MATURITY_STATE.COLD) {
  const text = normalizeText(value).toLowerCase()
  if (CATALOG_MATURITY_STATE_SET.has(text)) return text
  const fallbackText = normalizeText(fallback).toLowerCase()
  return CATALOG_MATURITY_STATE_SET.has(fallbackText)
    ? fallbackText
    : CATALOG_MATURITY_STATE.COLD
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

function hasExcludedNamePattern(name = "") {
  const text = normalizeText(name)
  return EXCLUDED_NAME_PATTERNS.some((pattern) => pattern.test(text))
}

function isOutOfScopePremiumName(name = "") {
  const text = normalizeText(name)
  if (/ case$/i.test(text) || /\bweapon case\b/i.test(text) || /\bsouvenir package\b/i.test(text)) {
    return false
  }
  if (/\bcapsule\b/i.test(text)) return false
  return /\b(gloves|glove|hand wraps|knife|bayonet|karambit|daggers)\b/i.test(text)
}

function extractWeaponPrefix(name = "") {
  const text = normalizeText(name)
  if (!text.includes("|")) return ""
  const withoutPrefix = text
    .replace(/^stattrak[™\u2122]?\s*/i, "")
    .replace(/^souvenir\s+/i, "")
    .trim()
  return normalizeText(withoutPrefix.split("|")[0])
}

function isLowValueWeaponName(name = "") {
  return LOW_VALUE_WEAPON_PATTERNS.some((pattern) => pattern.test(name))
}

function isEligibleWeaponSkinName(name = "") {
  const text = normalizeText(name)
  if (!text.includes("|")) return false
  if (!WEAR_PATTERN.test(text)) return false
  if (isOutOfScopePremiumName(text)) return false
  const weaponPrefix = extractWeaponPrefix(text)
  if (!WEAPON_PREFIX_ALLOWLIST.has(weaponPrefix)) return false
  if (isLowValueWeaponName(text)) return false
  return true
}

function isEligibleCaseName(name = "") {
  const text = normalizeText(name)
  if (!text) return false
  if (/\bkey\b/i.test(text)) return false
  if (/\bcapsule\b/i.test(text)) return false
  return / case$/i.test(text) || /\bweapon case\b/i.test(text) || /\bsouvenir package\b/i.test(text)
}

function isEligibleStickerCapsuleName(name = "") {
  const text = normalizeText(name)
  if (!text || /\bgraffiti\b/i.test(text)) return false
  const hasCapsule = /\bcapsule\b/i.test(text)
  const hasStickerOrAuto = /\b(sticker|autograph)\b/i.test(text)
  if (!hasCapsule || !hasStickerOrAuto) return false
  const hasMajorSignals =
    MAJOR_CAPSULE_EVENT_PATTERN.test(text) ||
    CAPSULE_EVENT_SIGNAL_PATTERN.test(text) ||
    CAPSULE_YEAR_PATTERN.test(text)
  return hasMajorSignals
}

function normalizeCategory(value, marketHashName = "") {
  const text = normalizeText(value).toLowerCase()
  if (isScannerScopeCategory(text)) {
    return text
  }

  const name = normalizeText(marketHashName)
  if (!name || hasExcludedNamePattern(name) || isOutOfScopePremiumName(name)) return ""
  if (isEligibleCaseName(name)) return ITEM_CATEGORIES.CASE
  if (isEligibleStickerCapsuleName(name)) return ITEM_CATEGORIES.STICKER_CAPSULE
  if (isEligibleWeaponSkinName(name)) return ITEM_CATEGORIES.WEAPON_SKIN
  return ""
}

function inferSubcategory(name = "", category = "") {
  const text = normalizeText(name)
  if (category === ITEM_CATEGORIES.CASE) {
    return /souvenir package/i.test(text) ? "souvenir_package" : "weapon_case"
  }
  if (category === ITEM_CATEGORIES.STICKER_CAPSULE) {
    if (/autograph/i.test(text)) return "major_team_autograph_capsule"
    return "major_sticker_capsule"
  }

  const prefix = extractWeaponPrefix(text)
  if (
    ["AK-47", "M4A1-S", "M4A4", "FAMAS", "Galil AR", "SG 553", "AUG"].includes(prefix)
  ) {
    return "rifle"
  }
  if (["AWP", "SSG 08", "SCAR-20", "G3SG1"].includes(prefix)) {
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
    ].includes(prefix)
  ) {
    return "pistol"
  }
  if (["MP9", "MP7", "MP5-SD", "MAC-10", "UMP-45", "P90", "PP-Bizon"].includes(prefix)) {
    return "smg"
  }
  if (["XM1014", "Nova", "MAG-7", "Sawed-Off"].includes(prefix)) {
    return "shotgun"
  }
  if (["Negev", "M249"].includes(prefix)) {
    return "machine_gun"
  }
  return "weapon_skin"
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

function scaleQuotaValue(baseValue, targetSize, quotaBaseTotal) {
  if (!Number(quotaBaseTotal || 0)) return 0
  return Math.max(
    Math.round((Number(baseValue || 0) * Number(targetSize || 0)) / Number(quotaBaseTotal || 0)),
    0
  )
}

function allocateCategorySlots(quotas = {}, remaining = 0, buckets = [], field = "targetScaled") {
  let slots = Math.max(Number(remaining || 0), 0)
  while (slots > 0) {
    const candidates = buckets
      .filter((bucket) => Number(quotas[bucket.category] || 0) < Number(bucket[field] || 0))
      .sort(
        (a, b) =>
          Number(b[field] || 0) - Number(quotas[b.category] || 0) -
            (Number(a[field] || 0) - Number(quotas[a.category] || 0)) ||
          Number(b.targetScaled || 0) - Number(a.targetScaled || 0)
      )
    if (!candidates.length) break
    quotas[candidates[0].category] += 1
    slots -= 1
  }
  return slots
}

function buildScaledQuotas(targetSize, categoryRules = {}, categories = [], baseTotal = 0) {
  const safeCategories = Array.isArray(categories) ? categories : []
  const safeTarget = Math.max(Math.round(Number(targetSize || 0)), 1)
  const quotas = Object.fromEntries(safeCategories.map((category) => [category, 0]))

  if (!Number(baseTotal || 0) || !safeCategories.length) {
    if (safeCategories.length) {
      quotas[safeCategories[0]] = safeTarget
    }
    return quotas
  }

  const bucketPlan = safeCategories.map((category) => {
    const rule = categoryRules[category] || {}
    const minScaled = scaleQuotaValue(rule.min, safeTarget, baseTotal)
    const targetScaled = scaleQuotaValue(rule.target, safeTarget, baseTotal)
    const maxScaled = Math.max(scaleQuotaValue(rule.max, safeTarget, baseTotal), minScaled)
    return {
      category,
      minScaled,
      targetScaled: Math.max(targetScaled, minScaled),
      maxScaled
    }
  })

  const minTotal = bucketPlan.reduce((sum, bucket) => sum + Number(bucket.minScaled || 0), 0)
  if (minTotal > safeTarget) {
    const targetBuckets = bucketPlan
      .map((bucket) => ({
        ...bucket,
        exactShare:
          (Number(bucket.targetScaled || 0) / Math.max(
            bucketPlan.reduce((sum, row) => sum + Number(row.targetScaled || 0), 0),
            1
          )) * safeTarget
      }))
      .sort((a, b) => Number(b.exactShare || 0) - Number(a.exactShare || 0))

    for (const bucket of targetBuckets) {
      quotas[bucket.category] = Math.floor(Number(bucket.exactShare || 0))
    }
    let remainder = safeTarget - Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
    let index = 0
    while (remainder > 0 && targetBuckets.length) {
      quotas[targetBuckets[index % targetBuckets.length].category] += 1
      remainder -= 1
      index += 1
    }
    return quotas
  }

  for (const bucket of bucketPlan) {
    quotas[bucket.category] = Number(bucket.minScaled || 0)
  }

  let remaining = safeTarget - minTotal
  remaining = allocateCategorySlots(quotas, remaining, bucketPlan, "targetScaled")
  remaining = allocateCategorySlots(quotas, remaining, bucketPlan, "maxScaled")

  if (remaining > 0) {
    const byPriority = bucketPlan.slice().sort(
      (a, b) => Number(b.targetScaled || 0) - Number(a.targetScaled || 0)
    )
    let index = 0
    while (remaining > 0 && byPriority.length) {
      quotas[byPriority[index % byPriority.length].category] += 1
      remaining -= 1
      index += 1
    }
  }

  return quotas
}

function buildCategoryQuotas(targetSize) {
  return buildScaledQuotas(targetSize, CATEGORY_QUOTA_RULES, CATEGORY_PRIORITY, CATEGORY_QUOTA_BASE_TOTAL)
}

function buildSourceCatalogQuotas(targetSize) {
  return buildScaledQuotas(
    targetSize,
    SOURCE_CATALOG_QUOTA_RULES,
    CATEGORY_PRIORITY,
    SOURCE_CATALOG_QUOTA_BASE_TOTAL
  )
}

function buildBaseDiagnostics() {
  return {
    generatedAt: new Date().toISOString(),
    targetUniverseSize: DEFAULT_UNIVERSE_TARGET,
    sourceCatalog: {
      targetRows: SOURCE_CATALOG_LIMIT,
      totalRows: 0,
      seededRows: 0,
      sourceCandidateRows: 0,
      selectedSeedRowsByCategory: buildCategoryNumberMap(),
      sourceCandidateRowsByCategory: buildCategoryNumberMap(),
      sourceExcludedRowsByReason: { ...BASE_INGEST_EXCLUDED_REASON_COUNTER },
      sourceCatalogQuotaTargetByCategory: buildSourceCatalogQuotas(SOURCE_CATALOG_LIMIT),
      sourceCatalogQuotaStageByCategory: buildCategoryNumberMap(),
      sourceCatalogQuotaShortfallByCategory: buildCategoryNumberMap(),
      sourceCatalogQuotaReallocationByCategory: buildCategoryNumberMap(),
      missingRowsToTarget: 0,
      missingRowsToTargetByCategory: buildCategoryNumberMap(),
      activeCatalogRows: 0,
      tradableRows: 0,
      candidateRows: 0,
      enrichingRows: 0,
      nearEligibleRows: 0,
      coldRows: 0,
      eligibleRows: 0,
      rejectedRows: 0,
      eligibleTradableRows: 0,
      promotedToEligible: 0,
      demotedToEnriching: 0,
      promotedToEligibleByCategory: buildCategoryNumberMap(),
      demotedToEnrichingByCategory: buildCategoryNumberMap(),
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0,
      excludedRowsByReason: { ...BASE_EXCLUDED_REASON_COUNTER },
      candidateFunnel: buildStatusNumberMap(),
      maturityFunnel: buildMaturityNumberMap(),
      maturityFunnelByCategory: buildMaturityByCategoryMap(),
      candidateFunnelByCategory: buildEmptyCategoryCounter(),
      eligibleRowsByCategory: buildCategoryNumberMap(),
      candidateRowsByCategory: buildCategoryNumberMap(),
      enrichingRowsByCategory: buildCategoryNumberMap(),
      byCategory: buildEmptyCategoryCounter()
    },
    universeBuild: {
      activeUniverseBuilt: 0,
      missingToTarget: 0,
      quotas: buildCategoryQuotas(DEFAULT_UNIVERSE_TARGET),
      quotaTargetByCategory: buildCategoryQuotas(DEFAULT_UNIVERSE_TARGET),
      selectedByCategory: buildCategoryNumberMap(),
      selectedByCategoryQuotaStage: buildCategoryNumberMap(),
      quotaShortfallByCategory: buildCategoryNumberMap(),
      quotaOverflowByCategory: buildCategoryNumberMap(),
      quotaReallocationByCategory: buildCategoryNumberMap(),
      reallocatedSlots: 0,
      eligibleRows: 0,
      candidateRows: 0,
      enrichingRows: 0,
      eligibleRowsByCategory: buildCategoryNumberMap(),
      candidateRowsByCategory: buildCategoryNumberMap(),
      enrichingRowsByCategory: buildCategoryNumberMap(),
      selectedFromEligible: 0,
      selectedFromEnriching: 0,
      selectedFromCandidate: 0,
      candidateBackfillUsed: false,
      seedPromotionActive: false,
      fallbackToMaxEligible: false
    },
    refreshed: false,
    skipped: false,
    refreshIntervalMs: SOURCE_CATALOG_REFRESH_MS
  }
}

function mergeCategoryCounter(counter = {}, category = "", field = "") {
  const key = normalizeCategory(category)
  if (!key) return
  if (!counter[key]) counter[key] = { ...CATEGORY_DEFAULT_COUNTER }
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

function classifyCatalogCandidate(marketHashName = "", categoryHint = "") {
  const name = normalizeText(marketHashName)
  if (!name) {
    return {
      category: "",
      exclusionReason: "excludedNamePattern"
    }
  }
  if (hasExcludedNamePattern(name)) {
    return {
      category: "",
      exclusionReason: "excludedNamePattern"
    }
  }

  const normalizedHint = normalizeText(categoryHint).toLowerCase()
  if (normalizedHint && !isScannerScopeCategory(normalizedHint)) {
    return {
      category: "",
      exclusionReason: "excludedOutOfScopeCategory"
    }
  }

  if (isOutOfScopePremiumName(name)) {
    return {
      category: "",
      exclusionReason: "excludedOutOfScopeCategory"
    }
  }

  const category = normalizeCategory(normalizedHint || "", name)
  if (category) {
    return {
      category,
      exclusionReason: ""
    }
  }

  if (name.includes("|")) {
    if (!WEAR_PATTERN.test(name)) {
      return {
        category: "",
        exclusionReason: "excludedMissingWear"
      }
    }
    const prefix = extractWeaponPrefix(name)
    if (!WEAPON_PREFIX_ALLOWLIST.has(prefix)) {
      return {
        category: "",
        exclusionReason: "excludedUnsupportedWeaponPrefix"
      }
    }
    if (isLowValueWeaponName(name)) {
      return {
        category: "",
        exclusionReason: "excludedLowValueName"
      }
    }
  }

  if (/case|package/i.test(name)) {
    return {
      category: "",
      exclusionReason: "excludedWeakCaseCandidate"
    }
  }
  if (/capsule/i.test(name)) {
    return {
      category: "",
      exclusionReason: "excludedWeakCapsuleCandidate"
    }
  }

  return {
    category: "",
    exclusionReason: "excludedOutOfScopeCategory"
  }
}

function scoreSourceCatalogCandidate(row = {}, sourceRank = 0) {
  const marketHashName = normalizeText(row?.marketHashName || row?.market_hash_name)
  const category = normalizeCategory(row?.category, marketHashName)
  const lowered = marketHashName.toLowerCase()
  let score = Number(sourceRank || 0)

  if (category === ITEM_CATEGORIES.WEAPON_SKIN) {
    const prefix = extractWeaponPrefix(marketHashName)
    score += 50
    if (/^stattrak/i.test(marketHashName)) score += 12
    if (/^souvenir/i.test(marketHashName)) score += 8
    if (HIGH_LIQUIDITY_WEAPON_PREFIXES.has(prefix)) score += 10
    if (LIQUID_WEAPON_KEYWORDS.some((keyword) => lowered.includes(keyword))) score += 8
    if (/\((factory new|minimal wear)\)$/i.test(marketHashName)) score += 2
  } else if (category === ITEM_CATEGORIES.CASE) {
    score += 40
    if (/operation|kilowatt|gallery|dreams & nightmares|revolution|recoil|fracture|snakebite/i.test(marketHashName)) {
      score += 10
    }
    if (/souvenir package/i.test(marketHashName)) score += 5
  } else if (category === ITEM_CATEGORIES.STICKER_CAPSULE) {
    score += 40
    if (MAJOR_CAPSULE_EVENT_PATTERN.test(marketHashName)) score += 10
    if (/\b(legends|challengers|contenders|champions)\b/i.test(marketHashName)) score += 8
    if (/autograph/i.test(marketHashName)) score += 6
  }

  return Number(score.toFixed(2))
}

function toSourceCatalogSeedRows(rows = [], sourceTag = "curated_seed", sourceRank = 0, counts = null) {
  const output = []
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeText(row?.marketHashName || row?.market_hash_name || row?.itemName || row?.item_name)
    const classification = classifyCatalogCandidate(marketHashName, row?.category)
    if (!classification.category) {
      if (counts && counts[classification.exclusionReason] != null) {
        counts[classification.exclusionReason] += 1
      }
      continue
    }

    output.push({
      marketHashName,
      itemName: normalizeText(row?.itemName || row?.item_name || marketHashName) || marketHashName,
      category: classification.category,
      subcategory: normalizeText(row?.subcategory) || inferSubcategory(marketHashName, classification.category),
      tradable: true,
      scanEligible: Boolean(row?.scanEligible ?? row?.scan_eligible ?? false),
      isActive: Boolean(row?.isActive ?? row?.is_active ?? true),
      sourceTag,
      sourceRank,
      candidateScore: scoreSourceCatalogCandidate(
        {
          marketHashName,
          category: classification.category
        },
        sourceRank
      )
    })
  }
  return output
}

function pickSourceCatalogRowsByQuota(candidates = [], limit = SOURCE_CATALOG_LIMIT) {
  const safeLimit = Math.max(Math.round(Number(limit || SOURCE_CATALOG_LIMIT)), 1)
  const deduped = []
  const seen = new Set()
  const excludedByReason = { ...BASE_INGEST_EXCLUDED_REASON_COUNTER }
  const candidateByCategory = buildCategoryNumberMap()

  for (const row of Array.isArray(candidates) ? candidates : []) {
    const marketHashName = normalizeText(row?.marketHashName || row?.market_hash_name)
    if (!marketHashName) continue
    const key = marketHashName.toLowerCase()
    if (seen.has(key)) {
      excludedByReason.excludedDuplicate += 1
      continue
    }
    const category = normalizeCategory(row?.category, marketHashName)
    if (!isScannerScopeCategory(category)) {
      excludedByReason.excludedOutOfScopeCategory += 1
      continue
    }

    seen.add(key)
    candidateByCategory[category] += 1
    deduped.push({
      ...row,
      marketHashName,
      category,
      candidateScore:
        toFiniteOrNull(row?.candidateScore) ??
        scoreSourceCatalogCandidate({ marketHashName, category }, Number(row?.sourceRank || 0))
    })
  }

  const quotas = buildSourceCatalogQuotas(safeLimit)
  const buckets = Object.fromEntries(CATEGORY_PRIORITY.map((category) => [category, []]))
  for (const row of deduped) {
    buckets[row.category].push(row)
  }
  for (const category of CATEGORY_PRIORITY) {
    buckets[category].sort(
      (a, b) =>
        Number(b.candidateScore || 0) - Number(a.candidateScore || 0) ||
        Number(b.sourceRank || 0) - Number(a.sourceRank || 0) ||
        String(a.marketHashName || "").localeCompare(String(b.marketHashName || ""))
    )
  }

  const selected = []
  const selectedByCategory = buildCategoryNumberMap()
  const selectedByQuotaStage = buildCategoryNumberMap()
  const leftovers = []

  for (const category of CATEGORY_PRIORITY) {
    const quota = Math.max(Number(quotas[category] || 0), 0)
    const rows = buckets[category]
    const stageRows = rows.slice(0, quota)
    selected.push(...stageRows)
    selectedByCategory[category] += stageRows.length
    selectedByQuotaStage[category] = stageRows.length
    leftovers.push(...rows.slice(quota))
  }

  leftovers.sort(
    (a, b) =>
      Number(b.candidateScore || 0) - Number(a.candidateScore || 0) ||
      Number(b.sourceRank || 0) - Number(a.sourceRank || 0)
  )

  for (const row of leftovers) {
    if (selected.length >= safeLimit) break
    selected.push(row)
    selectedByCategory[row.category] += 1
  }

  const quotaShortfallByCategory = buildCategoryNumberMap()
  const quotaReallocationByCategory = buildCategoryNumberMap()
  const missingRowsToTargetByCategory = buildCategoryNumberMap()
  for (const category of CATEGORY_PRIORITY) {
    const quota = Math.max(Number(quotas[category] || 0), 0)
    const stageSelected = Number(selectedByQuotaStage[category] || 0)
    const finalSelected = Number(selectedByCategory[category] || 0)
    quotaShortfallByCategory[category] = Math.max(quota - stageSelected, 0)
    quotaReallocationByCategory[category] = finalSelected - quota
    missingRowsToTargetByCategory[category] = Math.max(quota - finalSelected, 0)
  }

  const missingRowsToTarget = Math.max(safeLimit - Math.min(selected.length, safeLimit), 0)

  return {
    rows: selected.slice(0, safeLimit),
    selectedByCategory,
    candidateByCategory,
    quotas,
    selectedByQuotaStage,
    quotaShortfallByCategory,
    quotaReallocationByCategory,
    missingRowsToTarget,
    missingRowsToTargetByCategory,
    excludedByReason
  }
}

function resolveSeedBuilder(limit = SOURCE_CATALOG_LIMIT) {
  if (typeof sourceCatalogSeed?.buildSourceCatalogSeed === "function") {
    return sourceCatalogSeed.buildSourceCatalogSeed(limit)
  }
  return Array.isArray(sourceCatalogSeed) ? sourceCatalogSeed.slice(0, limit) : []
}

async function ingestSourceCatalogSeeds() {
  const ingestExclusions = { ...BASE_INGEST_EXCLUDED_REASON_COUNTER }
  const curatedSeedRows = toSourceCatalogSeedRows(
    resolveSeedBuilder(Math.max(SOURCE_CATALOG_LIMIT * 2, 1000)),
    "curated_seed",
    20,
    ingestExclusions
  )

  let skinIndexRows = []
  try {
    const allSkins = await skinRepo.listAll()
    skinIndexRows = toSourceCatalogSeedRows(
      allSkins.map((row) => ({
        marketHashName: row?.market_hash_name || row?.marketHashName
      })),
      "skin_index_curated",
      10,
      ingestExclusions
    )
  } catch (err) {
    console.error("[source-catalog] Failed to read skin index for source expansion", err.message)
  }

  const selection = pickSourceCatalogRowsByQuota(
    [...curatedSeedRows, ...skinIndexRows],
    SOURCE_CATALOG_LIMIT
  )
  for (const [reason, count] of Object.entries(selection.excludedByReason || {})) {
    if (ingestExclusions[reason] == null) continue
    ingestExclusions[reason] += Number(count || 0)
  }

  const seededRows = await marketSourceCatalogRepo.upsertRows(selection.rows)
  const skins = await ensureSkinsForCatalogNames(selection.rows.map((row) => row.marketHashName))

  return {
    seedRows: curatedSeedRows.length,
    sourceCandidateRows: curatedSeedRows.length + skinIndexRows.length,
    seededRows,
    seededSkins: Array.isArray(skins) ? skins.length : 0,
    selectedSeedRowsByCategory: selection.selectedByCategory,
    sourceCandidateRowsByCategory: selection.candidateByCategory,
    sourceExcludedRowsByReason: ingestExclusions,
    sourceCatalogQuotaTargetByCategory: selection.quotas,
    sourceCatalogQuotaStageByCategory: selection.selectedByQuotaStage,
    sourceCatalogQuotaShortfallByCategory: selection.quotaShortfallByCategory,
    sourceCatalogQuotaReallocationByCategory: selection.quotaReallocationByCategory,
    missingRowsToTarget: Number(selection.missingRowsToTarget || 0),
    missingRowsToTargetByCategory: selection.missingRowsToTargetByCategory
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

function computeEnrichmentPriority({
  candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE,
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  liquidityRank = 0,
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  missingSnapshot = false,
  missingReference = false,
  missingMarketCoverage = false,
  snapshotStale = false
} = {}) {
  const status = normalizeCandidateStatus(candidateStatus)
  if (status === CATALOG_CANDIDATE_STATUS.REJECTED) return 0

  const statusBoost =
    status === CATALOG_CANDIDATE_STATUS.ELIGIBLE
      ? 38
      : status === CATALOG_CANDIDATE_STATUS.ENRICHING
        ? 26
        : 18
  const categoryBoost =
    category === ITEM_CATEGORIES.CASE
      ? 5
      : category === ITEM_CATEGORIES.STICKER_CAPSULE
        ? 6
        : 0
  const referenceBoost = referencePrice == null ? 0 : Math.min(Number(referencePrice || 0) * 2, 18)
  const volumeBoost = volume7d == null ? 0 : Math.min(Number(volume7d || 0) / 12, 26)
  const coverageBoost = Math.min(Math.max(Number(marketCoverageCount || 0), 0) * 4, 16)
  const readinessBoost =
    (missingReference ? 0 : 6) +
    (missingMarketCoverage ? 0 : 6) +
    (missingSnapshot ? 0 : 5)
  const stalePenalty = snapshotStale ? 8 : 0
  const missingPenalty =
    (missingReference ? 8 : 0) +
    (missingMarketCoverage ? 7 : 0) +
    (missingSnapshot ? 7 : 0)

  const score =
    Number(liquidityRank || 0) +
    statusBoost +
    categoryBoost +
    referenceBoost +
    volumeBoost +
    coverageBoost +
    readinessBoost -
    stalePenalty -
    missingPenalty

  return Number(Math.max(score, 0).toFixed(2))
}

function computeCatalogMaturity({
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE,
  missingSnapshot = false,
  missingReference = false,
  missingMarketCoverage = false,
  missingLiquidityContext = false,
  snapshotStale = false,
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  liquidityRank = 0,
  eligibilityReason = ""
} = {}) {
  const normalizedCategory = normalizeCategory(category)
  const normalizedStatus = normalizeCandidateStatus(candidateStatus)
  const rules =
    SOURCE_QUALITY_RULES[normalizedCategory] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]

  const missingSignals =
    Number(Boolean(missingSnapshot)) +
    Number(Boolean(missingReference)) +
    Number(Boolean(missingMarketCoverage)) +
    Number(Boolean(missingLiquidityContext))
  const hasReference = referencePrice != null
  const hasCoverage = Number(marketCoverageCount || 0) >= Number(rules.minMarketCoverage || 2)
  const hasReasonableVolume =
    volume7d != null && Number(volume7d || 0) >= Math.max(Number(rules.minVolume7d || 40) * 0.6, 20)
  const hasStructuralReason = /\brejected|hard|outofscope|namepattern|unsupported\b/i.test(
    String(eligibilityReason || "")
  )

  let maturityState = CATALOG_MATURITY_STATE.COLD
  if (
    normalizedStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    !snapshotStale &&
    missingSignals === 0
  ) {
    maturityState = CATALOG_MATURITY_STATE.ELIGIBLE
  } else if (
    (normalizedStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
      normalizedStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) &&
    missingSignals <= 1 &&
    !snapshotStale &&
    hasReference &&
    (hasCoverage || hasReasonableVolume)
  ) {
    maturityState = CATALOG_MATURITY_STATE.NEAR_ELIGIBLE
  } else if (
    normalizedStatus === CATALOG_CANDIDATE_STATUS.ENRICHING ||
    normalizedStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE
  ) {
    maturityState =
      missingSignals >= 3
        ? CATALOG_MATURITY_STATE.COLD
        : CATALOG_MATURITY_STATE.ENRICHING
  }

  const baseScore =
    maturityState === CATALOG_MATURITY_STATE.ELIGIBLE
      ? 84
      : maturityState === CATALOG_MATURITY_STATE.NEAR_ELIGIBLE
        ? 66
        : maturityState === CATALOG_MATURITY_STATE.ENRICHING
          ? 46
          : 24
  const categoryBoost =
    normalizedCategory === ITEM_CATEGORIES.CASE
      ? 5
      : normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE
        ? 6
        : 0
  const freshnessBoost = snapshotStale ? -8 : 8
  const referenceBoost = hasReference ? Math.min(Number(referencePrice || 0), 12) : -8
  const coverageBoost = Math.min(Math.max(Number(marketCoverageCount || 0), 0) * 3, 15)
  const volumeBoost =
    volume7d == null
      ? -6
      : Math.min(
          (Number(volume7d || 0) / Math.max(Number(rules.minVolume7d || 1), 1)) * 12,
          16
        )
  const liquidityBoost = Math.min(Number(liquidityRank || 0) * 0.1, 10)
  const missingPenalty = missingSignals * 6
  const structuralPenalty = hasStructuralReason ? 12 : 0
  const score = Math.max(
    Math.min(
      baseScore +
        categoryBoost +
        freshnessBoost +
        referenceBoost +
        coverageBoost +
        volumeBoost +
        liquidityBoost -
        missingPenalty -
        structuralPenalty,
      100
    ),
    0
  )

  return {
    maturityState: normalizeMaturityState(maturityState),
    maturityScore: Number(score.toFixed(2)),
    missingSignals,
    hasStructuralReason
  }
}

function evaluateCandidateState({
  marketHashName = "",
  category = "",
  tradable = true,
  eligibility = {},
  referencePrice = null,
  volume7d = null,
  marketCoverageCount = 0,
  snapshot = null,
  snapshotStale = true,
  liquidityRank = 0
} = {}) {
  const normalizedCategory = normalizeCategory(category, marketHashName)
  const hardFloor = Number(
    SOURCE_CANDIDATE_HARD_FLOOR[normalizedCategory] ??
      SOURCE_CANDIDATE_HARD_FLOOR[ITEM_CATEGORIES.WEAPON_SKIN]
  )
  const missingSnapshot = !snapshot || snapshotStale
  const missingReference = referencePrice == null
  const rules =
    SOURCE_QUALITY_RULES[normalizedCategory] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const missingMarketCoverage =
    Number(marketCoverageCount || 0) < Number(rules.minMarketCoverage || 0)
  const missingLiquidityContext = volume7d == null

  let rejectedReason = ""
  if (!tradable) {
    rejectedReason = "rejectedNotTradable"
  } else if (!isScannerScopeCategory(normalizedCategory)) {
    rejectedReason = "rejectedOutOfScopeCategory"
  } else if (hasExcludedNamePattern(marketHashName)) {
    rejectedReason = "rejectedNamePattern"
  } else if (referencePrice != null && referencePrice < hardFloor) {
    rejectedReason = "rejectedHardValueFloor"
  }

  const strictEligible = Boolean(eligibility?.eligible)
  const strictReason = normalizeText(eligibility?.reason)
  let candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE
  if (rejectedReason) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.REJECTED
  } else if (strictEligible) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.ELIGIBLE
  } else if (missingSnapshot || missingReference || missingMarketCoverage || missingLiquidityContext) {
    candidateStatus = CATALOG_CANDIDATE_STATUS.ENRICHING
  }

  const eligibilityReason =
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
      ? ""
      : rejectedReason ||
        (missingSnapshot
          ? "missing_snapshot"
          : missingReference
            ? "missing_reference"
            : missingMarketCoverage
              ? "missing_market_coverage"
              : missingLiquidityContext
                ? "missing_liquidity_context"
                : strictReason || "candidate_not_ready")

  const enrichmentPriority = computeEnrichmentPriority({
    candidateStatus,
    category: normalizedCategory,
    liquidityRank,
    referencePrice,
    volume7d,
    marketCoverageCount,
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    snapshotStale
  })
  const maturity = computeCatalogMaturity({
    category: normalizedCategory,
    candidateStatus,
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    missingLiquidityContext,
    snapshotStale,
    referencePrice,
    volume7d,
    marketCoverageCount,
    liquidityRank,
    eligibilityReason
  })

  return {
    candidateStatus,
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    missingLiquidityContext,
    eligibilityReason,
    strictEligible,
    strictReason,
    enrichmentPriority,
    maturityState: maturity.maturityState,
    maturityScore: maturity.maturityScore,
    missingSignals: maturity.missingSignals
  }
}

function evaluateEligibility({ category, referencePrice, volume7d, marketCoverageCount, snapshotStale }) {
  const rules = SOURCE_QUALITY_RULES[category] || SOURCE_QUALITY_RULES[ITEM_CATEGORIES.WEAPON_SKIN]

  if (!isScannerScopeCategory(category)) {
    return { eligible: false, reason: "excludedOutOfScopeCategory" }
  }
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
  const rows = await marketSourceCatalogRepo.listActiveTradable({
    limit: SOURCE_CATALOG_LIMIT,
    categories: CATEGORY_PRIORITY
  })
  if (!rows.length) {
    return {
      totalRows: 0,
      activeCatalogRows: 0,
      tradableRows: 0,
      candidateRows: 0,
      enrichingRows: 0,
      nearEligibleRows: 0,
      coldRows: 0,
      eligibleRows: 0,
      rejectedRows: 0,
      eligibleTradableRows: 0,
      promotedToEligible: 0,
      demotedToEnriching: 0,
      excludedLowValueItems: 0,
      excludedLowLiquidityItems: 0,
      excludedWeakMarketCoverageItems: 0,
      excludedStaleItems: 0,
      excludedMissingReferenceItems: 0,
      excludedRowsByReason: { ...BASE_EXCLUDED_REASON_COUNTER },
      candidateFunnel: buildStatusNumberMap(),
      maturityFunnel: buildMaturityNumberMap(),
      maturityFunnelByCategory: buildMaturityByCategoryMap(),
      promotedToEligibleByCategory: buildCategoryNumberMap(),
      demotedToEnrichingByCategory: buildCategoryNumberMap(),
      candidateFunnelByCategory: buildEmptyCategoryCounter(),
      byCategory: buildEmptyCategoryCounter(),
      eligibleRowsByCategory: buildCategoryNumberMap(),
      candidateRowsByCategory: buildCategoryNumberMap(),
      enrichingRowsByCategory: buildCategoryNumberMap()
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

  const byCategory = buildEmptyCategoryCounter()
  const candidateFunnel = buildStatusNumberMap()
  const maturityFunnel = buildMaturityNumberMap()
  const maturityFunnelByCategory = buildMaturityByCategoryMap()
  const updates = []
  const counts = {
    totalRows: rows.length,
    activeCatalogRows: rows.length,
    tradableRows: 0,
    candidateRows: 0,
    enrichingRows: 0,
    nearEligibleRows: 0,
    coldRows: 0,
    eligibleRows: 0,
    rejectedRows: 0,
    eligibleTradableRows: 0,
    promotedToEligible: 0,
    demotedToEnriching: 0,
    excludedLowValueItems: 0,
    excludedLowLiquidityItems: 0,
    excludedWeakMarketCoverageItems: 0,
    excludedStaleItems: 0,
    excludedMissingReferenceItems: 0
  }

  const eligibleRowsByCategory = buildCategoryNumberMap()
  const candidateRowsByCategory = buildCategoryNumberMap()
  const enrichingRowsByCategory = buildCategoryNumberMap()
  const promotedToEligibleByCategory = buildCategoryNumberMap()
  const demotedToEnrichingByCategory = buildCategoryNumberMap()

  for (const row of rows) {
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue

    const category = normalizeCategory(row?.category, marketHashName)
    if (!isScannerScopeCategory(category)) {
      continue
    }
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
    const candidateState = evaluateCandidateState({
      marketHashName,
      category,
      tradable,
      eligibility,
      referencePrice,
      volume7d,
      marketCoverageCount,
      snapshot,
      snapshotStale,
      liquidityRank
    })
    const previousCandidateStatus = normalizeCandidateStatus(
      row?.candidate_status ?? row?.candidateStatus,
      row?.scan_eligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
    )
    const candidateStatus = normalizeCandidateStatus(candidateState.candidateStatus)
    const scanEligible =
      tradable &&
      candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
      candidateState.strictEligible
    const maturityState = normalizeMaturityState(candidateState.maturityState)
    candidateFunnel[candidateStatus] = Number(candidateFunnel[candidateStatus] || 0) + 1
    maturityFunnel[maturityState] = Number(maturityFunnel[maturityState] || 0) + 1
    if (!maturityFunnelByCategory[category]) {
      maturityFunnelByCategory[category] = buildMaturityNumberMap()
    }
    maturityFunnelByCategory[category][maturityState] =
      Number(maturityFunnelByCategory[category][maturityState] || 0) + 1

    if (scanEligible) {
      counts.eligibleTradableRows += 1
      counts.eligibleRows += 1
      byCategory[category].eligible += 1
      eligibleRowsByCategory[category] += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) {
      counts.enrichingRows += 1
      byCategory[category].enriching += 1
      enrichingRowsByCategory[category] += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE) {
      counts.candidateRows += 1
      byCategory[category].candidate += 1
      candidateRowsByCategory[category] += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED) {
      counts.rejectedRows += 1
      byCategory[category].rejected += 1
    }
    if (
      previousCandidateStatus !== CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
      candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
    ) {
      counts.promotedToEligible += 1
      promotedToEligibleByCategory[category] += 1
    }
    if (
      previousCandidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
      candidateStatus !== CATALOG_CANDIDATE_STATUS.ELIGIBLE
    ) {
      counts.demotedToEnriching += 1
      demotedToEnrichingByCategory[category] += 1
    }
    if (maturityState === CATALOG_MATURITY_STATE.NEAR_ELIGIBLE) {
      counts.nearEligibleRows += 1
      byCategory[category].nearEligible += 1
    }
    if (maturityState === CATALOG_MATURITY_STATE.COLD) {
      counts.coldRows += 1
      byCategory[category].cold += 1
    }
    if (candidateState.missingSnapshot) {
      byCategory[category].missingSnapshot += 1
    }
    if (candidateState.missingReference) {
      byCategory[category].missingReference += 1
    }
    if (candidateState.missingMarketCoverage) {
      byCategory[category].missingMarketCoverage += 1
    }
    if (eligibility.reason) {
      if (Object.prototype.hasOwnProperty.call(counts, eligibility.reason)) {
        counts[eligibility.reason] += 1
      }
      mergeCategoryCounter(byCategory, category, eligibility.reason)
    }

    const invalidReason = scanEligible
      ? null
      : normalizeText(
          candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED
            ? candidateState.eligibilityReason
            : eligibility.reason || candidateState.eligibilityReason || "candidate_not_ready"
        ) || "candidate_not_ready"

    updates.push({
      market_hash_name: marketHashName,
      item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
      category,
      subcategory: normalizeText(row?.subcategory) || inferSubcategory(marketHashName, category),
      tradable,
      scan_eligible: scanEligible,
      candidate_status: candidateStatus,
      missing_snapshot: Boolean(candidateState.missingSnapshot),
      missing_reference: Boolean(candidateState.missingReference),
      missing_market_coverage: Boolean(candidateState.missingMarketCoverage),
      enrichment_priority: candidateState.enrichmentPriority,
      eligibility_reason: scanEligible ? null : normalizeText(candidateState.eligibilityReason) || null,
      reference_price: referencePrice,
      market_coverage_count: marketCoverageCount,
      liquidity_rank: liquidityRank,
      volume_7d: volume7d,
      snapshot_stale: snapshotStale,
      snapshot_captured_at: snapshot?.captured_at || null,
      invalid_reason: invalidReason,
      source_tag: normalizeText(row?.source_tag || row?.sourceTag) || "curated_seed",
      is_active: row?.is_active == null ? true : Boolean(row.is_active),
      last_enriched_at: new Date().toISOString()
    })
  }

  await marketSourceCatalogRepo.upsertRows(updates)

  const excludedRowsByReason = {
    excludedLowValueItems: Number(counts.excludedLowValueItems || 0),
    excludedLowLiquidityItems: Number(counts.excludedLowLiquidityItems || 0),
    excludedWeakMarketCoverageItems: Number(counts.excludedWeakMarketCoverageItems || 0),
    excludedStaleItems: Number(counts.excludedStaleItems || 0),
    excludedMissingReferenceItems: Number(counts.excludedMissingReferenceItems || 0)
  }

  return {
    ...counts,
    excludedRowsByReason,
    candidateFunnel,
    maturityFunnel,
    maturityFunnelByCategory,
    promotedToEligibleByCategory,
    demotedToEnrichingByCategory,
    candidateFunnelByCategory: byCategory,
    byCategory,
    eligibleRowsByCategory,
    candidateRowsByCategory,
    enrichingRowsByCategory
  }
}

function takeTopByCategory(rows = [], quotas = {}) {
  const byCategory = Object.fromEntries(CATEGORY_PRIORITY.map((category) => [category, []]))

  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeCategory(row?.category, row?.market_hash_name)
    if (!isScannerScopeCategory(category)) continue
    byCategory[category].push(row)
  }

  const selected = []
  const used = new Set()
  const selectedByCategory = buildCategoryNumberMap()

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

function normalizeCatalogCandidateRows(rows = [], selectionTier = "") {
  const forcedTier = normalizeText(selectionTier)
  function resolveSelectionTier(candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE) {
    if (forcedTier) return forcedTier
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE) return "strict_eligible"
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) return "candidate_enriching"
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE) return "candidate_backfill"
    return "candidate_backfill"
  }
  function resolveTierRank(tier = "") {
    const normalizedTier = normalizeText(tier).toLowerCase()
    if (normalizedTier === "strict_eligible") return 3
    if (normalizedTier === "candidate_enriching") return 2
    return 1
  }

  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName)
      if (!marketHashName) return null
      const category = normalizeCategory(row?.category, marketHashName)
      if (!isScannerScopeCategory(category)) return null
      const scanEligible =
        row?.scan_eligible == null ? Boolean(row?.scanEligible) : Boolean(row.scan_eligible)
      const hasExplicitCandidateStatus = Boolean(
        normalizeText(row?.candidate_status ?? row?.candidateStatus)
      )
      const referencePrice = toFiniteOrNull(row?.reference_price ?? row?.referencePrice)
      const marketCoverageCount = Math.max(
        Number((row?.market_coverage_count ?? row?.marketCoverageCount) || 0),
        0
      )
      const snapshotStale =
        row?.snapshot_stale == null ? Boolean(row?.snapshotStale) : Boolean(row.snapshot_stale)
      const snapshotCapturedAt = normalizeText(
        row?.snapshot_captured_at || row?.snapshotCapturedAt
      )
      const missingSnapshot =
        row?.missing_snapshot == null
          ? !snapshotCapturedAt || snapshotStale
          : Boolean(row.missing_snapshot)
      const missingReference =
        row?.missing_reference == null
          ? referencePrice == null
          : Boolean(row.missing_reference)
      const missingMarketCoverage =
        row?.missing_market_coverage == null
          ? marketCoverageCount <= 0
          : Boolean(row.missing_market_coverage)

      let candidateStatus = normalizeCandidateStatus(
        row?.candidate_status ?? row?.candidateStatus,
        scanEligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
      )
      if (
        !hasExplicitCandidateStatus &&
        !scanEligible &&
        (missingSnapshot || missingReference || missingMarketCoverage)
      ) {
        candidateStatus = CATALOG_CANDIDATE_STATUS.ENRICHING
      }
      if (candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED) return null
      const tier = resolveSelectionTier(candidateStatus)
      return {
        ...row,
        market_hash_name: marketHashName,
        item_name: normalizeText(row?.item_name || row?.itemName || marketHashName) || marketHashName,
        category,
        candidate_status: candidateStatus,
        scan_eligible: scanEligible,
        missing_snapshot: missingSnapshot,
        missing_reference: missingReference,
        missing_market_coverage: missingMarketCoverage,
        enrichment_priority: toFiniteOrNull(row?.enrichment_priority ?? row?.enrichmentPriority) ?? 0,
        liquidity_rank: toFiniteOrNull(row?.liquidity_rank) ?? 0,
        market_coverage_count: marketCoverageCount,
        volume_7d: Math.max(Number(row?.volume_7d || 0), 0),
        reference_price: referencePrice ?? 0,
        selectionTier: tier,
        selectionTierRank: resolveTierRank(tier)
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

function countCatalogRowsByCategory(rows = []) {
  const counts = buildCategoryNumberMap()
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeCategory(
      row?.category || row?.itemCategory,
      row?.market_hash_name || row?.marketHashName || row?.item_name || row?.itemName
    )
    if (!isScannerScopeCategory(category)) continue
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

async function rebuildUniverseFromCatalog(targetSize = DEFAULT_UNIVERSE_TARGET) {
  const safeTarget = Math.max(Math.round(Number(targetSize || DEFAULT_UNIVERSE_TARGET)), 1)
  const strictEligibleRows = normalizeCatalogCandidateRows(
    await marketSourceCatalogRepo.listScanEligible({
      limit: Math.max(SOURCE_CATALOG_LIMIT, safeTarget * 3),
      categories: CATEGORY_PRIORITY
    }),
    "strict_eligible"
  )
  const candidatePoolRows = normalizeCatalogCandidateRows(
    await marketSourceCatalogRepo.listCandidatePool({
      limit: Math.max(SOURCE_CATALOG_LIMIT, safeTarget * 3),
      categories: CATEGORY_PRIORITY,
      candidateStatuses: [
        CATALOG_CANDIDATE_STATUS.ENRICHING,
        CATALOG_CANDIDATE_STATUS.CANDIDATE
      ]
    })
  )
  const enrichingRows = candidatePoolRows.filter(
    (row) => normalizeCandidateStatus(row?.candidate_status) === CATALOG_CANDIDATE_STATUS.ENRICHING
  )
  const candidateRows = candidatePoolRows.filter(
    (row) => normalizeCandidateStatus(row?.candidate_status) === CATALOG_CANDIDATE_STATUS.CANDIDATE
  )

  const rankedRows = dedupeByMarketHashName([
    ...strictEligibleRows,
    ...enrichingRows,
    ...candidateRows
  ])
    .sort(
      (a, b) =>
        Number(b.selectionTierRank || 0) - Number(a.selectionTierRank || 0) ||
        Number(b.enrichment_priority || 0) - Number(a.enrichment_priority || 0) ||
        Number(b.liquidity_rank || 0) - Number(a.liquidity_rank || 0) ||
        Number(b.market_coverage_count || 0) - Number(a.market_coverage_count || 0) ||
        Number(b.volume_7d || 0) - Number(a.volume_7d || 0) ||
        Number(b.reference_price || 0) - Number(a.reference_price || 0)
    )

  const quotas = buildCategoryQuotas(safeTarget)
  const { selected, leftovers, selectedByCategory } = takeTopByCategory(rankedRows, quotas)
  const selectedByCategoryQuotaStage = buildCategoryNumberMap()
  for (const category of CATEGORY_PRIORITY) {
    selectedByCategoryQuotaStage[category] = Number(selectedByCategory[category] || 0)
  }

  const finalRows = [...selected]
  for (const row of leftovers) {
    if (finalRows.length >= safeTarget) break
    finalRows.push(row)
    const category = normalizeCategory(row?.category, row?.market_hash_name)
    if (isScannerScopeCategory(category)) {
      selectedByCategory[category] = Number(selectedByCategory[category] || 0) + 1
    }
  }

  const selectedFromStrict = finalRows.filter((row) => row.selectionTier === "strict_eligible").length
  const selectedFromEnriching = finalRows.filter(
    (row) => row.selectionTier === "candidate_enriching"
  ).length
  const selectedFromCandidate = finalRows.filter(
    (row) => row.selectionTier === "candidate_backfill"
  ).length
  const selectedFromFallback = selectedFromEnriching + selectedFromCandidate
  const candidateBackfillUsed = selectedFromFallback > 0

  const normalizedUniverseRows = finalRows.slice(0, safeTarget).map((row, index) => ({
    marketHashName: row.market_hash_name,
    itemName: row.item_name || row.market_hash_name,
    category: normalizeCategory(row?.category, row?.market_hash_name),
    subcategory: normalizeText(row?.subcategory) || null,
    liquidityRank: index + 1
  }))

  const persist = await marketUniverseRepo.replaceActiveUniverse(normalizedUniverseRows)
  const quotaShortfallByCategory = buildCategoryNumberMap()
  const quotaOverflowByCategory = buildCategoryNumberMap()
  const quotaReallocationByCategory = buildCategoryNumberMap()
  for (const category of CATEGORY_PRIORITY) {
    const quota = Number(quotas[category] || 0)
    const selectedQuotaStage = Number(selectedByCategoryQuotaStage[category] || 0)
    const selectedFinal = Number(selectedByCategory[category] || 0)
    const shortfall = Math.max(quota - selectedQuotaStage, 0)
    const overflow = Math.max(selectedFinal - quota, 0)
    quotaShortfallByCategory[category] = shortfall
    quotaOverflowByCategory[category] = overflow
    quotaReallocationByCategory[category] = selectedFinal - quota
  }

  return {
    targetUniverseSize: safeTarget,
    eligibleRows: strictEligibleRows.length,
    candidateRows: candidateRows.length,
    enrichingRows: enrichingRows.length,
    strictEligibleRows: strictEligibleRows.length,
    fallbackTradableRows: candidatePoolRows.length,
    selectedFromStrict,
    selectedFromEligible: selectedFromStrict,
    selectedFromEnriching,
    selectedFromCandidate,
    selectedFromFallback,
    activeUniverseBuilt: normalizedUniverseRows.length,
    missingToTarget: Math.max(safeTarget - normalizedUniverseRows.length, 0),
    quotaTargetByCategory: quotas,
    selectedByCategory,
    selectedByCategoryQuotaStage,
    quotaShortfallByCategory,
    quotaOverflowByCategory,
    quotaReallocationByCategory,
    reallocatedSlots: Math.max(finalRows.length - selected.length, 0),
    eligibleRowsByCategory: countCatalogRowsByCategory(strictEligibleRows),
    candidateRowsByCategory: countCatalogRowsByCategory(candidateRows),
    enrichingRowsByCategory: countCatalogRowsByCategory(enrichingRows),
    candidateBackfillUsed,
    seedPromotionActive: candidateBackfillUsed,
    quotas,
    fallbackToMaxEligible: candidateBackfillUsed,
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
      targetRows: SOURCE_CATALOG_LIMIT,
      totalRows: Number(sourceCoverage?.totalRows || 0),
      seededRows: Number(ingest?.seededRows || 0),
      sourceCandidateRows: Number(ingest?.sourceCandidateRows || 0),
      selectedSeedRowsByCategory:
        ingest?.selectedSeedRowsByCategory || base.sourceCatalog.selectedSeedRowsByCategory,
      sourceCandidateRowsByCategory:
        ingest?.sourceCandidateRowsByCategory || base.sourceCatalog.sourceCandidateRowsByCategory,
      sourceExcludedRowsByReason:
        ingest?.sourceExcludedRowsByReason || base.sourceCatalog.sourceExcludedRowsByReason,
      sourceCatalogQuotaTargetByCategory:
        ingest?.sourceCatalogQuotaTargetByCategory || base.sourceCatalog.sourceCatalogQuotaTargetByCategory,
      sourceCatalogQuotaStageByCategory:
        ingest?.sourceCatalogQuotaStageByCategory || base.sourceCatalog.sourceCatalogQuotaStageByCategory,
      sourceCatalogQuotaShortfallByCategory:
        ingest?.sourceCatalogQuotaShortfallByCategory || base.sourceCatalog.sourceCatalogQuotaShortfallByCategory,
      sourceCatalogQuotaReallocationByCategory:
        ingest?.sourceCatalogQuotaReallocationByCategory || base.sourceCatalog.sourceCatalogQuotaReallocationByCategory,
      missingRowsToTarget: Number(ingest?.missingRowsToTarget || 0),
      missingRowsToTargetByCategory:
        ingest?.missingRowsToTargetByCategory || base.sourceCatalog.missingRowsToTargetByCategory,
      activeCatalogRows: Number(sourceCoverage?.activeCatalogRows || 0),
      tradableRows: Number(sourceCoverage?.tradableRows || 0),
      candidateRows: Number(sourceCoverage?.candidateRows || 0),
      enrichingRows: Number(sourceCoverage?.enrichingRows || 0),
      nearEligibleRows: Number(sourceCoverage?.nearEligibleRows || 0),
      coldRows: Number(sourceCoverage?.coldRows || 0),
      eligibleRows: Number(sourceCoverage?.eligibleRows || 0),
      rejectedRows: Number(sourceCoverage?.rejectedRows || 0),
      eligibleTradableRows: Number(sourceCoverage?.eligibleTradableRows || 0),
      excludedLowValueItems: Number(sourceCoverage?.excludedLowValueItems || 0),
      excludedLowLiquidityItems: Number(sourceCoverage?.excludedLowLiquidityItems || 0),
      excludedWeakMarketCoverageItems: Number(sourceCoverage?.excludedWeakMarketCoverageItems || 0),
      excludedStaleItems: Number(sourceCoverage?.excludedStaleItems || 0),
      excludedMissingReferenceItems: Number(sourceCoverage?.excludedMissingReferenceItems || 0),
      excludedRowsByReason:
        sourceCoverage?.excludedRowsByReason || base.sourceCatalog.excludedRowsByReason,
      candidateFunnel:
        sourceCoverage?.candidateFunnel || base.sourceCatalog.candidateFunnel,
      maturityFunnel: sourceCoverage?.maturityFunnel || base.sourceCatalog.maturityFunnel,
      maturityFunnelByCategory:
        sourceCoverage?.maturityFunnelByCategory || base.sourceCatalog.maturityFunnelByCategory,
      promotedToEligible: Number(
        sourceCoverage?.promotedToEligible || base.sourceCatalog.promotedToEligible || 0
      ),
      demotedToEnriching: Number(
        sourceCoverage?.demotedToEnriching || base.sourceCatalog.demotedToEnriching || 0
      ),
      promotedToEligibleByCategory:
        sourceCoverage?.promotedToEligibleByCategory ||
        base.sourceCatalog.promotedToEligibleByCategory,
      demotedToEnrichingByCategory:
        sourceCoverage?.demotedToEnrichingByCategory ||
        base.sourceCatalog.demotedToEnrichingByCategory,
      candidateFunnelByCategory:
        sourceCoverage?.candidateFunnelByCategory || base.sourceCatalog.candidateFunnelByCategory,
      eligibleRowsByCategory:
        sourceCoverage?.eligibleRowsByCategory || base.sourceCatalog.eligibleRowsByCategory,
      candidateRowsByCategory:
        sourceCoverage?.candidateRowsByCategory || base.sourceCatalog.candidateRowsByCategory,
      enrichingRowsByCategory:
        sourceCoverage?.enrichingRowsByCategory || base.sourceCatalog.enrichingRowsByCategory,
      byCategory: sourceCoverage?.byCategory || base.sourceCatalog.byCategory
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

function shouldBypassSkipForRecovery(diagnostics = {}, targetUniverseSize = DEFAULT_UNIVERSE_TARGET) {
  const sourceCatalog =
    diagnostics?.sourceCatalog && typeof diagnostics.sourceCatalog === "object"
      ? diagnostics.sourceCatalog
      : {}
  const universeBuild =
    diagnostics?.universeBuild && typeof diagnostics.universeBuild === "object"
      ? diagnostics.universeBuild
      : {}

  const activeCatalogRows = Math.max(
    Number(sourceCatalog?.activeCatalogRows || sourceCatalog?.totalRows || 0),
    0
  )
  const eligibleRows = Math.max(
    Number(sourceCatalog?.eligibleTradableRows || sourceCatalog?.eligibleRows || 0),
    0
  )
  const candidateRows = Math.max(Number(sourceCatalog?.candidateRows || 0), 0)
  const enrichingRows = Math.max(Number(sourceCatalog?.enrichingRows || 0), 0)
  const rejectedRows = Math.max(Number(sourceCatalog?.rejectedRows || 0), 0)
  const activeUniverseBuilt = Math.max(Number(universeBuild?.activeUniverseBuilt || 0), 0)
  const targetSize = Math.max(
    Math.round(
      Number(
        universeBuild?.targetUniverseSize || diagnostics?.targetUniverseSize || targetUniverseSize
      )
    ),
    1
  )
  const missingToTarget = Math.max(
    Number(universeBuild?.missingToTarget ?? Math.max(targetSize - activeUniverseBuilt, 0)),
    0
  )
  const hasCollapsedFunnel =
    activeCatalogRows > 250 &&
    eligibleRows > 0 &&
    candidateRows === 0 &&
    enrichingRows === 0 &&
    rejectedRows === 0
  const hasCollapsedUniverse =
    activeUniverseBuilt > 0 &&
    activeUniverseBuilt <= Math.max(eligibleRows, 1) &&
    missingToTarget > Math.max(Math.round(targetSize * 0.5), 500)

  return hasCollapsedFunnel || hasCollapsedUniverse
}

async function prepareSourceCatalog(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const targetUniverseSize = Number(options.targetUniverseSize || DEFAULT_UNIVERSE_TARGET)

  if (
    !shouldRefresh(forceRefresh) &&
    sourceCatalogState.lastDiagnostics &&
    !shouldBypassSkipForRecovery(sourceCatalogState.lastDiagnostics, targetUniverseSize)
  ) {
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
      sourceCatalogState.lastSuccessfulDiagnostics = diagnostics
      return diagnostics
    })
    .catch((err) => {
      const safeTarget = Math.max(
        Math.round(Number(targetUniverseSize || DEFAULT_UNIVERSE_TARGET)),
        1
      )
      const errorMessage = String(err?.message || "source_catalog_pipeline_failed")
      const lastSuccessful = sourceCatalogState.lastSuccessfulDiagnostics
      if (lastSuccessful && typeof lastSuccessful === "object") {
        const degraded = {
          ...lastSuccessful,
          generatedAt: new Date().toISOString(),
          refreshed: false,
          skipped: true,
          error: errorMessage,
          staleDiagnosticsRetained: true
        }
        sourceCatalogState.lastPreparedAt = Date.now()
        sourceCatalogState.lastDiagnostics = degraded
        return degraded
      }

      const fallback = {
        ...buildBaseDiagnostics(),
        generatedAt: new Date().toISOString(),
        targetUniverseSize: safeTarget,
        refreshed: false,
        skipped: false,
        error: errorMessage
      }
      fallback.universeBuild = {
        ...fallback.universeBuild,
        targetUniverseSize: safeTarget,
        activeUniverseBuilt: 0,
        missingToTarget: safeTarget,
        quotas: buildCategoryQuotas(safeTarget),
        quotaTargetByCategory: buildCategoryQuotas(safeTarget),
        fallbackToMaxEligible: false
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

async function getCatalogRowsByMarketHashNames(marketHashNames = [], options = {}) {
  return marketSourceCatalogRepo.listByMarketHashNames(marketHashNames, {
    categories: Array.isArray(options.categories) ? options.categories : CATEGORY_PRIORITY,
    activeOnly: options.activeOnly !== false,
    tradableOnly: options.tradableOnly !== false
  })
}

module.exports = {
  prepareSourceCatalog,
  getLastDiagnostics,
  getCatalogRowsByMarketHashNames,
  __testables: {
    normalizeCategory,
    normalizeCandidateStatus,
    normalizeMaturityState,
    computeSourceLiquidityScore,
    computeEnrichmentPriority,
    computeCatalogMaturity,
    evaluateCandidateState,
    evaluateEligibility,
    buildCategoryQuotas,
    buildSourceCatalogQuotas,
    resolveVolume7d,
    shouldBypassSkipForRecovery
  }
}

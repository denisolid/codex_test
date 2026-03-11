const AppError = require("../utils/AppError")
const skinRepo = require("../repositories/skinRepository")
const planService = require("./planService")

const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove"
})

const PREMIUM_CATEGORIES = Object.freeze(
  new Set([ITEM_CATEGORIES.KNIFE, ITEM_CATEGORIES.GLOVE])
)

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null) return null
  if (typeof value === "string" && !value.trim()) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function round2(value) {
  const numeric = toFiniteOrNull(value)
  if (numeric == null) return null
  return Number(numeric.toFixed(2))
}

function roundPrice(value) {
  const numeric = toFiniteOrNull(value)
  if (numeric == null) return null
  return Number(numeric.toFixed(4))
}

function normalizeItemCategory(value, marketHashName = "") {
  const raw = normalizeText(value).toLowerCase()
  if (raw === ITEM_CATEGORIES.KNIFE || raw === "knives") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === ITEM_CATEGORIES.GLOVE || raw === "gloves") {
    return ITEM_CATEGORIES.GLOVE
  }
  if (
    raw === ITEM_CATEGORIES.STICKER_CAPSULE ||
    raw === "sticker capsule" ||
    raw === "sticker_capsules" ||
    raw === "capsule" ||
    raw === "capsules"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === ITEM_CATEGORIES.CASE || raw === "cases") {
    return ITEM_CATEGORIES.CASE
  }
  if (raw === ITEM_CATEGORIES.WEAPON_SKIN || raw === "skin" || raw === "skins") {
    return ITEM_CATEGORIES.WEAPON_SKIN
  }

  const name = normalizeText(marketHashName)
  if (!name) return ITEM_CATEGORIES.WEAPON_SKIN
  if (/sticker capsule$/i.test(name)) return ITEM_CATEGORIES.STICKER_CAPSULE
  if (/case$/i.test(name)) return ITEM_CATEGORIES.CASE
  if (/\b(gloves|glove|hand wraps)\b/i.test(name)) return ITEM_CATEGORIES.GLOVE
  if (/\b(knife|bayonet|karambit|daggers)\b/i.test(name)) return ITEM_CATEGORIES.KNIFE
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function isPremiumCategory(value) {
  return PREMIUM_CATEGORIES.has(normalizeItemCategory(value))
}

function hasPremiumCategoryAccess(entitlements = {}) {
  return planService.canAccessKnivesAndGloves(entitlements)
}

function inferPremiumCategoryLabel(value) {
  const category = normalizeItemCategory(value)
  if (category === ITEM_CATEGORIES.KNIFE) return "knife"
  if (category === ITEM_CATEGORIES.GLOVE) return "glove"
  return "premium"
}

function redactPremiumOpportunityRow(row = {}) {
  const itemCategory = normalizeItemCategory(row?.itemCategory || row?.category, row?.itemName)
  const premiumCategoryLabel = inferPremiumCategoryLabel(itemCategory)
  const score = toFiniteOrNull(row?.score)
  const scoreBand =
    score == null
      ? "Locked"
      : score >= 80
        ? "High"
        : score >= 65
          ? "Medium"
          : "Emerging"

  return {
    ...row,
    itemId: null,
    buyMarket: null,
    buyPrice: null,
    sellMarket: null,
    sellNet: null,
    profit: null,
    spread: null,
    score: null,
    scoreCategory: null,
    executionConfidence: "Locked",
    liquidity: null,
    liquidityBand: null,
    liquidityLabel: "Premium",
    marketCoverage: null,
    referencePrice: null,
    buyUrl: null,
    sellUrl: null,
    isLockedPreview: true,
    premiumCategory: itemCategory,
    premiumCategoryLabel,
    lockReason: "premium_category",
    lockMessage: "Unlock knife and glove opportunities with Full Access",
    lockHint: "Premium high-value market category",
    previewSummary: `${scoreBand} quality setup`,
    previewBuyPrice: roundPrice(row?.buyPrice),
    previewSellNet: roundPrice(row?.sellNet),
    previewProfit: roundPrice(row?.profit),
    previewSpread: round2(row?.spread),
    previewScoreBand: scoreBand,
    badges: Array.from(
      new Set([
        ...(Array.isArray(row?.badges) ? row.badges : []),
        "LOCKED",
        "FULL ACCESS"
      ])
    )
  }
}

function applyPremiumPreviewLock(rows = [], entitlements = {}) {
  const safeRows = Array.isArray(rows) ? rows : []
  if (hasPremiumCategoryAccess(entitlements)) {
    return {
      rows: safeRows.map((row) => ({
        ...row,
        isLockedPreview: false,
        premiumCategory: null,
        premiumCategoryLabel: null,
        lockReason: null,
        lockMessage: null,
        lockHint: null,
        previewSummary: null,
        previewBuyPrice: null,
        previewSellNet: null,
        previewProfit: null,
        previewSpread: null,
        previewScoreBand: null
      })),
      lockedCount: 0
    }
  }

  let lockedCount = 0
  const mapped = safeRows.map((row) => {
    const category = normalizeItemCategory(row?.itemCategory || row?.category, row?.itemName)
    if (!PREMIUM_CATEGORIES.has(category)) {
      return {
        ...row,
        isLockedPreview: false,
        premiumCategory: null,
        premiumCategoryLabel: null,
        lockReason: null,
        lockMessage: null,
        lockHint: null,
        previewSummary: null,
        previewBuyPrice: null,
        previewSellNet: null,
        previewProfit: null,
        previewSpread: null,
        previewScoreBand: null
      }
    }
    lockedCount += 1
    return redactPremiumOpportunityRow(row)
  })

  return { rows: mapped, lockedCount }
}

function assertPremiumCategoryAccess(input = {}) {
  const entitlements = input?.entitlements || {}
  const itemCategory = normalizeItemCategory(input?.itemCategory, input?.marketHashName)
  if (!PREMIUM_CATEGORIES.has(itemCategory)) return itemCategory
  if (
    planService.canAccessCategory(entitlements, itemCategory) ||
    hasPremiumCategoryAccess(entitlements)
  ) {
    return itemCategory
  }
  throw new AppError(
    input?.message ||
      "Unlock knife and glove opportunities with Full Access to inspect and compare premium categories.",
    402,
    "PLAN_UPGRADE_REQUIRED"
  )
}

async function assertPremiumCategoryAccessForSkinId(skinId, entitlements = {}, options = {}) {
  const normalizedSkinId = Number(skinId)
  if (!Number.isInteger(normalizedSkinId) || normalizedSkinId <= 0) {
    return null
  }
  const skin = await skinRepo.getById(normalizedSkinId)
  if (!skin) {
    return null
  }
  assertPremiumCategoryAccess({
    entitlements,
    marketHashName: skin.market_hash_name,
    message:
      options?.message ||
      "Unlock knife and glove opportunities with Full Access to inspect premium categories."
  })
  return skin
}

module.exports = Object.freeze({
  ITEM_CATEGORIES,
  PREMIUM_CATEGORIES,
  normalizeItemCategory,
  isPremiumCategory,
  hasPremiumCategoryAccess,
  redactPremiumOpportunityRow,
  applyPremiumPreviewLock,
  assertPremiumCategoryAccess,
  assertPremiumCategoryAccessForSkinId
})

const { ITEM_CATEGORIES } = require("./config")

const CATEGORY_KEYS = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE,
  ITEM_CATEGORIES.KNIFE,
  ITEM_CATEGORIES.GLOVE,
  "other"
])

function normalizeText(value) {
  return String(value || "").trim()
}

function roundRatio(value, digits = 4) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return 0
  return Number(numeric.toFixed(digits))
}

function ratio(numerator, denominator) {
  const top = Number(numerator || 0)
  const bottom = Number(denominator || 0)
  if (!Number.isFinite(top) || !Number.isFinite(bottom) || bottom <= 0) return 0
  return roundRatio(top / bottom)
}

function normalizeCategory(value) {
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return "other"
  if (raw === ITEM_CATEGORIES.WEAPON_SKIN || raw === "skin" || raw === "skins") {
    return ITEM_CATEGORIES.WEAPON_SKIN
  }
  if (raw === ITEM_CATEGORIES.CASE || raw === "cases") return ITEM_CATEGORIES.CASE
  if (
    raw === ITEM_CATEGORIES.STICKER_CAPSULE ||
    raw === "sticker capsule" ||
    raw === "capsule" ||
    raw === "capsules" ||
    raw === "sticker_capsules"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === ITEM_CATEGORIES.KNIFE || raw === ITEM_CATEGORIES.FUTURE_KNIFE || raw === "knives") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === ITEM_CATEGORIES.GLOVE || raw === ITEM_CATEGORIES.FUTURE_GLOVE || raw === "gloves") {
    return ITEM_CATEGORIES.GLOVE
  }
  return "other"
}

function createCategoryCounts() {
  return CATEGORY_KEYS.reduce((acc, key) => {
    acc[key] = 0
    return acc
  }, {})
}

function incrementCategoryCount(target = {}, category = "") {
  const normalized = normalizeCategory(category)
  target[normalized] = Number(target[normalized] || 0) + 1
}

function createPublisherMetrics(engine = "scanner_v2") {
  return {
    engine: normalizeText(engine) || "scanner_v2",
    eligibleCount: 0,
    emittedCount: 0,
    blockedOnEmitCount: 0,
    staleOnEmitCount: 0,
    categoryCounts: {
      eligible: createCategoryCounts(),
      emitted: createCategoryCounts(),
      blockedOnEmit: createCategoryCounts()
    },
    weaponSkin: {
      eligibleCount: 0,
      emittedCount: 0,
      blockedOnEmitCount: 0
    }
  }
}

function trackEligible(metrics = {}, opportunity = {}) {
  metrics.eligibleCount = Number(metrics.eligibleCount || 0) + 1
  const category = opportunity?.itemCategory || opportunity?.category
  incrementCategoryCount(metrics.categoryCounts?.eligible, category)
  if (normalizeCategory(category) === ITEM_CATEGORIES.WEAPON_SKIN) {
    metrics.weaponSkin.eligibleCount = Number(metrics.weaponSkin?.eligibleCount || 0) + 1
  }
}

function trackEmitted(metrics = {}, opportunity = {}) {
  metrics.emittedCount = Number(metrics.emittedCount || 0) + 1
  const category = opportunity?.itemCategory || opportunity?.category
  incrementCategoryCount(metrics.categoryCounts?.emitted, category)
  if (normalizeCategory(category) === ITEM_CATEGORIES.WEAPON_SKIN) {
    metrics.weaponSkin.emittedCount = Number(metrics.weaponSkin?.emittedCount || 0) + 1
  }
}

function trackBlockedOnEmit(metrics = {}, opportunity = {}, options = {}) {
  metrics.blockedOnEmitCount = Number(metrics.blockedOnEmitCount || 0) + 1
  const category = opportunity?.itemCategory || opportunity?.category
  incrementCategoryCount(metrics.categoryCounts?.blockedOnEmit, category)
  if (normalizeCategory(category) === ITEM_CATEGORIES.WEAPON_SKIN) {
    metrics.weaponSkin.blockedOnEmitCount = Number(metrics.weaponSkin?.blockedOnEmitCount || 0) + 1
  }
  const reason = normalizeText(options.reason).toLowerCase()
  if (reason === "stale_on_emit") {
    metrics.staleOnEmitCount = Number(metrics.staleOnEmitCount || 0) + 1
  }
}

function buildCategoryMix(counts = {}) {
  const safeCounts = CATEGORY_KEYS.reduce((acc, key) => {
    acc[key] = Number(counts?.[key] || 0)
    return acc
  }, {})
  const total = Object.values(safeCounts).reduce((sum, value) => sum + Number(value || 0), 0)
  const shares = CATEGORY_KEYS.reduce((acc, key) => {
    acc[key] = ratio(safeCounts[key], total)
    return acc
  }, {})
  return {
    total,
    counts: safeCounts,
    shares
  }
}

function finalizePublisherMetrics(metrics = {}, options = {}) {
  const scannedCount = Math.max(Math.round(Number(options.scannedCount || 0)), 0)
  const eligibleCount = Math.max(Math.round(Number(metrics.eligibleCount || 0)), 0)
  const emittedCount = Math.max(Math.round(Number(metrics.emittedCount || 0)), 0)
  const blockedOnEmitCount = Math.max(Math.round(Number(metrics.blockedOnEmitCount || 0)), 0)
  const staleOnEmitCount = Math.max(Math.round(Number(metrics.staleOnEmitCount || 0)), 0)
  const weaponSkin = {
    eligibleCount: Math.max(Math.round(Number(metrics?.weaponSkin?.eligibleCount || 0)), 0),
    emittedCount: Math.max(Math.round(Number(metrics?.weaponSkin?.emittedCount || 0)), 0),
    blockedOnEmitCount: Math.max(
      Math.round(Number(metrics?.weaponSkin?.blockedOnEmitCount || 0)),
      0
    )
  }

  return {
    engine: normalizeText(metrics.engine) || "scanner_v2",
    scannedCount,
    eligibleCount,
    emittedCount,
    blockedOnEmitCount,
    staleOnEmitCount,
    categoryMix: {
      eligible: buildCategoryMix(metrics?.categoryCounts?.eligible),
      emitted: buildCategoryMix(metrics?.categoryCounts?.emitted),
      blockedOnEmit: buildCategoryMix(metrics?.categoryCounts?.blockedOnEmit)
    },
    weaponSkinYield: {
      eligibleCount: weaponSkin.eligibleCount,
      emittedCount: weaponSkin.emittedCount,
      blockedOnEmitCount: weaponSkin.blockedOnEmitCount,
      emittedEligibleRatio: ratio(weaponSkin.emittedCount, weaponSkin.eligibleCount),
      emittedScannedRatio: ratio(weaponSkin.emittedCount, scannedCount)
    },
    emittedScannedRatio: ratio(emittedCount, scannedCount)
  }
}

module.exports = {
  createPublisherMetrics,
  trackEligible,
  trackEmitted,
  trackBlockedOnEmit,
  finalizePublisherMetrics
}

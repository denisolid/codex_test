const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const sourceSeed = require("../src/config/marketSourceCatalogSeed")
const {
  __testables: {
    buildCategoryQuotas,
    buildSourceCatalogQuotas,
    evaluateEligibility,
    normalizeCategory
  }
} = require("../src/services/marketSourceCatalogService")

test("source catalog seed expands with scanner-scope categories only", () => {
  assert.equal(Array.isArray(sourceSeed), true)
  assert.equal(sourceSeed.length >= 2000, true)
  assert.equal(sourceSeed.length <= 5000, true)

  const categories = new Set(sourceSeed.map((row) => String(row?.category || "").trim()))
  assert.equal(categories.has("weapon_skin"), true)
  assert.equal(categories.has("case"), true)
  assert.equal(categories.has("sticker_capsule"), true)
  assert.equal(categories.has("knife"), false)
  assert.equal(categories.has("glove"), false)
})

test("source catalog seed excludes obvious junk prefixes", () => {
  const names = sourceSeed.map((row) => String(row?.marketHashName || "").trim().toLowerCase())
  assert.equal(names.some((name) => name.startsWith("sticker |")), false)
  assert.equal(names.some((name) => name.startsWith("graffiti |")), false)
  assert.equal(names.some((name) => name.startsWith("sealed graffiti |")), false)
})

test("source catalog seed includes curated cases/capsules and liquid skin variants", () => {
  const names = new Set(sourceSeed.map((row) => String(row?.marketHashName || "").trim()))
  assert.equal(names.has("Revolution Case"), true)
  assert.equal(names.has("Copenhagen 2024 Legends Sticker Capsule"), true)
  assert.equal(names.has("AK-47 | Redline (Field-Tested)"), true)
  assert.equal(names.has("StatTrak™ AK-47 | Redline (Field-Tested)"), true)
  assert.equal(names.has("Souvenir AK-47 | Redline (Field-Tested)"), true)
})

test("source catalog category quotas preserve 3k distribution", () => {
  const quotas = buildCategoryQuotas(3000)
  const total = Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
  assert.equal(total, 3000)
  assert.equal(Number(quotas.weapon_skin || 0), 2400)
  assert.equal(Number(quotas.case || 0), 350)
  assert.equal(Number(quotas.sticker_capsule || 0), 250)

  const scaled = buildCategoryQuotas(1000)
  assert.equal(Number(scaled.weapon_skin || 0), 800)
  assert.equal(Number(scaled.case || 0), 117)
  assert.equal(Number(scaled.sticker_capsule || 0), 83)
})

test("source catalog quotas preserve 5k composition with category-aware scaling", () => {
  const quotas = buildSourceCatalogQuotas(5000)
  const total = Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
  assert.equal(total, 5000)
  assert.equal(Number(quotas.weapon_skin || 0), 4400)
  assert.equal(Number(quotas.case || 0), 350)
  assert.equal(Number(quotas.sticker_capsule || 0), 250)

  const scaled = buildSourceCatalogQuotas(3000)
  assert.equal(Number(scaled.weapon_skin || 0), 2640)
  assert.equal(Number(scaled.case || 0), 210)
  assert.equal(Number(scaled.sticker_capsule || 0), 150)
})

test("source eligibility rejects weak liquidity and coverage before universe build", () => {
  const category = normalizeCategory("weapon_skin")
  const lowCoverage = evaluateEligibility({
    category,
    referencePrice: 8,
    volume7d: 220,
    marketCoverageCount: 1,
    snapshotStale: false
  })
  assert.equal(lowCoverage.eligible, false)
  assert.equal(lowCoverage.reason, "excludedWeakMarketCoverageItems")

  const good = evaluateEligibility({
    category,
    referencePrice: 8,
    volume7d: 220,
    marketCoverageCount: 3,
    snapshotStale: false
  })
  assert.equal(good.eligible, true)

  assert.equal(normalizeCategory("", "Karambit | Doppler (Factory New)"), "")
  assert.equal(normalizeCategory("", "Sport Gloves | Vice (Field-Tested)"), "")
})

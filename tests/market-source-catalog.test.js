const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const sourceSeed = require("../src/config/marketSourceCatalogSeed")
const {
  __testables: { buildCategoryQuotas, evaluateEligibility, normalizeCategory }
} = require("../src/services/marketSourceCatalogService")

test("source catalog seed expands beyond scanner target with curated categories", () => {
  assert.equal(Array.isArray(sourceSeed), true)
  assert.equal(sourceSeed.length >= 700, true)
  assert.equal(sourceSeed.length <= 1000, true)

  const categories = new Set(sourceSeed.map((row) => String(row?.category || "").trim()))
  assert.equal(categories.has("weapon_skin"), true)
  assert.equal(categories.has("case"), true)
  assert.equal(categories.has("sticker_capsule"), true)
})

test("source catalog seed excludes obvious junk prefixes", () => {
  const names = sourceSeed.map((row) => String(row?.marketHashName || "").trim().toLowerCase())
  assert.equal(names.some((name) => name.startsWith("sticker |")), false)
  assert.equal(names.some((name) => name.startsWith("graffiti |")), false)
  assert.equal(names.some((name) => name.startsWith("sealed graffiti |")), false)
})

test("source catalog seed includes curated cases, capsules, and skin wear variants", () => {
  const names = new Set(sourceSeed.map((row) => String(row?.marketHashName || "").trim()))
  assert.equal(names.has("Revolution Case"), true)
  assert.equal(names.has("Copenhagen 2024 Legends Sticker Capsule"), true)
  assert.equal(names.has("AK-47 | Redline (Field-Tested)"), true)
})

test("source catalog category quotas preserve mixed category support", () => {
  const quotas = buildCategoryQuotas(500)
  const total = Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
  assert.equal(total, 500)
  assert.equal(Number(quotas.weapon_skin || 0), 320)
  assert.equal(Number(quotas.case || 0), 120)
  assert.equal(Number(quotas.sticker_capsule || 0), 60)

  const scaled = buildCategoryQuotas(250)
  assert.equal(Number(scaled.weapon_skin || 0), 160)
  assert.equal(Number(scaled.case || 0), 60)
  assert.equal(Number(scaled.sticker_capsule || 0), 30)
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
})

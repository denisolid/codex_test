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
    computeCatalogMaturity,
    computeEnrichmentPriority,
    evaluateCandidateState,
    evaluateEligibility,
    normalizeCandidateStatus,
    normalizeMaturityState,
    normalizeCategory,
    shouldBypassSkipForRecovery
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

test("candidate state separates enriching from strict eligible", () => {
  const category = normalizeCategory("case", "Revolution Case")
  const lowContextState = evaluateCandidateState({
    marketHashName: "Revolution Case",
    category,
    tradable: true,
    eligibility: { eligible: false, reason: "excludedMissingReferenceItems" },
    referencePrice: null,
    volume7d: null,
    marketCoverageCount: 0,
    snapshot: null,
    snapshotStale: true,
    liquidityRank: 0
  })

  assert.equal(normalizeCandidateStatus(lowContextState.candidateStatus), "enriching")
  assert.equal(lowContextState.missingSnapshot, true)
  assert.equal(lowContextState.missingReference, true)
  assert.equal(lowContextState.missingMarketCoverage, true)
  assert.equal(lowContextState.strictEligible, false)
  assert.equal(lowContextState.enrichmentPriority > 0, true)

  const eligibleState = evaluateCandidateState({
    marketHashName: "Revolution Case",
    category,
    tradable: true,
    eligibility: { eligible: true, reason: "" },
    referencePrice: 3.2,
    volume7d: 410,
    marketCoverageCount: 3,
    snapshot: { captured_at: new Date().toISOString() },
    snapshotStale: false,
    liquidityRank: computeEnrichmentPriority({
      candidateStatus: "eligible",
      category,
      referencePrice: 3.2,
      volume7d: 410,
      marketCoverageCount: 3
    })
  })
  assert.equal(normalizeCandidateStatus(eligibleState.candidateStatus), "eligible")
  assert.equal(eligibleState.strictEligible, true)
})

test("catalog maturity scoring distinguishes near-eligible from cold", () => {
  const nearEligible = computeCatalogMaturity({
    category: "case",
    candidateStatus: "enriching",
    missingSnapshot: false,
    missingReference: false,
    missingMarketCoverage: true,
    missingLiquidityContext: false,
    snapshotStale: false,
    referencePrice: 2.8,
    volume7d: 180,
    marketCoverageCount: 1,
    liquidityRank: 62,
    eligibilityReason: "missing_market_coverage"
  })
  const cold = computeCatalogMaturity({
    category: "sticker_capsule",
    candidateStatus: "candidate",
    missingSnapshot: true,
    missingReference: true,
    missingMarketCoverage: true,
    missingLiquidityContext: true,
    snapshotStale: true,
    referencePrice: null,
    volume7d: null,
    marketCoverageCount: 0,
    liquidityRank: 12,
    eligibilityReason: "candidate_not_ready"
  })

  assert.equal(normalizeMaturityState(nearEligible.maturityState), "near_eligible")
  assert.equal(normalizeMaturityState(cold.maturityState), "cold")
  assert.equal(Number(nearEligible.maturityScore || 0) > Number(cold.maturityScore || 0), true)
})

test("skip recovery bypass triggers for collapsed legacy diagnostics", () => {
  const shouldBypass = shouldBypassSkipForRecovery(
    {
      targetUniverseSize: 3000,
      sourceCatalog: {
        activeCatalogRows: 2257,
        eligibleTradableRows: 20,
        candidateRows: 0,
        enrichingRows: 0,
        rejectedRows: 0
      },
      universeBuild: {
        targetUniverseSize: 3000,
        activeUniverseBuilt: 20,
        missingToTarget: 2980
      }
    },
    3000
  )
  assert.equal(shouldBypass, true)

  const shouldNotBypass = shouldBypassSkipForRecovery(
    {
      targetUniverseSize: 3000,
      sourceCatalog: {
        activeCatalogRows: 2384,
        eligibleTradableRows: 20,
        candidateRows: 6,
        enrichingRows: 2340,
        rejectedRows: 18
      },
      universeBuild: {
        targetUniverseSize: 3000,
        activeUniverseBuilt: 2294,
        missingToTarget: 706
      }
    },
    3000
  )
  assert.equal(shouldNotBypass, false)
})

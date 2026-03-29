const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const sourceSeed = require("../src/config/marketSourceCatalogSeed")
const {
  __testables: {
    CATEGORY_AWARE_EVALUATION_REASONS,
    buildCategoryQuotas,
    buildCategoryQuotasForCategories,
    buildSourceCatalogQuotas,
    buildSourceCatalogQuotasForCategories,
    computeCatalogMaturity,
    classifyCatalogStatus,
    computeEnrichmentPriority,
    computeSourceLiquidityScore,
    evaluateCandidateState,
    evaluateEligibility,
    isUniverseBackfillReadyRow,
    normalizeCandidateStatus,
    normalizeMaturityState,
    normalizeCategory,
    normalizeCatalogScopeCategories,
    resolveCompatibleCatalogStatusFields,
    resolveQuoteCoverageInputs,
    shouldBypassSkipForRecovery
  }
} = require("../src/services/marketSourceCatalogService")
const {
  __testables: { resolveConservativeMedian, buildCoverageByItem }
} = require("../src/repositories/marketQuoteRepository")

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

test("scoped catalog quotas zero blocked categories and reallocate healthy categories safely", () => {
  assert.deepEqual(normalizeCatalogScopeCategories(["case", "sticker_capsule", "case"]), [
    "case",
    "sticker_capsule"
  ])

  const universeQuotas = buildCategoryQuotasForCategories(1000, ["case", "sticker_capsule"])
  assert.equal(Number(universeQuotas.weapon_skin || 0), 0)
  assert.equal(Number(universeQuotas.case || 0) > 0, true)
  assert.equal(Number(universeQuotas.sticker_capsule || 0) > 0, true)
  assert.equal(
    Object.values(universeQuotas).reduce((sum, value) => sum + Number(value || 0), 0),
    1000
  )

  const sourceQuotas = buildSourceCatalogQuotasForCategories(500, ["case", "sticker_capsule"])
  assert.equal(Number(sourceQuotas.weapon_skin || 0), 0)
  assert.equal(Number(sourceQuotas.case || 0) > 0, true)
  assert.equal(Number(sourceQuotas.sticker_capsule || 0) > 0, true)
  assert.equal(
    Object.values(sourceQuotas).reduce((sum, value) => sum + Number(value || 0), 0),
    500
  )
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

test("source eligibility aligns weapon-skin liquidity floor with risky entry threshold", () => {
  const category = normalizeCategory("weapon_skin")
  const aligned = evaluateEligibility({
    category,
    referencePrice: 9,
    volume7d: 36,
    marketCoverageCount: 2,
    snapshotStale: false
  })

  assert.equal(aligned.eligible, true)
  assert.equal(aligned.reason, "")
})

test("source eligibility softens weapon-skin missing liquidity into penalty path", () => {
  const nowIso = new Date().toISOString()
  const category = normalizeCategory("weapon_skin", "AK-47 | Slate (Field-Tested)")
  const result = evaluateEligibility({
    marketHashName: "AK-47 | Slate (Field-Tested)",
    category,
    tradable: true,
    referencePrice: 9.5,
    volume7d: null,
    marketCoverageCount: 2,
    snapshotStale: false,
    quoteFetchedAt: nowIso,
    latestMarketSignalAt: nowIso
  })

  assert.equal(result.eligible, false)
  assert.equal(
    result.reason,
    CATEGORY_AWARE_EVALUATION_REASONS.PENALTY_MISSING_LIQUIDITY_WEAPON_SKIN
  )
  assert.equal(result.convertedHardRejectToPenalty, true)
  assert.equal(result.recoveryPath, "near_eligible")
})

test("source eligibility softens recoverable stale weapon skins into cooldown path", () => {
  const staleIso = new Date(Date.now() - 150 * 60 * 1000).toISOString()
  const category = normalizeCategory("weapon_skin", "AK-47 | Redline (Field-Tested)")
  const result = evaluateEligibility({
    marketHashName: "AK-47 | Redline (Field-Tested)",
    category,
    tradable: true,
    referencePrice: 12.5,
    volume7d: 75,
    marketCoverageCount: 3,
    snapshotStale: true,
    quoteFetchedAt: staleIso,
    latestReferencePriceAt: staleIso,
    latestMarketSignalAt: staleIso
  })

  assert.equal(result.eligible, false)
  assert.equal(
    result.reason,
    CATEGORY_AWARE_EVALUATION_REASONS.PENALTY_STALE_MARKET_WEAPON_SKIN
  )
  assert.equal(result.convertedHardRejectToPenalty, true)
  assert.equal(result.recoveryPath, "cooldown")
})

test("source eligibility treats low-value weapon skins contextually while keeping non-weapon strict", () => {
  const freshIso = new Date().toISOString()
  const weaponSkinCategory = normalizeCategory("weapon_skin", "USP-S | Blueprint (Field-Tested)")
  const weaponSkin = evaluateEligibility({
    marketHashName: "USP-S | Blueprint (Field-Tested)",
    category: weaponSkinCategory,
    tradable: true,
    referencePrice: 1.75,
    volume7d: 48,
    marketCoverageCount: 2,
    snapshotStale: false,
    quoteFetchedAt: freshIso,
    latestMarketSignalAt: freshIso
  })
  const caseCategory = normalizeCategory("case", "Fracture Case")
  const caseRow = evaluateEligibility({
    marketHashName: "Fracture Case",
    category: caseCategory,
    tradable: true,
    referencePrice: 1.75,
    volume7d: 200,
    marketCoverageCount: 2,
    snapshotStale: false,
    quoteFetchedAt: freshIso,
    latestMarketSignalAt: freshIso
  })

  assert.equal(
    weaponSkin.reason,
    CATEGORY_AWARE_EVALUATION_REASONS.CONTEXTUAL_LOW_VALUE_WEAPON_SKIN
  )
  assert.equal(weaponSkin.convertedHardRejectToPenalty, true)
  assert.equal(caseRow.reason, "excludedLowValueItems")
})

test("source eligibility blocks case items below $2", () => {
  const category = normalizeCategory("case", "Fracture Case")
  const lowCost = evaluateEligibility({
    category,
    referencePrice: 1.75,
    volume7d: 200,
    marketCoverageCount: 2,
    snapshotStale: false
  })

  assert.equal(lowCost.eligible, false)
  assert.equal(lowCost.reason, "excludedLowValueItems")
})

test("source eligibility keeps non-weapon low liquidity strict", () => {
  const category = normalizeCategory("case", "Revolution Case")
  const result = evaluateEligibility({
    marketHashName: "Revolution Case",
    category,
    tradable: true,
    referencePrice: 3.1,
    volume7d: 12,
    marketCoverageCount: 3,
    snapshotStale: false,
    quoteFetchedAt: new Date().toISOString()
  })

  assert.equal(result.eligible, false)
  assert.equal(result.reason, "excludedLowLiquidityItems")
  assert.equal(result.convertedHardRejectToPenalty, false)
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

test("candidate state promotes partial-ready rows to near-eligible when freshness is usable", () => {
  const marketHashName = "AK-47 | Redline (Field-Tested)"
  const category = normalizeCategory("weapon_skin", marketHashName)
  const state = evaluateCandidateState({
    marketHashName,
    category,
    tradable: true,
    eligibility: { eligible: false, reason: "excludedWeakMarketCoverageItems" },
    referencePrice: 10.8,
    volume7d: 62,
    marketCoverageCount: 1,
    snapshot: { captured_at: new Date(Date.now() - 70 * 60 * 1000).toISOString() },
    snapshotStale: true,
    quoteFetchedAt: new Date().toISOString(),
    liquidityRank: 58
  })

  assert.equal(normalizeCandidateStatus(state.candidateStatus), "near_eligible")
  assert.equal(state.missingSnapshot, false)
  assert.equal(state.freshnessState, "fresh")
  assert.equal(Array.isArray(state.nearEligibleBlockers), true)
  assert.equal(state.nearEligibleBlockers.length, 0)
  assert.equal(state.eligibleBlockers.includes("market_coverage_insufficient"), true)
})

test("candidate state promotes recoverable stale weapon skins into near-eligible with penalty reason", () => {
  const marketHashName = "AK-47 | Slate (Field-Tested)"
  const category = normalizeCategory("weapon_skin", marketHashName)
  const staleIso = new Date(Date.now() - 150 * 60 * 1000).toISOString()
  const eligibility = evaluateEligibility({
    marketHashName,
    category,
    tradable: true,
    referencePrice: 11.4,
    volume7d: 64,
    marketCoverageCount: 3,
    snapshotStale: true,
    quoteFetchedAt: staleIso,
    latestReferencePriceAt: staleIso,
    latestMarketSignalAt: staleIso
  })
  const state = evaluateCandidateState({
    marketHashName,
    category,
    tradable: true,
    eligibility,
    referencePrice: 11.4,
    volume7d: 64,
    marketCoverageCount: 3,
    snapshot: { captured_at: staleIso, average_7d_price: 11.4, volume_24h: 12 },
    snapshotStale: true,
    quoteFetchedAt: staleIso,
    liquidityRank: 56,
    latestReferencePriceAt: staleIso,
    latestMarketSignalAt: staleIso
  })

  assert.equal(
    eligibility.reason,
    CATEGORY_AWARE_EVALUATION_REASONS.PENALTY_STALE_MARKET_WEAPON_SKIN
  )
  assert.equal(normalizeCandidateStatus(state.candidateStatus), "near_eligible")
  assert.equal(state.progressionBlockers.includes("freshness_not_usable"), false)
  assert.equal(
    state.progressionBlockers.includes(
      CATEGORY_AWARE_EVALUATION_REASONS.PENALTY_STALE_MARKET_WEAPON_SKIN
    ),
    true
  )
  assert.equal(state.recoveryPath, "near_eligible")
})

test("candidate state promotes safe covered weapon skins without reference into near-eligible", () => {
  const marketHashName = "AK-47 | Slate (Field-Tested)"
  const category = normalizeCategory("weapon_skin", marketHashName)
  const liquidityRank = computeSourceLiquidityScore({
    category,
    referencePrice: null,
    volume7d: null,
    marketCoverage: 3,
    snapshotStale: false
  })
  const state = evaluateCandidateState({
    marketHashName,
    category,
    tradable: true,
    eligibility: { eligible: false, reason: "excludedMissingReferenceItems" },
    referencePrice: null,
    volume7d: null,
    marketCoverageCount: 3,
    snapshot: null,
    snapshotStale: false,
    quoteFetchedAt: new Date().toISOString(),
    liquidityRank
  })

  assert.equal(normalizeCandidateStatus(state.candidateStatus), "near_eligible")
  assert.equal(state.snapshotState, "missing_snapshot")
  assert.equal(state.referenceState, "missing_reference")
  assert.equal(state.liquidityState, "partial_liquidity")
  assert.equal(state.coverageState, "coverage_ready")
  assert.equal(state.progressionStatus, "blocked_from_eligible")
  assert.equal(state.progressionBlockers.includes("missing_reference"), true)
})

test("candidate state surfaces partial snapshot diagnostics for incomplete snapshot rows", () => {
  const marketHashName = "M4A4 | Neo-Noir (Field-Tested)"
  const category = normalizeCategory("weapon_skin", marketHashName)
  const state = evaluateCandidateState({
    marketHashName,
    category,
    tradable: true,
    eligibility: { eligible: false, reason: "excludedLowLiquidityItems" },
    referencePrice: 14.5,
    volume7d: null,
    marketCoverageCount: 2,
    snapshot: {
      captured_at: new Date().toISOString(),
      average_7d_price: 14.5
    },
    snapshotStale: false,
    quoteFetchedAt: new Date().toISOString(),
    liquidityRank: 42
  })

  assert.equal(state.snapshotState, "partial_snapshot")
  assert.equal(state.progressionBlockers.includes("partial_snapshot"), true)
  assert.equal(state.progressionBlockers.includes("missing_liquidity"), false)
  assert.equal(state.progressionBlockers.includes("partial_liquidity"), true)
})

test("candidate state keeps zero-coverage weapon skins in enriching", () => {
  const marketHashName = "AK-47 | Redline (Field-Tested)"
  const category = normalizeCategory("weapon_skin", marketHashName)
  const state = evaluateCandidateState({
    marketHashName,
    category,
    tradable: true,
    eligibility: { eligible: false, reason: "excludedWeakMarketCoverageItems" },
    referencePrice: 10.8,
    volume7d: 62,
    marketCoverageCount: 0,
    snapshot: { captured_at: new Date().toISOString() },
    snapshotStale: false,
    quoteFetchedAt: new Date().toISOString(),
    liquidityRank: 58
  })

  assert.equal(normalizeCandidateStatus(state.candidateStatus), "enriching")
  assert.equal(state.nearEligibleBlockers.includes("market_coverage_insufficient"), true)
})

test("candidate state uses explicit hard reject reasons for fake or untradable rows", () => {
  const marketHashName = "AK-47 | Slate (Field-Tested)"
  const category = normalizeCategory("weapon_skin", marketHashName)
  const eligibility = evaluateEligibility({
    marketHashName,
    category,
    tradable: false,
    referencePrice: 11,
    volume7d: 60,
    marketCoverageCount: 3,
    snapshotStale: false,
    quoteFetchedAt: new Date().toISOString()
  })
  const state = evaluateCandidateState({
    marketHashName,
    category,
    tradable: false,
    eligibility,
    referencePrice: 11,
    volume7d: 60,
    marketCoverageCount: 3,
    snapshot: { captured_at: new Date().toISOString() },
    snapshotStale: false,
    liquidityRank: 44
  })

  assert.equal(normalizeCandidateStatus(state.candidateStatus), "rejected")
  assert.equal(
    state.eligibilityReason,
    CATEGORY_AWARE_EVALUATION_REASONS.HARD_REJECT_FAKE_OR_UNTRADABLE
  )
  assert.equal(state.progressionBlockers.includes("anti_fake_guard"), true)
})

test("catalog status blocks fully data-empty rows and shadows stale-only rows", () => {
  const blocked = classifyCatalogStatus({
    category: "weapon_skin",
    referencePrice: null,
    marketCoverageCount: 0,
    snapshotCapturedAt: null,
    quoteFetchedAt: null,
    snapshotStale: true,
    invalidReason: "",
    liquidityRank: 0,
    candidateState: { maturityScore: 8, antiFakeBlocked: false }
  })
  const staleShadow = classifyCatalogStatus({
    category: "weapon_skin",
    referencePrice: 14,
    marketCoverageCount: 3,
    snapshotCapturedAt: new Date(Date.now() - 10 * 60 * 60 * 1000).toISOString(),
    quoteFetchedAt: new Date(Date.now() - 9 * 60 * 60 * 1000).toISOString(),
    snapshotStale: true,
    invalidReason: "",
    liquidityRank: 72,
    candidateState: { maturityScore: 70, antiFakeBlocked: false }
  })

  assert.equal(blocked.catalogStatus, "blocked")
  assert.equal(blocked.catalogBlockReason, "unusable_market_coverage")
  assert.equal(staleShadow.catalogStatus, "shadow")
  assert.equal(staleShadow.catalogBlockReason, "stale_only_signals")
})

test("catalog status marks complete market basis rows as scannable", () => {
  const status = classifyCatalogStatus({
    category: "case",
    referencePrice: 3.4,
    marketCoverageCount: 3,
    snapshotCapturedAt: new Date().toISOString(),
    quoteFetchedAt: new Date().toISOString(),
    snapshotStale: false,
    invalidReason: "",
    liquidityRank: 68,
    candidateState: { maturityScore: 88, antiFakeBlocked: false }
  })

  assert.equal(status.catalogStatus, "scannable")
  assert.equal(status.catalogBlockReason, null)
  assert.equal(Number(status.catalogQualityScore || 0) > 0, true)
})

test("catalog status respects canonical latest market signal with unix timestamps", () => {
  const oldSnapshotIso = new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString()
  const freshQuoteSeconds = Math.floor(Date.now() / 1000)
  const status = classifyCatalogStatus({
    category: "weapon_skin",
    referencePrice: 11.4,
    marketCoverageCount: 3,
    snapshotCapturedAt: oldSnapshotIso,
    quoteFetchedAt: freshQuoteSeconds,
    referenceState: "quote_reference",
    snapshotStale: true,
    invalidReason: "",
    liquidityRank: 60,
    candidateState: { maturityScore: 80, antiFakeBlocked: false }
  })

  assert.equal(status.catalogStatus, "scannable")
  assert.equal(Boolean(status.lastMarketSignalAt), true)
  assert.equal(status.staleResult, false)
  assert.equal(
    status.staleReasonSource === "latest_quote" || status.staleReasonSource === "latest_reference_price",
    true
  )
})

test("catalog-status compatibility keeps structurally valid legacy rows scannable without explicit status", () => {
  const oldIso = new Date(Date.now() - 10 * 60 * 60 * 1000).toISOString()
  const compatible = resolveCompatibleCatalogStatusFields({
    category: "weapon_skin",
    reference_price: 14,
    market_coverage_count: 3,
    snapshot_captured_at: oldIso,
    quote_fetched_at: oldIso,
    snapshot_stale: true,
    invalid_reason: "",
    liquidity_rank: 72
  })

  assert.equal(compatible.catalogStatus, "scannable")
  assert.equal(compatible.catalogBlockReason, null)
})

test("catalog-status compatibility still blocks structurally invalid legacy rows", () => {
  const compatible = resolveCompatibleCatalogStatusFields({
    category: "weapon_skin",
    reference_price: null,
    market_coverage_count: 0,
    snapshot_captured_at: null,
    quote_fetched_at: null,
    snapshot_stale: true,
    invalid_reason: "",
    liquidity_rank: 0
  })

  assert.equal(compatible.catalogStatus, "blocked")
  assert.equal(compatible.catalogBlockReason, "unusable_market_coverage")
})

test("universe backfill blocks zero-coverage weapon-skin enriching rows", () => {
  const recent = new Date().toISOString()
  const blocked = isUniverseBackfillReadyRow({
    market_hash_name: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    candidate_status: "enriching",
    maturity_state: "enriching",
    reference_price: 12,
    market_coverage_count: 0,
    volume_7d: 80,
    snapshot_captured_at: recent,
    snapshot_stale: false,
    quote_fetched_at: recent
  })
  const allowed = isUniverseBackfillReadyRow({
    market_hash_name: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    candidate_status: "enriching",
    maturity_state: "enriching",
    reference_price: 12,
    market_coverage_count: 1,
    volume_7d: 80,
    snapshot_captured_at: recent,
    snapshot_stale: false,
    quote_fetched_at: recent
  })

  assert.equal(blocked, false)
  assert.equal(allowed, true)
})

test("universe backfill allows candidate rows with safe reference proxy but blocks empty proxies", () => {
  const recent = new Date().toISOString()
  const allowedCandidate = isUniverseBackfillReadyRow({
    market_hash_name: "CS20 Case",
    category: "case",
    candidate_status: "candidate",
    maturity_state: "enriching",
    reference_price: null,
    market_coverage_count: 1,
    volume_7d: 120,
    snapshot_captured_at: recent,
    snapshot_stale: false,
    quote_fetched_at: recent,
    liquidity_rank: 44
  })
  const blockedCandidate = isUniverseBackfillReadyRow({
    market_hash_name: "CS20 Case",
    category: "case",
    candidate_status: "candidate",
    maturity_state: "enriching",
    reference_price: null,
    market_coverage_count: 1,
    volume_7d: null,
    snapshot_captured_at: null,
    snapshot_stale: true,
    quote_fetched_at: null,
    liquidity_rank: 0
  })

  assert.equal(allowedCandidate, true)
  assert.equal(blockedCandidate, false)
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

test("source eligibility accepts usable quote freshness for mature rows", () => {
  const category = normalizeCategory("weapon_skin")
  const usableFreshness = evaluateEligibility({
    category,
    referencePrice: 9,
    volume7d: 120,
    marketCoverageCount: 3,
    snapshotStale: true,
    snapshotCapturedAt: new Date(Date.now() - 95 * 60 * 1000).toISOString(),
    quoteFetchedAt: new Date().toISOString()
  })

  assert.equal(usableFreshness.eligible, true)
  assert.equal(usableFreshness.reason, "")
})

test("quote reference fallback uses a conservative median", () => {
  assert.equal(resolveConservativeMedian([8.4, 7.8, 9.2]), 8.4)
  assert.equal(resolveConservativeMedian([8.4, 7.8]), 7.8)
  assert.equal(resolveConservativeMedian([]), null)
})

test("quote coverage summary keeps per-market freshness and reference candidates", () => {
  const freshIso = new Date().toISOString()
  const staleIso = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString()
  const coverage = buildCoverageByItem([
    {
      item_name: "AK-47 | Slate (Field-Tested)",
      market: "steam",
      best_sell_net: 8.5,
      volume_7d: 22,
      fetched_at: freshIso
    },
    {
      item_name: "AK-47 | Slate (Field-Tested)",
      market: "csfloat",
      best_buy: 8.2,
      volume_7d: 18,
      fetched_at: staleIso
    }
  ])

  assert.equal(coverage["AK-47 | Slate (Field-Tested)"].marketCoverageCount, 2)
  assert.equal(coverage["AK-47 | Slate (Field-Tested)"].quoteMarketsReturned, 2)
  assert.equal(
    coverage["AK-47 | Slate (Field-Tested)"].markets.steam.hasReferenceCandidate,
    true
  )
  assert.equal(
    coverage["AK-47 | Slate (Field-Tested)"].markets.csfloat.referenceCandidate,
    8.2
  )
  assert.equal(
    coverage["AK-47 | Slate (Field-Tested)"].markets.steam.fetchedAt,
    freshIso
  )
  assert.equal(
    coverage["AK-47 | Slate (Field-Tested)"].referenceCandidateMarketCount,
    2
  )
})

test("weapon-skin quote inputs derive progression support from partial fresh quotes", () => {
  const freshIso = new Date().toISOString()
  const staleIso = new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString()
  const quoteInputs = resolveQuoteCoverageInputs({
    category: "weapon_skin",
    snapshotReferencePrice: null,
    quoteCoverage: {
      marketCoverageCount: 2,
      latestFetchedAt: freshIso,
      referencePriceMedian: 8.5,
      referencePriceCandidateCount: 1,
      markets: {
        steam: {
          fetchedAt: freshIso,
          hasReferenceCandidate: true,
          referenceCandidate: 8.5
        },
        csfloat: {
          fetchedAt: staleIso,
          hasReferenceCandidate: false,
          referenceCandidate: null
        }
      }
    }
  })

  assert.equal(quoteInputs.quoteMarketsReturned, 2)
  assert.equal(quoteInputs.freshQuoteMarketsUsable, 1)
  assert.equal(quoteInputs.marketCoverageCount, 1)
  assert.equal(quoteInputs.referencePriceCandidateCount, 1)
  assert.equal(quoteInputs.quoteReferencePrice, 8.5)
  assert.equal(quoteInputs.strictQuoteReferencePrice, null)
  assert.equal(quoteInputs.quoteReferenceMode, "progressive")
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

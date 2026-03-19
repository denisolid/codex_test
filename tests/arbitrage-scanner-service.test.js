const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const marketUniverseMvp = require("../src/config/marketUniverseMvp.json");
const {
  __testables: {
    normalizeUniverseEntries,
    normalizeItemCategory,
    normalizeCategoryFilter,
    computeLiquidityScoreFromSnapshot,
    resolveVolume7d,
    resolveStaleDataPenalty,
    resolveLiquidityMetrics,
    passesUniverseSeedFilters,
    passesScannerGuards,
    computeRiskAdjustments,
    buildApiOpportunityRow,
    buildFeedInsertRow,
    mapFeedRowToApiRow,
    classifyOpportunityFeedEvent,
    isMateriallyNewOpportunity,
    isScannerRunOverdue,
    buildRiskyProfileDiagnostics,
    trackRiskyDecision,
    trackRiskyBaselineOutcome,
    trackRiskyBorderlinePromotion,
    toRiskyProfileDiagnosticsSummary,
    computeStrictCoverageThreshold,
    resolveMaturityStateForSeed,
    resolveScanLayerForMaturity,
    resolveCatalogSeedFreshnessContext,
    evaluateOpportunitySeedAdmission,
    summarizeOpportunitySeedAdmissions,
    isOpportunityScanReadySeed,
    isMinimumOpportunityBackfillReadySeed,
    summarizeSnapshotWarmupBacklog,
    mergeSeedWithSnapshot,
    computeLayerPriority,
    selectSeedsForLayeredScanning,
    DEFAULT_UNIVERSE_LIMIT,
    HOT_OPPORTUNITY_SCAN_TARGET,
    SCAN_BATCH_SIZE,
    MAX_CONCURRENT_MARKET_REQUESTS,
    SCAN_TIMEOUT_PER_BATCH
  }
} = require("../src/services/arbitrageScannerService");

test("market universe is fixed and unique with category metadata", () => {
  assert.equal(Array.isArray(marketUniverseMvp), true);
  assert.equal(marketUniverseMvp.length >= 40, true);
  assert.equal(marketUniverseMvp.length <= 60, true);
  const normalized = normalizeUniverseEntries(marketUniverseMvp);
  assert.equal(
    normalized.length,
    marketUniverseMvp.length,
  );
  const skins = normalized.filter((row) => row.category === "weapon_skin");
  const cases = normalized.filter((row) => row.category === "case");
  const capsules = normalized.filter((row) => row.category === "sticker_capsule");
  assert.equal(skins.length >= 20 && skins.length <= 30, true);
  assert.equal(cases.length >= 10 && cases.length <= 15, true);
  assert.equal(capsules.length >= 10 && capsules.length <= 15, true);
  assert.equal(
    normalized.every((row) => Boolean(String(row.marketHashName || "").trim())),
    true,
  );
});

test("category normalization supports skins/cases/capsules aliases", () => {
  assert.equal(normalizeItemCategory("case", "Revolution Case"), "case");
  assert.equal(normalizeItemCategory("weapon_skin", "AK-47 | Redline"), "weapon_skin");
  assert.equal(normalizeItemCategory("", "Fracture Case"), "case");
  assert.equal(normalizeItemCategory("knife", "★ Karambit | Doppler"), "knife");
  assert.equal(normalizeItemCategory("", "★ Sport Gloves | Vice"), "glove");
  assert.equal(
    normalizeItemCategory("sticker_capsule", "Paris 2023 Legends Sticker Capsule"),
    "sticker_capsule",
  );
  assert.equal(
    normalizeItemCategory("", "Copenhagen 2024 Legends Sticker Capsule"),
    "sticker_capsule",
  );
  assert.equal(normalizeCategoryFilter("all"), "all");
  assert.equal(normalizeCategoryFilter("skins"), "weapon_skin");
  assert.equal(normalizeCategoryFilter("cases"), "case");
  assert.equal(normalizeCategoryFilter("capsules"), "sticker_capsule");
  assert.equal(normalizeCategoryFilter("knives"), "knife");
  assert.equal(normalizeCategoryFilter("gloves"), "glove");
});

test("snapshot-driven liquidity helpers produce bounded values", () => {
  const score = computeLiquidityScoreFromSnapshot({
    volume_24h: 120,
    volatility_7d_percent: 7,
    spread_percent: 4
  });
  const volume7d = resolveVolume7d({ volume_24h: 17 });

  assert.equal(Number.isFinite(score), true);
  assert.equal(score >= 0 && score <= 100, true);
  assert.equal(volume7d, 119);
});

test("warmup merge preserves existing volume when refreshed snapshot lacks volume", () => {
  const merged = mergeSeedWithSnapshot(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 9.4,
      marketVolume7d: 84,
      hasSnapshotData: true,
      snapshotStale: true
    },
    {
      captured_at: new Date().toISOString(),
      average_7d_price: 10.2,
      lowest_listing_price: 9.9,
      volume_24h: null
    }
  );

  assert.equal(merged.marketVolume7d, 84);
  assert.equal(merged.referencePrice, 10.2);
  assert.equal(merged.hasSnapshotData, true);
});

test("stale penalty increases with quote age", () => {
  const now = Date.now();
  const freshIso = new Date(now - 5 * 60 * 1000).toISOString();
  const staleIso = new Date(now - 110 * 60 * 1000).toISOString();

  const fresh = resolveStaleDataPenalty(
    [
      { source: "steam", updatedAt: freshIso },
      { source: "skinport", updatedAt: freshIso }
    ],
    { buyMarket: "steam", sellMarket: "skinport" }
  );
  const stale = resolveStaleDataPenalty(
    [
      { source: "steam", updatedAt: staleIso },
      { source: "skinport", updatedAt: staleIso }
    ],
    { buyMarket: "steam", sellMarket: "skinport" }
  );

  assert.equal(fresh.penalty, 0);
  assert.equal(stale.penalty > fresh.penalty, true);
});

test("scanner guards enforce spread/profit/liquidity thresholds", () => {
  const liquidity = resolveLiquidityMetrics(
    {
      antiFake: {
        liquidity: {
          signalType: "volume_7d",
          signalValue: 240
        }
      }
    },
    { marketVolume7d: 200, liquidityScore: 35 }
  );

  assert.equal(
    passesScannerGuards(
      {
        isOpportunity: true,
        profit: 4.5,
        spreadPercent: 12,
        buyPrice: 8.2,
        marketCoverage: 3
      },
      liquidity
    ),
    true
  );
  assert.equal(
    passesScannerGuards(
      {
        isOpportunity: true,
        profit: 0,
        spreadPercent: 12,
        buyPrice: 8.2,
        marketCoverage: 3
      },
      liquidity
    ),
    false
  );
  assert.equal(
    passesScannerGuards(
      {
        isOpportunity: true,
        profit: 4.5,
        spreadPercent: 2,
        buyPrice: 8.2,
        marketCoverage: 3
      },
      liquidity
    ),
    false
  );
  assert.equal(
    passesScannerGuards(
      {
        isOpportunity: true,
        profit: 4.5,
        spreadPercent: 12,
        buyPrice: 1.4,
        marketCoverage: 3
      },
      liquidity
    ),
    false
  );
  assert.equal(
    passesScannerGuards(
      {
        isOpportunity: true,
        profit: 4.5,
        spreadPercent: 12,
        buyPrice: 8.2,
        marketCoverage: 1
      },
      liquidity
    ),
    false
  );

  const knifeLiquidity = resolveLiquidityMetrics(
    {
      depthFlags: ["MISSING_DEPTH"],
      antiFake: {
        liquidity: {
          signalType: "volume_7d",
          signalValue: 40
        }
      }
    },
    { marketVolume7d: 40, liquidityScore: 55 }
  );

  assert.equal(
    passesScannerGuards(
      {
        isOpportunity: true,
        itemCategory: "knife",
        itemName: "★ Karambit | Doppler (Factory New)",
        profit: 20,
        spreadPercent: 6,
        buyPrice: 45,
        marketCoverage: 2
      },
      knifeLiquidity
    ),
    false
  );
});

test("universe seed filter forwards missing-liquidity weapon skins with a penalty", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: null,
      marketVolume7d: null
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_missing_liquidity || 0), 0);
  assert.equal(Number(weaponSkinDiagnostics.penalty_missing_liquidity_allowed_forward || 0), 1);
});

test("universe seed filter allows strong weapon skins with missing liquidity evidence", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const recent = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "AK-47 | Bloodsport (Field-Tested)",
      itemCategory: "weapon_skin",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: 16,
      marketVolume7d: null,
      marketCoverageCount: 2,
      quoteFetchedAt: recent,
      maturityState: "near_eligible"
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_missing_liquidity || 0), 0);
  assert.equal(
    Number(weaponSkinDiagnostics.penalty_missing_liquidity_allowed_forward || 0),
    1
  );
});

test("universe seed filter forwards borderline missing-liquidity weapon skins with a penalty", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "P250 | See Ya Later (Field-Tested)",
      itemCategory: "weapon_skin",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: 7,
      marketVolume7d: null,
      marketCoverageCount: 2,
      quoteFetchedAt: null,
      maturityState: "enriching"
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_missing_liquidity || 0), 0);
  assert.equal(
    Number(weaponSkinDiagnostics.penalty_missing_liquidity_allowed_forward || 0),
    1
  );
});

test("universe seed filter forwards weak missing-snapshot skin seeds with penalties", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "Five-SeveN | Coolant (Minimal Wear)",
      itemCategory: "weapon_skin",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: null,
      marketVolume7d: null
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_missing_liquidity || 0), 0);
  assert.equal(Number(discardStats.hard_reject_low_value || 0), 0);
  assert.equal(
    Number(weaponSkinDiagnostics.penalty_missing_liquidity_allowed_forward || 0),
    1
  );
});

test("universe seed filter forwards low-value skin finish patterns with penalties", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "P90 | Sand Spray (Well-Worn)",
      itemCategory: "weapon_skin",
      hasSnapshotData: true,
      snapshotStale: false,
      referencePrice: 2.2,
      marketVolume7d: 80
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_low_value || 0), 0);
  assert.equal(Number(weaponSkinDiagnostics.penalty_low_value_allowed_forward || 0), 1);
});

test("universe seed filter keeps useful low-value-pattern skins with a penalty", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const recent = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "P90 | Sand Spray (Factory New)",
      itemCategory: "weapon_skin",
      hasSnapshotData: true,
      snapshotCapturedAt: recent,
      snapshotStale: false,
      referencePrice: 6.5,
      marketVolume7d: 90,
      marketCoverageCount: 2,
      quoteFetchedAt: recent,
      scanEligible: true
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_low_value || 0), 0);
  assert.equal(Number(weaponSkinDiagnostics.penalty_low_value_allowed_forward || 0), 1);
});

test("universe seed filter keeps borderline-coverage weapon skins contextual", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const recent = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "P90 | Sand Spray (Factory New)",
      itemCategory: "weapon_skin",
      hasSnapshotData: true,
      snapshotCapturedAt: recent,
      snapshotStale: false,
      referencePrice: 6.5,
      marketVolume7d: 90,
      marketCoverageCount: 1,
      quoteFetchedAt: recent,
      maturityState: "near_eligible"
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.hard_reject_low_value || 0), 0);
  assert.equal(Number(weaponSkinDiagnostics.penalty_low_value_allowed_forward || 0), 1);
});

test("universe seed filter applies variant penalties without auto-rejecting StatTrak or Souvenir", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const recent = new Date(Date.now() - 10 * 60 * 1000).toISOString();

  const statTrakAllowed = passesUniverseSeedFilters(
    {
      marketHashName: "StatTrak™ AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: 18,
      marketVolume7d: null,
      marketCoverageCount: 2,
      quoteFetchedAt: recent,
      maturityState: "near_eligible"
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );
  const souvenirAllowed = passesUniverseSeedFilters(
    {
      marketHashName: "Souvenir M4A1-S | Printstream (Field-Tested)",
      itemCategory: "weapon_skin",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: 22,
      marketVolume7d: null,
      marketCoverageCount: 2,
      quoteFetchedAt: recent,
      maturityState: "near_eligible"
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(statTrakAllowed, true);
  assert.equal(souvenirAllowed, true);
  assert.equal(Number(weaponSkinDiagnostics.stattrak_penalty || 0), 1);
  assert.equal(Number(weaponSkinDiagnostics.souvenir_penalty || 0), 1);
});

test("universe seed filter forwards stale snapshot seeds with penalties", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      hasSnapshotData: true,
      snapshotStale: true,
      snapshotCapturedAt: new Date(Date.now() - 30 * 60 * 60 * 1000).toISOString(),
      referencePrice: 15,
      marketVolume7d: 300
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.ignored_stale_data || 0), 0);
  assert.equal(Number(weaponSkinDiagnostics.stale_penalty_allowed_forward || 0), 1);
});

test("universe seed filter forwards market-relevant stale weapon skins with a penalty", () => {
  const discardStats = {};
  const weaponSkinDiagnostics = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "M4A1-S | Printstream (Field-Tested)",
      itemCategory: "weapon_skin",
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString(),
      quoteFetchedAt: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
      referencePrice: 22,
      marketVolume7d: 180,
      marketCoverageCount: 2,
      scanEligible: true
    },
    discardStats,
    null,
    { weaponSkinDiagnostics }
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.ignored_stale_data || 0), 0);
  assert.equal(Number(weaponSkinDiagnostics.stale_penalty_allowed_forward || 0), 1);
});

test("universe seed filter does not hard-reject premium categories on reference price floors", () => {
  const discardStats = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "★ Karambit | Doppler (Factory New)",
      itemCategory: "knife",
      hasSnapshotData: true,
      snapshotStale: false,
      referencePrice: 19.5,
      marketVolume7d: 55
    },
    discardStats
  );

  assert.equal(allowed, true);
  assert.equal(Number(discardStats.ignored_low_value_universe || 0), 0);
});

test("api row keeps required shape with clamped score", () => {
  const row = buildApiOpportunityRow({
    opportunity: {
      itemId: 1001,
      itemName: "AK-47 | Redline (Field-Tested)",
      buyMarket: "steam",
      buyPrice: 100,
      sellMarket: "skinport",
      sellNet: 111,
      profit: 11,
      spreadPercent: 11,
      opportunityScore: 92
    },
    inputItem: {
      skinId: 1001,
      itemRarity: "Covert",
      itemRarityColor: "#eb4b4b"
    },
    liquidity: {
      liquidityScore: 65,
      volume7d: 420
    },
    stale: {
      penalty: 10
    },
    perMarket: [
      { source: "steam", url: "https://steamcommunity.com/market/listings/730/AK-47" },
      { source: "skinport", url: "https://skinport.com/market" }
    ]
  });

  assert.equal(row.itemId, 1001);
  assert.equal(row.score, 82);
  assert.equal(row.spread, 11);
  assert.equal(row.liquidity, 420);
  assert.equal(row.itemRarity, "Covert");
  assert.equal(row.itemRarityColor, "#eb4b4b");
  assert.equal(typeof row.itemName, "string");
  assert.equal(typeof row.buyMarket, "string");
  assert.equal(typeof row.sellMarket, "string");
});

test("risky weapon-skin evaluation allows speculative fallback for missing liquidity", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "StatTrak™ AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 12,
      profit: 2.2,
      spreadPercent: 14,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: null
    },
    stale: {
      selectedState: "fresh",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "StatTrak™ AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 14,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 10 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky",
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      minVolume7d: 40,
      minMarketCoverage: 2,
      allowMissingLiquidity: true
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(evaluation.speculativeEligible, true);
  assert.equal(evaluation.allowLowConfidencePath, true);
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("penalty_missing_liquidity_allowed_forward"),
    true
  );
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("stattrak_penalty"),
    true
  );
});

test("named risky weapon-skin profile preserves evidence-based missing-liquidity fallback", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "StatTrak™ AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 12,
      profit: 2.2,
      spreadPercent: 14,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: null
    },
    stale: {
      selectedState: "fresh",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "StatTrak™ AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 14,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 10 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky_weapon_skin",
      minPriceUsd: 3,
      minProfitUsd: 0.7,
      minSpreadPercent: 3.75,
      minVolume7d: 35,
      minMarketCoverage: 2,
      minScore: 38,
      allowMissingLiquidity: false,
      allowMissingDepthWithPenalty: false,
      allowBorderlinePromotion: true,
      requireFreshData: false,
      maxQuoteAgeMinutes: Infinity
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(evaluation.speculativeEligible, true);
  assert.equal(evaluation.allowLowConfidencePath, true);
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("penalty_missing_liquidity_allowed_forward"),
    true
  );
});

test("named risky weapon-skin profile can promote borderline single-market coverage", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 12,
      profit: 2.2,
      spreadPercent: 10,
      marketCoverage: 1
    },
    liquidity: {
      volume7d: 52
    },
    stale: {
      selectedState: "fresh",
      usableMarkets: 1,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 14,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
      marketCoverageCount: 1
    },
    profile: {
      name: "risky_weapon_skin",
      minPriceUsd: 3,
      minProfitUsd: 0.7,
      minSpreadPercent: 3.75,
      minVolume7d: 35,
      minMarketCoverage: 2,
      minScore: 38,
      allowMissingLiquidity: false,
      allowMissingDepthWithPenalty: false,
      allowBorderlinePromotion: true,
      requireFreshData: false,
      maxQuoteAgeMinutes: Infinity
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(
    evaluation.borderlinePromotionKeys.includes("borderline_market_coverage_promoted"),
    true
  );
  assert.equal(Number(evaluation.penalty || 0) >= 5, true);
});

test("risky weapon-skin evaluation does not reject only because liquidity is missing", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "Five-SeveN | Coolant (Minimal Wear)",
      itemCategory: "weapon_skin",
      buyPrice: 3.4,
      profit: 0.85,
      spreadPercent: 4.2,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: null
    },
    stale: {
      selectedState: "stale",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "Five-SeveN | Coolant (Minimal Wear)",
      itemCategory: "weapon_skin",
      referencePrice: 3.6,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 10 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky",
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      minVolume7d: 40,
      minMarketCoverage: 2,
      allowMissingLiquidity: true
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(evaluation.speculativeEligible, true);
  assert.equal(evaluation.allowLowConfidencePath, true);
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("penalty_low_value_allowed_forward"),
    true
  );
  assert.equal(Number(evaluation.penalty || 0) >= 30, true);
});

test("risky weapon-skin evaluation forwards borderline low-value skins into speculative", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "P90 | Sand Spray (Factory New)",
      itemCategory: "weapon_skin",
      buyPrice: 6.2,
      profit: 1.35,
      spreadPercent: 7.5,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: 55
    },
    stale: {
      selectedState: "fresh",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "P90 | Sand Spray (Factory New)",
      itemCategory: "weapon_skin",
      referencePrice: 6.8,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 10 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky",
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      minVolume7d: 40,
      minMarketCoverage: 2,
      allowMissingLiquidity: true
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(evaluation.speculativeEligible, true);
  assert.equal(evaluation.allowLowConfidencePath, true);
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("penalty_low_value_allowed_forward"),
    true
  );
});

test("named risky weapon-skin profile admits borderline medium-liquidity candidates", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "AK-47 | Neon Rider (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 4.2,
      profit: 1.05,
      spreadPercent: 4.9,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: 30
    },
    stale: {
      selectedState: "aging",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "AK-47 | Neon Rider (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 5,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 25 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky_weapon_skin",
      minPriceUsd: 3,
      minProfitUsd: 0.7,
      minSpreadPercent: 3.75,
      minVolume7d: 35,
      minMarketCoverage: 2,
      allowMissingLiquidity: false,
      allowBorderlinePromotion: true
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(
    evaluation.borderlinePromotionKeys.includes("borderline_liquidity_promoted"),
    true
  );
});

test("risky weapon-skin evaluation still rejects weak medium-liquidity borderlines", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "AK-47 | Emerald Pinstripe (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 4.1,
      profit: 0.78,
      spreadPercent: 4.4,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: 20
    },
    stale: {
      selectedState: "aging",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "AK-47 | Emerald Pinstripe (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 4.8,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 20 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky_weapon_skin",
      minPriceUsd: 3,
      minProfitUsd: 0.7,
      minSpreadPercent: 3.75,
      minVolume7d: 35,
      minMarketCoverage: 2,
      allowMissingLiquidity: false,
      allowBorderlinePromotion: true
    }
  });

  assert.equal(evaluation.passed, false);
  assert.equal(evaluation.primaryReason, "ignored_low_liquidity");
});

test("risky weapon-skin evaluation applies aging penalty without hard rejection", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "USP-S | Printstream (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 14,
      profit: 1.9,
      spreadPercent: 9.5,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: 140
    },
    stale: {
      selectedState: "aging",
      usableMarkets: 2,
      hasInsufficientUsableMarkets: false
    },
    inputItem: {
      marketHashName: "USP-S | Printstream (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 16,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 70 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky",
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      minVolume7d: 40,
      minMarketCoverage: 2,
      allowMissingLiquidity: true
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(evaluation.allowLowConfidencePath, false);
  assert.equal(evaluation.staleForwardedTier, "risky");
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("aging_penalty_allowed_forward"),
    true
  );
});

test("risky weapon-skin evaluation forwards strong stale skins into speculative", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 18,
      profit: 2.6,
      spreadPercent: 14,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: 140
    },
    stale: {
      selectedState: "stale",
      usableMarkets: 0,
      hasInsufficientUsableMarkets: true
    },
    inputItem: {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 20,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 25 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky",
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      minVolume7d: 40,
      minMarketCoverage: 2,
      allowMissingLiquidity: true
    }
  });

  assert.equal(evaluation.passed, true);
  assert.equal(evaluation.speculativeEligible, true);
  assert.equal(evaluation.allowLowConfidencePath, true);
  assert.equal(evaluation.staleForwardedTier, "speculative");
  assert.equal(
    evaluation.diagnosticPenaltyKeys.includes("stale_penalty_allowed_forward"),
    true
  );
});

test("risky weapon-skin evaluation still rejects weak stale combinations", () => {
  const evaluation = computeRiskAdjustments({
    opportunity: {
      itemName: "PP-Bizon | Facility Sketch (Field-Tested)",
      itemCategory: "weapon_skin",
      buyPrice: 4.2,
      profit: 0.9,
      spreadPercent: 4.5,
      marketCoverage: 2
    },
    liquidity: {
      volume7d: 35,
      hasExtremeReferenceDeviation: true
    },
    stale: {
      selectedState: "stale",
      usableMarkets: 0,
      hasInsufficientUsableMarkets: true
    },
    inputItem: {
      marketHashName: "PP-Bizon | Facility Sketch (Field-Tested)",
      itemCategory: "weapon_skin",
      referencePrice: 4.4,
      hasSnapshotData: true,
      snapshotCapturedAt: new Date(Date.now() - 20 * 60 * 1000).toISOString()
    },
    profile: {
      name: "risky",
      minPriceUsd: 3,
      minProfitUsd: 0.75,
      minSpreadPercent: 4,
      minVolume7d: 40,
      minMarketCoverage: 2,
      allowMissingLiquidity: true
    }
  });

  assert.equal(evaluation.passed, false);
  assert.equal(evaluation.primaryReason, "ignored_stale_data");
  assert.equal(evaluation.diagnosticRejectionKey, "hard_reject_stale");
});

test("risky diagnostics summarize previous vs current thresholds and added admits", () => {
  const diagnostics = buildRiskyProfileDiagnostics();
  trackRiskyDecision(diagnostics, "weapon_skin", "attempted");
  trackRiskyDecision(diagnostics, "weapon_skin", "accepted", "accepted_borderline_promotion");
  trackRiskyBaselineOutcome(diagnostics, "weapon_skin", false);
  trackRiskyBorderlinePromotion(diagnostics, "weapon_skin", ["borderline_liquidity_promoted"]);
  trackRiskyDecision(diagnostics, "weapon_skin", "rejected", "ignored_low_liquidity");
  diagnostics.weapon_skin.additionalAcceptedVsBaseline = 1;

  const summary = toRiskyProfileDiagnosticsSummary(diagnostics);

  assert.equal(summary.weapon_skin.previousProfile.minPriceUsd > summary.weapon_skin.profile.minPriceUsd, true);
  assert.equal(summary.weapon_skin.previousProfile.minVolume7d > summary.weapon_skin.profile.minVolume7d, true);
  assert.equal(summary.weapon_skin.additionalAcceptedVsBaseline, 1);
  assert.equal(summary.weapon_skin.borderlinePromoted, 1);
  assert.equal(summary.weapon_skin.topRejectedReasons[0].reason, "ignored_low_liquidity");
});

test("api row keeps high confidence when quotes are fresh and snapshot is only aging", () => {
  const originalNow = Date.now;
  Date.now = () => Date.parse("2026-03-13T12:00:00.000Z");

  try {
    const row = buildApiOpportunityRow({
      opportunity: {
        itemId: 1002,
        itemName: "M4A1-S | Printstream (Field-Tested)",
        buyMarket: "steam",
        buyPrice: 100,
        sellMarket: "skinport",
        sellNet: 115,
        profit: 15,
        spreadPercent: 15,
        opportunityScore: 91,
        executionConfidence: "High"
      },
      inputItem: {
        skinId: 1002,
        hasSnapshotData: true,
        snapshotCapturedAt: "2026-03-13T11:10:00.000Z"
      },
      liquidity: {
        liquidityScore: 72,
        volume7d: 240
      },
      stale: {
        selectedState: "fresh",
        state: "fresh",
        penalty: 0
      }
    });

    assert.equal(row.snapshotFreshnessState, "aging");
    assert.equal(row.executionConfidence, "High");
  } finally {
    Date.now = originalNow;
  }
});

test("api row only downgrades high confidence one level for stale snapshots", () => {
  const originalNow = Date.now;
  Date.now = () => Date.parse("2026-03-13T12:00:00.000Z");

  try {
    const row = buildApiOpportunityRow({
      opportunity: {
        itemId: 1003,
        itemName: "USP-S | Kill Confirmed (Field-Tested)",
        buyMarket: "steam",
        buyPrice: 100,
        sellMarket: "skinport",
        sellNet: 114,
        profit: 14,
        spreadPercent: 14,
        opportunityScore: 89,
        executionConfidence: "High"
      },
      inputItem: {
        skinId: 1003,
        hasSnapshotData: true,
        snapshotCapturedAt: "2026-03-13T10:20:00.000Z"
      },
      liquidity: {
        liquidityScore: 70,
        volume7d: 220
      },
      stale: {
        selectedState: "fresh",
        state: "fresh",
        penalty: 0
      }
    });

    assert.equal(row.snapshotFreshnessState, "stale");
    assert.equal(row.executionConfidence, "Medium");
  } finally {
    Date.now = originalNow;
  }
});

test("feed mapper keeps scanner row core fields", () => {
  const insertRow = buildFeedInsertRow(
    {
      itemName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      buyMarket: "steam",
      buyPrice: 100,
      sellMarket: "skinport",
      sellNet: 113,
      profit: 13,
      spread: 13,
      score: 86,
      executionConfidence: "High",
      liquidityBand: "High",
      itemId: 1001,
      itemRarity: "Covert",
      itemRarityColor: "#eb4b4b",
      itemImageUrl: "https://cdn.example.com/item.png",
      badges: ["High liquidity"],
      flags: ["MISSING_DEPTH"],
      isHighConfidenceEligible: true,
      isRiskyEligible: true
    },
    "11111111-1111-1111-1111-111111111111",
    {
      detectedAt: "2026-03-07T00:00:00.000Z",
      isDuplicate: false,
      eventType: "updated",
      eventAnalysis: {
        changeReasons: ["profit", "score"],
        profitDeltaPercent: 18,
        scoreDelta: 9
      },
      previousRow: {
        id: "00000000-0000-0000-0000-000000000000",
        detected_at: "2026-03-06T23:30:00.000Z"
      }
    }
  );

  const apiRow = mapFeedRowToApiRow({
    id: "22222222-2222-2222-2222-222222222222",
    ...insertRow
  });

  assert.equal(apiRow.feedId, "22222222-2222-2222-2222-222222222222");
  assert.equal(apiRow.itemName, "AK-47 | Redline (Field-Tested)");
  assert.equal(apiRow.buyMarket, "steam");
  assert.equal(apiRow.sellMarket, "skinport");
  assert.equal(apiRow.score, 86);
  assert.equal(apiRow.executionConfidence, "High");
  assert.equal(apiRow.itemRarity, "Covert");
  assert.equal(apiRow.itemRarityColor, "#eb4b4b");
  assert.equal(apiRow.eventType, "updated");
  assert.deepEqual(apiRow.changeReasons, ["profit", "score"]);
  assert.equal(apiRow.previousFeedId, "00000000-0000-0000-0000-000000000000");
  assert.equal(Array.isArray(apiRow.badges), true);
});

test("feed mapper drops known broken item image hosts", () => {
  const apiRow = mapFeedRowToApiRow({
    id: "33333333-3333-3333-3333-333333333333",
    item_name: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    buy_market: "steam",
    sell_market: "skinport",
    opportunity_score: 80,
    execution_confidence: "medium",
    metadata: {
      item_image_url: "https://example.com/ak-redline.png"
    }
  });

  assert.equal(apiRow.itemImageUrl, null);
});

test("material dedupe detection uses profit and score thresholds", () => {
  assert.equal(
    isMateriallyNewOpportunity(
      { profit: 120, score: 82 },
      { profit: 100, opportunity_score: 81 }
    ),
    true
  );
  assert.equal(
    isMateriallyNewOpportunity(
      { profit: 104, score: 90 },
      { profit: 100, opportunity_score: 80 }
    ),
    true
  );
  assert.equal(
    isMateriallyNewOpportunity(
      { profit: 103, score: 84 },
      { profit: 100, opportunity_score: 80 }
    ),
    false
  );
});

test("feed event classifier distinguishes updated and reactivated signals", () => {
  const updated = classifyOpportunityFeedEvent(
    {
      profit: 108,
      spread: 15,
      score: 89,
      executionConfidence: "High",
      liquidityBand: "High",
      liquidity: 220
    },
    {
      profit: 100,
      spread_pct: 11,
      opportunity_score: 80,
      execution_confidence: "Medium",
      liquidity_label: "Medium",
      is_active: true,
      metadata: {
        liquidity_value: 120
      }
    }
  );
  const reactivated = classifyOpportunityFeedEvent(
    {
      profit: 101,
      spread: 11,
      score: 80,
      executionConfidence: "Medium",
      liquidityBand: "Medium",
      liquidity: 100
    },
    {
      profit: 101,
      spread_pct: 11,
      opportunity_score: 80,
      execution_confidence: "Medium",
      liquidity_label: "Medium",
      is_active: false,
      metadata: {
        liquidity_value: 100
      }
    }
  );

  assert.equal(updated.eventType, "updated");
  assert.equal(updated.changeReasons.includes("spread"), true);
  assert.equal(updated.changeReasons.includes("score"), true);
  assert.equal(updated.changeReasons.includes("confidence"), true);
  assert.equal(updated.changeReasons.includes("liquidity"), true);
  assert.equal(reactivated.eventType, "reactivated");
  assert.equal(reactivated.materiallyChanged, true);
});

test("scanner overdue watchdog respects running state and stale timestamps", () => {
  const now = Date.parse("2026-03-07T12:00:00.000Z");

  assert.equal(
    isScannerRunOverdue({
      latestRun: { status: "running", started_at: "2026-03-07T11:58:00.000Z" },
      latestCompletedRun: { completed_at: "2026-03-07T11:55:00.000Z" }
    }, now),
    false
  );

  assert.equal(
    isScannerRunOverdue({
      latestRun: { status: "completed", started_at: "2026-03-07T11:56:00.000Z" },
      latestCompletedRun: { completed_at: "2026-03-07T11:56:00.000Z" }
    }, now),
    false
  );

  assert.equal(
    isScannerRunOverdue({
      latestRun: { status: "completed", started_at: "2026-03-07T11:40:00.000Z" },
      latestCompletedRun: { completed_at: "2026-03-07T11:40:00.000Z" }
    }, now),
    true
  );

  assert.equal(isScannerRunOverdue({}, now), true);
});

test("scanner defaults are configured for 3k-scale batching", () => {
  // Runtime env can override this lower in tests; service fallback baseline is 3000.
  assert.equal(DEFAULT_UNIVERSE_LIMIT >= 1000, true);
  assert.equal(SCAN_BATCH_SIZE >= 25 && SCAN_BATCH_SIZE <= 50, true);
  assert.equal(MAX_CONCURRENT_MARKET_REQUESTS >= 1, true);
  assert.equal(SCAN_TIMEOUT_PER_BATCH >= 1000, true);
});

test("strict seed coverage threshold scales with universe size", () => {
  assert.equal(computeStrictCoverageThreshold(0) >= 10, true);
  assert.equal(computeStrictCoverageThreshold(50), 30);
  assert.equal(computeStrictCoverageThreshold(198), 119);
});

test("seed maturity model maps ready items to hot layer", () => {
  const maturity = resolveMaturityStateForSeed({
    marketHashName: "Revolution Case",
    itemCategory: "case",
    candidateStatus: "eligible",
    scanEligible: true,
    hasSnapshotData: true,
    snapshotStale: false,
    referencePrice: 3.4,
    marketCoverageCount: 3,
    marketVolume7d: 240,
    liquidityRank: 78
  });
  const cold = resolveMaturityStateForSeed({
    marketHashName: "Dreams & Nightmares Sticker Capsule",
    itemCategory: "sticker_capsule",
    candidateStatus: "candidate",
    scanEligible: false,
    hasSnapshotData: false,
    snapshotStale: true,
    referencePrice: null,
    marketCoverageCount: 0,
    marketVolume7d: null,
    liquidityRank: 10
  });

  assert.equal(maturity.maturityState, "eligible");
  assert.equal(resolveScanLayerForMaturity(maturity), "hot");
  assert.equal(cold.maturityState, "cold");
  assert.equal(resolveScanLayerForMaturity(cold), "cold");
  assert.equal(Number(maturity.maturityScore || 0) > Number(cold.maturityScore || 0), true);
  assert.equal(computeLayerPriority({ ...maturity, itemCategory: "case" }) > 0, true);
});

test("seed maturity keeps fresh-quote near-eligible rows out of enriching", () => {
  const maturity = resolveMaturityStateForSeed({
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "near_eligible",
    scanEligible: false,
    hasSnapshotData: true,
    snapshotCapturedAt: new Date(Date.now() - 80 * 60 * 1000).toISOString(),
    snapshotStale: true,
    quoteFetchedAt: new Date().toISOString(),
    referencePrice: 11.2,
    marketCoverageCount: 1,
    marketVolume7d: 52,
    liquidityRank: 61
  });

  assert.equal(maturity.maturityState, "near_eligible");
  assert.equal(resolveScanLayerForMaturity(maturity), "warm");
});

test("opportunity readiness allows safe partial weapon skins (including uncatalogued rows)", () => {
  const recent = new Date().toISOString();
  const readyNearEligible = {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "near_eligible",
    maturityState: "near_eligible",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    progressionStatus: "blocked_from_eligible",
    marketCoverageCount: 1
  };
  const partialButSafe = {
    ...readyNearEligible,
    marketHashName: "USP-S | Cortex (Field-Tested)",
    missingSnapshot: true,
    missingReference: true,
    hasSnapshotData: false,
    snapshotCapturedAt: null,
    snapshotState: "missing_snapshot",
    referenceState: "missing_reference",
    liquidityState: "missing_liquidity",
    referencePrice: null,
    marketVolume7d: null,
    marketCoverageCount: 3
  };
  const uncataloguedPartial = {
    ...partialButSafe,
    marketHashName: "M4A1-S | Cyrex (Field-Tested)",
    candidateStatus: "candidate",
    progressionStatus: "blocked_from_near_eligible"
  };

  assert.equal(resolveCatalogSeedFreshnessContext(readyNearEligible, "weapon_skin").usable, true);
  assert.equal(isOpportunityScanReadySeed(readyNearEligible), true);
  assert.equal(isOpportunityScanReadySeed(partialButSafe), true);
  assert.equal(isOpportunityScanReadySeed(uncataloguedPartial), true);
});

test("opportunity readiness forwards zero-coverage weapon skins and still flags enriching backfill readiness separately", () => {
  const recent = new Date().toISOString();
  const zeroCoverage = {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "near_eligible",
    progressionStatus: "blocked_from_eligible",
    maturityState: "near_eligible",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    referencePrice: 11.5,
    marketCoverageCount: 0
  };
  const blockedEnriching = {
    marketHashName: "AK-47 | Vulcan (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "enriching",
    progressionStatus: "blocked_from_near_eligible",
    maturityState: "enriching",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    referencePrice: 24,
    marketCoverageCount: 0
  };
  const allowedEnriching = {
    ...blockedEnriching,
    marketHashName: "AK-47 | Bloodsport (Field-Tested)",
    marketCoverageCount: 1
  };

  assert.equal(isOpportunityScanReadySeed(zeroCoverage), true);
  assert.equal(isMinimumOpportunityBackfillReadySeed(blockedEnriching), false);
  assert.equal(isMinimumOpportunityBackfillReadySeed(allowedEnriching), true);
});

test("layered scanning prioritizes hot core and limits cold scan share", () => {
  const hotSeeds = Array.from({ length: 180 }, (_, index) => ({
    marketHashName: `Hot Seed ${index}`,
    itemCategory: index % 3 === 0 ? "case" : "weapon_skin",
    maturityState: "eligible",
    maturityScore: 85,
    liquidityRank: 70 - (index % 20),
    enrichmentPriority: 60,
    scanLayer: "hot",
    layerPriority: 120 - index
  }));
  const warmSeeds = Array.from({ length: 300 }, (_, index) => ({
    marketHashName: `Warm Seed ${index}`,
    itemCategory: index % 2 === 0 ? "sticker_capsule" : "weapon_skin",
    maturityState: "near_eligible",
    maturityScore: 64,
    liquidityRank: 48 - (index % 12),
    enrichmentPriority: 72 - (index % 10),
    scanLayer: "warm",
    layerPriority: 95 - (index % 15)
  }));
  const coldSeeds = Array.from({ length: 220 }, (_, index) => ({
    marketHashName: `Cold Seed ${index}`,
    itemCategory: "weapon_skin",
    maturityState: "cold",
    maturityScore: 22,
    liquidityRank: 12,
    enrichmentPriority: 28,
    scanLayer: "cold",
    layerPriority: 30 - (index % 8)
  }));

  const selection = selectSeedsForLayeredScanning([...hotSeeds, ...warmSeeds, ...coldSeeds]);
  const opportunityLayers = selection?.diagnostics?.opportunity?.layers || {};
  const enrichmentLayers = selection?.diagnostics?.enrichment?.layers || {};
  const hotUniverse = selection?.diagnostics?.hotUniverse || {};

  assert.equal(Number(selection?.coreSeeds?.length || 0) >= 25, true);
  assert.equal(Number(selection?.opportunitySeeds?.length || 0) > 0, true);
  assert.equal(Number(selection?.opportunitySeeds?.length || 0), HOT_OPPORTUNITY_SCAN_TARGET);
  assert.equal(Number(opportunityLayers.hot || 0) >= Number(opportunityLayers.cold || 0), true);
  assert.equal(Number(hotUniverse.eligibleCount || 0) > 0, true);
  assert.equal(Number(hotUniverse.nearEligibleCount || 0) > 0, true);
  assert.equal(Number(selection?.enrichmentSeeds?.length || 0) > 0, true);
  assert.equal(Number(enrichmentLayers.hot || 0), 0);
  assert.equal(Number(selection?.enrichmentSeeds?.length || 0) <= 150, true);
});

test("snapshot warmup backlog can be deferred out of opportunity scan", () => {
  const seeds = [
    {
      marketHashName: "AK-47 | Vulcan (Field-Tested)",
      itemCategory: "weapon_skin",
      skinId: 11,
      hasSnapshotData: false,
      snapshotStale: true,
      maturityScore: 72,
      enrichmentPriority: 64,
      liquidityRank: 45
    },
    {
      marketHashName: "Revolution Case",
      itemCategory: "case",
      skinId: 12,
      hasSnapshotData: true,
      snapshotStale: true,
      maturityScore: 68,
      enrichmentPriority: 48,
      liquidityRank: 30
    },
    {
      marketHashName: "Operation Breakout Case",
      itemCategory: "case",
      skinId: 13,
      hasSnapshotData: true,
      snapshotStale: false,
      maturityScore: 84,
      enrichmentPriority: 52,
      liquidityRank: 28
    }
  ];

  const backlog = summarizeSnapshotWarmupBacklog(seeds);

  assert.equal(Number(backlog.freshSeedsBefore || 0), 1);
  assert.equal(Number(backlog.missingSnapshotBacklog || 0), 1);
  assert.equal(Number(backlog.warmupCandidates || 0), 2);
  assert.equal(Number(backlog.warmupCandidatesByCategory.weapon_skin || 0), 1);
  assert.equal(Number(backlog.warmupCandidatesByCategory.case || 0), 1);
});

test("layered scanning fills opportunity target with enriching backfill when mature supply is thin", () => {
  const hotSeeds = Array.from({ length: 10 }, (_, index) => ({
    marketHashName: `Limited Hot ${index}`,
    itemCategory: "weapon_skin",
    maturityState: "eligible",
    maturityScore: 82,
    liquidityRank: 60,
    enrichmentPriority: 55,
    scanLayer: "hot",
    layerPriority: 120 - index
  }));
  const nearEligibleSeeds = Array.from({ length: 10 }, (_, index) => ({
    marketHashName: `Limited Near ${index}`,
    itemCategory: "case",
    maturityState: "near_eligible",
    maturityScore: 66,
    liquidityRank: 44,
    enrichmentPriority: 58,
    scanLayer: "warm",
    layerPriority: 90 - index
  }));
  const enrichingSeeds = Array.from({ length: 100 }, (_, index) => ({
    marketHashName: `Limited Enriching ${index}`,
    itemCategory: "weapon_skin",
    maturityState: "enriching",
    maturityScore: 48,
    liquidityRank: 26,
    enrichmentPriority: 72 - (index % 12),
    scanLayer: "warm",
    layerPriority: 65 - (index % 20)
  }));

  const selection = selectSeedsForLayeredScanning(
    [...hotSeeds, ...nearEligibleSeeds, ...enrichingSeeds],
    {
      opportunityTarget: 50,
      hotTarget: 20,
      nearEligibleTarget: 20
    }
  );

  assert.equal(Number(selection?.diagnostics?.selectedEnrichingForOpportunity || 0) > 0, true);
  assert.equal(Number(selection?.diagnostics?.matureOpportunityShortfall || 0), 0);
  assert.equal(Number(selection?.opportunitySeeds?.length || 0), 50);
});

test("layered scanning can include enrichment-needing rows in opportunity scan", () => {
  const matureSeeds = Array.from({ length: 12 }, (_, index) => ({
    marketHashName: `Ready ${index}`,
    itemCategory: index % 2 === 0 ? "weapon_skin" : "case",
    candidateStatus: index < 6 ? "eligible" : "near_eligible",
    progressionStatus: index < 6 ? "eligible" : "blocked_from_eligible",
    maturityState: index < 6 ? "eligible" : "near_eligible",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: new Date().toISOString(),
    snapshotStale: false,
    quoteFetchedAt: new Date().toISOString(),
    marketCoverageCount: 2,
    referencePrice: 12 + index,
    marketVolume7d: 80 + index,
    maturityScore: 80 - index,
    liquidityRank: 50 - index,
    enrichmentPriority: 60 - index,
    scanLayer: index < 6 ? "hot" : "warm",
    layerPriority: 120 - index
  }));
  const enrichingSeeds = Array.from({ length: 20 }, (_, index) => ({
    marketHashName: `Needs Enrichment ${index}`,
    itemCategory: "weapon_skin",
    candidateStatus: "candidate",
    progressionStatus: "blocked_from_near_eligible",
    maturityState: "near_eligible",
    missingSnapshot: true,
    missingReference: false,
    hasSnapshotData: false,
    snapshotCapturedAt: null,
    snapshotStale: true,
    quoteFetchedAt: new Date().toISOString(),
    maturityScore: 70 - (index % 5),
    liquidityRank: 30,
    enrichmentPriority: 55 - (index % 7),
    scanLayer: "warm",
    layerPriority: 90 - index
  }));

  const selection = selectSeedsForLayeredScanning([...matureSeeds, ...enrichingSeeds], {
    opportunityTarget: 20,
    hotTarget: 10,
    nearEligibleTarget: 10,
    opportunityFilter: isOpportunityScanReadySeed
  });

  assert.equal(Number(selection?.diagnostics?.matureOnlyOpportunitySelection || 0), 1);
  assert.equal(Number(selection?.diagnostics?.deferredToEnrichmentItems || 0), 0);
  assert.equal(Number(selection?.diagnostics?.selectedEnrichingForOpportunity || 0), 0);
  assert.equal(Number(selection?.opportunitySeeds?.length || 0), 20);
});

test("opportunity admission diagnostics report scan states and reasons", () => {
  const recent = new Date().toISOString();
  const staleTs = new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString();
  const readyPartialWeaponSkin = {
    marketHashName: "AK-47 | Asiimov (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "near_eligible",
    progressionStatus: "blocked_from_eligible",
    maturityState: "near_eligible",
    missingSnapshot: true,
    missingReference: true,
    hasSnapshotData: false,
    snapshotCapturedAt: null,
    snapshotState: "missing_snapshot",
    referenceState: "missing_reference",
    liquidityState: "missing_liquidity",
    quoteFetchedAt: recent,
    marketCoverageCount: 3,
    referencePrice: null,
    marketVolume7d: null
  };
  const blockedMissingReference = {
    marketHashName: "Revolution Case",
    itemCategory: "case",
    candidateStatus: "near_eligible",
    progressionStatus: "blocked_from_eligible",
    maturityState: "near_eligible",
    missingSnapshot: false,
    missingReference: true,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    marketCoverageCount: 2,
    referencePrice: null,
    marketVolume7d: 220
  };
  const blockedMissingLiquidity = {
    marketHashName: "Paris 2023 Legends Sticker Capsule",
    itemCategory: "sticker_capsule",
    candidateStatus: "near_eligible",
    progressionStatus: "blocked_from_eligible",
    maturityState: "near_eligible",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    marketCoverageCount: 2,
    referencePrice: 7.5,
    marketVolume7d: null,
    liquidityState: "missing_liquidity"
  };
  const blockedStale = {
    marketHashName: "AWP | Neo-Noir (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "near_eligible",
    progressionStatus: "blocked_from_eligible",
    maturityState: "near_eligible",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: staleTs,
    snapshotStale: true,
    quoteFetchedAt: staleTs,
    marketCoverageCount: 2,
    referencePrice: 18,
    marketVolume7d: 90
  };
  const blockedRisk = {
    marketHashName: "AK-47 | Case Hardened",
    itemCategory: "weapon_skin",
    candidateStatus: "near_eligible",
    progressionStatus: "blocked_from_eligible",
    maturityState: "near_eligible",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    marketCoverageCount: 2,
    referencePrice: 145,
    marketVolume7d: 55,
    invalidReason: "rejectedNamePattern"
  };
  const blockedMaturity = {
    marketHashName: "USP-S | Printstream (Field-Tested)",
    itemCategory: "weapon_skin",
    candidateStatus: "enriching",
    progressionStatus: "blocked_from_near_eligible",
    maturityState: "enriching",
    missingSnapshot: false,
    missingReference: false,
    hasSnapshotData: true,
    snapshotCapturedAt: recent,
    snapshotStale: false,
    quoteFetchedAt: recent,
    marketCoverageCount: 2,
    referencePrice: 42,
    marketVolume7d: 80
  };

  const admissions = [
    readyPartialWeaponSkin,
    blockedMissingReference,
    blockedMissingLiquidity,
    blockedStale,
    blockedRisk,
    blockedMaturity
  ].map((row) => ({
    row,
    admission: evaluateOpportunitySeedAdmission(row)
  }));
  const summary = summarizeOpportunitySeedAdmissions(admissions, [readyPartialWeaponSkin]);

  assert.equal(summary.universe_total, 6);
  assert.equal(summary.universe_eligible, 0);
  assert.equal(summary.universe_near_eligible, 5);
  assert.equal(summary.universe_blocked, 1);
  assert.equal(summary.scan_candidates_loaded, 5);
  assert.equal(summary.scan_candidates_deferred, 1);
  assert.equal(summary.scan_candidates_executed, 1);

  assert.equal(summary.scan_states.scan_now, 0);
  assert.equal(summary.scan_states.scan_with_penalties, 5);
  assert.equal(summary.scan_states.speculative_candidate, 0);
  assert.equal(summary.scan_states.hard_reject, 1);

  assert.equal(summary.penalty_reasons.missing_snapshot, 1);
  assert.equal(summary.penalty_reasons.missing_reference, 2);
  assert.equal(summary.penalty_reasons.missing_liquidity, 2);
  assert.equal(summary.penalty_reasons.stale_data, 1);
  assert.equal(summary.penalty_reasons.needs_enrichment, 1);
  assert.equal(Number(summary.speculative_reasons.multiple_missing_signals || 0), 0);
  assert.equal(summary.hard_reject_reasons.structural_or_antifake_block, 1);
});

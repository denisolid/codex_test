const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const marketUniverseTop100 = require("../src/config/marketUniverseTop100.json");
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
    buildApiOpportunityRow,
    buildFeedInsertRow,
    mapFeedRowToApiRow,
    isMateriallyNewOpportunity,
    isScannerRunOverdue
  }
} = require("../src/services/arbitrageScannerService");

test("market universe is fixed and unique with category metadata", () => {
  assert.equal(Array.isArray(marketUniverseTop100), true);
  assert.equal(marketUniverseTop100.length >= 100, true);
  assert.equal(marketUniverseTop100.length <= 160, true);
  const normalized = normalizeUniverseEntries(marketUniverseTop100);
  assert.equal(
    normalized.length,
    marketUniverseTop100.length,
  );
  assert.equal(
    normalized.some((row) => row.category === "weapon_skin"),
    true,
  );
  assert.equal(normalized.some((row) => row.category === "case"), true);
  assert.equal(
    normalized.some((row) => row.category === "sticker_capsule"),
    true,
  );
});

test("category normalization supports skins/cases/capsules aliases", () => {
  assert.equal(normalizeItemCategory("case", "Revolution Case"), "case");
  assert.equal(normalizeItemCategory("weapon_skin", "AK-47 | Redline"), "weapon_skin");
  assert.equal(normalizeItemCategory("", "Fracture Case"), "case");
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

test("stale penalty increases with quote age", () => {
  const now = Date.now();
  const freshIso = new Date(now - 5 * 60 * 1000).toISOString();
  const staleIso = new Date(now - 70 * 60 * 1000).toISOString();

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
});

test("universe seed filter allows fallback items without snapshot data", () => {
  const discardStats = {};
  const allowed = passesUniverseSeedFilters(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      hasSnapshotData: false,
      snapshotStale: false,
      referencePrice: null,
      marketVolume7d: null
    },
    discardStats
  );

  assert.equal(allowed, true);
  assert.equal(Object.keys(discardStats).length, 0);
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
      skinId: 1001
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
  assert.equal(typeof row.itemName, "string");
  assert.equal(typeof row.buyMarket, "string");
  assert.equal(typeof row.sellMarket, "string");
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
      itemImageUrl: "https://cdn.example.com/item.png",
      badges: ["High liquidity"],
      flags: ["MISSING_DEPTH"],
      isHighConfidenceEligible: true,
      isRiskyEligible: true
    },
    "11111111-1111-1111-1111-111111111111",
    {
      detectedAt: "2026-03-07T00:00:00.000Z",
      isDuplicate: false
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
  assert.equal(Array.isArray(apiRow.badges), true);
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

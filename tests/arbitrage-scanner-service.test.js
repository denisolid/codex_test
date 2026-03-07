const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const marketUniverseTop100 = require("../src/config/marketUniverseTop100.json");
const {
  __testables: {
    normalizeUniverseNames,
    computeLiquidityScoreFromSnapshot,
    resolveVolume7d,
    resolveStaleDataPenalty,
    resolveLiquidityMetrics,
    passesScannerGuards,
    buildApiOpportunityRow
  }
} = require("../src/services/arbitrageScannerService");

test("market universe is fixed and unique (Top 100)", () => {
  assert.equal(Array.isArray(marketUniverseTop100), true);
  assert.equal(marketUniverseTop100.length, 100);
  assert.equal(normalizeUniverseNames(marketUniverseTop100).length, 100);
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

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  evaluateItemOpportunity,
  rankOpportunities,
  categorizeOpportunityScore,
  __testables: {
    getSpreadScore,
    getLiquidityScore,
    getStabilityScore,
    getMarketScore
  }
} = require("../src/services/arbitrageEngineService");

test("evaluateItemOpportunity returns profitable opportunity with score and category", () => {
  const result = evaluateItemOpportunity({
    skinId: 1001,
    marketHashName: "AK-47 | Redline (Field-Tested)",
    sevenDayChangePercent: 3.2,
    liquiditySales: 240,
    perMarket: [
      { source: "steam", available: true, grossPrice: 123, netPriceAfterFees: 107, url: "https://steam" },
      { source: "skinport", available: true, grossPrice: 112, netPriceAfterFees: 98, url: "https://skinport" },
      { source: "csfloat", available: true, grossPrice: 119, netPriceAfterFees: 121.2, url: "https://csfloat" }
    ]
  });

  assert.equal(result.itemId, 1001);
  assert.equal(result.buyMarket, "skinport");
  assert.equal(result.sellMarket, "csfloat");
  assert.equal(result.buyPrice, 112);
  assert.equal(result.sellNet, 121.2);
  assert.equal(result.profit, 9.2);
  assert.equal(result.isOpportunity, true);
  assert.ok(Number(result.spreadPercent) >= 5);
  assert.ok(Number(result.opportunityScore) >= 0);
  assert.equal(result.scoreCategory, "Good");
  assert.equal(typeof result.executionConfidence, "string");
  assert.equal(result.scoreCategory, categorizeOpportunityScore(result.opportunityScore));
});

test("evaluateItemOpportunity flags non-opportunity when spread is below threshold", () => {
  const result = evaluateItemOpportunity({
    marketHashName: "AWP | Asiimov",
    sevenDayChangePercent: 1,
    liquiditySales: 220,
    perMarket: [
      { source: "steam", available: true, grossPrice: 100, netPriceAfterFees: 87 },
      { source: "dmarket", available: true, grossPrice: 99.2, netPriceAfterFees: 101.5 }
    ]
  });

  assert.equal(result.buyMarket, "dmarket");
  assert.equal(result.sellMarket, "dmarket");
  assert.equal(result.profit > 0, true);
  assert.equal(result.spreadPercent < 5, true);
  assert.equal(result.isOpportunity, false);
});

test("score helper buckets follow expected thresholds", () => {
  assert.equal(getSpreadScore(16), 80);
  assert.equal(getSpreadScore(28), 90);
  assert.equal(getSpreadScore(55), 70);
  assert.equal(getSpreadScore(7), 60);
  assert.equal(getSpreadScore(2.9), 20);

  assert.equal(getLiquidityScore(250), 85);
  assert.equal(getLiquidityScore(180), 70);
  assert.equal(getLiquidityScore(70), 30);
  assert.equal(getLiquidityScore(20), 30);

  assert.equal(getStabilityScore(2), 100);
  assert.equal(getStabilityScore(8), 80);
  assert.equal(getStabilityScore(15), 50);
  assert.equal(getStabilityScore(31), 20);

  assert.equal(getMarketScore("steam", "skinport"), 94);
});

test("rankOpportunities applies filters and sort order", () => {
  const rows = [
    {
      itemName: "Item A",
      buyMarket: "steam",
      sellMarket: "skinport",
      buyPrice: 40,
      profit: 2,
      spreadPercent: 8,
      opportunityScore: 66,
      isOpportunity: true,
      liquiditySample: 80
    },
    {
      itemName: "Item B",
      buyMarket: "skinport",
      sellMarket: "steam",
      buyPrice: 100,
      profit: 10,
      spreadPercent: 8,
      opportunityScore: 84,
      isOpportunity: true,
      liquiditySample: 240
    },
    {
      itemName: "Item C",
      buyMarket: "csfloat",
      sellMarket: "dmarket",
      buyPrice: 150,
      profit: 4,
      spreadPercent: 6,
      opportunityScore: 74,
      isOpportunity: true,
      liquiditySample: 30
    }
  ];

  const filtered = rankOpportunities(rows, {
    minProfit: 3,
    minSpreadPercent: 5,
    minScore: 70,
    liquidityMin: 50,
    sortBy: "score"
  });
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].itemName, "Item B");
});

test("evaluateItemOpportunity filters fake extreme spread opportunities", () => {
  const result = evaluateItemOpportunity({
    skinId: 501,
    marketHashName: "P250 | Sand Dune",
    perMarket: [
      { source: "steam", available: true, grossPrice: 0.02, netPriceAfterFees: 0.0174 },
      { source: "dmarket", available: true, grossPrice: 1.08, netPriceAfterFees: 1.0 }
    ]
  });

  assert.equal(result.isOpportunity, false);
  assert.ok(Array.isArray(result?.antiFake?.reasons));
  assert.ok(result.antiFake.reasons.includes("ignored_extreme_spread"));
});

test("evaluateItemOpportunity rejects low-liquidity setups", () => {
  const result = evaluateItemOpportunity({
    skinId: 777,
    marketHashName: "Dual Berettas | Colony",
    perMarket: [
      {
        source: "steam",
        available: true,
        grossPrice: 0.55,
        netPriceAfterFees: 0.4785,
        volume7d: 20
      },
      {
        source: "skinport",
        available: true,
        grossPrice: 0.61,
        netPriceAfterFees: 0.73,
        volume7d: 20
      }
    ]
  });

  assert.equal(result.isOpportunity, false);
  assert.ok(result?.antiFake?.reasons?.includes("ignored_low_liquidity"));
});

test("evaluateItemOpportunity replaces outlier buy_top1 with buy_top2", () => {
  const result = evaluateItemOpportunity({
    skinId: 901,
    marketHashName: "Five-SeveN | Orange Peel",
    liquiditySales: 260,
    perMarket: [
      {
        source: "steam",
        available: true,
        grossPrice: 4.1,
        netPriceAfterFees: 3.57,
        volume7d: 260,
        orderbook: {
          buy_top1: 4.1,
          buy_top2: 12.4
        }
      },
      {
        source: "dmarket",
        available: true,
        grossPrice: 14.3,
        netPriceAfterFees: 13.7,
        volume7d: 260
      }
    ]
  });

  assert.equal(result.isOpportunity, true);
  assert.equal(result.buyMarket, "steam");
  assert.equal(result.buyPrice, 12.4);
  assert.equal(result?.antiFake?.outlier?.buyAdjusted, true);
  assert.ok(Array.isArray(result.depthFlags));
  assert.ok(result.depthFlags.includes("BUY_OUTLIER_ADJUSTED"));
});

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
      { source: "csfloat", available: true, grossPrice: 119, netPriceAfterFees: 116.2, url: "https://csfloat" }
    ]
  });

  assert.equal(result.itemId, 1001);
  assert.equal(result.buyMarket, "skinport");
  assert.equal(result.sellMarket, "csfloat");
  assert.equal(result.buyPrice, 112);
  assert.equal(result.sellNet, 116.2);
  assert.equal(result.profit, 4.2);
  assert.equal(result.isOpportunity, true);
  assert.ok(Number(result.spreadPercent) > 3);
  assert.ok(Number(result.opportunityScore) >= 0);
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
  assert.equal(result.spreadPercent <= 3, true);
  assert.equal(result.isOpportunity, false);
});

test("score helper buckets follow expected thresholds", () => {
  assert.equal(getSpreadScore(16), 100);
  assert.equal(getSpreadScore(12), 80);
  assert.equal(getSpreadScore(7), 60);
  assert.equal(getSpreadScore(2.9), 40);

  assert.equal(getLiquidityScore(250), 100);
  assert.equal(getLiquidityScore(180), 80);
  assert.equal(getLiquidityScore(70), 60);
  assert.equal(getLiquidityScore(20), 30);

  assert.equal(getStabilityScore(2), 100);
  assert.equal(getStabilityScore(8), 80);
  assert.equal(getStabilityScore(15), 50);
  assert.equal(getStabilityScore(31), 20);

  assert.equal(getMarketScore("steam", "skinport"), 95);
});

test("rankOpportunities applies filters and sort order", () => {
  const rows = [
    {
      itemName: "Item A",
      buyMarket: "steam",
      sellMarket: "skinport",
      profit: 2,
      spreadPercent: 4,
      opportunityScore: 66,
      isOpportunity: true,
      liquiditySample: 80
    },
    {
      itemName: "Item B",
      buyMarket: "skinport",
      sellMarket: "steam",
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
    sortBy: "profit"
  });
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].itemName, "Item B");
});

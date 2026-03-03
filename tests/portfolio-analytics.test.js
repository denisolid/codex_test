const test = require("node:test");
const assert = require("node:assert/strict");

const { buildPortfolioAnalytics } = require("../src/utils/portfolioAnalytics");

test("portfolio analytics computes concentration, breadth, and leaders", () => {
  const items = [
    {
      skinId: 101,
      primarySteamItemId: "90001",
      marketHashName: "Item A",
      currentPrice: 70,
      lineValue: 700,
      sevenDayChangePercent: 25
    },
    {
      skinId: 202,
      primarySteamItemId: "90002",
      marketHashName: "Item B",
      currentPrice: 20,
      lineValue: 200,
      sevenDayChangePercent: -12
    },
    {
      skinId: 303,
      primarySteamItemId: "90003",
      marketHashName: "Item C",
      currentPrice: 10,
      lineValue: 100,
      sevenDayChangePercent: 2
    }
  ];

  const analytics = buildPortfolioAnalytics(items, 1000);
  assert.equal(analytics.holdingsCount, 3);
  assert.equal(analytics.concentrationTop1Percent, 70);
  assert.equal(analytics.concentrationTop3Percent, 100);
  assert.equal(analytics.concentrationRisk, "high");
  assert.equal(analytics.breadth.advancers, 2);
  assert.equal(analytics.breadth.decliners, 1);
  assert.equal(analytics.breadth.unchanged, 0);
  assert.equal(analytics.breadth.advancerRatioPercent, 66.67);
  assert.equal(analytics.leaders.topGainer.marketHashName, "Item A");
  assert.equal(analytics.leaders.topGainer.skinId, 101);
  assert.equal(analytics.leaders.topGainer.currentPrice, 70);
  assert.equal(analytics.leaders.topLoser.marketHashName, "Item B");
  assert.equal(analytics.leaders.topLoser.skinId, 202);
  assert.equal(analytics.leaders.topLoser.currentPrice, 20);
});

test("portfolio analytics returns safe defaults for empty data", () => {
  const analytics = buildPortfolioAnalytics([], 0);
  assert.equal(analytics.holdingsCount, 0);
  assert.equal(analytics.concentrationTop1Percent, null);
  assert.equal(analytics.leaders.topGainer, null);
  assert.equal(analytics.breadth.advancerRatioPercent, null);
});

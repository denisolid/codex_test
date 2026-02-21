const test = require("node:test");
const assert = require("node:assert/strict");

const { buildPortfolioAnalytics } = require("../src/utils/portfolioAnalytics");

test("portfolio analytics computes concentration, breadth, and leaders", () => {
  const items = [
    {
      marketHashName: "Item A",
      lineValue: 700,
      sevenDayChangePercent: 25
    },
    {
      marketHashName: "Item B",
      lineValue: 200,
      sevenDayChangePercent: -12
    },
    {
      marketHashName: "Item C",
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
  assert.equal(analytics.leaders.topLoser.marketHashName, "Item B");
});

test("portfolio analytics returns safe defaults for empty data", () => {
  const analytics = buildPortfolioAnalytics([], 0);
  assert.equal(analytics.holdingsCount, 0);
  assert.equal(analytics.concentrationTop1Percent, null);
  assert.equal(analytics.leaders.topGainer, null);
  assert.equal(analytics.breadth.advancerRatioPercent, null);
});

const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: {
    normalizePricingMode,
    pickBestBuy,
    pickBestSellNet,
    selectByPricingMode,
    getModeUnitPrice
  }
} = require("../src/services/marketComparisonService");

test("normalizePricingMode keeps known values and falls back safely", () => {
  assert.equal(normalizePricingMode("steam"), "steam");
  assert.equal(normalizePricingMode("best_sell_net"), "best_sell_net");
  assert.equal(normalizePricingMode("LOWEST_BUY"), "lowest_buy");
  assert.equal(normalizePricingMode("unknown"), "lowest_buy");
});

test("best buy and best sell net pick expected sources", () => {
  const perMarket = [
    {
      source: "steam",
      available: true,
      grossPrice: 13,
      netPriceAfterFees: 11.31
    },
    {
      source: "skinport",
      available: true,
      grossPrice: 12.4,
      netPriceAfterFees: 10.91
    },
    {
      source: "csfloat",
      available: true,
      grossPrice: 12.8,
      netPriceAfterFees: 12.54
    }
  ];

  assert.equal(pickBestBuy(perMarket).source, "skinport");
  assert.equal(pickBestSellNet(perMarket).source, "csfloat");
});

test("mode selection chooses expected market row and unit value", () => {
  const steam = { source: "steam", grossPrice: 20, netPriceAfterFees: 17.4 };
  const bestBuy = { source: "skinport", grossPrice: 18, netPriceAfterFees: 15.84 };
  const bestSellNet = { source: "csfloat", grossPrice: 19, netPriceAfterFees: 18.62 };

  const stream = { steam, bestBuy, bestSellNet };

  const steamSelected = selectByPricingMode("steam", stream);
  const lowSelected = selectByPricingMode("lowest_buy", stream);
  const netSelected = selectByPricingMode("best_sell_net", stream);

  assert.equal(steamSelected.source, "steam");
  assert.equal(lowSelected.source, "skinport");
  assert.equal(netSelected.source, "csfloat");

  assert.equal(getModeUnitPrice("steam", steamSelected), 20);
  assert.equal(getModeUnitPrice("lowest_buy", lowSelected), 18);
  assert.equal(getModeUnitPrice("best_sell_net", netSelected), 18.62);
});

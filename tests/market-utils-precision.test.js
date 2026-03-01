const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildMarketPriceRecord,
  computeNetPrice
} = require("../src/markets/marketUtils");

test("computeNetPrice preserves precision for low-priced items", () => {
  const net = computeNetPrice(0.03, 13);
  assert.equal(net, 0.0261);
});

test("buildMarketPriceRecord keeps sub-cent precision for compare", () => {
  const row = buildMarketPriceRecord({
    source: "steam",
    marketHashName: "Fracture Case",
    grossPrice: 0.03,
    currency: "USD"
  });

  assert.equal(row.grossPrice, 0.03);
  assert.equal(row.netPriceAfterFees, 0.0261);
});

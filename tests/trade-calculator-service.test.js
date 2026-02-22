const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const tradeCalculatorService = require("../src/services/tradeCalculatorService");

test("trade calculator returns net profit, ROI, and break-even", () => {
  const result = tradeCalculatorService.calculateTrade({
    buyPrice: 100,
    sellPrice: 140,
    quantity: 2,
    commissionPercent: 13
  });

  assert.equal(result.grossBuy, 200);
  assert.equal(result.grossSell, 280);
  assert.equal(result.commissionAmount, 36.4);
  assert.equal(result.netSell, 243.6);
  assert.equal(result.netProfit, 43.6);
  assert.equal(result.roiPercent, 21.8);
  assert.equal(result.breakEvenSellPrice, 114.94);
});

test("trade calculator rejects invalid commission", () => {
  assert.throws(
    () =>
      tradeCalculatorService.calculateTrade({
        buyPrice: 10,
        sellPrice: 11,
        commissionPercent: 100
      }),
    /commissionPercent/
  );
});

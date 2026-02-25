const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { computeLiquidityScore, buildQuickSellTiers, deriveStatsFromHistory }
} = require("../src/services/marketService");

test("liquidity score is high for high-volume low-risk items", () => {
  const result = computeLiquidityScore({
    volume_24h: 950,
    volatility_7d_percent: 3,
    spread_percent: 1
  });

  assert.equal(result.band, "high");
  assert.ok(result.score >= 75);
});

test("quick sell tiers are ordered from fast to max profit", () => {
  const tiers = buildQuickSellTiers(
    {
      lowest_listing_price: 120,
      average_7d_price: 130,
      spread_percent: 4
    },
    68,
    13
  );

  assert.equal(tiers[0].tier, "fast_sell");
  assert.equal(tiers[1].tier, "balanced");
  assert.equal(tiers[2].tier, "max_profit");
  assert.ok(tiers[0].listPrice <= tiers[1].listPrice);
  assert.ok(tiers[1].listPrice <= tiers[2].listPrice);
});

test("history derivation uses fallback values when history is empty", () => {
  const stats = deriveStatsFromHistory([], 99);
  assert.equal(stats.average7dPrice, 99);
  assert.equal(stats.volume24h, 0);
});

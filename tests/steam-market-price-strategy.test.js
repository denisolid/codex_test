const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { pickLatestPriceFromOverview }
} = require("../src/services/steamMarketPriceService");

test("balanced strategy prefers median on thin volume", () => {
  const picked = pickLatestPriceFromOverview(
    { lowestPrice: 1.1, medianPrice: 1.4, volume: 1 },
    "balanced"
  );
  assert.equal(picked, 1.4);
});

test("balanced strategy guards against low outlier lowest price", () => {
  const picked = pickLatestPriceFromOverview(
    { lowestPrice: 0.8, medianPrice: 1.2, volume: 50 },
    "balanced"
  );
  assert.equal(picked, 1.2);
});

test("strategy can force lowest or median", () => {
  const overview = { lowestPrice: 1.03, medianPrice: 1.27, volume: 20 };
  assert.equal(pickLatestPriceFromOverview(overview, "lowest"), 1.03);
  assert.equal(pickLatestPriceFromOverview(overview, "median"), 1.27);
});

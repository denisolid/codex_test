const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { parseItemsPayload }
} = require("../src/controllers/marketController");

test("parseItemsPayload preserves optional liquidity and category metadata", () => {
  const [item] = parseItemsPayload([
    {
      skinId: 10,
      marketHashName: "AWP | Neo-Noir (Field-Tested)",
      quantity: 1,
      steamPrice: 41.84,
      steamCurrency: "USD",
      itemCategory: "weapon_skin",
      itemSubcategory: "sniper",
      volume7d: 273,
      marketVolume7d: 273,
      liquiditySales: 273,
      liquidityScore: 88
    }
  ]);

  assert.equal(item.skinId, 10);
  assert.equal(item.marketHashName, "AWP | Neo-Noir (Field-Tested)");
  assert.equal(item.itemCategory, "weapon_skin");
  assert.equal(item.itemSubcategory, "sniper");
  assert.equal(item.volume7d, 273);
  assert.equal(item.marketVolume7d, 273);
  assert.equal(item.liquiditySales, 273);
  assert.equal(item.liquidityScore, 88);
});


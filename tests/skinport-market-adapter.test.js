const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { extractPrice, normalizeItemsPayload, resolveApiCurrency }
} = require("../src/markets/skinport.market");

test("skinport adapter falls back to USD for unsupported API currency", () => {
  assert.equal(resolveApiCurrency("UAH"), "USD");
  assert.equal(resolveApiCurrency("EUR"), "EUR");
});

test("skinport adapter extracts price from sales history payload", () => {
  const payload = [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      currency: "USD",
      market_page: "https://skinport.com/market?item=Redline",
      last_24_hours: {
        min: 35.95,
        median: 39.35
      }
    }
  ];
  const rows = normalizeItemsPayload(payload);

  assert.equal(rows.length, 1);
  assert.equal(rows[0].marketHashName, "AK-47 | Redline (Field-Tested)");
  assert.equal(rows[0].price, 35.95);
  assert.equal(rows[0].currency, "USD");
  assert.equal(rows[0].url, "https://skinport.com/market?item=Redline");
});

test("skinport adapter extractPrice uses history-first candidates", () => {
  const price = extractPrice({
    last_24_hours: {
      min: 0.86
    },
    suggested_price: 2.12
  });
  assert.equal(price, 0.86);
});

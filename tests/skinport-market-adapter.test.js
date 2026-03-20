const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: {
    extractPrice,
    extractLiveExecutableQuote,
    extractHistoricalSummaryQuote,
    normalizeItemsPayload,
    resolveApiCurrency,
    resolvePriceIntegrityStatus
  }
} = require("../src/markets/skinport.market");

test("skinport adapter falls back to USD for unsupported API currency", () => {
  assert.equal(resolveApiCurrency("UAH"), "USD");
  assert.equal(resolveApiCurrency("EUR"), "EUR");
});

test("skinport adapter extracts executable price from live listing fields", () => {
  const payload = [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      currency: "USD",
      item_page: "https://skinport.com/item/ak-47-redline-field-tested",
      listing_id: "sp-123",
      min_price: 34.95,
      current_price: 36.1,
      market_page: "https://skinport.com/market?item=Redline",
      last_24_hours: { min: 32.95, median: 38.35 }
    }
  ];
  const rows = normalizeItemsPayload(payload);

  assert.equal(rows.length, 1);
  assert.equal(rows[0].marketHashName, "AK-47 | Redline (Field-Tested)");
  assert.equal(rows[0].price, 34.95);
  assert.equal(rows[0].selectedPriceField, "min_price");
  assert.equal(rows[0].quoteType, "live_executable");
  assert.equal(rows[0].priceIntegrityStatus, "confirmed");
  assert.equal(rows[0].currency, "USD");
  assert.equal(rows[0].url, "https://skinport.com/item/ak-47-redline-field-tested");
});

test("skinport adapter marks history-only quotes as non-executable", () => {
  const live = extractLiveExecutableQuote({
    last_24_hours: { min: 0.86 },
    suggested_price: 2.12
  });
  const summary = extractHistoricalSummaryQuote({
    last_24_hours: { min: 0.86 },
    suggested_price: 2.12
  });

  assert.equal(live, null);
  assert.equal(summary.quoteType, "historical_summary");
  assert.equal(summary.price, 0.86);
  assert.equal(extractPrice({ last_24_hours: { min: 0.86 } }), null);
});

test("skinport price integrity requires live executable quote with identifier", () => {
  assert.equal(
    resolvePriceIntegrityStatus({
      quoteType: "live_executable",
      marketHashName: "AK-47 | Redline (Field-Tested)",
      currency: "USD",
      itemSlug: "ak-47-redline-field-tested",
      listingId: null
    }),
    "confirmed"
  );
  assert.equal(
    resolvePriceIntegrityStatus({
      quoteType: "historical_summary",
      marketHashName: "AK-47 | Redline (Field-Tested)",
      currency: "USD",
      itemSlug: "ak-47-redline-field-tested",
      listingId: null
    }),
    "unconfirmed"
  );
});

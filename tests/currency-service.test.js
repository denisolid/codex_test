const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const currencyService = require("../src/services/currencyService");

test("currency service converts USD amount into selected currency", () => {
  assert.equal(currencyService.convertUsdAmount(100, "USD"), 100);
  assert.equal(currencyService.convertUsdAmount(100, "EUR"), 92);
  assert.equal(currencyService.convertAmount(92, "EUR", "USD"), 100);
});

test("currency service rejects unsupported currency codes", () => {
  assert.throws(() => currencyService.resolveCurrency("ABC"), /Unsupported currency/);
});

test("currency service exposes supported currencies list", () => {
  const codes = currencyService.getSupportedCurrencies();
  assert.ok(codes.includes("USD"));
  assert.ok(codes.includes("EUR"));
});

test("currency service merges live rates payload with static fallbacks", () => {
  const map = currencyService.__testables.sanitizeLiveRates({
    result: "success",
    rates: {
      EUR: 0.95,
      GBP: 0.81
    }
  });

  assert.equal(map.USD, 1);
  assert.equal(map.EUR, 0.95);
  assert.equal(map.GBP, 0.81);
  assert.ok(Number.isFinite(map.UAH));
});

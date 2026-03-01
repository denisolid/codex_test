const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { extractBestListing }
} = require("../src/markets/csfloat.market");
const {
  __testables: { resolveOfferUrl, extractPrice }
} = require("../src/markets/dmarket.market");
const {
  __testables: { normalizeItemsPayload }
} = require("../src/markets/skinport.market");

test("csfloat listing extractor uses direct listing URL when provided", () => {
  const payload = {
    data: [
      {
        price: "1450",
        currency: "USD",
        url: "https://csfloat.com/item/abc123"
      }
    ]
  };

  const row = extractBestListing(payload, "AK-47 | Redline (Field-Tested)");
  assert.ok(row);
  assert.equal(row.price, 14.5);
  assert.equal(row.url, "https://csfloat.com/item/abc123");
});

test("csfloat listing extractor falls back to id-based listing URL", () => {
  const payload = {
    data: [
      {
        price: "9.81",
        id: "99887766"
      }
    ]
  };

  const row = extractBestListing(payload, "Fracture Case");
  assert.ok(row);
  assert.equal(row.url, "https://csfloat.com/item/99887766");
});

test("dmarket offer URL resolver prefers exact item page and falls back to search", () => {
  const exact = resolveOfferUrl(
    {
      itemPage: "https://dmarket.com/ingame-items/item-list/csgo-skins?someExactOffer=1"
    },
    "AK-47 | Redline (Field-Tested)"
  );
  assert.equal(
    exact,
    "https://dmarket.com/ingame-items/item-list/csgo-skins?someExactOffer=1"
  );

  const fallback = resolveOfferUrl({}, "Fracture Case");
  assert.equal(
    fallback,
    "https://dmarket.com/ingame-items/item-list/csgo-skins?searchTitle=Fracture%20Case"
  );
});

test("dmarket price extractor prefers USD minor-unit price over amount", () => {
  const price = extractPrice({
    amount: "1",
    price: {
      USD: "174"
    }
  });
  assert.equal(price, 1.74);
});

test("dmarket price extractor supports decimal USD strings", () => {
  const price = extractPrice({
    price: {
      USD: "0.93"
    }
  });
  assert.equal(price, 0.93);
});

test("skinport payload normalization prefers item_page over market_page", () => {
  const rows = normalizeItemsPayload([
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      currency: "USD",
      item_page: "https://skinport.com/item/ak-47-redline-field-tested",
      market_page: "https://skinport.com/market?item=Redline",
      last_24_hours: {
        min: 35.95
      }
    }
  ]);

  assert.equal(rows.length, 1);
  assert.equal(rows[0].url, "https://skinport.com/item/ak-47-redline-field-tested");
});

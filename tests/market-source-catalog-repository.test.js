const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { normalizeRows }
} = require("../src/repositories/marketSourceCatalogRepository");

test("normalizeRows defaults market_coverage_count to zero when missing", () => {
  const [row] = normalizeRows([
    {
      marketHashName: "AWP | Neo-Noir (Field-Tested)",
      category: "weapon_skin"
    }
  ]);

  assert.equal(row.market_hash_name, "AWP | Neo-Noir (Field-Tested)");
  assert.equal(row.market_coverage_count, 0);
});

test("normalizeRows preserves explicit non-negative market_coverage_count", () => {
  const [row] = normalizeRows([
    {
      marketHashName: "Chroma 3 Case",
      category: "case",
      marketCoverageCount: 4
    }
  ]);

  assert.equal(row.market_hash_name, "Chroma 3 Case");
  assert.equal(row.market_coverage_count, 4);
});

test("normalizeRows preserves knife and glove categories", () => {
  const [knifeRow, gloveRow] = normalizeRows([
    {
      marketHashName: "★ Karambit | Doppler (Factory New)",
      category: "knife"
    },
    {
      marketHashName: "★ Sport Gloves | Vice (Field-Tested)",
      category: "glove"
    }
  ]);

  assert.equal(knifeRow.category, "knife");
  assert.equal(gloveRow.category, "glove");
});

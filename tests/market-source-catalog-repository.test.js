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
  assert.equal(row.enrichment_priority, 0);
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

test("normalizeRows assigns candidate status defaults and enrichment flags", () => {
  const [eligibleRow, candidateRow] = normalizeRows([
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      scanEligible: true
    },
    {
      marketHashName: "Copenhagen 2024 Legends Sticker Capsule",
      category: "sticker_capsule",
      missingSnapshot: true,
      missingReference: true,
      missingMarketCoverage: true,
      enrichmentPriority: 27.5
    }
  ]);

  assert.equal(eligibleRow.candidate_status, "eligible");
  assert.equal(candidateRow.candidate_status, "candidate");
  assert.equal(candidateRow.missing_snapshot, true);
  assert.equal(candidateRow.missing_reference, true);
  assert.equal(candidateRow.missing_market_coverage, true);
  assert.equal(candidateRow.enrichment_priority, 27.5);
});

const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: { normalizeRows, applyCatalogStatusCompatibility }
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
  assert.equal(row.catalog_status, "shadow");
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
  assert.equal(eligibleRow.catalog_status, "scannable");
  assert.equal(candidateRow.candidate_status, "candidate");
  assert.equal(candidateRow.catalog_status, "shadow");
  assert.equal(candidateRow.missing_snapshot, true);
  assert.equal(candidateRow.missing_reference, true);
  assert.equal(candidateRow.missing_market_coverage, true);
  assert.equal(candidateRow.enrichment_priority, 27.5);
});

test("catalog-status compatibility preserves explicit catalog_status when present", () => {
  const [row] = applyCatalogStatusCompatibility([
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "blocked",
      catalog_block_reason: "invalid_catalog_reason",
      catalog_quality_score: 0
    }
  ]);

  assert.equal(row.catalog_status, "blocked");
  assert.equal(row.catalog_block_reason, "invalid_catalog_reason");
  assert.equal(row.catalog_quality_score, 0);
});

test("catalog-status compatibility derives scannable state when catalog_status column is absent", () => {
  const nowIso = new Date().toISOString();
  const [row] = applyCatalogStatusCompatibility([
    {
      market_hash_name: "M4A1-S | Decimator (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      reference_price: 12.5,
      market_coverage_count: 3,
      liquidity_rank: 42,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }
  ]);

  assert.equal(row.catalog_status, "scannable");
  assert.equal(Boolean(row.last_market_signal_at), true);
  assert.equal(Number(row.catalog_quality_score || 0) > 0, true);
});

test("catalog-status compatibility backfills null catalog_status rows conservatively", () => {
  const oldIso = new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString();
  const [row] = applyCatalogStatusCompatibility([
    {
      market_hash_name: "Fracture Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: null,
      reference_price: 2.2,
      market_coverage_count: 2,
      snapshot_captured_at: oldIso,
      quote_fetched_at: oldIso
    }
  ]);

  assert.equal(row.catalog_status, "shadow");
  assert.equal(row.catalog_block_reason, "stale_only_signals");
});

const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const marketSourceCatalogService = require("../src/services/marketSourceCatalogService")
const catalogPriorityCoverageService = require("../src/services/catalogPriorityCoverageService")
const candidateProgressionService = require("../src/services/scanner/candidateProgressionService")
const { ENRICHMENT_INTERVAL_MS } = require("../src/services/scanner/config")

test("candidate progression due cadence follows state-specific retry windows", () => {
  const { isRowDueForProgression } = candidateProgressionService.__testables
  const nowMs = Date.now()

  assert.equal(
    isRowDueForProgression(
      {
        candidate_status: "near_eligible",
        last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS - 1000).toISOString()
      },
      nowMs
    ),
    true
  )
  assert.equal(
    isRowDueForProgression(
      {
        candidate_status: "eligible",
        last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS).toISOString()
      },
      nowMs
    ),
    false
  )
  assert.equal(
    isRowDueForProgression(
      {
        candidate_status: "candidate",
        last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS * 5).toISOString()
      },
      nowMs
    ),
    false
  )
  assert.equal(
    isRowDueForProgression(
      {
        candidate_status: "candidate",
        last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS * 6 - 1000).toISOString()
      },
      nowMs
    ),
    true
  )
})

test("candidate progression batch exposes backlog, promotions, and cohort metrics", async () => {
  const originals = {
    listDueProgressionRows: marketSourceCatalogRepo.listDueProgressionRows,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    recomputeCandidateReadinessRows: marketSourceCatalogService.recomputeCandidateReadinessRows,
    syncPriorityCoverageSet: catalogPriorityCoverageService.syncPriorityCoverageSet
  }

  const nowMs = Date.now()
  const dueNearEligible = {
    market_hash_name: "Revolution Case",
    category: "case",
    tradable: true,
    is_active: true,
    candidate_status: "near_eligible",
    catalog_status: "scannable",
    scan_eligible: false,
    last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS * 2).toISOString()
  }
  const dueEnriching = {
    market_hash_name: "AK-47 | Slate (Field-Tested)",
    category: "weapon_skin",
    tradable: true,
    is_active: true,
    candidate_status: "enriching",
    catalog_status: "scannable",
    scan_eligible: false,
    last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS * 3).toISOString()
  }

  marketSourceCatalogRepo.listDueProgressionRows = async ({
    candidateStatuses = [],
    dueBeforeIso
  } = {}) => {
    const state = String(candidateStatuses[0] || "")
    assert.equal(Boolean(dueBeforeIso), true)
    if (state === "near_eligible") return [dueNearEligible]
    if (state === "eligible") return []
    if (state === "enriching") return [dueEnriching]
    if (state === "candidate") return []
    return []
  }
  marketSourceCatalogService.recomputeCandidateReadinessRows = async (rows = []) => ({
    promotedToNearEligible: 0,
    promotedToEligible: 1,
    processedMarketHashNames: rows.map((row) => row.market_hash_name)
  })
  marketSourceCatalogRepo.listByMarketHashNames = async () => [
    {
      ...dueNearEligible,
      candidate_status: "eligible",
      scan_eligible: true
    },
    {
      ...dueEnriching,
      candidate_status: "near_eligible",
      scan_eligible: false
    }
  ]
  marketSourceCatalogRepo.listCoverageSummary = async () => [
    {
      market_hash_name: "Revolution Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    },
    {
      market_hash_name: "AK-47 | Slate (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable"
    },
    {
      market_hash_name: "Kilowatt Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "candidate",
      scan_eligible: false,
      catalog_status: "scannable",
      maturity_state: "enriching",
      market_coverage_count: 1,
      volume_7d: 120,
      snapshot_captured_at: new Date().toISOString(),
      snapshot_stale: false,
      quote_fetched_at: new Date().toISOString(),
      liquidity_rank: 44,
      eligibility_reason: "candidate_not_ready"
    }
  ]
  catalogPriorityCoverageService.syncPriorityCoverageSet = async (options = {}) => {
    assert.equal(options.allowCatalogInsert, false)
    return {
      totalPriorityItemsConfigured: 0,
      matchedExistingCatalogItems: 0,
      insertedMissingCatalogItems: 0,
      unmatchedPriorityItems: [],
      entries: [],
      byKey: new Map(),
      policyHintsByTier: {}
    }
  }

  try {
    const result = await candidateProgressionService.runProgressionBatch({
      batchSize: 10,
      nowMs
    })
    assert.equal(result.processedCount, 2)
    assert.equal(result.diagnostics.progression_rows_processed_total, 2)
    assert.equal(result.diagnostics.progression_rows_processed_by_state.near_eligible, 1)
    assert.equal(result.diagnostics.progression_rows_processed_by_state.enriching, 1)
    assert.equal(result.diagnostics.near_eligible_due_backlog, 1)
    assert.equal(result.diagnostics.promoted_to_eligible_total, 1)
    assert.equal(result.diagnostics.eligible_tradable_rows, 1)
    assert.equal(result.diagnostics.hot_cohort_size, 1)
    assert.equal(result.diagnostics.warm_cohort_size, 1)
    assert.equal(result.diagnostics.cold_probe_size, 1)
  } finally {
    marketSourceCatalogRepo.listDueProgressionRows = originals.listDueProgressionRows
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketSourceCatalogService.recomputeCandidateReadinessRows =
      originals.recomputeCandidateReadinessRows
    catalogPriorityCoverageService.syncPriorityCoverageSet = originals.syncPriorityCoverageSet
  }
})

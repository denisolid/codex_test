const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const scanSourceCohortService = require("../src/services/scanner/scanSourceCohortService")
const {
  SCAN_COHORT_PRIMARY_POOL_MULTIPLIER
} = require("../src/services/scanner/config")

test("scan source cohort loader stays on persisted hot/warm/cold path when primary cohorts are healthy", async () => {
  const originals = {
    listHotScanCohort: marketSourceCatalogRepo.listHotScanCohort,
    listWarmScanCohort: marketSourceCatalogRepo.listWarmScanCohort,
    listColdScanCohort: marketSourceCatalogRepo.listColdScanCohort,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  const batchSize = 8
  marketSourceCatalogRepo.listHotScanCohort = async () =>
    Array.from({ length: 8 }, (_, index) => ({
      market_hash_name: `AK-47 | Redline (Field-Tested) #${index + 1}`,
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    }))
  marketSourceCatalogRepo.listWarmScanCohort = async () => [
    ...Array.from({ length: 6 }, (_, index) => ({
      market_hash_name: `Revolution Case #${index + 1}`,
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable"
    })),
    ...Array.from({ length: 6 }, (_, index) => ({
      market_hash_name: `Stockholm 2021 Contenders Sticker Capsule #${index + 1}`,
      category: "sticker_capsule",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable"
    }))
  ]
  marketSourceCatalogRepo.listColdScanCohort = async () => []
  marketSourceCatalogRepo.listCandidatePool = async () => []
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize })
    assert.equal(result.diagnostics.sourceMode, "persisted_cohorts")
    assert.equal(result.diagnostics.fallbackUsed, false)
    assert.deepEqual(result.diagnostics.missingCategoriesAfterPrimary, [])
    assert.equal(
      result.rows.length >= batchSize * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER,
      true
    )
  } finally {
    marketSourceCatalogRepo.listHotScanCohort = originals.listHotScanCohort
    marketSourceCatalogRepo.listWarmScanCohort = originals.listWarmScanCohort
    marketSourceCatalogRepo.listColdScanCohort = originals.listColdScanCohort
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("scan source cohort loader escalates to active tradable fallback only after candidate pool fallback fails", async () => {
  const originals = {
    listHotScanCohort: marketSourceCatalogRepo.listHotScanCohort,
    listWarmScanCohort: marketSourceCatalogRepo.listWarmScanCohort,
    listColdScanCohort: marketSourceCatalogRepo.listColdScanCohort,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  marketSourceCatalogRepo.listHotScanCohort = async () => [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    }
  ]
  marketSourceCatalogRepo.listWarmScanCohort = async () => []
  marketSourceCatalogRepo.listColdScanCohort = async () => []
  marketSourceCatalogRepo.listCandidatePool = async () => {
    throw new Error("candidate_pool_failed")
  }
  marketSourceCatalogRepo.listActiveTradable = async () => [
    {
      market_hash_name: "Recoil Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "candidate",
      scan_eligible: false,
      catalog_status: "scannable"
    }
  ]

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize: 4 })
    const fallbackCase = result.rows.find(
      (row) =>
        row.market_hash_name === "Recoil Case" &&
        String(row.fallbackSource || "").toLowerCase() === "activetradable"
    )
    assert.equal(Boolean(fallbackCase), true)
    assert.equal(result.diagnostics.fallbackUsed, true)
    assert.equal(result.diagnostics.cohortQueryFailures.candidatePool, true)
    assert.equal(Number(result.diagnostics.fallbackRowsLoadedBySource.activeTradable || 0) >= 1, true)
  } finally {
    marketSourceCatalogRepo.listHotScanCohort = originals.listHotScanCohort
    marketSourceCatalogRepo.listWarmScanCohort = originals.listWarmScanCohort
    marketSourceCatalogRepo.listColdScanCohort = originals.listColdScanCohort
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("scan source cohort loader still returns rows when catalog_status is missing or null", async () => {
  const originals = {
    listHotScanCohort: marketSourceCatalogRepo.listHotScanCohort,
    listWarmScanCohort: marketSourceCatalogRepo.listWarmScanCohort,
    listColdScanCohort: marketSourceCatalogRepo.listColdScanCohort,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  const batchSize = 6
  const requiredPrimaryPoolSize = batchSize * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER
  const warmCount = Math.max(requiredPrimaryPoolSize - batchSize, 2)
  const caseWarmCount = Math.max(Math.ceil(warmCount / 2), 1)
  const capsuleWarmCount = Math.max(warmCount - caseWarmCount, 1)
  const nowIso = new Date().toISOString()
  marketSourceCatalogRepo.listHotScanCohort = async () =>
    Array.from({ length: 6 }, (_, index) => ({
      market_hash_name: `AK-47 | Slate (Field-Tested) #${index + 1}`,
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: index % 2 === 0 ? null : undefined,
      reference_price: 8 + index,
      market_coverage_count: 3,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }))
  marketSourceCatalogRepo.listWarmScanCohort = async () => [
    ...Array.from({ length: caseWarmCount }, (_, index) => ({
      market_hash_name: `Revolution Case #${index + 1}`,
      item_name: `Revolution Case #${index + 1}`,
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: null,
      reference_price: 2.6,
      market_coverage_count: 3,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    })),
    ...Array.from({ length: capsuleWarmCount }, (_, index) => ({
      market_hash_name: `Stockholm 2021 Contenders Sticker Capsule #${index + 1}`,
      category: "sticker_capsule",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      reference_price: 3.1,
      market_coverage_count: 3,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }))
  ]
  marketSourceCatalogRepo.listColdScanCohort = async () => []
  marketSourceCatalogRepo.listCandidatePool = async () => []
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize })
    assert.equal(result.rows.length >= batchSize, true)
    assert.equal(result.diagnostics.fallbackUsed, false)
    assert.equal(
      result.rows.every((row) => String(row?.catalog_status || "").toLowerCase() === "scannable"),
      true
    )
  } finally {
    marketSourceCatalogRepo.listHotScanCohort = originals.listHotScanCohort
    marketSourceCatalogRepo.listWarmScanCohort = originals.listWarmScanCohort
    marketSourceCatalogRepo.listColdScanCohort = originals.listColdScanCohort
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

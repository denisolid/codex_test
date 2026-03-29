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
const upstreamMarketFreshnessRecoveryService = require("../src/services/upstreamMarketFreshnessRecoveryService")
const enrichmentRepairService = require("../src/services/scanner/enrichmentRepairService")
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
    upsertRows: marketSourceCatalogRepo.upsertRows,
    recomputeCandidateReadinessRows: marketSourceCatalogService.recomputeCandidateReadinessRows,
    refreshActiveUniverseFromCurrentCatalog:
      marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog,
    syncPriorityCoverageSet: catalogPriorityCoverageService.syncPriorityCoverageSet,
    repairCatalogRows: upstreamMarketFreshnessRecoveryService.repairCatalogRows
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
    processedMarketHashNames: rows.map((row) => row.market_hash_name),
    hard_reject_to_penalty_conversions_by_category: {
      weapon_skin: 1,
      case: 0,
      sticker_capsule: 0
    },
    near_eligible_by_category: {
      weapon_skin: 1,
      case: 0,
      sticker_capsule: 0
    },
    eligible_by_category: {
      weapon_skin: 0,
      case: 1,
      sticker_capsule: 0
    },
    top_reject_reasons_by_category: {
      weapon_skin: {},
      case: {},
      sticker_capsule: {}
    },
    weapon_skin_recovery_paths: {
      penalty: 1,
      fallback: 0,
      near_eligible: 1,
      cooldown: 0,
      eligible: 0,
      rejected: 0,
      penalty_missing_liquidity_weapon_skin: 1,
      penalty_stale_market_weapon_skin: 0,
      contextual_low_value_weapon_skin: 0
    }
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
  marketSourceCatalogRepo.upsertRows = async (rows = []) => rows
  marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog = async () => ({
    targetUniverseSize: 3000,
    universeRowsBeforeRefresh: 401,
    universeRowsAfterRefresh: 351,
    universeRowsDroppedAsStale: 50,
    universeRowsAdded: 0,
    activeUniverseBuilt: 351,
    persisted: {
      skipped: false
    }
  })
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
  upstreamMarketFreshnessRecoveryService.repairCatalogRows = async () => ({
    attemptedRows: 0,
    quoteRowsSelected: 0,
    snapshotRowsSelected: 0,
    quoteRefresh: {},
    snapshotRefresh: {
      blocked: false,
      blockedReason: null,
      rowOutcomes: []
    },
    processedMarketHashNames: []
  })

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
    assert.equal(result.diagnostics.universeRowsBeforeRefresh, 401)
    assert.equal(result.diagnostics.universeRowsAfterRefresh, 351)
    assert.equal(result.diagnostics.universeRowsDroppedAsStale, 50)
    assert.equal(result.diagnostics.universeRowsAdded, 0)
    assert.equal(result.diagnostics.eligible_tradable_rows, 1)
    assert.equal(result.diagnostics.hot_cohort_size, 1)
    assert.equal(result.diagnostics.warm_cohort_size, 1)
    assert.equal(result.diagnostics.cold_probe_size, 1)
    assert.equal(
      result.diagnostics.hard_reject_to_penalty_conversions_by_category.weapon_skin,
      1
    )
    assert.equal(result.diagnostics.near_eligible_by_category.weapon_skin, 1)
    assert.equal(result.diagnostics.eligible_by_category.case, 1)
    assert.equal(result.diagnostics.weapon_skin_recovery_paths.penalty, 1)
  } finally {
    marketSourceCatalogRepo.listDueProgressionRows = originals.listDueProgressionRows
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketSourceCatalogRepo.upsertRows = originals.upsertRows
    marketSourceCatalogService.recomputeCandidateReadinessRows =
      originals.recomputeCandidateReadinessRows
    marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog =
      originals.refreshActiveUniverseFromCurrentCatalog
    catalogPriorityCoverageService.syncPriorityCoverageSet = originals.syncPriorityCoverageSet
    upstreamMarketFreshnessRecoveryService.repairCatalogRows = originals.repairCatalogRows
  }
})

test("repair lane prioritizes partially usable rows over empty rows", () => {
  const nowMs = Date.now()
  const partialRow = {
    market_hash_name: "AK-47 | Slate (Field-Tested)",
    category: "weapon_skin",
    candidate_status: "enriching",
    tradable: true,
    is_active: true,
    market_coverage_count: 1,
    reference_price: 12.3,
    quote_fetched_at: new Date(nowMs - 20 * 60 * 1000).toISOString(),
    snapshot_captured_at: null,
    liquidity_rank: 44,
    enrichment_priority: 28
  }
  const emptyRow = {
    market_hash_name: "USP-S | Blueprint (Field-Tested)",
    category: "weapon_skin",
    candidate_status: "candidate",
    tradable: true,
    is_active: true,
    market_coverage_count: 0,
    reference_price: null,
    quote_fetched_at: null,
    snapshot_captured_at: null,
    liquidity_rank: 0,
    enrichment_priority: 3
  }

  const selection = enrichmentRepairService.selectRepairCandidates([emptyRow, partialRow], {
    limit: 2,
    nowMs
  })

  assert.equal(selection.rows.length, 2)
  assert.equal(selection.rows[0].market_hash_name, partialRow.market_hash_name)
})

test("repair lane cools down repeatedly unrepaired rows and records failure reasons", async () => {
  const originals = {
    listDueProgressionRows: marketSourceCatalogRepo.listDueProgressionRows,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    upsertRows: marketSourceCatalogRepo.upsertRows,
    recomputeCandidateReadinessRows: marketSourceCatalogService.recomputeCandidateReadinessRows,
    refreshActiveUniverseFromCurrentCatalog:
      marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog,
    syncPriorityCoverageSet: catalogPriorityCoverageService.syncPriorityCoverageSet,
    repairCatalogRows: upstreamMarketFreshnessRecoveryService.repairCatalogRows
  }

  const nowMs = Date.now()
  const dueCandidate = {
    market_hash_name: "M4A1-S | Nitrogen (Field-Tested)",
    category: "weapon_skin",
    tradable: true,
    is_active: true,
    candidate_status: "enriching",
    scan_eligible: false,
    catalog_status: "shadow",
    market_coverage_count: 0,
    reference_price: null,
    quote_fetched_at: null,
    snapshot_captured_at: null,
    progression_status: "blocked_from_near_eligible",
    progression_blockers: ["repair_attempts:1"],
    last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS * 3).toISOString()
  }

  marketSourceCatalogRepo.listDueProgressionRows = async ({ candidateStatuses = [] } = {}) => {
    return candidateStatuses[0] === "enriching" ? [dueCandidate] : []
  }
  marketSourceCatalogService.recomputeCandidateReadinessRows = async (rows = []) => ({
    promotedToNearEligible: 0,
    promotedToEligible: 0,
    processedMarketHashNames: rows.map((row) => row.market_hash_name)
  })
  marketSourceCatalogRepo.listByMarketHashNames = async () => [
    {
      ...dueCandidate,
      progression_status: "blocked_from_near_eligible",
      progression_blockers: ["missing_reference", "missing_snapshot"],
      candidate_status: "enriching",
      scan_eligible: false,
      catalog_status: "shadow",
      last_enriched_at: new Date(nowMs).toISOString()
    }
  ]
  marketSourceCatalogRepo.listCoverageSummary = async () => []
  marketSourceCatalogRepo.upsertRows = async (rows = []) => rows
  marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog = async () => ({
    targetUniverseSize: 3000,
    universeRowsBeforeRefresh: 1,
    universeRowsAfterRefresh: 0,
    universeRowsDroppedAsStale: 1,
    universeRowsAdded: 0,
    activeUniverseBuilt: 0,
    persisted: {
      skipped: false
    }
  })
  catalogPriorityCoverageService.syncPriorityCoverageSet = async () => ({
    totalPriorityItemsConfigured: 0,
    matchedExistingCatalogItems: 0,
    insertedMissingCatalogItems: 0,
    unmatchedPriorityItems: [],
    entries: [],
    byKey: new Map(),
    policyHintsByTier: {}
  })
  upstreamMarketFreshnessRecoveryService.repairCatalogRows = async () => ({
    attemptedRows: 1,
    quoteRowsSelected: 1,
    snapshotRowsSelected: 1,
    quoteRefresh: {},
    snapshotRefresh: {
      blocked: false,
      blockedReason: null,
      rowOutcomes: [
        {
          marketHashName: dueCandidate.market_hash_name,
          category: "weapon_skin",
          reason: "snapshot_live_overview_missing",
          refreshed: false
        }
      ]
    },
    processedMarketHashNames: [dueCandidate.market_hash_name]
  })

  try {
    const result = await candidateProgressionService.runProgressionBatch({
      batchSize: 5,
      nowMs
    })

    assert.equal(result.diagnostics.repair_candidates_selected, 1)
    assert.equal(result.diagnostics.repaired_rows, 0)
    assert.equal(result.diagnostics.cooldown_after_failed_repair, 1)
    assert.equal(
      Number(result.diagnostics.top_failed_repair_reasons.still_unusable_market_coverage || 0) >= 1,
      true
    )
  } finally {
    marketSourceCatalogRepo.listDueProgressionRows = originals.listDueProgressionRows
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketSourceCatalogRepo.upsertRows = originals.upsertRows
    marketSourceCatalogService.recomputeCandidateReadinessRows =
      originals.recomputeCandidateReadinessRows
    marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog =
      originals.refreshActiveUniverseFromCurrentCatalog
    catalogPriorityCoverageService.syncPriorityCoverageSet = originals.syncPriorityCoverageSet
    upstreamMarketFreshnessRecoveryService.repairCatalogRows = originals.repairCatalogRows
  }
})

test("repair lane promotes refreshed rows into near_eligible or eligible outcomes", async () => {
  const originals = {
    listDueProgressionRows: marketSourceCatalogRepo.listDueProgressionRows,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    upsertRows: marketSourceCatalogRepo.upsertRows,
    recomputeCandidateReadinessRows: marketSourceCatalogService.recomputeCandidateReadinessRows,
    refreshActiveUniverseFromCurrentCatalog:
      marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog,
    syncPriorityCoverageSet: catalogPriorityCoverageService.syncPriorityCoverageSet,
    repairCatalogRows: upstreamMarketFreshnessRecoveryService.repairCatalogRows
  }

  const nowMs = Date.now()
  const dueCandidate = {
    market_hash_name: "AK-47 | Frontside Misty (Field-Tested)",
    category: "weapon_skin",
    tradable: true,
    is_active: true,
    candidate_status: "near_eligible",
    scan_eligible: false,
    catalog_status: "shadow",
    market_coverage_count: 0,
    reference_price: null,
    quote_fetched_at: null,
    snapshot_captured_at: null,
    last_enriched_at: new Date(nowMs - ENRICHMENT_INTERVAL_MS * 2).toISOString()
  }

  marketSourceCatalogRepo.listDueProgressionRows = async ({ candidateStatuses = [] } = {}) => {
    return candidateStatuses[0] === "near_eligible" ? [dueCandidate] : []
  }
  marketSourceCatalogService.recomputeCandidateReadinessRows = async (rows = []) => ({
    promotedToNearEligible: 0,
    promotedToEligible: 1,
    processedMarketHashNames: rows.map((row) => row.market_hash_name)
  })
  marketSourceCatalogRepo.listByMarketHashNames = async () => [
    {
      ...dueCandidate,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable",
      market_coverage_count: 3,
      reference_price: 24.4,
      quote_fetched_at: new Date(nowMs).toISOString(),
      snapshot_captured_at: new Date(nowMs).toISOString(),
      progression_status: "eligible",
      progression_blockers: []
    }
  ]
  marketSourceCatalogRepo.listCoverageSummary = async () => [
    {
      ...dueCandidate,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    }
  ]
  marketSourceCatalogRepo.upsertRows = async (rows = []) => rows
  marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog = async () => ({
    targetUniverseSize: 3000,
    universeRowsBeforeRefresh: 0,
    universeRowsAfterRefresh: 1,
    universeRowsDroppedAsStale: 0,
    universeRowsAdded: 1,
    activeUniverseBuilt: 1,
    persisted: {
      skipped: false
    }
  })
  catalogPriorityCoverageService.syncPriorityCoverageSet = async () => ({
    totalPriorityItemsConfigured: 0,
    matchedExistingCatalogItems: 0,
    insertedMissingCatalogItems: 0,
    unmatchedPriorityItems: [],
    entries: [],
    byKey: new Map(),
    policyHintsByTier: {}
  })
  upstreamMarketFreshnessRecoveryService.repairCatalogRows = async () => ({
    attemptedRows: 1,
    quoteRowsSelected: 1,
    snapshotRowsSelected: 1,
    quoteRefresh: {},
    snapshotRefresh: {
      blocked: false,
      blockedReason: null,
      rowOutcomes: [
        {
          marketHashName: dueCandidate.market_hash_name,
          category: "weapon_skin",
          reason: "snapshot_write_succeeded",
          refreshed: true
        }
      ]
    },
    processedMarketHashNames: [dueCandidate.market_hash_name]
  })

  try {
    const result = await candidateProgressionService.runProgressionBatch({
      batchSize: 5,
      nowMs
    })

    assert.equal(result.diagnostics.repair_candidates_selected, 1)
    assert.equal(result.diagnostics.repaired_rows, 1)
    assert.equal(result.diagnostics.repaired_to_eligible, 1)
    assert.equal(result.diagnostics.cooldown_after_failed_repair, 0)
  } finally {
    marketSourceCatalogRepo.listDueProgressionRows = originals.listDueProgressionRows
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketSourceCatalogRepo.upsertRows = originals.upsertRows
    marketSourceCatalogService.recomputeCandidateReadinessRows =
      originals.recomputeCandidateReadinessRows
    marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog =
      originals.refreshActiveUniverseFromCurrentCatalog
    catalogPriorityCoverageService.syncPriorityCoverageSet = originals.syncPriorityCoverageSet
    upstreamMarketFreshnessRecoveryService.repairCatalogRows = originals.repairCatalogRows
  }
})

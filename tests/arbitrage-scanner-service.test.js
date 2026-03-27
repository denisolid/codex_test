const test = require("node:test")
const assert = require("node:assert/strict")
const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")
const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const scannerRunRepo = require("../src/repositories/scannerRunRepository")
const globalOpportunityLifecycleLogRepo = require("../src/repositories/globalOpportunityLifecycleLogRepository")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const env = require("../src/config/env")
const globalFeedPublisher = require("../src/services/feed/globalFeedPublisher")
const marketComparisonService = require("../src/services/marketComparisonService")
const scanSourceCohortService = require("../src/services/scanner/scanSourceCohortService")
const {
  evaluatePublishValidation,
  buildPublishValidationPreview,
  resolvePublishValidationContextForOpportunity
} = require("../src/services/scanner/publishValidation")
const {
  OPPORTUNITY_BATCH_RUNTIME_TARGET,
  SCAN_COHORT_PRIMARY_POOL_MULTIPLIER
} = require("../src/services/scanner/config")

const {
  __testables: {
    normalizeCategoryFilter,
    classifyCatalogState,
    compareCandidates,
    buildRoundRobinPool,
    buildEnrichmentJobExecutionDiagnostics,
    buildOpportunityJobExecutionDiagnostics,
    selectScanCandidates,
    evaluateCandidateOpportunity,
    summarizeEvaluations,
    buildOpportunityFingerprint,
    buildMaterialChangeHash,
    classifyOpportunityFeedEvent,
    isMateriallyNewOpportunity,
    buildFeedInsertRow,
    mapFeedRowToApiRow,
    mapFeedRowToCard,
    dedupeFeedCards,
    countScannableRowsByScannerCategory,
    resolveScannerFamilyKey,
    rebalanceSelectionForFeedDiversity,
    loadScannerSourceRows,
    persistFeedRows,
    normalizeCursorPayload,
    encodeCursorPayload,
    buildFeedPageCacheKey,
    clearFeedFirstPageCache,
    isScannerRunOverdue,
    runJobWithLock,
    DEFAULT_UNIVERSE_LIMIT,
    OPPORTUNITY_BATCH_TARGET,
    SCAN_CHUNK_SIZE,
    SCAN_TIMEOUT_PER_BATCH_MS,
    SCAN_STATE
  }
} = require("../src/services/arbitrageScannerService")

function buildCatalogRow(index, category = "weapon_skin") {
  return {
    market_hash_name: `${category}-item-${index}`,
    item_name: `${category.toUpperCase()} Item ${index}`,
    category,
    tradable: true,
    is_active: true,
    candidate_status: "eligible",
    scan_eligible: true,
    catalog_status: "scannable",
    scanCohort: "hot",
    reference_price: 12 + index,
    market_coverage_count: 2,
    volume_7d: 120 + index,
    snapshot_stale: false,
    snapshot_captured_at: new Date().toISOString(),
    quote_fetched_at: new Date().toISOString()
  }
}

function buildEmitComparedItem(opportunity = {}, overrides = {}) {
  const metadata = opportunity?.metadata || {}
  const buyMarket = overrides.buyMarket || opportunity.buyMarket || "steam"
  const sellMarket = overrides.sellMarket || opportunity.sellMarket || "skinport"
  const buyRouteUpdatedAt =
    overrides.buyRouteUpdatedAt ??
    opportunity.buyRouteUpdatedAt ??
    metadata.buy_route_updated_at ??
    null
  const sellRouteUpdatedAt =
    overrides.sellRouteUpdatedAt ??
    opportunity.sellRouteUpdatedAt ??
    metadata.sell_route_updated_at ??
    null
  const buyRouteAvailable =
    overrides.buyRouteAvailable ??
    opportunity.buyRouteAvailable ??
    metadata.buy_route_available ??
    true
  const sellRouteAvailable =
    overrides.sellRouteAvailable ??
    opportunity.sellRouteAvailable ??
    metadata.sell_route_available ??
    true
  const sellListingAvailable =
    overrides.sellListingAvailable ??
    opportunity.sellListingAvailable ??
    metadata.sell_listing_available ??
    (sellMarket === "skinport" ? true : null)

  return {
    marketHashName: opportunity.marketHashName || opportunity.itemName,
    itemCategory: opportunity.itemCategory || opportunity.category || "weapon_skin",
    referencePrice: opportunity.referencePrice || 10,
    volume7d: opportunity.liquidity || 100,
    perMarket: [
      {
        source: buyMarket,
        available: Boolean(buyRouteAvailable),
        grossPrice: overrides.buyGrossPrice ?? opportunity.buyPrice ?? 10,
        updatedAt: buyRouteUpdatedAt,
        orderbook:
          overrides.buyOrderbook || {
            buy_top1: overrides.buyGrossPrice ?? opportunity.buyPrice ?? 10,
            buy_top2: (overrides.buyGrossPrice ?? opportunity.buyPrice ?? 10) * 1.01
          },
        raw: {
          listing_available: null
        }
      },
      {
        source: sellMarket,
        available: Boolean(sellRouteAvailable),
        grossPrice: overrides.sellGrossPrice ?? opportunity.sellNet ?? 12,
        netPriceAfterFees: overrides.sellNetPrice ?? opportunity.sellNet ?? 12,
        updatedAt: sellRouteUpdatedAt,
        orderbook:
          overrides.sellOrderbook || {
            sell_top1: overrides.sellGrossPrice ?? opportunity.sellNet ?? 12,
            sell_top2: (overrides.sellGrossPrice ?? opportunity.sellNet ?? 12) * 0.99
          },
        raw: {
          listing_available: sellListingAvailable,
          listing_id:
            overrides.sellListingId ??
            metadata.skinport_listing_id ??
            (sellListingAvailable === false ? null : "sp-emit-test")
        }
      }
    ]
  }
}

test("category filter normalization keeps API aliases stable", () => {
  assert.equal(normalizeCategoryFilter("all"), "all")
  assert.equal(normalizeCategoryFilter("skins"), "weapon_skin")
  assert.equal(normalizeCategoryFilter("cases"), "case")
  assert.equal(normalizeCategoryFilter("capsules"), "sticker_capsule")
  assert.equal(normalizeCategoryFilter("knives"), "knife")
  assert.equal(normalizeCategoryFilter("gloves"), "glove")
  assert.equal(normalizeCategoryFilter("future_knife"), "knife")
  assert.equal(normalizeCategoryFilter("future_glove"), "glove")
})

test("feed cursor helpers round-trip created_at + id payload", () => {
  clearFeedFirstPageCache()
  const cursor = encodeCursorPayload(
    "2026-03-21T10:15:30.000Z",
    "00000000-0000-0000-0000-000000000777"
  )
  assert.equal(Boolean(cursor), true)

  const decoded = normalizeCursorPayload(cursor)
  assert.equal(Boolean(decoded), true)
  assert.equal(decoded.createdAt, "2026-03-21T10:15:30.000Z")
  assert.equal(decoded.id, "00000000-0000-0000-0000-000000000777")
  assert.equal(normalizeCursorPayload("not-a-valid-cursor"), null)
})

test("feed cache key normalization is stable across category aliases", () => {
  const a = buildFeedPageCacheKey({
    includeInactive: false,
    highConfidenceOnly: false,
    category: "skins"
  })
  const b = buildFeedPageCacheKey({
    includeInactive: false,
    highConfidenceOnly: false,
    category: "weapon_skin"
  })
  assert.equal(a, b)
})

test("scan-first state model forwards rows with penalties instead of hard-blocking", () => {
  const result = classifyCatalogState({
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    tradable: true,
    isActive: true,
    referencePrice: 11,
    marketCoverageCount: 1,
    volume7d: null,
    snapshotCapturedAt: null,
    quoteFetchedAt: null
  })

  assert.equal(result.state, SCAN_STATE.SCANABLE_WITH_PENALTIES)
  assert.equal(result.hardRejectReasons.length, 0)
  assert.equal(result.penaltyFlags.includes("missing_liquidity"), true)
  assert.equal(result.penaltyFlags.includes("weak_coverage"), true)
})

test("state model accepts sell-side volume aliases as liquidity evidence", () => {
  const result = classifyCatalogState({
    marketHashName: "M4A1-S | Decimator (Field-Tested)",
    itemName: "M4A1-S | Decimator (Field-Tested)",
    category: "weapon_skin",
    tradable: true,
    isActive: true,
    referencePrice: 12.4,
    marketCoverageCount: 2,
    volume7d: 0,
    sell_volume_7d: 46,
    quoteFetchedAt: new Date().toISOString()
  })

  assert.equal(result.state, SCAN_STATE.SCANABLE)
  assert.equal(result.penaltyFlags.includes("missing_liquidity"), false)
})

test("state model still keeps true hard rejects narrow and explicit", () => {
  const result = classifyCatalogState({
    marketHashName: "",
    itemName: "",
    category: "weapon_skin",
    tradable: true,
    isActive: true,
    referencePrice: null,
    marketCoverageCount: 0,
    snapshotCapturedAt: null,
    quoteFetchedAt: null
  })

  assert.equal(result.state, SCAN_STATE.HARD_REJECT)
  assert.equal(result.hardRejectReasons.includes("invalid_row"), true)
})

test("state model hard-rejects rows below $2 cost floor", () => {
  const result = classifyCatalogState({
    marketHashName: "Fracture Case",
    itemName: "Fracture Case",
    category: "case",
    tradable: true,
    isActive: true,
    referencePrice: 1.75,
    marketCoverageCount: 3,
    volume7d: 500,
    snapshotCapturedAt: new Date().toISOString(),
    quoteFetchedAt: new Date().toISOString()
  })

  assert.equal(result.state, SCAN_STATE.HARD_REJECT)
  assert.equal(result.hardRejectReasons.includes("below_min_cost_floor"), true)
})

test("round-robin pool keeps category-aware distribution", () => {
  const rows = [
    buildCatalogRow(1, "weapon_skin"),
    buildCatalogRow(2, "weapon_skin"),
    buildCatalogRow(1, "case"),
    buildCatalogRow(1, "sticker_capsule")
  ]
  const pool = buildRoundRobinPool(rows, { lastScannedAtByName: new Map() })

  assert.equal(pool.length, 4)
  assert.equal(pool[0].category, "weapon_skin")
  assert.equal(pool[1].category, "case")
  assert.equal(pool[2].category, "sticker_capsule")
})

test("candidate selection fills configured batch size without using last-scanned rotation", () => {
  const catalogRows = []
  for (let index = 0; index < 30; index += 1) {
    catalogRows.push(buildCatalogRow(index + 1, index % 3 === 0 ? "case" : "weapon_skin"))
  }
  const tracker = new Map()
  const first = selectScanCandidates({
    catalogRows,
    batchSize: 12,
    cursor: 0,
    lastScannedAtByName: tracker
  })
  const second = selectScanCandidates({
    catalogRows,
    batchSize: 12,
    cursor: first.nextCursor,
    lastScannedAtByName: tracker
  })

  assert.equal(first.selected.length, 12)
  assert.equal(first.attemptedBatchSize, 12)
  assert.equal(second.selected.length, 12)
  assert.equal(first.selected[0].marketHashName, second.selected[0].marketHashName)
  assert.equal(Number(first.diagnostics.scanable + first.diagnostics.scanableWithPenalties) > 0, true)
})

test("candidate selection requires explicit cohort metadata and does not infer normal provenance", () => {
  const withoutExplicitCohort = buildCatalogRow(1, "weapon_skin")
  delete withoutExplicitCohort.scanCohort

  const withExplicitCohort = {
    ...buildCatalogRow(2, "case"),
    scanCohort: "hot"
  }

  const selection = selectScanCandidates({
    catalogRows: [withoutExplicitCohort, withExplicitCohort],
    batchSize: 2,
    cursor: 0,
    lastScannedAtByName: new Map()
  })

  assert.equal(selection.selected.length, 1)
  assert.equal(selection.selected[0].marketHashName, withExplicitCohort.market_hash_name)
  assert.equal(selection.diagnostics.poolByCohort.hot, 1)
})

test("candidate selection consumes warm before cold and caps cold probe rows", () => {
  const nowIso = new Date().toISOString()
  const catalogRows = [
    {
      ...buildCatalogRow(1, "weapon_skin"),
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      scanCohort: "hot"
    },
    {
      ...buildCatalogRow(2, "case"),
      market_hash_name: "Revolution Case",
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm"
    },
    {
      ...buildCatalogRow(3, "sticker_capsule"),
      market_hash_name: "Stockholm 2021 Contenders Sticker Capsule",
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm"
    },
    {
      ...buildCatalogRow(4, "weapon_skin"),
      market_hash_name: "M4A4 | Neo-Noir (Field-Tested)",
      candidate_status: "candidate",
      scan_eligible: false,
      scanCohort: "cold",
      quote_fetched_at: nowIso,
      market_coverage_count: 1
    },
    {
      ...buildCatalogRow(5, "case"),
      market_hash_name: "Kilowatt Case",
      candidate_status: "candidate",
      scan_eligible: false,
      scanCohort: "cold",
      quote_fetched_at: nowIso,
      market_coverage_count: 1
    }
  ]

  const selection = selectScanCandidates({
    catalogRows,
    batchSize: 4,
    cursor: 0,
    lastScannedAtByName: new Map()
  })

  assert.equal(selection.selected.length, 4)
  assert.equal(selection.diagnostics.selectedByCohort.hot, 1)
  assert.equal(selection.diagnostics.selectedByCohort.warm, 2)
  assert.equal(selection.diagnostics.selectedByCohort.cold, 1)
})

test("candidate selection prioritizes tier_a then tier_b then non-priority", () => {
  const nowIso = new Date().toISOString()
  const catalogRows = [
    {
      ...buildCatalogRow(1, "weapon_skin"),
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      priority_tier: "tier_b",
      priority_boost: 220
    },
    {
      ...buildCatalogRow(2, "weapon_skin"),
      market_hash_name: "AWP | Dragon Lore (Field-Tested)",
      priority_tier: "tier_a",
      priority_boost: 380
    },
    {
      ...buildCatalogRow(3, "weapon_skin"),
      market_hash_name: "M4A4 | The Emperor (Field-Tested)",
      priority_tier: null,
      priority_boost: 0
    }
  ].map((row) => ({
    ...row,
    snapshot_captured_at: nowIso,
    quote_fetched_at: nowIso
  }))

  const selection = selectScanCandidates({
    catalogRows,
    batchSize: 3,
    cursor: 0,
    lastScannedAtByName: new Map()
  })

  assert.equal(selection.selected.length, 3)
  assert.equal(selection.selected[0].priorityTier, "tier_a")
  assert.equal(selection.selected[1].priorityTier, "tier_b")
  assert.equal(selection.selected[2].priorityTier, null)
  assert.equal(Number(selection.diagnostics.selectedByPriorityTier.tier_a || 0), 1)
  assert.equal(Number(selection.diagnostics.selectedByPriorityTier.tier_b || 0), 1)
  assert.equal(Number(selection.diagnostics.selectedByPriorityTier.non_priority || 0), 1)
})

test("candidate selection ordering uses approved persisted fields and keeps penalty rows eligible", () => {
  const oldIso = "2026-03-20T10:00:00.000Z"
  const freshIso = "2026-03-21T10:00:00.000Z"
  const selection = selectScanCandidates({
    catalogRows: [
      {
        ...buildCatalogRow(1, "weapon_skin"),
        market_hash_name: "AK-47 | Redline (Field-Tested)",
        item_name: "AK-47 | Redline (Field-Tested)",
        scanCohort: "hot",
        priority_tier: "tier_a",
        priority_boost: 300,
        last_market_signal_at: oldIso,
        market_coverage_count: 1,
        volume_7d: 0,
        snapshot_captured_at: freshIso,
        quote_fetched_at: freshIso
      },
      {
        ...buildCatalogRow(2, "weapon_skin"),
        market_hash_name: "M4A4 | The Emperor (Field-Tested)",
        item_name: "M4A4 | The Emperor (Field-Tested)",
        scanCohort: "hot",
        priority_tier: null,
        priority_boost: 0,
        last_market_signal_at: freshIso,
        market_coverage_count: 5,
        volume_7d: 400,
        snapshot_captured_at: freshIso,
        quote_fetched_at: freshIso
      }
    ],
    batchSize: 2,
    cursor: 0,
    lastScannedAtByName: new Map()
  })

  assert.equal(selection.selected.length, 2)
  assert.equal(selection.selected[0].marketHashName, "AK-47 | Redline (Field-Tested)")
  assert.equal(selection.selected[0].scanState, SCAN_STATE.SCANABLE_WITH_PENALTIES)
  assert.equal(selection.selected[1].marketHashName, "M4A4 | The Emperor (Field-Tested)")
})

test("candidate selection preserves sell-side volume when generic volume is zero", () => {
  const nowIso = new Date().toISOString()
  const selection = selectScanCandidates({
    catalogRows: [
      {
        ...buildCatalogRow(101, "weapon_skin"),
        market_hash_name: "USP-S | Ticket to Hell (Field-Tested)",
        item_name: "USP-S | Ticket to Hell (Field-Tested)",
        volume_7d: 0,
        sell_volume_7d: 37,
        snapshot_captured_at: nowIso,
        quote_fetched_at: nowIso
      }
    ],
    batchSize: 1,
    cursor: 0,
    lastScannedAtByName: new Map()
  })

  assert.equal(selection.selected.length, 1)
  assert.equal(selection.selected[0].volume7d, 37)
})

test("enrichment job diagnostics stay enrichment-only and expose requested counters", () => {
  const diagnostics = buildEnrichmentJobExecutionDiagnostics({
    forceRefresh: false,
    selectedRows: 6,
    enrichedRows: 6,
    sourceCatalogDiagnostics: {
      progression_rows_processed_total: 6,
      due_backlog_rows_by_state: {
        eligible: 3,
        near_eligible: 4,
        enriching: 1
      },
      eligible_tradable_rows: 11
    }
  })

  assert.deepEqual(diagnostics, {
    job_type: "enrichment",
    selected_rows: 6,
    skipped_rows: 2,
    enriched_rows: 6,
    eligible_rows: 11,
    emitted_rows: 0,
    blocked_rows: 0
  })
})

test("opportunity job diagnostics separate skipped blocked and emitted rows", () => {
  const diagnostics = buildOpportunityJobExecutionDiagnostics({
    selection: {
      selected: [{ marketHashName: "AK-47 | Redline" }, { marketHashName: "AWP | Asiimov" }]
    },
    eligibleRows: 1,
    persisted: {
      activeRowsWritten: 1,
      publishValidation: {
        blocked: 1
      }
    }
  })

  assert.deepEqual(diagnostics, {
    job_type: "opportunity_scan",
    selected_rows: 2,
    skipped_rows: 1,
    enriched_rows: 0,
    eligible_rows: 1,
    emitted_rows: 1,
    blocked_rows: 1
  })
})

test("opportunity comparison keeps live refresh disabled even when refresh is requested", async () => {
  const originalCompareItems = marketComparisonService.compareItems
  let capturedOptions = null

  marketComparisonService.compareItems = async (_rows, options = {}) => {
    capturedOptions = options
    return {
      items: [
        {
          marketHashName: "AK-47 | Redline (Field-Tested)"
        }
      ],
      diagnostics: {
        liveFetch: {
          bySource: {}
        }
      }
    }
  }

  try {
    const result = await compareCandidates([
      {
        marketHashName: "AK-47 | Redline (Field-Tested)",
        category: "weapon_skin",
        referencePrice: 13.25
      }
    ], true)

    assert.equal(Boolean(capturedOptions), true)
    assert.equal(capturedOptions.allowLiveFetch, false)
    assert.equal(capturedOptions.forceRefresh, false)
    assert.equal(result.diagnostics.allowLiveFetch, false)
    assert.equal(result.diagnostics.forceRefresh, false)
  } finally {
    marketComparisonService.compareItems = originalCompareItems
  }
})

test("job lock enforces one active run per job type and records job diagnostics", async () => {
  const originals = {
    tryCreateRunningRun: scannerRunRepo.tryCreateRunningRun,
    markCompleted: scannerRunRepo.markCompleted,
    timeoutStaleRunningRuns: scannerRunRepo.timeoutStaleRunningRuns,
    deleteOlderThan: scannerRunRepo.deleteOlderThan
  }

  const completions = []
  const state = {
    inFlight: null,
    currentRunId: null,
    currentRunStartedAt: null
  }
  let workerRuns = 0

  scannerRunRepo.tryCreateRunningRun = async () => ({
    run: { id: "enrichment-run-1" },
    alreadyRunning: false,
    conflictReason: null,
    existingRun: null
  })
  scannerRunRepo.markCompleted = async (_runId, payload = {}) => {
    completions.push(payload)
    return payload
  }
  scannerRunRepo.timeoutStaleRunningRuns = async () => 0
  scannerRunRepo.deleteOlderThan = async () => 0

  try {
    const started = await runJobWithLock({
      scannerType: "enrichment",
      state,
      timeoutMs: 5000,
      hardTimeoutMs: 5000,
      trigger: "test",
      forceRefresh: false,
      worker: async () => {
        workerRuns += 1
        return {
          selectedCount: 4,
          opportunitiesFound: 0,
          newOpportunitiesAdded: 0,
          diagnostics: {
            job_type: "enrichment",
            selected_rows: 4,
            skipped_rows: 1,
            enriched_rows: 4,
            eligible_rows: 3,
            emitted_rows: 0,
            blocked_rows: 0
          }
        }
      }
    })

    assert.equal(started.status, "started")
    assert.equal(started.job_type, "enrichment")
    await state.inFlight
    assert.equal(workerRuns, 1)
    assert.equal(completions.length, 1)
    assert.equal(completions[0].diagnosticsSummary.job_type, "enrichment")
    assert.equal(completions[0].diagnosticsSummary.selected_rows, 4)

    state.inFlight = Promise.resolve()
    state.currentRunId = "enrichment-run-2"
    state.currentRunStartedAt = new Date().toISOString()
    const blocked = await runJobWithLock({
      scannerType: "enrichment",
      state,
      timeoutMs: 5000,
      hardTimeoutMs: 5000,
      trigger: "test",
      forceRefresh: false,
      worker: async () => {
        throw new Error("worker should not run while enrichment is already active")
      }
    })

    assert.equal(blocked.status, "already_running")
    assert.equal(blocked.alreadyRunning, true)
    assert.equal(blocked.job_type, "enrichment")
  } finally {
    scannerRunRepo.tryCreateRunningRun = originals.tryCreateRunningRun
    scannerRunRepo.markCompleted = originals.markCompleted
    scannerRunRepo.timeoutStaleRunningRuns = originals.timeoutStaleRunningRuns
    scannerRunRepo.deleteOlderThan = originals.deleteOlderThan
  }
})

test("candidate selection reserves one knife and one glove when quality gates pass", () => {
  const nowIso = new Date().toISOString()
  const catalogRows = []
  for (let index = 0; index < 12; index += 1) {
    catalogRows.push({
      ...buildCatalogRow(index + 1, "weapon_skin"),
      reference_price: 40 + index
    })
  }
  catalogRows.push({
    ...buildCatalogRow(999, "knife"),
    market_hash_name: "★ Karambit | Fade (Factory New)",
    item_name: "★ Karambit | Fade (Factory New)",
    reference_price: 1200,
    market_coverage_count: 3,
    volume_7d: 12,
    snapshot_captured_at: nowIso,
    quote_fetched_at: nowIso
  })
  catalogRows.push({
    ...buildCatalogRow(1000, "glove"),
    market_hash_name: "★ Sport Gloves | Vice (Field-Tested)",
    item_name: "★ Sport Gloves | Vice (Field-Tested)",
    reference_price: 1800,
    market_coverage_count: 3,
    volume_7d: 10,
    snapshot_captured_at: nowIso,
    quote_fetched_at: nowIso
  })

  const selection = selectScanCandidates({
    catalogRows,
    batchSize: 6,
    cursor: 0,
    lastScannedAtByName: new Map()
  })
  const selectedCategories = selection.selected.map((row) => row.category)

  assert.equal(selectedCategories.includes("knife"), true)
  assert.equal(selectedCategories.includes("glove"), true)
  assert.equal(Number(selection.diagnostics?.reservedPremiumByCategory?.knife || 0), 1)
  assert.equal(Number(selection.diagnostics?.reservedPremiumByCategory?.glove || 0), 1)
})

test("scannable category counts ignore hard-reject placeholder rows", () => {
  const nowIso = new Date().toISOString()
  const counts = countScannableRowsByScannerCategory([
    {
      market_hash_name: "★ Karambit | Doppler (Factory New)",
      item_name: "★ Karambit | Doppler (Factory New)",
      category: "knife",
      tradable: true,
      is_active: true,
      reference_price: null,
      market_coverage_count: 0,
      snapshot_captured_at: null,
      quote_fetched_at: null
    },
    {
      market_hash_name: "★ Karambit | Fade (Factory New)",
      item_name: "★ Karambit | Fade (Factory New)",
      category: "knife",
      tradable: true,
      is_active: true,
      reference_price: 900,
      market_coverage_count: 3,
      volume_7d: 12,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }
  ])

  assert.equal(Number(counts.knife || 0), 1)
})

test("feed diversity rebalance caps family streaks when alternatives exist", () => {
  const rows = [
    {
      marketHashName: "USP-S | Neo-Noir (Field-Tested)",
      itemName: "USP-S | Neo-Noir (Field-Tested)",
      category: "weapon_skin",
      itemSubcategory: "pistol"
    },
    {
      marketHashName: "Glock-18 | Fade (Factory New)",
      itemName: "Glock-18 | Fade (Factory New)",
      category: "weapon_skin",
      itemSubcategory: "pistol"
    },
    {
      marketHashName: "P250 | See Ya Later (Minimal Wear)",
      itemName: "P250 | See Ya Later (Minimal Wear)",
      category: "weapon_skin",
      itemSubcategory: "pistol"
    },
    {
      marketHashName: "AWP | Asiimov (Field-Tested)",
      itemName: "AWP | Asiimov (Field-Tested)",
      category: "weapon_skin",
      itemSubcategory: "sniper"
    },
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemName: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      itemSubcategory: "rifle"
    },
    {
      marketHashName: "Desert Eagle | Blaze (Factory New)",
      itemName: "Desert Eagle | Blaze (Factory New)",
      category: "weapon_skin",
      itemSubcategory: "pistol"
    }
  ]

  const reordered = rebalanceSelectionForFeedDiversity(rows, { maxConsecutiveFamily: 2 })
  assert.equal(reordered.length, rows.length)

  let previousFamily = ""
  let streak = 0
  let maxStreak = 0
  for (const row of reordered) {
    const family = resolveScannerFamilyKey(row)
    if (family === previousFamily) {
      streak += 1
    } else {
      previousFamily = family
      streak = 1
    }
    maxStreak = Math.max(maxStreak, streak)
  }

  assert.equal(maxStreak <= 2, true)
})

test.skip("legacy scanner source loader backfills missing categories from candidate pool", async () => {
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
      item_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable",
      reference_price: 18,
      market_coverage_count: 3,
      volume_7d: 140,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
    }
  ]
  marketSourceCatalogRepo.listWarmScanCohort = async () => [
    {
      market_hash_name: "Revolution Case",
      item_name: "Revolution Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable",
      reference_price: 2.6,
      market_coverage_count: 3,
      volume_7d: 550,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
    },
    {
      market_hash_name: "Stockholm 2021 Contenders Sticker Capsule",
      item_name: "Stockholm 2021 Contenders Sticker Capsule",
      category: "sticker_capsule",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable",
      reference_price: 3.1,
      market_coverage_count: 3,
      volume_7d: 210,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
    },
    {
      market_hash_name: "★ Karambit | Doppler (Factory New)",
      item_name: "★ Karambit | Doppler (Factory New)",
      category: "knife",
      tradable: true,
      is_active: true,
      reference_price: 1200,
      market_coverage_count: 3,
      volume_7d: 9,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
    },
    {
      market_hash_name: "★ Sport Gloves | Vice (Field-Tested)",
      item_name: "★ Sport Gloves | Vice (Field-Tested)",
      category: "glove",
      tradable: true,
      is_active: true,
      reference_price: 1600,
      market_coverage_count: 3,
      volume_7d: 8,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
    }
  ]
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    const byCategory = {}
    for (const row of loaded.rows || []) {
      byCategory[row.category] = Number(byCategory[row.category] || 0) + 1
    }
    assert.equal(Number(byCategory.weapon_skin || 0) >= 1, true)
    assert.equal(Number(byCategory.case || 0) >= 1, true)
    assert.equal(Number(byCategory.sticker_capsule || 0) >= 1, true)
    assert.equal(Number(byCategory.knife || 0) >= 1, true)
    assert.equal(Number(byCategory.glove || 0) >= 1, true)
    assert.equal(loaded?.diagnostics?.topup?.candidatePoolAttempted, true)
    assert.equal(Number(loaded?.diagnostics?.topup?.candidatePoolRowsAdded || 0) >= 4, true)
    assert.deepEqual(loaded?.diagnostics?.missingCategoriesAfterTopup || [], [])
  } finally {
    marketSourceCatalogRepo.listScannerSource = originals.listScannerSource
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test.skip("legacy scanner source loader uses active tradable fallback when candidate pool fails", async () => {
  const originals = {
    listScannerSource: marketSourceCatalogRepo.listScannerSource,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  marketSourceCatalogRepo.listScannerSource = async () => [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      item_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true
    }
  ]
  marketSourceCatalogRepo.listCandidatePool = async () => {
    throw new Error("candidate_pool_failed")
  }
  marketSourceCatalogRepo.listActiveTradable = async () => [
    {
      market_hash_name: "★ Broken Fang Gloves | Jade (Field-Tested)",
      item_name: "★ Broken Fang Gloves | Jade (Field-Tested)",
      category: "glove",
      tradable: true,
      is_active: true
    }
  ]

  try {
    const loaded = await loadScannerSourceRows()
    const hasGlove = (loaded.rows || []).some((row) => row?.category === "glove")
    assert.equal(hasGlove, true)
    assert.equal(loaded?.diagnostics?.topup?.candidatePoolAttempted, true)
    assert.equal(loaded?.diagnostics?.topup?.candidatePoolFailed, true)
    assert.equal(loaded?.diagnostics?.topup?.activeTradableAttempted, true)
    assert.equal(Number(loaded?.diagnostics?.topup?.activeTradableRowsAdded || 0) >= 1, true)
  } finally {
    marketSourceCatalogRepo.listScannerSource = originals.listScannerSource
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("scanner source loader uses persisted hot/warm/cold cohorts on the healthy path", async () => {
  const originals = {
    listHotScanCohort: marketSourceCatalogRepo.listHotScanCohort,
    listWarmScanCohort: marketSourceCatalogRepo.listWarmScanCohort,
    listColdScanCohort: marketSourceCatalogRepo.listColdScanCohort,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }
  const requiredPrimaryPoolSize =
    OPPORTUNITY_BATCH_RUNTIME_TARGET * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER
  const hotCount = OPPORTUNITY_BATCH_RUNTIME_TARGET
  const warmCount = Math.max(requiredPrimaryPoolSize - hotCount, 2)
  const caseWarmCount = Math.max(Math.ceil(warmCount / 2), 1)
  const capsuleWarmCount = Math.max(warmCount - caseWarmCount, 1)

  marketSourceCatalogRepo.listHotScanCohort = async () =>
    Array.from({ length: hotCount }, (_, index) => ({
      ...buildCatalogRow(index + 1, "weapon_skin"),
      market_hash_name: `AK-47 | Redline (Field-Tested) #${index + 1}`,
      item_name: `AK-47 | Redline (Field-Tested) #${index + 1}`,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    }))
  marketSourceCatalogRepo.listWarmScanCohort = async () => [
    ...Array.from({ length: caseWarmCount }, (_, index) => ({
      ...buildCatalogRow(index + 1, "case"),
      market_hash_name: `Revolution Case #${index + 1}`,
      item_name: `Revolution Case #${index + 1}`,
      category: "case",
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable"
    })),
    ...Array.from({ length: capsuleWarmCount }, (_, index) => ({
      ...buildCatalogRow(index + 1, "sticker_capsule"),
      market_hash_name: `Sticker Capsule #${index + 1}`,
      item_name: `Sticker Capsule #${index + 1}`,
      category: "sticker_capsule",
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable"
    }))
  ]
  marketSourceCatalogRepo.listColdScanCohort = async () => []
  marketSourceCatalogRepo.listCandidatePool = async () => []
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    const byCategory = {}
    for (const row of loaded.rows || []) {
      byCategory[row.category] = Number(byCategory[row.category] || 0) + 1
    }
    assert.equal(Number(byCategory.weapon_skin || 0) >= 1, true)
    assert.equal(Number(byCategory.case || 0) >= 1, true)
    assert.equal(Number(byCategory.sticker_capsule || 0) >= 1, true)
    assert.equal(loaded?.diagnostics?.sourceMode, "persisted_cohorts")
    assert.equal(loaded?.diagnostics?.fallbackUsed, false)
    assert.deepEqual(loaded?.diagnostics?.missingCategoriesAfterPrimary || [], [])
    assert.equal(Number(loaded?.diagnostics?.primaryCohortCounts?.hot || 0) >= 1, true)
    assert.equal(Number(loaded?.diagnostics?.primaryCohortCounts?.warm || 0) >= 2, true)
  } finally {
    marketSourceCatalogRepo.listHotScanCohort = originals.listHotScanCohort
    marketSourceCatalogRepo.listWarmScanCohort = originals.listWarmScanCohort
    marketSourceCatalogRepo.listColdScanCohort = originals.listColdScanCohort
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("scanner source loader prefers alpha hot universe over broad persisted cohort rows", async () => {
  const originals = {
    listHotScanCohort: marketSourceCatalogRepo.listHotScanCohort,
    listWarmScanCohort: marketSourceCatalogRepo.listWarmScanCohort,
    listColdScanCohort: marketSourceCatalogRepo.listColdScanCohort,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  marketSourceCatalogRepo.listHotScanCohort = async () => [
    ...Array.from({ length: 10 }, (_, index) => ({
      ...buildCatalogRow(index + 1, "weapon_skin"),
      market_hash_name: `AWP | Lightning Strike #${index + 1}`,
      item_name: `AWP | Lightning Strike #${index + 1}`,
      liquidity_rank: 90 - index
    })),
    ...Array.from({ length: 6 }, (_, index) => ({
      ...buildCatalogRow(index + 1, "weapon_skin"),
      market_hash_name: `AK-47 | Slate #${index + 1}`,
      item_name: `AK-47 | Slate #${index + 1}`,
      liquidity_rank: 70 - index
    })),
    {
      ...buildCatalogRow(99, "weapon_skin"),
      market_hash_name: "Missing Coverage | Test",
      item_name: "Missing Coverage | Test",
      reference_price: null,
      market_coverage_count: 0
    }
  ]
  marketSourceCatalogRepo.listWarmScanCohort = async () => [
    {
      ...buildCatalogRow(1, "case"),
      market_hash_name: "Revolution Case",
      item_name: "Revolution Case",
      category: "case",
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm"
    },
    {
      ...buildCatalogRow(2, "sticker_capsule"),
      market_hash_name: "Sticker Capsule 2",
      item_name: "Sticker Capsule 2",
      category: "sticker_capsule",
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm"
    }
  ]
  marketSourceCatalogRepo.listColdScanCohort = async () => [
    {
      ...buildCatalogRow(3, "weapon_skin"),
      market_hash_name: "Cold Candidate | Test",
      item_name: "Cold Candidate | Test",
      candidate_status: "candidate",
      scan_eligible: false,
      scanCohort: "cold"
    }
  ]
  marketSourceCatalogRepo.listCandidatePool = async () => []
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    assert.equal(loaded?.diagnostics?.selection_layer, "alpha_hot_universe")
    assert.equal(loaded?.diagnostics?.hot_universe_size, loaded.rows.length)
    assert.equal(loaded.rows.some((row) => row?.alpha_hot_universe_source === "alpha_hot_universe"), true)
    assert.equal(loaded.rows.some((row) => row?.scanCohort === "cold"), false)
    assert.equal(loaded.rows.some((row) => row?.market_hash_name === "Missing Coverage | Test"), false)
    assert.equal(Number(loaded?.diagnostics?.hot_universe_by_category?.case || 0) >= 1, true)
    assert.equal(Number(loaded?.diagnostics?.hot_universe_by_category?.sticker_capsule || 0) >= 1, true)
    assert.equal(Number(loaded?.diagnostics?.intake_by_category?.weapon_skin || 0) >= 1, true)
    assert.equal(Number(loaded?.diagnostics?.intake_by_subtype?.sniper || 0) >= 1, true)
    assert.equal(Number(loaded?.diagnostics?.intake_by_subtype?.rifle || 0) >= 1, true)
    assert.equal(
      Number.isFinite(Number(loaded?.diagnostics?.quota_skips_by_category?.weapon_skin || 0)),
      true
    )
    assert.equal(Number(loaded?.diagnostics?.rows_excluded_for_missing_coverage || 0) >= 1, true)
    const weaponFamilies = new Set(
      loaded.rows
        .filter((row) => row?.category === "weapon_skin")
        .map((row) => String(row?.alpha_hot_diversity_bucket || "").split(":")[1] || "unknown")
    )
    assert.equal(weaponFamilies.has("awp"), true)
    assert.equal(weaponFamilies.has("ak-47"), true)
  } finally {
    marketSourceCatalogRepo.listHotScanCohort = originals.listHotScanCohort
    marketSourceCatalogRepo.listWarmScanCohort = originals.listWarmScanCohort
    marketSourceCatalogRepo.listColdScanCohort = originals.listColdScanCohort
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("scanner source loader still produces rows when catalog_status is missing or null", async () => {
  const originals = {
    listHotScanCohort: marketSourceCatalogRepo.listHotScanCohort,
    listWarmScanCohort: marketSourceCatalogRepo.listWarmScanCohort,
    listColdScanCohort: marketSourceCatalogRepo.listColdScanCohort,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  const requiredPrimaryPoolSize =
    OPPORTUNITY_BATCH_RUNTIME_TARGET * SCAN_COHORT_PRIMARY_POOL_MULTIPLIER
  const hotCount = OPPORTUNITY_BATCH_RUNTIME_TARGET
  const warmCount = Math.max(requiredPrimaryPoolSize - hotCount, 2)
  const caseWarmCount = Math.max(Math.ceil(warmCount / 2), 1)
  const capsuleWarmCount = Math.max(warmCount - caseWarmCount, 1)
  const nowIso = new Date().toISOString()

  marketSourceCatalogRepo.listHotScanCohort = async () =>
    Array.from({ length: hotCount }, (_, index) => ({
      ...buildCatalogRow(index + 1, "weapon_skin"),
      market_hash_name: `AK-47 | Slate (Field-Tested) #${index + 1}`,
      item_name: `AK-47 | Slate (Field-Tested) #${index + 1}`,
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
      ...buildCatalogRow(index + 1, "case"),
      market_hash_name: `Revolution Case #${index + 1}`,
      item_name: `Revolution Case #${index + 1}`,
      category: "case",
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: null,
      reference_price: 2.4,
      market_coverage_count: 3,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    })),
    ...Array.from({ length: capsuleWarmCount }, (_, index) => ({
      ...buildCatalogRow(index + 1, "sticker_capsule"),
      market_hash_name: `Sticker Capsule #${index + 1}`,
      item_name: `Sticker Capsule #${index + 1}`,
      category: "sticker_capsule",
      candidate_status: "near_eligible",
      scan_eligible: false,
      reference_price: 2.9,
      market_coverage_count: 3,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }))
  ]
  marketSourceCatalogRepo.listColdScanCohort = async () => []
  marketSourceCatalogRepo.listCandidatePool = async () => []
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    assert.equal((loaded.rows || []).length >= OPPORTUNITY_BATCH_RUNTIME_TARGET, true)
    assert.equal(loaded?.diagnostics?.fallbackUsed, false)
    assert.equal(
      (loaded.rows || []).every((row) => String(row?.catalog_status || "").toLowerCase() === "scannable"),
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

test("scanner source loader uses active tradable fallback only after cohort fallback fails", async () => {
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
      item_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable",
      reference_price: 18,
      market_coverage_count: 3,
      volume_7d: 140,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
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
      item_name: "Recoil Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "candidate",
      scan_eligible: false,
      catalog_status: "scannable",
      reference_price: 2.4,
      market_coverage_count: 3,
      volume_7d: 400,
      snapshot_captured_at: new Date().toISOString(),
      quote_fetched_at: new Date().toISOString()
    }
  ]

  try {
    const loaded = await loadScannerSourceRows()
    const hasFallbackCase = (loaded.rows || []).some(
      (row) =>
        row?.category === "case" &&
        String(row?.fallbackSource || "").toLowerCase() === "activetradable"
    )
    assert.equal(hasFallbackCase, false)
    assert.equal(loaded?.diagnostics?.fallbackUsed, true)
    assert.equal(loaded?.diagnostics?.cohortQueryFailures?.candidatePool, true)
    assert.equal(
      Number(loaded?.diagnostics?.fallbackRowsLoadedBySource?.activeTradable || 0) >= 1,
      true
    )
    assert.equal(Number(loaded?.diagnostics?.hot_universe_by_category?.weapon_skin || 0) >= 1, true)
    assert.equal(Number(loaded?.diagnostics?.hot_universe_by_category?.case || 0), 0)
    assert.equal(Number(loaded?.diagnostics?.rows_excluded_for_low_maturity || 0) >= 1, true)
  } finally {
    marketSourceCatalogRepo.listHotScanCohort = originals.listHotScanCohort
    marketSourceCatalogRepo.listWarmScanCohort = originals.listWarmScanCohort
    marketSourceCatalogRepo.listColdScanCohort = originals.listColdScanCohort
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("recovery loader preserves fallback provenance through selection", async () => {
  const originals = {
    loadScanSource: scanSourceCohortService.loadScanSource,
    listScannerSource: marketSourceCatalogRepo.listScannerSource,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  scanSourceCohortService.loadScanSource = async () => {
    throw new Error("cohort_loader_failed")
  }
  marketSourceCatalogRepo.listScannerSource = async () => [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      item_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable",
      last_market_signal_at: new Date().toISOString()
    }
  ]
  marketSourceCatalogRepo.listCandidatePool = async () => [
    {
      market_hash_name: "Revolution Case",
      item_name: "Revolution Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable",
      last_market_signal_at: new Date().toISOString()
    }
  ]
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    const fallbackRow = (loaded.rows || []).find(
      (row) => String(row?.fallbackSource || "").toLowerCase() === "candidatepool"
    )
    assert.equal(Boolean(fallbackRow), true)
    assert.equal(fallbackRow.scanCohort, "fallback")

    const selection = selectScanCandidates({
      catalogRows: loaded.rows || [],
      batchSize: 4,
      cursor: 0,
      lastScannedAtByName: new Map()
    })
    const selectedFallback = selection.selected.find(
      (row) => String(row?.fallbackSource || "").toLowerCase() === "candidatepool"
    )
    assert.equal(Boolean(selectedFallback), true)
    assert.equal(selection.diagnostics.selectedByCohort.fallback >= 1, true)
    assert.equal(
      Number(selection.diagnostics.fallbackSelectedBySource.candidatePool || 0) >= 1,
      true
    )
  } finally {
    scanSourceCohortService.loadScanSource = originals.loadScanSource
    marketSourceCatalogRepo.listScannerSource = originals.listScannerSource
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("recovery loader derives scannable fallback rows when catalog_status is missing", async () => {
  const originals = {
    loadScanSource: scanSourceCohortService.loadScanSource,
    listScannerSource: marketSourceCatalogRepo.listScannerSource,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  scanSourceCohortService.loadScanSource = async () => {
    throw new Error("cohort_loader_failed")
  }
  marketSourceCatalogRepo.listScannerSource = async () => []
  marketSourceCatalogRepo.listCandidatePool = async () => [
    {
      market_hash_name: "Revolution Case",
      item_name: "Revolution Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      scan_eligible: false,
      reference_price: 2.5,
      market_coverage_count: 3,
      snapshot_captured_at: new Date().toISOString(),
      last_market_signal_at: new Date().toISOString()
    }
  ]
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    assert.equal((loaded.rows || []).length >= 1, true)
    assert.equal(
      (loaded.rows || []).some(
        (row) =>
          row?.market_hash_name === "Revolution Case" &&
          String(row?.catalog_status || "").toLowerCase() === "scannable"
      ),
      true
    )
    assert.equal(
      Number(loaded?.diagnostics?.fallbackRowsLoadedBySource?.candidatePool || 0) >= 1,
      true
    )
  } finally {
    scanSourceCohortService.loadScanSource = originals.loadScanSource
    marketSourceCatalogRepo.listScannerSource = originals.listScannerSource
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("weapon skin missing liquidity but otherwise supported downgrades instead of hard rejecting", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Bloodsport (Field-Tested)",
      itemName: "AK-47 | Bloodsport (Field-Tested)",
      category: "weapon_skin",
      itemSubcategory: null,
      referencePrice: 18,
      marketCoverageCount: 2,
      volume7d: 140,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso,
      scanPenaltyFlags: [],
      scanFreshness: { state: "fresh" }
    },
    {
      marketHashName: "AK-47 | Bloodsport (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 11.2,
          netPriceAfterFees: 9.74,
          updatedAt: nowIso
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 12.1,
          netPriceAfterFees: 13.9,
          updatedAt: nowIso,
          raw: { listing_id: "skinport-bloodsport-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 11.2, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 13.9, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 11.2,
        sellMarket: "skinport",
        sellNet: 13.9,
        profit: 2.7,
        spreadPercent: 24.1,
        opportunityScore: 71,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 18,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, false)
  assert.equal(evaluation.tier, "risky")
  assert.equal(evaluation.penaltyFlags.includes("low_sales_liquidity"), true)
  assert.equal(evaluation.riskLabels.includes("derived_liquidity_support"), true)
  assert.equal(evaluation.evaluationDisposition, "risky_eligible")
  assert.equal(evaluation.publishPreviewResult, "publishable")
  assert.equal(
    Boolean(evaluation?.metadata?.weapon_skin_evaluator_diagnostics?.missing_liquidity_penalty),
    true
  )
  assert.equal(evaluation?.metadata?.publish_validation_preview?.is_publishable, true)
})

test("weapon skin partial coverage but valid route/economics downgrades instead of hard rejecting", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemName: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 10.2,
      marketCoverageCount: 1,
      volume7d: 82,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 8.2,
          netPriceAfterFees: 9.4,
          updatedAt: nowIso,
          volume7d: 82
        }
      ],
      bestBuy: { source: "steam", grossPrice: 8.2, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "steam", netPriceAfterFees: 9.4, url: "https://steamcommunity.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 8.2,
        sellMarket: "steam",
        sellNet: 9.4,
        profit: 1.2,
        spreadPercent: 14.63,
        opportunityScore: 69,
        executionConfidence: "Medium",
        marketCoverage: 1,
        referencePrice: 10.2,
        depthFlags: [],
        antiFake: { reasons: ["ignored_missing_markets"] }
      }
    }
  )

  assert.equal(evaluation.rejected, false)
  assert.equal(evaluation.penaltyFlags.includes("limited_market_coverage"), true)
  assert.equal(evaluation.riskLabels.includes("partial_market_coverage"), true)
  assert.equal(
    Boolean(
      evaluation?.metadata?.weapon_skin_evaluator_diagnostics?.partial_market_coverage_penalty
    ),
    true
  )
  assert.equal(evaluation.hardRejectReasons.includes("unusable_market_coverage"), false)
})

test("opportunity diagnostics avoid contradictory tags for high-coverage fresh cases", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Revolution Case",
      itemName: "Revolution Case",
      category: "case",
      referencePrice: 2.05,
      marketCoverageCount: 4,
      volume7d: 4039,
      snapshotCapturedAt: nowIso,
      quoteFetchedAt: nowIso
    },
    {
      marketHashName: "Revolution Case",
      perMarket: [
        { source: "steam", available: true, grossPrice: 1.88, netPriceAfterFees: 1.64, updatedAt: nowIso, volume7d: 4039 },
        { source: "skinport", available: true, grossPrice: 1.92, netPriceAfterFees: 1.69, updatedAt: nowIso, volume7d: 1880 },
        { source: "dmarket", available: true, grossPrice: 1.9, netPriceAfterFees: 1.77, updatedAt: nowIso, volume7d: 640 },
        { source: "csfloat", available: true, grossPrice: 1.94, netPriceAfterFees: 1.9, updatedAt: nowIso, volume7d: 320 }
      ],
      bestBuy: { source: "steam", grossPrice: 1.88, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "csfloat", netPriceAfterFees: 2.26, url: "https://csfloat.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 1.88,
        sellMarket: "csfloat",
        sellNet: 2.26,
        profit: 0.38,
        spreadPercent: 20.21,
        opportunityScore: 74,
        executionConfidence: "Medium",
        marketCoverage: 4,
        referencePrice: 2.05,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.penaltyFlags.includes("limited_market_coverage"), false)
  assert.equal(evaluation.penaltyFlags.includes("low_sales_liquidity"), false)
  assert.equal(evaluation.penaltyFlags.includes("stale_market_signal"), false)
  assert.equal(evaluation.marketCoverageBand, "High")
  assert.equal(
    Number(evaluation?.metadata?.diagnostics_debug?.market_coverage_score || 0) >= 80,
    true
  )
})

test("opportunity diagnostics expose dimension debug output", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemName: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 10.2,
      marketCoverageCount: 2,
      volume7d: 80,
      snapshotCapturedAt: nowIso,
      quoteFetchedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      perMarket: [
        { source: "steam", available: true, grossPrice: 8.2, netPriceAfterFees: 7.13, updatedAt: nowIso, volume7d: 80 },
        { source: "skinport", available: true, grossPrice: 8.9, netPriceAfterFees: 7.83, updatedAt: nowIso, volume7d: 34 }
      ],
      bestBuy: { source: "steam", grossPrice: 8.2, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 9.45, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 8.2,
        sellMarket: "skinport",
        sellNet: 9.45,
        profit: 1.25,
        spreadPercent: 15.24,
        opportunityScore: 73,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 10.2,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  const debug = evaluation?.metadata?.diagnostics_debug || {}
  assert.equal(Number.isFinite(Number(debug.sales_liquidity_score)), true)
  assert.equal(Number.isFinite(Number(debug.executable_depth_score)), true)
  assert.equal(Number.isFinite(Number(debug.market_coverage_score)), true)
  assert.equal(Number.isFinite(Number(debug.data_freshness_score)), true)
  assert.equal(typeof debug.category, "string")
  assert.equal(Boolean(debug.latest_quote_at), true)
  assert.equal(Boolean(debug.latest_snapshot_at), true)
  assert.equal(Boolean(debug.latest_reference_price_at), true)
  assert.equal(Boolean(debug.latest_market_signal_at), true)
  assert.equal(Number.isFinite(Number(debug.stale_threshold_used)), true)
  assert.equal(typeof debug.stale_result, "boolean")
  assert.equal(typeof debug.stale_reason_source, "string")
  assert.equal(Array.isArray(debug?.raw_reasons?.sales_liquidity), true)
  assert.equal(Array.isArray(debug?.raw_reasons?.emitted_tags), true)
})

test("weapon skin stale supporting inputs downgrade while publish freshness still passes", () => {
  const oldIso = new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString()
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      itemName: "AK-47 | Slate (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 8.4,
      marketCoverageCount: 2,
      volume7d: 96,
      snapshotCapturedAt: oldIso,
      quoteFetchedAt: oldIso,
      latest_reference_price_at: oldIso,
      last_market_signal_at: oldIso
    },
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 6.8,
          netPriceAfterFees: 5.9,
          updatedAt: nowIso,
          volume7d: 96
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 7.1,
          netPriceAfterFees: 7.9,
          updatedAt: nowIso,
          volume7d: 45,
          raw: { listing_id: "skinport-slate-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 6.8, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 7.9, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 6.8,
        sellMarket: "skinport",
        sellNet: 7.9,
        profit: 1.1,
        spreadPercent: 16.17,
        opportunityScore: 70,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 8.4,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, false)
  assert.equal(evaluation.penaltyFlags.includes("stale_market_signal"), true)
  assert.equal(evaluation.riskLabels.includes("stale_supporting_signal"), true)
  assert.equal(
    Boolean(evaluation?.metadata?.weapon_skin_evaluator_diagnostics?.stale_supporting_input_penalty),
    true
  )
  assert.equal(evaluation?.metadata?.publish_validation_preview?.is_publishable, true)
})

test("weapon skin true stale route hard rejects even if economics look positive", () => {
  const staleIso = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Asiimov (Field-Tested)",
      itemName: "AK-47 | Asiimov (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 24,
      marketCoverageCount: 2,
      volume7d: 110,
      quoteFetchedAt: new Date().toISOString(),
      snapshotCapturedAt: new Date().toISOString()
    },
    {
      marketHashName: "AK-47 | Asiimov (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 18.4,
          netPriceAfterFees: 16.01,
          updatedAt: staleIso,
          volume7d: 110
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 19.1,
          netPriceAfterFees: 21.8,
          updatedAt: staleIso,
          volume7d: 56,
          raw: { listing_id: "skinport-asiimov-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 18.4, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 21.8, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 18.4,
        sellMarket: "skinport",
        sellNet: 21.8,
        profit: 3.4,
        spreadPercent: 18.48,
        opportunityScore: 77,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 24,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("buy_and_sell_route_stale"), true)
  assert.equal(evaluation.evaluationDisposition, "hard_reject")
  assert.equal(
    evaluation?.metadata?.weapon_skin_evaluator_diagnostics?.hard_reject_reason,
    "buy_and_sell_route_stale"
  )
})

test("weapon skin anti-fake failure remains a hard reject", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "M4A4 | Buzz Kill (Field-Tested)",
      itemName: "M4A4 | Buzz Kill (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 12,
      marketCoverageCount: 2,
      volume7d: 70,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "M4A4 | Buzz Kill (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 8.8,
          netPriceAfterFees: 7.66,
          updatedAt: nowIso,
          volume7d: 70
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 9.5,
          netPriceAfterFees: 11.4,
          updatedAt: nowIso,
          volume7d: 36,
          raw: { listing_id: "skinport-buzzkill-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 8.8, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 11.4, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 8.8,
        sellMarket: "skinport",
        sellNet: 11.4,
        profit: 2.6,
        spreadPercent: 29.55,
        opportunityScore: 74,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 12,
        depthFlags: [],
        antiFake: { reasons: ["ignored_reference_deviation"] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("extreme_reference_deviation"), true)
  assert.equal(evaluation.evaluationDisposition, "hard_reject")
})

test("weapon skin risky-but-eligible output stays publishable with explicit diagnostics", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "M4A1-S | Decimator (Field-Tested)",
      itemName: "M4A1-S | Decimator (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 9.6,
      marketCoverageCount: 2,
      volume7d: 88,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "M4A1-S | Decimator (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 7.3,
          netPriceAfterFees: 6.35,
          updatedAt: nowIso,
          volume7d: 88
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 8.1,
          netPriceAfterFees: 9.05,
          updatedAt: nowIso,
          volume7d: 41,
          raw: { listing_id: "skinport-decimator-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 7.3, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 9.05, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 7.3,
        sellMarket: "skinport",
        sellNet: 9.05,
        profit: 1.75,
        spreadPercent: 23.97,
        opportunityScore: 72,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 9.6,
        depthFlags: ["BUY_DEPTH_GAP_SUSPICIOUS"],
        antiFake: { reasons: ["ignored_missing_depth"] }
      }
    }
  )

  assert.equal(evaluation.rejected, false)
  assert.equal(evaluation.qualityGrade, "RISKY")
  assert.equal(evaluation.evaluationDisposition, "risky_eligible")
  assert.equal(evaluation.finalTier, "risky")
  assert.equal(evaluation.publishPreviewResult, "publishable")
  assert.equal(evaluation.riskLabels.includes("missing_executable_depth"), true)
  assert.equal(evaluation.penaltyFlags.includes("thin_executable_depth"), true)
  assert.equal(
    Boolean(evaluation?.metadata?.weapon_skin_evaluator_diagnostics?.thin_executable_depth_penalty),
    true
  )
  assert.equal(evaluation?.metadata?.publish_validation_preview?.is_publishable, true)
})

test("weapon skin evaluator preview stays aligned with publisher gate for representative cases", () => {
  const nowIso = new Date().toISOString()
  const publishable = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Fuel Injector (Field-Tested)",
      itemName: "AK-47 | Fuel Injector (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 17.4,
      marketCoverageCount: 2,
      volume7d: 73,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Fuel Injector (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 13.8,
          netPriceAfterFees: 12,
          updatedAt: nowIso,
          volume7d: 73
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 14.4,
          netPriceAfterFees: 16.7,
          updatedAt: nowIso,
          volume7d: 38,
          raw: { listing_id: "skinport-fuel-injector-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 13.8, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 16.7, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 13.8,
        sellMarket: "skinport",
        sellNet: 16.7,
        profit: 2.9,
        spreadPercent: 21.01,
        opportunityScore: 82,
        executionConfidence: "High",
        marketCoverage: 2,
        referencePrice: 17.4,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )
  const publishableExpected = buildPublishValidationPreview(
    evaluatePublishValidation({
      buyMarket: publishable.buyMarket,
      sellMarket: publishable.sellMarket,
      buyRouteAvailable: publishable.buyRouteAvailable,
      sellRouteAvailable: publishable.sellRouteAvailable,
      buyRouteUpdatedAt: publishable.buyRouteUpdatedAt,
      sellRouteUpdatedAt: publishable.sellRouteUpdatedAt,
      buyListingAvailable: publishable.buyListingAvailable,
      sellListingAvailable: publishable.sellListingAvailable
    })
  )
  assert.equal(
    publishable.metadata.publish_validation_preview.result_label,
    publishableExpected.result_label
  )
  assert.equal(
    publishable.metadata.publish_validation_preview.is_publishable,
    publishableExpected.is_publishable
  )
  assert.equal(
    publishable.metadata.publish_validation_preview.required_route_state,
    publishableExpected.required_route_state
  )
  assert.equal(
    publishable.metadata.publish_validation_preview.listing_availability_state,
    publishableExpected.listing_availability_state
  )
  assert.equal(
    publishable.metadata.publish_validation_preview.publish_freshness_state,
    publishableExpected.publish_freshness_state
  )
  assert.equal(
    publishable.metadata.publish_validation_preview.route_signal_observed_at,
    publishableExpected.route_signal_observed_at
  )
  assert.equal(
    Math.abs(
      Number(publishable.metadata.publish_validation_preview.signal_age_ms || 0) -
        Number(publishableExpected.signal_age_ms || 0)
    ) <= 5,
    true
  )

  const staleIso = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const blocked = evaluateCandidateOpportunity(
    {
      marketHashName: "M4A4 | Emperor (Field-Tested)",
      itemName: "M4A4 | Emperor (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 15.2,
      marketCoverageCount: 2,
      volume7d: 64,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "M4A4 | Emperor (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 11.7,
          netPriceAfterFees: 10.18,
          updatedAt: staleIso,
          volume7d: 64
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 12.4,
          netPriceAfterFees: 14.1,
          updatedAt: staleIso,
          volume7d: 31,
          raw: { listing_id: "skinport-emperor-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 11.7, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 14.1, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 11.7,
        sellMarket: "skinport",
        sellNet: 14.1,
        profit: 2.4,
        spreadPercent: 20.51,
        opportunityScore: 75,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 15.2,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )
  const blockedExpected = buildPublishValidationPreview(
    evaluatePublishValidation({
      buyMarket: blocked.buyMarket,
      sellMarket: blocked.sellMarket,
      buyRouteAvailable: blocked.buyRouteAvailable,
      sellRouteAvailable: blocked.sellRouteAvailable,
      buyRouteUpdatedAt: blocked.buyRouteUpdatedAt,
      sellRouteUpdatedAt: blocked.sellRouteUpdatedAt,
      buyListingAvailable: blocked.buyListingAvailable,
      sellListingAvailable: blocked.sellListingAvailable
    })
  )
  assert.equal(blocked.metadata.publish_validation_preview.result_label, blockedExpected.result_label)
  assert.equal(
    blocked.metadata.publish_validation_preview.is_publishable,
    blockedExpected.is_publishable
  )
  assert.equal(
    blocked.metadata.publish_validation_preview.required_route_state,
    blockedExpected.required_route_state
  )
  assert.equal(
    blocked.metadata.publish_validation_preview.listing_availability_state,
    blockedExpected.listing_availability_state
  )
  assert.equal(
    blocked.metadata.publish_validation_preview.publish_freshness_state,
    blockedExpected.publish_freshness_state
  )
  assert.equal(
    blocked.metadata.publish_validation_preview.stale_reason,
    blockedExpected.stale_reason
  )
})

test("weapon skin compare freshness contract preserves missing route timestamps explicitly", () => {
  const nowIso = new Date().toISOString()
  const missingBuyTimestamp = evaluateCandidateOpportunity(
    {
      marketHashName: "USP-S | Cortex (Field-Tested)",
      itemName: "USP-S | Cortex (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 8.4,
      marketCoverageCount: 2,
      volume7d: 58,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "USP-S | Cortex (Field-Tested)",
      routeFreshnessContract: {
        buyMarket: "steam",
        sellMarket: "skinport",
        buyRouteAvailable: true,
        sellRouteAvailable: true,
        buyRouteUpdatedAt: null,
        sellRouteUpdatedAt: nowIso,
        sellListingAvailable: true
      },
      bestBuy: { source: "steam", grossPrice: 6.5, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 7.7, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 6.5,
        sellMarket: "skinport",
        sellNet: 7.7,
        profit: 1.2,
        spreadPercent: 18.46,
        opportunityScore: 68,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 8.4,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )
  const missingSellTimestamp = evaluateCandidateOpportunity(
    {
      marketHashName: "USP-S | Cortex (Field-Tested)",
      itemName: "USP-S | Cortex (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 8.4,
      marketCoverageCount: 2,
      volume7d: 58,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "USP-S | Cortex (Field-Tested)",
      routeFreshnessContract: {
        buyMarket: "steam",
        sellMarket: "skinport",
        buyRouteAvailable: true,
        sellRouteAvailable: true,
        buyRouteUpdatedAt: nowIso,
        sellRouteUpdatedAt: null,
        sellListingAvailable: true
      },
      bestBuy: { source: "steam", grossPrice: 6.5, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 7.7, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 6.5,
        sellMarket: "skinport",
        sellNet: 7.7,
        profit: 1.2,
        spreadPercent: 18.46,
        opportunityScore: 68,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 8.4,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(missingBuyTimestamp.publishPreviewResult, "missing_buy_route_timestamp")
  assert.equal(
    missingBuyTimestamp?.metadata?.freshness_contract_diagnostics?.missing_buy_route_timestamp,
    true
  )
  assert.equal(
    missingBuyTimestamp?.metadata?.freshness_contract_diagnostics?.missing_sell_route_timestamp,
    false
  )
  assert.equal(missingSellTimestamp.publishPreviewResult, "missing_sell_route_timestamp")
  assert.equal(
    missingSellTimestamp?.metadata?.freshness_contract_diagnostics?.missing_sell_route_timestamp,
    true
  )
  assert.equal(
    missingSellTimestamp?.metadata?.freshness_contract_diagnostics?.missing_buy_route_timestamp,
    false
  )
})

test("weapon skin freshness diagnostics distinguish one stale route from both stale routes", () => {
  const nowIso = new Date().toISOString()
  const staleIso = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const buyStale = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      itemName: "AK-47 | Slate (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 4.9,
      marketCoverageCount: 2,
      volume7d: 91,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      routeFreshnessContract: {
        buyMarket: "steam",
        sellMarket: "skinport",
        buyRouteAvailable: true,
        sellRouteAvailable: true,
        buyRouteUpdatedAt: staleIso,
        sellRouteUpdatedAt: nowIso,
        sellListingAvailable: true
      },
      bestBuy: { source: "steam", grossPrice: 3.6, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 4.2, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 3.6,
        sellMarket: "skinport",
        sellNet: 4.2,
        profit: 0.6,
        spreadPercent: 16.67,
        opportunityScore: 62,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 4.9,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )
  const bothStale = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      itemName: "AK-47 | Slate (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 4.9,
      marketCoverageCount: 2,
      volume7d: 91,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      routeFreshnessContract: {
        buyMarket: "steam",
        sellMarket: "skinport",
        buyRouteAvailable: true,
        sellRouteAvailable: true,
        buyRouteUpdatedAt: staleIso,
        sellRouteUpdatedAt: staleIso,
        sellListingAvailable: true
      },
      bestBuy: { source: "steam", grossPrice: 3.6, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 4.2, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 3.6,
        sellMarket: "skinport",
        sellNet: 4.2,
        profit: 0.6,
        spreadPercent: 16.67,
        opportunityScore: 62,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 4.9,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(buyStale.publishPreviewResult, "buy_route_stale")
  assert.equal(buyStale?.metadata?.freshness_contract_diagnostics?.buy_route_stale, true)
  assert.equal(
    buyStale?.metadata?.freshness_contract_diagnostics?.buy_and_sell_route_stale,
    false
  )
  assert.equal(bothStale.publishPreviewResult, "buy_and_sell_route_stale")
  assert.equal(
    bothStale?.metadata?.freshness_contract_diagnostics?.buy_and_sell_route_stale,
    true
  )
})

test("weapon skin missing listing availability is surfaced without corrupting publish contract", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "M4A1-S | Nightmare (Field-Tested)",
      itemName: "M4A1-S | Nightmare (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 7.6,
      marketCoverageCount: 2,
      volume7d: 63,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "M4A1-S | Nightmare (Field-Tested)",
      routeFreshnessContract: {
        buyMarket: "steam",
        sellMarket: "skinport",
        buyRouteAvailable: true,
        sellRouteAvailable: true,
        buyRouteUpdatedAt: nowIso,
        sellRouteUpdatedAt: nowIso,
        sellListingAvailable: null
      },
      bestBuy: { source: "steam", grossPrice: 5.9, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 7.1, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 5.9,
        sellMarket: "skinport",
        sellNet: 7.1,
        profit: 1.2,
        spreadPercent: 20.34,
        opportunityScore: 66,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 7.6,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.publishPreviewResult, "publishable")
  assert.equal(
    evaluation?.metadata?.freshness_contract_diagnostics?.missing_listing_availability,
    true
  )
  assert.equal(
    evaluation?.metadata?.freshness_contract_diagnostics?.freshness_contract_incomplete,
    true
  )
  assert.equal(
    evaluation?.metadata?.publish_validation_preview?.listing_availability_state,
    "unknown_sell_listing"
  )
})

test("weapon skin evaluator and publisher consume the same freshness contract fields", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Desert Eagle | Mecha Industries (Field-Tested)",
      itemName: "Desert Eagle | Mecha Industries (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 9.8,
      marketCoverageCount: 2,
      volume7d: 77,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "Desert Eagle | Mecha Industries (Field-Tested)",
      routeFreshnessContract: {
        buyMarket: "steam",
        sellMarket: "skinport",
        buyRouteAvailable: true,
        sellRouteAvailable: true,
        buyRouteUpdatedAt: nowIso,
        sellRouteUpdatedAt: nowIso,
        sellListingAvailable: true,
        contractSource: "compare_result"
      },
      bestBuy: { source: "steam", grossPrice: 7.1, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 8.8, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 7.1,
        sellMarket: "skinport",
        sellNet: 8.8,
        profit: 1.7,
        spreadPercent: 23.94,
        opportunityScore: 73,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 9.8,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  const publishValidation = resolvePublishValidationContextForOpportunity(evaluation, Date.now(), nowIso)
  assert.equal(
    publishValidation.routeFreshnessContract.buyRouteUpdatedAt,
    evaluation.routeFreshnessContract.buyRouteUpdatedAt
  )
  assert.equal(
    publishValidation.routeFreshnessContract.sellRouteUpdatedAt,
    evaluation.routeFreshnessContract.sellRouteUpdatedAt
  )
  assert.equal(
    publishValidation.routeFreshnessContract.buyRouteAvailable,
    evaluation.routeFreshnessContract.buyRouteAvailable
  )
  assert.equal(
    publishValidation.routeFreshnessContract.sellRouteAvailable,
    evaluation.routeFreshnessContract.sellRouteAvailable
  )
  assert.equal(
    publishValidation.routeFreshnessContract.sellListingAvailable,
    evaluation.routeFreshnessContract.sellListingAvailable
  )
  assert.equal(
    publishValidation.routeFreshnessContract.requiredRouteState,
    evaluation.routeFreshnessContract.requiredRouteState
  )
  assert.equal(
    publishValidation.routeFreshnessContract.listingAvailabilityState,
    evaluation.routeFreshnessContract.listingAvailabilityState
  )
})

test("weapon skin hard reject and soft skip are distinguishable in output and diagnostics", () => {
  const nowIso = new Date().toISOString()
  const softSkip = evaluateCandidateOpportunity(
    {
      marketHashName: "Galil AR | Stone Cold (Field-Tested)",
      itemName: "Galil AR | Stone Cold (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 3.2,
      marketCoverageCount: 1,
      volume7d: 24,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "Galil AR | Stone Cold (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 2.4,
          netPriceAfterFees: 2.85,
          updatedAt: nowIso,
          volume7d: 24
        }
      ],
      bestBuy: { source: "steam", grossPrice: 2.4, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "steam", netPriceAfterFees: 2.85, url: "https://steamcommunity.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 2.4,
        sellMarket: "steam",
        sellNet: 2.85,
        profit: 0.45,
        spreadPercent: 18.75,
        opportunityScore: 49,
        executionConfidence: "Medium",
        marketCoverage: 1,
        referencePrice: 3.2,
        depthFlags: [],
        antiFake: { reasons: ["ignored_missing_markets"] }
      }
    }
  )

  assert.equal(softSkip.rejected, true)
  assert.equal(softSkip.evaluationDisposition, "soft_skip")
  assert.equal(softSkip.hardRejectReasons.length, 0)
  assert.equal(softSkip.softSkipReasons.includes("low_value_low_support_weapon_skin"), true)
  assert.equal(
    softSkip?.metadata?.weapon_skin_evaluator_diagnostics?.soft_skip_reason,
    "low_value_low_support_weapon_skin"
  )

  const staleIso = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const hardReject = evaluateCandidateOpportunity(
    {
      marketHashName: "USP-S | Cortex (Field-Tested)",
      itemName: "USP-S | Cortex (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 11,
      marketCoverageCount: 2,
      volume7d: 58,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "USP-S | Cortex (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 8.2,
          netPriceAfterFees: 7.13,
          updatedAt: staleIso,
          volume7d: 58
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 8.8,
          netPriceAfterFees: 10.2,
          updatedAt: staleIso,
          volume7d: 24,
          raw: { listing_id: "skinport-cortex-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 8.2, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 10.2, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 8.2,
        sellMarket: "skinport",
        sellNet: 10.2,
        profit: 2,
        spreadPercent: 24.39,
        opportunityScore: 74,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 11,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(hardReject.rejected, true)
  assert.equal(hardReject.evaluationDisposition, "hard_reject")
  assert.equal(hardReject.hardRejectReasons.includes("buy_and_sell_route_stale"), true)
  assert.equal(hardReject.softSkipReasons.length, 0)
})

test("weapon skin risky and strong eligible outputs stay distinguishable in diagnostics", () => {
  const nowIso = new Date().toISOString()
  const risky = evaluateCandidateOpportunity(
    {
      marketHashName: "FAMAS | Commemoration (Field-Tested)",
      itemName: "FAMAS | Commemoration (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 8.7,
      marketCoverageCount: 2,
      volume7d: 64,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "FAMAS | Commemoration (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 6.5,
          netPriceAfterFees: 5.66,
          updatedAt: nowIso,
          volume7d: 64
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 7.3,
          netPriceAfterFees: 8.15,
          updatedAt: nowIso,
          volume7d: 29,
          raw: { listing_id: "skinport-commemoration-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 6.5, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 8.15, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 6.5,
        sellMarket: "skinport",
        sellNet: 8.15,
        profit: 1.65,
        spreadPercent: 25.38,
        opportunityScore: 72,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 8.7,
        depthFlags: ["SELL_DEPTH_GAP_SUSPICIOUS"],
        antiFake: { reasons: ["ignored_missing_depth"] }
      }
    }
  )
  const strong = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Neon Rider (Field-Tested)",
      itemName: "AK-47 | Neon Rider (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 24.5,
      marketCoverageCount: 2,
      volume7d: 144,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Neon Rider (Field-Tested)",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 17.9,
          netPriceAfterFees: 15.57,
          updatedAt: nowIso,
          volume7d: 144
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 18.6,
          netPriceAfterFees: 21.4,
          updatedAt: nowIso,
          volume7d: 88,
          raw: { listing_id: "skinport-neon-rider-1" }
        }
      ],
      bestBuy: { source: "steam", grossPrice: 17.9, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 21.4, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 17.9,
        sellMarket: "skinport",
        sellNet: 21.4,
        profit: 3.5,
        spreadPercent: 19.55,
        opportunityScore: 84,
        executionConfidence: "High",
        marketCoverage: 2,
        referencePrice: 24.5,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(risky.evaluationDisposition, "risky_eligible")
  assert.equal(risky.finalTier, "risky")
  assert.equal(risky.qualityGrade, "RISKY")
  assert.equal(strong.evaluationDisposition, "strong_eligible")
  assert.equal(strong.finalTier, "strong")
  assert.equal(strong.qualityGrade, "STRONG")
  assert.equal(strong.riskLabels.length, 0)
})

test("weapon skin evaluation summary exposes outcome and penalty telemetry", () => {
  const summary = summarizeEvaluations([
    {
      itemCategory: "weapon_skin",
      tier: "strong",
      finalTier: "strong",
      evaluationDisposition: "strong_eligible",
      hardRejectReasons: [],
      softSkipReasons: [],
      metadata: {
        weapon_skin_evaluator_diagnostics: {
          missing_liquidity_penalty: false,
          partial_market_coverage_penalty: false,
          stale_supporting_input_penalty: false,
          thin_executable_depth_penalty: false,
          low_value_contextual_penalty: false,
          soft_skip_reason: null,
          hard_reject_reason: null,
          publish_preview_result: "publishable",
          final_tier: "strong"
        },
        freshness_contract_diagnostics: {
          missing_buy_route_timestamp: false,
          missing_sell_route_timestamp: false,
          buy_route_stale: false,
          sell_route_stale: false,
          buy_and_sell_route_stale: false,
          buy_route_unavailable: false,
          sell_route_unavailable: false,
          missing_listing_availability: false,
          freshness_contract_incomplete: false
        }
      }
    },
    {
      itemCategory: "weapon_skin",
      tier: "risky",
      finalTier: "risky",
      evaluationDisposition: "risky_eligible",
      hardRejectReasons: [],
      softSkipReasons: [],
      metadata: {
        weapon_skin_evaluator_diagnostics: {
          missing_liquidity_penalty: true,
          partial_market_coverage_penalty: false,
          stale_supporting_input_penalty: false,
          thin_executable_depth_penalty: true,
          low_value_contextual_penalty: false,
          soft_skip_reason: null,
          hard_reject_reason: null,
          publish_preview_result: "publishable",
          final_tier: "risky"
        },
        freshness_contract_diagnostics: {
          missing_buy_route_timestamp: true,
          missing_sell_route_timestamp: false,
          buy_route_stale: false,
          sell_route_stale: false,
          buy_and_sell_route_stale: false,
          buy_route_unavailable: false,
          sell_route_unavailable: false,
          missing_listing_availability: true,
          freshness_contract_incomplete: true
        }
      }
    },
    {
      itemCategory: "weapon_skin",
      tier: "rejected",
      finalTier: "rejected",
      evaluationDisposition: "soft_skip",
      hardRejectReasons: [],
      softSkipReasons: ["low_value_low_support_weapon_skin"],
      metadata: {
        weapon_skin_evaluator_diagnostics: {
          missing_liquidity_penalty: false,
          partial_market_coverage_penalty: true,
          stale_supporting_input_penalty: false,
          thin_executable_depth_penalty: false,
          low_value_contextual_penalty: true,
          soft_skip_reason: "low_value_low_support_weapon_skin",
          hard_reject_reason: null,
          publish_preview_result: "publishable",
          final_tier: "rejected"
        },
        freshness_contract_diagnostics: {
          missing_buy_route_timestamp: false,
          missing_sell_route_timestamp: false,
          buy_route_stale: false,
          sell_route_stale: false,
          buy_and_sell_route_stale: false,
          buy_route_unavailable: false,
          sell_route_unavailable: true,
          missing_listing_availability: false,
          freshness_contract_incomplete: false
        }
      }
    },
    {
      itemCategory: "weapon_skin",
      tier: "rejected",
      finalTier: "rejected",
      evaluationDisposition: "hard_reject",
      hardRejectReasons: ["buy_and_sell_route_stale"],
      softSkipReasons: [],
      metadata: {
        weapon_skin_evaluator_diagnostics: {
          missing_liquidity_penalty: false,
          partial_market_coverage_penalty: false,
          stale_supporting_input_penalty: false,
          thin_executable_depth_penalty: false,
          low_value_contextual_penalty: false,
          soft_skip_reason: null,
          hard_reject_reason: "buy_and_sell_route_stale",
          publish_preview_result: "buy_and_sell_route_stale",
          final_tier: "rejected"
        },
        freshness_contract_diagnostics: {
          missing_buy_route_timestamp: false,
          missing_sell_route_timestamp: false,
          buy_route_stale: false,
          sell_route_stale: false,
          buy_and_sell_route_stale: true,
          buy_route_unavailable: false,
          sell_route_unavailable: false,
          missing_listing_availability: false,
          freshness_contract_incomplete: false
        }
      }
    }
  ])

  assert.equal(summary.weaponSkinEvaluator.outcome.strong_eligible, 1)
  assert.equal(summary.weaponSkinEvaluator.outcome.risky_eligible, 1)
  assert.equal(summary.weaponSkinEvaluator.outcome.soft_skip, 1)
  assert.equal(summary.weaponSkinEvaluator.outcome.hard_reject, 1)
  assert.equal(summary.weaponSkinEvaluator.missing_liquidity_penalty, 1)
  assert.equal(summary.weaponSkinEvaluator.partial_market_coverage_penalty, 1)
  assert.equal(summary.weaponSkinEvaluator.thin_executable_depth_penalty, 1)
  assert.equal(summary.weaponSkinEvaluator.low_value_contextual_penalty, 1)
  assert.equal(summary.weaponSkinEvaluator.soft_skip_reason.low_value_low_support_weapon_skin, 1)
  assert.equal(summary.weaponSkinEvaluator.hard_reject_reason.buy_and_sell_route_stale, 1)
  assert.equal(summary.weaponSkinEvaluator.publish_preview_result.publishable, 3)
  assert.equal(summary.weaponSkinEvaluator.publish_preview_result.buy_and_sell_route_stale, 1)
  assert.equal(summary.weaponSkinEvaluator.final_tier.strong, 1)
  assert.equal(summary.weaponSkinEvaluator.final_tier.risky, 1)
  assert.equal(summary.weaponSkinEvaluator.final_tier.rejected, 2)
  assert.equal(summary.weaponSkinEvaluator.freshness_contract.missing_buy_route_timestamp, 1)
  assert.equal(summary.weaponSkinEvaluator.freshness_contract.buy_and_sell_route_stale, 1)
  assert.equal(summary.weaponSkinEvaluator.freshness_contract.sell_route_unavailable, 1)
  assert.equal(summary.weaponSkinEvaluator.freshness_contract.missing_listing_availability, 1)
  assert.equal(summary.weaponSkinEvaluator.freshness_contract.freshness_contract_incomplete, 1)
})

test("opportunity evaluation enforces true hard reject reasons", () => {
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Case Hardened Item",
      itemName: "Case Hardened Item",
      category: "weapon_skin",
      referencePrice: 25,
      marketCoverageCount: 2,
      volume7d: 120,
      scanPenaltyFlags: [],
      scanFreshness: { state: "fresh" }
    },
    {
      marketHashName: "Case Hardened Item",
      bestBuy: { source: "steam", grossPrice: 15 },
      bestSellNet: { source: "skinport", netPriceAfterFees: 14 },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 15,
        sellMarket: "skinport",
        sellNet: 14,
        profit: -1,
        spreadPercent: -6.7,
        opportunityScore: 40,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 25,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("non_positive_profit"), true)
})

test("opportunity evaluation rejects opportunities below $2 cost floor", () => {
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Low Cost Case",
      itemName: "Low Cost Case",
      category: "case",
      referencePrice: 1.8,
      marketCoverageCount: 2,
      volume7d: 220,
      scanPenaltyFlags: [],
      scanFreshness: { state: "fresh" }
    },
    {
      marketHashName: "Low Cost Case",
      bestBuy: { source: "steam", grossPrice: 1.7 },
      bestSellNet: { source: "skinport", netPriceAfterFees: 2.4 },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 1.7,
        sellMarket: "skinport",
        sellNet: 2.4,
        profit: 0.7,
        spreadPercent: 41.2,
        opportunityScore: 72,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 1.8,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("below_min_cost_floor"), true)
})

test("non-weapon categories keep the current generic coverage hard reject", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Revolution Case",
      itemName: "Revolution Case",
      category: "case",
      referencePrice: 2.4,
      marketCoverageCount: 1,
      volume7d: 480,
      quoteFetchedAt: nowIso,
      snapshotCapturedAt: nowIso
    },
    {
      marketHashName: "Revolution Case",
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 1.9,
          netPriceAfterFees: 2.35,
          updatedAt: nowIso,
          volume7d: 480
        }
      ],
      bestBuy: { source: "steam", grossPrice: 1.9, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "steam", netPriceAfterFees: 2.35, url: "https://steamcommunity.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 1.9,
        sellMarket: "steam",
        sellNet: 2.35,
        profit: 0.45,
        spreadPercent: 23.68,
        opportunityScore: 69,
        executionConfidence: "Medium",
        marketCoverage: 1,
        referencePrice: 2.4,
        depthFlags: [],
        antiFake: { reasons: ["ignored_missing_markets"] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("unusable_market_coverage"), true)
  assert.equal(evaluation?.metadata?.weapon_skin_evaluator_diagnostics || null, null)
})

test("feed event classifier detects new, updated, duplicate, and reactivated rows", () => {
  const baseOpportunity = {
    itemName: "AK-47 | Redline (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport",
    profit: 12,
    spread: 13,
    score: 85,
    executionConfidence: "High",
    liquidity: 220
  }

  const newEvent = classifyOpportunityFeedEvent(baseOpportunity, null)
  assert.equal(newEvent.eventType, "new")

  const updatedEvent = classifyOpportunityFeedEvent(baseOpportunity, {
    is_active: true,
    profit: 8,
    spread_pct: 8,
    opportunity_score: 66,
    execution_confidence: "Low",
    metadata: { liquidity_value: 100 }
  })
  assert.equal(updatedEvent.eventType, "updated")
  assert.equal(updatedEvent.changeReasons.includes("score"), true)

  const duplicateEvent = classifyOpportunityFeedEvent(baseOpportunity, {
    is_active: true,
    buy_market: "steam",
    sell_market: "skinport",
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  })
  assert.equal(duplicateEvent.eventType, "duplicate")
  assert.equal(isMateriallyNewOpportunity(baseOpportunity, {
    is_active: true,
    buy_market: "steam",
    sell_market: "skinport",
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  }), false)

  const reactivatedEvent = classifyOpportunityFeedEvent(baseOpportunity, {
    is_active: false,
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  })
  assert.equal(reactivatedEvent.eventType, "reactivated")
})

test("feed event classifier marks diagnostics-only flag changes as updated", () => {
  const opportunity = {
    itemName: "AWP | Asiimov (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport",
    profit: 3.2,
    spread: 12.1,
    score: 69,
    executionConfidence: "Medium",
    liquidity: 180,
    flags: ["stale_market_signal"],
    badges: ["Stale market signal"]
  }
  const previousRow = {
    is_active: true,
    profit: 3.2,
    spread_pct: 12.1,
    opportunity_score: 69,
    execution_confidence: "Medium",
    metadata: {
      liquidity_value: 180,
      flags: ["limited_market_coverage"],
      badges: ["Limited market coverage"]
    }
  }
  const event = classifyOpportunityFeedEvent(opportunity, previousRow)
  assert.equal(event.eventType, "updated")
  assert.equal(event.changeReasons.includes("diagnostics"), true)
})

test("feed event classifier treats canonical freshness changes as updates", () => {
  const nowIso = new Date().toISOString()
  const oldIso = new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString()
  const opportunity = {
    itemName: "AWP | Asiimov (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport",
    profit: 3.2,
    spread: 12.1,
    score: 69,
    executionConfidence: "Medium",
    liquidity: 180,
    flags: ["limited_market_coverage"],
    badges: ["Limited market coverage"],
    metadata: {
      latest_market_signal_at: nowIso,
      stale_result: false,
      stale_threshold_used: 120
    }
  }
  const previousRow = {
    is_active: true,
    profit: 3.2,
    spread_pct: 12.1,
    opportunity_score: 69,
    execution_confidence: "Medium",
    metadata: {
      liquidity_value: 180,
      flags: ["limited_market_coverage"],
      badges: ["Limited market coverage"],
      latest_market_signal_at: oldIso,
      stale_result: true,
      stale_threshold_used: 120
    }
  }
  const event = classifyOpportunityFeedEvent(opportunity, previousRow)
  assert.equal(event.eventType, "updated")
  assert.equal(event.changeReasons.includes("freshness"), true)
})

test("opportunity fingerprint changes when listing identity changes", () => {
  const base = {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 12.1,
    sellMarket: "skinport",
    sellNet: 13.5,
    metadata: {
      skinport_listing_id: "sp-001",
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested"
    }
  }
  const changedListing = {
    ...base,
    metadata: {
      ...base.metadata,
      skinport_listing_id: "sp-002"
    }
  }

  const fpA = buildOpportunityFingerprint(base)
  const fpB = buildOpportunityFingerprint(changedListing)
  assert.equal(Boolean(fpA), true)
  assert.equal(Boolean(fpB), true)
  assert.notEqual(fpA, fpB)
})

test("material hash captures meaningful path/quality changes", () => {
  const a = buildMaterialChangeHash({
    buyMarket: "steam",
    sellMarket: "skinport",
    buyPrice: 10,
    sellNet: 12,
    profit: 2,
    spread: 20,
    qualityGrade: "RISKY",
    executionConfidence: "Medium",
    verdict: "watch",
    metadata: {
      skinport_listing_id: "sp-1"
    }
  })
  const b = buildMaterialChangeHash({
    buyMarket: "steam",
    sellMarket: "skinport",
    buyPrice: 10.01,
    sellNet: 12.01,
    profit: 2.01,
    spread: 20.05,
    qualityGrade: "RISKY",
    executionConfidence: "Medium",
    verdict: "watch",
    metadata: {
      skinport_listing_id: "sp-1"
    }
  })
  const c = buildMaterialChangeHash({
    buyMarket: "steam",
    sellMarket: "dmarket",
    buyPrice: 10,
    sellNet: 12,
    profit: 2,
    spread: 20,
    qualityGrade: "STRONG",
    executionConfidence: "High",
    verdict: "strong_buy",
    metadata: {
      skinport_listing_id: "sp-1"
    }
  })

  assert.equal(Boolean(a), true)
  assert.equal(Boolean(b), true)
  assert.equal(Boolean(c), true)
  assert.equal(a, b)
  assert.notEqual(a, c)
})

test("feed mapper keeps core scanner fields stable", () => {
  const insertRow = buildFeedInsertRow(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemName: "AK-47 | Redline (Field-Tested)",
      itemCategory: "weapon_skin",
      buyMarket: "steam",
      buyPrice: 10,
      sellMarket: "skinport",
      sellNet: 12.5,
      profit: 2.5,
      spread: 25,
      score: 80,
      executionConfidence: "Medium",
      qualityGrade: "RISKY",
      liquidityBand: "Medium",
      liquidity: 150,
      marketCoverage: 2,
      referencePrice: 11.2,
      flags: ["missing_liquidity"],
      badges: ["Risk-adjusted"],
      latestMarketSignalAt: "2026-03-19T09:58:00.000Z",
      latestQuoteAt: "2026-03-19T09:58:00.000Z",
      latestSnapshotAt: "2026-03-19T09:56:00.000Z",
      latestReferencePriceAt: "2026-03-19T09:58:00.000Z",
      staleThresholdUsed: 120,
      staleResult: false,
      staleReasonSource: "latest_quote:catalog",
      tier: "risky",
      isHighConfidenceEligible: false,
      isRiskyEligible: true
    },
    {
      scanRunId: "123e4567-e89b-12d3-a456-426614174000",
      detectedAt: "2026-03-19T10:00:00.000Z"
    }
  )
  const mapped = mapFeedRowToApiRow({
    id: "feed-row-1",
    ...insertRow
  })

  assert.equal(mapped.itemName, "AK-47 | Redline (Field-Tested)")
  assert.equal(mapped.itemCategory, "weapon_skin")
  assert.equal(mapped.buyMarket, "steam")
  assert.equal(mapped.sellMarket, "skinport")
  assert.equal(mapped.score, 80)
  assert.equal(mapped.qualityScoreDisplay, 80)
  assert.equal(mapped.quality_score_display, 80)
  assert.equal(mapped.isRiskyEligible, true)
  assert.equal(mapped.isHighConfidenceEligible, false)
  assert.equal(mapped.latestMarketSignalAt, "2026-03-19T09:58:00.000Z")
  assert.equal(mapped.latestQuoteAt, "2026-03-19T09:58:00.000Z")
  assert.equal(mapped.latestSnapshotAt, "2026-03-19T09:56:00.000Z")
  assert.equal(mapped.latestReferencePriceAt, "2026-03-19T09:58:00.000Z")
  assert.equal(mapped.staleResult, false)
  assert.equal(mapped.staleReasonSource, "latest_quote:catalog")
  assert.equal(Boolean(insertRow.opportunity_fingerprint), true)
  assert.equal(Boolean(insertRow.material_change_hash), true)
  assert.equal(insertRow.times_seen, 1)
  assert.equal(Boolean(mapped.opportunityFingerprint), true)
  assert.equal(Boolean(mapped.materialChangeHash), true)
  assert.equal(mapped.timesSeen, 1)
})

test("feed mapper reads FEED_CARD alias fields when metadata object is absent", () => {
  const mapped = mapFeedRowToApiRow({
    id: "feed-row-aliased-1",
    item_name: "AK-47 | Redline (Field-Tested)",
    market_hash_name: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    buy_market: "steam",
    buy_price: 10,
    sell_market: "skinport",
    sell_net: 12.5,
    profit: 2.5,
    spread_pct: 25,
    opportunity_score: 80,
    execution_confidence: "Medium",
    quality_grade: "RISKY",
    liquidity_label: "High",
    volume_7d: "4039",
    market_coverage: "4",
    reference_price: "11.2",
    item_id: "91",
    item_subcategory: "rifle",
    item_rarity: "Classified",
    item_rarity_color: "#d32ce6",
    item_image_url: "https://cdn.example.com/redline.png"
  })

  assert.equal(mapped.volume7d, 4039)
  assert.equal(mapped.liquidity, 4039)
  assert.equal(mapped.marketCoverage, 4)
  assert.equal(mapped.referencePrice, 11.2)
  assert.equal(mapped.itemId, "91")
  assert.equal(mapped.itemSubcategory, "rifle")
  assert.equal(mapped.itemCanonicalRarity, "classified")
  assert.equal(mapped.itemRarity, "Classified")
  assert.equal(mapped.itemRarityColor, "#d32ce6")
  assert.equal(mapped.itemImageUrl, "https://cdn.example.com/redline.png")
})

test("feed mapper prefers sell-side liquidity over stale generic volume aliases", () => {
  const mapped = mapFeedRowToApiRow({
    id: "feed-row-sell-liquidity",
    item_name: "AWP | Hyper Beast (Field-Tested)",
    market_hash_name: "AWP | Hyper Beast (Field-Tested)",
    category: "weapon_skin",
    buy_market: "steam",
    buy_price: 18.2,
    sell_market: "skinport",
    sell_net: 21.7,
    profit: 3.5,
    spread_pct: 19.23,
    opportunity_score: 77,
    execution_confidence: "Medium",
    quality_grade: "RISKY",
    liquidity_label: "Medium",
    volume_7d: "0",
    sell_volume_7d: "148",
    buy_volume_7d: "22",
    market_max_volume_7d: "201",
    liquidity_source: "sell_route"
  })

  assert.equal(mapped.volume7d, 148)
  assert.equal(mapped.liquidity, 148)
  assert.equal(mapped.sellVolume7d, 148)
  assert.equal(mapped.buyVolume7d, 22)
  assert.equal(mapped.marketMaxVolume7d, 201)
  assert.equal(mapped.liquiditySource, "sell_route")
})

test("display score soft-normalizes visible feed values without changing raw score ordering inputs", () => {
  const mappedRows = [0, 10, 40, 70, 85, 100].map((score) =>
    mapFeedRowToApiRow({
      opportunity_score: score,
      metadata: {}
    })
  )

  const displayScores = mappedRows.map((row) => Number(row.qualityScoreDisplay))

  assert.equal(displayScores[0] >= 15, true)
  assert.equal(displayScores[displayScores.length - 1] < 100, true)
  assert.equal(displayScores[4], 85)

  for (let index = 1; index < displayScores.length; index += 1) {
    assert.equal(displayScores[index] > displayScores[index - 1], true)
  }
})

test("feed card dedupe keeps newest row per fingerprint and preserves stronger counters", () => {
  const deduped = dedupeFeedCards([
    {
      feedId: "row-new",
      itemName: "AK-47 | Redline (Field-Tested)",
      buyMarket: "steam",
      sellMarket: "skinport",
      opportunityFingerprint: "ofp_same",
      materialChangeHash: "mch_same",
      detectedAt: "2026-03-21T10:00:00.000Z",
      lastSeenAt: "2026-03-21T10:00:00.000Z",
      timesSeen: 2
    },
    {
      feedId: "row-old",
      itemName: "AK-47 | Redline (Field-Tested)",
      buyMarket: "steam",
      sellMarket: "skinport",
      opportunityFingerprint: "ofp_same",
      materialChangeHash: "mch_same",
      detectedAt: "2026-03-21T09:00:00.000Z",
      lastSeenAt: "2026-03-21T09:00:00.000Z",
      timesSeen: 7
    }
  ])

  assert.equal(deduped.length, 1)
  assert.equal(deduped[0].feedId, "row-new")
  assert.equal(deduped[0].timesSeen, 7)
  assert.equal(deduped[0].times_seen, 7)
  assert.equal(deduped[0].lastSeenAt, "2026-03-21T10:00:00.000Z")
})

test("feed card dedupe keeps separate rows when fallback signature material hash differs", () => {
  const deduped = dedupeFeedCards([
    {
      feedId: "row-a",
      itemName: "★ Broken Fang Gloves | Jade (Field-Tested)",
      buyMarket: "csfloat",
      sellMarket: "steam",
      materialChangeHash: "mch_a"
    },
    {
      feedId: "row-b",
      itemName: "★ Broken Fang Gloves | Jade (Field-Tested)",
      buyMarket: "csfloat",
      sellMarket: "steam",
      materialChangeHash: "mch_b"
    }
  ])

  assert.equal(deduped.length, 2)
})

test("persistFeedRows delegates to global feed publisher when v2 flag is enabled", async () => {
  const originalFlag = env.globalFeedV2Enabled
  const originalPublishBatch = globalFeedPublisher.publishBatch
  const opportunity = {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 10,
    sellMarket: "skinport",
    sellNet: 12.5,
    profit: 2.5,
    spread: 25,
    score: 80,
    executionConfidence: "Medium",
    qualityGrade: "RISKY",
    liquidityBand: "Medium",
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: new Date().toISOString(),
      sell_route_updated_at: new Date().toISOString(),
      sell_listing_available: true
    }
  }

  let callPayload = null
  env.globalFeedV2Enabled = true
  globalFeedPublisher.publishBatch = async (payload = {}) => {
    callPayload = payload
    return {
      publishedCount: 1,
      blockedCount: 0,
      updatedCount: 0,
      reactivatedCount: 0,
      unchangedCount: 0,
      activeRowsWritten: 1,
      historyRowsWritten: 1,
      compatibilityRowsWritten: 1,
      validationReasons: {}
    }
  }

  try {
    const result = await persistFeedRows([opportunity], "scan-run-v2")
    assert.equal(Boolean(callPayload), true)
    assert.equal(callPayload.scanRunId, "scan-run-v2")
    assert.equal(callPayload.opportunities.length, 1)
    assert.equal(result.insertedCount, 1)
    assert.equal(result.newCount, 1)
    assert.equal(result.historyRowsWritten, 1)
    assert.equal(result.compatibilityRowsWritten, 1)
  } finally {
    env.globalFeedV2Enabled = originalFlag
    globalFeedPublisher.publishBatch = originalPublishBatch
  }
})

test("persistFeedRows updates active fingerprint match instead of inserting duplicate row", async () => {
  const nowIso = new Date().toISOString()
  const opportunity = {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 10,
    sellMarket: "skinport",
    sellNet: 12.5,
    profit: 2.5,
    spread: 25,
    score: 80,
    executionConfidence: "Medium",
    qualityGrade: "RISKY",
    liquidityBand: "Medium",
    liquidity: 150,
    marketCoverage: 2,
    referencePrice: 11.2,
    buyRouteAvailable: true,
    sellRouteAvailable: true,
    buyRouteUpdatedAt: nowIso,
    sellRouteUpdatedAt: nowIso,
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: nowIso,
      sell_route_updated_at: nowIso,
      skinport_listing_id: "sp-live-1",
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested"
    }
  }
  const fingerprint = buildOpportunityFingerprint(opportunity)
  const materialHash = buildMaterialChangeHash(opportunity)
  const existingRow = {
    id: "feed-row-dup-1",
    item_name: opportunity.itemName,
    category: "weapon_skin",
    buy_market: "steam",
    buy_price: 10,
    sell_market: "skinport",
    sell_net: 12.5,
    profit: 2.5,
    spread_pct: 25,
    opportunity_score: 80,
    execution_confidence: "Medium",
    quality_grade: "RISKY",
    is_active: true,
    opportunity_fingerprint: fingerprint,
    material_change_hash: materialHash,
    times_seen: 2,
    first_seen_at: "2026-03-19T10:00:00.000Z",
    metadata: {
      liquidity_value: 150,
      item_rarity: "Classified",
      item_rarity_color: "#d32ce6",
      skinport_listing_id: "sp-live-1",
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested",
      opportunity_fingerprint: fingerprint,
      material_change_hash: materialHash
    }
  }

  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows
  }

  let updateRowsPayload = null
  let insertRowsPayload = null
  let lifecyclePayload = null

  marketComparisonService.compareItems = async () => ({
    items: [buildEmitComparedItem(opportunity)]
  })
  arbitrageFeedRepo.getRecentRowsByItems = async () => [existingRow]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [existingRow]
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    updateRowsPayload = rows
    return rows.length
  }
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows.map((row, index) => ({ id: row?.id || `new-${index}` }))
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }

  try {
    const result = await persistFeedRows([opportunity], "scan-run-1")
    assert.equal(result.insertedCount, 0)
    assert.equal(result.newCount, 0)
    assert.equal(Number(result.duplicateCount || 0) + Number(result.updatedCount || 0) >= 1, true)
    assert.equal(result.skippedUnchanged, 0)
    assert.equal(Array.isArray(updateRowsPayload), true)
    assert.equal(updateRowsPayload.length, 1)
    assert.equal(insertRowsPayload, null)
    assert.equal(updateRowsPayload[0].id, existingRow.id)
    assert.equal(
      Number(updateRowsPayload[0]?.patch?.times_seen || 0) >= 3,
      true
    )
    assert.equal(Boolean(updateRowsPayload[0]?.patch?.last_seen_at), true)
    assert.equal(
      updateRowsPayload[0]?.patch?.opportunity_fingerprint,
      fingerprint
    )
    assert.equal(updateRowsPayload[0]?.patch?.metadata?.item_canonical_rarity, "classified")
    assert.equal(updateRowsPayload[0]?.patch?.metadata?.item_rarity, "Classified")
    assert.equal(updateRowsPayload[0]?.patch?.metadata?.item_rarity_color, "#d32ce6")
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "published"]
    )
    assert.equal(result.lifecycle.detected_total, 1)
    assert.equal(result.lifecycle.published_total, 1)
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
  }
})

test("persistFeedRows blocks stale candidates at publish time", async () => {
  const staleIso = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const opportunity = {
    marketHashName: "M4A1-S | Basilisk (Field-Tested)",
    itemName: "M4A1-S | Basilisk (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 8.4,
    sellMarket: "skinport",
    sellNet: 9.5,
    profit: 1.1,
    spread: 13.1,
    score: 72,
    executionConfidence: "Medium",
    qualityGrade: "RISKY",
    liquidityBand: "Medium",
    liquidity: 74,
    marketCoverage: 2,
    referencePrice: 8.9,
    buyRouteAvailable: true,
    sellRouteAvailable: true,
    buyRouteUpdatedAt: staleIso,
    sellRouteUpdatedAt: staleIso,
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: staleIso,
      sell_route_updated_at: staleIso,
      sell_url: "https://skinport.com/item/m4a1-s-basilisk-field-tested"
    }
  }

  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows
  }

  let updateRowsPayload = null
  let insertRowsPayload = null
  let lifecyclePayload = null

  marketComparisonService.compareItems = async () => ({
    items: [
      buildEmitComparedItem(opportunity, {
        buyRouteUpdatedAt: staleIso,
        sellRouteUpdatedAt: staleIso
      })
    ]
  })
  arbitrageFeedRepo.getRecentRowsByItems = async () => []
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => []
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    updateRowsPayload = rows
    return rows.length
  }
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows.map((row, index) => ({ id: row?.id || `new-${index}` }))
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }

  try {
    const result = await persistFeedRows([opportunity], "scan-run-stale-block")
    assert.equal(result.insertedCount, 0)
    assert.equal(result.publishValidation.blocked, 1)
    assert.equal(result.publishValidation.reasons.buy_and_sell_route_stale, 1)
    assert.equal(result.publishValidation.freshnessContract.buy_and_sell_route_stale, 1)
    assert.equal(result.emitRevalidation.stale_on_emit_count, 1)
    assert.equal(result.emitRevalidation.blocked_on_emit_by_reason.stale_on_emit, 1)
    assert.equal(result.publishValidation.deactivated, 0)
    assert.equal(updateRowsPayload, null)
    assert.equal(insertRowsPayload, null)
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "blocked_on_emit"]
    )
    assert.equal(result.lifecycle.blocked_on_emit_total, 1)
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
  }
})

test("persistFeedRows blocks missing route and missing listing at publish time", async () => {
  const nowIso = new Date().toISOString()
  const missingRoute = {
    marketHashName: "FAMAS | Djinn (Field-Tested)",
    itemName: "FAMAS | Djinn (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 4.2,
    sellMarket: "skinport",
    sellNet: 4.7,
    profit: 0.5,
    spread: 11.9,
    score: 61,
    executionConfidence: "Medium",
    qualityGrade: "RISKY",
    liquidityBand: "Medium",
    liquidity: 55,
    marketCoverage: 2,
    referencePrice: 4.4,
    buyRouteAvailable: true,
    sellRouteAvailable: false,
    buyRouteUpdatedAt: nowIso,
    sellRouteUpdatedAt: null,
    metadata: {
      buy_route_available: true,
      sell_route_available: false,
      buy_route_updated_at: nowIso,
      sell_route_updated_at: null
    }
  }
  const missingListing = {
    marketHashName: "USP-S | Orion (Field-Tested)",
    itemName: "USP-S | Orion (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 21,
    sellMarket: "skinport",
    sellNet: 23.5,
    profit: 2.5,
    spread: 11.9,
    score: 78,
    executionConfidence: "Medium",
    qualityGrade: "RISKY",
    liquidityBand: "Medium",
    liquidity: 52,
    marketCoverage: 2,
    referencePrice: 22.4,
    buyRouteAvailable: true,
    sellRouteAvailable: true,
    buyRouteUpdatedAt: nowIso,
    sellRouteUpdatedAt: nowIso,
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: nowIso,
      sell_route_updated_at: nowIso,
      sell_listing_available: false
    }
  }

  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows
  }

  let insertRowsPayload = null
  let lifecyclePayload = null

  marketComparisonService.compareItems = async (rows = []) => ({
    items: rows.map((row) => {
      if (row.marketHashName === missingRoute.marketHashName) {
        return buildEmitComparedItem(missingRoute, {
          sellRouteAvailable: false,
          sellRouteUpdatedAt: null,
          sellListingAvailable: false,
          sellListingId: null
        })
      }
      return buildEmitComparedItem(missingListing, {
        sellListingAvailable: false,
        sellListingId: null
      })
    })
  })
  arbitrageFeedRepo.getRecentRowsByItems = async () => []
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => []
  arbitrageFeedRepo.updateRowsById = async () => 0
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows.map((row, index) => ({ id: row?.id || `new-${index}` }))
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }

  try {
    const result = await persistFeedRows(
      [missingRoute, missingListing],
      "scan-run-route-listing-block"
    )
    assert.equal(result.insertedCount, 0)
    assert.equal(result.publishValidation.blocked, 2)
    assert.equal(result.publishValidation.reasons.missing_sell_route, 1)
    assert.equal(result.publishValidation.reasons.missing_sell_listing, 1)
    assert.equal(result.publishValidation.freshnessContract.sell_route_unavailable, 1)
    assert.equal(result.emitRevalidation.unavailable_on_emit_count, 2)
    assert.equal(result.emitRevalidation.blocked_on_emit_by_reason.unavailable_on_emit, 2)
    assert.equal(insertRowsPayload, null)
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "blocked_on_emit", "detected", "blocked_on_emit"]
    )
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
  }
})

test("persistFeedRows blocks non-executable rows at emit time", async () => {
  const nowIso = new Date().toISOString()
  const opportunity = {
    marketHashName: "Galil AR | Stone Cold (Field-Tested)",
    itemName: "Galil AR | Stone Cold (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 6.2,
    sellMarket: "skinport",
    sellNet: 7.1,
    profit: 0.9,
    spread: 14.5,
    score: 68,
    executionConfidence: "Medium",
    qualityGrade: "RISKY",
    liquidityBand: "Medium",
    liquidity: 70,
    marketCoverage: 2,
    referencePrice: 6.6,
    buyRouteAvailable: true,
    sellRouteAvailable: true,
    buyRouteUpdatedAt: nowIso,
    sellRouteUpdatedAt: nowIso,
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: nowIso,
      sell_route_updated_at: nowIso,
      sell_listing_available: true,
      skinport_listing_id: "sp-galil"
    }
  }

  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows
  }

  let insertRowsPayload = null
  let lifecyclePayload = null

  marketComparisonService.compareItems = async () => ({
    items: [
      buildEmitComparedItem(opportunity, {
        buyGrossPrice: 7.5,
        sellGrossPrice: 7.1,
        sellNetPrice: 6.9
      })
    ]
  })
  arbitrageFeedRepo.getRecentRowsByItems = async () => []
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => []
  arbitrageFeedRepo.updateRowsById = async () => 0
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }

  try {
    const result = await persistFeedRows([opportunity], "scan-run-non-executable")
    assert.equal(result.insertedCount, 0)
    assert.equal(result.publishValidation.blocked, 1)
    assert.equal(result.publishValidation.reasons.non_positive_profit, 1)
    assert.equal(result.emitRevalidation.non_executable_on_emit_count, 1)
    assert.equal(result.emitRevalidation.blocked_on_emit_by_reason.non_executable_on_emit, 1)
    assert.equal(insertRowsPayload, null)
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "blocked_on_emit"]
    )
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
  }
})

test("persistFeedRows deactivates older active duplicate rows for same fingerprint before update", async () => {
  const nowIso = new Date().toISOString()
  const opportunity = {
    marketHashName: "★ Broken Fang Gloves | Jade (Field-Tested)",
    itemName: "★ Broken Fang Gloves | Jade (Field-Tested)",
    itemCategory: "glove",
    buyMarket: "csfloat",
    buyPrice: 97.67,
    sellMarket: "steam",
    sellNet: 109.39,
    profit: 11.72,
    spread: 11.99,
    score: 43,
    executionConfidence: "Low",
    qualityGrade: "RISKY",
    liquidityBand: "High",
    liquidity: 500,
    marketCoverage: 2,
    referencePrice: 103.2,
    buyRouteAvailable: true,
    sellRouteAvailable: true,
    buyRouteUpdatedAt: nowIso,
    sellRouteUpdatedAt: nowIso,
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: nowIso,
      sell_route_updated_at: nowIso,
      buy_listing_available: true,
      sell_listing_available: true,
      buy_url: "https://csfloat.com/search?market_hash_name=Broken+Fang+Gloves+Jade",
      sell_url:
        "https://steamcommunity.com/market/listings/730/%E2%98%85%20Broken%20Fang%20Gloves%20%7C%20Jade%20(Field-Tested)"
    }
  }
  const fingerprint = buildOpportunityFingerprint(opportunity)
  const materialHash = buildMaterialChangeHash(opportunity)
  const existingNewest = {
    id: "feed-row-newest",
    item_name: opportunity.itemName,
    category: "glove",
    buy_market: "csfloat",
    buy_price: 97.67,
    sell_market: "steam",
    sell_net: 109.39,
    profit: 11.72,
    spread_pct: 11.99,
    opportunity_score: 43,
    execution_confidence: "Low",
    quality_grade: "RISKY",
    is_active: true,
    opportunity_fingerprint: fingerprint,
    material_change_hash: materialHash,
    times_seen: 3,
    detected_at: "2026-03-21T10:00:00.000Z",
    last_seen_at: "2026-03-21T10:00:00.000Z",
    first_seen_at: "2026-03-20T10:00:00.000Z",
    metadata: {
      opportunity_fingerprint: fingerprint,
      material_change_hash: materialHash
    }
  }
  const existingOlder = {
    ...existingNewest,
    id: "feed-row-older",
    times_seen: 1,
    detected_at: "2026-03-21T09:00:00.000Z",
    last_seen_at: "2026-03-21T09:00:00.000Z"
  }

  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    markRowsInactiveByIds: arbitrageFeedRepo.markRowsInactiveByIds,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows
  }

  let markedInactiveIds = null
  let updateRowsPayload = null
  let insertRowsPayload = null
  let lifecyclePayload = null

  marketComparisonService.compareItems = async () => ({
    items: [buildEmitComparedItem(opportunity)]
  })
  arbitrageFeedRepo.getRecentRowsByItems = async () => [existingNewest, existingOlder]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [existingNewest, existingOlder]
  arbitrageFeedRepo.markRowsInactiveByIds = async (ids = []) => {
    markedInactiveIds = ids
    return ids.length
  }
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    updateRowsPayload = rows
    return rows.length
  }
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows.map((row, index) => ({ id: row?.id || `new-${index}` }))
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }

  try {
    const result = await persistFeedRows([opportunity], "scan-run-dup-cleanup")
    assert.deepEqual(markedInactiveIds, ["feed-row-older"])
    assert.equal(result.cleanup.duplicateActivesMarkedInactive, 1)
    assert.equal(Array.isArray(updateRowsPayload), true)
    assert.equal(updateRowsPayload.length, 1)
    assert.equal(updateRowsPayload[0].id, "feed-row-newest")
    assert.equal(insertRowsPayload, null)
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "published"]
    )
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.markRowsInactiveByIds = originals.markRowsInactiveByIds
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
  }
})

test("overdue watchdog tolerates active running scanner and flags stale completion", () => {
  const now = Date.parse("2026-03-19T12:00:00.000Z")
  assert.equal(
    isScannerRunOverdue(
      {
        latestRun: { status: "running", started_at: "2026-03-19T11:59:00.000Z" },
        latestCompletedRun: { completed_at: "2026-03-19T11:55:00.000Z" }
      },
      now
    ),
    false
  )
  assert.equal(
    isScannerRunOverdue(
      {
        latestRun: { status: "completed", started_at: "2026-03-19T11:30:00.000Z" },
        latestCompletedRun: { completed_at: "2026-03-19T11:30:00.000Z" }
      },
      now
    ),
    true
  )
})

test("scanner defaults are tuned for batch-first throughput", () => {
  assert.equal(DEFAULT_UNIVERSE_LIMIT >= 500, true)
  assert.equal(OPPORTUNITY_BATCH_TARGET >= 20, true)
  assert.equal(SCAN_CHUNK_SIZE >= 10, true)
  assert.equal(SCAN_TIMEOUT_PER_BATCH_MS >= 1000, true)
})

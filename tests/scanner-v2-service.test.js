const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const catalogGenerationRepo = require("../src/repositories/catalogGenerationRepository")
const globalActiveOpportunityRepo = require("../src/repositories/globalActiveOpportunityRepository")
const scannerRunRepo = require("../src/repositories/scannerRunRepository")
const planService = require("../src/services/planService")
const feedRevalidationService = require("../src/services/feed/feedRevalidationService")
const legacyScannerService = require("../src/services/arbitrageScannerService")
const scannerV2Service = require("../src/services/scannerV2Service")

test("scannerV2Service.getFeed reads active opportunities and preserves premium locks", async () => {
  const originals = {
    getCurrentGeneration: catalogGenerationRepo.getCurrentGeneration,
    listFeedByCursor: globalActiveOpportunityRepo.listFeedByCursor,
    countFeed: globalActiveOpportunityRepo.countFeed,
    getLatestRun: scannerRunRepo.getLatestRun,
    getLatestCompletedRun: scannerRunRepo.getLatestCompletedRun,
    canUseAdvancedFilters: planService.canUseAdvancedFilters,
    canAccessKnivesAndGloves: planService.canAccessKnivesAndGloves
  }

  let feedQuery = null

  catalogGenerationRepo.getCurrentGeneration = async () => ({
    id: "generation-active",
    generation_key: "catalog-reset-active",
    status: "active",
    is_active: true,
    opportunity_scan_enabled: true,
    activated_at: "2026-03-27T11:00:00.000Z"
  })
  globalActiveOpportunityRepo.listFeedByCursor = async (query = {}) => {
    feedQuery = query
    return [
      {
        id: "active-knife-1",
        item_name: "Karambit | Doppler (Factory New)",
        market_hash_name: "Karambit | Doppler (Factory New)",
        category: "knife",
        buy_market: "steam",
        buy_price: 1000,
        sell_market: "skinport",
        sell_net: 1125,
        profit: 125,
        spread_pct: 12.5,
        opportunity_score: 88,
        execution_confidence: "High",
        quality_grade: "STRONG",
        liquidity_label: "Medium",
        first_seen_at: "2026-03-27T11:50:00.000Z",
        last_seen_at: "2026-03-27T12:00:00.000Z",
        last_published_at: "2026-03-27T12:00:00.000Z",
        live_status: "live",
        refresh_status: "pending",
        metadata: {
          volume_7d: 8,
          market_coverage: 2
        }
      }
    ]
  }
  globalActiveOpportunityRepo.countFeed = async () => 1
  scannerRunRepo.getLatestRun = async () => null
  scannerRunRepo.getLatestCompletedRun = async (scannerType) =>
    scannerType === "opportunity_scan"
      ? {
          id: "run-1",
          status: "completed",
          started_at: "2026-03-27T11:55:00.000Z",
          completed_at: "2026-03-27T12:00:00.000Z",
          items_scanned: 42
        }
      : null
  planService.canUseAdvancedFilters = () => true
  planService.canAccessKnivesAndGloves = () => false

  try {
    const result = await scannerV2Service.getFeed({
      category: "knife",
      includeCount: true,
      entitlements: {
        planTier: "free",
        advancedFilters: true,
        visibleFeedLimit: 200,
        delayedSignals: false,
        signalDelayMinutes: 0
      }
    })

    assert.equal(feedQuery.category, "knife")
    assert.equal(result.pagination.totalCount, 1)
    assert.equal(result.summary.plan.lockedPremiumPreviewRows, 1)
    assert.equal(result.status.activeOpportunities, 1)
    assert.equal(result.opportunities.length, 1)
    assert.equal(result.opportunities[0].feedId, "active-knife-1")
    assert.equal(result.opportunities[0].isLockedPreview, true)
    assert.equal(result.opportunities[0].lockReason, "premium_category")
    assert.equal(result.opportunities[0].previewBuyPrice, 1000)
    assert.equal(result.opportunities[0].buyMarket, null)
  } finally {
    catalogGenerationRepo.getCurrentGeneration = originals.getCurrentGeneration
    globalActiveOpportunityRepo.listFeedByCursor = originals.listFeedByCursor
    globalActiveOpportunityRepo.countFeed = originals.countFeed
    scannerRunRepo.getLatestRun = originals.getLatestRun
    scannerRunRepo.getLatestCompletedRun = originals.getLatestCompletedRun
    planService.canUseAdvancedFilters = originals.canUseAdvancedFilters
    planService.canAccessKnivesAndGloves = originals.canAccessKnivesAndGloves
  }
})

test("scannerV2Service.startScheduler owns v2 timers and feed revalidation", async () => {
  const originals = {
    getCurrentGeneration: catalogGenerationRepo.getCurrentGeneration,
    startScheduler: feedRevalidationService.startScheduler,
    stopScheduler: feedRevalidationService.stopScheduler,
    runtime: legacyScannerService.__runtime
  }

  let scanEnqueueCalls = 0
  let enrichmentEnqueueCalls = 0
  let revalidationStarts = 0

  feedRevalidationService.startScheduler = () => {
    revalidationStarts += 1
  }
  feedRevalidationService.stopScheduler = () => null
  catalogGenerationRepo.getCurrentGeneration = async () => ({
    id: "generation-active",
    generation_key: "catalog-reset-active",
    status: "active",
    is_active: true,
    opportunity_scan_enabled: true,
    activated_at: "2026-03-27T11:00:00.000Z"
  })
  legacyScannerService.__runtime = {
    enqueueScan: async () => {
      scanEnqueueCalls += 1
      return { scanRunId: "scan-run-1", alreadyRunning: false }
    },
    enqueueEnrichment: async () => {
      enrichmentEnqueueCalls += 1
      return { scanRunId: "enrichment-run-1", alreadyRunning: false }
    }
  }

  try {
    const result = scannerV2Service.startScheduler()
    await new Promise((resolve) => setTimeout(resolve, 0))

    assert.equal(result.engine, "scanner_v2")
    assert.equal(result.feedRevalidationStarted, true)
    assert.equal(revalidationStarts, 1)
    assert.equal(scanEnqueueCalls, 1)
    assert.equal(enrichmentEnqueueCalls, 1)
  } finally {
    scannerV2Service.stopScheduler()
    catalogGenerationRepo.getCurrentGeneration = originals.getCurrentGeneration
    feedRevalidationService.startScheduler = originals.startScheduler
    feedRevalidationService.stopScheduler = originals.stopScheduler
    legacyScannerService.__runtime = originals.runtime
  }
})

test("scannerV2Service blocks manual opportunity scans while the active generation is enrichment-only", async () => {
  const originals = {
    getCurrentGeneration: catalogGenerationRepo.getCurrentGeneration,
    runtime: legacyScannerService.__runtime
  }

  let scanEnqueueCalls = 0

  catalogGenerationRepo.getCurrentGeneration = async () => ({
    id: "generation-rebuild",
    generation_key: "catalog-reset-rebuild",
    status: "active",
    is_active: true,
    opportunity_scan_enabled: false,
    activated_at: "2026-03-27T11:00:00.000Z"
  })
  legacyScannerService.__runtime = {
    enqueueScan: async () => {
      scanEnqueueCalls += 1
      return { scanRunId: "scan-run-1", alreadyRunning: false }
    },
    enqueueEnrichment: async () => ({ scanRunId: "enrichment-run-1", alreadyRunning: false })
  }

  try {
    const result = await scannerV2Service.triggerRefresh({
      jobType: "opportunity_scan",
      entitlements: {
        planTier: "pro",
        advancedFilters: true,
        visibleFeedLimit: 200,
        delayedSignals: false,
        signalDelayMinutes: 0,
        manualRefreshCooldownMs: 0
      }
    })

    const opportunityJob = result.jobs?.opportunity_scan
    assert.equal(scanEnqueueCalls, 0)
    assert.equal(opportunityJob?.status, "blocked_generation_not_ready")
    assert.equal(opportunityJob?.catalogGeneration?.id, "generation-rebuild")
  } finally {
    catalogGenerationRepo.getCurrentGeneration = originals.getCurrentGeneration
    legacyScannerService.__runtime = originals.runtime
  }
})

const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const globalActiveOpportunityRepo = require("../src/repositories/globalActiveOpportunityRepository")
const globalOpportunityHistoryRepo = require("../src/repositories/globalOpportunityHistoryRepository")
const globalOpportunityLifecycleLogRepo = require("../src/repositories/globalOpportunityLifecycleLogRepository")
const diagnosticsWriter = require("../src/services/diagnosticsWriter")
const marketComparisonService = require("../src/services/marketComparisonService")
const globalFeedPublisher = require("../src/services/feed/globalFeedPublisher")

function buildOpportunity(overrides = {}) {
  const nowIso = new Date().toISOString()
  return {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 10,
    sellMarket: "skinport",
    sellNet: 12.5,
    profit: 2.5,
    spread: 25,
    score: 76,
    executionConfidence: "Medium",
    qualityGrade: "NEAR_ELIGIBLE",
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
      sell_listing_available: true,
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested",
      skinport_listing_id: "sp-live-1"
    },
    ...overrides
  }
}

function buildComparedItem(opportunity = {}, overrides = {}) {
  const metadata = opportunity?.metadata || {}
  return {
    marketHashName: opportunity.marketHashName || opportunity.itemName,
    itemCategory: opportunity.itemCategory || "weapon_skin",
    referencePrice: opportunity.referencePrice || 11.2,
    volume7d: opportunity.liquidity || 150,
    perMarket: [
      {
        source: overrides.buyMarket || opportunity.buyMarket || "steam",
        available:
          overrides.buyRouteAvailable ?? opportunity.buyRouteAvailable ?? metadata.buy_route_available ?? true,
        grossPrice: overrides.buyGrossPrice ?? opportunity.buyPrice ?? 10,
        updatedAt:
          overrides.buyRouteUpdatedAt ??
          opportunity.buyRouteUpdatedAt ??
          metadata.buy_route_updated_at ??
          null,
        raw: {
          listing_available: overrides.buyListingAvailable ?? true
        }
      },
      {
        source: overrides.sellMarket || opportunity.sellMarket || "skinport",
        available:
          overrides.sellRouteAvailable ??
          opportunity.sellRouteAvailable ??
          metadata.sell_route_available ??
          true,
        grossPrice: overrides.sellGrossPrice ?? opportunity.sellNet ?? 12.5,
        netPriceAfterFees: overrides.sellNetPrice ?? opportunity.sellNet ?? 12.5,
        updatedAt:
          overrides.sellRouteUpdatedAt ??
          opportunity.sellRouteUpdatedAt ??
          metadata.sell_route_updated_at ??
          null,
        raw: {
          listing_available:
            overrides.sellListingAvailable ??
            metadata.sell_listing_available ??
            true,
          listing_id:
            overrides.sellListingId ??
            metadata.skinport_listing_id ??
            "sp-live-1"
        }
      }
    ]
  }
}

test("global feed publisher writes active state, history, lifecycle, and scanner_v2 metrics", async () => {
  const fixedNowIso = "2026-03-25T12:00:00.000Z"
  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: globalActiveOpportunityRepo.getRecentRowsByItems,
    getRowsByFingerprints: globalActiveOpportunityRepo.getRowsByFingerprints,
    upsertRows: globalActiveOpportunityRepo.upsertRows,
    updateRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows,
    writePublishBatch: diagnosticsWriter.writePublishBatch,
    writePublishDecisions: diagnosticsWriter.writePublishDecisions
  }

  let activeInsertPayload = null
  let historyPayload = null
  let lifecyclePayload = null
  let persistedActiveRows = []

  marketComparisonService.compareItems = async () => ({
    items: [buildComparedItem(buildOpportunity({ buyRouteUpdatedAt: fixedNowIso, sellRouteUpdatedAt: fixedNowIso }))]
  })
  globalActiveOpportunityRepo.getRecentRowsByItems = async () => []
  globalActiveOpportunityRepo.getRowsByFingerprints = async ({ fingerprints = [] } = {}) =>
    persistedActiveRows.filter((row) => fingerprints.includes(row.opportunity_fingerprint))
  globalActiveOpportunityRepo.upsertRows = async (rows = []) => {
    activeInsertPayload = rows
    persistedActiveRows = rows.map((row, index) => ({
      id: row.id || `active-${index}`,
      ...row
    }))
    return persistedActiveRows.map((row) => ({
      id: row.id,
      opportunity_fingerprint: row.opportunity_fingerprint
    }))
  }
  globalActiveOpportunityRepo.updateRowsById = async () => 0
  globalOpportunityHistoryRepo.insertRows = async (rows = []) => {
    historyPayload = rows
    return rows
  }
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }
  diagnosticsWriter.writePublishBatch = async () => null
  diagnosticsWriter.writePublishDecisions = async () => null

  try {
    const result = await globalFeedPublisher.publishBatch({
      scanRunId: "scan-run-1",
      opportunities: [buildOpportunity({ buyRouteUpdatedAt: fixedNowIso, sellRouteUpdatedAt: fixedNowIso })],
      nowIso: fixedNowIso,
      scannedCount: 1
    })

    assert.equal(result.blockedCount, 0)
    assert.equal(result.publishedCount, 1)
    assert.equal(result.activeRowsWritten, 1)
    assert.equal(Array.isArray(activeInsertPayload), true)
    assert.equal(activeInsertPayload[0].live_status, "live")
    assert.equal(activeInsertPayload[0].refresh_status, "pending")
    assert.equal(activeInsertPayload[0].quality_grade, "NEAR_ELIGIBLE")
    assert.equal(Array.isArray(historyPayload), true)
    assert.equal(historyPayload[0].event_type, "new")
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "published"]
    )
    assert.equal(result.publisherMetrics.engine, "scanner_v2")
    assert.equal(result.publisherMetrics.eligibleCount, 1)
    assert.equal(result.publisherMetrics.emittedCount, 1)
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    globalActiveOpportunityRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    globalActiveOpportunityRepo.getRowsByFingerprints = originals.getRowsByFingerprints
    globalActiveOpportunityRepo.upsertRows = originals.upsertRows
    globalActiveOpportunityRepo.updateRowsById = originals.updateRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
    diagnosticsWriter.writePublishBatch = originals.writePublishBatch
    diagnosticsWriter.writePublishDecisions = originals.writePublishDecisions
  }
})

test("global feed publisher expires previously live rows when emit freshness fails", async () => {
  const staleIso = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const previousActive = {
    id: "active-1",
    opportunity_fingerprint: "ofp-existing",
    material_change_hash: "mch-existing",
    item_name: "AK-47 | Redline (Field-Tested)",
    market_hash_name: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    buy_market: "steam",
    sell_market: "skinport",
    buy_price: 10,
    sell_net: 12.5,
    profit: 2.5,
    spread_pct: 25,
    opportunity_score: 80,
    execution_confidence: "Medium",
    quality_grade: "NEAR_ELIGIBLE",
    live_status: "live",
    refresh_status: "pending",
    metadata: {}
  }

  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: globalActiveOpportunityRepo.getRecentRowsByItems,
    getRowsByFingerprints: globalActiveOpportunityRepo.getRowsByFingerprints,
    upsertRows: globalActiveOpportunityRepo.upsertRows,
    updateRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows,
    writePublishBatch: diagnosticsWriter.writePublishBatch,
    writePublishDecisions: diagnosticsWriter.writePublishDecisions
  }

  let activeUpdatePayload = null
  let historyPayload = null
  let lifecyclePayload = null
  let persistedActiveRows = [previousActive]

  marketComparisonService.compareItems = async () => ({
    items: [
      buildComparedItem(buildOpportunity({ buyRouteUpdatedAt: staleIso, sellRouteUpdatedAt: staleIso }), {
        buyRouteUpdatedAt: staleIso,
        sellRouteUpdatedAt: staleIso
      })
    ]
  })
  globalActiveOpportunityRepo.getRecentRowsByItems = async () => [previousActive]
  globalActiveOpportunityRepo.getRowsByFingerprints = async ({ fingerprints = [] } = {}) =>
    persistedActiveRows.filter((row) => fingerprints.includes(row.opportunity_fingerprint))
  globalActiveOpportunityRepo.upsertRows = async () => []
  globalActiveOpportunityRepo.updateRowsById = async (rows = []) => {
    activeUpdatePayload = rows
    persistedActiveRows = persistedActiveRows.map((row) => {
      const update = rows.find((entry) => entry.id === row.id)
      return update ? { ...row, ...update.patch } : row
    })
    return rows.length
  }
  globalOpportunityHistoryRepo.insertRows = async (rows = []) => {
    historyPayload = rows
    return rows
  }
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }
  diagnosticsWriter.writePublishBatch = async () => null
  diagnosticsWriter.writePublishDecisions = async () => null

  try {
    const result = await globalFeedPublisher.publishBatch({
      scanRunId: "scan-run-stale",
      opportunities: [buildOpportunity({ buyRouteUpdatedAt: staleIso, sellRouteUpdatedAt: staleIso })]
    })

    assert.equal(result.blockedCount, 1)
    assert.equal(result.emitRevalidation.stale_on_emit_count, 1)
    assert.equal(Array.isArray(activeUpdatePayload), true)
    assert.equal(activeUpdatePayload[0].patch.live_status, "stale")
    assert.equal(activeUpdatePayload[0].patch.refresh_status, "stale")
    assert.equal(Array.isArray(historyPayload), true)
    assert.equal(historyPayload[0].event_type, "expired")
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "blocked_on_emit", "expired"]
    )
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    globalActiveOpportunityRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    globalActiveOpportunityRepo.getRowsByFingerprints = originals.getRowsByFingerprints
    globalActiveOpportunityRepo.upsertRows = originals.upsertRows
    globalActiveOpportunityRepo.updateRowsById = originals.updateRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
    diagnosticsWriter.writePublishBatch = originals.writePublishBatch
    diagnosticsWriter.writePublishDecisions = originals.writePublishDecisions
  }
})

test("global feed publisher blocks non-executable opportunities without mutating active state", async () => {
  const originals = {
    compareItems: marketComparisonService.compareItems,
    getRecentRowsByItems: globalActiveOpportunityRepo.getRecentRowsByItems,
    getRowsByFingerprints: globalActiveOpportunityRepo.getRowsByFingerprints,
    upsertRows: globalActiveOpportunityRepo.upsertRows,
    updateRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    insertLifecycleRows: globalOpportunityLifecycleLogRepo.insertRows,
    writePublishBatch: diagnosticsWriter.writePublishBatch,
    writePublishDecisions: diagnosticsWriter.writePublishDecisions
  }

  let lifecyclePayload = null

  marketComparisonService.compareItems = async () => ({
    items: [
      buildComparedItem(buildOpportunity(), {
        buyGrossPrice: 13,
        sellGrossPrice: 12,
        sellNetPrice: 11.5
      })
    ]
  })
  globalActiveOpportunityRepo.getRecentRowsByItems = async () => []
  globalActiveOpportunityRepo.getRowsByFingerprints = async () => []
  globalActiveOpportunityRepo.upsertRows = async () => {
    throw new Error("should_not_write_active_rows_when_emit_is_blocked")
  }
  globalActiveOpportunityRepo.updateRowsById = async () => {
    throw new Error("should_not_update_active_rows_when_emit_is_blocked")
  }
  globalOpportunityHistoryRepo.insertRows = async () => []
  globalOpportunityLifecycleLogRepo.insertRows = async (rows = []) => {
    lifecyclePayload = rows
    return rows
  }
  diagnosticsWriter.writePublishBatch = async () => null
  diagnosticsWriter.writePublishDecisions = async () => null

  try {
    const result = await globalFeedPublisher.publishBatch({
      scanRunId: "scan-run-non-executable",
      opportunities: [buildOpportunity()],
      scannedCount: 1
    })

    assert.equal(result.publishedCount, 0)
    assert.equal(result.blockedCount, 1)
    assert.equal(result.emitRevalidation.non_executable_on_emit_count, 1)
    assert.equal(Array.isArray(lifecyclePayload), true)
    assert.deepEqual(
      lifecyclePayload.map((row) => row.lifecycle_status),
      ["detected", "blocked_on_emit"]
    )
    assert.equal(result.publisherMetrics.blockedOnEmitCount, 1)
    assert.equal(result.publisherMetrics.emittedCount, 0)
  } finally {
    marketComparisonService.compareItems = originals.compareItems
    globalActiveOpportunityRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    globalActiveOpportunityRepo.getRowsByFingerprints = originals.getRowsByFingerprints
    globalActiveOpportunityRepo.upsertRows = originals.upsertRows
    globalActiveOpportunityRepo.updateRowsById = originals.updateRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    globalOpportunityLifecycleLogRepo.insertRows = originals.insertLifecycleRows
    diagnosticsWriter.writePublishBatch = originals.writePublishBatch
    diagnosticsWriter.writePublishDecisions = originals.writePublishDecisions
  }
})

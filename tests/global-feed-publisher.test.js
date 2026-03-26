const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")
const globalActiveOpportunityRepo = require("../src/repositories/globalActiveOpportunityRepository")
const globalOpportunityHistoryRepo = require("../src/repositories/globalOpportunityHistoryRepository")
const diagnosticsWriter = require("../src/services/diagnosticsWriter")
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
      sell_listing_available: true,
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested",
      skinport_listing_id: "sp-live-1"
    },
    ...overrides
  }
}

test("global feed publisher writes active state, history, and legacy projection for new rows", async () => {
  const originals = {
    getRecentRowsByItems: globalActiveOpportunityRepo.getRecentRowsByItems,
    getRowsByFingerprints: globalActiveOpportunityRepo.getRowsByFingerprints,
    upsertRows: globalActiveOpportunityRepo.upsertRows,
    updateRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    legacyGetRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    legacyGetActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    legacyInsertRows: arbitrageFeedRepo.insertRows,
    legacyUpdateRowsById: arbitrageFeedRepo.updateRowsById,
    markRowsInactiveByIds: arbitrageFeedRepo.markRowsInactiveByIds,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    writePublishBatch: diagnosticsWriter.writePublishBatch,
    writePublishDecisions: diagnosticsWriter.writePublishDecisions
  }

  let activeInsertPayload = null
  let historyPayload = null
  let legacyInsertPayload = null
  let persistedActiveRows = []

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
    return rows.map((row, index) => ({ id: `history-${index}`, source_event_key: row.source_event_key }))
  }
  arbitrageFeedRepo.getRecentRowsByItems = async () => []
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => []
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    legacyInsertPayload = rows
    return rows.map((row, index) => ({ id: `legacy-${index}` }))
  }
  arbitrageFeedRepo.updateRowsById = async () => 0
  arbitrageFeedRepo.markRowsInactiveByIds = async () => 0
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  diagnosticsWriter.writePublishBatch = async () => null
  diagnosticsWriter.writePublishDecisions = async () => null

  try {
    const opportunity = buildOpportunity()
    const result = await globalFeedPublisher.publishBatch({
      scanRunId: "scan-run-1",
      opportunities: [opportunity],
      nowIso: "2026-03-25T12:00:00.000Z"
    })

    assert.equal(result.blockedCount, 0)
    assert.equal(result.publishedCount, 1)
    assert.equal(Array.isArray(activeInsertPayload), true)
    assert.equal(activeInsertPayload.length, 1)
    assert.equal(activeInsertPayload[0].live_status, "live")
    assert.equal(activeInsertPayload[0].refresh_status, "pending")
    assert.equal(
      activeInsertPayload[0]?.metadata?.route_freshness_contract?.sellRouteUpdatedAt,
      opportunity.sellRouteUpdatedAt
    )
    assert.equal(
      activeInsertPayload[0]?.metadata?.freshness_contract_diagnostics?.buy_and_sell_route_stale,
      false
    )
    assert.equal(Array.isArray(historyPayload), true)
    assert.equal(historyPayload[0].event_type, "new")
    assert.equal(historyPayload[0].active_opportunity_id, "active-0")
    assert.equal(historyPayload[0].live_status, "live")
    assert.equal(Array.isArray(legacyInsertPayload), true)
    assert.equal(legacyInsertPayload.length, 1)
    assert.equal(legacyInsertPayload[0].is_active, true)
    assert.equal(legacyInsertPayload[0].refresh_status, "pending")
    assert.equal(legacyInsertPayload[0].live_status, "live")
  } finally {
    globalActiveOpportunityRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    globalActiveOpportunityRepo.getRowsByFingerprints = originals.getRowsByFingerprints
    globalActiveOpportunityRepo.upsertRows = originals.upsertRows
    globalActiveOpportunityRepo.updateRowsById = originals.updateRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    arbitrageFeedRepo.getRecentRowsByItems = originals.legacyGetRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.legacyGetActiveRowsByFingerprints
    arbitrageFeedRepo.insertRows = originals.legacyInsertRows
    arbitrageFeedRepo.updateRowsById = originals.legacyUpdateRowsById
    arbitrageFeedRepo.markRowsInactiveByIds = originals.markRowsInactiveByIds
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    diagnosticsWriter.writePublishBatch = originals.writePublishBatch
    diagnosticsWriter.writePublishDecisions = originals.writePublishDecisions
  }
})

test("global feed publisher expires previously live rows when publish validation fails", async () => {
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
    quality_grade: "RISKY",
    liquidity_label: "Medium",
    live_status: "live",
    refresh_status: "pending",
    metadata: {}
  }
  const previousLegacy = {
    id: "legacy-1",
    is_active: true,
    opportunity_fingerprint: "ofp-existing",
    item_name: "AK-47 | Redline (Field-Tested)",
    buy_market: "steam",
    sell_market: "skinport",
    metadata: {}
  }

  const originals = {
    getRecentRowsByItems: globalActiveOpportunityRepo.getRecentRowsByItems,
    getRowsByFingerprints: globalActiveOpportunityRepo.getRowsByFingerprints,
    upsertRows: globalActiveOpportunityRepo.upsertRows,
    updateRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    legacyGetRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    legacyGetActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    legacyInsertRows: arbitrageFeedRepo.insertRows,
    legacyUpdateRowsById: arbitrageFeedRepo.updateRowsById,
    markRowsInactiveByIds: arbitrageFeedRepo.markRowsInactiveByIds,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    writePublishBatch: diagnosticsWriter.writePublishBatch,
    writePublishDecisions: diagnosticsWriter.writePublishDecisions
  }

  let activeUpdatePayload = null
  let legacyUpdatePayload = null
  let historyPayload = null
  let persistedActiveRows = [previousActive]

  globalActiveOpportunityRepo.getRecentRowsByItems = async () => [previousActive]
  globalActiveOpportunityRepo.getRowsByFingerprints = async ({ fingerprints = [] } = {}) =>
    persistedActiveRows.filter((row) => fingerprints.includes(row.opportunity_fingerprint))
  globalActiveOpportunityRepo.upsertRows = async () => []
  globalActiveOpportunityRepo.updateRowsById = async (rows = []) => {
    activeUpdatePayload = rows
    persistedActiveRows = persistedActiveRows.map((row) => {
      const update = rows.find((entry) => entry.id === row.id)
      if (!update) return row
      return {
        ...row,
        ...update.patch
      }
    })
    return rows.length
  }
  globalOpportunityHistoryRepo.insertRows = async (rows = []) => {
    historyPayload = rows
    return rows
  }
  arbitrageFeedRepo.getRecentRowsByItems = async () => [previousLegacy]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [previousLegacy]
  arbitrageFeedRepo.insertRows = async () => []
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    legacyUpdatePayload = rows
    return rows.length
  }
  arbitrageFeedRepo.markRowsInactiveByIds = async () => 0
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  diagnosticsWriter.writePublishBatch = async () => null
  diagnosticsWriter.writePublishDecisions = async () => null

  try {
    const result = await globalFeedPublisher.publishBatch({
      scanRunId: "scan-run-stale",
      opportunities: [
        buildOpportunity({
          buyRouteUpdatedAt: staleIso,
          sellRouteUpdatedAt: staleIso,
          metadata: {
            buy_route_available: true,
            sell_route_available: true,
            buy_route_updated_at: staleIso,
            sell_route_updated_at: staleIso,
            sell_listing_available: true,
            sell_url: "https://skinport.com/item/ak-47-redline-field-tested"
          }
        })
      ]
    })

    assert.equal(result.blockedCount, 1)
    assert.equal(Array.isArray(activeUpdatePayload), true)
    assert.equal(activeUpdatePayload.length, 1)
    assert.equal(activeUpdatePayload[0].patch.live_status, "stale")
    assert.equal(activeUpdatePayload[0].patch.refresh_status, "stale")
    assert.equal(
      activeUpdatePayload[0]?.patch?.metadata?.freshness_contract_diagnostics?.buy_and_sell_route_stale,
      true
    )
    assert.equal(Array.isArray(historyPayload), true)
    assert.equal(historyPayload[0].event_type, "expired")
    assert.equal(historyPayload[0].active_opportunity_id, "active-1")
    assert.equal(Array.isArray(legacyUpdatePayload), true)
    assert.equal(legacyUpdatePayload[0].patch.is_active, false)
    assert.equal(legacyUpdatePayload[0].patch.live_status, "stale")
  } finally {
    globalActiveOpportunityRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    globalActiveOpportunityRepo.getRowsByFingerprints = originals.getRowsByFingerprints
    globalActiveOpportunityRepo.upsertRows = originals.upsertRows
    globalActiveOpportunityRepo.updateRowsById = originals.updateRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    arbitrageFeedRepo.getRecentRowsByItems = originals.legacyGetRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.legacyGetActiveRowsByFingerprints
    arbitrageFeedRepo.insertRows = originals.legacyInsertRows
    arbitrageFeedRepo.updateRowsById = originals.legacyUpdateRowsById
    arbitrageFeedRepo.markRowsInactiveByIds = originals.markRowsInactiveByIds
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    diagnosticsWriter.writePublishBatch = originals.writePublishBatch
    diagnosticsWriter.writePublishDecisions = originals.writePublishDecisions
  }
})

test("global feed publisher writes degraded history event for structurally invalid publish failures", async () => {
  const nowIso = new Date().toISOString()
  const previousActive = {
    id: "active-2",
    opportunity_fingerprint: "ofp-existing-degraded",
    material_change_hash: "mch-existing-degraded",
    item_name: "USP-S | Neo-Noir (Field-Tested)",
    market_hash_name: "USP-S | Neo-Noir (Field-Tested)",
    category: "weapon_skin",
    buy_market: "steam",
    sell_market: "skinport",
    buy_price: 21,
    sell_net: 24,
    profit: 3,
    spread_pct: 14.28,
    opportunity_score: 79,
    execution_confidence: "Medium",
    quality_grade: "RISKY",
    liquidity_label: "Medium",
    live_status: "live",
    refresh_status: "pending",
    metadata: {}
  }
  const previousLegacy = {
    id: "legacy-2",
    is_active: true,
    opportunity_fingerprint: previousActive.opportunity_fingerprint,
    item_name: previousActive.item_name,
    buy_market: "steam",
    sell_market: "skinport",
    metadata: {}
  }

  const originals = {
    getRecentRowsByItems: globalActiveOpportunityRepo.getRecentRowsByItems,
    getRowsByFingerprints: globalActiveOpportunityRepo.getRowsByFingerprints,
    upsertRows: globalActiveOpportunityRepo.upsertRows,
    updateRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    legacyGetRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    legacyGetActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    legacyInsertRows: arbitrageFeedRepo.insertRows,
    legacyUpdateRowsById: arbitrageFeedRepo.updateRowsById,
    markRowsInactiveByIds: arbitrageFeedRepo.markRowsInactiveByIds,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan,
    writePublishBatch: diagnosticsWriter.writePublishBatch,
    writePublishDecisions: diagnosticsWriter.writePublishDecisions
  }

  let historyPayload = null
  let legacyUpdatePayload = null
  let persistedActiveRows = [previousActive]

  globalActiveOpportunityRepo.getRecentRowsByItems = async () => [previousActive]
  globalActiveOpportunityRepo.getRowsByFingerprints = async ({ fingerprints = [] } = {}) =>
    persistedActiveRows.filter((row) => fingerprints.includes(row.opportunity_fingerprint))
  globalActiveOpportunityRepo.upsertRows = async () => []
  globalActiveOpportunityRepo.updateRowsById = async (rows = []) => {
    persistedActiveRows = persistedActiveRows.map((row) => {
      const update = rows.find((entry) => entry.id === row.id)
      if (!update) return row
      return {
        ...row,
        ...update.patch
      }
    })
    return rows.length
  }
  globalOpportunityHistoryRepo.insertRows = async (rows = []) => {
    historyPayload = rows
    return rows
  }
  arbitrageFeedRepo.getRecentRowsByItems = async () => [previousLegacy]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [previousLegacy]
  arbitrageFeedRepo.insertRows = async () => []
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    legacyUpdatePayload = rows
    return rows.length
  }
  arbitrageFeedRepo.markRowsInactiveByIds = async () => 0
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0
  diagnosticsWriter.writePublishBatch = async () => null
  diagnosticsWriter.writePublishDecisions = async () => null

  try {
    const result = await globalFeedPublisher.publishBatch({
      scanRunId: "scan-run-degraded",
      opportunities: [
        buildOpportunity({
          marketHashName: previousActive.item_name,
          itemName: previousActive.item_name,
          buyPrice: previousActive.buy_price,
          sellNet: previousActive.sell_net,
          profit: previousActive.profit,
          spread: previousActive.spread_pct,
          score: previousActive.opportunity_score,
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
        })
      ]
    })

    assert.equal(result.blockedCount, 1)
    assert.equal(Array.isArray(historyPayload), true)
    assert.equal(historyPayload[0].event_type, "degraded")
    assert.equal(historyPayload[0].active_opportunity_id, "active-2")
    assert.equal(Array.isArray(legacyUpdatePayload), true)
    assert.equal(legacyUpdatePayload[0].patch.is_active, false)
    assert.equal(legacyUpdatePayload[0].patch.live_status, "degraded")
  } finally {
    globalActiveOpportunityRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    globalActiveOpportunityRepo.getRowsByFingerprints = originals.getRowsByFingerprints
    globalActiveOpportunityRepo.upsertRows = originals.upsertRows
    globalActiveOpportunityRepo.updateRowsById = originals.updateRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    arbitrageFeedRepo.getRecentRowsByItems = originals.legacyGetRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.legacyGetActiveRowsByFingerprints
    arbitrageFeedRepo.insertRows = originals.legacyInsertRows
    arbitrageFeedRepo.updateRowsById = originals.legacyUpdateRowsById
    arbitrageFeedRepo.markRowsInactiveByIds = originals.markRowsInactiveByIds
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
    diagnosticsWriter.writePublishBatch = originals.writePublishBatch
    diagnosticsWriter.writePublishDecisions = originals.writePublishDecisions
  }
})

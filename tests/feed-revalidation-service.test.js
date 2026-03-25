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
const marketStateReadService = require("../src/services/marketStateReadService")
const scannerRunLeaseService = require("../src/services/scannerRunLeaseService")
const feedRevalidationService = require("../src/services/feed/feedRevalidationService")

function buildActiveRow(overrides = {}) {
  const nowIso = new Date().toISOString()
  return {
    id: "active-row-1",
    opportunity_fingerprint: "ofp-active-1",
    material_change_hash: "mch-active-1",
    item_name: "AK-47 | Vulcan (Field-Tested)",
    market_hash_name: "AK-47 | Vulcan (Field-Tested)",
    category: "weapon_skin",
    buy_market: "steam",
    buy_price: 20,
    sell_market: "skinport",
    sell_net: 23,
    profit: 3,
    spread_pct: 15,
    opportunity_score: 82,
    execution_confidence: "High",
    quality_grade: "STRONG",
    liquidity_label: "High",
    market_signal_observed_at: nowIso,
    first_seen_at: nowIso,
    last_seen_at: nowIso,
    last_published_at: nowIso,
    refresh_status: "pending",
    live_status: "live",
    latest_signal_age_hours: 0.2,
    metadata: {
      market_coverage: 3,
      volume_7d: 180
    },
    ...overrides
  }
}

test("feed revalidation expires stale active rows and updates legacy projection", async () => {
  const nowMs = Date.now()
  const staleIso = new Date(nowMs - 3 * 60 * 60 * 1000).toISOString()
  const row = buildActiveRow()
  const legacyRow = {
    id: "legacy-row-1",
    opportunity_fingerprint: row.opportunity_fingerprint,
    is_active: true,
    metadata: {}
  }

  const originals = {
    recoverExpired: scannerRunLeaseService.recoverExpired,
    acquire: scannerRunLeaseService.acquire,
    complete: scannerRunLeaseService.complete,
    fail: scannerRunLeaseService.fail,
    listRowsForRevalidation: globalActiveOpportunityRepo.listRowsForRevalidation,
    updateActiveRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    getLatestQuotesByItemNames: marketStateReadService.getLatestQuotesByItemNames,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateLegacyRowsById: arbitrageFeedRepo.updateRowsById,
    writeRevalidationBatch: diagnosticsWriter.writeRevalidationBatch
  }

  let activeUpdatePayload = null
  let legacyUpdatePayload = null
  let historyPayload = null

  scannerRunLeaseService.recoverExpired = async () => 0
  scannerRunLeaseService.acquire = async () => ({
    acquired: true,
    leaseId: "revalidation-run-1"
  })
  scannerRunLeaseService.complete = async () => null
  scannerRunLeaseService.fail = async () => null
  globalActiveOpportunityRepo.listRowsForRevalidation = async () => [row]
  globalActiveOpportunityRepo.updateRowsById = async (rows = []) => {
    activeUpdatePayload = rows
    return rows.length
  }
  globalOpportunityHistoryRepo.insertRows = async (rows = []) => {
    historyPayload = rows
    return rows
  }
  marketStateReadService.getLatestQuotesByItemNames = async () => ({
    [row.item_name]: {
      steam: {
        market: "steam",
        best_buy: 19.8,
        best_sell: 20.9,
        best_sell_net: 18.1,
        volume_7d: 181,
        fetched_at: staleIso
      },
      skinport: {
        market: "skinport",
        best_buy: 23.9,
        best_sell: 26.2,
        best_sell_net: 23.1,
        volume_7d: 177,
        fetched_at: staleIso,
        quality_flags: {
          skinport_quote_type: "live_executable",
          skinport_price_integrity_status: "confirmed",
          skinport_listing_id: "sp-live-123"
        }
      }
    }
  })
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [legacyRow]
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    legacyUpdatePayload = rows
    return rows.length
  }
  diagnosticsWriter.writeRevalidationBatch = async () => null

  try {
    const result = await feedRevalidationService.runScheduledSweep({
      nowIso: new Date(nowMs).toISOString(),
      limit: 50
    })

    assert.equal(result.scannedCount, 1)
    assert.equal(result.staleExpiredCount, 1)
    assert.equal(Array.isArray(activeUpdatePayload), true)
    assert.equal(activeUpdatePayload[0].patch.live_status, "stale")
    assert.equal(activeUpdatePayload[0].patch.refresh_status, "stale")
    assert.equal(Array.isArray(historyPayload), true)
    assert.equal(historyPayload[0].event_type, "expired")
    assert.equal(Array.isArray(legacyUpdatePayload), true)
    assert.equal(legacyUpdatePayload[0].patch.is_active, false)
    assert.equal(legacyUpdatePayload[0].patch.live_status, "stale")
  } finally {
    scannerRunLeaseService.recoverExpired = originals.recoverExpired
    scannerRunLeaseService.acquire = originals.acquire
    scannerRunLeaseService.complete = originals.complete
    scannerRunLeaseService.fail = originals.fail
    globalActiveOpportunityRepo.listRowsForRevalidation = originals.listRowsForRevalidation
    globalActiveOpportunityRepo.updateRowsById = originals.updateActiveRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    marketStateReadService.getLatestQuotesByItemNames = originals.getLatestQuotesByItemNames
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateLegacyRowsById
    diagnosticsWriter.writeRevalidationBatch = originals.writeRevalidationBatch
  }
})

test("feed revalidation keeps live rows active without writing expiry history", async () => {
  const nowMs = Date.now()
  const freshIso = new Date(nowMs - 15 * 60 * 1000).toISOString()
  const row = buildActiveRow({
    market_signal_observed_at: freshIso
  })
  const legacyRow = {
    id: "legacy-row-2",
    opportunity_fingerprint: row.opportunity_fingerprint,
    is_active: true,
    metadata: {}
  }

  const originals = {
    recoverExpired: scannerRunLeaseService.recoverExpired,
    acquire: scannerRunLeaseService.acquire,
    complete: scannerRunLeaseService.complete,
    fail: scannerRunLeaseService.fail,
    listRowsForRevalidation: globalActiveOpportunityRepo.listRowsForRevalidation,
    updateActiveRowsById: globalActiveOpportunityRepo.updateRowsById,
    insertHistoryRows: globalOpportunityHistoryRepo.insertRows,
    getLatestQuotesByItemNames: marketStateReadService.getLatestQuotesByItemNames,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateLegacyRowsById: arbitrageFeedRepo.updateRowsById,
    writeRevalidationBatch: diagnosticsWriter.writeRevalidationBatch
  }

  let historyPayload = null

  scannerRunLeaseService.recoverExpired = async () => 0
  scannerRunLeaseService.acquire = async () => ({
    acquired: true,
    leaseId: "revalidation-run-2"
  })
  scannerRunLeaseService.complete = async () => null
  scannerRunLeaseService.fail = async () => null
  globalActiveOpportunityRepo.listRowsForRevalidation = async () => [row]
  globalActiveOpportunityRepo.updateRowsById = async () => 1
  globalOpportunityHistoryRepo.insertRows = async (rows = []) => {
    historyPayload = rows
    return rows
  }
  marketStateReadService.getLatestQuotesByItemNames = async () => ({
    [row.item_name]: {
      steam: {
        market: "steam",
        best_buy: 19.4,
        best_sell: 20.6,
        best_sell_net: 17.9,
        volume_7d: 220,
        fetched_at: freshIso
      },
      skinport: {
        market: "skinport",
        best_buy: 23.2,
        best_sell: 25.8,
        best_sell_net: 22.7,
        volume_7d: 210,
        fetched_at: freshIso,
        quality_flags: {
          skinport_quote_type: "live_executable",
          skinport_price_integrity_status: "confirmed",
          skinport_quote_currency: "USD",
          skinport_item_slug: "ak-47-vulcan-field-tested",
          skinport_listing_id: "sp-live-1"
        }
      }
    }
  })
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [legacyRow]
  arbitrageFeedRepo.updateRowsById = async () => 1
  diagnosticsWriter.writeRevalidationBatch = async () => null

  try {
    const result = await feedRevalidationService.runScheduledSweep({
      nowIso: new Date(nowMs).toISOString(),
      limit: 50
    })

    assert.equal(result.scannedCount, 1)
    assert.equal(result.refreshedLiveCount + result.unchangedCount >= 1, true)
    assert.equal(Array.isArray(historyPayload), false)
  } finally {
    scannerRunLeaseService.recoverExpired = originals.recoverExpired
    scannerRunLeaseService.acquire = originals.acquire
    scannerRunLeaseService.complete = originals.complete
    scannerRunLeaseService.fail = originals.fail
    globalActiveOpportunityRepo.listRowsForRevalidation = originals.listRowsForRevalidation
    globalActiveOpportunityRepo.updateRowsById = originals.updateActiveRowsById
    globalOpportunityHistoryRepo.insertRows = originals.insertHistoryRows
    marketStateReadService.getLatestQuotesByItemNames = originals.getLatestQuotesByItemNames
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateLegacyRowsById
    diagnosticsWriter.writeRevalidationBatch = originals.writeRevalidationBatch
  }
})

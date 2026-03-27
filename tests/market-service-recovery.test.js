const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const priceRepo = require("../src/repositories/priceHistoryRepository")
const snapshotRepo = require("../src/repositories/marketSnapshotRepository")
const steamMarketPriceService = require("../src/services/steamMarketPriceService")
const marketService = require("../src/services/marketService")

test("refreshSnapshotsForSkins does not fake fresh snapshots when strict live overview fails", async () => {
  const originals = {
    getLatestPriceBySkinId: priceRepo.getLatestPriceBySkinId,
    getHistoryBySkinId: priceRepo.getHistoryBySkinId,
    getLatestBySkinIds: snapshotRepo.getLatestBySkinIds,
    insertSnapshot: snapshotRepo.insertSnapshot,
    getPriceOverview: steamMarketPriceService.getPriceOverview
  }

  let insertCalled = false
  priceRepo.getLatestPriceBySkinId = async () => ({ price: 12.5 })
  priceRepo.getHistoryBySkinId = async () => [
    { price: 12.5, recorded_at: "2026-03-20T10:00:00.000Z" }
  ]
  snapshotRepo.getLatestBySkinIds = async () => ({})
  snapshotRepo.insertSnapshot = async () => {
    insertCalled = true
    return { skin_id: 1, captured_at: new Date().toISOString() }
  }
  steamMarketPriceService.getPriceOverview = async () => {
    throw new Error("upstream_unavailable")
  }

  try {
    const result = await marketService.refreshSnapshotsForSkins(
      [{ id: 1, market_hash_name: "AK-47 | Redline (Field-Tested)" }],
      {
        refreshStaleOnly: false,
        requireLiveOverview: true
      }
    )

    assert.equal(result.length, 1)
    assert.equal(result[0].refreshed, false)
    assert.equal(Boolean(result[0].error), true)
    assert.equal(insertCalled, false)
  } finally {
    priceRepo.getLatestPriceBySkinId = originals.getLatestPriceBySkinId
    priceRepo.getHistoryBySkinId = originals.getHistoryBySkinId
    snapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    snapshotRepo.insertSnapshot = originals.insertSnapshot
    steamMarketPriceService.getPriceOverview = originals.getPriceOverview
  }
})

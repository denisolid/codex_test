const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")
const feedCompatibilityProjector = require("../src/services/feed/feedCompatibilityProjector")

test("feed compatibility projector deactivates duplicate active legacy rows and updates the canonical row", async () => {
  const activeRow = {
    id: "active-1",
    opportunity_fingerprint: "ofp-compat-1",
    material_change_hash: "mch-compat-1",
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
    liquidity_label: "Medium",
    first_seen_at: "2026-03-20T10:00:00.000Z",
    last_seen_at: "2026-03-25T10:00:00.000Z",
    last_published_at: "2026-03-25T10:00:00.000Z",
    refresh_status: "pending",
    live_status: "live",
    latest_signal_age_hours: null,
    metadata: {
      feed_event: "updated"
    }
  }
  const newestLegacy = {
    id: "legacy-newest",
    opportunity_fingerprint: "ofp-compat-1",
    item_name: activeRow.item_name,
    buy_market: "steam",
    sell_market: "skinport",
    detected_at: "2026-03-25T10:00:00.000Z",
    last_seen_at: "2026-03-25T10:00:00.000Z",
    last_published_at: "2026-03-25T10:00:00.000Z",
    times_seen: 2,
    is_active: true,
    metadata: {}
  }
  const olderLegacy = {
    ...newestLegacy,
    id: "legacy-older",
    detected_at: "2026-03-25T09:00:00.000Z",
    last_seen_at: "2026-03-25T09:00:00.000Z"
  }

  const originals = {
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    markRowsInactiveByIds: arbitrageFeedRepo.markRowsInactiveByIds,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows
  }

  let markedIds = null
  let updateRowsPayload = null

  arbitrageFeedRepo.getRecentRowsByItems = async () => [newestLegacy, olderLegacy]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [newestLegacy, olderLegacy]
  arbitrageFeedRepo.markRowsInactiveByIds = async (ids = []) => {
    markedIds = ids
    return ids.length
  }
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    updateRowsPayload = rows
    return rows.length
  }
  arbitrageFeedRepo.insertRows = async () => []

  try {
    const result = await feedCompatibilityProjector.syncRows({
      activeRows: [activeRow],
      stage: "publish",
      nowIso: "2026-03-25T10:00:00.000Z"
    })

    assert.deepEqual(markedIds, ["legacy-older"])
    assert.equal(result.duplicateActivesMarkedInactive, 1)
    assert.equal(Array.isArray(updateRowsPayload), true)
    assert.equal(updateRowsPayload.length, 1)
    assert.equal(updateRowsPayload[0].id, "legacy-newest")
    assert.equal(updateRowsPayload[0].patch.opportunity_fingerprint, activeRow.opportunity_fingerprint)
  } finally {
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.markRowsInactiveByIds = originals.markRowsInactiveByIds
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
  }
})

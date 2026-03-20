const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const {
  __testables: {
    computeAgeHours,
    resolveRefreshOutcome,
    shouldAdmitToFeed,
    buildRefreshedOpportunityRow
  },
  LIVE_MAX_SIGNAL_AGE_HOURS
} = require("../src/services/feedPublishRefreshService")

function buildRawFeedRow(overrides = {}) {
  const nowIso = new Date().toISOString()
  return {
    id: "00000000-0000-0000-0000-000000000001",
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
    detected_at: nowIso,
    discovered_at: nowIso,
    scan_run_id: "00000000-0000-0000-0000-000000000010",
    is_active: true,
    is_duplicate: false,
    metadata: {
      market_coverage: 3,
      volume_7d: 180
    },
    ...overrides
  }
}

test("computeAgeHours returns numeric age for valid iso", () => {
  const nowMs = Date.now()
  const iso = new Date(nowMs - 90 * 60 * 1000).toISOString()
  const age = computeAgeHours(iso, nowMs)
  assert.equal(Number.isFinite(age), true)
  assert.equal(age > 1.4 && age < 1.6, true)
})

test("resolveRefreshOutcome maps live/stale/degraded outcomes", () => {
  const live = resolveRefreshOutcome({
    ageHours: 0.5,
    netProfitAfterFees: 2.1,
    verdict: "strong_buy"
  })
  assert.equal(live.refreshStatus, "ok")
  assert.equal(live.liveStatus, "live")

  const stale = resolveRefreshOutcome({
    ageHours: LIVE_MAX_SIGNAL_AGE_HOURS + 1,
    netProfitAfterFees: 1.2,
    verdict: "watch"
  })
  assert.equal(stale.refreshStatus, "stale")
  assert.equal(stale.liveStatus, "stale")

  const degraded = resolveRefreshOutcome({
    ageHours: 1.1,
    netProfitAfterFees: -0.1,
    verdict: "skip"
  })
  assert.equal(degraded.refreshStatus, "degraded")
  assert.equal(degraded.liveStatus, "degraded")
})

test("buildRefreshedOpportunityRow refreshes prices and marks live status", () => {
  const nowMs = Date.now()
  const nowIso = new Date(nowMs).toISOString()
  const raw = buildRawFeedRow({
    detected_at: new Date(nowMs - 45 * 60 * 1000).toISOString()
  })

  const refreshed = buildRefreshedOpportunityRow(
    raw,
    {
      [raw.item_name]: {
        steam: {
          market: "steam",
          best_buy: 19.4,
          best_sell: 20.6,
          best_sell_net: 17.9,
          volume_7d: 220,
          fetched_at: new Date(nowMs - 20 * 60 * 1000).toISOString()
        },
        skinport: {
          market: "skinport",
          best_buy: 23.2,
          best_sell: 25.8,
          best_sell_net: 22.7,
          volume_7d: 210,
          fetched_at: new Date(nowMs - 15 * 60 * 1000).toISOString(),
          quality_flags: {
            skinport_quote_type: "live_executable",
            skinport_price_integrity_status: "confirmed",
            skinport_quote_currency: "USD",
            skinport_item_slug: "ak-47-vulcan-field-tested",
            skinport_listing_id: "sp-live-1"
          }
        }
      }
    },
    { nowIso, nowMs }
  )

  assert.equal(Number(refreshed.api.buyPrice) > 0, true)
  assert.equal(Number(refreshed.api.sellNet) > 0, true)
  assert.equal(refreshed.api.liveStatus, "live")
  assert.equal(refreshed.api.refreshStatus, "ok")
  assert.equal(Number(refreshed.api.latestSignalAgeHours || 0) <= 2, true)
  assert.equal(refreshed.api.skinportQuoteType, "live_executable")
  assert.equal(refreshed.api.skinportPriceIntegrityStatus, "confirmed")
  assert.equal(Boolean(refreshed.api.skinportListingId), true)
})

test("shouldAdmitToFeed respects live and risky admission rules", () => {
  const liveRow = {
    liveStatus: "live",
    refreshStatus: "ok",
    latestSignalAgeHours: 1.2,
    netProfitAfterFees: 1.4
  }
  assert.equal(shouldAdmitToFeed(liveRow, { includeRisky: false }), true)
  assert.equal(shouldAdmitToFeed(liveRow, { includeRisky: true }), true)

  const staleRow = {
    liveStatus: "stale",
    refreshStatus: "stale",
    latestSignalAgeHours: 4.5,
    netProfitAfterFees: 0.8
  }
  assert.equal(shouldAdmitToFeed(staleRow, { includeRisky: false }), false)
  assert.equal(shouldAdmitToFeed(staleRow, { includeRisky: true }), true)

  const degradedRow = {
    liveStatus: "degraded",
    refreshStatus: "degraded",
    latestSignalAgeHours: 1.1,
    netProfitAfterFees: -0.2
  }
  assert.equal(shouldAdmitToFeed(degradedRow, { includeRisky: true }), false)
})

test("shouldAdmitToFeed blocks skinport rows unless live executable quote is confirmed", () => {
  const blocked = {
    buyMarket: "skinport",
    sellMarket: "steam",
    liveStatus: "live",
    refreshStatus: "ok",
    latestSignalAgeHours: 0.8,
    netProfitAfterFees: 1.1,
    skinportQuoteType: "historical_summary",
    skinportPriceIntegrityStatus: "unconfirmed",
    skinportQuoteObservedAt: new Date().toISOString()
  }
  const allowed = {
    buyMarket: "skinport",
    sellMarket: "steam",
    liveStatus: "live",
    refreshStatus: "ok",
    latestSignalAgeHours: 0.8,
    netProfitAfterFees: 1.1,
    skinportQuoteType: "live_executable",
    skinportPriceIntegrityStatus: "confirmed",
    skinportQuoteObservedAt: new Date().toISOString()
  }

  assert.equal(shouldAdmitToFeed(blocked, { includeRisky: true }), false)
  assert.equal(shouldAdmitToFeed(allowed, { includeRisky: false }), true)
})

test("shouldAdmitToFeed blocks skinport rows when quote timestamp is stale", () => {
  const staleObservedAt = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
  const row = {
    buyMarket: "skinport",
    sellMarket: "steam",
    liveStatus: "live",
    refreshStatus: "ok",
    latestSignalAgeHours: 0.7,
    netProfitAfterFees: 1.2,
    skinportQuoteType: "live_executable",
    skinportPriceIntegrityStatus: "confirmed",
    skinportQuoteObservedAt: staleObservedAt
  }

  assert.equal(shouldAdmitToFeed(row, { includeRisky: false }), false)
})

const test = require("node:test")
const assert = require("node:assert/strict")
const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")
const marketQuoteRepo = require("../src/repositories/marketQuoteRepository")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const {
  __testables: {
    computeAgeHours,
    resolveRefreshOutcome,
    buildSkinportValidation,
    evaluateFeedAdmission,
    shouldAdmitToFeed,
    buildRefreshedOpportunityRow
  },
  LIVE_MAX_SIGNAL_AGE_HOURS,
  refreshForFeedPublish
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

test("skinport validation applies safe fallback when quality flags are missing but quote row identity matches", () => {
  const mapped = {
    marketHashName: "AK-47 | Vulcan (Field-Tested)",
    itemName: "AK-47 | Vulcan (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport"
  }
  const sellQuote = {
    market: "skinport",
    item_name: "AK-47 | Vulcan (Field-Tested)",
    best_sell_net: 22.7,
    fetched_at: new Date().toISOString(),
    quality_flags: {}
  }
  const validation = buildSkinportValidation(mapped, null, sellQuote)

  assert.equal(validation.applicable, true)
  assert.equal(validation.confirmed, true)
  assert.equal(validation.validationTier, "fallback")
  assert.equal(validation.quoteType, "live_executable")
  assert.equal(validation.priceIntegrityStatus, "confirmed")
})

test("admission decision returns publish-gate reject reason for skinport", () => {
  const decision = evaluateFeedAdmission(
    {
      buyMarket: "skinport",
      sellMarket: "steam",
      liveStatus: "live",
      refreshStatus: "ok",
      latestSignalAgeHours: 0.5,
      netProfitAfterFees: 1.2,
      skinportQuoteType: "live_executable",
      skinportPriceIntegrityStatus: "confirmed",
      skinportQuoteObservedAt: new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()
    },
    { includeRisky: false }
  )

  assert.equal(decision.admit, false)
  assert.equal(decision.stage, "publish_gate")
  assert.equal(decision.reason, "stale_skinport_quote")
})

test("refreshForFeedPublish deactivates stale active feed rows", async () => {
  const nowMs = Date.now()
  const staleIso = new Date(nowMs - 3 * 60 * 60 * 1000).toISOString()
  const raw = buildRawFeedRow({
    id: "00000000-0000-0000-0000-000000000123",
    detected_at: new Date(nowMs - 20 * 60 * 1000).toISOString(),
    market_signal_observed_at: new Date(nowMs - 2 * 60 * 1000).toISOString()
  })

  const originals = {
    getLatestRowsByItemNames: marketQuoteRepo.getLatestRowsByItemNames,
    updatePublishRefreshState: arbitrageFeedRepo.updatePublishRefreshState
  }

  const quoteRows = {
    [raw.item_name]: {
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
  }

  let patchRowsPayload = null
  marketQuoteRepo.getLatestRowsByItemNames = async () => quoteRows
  arbitrageFeedRepo.updatePublishRefreshState = async (rows = []) => {
    patchRowsPayload = rows
    return rows.length
  }

  try {
    const result = await refreshForFeedPublish([raw], { includeRisky: true })
    assert.equal(result.rows.length, 0)
    assert.equal(result.diagnostics.publishValidation.blocked, 1)
    assert.equal(result.diagnostics.publishValidation.deactivated, 1)
    assert.equal(result.diagnostics.publishValidation.reasons.buy_and_sell_route_stale, 1)
    assert.equal(Array.isArray(patchRowsPayload), true)
    assert.equal(patchRowsPayload.length, 1)
    assert.equal(patchRowsPayload[0].id, raw.id)
    assert.equal(patchRowsPayload[0].patch.is_active, false)
    assert.equal(patchRowsPayload[0].patch.refresh_status, "stale")
    assert.equal(patchRowsPayload[0].patch.live_status, "stale")
  } finally {
    marketQuoteRepo.getLatestRowsByItemNames = originals.getLatestRowsByItemNames
    arbitrageFeedRepo.updatePublishRefreshState = originals.updatePublishRefreshState
  }
})

test("fresh generic signal cannot override stale route timestamps", () => {
  const nowMs = Date.now()
  const nowIso = new Date(nowMs).toISOString()
  const staleIso = new Date(nowMs - 3 * 60 * 60 * 1000).toISOString()
  const raw = buildRawFeedRow({
    detected_at: new Date(nowMs - 5 * 60 * 1000).toISOString(),
    market_signal_observed_at: nowIso,
    metadata: {
      latest_market_signal_at: nowIso,
      volume_7d: 200
    }
  })

  const refreshed = buildRefreshedOpportunityRow(
    raw,
    {
      [raw.item_name]: {
        steam: {
          market: "steam",
          best_buy: 19.9,
          best_sell: 20.7,
          best_sell_net: 18.2,
          volume_7d: 120,
          fetched_at: staleIso
        },
        skinport: {
          market: "skinport",
          best_buy: 24.1,
          best_sell: 26.9,
          best_sell_net: 23.3,
          volume_7d: 118,
          fetched_at: staleIso,
          quality_flags: {
            skinport_quote_type: "live_executable",
            skinport_price_integrity_status: "confirmed",
            skinport_listing_id: "sp-stale-1"
          }
        }
      }
    },
    { nowIso, nowMs }
  )

  assert.equal(refreshed.api.publishFreshnessState, "stale")
  assert.equal(refreshed.api.requiredRoutePublishable, false)
  assert.equal(
    Number(refreshed.api.latestSignalAgeHours || 0) > LIVE_MAX_SIGNAL_AGE_HOURS,
    true
  )
  assert.equal(refreshed.api.marketSignalObservedAt, staleIso)
})

test("buildRefreshedOpportunityRow keeps legacy feed fields while adding publish metadata", () => {
  const nowMs = Date.now()
  const nowIso = new Date(nowMs).toISOString()
  const freshIso = new Date(nowMs - 30 * 60 * 1000).toISOString()
  const raw = buildRawFeedRow({
    detected_at: new Date(nowMs - 40 * 60 * 1000).toISOString()
  })

  const refreshed = buildRefreshedOpportunityRow(
    raw,
    {
      [raw.item_name]: {
        steam: {
          market: "steam",
          best_buy: 19.7,
          best_sell: 20.5,
          best_sell_net: 18,
          volume_7d: 130,
          fetched_at: freshIso
        },
        skinport: {
          market: "skinport",
          best_buy: 24.2,
          best_sell: 26.8,
          best_sell_net: 23.2,
          volume_7d: 126,
          fetched_at: freshIso,
          quality_flags: {
            skinport_quote_type: "live_executable",
            skinport_price_integrity_status: "confirmed",
            skinport_listing_id: "sp-fresh-1"
          }
        }
      }
    },
    { nowIso, nowMs }
  )

  assert.equal(Number(refreshed.api.buyPrice) > 0, true)
  assert.equal(Number(refreshed.api.sellNet) > 0, true)
  assert.equal(Number(refreshed.api.profit) > 0, true)
  assert.equal(Number(refreshed.api.spread) > 0, true)
  assert.equal(Boolean(refreshed.api.itemName), true)
  assert.equal(Boolean(refreshed.api.marketHashName), true)
  assert.equal(Boolean(refreshed.api.qualityGrade), true)
  assert.equal(Boolean(refreshed.api.executionConfidence), true)
  assert.equal(refreshed.api.requiredRoutePublishable, true)
  assert.equal(refreshed.api.publishFreshnessState, "fresh")
  assert.equal(typeof refreshed.api.signalAgeMs, "number")
  assert.equal(Boolean(refreshed.patch?.metadata?.publish_validation), true)
})

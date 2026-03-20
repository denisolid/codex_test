const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const {
  __testables: { buildInsightPayloadFromOpportunity }
} = require("../src/services/opportunityInsightService")

function buildOpportunity(overrides = {}) {
  const nowIso = new Date().toISOString()
  return {
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    sellMarket: "skinport",
    buyPrice: 10,
    sellNet: 12.3,
    profit: 2.3,
    score: 84,
    executionConfidence: "High",
    volume7d: 220,
    marketCoverage: 4,
    latestMarketSignalAt: nowIso,
    latestQuoteAt: nowIso,
    latestSnapshotAt: nowIso,
    staleThresholdUsed: 120,
    staleResult: false,
    flags: [],
    ...overrides
  }
}

test("insight payload exposes required deterministic fields", () => {
  const payload = buildInsightPayloadFromOpportunity(buildOpportunity(), {
    nowMs: Date.now()
  })

  for (const key of [
    "gross_profit_pct",
    "gross_profit_usd",
    "net_profit_pct_after_fees",
    "net_profit_usd_after_fees",
    "confidence_score",
    "liquidity_score",
    "freshness_score",
    "exit_eta_hours",
    "recommended_position_size",
    "risk_flags",
    "reason_summary",
    "failure_conditions",
    "verdict",
    "why_this_trade_exists",
    "what_can_break_it",
    "why_exit_may_be_easy_or_hard"
  ]) {
    assert.equal(Object.prototype.hasOwnProperty.call(payload, key), true)
  }
})

test("high quality opportunity resolves to strong_buy", () => {
  const nowMs = Date.now()
  const payload = buildInsightPayloadFromOpportunity(
    buildOpportunity({
      latestMarketSignalAt: new Date(nowMs - 20 * 60 * 1000).toISOString(),
      latestQuoteAt: new Date(nowMs - 15 * 60 * 1000).toISOString(),
      latestSnapshotAt: new Date(nowMs - 25 * 60 * 1000).toISOString()
    }),
    { nowMs }
  )

  assert.equal(payload.verdict, "strong_buy")
  assert.equal(Number(payload.net_profit_usd_after_fees || 0) > 0, true)
  assert.equal(Number(payload.confidence_score || 0) >= 80, true)
  assert.equal(Number(payload.liquidity_score || 0) >= 70, true)
  assert.equal(Array.isArray(payload.failure_conditions), true)
})

test("stale thin-liquidity setup downgrades to risky", () => {
  const nowMs = Date.now()
  const payload = buildInsightPayloadFromOpportunity(
    buildOpportunity({
      buyPrice: 25,
      sellNet: 25.6,
      profit: 0.6,
      score: 54,
      executionConfidence: "Medium",
      volume7d: 2,
      marketCoverage: 1,
      staleThresholdUsed: 120,
      staleResult: true,
      latestMarketSignalAt: new Date(nowMs - 13 * 60 * 60 * 1000).toISOString(),
      latestQuoteAt: new Date(nowMs - 13 * 60 * 60 * 1000).toISOString(),
      latestSnapshotAt: new Date(nowMs - 13 * 60 * 60 * 1000).toISOString(),
      flags: ["limited_market_coverage", "stale_market_signal"]
    }),
    { nowMs }
  )

  assert.equal(payload.verdict, "risky")
  assert.equal(payload.risk_flags.includes("low_liquidity"), true)
  assert.equal(payload.risk_flags.includes("stale_signal"), true)
})

test("non-positive net profit is always skip", () => {
  const payload = buildInsightPayloadFromOpportunity(
    buildOpportunity({
      buyPrice: 19.5,
      sellNet: 19.2,
      profit: -0.3,
      score: 82,
      executionConfidence: "High"
    }),
    {
      nowMs: Date.now()
    }
  )

  assert.equal(payload.verdict, "skip")
  assert.equal(payload.risk_flags.includes("non_positive_net_profit"), true)
})

const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const {
  __testables: { buildInsightPayloadFromOpportunity, buildInsightHeadline }
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
    "headline_text",
    "headline_style",
    "primary_reason",
    "secondary_reason",
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
  assert.match(String(payload.headline_text || ""), /^Strong buy setup/i)
  assert.equal(payload.headline_style, "positive")
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
  assert.equal(payload.primary_reason, "stale_signal")
  assert.match(String(payload.headline_text || ""), /stale signal/i)
  assert.equal(payload.headline_style, "danger-soft")
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
  assert.match(String(payload.headline_text || ""), /^Skip for now/i)
  assert.equal(payload.headline_style, "danger")
})

test("liquidity-dominant setup headline mentions thin or weak exit", () => {
  const nowMs = Date.now()
  const payload = buildInsightPayloadFromOpportunity(
    buildOpportunity({
      buyPrice: 7.1,
      sellNet: 7.45,
      profit: 0.35,
      score: 61,
      executionConfidence: "Medium",
      volume7d: 1,
      marketCoverage: 1,
      staleThresholdUsed: 120,
      staleResult: false,
      latestMarketSignalAt: new Date(nowMs - 55 * 60 * 1000).toISOString(),
      latestQuoteAt: new Date(nowMs - 50 * 60 * 1000).toISOString(),
      latestSnapshotAt: new Date(nowMs - 60 * 60 * 1000).toISOString(),
      flags: ["low_liquidity"]
    }),
    { nowMs }
  )

  assert.equal(["watch", "risky", "skip"].includes(payload.verdict), true)
  assert.equal(["low_liquidity", "weak_exit"].includes(payload.primary_reason), true)
  assert.match(String(payload.headline_text || ""), /(thin liquidity|weak exit)/i)
})

test("buildInsightHeadline uses different deterministic families per verdict", () => {
  const base = {
    confidence_score: 74,
    freshness_score: 76,
    liquidity_score: 78,
    net_profit_pct_after_fees: 6.2,
    exit_eta_hours: 8.5,
    risk_flags: []
  }

  const byVerdict = {
    strong_buy: buildInsightHeadline({ ...base, verdict: "strong_buy" }),
    good_small_size: buildInsightHeadline({ ...base, verdict: "good_small_size" }),
    watch: buildInsightHeadline({
      ...base,
      verdict: "watch",
      confidence_score: 58,
      risk_flags: ["execution_uncertainty"]
    }),
    risky: buildInsightHeadline({
      ...base,
      verdict: "risky",
      freshness_score: 34,
      risk_flags: ["stale_signal"]
    }),
    skip: buildInsightHeadline({
      ...base,
      verdict: "skip",
      liquidity_score: 38,
      risk_flags: ["low_liquidity"]
    })
  }

  assert.match(byVerdict.strong_buy.headline_text, /^Strong /i)
  assert.match(byVerdict.good_small_size.headline_text, /^Good /i)
  assert.match(byVerdict.watch.headline_text, /^Watch/i)
  assert.match(byVerdict.risky.headline_text, /^Risky /i)
  assert.match(byVerdict.skip.headline_text, /^Skip /i)
})

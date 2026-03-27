const assert = require("node:assert/strict")
const { test } = require("node:test")

const {
  buildAlphaHotUniverse
} = require("../src/services/scanner/alphaHotUniverseService")

function buildRow(name, category = "weapon_skin", overrides = {}) {
  return {
    market_hash_name: name,
    item_name: name,
    category,
    tradable: true,
    is_active: true,
    candidate_status: "eligible",
    scan_eligible: true,
    scanCohort: "hot",
    catalog_status: "scannable",
    reference_price: 14,
    market_coverage_count: 3,
    volume_7d: 140,
    liquidity_rank: 60,
    priority_tier: "tier_a",
    priority_boost: 8,
    maturity_score: 84,
    last_market_signal_at: "2026-03-27T10:00:00.000Z",
    snapshot_captured_at: "2026-03-27T10:00:00.000Z",
    quote_fetched_at: "2026-03-27T10:00:00.000Z",
    ...overrides
  }
}

test("alpha hot universe keeps a smaller category-balanced ready pool with bounded near-eligible rows", () => {
  const nowIso = new Date().toISOString()
  const staleIso = new Date(Date.now() - 36 * 60 * 60 * 1000).toISOString()
  const rows = [
    buildRow("AK-47 | Slate (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("AK-47 | Legion of Anubis (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("M4A1-S | Decimator (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("M4A4 | The Battlestar (Factory New)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("FAMAS | Roll Cage (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("AWP | Asiimov (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("AWP | Neo-Noir (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("StatTrak AWP | Fever Dream (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("USP-S | Cortex (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Glock-18 | Vogue (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("P250 | See Ya Later (Battle-Scarred)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("MP9 | Mount Fuji (Field-Tested)", "weapon_skin", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Revolution Case", "case", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Fracture Case", "case", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Prisma 2 Case", "case", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Sticker Capsule 2", "sticker_capsule", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Paris 2023 Legends Autograph Capsule", "sticker_capsule", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("DreamHack 2014 Legends Sticker Capsule", "sticker_capsule", {
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Souvenir USP-S | Jawbreaker (Field-Tested)", "weapon_skin", {
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm",
      priority_tier: "tier_b",
      priority_boost: 3,
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Souvenir M4A1-S | Control Panel (Field-Tested)", "weapon_skin", {
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm",
      priority_tier: "tier_b",
      priority_boost: 2,
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Cold Candidate | Test", "weapon_skin", {
      candidate_status: "candidate",
      scan_eligible: false,
      scanCohort: "cold",
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Stale Candidate | Test", "weapon_skin", {
      last_market_signal_at: staleIso,
      snapshot_captured_at: staleIso,
      quote_fetched_at: staleIso
    }),
    buildRow("Missing Coverage | Test", "weapon_skin", {
      reference_price: null,
      market_coverage_count: 0,
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    }),
    buildRow("Low Quality | Test", "weapon_skin", {
      liquidity_rank: 0,
      volume_7d: 0,
      candidate_status: "near_eligible",
      scan_eligible: false,
      scanCohort: "warm",
      maturity_score: 10,
      priority_tier: null,
      priority_boost: 0,
      last_market_signal_at: nowIso,
      snapshot_captured_at: nowIso,
      quote_fetched_at: nowIso
    })
  ]

  const result = buildAlphaHotUniverse({
    rows,
    batchSize: 10,
    nowMs: Date.now()
  })

  assert.equal(result.diagnostics.selection_layer, "alpha_hot_universe")
  assert.equal(result.diagnostics.hot_universe_size, result.rows.length)
  assert.equal(result.diagnostics.rows_excluded_for_low_maturity, 1)
  assert.equal(result.diagnostics.rows_excluded_for_staleness, 1)
  assert.equal(result.diagnostics.rows_excluded_for_missing_coverage, 1)
  assert.equal(result.diagnostics.excluded_for_low_quality, 1)
  assert.equal(result.diagnostics.hot_universe_by_category.weapon_skin > result.diagnostics.hot_universe_by_category.case, true)
  assert.equal(result.diagnostics.hot_universe_by_category.case <= 6, true)
  assert.equal(result.diagnostics.hot_universe_by_category.sticker_capsule <= 4, true)
  assert.equal(result.diagnostics.intake_by_category.weapon_skin, result.diagnostics.hot_universe_by_category.weapon_skin)
  assert.equal(result.diagnostics.intake_by_category.case > 0, true)
  assert.equal(result.diagnostics.intake_by_category.sticker_capsule > 0, true)
  assert.equal(
    result.diagnostics.hot_universe_by_state.near_eligible <= result.diagnostics.near_eligible_cap,
    true
  )
  assert.equal(Number(result.diagnostics.quota_hits_by_category.weapon_skin || 0) >= 1, true)
  assert.equal(
    Number.isFinite(Number(result.diagnostics.quota_skips_by_category.weapon_skin || 0)),
    true
  )
  assert.equal(Number(result.diagnostics.intake_by_subtype.pistol || 0) > 0, true)
  assert.equal(Number(result.diagnostics.intake_by_subtype.rifle || 0) > 0, true)
  assert.equal(Number(result.diagnostics.intake_by_subtype.sniper || 0) > 0, true)
  assert.equal(result.rows.every((row) => row.alpha_hot_universe_source === "alpha_hot_universe"), true)

  const names = new Set(result.rows.map((row) => row.market_hash_name))
  assert.equal(names.has("Cold Candidate | Test"), false)
  assert.equal(names.has("Stale Candidate | Test"), false)
  assert.equal(names.has("Missing Coverage | Test"), false)
  assert.equal(names.has("Low Quality | Test"), false)

  const weaponFamilies = new Set(
    result.rows
      .filter((row) => row.category === "weapon_skin")
      .map((row) => String(row.alpha_hot_diversity_bucket || "").split(":")[1] || "unknown")
  )
  assert.equal(weaponFamilies.size >= 4, true)

  let previousSubtype = ""
  let subtypeStreak = 0
  let maxSubtypeStreak = 0
  for (const row of result.rows.filter((entry) => entry.category === "weapon_skin")) {
    const subtype = String(row.alpha_hot_diversity_bucket || "").split(":")[0] || "unknown"
    if (subtype === previousSubtype) {
      subtypeStreak += 1
    } else {
      previousSubtype = subtype
      subtypeStreak = 1
    }
    maxSubtypeStreak = Math.max(maxSubtypeStreak, subtypeStreak)
  }

  assert.equal(maxSubtypeStreak <= 2, true)
  assert.equal(typeof result.diagnostics.diversity_rebalancing_applied, "boolean")
})

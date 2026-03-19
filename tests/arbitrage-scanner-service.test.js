const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const {
  __testables: {
    normalizeCategoryFilter,
    classifyCatalogState,
    buildRoundRobinPool,
    selectScanCandidates,
    evaluateCandidateOpportunity,
    classifyOpportunityFeedEvent,
    isMateriallyNewOpportunity,
    buildFeedInsertRow,
    mapFeedRowToApiRow,
    isScannerRunOverdue,
    DEFAULT_UNIVERSE_LIMIT,
    OPPORTUNITY_BATCH_TARGET,
    SCAN_CHUNK_SIZE,
    SCAN_TIMEOUT_PER_BATCH_MS,
    SCAN_STATE
  }
} = require("../src/services/arbitrageScannerService")

function buildCatalogRow(index, category = "weapon_skin") {
  return {
    market_hash_name: `${category}-item-${index}`,
    item_name: `${category.toUpperCase()} Item ${index}`,
    category,
    tradable: true,
    is_active: true,
    reference_price: 12 + index,
    market_coverage_count: 2,
    volume_7d: 120 + index,
    snapshot_stale: false,
    snapshot_captured_at: new Date().toISOString(),
    quote_fetched_at: new Date().toISOString()
  }
}

test("category filter normalization keeps API aliases stable", () => {
  assert.equal(normalizeCategoryFilter("all"), "all")
  assert.equal(normalizeCategoryFilter("skins"), "weapon_skin")
  assert.equal(normalizeCategoryFilter("cases"), "case")
  assert.equal(normalizeCategoryFilter("capsules"), "sticker_capsule")
  assert.equal(normalizeCategoryFilter("knives"), "knife")
  assert.equal(normalizeCategoryFilter("gloves"), "glove")
  assert.equal(normalizeCategoryFilter("future_knife"), "knife")
  assert.equal(normalizeCategoryFilter("future_glove"), "glove")
})

test("scan-first state model forwards rows with penalties instead of hard-blocking", () => {
  const result = classifyCatalogState({
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    category: "weapon_skin",
    tradable: true,
    isActive: true,
    referencePrice: 11,
    marketCoverageCount: 1,
    volume7d: null,
    snapshotCapturedAt: null,
    quoteFetchedAt: null
  })

  assert.equal(result.state, SCAN_STATE.SCANABLE_WITH_PENALTIES)
  assert.equal(result.hardRejectReasons.length, 0)
  assert.equal(result.penaltyFlags.includes("missing_liquidity"), true)
  assert.equal(result.penaltyFlags.includes("weak_coverage"), true)
})

test("state model still keeps true hard rejects narrow and explicit", () => {
  const result = classifyCatalogState({
    marketHashName: "",
    itemName: "",
    category: "weapon_skin",
    tradable: true,
    isActive: true,
    referencePrice: null,
    marketCoverageCount: 0,
    snapshotCapturedAt: null,
    quoteFetchedAt: null
  })

  assert.equal(result.state, SCAN_STATE.HARD_REJECT)
  assert.equal(result.hardRejectReasons.includes("invalid_row"), true)
})

test("state model hard-rejects rows below $2 cost floor", () => {
  const result = classifyCatalogState({
    marketHashName: "Fracture Case",
    itemName: "Fracture Case",
    category: "case",
    tradable: true,
    isActive: true,
    referencePrice: 1.75,
    marketCoverageCount: 3,
    volume7d: 500,
    snapshotCapturedAt: new Date().toISOString(),
    quoteFetchedAt: new Date().toISOString()
  })

  assert.equal(result.state, SCAN_STATE.HARD_REJECT)
  assert.equal(result.hardRejectReasons.includes("below_min_cost_floor"), true)
})

test("round-robin pool keeps category-aware distribution", () => {
  const rows = [
    buildCatalogRow(1, "weapon_skin"),
    buildCatalogRow(2, "weapon_skin"),
    buildCatalogRow(1, "case"),
    buildCatalogRow(1, "sticker_capsule")
  ]
  const pool = buildRoundRobinPool(rows, { lastScannedAtByName: new Map() })

  assert.equal(pool.length, 4)
  assert.equal(pool[0].category, "weapon_skin")
  assert.equal(pool[1].category, "case")
  assert.equal(pool[2].category, "sticker_capsule")
})

test("candidate selection tries to fill configured batch size and rotates cursor", () => {
  const catalogRows = []
  for (let index = 0; index < 30; index += 1) {
    catalogRows.push(buildCatalogRow(index + 1, index % 3 === 0 ? "case" : "weapon_skin"))
  }
  const tracker = new Map()
  const first = selectScanCandidates({
    catalogRows,
    batchSize: 12,
    cursor: 0,
    lastScannedAtByName: tracker
  })
  const second = selectScanCandidates({
    catalogRows,
    batchSize: 12,
    cursor: first.nextCursor,
    lastScannedAtByName: tracker
  })

  assert.equal(first.selected.length, 12)
  assert.equal(first.attemptedBatchSize, 12)
  assert.equal(second.selected.length, 12)
  assert.notEqual(first.selected[0].marketHashName, second.selected[0].marketHashName)
  assert.equal(Number(first.diagnostics.scanable + first.diagnostics.scanableWithPenalties) > 0, true)
})

test("candidate selection cycles through full scannable pool before repeating under last-scanned ordering", () => {
  const catalogRows = []
  const categories = ["weapon_skin", "case", "sticker_capsule", "knife", "glove"]
  for (let index = 0; index < 25; index += 1) {
    catalogRows.push(buildCatalogRow(index + 1, categories[index % categories.length]))
  }

  const tracker = new Map()
  let cursor = 0
  const seen = new Set()
  for (let run = 0; run < 3; run += 1) {
    const selection = selectScanCandidates({
      catalogRows,
      batchSize: 10,
      cursor,
      lastScannedAtByName: tracker,
      nowMs: Date.now() + run + 1
    })
    selection.selected.forEach((row) => seen.add(row.marketHashName))
    cursor = selection.nextCursor
  }

  assert.equal(seen.size, 25)
})

test("candidate selection prioritizes tier_a then tier_b then non-priority", () => {
  const nowIso = new Date().toISOString()
  const catalogRows = [
    {
      ...buildCatalogRow(1, "weapon_skin"),
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      priority_tier: "tier_b",
      priority_boost: 220
    },
    {
      ...buildCatalogRow(2, "weapon_skin"),
      market_hash_name: "AWP | Dragon Lore (Field-Tested)",
      priority_tier: "tier_a",
      priority_boost: 380
    },
    {
      ...buildCatalogRow(3, "weapon_skin"),
      market_hash_name: "M4A4 | The Emperor (Field-Tested)",
      priority_tier: null,
      priority_boost: 0
    }
  ].map((row) => ({
    ...row,
    snapshot_captured_at: nowIso,
    quote_fetched_at: nowIso
  }))

  const selection = selectScanCandidates({
    catalogRows,
    batchSize: 3,
    cursor: 0,
    lastScannedAtByName: new Map()
  })

  assert.equal(selection.selected.length, 3)
  assert.equal(selection.selected[0].priorityTier, "tier_a")
  assert.equal(selection.selected[1].priorityTier, "tier_b")
  assert.equal(selection.selected[2].priorityTier, null)
  assert.equal(Number(selection.diagnostics.selectedByPriorityTier.tier_a || 0), 1)
  assert.equal(Number(selection.diagnostics.selectedByPriorityTier.tier_b || 0), 1)
  assert.equal(Number(selection.diagnostics.selectedByPriorityTier.non_priority || 0), 1)
})

test("opportunity evaluation keeps missing-liquidity items scannable with downgraded tier", () => {
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Bloodsport (Field-Tested)",
      itemName: "AK-47 | Bloodsport (Field-Tested)",
      category: "weapon_skin",
      itemSubcategory: null,
      referencePrice: 18,
      marketCoverageCount: 2,
      volume7d: null,
      scanPenaltyFlags: ["missing_liquidity"],
      scanFreshness: { state: "fresh" }
    },
    {
      marketHashName: "AK-47 | Bloodsport (Field-Tested)",
      bestBuy: { source: "steam", grossPrice: 11.2, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 13.9, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 11.2,
        sellMarket: "skinport",
        sellNet: 13.9,
        profit: 2.7,
        spreadPercent: 24.1,
        opportunityScore: 71,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 18,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, false)
  assert.equal(
    evaluation.tier === "strong" || evaluation.tier === "risky" || evaluation.tier === "speculative",
    true
  )
  assert.equal(evaluation.penaltyFlags.includes("missing_liquidity"), true)
})

test("opportunity evaluation enforces true hard reject reasons", () => {
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Case Hardened Item",
      itemName: "Case Hardened Item",
      category: "weapon_skin",
      referencePrice: 25,
      marketCoverageCount: 2,
      volume7d: 120,
      scanPenaltyFlags: [],
      scanFreshness: { state: "fresh" }
    },
    {
      marketHashName: "Case Hardened Item",
      bestBuy: { source: "steam", grossPrice: 15 },
      bestSellNet: { source: "skinport", netPriceAfterFees: 14 },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 15,
        sellMarket: "skinport",
        sellNet: 14,
        profit: -1,
        spreadPercent: -6.7,
        opportunityScore: 40,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 25,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("non_positive_profit"), true)
})

test("opportunity evaluation rejects opportunities below $2 cost floor", () => {
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Low Cost Case",
      itemName: "Low Cost Case",
      category: "case",
      referencePrice: 1.8,
      marketCoverageCount: 2,
      volume7d: 220,
      scanPenaltyFlags: [],
      scanFreshness: { state: "fresh" }
    },
    {
      marketHashName: "Low Cost Case",
      bestBuy: { source: "steam", grossPrice: 1.7 },
      bestSellNet: { source: "skinport", netPriceAfterFees: 2.4 },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 1.7,
        sellMarket: "skinport",
        sellNet: 2.4,
        profit: 0.7,
        spreadPercent: 41.2,
        opportunityScore: 72,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 1.8,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.rejected, true)
  assert.equal(evaluation.hardRejectReasons.includes("below_min_cost_floor"), true)
})

test("feed event classifier detects new, updated, duplicate, and reactivated rows", () => {
  const baseOpportunity = {
    itemName: "AK-47 | Redline (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport",
    profit: 12,
    spread: 13,
    score: 85,
    executionConfidence: "High",
    liquidity: 220
  }

  const newEvent = classifyOpportunityFeedEvent(baseOpportunity, null)
  assert.equal(newEvent.eventType, "new")

  const updatedEvent = classifyOpportunityFeedEvent(baseOpportunity, {
    is_active: true,
    profit: 8,
    spread_pct: 8,
    opportunity_score: 66,
    execution_confidence: "Low",
    metadata: { liquidity_value: 100 }
  })
  assert.equal(updatedEvent.eventType, "updated")
  assert.equal(updatedEvent.changeReasons.includes("score"), true)

  const duplicateEvent = classifyOpportunityFeedEvent(baseOpportunity, {
    is_active: true,
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  })
  assert.equal(duplicateEvent.eventType, "duplicate")
  assert.equal(isMateriallyNewOpportunity(baseOpportunity, {
    is_active: true,
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  }), false)

  const reactivatedEvent = classifyOpportunityFeedEvent(baseOpportunity, {
    is_active: false,
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  })
  assert.equal(reactivatedEvent.eventType, "reactivated")
})

test("feed mapper keeps core scanner fields stable", () => {
  const insertRow = buildFeedInsertRow(
    {
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
      flags: ["missing_liquidity"],
      badges: ["Risk-adjusted"],
      tier: "risky",
      isHighConfidenceEligible: false,
      isRiskyEligible: true
    },
    {
      scanRunId: "123e4567-e89b-12d3-a456-426614174000",
      detectedAt: "2026-03-19T10:00:00.000Z"
    }
  )
  const mapped = mapFeedRowToApiRow({
    id: "feed-row-1",
    ...insertRow
  })

  assert.equal(mapped.itemName, "AK-47 | Redline (Field-Tested)")
  assert.equal(mapped.itemCategory, "weapon_skin")
  assert.equal(mapped.buyMarket, "steam")
  assert.equal(mapped.sellMarket, "skinport")
  assert.equal(mapped.score, 80)
  assert.equal(mapped.qualityScoreDisplay, 80)
  assert.equal(mapped.quality_score_display, 80)
  assert.equal(mapped.isRiskyEligible, true)
  assert.equal(mapped.isHighConfidenceEligible, false)
})

test("display score soft-normalizes visible feed values without changing raw score ordering inputs", () => {
  const mappedRows = [0, 10, 40, 70, 85, 100].map((score) =>
    mapFeedRowToApiRow({
      opportunity_score: score,
      metadata: {}
    })
  )

  const displayScores = mappedRows.map((row) => Number(row.qualityScoreDisplay))

  assert.equal(displayScores[0] >= 15, true)
  assert.equal(displayScores[displayScores.length - 1] < 100, true)
  assert.equal(displayScores[4], 85)

  for (let index = 1; index < displayScores.length; index += 1) {
    assert.equal(displayScores[index] > displayScores[index - 1], true)
  }
})

test("overdue watchdog tolerates active running scanner and flags stale completion", () => {
  const now = Date.parse("2026-03-19T12:00:00.000Z")
  assert.equal(
    isScannerRunOverdue(
      {
        latestRun: { status: "running", started_at: "2026-03-19T11:59:00.000Z" },
        latestCompletedRun: { completed_at: "2026-03-19T11:55:00.000Z" }
      },
      now
    ),
    false
  )
  assert.equal(
    isScannerRunOverdue(
      {
        latestRun: { status: "completed", started_at: "2026-03-19T11:30:00.000Z" },
        latestCompletedRun: { completed_at: "2026-03-19T11:30:00.000Z" }
      },
      now
    ),
    true
  )
})

test("scanner defaults are tuned for batch-first throughput", () => {
  assert.equal(DEFAULT_UNIVERSE_LIMIT >= 500, true)
  assert.equal(OPPORTUNITY_BATCH_TARGET >= 20, true)
  assert.equal(SCAN_CHUNK_SIZE >= 10, true)
  assert.equal(SCAN_TIMEOUT_PER_BATCH_MS >= 1000, true)
})

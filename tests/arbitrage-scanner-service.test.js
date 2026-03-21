const test = require("node:test")
const assert = require("node:assert/strict")
const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")
const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")

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
    buildOpportunityFingerprint,
    buildMaterialChangeHash,
    classifyOpportunityFeedEvent,
    isMateriallyNewOpportunity,
    buildFeedInsertRow,
    mapFeedRowToApiRow,
    mapFeedRowToCard,
    dedupeFeedCards,
    loadScannerSourceRows,
    persistFeedRows,
    normalizeCursorPayload,
    encodeCursorPayload,
    buildFeedPageCacheKey,
    clearFeedFirstPageCache,
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

test("feed cursor helpers round-trip created_at + id payload", () => {
  clearFeedFirstPageCache()
  const cursor = encodeCursorPayload(
    "2026-03-21T10:15:30.000Z",
    "00000000-0000-0000-0000-000000000777"
  )
  assert.equal(Boolean(cursor), true)

  const decoded = normalizeCursorPayload(cursor)
  assert.equal(Boolean(decoded), true)
  assert.equal(decoded.createdAt, "2026-03-21T10:15:30.000Z")
  assert.equal(decoded.id, "00000000-0000-0000-0000-000000000777")
  assert.equal(normalizeCursorPayload("not-a-valid-cursor"), null)
})

test("feed cache key normalization is stable across category aliases", () => {
  const a = buildFeedPageCacheKey({
    includeInactive: false,
    highConfidenceOnly: false,
    category: "skins"
  })
  const b = buildFeedPageCacheKey({
    includeInactive: false,
    highConfidenceOnly: false,
    category: "weapon_skin"
  })
  assert.equal(a, b)
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

test("scanner source loader backfills missing categories from candidate pool", async () => {
  const originals = {
    listScannerSource: marketSourceCatalogRepo.listScannerSource,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  marketSourceCatalogRepo.listScannerSource = async () => [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      item_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true
    }
  ]
  marketSourceCatalogRepo.listCandidatePool = async () => [
    {
      market_hash_name: "Revolution Case",
      item_name: "Revolution Case",
      category: "case",
      tradable: true,
      is_active: true
    },
    {
      market_hash_name: "Stockholm 2021 Contenders Sticker Capsule",
      item_name: "Stockholm 2021 Contenders Sticker Capsule",
      category: "sticker_capsule",
      tradable: true,
      is_active: true
    },
    {
      market_hash_name: "★ Karambit | Doppler (Factory New)",
      item_name: "★ Karambit | Doppler (Factory New)",
      category: "knife",
      tradable: true,
      is_active: true
    },
    {
      market_hash_name: "★ Sport Gloves | Vice (Field-Tested)",
      item_name: "★ Sport Gloves | Vice (Field-Tested)",
      category: "glove",
      tradable: true,
      is_active: true
    }
  ]
  marketSourceCatalogRepo.listActiveTradable = async () => []

  try {
    const loaded = await loadScannerSourceRows()
    const byCategory = {}
    for (const row of loaded.rows || []) {
      byCategory[row.category] = Number(byCategory[row.category] || 0) + 1
    }
    assert.equal(Number(byCategory.weapon_skin || 0) >= 1, true)
    assert.equal(Number(byCategory.case || 0) >= 1, true)
    assert.equal(Number(byCategory.sticker_capsule || 0) >= 1, true)
    assert.equal(Number(byCategory.knife || 0) >= 1, true)
    assert.equal(Number(byCategory.glove || 0) >= 1, true)
    assert.equal(loaded?.diagnostics?.topup?.candidatePoolAttempted, true)
    assert.equal(Number(loaded?.diagnostics?.topup?.candidatePoolRowsAdded || 0) >= 4, true)
    assert.deepEqual(loaded?.diagnostics?.missingCategoriesAfterTopup || [], [])
  } finally {
    marketSourceCatalogRepo.listScannerSource = originals.listScannerSource
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
})

test("scanner source loader uses active tradable fallback when candidate pool fails", async () => {
  const originals = {
    listScannerSource: marketSourceCatalogRepo.listScannerSource,
    listCandidatePool: marketSourceCatalogRepo.listCandidatePool,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable
  }

  marketSourceCatalogRepo.listScannerSource = async () => [
    {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      item_name: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true
    }
  ]
  marketSourceCatalogRepo.listCandidatePool = async () => {
    throw new Error("candidate_pool_failed")
  }
  marketSourceCatalogRepo.listActiveTradable = async () => [
    {
      market_hash_name: "★ Broken Fang Gloves | Jade (Field-Tested)",
      item_name: "★ Broken Fang Gloves | Jade (Field-Tested)",
      category: "glove",
      tradable: true,
      is_active: true
    }
  ]

  try {
    const loaded = await loadScannerSourceRows()
    const hasGlove = (loaded.rows || []).some((row) => row?.category === "glove")
    assert.equal(hasGlove, true)
    assert.equal(loaded?.diagnostics?.topup?.candidatePoolAttempted, true)
    assert.equal(loaded?.diagnostics?.topup?.candidatePoolFailed, true)
    assert.equal(loaded?.diagnostics?.topup?.activeTradableAttempted, true)
    assert.equal(Number(loaded?.diagnostics?.topup?.activeTradableRowsAdded || 0) >= 1, true)
  } finally {
    marketSourceCatalogRepo.listScannerSource = originals.listScannerSource
    marketSourceCatalogRepo.listCandidatePool = originals.listCandidatePool
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
  }
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
  assert.equal(evaluation.penaltyFlags.includes("low_sales_liquidity"), true)
})

test("opportunity diagnostics avoid contradictory tags for high-coverage fresh cases", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "Revolution Case",
      itemName: "Revolution Case",
      category: "case",
      referencePrice: 2.05,
      marketCoverageCount: 4,
      volume7d: 4039,
      snapshotCapturedAt: nowIso,
      quoteFetchedAt: nowIso
    },
    {
      marketHashName: "Revolution Case",
      perMarket: [
        { source: "steam", available: true, grossPrice: 1.88, netPriceAfterFees: 1.64, updatedAt: nowIso, volume7d: 4039 },
        { source: "skinport", available: true, grossPrice: 1.92, netPriceAfterFees: 1.69, updatedAt: nowIso, volume7d: 1880 },
        { source: "dmarket", available: true, grossPrice: 1.9, netPriceAfterFees: 1.77, updatedAt: nowIso, volume7d: 640 },
        { source: "csfloat", available: true, grossPrice: 1.94, netPriceAfterFees: 1.9, updatedAt: nowIso, volume7d: 320 }
      ],
      bestBuy: { source: "steam", grossPrice: 1.88, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "csfloat", netPriceAfterFees: 2.26, url: "https://csfloat.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 1.88,
        sellMarket: "csfloat",
        sellNet: 2.26,
        profit: 0.38,
        spreadPercent: 20.21,
        opportunityScore: 74,
        executionConfidence: "Medium",
        marketCoverage: 4,
        referencePrice: 2.05,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.penaltyFlags.includes("limited_market_coverage"), false)
  assert.equal(evaluation.penaltyFlags.includes("low_sales_liquidity"), false)
  assert.equal(evaluation.penaltyFlags.includes("stale_market_signal"), false)
  assert.equal(evaluation.marketCoverageBand, "High")
  assert.equal(
    Number(evaluation?.metadata?.diagnostics_debug?.market_coverage_score || 0) >= 80,
    true
  )
})

test("opportunity diagnostics expose dimension debug output", () => {
  const nowIso = new Date().toISOString()
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      itemName: "AK-47 | Redline (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 10.2,
      marketCoverageCount: 2,
      volume7d: 80,
      snapshotCapturedAt: nowIso,
      quoteFetchedAt: nowIso
    },
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      perMarket: [
        { source: "steam", available: true, grossPrice: 8.2, netPriceAfterFees: 7.13, updatedAt: nowIso, volume7d: 80 },
        { source: "skinport", available: true, grossPrice: 8.9, netPriceAfterFees: 7.83, updatedAt: nowIso, volume7d: 34 }
      ],
      bestBuy: { source: "steam", grossPrice: 8.2, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 9.45, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 8.2,
        sellMarket: "skinport",
        sellNet: 9.45,
        profit: 1.25,
        spreadPercent: 15.24,
        opportunityScore: 73,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 10.2,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  const debug = evaluation?.metadata?.diagnostics_debug || {}
  assert.equal(Number.isFinite(Number(debug.sales_liquidity_score)), true)
  assert.equal(Number.isFinite(Number(debug.executable_depth_score)), true)
  assert.equal(Number.isFinite(Number(debug.market_coverage_score)), true)
  assert.equal(Number.isFinite(Number(debug.data_freshness_score)), true)
  assert.equal(typeof debug.category, "string")
  assert.equal(Boolean(debug.latest_quote_at), true)
  assert.equal(Boolean(debug.latest_snapshot_at), true)
  assert.equal(Boolean(debug.latest_reference_price_at), true)
  assert.equal(Boolean(debug.latest_market_signal_at), true)
  assert.equal(Number.isFinite(Number(debug.stale_threshold_used)), true)
  assert.equal(typeof debug.stale_result, "boolean")
  assert.equal(typeof debug.stale_reason_source, "string")
  assert.equal(Array.isArray(debug?.raw_reasons?.sales_liquidity), true)
  assert.equal(Array.isArray(debug?.raw_reasons?.emitted_tags), true)
})

test("fresh quote inside SLA prevents stale market signal even with older fallback timestamps", () => {
  const oldIso = new Date(Date.now() - 8 * 60 * 60 * 1000).toISOString()
  const freshQuoteSeconds = Math.floor(Date.now() / 1000)
  const evaluation = evaluateCandidateOpportunity(
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      itemName: "AK-47 | Slate (Field-Tested)",
      category: "weapon_skin",
      referencePrice: 8.4,
      marketCoverageCount: 2,
      volume7d: 96,
      snapshotCapturedAt: oldIso,
      quoteFetchedAt: freshQuoteSeconds,
      last_market_signal_at: oldIso
    },
    {
      marketHashName: "AK-47 | Slate (Field-Tested)",
      perMarket: [
        { source: "steam", available: true, grossPrice: 6.8, netPriceAfterFees: 5.9, updatedAt: oldIso, volume7d: 96 },
        { source: "skinport", available: true, grossPrice: 7.1, netPriceAfterFees: 6.3, updatedAt: oldIso, volume7d: 45 }
      ],
      bestBuy: { source: "steam", grossPrice: 6.8, url: "https://steamcommunity.com/item" },
      bestSellNet: { source: "skinport", netPriceAfterFees: 7.9, url: "https://skinport.com/item" },
      arbitrage: {
        buyMarket: "steam",
        buyPrice: 6.8,
        sellMarket: "skinport",
        sellNet: 7.9,
        profit: 1.1,
        spreadPercent: 16.17,
        opportunityScore: 70,
        executionConfidence: "Medium",
        marketCoverage: 2,
        referencePrice: 8.4,
        depthFlags: [],
        antiFake: { reasons: [] }
      }
    }
  )

  assert.equal(evaluation.penaltyFlags.includes("stale_market_signal"), false)
  assert.equal(evaluation.badges.includes("Stale market signal"), false)
  assert.equal(Boolean(evaluation?.metadata?.latest_market_signal_at), true)
  assert.equal(evaluation?.metadata?.stale_result, false)
  assert.equal(String(evaluation?.metadata?.stale_reason_source || "").includes("latest_quote"), true)
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
    buy_market: "steam",
    sell_market: "skinport",
    profit: 12,
    spread_pct: 13,
    opportunity_score: 85,
    execution_confidence: "High",
    metadata: { liquidity_value: 220 }
  })
  assert.equal(duplicateEvent.eventType, "duplicate")
  assert.equal(isMateriallyNewOpportunity(baseOpportunity, {
    is_active: true,
    buy_market: "steam",
    sell_market: "skinport",
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

test("feed event classifier marks diagnostics-only flag changes as updated", () => {
  const opportunity = {
    itemName: "AWP | Asiimov (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport",
    profit: 3.2,
    spread: 12.1,
    score: 69,
    executionConfidence: "Medium",
    liquidity: 180,
    flags: ["stale_market_signal"],
    badges: ["Stale market signal"]
  }
  const previousRow = {
    is_active: true,
    profit: 3.2,
    spread_pct: 12.1,
    opportunity_score: 69,
    execution_confidence: "Medium",
    metadata: {
      liquidity_value: 180,
      flags: ["limited_market_coverage"],
      badges: ["Limited market coverage"]
    }
  }
  const event = classifyOpportunityFeedEvent(opportunity, previousRow)
  assert.equal(event.eventType, "updated")
  assert.equal(event.changeReasons.includes("diagnostics"), true)
})

test("feed event classifier treats canonical freshness changes as updates", () => {
  const nowIso = new Date().toISOString()
  const oldIso = new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString()
  const opportunity = {
    itemName: "AWP | Asiimov (Field-Tested)",
    buyMarket: "steam",
    sellMarket: "skinport",
    profit: 3.2,
    spread: 12.1,
    score: 69,
    executionConfidence: "Medium",
    liquidity: 180,
    flags: ["limited_market_coverage"],
    badges: ["Limited market coverage"],
    metadata: {
      latest_market_signal_at: nowIso,
      stale_result: false,
      stale_threshold_used: 120
    }
  }
  const previousRow = {
    is_active: true,
    profit: 3.2,
    spread_pct: 12.1,
    opportunity_score: 69,
    execution_confidence: "Medium",
    metadata: {
      liquidity_value: 180,
      flags: ["limited_market_coverage"],
      badges: ["Limited market coverage"],
      latest_market_signal_at: oldIso,
      stale_result: true,
      stale_threshold_used: 120
    }
  }
  const event = classifyOpportunityFeedEvent(opportunity, previousRow)
  assert.equal(event.eventType, "updated")
  assert.equal(event.changeReasons.includes("freshness"), true)
})

test("opportunity fingerprint changes when listing identity changes", () => {
  const base = {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 12.1,
    sellMarket: "skinport",
    sellNet: 13.5,
    metadata: {
      skinport_listing_id: "sp-001",
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested"
    }
  }
  const changedListing = {
    ...base,
    metadata: {
      ...base.metadata,
      skinport_listing_id: "sp-002"
    }
  }

  const fpA = buildOpportunityFingerprint(base)
  const fpB = buildOpportunityFingerprint(changedListing)
  assert.equal(Boolean(fpA), true)
  assert.equal(Boolean(fpB), true)
  assert.notEqual(fpA, fpB)
})

test("material hash captures meaningful path/quality changes", () => {
  const a = buildMaterialChangeHash({
    buyMarket: "steam",
    sellMarket: "skinport",
    buyPrice: 10,
    sellNet: 12,
    profit: 2,
    spread: 20,
    qualityGrade: "RISKY",
    executionConfidence: "Medium",
    verdict: "watch",
    metadata: {
      skinport_listing_id: "sp-1"
    }
  })
  const b = buildMaterialChangeHash({
    buyMarket: "steam",
    sellMarket: "skinport",
    buyPrice: 10.01,
    sellNet: 12.01,
    profit: 2.01,
    spread: 20.05,
    qualityGrade: "RISKY",
    executionConfidence: "Medium",
    verdict: "watch",
    metadata: {
      skinport_listing_id: "sp-1"
    }
  })
  const c = buildMaterialChangeHash({
    buyMarket: "steam",
    sellMarket: "dmarket",
    buyPrice: 10,
    sellNet: 12,
    profit: 2,
    spread: 20,
    qualityGrade: "STRONG",
    executionConfidence: "High",
    verdict: "strong_buy",
    metadata: {
      skinport_listing_id: "sp-1"
    }
  })

  assert.equal(Boolean(a), true)
  assert.equal(Boolean(b), true)
  assert.equal(Boolean(c), true)
  assert.equal(a, b)
  assert.notEqual(a, c)
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
      latestMarketSignalAt: "2026-03-19T09:58:00.000Z",
      latestQuoteAt: "2026-03-19T09:58:00.000Z",
      latestSnapshotAt: "2026-03-19T09:56:00.000Z",
      latestReferencePriceAt: "2026-03-19T09:58:00.000Z",
      staleThresholdUsed: 120,
      staleResult: false,
      staleReasonSource: "latest_quote:catalog",
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
  assert.equal(mapped.latestMarketSignalAt, "2026-03-19T09:58:00.000Z")
  assert.equal(mapped.latestQuoteAt, "2026-03-19T09:58:00.000Z")
  assert.equal(mapped.latestSnapshotAt, "2026-03-19T09:56:00.000Z")
  assert.equal(mapped.latestReferencePriceAt, "2026-03-19T09:58:00.000Z")
  assert.equal(mapped.staleResult, false)
  assert.equal(mapped.staleReasonSource, "latest_quote:catalog")
  assert.equal(Boolean(insertRow.opportunity_fingerprint), true)
  assert.equal(Boolean(insertRow.material_change_hash), true)
  assert.equal(insertRow.times_seen, 1)
  assert.equal(Boolean(mapped.opportunityFingerprint), true)
  assert.equal(Boolean(mapped.materialChangeHash), true)
  assert.equal(mapped.timesSeen, 1)
})

test("feed mapper reads FEED_CARD alias fields when metadata object is absent", () => {
  const mapped = mapFeedRowToApiRow({
    id: "feed-row-aliased-1",
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
    liquidity_label: "High",
    volume_7d: "4039",
    market_coverage: "4",
    reference_price: "11.2",
    item_id: "91",
    item_subcategory: "rifle",
    item_rarity: "Classified",
    item_rarity_color: "#d32ce6",
    item_image_url: "https://cdn.example.com/redline.png"
  })

  assert.equal(mapped.volume7d, 4039)
  assert.equal(mapped.liquidity, 4039)
  assert.equal(mapped.marketCoverage, 4)
  assert.equal(mapped.referencePrice, 11.2)
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

test("feed card dedupe keeps newest row per fingerprint and preserves stronger counters", () => {
  const deduped = dedupeFeedCards([
    {
      feedId: "row-new",
      itemName: "AK-47 | Redline (Field-Tested)",
      buyMarket: "steam",
      sellMarket: "skinport",
      opportunityFingerprint: "ofp_same",
      materialChangeHash: "mch_same",
      detectedAt: "2026-03-21T10:00:00.000Z",
      lastSeenAt: "2026-03-21T10:00:00.000Z",
      timesSeen: 2
    },
    {
      feedId: "row-old",
      itemName: "AK-47 | Redline (Field-Tested)",
      buyMarket: "steam",
      sellMarket: "skinport",
      opportunityFingerprint: "ofp_same",
      materialChangeHash: "mch_same",
      detectedAt: "2026-03-21T09:00:00.000Z",
      lastSeenAt: "2026-03-21T09:00:00.000Z",
      timesSeen: 7
    }
  ])

  assert.equal(deduped.length, 1)
  assert.equal(deduped[0].feedId, "row-new")
  assert.equal(deduped[0].timesSeen, 7)
  assert.equal(deduped[0].times_seen, 7)
  assert.equal(deduped[0].lastSeenAt, "2026-03-21T10:00:00.000Z")
})

test("feed card dedupe keeps separate rows when fallback signature material hash differs", () => {
  const deduped = dedupeFeedCards([
    {
      feedId: "row-a",
      itemName: "★ Broken Fang Gloves | Jade (Field-Tested)",
      buyMarket: "csfloat",
      sellMarket: "steam",
      materialChangeHash: "mch_a"
    },
    {
      feedId: "row-b",
      itemName: "★ Broken Fang Gloves | Jade (Field-Tested)",
      buyMarket: "csfloat",
      sellMarket: "steam",
      materialChangeHash: "mch_b"
    }
  ])

  assert.equal(deduped.length, 2)
})

test("persistFeedRows updates active fingerprint match instead of inserting duplicate row", async () => {
  const opportunity = {
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
    metadata: {
      skinport_listing_id: "sp-live-1",
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested"
    }
  }
  const fingerprint = buildOpportunityFingerprint(opportunity)
  const materialHash = buildMaterialChangeHash(opportunity)
  const existingRow = {
    id: "feed-row-dup-1",
    item_name: opportunity.itemName,
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
    is_active: true,
    opportunity_fingerprint: fingerprint,
    material_change_hash: materialHash,
    times_seen: 2,
    first_seen_at: "2026-03-19T10:00:00.000Z",
    metadata: {
      liquidity_value: 150,
      skinport_listing_id: "sp-live-1",
      buy_url: "https://steamcommunity.com/market/listings/730/AK-47",
      sell_url: "https://skinport.com/item/ak-47-redline-field-tested",
      opportunity_fingerprint: fingerprint,
      material_change_hash: materialHash
    }
  }

  const originals = {
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan
  }

  let updateRowsPayload = null
  let insertRowsPayload = null

  arbitrageFeedRepo.getRecentRowsByItems = async () => [existingRow]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [existingRow]
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    updateRowsPayload = rows
    return rows.length
  }
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows.map((row, index) => ({ id: row?.id || `new-${index}` }))
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0

  try {
    const result = await persistFeedRows([opportunity], "scan-run-1")
    assert.equal(result.insertedCount, 0)
    assert.equal(result.newCount, 0)
    assert.equal(result.duplicateCount, 1)
    assert.equal(result.skippedUnchanged, 0)
    assert.equal(Array.isArray(updateRowsPayload), true)
    assert.equal(updateRowsPayload.length, 1)
    assert.equal(insertRowsPayload, null)
    assert.equal(updateRowsPayload[0].id, existingRow.id)
    assert.equal(
      Number(updateRowsPayload[0]?.patch?.times_seen || 0) >= 3,
      true
    )
    assert.equal(Boolean(updateRowsPayload[0]?.patch?.last_seen_at), true)
    assert.equal(
      updateRowsPayload[0]?.patch?.opportunity_fingerprint,
      fingerprint
    )
  } finally {
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
  }
})

test("persistFeedRows deactivates older active duplicate rows for same fingerprint before update", async () => {
  const opportunity = {
    marketHashName: "★ Broken Fang Gloves | Jade (Field-Tested)",
    itemName: "★ Broken Fang Gloves | Jade (Field-Tested)",
    itemCategory: "glove",
    buyMarket: "csfloat",
    buyPrice: 97.67,
    sellMarket: "steam",
    sellNet: 109.39,
    profit: 11.72,
    spread: 11.99,
    score: 43,
    executionConfidence: "Low",
    qualityGrade: "RISKY",
    liquidityBand: "High",
    liquidity: 500,
    marketCoverage: 2,
    referencePrice: 103.2,
    metadata: {
      buy_url: "https://csfloat.com/search?market_hash_name=Broken+Fang+Gloves+Jade",
      sell_url:
        "https://steamcommunity.com/market/listings/730/%E2%98%85%20Broken%20Fang%20Gloves%20%7C%20Jade%20(Field-Tested)"
    }
  }
  const fingerprint = buildOpportunityFingerprint(opportunity)
  const materialHash = buildMaterialChangeHash(opportunity)
  const existingNewest = {
    id: "feed-row-newest",
    item_name: opportunity.itemName,
    category: "glove",
    buy_market: "csfloat",
    buy_price: 97.67,
    sell_market: "steam",
    sell_net: 109.39,
    profit: 11.72,
    spread_pct: 11.99,
    opportunity_score: 43,
    execution_confidence: "Low",
    quality_grade: "RISKY",
    is_active: true,
    opportunity_fingerprint: fingerprint,
    material_change_hash: materialHash,
    times_seen: 3,
    detected_at: "2026-03-21T10:00:00.000Z",
    last_seen_at: "2026-03-21T10:00:00.000Z",
    first_seen_at: "2026-03-20T10:00:00.000Z",
    metadata: {
      opportunity_fingerprint: fingerprint,
      material_change_hash: materialHash
    }
  }
  const existingOlder = {
    ...existingNewest,
    id: "feed-row-older",
    times_seen: 1,
    detected_at: "2026-03-21T09:00:00.000Z",
    last_seen_at: "2026-03-21T09:00:00.000Z"
  }

  const originals = {
    getRecentRowsByItems: arbitrageFeedRepo.getRecentRowsByItems,
    getActiveRowsByFingerprints: arbitrageFeedRepo.getActiveRowsByFingerprints,
    markRowsInactiveByIds: arbitrageFeedRepo.markRowsInactiveByIds,
    updateRowsById: arbitrageFeedRepo.updateRowsById,
    insertRows: arbitrageFeedRepo.insertRows,
    markInactiveOlderThan: arbitrageFeedRepo.markInactiveOlderThan
  }

  let markedInactiveIds = null
  let updateRowsPayload = null
  let insertRowsPayload = null

  arbitrageFeedRepo.getRecentRowsByItems = async () => [existingNewest, existingOlder]
  arbitrageFeedRepo.getActiveRowsByFingerprints = async () => [existingNewest, existingOlder]
  arbitrageFeedRepo.markRowsInactiveByIds = async (ids = []) => {
    markedInactiveIds = ids
    return ids.length
  }
  arbitrageFeedRepo.updateRowsById = async (rows = []) => {
    updateRowsPayload = rows
    return rows.length
  }
  arbitrageFeedRepo.insertRows = async (rows = []) => {
    insertRowsPayload = rows
    return rows.map((row, index) => ({ id: row?.id || `new-${index}` }))
  }
  arbitrageFeedRepo.markInactiveOlderThan = async () => 0

  try {
    const result = await persistFeedRows([opportunity], "scan-run-dup-cleanup")
    assert.deepEqual(markedInactiveIds, ["feed-row-older"])
    assert.equal(result.cleanup.duplicateActivesMarkedInactive, 1)
    assert.equal(Array.isArray(updateRowsPayload), true)
    assert.equal(updateRowsPayload.length, 1)
    assert.equal(updateRowsPayload[0].id, "feed-row-newest")
    assert.equal(insertRowsPayload, null)
  } finally {
    arbitrageFeedRepo.getRecentRowsByItems = originals.getRecentRowsByItems
    arbitrageFeedRepo.getActiveRowsByFingerprints = originals.getActiveRowsByFingerprints
    arbitrageFeedRepo.markRowsInactiveByIds = originals.markRowsInactiveByIds
    arbitrageFeedRepo.updateRowsById = originals.updateRowsById
    arbitrageFeedRepo.insertRows = originals.insertRows
    arbitrageFeedRepo.markInactiveOlderThan = originals.markInactiveOlderThan
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

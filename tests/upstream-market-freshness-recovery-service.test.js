const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const marketQuoteRepo = require("../src/repositories/marketQuoteRepository")
const marketSnapshotRepo = require("../src/repositories/marketSnapshotRepository")
const marketPriceRepo = require("../src/repositories/marketPriceRepository")
const skinRepo = require("../src/repositories/skinRepository")
const marketSourceCatalogService = require("../src/services/marketSourceCatalogService")
const marketService = require("../src/services/marketService")
const steamMarket = require("../src/markets/steam.market")
const skinportMarket = require("../src/markets/skinport.market")
const csfloatMarket = require("../src/markets/csfloat.market")
const dmarketMarket = require("../src/markets/dmarket.market")
const upstreamMarketFreshnessRecoveryService = require("../src/services/upstreamMarketFreshnessRecoveryService")

function buildRows(category, count, start = 0) {
  return Array.from({ length: count }, (_, index) => ({
    market_hash_name: `${category}-${start + index}`,
    category,
    tradable: true,
    is_active: true,
    volume_7d: 120,
    liquidity_rank: 55,
    last_market_signal_at: "2026-03-19T10:00:00.000Z"
  }))
}

function buildFreshCoverage(rows, iso) {
  return Object.fromEntries(
    rows.map((row) => [
      row.market_hash_name,
      {
        latestFetchedAt: iso,
        marketCoverageCount: 2,
        volume7dMax: 120
      }
    ])
  )
}

function buildSnapshotMap(rows, iso, source = "steam-market-overview+price-history") {
  return Object.fromEntries(
    rows.map((row, index) => [
      index + 1,
      {
        skin_id: index + 1,
        captured_at: iso,
        source
      }
    ])
  )
}

function buildCatalogRecomputeDiagnostics({
  scannableRowsByCategory = {},
  eligibleTradableRowsByCategory = {},
  nearEligibleRowsByCategory = {},
  generatedAt = new Date().toISOString()
} = {}) {
  const categories = ["weapon_skin", "case", "sticker_capsule"]
  const byCategory = Object.fromEntries(
    categories.map((category) => [
      category,
      {
        scannable: Number(scannableRowsByCategory?.[category] || 0),
        eligible: Number(eligibleTradableRowsByCategory?.[category] || 0),
        nearEligible: Number(nearEligibleRowsByCategory?.[category] || 0),
        shadow: 0,
        blocked: 0
      }
    ])
  )
  const totalScannable = Object.values(scannableRowsByCategory).reduce(
    (sum, value) => sum + Number(value || 0),
    0
  )
  const totalEligibleTradable = Object.values(eligibleTradableRowsByCategory).reduce(
    (sum, value) => sum + Number(value || 0),
    0
  )
  const totalNearEligible = Object.values(nearEligibleRowsByCategory).reduce(
    (sum, value) => sum + Number(value || 0),
    0
  )

  return {
    generatedAt,
    sourceCatalog: {
      scannable: totalScannable,
      shadow: 0,
      blocked: 0,
      eligibleTradableRows: totalEligibleTradable,
      nearEligibleRows: totalNearEligible,
      eligibleRows: totalEligibleTradable,
      scanner_source_size: totalScannable,
      byCategory,
      eligibleRowsByCategory: Object.fromEntries(
        categories.map((category) => [category, Number(eligibleTradableRowsByCategory?.[category] || 0)])
      ),
      nearEligibleRowsByCategory: Object.fromEntries(
        categories.map((category) => [category, Number(nearEligibleRowsByCategory?.[category] || 0)])
      ),
      scannerSourceSizeByCategory: Object.fromEntries(
        categories.map((category) => [category, Number(scannableRowsByCategory?.[category] || 0)])
      )
    }
  }
}

function createListActiveTradableStub(rowsByCategory = {}) {
  return async ({ categories, limit, offset = 0 }) => {
    const category = Array.isArray(categories) ? categories[0] : ""
    const rows = Array.isArray(rowsByCategory[category]) ? rowsByCategory[category] : []
    return rows.slice(offset, offset + limit)
  }
}

test("runFreshnessRecovery refreshes quotes and snapshots before forced catalog recompute", async () => {
  const weaponRows = buildRows("weapon_skin", 50)
  const caseRows = buildRows("case", 5)
  const stickerRows = buildRows("sticker_capsule", 5)
  const allRows = [...weaponRows, ...caseRows, ...stickerRows]
  const skins = allRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const staleIso = "2026-03-19T10:00:00.000Z"
  const freshIso = new Date().toISOString()

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  let quoteLookupCount = 0
  let snapshotLookupCount = 0
  const insertedQuoteRows = []
  const upsertedPriceRows = []
  let prepareArgs = null

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows,
    case: caseRows,
    sticker_capsule: stickerRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async () => {
    quoteLookupCount += 1
    return quoteLookupCount === 1 ? buildFreshCoverage(allRows, staleIso) : buildFreshCoverage(allRows, freshIso)
  }
  marketSnapshotRepo.getLatestBySkinIds = async () => {
    snapshotLookupCount += 1
    return snapshotLookupCount === 1
      ? buildSnapshotMap(weaponRows, staleIso)
      : buildSnapshotMap(weaponRows, freshIso)
  }
  marketQuoteRepo.insertRows = async (rows = []) => {
    insertedQuoteRows.push(...rows)
    return rows.length
  }
  marketPriceRepo.upsertRows = async (rows = []) => {
    upsertedPriceRows.push(...rows)
    return rows.length
  }
  skinRepo.getByMarketHashNames = async () => skins
  marketService.refreshSnapshotsForSkins = async (rows = []) =>
    rows.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: true,
      snapshot: {
        skin_id: skin.id,
        captured_at: freshIso,
        source: "steam-market-overview+price-history"
      }
    }))
  marketSourceCatalogService.prepareSourceCatalog = async (options = {}) => {
    prepareArgs = options
    return buildCatalogRecomputeDiagnostics({
      generatedAt: freshIso,
      scannableRowsByCategory: {
        weapon_skin: 50,
        case: 6,
        sticker_capsule: 5
      },
      eligibleTradableRowsByCategory: {
        weapon_skin: 12,
        case: 3,
        sticker_capsule: 3
      },
      nearEligibleRowsByCategory: {
        weapon_skin: 5,
        case: 1,
        sticker_capsule: 1
      }
    })
  }

  const buildRecords = (items = [], source) =>
    Object.fromEntries(
      items.map((item, index) => [
        item.marketHashName,
        {
          source,
          marketHashName: item.marketHashName,
          grossPrice: 10 + index,
          netPriceAfterFees: 9 + index,
          currency: "USD",
          url: `https://example.com/${encodeURIComponent(item.marketHashName)}`,
          updatedAt: freshIso,
          confidence: "high",
          raw: {}
        }
      ])
    )
  steamMarket.batchGetPrices = async (items = []) => buildRecords(items, "steam")
  skinportMarket.batchGetPrices = async (items = []) => buildRecords(items, "skinport")
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      limit: 200,
      quoteBatchSize: 200,
      snapshotBatchSize: 200
    })

    assert.equal(diagnostics.healthGate.healthyEnough, true)
    assert.deepEqual(diagnostics.categoryHealthGate.healthyCategories, [
      "weapon_skin",
      "case",
      "sticker_capsule"
    ])
    assert.equal(diagnostics.catalogRecompute.executed, true)
    assert.equal(diagnostics.catalogRecompute.recomputeMode, "full")
    assert.deepEqual(diagnostics.catalogRecompute.recomputedCategories, [
      "weapon_skin",
      "case",
      "sticker_capsule"
    ])
    assert.equal(diagnostics.postRefresh.byCategory.weapon_skin.quote.coverageReady >= 40, true)
    assert.equal(diagnostics.postRefresh.byCategory.weapon_skin.snapshot.fresh >= 25, true)
    assert.equal(diagnostics.catalogRecompute.scannableRows, 61)
    assert.equal(diagnostics.catalogRecompute.scannableRowsByCategory.weapon_skin, 50)
    assert.equal(diagnostics.catalogRecompute.opportunityScanSafeToResume, true)
    assert.deepEqual(diagnostics.catalogRecompute.opportunityScanResumeCategories, [
      "weapon_skin",
      "case",
      "sticker_capsule"
    ])
    assert.deepEqual(prepareArgs?.categories, ["weapon_skin", "case", "sticker_capsule"])
    assert.equal(insertedQuoteRows.length > 0, true)
    assert.equal(upsertedPriceRows.length > 0, true)
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery skips recompute when upstream freshness is still unhealthy", async () => {
  const weaponRows = buildRows("weapon_skin", 10)
  const caseRows = buildRows("case", 2)
  const stickerRows = buildRows("sticker_capsule", 2)
  const allRows = [...weaponRows, ...caseRows, ...stickerRows]
  const skins = allRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const staleIso = "2026-03-19T10:00:00.000Z"

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows,
    case: caseRows,
    sticker_capsule: stickerRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async () => buildFreshCoverage(allRows, staleIso)
  marketSnapshotRepo.getLatestBySkinIds = async () => buildSnapshotMap(weaponRows, staleIso)
  marketQuoteRepo.insertRows = async () => 0
  marketPriceRepo.upsertRows = async () => 0
  skinRepo.getByMarketHashNames = async () => skins
  marketService.refreshSnapshotsForSkins = async () => []
  marketSourceCatalogService.prepareSourceCatalog = async () => {
    throw new Error("should not recompute while unhealthy")
  }
  const buildRecords = (items = [], source) =>
    Object.fromEntries(
      items.map((item, index) => [
        item.marketHashName,
        {
          source,
          marketHashName: item.marketHashName,
          grossPrice: 10 + index,
          netPriceAfterFees: 9 + index,
          currency: "USD",
          url: `https://example.com/${encodeURIComponent(item.marketHashName)}`,
          updatedAt: freshIso,
          confidence: "high",
          raw: {}
        }
      ])
    )
  steamMarket.batchGetPrices = async (items = []) => buildRecords(items, "steam")
  skinportMarket.batchGetPrices = async (items = []) => buildRecords(items, "skinport")
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      limit: 200,
      quoteBatchSize: 200,
      snapshotBatchSize: 200
    })

    assert.equal(diagnostics.healthGate.healthyEnough, false)
    assert.equal(diagnostics.catalogRecompute.executed, false)
    assert.equal(
      diagnostics.healthGate.reasons.includes("weapon_skin:insufficient_fresh_quote_coverage"),
      true
    )
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery preserves earlier batch progress and returns a checkpoint on timeout", async () => {
  const weaponRows = buildRows("weapon_skin", 70)
  const caseRows = buildRows("case", 5)
  const stickerRows = buildRows("sticker_capsule", 5)
  const allRows = [...weaponRows, ...caseRows, ...stickerRows]
  const skins = allRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const staleIso = "2026-03-19T10:00:00.000Z"
  const freshIso = new Date().toISOString()

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  const insertedQuoteRows = []
  const upsertedPriceRows = []

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows,
    case: caseRows,
    sticker_capsule: stickerRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async (names = []) => {
    if (names.some((name) => name === "weapon_skin-30")) {
      const error = new Error("canceling statement due to statement timeout")
      error.code = "57014"
      throw error
    }
    return buildFreshCoverage(
      names.map((name) => ({ market_hash_name: name })),
      staleIso
    )
  }
  marketSnapshotRepo.getLatestBySkinIds = async (skinIds = []) =>
    Object.fromEntries(
      skinIds.map((skinId) => [
        skinId,
        {
          skin_id: skinId,
          captured_at: staleIso,
          source: "steam-market-overview+price-history"
        }
      ])
    )
  marketQuoteRepo.insertRows = async (rows = []) => {
    insertedQuoteRows.push(...rows)
    return rows.length
  }
  marketPriceRepo.upsertRows = async (rows = []) => {
    upsertedPriceRows.push(...rows)
    return rows.length
  }
  skinRepo.getByMarketHashNames = async (names = []) =>
    skins.filter((skin) => names.includes(skin.market_hash_name))
  marketService.refreshSnapshotsForSkins = async (rows = []) =>
    rows.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: true,
      snapshot: {
        skin_id: skin.id,
        captured_at: freshIso,
        source: "steam-market-overview+price-history"
      }
    }))
  marketSourceCatalogService.prepareSourceCatalog = async () => {
    throw new Error("should not recompute after timeout")
  }

  const buildRecords = (items = [], source) =>
    Object.fromEntries(
      items.map((item, index) => [
        item.marketHashName,
        {
          source,
          marketHashName: item.marketHashName,
          grossPrice: 10 + index,
          netPriceAfterFees: 9 + index,
          currency: "USD",
          url: `https://example.com/${encodeURIComponent(item.marketHashName)}`,
          updatedAt: freshIso,
          confidence: "high",
          raw: {}
        }
      ])
    )
  steamMarket.batchGetPrices = async (items = []) => buildRecords(items, "steam")
  skinportMarket.batchGetPrices = async (items = []) => buildRecords(items, "skinport")
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      limit: 120,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10
    })

    assert.equal(diagnostics.completed, false)
    assert.equal(diagnostics.timedOut, true)
    assert.equal(diagnostics.failedStage, "quote_refresh_selection")
    assert.equal(diagnostics.checkpoint.nextCategory, "weapon_skin")
    assert.equal(diagnostics.checkpoint.nextOffset, 30)
    assert.equal(diagnostics.catalogRecompute.executed, false)
    assert.equal(insertedQuoteRows.length > 0, true)
    assert.equal(upsertedPriceRows.length > 0, true)
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery can pause after a limited number of batches and expose a resume checkpoint", async () => {
  const weaponRows = buildRows("weapon_skin", 70)
  const allRows = [...weaponRows]
  const skins = allRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const staleIso = "2026-03-19T10:00:00.000Z"
  const freshIso = new Date().toISOString()

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async (names = []) =>
    buildFreshCoverage(
      names.map((name) => ({ market_hash_name: name })),
      staleIso
    )
  marketSnapshotRepo.getLatestBySkinIds = async (skinIds = []) =>
    Object.fromEntries(
      skinIds.map((skinId) => [
        skinId,
        {
          skin_id: skinId,
          captured_at: staleIso,
          source: "steam-market-overview+price-history"
        }
      ])
    )
  marketQuoteRepo.insertRows = async (rows = []) => rows.length
  marketPriceRepo.upsertRows = async (rows = []) => rows.length
  skinRepo.getByMarketHashNames = async (names = []) =>
    skins.filter((skin) => names.includes(skin.market_hash_name))
  marketService.refreshSnapshotsForSkins = async (rows = []) =>
    rows.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: true,
      snapshot: {
        skin_id: skin.id,
        captured_at: freshIso,
        source: "steam-market-overview+price-history"
      }
    }))
  marketSourceCatalogService.prepareSourceCatalog = async () => {
    throw new Error("should not recompute while paused")
  }

  const buildRecords = (items = [], source) =>
    Object.fromEntries(
      items.map((item, index) => [
        item.marketHashName,
        {
          source,
          marketHashName: item.marketHashName,
          grossPrice: 10 + index,
          netPriceAfterFees: 9 + index,
          currency: "USD",
          url: `https://example.com/${encodeURIComponent(item.marketHashName)}`,
          updatedAt: freshIso,
          confidence: "high",
          raw: {}
        }
      ])
    )
  steamMarket.batchGetPrices = async (items = []) => buildRecords(items, "steam")
  skinportMarket.batchGetPrices = async (items = []) => buildRecords(items, "skinport")
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      categories: ["weapon_skin"],
      limit: 60,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10,
      maxBatches: 1
    })

    assert.equal(diagnostics.completed, false)
    assert.equal(diagnostics.paused, true)
    assert.equal(diagnostics.timedOut, false)
    assert.equal(diagnostics.failedStage, null)
    assert.equal(diagnostics.checkpoint.nextCategory, "weapon_skin")
    assert.equal(diagnostics.checkpoint.nextOffset, 30)
    assert.equal(diagnostics.catalogRecompute.executed, false)
    assert.deepEqual(diagnostics.checkpoint.resumeArgs.slice(0, 2), [
      "--start-category=weapon_skin",
      "--start-offset=30"
    ])
    assert.equal(
      diagnostics.checkpoint.resumeArgs[2].startsWith("--resume-state="),
      true
    )
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery reports snapshot failure reasons and keeps the health gate strict", async () => {
  const weaponRows = buildRows("weapon_skin", 30)
  const mappedSkins = weaponRows.slice(0, 20).map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const freshIso = new Date().toISOString()

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async (names = []) =>
    buildFreshCoverage(
      names.map((name) => ({ market_hash_name: name })),
      freshIso
    )
  marketSnapshotRepo.getLatestBySkinIds = async () => ({})
  marketQuoteRepo.insertRows = async () => 0
  marketPriceRepo.upsertRows = async () => 0
  skinRepo.getByMarketHashNames = async (names = []) =>
    mappedSkins.filter((skin) => names.includes(skin.market_hash_name))
  marketService.refreshSnapshotsForSkins = async (skins = []) =>
    skins.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: false,
      skippedFresh: false,
      snapshot: null,
      error: "Live market overview did not provide a usable price signal",
      refreshReason: "snapshot_live_overview_missing",
      errorCode: "SNAPSHOT_LIVE_OVERVIEW_MISSING",
      errorStatusCode: 502
    }))
  marketSourceCatalogService.prepareSourceCatalog = async () => {
    throw new Error("should not recompute while snapshots remain unhealthy")
  }
  steamMarket.batchGetPrices = async () => ({})
  skinportMarket.batchGetPrices = async () => ({})
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      categories: ["weapon_skin"],
      limit: 30,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10
    })

    assert.equal(diagnostics.completed, true)
    assert.equal(diagnostics.healthGate.healthyEnough, false)
    assert.equal(
      diagnostics.healthGate.reasons.includes("weapon_skin:insufficient_fresh_snapshots"),
      true
    )
    assert.equal(diagnostics.snapshotRefresh.failureReasons.snapshot_missing_skin_mapping, 10)
    assert.equal(diagnostics.snapshotRefresh.failureReasons.snapshot_live_overview_missing, 20)
    assert.equal(diagnostics.snapshotRefresh.dominantFailureReason, "snapshot_live_overview_missing")
    assert.equal(diagnostics.snapshotRefresh.failureClassification, "internal_or_strict")
    assert.equal(diagnostics.snapshotRefresh.retryLikelyHelpful, false)
    assert.equal(
      diagnostics.snapshotRefresh.byCategory.weapon_skin.reasons.snapshot_missing_skin_mapping,
      10
    )
    assert.equal(
      diagnostics.snapshotRefresh.byCategory.weapon_skin.reasons.snapshot_live_overview_missing,
      20
    )
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery applies snapshot cooldown after rate limiting and continues with other categories", async () => {
  const weaponRows = buildRows("weapon_skin", 30)
  const caseRows = buildRows("case", 30)
  const stickerRows = buildRows("sticker_capsule", 30)
  const allRows = [...weaponRows, ...caseRows, ...stickerRows]
  const mappedSkins = weaponRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const staleIso = "2026-03-19T10:00:00.000Z"
  const freshIso = new Date().toISOString()
  const nowMs = new Date(freshIso).getTime()
  let prepareArgs = null
  let quoteLookupCount = 0

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows,
    case: caseRows,
    sticker_capsule: stickerRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async (names = []) => {
    quoteLookupCount += 1
    return buildFreshCoverage(
      names.map((name) => ({ market_hash_name: name })),
      quoteLookupCount === 1 ? staleIso : freshIso
    )
  }
  marketSnapshotRepo.getLatestBySkinIds = async () => ({})
  marketQuoteRepo.insertRows = async () => 0
  marketPriceRepo.upsertRows = async () => 0
  skinRepo.getByMarketHashNames = async (names = []) =>
    mappedSkins.filter((skin) => names.includes(skin.market_hash_name))
  marketService.refreshSnapshotsForSkins = async (skins = []) =>
    skins.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: false,
      skippedFresh: false,
      snapshot: null,
      error: "Steam market rate limited",
      refreshReason: "snapshot_rate_limited",
      errorCode: "STEAM_MARKET_RATE_LIMITED",
      errorStatusCode: 429
    }))
  marketSourceCatalogService.prepareSourceCatalog = async (options = {}) => {
    prepareArgs = options
    return buildCatalogRecomputeDiagnostics({
      generatedAt: freshIso,
      scannableRowsByCategory: {
        weapon_skin: 0,
        case: 18,
        sticker_capsule: 14
      },
      eligibleTradableRowsByCategory: {
        weapon_skin: 0,
        case: 7,
        sticker_capsule: 5
      },
      nearEligibleRowsByCategory: {
        weapon_skin: 0,
        case: 3,
        sticker_capsule: 2
      }
    })
  }
  steamMarket.batchGetPrices = async () => ({})
  skinportMarket.batchGetPrices = async () => ({})
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      limit: 180,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10,
      nowMs
    })

    assert.equal(diagnostics.paused, true)
    assert.equal(diagnostics.pauseReason, "active_cooldown_retry_later")
    assert.equal(diagnostics.healthGate.healthyEnough, false)
    assert.equal(diagnostics.categoryHealthGate.byCategory.weapon_skin.healthyEnough, false)
    assert.equal(diagnostics.categoryHealthGate.byCategory.case.healthyEnough, true)
    assert.equal(diagnostics.categoryHealthGate.byCategory.sticker_capsule.healthyEnough, true)
    assert.equal(diagnostics.snapshotRefresh.failureReasons.snapshot_rate_limited > 0, true)
    assert.equal(diagnostics.snapshotRefresh.cooldownAppliedCount, 1)
    assert.equal(
      diagnostics.snapshotRefresh.byCategory.weapon_skin.pacing.cooldownActive,
      true
    )
    assert.equal(
      Boolean(diagnostics.snapshotRefresh.byCategory.weapon_skin.pacing.nextSafeRetryAt),
      true
    )
    assert.equal(diagnostics.processedRowsByCategory.case > 0, true)
    assert.equal(diagnostics.processedRowsByCategory.sticker_capsule > 0, true)
    assert.equal(diagnostics.checkpoint.categoryProgressState.weapon_skin.nextOffset, 0)
    assert.equal(diagnostics.checkpoint.categoryProgressState.case.done, true)
    assert.equal(diagnostics.checkpoint.categoryProgressState.sticker_capsule.done, true)
    assert.equal(diagnostics.catalogRecompute.executed, true)
    assert.equal(diagnostics.catalogRecompute.recomputeMode, "partial")
    assert.deepEqual(diagnostics.catalogRecompute.recomputedCategories, [
      "case",
      "sticker_capsule"
    ])
    assert.deepEqual(diagnostics.catalogRecompute.blockedCategories, ["weapon_skin"])
    assert.equal(diagnostics.catalogRecompute.scannableRowsByCategory.weapon_skin, 0)
    assert.equal(diagnostics.catalogRecompute.scannableRowsByCategory.case, 18)
    assert.equal(diagnostics.catalogRecompute.scannableRowsByCategory.sticker_capsule, 14)
    assert.equal(diagnostics.catalogRecompute.opportunityScanSafeToResume, true)
    assert.deepEqual(diagnostics.catalogRecompute.opportunityScanResumeCategories, [
      "case",
      "sticker_capsule"
    ])
    assert.deepEqual(prepareArgs?.categories, ["case", "sticker_capsule"])
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery skips snapshot retries while cooldown is active and preserves resume pacing state", async () => {
  const weaponRows = buildRows("weapon_skin", 30)
  const mappedSkins = weaponRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const freshIso = "2026-03-27T12:00:00.000Z"
  const nowMs = new Date(freshIso).getTime()

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  let snapshotCallCount = 0
  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async (names = []) =>
    buildFreshCoverage(
      names.map((name) => ({ market_hash_name: name })),
      freshIso
    )
  marketSnapshotRepo.getLatestBySkinIds = async () => ({})
  marketQuoteRepo.insertRows = async () => 0
  marketPriceRepo.upsertRows = async () => 0
  skinRepo.getByMarketHashNames = async (names = []) =>
    mappedSkins.filter((skin) => names.includes(skin.market_hash_name))
  marketService.refreshSnapshotsForSkins = async (skins = []) => {
    snapshotCallCount += 1
    return skins.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: false,
      skippedFresh: false,
      snapshot: null,
      error: "Steam market rate limited",
      refreshReason: "snapshot_rate_limited",
      errorCode: "STEAM_MARKET_RATE_LIMITED",
      errorStatusCode: 429
    }))
  }
  marketSourceCatalogService.prepareSourceCatalog = async () => {
    throw new Error("should not recompute while cooldown is active")
  }
  steamMarket.batchGetPrices = async () => ({})
  skinportMarket.batchGetPrices = async () => ({})
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const first = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      categories: ["weapon_skin"],
      limit: 180,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10,
      nowMs
    })
    assert.equal(first.pauseReason, "active_cooldown_retry_later")
    assert.equal(snapshotCallCount > 0, true)

    snapshotCallCount = 0
    const second = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      categories: ["weapon_skin"],
      limit: 180,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10,
      nowMs: nowMs + 60 * 1000,
      resumeState: first.checkpoint.resumeState
    })

    assert.equal(snapshotCallCount, 0)
    assert.equal(second.pauseReason, "active_cooldown_retry_later")
    assert.equal(second.snapshotRefresh.batchesSkippedDueToCooldown, 1)
    assert.equal(second.snapshotRefresh.retryTemporarilyBlocked, true)
    assert.equal(
      second.snapshotRefresh.byCategory.weapon_skin.pacing.retriesRemaining,
      first.snapshotRefresh.byCategory.weapon_skin.pacing.retriesRemaining
    )
    assert.equal(
      Boolean(second.checkpoint.snapshotPacingState.byCategory.weapon_skin.bySource.steam_market_overview.nextSafeRetryAt),
      true
    )
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

test("runFreshnessRecovery surfaces snapshot retry budget exhaustion clearly", async () => {
  const weaponRows = buildRows("weapon_skin", 30)
  const mappedSkins = weaponRows.map((row, index) => ({
    id: index + 1,
    market_hash_name: row.market_hash_name
  }))
  const freshIso = "2026-03-27T12:00:00.000Z"
  const nowMs = new Date(freshIso).getTime()

  const originals = {
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds,
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins,
    steamBatchGetPrices: steamMarket.batchGetPrices,
    skinportBatchGetPrices: skinportMarket.batchGetPrices,
    csfloatBatchGetPrices: csfloatMarket.batchGetPrices,
    dmarketBatchGetPrices: dmarketMarket.batchGetPrices
  }

  marketSourceCatalogRepo.listActiveTradable = createListActiveTradableStub({
    weapon_skin: weaponRows
  })
  marketQuoteRepo.getLatestCoverageByItemNames = async (names = []) =>
    buildFreshCoverage(
      names.map((name) => ({ market_hash_name: name })),
      freshIso
    )
  marketSnapshotRepo.getLatestBySkinIds = async () => ({})
  marketQuoteRepo.insertRows = async () => 0
  marketPriceRepo.upsertRows = async () => 0
  skinRepo.getByMarketHashNames = async (names = []) =>
    mappedSkins.filter((skin) => names.includes(skin.market_hash_name))
  marketService.refreshSnapshotsForSkins = async (skins = []) =>
    skins.map((skin) => ({
      skinId: skin.id,
      marketHashName: skin.market_hash_name,
      refreshed: false,
      skippedFresh: false,
      snapshot: null,
      error: "Steam market rate limited",
      refreshReason: "snapshot_rate_limited",
      errorCode: "STEAM_MARKET_RATE_LIMITED",
      errorStatusCode: 429
    }))
  marketSourceCatalogService.prepareSourceCatalog = async () => {
    throw new Error("should not recompute while retry budget is exhausted")
  }
  steamMarket.batchGetPrices = async () => ({})
  skinportMarket.batchGetPrices = async () => ({})
  csfloatMarket.batchGetPrices = async () => ({})
  dmarketMarket.batchGetPrices = async () => ({})

  try {
    const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
      categories: ["weapon_skin"],
      limit: 180,
      selectionBatchSize: 30,
      quoteBatchSize: 20,
      snapshotBatchSize: 10,
      nowMs,
      snapshotPacingOverrides: {
        weapon_skin: {
          retryBudget: 1,
          cooldownMs: 60000,
          maxCooldownMs: 60000
        }
      }
    })

    assert.equal(diagnostics.paused, true)
    assert.equal(diagnostics.pauseReason, "retry_budget_exhausted")
    assert.equal(diagnostics.snapshotRefresh.retryBudgetExhaustedCount, 1)
    assert.equal(
      diagnostics.snapshotRefresh.byCategory.weapon_skin.retryBudgetExhaustedCount,
      1
    )
    assert.equal(
      diagnostics.snapshotRefresh.byCategory.weapon_skin.pacing.retryBudgetExhausted,
      true
    )
    assert.equal(
      diagnostics.snapshotRefresh.byCategory.weapon_skin.pacing.retriesRemaining,
      0
    )
    assert.equal(diagnostics.catalogRecompute.executed, false)
  } finally {
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
    marketQuoteRepo.insertRows = originals.insertRows
    marketPriceRepo.upsertRows = originals.upsertRows
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins
    steamMarket.batchGetPrices = originals.steamBatchGetPrices
    skinportMarket.batchGetPrices = originals.skinportBatchGetPrices
    csfloatMarket.batchGetPrices = originals.csfloatBatchGetPrices
    dmarketMarket.batchGetPrices = originals.dmarketBatchGetPrices
  }
})

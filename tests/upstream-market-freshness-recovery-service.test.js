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
  marketSourceCatalogService.prepareSourceCatalog = async () => ({
    generatedAt: freshIso,
    sourceCatalog: {
      scannable: 61,
      shadow: 9,
      blocked: 3,
      eligibleTradableRows: 18,
      nearEligibleRows: 7,
      eligibleRows: 18,
      scanner_source_size: 61
    }
  })

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
    assert.equal(diagnostics.catalogRecompute.executed, true)
    assert.equal(diagnostics.postRefresh.byCategory.weapon_skin.quote.coverageReady >= 40, true)
    assert.equal(diagnostics.postRefresh.byCategory.weapon_skin.snapshot.fresh >= 25, true)
    assert.equal(diagnostics.catalogRecompute.scannableRows, 61)
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
  steamMarket.batchGetPrices = async () => ({})
  skinportMarket.batchGetPrices = async () => ({})
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
    assert.deepEqual(diagnostics.checkpoint.resumeArgs, [
      "--start-category=weapon_skin",
      "--start-offset=30"
    ])
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

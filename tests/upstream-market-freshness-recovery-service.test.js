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

  marketSourceCatalogRepo.listActiveTradable = async ({ categories }) => {
    const category = Array.isArray(categories) ? categories[0] : ""
    if (category === "weapon_skin") return weaponRows
    if (category === "case") return caseRows
    if (category === "sticker_capsule") return stickerRows
    return []
  }
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

  marketSourceCatalogRepo.listActiveTradable = async ({ categories }) => {
    const category = Array.isArray(categories) ? categories[0] : ""
    if (category === "weapon_skin") return weaponRows
    if (category === "case") return caseRows
    if (category === "sticker_capsule") return stickerRows
    return []
  }
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

const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../src/repositories/marketUniverseRepository")
const scanSourceCohortService = require("../src/services/scanner/scanSourceCohortService")

function buildUniverseRows(names = [], category = "weapon_skin", startRank = 1) {
  return names.map((marketHashName, index) => ({
    catalog_generation_id: "generation-active",
    market_hash_name: marketHashName,
    item_name: marketHashName,
    category,
    liquidity_rank: startRank + index,
    is_active: true
  }))
}

function buildCatalogRows(names = [], patch = {}) {
  const nowIso = new Date().toISOString()
  return names.map((marketHashName, index) => ({
    market_hash_name: marketHashName,
    item_name: marketHashName,
    category: patch.category || "weapon_skin",
    tradable: true,
    is_active: true,
    candidate_status: patch.candidate_status || "eligible",
    scan_eligible: patch.scan_eligible == null ? true : patch.scan_eligible,
    catalog_status: patch.catalog_status,
    reference_price: 5 + index,
    market_coverage_count: 3,
    liquidity_rank: 200 - index,
    volume_7d: 100,
    maturity_score: 80,
    snapshot_captured_at: nowIso,
    quote_fetched_at: nowIso,
    last_market_signal_at: nowIso
  }))
}

test("scan source cohort loader reads the active generation universe and hydrates rows from catalog", async () => {
  const originals = {
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames
  }

  const batchSize = 8
  const weaponSkinNames = Array.from({ length: 8 }, (_, index) => `AK-47 | Redline #${index + 1}`)
  const caseNames = Array.from({ length: 4 }, (_, index) => `Revolution Case #${index + 1}`)
  const capsuleNames = Array.from(
    { length: 4 },
    (_, index) => `Stockholm 2021 Contenders Sticker Capsule #${index + 1}`
  )
  const universeRows = [
    ...buildUniverseRows(weaponSkinNames, "weapon_skin", 1),
    ...buildUniverseRows(caseNames, "case", 20),
    ...buildUniverseRows(capsuleNames, "sticker_capsule", 40)
  ]

  marketUniverseRepo.listActiveByLiquidityRank = async () => universeRows
  marketSourceCatalogRepo.listByMarketHashNames = async (marketHashNames = []) => {
    const requested = new Set(marketHashNames)
    return [
      ...buildCatalogRows(
        weaponSkinNames.filter((name) => requested.has(name)),
        { category: "weapon_skin", candidate_status: "eligible", scan_eligible: true, catalog_status: "scannable" }
      ),
      ...buildCatalogRows(
        caseNames.filter((name) => requested.has(name)),
        { category: "case", candidate_status: "near_eligible", scan_eligible: false, catalog_status: "scannable" }
      ),
      ...buildCatalogRows(
        capsuleNames.filter((name) => requested.has(name)),
        { category: "sticker_capsule", candidate_status: "near_eligible", scan_eligible: false, catalog_status: "scannable" }
      )
    ]
  }

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize })
    assert.equal(result.diagnostics.sourceMode, "active_generation_universe")
    assert.equal(result.diagnostics.fallbackUsed, false)
    assert.equal(result.diagnostics.universeRowsLoaded, universeRows.length)
    assert.equal(result.diagnostics.catalogRowsResolved, universeRows.length)
    assert.deepEqual(result.diagnostics.missingCategoriesAfterPrimary, [])
    assert.equal(result.rows.length >= batchSize, true)
  } finally {
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
  }
})

test("scan source cohort loader does not bypass the active universe when rows are missing or blocked", async () => {
  const originals = {
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames
  }

  const universeRows = [
    ...buildUniverseRows(["AK-47 | Slate"], "weapon_skin", 1),
    ...buildUniverseRows(["Recoil Case"], "case", 2)
  ]

  marketUniverseRepo.listActiveByLiquidityRank = async () => universeRows
  marketSourceCatalogRepo.listByMarketHashNames = async () => [
    ...buildCatalogRows(["AK-47 | Slate"], {
      category: "weapon_skin",
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    }),
    ...buildCatalogRows(["Shadow Extra Row"], {
      category: "sticker_capsule",
      candidate_status: "near_eligible",
      scan_eligible: false,
      catalog_status: "scannable"
    })
  ]

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize: 4 })
    const names = result.rows.map((row) => row.market_hash_name)
    assert.deepEqual(names, ["AK-47 | Slate"])
    assert.equal(result.diagnostics.fallbackUsed, false)
    assert.equal(result.diagnostics.universeRowsLoaded, 2)
    assert.equal(result.diagnostics.catalogRowsResolved, 1)
    assert.equal(result.diagnostics.universeRowsMissingCatalog, 1)
    assert.equal(
      result.diagnostics.fallbackReasons.includes("active_generation_universe_under_target"),
      true
    )
  } finally {
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
  }
})

test("scan source cohort loader still normalizes null catalog status rows from the active universe hydration path", async () => {
  const originals = {
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames
  }

  const batchSize = 6
  const weaponSkinNames = Array.from({ length: 6 }, (_, index) => `AK-47 | Slate #${index + 1}`)
  const caseNames = Array.from({ length: 4 }, (_, index) => `Fracture Case #${index + 1}`)
  const capsuleNames = Array.from(
    { length: 4 },
    (_, index) => `Copenhagen 2024 Legends Sticker Capsule #${index + 1}`
  )
  marketUniverseRepo.listActiveByLiquidityRank = async () => [
    ...buildUniverseRows(weaponSkinNames, "weapon_skin", 1),
    ...buildUniverseRows(caseNames, "case", 20),
    ...buildUniverseRows(capsuleNames, "sticker_capsule", 40)
  ]
  marketSourceCatalogRepo.listByMarketHashNames = async (marketHashNames = []) => {
    const requested = new Set(marketHashNames)
    return [
      ...buildCatalogRows(
        weaponSkinNames.filter((name) => requested.has(name)),
        { category: "weapon_skin", candidate_status: "eligible", scan_eligible: true, catalog_status: null }
      ),
      ...buildCatalogRows(
        caseNames.filter((name) => requested.has(name)),
        { category: "case", candidate_status: "near_eligible", scan_eligible: false, catalog_status: null }
      ),
      ...buildCatalogRows(
        capsuleNames.filter((name) => requested.has(name)),
        { category: "sticker_capsule", candidate_status: "near_eligible", scan_eligible: false, catalog_status: undefined }
      )
    ]
  }

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize })
    assert.equal(result.rows.length > 0, true)
    assert.equal(result.diagnostics.fallbackUsed, false)
    assert.equal(
      result.rows.every((row) => String(row?.catalog_status || "").toLowerCase() === "scannable"),
      true
    )
  } finally {
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
  }
})

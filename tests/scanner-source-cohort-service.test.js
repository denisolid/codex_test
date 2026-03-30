const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../src/repositories/marketUniverseRepository")
const catalogGenerationRepo = require("../src/repositories/catalogGenerationRepository")
const marketSourceCatalogService = require("../src/services/marketSourceCatalogService")
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
    reference_price:
      Object.prototype.hasOwnProperty.call(patch, "reference_price")
        ? patch.reference_price
        : 5 + index,
    market_coverage_count:
      Object.prototype.hasOwnProperty.call(patch, "market_coverage_count")
        ? patch.market_coverage_count
        : 3,
    liquidity_rank:
      Object.prototype.hasOwnProperty.call(patch, "liquidity_rank")
        ? patch.liquidity_rank
        : 200 - index,
    volume_7d:
      Object.prototype.hasOwnProperty.call(patch, "volume_7d")
        ? patch.volume_7d
        : 100,
    maturity_score:
      Object.prototype.hasOwnProperty.call(patch, "maturity_score")
        ? patch.maturity_score
        : 80,
    snapshot_captured_at:
      Object.prototype.hasOwnProperty.call(patch, "snapshot_captured_at")
        ? patch.snapshot_captured_at
        : nowIso,
    quote_fetched_at:
      Object.prototype.hasOwnProperty.call(patch, "quote_fetched_at")
        ? patch.quote_fetched_at
        : nowIso,
    last_market_signal_at:
      Object.prototype.hasOwnProperty.call(patch, "last_market_signal_at")
        ? patch.last_market_signal_at
        : nowIso
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

test("scan source cohort loader repairs the active universe once when it is empty but the active generation catalog is populated", async () => {
  const originals = {
    getActiveGeneration: catalogGenerationRepo.getActiveGeneration,
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames,
    refreshActiveUniverseFromCurrentCatalog:
      marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog
  }

  const repairedNames = [
    "AK-47 | Head Shot (Field-Tested)",
    "M4A1-S | Decimator (Field-Tested)"
  ]
  let universeReads = 0
  let repairRequest = null

  catalogGenerationRepo.getActiveGeneration = async () => ({
    id: "generation-active",
    opportunity_scan_enabled: true
  })
  marketUniverseRepo.listActiveByLiquidityRank = async () => {
    universeReads += 1
    return universeReads === 1 ? [] : buildUniverseRows(repairedNames, "weapon_skin", 1)
  }
  marketSourceCatalogRepo.listCoverageSummary = async () =>
    buildCatalogRows(repairedNames, {
      category: "weapon_skin",
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    })
  marketSourceCatalogRepo.listActiveTradable = async () =>
    buildCatalogRows(repairedNames, {
      category: "weapon_skin",
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    })
  marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog = async (options = {}) => {
    repairRequest = options
    return {
      catalogGenerationId: "generation-active",
      targetUniverseSize: 3000,
      universeRowsBeforeRefresh: 0,
      universeRowsAfterRefresh: 2,
      universeRowsDroppedAsStale: 0,
      universeRowsAdded: 2,
      activeUniverseBuilt: 2
    }
  }
  marketSourceCatalogRepo.listByMarketHashNames = async (marketHashNames = []) => {
    const requested = new Set(marketHashNames)
    return buildCatalogRows(
      repairedNames.filter((name) => requested.has(name)),
      { category: "weapon_skin", candidate_status: "eligible", scan_eligible: true, catalog_status: "scannable" }
    )
  }

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize: 2 })
    assert.equal(result.rows.length, 2)
    assert.equal(repairRequest?.generationId, "generation-active")
    assert.equal(result.diagnostics.catalogGenerationId, "generation-active")
    assert.equal(result.diagnostics.activeUniverseRowsBeforeRepair, 0)
    assert.equal(result.diagnostics.activeCatalogRowsForGeneration, 2)
    assert.equal(result.diagnostics.universeRepairTriggered, true)
    assert.equal(result.diagnostics.activeUniverseRowsAfterRepair, 2)
    assert.equal(result.diagnostics.retriedSourceLoad, true)
    assert.equal(
      result.diagnostics.fallbackReasons.includes("active_generation_universe_repair_triggered"),
      true
    )
  } finally {
    catalogGenerationRepo.getActiveGeneration = originals.getActiveGeneration
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
    marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog =
      originals.refreshActiveUniverseFromCurrentCatalog
  }
})

test("scan source cohort loader leaves explicit diagnostics when the active universe stays empty after one repair attempt", async () => {
  const originals = {
    getActiveGeneration: catalogGenerationRepo.getActiveGeneration,
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    refreshActiveUniverseFromCurrentCatalog:
      marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog
  }

  catalogGenerationRepo.getActiveGeneration = async () => ({
    id: "generation-active",
    opportunity_scan_enabled: true
  })
  marketUniverseRepo.listActiveByLiquidityRank = async () => []
  marketSourceCatalogRepo.listCoverageSummary = async () =>
    buildCatalogRows(["AK-47 | Asiimov (Battle-Scarred)"], {
      category: "weapon_skin",
      candidate_status: "candidate",
      scan_eligible: false,
      catalog_status: "shadow"
    })
  marketSourceCatalogRepo.listActiveTradable = async () => []
  marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog = async () => ({
    catalogGenerationId: "generation-active",
    universeRowsBeforeRefresh: 0,
    universeRowsAfterRefresh: 0,
    activeUniverseBuilt: 0
  })

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize: 2 })
    assert.equal(result.rows.length, 0)
    assert.equal(result.diagnostics.activeUniverseRowsBeforeRepair, 0)
    assert.equal(result.diagnostics.activeCatalogRowsForGeneration, 1)
    assert.equal(result.diagnostics.universeRepairTriggered, true)
    assert.equal(result.diagnostics.activeUniverseRowsAfterRepair, 0)
    assert.equal(result.diagnostics.retriedSourceLoad, true)
    assert.equal(
      result.diagnostics.fallbackReasons.includes("active_generation_universe_still_empty_after_repair"),
      true
    )
  } finally {
    catalogGenerationRepo.getActiveGeneration = originals.getActiveGeneration
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog =
      originals.refreshActiveUniverseFromCurrentCatalog
  }
})

test("scan source cohort loader repairs a stale non-empty active universe once when hydrated rows fail the zero-signal contract", async () => {
  const originals = {
    getActiveGeneration: catalogGenerationRepo.getActiveGeneration,
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    listActiveTradable: marketSourceCatalogRepo.listActiveTradable,
    listByMarketHashNames: marketSourceCatalogRepo.listByMarketHashNames,
    refreshActiveUniverseFromCurrentCatalog:
      marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog
  }

  const staleWeaponSkinNames = ["AK-47 | Slate", "M4A1-S | Decimator"]
  const staleCaseNames = ["Revolution Case"]
  const repairedWeaponSkinNames = [
    "AK-47 | Head Shot (Field-Tested)",
    "M4A1-S | Player Two (Field-Tested)"
  ]
  const repairedCaseNames = ["Kilowatt Case"]
  let universeReads = 0
  let repairRequest = null

  catalogGenerationRepo.getActiveGeneration = async () => ({
    id: "generation-active",
    opportunity_scan_enabled: true
  })
  marketUniverseRepo.listActiveByLiquidityRank = async () => {
    universeReads += 1
    if (universeReads === 1) {
      return [
        ...buildUniverseRows(staleWeaponSkinNames, "weapon_skin", 1),
        ...buildUniverseRows(staleCaseNames, "case", 10)
      ]
    }
    return [
      ...buildUniverseRows(repairedWeaponSkinNames, "weapon_skin", 1),
      ...buildUniverseRows(repairedCaseNames, "case", 10)
    ]
  }
  marketSourceCatalogRepo.listActiveTradable = async () => [
    ...buildCatalogRows(repairedWeaponSkinNames, {
      category: "weapon_skin",
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    }),
    ...buildCatalogRows(repairedCaseNames, {
      category: "case",
      candidate_status: "eligible",
      scan_eligible: true,
      catalog_status: "scannable"
    })
  ]
  marketSourceCatalogRepo.listByMarketHashNames = async (marketHashNames = []) => {
    const requested = new Set(marketHashNames)
    if (requested.has(staleWeaponSkinNames[0]) || requested.has(staleCaseNames[0])) {
      return [
        ...buildCatalogRows(
          staleWeaponSkinNames.filter((name) => requested.has(name)),
          {
            category: "weapon_skin",
            candidate_status: "eligible",
            scan_eligible: true,
            catalog_status: "scannable",
            reference_price: null,
            market_coverage_count: 0,
            snapshot_captured_at: null,
            quote_fetched_at: null,
            last_market_signal_at: null
          }
        ),
        ...buildCatalogRows(
          staleCaseNames.filter((name) => requested.has(name)),
          {
            category: "case",
            candidate_status: "eligible",
            scan_eligible: true,
            catalog_status: "scannable",
            reference_price: null,
            market_coverage_count: 0,
            snapshot_captured_at: null,
            quote_fetched_at: null,
            last_market_signal_at: null
          }
        )
      ]
    }
    return [
      ...buildCatalogRows(
        repairedWeaponSkinNames.filter((name) => requested.has(name)),
        {
          category: "weapon_skin",
          candidate_status: "eligible",
          scan_eligible: true,
          catalog_status: "scannable"
        }
      ),
      ...buildCatalogRows(
        repairedCaseNames.filter((name) => requested.has(name)),
        {
          category: "case",
          candidate_status: "eligible",
          scan_eligible: true,
          catalog_status: "scannable"
        }
      )
    ]
  }
  marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog = async (options = {}) => {
    repairRequest = options
    return {
      catalogGenerationId: "generation-active",
      universeRowsBeforeRefresh: 3,
      universeRowsAfterRefresh: 3,
      universeRowsDroppedAsStale: 3,
      universeRowsAdded: 3,
      activeUniverseBuilt: 3
    }
  }

  try {
    const result = await scanSourceCohortService.loadScanSource({ batchSize: 3 })
    assert.equal(result.rows.length, 3)
    assert.equal(repairRequest?.generationId, "generation-active")
    assert.equal(result.diagnostics.catalogGenerationId, "generation-active")
    assert.equal(result.diagnostics.activeUniverseRowsBeforeRepair, 3)
    assert.equal(result.diagnostics.rowsRejectedByZeroSignalContract, 3)
    assert.equal(result.diagnostics.rowsRejectedByMissingReference, 3)
    assert.equal(result.diagnostics.rowsRejectedByMissingCoverage, 3)
    assert.equal(result.diagnostics.rowsRejectedByMissingFreshness, 3)
    assert.equal(result.diagnostics.staleUniverseRepairTriggered, true)
    assert.equal(result.diagnostics.staleUniverseRowsLoaded, 3)
    assert.equal(result.diagnostics.staleUniverseRowsDropped, 3)
    assert.equal(result.diagnostics.scannableRowsInCatalogAtRepair, 3)
    assert.equal(result.diagnostics.activeUniverseRowsAfterRepair, 3)
    assert.equal(result.diagnostics.universeRowsAfterRepair, 3)
    assert.equal(result.diagnostics.retriedSourceLoad, true)
    assert.equal(
      result.diagnostics.fallbackReasons.includes("active_generation_universe_stale_repair_triggered"),
      true
    )
  } finally {
    catalogGenerationRepo.getActiveGeneration = originals.getActiveGeneration
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogRepo.listActiveTradable = originals.listActiveTradable
    marketSourceCatalogRepo.listByMarketHashNames = originals.listByMarketHashNames
    marketSourceCatalogService.refreshActiveUniverseFromCurrentCatalog =
      originals.refreshActiveUniverseFromCurrentCatalog
  }
})

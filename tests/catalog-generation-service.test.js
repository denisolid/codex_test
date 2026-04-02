const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const catalogGenerationRepo = require("../src/repositories/catalogGenerationRepository")
const marketSourceCatalogRepo = require("../src/repositories/marketSourceCatalogRepository")
const marketUniverseRepo = require("../src/repositories/marketUniverseRepository")
const marketSourceCatalogService = require("../src/services/marketSourceCatalogService")
const catalogGenerationService = require("../src/services/catalogGenerationService")

const {
  __testables: {
    summarizeCoverageRows,
    buildReadinessSummary,
    buildCategoryFocusComparison,
    buildCatalogLivenessSummary
  }
} = catalogGenerationService

function buildCoverageRows({
  category = "weapon_skin",
  count = 1,
  candidateStatus = "eligible",
  catalogStatus = "scannable",
  scanEligible = candidateStatus === "eligible"
} = {}) {
  return Array.from({ length: count }, (_, index) => ({
    market_hash_name: `${category}-${candidateStatus}-${index + 1}`,
    category,
    tradable: true,
    scan_eligible: scanEligible,
    candidate_status: candidateStatus,
    catalog_status: catalogStatus,
    catalog_block_reason: catalogStatus === "scannable" ? null : "test_reason"
  }))
}

test("summarizeCoverageRows and readiness capture generation maturity", () => {
  const generation = {
    id: "generation-1",
    generation_key: "catalog-reset-1",
    status: "active",
    is_active: true,
    opportunity_scan_enabled: false
  }
  const rows = [
    ...buildCoverageRows({ category: "weapon_skin", count: 140, candidateStatus: "eligible" }),
    ...buildCoverageRows({ category: "weapon_skin", count: 36, candidateStatus: "near_eligible", scanEligible: false }),
    ...buildCoverageRows({ category: "case", count: 48, candidateStatus: "eligible" }),
    ...buildCoverageRows({ category: "case", count: 20, candidateStatus: "candidate", scanEligible: false }),
    ...buildCoverageRows({ category: "sticker_capsule", count: 6, candidateStatus: "candidate", catalogStatus: "shadow", scanEligible: false })
  ]

  const summary = summarizeCoverageRows(rows, generation)
  const readiness = buildReadinessSummary(summary, {
    universeSummary: {
      totalRows: 120
    },
    minScannerSourceSize: 120,
    minEligibleTradableRows: 80,
    minNearEligibleRows: 20,
    minReadyCategories: 2,
    minActiveUniverseRows: 100
  })

  assert.equal(summary.generation.id, "generation-1")
  assert.equal(summary.scannerSourceSize >= 120, true)
  assert.equal(summary.eligibleTradableRows >= 100, true)
  assert.equal(summary.readyCategoryCount >= 2, true)
  assert.equal(readiness.readyForOpportunityScan, true)
})

test("buildReadinessSummary allows eligible supply to unlock scan without near-eligible rows", () => {
  const summary = summarizeCoverageRows(
    [
      ...buildCoverageRows({ category: "case", count: 24, candidateStatus: "eligible" }),
      ...buildCoverageRows({
        category: "weapon_skin",
        count: 12,
        candidateStatus: "enriching",
        scanEligible: false
      })
    ],
    {
      id: "generation-weapon-skin-blocked",
      generation_key: "catalog-reset-weapon-skin-blocked",
      status: "active",
      is_active: true,
      opportunity_scan_enabled: false
    }
  )

  const readiness = buildReadinessSummary(summary, {
    universeSummary: {
      totalRows: 24
    },
    minScannerSourceSize: 1,
    minEligibleTradableRows: 1,
    minNearEligibleRows: 0,
    minReadyCategories: 1,
    minActiveUniverseRows: 1
  })

  assert.equal(readiness.signals.scannerSourceNonZero, true)
  assert.equal(readiness.signals.eligibleSupplyReady, true)
  assert.equal(readiness.signals.nearEligibleSupplyReady, false)
  assert.equal(readiness.signals.supplyReady, true)
  assert.equal(readiness.signals.weaponSkinEligibleReady, false)
  assert.equal(readiness.signals.weaponSkinNearEligibleReady, false)
  assert.equal(readiness.readinessSource, "eligible_supply")
  assert.equal(readiness.weaponSkinReadinessSource, "not_ready")
  assert.equal(readiness.readyForOpportunityScan, true)
})

test("buildReadinessSummary unlocks scan on positive mature-pool counts while keeping weapon_skin diagnostics optional", () => {
  const summary = summarizeCoverageRows(
    [
      ...buildCoverageRows({ category: "case", count: 8, candidateStatus: "eligible" }),
      ...buildCoverageRows({
        category: "case",
        count: 4,
        candidateStatus: "near_eligible",
        scanEligible: false
      }),
      ...buildCoverageRows({
        category: "weapon_skin",
        count: 12,
        candidateStatus: "enriching",
        scanEligible: false,
        catalogStatus: "shadow"
      })
    ],
    {
      id: "generation-general-ready",
      generation_key: "catalog-reset-general-ready",
      status: "active",
      is_active: true,
      opportunity_scan_enabled: false
    }
  )

  const readiness = buildReadinessSummary(summary, {
    universeSummary: {
      totalRows: 12
    },
    minScannerSourceSize: 1,
    minEligibleRows: 1,
    minNearEligibleRows: 1,
    minReadyCategories: 1,
    minActiveUniverseRows: 1
  })

  assert.equal(readiness.signals.scannerSourceNonZero, true)
  assert.equal(readiness.signals.eligibleRowsReady, true)
  assert.equal(readiness.signals.nearEligibleReady, true)
  assert.equal(readiness.signals.supplyReady, true)
  assert.equal(readiness.signals.weaponSkinEligibleReady, false)
  assert.equal(readiness.signals.weaponSkinNearEligibleReady, false)
  assert.equal(readiness.readinessSource, "eligible_supply")
  assert.equal(readiness.readyForOpportunityScan, true)
  assert.deepEqual(readiness.optionalSignalKeys, [
    "eligibleRowsReady",
    "nearEligibleReady",
    "weaponSkinEligibleReady",
    "weaponSkinNearEligibleReady"
  ])
})

test("buildReadinessSummary can explain near-eligible-led readiness when eligible supply is absent", () => {
  const summary = summarizeCoverageRows(
    [
      ...buildCoverageRows({
        category: "weapon_skin",
        count: 9,
        candidateStatus: "near_eligible",
        scanEligible: false
      })
    ],
    {
      id: "generation-near-eligible-ready",
      generation_key: "catalog-reset-near-eligible-ready",
      status: "active",
      is_active: true,
      opportunity_scan_enabled: false
    }
  )

  const readiness = buildReadinessSummary(summary, {
    universeSummary: {
      totalRows: 9
    },
    minScannerSourceSize: 1,
    minEligibleRows: 10,
    minNearEligibleRows: 1,
    minReadyCategories: 1,
    minActiveUniverseRows: 1
  })

  assert.equal(readiness.signals.eligibleSupplyReady, false)
  assert.equal(readiness.signals.nearEligibleSupplyReady, true)
  assert.equal(readiness.signals.supplyReady, true)
  assert.equal(readiness.readinessSource, "near_eligible_supply")
  assert.equal(readiness.readyForOpportunityScan, true)
})

test("ensureOpportunityScanEnabledForActiveGeneration auto-enables when eligible supply is ready", async () => {
  const originals = {
    getCurrentGeneration: catalogGenerationRepo.getCurrentGeneration,
    enableOpportunityScan: catalogGenerationRepo.enableOpportunityScan,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank
  }

  let enablePayload = null
  let currentGeneration = {
    id: "generation-eligible-unlock",
    generation_key: "catalog-reset-eligible-unlock",
    status: "active",
    is_active: true,
    opportunity_scan_enabled: false,
    diagnostics_summary: {}
  }

  catalogGenerationRepo.getCurrentGeneration = async () => currentGeneration
  catalogGenerationRepo.enableOpportunityScan = async (_id, payload = {}) => {
    enablePayload = payload
    currentGeneration = {
      ...currentGeneration,
      opportunity_scan_enabled: true,
      diagnostics_summary: payload.diagnosticsSummary || {}
    }
    return currentGeneration
  }
  marketSourceCatalogRepo.listCoverageSummary = async () =>
    buildCoverageRows({ category: "weapon_skin", count: 351, candidateStatus: "eligible" })
  marketUniverseRepo.listActiveByLiquidityRank = async () =>
    Array.from({ length: 401 }, (_, index) => ({
      market_hash_name: `eligible-${index + 1}`,
      item_name: `eligible-${index + 1}`,
      category: "weapon_skin",
      liquidity_rank: index + 1,
      is_active: true
    }))

  try {
    const result = await catalogGenerationService.ensureOpportunityScanEnabledForActiveGeneration({
      minScannerSourceSize: 1,
      minEligibleRows: 1,
      minNearEligibleRows: 1,
      minReadyCategories: 1,
      minActiveUniverseRows: 1
    })

    assert.equal(result.allowed, true)
    assert.equal(result.autoEnabled, true)
    assert.equal(result.readiness?.readinessSource, "eligible_supply")
    assert.equal(result.diagnostics.blocked_by_generation_flag, false)
    assert.equal(result.diagnostics.blocked_by_readiness_gate, false)
    assert.equal(result.diagnostics.blocked_by_empty_scanner_source, false)
    assert.equal(enablePayload?.diagnosticsSummary?.readiness?.readyForOpportunityScan, true)
    assert.equal(
      enablePayload?.diagnosticsSummary?.opportunityScanUnlock?.readinessSource,
      "eligible_supply"
    )
  } finally {
    catalogGenerationRepo.getCurrentGeneration = originals.getCurrentGeneration
    catalogGenerationRepo.enableOpportunityScan = originals.enableOpportunityScan
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
  }
})

test("buildCategoryFocusComparison highlights weapon_skin deltas across generations", () => {
  const previousSummary = summarizeCoverageRows([
    ...buildCoverageRows({ category: "weapon_skin", count: 4, candidateStatus: "eligible" }),
    ...buildCoverageRows({
      category: "weapon_skin",
      count: 2,
      candidateStatus: "near_eligible",
      scanEligible: false
    })
  ])
  const nextSummary = summarizeCoverageRows([
    ...buildCoverageRows({ category: "weapon_skin", count: 9, candidateStatus: "eligible" }),
    ...buildCoverageRows({
      category: "weapon_skin",
      count: 5,
      candidateStatus: "near_eligible",
      scanEligible: false
    })
  ])

  const focus = buildCategoryFocusComparison(
    previousSummary,
    nextSummary,
    {
      byCategory: {
        weapon_skin: {
          totalRows: 6
        }
      }
    },
    {
      byCategory: {
        weapon_skin: {
          totalRows: 14
        }
      }
    }
  )

  assert.equal(focus.category, "weapon_skin")
  assert.equal(focus.delta.eligibleRows, 5)
  assert.equal(focus.delta.nearEligibleRows, 3)
  assert.equal(focus.delta.activeUniverseRows, 8)
})

test("runCatalogGenerationReset archives the previous generation and enables scan only after readiness", async () => {
  const originals = {
    getCurrentGeneration: catalogGenerationRepo.getCurrentGeneration,
    getById: catalogGenerationRepo.getById,
    createGeneration: catalogGenerationRepo.createGeneration,
    archiveGeneration: catalogGenerationRepo.archiveGeneration,
    activateGeneration: catalogGenerationRepo.activateGeneration,
    updateGeneration: catalogGenerationRepo.updateGeneration,
    enableOpportunityScan: catalogGenerationRepo.enableOpportunityScan,
    listCoverageSummary: marketSourceCatalogRepo.listCoverageSummary,
    listActiveByLiquidityRank: marketUniverseRepo.listActiveByLiquidityRank,
    prepareSourceCatalog: marketSourceCatalogService.prepareSourceCatalog
  }

  const oldGeneration = {
    id: "generation-old",
    generation_key: "catalog-old",
    status: "active",
    is_active: true,
    opportunity_scan_enabled: true,
    diagnostics_summary: {
      previous: true
    }
  }
  const newGeneration = {
    id: "generation-new",
    generation_key: "catalog-new",
    status: "archived",
    is_active: false,
    opportunity_scan_enabled: false,
    diagnostics_summary: {}
  }
  let currentGeneration = oldGeneration
  let archivedPayload = null
  let enabledPayload = null

  catalogGenerationRepo.getCurrentGeneration = async () => currentGeneration
  catalogGenerationRepo.getById = async (id) =>
    id === oldGeneration.id ? oldGeneration : id === newGeneration.id ? newGeneration : null
  catalogGenerationRepo.createGeneration = async (payload = {}) => ({
    ...newGeneration,
    generation_key: payload.generationKey || newGeneration.generation_key,
    diagnostics_summary: payload.diagnosticsSummary || {}
  })
  catalogGenerationRepo.archiveGeneration = async (_id, payload = {}) => {
    archivedPayload = payload
    oldGeneration.status = "archived"
    oldGeneration.is_active = false
    oldGeneration.opportunity_scan_enabled = false
    oldGeneration.diagnostics_summary = payload.diagnosticsSummary || {}
    return oldGeneration
  }
  catalogGenerationRepo.activateGeneration = async (_id, payload = {}) => {
    currentGeneration = {
      ...newGeneration,
      status: "active",
      is_active: true,
      opportunity_scan_enabled: false,
      diagnostics_summary: payload.diagnosticsSummary || {}
    }
    return currentGeneration
  }
  catalogGenerationRepo.updateGeneration = async (_id, payload = {}) => {
    currentGeneration = {
      ...currentGeneration,
      diagnostics_summary: payload.diagnosticsSummary || currentGeneration.diagnostics_summary
    }
    return currentGeneration
  }
  catalogGenerationRepo.enableOpportunityScan = async (_id, payload = {}) => {
    enabledPayload = payload
    currentGeneration = {
      ...currentGeneration,
      opportunity_scan_enabled: true,
      diagnostics_summary: payload.diagnosticsSummary || currentGeneration.diagnostics_summary
    }
    return currentGeneration
  }
  marketSourceCatalogService.prepareSourceCatalog = async () => ({
    generatedAt: "2026-03-28T10:00:00.000Z",
    sourceCatalog: {
      scannerSourceSize: 140
    }
  })
  marketUniverseRepo.listActiveByLiquidityRank = async (options = {}) => {
    if (options.generationId === oldGeneration.id) {
      return Array.from({ length: 12 }, (_, index) => ({
        market_hash_name: `old-${index + 1}`,
        item_name: `old-${index + 1}`,
        category: "weapon_skin",
        liquidity_rank: index + 1,
        is_active: true
      }))
    }
    return Array.from({ length: 120 }, (_, index) => ({
      market_hash_name: `new-${index + 1}`,
      item_name: `new-${index + 1}`,
      category: index < 80 ? "weapon_skin" : "case",
      liquidity_rank: index + 1,
      is_active: true
    }))
  }
  marketSourceCatalogRepo.listCoverageSummary = async (options = {}) => {
    if (options.generationId === oldGeneration.id) {
      return buildCoverageRows({ category: "weapon_skin", count: 12, candidateStatus: "eligible" })
    }
    return [
      ...buildCoverageRows({ category: "weapon_skin", count: 140, candidateStatus: "eligible" }),
      ...buildCoverageRows({ category: "weapon_skin", count: 36, candidateStatus: "near_eligible", scanEligible: false }),
      ...buildCoverageRows({ category: "case", count: 48, candidateStatus: "eligible" }),
      ...buildCoverageRows({ category: "case", count: 20, candidateStatus: "candidate", scanEligible: false })
    ]
  }

  try {
    const result = await catalogGenerationService.runCatalogGenerationReset({
      targetUniverseSize: 120,
      categories: ["weapon_skin", "case"],
      minScannerSourceSize: 120,
      minEligibleTradableRows: 80,
      minNearEligibleRows: 20,
      minReadyCategories: 2,
      minActiveUniverseRows: 100
    })

    assert.equal(result.previousGeneration.id, "generation-old")
    assert.equal(result.activeGeneration.id, "generation-new")
    assert.equal(result.activeGeneration.opportunityScanEnabled, true)
    assert.equal(
      archivedPayload?.diagnosticsSummary?.replacedByGenerationId,
      "generation-new"
    )
    assert.equal(
      enabledPayload?.diagnosticsSummary?.readiness?.readyForOpportunityScan,
      true
    )
    assert.equal(
      enabledPayload?.diagnosticsSummary?.readiness?.readinessSource,
      "eligible_supply"
    )
    assert.equal(
      enabledPayload?.diagnosticsSummary?.comparison?.weaponSkin?.category,
      "weapon_skin"
    )
    assert.equal(
      result.diagnostics.comparison.delta.scannerSourceSize > 0,
      true
    )
  } finally {
    catalogGenerationRepo.getCurrentGeneration = originals.getCurrentGeneration
    catalogGenerationRepo.getById = originals.getById
    catalogGenerationRepo.createGeneration = originals.createGeneration
    catalogGenerationRepo.archiveGeneration = originals.archiveGeneration
    catalogGenerationRepo.activateGeneration = originals.activateGeneration
    catalogGenerationRepo.updateGeneration = originals.updateGeneration
    catalogGenerationRepo.enableOpportunityScan = originals.enableOpportunityScan
    marketSourceCatalogRepo.listCoverageSummary = originals.listCoverageSummary
    marketUniverseRepo.listActiveByLiquidityRank = originals.listActiveByLiquidityRank
    marketSourceCatalogService.prepareSourceCatalog = originals.prepareSourceCatalog
  }
})

test("buildCatalogLivenessSummary reflects healthy reference-sized output", () => {
  const summary = summarizeCoverageRows([
    ...buildCoverageRows({ category: "weapon_skin", count: 140, candidateStatus: "eligible" }),
    ...buildCoverageRows({ category: "weapon_skin", count: 80, candidateStatus: "near_eligible", scanEligible: false }),
    ...buildCoverageRows({ category: "case", count: 45, candidateStatus: "eligible" }),
    ...buildCoverageRows({ category: "sticker_capsule", count: 35, candidateStatus: "eligible" }),
    ...buildCoverageRows({ category: "weapon_skin", count: 200, candidateStatus: "candidate", catalogStatus: "shadow", scanEligible: false })
  ])

  const liveness = buildCatalogLivenessSummary(summary)

  assert.equal(liveness.status, "healthy")
  assert.equal(liveness.withinTarget.active_generation, true)
  assert.equal(liveness.withinTarget.scannable, true)
  assert.equal(liveness.withinTarget.hot_universe, true)
})

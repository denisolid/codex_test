const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const globalFeedPublisher = require("../src/services/feed/globalFeedPublisher")
const { SCANNER_V2_TUNING_SURFACE } = require("../src/services/scanner/config")
const {
  __testables: {
    normalizeCategoryFilter,
    classifyCatalogState,
    buildFeedInsertRow,
    mapFeedRowToCard,
    dedupeFeedCards,
    countScannableRowsByScannerCategory,
    persistFeedRows,
    mergeDiagnostics,
    isScannerRunOverdue,
    DEFAULT_UNIVERSE_LIMIT,
    OPPORTUNITY_BATCH_TARGET,
    SCAN_CHUNK_SIZE,
    SCAN_TIMEOUT_PER_BATCH_MS,
    SCAN_STATE
  }
} = require("../src/services/arbitrageScannerService")

function buildOpportunity(overrides = {}) {
  const nowIso = new Date().toISOString()
  return {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    itemName: "AK-47 | Redline (Field-Tested)",
    itemCategory: "weapon_skin",
    buyMarket: "steam",
    buyPrice: 10,
    sellMarket: "skinport",
    sellNet: 12.5,
    profit: 2.5,
    spread: 25,
    score: 76,
    executionConfidence: "Medium",
    qualityGrade: "NEAR_ELIGIBLE",
    liquidityBand: "Medium",
    liquidity: 150,
    marketCoverage: 2,
    referencePrice: 11.2,
    buyRouteAvailable: true,
    sellRouteAvailable: true,
    buyRouteUpdatedAt: nowIso,
    sellRouteUpdatedAt: nowIso,
    metadata: {
      buy_route_available: true,
      sell_route_available: true,
      buy_route_updated_at: nowIso,
      sell_route_updated_at: nowIso,
      sell_listing_available: true
    },
    ...overrides
  }
}

test("normalizeCategoryFilter keeps active scanner category aliases aligned", () => {
  assert.equal(normalizeCategoryFilter("skins"), "weapon_skin")
  assert.equal(normalizeCategoryFilter("knife"), "knife")
  assert.equal(normalizeCategoryFilter("future_glove"), "glove")
  assert.equal(normalizeCategoryFilter("unknown"), "all")
})

test("classifyCatalogState promotes recoverable rows to near_eligible instead of rejecting them", () => {
  const result = classifyCatalogState({
    market_hash_name: "Revolution Case",
    item_name: "Revolution Case",
    category: "case",
    tradable: true,
    is_active: true,
    candidate_status: "near_eligible",
    scan_eligible: false,
    reference_price: 2.9,
    market_coverage_count: 1,
    volume_7d: null
  })

  assert.equal(result.state, SCAN_STATE.NEAR_ELIGIBLE)
  assert.equal(result.hardRejectReasons.length, 0)
  assert.equal(Array.isArray(result.penaltyFlags), true)
})

test("buildFeedInsertRow and mapFeedRowToCard preserve eligible naming", () => {
  const row = buildFeedInsertRow(buildOpportunity(), {
    scanRunId: "scan-run-1",
    detectedAt: "2026-03-28T12:00:00.000Z",
    firstSeenAt: "2026-03-28T12:00:00.000Z",
    lastSeenAt: "2026-03-28T12:00:00.000Z",
    lastPublishedAt: "2026-03-28T12:00:00.000Z",
    timesSeen: 1
  })
  const card = mapFeedRowToCard({
    id: "active-1",
    ...row
  })

  assert.equal(card.qualityGrade, "NEAR_ELIGIBLE")
  assert.equal(card.feedId, "active-1")
  assert.equal(card.itemCategory, "weapon_skin")
})

test("dedupeFeedCards keeps the newest row while preserving stronger counters", () => {
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
  assert.equal(deduped[0].lastSeenAt, "2026-03-21T10:00:00.000Z")
})

test("countScannableRowsByScannerCategory counts eligible and near_eligible rows only", () => {
  const counts = countScannableRowsByScannerCategory([
    {
      market_hash_name: "AK-47 | Slate (Field-Tested)",
      item_name: "AK-47 | Slate (Field-Tested)",
      category: "weapon_skin",
      tradable: true,
      is_active: true,
      candidate_status: "eligible",
      reference_price: 8,
      market_coverage_count: 2,
      volume_7d: 100
    },
    {
      market_hash_name: "Revolution Case",
      item_name: "Revolution Case",
      category: "case",
      tradable: true,
      is_active: true,
      candidate_status: "near_eligible",
      reference_price: 3,
      market_coverage_count: 1,
      volume_7d: null
    },
    {
      market_hash_name: "Broken Item",
      item_name: "Broken Item",
      category: "weapon_skin",
      tradable: false,
      is_active: false,
      candidate_status: "rejected",
      reference_price: 0.1,
      market_coverage_count: 0,
      volume_7d: 0
    }
  ])

  assert.equal(counts.weapon_skin, 1)
  assert.equal(counts.case, 1)
})

test("persistFeedRows routes the active pipeline through globalFeedPublisher only", async () => {
  const originalPublishBatch = globalFeedPublisher.publishBatch
  let callPayload = null

  globalFeedPublisher.publishBatch = async (payload = {}) => {
    callPayload = payload
    return {
      publishedCount: 1,
      blockedCount: 0,
      updatedCount: 0,
      reactivatedCount: 0,
      unchangedCount: 0,
      activeRowsWritten: 1,
      historyRowsWritten: 1,
      validationReasons: {},
      emitRevalidation: {
        emit_revalidation_checked: 1,
        emitted_after_revalidation: 1,
        blocked_on_emit_total: 0,
        blocked_on_emit_by_reason: {}
      },
      lifecycle: {
        detected_total: 1,
        published_total: 1,
        expired_total: 0,
        invalidated_total: 0,
        blocked_on_emit_total: 0
      },
      publisherMetrics: {
        engine: "scanner_v2",
        scannedCount: 1,
        eligibleCount: 1,
        emittedCount: 1,
        blockedOnEmitCount: 0,
        staleOnEmitCount: 0,
        categoryMix: {},
        weaponSkinYield: {},
        emittedScannedRatio: 1
      }
    }
  }

  try {
    const result = await persistFeedRows([buildOpportunity()], "scan-run-v2", {
      scannedCount: 1
    })

    assert.equal(Boolean(callPayload), true)
    assert.equal(callPayload.scanRunId, "scan-run-v2")
    assert.equal(callPayload.trigger, "opportunity_scan")
    assert.equal(callPayload.scannedCount, 1)
    assert.equal(result.insertedCount, 1)
    assert.equal(result.newCount, 1)
    assert.equal(result.publishValidation.blocked, 0)
    assert.equal(result.emitRevalidation.emitted_after_revalidation, 1)
    assert.equal(result.publisherMetrics.engine, "scanner_v2")
  } finally {
    globalFeedPublisher.publishBatch = originalPublishBatch
  }
})

test("mergeDiagnostics adds one consolidated scanner_v2 summary and bounded tuning surface", () => {
  const diagnostics = mergeDiagnostics({
    evaluations: {
      scannedItems: 8,
      eligibleFound: 2,
      nearEligibleFound: 1,
      candidateFound: 1,
      rejectedFound: 4,
      rejectedByReason: {
        low_profit: 3,
        low_spread: 1
      }
    },
    persisted: {
      insertedCount: 2,
      emittedCount: 2,
      publishValidation: {
        blocked: 2,
        reasons: {
          missing_listing_availability: 2,
          buy_route_unavailable: 1
        }
      },
      emitRevalidation: {
        blocked_on_emit_total: 2,
        stale_on_emit_count: 1,
        blocked_on_emit_by_reason: {
          stale_on_emit: 1,
          unavailable_on_emit: 1
        }
      },
      lifecycle: {
        detected_total: 4,
        published_total: 2,
        expired_total: 1,
        invalidated_total: 0,
        blocked_on_emit_total: 2
      },
      publisherMetrics: {
        scannedCount: 8,
        eligibleCount: 4,
        emittedCount: 2,
        blockedOnEmitCount: 2,
        staleOnEmitCount: 1,
        emittedScannedRatio: 0.25
      }
    },
    sourceCatalog: {
      mode: "persisted_cohorts",
      catalogLoad: {
        selection_layer: "alpha_hot_universe",
        hot_universe_size: 10,
        hot_universe_by_category: {
          weapon_skin: 6,
          case: 3,
          sticker_capsule: 1
        },
        hot_universe_by_state: {
          eligible: 7,
          near_eligible: 3
        },
        intake_by_category: {
          weapon_skin: 14,
          case: 5,
          sticker_capsule: 3
        },
        near_eligible_allowed: true,
        near_eligible_cap: 3,
        category_quotas: {
          weapon_skin: 6,
          case: 3,
          sticker_capsule: 1
        },
        quota_hits_by_category: {
          weapon_skin: 1
        },
        quota_skips_by_category: {
          case: 1
        },
        repair_candidates_selected: 4,
        repaired_rows: 3,
        repaired_to_near_eligible: 1,
        repaired_to_eligible: 2,
        cooldown_after_failed_repair: 1,
        rejected_after_failed_repair: 1,
        top_failed_repair_reasons: {
          still_stale_after_repair: 2,
          still_missing_reference_price: 1
        },
        top_reject_reasons_by_category: {
          weapon_skin: {
            low_profit: 3,
            low_spread: 1
          },
          case: {
            missing_market_coverage: 2
          }
        }
      }
    }
  })

  assert.deepEqual(diagnostics.consolidatedSummary.lifecycleDistribution, {
    detected: 4,
    published: 2,
    expired: 1,
    invalidated: 0,
    blockedOnEmit: 2
  })
  assert.deepEqual(diagnostics.consolidatedSummary.repairOutcomes.topFailedRepairReasons[0], {
    reason: "still_stale_after_repair",
    count: 2
  })
  assert.equal(diagnostics.consolidatedSummary.hotUniverseComposition.hotUniverseSize, 10)
  assert.equal(diagnostics.consolidatedSummary.emittedVsBlockedOnEmit.emittedScannedRatio, 0.25)
  assert.deepEqual(diagnostics.consolidatedSummary.topRejectReasonsByCategory.weapon_skin[0], {
    reason: "low_profit",
    count: 3
  })
  assert.deepEqual(diagnostics.consolidatedSummary.topBlockReasonsOnEmit[0], {
    reason: "missing_listing_availability",
    count: 2
  })
  assert.deepEqual(diagnostics.tuningSurface, SCANNER_V2_TUNING_SURFACE)
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

test("scanner defaults stay tuned for batch-first throughput", () => {
  assert.equal(DEFAULT_UNIVERSE_LIMIT >= 500, true)
  assert.equal(OPPORTUNITY_BATCH_TARGET >= 20, true)
  assert.equal(SCAN_CHUNK_SIZE >= 10, true)
  assert.equal(SCAN_TIMEOUT_PER_BATCH_MS >= 1000, true)
  assert.equal(SCANNER_V2_TUNING_SURFACE.freshnessThresholds.publishMaxSignalAgeMinutes, 120)
  assert.equal(SCANNER_V2_TUNING_SURFACE.retryBackoff.repairMaxRejectAttempts, 3)
  assert.equal(SCANNER_V2_TUNING_SURFACE.hotUniverseQuotas.nearEligibleMax, 6)
  assert.equal(SCANNER_V2_TUNING_SURFACE.emitStrictness.emitRevalidationChunkSize <= 20, true)
})

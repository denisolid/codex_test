const planService = require("../planService")
const marketComparisonService = require("../marketComparisonService")
const arbitrageEngineService = require("../arbitrageEngineService")
const {
  SCAN_CHUNK_SIZE,
  SCAN_TIMEOUT_PER_BATCH_MS
} = require("./config")
const {
  buildRouteFreshnessContractFromCompareResult,
  evaluatePublishValidation,
  buildFreshnessContractDiagnostics
} = require("./publishValidation")

const emitRevalidationEntitlements = planService.getEntitlements("full_access")
const EMIT_REVALIDATION_CHUNK_SIZE = Math.max(
  1,
  Math.min(Number(SCAN_CHUNK_SIZE || 20), 20)
)
const EMIT_REVALIDATION_TIMEOUT_MS = Math.max(
  1000,
  Math.min(Number(SCAN_TIMEOUT_PER_BATCH_MS || 8000), 8000)
)

const EMIT_BLOCK_REASONS = Object.freeze({
  STALE: "stale_on_emit",
  UNAVAILABLE: "unavailable_on_emit",
  NON_EXECUTABLE: "non_executable_on_emit",
  INVALID_MARKET_INTEGRITY: "invalid_market_integrity_on_emit"
})

const INTEGRITY_FAILURE_REASONS = new Set([
  "ignored_extreme_spread",
  "ignored_reference_deviation",
  "ignored_missing_depth"
])

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function toIsoOrNull(value) {
  const raw = normalizeText(value)
  if (!raw) return null
  const ts = new Date(raw).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function normalizeSource(value) {
  return normalizeText(value).toLowerCase()
}

function chunkArray(values = [], chunkSize = 20) {
  const safeValues = Array.isArray(values) ? values : []
  const safeChunkSize = Math.max(Math.round(Number(chunkSize || 0)), 1)
  const chunks = []
  for (let index = 0; index < safeValues.length; index += safeChunkSize) {
    chunks.push(safeValues.slice(index, index + safeChunkSize))
  }
  return chunks
}

function withTimeout(promise, timeoutMs, code = "EMIT_REVALIDATION_TIMEOUT") {
  const safeTimeoutMs = Math.max(Math.round(Number(timeoutMs || 0)), 1)
  let settled = false
  let timeoutRef = null
  return new Promise((resolve, reject) => {
    timeoutRef = setTimeout(() => {
      if (settled) return
      settled = true
      const err = new Error(`Timed out after ${safeTimeoutMs}ms`)
      err.code = code
      reject(err)
    }, safeTimeoutMs)

    Promise.resolve(promise)
      .then((value) => {
        if (settled) return
        settled = true
        clearTimeout(timeoutRef)
        resolve(value)
      })
      .catch((err) => {
        if (settled) return
        settled = true
        clearTimeout(timeoutRef)
        reject(err)
      })
  })
}

function createEmitRevalidationDiagnostics(checkedCount = 0) {
  return {
    emit_revalidation_checked: Math.max(Math.round(Number(checkedCount || 0)), 0),
    emitted_after_revalidation: 0,
    blocked_on_emit_total: 0,
    blocked_on_emit_by_reason: {
      [EMIT_BLOCK_REASONS.STALE]: 0,
      [EMIT_BLOCK_REASONS.UNAVAILABLE]: 0,
      [EMIT_BLOCK_REASONS.NON_EXECUTABLE]: 0,
      [EMIT_BLOCK_REASONS.INVALID_MARKET_INTEGRITY]: 0
    },
    stale_on_emit_count: 0,
    unavailable_on_emit_count: 0,
    non_executable_on_emit_count: 0,
    invalid_market_integrity_on_emit_count: 0,
    batches_attempted: 0,
    batches_completed: 0,
    batches_timed_out: 0,
    batches_failed: 0,
    chunk_size: EMIT_REVALIDATION_CHUNK_SIZE,
    timeout_ms: EMIT_REVALIDATION_TIMEOUT_MS
  }
}

function incrementCounter(target = {}, key = "", amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

function resolveOpportunityItemName(opportunity = {}) {
  return normalizeText(opportunity?.marketHashName || opportunity?.itemName)
}

function resolveOpportunityCategory(opportunity = {}) {
  return normalizeText(opportunity?.itemCategory || opportunity?.category).toLowerCase() || null
}

function resolveOpportunitySubcategory(opportunity = {}) {
  const metadata = toJsonObject(opportunity?.metadata)
  return (
    normalizeText(
      opportunity?.itemSubcategory ||
        opportunity?.item_subcategory ||
        metadata?.item_subcategory ||
        metadata?.itemSubcategory
    ) || null
  )
}

function resolveOpportunityLatestSignalAt(opportunity = {}) {
  const metadata = toJsonObject(opportunity?.metadata)
  return (
    toIsoOrNull(opportunity?.latestMarketSignalAt) ||
    toIsoOrNull(opportunity?.latest_market_signal_at) ||
    toIsoOrNull(opportunity?.lastMarketSignalAt) ||
    toIsoOrNull(opportunity?.last_market_signal_at) ||
    toIsoOrNull(opportunity?.quoteFetchedAt) ||
    toIsoOrNull(opportunity?.snapshotCapturedAt) ||
    toIsoOrNull(metadata?.latest_market_signal_at) ||
    toIsoOrNull(metadata?.route_signal_observed_at) ||
    null
  )
}

function buildEmitCompareInput(opportunity = {}) {
  const referencePrice =
    toPositiveOrNull(opportunity?.referencePrice) ??
    toPositiveOrNull(opportunity?.reference_price) ??
    0
  return {
    marketHashName: resolveOpportunityItemName(opportunity),
    itemCategory: resolveOpportunityCategory(opportunity),
    itemSubcategory: resolveOpportunitySubcategory(opportunity),
    quantity: 1,
    steamPrice: referencePrice,
    steamCurrency: "USD",
    steamRecordedAt: resolveOpportunityLatestSignalAt(opportunity),
    volume7d:
      toFiniteOrNull(opportunity?.volume7d) ??
      toFiniteOrNull(opportunity?.volume_7d) ??
      toFiniteOrNull(opportunity?.liquidity),
    marketCoverageCount:
      toFiniteOrNull(opportunity?.marketCoverageCount) ??
      toFiniteOrNull(opportunity?.marketCoverage) ??
      toFiniteOrNull(opportunity?.market_coverage_count),
    marketVolume7d:
      toFiniteOrNull(opportunity?.volume7d) ??
      toFiniteOrNull(opportunity?.volume_7d) ??
      toFiniteOrNull(opportunity?.liquidity),
    referencePrice
  }
}

function resolveMarketRow(comparedItem = {}, market = "") {
  const normalizedMarket = normalizeSource(market)
  if (!normalizedMarket) return null
  return (
    (Array.isArray(comparedItem?.perMarket) ? comparedItem.perMarket : []).find(
      (row) => normalizeSource(row?.source || row?.market) === normalizedMarket
    ) || null
  )
}

function resolveExactRouteEvaluation(opportunity = {}, comparedItem = {}, publishValidation = {}) {
  const buyMarket = normalizeSource(opportunity?.buyMarket || opportunity?.buy_market)
  const sellMarket = normalizeSource(opportunity?.sellMarket || opportunity?.sell_market)
  const buyRow = resolveMarketRow(comparedItem, buyMarket)
  const sellRow = resolveMarketRow(comparedItem, sellMarket)
  const currentBuyPrice = toPositiveOrNull(buyRow?.grossPrice)
  const currentSellNet = toPositiveOrNull(sellRow?.netPriceAfterFees)

  if (!buyMarket || !sellMarket) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "missing_buy_or_sell_market"
    }
  }

  if (buyMarket === sellMarket) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "same_buy_and_sell_market"
    }
  }

  if (!buyRow) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "missing_buy_route_market"
    }
  }

  if (!sellRow) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "missing_sell_route_market"
    }
  }

  if (!Boolean(buyRow?.available)) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "buy_leg_unavailable"
    }
  }

  if (!Boolean(sellRow?.available)) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "sell_leg_unavailable"
    }
  }

  if (currentBuyPrice == null) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "missing_buy_leg_price"
    }
  }

  if (currentSellNet == null) {
    return {
      buyRow,
      sellRow,
      currentBuyPrice,
      currentSellNet,
      currentProfit: null,
      currentSpreadPct: null,
      evaluation: null,
      missingRouteReason: "missing_sell_leg_price"
    }
  }

  const exactItem = {
    marketHashName: resolveOpportunityItemName(opportunity),
    itemCategory: resolveOpportunityCategory(opportunity),
    itemSubcategory: resolveOpportunitySubcategory(opportunity),
    referencePrice:
      toPositiveOrNull(opportunity?.referencePrice) ??
      toPositiveOrNull(opportunity?.reference_price) ??
      toPositiveOrNull(comparedItem?.referencePrice) ??
      currentBuyPrice,
    volume7d:
      toFiniteOrNull(opportunity?.volume7d) ??
      toFiniteOrNull(opportunity?.volume_7d) ??
      toFiniteOrNull(comparedItem?.volume7d),
    maxQuoteAgeMinutes:
      publishValidation?.signalAgeMs != null
        ? Number(Number(publishValidation.signalAgeMs) / (60 * 1000))
        : null,
    perMarket: [buyRow, sellRow]
  }
  const evaluation = arbitrageEngineService.evaluateItemOpportunity(exactItem)
  const currentProfit =
    currentBuyPrice != null && currentSellNet != null
      ? Number((Number(currentSellNet) - Number(currentBuyPrice)).toFixed(2))
      : null
  const currentSpreadPct =
    currentProfit != null && Number(currentBuyPrice) > 0
      ? Number(((Number(currentProfit) / Number(currentBuyPrice)) * 100).toFixed(2))
      : null

  return {
    buyRow,
    sellRow,
    currentBuyPrice,
    currentSellNet,
    currentProfit,
    currentSpreadPct,
    evaluation,
    missingRouteReason: null
  }
}

function classifyPublishValidationFailure(publishValidation = {}, freshnessDiagnostics = {}) {
  const requiredRouteState = normalizeText(publishValidation?.requiredRouteState).toLowerCase()
  const listingAvailabilityState = normalizeText(
    publishValidation?.listingAvailabilityState
  ).toLowerCase()
  const staleReason = normalizeText(publishValidation?.staleReason).toLowerCase()

  const unavailable =
    (requiredRouteState && requiredRouteState !== "ready") ||
    listingAvailabilityState.startsWith("missing_") ||
    listingAvailabilityState.startsWith("unknown_") ||
    Boolean(freshnessDiagnostics?.buy_route_unavailable) ||
    Boolean(freshnessDiagnostics?.sell_route_unavailable) ||
    Boolean(freshnessDiagnostics?.missing_listing_availability)

  if (unavailable) {
    return {
      blockReason: EMIT_BLOCK_REASONS.UNAVAILABLE,
      detailReason:
        requiredRouteState && requiredRouteState !== "ready"
          ? requiredRouteState
          : listingAvailabilityState ||
            normalizeText(freshnessDiagnostics?.primary_failure_bucket) ||
            "required_marketplace_unavailable"
    }
  }

  return {
    blockReason: EMIT_BLOCK_REASONS.STALE,
    detailReason:
      staleReason ||
      normalizeText(freshnessDiagnostics?.primary_failure_bucket) ||
      "stale_on_emit"
  }
}

function classifyExactEvaluationFailure(exactRoute = {}) {
  const evaluation = exactRoute?.evaluation
  const antiFakeReasons = Array.isArray(evaluation?.antiFake?.reasons)
    ? evaluation.antiFake.reasons.map((reason) => normalizeText(reason)).filter(Boolean)
    : []
  const integrityReason = antiFakeReasons.find((reason) =>
    INTEGRITY_FAILURE_REASONS.has(reason)
  )

  if (integrityReason) {
    return {
      blockReason: EMIT_BLOCK_REASONS.INVALID_MARKET_INTEGRITY,
      detailReason: integrityReason
    }
  }

  return {
    blockReason: EMIT_BLOCK_REASONS.NON_EXECUTABLE,
    detailReason:
      antiFakeReasons[0] ||
      normalizeText(exactRoute?.missingRouteReason) ||
      "non_executable_on_emit"
  }
}

function buildEmitRevalidationResult({
  opportunity = {},
  comparedItem = null,
  compareError = null,
  nowIso = null,
  nowMs = Date.now()
} = {}) {
  const safeNowIso = toIsoOrNull(nowIso) || new Date(nowMs).toISOString()
  const marketHashName = resolveOpportunityItemName(opportunity)
  const buyMarket = normalizeSource(opportunity?.buyMarket || opportunity?.buy_market)
  const sellMarket = normalizeSource(opportunity?.sellMarket || opportunity?.sell_market)

  if (compareError) {
    const detailReason =
      normalizeText(compareError?.code) === "EMIT_REVALIDATION_TIMEOUT"
        ? "emit_revalidation_compare_timeout"
        : "emit_revalidation_compare_failed"
    return {
      checked: true,
      passed: false,
      blockReason: EMIT_BLOCK_REASONS.STALE,
      detailReason,
      revalidatedAt: safeNowIso,
      marketHashName,
      buyMarket,
      sellMarket,
      publishValidation: {
        isPublishable: false,
        publishValidatedAt: safeNowIso,
        publishFreshnessState: "missing",
        requiredRouteState: "missing_buy_and_sell_route",
        listingAvailabilityState: "unknown",
        staleReason: detailReason,
        routeSignalObservedAt: null,
        signalAgeMs: null,
        routeFreshnessContract: null,
        freshnessContractDiagnostics: {
          freshness_contract_incomplete: true,
          primary_failure_bucket: detailReason
        }
      },
      currentRoute: {
        currentBuyPrice: null,
        currentSellNet: null,
        currentProfit: null,
        currentSpreadPct: null,
        antiFakeReasons: [],
        antiFakePassed: false
      }
    }
  }

  if (!comparedItem || !Array.isArray(comparedItem?.perMarket) || !comparedItem.perMarket.length) {
    return buildEmitRevalidationResult({
      opportunity,
      nowIso: safeNowIso,
      nowMs,
      compareError: { code: "EMIT_REVALIDATION_MISSING_COMPARE_RESULT" }
    })
  }

  const routeFreshnessContract = buildRouteFreshnessContractFromCompareResult(comparedItem, {
    buyMarket,
    sellMarket
  })
  const publishValidation = evaluatePublishValidation({
    nowMs,
    nowIso: safeNowIso,
    buyMarket: routeFreshnessContract.buyMarket,
    sellMarket: routeFreshnessContract.sellMarket,
    buyRouteAvailable: routeFreshnessContract.buyRouteAvailable === true,
    sellRouteAvailable: routeFreshnessContract.sellRouteAvailable === true,
    buyRouteUpdatedAt: routeFreshnessContract.buyRouteUpdatedAt,
    sellRouteUpdatedAt: routeFreshnessContract.sellRouteUpdatedAt,
    buyListingAvailable: routeFreshnessContract.buyListingAvailable,
    sellListingAvailable: routeFreshnessContract.sellListingAvailable
  })
  const freshnessContractDiagnostics = buildFreshnessContractDiagnostics(
    routeFreshnessContract,
    publishValidation
  )

  const failedPublishValidation =
    !publishValidation.isPublishable ||
    String(publishValidation?.listingAvailabilityState || "").startsWith("unknown_") ||
    Boolean(freshnessContractDiagnostics?.missing_listing_availability)

  if (failedPublishValidation) {
    const classification = classifyPublishValidationFailure(
      publishValidation,
      freshnessContractDiagnostics
    )
    return {
      checked: true,
      passed: false,
      blockReason: classification.blockReason,
      detailReason: classification.detailReason,
      revalidatedAt: safeNowIso,
      marketHashName,
      buyMarket,
      sellMarket,
      publishValidation: {
        ...publishValidation,
        routeFreshnessContract,
        freshnessContractDiagnostics
      },
      currentRoute: {
        currentBuyPrice: null,
        currentSellNet: null,
        currentProfit: null,
        currentSpreadPct: null,
        antiFakeReasons: [],
        antiFakePassed: false
      }
    }
  }

  const exactRoute = resolveExactRouteEvaluation(opportunity, comparedItem, publishValidation)
  if (!exactRoute?.evaluation || exactRoute?.missingRouteReason) {
    return {
      checked: true,
      passed: false,
      blockReason: EMIT_BLOCK_REASONS.UNAVAILABLE,
      detailReason: normalizeText(exactRoute?.missingRouteReason) || "required_leg_unavailable",
      revalidatedAt: safeNowIso,
      marketHashName,
      buyMarket,
      sellMarket,
      publishValidation: {
        ...publishValidation,
        routeFreshnessContract,
        freshnessContractDiagnostics
      },
      currentRoute: {
        currentBuyPrice: exactRoute?.currentBuyPrice ?? null,
        currentSellNet: exactRoute?.currentSellNet ?? null,
        currentProfit: exactRoute?.currentProfit ?? null,
        currentSpreadPct: exactRoute?.currentSpreadPct ?? null,
        antiFakeReasons: [],
        antiFakePassed: false
      }
    }
  }

  if (!exactRoute.evaluation.isOpportunity) {
    const classification = classifyExactEvaluationFailure(exactRoute)
    return {
      checked: true,
      passed: false,
      blockReason: classification.blockReason,
      detailReason: classification.detailReason,
      revalidatedAt: safeNowIso,
      marketHashName,
      buyMarket,
      sellMarket,
      publishValidation: {
        ...publishValidation,
        routeFreshnessContract,
        freshnessContractDiagnostics
      },
      currentRoute: {
        currentBuyPrice: exactRoute?.currentBuyPrice ?? null,
        currentSellNet: exactRoute?.currentSellNet ?? null,
        currentProfit: exactRoute?.currentProfit ?? null,
        currentSpreadPct: exactRoute?.currentSpreadPct ?? null,
        antiFakeReasons: Array.isArray(exactRoute?.evaluation?.antiFake?.reasons)
          ? exactRoute.evaluation.antiFake.reasons
          : [],
        antiFakePassed: false
      }
    }
  }

  return {
    checked: true,
    passed: true,
    blockReason: null,
    detailReason: null,
    revalidatedAt: safeNowIso,
    marketHashName,
    buyMarket,
    sellMarket,
    publishValidation: {
      ...publishValidation,
      routeFreshnessContract,
      freshnessContractDiagnostics
    },
    currentRoute: {
      currentBuyPrice: exactRoute?.currentBuyPrice ?? null,
      currentSellNet: exactRoute?.currentSellNet ?? null,
      currentProfit: exactRoute?.currentProfit ?? null,
      currentSpreadPct: exactRoute?.currentSpreadPct ?? null,
      antiFakeReasons: Array.isArray(exactRoute?.evaluation?.antiFake?.reasons)
        ? exactRoute.evaluation.antiFake.reasons
        : [],
      antiFakePassed: true
    }
  }
}

function buildEmitRevalidationMetadata(result = {}) {
  const publishValidation = toJsonObject(result?.publishValidation)
  const freshnessContract = toJsonObject(publishValidation?.routeFreshnessContract)
  const freshnessContractDiagnostics = toJsonObject(
    publishValidation?.freshnessContractDiagnostics
  )
  const currentRoute = toJsonObject(result?.currentRoute)
  const revalidatedAt = toIsoOrNull(result?.revalidatedAt)
  const blockReason = normalizeText(result?.blockReason) || null
  const detailReason = normalizeText(result?.detailReason) || null

  return {
    emit_revalidation_checked: Boolean(result?.checked),
    emitRevalidationChecked: Boolean(result?.checked),
    emit_revalidated_at: revalidatedAt,
    emitRevalidatedAt: revalidatedAt,
    emit_revalidation_passed: Boolean(result?.passed),
    emitRevalidationPassed: Boolean(result?.passed),
    emit_block_reason: blockReason,
    emitBlockReason: blockReason,
    emit_block_detail_reason: detailReason,
    emitBlockDetailReason: detailReason,
    emit_revalidation: {
      checked: Boolean(result?.checked),
      passed: Boolean(result?.passed),
      emit_revalidated_at: revalidatedAt,
      block_reason: blockReason,
      block_detail_reason: detailReason,
      buy_market: normalizeText(result?.buyMarket) || null,
      sell_market: normalizeText(result?.sellMarket) || null,
      current_buy_price: toPositiveOrNull(currentRoute?.currentBuyPrice),
      current_sell_net: toPositiveOrNull(currentRoute?.currentSellNet),
      current_profit: toFiniteOrNull(currentRoute?.currentProfit),
      current_spread_pct: toFiniteOrNull(currentRoute?.currentSpreadPct),
      anti_fake_passed: Boolean(currentRoute?.antiFakePassed),
      anti_fake_reasons: Array.isArray(currentRoute?.antiFakeReasons)
        ? currentRoute.antiFakeReasons
        : [],
      publish_freshness_state: normalizeText(publishValidation?.publishFreshnessState) || "missing",
      required_route_state:
        normalizeText(publishValidation?.requiredRouteState) || "missing_buy_and_sell_route",
      listing_availability_state:
        normalizeText(publishValidation?.listingAvailabilityState) || "unknown",
      stale_reason: normalizeText(publishValidation?.staleReason) || null,
      route_signal_observed_at: toIsoOrNull(publishValidation?.routeSignalObservedAt),
      route_freshness_contract: Object.keys(freshnessContract).length ? freshnessContract : null,
      freshness_contract_diagnostics: Object.keys(freshnessContractDiagnostics).length
        ? freshnessContractDiagnostics
        : null
    }
  }
}

async function revalidateOpportunitiesForEmit(opportunities = [], options = {}) {
  const rows = Array.isArray(opportunities) ? opportunities : []
  const safeNowIso = toIsoOrNull(options?.nowIso) || new Date().toISOString()
  const nowMs = new Date(safeNowIso).getTime()
  const diagnostics = createEmitRevalidationDiagnostics(rows.length)
  if (!rows.length) {
    return { results: [], diagnostics }
  }

  const compareInputsByName = new Map()
  for (const opportunity of rows) {
    const marketHashName = resolveOpportunityItemName(opportunity)
    if (!marketHashName || compareInputsByName.has(marketHashName)) continue
    compareInputsByName.set(marketHashName, buildEmitCompareInput(opportunity))
  }

  const comparedByName = {}
  const compareErrorsByName = {}
  const chunkSize = Math.max(
    Math.round(Number(options?.chunkSize || EMIT_REVALIDATION_CHUNK_SIZE)),
    1
  )
  const timeoutMs = Math.max(
    Math.round(Number(options?.timeoutMs || EMIT_REVALIDATION_TIMEOUT_MS)),
    1
  )

  for (const chunk of chunkArray(Array.from(compareInputsByName.values()), chunkSize)) {
    diagnostics.batches_attempted += 1
    try {
      const compared = await withTimeout(
        marketComparisonService.compareItems(chunk, {
          planTier: "full_access",
          entitlements: emitRevalidationEntitlements,
          allowLiveFetch: false,
          forceRefresh: false
        }),
        timeoutMs,
        "EMIT_REVALIDATION_TIMEOUT"
      )
      for (const row of Array.isArray(compared?.items) ? compared.items : []) {
        const marketHashName = resolveOpportunityItemName(row)
        if (marketHashName) comparedByName[marketHashName] = row
      }
      diagnostics.batches_completed += 1
    } catch (err) {
      if (normalizeText(err?.code) === "EMIT_REVALIDATION_TIMEOUT") {
        diagnostics.batches_timed_out += 1
      } else {
        diagnostics.batches_failed += 1
      }
      for (const item of chunk) {
        const marketHashName = resolveOpportunityItemName(item)
        if (!marketHashName) continue
        compareErrorsByName[marketHashName] = err
      }
    }
  }

  const results = rows.map((opportunity) =>
    buildEmitRevalidationResult({
      opportunity,
      comparedItem: comparedByName[resolveOpportunityItemName(opportunity)],
      compareError: compareErrorsByName[resolveOpportunityItemName(opportunity)] || null,
      nowIso: safeNowIso,
      nowMs
    })
  )

  for (const result of results) {
    if (result?.passed) continue
    diagnostics.blocked_on_emit_total += 1
    incrementCounter(
      diagnostics.blocked_on_emit_by_reason,
      normalizeText(result?.blockReason) || EMIT_BLOCK_REASONS.STALE,
      1
    )
    if (result?.blockReason === EMIT_BLOCK_REASONS.STALE) {
      diagnostics.stale_on_emit_count += 1
    } else if (result?.blockReason === EMIT_BLOCK_REASONS.UNAVAILABLE) {
      diagnostics.unavailable_on_emit_count += 1
    } else if (result?.blockReason === EMIT_BLOCK_REASONS.NON_EXECUTABLE) {
      diagnostics.non_executable_on_emit_count += 1
    } else if (result?.blockReason === EMIT_BLOCK_REASONS.INVALID_MARKET_INTEGRITY) {
      diagnostics.invalid_market_integrity_on_emit_count += 1
    }
  }

  return {
    results,
    diagnostics
  }
}

module.exports = {
  EMIT_BLOCK_REASONS,
  revalidateOpportunitiesForEmit,
  buildEmitRevalidationMetadata
}

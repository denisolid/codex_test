const globalActiveOpportunityRepo = require("../../repositories/globalActiveOpportunityRepository")
const globalOpportunityHistoryRepo = require("../../repositories/globalOpportunityHistoryRepository")
const globalOpportunityLifecycleLogRepo = require("../../repositories/globalOpportunityLifecycleLogRepository")
const diagnosticsWriter = require("../diagnosticsWriter")
const { buildHistoryRow, resolveExitEventType } = require("./feedHistoryPolicy")
const {
  LIFECYCLE_STATUS,
  resolveLifecycleStatusFromState,
  buildLifecycleRow
} = require("./opportunityLifecyclePolicy")
const {
  MIN_CONFIDENCE_CHANGE_LEVELS,
  MIN_LIQUIDITY_CHANGE_PCT,
  MIN_PROFIT_CHANGE_PCT,
  MIN_SCORE_CHANGE,
  MIN_SPREAD_CHANGE_PCT
} = require("../scanner/config")
const {
  buildSignature,
  buildOpportunityFingerprint,
  buildMaterialChangeHash,
  classifyOpportunityFeedEvent,
  buildFeedInsertRow
} = require("../scanner/feedPipeline")
const {
  resolvePublishValidationContextForOpportunity: resolveSharedPublishValidationContextForOpportunity
} = require("../scanner/publishValidation")
const {
  revalidateOpportunitiesForEmit,
  buildEmitRevalidationMetadata
} = require("../scanner/emitRevalidationService")
const {
  createPublisherMetrics,
  trackEligible,
  trackEmitted,
  trackBlockedOnEmit,
  finalizePublisherMetrics
} = require("../scanner/publisherMetricsService")

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
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function toBooleanOrNull(value) {
  if (value === true) return true
  if (value === false) return false
  if (value == null || value === "") return null
  const parsed = Number(value)
  if (Number.isFinite(parsed)) {
    if (parsed === 1) return true
    if (parsed === 0) return false
  }
  const raw = normalizeText(value).toLowerCase()
  if (!raw) return null
  if (raw === "true" || raw === "yes" || raw === "on") return true
  if (raw === "false" || raw === "no" || raw === "off") return false
  return null
}

function toSafeInteger(value, fallback = 1, min = 1) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), min)
}

function incrementCounterBy(target, key, amount = 1) {
  const safeKey = normalizeText(key)
  if (!safeKey) return
  target[safeKey] = Number(target[safeKey] || 0) + Number(amount || 0)
}

function safePercentChange(current, previous) {
  const now = toFiniteOrNull(current)
  const prev = toFiniteOrNull(previous)
  if (now == null || prev == null || prev === 0) return 0
  return Math.abs(((now - prev) / prev) * 100)
}

function confidenceLevel(value) {
  const normalized = normalizeText(value).toLowerCase()
  if (normalized === "high") return 3
  if (normalized === "medium") return 2
  return 1
}

function normalizeStringSet(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeText(value).toLowerCase())
        .filter(Boolean)
    )
  ).sort()
}

function buildFeedKey(row = {}) {
  return buildSignature({
    itemName: row.itemName || row.item_name,
    buyMarket: row.buyMarket || row.buy_market,
    sellMarket: row.sellMarket || row.sell_market
  })
}

function resolvePublishValidationContextForOpportunity(
  opportunity = {},
  nowMs = Date.now(),
  nowIso = null
) {
  return resolveSharedPublishValidationContextForOpportunity(opportunity, nowMs, nowIso)
}

function buildPublishValidationMetadata(validation = {}) {
  const signalAgeMs = toFiniteOrNull(validation?.signalAgeMs)
  const publishValidatedAt = toIsoOrNull(validation?.publishValidatedAt)
  const publishFreshnessState = normalizeText(validation?.publishFreshnessState) || "missing"
  const requiredRouteState =
    normalizeText(validation?.requiredRouteState) || "missing_buy_and_sell_route"
  const listingAvailabilityState =
    normalizeText(validation?.listingAvailabilityState) || "unknown"
  const staleReason = normalizeText(validation?.staleReason) || null
  const routeSignalObservedAt = toIsoOrNull(validation?.routeSignalObservedAt)
  const routeFreshnessContract =
    validation?.routeFreshnessContract &&
    typeof validation.routeFreshnessContract === "object" &&
    !Array.isArray(validation.routeFreshnessContract)
      ? validation.routeFreshnessContract
      : null
  const freshnessContractDiagnostics =
    validation?.freshnessContractDiagnostics &&
    typeof validation.freshnessContractDiagnostics === "object" &&
    !Array.isArray(validation.freshnessContractDiagnostics)
      ? validation.freshnessContractDiagnostics
      : null
  return {
    signal_age_ms: signalAgeMs,
    signalAgeMs: signalAgeMs,
    publish_validated_at: publishValidatedAt,
    publishValidatedAt: publishValidatedAt,
    publish_freshness_state: publishFreshnessState,
    publishFreshnessState: publishFreshnessState,
    required_route_state: requiredRouteState,
    requiredRouteState: requiredRouteState,
    listing_availability_state: listingAvailabilityState,
    listingAvailabilityState: listingAvailabilityState,
    stale_reason: staleReason,
    staleReason: staleReason,
    route_signal_observed_at: routeSignalObservedAt,
    routeSignalObservedAt: routeSignalObservedAt,
    route_freshness_contract: routeFreshnessContract,
    routeFreshnessContract: routeFreshnessContract,
    freshness_contract_diagnostics: freshnessContractDiagnostics,
    publish_validation: {
      is_publishable: Boolean(validation?.isPublishable),
      signal_age_ms: signalAgeMs,
      publish_validated_at: publishValidatedAt,
      publish_freshness_state: publishFreshnessState,
      required_route_state: requiredRouteState,
      listing_availability_state: listingAvailabilityState,
      stale_reason: staleReason,
      route_signal_observed_at: routeSignalObservedAt,
      route_freshness_contract: routeFreshnessContract,
      freshness_contract_diagnostics: freshnessContractDiagnostics
    }
  }
}

function resolveRowFingerprint(row = {}) {
  return normalizeText(
    row?.opportunity_fingerprint ||
      row?.opportunityFingerprint ||
      row?.metadata?.opportunity_fingerprint
  ).toLowerCase()
}

function resolveRowMaterialHash(row = {}) {
  return normalizeText(
    row?.material_change_hash ||
      row?.materialChangeHash ||
      row?.metadata?.material_change_hash
  ).toLowerCase()
}

function buildRowDedupIdentity(row = {}) {
  const fingerprint = resolveRowFingerprint(row)
  if (fingerprint) return `fp:${fingerprint}`
  const signature = normalizeText(buildFeedKey(row)).toLowerCase()
  if (!signature) return ""
  return `sig:${signature}::material:${resolveRowMaterialHash(row) || "na"}`
}

function resolveRowRecencyMs(row = {}) {
  const candidates = [
    row?.created_at,
    row?.createdAt,
    row?.last_seen_at,
    row?.lastSeenAt,
    row?.last_published_at,
    row?.lastPublishedAt,
    row?.detected_at,
    row?.detectedAt
  ]
  for (const value of candidates) {
    const iso = toIsoOrNull(value)
    if (!iso) continue
    const ts = new Date(iso).getTime()
    if (Number.isFinite(ts)) return ts
  }
  return 0
}

function collectOlderActiveDuplicateIds(rows = []) {
  const uniqueRows = []
  const seenIds = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!Boolean(row?.is_active)) continue
    const id = normalizeText(row?.id)
    if (!id || seenIds.has(id)) continue
    seenIds.add(id)
    uniqueRows.push(row)
  }

  uniqueRows.sort((left, right) => {
    const delta = resolveRowRecencyMs(right) - resolveRowRecencyMs(left)
    if (delta !== 0) return delta
    return normalizeText(right?.id).localeCompare(normalizeText(left?.id))
  })

  const seen = new Set()
  const duplicateIds = []
  for (const row of uniqueRows) {
    const dedupIdentity = buildRowDedupIdentity(row)
    if (!dedupIdentity) continue
    if (seen.has(dedupIdentity)) {
      duplicateIds.push(normalizeText(row?.id))
      continue
    }
    seen.add(dedupIdentity)
  }
  return Array.from(new Set(duplicateIds.filter(Boolean)))
}

function cloneWithEventMetadata(metadata = {}, event = {}) {
  return {
    ...toJsonObject(metadata),
    feed_event: normalizeText(event?.eventType || "new").toLowerCase() || "new",
    feed_event_reasons: Array.isArray(event?.changeReasons) ? event.changeReasons : [],
    feed_event_materially_changed: Boolean(event?.materiallyChanged)
  }
}

function buildPreparedInsertRow(opportunity = {}, { nowIso, scanRunId } = {}) {
  const insertRow = buildFeedInsertRow(opportunity, {
    scanRunId,
    detectedAt: nowIso,
    firstSeenAt: nowIso,
    lastSeenAt: nowIso,
    lastPublishedAt: nowIso,
    timesSeen: 1
  })
  const fingerprint =
    normalizeText(insertRow?.opportunity_fingerprint) ||
    buildOpportunityFingerprint(opportunity)
  const materialChangeHash =
    normalizeText(insertRow?.material_change_hash) || buildMaterialChangeHash(opportunity)
  insertRow.opportunity_fingerprint = fingerprint
  insertRow.material_change_hash = materialChangeHash
  insertRow.metadata = {
    ...toJsonObject(insertRow.metadata),
    opportunity_fingerprint: fingerprint || null,
    material_change_hash: materialChangeHash || null
  }
  return insertRow
}

function buildActiveRowFromInsert(insertRow = {}, options = {}) {
  const nowIso = options.nowIso || new Date().toISOString()
  const event = options.event || { eventType: "new", materiallyChanged: true, changeReasons: [] }
  return {
    opportunity_fingerprint: normalizeText(insertRow.opportunity_fingerprint).toLowerCase(),
    material_change_hash: normalizeText(insertRow.material_change_hash) || null,
    scan_run_id: normalizeText(options.scanRunId) || null,
    market_hash_name: insertRow.market_hash_name,
    item_name: insertRow.item_name,
    category: insertRow.category,
    buy_market: insertRow.buy_market,
    buy_price: insertRow.buy_price,
    sell_market: insertRow.sell_market,
    sell_net: insertRow.sell_net,
    profit: insertRow.profit,
    spread_pct: insertRow.spread_pct,
    opportunity_score: insertRow.opportunity_score,
    execution_confidence: insertRow.execution_confidence,
    quality_grade: insertRow.quality_grade,
    liquidity_label: insertRow.liquidity_label,
    market_signal_observed_at: insertRow.market_signal_observed_at || null,
    first_seen_at: toIsoOrNull(insertRow.first_seen_at) || nowIso,
    last_seen_at: nowIso,
    last_published_at: nowIso,
    refresh_status: "pending",
    live_status: "live",
    latest_signal_age_hours: null,
    metadata: cloneWithEventMetadata(insertRow.metadata, event)
  }
}

function buildActiveUpdatePatch(previousRow = {}, insertRow = {}, options = {}) {
  const nowIso = options.nowIso || new Date().toISOString()
  const event = options.event || { eventType: "updated", materiallyChanged: true, changeReasons: [] }
  const previousMetadata = toJsonObject(previousRow.metadata)
  const insertMetadata = toJsonObject(insertRow.metadata)
  const firstSeenAt =
    toIsoOrNull(previousRow.first_seen_at || previousRow.firstSeenAt) ||
    toIsoOrNull(insertRow.first_seen_at || insertRow.firstSeenAt) ||
    nowIso
  return {
    opportunity_fingerprint:
      normalizeText(insertRow.opportunity_fingerprint) ||
      normalizeText(previousRow.opportunity_fingerprint),
    material_change_hash:
      normalizeText(insertRow.material_change_hash) ||
      normalizeText(previousRow.material_change_hash) ||
      null,
    scan_run_id: normalizeText(options.scanRunId) || null,
    market_hash_name: insertRow.market_hash_name,
    item_name: insertRow.item_name,
    category: insertRow.category,
    buy_market: insertRow.buy_market,
    buy_price: insertRow.buy_price,
    sell_market: insertRow.sell_market,
    sell_net: insertRow.sell_net,
    profit: insertRow.profit,
    spread_pct: insertRow.spread_pct,
    opportunity_score: insertRow.opportunity_score,
    execution_confidence: insertRow.execution_confidence,
    quality_grade: insertRow.quality_grade,
    liquidity_label: insertRow.liquidity_label,
    market_signal_observed_at:
      insertRow.market_signal_observed_at ?? previousRow.market_signal_observed_at ?? null,
    first_seen_at: firstSeenAt,
    last_seen_at: nowIso,
    last_published_at: nowIso,
    refresh_status: "pending",
    live_status: "live",
    latest_signal_age_hours: null,
    metadata: cloneWithEventMetadata(
      {
        ...previousMetadata,
        ...insertMetadata
      },
      event
    )
  }
}

function buildLegacyUpdatePatch(previousRow = {}, insertRow = {}, options = {}) {
  const nowIso = options.nowIso || new Date().toISOString()
  const event = options.event || { eventType: "updated", materiallyChanged: true, changeReasons: [] }
  const previousMetadata = toJsonObject(previousRow.metadata)
  const insertMetadata = toJsonObject(insertRow.metadata)
  const firstSeenAt =
    toIsoOrNull(previousRow.first_seen_at || previousRow.firstSeenAt) ||
    toIsoOrNull(insertRow.first_seen_at || insertRow.firstSeenAt) ||
    toIsoOrNull(previousRow.discovered_at || previousRow.detected_at) ||
    nowIso
  const timesSeen = toSafeInteger(previousRow.times_seen ?? previousRow.timesSeen, 1, 1) + 1
  return {
    item_name: insertRow.item_name,
    market_hash_name: insertRow.market_hash_name,
    category: insertRow.category,
    buy_market: insertRow.buy_market,
    buy_price: insertRow.buy_price,
    sell_market: insertRow.sell_market,
    sell_net: insertRow.sell_net,
    profit: insertRow.profit,
    spread_pct: insertRow.spread_pct,
    opportunity_score: insertRow.opportunity_score,
    execution_confidence: insertRow.execution_confidence,
    quality_grade: insertRow.quality_grade,
    liquidity_label: insertRow.liquidity_label,
    detected_at: nowIso,
    discovered_at: firstSeenAt,
    first_seen_at: firstSeenAt,
    last_seen_at: nowIso,
    last_published_at: nowIso,
    times_seen: timesSeen,
    opportunity_fingerprint:
      normalizeText(insertRow.opportunity_fingerprint) ||
      normalizeText(previousRow.opportunity_fingerprint) ||
      null,
    material_change_hash:
      normalizeText(insertRow.material_change_hash) ||
      normalizeText(previousRow.material_change_hash) ||
      null,
    market_signal_observed_at:
      insertRow.market_signal_observed_at ?? previousRow.market_signal_observed_at ?? null,
    feed_published_at: nowIso,
    refresh_status: "pending",
    live_status: "live",
    latest_signal_age_hours: null,
    scan_run_id: normalizeText(options.scanRunId) || null,
    is_active: true,
    is_duplicate: event.eventType === "duplicate",
    metadata: cloneWithEventMetadata(
      {
        ...previousMetadata,
        ...insertMetadata,
        first_seen_at: firstSeenAt,
        last_seen_at: nowIso,
        last_published_at: nowIso,
        times_seen: timesSeen
      },
      event
    )
  }
}

function buildExpiredStatuses(validation = {}) {
  const refreshStatus =
    validation.publishFreshnessState === "stale" ? "stale" : "degraded"
  const liveStatus = validation.publishFreshnessState === "stale" ? "stale" : "degraded"
  const latestSignalAgeHours =
    validation.signalAgeMs == null
      ? null
      : Number((Number(validation.signalAgeMs) / (60 * 60 * 1000)).toFixed(3))
  return { refreshStatus, liveStatus, latestSignalAgeHours }
}

function buildExpiredActivePatch(previousRow = {}, validation = {}, nowIso, options = {}) {
  const existingMetadata = toJsonObject(previousRow.metadata)
  const publishValidationMetadata = buildPublishValidationMetadata(validation)
  const { refreshStatus, liveStatus, latestSignalAgeHours } = buildExpiredStatuses(validation)
  const additionalMetadata = toJsonObject(options?.additionalMetadata)
  const reason =
    normalizeText(options?.reasonOverride) ||
    normalizeText(validation.staleReason) ||
    "publish_validation_failed"
  const eventType =
    resolveExitEventType({
      liveStatus,
      refreshStatus
    }) || "degraded"
  return {
    last_seen_at: nowIso,
    market_signal_observed_at:
      toIsoOrNull(validation.routeSignalObservedAt) || previousRow.market_signal_observed_at || null,
    refresh_status: refreshStatus,
    live_status: liveStatus,
    latest_signal_age_hours: latestSignalAgeHours,
    metadata: {
      ...existingMetadata,
      ...publishValidationMetadata,
      ...additionalMetadata,
      publish_validation: {
        ...toJsonObject(existingMetadata.publish_validation),
        ...toJsonObject(publishValidationMetadata.publish_validation),
        is_publishable: false
      },
      feed_event: eventType,
      feed_event_reasons: [reason],
      feed_event_materially_changed: true
    }
  }
}

function toEventPreviousRow(previousRow = {}) {
  if (!previousRow) return null
  return {
    ...previousRow,
    is_active: normalizeText(previousRow.live_status).toLowerCase() === "live"
  }
}

function classifyNormalizedEvent(opportunity = {}, previousRow = null, insertRow = {}) {
  const baseEvent = classifyOpportunityFeedEvent(opportunity, toEventPreviousRow(previousRow))

  const buyPriceChangePct = safePercentChange(
    opportunity.buyPrice ?? opportunity.buy_price,
    previousRow?.buy_price ?? previousRow?.buyPrice
  )
  const sellNetChangePct = safePercentChange(
    opportunity.sellNet ?? opportunity.sell_net,
    previousRow?.sell_net ?? previousRow?.sellNet
  )
  const profitChangePct = safePercentChange(opportunity.profit, previousRow?.profit)
  const scoreNow = toFiniteOrNull(opportunity.score ?? opportunity.opportunity_score)
  const scorePrev = toFiniteOrNull(previousRow?.opportunity_score)
  const spreadChangePct = safePercentChange(opportunity.spread, previousRow?.spread_pct)
  const liquidityNow = toFiniteOrNull(opportunity.liquidity)
  const liquidityPrev = toFiniteOrNull(previousRow?.metadata?.liquidity_value)
  const liquidityChangePct = safePercentChange(liquidityNow, liquidityPrev)
  const confidenceDelta = Math.abs(
    confidenceLevel(opportunity.executionConfidence) -
      confidenceLevel(previousRow?.execution_confidence)
  )
  const flagsShifted =
    normalizeStringSet(opportunity.flags).join("|") !==
    normalizeStringSet(previousRow?.metadata?.flags).join("|")
  const badgesShifted =
    normalizeStringSet(opportunity.badges).join("|") !==
    normalizeStringSet(previousRow?.metadata?.badges).join("|")
  const freshnessShifted =
    toIsoOrNull(
      opportunity?.metadata?.latest_market_signal_at ?? opportunity?.latestMarketSignalAt
    ) !== toIsoOrNull(previousRow?.metadata?.latest_market_signal_at) ||
    toBooleanOrNull(opportunity?.metadata?.stale_result ?? opportunity?.staleResult) !==
      toBooleanOrNull(previousRow?.metadata?.stale_result) ||
    toFiniteOrNull(
      opportunity?.metadata?.stale_threshold_used ?? opportunity?.staleThresholdUsed
    ) !== toFiniteOrNull(previousRow?.metadata?.stale_threshold_used)
  const previousMaterialHash = normalizeText(previousRow?.material_change_hash)
  const nextMaterialHash = normalizeText(insertRow?.material_change_hash)
  const fingerprint = normalizeText(insertRow?.opportunity_fingerprint)
  const fingerprintShifted =
    Boolean(previousRow) &&
    Boolean(fingerprint) &&
    normalizeText(previousRow?.opportunity_fingerprint) &&
    normalizeText(previousRow?.opportunity_fingerprint) !== fingerprint
  const materialHashShifted =
    Boolean(previousRow) &&
    Boolean(previousMaterialHash) &&
    Boolean(nextMaterialHash) &&
    previousMaterialHash !== nextMaterialHash
  const materiallyChanged =
    Boolean(baseEvent?.materiallyChanged) ||
    buyPriceChangePct >= 2 ||
    sellNetChangePct >= 2 ||
    profitChangePct >= Number(MIN_PROFIT_CHANGE_PCT || 0) ||
    (scoreNow != null &&
      scorePrev != null &&
      Math.abs(scoreNow - scorePrev) >= Number(MIN_SCORE_CHANGE || 0)) ||
    spreadChangePct >= Number(MIN_SPREAD_CHANGE_PCT || 0) ||
    liquidityChangePct >= Number(MIN_LIQUIDITY_CHANGE_PCT || 0) ||
    confidenceDelta >= Number(MIN_CONFIDENCE_CHANGE_LEVELS || 0) ||
    flagsShifted ||
    badgesShifted ||
    freshnessShifted ||
    fingerprintShifted ||
    materialHashShifted
  const changeReasons = Array.from(
    new Set([
      ...(Array.isArray(baseEvent?.changeReasons) ? baseEvent.changeReasons : []),
      ...(fingerprintShifted ? ["quote_identity", "fingerprint_shift"] : []),
      ...(materialHashShifted ? ["material_hash"] : []),
      ...(flagsShifted || badgesShifted ? ["diagnostics"] : []),
      ...(freshnessShifted ? ["freshness"] : [])
    ])
  )
  const normalizedEventType = !previousRow
    ? "new"
    : normalizeText(previousRow?.live_status).toLowerCase() !== "live"
      ? "reactivated"
      : materiallyChanged
        ? "updated"
        : "duplicate"

  return {
    eventType: normalizedEventType,
    materiallyChanged,
    changeReasons
  }
}

function createPreparedOpportunity(opportunity = {}, nowIso, scanRunId) {
  const insertRow = buildPreparedInsertRow(opportunity, { nowIso, scanRunId })
  return {
    opportunity,
    insertRow,
    key: buildFeedKey(insertRow) || buildFeedKey(opportunity),
    fingerprint: normalizeText(insertRow.opportunity_fingerprint).toLowerCase()
  }
}

function maybeTrackSet(setRef, value) {
  const text = normalizeText(value)
  if (text) setRef.add(text)
}

function buildRowsByFingerprint(rows = []) {
  const byFingerprint = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const fingerprint = normalizeText(row?.opportunity_fingerprint).toLowerCase()
    if (fingerprint && !byFingerprint[fingerprint]) {
      byFingerprint[fingerprint] = row
    }
  }
  return byFingerprint
}

async function publishBatch({
  scanRunId = null,
  opportunities = [],
  nowIso = null,
  trigger = "scan_publish",
  scannedCount = 0
} = {}) {
  const safeNowIso = toIsoOrNull(nowIso) || new Date().toISOString()
  const counters = {
    publishedCount: 0,
    blockedCount: 0,
    updatedCount: 0,
    reactivatedCount: 0,
    duplicateCount: 0,
    skippedUnchanged: 0,
    activeRowsWritten: 0,
    historyRowsWritten: 0,
    validationReasons: {},
    emitRevalidation: {
      emit_revalidation_checked: 0,
      emitted_after_revalidation: 0,
      blocked_on_emit_total: 0,
      blocked_on_emit_by_reason: {},
      stale_on_emit_count: 0,
      unavailable_on_emit_count: 0,
      non_executable_on_emit_count: 0,
      invalid_market_integrity_on_emit_count: 0
    },
    lifecycle: {
      detected_total: 0,
      published_total: 0,
      expired_total: 0,
      invalidated_total: 0,
      blocked_on_emit_total: 0
    },
    touchedFingerprints: [],
    touchedItemNames: []
  }
  const publisherMetrics = createPublisherMetrics("scanner_v2")

  const rows = (Array.isArray(opportunities) ? opportunities : []).filter(
    (row) => row?.rejected !== true
  )
  if (!rows.length) {
    return {
      ...counters,
      unchangedCount: 0,
      emittedCount: 0,
      publisherMetrics: finalizePublisherMetrics(publisherMetrics, { scannedCount })
    }
  }

  const preparedRows = rows.map((row) => createPreparedOpportunity(row, safeNowIso, scanRunId))
  const emitRevalidation = await revalidateOpportunitiesForEmit(rows, {
    nowIso: safeNowIso
  })
  counters.emitRevalidation = {
    ...counters.emitRevalidation,
    ...(emitRevalidation?.diagnostics || {})
  }
  const itemNames = Array.from(
    new Set(
      preparedRows
        .map((row) => normalizeText(row.insertRow.item_name || row.opportunity.itemName))
        .filter(Boolean)
    )
  )
  const fingerprints = Array.from(
    new Set(preparedRows.map((row) => normalizeText(row.fingerprint)).filter(Boolean))
  )

  const [activeRecentRows, activeFingerprintRows] = await Promise.all([
      globalActiveOpportunityRepo.getRecentRowsByItems({
        itemNames,
        includeExpired: true,
        limit: 1200
      }),
      fingerprints.length
        ? globalActiveOpportunityRepo.getRowsByFingerprints({
            fingerprints,
            includeExpired: true,
            limit: Math.max(1200, fingerprints.length * 2)
          })
        : Promise.resolve([])
    ])

  const activeLatestByKey = {}
  for (const row of activeRecentRows || []) {
    const key = buildFeedKey(row)
    if (key && !activeLatestByKey[key]) activeLatestByKey[key] = row
  }
  const activeByFingerprint = {}
  for (const row of activeFingerprintRows || []) {
    const fingerprint = normalizeText(row?.opportunity_fingerprint).toLowerCase()
    if (fingerprint && !activeByFingerprint[fingerprint]) {
      activeByFingerprint[fingerprint] = row
    }
  }

  const activeInserts = []
  const activeUpdates = []
  const historyPlans = []
  const lifecycleRows = []
  const publishLifecyclePlans = []
  const pendingInsertFingerprints = new Set()
  const touchedFingerprints = new Set()
  const touchedItemNames = new Set()
  const writtenFingerprints = new Set()

  for (const [index, prepared] of preparedRows.entries()) {
    const opportunity = prepared.opportunity
    trackEligible(publisherMetrics, opportunity)
    const insertRow = prepared.insertRow
    const key = prepared.key
    const fingerprint = prepared.fingerprint
    const previousActive = activeByFingerprint[fingerprint] || activeLatestByKey[key] || null

    const emitRevalidationResult =
      Array.isArray(emitRevalidation?.results) && emitRevalidation.results[index]
        ? emitRevalidation.results[index]
        : null
    const publishValidation =
      emitRevalidationResult?.publishValidation ||
      resolvePublishValidationContextForOpportunity(opportunity, Date.now(), safeNowIso)
    const publishValidationMetadata = buildPublishValidationMetadata(publishValidation)
    const emitRevalidationMetadata = buildEmitRevalidationMetadata(emitRevalidationResult)
    const routeSignalObservedAt =
      toIsoOrNull(publishValidation.routeSignalObservedAt) ||
      toIsoOrNull(insertRow.market_signal_observed_at) ||
      null
    const opportunityWithValidation = {
      ...opportunity,
      metadata: {
        ...toJsonObject(opportunity?.metadata),
        latest_market_signal_at:
          routeSignalObservedAt ||
          toIsoOrNull(opportunity?.metadata?.latest_market_signal_at) ||
          toIsoOrNull(opportunity?.latestMarketSignalAt) ||
          null,
        ...publishValidationMetadata,
        ...emitRevalidationMetadata
      }
    }
    const insertRowForPublish = {
      ...insertRow,
      market_signal_observed_at: routeSignalObservedAt,
      metadata: {
        ...toJsonObject(insertRow.metadata),
        latest_market_signal_at:
          routeSignalObservedAt ||
          toIsoOrNull(insertRow?.metadata?.latest_market_signal_at) ||
          null,
        ...publishValidationMetadata,
        ...emitRevalidationMetadata
      }
    }
    const lifecycleSourceRow = {
      ...insertRowForPublish,
      ...opportunityWithValidation,
      metadata: {
        ...toJsonObject(insertRowForPublish.metadata),
        ...toJsonObject(opportunityWithValidation.metadata)
      }
    }

    maybeTrackSet(touchedFingerprints, fingerprint)
    maybeTrackSet(touchedItemNames, insertRowForPublish.item_name)
    const detectedLifecycleRow = buildLifecycleRow({
      writerStage: "publish",
      scanRunId,
      lifecycleStatus: LIFECYCLE_STATUS.DETECTED,
      sourceRow: lifecycleSourceRow,
      eventAt: safeNowIso,
      publishTimestamp: safeNowIso
    })
    if (detectedLifecycleRow) {
      lifecycleRows.push(detectedLifecycleRow)
      counters.lifecycle.detected_total += 1
    }

    if (!emitRevalidationResult?.passed) {
      const blockedReason =
        normalizeText(emitRevalidationResult?.blockReason) ||
        normalizeText(publishValidation.staleReason) ||
        "emit_revalidation_failed"
      trackBlockedOnEmit(publisherMetrics, opportunity, {
        reason: blockedReason
      })
      counters.blockedCount += 1
      counters.lifecycle.blocked_on_emit_total += 1
      incrementCounterBy(
        counters.validationReasons,
        normalizeText(emitRevalidationResult?.detailReason) ||
          normalizeText(publishValidation.staleReason) ||
          blockedReason ||
          "emit_revalidation_failed",
        1
      )
      const blockedLifecycleRow = buildLifecycleRow({
        writerStage: "publish",
        scanRunId,
        lifecycleStatus: LIFECYCLE_STATUS.BLOCKED_ON_EMIT,
        sourceRow: lifecycleSourceRow,
        eventAt: safeNowIso,
        reason:
          blockedReason,
        publishTimestamp: safeNowIso
      })
      if (blockedLifecycleRow) {
        lifecycleRows.push(blockedLifecycleRow)
      }

      if (previousActive && normalizeText(previousActive.live_status).toLowerCase() === "live") {
        const activePatch = buildExpiredActivePatch(previousActive, publishValidation, safeNowIso, {
          reasonOverride:
            normalizeText(emitRevalidationResult?.blockReason) ||
            normalizeText(publishValidation.staleReason) ||
            "emit_revalidation_failed",
          additionalMetadata: emitRevalidationMetadata
        })
        activeUpdates.push({ id: previousActive.id, patch: activePatch })
        const exitEventType = resolveExitEventType({
          liveStatus: activePatch.live_status,
          refreshStatus: activePatch.refresh_status
        })
        historyPlans.push({
          fingerprint:
            normalizeText(activePatch.opportunity_fingerprint) ||
            normalizeText(previousActive.opportunity_fingerprint),
          eventType: exitEventType,
          materiallyChanged: true,
          reason:
            blockedReason
        })
        const lifecycleStatus = resolveLifecycleStatusFromState({
          liveStatus: activePatch.live_status,
          refreshStatus: activePatch.refresh_status
        })
        const blockedActiveLifecycleRow = buildLifecycleRow({
          writerStage: "publish",
          scanRunId,
          lifecycleStatus,
          activeOpportunityId: previousActive.id,
          sourceRow: {
            ...previousActive,
            ...activePatch
          },
          eventAt: safeNowIso,
          reason:
            blockedReason,
          publishTimestamp:
            toIsoOrNull(previousActive?.last_published_at || previousActive?.lastPublishedAt) ||
            safeNowIso
        })
        if (blockedActiveLifecycleRow) {
          lifecycleRows.push(blockedActiveLifecycleRow)
          if (lifecycleStatus === LIFECYCLE_STATUS.EXPIRED) {
            counters.lifecycle.expired_total += 1
          } else if (lifecycleStatus === LIFECYCLE_STATUS.INVALIDATED) {
            counters.lifecycle.invalidated_total += 1
          }
        }
        maybeTrackSet(
          writtenFingerprints,
          normalizeText(activePatch.opportunity_fingerprint) ||
            normalizeText(previousActive.opportunity_fingerprint)
        )
      }
      continue
    }

    const normalizedEvent = classifyNormalizedEvent(
      opportunityWithValidation,
      previousActive,
      insertRowForPublish
    )
    const insertRowWithEvent = {
      ...insertRowForPublish,
      refresh_status: "pending",
      live_status: "live",
      metadata: cloneWithEventMetadata(insertRowForPublish.metadata, normalizedEvent)
    }

    if (!previousActive) {
      if (fingerprint && pendingInsertFingerprints.has(fingerprint)) {
        counters.skippedUnchanged += 1
        continue
      }
      pendingInsertFingerprints.add(fingerprint)
      const activeInsert = buildActiveRowFromInsert(insertRowWithEvent, {
        nowIso: safeNowIso,
        scanRunId,
        event: normalizedEvent
      })
      activeInserts.push(activeInsert)
      trackEmitted(publisherMetrics, opportunity)
      historyPlans.push({
        fingerprint: activeInsert.opportunity_fingerprint,
        eventType: normalizedEvent.eventType,
        materiallyChanged: normalizedEvent.materiallyChanged,
        reason: null
      })
      publishLifecyclePlans.push({
        fingerprint: activeInsert.opportunity_fingerprint,
        publishTimestamp: safeNowIso
      })
      maybeTrackSet(writtenFingerprints, activeInsert.opportunity_fingerprint)
      continue
    }

    if (normalizedEvent.eventType === "updated") {
      counters.updatedCount += 1
      trackEmitted(publisherMetrics, opportunity)
    } else if (normalizedEvent.eventType === "reactivated") {
      counters.reactivatedCount += 1
      trackEmitted(publisherMetrics, opportunity)
    } else {
      counters.duplicateCount += 1
    }

    const activePatch = buildActiveUpdatePatch(previousActive, insertRowWithEvent, {
      nowIso: safeNowIso,
      scanRunId,
      event: normalizedEvent
    })
    activeUpdates.push({
      id: previousActive.id,
      patch: activePatch
    })
    historyPlans.push({
      fingerprint:
        normalizeText(activePatch.opportunity_fingerprint) ||
        normalizeText(previousActive.opportunity_fingerprint),
      eventType: normalizedEvent.eventType,
      materiallyChanged: normalizedEvent.materiallyChanged,
      reason: null
    })
    if (normalizedEvent.eventType !== "duplicate") {
      publishLifecyclePlans.push({
        fingerprint:
          normalizeText(activePatch.opportunity_fingerprint) ||
          normalizeText(previousActive.opportunity_fingerprint),
        publishTimestamp: safeNowIso
      })
    }
    maybeTrackSet(
      writtenFingerprints,
      normalizeText(activePatch.opportunity_fingerprint) ||
        normalizeText(previousActive.opportunity_fingerprint)
    )
  }

  counters.publishedCount =
    activeInserts.length + counters.updatedCount + counters.reactivatedCount
  counters.emitRevalidation.emitted_after_revalidation = counters.publishedCount
  counters.touchedFingerprints = Array.from(touchedFingerprints)
  counters.touchedItemNames = Array.from(touchedItemNames)

  if (activeUpdates.length) {
    counters.activeRowsWritten += await globalActiveOpportunityRepo.updateRowsById(activeUpdates)
  }
  if (activeInserts.length) {
    const insertedRows = await globalActiveOpportunityRepo.upsertRows(activeInserts)
    counters.activeRowsWritten += Array.isArray(insertedRows) ? insertedRows.length : 0
  }

  const persistedActiveRows = Array.from(writtenFingerprints).length
    ? await globalActiveOpportunityRepo.getRowsByFingerprints({
        fingerprints: Array.from(writtenFingerprints),
        includeExpired: true,
        limit: Math.max(1200, Array.from(writtenFingerprints).length * 2)
      })
    : []
  const persistedActiveByFingerprint = buildRowsByFingerprint(persistedActiveRows)
  for (const plan of publishLifecyclePlans) {
    const persistedRow = persistedActiveByFingerprint[normalizeText(plan.fingerprint).toLowerCase()]
    const publishedLifecycleRow = buildLifecycleRow({
      writerStage: "publish",
      scanRunId,
      lifecycleStatus: LIFECYCLE_STATUS.PUBLISHED,
      activeOpportunityId: persistedRow?.id || null,
      sourceRow: persistedRow || null,
      eventAt: safeNowIso,
      publishTimestamp: plan.publishTimestamp || safeNowIso
    })
    if (publishedLifecycleRow) {
      lifecycleRows.push(publishedLifecycleRow)
      counters.lifecycle.published_total += 1
    }
  }

  const historyRows = historyPlans
    .map((plan) =>
      buildHistoryRow({
        writerStage: "publish",
        scanRunId,
        eventType: plan.eventType,
        activeRow: persistedActiveByFingerprint[normalizeText(plan.fingerprint).toLowerCase()],
        eventAt: safeNowIso,
        reason: plan.reason,
        materiallyChanged: plan.materiallyChanged
      })
    )
    .filter(Boolean)

  if (historyRows.length) {
    const insertedHistory = await globalOpportunityHistoryRepo.insertRows(historyRows)
    counters.historyRowsWritten = Array.isArray(insertedHistory) ? insertedHistory.length : 0
  }
  if (lifecycleRows.length) {
    await globalOpportunityLifecycleLogRepo.insertRows(lifecycleRows)
  }

  await diagnosticsWriter.writePublishBatch({
    scanRunId,
    counters,
    lifecycle: counters.lifecycle,
    emitRevalidation: counters.emitRevalidation,
    validationReasons: counters.validationReasons,
    touchedItemNames: counters.touchedItemNames,
    timings: {
      trigger,
      publishedAt: safeNowIso
    }
  })

  if (historyRows.length) {
    await diagnosticsWriter.writePublishDecisions({
      scanRunId,
      rows: historyRows
    })
  }

  return {
    publishedCount: counters.publishedCount,
    blockedCount: counters.blockedCount,
    updatedCount: counters.updatedCount,
    reactivatedCount: counters.reactivatedCount,
    unchangedCount: counters.duplicateCount + counters.skippedUnchanged,
    activeRowsWritten: counters.activeRowsWritten,
    historyRowsWritten: counters.historyRowsWritten,
    validationReasons: counters.validationReasons,
    lifecycle: counters.lifecycle,
    emitRevalidation: counters.emitRevalidation,
    touchedFingerprints: counters.touchedFingerprints,
    touchedItemNames: counters.touchedItemNames,
    emittedCount: counters.publishedCount,
    publisherMetrics: finalizePublisherMetrics(publisherMetrics, {
      scannedCount
    })
  }
}

module.exports = {
  publishBatch
}

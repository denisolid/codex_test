const { createHash } = require("crypto")

const LIFECYCLE_STATUS = Object.freeze({
  DETECTED: "detected",
  PUBLISHED: "published",
  EXPIRED: "expired",
  INVALIDATED: "invalidated",
  BLOCKED_ON_EMIT: "blocked_on_emit"
})

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
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

function resolveValue(source = {}, aliases = []) {
  for (const alias of Array.isArray(aliases) ? aliases : []) {
    const value = source?.[alias]
    if (value != null && value !== "") return value
  }
  return null
}

function resolveMetadataValue(metadata = {}, aliases = []) {
  for (const alias of Array.isArray(aliases) ? aliases : []) {
    const value = metadata?.[alias]
    if (value != null && value !== "") return value
  }
  return null
}

function resolveField(source = {}, metadata = {}, aliases = []) {
  return resolveValue(source, aliases) ?? resolveMetadataValue(metadata, aliases) ?? null
}

function normalizeLifecycleStatus(value = "") {
  const normalized = normalizeText(value).toLowerCase()
  if (
    normalized === LIFECYCLE_STATUS.DETECTED ||
    normalized === LIFECYCLE_STATUS.PUBLISHED ||
    normalized === LIFECYCLE_STATUS.EXPIRED ||
    normalized === LIFECYCLE_STATUS.INVALIDATED ||
    normalized === LIFECYCLE_STATUS.BLOCKED_ON_EMIT
  ) {
    return normalized
  }
  return ""
}

function buildLifecycleEventKey({
  writerStage,
  scanRunId,
  lifecycleStatus,
  fingerprint,
  materialChangeHash,
  activeOpportunityId,
  reason,
  eventAt,
  itemName,
  buyMarket,
  sellMarket
} = {}) {
  const payload = [
    normalizeText(writerStage).toLowerCase() || "unknown",
    normalizeText(scanRunId).toLowerCase() || "na",
    normalizeText(lifecycleStatus).toLowerCase() || "na",
    normalizeText(fingerprint).toLowerCase() || "na",
    normalizeText(materialChangeHash).toLowerCase() || "na",
    normalizeText(activeOpportunityId).toLowerCase() || "na",
    normalizeText(reason).toLowerCase() || "na",
    normalizeText(eventAt).toLowerCase() || "na",
    normalizeText(itemName).toLowerCase() || "na",
    normalizeText(buyMarket).toLowerCase() || "na",
    normalizeText(sellMarket).toLowerCase() || "na"
  ].join("|")
  return `gol_${createHash("sha1").update(payload).digest("hex")}`
}

function resolveLifecycleStatusFromState({ liveStatus, refreshStatus } = {}) {
  const normalizedLive = normalizeText(liveStatus).toLowerCase()
  const normalizedRefresh = normalizeText(refreshStatus).toLowerCase()
  if (normalizedLive === "stale" && normalizedRefresh === "stale") {
    return LIFECYCLE_STATUS.EXPIRED
  }
  if (
    normalizedLive === "degraded" ||
    normalizedRefresh === "degraded" ||
    normalizedRefresh === "failed"
  ) {
    return LIFECYCLE_STATUS.INVALIDATED
  }
  return ""
}

function buildLifecycleSnapshot(source = {}, options = {}) {
  const metadata = toJsonObject(source?.metadata)
  const lifecycleStatus =
    normalizeLifecycleStatus(options?.lifecycleStatus) ||
    normalizeLifecycleStatus(resolveField(source, metadata, ["lifecycle_status"])) ||
    null
  const eventAt = toIsoOrNull(options?.eventAt) || new Date().toISOString()
  const publishTimestamp =
    toIsoOrNull(
      options?.publishTimestamp ||
        resolveField(source, metadata, [
          "last_published_at",
          "lastPublishedAt",
          "feed_published_at",
          "feedPublishedAt"
        ])
    ) || null

  return {
    lifecycle_status: lifecycleStatus,
    writer_stage: normalizeText(options?.writerStage).toLowerCase() || "unknown",
    reason: normalizeText(options?.reason) || null,
    event_at: eventAt,
    publish_timestamp: publishTimestamp,
    item_name: resolveField(source, metadata, ["item_name", "itemName", "market_hash_name", "marketHashName"]),
    market_hash_name: resolveField(source, metadata, ["market_hash_name", "marketHashName", "item_name", "itemName"]),
    item_id: resolveField(source, metadata, ["item_id", "itemId"]),
    item_subcategory: resolveField(source, metadata, ["item_subcategory", "itemSubcategory"]),
    category: resolveField(source, metadata, ["category", "itemCategory"]),
    opportunity_fingerprint: normalizeText(
      resolveField(source, metadata, ["opportunity_fingerprint", "opportunityFingerprint"])
    ).toLowerCase() || null,
    material_change_hash:
      normalizeText(resolveField(source, metadata, ["material_change_hash", "materialChangeHash"])) ||
      null,
    scan_run_id: normalizeText(resolveField(source, metadata, ["scan_run_id", "scanRunId"])) || null,
    buy_market: normalizeText(resolveField(source, metadata, ["buy_market", "buyMarket"])).toLowerCase() || null,
    buy_price: toFiniteOrNull(resolveField(source, metadata, ["buy_price", "buyPrice"])),
    buy_url: resolveField(source, metadata, ["buy_url", "buyUrl"]),
    buy_route_available: resolveField(source, metadata, ["buy_route_available", "buyRouteAvailable"]),
    buy_route_updated_at:
      toIsoOrNull(resolveField(source, metadata, ["buy_route_updated_at", "buyRouteUpdatedAt"])) || null,
    buy_listing_available: resolveField(source, metadata, ["buy_listing_available", "buyListingAvailable"]),
    buy_listing_id: resolveField(source, metadata, ["buy_listing_id", "buyListingId"]),
    sell_market:
      normalizeText(resolveField(source, metadata, ["sell_market", "sellMarket"])).toLowerCase() || null,
    sell_net: toFiniteOrNull(resolveField(source, metadata, ["sell_net", "sellNet"])),
    sell_url: resolveField(source, metadata, ["sell_url", "sellUrl"]),
    sell_route_available: resolveField(source, metadata, ["sell_route_available", "sellRouteAvailable"]),
    sell_route_updated_at:
      toIsoOrNull(resolveField(source, metadata, ["sell_route_updated_at", "sellRouteUpdatedAt"])) || null,
    sell_listing_available: resolveField(source, metadata, ["sell_listing_available", "sellListingAvailable"]),
    sell_listing_id: resolveField(source, metadata, ["sell_listing_id", "sellListingId", "skinport_listing_id", "skinportListingId"]),
    profit: toFiniteOrNull(resolveField(source, metadata, ["profit"])),
    spread_pct: toFiniteOrNull(resolveField(source, metadata, ["spread_pct", "spread"])),
    reference_price: toFiniteOrNull(resolveField(source, metadata, ["reference_price", "referencePrice"])),
    market_coverage_count: toFiniteOrNull(
      resolveField(source, metadata, ["market_coverage_count", "marketCoverageCount", "marketCoverage"])
    ),
    volume_7d: toFiniteOrNull(resolveField(source, metadata, ["volume_7d", "volume7d"])),
    liquidity_label: resolveField(source, metadata, ["liquidity_label", "liquidityLabel"]),
    liquidity_value: toFiniteOrNull(resolveField(source, metadata, ["liquidity_value", "liquidityValue", "liquidity"])),
    liquidity_rank: toFiniteOrNull(resolveField(source, metadata, ["liquidity_rank", "liquidityRank"])),
    opportunity_score: toFiniteOrNull(resolveField(source, metadata, ["opportunity_score", "opportunityScore", "score"])),
    execution_confidence: resolveField(source, metadata, ["execution_confidence", "executionConfidence"]),
    quality_grade: resolveField(source, metadata, ["quality_grade", "qualityGrade"]),
    final_tier: resolveField(source, metadata, ["final_tier", "finalTier", "tier"]),
    market_signal_observed_at:
      toIsoOrNull(
        resolveField(source, metadata, ["market_signal_observed_at", "marketSignalObservedAt"])
      ) || null,
    latest_market_signal_at:
      toIsoOrNull(
        resolveField(source, metadata, ["latest_market_signal_at", "latestMarketSignalAt"])
      ) || null,
    publish_validated_at:
      toIsoOrNull(resolveField(source, metadata, ["publish_validated_at", "publishValidatedAt"])) || null,
    emit_revalidated_at:
      toIsoOrNull(resolveField(source, metadata, ["emit_revalidated_at", "emitRevalidatedAt"])) || null,
    last_revalidation_attempt_at:
      toIsoOrNull(
        resolveField(source, metadata, ["last_revalidation_attempt_at", "lastRevalidationAttemptAt"])
      ) || null,
    freshness_contract_diagnostics: toJsonObject(
      resolveField(source, metadata, ["freshness_contract_diagnostics", "freshnessContractDiagnostics"])
    ),
    route_freshness_contract: toJsonObject(
      resolveField(source, metadata, ["route_freshness_contract", "routeFreshnessContract"])
    ),
    publish_validation: toJsonObject(
      resolveField(source, metadata, ["publish_validation", "publishValidation"])
    ),
    emit_revalidation: toJsonObject(
      resolveField(source, metadata, ["emit_revalidation", "emitRevalidation"])
    ),
    metadata
  }
}

function buildLifecycleRow({
  writerStage,
  scanRunId,
  lifecycleStatus,
  activeOpportunityId = null,
  sourceRow = {},
  eventAt,
  reason = null,
  publishTimestamp = null
} = {}) {
  const safeLifecycleStatus = normalizeLifecycleStatus(lifecycleStatus)
  if (!safeLifecycleStatus) return null
  const safeSourceRow =
    sourceRow && typeof sourceRow === "object" && !Array.isArray(sourceRow) ? sourceRow : {}
  const snapshot = buildLifecycleSnapshot(safeSourceRow, {
    lifecycleStatus: safeLifecycleStatus,
    writerStage,
    reason,
    eventAt,
    publishTimestamp
  })
  const fingerprint = normalizeText(snapshot.opportunity_fingerprint).toLowerCase()
  if (!fingerprint) return null
  const safeEventAt = toIsoOrNull(eventAt) || new Date().toISOString()
  const safeReason = normalizeText(reason) || null

  return {
    lifecycle_event_key: buildLifecycleEventKey({
      writerStage,
      scanRunId,
      lifecycleStatus: safeLifecycleStatus,
      fingerprint,
      materialChangeHash: snapshot.material_change_hash,
      activeOpportunityId,
      reason: safeReason,
      eventAt: safeEventAt,
      itemName: snapshot.item_name,
      buyMarket: snapshot.buy_market,
      sellMarket: snapshot.sell_market
    }),
    active_opportunity_id: normalizeText(activeOpportunityId) || null,
    opportunity_fingerprint: fingerprint,
    scan_run_id: normalizeText(scanRunId) || null,
    lifecycle_status: safeLifecycleStatus,
    event_at: safeEventAt,
    category: normalizeText(snapshot.category).toLowerCase() || null,
    market_hash_name: snapshot.market_hash_name || null,
    item_name: snapshot.item_name || null,
    reason: safeReason,
    snapshot
  }
}

module.exports = {
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  resolveLifecycleStatusFromState,
  buildLifecycleSnapshot,
  buildLifecycleRow
}

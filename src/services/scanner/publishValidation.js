const { PUBLISH_MAX_SIGNAL_AGE_MS } = require("./config")

const DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS = PUBLISH_MAX_SIGNAL_AGE_MS

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIsoOrNull(value) {
  if (value == null || value === "") return null
  if (value instanceof Date) {
    const ts = value.getTime()
    if (!Number.isFinite(ts)) return null
    return new Date(ts).toISOString()
  }

  const numeric = toFiniteOrNull(value)
  if (numeric != null) {
    const normalizedTs =
      numeric >= 1e12
        ? Math.round(numeric)
        : numeric >= 1e9
          ? Math.round(numeric * 1000)
          : null
    if (normalizedTs != null) {
      const ts = new Date(normalizedTs).getTime()
      if (Number.isFinite(ts)) return new Date(ts).toISOString()
    }
  }

  const raw = normalizeText(value)
  if (!raw) return null
  const ts = new Date(raw).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toTriStateBoolean(value) {
  if (value === true) return true
  if (value === false) return false
  return null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function toArray(value) {
  return Array.isArray(value) ? value : []
}

function toJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {}
}

function normalizeSource(value) {
  return normalizeText(value).toLowerCase()
}

function computeAgeMs(isoValue, nowMs = Date.now()) {
  const safeIso = toIsoOrNull(isoValue)
  if (!safeIso) return null
  const ts = new Date(safeIso).getTime()
  if (!Number.isFinite(ts)) return null
  const ageMs = nowMs - ts
  if (!Number.isFinite(ageMs) || ageMs < 0) return null
  return Math.round(ageMs)
}

function resolveRequiredRouteState({
  buyMarket = "",
  sellMarket = "",
  buyRouteAvailable = false,
  sellRouteAvailable = false
} = {}) {
  const needsBuyRoute = Boolean(normalizeText(buyMarket))
  const needsSellRoute = Boolean(normalizeText(sellMarket))
  const missingBuyRoute = needsBuyRoute && !Boolean(buyRouteAvailable)
  const missingSellRoute = needsSellRoute && !Boolean(sellRouteAvailable)

  if (missingBuyRoute && missingSellRoute) return "missing_buy_and_sell_route"
  if (missingBuyRoute) return "missing_buy_route"
  if (missingSellRoute) return "missing_sell_route"
  if (!needsBuyRoute && !needsSellRoute) return "missing_buy_and_sell_route"
  return "ready"
}

function resolveListingAvailabilityState({
  buyMarket = "",
  sellMarket = "",
  buyListingAvailable = null,
  sellListingAvailable = null
} = {}) {
  const requiresBuyListing = normalizeText(buyMarket).toLowerCase() === "skinport"
  const requiresSellListing = normalizeText(sellMarket).toLowerCase() === "skinport"

  if (!requiresBuyListing && !requiresSellListing) {
    return "not_required"
  }

  const buyState = toTriStateBoolean(buyListingAvailable)
  const sellState = toTriStateBoolean(sellListingAvailable)
  const missingBuy = requiresBuyListing && buyState === false
  const missingSell = requiresSellListing && sellState === false
  const unknownBuy = requiresBuyListing && buyState == null
  const unknownSell = requiresSellListing && sellState == null

  if (missingBuy && missingSell) return "missing_buy_and_sell_listing"
  if (missingBuy) return "missing_buy_listing"
  if (missingSell) return "missing_sell_listing"
  if (unknownBuy && unknownSell) return "unknown_buy_and_sell_listing"
  if (unknownBuy) return "unknown_buy_listing"
  if (unknownSell) return "unknown_sell_listing"
  return "available"
}

function resolveRouteSignalObservedAt(buyRouteUpdatedAt, sellRouteUpdatedAt) {
  const buyIso = toIsoOrNull(buyRouteUpdatedAt)
  const sellIso = toIsoOrNull(sellRouteUpdatedAt)
  if (!buyIso || !sellIso) return null
  const buyTs = new Date(buyIso).getTime()
  const sellTs = new Date(sellIso).getTime()
  if (!Number.isFinite(buyTs) || !Number.isFinite(sellTs)) return null
  return new Date(Math.min(buyTs, sellTs)).toISOString()
}

function resolveRouteMarketRow(perMarket = [], source = "") {
  const normalizedSource = normalizeSource(source)
  if (!normalizedSource) return null
  return (
    toArray(perMarket).find(
      (row) => normalizeSource(row?.source || row?.market) === normalizedSource
    ) || null
  )
}

function resolveRouteUpdatedAt(row = {}) {
  return toIsoOrNull(row?.updatedAt || row?.updated_at || row?.fetched_at || row?.fetchedAt)
}

function resolveRouteListingId(row = {}) {
  const raw = toJsonObject(row?.raw)
  return (
    normalizeText(
      row?.listing_id ??
        row?.listingId ??
        raw?.listing_id ??
        raw?.listingId ??
        raw?.skinport_listing_id ??
        raw?.skinportListingId
    ) || null
  )
}

function resolveExplicitListingAvailability(row = {}) {
  const raw = toJsonObject(row?.raw)
  return toTriStateBoolean(
    row?.listing_available ??
      row?.listingAvailable ??
      raw?.listing_available ??
      raw?.listingAvailable
  )
}

function buildRouteFreshnessContract(options = {}) {
  const buyMarket = normalizeSource(options.buyMarket)
  const sellMarket = normalizeSource(options.sellMarket)
  const buyRouteAvailable = toTriStateBoolean(options.buyRouteAvailable)
  const sellRouteAvailable = toTriStateBoolean(options.sellRouteAvailable)
  const buyListingAvailable = toTriStateBoolean(options.buyListingAvailable)
  const sellListingAvailable = toTriStateBoolean(options.sellListingAvailable)
  const inferredFields = Array.from(
    new Set(
      toArray(options.inferredFields)
        .map((value) => normalizeText(value))
        .filter(Boolean)
    )
  )

  return {
    buyMarket,
    sellMarket,
    buyRouteAvailable,
    sellRouteAvailable,
    buyRouteUpdatedAt: toIsoOrNull(options.buyRouteUpdatedAt),
    sellRouteUpdatedAt: toIsoOrNull(options.sellRouteUpdatedAt),
    buyListingAvailable,
    sellListingAvailable,
    buyListingId: normalizeText(options.buyListingId) || null,
    sellListingId: normalizeText(options.sellListingId) || null,
    requiredRouteState:
      normalizeText(options.requiredRouteState) ||
      resolveRequiredRouteState({
        buyMarket,
        sellMarket,
        buyRouteAvailable: buyRouteAvailable === true,
        sellRouteAvailable: sellRouteAvailable === true
      }),
    listingAvailabilityState:
      normalizeText(options.listingAvailabilityState) ||
      resolveListingAvailabilityState({
        buyMarket,
        sellMarket,
        buyListingAvailable,
        sellListingAvailable
      }),
    contractSource: normalizeText(options.contractSource) || null,
    contractVersion: normalizeText(options.contractVersion) || "v1",
    inferredFields
  }
}

function buildRouteFreshnessContractFromCompareResult(comparedItem = {}, base = {}) {
  const perMarket = toArray(comparedItem?.perMarket)
  const buyMarket = normalizeSource(
    base?.buyMarket ?? comparedItem?.arbitrage?.buyMarket ?? comparedItem?.bestBuy?.source
  )
  const sellMarket = normalizeSource(
    base?.sellMarket ?? comparedItem?.arbitrage?.sellMarket ?? comparedItem?.bestSellNet?.source
  )
  const buyRow = resolveRouteMarketRow(perMarket, buyMarket)
  const sellRow = resolveRouteMarketRow(perMarket, sellMarket)
  const buyRoutePrice = toPositiveOrNull(buyRow?.grossPrice)
  const sellRoutePrice = toPositiveOrNull(sellRow?.netPriceAfterFees)
  const buyListingId = resolveRouteListingId(buyRow)
  const sellListingId = resolveRouteListingId(sellRow)

  let buyListingAvailable = resolveExplicitListingAvailability(buyRow)
  let sellListingAvailable = resolveExplicitListingAvailability(sellRow)
  if (buyListingAvailable == null && buyMarket === "skinport") {
    buyListingAvailable = buyListingId ? true : null
  }
  if (sellListingAvailable == null && sellMarket === "skinport") {
    sellListingAvailable = sellListingId ? true : null
  }

  return buildRouteFreshnessContract({
    buyMarket,
    sellMarket,
    buyRouteAvailable: Boolean(buyRow?.available) && buyRoutePrice != null,
    sellRouteAvailable: Boolean(sellRow?.available) && sellRoutePrice != null,
    buyRouteUpdatedAt: resolveRouteUpdatedAt(buyRow),
    sellRouteUpdatedAt: resolveRouteUpdatedAt(sellRow),
    buyListingAvailable,
    sellListingAvailable,
    buyListingId,
    sellListingId,
    contractSource: "compare_result",
    contractVersion: "v1"
  })
}

function resolveOpportunityRouteFreshnessContract(opportunity = {}, options = {}) {
  const metadata = toJsonObject(opportunity?.metadata)
  const explicitContract = toJsonObject(
    opportunity?.routeFreshnessContract ??
      opportunity?.route_freshness_contract ??
      metadata?.route_freshness_contract ??
      metadata?.routeFreshnessContract
  )
  const allowLegacyInference = options?.allowLegacyInference !== false
  const inferredFields = new Set(
    toArray(explicitContract?.inferredFields).map((value) => normalizeText(value)).filter(Boolean)
  )
  const buyMarket = normalizeSource(
    explicitContract?.buyMarket ??
      explicitContract?.buy_market ??
      opportunity?.buyMarket ??
      opportunity?.buy_market
  )
  const sellMarket = normalizeSource(
    explicitContract?.sellMarket ??
      explicitContract?.sell_market ??
      opportunity?.sellMarket ??
      opportunity?.sell_market
  )

  let buyRouteAvailable = toTriStateBoolean(
    explicitContract?.buyRouteAvailable ??
      explicitContract?.buy_route_available ??
      opportunity?.buyRouteAvailable ??
      opportunity?.buy_route_available ??
      metadata?.buy_route_available ??
      metadata?.buyRouteAvailable
  )
  let sellRouteAvailable = toTriStateBoolean(
    explicitContract?.sellRouteAvailable ??
      explicitContract?.sell_route_available ??
      opportunity?.sellRouteAvailable ??
      opportunity?.sell_route_available ??
      metadata?.sell_route_available ??
      metadata?.sellRouteAvailable
  )

  if (buyRouteAvailable == null && allowLegacyInference) {
    buyRouteAvailable =
      Boolean(
        buyMarket &&
          toPositiveOrNull(
            opportunity?.buyPrice ??
              opportunity?.buy_price ??
              metadata?.buy_route_price ??
              metadata?.buyRoutePrice
          ) != null
      ) || false
    inferredFields.add("buyRouteAvailable")
  }
  if (sellRouteAvailable == null && allowLegacyInference) {
    sellRouteAvailable =
      Boolean(
        sellMarket &&
          toPositiveOrNull(
            opportunity?.sellNet ??
              opportunity?.sell_net ??
              metadata?.sell_route_price ??
              metadata?.sellRoutePrice
          ) != null
      ) || false
    inferredFields.add("sellRouteAvailable")
  }

  const buyListingId =
    normalizeText(
      explicitContract?.buyListingId ??
        explicitContract?.buy_listing_id ??
        metadata?.buy_listing_id ??
        metadata?.buyListingId ??
        (buyMarket === "skinport" ? metadata?.skinport_listing_id ?? metadata?.skinportListingId : null)
    ) || null
  const sellListingId =
    normalizeText(
      explicitContract?.sellListingId ??
        explicitContract?.sell_listing_id ??
        metadata?.sell_listing_id ??
        metadata?.sellListingId ??
        (sellMarket === "skinport"
          ? metadata?.skinport_listing_id ?? metadata?.skinportListingId
          : null)
    ) || null

  let buyListingAvailable = toTriStateBoolean(
    explicitContract?.buyListingAvailable ??
      explicitContract?.buy_listing_available ??
      opportunity?.buyListingAvailable ??
      opportunity?.buy_listing_available ??
      metadata?.buy_listing_available ??
      metadata?.buyListingAvailable
  )
  let sellListingAvailable = toTriStateBoolean(
    explicitContract?.sellListingAvailable ??
      explicitContract?.sell_listing_available ??
      opportunity?.sellListingAvailable ??
      opportunity?.sell_listing_available ??
      metadata?.sell_listing_available ??
      metadata?.sellListingAvailable
  )
  if (buyListingAvailable == null && buyMarket === "skinport" && buyListingId) {
    buyListingAvailable = true
  }
  if (sellListingAvailable == null && sellMarket === "skinport" && sellListingId) {
    sellListingAvailable = true
  }

  return buildRouteFreshnessContract({
    buyMarket,
    sellMarket,
    buyRouteAvailable,
    sellRouteAvailable,
    buyRouteUpdatedAt:
      explicitContract?.buyRouteUpdatedAt ??
      explicitContract?.buy_route_updated_at ??
      opportunity?.buyRouteUpdatedAt ??
      opportunity?.buy_route_updated_at ??
      metadata?.buy_route_updated_at ??
      metadata?.buyRouteUpdatedAt,
    sellRouteUpdatedAt:
      explicitContract?.sellRouteUpdatedAt ??
      explicitContract?.sell_route_updated_at ??
      opportunity?.sellRouteUpdatedAt ??
      opportunity?.sell_route_updated_at ??
      metadata?.sell_route_updated_at ??
      metadata?.sellRouteUpdatedAt,
    buyListingAvailable,
    sellListingAvailable,
    buyListingId,
    sellListingId,
    requiredRouteState:
      explicitContract?.requiredRouteState ??
      explicitContract?.required_route_state ??
      opportunity?.requiredRouteState ??
      opportunity?.required_route_state ??
      metadata?.required_route_state ??
      metadata?.requiredRouteState,
    listingAvailabilityState:
      explicitContract?.listingAvailabilityState ??
      explicitContract?.listing_availability_state ??
      opportunity?.listingAvailabilityState ??
      opportunity?.listing_availability_state ??
      metadata?.listing_availability_state ??
      metadata?.listingAvailabilityState,
    contractSource:
      explicitContract?.contractSource ??
      explicitContract?.contract_source ??
      (Object.keys(explicitContract).length ? "opportunity_contract" : "opportunity_fields"),
    contractVersion:
      explicitContract?.contractVersion ?? explicitContract?.contract_version ?? "v1",
    inferredFields: Array.from(inferredFields)
  })
}

function buildFreshnessContractDiagnostics(contract = {}, validation = {}) {
  const buyMarket = normalizeSource(contract?.buyMarket)
  const sellMarket = normalizeSource(contract?.sellMarket)
  const buyRouteAvailable = toTriStateBoolean(contract?.buyRouteAvailable)
  const sellRouteAvailable = toTriStateBoolean(contract?.sellRouteAvailable)
  const buyRouteUpdatedAt = toIsoOrNull(contract?.buyRouteUpdatedAt)
  const sellRouteUpdatedAt = toIsoOrNull(contract?.sellRouteUpdatedAt)
  const buyListingAvailable = toTriStateBoolean(contract?.buyListingAvailable)
  const sellListingAvailable = toTriStateBoolean(contract?.sellListingAvailable)
  const requiresBuyListing = buyMarket === "skinport"
  const requiresSellListing = sellMarket === "skinport"
  const missingBuyRouteTimestamp = Boolean(buyMarket) && buyRouteAvailable === true && !buyRouteUpdatedAt
  const missingSellRouteTimestamp =
    Boolean(sellMarket) && sellRouteAvailable === true && !sellRouteUpdatedAt
  const missingListingAvailability =
    (requiresBuyListing && buyListingAvailable == null) ||
    (requiresSellListing && sellListingAvailable == null)
  const staleReason = normalizeText(validation?.staleReason).toLowerCase()
  const inferredFields = Array.from(
    new Set(toArray(contract?.inferredFields).map((value) => normalizeText(value)).filter(Boolean))
  )
  const freshnessContractIncomplete =
    inferredFields.length > 0 ||
    missingBuyRouteTimestamp ||
    missingSellRouteTimestamp ||
    missingListingAvailability ||
    (Boolean(buyMarket) && buyRouteAvailable == null) ||
    (Boolean(sellMarket) && sellRouteAvailable == null)

  const primaryFailureBucket =
    staleReason ||
    (Boolean(buyMarket) && buyRouteAvailable === false
      ? "buy_route_unavailable"
      : Boolean(sellMarket) && sellRouteAvailable === false
        ? "sell_route_unavailable"
        : missingListingAvailability
          ? "missing_listing_availability"
          : freshnessContractIncomplete
            ? "freshness_contract_incomplete"
            : null)

  return {
    missing_buy_route_timestamp: missingBuyRouteTimestamp,
    missing_sell_route_timestamp: missingSellRouteTimestamp,
    buy_route_stale: staleReason === "buy_route_stale",
    sell_route_stale: staleReason === "sell_route_stale",
    buy_and_sell_route_stale: staleReason === "buy_and_sell_route_stale",
    buy_route_unavailable: Boolean(buyMarket) && buyRouteAvailable === false,
    sell_route_unavailable: Boolean(sellMarket) && sellRouteAvailable === false,
    missing_listing_availability: missingListingAvailability,
    freshness_contract_incomplete: freshnessContractIncomplete,
    primary_failure_bucket: primaryFailureBucket,
    contract_source: normalizeText(contract?.contractSource) || null,
    inferred_fields: inferredFields
  }
}

function evaluatePublishValidation(options = {}) {
  const nowMs = Number(options.nowMs || Date.now())
  const nowIso = toIsoOrNull(options.nowIso) || new Date(nowMs).toISOString()
  const maxSignalAgeMs = Math.max(
    Number(options.maxSignalAgeMs || DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS),
    1
  )

  const buyMarket = normalizeText(options.buyMarket).toLowerCase()
  const sellMarket = normalizeText(options.sellMarket).toLowerCase()
  const buyRouteAvailable = Boolean(options.buyRouteAvailable)
  const sellRouteAvailable = Boolean(options.sellRouteAvailable)
  const buyRouteUpdatedAt = toIsoOrNull(options.buyRouteUpdatedAt)
  const sellRouteUpdatedAt = toIsoOrNull(options.sellRouteUpdatedAt)
  const buyRouteAgeMs = computeAgeMs(buyRouteUpdatedAt, nowMs)
  const sellRouteAgeMs = computeAgeMs(sellRouteUpdatedAt, nowMs)
  const requiredRouteState = resolveRequiredRouteState({
    buyMarket,
    sellMarket,
    buyRouteAvailable,
    sellRouteAvailable
  })
  const listingAvailabilityState = resolveListingAvailabilityState({
    buyMarket,
    sellMarket,
    buyListingAvailable: options.buyListingAvailable,
    sellListingAvailable: options.sellListingAvailable
  })
  const listingBlocked = String(listingAvailabilityState).startsWith("missing_")
  const routeSignalObservedAt = resolveRouteSignalObservedAt(buyRouteUpdatedAt, sellRouteUpdatedAt)

  let signalAgeMs = null
  if (buyRouteAgeMs != null && sellRouteAgeMs != null) {
    signalAgeMs = Math.max(buyRouteAgeMs, sellRouteAgeMs)
  }

  let publishFreshnessState = "missing"
  let staleReason = null

  if (requiredRouteState !== "ready") {
    staleReason = requiredRouteState
  } else if (buyRouteAgeMs == null && sellRouteAgeMs == null) {
    staleReason = "missing_buy_and_sell_route_timestamp"
  } else if (buyRouteAgeMs == null) {
    staleReason = "missing_buy_route_timestamp"
  } else if (sellRouteAgeMs == null) {
    staleReason = "missing_sell_route_timestamp"
  } else if (signalAgeMs != null && signalAgeMs > maxSignalAgeMs) {
    const buyRouteStale = buyRouteAgeMs > maxSignalAgeMs
    const sellRouteStale = sellRouteAgeMs > maxSignalAgeMs
    publishFreshnessState = "stale"
    if (buyRouteStale && sellRouteStale) staleReason = "buy_and_sell_route_stale"
    else if (buyRouteStale) staleReason = "buy_route_stale"
    else if (sellRouteStale) staleReason = "sell_route_stale"
    else staleReason = "route_signal_stale"
  } else {
    publishFreshnessState = "fresh"
  }

  if (publishFreshnessState === "fresh" && listingBlocked) {
    staleReason = listingAvailabilityState
  }

  const isPublishable =
    requiredRouteState === "ready" &&
    publishFreshnessState === "fresh" &&
    !listingBlocked

  return {
    isPublishable,
    signalAgeMs,
    publishValidatedAt: nowIso,
    publishFreshnessState,
    requiredRouteState,
    listingAvailabilityState,
    staleReason: normalizeText(staleReason) || null,
    buyRouteUpdatedAt,
    sellRouteUpdatedAt,
    buyRouteAgeMs,
    sellRouteAgeMs,
    routeSignalObservedAt,
    maxSignalAgeMs
  }
}

function resolvePublishValidationContextForOpportunity(
  opportunity = {},
  nowMs = Date.now(),
  nowIso = null,
  options = {}
) {
  const routeFreshnessContract = resolveOpportunityRouteFreshnessContract(opportunity, options)
  const validation = evaluatePublishValidation({
    nowMs,
    nowIso,
    buyMarket: routeFreshnessContract.buyMarket,
    sellMarket: routeFreshnessContract.sellMarket,
    buyRouteAvailable: routeFreshnessContract.buyRouteAvailable === true,
    sellRouteAvailable: routeFreshnessContract.sellRouteAvailable === true,
    buyRouteUpdatedAt: routeFreshnessContract.buyRouteUpdatedAt,
    sellRouteUpdatedAt: routeFreshnessContract.sellRouteUpdatedAt,
    buyListingAvailable: routeFreshnessContract.buyListingAvailable,
    sellListingAvailable: routeFreshnessContract.sellListingAvailable
  })

  return {
    ...validation,
    routeFreshnessContract,
    freshnessContractDiagnostics: buildFreshnessContractDiagnostics(
      routeFreshnessContract,
      validation
    )
  }
}

function resolvePublishPreviewResult(validation = {}) {
  if (validation?.isPublishable) return "publishable"

  const requiredRouteState = normalizeText(validation?.requiredRouteState)
  if (requiredRouteState && requiredRouteState !== "ready") {
    return requiredRouteState
  }

  const staleReason = normalizeText(validation?.staleReason)
  if (staleReason) return staleReason

  const publishFreshnessState = normalizeText(validation?.publishFreshnessState).toLowerCase()
  if (publishFreshnessState && publishFreshnessState !== "fresh") {
    return `publish_${publishFreshnessState}`
  }

  return "blocked"
}

function buildPublishValidationPreview(validation = {}) {
  return {
    is_publishable: Boolean(validation?.isPublishable),
    publish_freshness_state: normalizeText(validation?.publishFreshnessState) || "missing",
    required_route_state: normalizeText(validation?.requiredRouteState) || null,
    listing_availability_state: normalizeText(validation?.listingAvailabilityState) || null,
    stale_reason: normalizeText(validation?.staleReason) || null,
    signal_age_ms: toFiniteOrNull(validation?.signalAgeMs),
    route_signal_observed_at: toIsoOrNull(validation?.routeSignalObservedAt),
    result_label: resolvePublishPreviewResult(validation)
  }
}

module.exports = {
  DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS,
  buildFreshnessContractDiagnostics,
  buildRouteFreshnessContract,
  buildRouteFreshnessContractFromCompareResult,
  evaluatePublishValidation,
  resolvePublishPreviewResult,
  buildPublishValidationPreview,
  resolveListingAvailabilityState,
  resolveOpportunityRouteFreshnessContract,
  resolvePublishValidationContextForOpportunity,
  resolveRequiredRouteState,
  resolveRouteSignalObservedAt
}

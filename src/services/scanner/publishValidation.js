const DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS = 2 * 60 * 60 * 1000

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

module.exports = {
  DEFAULT_PUBLISH_MAX_SIGNAL_AGE_MS,
  evaluatePublishValidation
}

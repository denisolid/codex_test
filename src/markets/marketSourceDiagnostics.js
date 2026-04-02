const SOURCE_STATES = Object.freeze({
  OK: "ok",
  NO_LISTING: "no_listing",
  NO_DATA: "no_data",
  AUTH_FAILED: "auth_failed",
  TIMEOUT: "timeout",
  UNAVAILABLE: "unavailable",
  PARSING_FAILED: "parsing_failed",
  STALE: "stale",
  DISABLED: "disabled"
});

const LEGACY_STATE_ALIASES = Object.freeze({
  source_unavailable: SOURCE_STATES.UNAVAILABLE,
  no_quote_data: SOURCE_STATES.NO_DATA
});

const SOURCE_STATE_SET = new Set(Object.values(SOURCE_STATES));

function toFiniteOrNull(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toIsoOrNull(value) {
  if (value == null || value === "") return null;
  if (value instanceof Date) {
    const ts = value.getTime();
    return Number.isFinite(ts) ? new Date(ts).toISOString() : null;
  }

  const numeric = toFiniteOrNull(value);
  if (numeric != null) {
    const normalizedTs =
      numeric >= 1e12
        ? Math.round(numeric)
        : numeric >= 1e9
          ? Math.round(numeric * 1000)
          : null;
    if (normalizedTs != null) {
      const ts = new Date(normalizedTs).getTime();
      if (Number.isFinite(ts)) {
        return new Date(ts).toISOString();
      }
    }
  }

  const raw = String(value || "").trim();
  if (!raw) return null;
  const ts = new Date(raw).getTime();
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toISOString();
}

function toNullableBoolean(value) {
  if (value === true) return true;
  if (value === false) return false;
  if (value == null || value === "") return null;
  const numeric = toFiniteOrNull(value);
  if (numeric != null) {
    if (numeric === 1) return true;
    if (numeric === 0) return false;
  }
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;
  if (["true", "yes", "on"].includes(raw)) return true;
  if (["false", "no", "off"].includes(raw)) return false;
  return null;
}

function normalizeSourceState(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  const normalized = LEGACY_STATE_ALIASES[raw] || raw;
  return SOURCE_STATE_SET.has(normalized) ? normalized : null;
}

function buildMarketHealthDiagnostics(input = {}) {
  const responseStatus = toFiniteOrNull(input.responseStatus);
  return {
    market_enabled:
      toNullableBoolean(input.marketEnabled) == null ? true : Boolean(input.marketEnabled),
    credentials_present: toNullableBoolean(input.credentialsPresent),
    auth_ok: toNullableBoolean(input.authOk),
    request_sent: toNullableBoolean(input.requestSent),
    response_status: responseStatus == null ? null : Math.round(responseStatus),
    response_parsed: toNullableBoolean(input.responseParsed),
    listings_found: toNullableBoolean(input.listingsFound),
    buy_price_present: toNullableBoolean(input.buyPricePresent),
    sell_price_present: toNullableBoolean(input.sellPricePresent),
    freshness_present: toNullableBoolean(input.freshnessPresent),
    listing_url_present: toNullableBoolean(input.listingUrlPresent),
    source_failure_reason: normalizeSourceState(input.sourceFailureReason),
    last_success_at: toIsoOrNull(input.lastSuccessAt),
    last_failure_at: toIsoOrNull(input.lastFailureAt)
  };
}

function hasMarketHealthDiagnostics(value = null) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  return [
    "market_enabled",
    "credentials_present",
    "auth_ok",
    "request_sent",
    "response_status",
    "response_parsed",
    "listings_found",
    "buy_price_present",
    "sell_price_present",
    "freshness_present",
    "listing_url_present",
    "source_failure_reason",
    "last_success_at",
    "last_failure_at"
  ].some((key) => Object.prototype.hasOwnProperty.call(value, key));
}

function attachMarketHealth(raw = null, diagnostics = {}) {
  const base = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
  return {
    ...base,
    market_health: buildMarketHealthDiagnostics(diagnostics)
  };
}

function readMarketHealth(raw = null) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const direct =
    raw.market_health && typeof raw.market_health === "object" ? raw.market_health : null;
  if (hasMarketHealthDiagnostics(direct)) {
    return buildMarketHealthDiagnostics({
      marketEnabled: direct.market_enabled,
      credentialsPresent: direct.credentials_present,
      authOk: direct.auth_ok,
      requestSent: direct.request_sent,
      responseStatus: direct.response_status,
      responseParsed: direct.response_parsed,
      listingsFound: direct.listings_found,
      buyPricePresent: direct.buy_price_present,
      sellPricePresent: direct.sell_price_present,
      freshnessPresent: direct.freshness_present,
      listingUrlPresent: direct.listing_url_present,
      sourceFailureReason: direct.source_failure_reason,
      lastSuccessAt: direct.last_success_at,
      lastFailureAt: direct.last_failure_at
    });
  }
  return null;
}

module.exports = {
  SOURCE_STATES,
  normalizeSourceState,
  buildMarketHealthDiagnostics,
  hasMarketHealthDiagnostics,
  attachMarketHealth,
  readMarketHealth,
  toIsoOrNull
};

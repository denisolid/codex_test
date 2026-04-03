const SCANNER_MARKET_POLICY = Object.freeze({
  steam: Object.freeze({
    source: "steam",
    mode: "degraded",
    primary: false,
    useFreshCacheOnRateLimit: true
  }),
  skinport: Object.freeze({
    source: "skinport",
    mode: "enabled",
    primary: true,
    useFreshCacheOnRateLimit: false
  }),
  csfloat: Object.freeze({
    source: "csfloat",
    mode: "enabled",
    primary: true,
    useFreshCacheOnRateLimit: false
  }),
  dmarket: Object.freeze({
    source: "dmarket",
    mode: "disabled",
    primary: false,
    useFreshCacheOnRateLimit: false
  })
})

const POLICY_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"])

function normalizeMarketSource(value) {
  return String(value || "").trim().toLowerCase()
}

function getScannerMarketPolicy(source = "") {
  const normalized = normalizeMarketSource(source)
  return SCANNER_MARKET_POLICY[normalized] || null
}

function listScannerMarketSourcesByMode(mode = "enabled") {
  return POLICY_ORDER.filter((source) => getScannerMarketPolicy(source)?.mode === mode)
}

function getScannerEnabledMarkets() {
  return listScannerMarketSourcesByMode("enabled")
}

function getScannerDegradedMarkets() {
  return listScannerMarketSourcesByMode("degraded")
}

function getScannerDisabledMarkets() {
  return listScannerMarketSourcesByMode("disabled")
}

function getScannerCoverageMarkets() {
  return POLICY_ORDER.filter((source) => {
    const policy = getScannerMarketPolicy(source)
    return policy && policy.mode !== "disabled"
  })
}

function isScannerMarketEnabled(source = "") {
  return getScannerMarketPolicy(source)?.mode === "enabled"
}

function isScannerMarketDegraded(source = "") {
  return getScannerMarketPolicy(source)?.mode === "degraded"
}

function isScannerMarketDisabled(source = "") {
  return getScannerMarketPolicy(source)?.mode === "disabled"
}

function shouldUseFreshCacheOnRateLimit(source = "") {
  return Boolean(getScannerMarketPolicy(source)?.useFreshCacheOnRateLimit)
}

function buildScannerMarketPolicyDiagnostics() {
  return {
    markets_enabled_for_scanner: getScannerEnabledMarkets(),
    markets_degraded_for_scanner: getScannerDegradedMarkets(),
    markets_disabled_for_scanner: getScannerDisabledMarkets()
  }
}

module.exports = {
  SCANNER_MARKET_POLICY,
  POLICY_ORDER,
  normalizeMarketSource,
  getScannerMarketPolicy,
  getScannerEnabledMarkets,
  getScannerDegradedMarkets,
  getScannerDisabledMarkets,
  getScannerCoverageMarkets,
  isScannerMarketEnabled,
  isScannerMarketDegraded,
  isScannerMarketDisabled,
  shouldUseFreshCacheOnRateLimit,
  buildScannerMarketPolicyDiagnostics
}

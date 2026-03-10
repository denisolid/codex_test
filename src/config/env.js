require("dotenv").config();

const required = [
  "SUPABASE_URL",
  "SUPABASE_ANON_KEY",
  "SUPABASE_SERVICE_ROLE_KEY"
];

required.forEach((k) => {
  if (!process.env[k]) {
    throw new Error(`Missing env var: ${k}`);
  }
});

function parseCsv(value) {
  return String(value || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

function normalizeOrigin(value) {
  const trimmed = String(value || "").trim().replace(/\/+$/, "");
  if (!trimmed) {
    return "";
  }

  try {
    return new URL(trimmed).origin;
  } catch (_err) {
    return trimmed;
  }
}

function normalizeEnum(value, allowed, fallback) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (allowed.includes(normalized)) {
    return normalized;
  }
  return fallback;
}

const frontendOrigins = Array.from(
  new Set([
    ...parseCsv(process.env.FRONTEND_URL),
    ...parseCsv(process.env.FRONTEND_ORIGINS),
    ...parseCsv(process.env.FRONTEND_ORIGIN)
  ].map(normalizeOrigin))
).filter(Boolean);
if (!frontendOrigins.length) {
  frontendOrigins.push("http://localhost:5173");
}

function joinUrl(base, path) {
  const cleanBase = String(base || "").replace(/\/+$/, "");
  const cleanPath = String(path || "").startsWith("/") ? path : `/${path}`;
  return `${cleanBase}${cleanPath}`;
}

const authEmailRedirectTo =
  process.env.AUTH_EMAIL_REDIRECT_TO ||
  joinUrl(frontendOrigins[0], "/login.html?confirmed=1");

const apiPublicUrl =
  String(process.env.API_PUBLIC_URL || process.env.BACKEND_URL || "").trim() || "";

const appAuthSecret = String(
  process.env.APP_AUTH_SECRET || process.env.SUPABASE_SERVICE_ROLE_KEY || ""
).trim();
if (!appAuthSecret) {
  throw new Error("Missing env var: APP_AUTH_SECRET");
}

module.exports = {
  nodeEnv: process.env.NODE_ENV || "development",
  port: Number(process.env.PORT || 4000),
  frontendOrigin: frontendOrigins[0],
  frontendOrigins,
  apiPublicUrl,
  authEmailRedirectTo,
  appAuthSecret,
  emailProvider: normalizeEnum(process.env.EMAIL_PROVIDER, ["console", "resend"], "console"),
  resendApiKey: String(process.env.RESEND_API_KEY || "").trim(),
  emailFrom: String(process.env.EMAIL_FROM || "").trim(),
  emailReplyTo: String(process.env.EMAIL_REPLY_TO || "").trim(),
  emailVerificationTtlMinutes: Number(process.env.EMAIL_VERIFICATION_TTL_MINUTES || 30),
  steamWebApiKey: String(process.env.STEAM_WEB_API_KEY || "").trim(),
  adminApiToken: process.env.ADMIN_API_TOKEN || "",
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseAnonKey: process.env.SUPABASE_ANON_KEY,
  supabaseServiceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  steamInventorySource: normalizeEnum(
    process.env.STEAM_INVENTORY_SOURCE,
    ["auto", "real", "mock"],
    "auto"
  ),
  steamInventoryTimeoutMs: Number(process.env.STEAM_INVENTORY_TIMEOUT_MS || 12000),
  steamInventoryMaxRetries: Number(process.env.STEAM_INVENTORY_MAX_RETRIES || 3),
  steamInventoryRetryBaseMs: Number(
    process.env.STEAM_INVENTORY_RETRY_BASE_MS || 1200
  ),
  marketPriceSource: normalizeEnum(
    process.env.MARKET_PRICE_SOURCE,
    ["auto", "steam", "mock"],
    "auto"
  ),
  marketPriceFallbackToMock:
    process.env.MARKET_PRICE_FALLBACK_TO_MOCK == null
      ? process.env.MARKET_PRICE_SOURCE !== "steam"
      : process.env.MARKET_PRICE_FALLBACK_TO_MOCK === "true",
  marketPriceRateLimitPerSecond: Number(
    process.env.MARKET_PRICE_RATE_LIMIT_PER_SECOND || 2
  ),
  skinEnrichmentMaxAgeDays: Number(process.env.SKIN_ENRICHMENT_MAX_AGE_DAYS || 14),
  defaultSkinImageUrl:
    process.env.DEFAULT_SKIN_IMAGE_URL ||
    "https://community.akamai.steamstatic.com/public/images/apps/730/header.jpg",
  marketPriceStaleHours: Number(process.env.MARKET_PRICE_STALE_HOURS || 24),
  marketPriceCacheTtlMinutes: Number(process.env.MARKET_PRICE_CACHE_TTL_MINUTES || 60),
  inventorySyncPriceConcurrency: Number(
    process.env.INVENTORY_SYNC_PRICE_CONCURRENCY || 8
  ),
  steamMarketCurrency: Number(process.env.STEAM_MARKET_CURRENCY || 1),
  steamMarketTimeoutMs: Number(process.env.STEAM_MARKET_TIMEOUT_MS || 10000),
  steamMarketMaxRetries: Number(process.env.STEAM_MARKET_MAX_RETRIES || 3),
  steamMarketRetryBaseMs: Number(process.env.STEAM_MARKET_RETRY_BASE_MS || 350),
  marketCommissionPercent: Number(process.env.MARKET_COMMISSION_PERCENT || 13),
  marketSnapshotTtlMinutes: Number(process.env.MARKET_SNAPSHOT_TTL_MINUTES || 30),
  defaultDisplayCurrency: process.env.DEFAULT_DISPLAY_CURRENCY || "USD",
  fxRatesUsdJson: process.env.FX_RATES_USD_JSON || "",
  fxRatesSource:
    process.env.FX_RATES_SOURCE || (process.env.NODE_ENV === "production" ? "live" : "static"),
  fxRatesApiUrl:
    process.env.FX_RATES_API_URL || "https://open.er-api.com/v6/latest/USD",
  fxRatesRefreshMinutes: Number(process.env.FX_RATES_REFRESH_MINUTES || 30),
  fxRatesRequestTimeoutMs: Number(process.env.FX_RATES_REQUEST_TIMEOUT_MS || 2500),
  fxRatesFailureCooldownSeconds: Number(
    process.env.FX_RATES_FAILURE_COOLDOWN_SECONDS || 120
  ),
  steamMarketPriceStrategy:
    process.env.STEAM_MARKET_PRICE_STRATEGY || "balanced",
  marketCompareCacheTtlMinutes: Number(
    process.env.MARKET_COMPARE_CACHE_TTL_MINUTES || 60
  ),
  marketCompareConcurrency: Number(process.env.MARKET_COMPARE_CONCURRENCY || 4),
  marketCompareTimeoutMs: Number(process.env.MARKET_COMPARE_TIMEOUT_MS || 9000),
  marketCompareMaxRetries: Number(process.env.MARKET_COMPARE_MAX_RETRIES || 3),
  marketCompareRetryBaseMs: Number(process.env.MARKET_COMPARE_RETRY_BASE_MS || 350),
  marketFeeSteamPercent: Number(process.env.MARKET_FEE_STEAM_PERCENT || 13),
  marketFeeSkinportPercent: Number(process.env.MARKET_FEE_SKINPORT_PERCENT || 12),
  marketFeeCsfloatPercent: Number(process.env.MARKET_FEE_CSFLOAT_PERCENT || 2),
  marketFeeDmarketPercent: Number(process.env.MARKET_FEE_DMARKET_PERCENT || 7),
  skinportApiUrl: process.env.SKINPORT_API_URL || "https://api.skinport.com/v1",
  skinportApiKey: String(process.env.SKINPORT_API_KEY || "").trim(),
  csfloatApiUrl: process.env.CSFLOAT_API_URL || "https://csfloat.com/api/v1",
  csfloatApiKey: String(process.env.CSFLOAT_API_KEY || "").trim(),
  dmarketApiUrl: process.env.DMARKET_API_URL || "https://api.dmarket.com/exchange/v1",
  dmarketPublicKey: String(process.env.DMARKET_PUBLIC_KEY || "").trim(),
  dmarketSecretKey: String(process.env.DMARKET_SECRET_KEY || "").trim(),
  authRateLimitWindowMs: Number(process.env.AUTH_RATE_LIMIT_WINDOW_MS || 60000),
  authRateLimitMax: Number(process.env.AUTH_RATE_LIMIT_MAX || 20),
  syncRateLimitWindowMs: Number(process.env.SYNC_RATE_LIMIT_WINDOW_MS || 60000),
  syncRateLimitMax: Number(process.env.SYNC_RATE_LIMIT_MAX || 6),
  priceUpdaterIntervalMinutes: Number(process.env.PRICE_UPDATER_INTERVAL_MINUTES || 60),
  priceUpdaterRateLimitPerSecond: Number(
    process.env.PRICE_UPDATER_RATE_LIMIT_PER_SECOND || 5
  ),
  arbitrageScannerIntervalMinutes: Number(
    process.env.ARBITRAGE_SCANNER_INTERVAL_MINUTES || 5
  ),
  arbitrageDefaultUniverseLimit: Number(
    process.env.ARBITRAGE_DEFAULT_UNIVERSE_LIMIT || 500
  ),
  arbitrageScannerUniverseTargetSize: Number(
    process.env.ARBITRAGE_SCANNER_UNIVERSE_TARGET_SIZE ||
      process.env.ARBITRAGE_DEFAULT_UNIVERSE_LIMIT ||
      500
  ),
  arbitrageScanBatchSize: Number(
    process.env.ARBITRAGE_SCAN_BATCH_SIZE ||
      process.env.ARBITRAGE_QUOTE_REFRESH_BATCH_SIZE ||
      40
  ),
  arbitrageMaxConcurrentMarketRequests: Number(
    process.env.ARBITRAGE_MAX_CONCURRENT_MARKET_REQUESTS ||
      process.env.MARKET_COMPARE_CONCURRENCY ||
      4
  ),
  arbitrageScanTimeoutPerBatchMs: Number(
    process.env.ARBITRAGE_SCAN_TIMEOUT_PER_BATCH_MS || 30000
  ),
  arbitrageImageEnrichBatchSize: Number(
    process.env.ARBITRAGE_IMAGE_ENRICH_BATCH_SIZE || 30
  ),
  arbitrageImageEnrichConcurrency: Number(
    process.env.ARBITRAGE_IMAGE_ENRICH_CONCURRENCY || 2
  ),
  arbitrageImageEnrichTimeoutMs: Number(
    process.env.ARBITRAGE_IMAGE_ENRICH_TIMEOUT_MS || 9000
  ),
  arbitrageQuoteRefreshBatchSize: Number(
    process.env.ARBITRAGE_QUOTE_REFRESH_BATCH_SIZE || 40
  ),
  arbitrageQuoteComputeBatchSize: Number(
    process.env.ARBITRAGE_QUOTE_COMPUTE_BATCH_SIZE || 80
  ),
  arbitrageUniverseDbLimit: Number(
    process.env.ARBITRAGE_UNIVERSE_DB_LIMIT || 800
  ),
  arbitrageSourceCatalogLimit: Number(
    process.env.ARBITRAGE_SOURCE_CATALOG_LIMIT || 1000
  ),
  arbitrageSourceCatalogRefreshMinutes: Number(
    process.env.ARBITRAGE_SOURCE_CATALOG_REFRESH_MINUTES || 60
  ),
  arbitrageFeedRetentionHours: Number(process.env.ARBITRAGE_FEED_RETENTION_HOURS || 24),
  arbitrageFeedActiveLimit: Number(process.env.ARBITRAGE_FEED_ACTIVE_LIMIT || 500),
  arbitrageDuplicateWindowHours: Number(process.env.ARBITRAGE_DUPLICATE_WINDOW_HOURS || 4),
  arbitrageMinProfitChangePct: Number(process.env.ARBITRAGE_MIN_PROFIT_CHANGE_PCT || 10),
  arbitrageMinScoreChange: Number(process.env.ARBITRAGE_MIN_SCORE_CHANGE || 8),
  arbitrageInsertDuplicates:
    process.env.ARBITRAGE_INSERT_DUPLICATES == null
      ? false
      : process.env.ARBITRAGE_INSERT_DUPLICATES === "true",
  alertCheckIntervalMinutes: Number(process.env.ALERT_CHECK_INTERVAL_MINUTES || 5),
  alertCheckBatchSize: Number(process.env.ALERT_CHECK_BATCH_SIZE || 250),
  traderModePriceUsd: Number(process.env.TRADER_MODE_PRICE_USD || 29),
  traderModeMockCheckoutEnabled:
    process.env.TRADER_MODE_MOCK_CHECKOUT_ENABLED == null
      ? true
      : process.env.TRADER_MODE_MOCK_CHECKOUT_ENABLED === "true",
  testSubscriptionSwitcherEnabled:
    process.env.TEST_SUBSCRIPTION_SWITCHER == null
      ? false
      : process.env.TEST_SUBSCRIPTION_SWITCHER === "true"
};

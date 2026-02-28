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

const frontendOrigins = Array.from(
  new Set([
    ...parseCsv(process.env.FRONTEND_URL),
    ...parseCsv(process.env.FRONTEND_ORIGINS),
    ...parseCsv(process.env.FRONTEND_ORIGIN)
  ])
);
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
  steamWebApiKey: String(process.env.STEAM_WEB_API_KEY || "").trim(),
  adminApiToken: process.env.ADMIN_API_TOKEN || "",
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseAnonKey: process.env.SUPABASE_ANON_KEY,
  supabaseServiceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  steamInventorySource: process.env.STEAM_INVENTORY_SOURCE || "auto",
  steamInventoryTimeoutMs: Number(process.env.STEAM_INVENTORY_TIMEOUT_MS || 12000),
  marketPriceSource: process.env.MARKET_PRICE_SOURCE || "auto",
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
  authRateLimitWindowMs: Number(process.env.AUTH_RATE_LIMIT_WINDOW_MS || 60000),
  authRateLimitMax: Number(process.env.AUTH_RATE_LIMIT_MAX || 20),
  syncRateLimitWindowMs: Number(process.env.SYNC_RATE_LIMIT_WINDOW_MS || 60000),
  syncRateLimitMax: Number(process.env.SYNC_RATE_LIMIT_MAX || 6),
  priceUpdaterIntervalMinutes: Number(process.env.PRICE_UPDATER_INTERVAL_MINUTES || 60),
  priceUpdaterRateLimitPerSecond: Number(
    process.env.PRICE_UPDATER_RATE_LIMIT_PER_SECOND || 5
  ),
  alertCheckIntervalMinutes: Number(process.env.ALERT_CHECK_INTERVAL_MINUTES || 5),
  alertCheckBatchSize: Number(process.env.ALERT_CHECK_BATCH_SIZE || 250),
  traderModePriceUsd: Number(process.env.TRADER_MODE_PRICE_USD || 29),
  traderModeMockCheckoutEnabled:
    process.env.TRADER_MODE_MOCK_CHECKOUT_ENABLED == null
      ? true
      : process.env.TRADER_MODE_MOCK_CHECKOUT_ENABLED === "true"
};

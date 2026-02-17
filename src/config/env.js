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

module.exports = {
  nodeEnv: process.env.NODE_ENV || "development",
  port: Number(process.env.PORT || 4000),
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
  marketPriceCacheTtlMinutes: Number(process.env.MARKET_PRICE_CACHE_TTL_MINUTES || 60),
  steamMarketCurrency: Number(process.env.STEAM_MARKET_CURRENCY || 1),
  steamMarketTimeoutMs: Number(process.env.STEAM_MARKET_TIMEOUT_MS || 10000),
  priceUpdaterIntervalMinutes: Number(process.env.PRICE_UPDATER_INTERVAL_MINUTES || 60),
  priceUpdaterRateLimitPerSecond: Number(
    process.env.PRICE_UPDATER_RATE_LIMIT_PER_SECOND || 5
  )
};

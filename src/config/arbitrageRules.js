function toNumber(input, fallback, options = {}) {
  const parsed = Number(input);
  const base = Number.isFinite(parsed) ? parsed : fallback;
  const min = Number.isFinite(Number(options.min)) ? Number(options.min) : -Infinity;
  const max = Number.isFinite(Number(options.max)) ? Number(options.max) : Infinity;
  return Math.min(Math.max(base, min), max);
}

module.exports = Object.freeze({
  MIN_EXECUTION_PRICE_USD: toNumber(process.env.ARBITRAGE_MIN_EXECUTION_PRICE_USD, 3, {
    min: 0.01
  }),
  MIN_SPREAD_PERCENT_BASELINE: toNumber(process.env.ARBITRAGE_MIN_SPREAD_PERCENT, 5, {
    min: 0
  }),
  SPREAD_SUSPICIOUS_PENALTY_THRESHOLD: toNumber(
    process.env.ARBITRAGE_SPREAD_SUSPICIOUS_PENALTY_THRESHOLD,
    120,
    { min: 20 }
  ),
  SPREAD_SANITY_MAX_PERCENT: toNumber(process.env.ARBITRAGE_SPREAD_SANITY_MAX_PERCENT, 300, {
    min: 10
  }),
  REFERENCE_DEVIATION_RATIO_MAX: toNumber(
    process.env.ARBITRAGE_REFERENCE_DEVIATION_RATIO_MAX,
    3,
    { min: 1.5 }
  ),
  MIN_MARKET_COVERAGE: toNumber(process.env.ARBITRAGE_MIN_MARKET_COVERAGE, 2, {
    min: 1,
    max: 4
  }),
  LIQUIDITY_VOLUME_PASS: toNumber(process.env.ARBITRAGE_LIQUIDITY_VOLUME_PASS, 100, {
    min: 1
  }),
  LIQUIDITY_VOLUME_HIGH: toNumber(process.env.ARBITRAGE_LIQUIDITY_VOLUME_HIGH, 200, {
    min: 1
  }),
  LIQUIDITY_VOLUME_MEDIUM: toNumber(process.env.ARBITRAGE_LIQUIDITY_VOLUME_MEDIUM, 50, {
    min: 1
  }),
  LIQUIDITY_SCORE_PASS: toNumber(process.env.ARBITRAGE_LIQUIDITY_SCORE_PASS, 40, {
    min: 0,
    max: 100
  }),
  LIQUIDITY_SCORE_MEDIUM: toNumber(process.env.ARBITRAGE_LIQUIDITY_SCORE_MEDIUM, 30, {
    min: 0,
    max: 100
  }),
  ORDERBOOK_OUTLIER_RATIO: toNumber(process.env.ARBITRAGE_ORDERBOOK_OUTLIER_RATIO, 3, {
    min: 1
  }),
  ORDERBOOK_OUTLIER_PRICE_MAX: toNumber(process.env.ARBITRAGE_ORDERBOOK_OUTLIER_PRICE_MAX, 1, {
    min: 0.01
  }),
  DEFAULT_SCORE_CUTOFF: toNumber(process.env.ARBITRAGE_DEFAULT_SCORE_CUTOFF, 75, {
    min: 0,
    max: 100
  }),
  RISKY_SCORE_CUTOFF: 40,
  DEFAULT_MIN_PROFIT_ABSOLUTE: toNumber(process.env.ARBITRAGE_DEFAULT_MIN_PROFIT_ABSOLUTE, 0.5, {
    min: 0
  }),
  DEFAULT_MIN_PROFIT_BUY_PERCENT: toNumber(
    process.env.ARBITRAGE_DEFAULT_MIN_PROFIT_BUY_PERCENT,
    2,
    { min: 0 }
  ),
  UNKNOWN_LIQUIDITY_SCORE_BASE: toNumber(process.env.ARBITRAGE_UNKNOWN_LIQUIDITY_SCORE_BASE, 50, {
    min: 0,
    max: 100
  }),
  UNKNOWN_LIQUIDITY_SCORE_PENALTY: toNumber(
    process.env.ARBITRAGE_UNKNOWN_LIQUIDITY_SCORE_PENALTY,
    15,
    { min: 0, max: 100 }
  )
});

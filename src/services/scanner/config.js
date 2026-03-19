const {
  arbitrageScannerIntervalMinutes,
  arbitrageEnrichmentIntervalMinutes,
  arbitrageOpportunityScanIntervalMinutes,
  arbitrageDefaultUniverseLimit,
  arbitrageOpportunityBatchSize,
  arbitrageEnrichmentBatchSize,
  arbitrageScanBatchSize,
  arbitrageUniverseDbLimit,
  arbitrageFeedRetentionHours,
  arbitrageFeedActiveLimit,
  arbitrageDuplicateWindowHours,
  arbitrageEnrichmentJobTimeoutMs,
  arbitrageOpportunityJobTimeoutMs,
  arbitrageAllowCrossJobParallelism,
  arbitrageRecordSkippedAlreadyRunning,
  arbitrageScanTimeoutPerBatchMs,
  arbitrageInsertDuplicates,
  arbitrageMinProfitChangePct,
  arbitrageMinScoreChange,
  arbitrageMinSpreadChangePct,
  arbitrageMinLiquidityChangePct,
  arbitrageMinConfidenceChangeLevels
} = require("../../config/env")

function toNumber(value, fallback, options = {}) {
  const parsed = Number(value)
  const base = Number.isFinite(parsed) ? parsed : Number(fallback || 0)
  const min = Number.isFinite(Number(options.min)) ? Number(options.min) : -Infinity
  const max = Number.isFinite(Number(options.max)) ? Number(options.max) : Infinity
  return Math.min(Math.max(base, min), max)
}

const SCANNER_TYPES = Object.freeze({
  ENRICHMENT: "enrichment",
  OPPORTUNITY_SCAN: "opportunity_scan"
})

const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove",
  FUTURE_KNIFE: "future_knife",
  FUTURE_GLOVE: "future_glove"
})

const SUPPORTED_SCAN_CATEGORIES = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE,
  ITEM_CATEGORIES.KNIFE,
  ITEM_CATEGORIES.GLOVE,
  ITEM_CATEGORIES.FUTURE_KNIFE,
  ITEM_CATEGORIES.FUTURE_GLOVE
])

const ROUND_ROBIN_CATEGORY_ORDER = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE,
  ITEM_CATEGORIES.KNIFE,
  ITEM_CATEGORIES.GLOVE
])

const CATEGORY_PROFILES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    minPriceUsd: 3,
    minProfitUsd: 0.75,
    minSpreadPercent: 3.5,
    minVolume7d: 35,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 320,
    referenceRejectRatio: 2.7,
    strongScoreFloor: 76,
    riskyScoreFloor: 50,
    speculativeScoreFloor: 28
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    minPriceUsd: 1,
    minProfitUsd: 0.3,
    minSpreadPercent: 2.5,
    minVolume7d: 18,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 320,
    referenceRejectRatio: 2.7,
    strongScoreFloor: 74,
    riskyScoreFloor: 47,
    speculativeScoreFloor: 24
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    minPriceUsd: 1,
    minProfitUsd: 0.35,
    minSpreadPercent: 2.5,
    minVolume7d: 18,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 320,
    referenceRejectRatio: 2.7,
    strongScoreFloor: 74,
    riskyScoreFloor: 47,
    speculativeScoreFloor: 24
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    minPriceUsd: 20,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    minVolume7d: 6,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 260,
    referenceRejectRatio: 2.45,
    strongScoreFloor: 70,
    riskyScoreFloor: 44,
    speculativeScoreFloor: 22
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    minPriceUsd: 20,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    minVolume7d: 6,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 260,
    referenceRejectRatio: 2.45,
    strongScoreFloor: 70,
    riskyScoreFloor: 44,
    speculativeScoreFloor: 22
  }),
  [ITEM_CATEGORIES.FUTURE_KNIFE]: Object.freeze({
    minPriceUsd: 20,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    minVolume7d: 4,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 260,
    referenceRejectRatio: 2.45,
    strongScoreFloor: 68,
    riskyScoreFloor: 42,
    speculativeScoreFloor: 22
  }),
  [ITEM_CATEGORIES.FUTURE_GLOVE]: Object.freeze({
    minPriceUsd: 20,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    minVolume7d: 4,
    minMarketCoverage: 2,
    hardSpreadMaxPercent: 260,
    referenceRejectRatio: 2.45,
    strongScoreFloor: 68,
    riskyScoreFloor: 42,
    speculativeScoreFloor: 22
  })
})

const PENALTY_WEIGHTS = Object.freeze({
  missing_liquidity: 14,
  aging_data: 8,
  weak_depth: 9,
  low_value_flag: 7,
  weak_coverage: 10,
  low_price: 8,
  low_profit: 7,
  low_spread: 6,
  reference_deviation_warning: 12
})

const SCAN_STATE = Object.freeze({
  SCANABLE: "scanable",
  SCANABLE_WITH_PENALTIES: "scanable_with_penalties",
  HARD_REJECT: "hard_reject"
})

const OPPORTUNITY_TIERS = Object.freeze({
  STRONG: "strong",
  RISKY: "risky",
  SPECULATIVE: "speculative",
  REJECTED: "rejected"
})

const DEFAULT_UNIVERSE_LIMIT = Math.max(
  Math.round(toNumber(arbitrageDefaultUniverseLimit, 3000, { min: 200 })),
  200
)
const UNIVERSE_DB_LIMIT = Math.max(
  Math.round(toNumber(arbitrageUniverseDbLimit, DEFAULT_UNIVERSE_LIMIT * 2, { min: 500 })),
  DEFAULT_UNIVERSE_LIMIT
)
const OPPORTUNITY_BATCH_TARGET = Math.max(
  Math.round(toNumber(arbitrageOpportunityBatchSize, 150, { min: 20 })),
  20
)
const ENRICHMENT_BATCH_TARGET = Math.max(
  Math.round(toNumber(arbitrageEnrichmentBatchSize, 120, { min: 20 })),
  20
)
const SCAN_CHUNK_SIZE = Math.max(
  Math.round(toNumber(arbitrageScanBatchSize, 40, { min: 10, max: 250 })),
  10
)

const ENRICHMENT_INTERVAL_MINUTES = Math.max(
  Math.round(toNumber(arbitrageEnrichmentIntervalMinutes, arbitrageScannerIntervalMinutes || 10, { min: 1 })),
  1
)
const OPPORTUNITY_SCAN_INTERVAL_MINUTES = Math.max(
  Math.round(
    toNumber(arbitrageOpportunityScanIntervalMinutes, arbitrageScannerIntervalMinutes || 5, {
      min: 1
    })
  ),
  1
)
const ENRICHMENT_INTERVAL_MS = ENRICHMENT_INTERVAL_MINUTES * 60 * 1000
const OPPORTUNITY_SCAN_INTERVAL_MS = OPPORTUNITY_SCAN_INTERVAL_MINUTES * 60 * 1000

const ENRICHMENT_JOB_TIMEOUT_MS = Math.max(
  Math.round(toNumber(arbitrageEnrichmentJobTimeoutMs, 420000, { min: 60000 })),
  60000
)
const OPPORTUNITY_JOB_TIMEOUT_MS = Math.max(
  Math.round(toNumber(arbitrageOpportunityJobTimeoutMs, 420000, { min: 60000 })),
  60000
)
const SCAN_TIMEOUT_PER_BATCH_MS = Math.max(
  Math.round(toNumber(arbitrageScanTimeoutPerBatchMs, 30000, { min: 1000 })),
  1000
)

const FEED_RETENTION_HOURS = Math.max(
  Math.round(toNumber(arbitrageFeedRetentionHours, 24, { min: 1 })),
  1
)
const FEED_ACTIVE_LIMIT = Math.max(
  Math.round(toNumber(arbitrageFeedActiveLimit, 500, { min: 50 })),
  50
)
const DUPLICATE_WINDOW_HOURS = Math.max(
  Math.round(toNumber(arbitrageDuplicateWindowHours, 4, { min: 1 })),
  1
)
const INSERT_DUPLICATES = Boolean(arbitrageInsertDuplicates)

const ALLOW_CROSS_JOB_PARALLELISM = arbitrageAllowCrossJobParallelism !== false
const RECORD_SKIPPED_ALREADY_RUNNING = Boolean(arbitrageRecordSkippedAlreadyRunning)

const MIN_PROFIT_CHANGE_PCT = Math.max(
  toNumber(arbitrageMinProfitChangePct, 10, { min: 0 }),
  0
)
const MIN_SCORE_CHANGE = Math.max(toNumber(arbitrageMinScoreChange, 8, { min: 0 }), 0)
const MIN_SPREAD_CHANGE_PCT = Math.max(
  toNumber(arbitrageMinSpreadChangePct, 3, { min: 0 }),
  0
)
const MIN_LIQUIDITY_CHANGE_PCT = Math.max(
  toNumber(arbitrageMinLiquidityChangePct, 20, { min: 0 }),
  0
)
const MIN_CONFIDENCE_CHANGE_LEVELS = Math.max(
  Math.round(toNumber(arbitrageMinConfidenceChangeLevels, 1, { min: 0 })),
  0
)

module.exports = Object.freeze({
  SCANNER_TYPES,
  ITEM_CATEGORIES,
  SUPPORTED_SCAN_CATEGORIES,
  ROUND_ROBIN_CATEGORY_ORDER,
  CATEGORY_PROFILES,
  PENALTY_WEIGHTS,
  SCAN_STATE,
  OPPORTUNITY_TIERS,
  DEFAULT_UNIVERSE_LIMIT,
  UNIVERSE_DB_LIMIT,
  OPPORTUNITY_BATCH_TARGET,
  ENRICHMENT_BATCH_TARGET,
  SCAN_CHUNK_SIZE,
  ENRICHMENT_INTERVAL_MINUTES,
  OPPORTUNITY_SCAN_INTERVAL_MINUTES,
  ENRICHMENT_INTERVAL_MS,
  OPPORTUNITY_SCAN_INTERVAL_MS,
  ENRICHMENT_JOB_TIMEOUT_MS,
  OPPORTUNITY_JOB_TIMEOUT_MS,
  SCAN_TIMEOUT_PER_BATCH_MS,
  FEED_RETENTION_HOURS,
  FEED_ACTIVE_LIMIT,
  DUPLICATE_WINDOW_HOURS,
  INSERT_DUPLICATES,
  ALLOW_CROSS_JOB_PARALLELISM,
  RECORD_SKIPPED_ALREADY_RUNNING,
  MIN_PROFIT_CHANGE_PCT,
  MIN_SCORE_CHANGE,
  MIN_SPREAD_CHANGE_PCT,
  MIN_LIQUIDITY_CHANGE_PCT,
  MIN_CONFIDENCE_CHANGE_LEVELS
})

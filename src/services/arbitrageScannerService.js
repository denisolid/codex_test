const { round2, roundPrice } = require("../markets/marketUtils")
const {
  arbitrageScannerIntervalMinutes,
  arbitrageEnrichmentIntervalMinutes,
  arbitrageOpportunityScanIntervalMinutes,
  arbitrageDefaultUniverseLimit,
  arbitrageScannerUniverseTargetSize,
  arbitrageScanBatchSize,
  arbitrageEnrichmentBatchSize,
  arbitrageOpportunityBatchSize,
  arbitrageMaxConcurrentMarketRequests,
  arbitrageEnrichmentJobTimeoutMs,
  arbitrageOpportunityJobTimeoutMs,
  arbitrageAllowCrossJobParallelism,
  arbitrageRecordSkippedAlreadyRunning,
  arbitrageScanTimeoutPerBatchMs,
  arbitrageImageEnrichBatchSize,
  arbitrageImageEnrichConcurrency,
  arbitrageImageEnrichTimeoutMs,
  arbitrageQuoteRefreshBatchSize,
  arbitrageQuoteComputeBatchSize,
  arbitrageUniverseDbLimit,
  marketSnapshotTtlMinutes,
  arbitrageFeedRetentionHours,
  arbitrageFeedActiveLimit,
  arbitrageDuplicateWindowHours,
  arbitrageHotOpportunityScanTarget,
  arbitrageHotMaturePoolLimit,
  arbitrageMinProfitChangePct,
  arbitrageMinScoreChange,
  arbitrageMinSpreadChangePct,
  arbitrageMinLiquidityChangePct,
  arbitrageMinConfidenceChangeLevels,
  arbitrageSignalHistoryLookbackHours,
  arbitrageInsertDuplicates
} = require("../config/env")
const marketUniverseTop100 = require("../config/marketUniverseTop100.json")
const skinRepo = require("../repositories/skinRepository")
const marketSnapshotRepo = require("../repositories/marketSnapshotRepository")
const marketUniverseRepo = require("../repositories/marketUniverseRepository")
const marketSourceCatalogRepo = require("../repositories/marketSourceCatalogRepository")
const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const arbitrageFeedRepo = require("../repositories/arbitrageFeedRepository")
const scannerRunRepo = require("../repositories/scannerRunRepository")
const marketComparisonService = require("./marketComparisonService")
const arbitrageEngine = require("./arbitrageEngineService")
const marketService = require("./marketService")
const marketSourceCatalogService = require("./marketSourceCatalogService")
const marketImageService = require("./marketImageService")
const planService = require("./planService")
const premiumCategoryAccessService = require("./premiumCategoryAccessService")
const AppError = require("../utils/AppError")

const SCANNER_TYPES = Object.freeze({
  ENRICHMENT: "enrichment",
  OPPORTUNITY_SCAN: "opportunity_scan"
})
const LEGACY_SCANNER_TYPE = "global_arbitrage"
const ENRICHMENT_INTERVAL_MINUTES = Math.max(
  Number(arbitrageEnrichmentIntervalMinutes || arbitrageScannerIntervalMinutes || 10),
  1
)
const OPPORTUNITY_SCAN_INTERVAL_MINUTES = Math.max(
  Number(arbitrageOpportunityScanIntervalMinutes || arbitrageScannerIntervalMinutes || 10),
  1
)
const SCANNER_INTERVAL_MINUTES = OPPORTUNITY_SCAN_INTERVAL_MINUTES
const ENRICHMENT_INTERVAL_MS = ENRICHMENT_INTERVAL_MINUTES * 60 * 1000
const OPPORTUNITY_SCAN_INTERVAL_MS = OPPORTUNITY_SCAN_INTERVAL_MINUTES * 60 * 1000
const SCANNER_INTERVAL_MS = OPPORTUNITY_SCAN_INTERVAL_MS
const CACHE_TTL_MS = OPPORTUNITY_SCAN_INTERVAL_MS
const SCANNER_OVERDUE_GRACE_MS = Math.max(
  Math.round(OPPORTUNITY_SCAN_INTERVAL_MS * 0.2),
  15 * 1000
)
const ENRICHMENT_OVERDUE_GRACE_MS = Math.max(
  Math.round(ENRICHMENT_INTERVAL_MS * 0.2),
  15 * 1000
)
const ENRICHMENT_JOB_TIMEOUT_MS = Math.max(Number(arbitrageEnrichmentJobTimeoutMs || 420000), 60000)
const OPPORTUNITY_JOB_TIMEOUT_MS = Math.max(
  Number(arbitrageOpportunityJobTimeoutMs || 420000),
  60000
)
const ALLOW_CROSS_JOB_PARALLELISM = arbitrageAllowCrossJobParallelism !== false
const RECORD_SKIPPED_ALREADY_RUNNING = Boolean(arbitrageRecordSkippedAlreadyRunning)
const STALE_RUN_RECONCILE_COOLDOWN_MS = 60 * 1000
const HIGH_CONFIDENCE_MIN_PRICE_USD = 5
const HIGH_CONFIDENCE_MIN_SPREAD_PERCENT = 5
const HIGH_CONFIDENCE_MAX_SPREAD_PERCENT = 120
const HIGH_CONFIDENCE_MIN_VOLUME_7D = 100
const HIGH_CONFIDENCE_MIN_SCORE = 75
const RISKY_MIN_PRICE_USD = 3
const RISKY_MIN_SPREAD_PERCENT = 3
const RISKY_MAX_SPREAD_PERCENT = 250
const RISKY_MIN_VOLUME_7D = 20
const RISKY_MIN_SCORE = 40
const PREMIUM_MIN_PRICE_USD = 20
const PREMIUM_MIN_SPREAD_PERCENT = 3
const PREMIUM_SPREAD_HEAVY_PENALTY_PERCENT = 150
const PREMIUM_MAX_SPREAD_PERCENT = 250
const PREMIUM_MIN_VOLUME_REJECT = 5
const PREMIUM_MIN_VOLUME_MEDIUM = 10
const PREMIUM_MIN_VOLUME_HIGH = 20
const PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE = 3
const PREMIUM_REFERENCE_PENALTY_RATIO = 1.8
const PREMIUM_REFERENCE_REJECT_RATIO = 2.5
const MIN_SPREAD_PERCENT = HIGH_CONFIDENCE_MIN_SPREAD_PERCENT
const MAX_SPREAD_PERCENT = RISKY_MAX_SPREAD_PERCENT
const MIN_VOLUME_7D = HIGH_CONFIDENCE_MIN_VOLUME_7D
const MIN_EXECUTION_PRICE_USD = HIGH_CONFIDENCE_MIN_PRICE_USD
const MIN_MARKET_COVERAGE = 2
const DEFAULT_SCORE_CUTOFF = HIGH_CONFIDENCE_MIN_SCORE
const RISKY_SCORE_CUTOFF = RISKY_MIN_SCORE
const FEED_RISKY_MIN_SCORE = 38
const MAX_API_LIMIT = 200
const DEFAULT_API_LIMIT = 100
const MAX_FEED_LIMIT = 500
const FEED_METADATA_ENRICH_LIMIT = 60
const FEED_RETENTION_HOURS = Math.max(Number(arbitrageFeedRetentionHours || 24), 1)
const FEED_ACTIVE_LIMIT = Math.max(Number(arbitrageFeedActiveLimit || 500), 50)
const DUPLICATE_WINDOW_HOURS = Math.max(Number(arbitrageDuplicateWindowHours || 4), 1)
const HOT_MATURE_POOL_LIMIT = Math.max(Number(arbitrageHotMaturePoolLimit || 400), 120)
const MIN_PROFIT_CHANGE_PCT = Math.max(Number(arbitrageMinProfitChangePct || 10), 0)
const MIN_SCORE_CHANGE = Math.max(Number(arbitrageMinScoreChange || 8), 0)
const MIN_SPREAD_CHANGE_PCT = Math.max(Number(arbitrageMinSpreadChangePct || 3), 0)
const MIN_LIQUIDITY_CHANGE_PCT = Math.max(Number(arbitrageMinLiquidityChangePct || 20), 0)
const MIN_CONFIDENCE_CHANGE_LEVELS = Math.max(
  Math.round(Number(arbitrageMinConfidenceChangeLevels || 1)),
  0
)
const SIGNAL_HISTORY_LOOKBACK_HOURS = Math.max(
  Number(arbitrageSignalHistoryLookbackHours || 72),
  1
)
const INSERT_DUPLICATES = Boolean(arbitrageInsertDuplicates)
const MANUAL_REFRESH_TRACKER_MAX = 4000
const manualRefreshTracker = new Map()

const FRESHNESS_STATES = Object.freeze({
  FRESH: "fresh",
  AGING: "aging",
  STALE: "stale"
})

const CATEGORY_STALE_RULES = Object.freeze({
  weapon_skin: Object.freeze({
    freshMaxMinutes: 45,
    agingMaxMinutes: 90,
    agingPenalty: 8,
    stalePenalty: 20
  }),
  case: Object.freeze({
    freshMaxMinutes: 60,
    agingMaxMinutes: 120,
    agingPenalty: 6,
    stalePenalty: 16
  }),
  sticker_capsule: Object.freeze({
    freshMaxMinutes: 90,
    agingMaxMinutes: 180,
    agingPenalty: 5,
    stalePenalty: 14
  }),
  knife: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240,
    agingPenalty: 4,
    stalePenalty: 12
  }),
  glove: Object.freeze({
    freshMaxMinutes: 120,
    agingMaxMinutes: 240,
    agingPenalty: 4,
    stalePenalty: 12
  })
})

const SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"])
const ITEM_CATEGORIES = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove"
})
const CATALOG_CANDIDATE_STATUS = Object.freeze({
  CANDIDATE: "candidate",
  ENRICHING: "enriching",
  NEAR_ELIGIBLE: "near_eligible",
  ELIGIBLE: "eligible",
  REJECTED: "rejected"
})
const MATURITY_STATES = Object.freeze({
  COLD: "cold",
  ENRICHING: "enriching",
  NEAR_ELIGIBLE: "near_eligible",
  ELIGIBLE: "eligible"
})
const SOURCE_CATALOG_SNAPSHOT_STATES = Object.freeze({
  MISSING: "missing_snapshot",
  STALE: "stale_snapshot",
  PARTIAL: "partial_snapshot",
  READY: "snapshot_ready"
})
const SOURCE_CATALOG_REFERENCE_STATES = Object.freeze({
  MISSING: "missing_reference",
  SNAPSHOT: "snapshot_reference",
  QUOTE: "quote_reference"
})
const SOURCE_CATALOG_LIQUIDITY_STATES = Object.freeze({
  MISSING: "missing_liquidity",
  PARTIAL: "partial_liquidity",
  READY: "liquidity_ready"
})
const SOURCE_CATALOG_COVERAGE_STATES = Object.freeze({
  MISSING: "missing_coverage",
  INSUFFICIENT: "insufficient_coverage",
  READY: "coverage_ready"
})
const SOURCE_CATALOG_PROGRESSION_STATUS = Object.freeze({
  ELIGIBLE: "eligible",
  BLOCKED_ELIGIBLE: "blocked_from_eligible",
  BLOCKED_NEAR_ELIGIBLE: "blocked_from_near_eligible",
  REJECTED: "rejected"
})
const SCAN_LAYERS = Object.freeze({
  HOT: "hot",
  WARM: "warm",
  COLD: "cold"
})
const SCANNER_AUDIT_CATEGORIES = Object.freeze([
  ITEM_CATEGORIES.WEAPON_SKIN,
  ITEM_CATEGORIES.CASE,
  ITEM_CATEGORIES.STICKER_CAPSULE
])
const PERFORMANCE_STAGE_KEYS = Object.freeze([
  "sourceCatalogPreparationMs",
  "inputHydrationMs",
  "quoteFetchingMs",
  "normalizationMs",
  "opportunityComputationMs",
  "dbWritesMs",
  "diagnosticsAggregationMs"
])
const FALLBACK_UNIVERSE = Object.freeze(normalizeUniverseEntries(marketUniverseTop100))
const DEFAULT_UNIVERSE_LIMIT_BASELINE = 3000
const DEFAULT_UNIVERSE_LIMIT = Math.max(
  Number(arbitrageDefaultUniverseLimit || DEFAULT_UNIVERSE_LIMIT_BASELINE),
  100
)
const UNIVERSE_TARGET_SIZE = Math.max(
  Number(arbitrageScannerUniverseTargetSize || DEFAULT_UNIVERSE_LIMIT),
  20
)
const PRE_COMPARE_UNIVERSE_LIMIT = Math.max(
  UNIVERSE_TARGET_SIZE * 2,
  DEFAULT_UNIVERSE_LIMIT
)
const HIGH_YIELD_CORE_TARGET = Math.max(
  Math.min(
    Number(process.env.ARBITRAGE_CORE_UNIVERSE_TARGET || Math.round(UNIVERSE_TARGET_SIZE * 0.08)),
    300
  ),
  100
)
const HOT_LAYER_SCAN_TARGET = Math.max(
  Math.min(
    Number(process.env.ARBITRAGE_HOT_LAYER_SCAN_TARGET || Math.round(UNIVERSE_TARGET_SIZE * 0.25)),
    PRE_COMPARE_UNIVERSE_LIMIT
  ),
  400
)
const WARM_LAYER_SCAN_TARGET = Math.max(
  Math.min(
    Number(process.env.ARBITRAGE_WARM_LAYER_SCAN_TARGET || Math.round(UNIVERSE_TARGET_SIZE * 0.1)),
    PRE_COMPARE_UNIVERSE_LIMIT
  ),
  120
)
const COLD_LAYER_SCAN_TARGET = Math.max(
  Math.min(
    Number(process.env.ARBITRAGE_COLD_LAYER_SCAN_TARGET || Math.round(UNIVERSE_TARGET_SIZE * 0.03)),
    PRE_COMPARE_UNIVERSE_LIMIT
  ),
  40
)
const OPPORTUNITY_SCAN_TARGET = Math.max(
  Math.min(
    Number(
      process.env.ARBITRAGE_OPPORTUNITY_SCAN_TARGET ||
        HOT_LAYER_SCAN_TARGET + WARM_LAYER_SCAN_TARGET + COLD_LAYER_SCAN_TARGET
    ),
    PRE_COMPARE_UNIVERSE_LIMIT
  ),
  600
)
const ENRICHMENT_ONLY_TARGET = Math.max(
  Math.min(
    Number(process.env.ARBITRAGE_ENRICHMENT_ONLY_TARGET || Math.round(UNIVERSE_TARGET_SIZE * 0.12)),
    PRE_COMPARE_UNIVERSE_LIMIT
  ),
  120
)
const ENRICHMENT_BATCH_SIZE = Math.max(
  Math.min(
    Number(arbitrageEnrichmentBatchSize || ENRICHMENT_ONLY_TARGET),
    Math.min(PRE_COMPARE_UNIVERSE_LIMIT, 200)
  ),
  50
)
const OPPORTUNITY_BATCH_SIZE = Math.max(
  Math.min(
    Number(arbitrageOpportunityBatchSize || OPPORTUNITY_SCAN_TARGET),
    Math.min(PRE_COMPARE_UNIVERSE_LIMIT, 500)
  ),
  100
)
const HOT_OPPORTUNITY_SCAN_TARGET = Math.max(
  Math.min(
    Number(arbitrageHotOpportunityScanTarget || 50),
    OPPORTUNITY_BATCH_SIZE,
    PRE_COMPARE_UNIVERSE_LIMIT
  ),
  20
)
const OPPORTUNITY_NEAR_ELIGIBLE_LIMIT = Math.max(
  Math.min(
    Number(
      process.env.ARBITRAGE_OPPORTUNITY_NEAR_ELIGIBLE_LIMIT ||
        Math.max(Math.round(HOT_OPPORTUNITY_SCAN_TARGET * 0.3), 12)
    ),
    HOT_OPPORTUNITY_SCAN_TARGET
  ),
  0
)
const UNIVERSE_DB_LIMIT = Math.max(
  Number(arbitrageUniverseDbLimit || DEFAULT_UNIVERSE_LIMIT * 2),
  PRE_COMPARE_UNIVERSE_LIMIT
)
const SCAN_BATCH_SIZE = Math.max(
  Math.min(
    Number(
      arbitrageScanBatchSize ||
        arbitrageQuoteRefreshBatchSize ||
        arbitrageQuoteComputeBatchSize ||
        40
    ),
    100
  ),
  10
)
const MAX_CONCURRENT_MARKET_REQUESTS = Math.max(
  Number(arbitrageMaxConcurrentMarketRequests || 6),
  1
)
const SCAN_TIMEOUT_PER_BATCH = Math.max(
  Number(arbitrageScanTimeoutPerBatchMs || 30000),
  1000
)
const IMAGE_ENRICH_BATCH_SIZE = Math.max(
  Math.min(Number(arbitrageImageEnrichBatchSize || 30), 120),
  0
)
const IMAGE_ENRICH_CONCURRENCY = Math.max(
  Math.min(Number(arbitrageImageEnrichConcurrency || 2), 6),
  1
)
const IMAGE_ENRICH_TIMEOUT_MS = Math.max(
  Number(arbitrageImageEnrichTimeoutMs || 9000),
  1000
)
const SNAPSHOT_WARMUP_MAX_ITEMS = Math.max(Math.min(SCAN_BATCH_SIZE, 30), 10)
const SNAPSHOT_WARMUP_CONCURRENCY = Math.max(
  Math.min(MAX_CONCURRENT_MARKET_REQUESTS, 3),
  1
)
const SNAPSHOT_WARMUP_TRIGGER_FRESH_MIN = Math.max(SCAN_BATCH_SIZE, 20)
const MIN_STRICT_SEED_COVERAGE = Math.max(Math.round(SCAN_BATCH_SIZE / 2), 10)
const STRICT_SEED_COVERAGE_RATIO_TARGET = 0.6
const UNIVERSE_MIN_PRICE_FLOOR_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 3,
  [ITEM_CATEGORIES.CASE]: 1,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 1,
  [ITEM_CATEGORIES.KNIFE]: 20,
  [ITEM_CATEGORIES.GLOVE]: 20
})
const UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: 35,
  [ITEM_CATEGORIES.CASE]: 20,
  [ITEM_CATEGORIES.STICKER_CAPSULE]: 20,
  [ITEM_CATEGORIES.KNIFE]: PREMIUM_MIN_VOLUME_REJECT,
  [ITEM_CATEGORIES.GLOVE]: PREMIUM_MIN_VOLUME_REJECT
})
const LOW_VALUE_NAME_PATTERNS = Object.freeze([
  /^sticker\s*\|/i,
  /^graffiti\s*\|/i,
  /^sealed graffiti\s*\|/i,
  /\|\s*(Sand Spray|Sand Dune|Grey Smoke|Coolant|Mudder|Gator Mesh|Orange Peel|Mandrel|Facility Draft|Facility Sketch|Short Ochre|Blue Spruce|Predator)\b/i
])
const LIQUID_NAME_SIGNAL_PATTERNS = Object.freeze([
  /\basiimov\b/i,
  /\bprintstream\b/i,
  /\bfade\b/i,
  /\bdoppler\b/i,
  /\bgamma\b/i,
  /\bvulcan\b/i,
  /\bredline\b/i,
  /\bneo-noir\b/i,
  /\bbloodsport\b/i,
  /\bcase hardened\b/i,
  /\btiger tooth\b/i,
  /\bslaughter\b/i,
  /\bmarble fade\b/i,
  /\bkill confirmed\b/i,
  /\btemukau\b/i,
  /\bthe emperor\b/i,
  /\bthe empress\b/i,
  /\bcyrex\b/i,
  /\bfrontside misty\b/i,
  /\bhyper beast\b/i
])
const HIGH_SIGNAL_WEAPON_PREFIXES = Object.freeze(
  new Set([
    "AK-47",
    "AWP",
    "M4A1-S",
    "M4A4",
    "USP-S",
    "Glock-18",
    "Desert Eagle",
    "FAMAS",
    "Galil AR",
    "MP9",
    "MAC-10"
  ])
)
const WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_PRICE_USD = 4
const WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_PROFIT_USD = 1
const WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_SPREAD_PERCENT = 5
const WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_VOLUME_7D = 60
const WEAPON_SKIN_FALLBACK_MIN_REFERENCE_PRICE_USD = 4
const WEAPON_SKIN_FALLBACK_MIN_PROFIT_USD = 0.9
const WEAPON_SKIN_FALLBACK_MIN_SPREAD_PERCENT = 4
const WEAPON_SKIN_FALLBACK_MAX_SPREAD_PERCENT = 140
const WEAPON_SKIN_STALE_SNAPSHOT_HARD_REJECT_TTL_MULTIPLIER = 24
const WEAPON_SKIN_MISSING_LIQUIDITY_PENALTY = 14
const WEAPON_SKIN_LOW_VALUE_PENALTY = 9
const WEAPON_SKIN_STATTRAK_VARIANT_PENALTY = 4
const WEAPON_SKIN_SOUVENIR_VARIANT_PENALTY = 4
const KNOWN_BAD_IMAGE_HOSTS = Object.freeze(
  new Set(["example.com", "www.example.com"])
)
const DIAGNOSTIC_REASON_KEYS = Object.freeze([
  "ignored_execution_floor",
  "ignored_low_value_universe",
  "hard_reject_low_value",
  "ignored_low_liquidity",
  "risky_low_price",
  "risky_low_profit",
  "risky_low_score",
  "risky_low_confidence",
  "risky_missing_depth",
  "spread_below_min",
  "non_positive_profit",
  "ignored_extreme_spread",
  "ignored_reference_deviation",
  "ignored_missing_markets",
  "ignored_missing_liquidity_data",
  "hard_reject_missing_liquidity",
  "ignored_missing_depth",
  "ignored_low_score",
  "ignored_stale_data"
])
const DIAGNOSTIC_REASON_ALIAS = Object.freeze({
  ignored_low_price: "ignored_execution_floor",
  insufficient_market_data: "ignored_missing_markets"
})
const WEAPON_SKIN_FILTER_DIAGNOSTIC_KEYS = Object.freeze([
  "hard_reject_missing_liquidity",
  "penalty_missing_liquidity_allowed_forward",
  "hard_reject_stale",
  "stale_penalty_allowed_forward",
  "aging_penalty_allowed_forward",
  "hard_reject_low_value",
  "penalty_low_value_allowed_forward",
  "stattrak_penalty",
  "souvenir_penalty",
  "stale_forwarded_to_risky",
  "stale_forwarded_to_speculative",
  "survived_into_risky",
  "survived_into_speculative"
])
const WEAPON_SKIN_FILTER_DIAGNOSTIC_ALIAS = Object.freeze({
  penalty_missing_liquidity_but_allowed: "penalty_missing_liquidity_allowed_forward",
  penalty_low_value_but_allowed: "penalty_low_value_allowed_forward",
  variant_penalty_stattrak: "stattrak_penalty",
  variant_penalty_souvenir: "souvenir_penalty",
  risky_pass_count: "survived_into_risky",
  speculative_pass_count: "survived_into_speculative"
})

const HARD_REJECTION_REASONS = Object.freeze(
  new Set([
    "insufficient_market_data",
    "non_positive_profit",
    "ignored_reference_deviation",
    "ignored_extreme_spread",
    "ignored_missing_markets"
  ])
)

const STRICT_SCAN_PROFILE = Object.freeze({
  name: "strict",
  minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
  minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
  minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
  allowMissingLiquidity: false,
  requireFreshData: true,
  maxQuoteAgeMinutes: 60,
  minScore: HIGH_CONFIDENCE_MIN_SCORE
})

const RISKY_SCAN_PROFILE = Object.freeze({
  name: "risky",
  minPriceUsd: RISKY_MIN_PRICE_USD,
  minSpreadPercent: RISKY_MIN_SPREAD_PERCENT,
  minVolume7d: RISKY_MIN_VOLUME_7D,
  allowMissingLiquidity: true,
  requireFreshData: false,
  maxQuoteAgeMinutes: Infinity,
  minScore: RISKY_MIN_SCORE
})

const CATEGORY_SCAN_RULES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
      minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
      maxSpreadPercent: HIGH_CONFIDENCE_MAX_SPREAD_PERCENT,
      minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 3,
      minProfitUsd: 0.7,
      minSpreadPercent: 3.75,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: 35,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 38
    })
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
      minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
      maxSpreadPercent: HIGH_CONFIDENCE_MAX_SPREAD_PERCENT,
      minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 1,
      minProfitUsd: 0.35,
      minSpreadPercent: 2.75,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: 18,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 39
    })
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: HIGH_CONFIDENCE_MIN_PRICE_USD,
      minSpreadPercent: HIGH_CONFIDENCE_MIN_SPREAD_PERCENT,
      maxSpreadPercent: HIGH_CONFIDENCE_MAX_SPREAD_PERCENT,
      minVolume7d: HIGH_CONFIDENCE_MIN_VOLUME_7D,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 1,
      minProfitUsd: 0.45,
      minSpreadPercent: 2.75,
      maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
      minVolume7d: 18,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: 39
    })
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: PREMIUM_MIN_PRICE_USD,
      minSpreadPercent: PREMIUM_MIN_SPREAD_PERCENT,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_MEDIUM,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 40,
      minProfitUsd: 3,
      minSpreadPercent: 2.5,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: RISKY_MIN_SCORE,
      allowMissingDepthWithPenalty: true
    })
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    strict: Object.freeze({
      minPriceUsd: PREMIUM_MIN_PRICE_USD,
      minSpreadPercent: PREMIUM_MIN_SPREAD_PERCENT,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_MEDIUM,
      minMarketCoverage: MIN_MARKET_COVERAGE
    }),
    risky: Object.freeze({
      minPriceUsd: 40,
      minProfitUsd: 3,
      minSpreadPercent: 2.5,
      maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
      minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
      minMarketCoverage: MIN_MARKET_COVERAGE,
      minScore: RISKY_MIN_SCORE,
      allowMissingDepthWithPenalty: true
    })
  })
})

const PREMIUM_ITEM_CATEGORIES = Object.freeze(
  new Set([ITEM_CATEGORIES.KNIFE, ITEM_CATEGORIES.GLOVE])
)

const CATEGORY_RISKY_MODE_PROFILES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    name: "risky_weapon_skin",
    minPriceUsd: 3,
    minProfitUsd: 0.7,
    minSpreadPercent: 3.75,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 35,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 38,
    allowMissingLiquidity: false,
    allowMissingDepthWithPenalty: false,
    allowBorderlinePromotion: true,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    name: "risky_case",
    minPriceUsd: 1,
    minProfitUsd: 0.35,
    minSpreadPercent: 2.75,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 18,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 39,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: false,
    allowBorderlinePromotion: true,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    name: "risky_sticker_capsule",
    minPriceUsd: 1,
    minProfitUsd: 0.45,
    minSpreadPercent: 2.75,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 18,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: 39,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: false,
    allowBorderlinePromotion: true,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    name: "risky_knife",
    minPriceUsd: 40,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
    minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: true,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    name: "risky_glove",
    minPriceUsd: 40,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
    minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: true,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  })
})

const RISKY_QUALITY_FLOOR_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({ minScore: 38, minProfitUsd: 0.7 }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({ minScore: 39, minProfitUsd: 0.35 }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({ minScore: 39, minProfitUsd: 0.45 }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 3 }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 3 })
})

const LEGACY_CATEGORY_RISKY_MODE_PROFILES = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    name: "legacy_weapon_skin",
    minPriceUsd: 3,
    minProfitUsd: 0.75,
    minSpreadPercent: 4,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 40,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: false,
    allowMissingDepthWithPenalty: false,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    name: "legacy_case",
    minPriceUsd: 1,
    minProfitUsd: 0.4,
    minSpreadPercent: 3,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 20,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: false,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    name: "legacy_sticker_capsule",
    minPriceUsd: 1,
    minProfitUsd: 0.5,
    minSpreadPercent: 3,
    maxSpreadPercent: RISKY_MAX_SPREAD_PERCENT,
    minVolume7d: 20,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: false,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({
    name: "legacy_knife",
    minPriceUsd: 40,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
    minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: true,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({
    name: "legacy_glove",
    minPriceUsd: 40,
    minProfitUsd: 3,
    minSpreadPercent: 2.5,
    maxSpreadPercent: PREMIUM_MAX_SPREAD_PERCENT,
    minVolume7d: PREMIUM_MIN_VOLUME_REJECT,
    minMarketCoverage: MIN_MARKET_COVERAGE,
    minScore: RISKY_MIN_SCORE,
    allowMissingLiquidity: true,
    allowMissingDepthWithPenalty: true,
    allowBorderlinePromotion: false,
    requireFreshData: false,
    maxQuoteAgeMinutes: Infinity
  })
})

const LEGACY_RISKY_QUALITY_FLOOR_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 0.75 }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 0.4 }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 0.5 }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 3 }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({ minScore: RISKY_MIN_SCORE, minProfitUsd: 3 })
})

const BORDERLINE_RISKY_PROMOTION_BY_CATEGORY = Object.freeze({
  [ITEM_CATEGORIES.WEAPON_SKIN]: Object.freeze({
    enabled: true,
    maxProfitShortfallUsd: 0.12,
    maxSpreadShortfallPercent: 0.35,
    minVolumeRatio: 0.82,
    minProfitBufferUsd: 0.2,
    minSpreadBufferPercent: 0.75
  }),
  [ITEM_CATEGORIES.CASE]: Object.freeze({
    enabled: true,
    maxProfitShortfallUsd: 0.06,
    maxSpreadShortfallPercent: 0.2,
    minVolumeRatio: 0.85,
    minProfitBufferUsd: 0.12,
    minSpreadBufferPercent: 0.5
  }),
  [ITEM_CATEGORIES.STICKER_CAPSULE]: Object.freeze({
    enabled: true,
    maxProfitShortfallUsd: 0.08,
    maxSpreadShortfallPercent: 0.2,
    minVolumeRatio: 0.85,
    minProfitBufferUsd: 0.14,
    minSpreadBufferPercent: 0.5
  }),
  [ITEM_CATEGORIES.KNIFE]: Object.freeze({ enabled: false }),
  [ITEM_CATEGORIES.GLOVE]: Object.freeze({ enabled: false })
})

function normalizeMarketHashName(value) {
  return String(value || "").trim()
}

function sanitizeImageUrl(value) {
  const raw = String(value || "").trim()
  if (!raw) return null
  try {
    const parsed = new URL(raw)
    if (!["http:", "https:"].includes(parsed.protocol)) return null
    if (KNOWN_BAD_IMAGE_HOSTS.has(String(parsed.hostname || "").trim().toLowerCase())) {
      return null
    }
    return parsed.toString()
  } catch (_err) {
    return null
  }
}

function normalizeItemCategory(value, marketHashName = "") {
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  if (raw === ITEM_CATEGORIES.KNIFE || raw === "knives") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === ITEM_CATEGORIES.GLOVE || raw === "gloves") {
    return ITEM_CATEGORIES.GLOVE
  }
  if (
    raw === ITEM_CATEGORIES.STICKER_CAPSULE ||
    raw === "sticker capsule" ||
    raw === "capsule" ||
    raw === "sticker_capsules"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (raw === ITEM_CATEGORIES.CASE) return ITEM_CATEGORIES.CASE
  if (raw === ITEM_CATEGORIES.WEAPON_SKIN) return ITEM_CATEGORIES.WEAPON_SKIN
  if (/sticker capsule$/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  if (/case$/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.CASE
  }
  if (/\b(gloves|glove|hand wraps)\b/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.GLOVE
  }
  if (/\b(knife|bayonet|karambit|daggers)\b/i.test(String(marketHashName || "").trim())) {
    return ITEM_CATEGORIES.KNIFE
  }
  return ITEM_CATEGORIES.WEAPON_SKIN
}

function normalizeUniverseEntries(items = []) {
  const seen = new Set()
  const normalized = []
  for (const rawEntry of Array.isArray(items) ? items : []) {
    const entry =
      rawEntry && typeof rawEntry === "object"
        ? rawEntry
        : { marketHashName: rawEntry, category: ITEM_CATEGORIES.WEAPON_SKIN, scan_enabled: true }
    const marketHashName = normalizeMarketHashName(
      entry?.marketHashName || entry?.market_hash_name || entry?.name
    )
    if (!marketHashName) continue
    const key = marketHashName.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    const category = normalizeItemCategory(entry?.category, marketHashName)
    const subcategory = String(entry?.subcategory || "").trim() || null
    const scanEnabled = entry?.scan_enabled == null ? true : Boolean(entry.scan_enabled)
    if (!scanEnabled) continue
    normalized.push({
      marketHashName,
      category,
      subcategory,
      scan_enabled: true
    })
  }
  return normalized
}

function toFiniteOrNull(value) {
  if (value == null) return null
  if (typeof value === "string" && !value.trim()) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  return parsed > 0 ? parsed : null
}

function toIsoStringOrNull(value) {
  const text = String(value || "").trim()
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function normalizeTextValue(value) {
  return String(value || "").trim()
}

function normalizeLowerText(value) {
  return normalizeTextValue(value).toLowerCase()
}

function normalizeTextList(values = []) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => normalizeLowerText(value))
        .filter(Boolean)
    )
  )
}

function clampScore(value) {
  const parsed = toFiniteOrNull(value) ?? 0
  return round2(Math.min(Math.max(parsed, 0), 100))
}

function toTitle(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\b\w/g, (token) => token.toUpperCase())
}

function normalizeBoolean(value) {
  if (typeof value === "boolean") return value
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on"
}

function normalizeCategoryFilter(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase()
  if (!raw || raw === "all") return "all"
  if (raw === "skin" || raw === "skins" || raw === "weapon_skin") {
    return ITEM_CATEGORIES.WEAPON_SKIN
  }
  if (raw === "case" || raw === "cases") {
    return ITEM_CATEGORIES.CASE
  }
  if (raw === "knife" || raw === "knives") {
    return ITEM_CATEGORIES.KNIFE
  }
  if (raw === "glove" || raw === "gloves") {
    return ITEM_CATEGORIES.GLOVE
  }
  if (
    raw === "capsule" ||
    raw === "capsules" ||
    raw === "sticker_capsule" ||
    raw === "sticker_capsules"
  ) {
    return ITEM_CATEGORIES.STICKER_CAPSULE
  }
  return "all"
}

function getCategoryStaleRules(itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalized = normalizeItemCategory(itemCategory)
  return CATEGORY_STALE_RULES[normalized] || CATEGORY_STALE_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
}

function resolveFreshnessState(ageMinutes = null, rules = {}) {
  const age = toFiniteOrNull(ageMinutes)
  if (age == null || age < 0) return FRESHNESS_STATES.STALE
  if (age <= Number(rules.freshMaxMinutes || 0)) return FRESHNESS_STATES.FRESH
  if (age <= Number(rules.agingMaxMinutes || 0)) return FRESHNESS_STATES.AGING
  return FRESHNESS_STATES.STALE
}

function computeLiquidityScoreFromSnapshot(snapshot = {}) {
  const volume24h = Math.max(toFiniteOrNull(snapshot?.volume_24h) ?? 0, 0)
  const volatility7dPercent = Math.max(toFiniteOrNull(snapshot?.volatility_7d_percent) ?? 0, 0)
  const spreadPercent = Math.max(toFiniteOrNull(snapshot?.spread_percent) ?? 0, 0)

  const volumeScore = Math.min(Math.max((Math.log10(volume24h + 1) / 3) * 100, 0), 100)
  const volatilityScore = 100 - Math.min(Math.max((volatility7dPercent / 25) * 100, 0), 100)
  const spreadScore = 100 - Math.min(Math.max((spreadPercent / 15) * 100, 0), 100)

  return round2(
    Math.min(Math.max(volumeScore * 0.55 + volatilityScore * 0.25 + spreadScore * 0.2, 0), 100)
  )
}

function resolveSevenDayChangePercent(snapshot = {}) {
  const average7d = toFiniteOrNull(snapshot?.average_7d_price)
  const lowestListing = toFiniteOrNull(snapshot?.lowest_listing_price)
  if (average7d == null || average7d <= 0 || lowestListing == null) {
    return null
  }
  return round2(((lowestListing - average7d) / average7d) * 100)
}

function resolveVolume7d(snapshot = {}) {
  const volume24h = toFiniteOrNull(snapshot?.volume_24h)
  if (volume24h == null || volume24h < 0) return null
  return round2(volume24h * 7)
}

function isSnapshotStale(snapshot = {}) {
  const capturedAt = toIsoStringOrNull(snapshot?.captured_at)
  if (!capturedAt) return true
  const ageMs = Date.now() - new Date(capturedAt).getTime()
  return ageMs > Math.max(Number(marketSnapshotTtlMinutes || 30), 1) * 60 * 1000
}

function toByNameMap(rows = [], keyField) {
  const map = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const key = String(row?.[keyField] || "").trim()
    if (!key) continue
    map[key] = row
  }
  return map
}

function toBySourceMap(rows = []) {
  const map = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const source = String(row?.source || row?.market || "")
      .trim()
      .toLowerCase()
    if (!source) continue
    map[source] = row
  }
  return map
}

function chunkArray(items = [], chunkSize = 50) {
  const safeChunkSize = Math.max(Number(chunkSize || 0), 1)
  const rows = Array.isArray(items) ? items : []
  const chunks = []
  for (let index = 0; index < rows.length; index += safeChunkSize) {
    chunks.push(rows.slice(index, index + safeChunkSize))
  }
  return chunks
}

function toNameKey(value) {
  return normalizeMarketHashName(value).toLowerCase()
}

function sourceRank(value) {
  const source = String(value || "")
    .trim()
    .toLowerCase()
  if (source === "catalog_eligible") return 5
  if (source === "catalog_near_eligible") return 4
  if (source === "curated_db") return 4
  if (source === "catalog_enriching") return 3
  if (source === "dynamic_snapshot") return 3
  if (source === "fallback_curated") return 2
  if (source === "fallback_mvp") return 2
  return 1
}

function resolveCatalogSeedSource(candidateStatus = CATALOG_CANDIDATE_STATUS.CANDIDATE) {
  const normalizedStatus = normalizeCatalogCandidateStatus(candidateStatus)
  if (normalizedStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE) return "catalog_eligible"
  if (normalizedStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) return "catalog_near_eligible"
  if (normalizedStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) return "catalog_enriching"
  return "dynamic_snapshot"
}

function computeUniverseSeedRank(seed = {}) {
  const volume7d = toFiniteOrNull(seed?.marketVolume7d) ?? 0
  const liquidityScore = toFiniteOrNull(seed?.liquidityScore) ?? 0
  const referencePrice = toFiniteOrNull(seed?.referencePrice) ?? 0
  const snapshotFreshness = seed?.hasSnapshotData ? (seed?.snapshotStale ? 0 : 1) : 0
  return (
    sourceRank(seed?.universeSource) * 100000 +
    snapshotFreshness * 50000 +
    Math.min(volume7d, 6000) * 8 +
    Math.min(liquidityScore, 100) * 55 +
    Math.min(referencePrice, 1000)
  )
}

function mergeUniverseSeeds(seedSets = []) {
  const byName = {}
  for (const set of Array.isArray(seedSets) ? seedSets : []) {
    for (const seed of Array.isArray(set) ? set : []) {
      const marketHashName = normalizeMarketHashName(seed?.marketHashName)
      if (!marketHashName) continue
      const key = toNameKey(marketHashName)
      const nextSeed = {
        ...seed,
        marketHashName
      }
      const existing = byName[key]
      if (!existing) {
        byName[key] = nextSeed
        continue
      }
      if (computeUniverseSeedRank(nextSeed) > computeUniverseSeedRank(existing)) {
        byName[key] = nextSeed
      }
    }
  }
  return Object.values(byName)
}

function resolveQuoteAgeMinutes(quote = {}) {
  const updatedAt = toIsoStringOrNull(
    quote?.updatedAt || quote?.fetched_at || quote?.fetchedAt || quote?.recorded_at
  )
  if (!updatedAt) return null
  const ageMinutes = (Date.now() - new Date(updatedAt).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return round2(ageMinutes)
}

function resolveSnapshotAgeMinutes(inputItem = {}) {
  const capturedAt = toIsoStringOrNull(inputItem?.snapshotCapturedAt || inputItem?.steamRecordedAt)
  if (!capturedAt) return null
  const ageMinutes = (Date.now() - new Date(capturedAt).getTime()) / (60 * 1000)
  if (!Number.isFinite(ageMinutes) || ageMinutes < 0) return null
  return round2(ageMinutes)
}

function resolveSnapshotFreshnessState(inputItem = {}, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const rules = getCategoryStaleRules(itemCategory)
  const ageMinutes = resolveSnapshotAgeMinutes(inputItem)
  const hasSnapshotData = Boolean(inputItem?.hasSnapshotData || inputItem?.snapshotCapturedAt)
  if (!hasSnapshotData || ageMinutes == null) {
    return {
      state: FRESHNESS_STATES.STALE,
      ageMinutes: ageMinutes,
      hasSnapshotData
    }
  }
  return {
    state: resolveFreshnessState(ageMinutes, rules),
    ageMinutes,
    hasSnapshotData
  }
}

function resolveCatalogSeedFreshnessContext(
  inputItem = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const rules = getCategoryStaleRules(itemCategory)
  const snapshotFreshnessBase = resolveSnapshotFreshnessState(inputItem, itemCategory)
  const snapshotState =
    snapshotFreshnessBase.ageMinutes == null && Boolean(inputItem?.hasSnapshotData)
      ? Boolean(inputItem?.snapshotStale)
        ? FRESHNESS_STATES.STALE
        : FRESHNESS_STATES.FRESH
      : snapshotFreshnessBase.state
  const snapshotFreshness = {
    ...snapshotFreshnessBase,
    state: snapshotState
  }
  const quoteAgeMinutes = resolveQuoteAgeMinutes({ fetchedAt: inputItem?.quoteFetchedAt })
  const quoteState = resolveFreshnessState(quoteAgeMinutes, rules)
  const state =
    snapshotFreshness.state === FRESHNESS_STATES.FRESH || quoteState === FRESHNESS_STATES.FRESH
      ? FRESHNESS_STATES.FRESH
      : snapshotFreshness.state === FRESHNESS_STATES.AGING || quoteState === FRESHNESS_STATES.AGING
        ? FRESHNESS_STATES.AGING
        : FRESHNESS_STATES.STALE

  return {
    state,
    usable:
      snapshotFreshness.state !== FRESHNESS_STATES.STALE ||
      quoteState !== FRESHNESS_STATES.STALE,
    snapshotFreshness,
    quoteState,
    quoteAgeMinutes
  }
}

function hasUsableQuotePrice(quote = {}) {
  const hasGross = Number.isFinite(Number(quote?.grossPrice)) && Number(quote.grossPrice) > 0
  const hasNet =
    Number.isFinite(Number(quote?.netPriceAfterFees)) && Number(quote.netPriceAfterFees) > 0
  return Boolean(quote?.available) && (hasGross || hasNet)
}

function resolveQuoteFreshnessEntry(quote = {}, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const source = normalizeMarketLabel(quote?.source || quote?.market)
  const ageMinutes = resolveQuoteAgeMinutes(quote)
  const rules = getCategoryStaleRules(itemCategory)
  const state = resolveFreshnessState(ageMinutes, rules)
  const hasUsablePrice = hasUsableQuotePrice(quote)
  return {
    source,
    ageMinutes,
    state,
    hasUsablePrice,
    usable: hasUsablePrice && state !== FRESHNESS_STATES.STALE
  }
}

function resolveStaleDataPenalty(
  perMarket = [],
  opportunity = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const rules = getCategoryStaleRules(itemCategory)
  const bySource = {}
  for (const row of Array.isArray(perMarket) ? perMarket : []) {
    const entry = resolveQuoteFreshnessEntry(row, itemCategory)
    if (!entry.source || !SOURCE_ORDER.includes(entry.source)) continue
    const existing = bySource[entry.source]
    if (!existing) {
      bySource[entry.source] = entry
      continue
    }
    const existingAge = toFiniteOrNull(existing.ageMinutes)
    const nextAge = toFiniteOrNull(entry.ageMinutes)
    if (existingAge == null && nextAge != null) {
      bySource[entry.source] = entry
      continue
    }
    if (existingAge != null && nextAge != null && nextAge < existingAge) {
      bySource[entry.source] = entry
    }
  }

  const buySource = normalizeMarketLabel(opportunity?.buyMarket)
  const sellSource = normalizeMarketLabel(opportunity?.sellMarket)
  const buyEntry = bySource[buySource] || null
  const sellEntry = bySource[sellSource] || null
  const buyAgeMinutes = toFiniteOrNull(buyEntry?.ageMinutes)
  const sellAgeMinutes = toFiniteOrNull(sellEntry?.ageMinutes)
  const maxAgeMinutes = [buyAgeMinutes, sellAgeMinutes].filter((value) => value != null).sort((a, b) => b - a)[0] ?? null

  let freshMarkets = 0
  let agingMarkets = 0
  let staleMarkets = 0
  let usableMarkets = 0
  for (const source of SOURCE_ORDER) {
    const entry = bySource[source]
    if (!entry) continue
    if (entry.state === FRESHNESS_STATES.FRESH) freshMarkets += 1
    else if (entry.state === FRESHNESS_STATES.AGING) agingMarkets += 1
    else staleMarkets += 1
    if (entry.usable) usableMarkets += 1
  }

  const selectedState =
    !buyEntry ||
    !sellEntry ||
    buyEntry.state === FRESHNESS_STATES.STALE ||
    sellEntry.state === FRESHNESS_STATES.STALE
      ? FRESHNESS_STATES.STALE
      : buyEntry.state === FRESHNESS_STATES.AGING || sellEntry.state === FRESHNESS_STATES.AGING
        ? FRESHNESS_STATES.AGING
        : FRESHNESS_STATES.FRESH

  let penalty = 0
  if (selectedState === FRESHNESS_STATES.AGING) {
    penalty += Number(rules.agingPenalty || 0)
  } else if (selectedState === FRESHNESS_STATES.STALE) {
    penalty += Number(rules.stalePenalty || 0)
  }
  if (usableMarkets === MIN_MARKET_COVERAGE && selectedState === FRESHNESS_STATES.AGING) {
    penalty += 3
  }

  return {
    penalty: round2(Math.max(penalty, 0)),
    maxAgeMinutes: maxAgeMinutes != null ? round2(maxAgeMinutes) : null,
    selectedState,
    selectedBuyState: String(buyEntry?.state || FRESHNESS_STATES.STALE),
    selectedSellState: String(sellEntry?.state || FRESHNESS_STATES.STALE),
    buyAgeMinutes,
    sellAgeMinutes,
    usableMarkets,
    freshMarkets,
    agingMarkets,
    staleMarkets,
    hasInsufficientUsableMarkets: usableMarkets < MIN_MARKET_COVERAGE,
    bySource,
    rules
  }
}

function buildFreshnessStateCounter() {
  return {
    [FRESHNESS_STATES.FRESH]: 0,
    [FRESHNESS_STATES.AGING]: 0,
    [FRESHNESS_STATES.STALE]: 0
  }
}

function normalizeFreshnessState(value = "") {
  const safe = String(value || "")
    .trim()
    .toLowerCase()
  if (safe === FRESHNESS_STATES.FRESH) return FRESHNESS_STATES.FRESH
  if (safe === FRESHNESS_STATES.AGING) return FRESHNESS_STATES.AGING
  return FRESHNESS_STATES.STALE
}

function incrementFreshnessCounter(counter = {}, state = FRESHNESS_STATES.STALE, count = 1) {
  const normalizedState = normalizeFreshnessState(state)
  counter[normalizedState] = Number(counter?.[normalizedState] || 0) + Number(count || 0)
}

function buildStaleDiagnosticsAccumulator() {
  const byMarket = Object.fromEntries(
    SOURCE_ORDER.map((source) => [source, buildFreshnessStateCounter()])
  )
  const byCategory = Object.fromEntries(
    Object.values(ITEM_CATEGORIES).map((category) => [category, buildFreshnessStateCounter()])
  )
  return {
    byMarket,
    byCategory
  }
}

function trackStaleDiagnostics(
  accumulator = null,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN,
  stale = {}
) {
  if (!accumulator || typeof accumulator !== "object") return
  if (!accumulator.byMarket || typeof accumulator.byMarket !== "object") {
    accumulator.byMarket = Object.fromEntries(
      SOURCE_ORDER.map((source) => [source, buildFreshnessStateCounter()])
    )
  }
  if (!accumulator.byCategory || typeof accumulator.byCategory !== "object") {
    accumulator.byCategory = Object.fromEntries(
      Object.values(ITEM_CATEGORIES).map((category) => [category, buildFreshnessStateCounter()])
    )
  }

  const normalizedCategory = normalizeItemCategory(itemCategory)
  if (!accumulator.byCategory[normalizedCategory]) {
    accumulator.byCategory[normalizedCategory] = buildFreshnessStateCounter()
  }
  incrementFreshnessCounter(accumulator.byCategory[normalizedCategory], stale?.selectedState, 1)

  const bySource = stale?.bySource && typeof stale.bySource === "object" ? stale.bySource : {}
  for (const source of SOURCE_ORDER) {
    const entry = bySource[source]
    if (!entry) continue
    if (!accumulator.byMarket[source]) {
      accumulator.byMarket[source] = buildFreshnessStateCounter()
    }
    incrementFreshnessCounter(accumulator.byMarket[source], entry?.state, 1)
  }
}

function toStaleDiagnosticsSummary(accumulator = {}) {
  const byMarket = {}
  const staleByMarket = {}
  const agingByMarket = {}
  for (const source of SOURCE_ORDER) {
    const bucket = accumulator?.byMarket?.[source] || {}
    const fresh = Number(bucket?.[FRESHNESS_STATES.FRESH] || 0)
    const aging = Number(bucket?.[FRESHNESS_STATES.AGING] || 0)
    const stale = Number(bucket?.[FRESHNESS_STATES.STALE] || 0)
    byMarket[source] = {
      fresh,
      aging,
      stale
    }
    staleByMarket[source] = stale
    agingByMarket[source] = aging
  }

  const byCategory = {}
  const staleByCategory = {}
  const agingByCategory = {}
  for (const category of Object.values(ITEM_CATEGORIES)) {
    const bucket = accumulator?.byCategory?.[category] || {}
    const fresh = Number(bucket?.[FRESHNESS_STATES.FRESH] || 0)
    const aging = Number(bucket?.[FRESHNESS_STATES.AGING] || 0)
    const stale = Number(bucket?.[FRESHNESS_STATES.STALE] || 0)
    byCategory[category] = {
      fresh,
      aging,
      stale
    }
    staleByCategory[category] = stale
    agingByCategory[category] = aging
  }

  return {
    quoteStateByMarket: byMarket,
    selectedStateByCategory: byCategory,
    staleByMarket,
    agingByMarket,
    staleByCategory,
    agingByCategory,
    totals: {
      staleQuotes: Object.values(staleByMarket).reduce((sum, value) => sum + Number(value || 0), 0),
      agingQuotes: Object.values(agingByMarket).reduce((sum, value) => sum + Number(value || 0), 0),
      staleItems: Object.values(staleByCategory).reduce((sum, value) => sum + Number(value || 0), 0),
      agingItems: Object.values(agingByCategory).reduce((sum, value) => sum + Number(value || 0), 0)
    }
  }
}

function normalizeMarketLabel(value) {
  const text = String(value || "").trim().toLowerCase()
  if (!text) return ""
  if (SOURCE_ORDER.includes(text)) return text
  return text
}

function countAvailableMarkets(perMarket = []) {
  const available = new Set()
  for (const row of Array.isArray(perMarket) ? perMarket : []) {
    const source = normalizeMarketLabel(row?.source || row?.market)
    if (!source || !SOURCE_ORDER.includes(source)) continue
    const hasGross = Number.isFinite(Number(row?.grossPrice)) && Number(row.grossPrice) > 0
    const hasNet =
      Number.isFinite(Number(row?.netPriceAfterFees)) && Number(row.netPriceAfterFees) > 0
    if (Boolean(row?.available) && (hasGross || hasNet)) {
      available.add(source)
    }
  }
  return available.size
}

function isLowValueJunkName(marketHashName = "") {
  const name = String(marketHashName || "").trim()
  if (!name) return true
  return LOW_VALUE_NAME_PATTERNS.some((pattern) => pattern.test(name))
}

function extractWeaponPrefix(name = "") {
  const text = String(name || "").trim()
  if (!text.includes("|")) return ""
  return String(
    text
      .replace(/^stattrak[â„¢\u2122]?\s*/i, "")
      .replace(/^souvenir\s+/i, "")
      .split("|")[0] || ""
  ).trim()
}

function hasLiquidNameSignal(name = "") {
  return LIQUID_NAME_SIGNAL_PATTERNS.some((pattern) => pattern.test(String(name || "")))
}

function isHighSignalMissingSnapshotSeed(itemName = "", itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalizedCategory = normalizeItemCategory(itemCategory, itemName)
  if (normalizedCategory === ITEM_CATEGORIES.CASE) {
    return /\b(case|souvenir package)\b/i.test(itemName)
  }
  if (normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE) {
    return /\b(sticker capsule|autograph capsule)\b/i.test(itemName)
  }
  if (normalizedCategory !== ITEM_CATEGORIES.WEAPON_SKIN) {
    return true
  }

  const prefix = extractWeaponPrefix(itemName)
  if (HIGH_SIGNAL_WEAPON_PREFIXES.has(prefix) && hasLiquidNameSignal(itemName)) {
    return true
  }
  return false
}

function countTrueValues(values = []) {
  return (Array.isArray(values) ? values : []).reduce(
    (sum, value) => sum + Number(Boolean(value)),
    0
  )
}

function isStatTrakVariantName(name = "") {
  return /^stattrak[Ã¢â€žÂ¢\u2122]?\s*/i.test(String(name || "").trim())
}

function isSouvenirVariantName(name = "") {
  return /^souvenir\s+/i.test(String(name || "").trim())
}

function buildWeaponSkinFilterDiagnostics() {
  const diagnostics = Object.fromEntries(
    WEAPON_SKIN_FILTER_DIAGNOSTIC_KEYS.map((key) => [key, 0])
  )
  diagnostics.__seen = {}
  return diagnostics
}

function incrementWeaponSkinFilterDiagnostic(diagnostics = {}, key = "", itemName = "") {
  const normalizedKey =
    WEAPON_SKIN_FILTER_DIAGNOSTIC_ALIAS[String(key || "").trim()] || String(key || "").trim()
  if (!WEAPON_SKIN_FILTER_DIAGNOSTIC_KEYS.includes(normalizedKey)) return
  if (!diagnostics || typeof diagnostics !== "object") return
  if (!diagnostics.__seen || typeof diagnostics.__seen !== "object") {
    diagnostics.__seen = {}
  }
  if (!diagnostics.__seen[normalizedKey]) {
    diagnostics.__seen[normalizedKey] = {}
  }

  const normalizedItem =
    normalizeMarketHashName(itemName) ||
    String(itemName || "")
      .trim()
      .toLowerCase()
  if (normalizedItem && diagnostics.__seen[normalizedKey][normalizedItem]) {
    return
  }
  if (normalizedItem) {
    diagnostics.__seen[normalizedKey][normalizedItem] = true
  }
  diagnostics[normalizedKey] = Number(diagnostics[normalizedKey] || 0) + 1
}

function toWeaponSkinFilterDiagnosticsSummary(diagnostics = {}) {
  return Object.fromEntries(
    WEAPON_SKIN_FILTER_DIAGNOSTIC_KEYS.map((key) => [key, Number(diagnostics?.[key] || 0)])
  )
}

function resolveSeedQuoteFreshnessState(
  inputItem = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const ageMinutes = resolveQuoteAgeMinutes({ fetchedAt: inputItem?.quoteFetchedAt })
  if (ageMinutes == null) return FRESHNESS_STATES.STALE
  return resolveFreshnessState(ageMinutes, getCategoryStaleRules(itemCategory))
}

function buildWeaponSkinSupportSignals({
  marketHashName = "",
  referencePrice = null,
  executionPrice = null,
  profit = null,
  spread = null,
  volume7d = null,
  marketCoverage = 0,
  snapshotFreshnessState = FRESHNESS_STATES.STALE,
  quoteFreshnessState = FRESHNESS_STATES.STALE,
  hasStrongReferenceDeviation = false,
  hasExtremeReferenceDeviation = false,
  isUsefulCandidate = false
} = {}) {
  const name = String(marketHashName || "").trim()
  const prefix = extractWeaponPrefix(name)
  const pricePoint = toFiniteOrNull(executionPrice) ?? toFiniteOrNull(referencePrice)
  const coverageCount = Math.max(Number(marketCoverage || 0), 0)

  return {
    lowValuePattern: isLowValueJunkName(name),
    hasNameSignal:
      hasLiquidNameSignal(name) || HIGH_SIGNAL_WEAPON_PREFIXES.has(prefix),
    hasNonTrivialPrice:
      pricePoint != null && pricePoint >= WEAPON_SKIN_FALLBACK_MIN_REFERENCE_PRICE_USD,
    hasMeaningfulProfit:
      toFiniteOrNull(profit) != null &&
      toFiniteOrNull(profit) >= WEAPON_SKIN_FALLBACK_MIN_PROFIT_USD,
    hasSaneSpread:
      toFiniteOrNull(spread) != null &&
      toFiniteOrNull(spread) >= WEAPON_SKIN_FALLBACK_MIN_SPREAD_PERCENT &&
      toFiniteOrNull(spread) <= WEAPON_SKIN_FALLBACK_MAX_SPREAD_PERCENT,
    coverageState:
      coverageCount >= MIN_MARKET_COVERAGE
        ? "normal"
        : coverageCount >= 1
          ? "borderline"
          : "blocked",
    hasAnyCoverage: coverageCount >= 1,
    hasBorderlineCoverage: coverageCount === 1,
    hasCoverage: coverageCount >= MIN_MARKET_COVERAGE,
    hasUsefulVolume:
      toFiniteOrNull(volume7d) != null &&
      toFiniteOrNull(volume7d) >= WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_VOLUME_7D,
    hasFreshData:
      normalizeFreshnessState(quoteFreshnessState) !== FRESHNESS_STATES.STALE ||
      normalizeFreshnessState(snapshotFreshnessState) !== FRESHNESS_STATES.STALE,
    hasNonStaleQuotes:
      normalizeFreshnessState(quoteFreshnessState) !== FRESHNESS_STATES.STALE,
    hasAcceptableReference:
      !Boolean(hasStrongReferenceDeviation) && !Boolean(hasExtremeReferenceDeviation),
    isUsefulCandidate: Boolean(isUsefulCandidate)
  }
}

function evaluateWeaponSkinLowValuePolicy({
  weakSignals = [],
  supportSignals = [],
  allowForward = false,
  hardRejectSupportMax = 1,
  penaltySignals = []
} = {}) {
  const weakCount = countTrueValues(weakSignals)
  const supportCount = countTrueValues(supportSignals)
  const hardReject =
    weakCount >= 5 ||
    (weakCount >= 4 && supportCount <= Number(hardRejectSupportMax || 0) && !allowForward)
  const penalty =
    !hardReject &&
    (weakCount >= 2 || countTrueValues(penaltySignals) > 0)

  return {
    weakCount,
    supportCount,
    hardReject,
    penalty
  }
}

function evaluateWeaponSkinSeedFreshnessContext({
  supportSignals = {},
  snapshotFreshness = {},
  quoteFreshnessState = FRESHNESS_STATES.STALE
} = {}) {
  const snapshotState = normalizeFreshnessState(snapshotFreshness?.state)
  const quoteState = normalizeFreshnessState(quoteFreshnessState)
  const hasStaleFreshness =
    snapshotState === FRESHNESS_STATES.STALE || quoteState === FRESHNESS_STATES.STALE
  const hasAgingFreshness =
    !hasStaleFreshness &&
    (snapshotState === FRESHNESS_STATES.AGING || quoteState === FRESHNESS_STATES.AGING)

  if (!hasStaleFreshness && !hasAgingFreshness) {
    return {
      hardRejectStale: false,
      penaltyKey: ""
    }
  }

  if (hasAgingFreshness) {
    return {
      hardRejectStale: false,
      penaltyKey: "aging_penalty_allowed_forward"
    }
  }

  const snapshotAgeMinutes = toFiniteOrNull(snapshotFreshness?.ageMinutes)
  const staleHardRejectMinutes =
    Math.min(
      Math.max(Number(marketSnapshotTtlMinutes || 30), 1) *
        WEAPON_SKIN_STALE_SNAPSHOT_HARD_REJECT_TTL_MULTIPLIER,
      24 * 60
    )
  const supportCount = countTrueValues([
    supportSignals.hasCoverage,
    supportSignals.hasNonTrivialPrice,
    supportSignals.hasNameSignal,
    supportSignals.hasUsefulVolume,
    supportSignals.isUsefulCandidate,
    quoteState !== FRESHNESS_STATES.STALE
  ])
  const weakCount = countTrueValues([
    !supportSignals.hasCoverage,
    !supportSignals.hasNonTrivialPrice,
    !supportSignals.hasNameSignal,
    !supportSignals.hasUsefulVolume && !supportSignals.isUsefulCandidate,
    supportSignals.lowValuePattern,
    quoteState === FRESHNESS_STATES.STALE
  ])
  const clearlyTooOld = snapshotAgeMinutes != null && snapshotAgeMinutes > staleHardRejectMinutes
  const hardRejectStale =
    (clearlyTooOld && quoteState === FRESHNESS_STATES.STALE && supportCount <= 4) ||
    weakCount >= 5 ||
    (weakCount >= 4 && supportCount <= 2)

  return {
    hardRejectStale,
    penaltyKey: hardRejectStale ? "" : "stale_penalty_allowed_forward"
  }
}

function evaluateWeaponSkinSeedFilters(inputItem = {}) {
  const referencePrice = toFiniteOrNull(inputItem?.referencePrice)
  const volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
  const marketCoverageCount = Math.max(Number(inputItem?.marketCoverageCount || 0), 0)
  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, ITEM_CATEGORIES.WEAPON_SKIN)
  const quoteFreshnessState = resolveSeedQuoteFreshnessState(inputItem, ITEM_CATEGORIES.WEAPON_SKIN)
  const supportSignals = buildWeaponSkinSupportSignals({
    marketHashName: inputItem?.marketHashName,
    referencePrice,
    volume7d,
    marketCoverage: marketCoverageCount,
    snapshotFreshnessState: snapshotFreshness?.state,
    quoteFreshnessState,
    isUsefulCandidate:
      Boolean(inputItem?.scanEligible) ||
      [MATURITY_STATES.ELIGIBLE, MATURITY_STATES.NEAR_ELIGIBLE].includes(
        normalizeMaturityState(inputItem?.maturityState)
      )
  })
  const hasMissingLiquidityContext = !Boolean(inputItem?.hasSnapshotData)
  const veryLowPrice =
    referencePrice != null && referencePrice < WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_PRICE_USD
  const weakCoverage = !supportSignals.hasAnyCoverage
  const weakVolume =
    volume7d != null &&
    volume7d < Number(UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN] || 0)
  const weakUtility = !supportSignals.isUsefulCandidate && !supportSignals.hasUsefulVolume
  const canForwardAsUsefulSkin =
    (supportSignals.hasCoverage && supportSignals.hasNonTrivialPrice) ||
    (supportSignals.hasCoverage && supportSignals.isUsefulCandidate) ||
    (
      supportSignals.hasBorderlineCoverage &&
      supportSignals.hasNonTrivialPrice &&
      (supportSignals.isUsefulCandidate || supportSignals.hasFreshData)
    ) ||
    (supportSignals.hasNameSignal && supportSignals.hasNonTrivialPrice && supportSignals.isUsefulCandidate)
  const lowValuePolicy = evaluateWeaponSkinLowValuePolicy({
    weakSignals: [
      supportSignals.lowValuePattern,
      !supportSignals.hasNameSignal,
      veryLowPrice,
      weakCoverage,
      weakUtility,
      !supportSignals.hasFreshData
    ],
    supportSignals: [
      supportSignals.hasNameSignal,
      supportSignals.hasNonTrivialPrice,
      supportSignals.hasAnyCoverage,
      supportSignals.hasUsefulVolume,
      supportSignals.hasFreshData,
      supportSignals.isUsefulCandidate
    ],
    allowForward: canForwardAsUsefulSkin,
    hardRejectSupportMax: 1,
    penaltySignals: [
      supportSignals.lowValuePattern,
      !supportSignals.hasNameSignal,
      veryLowPrice,
      weakUtility,
      weakVolume
    ]
  })
  const hardRejectLowValue = lowValuePolicy.hardReject
  const penaltyLowValue = lowValuePolicy.penalty

  const hardRejectMissingLiquidity =
    false
  const penaltyMissingLiquidity = hasMissingLiquidityContext
  const freshnessContext = hasMissingLiquidityContext
    ? {
        hardRejectStale: false,
        penaltyKey: ""
      }
    : evaluateWeaponSkinSeedFreshnessContext({
        supportSignals,
        snapshotFreshness,
        quoteFreshnessState
      })

  return {
    hardRejectLowValue,
    penaltyLowValue,
    hardRejectMissingLiquidity,
    penaltyMissingLiquidity,
    hardRejectStale: freshnessContext.hardRejectStale,
    freshnessPenaltyKey: freshnessContext.penaltyKey,
    variantPenaltyKeys: [
      isStatTrakVariantName(inputItem?.marketHashName) ? "stattrak_penalty" : "",
      isSouvenirVariantName(inputItem?.marketHashName) ? "souvenir_penalty" : ""
    ].filter(Boolean)
  }
}

function computeVolumeScore(volume7d, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalizedCategory = normalizeItemCategory(itemCategory)
  const volume = toFiniteOrNull(volume7d)
  if (volume == null || volume <= 0) return 0
  if (PREMIUM_ITEM_CATEGORIES.has(normalizedCategory)) {
    if (volume >= 80) return 100
    if (volume >= 40) return 90
    if (volume >= PREMIUM_MIN_VOLUME_HIGH) return 82
    if (volume >= PREMIUM_MIN_VOLUME_MEDIUM) return 70
    if (volume >= PREMIUM_MIN_VOLUME_REJECT) return 56
    return 20
  }
  if (volume >= 1000) return 100
  if (volume >= 500) return 92
  if (volume >= 200) return 80
  if (volume >= 100) return 65
  return 35
}

function computeMarketCoverageScore(coverageCount) {
  const value = Number(coverageCount || 0)
  if (value >= 4) return 100
  if (value === 3) return 85
  if (value === 2) return 70
  if (value === 1) return 35
  return 0
}

function computePriceStabilityScore(sevenDayChangePercent) {
  const change = toFiniteOrNull(sevenDayChangePercent)
  if (change == null) return 55
  const absChange = Math.abs(change)
  if (absChange < 5) return 100
  if (absChange <= 10) return 80
  if (absChange <= 20) return 50
  return 20
}

function computeReferencePriceScore(referencePrice, itemCategory = ITEM_CATEGORIES.WEAPON_SKIN) {
  const normalizedCategory = normalizeItemCategory(itemCategory)
  const price = toFiniteOrNull(referencePrice)
  if (PREMIUM_ITEM_CATEGORIES.has(normalizedCategory)) {
    if (price == null || price < 20) return 0
    if (price >= 250) return 100
    if (price >= 120) return 92
    if (price >= 60) return 82
    return 68
  }
  if (normalizedCategory === ITEM_CATEGORIES.STICKER_CAPSULE) {
    if (price == null || price < 0.75) return 0
    if (price >= 8) return 100
    if (price >= 3) return 85
    return 65
  }
  if (normalizedCategory === ITEM_CATEGORIES.CASE) {
    if (price == null || price < 0.5) return 0
    if (price >= 5) return 100
    if (price >= 2) return 85
    return 65
  }
  if (price == null || price < MIN_EXECUTION_PRICE_USD) return 0
  if (price >= 30) return 100
  if (price >= 10) return 85
  return 65
}

function computeLiquidityRank({
  volume7d = null,
  marketCoverage = 0,
  sevenDayChangePercent = null,
  referencePrice = null,
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
} = {}) {
  const volumeScore = computeVolumeScore(volume7d, itemCategory)
  const marketCoverageScore = computeMarketCoverageScore(marketCoverage)
  const priceStabilityScore = computePriceStabilityScore(sevenDayChangePercent)
  const referencePriceScore = computeReferencePriceScore(referencePrice, itemCategory)
  const liquidityRank = round2(
    volumeScore * 0.5 +
      marketCoverageScore * 0.2 +
      priceStabilityScore * 0.15 +
      referencePriceScore * 0.15
  )

  return {
    liquidityRank,
    volumeScore,
    marketCoverageScore,
    priceStabilityScore,
    referencePriceScore
  }
}

function normalizeDiagnosticReason(reason = "") {
  const raw = String(reason || "").trim()
  if (!raw) return ""
  return DIAGNOSTIC_REASON_ALIAS[raw] || raw
}

function incrementReasonCounter(counter = {}, reason, category = "") {
  const key = normalizeDiagnosticReason(reason)
  if (!key) return
  counter[key] = Number(counter[key] || 0) + 1

  const normalizedCategory = String(category || "").trim()
    ? normalizeItemCategory(category)
    : ""
  if (!normalizedCategory) return

  if (!counter.__byCategory || typeof counter.__byCategory !== "object") {
    counter.__byCategory = {}
  }
  if (!counter.__byCategory[normalizedCategory]) {
    counter.__byCategory[normalizedCategory] = {}
  }

  const categoryBucket = counter.__byCategory[normalizedCategory]
  categoryBucket[key] = Number(categoryBucket[key] || 0) + 1
}

function incrementItemReasonCounter(rejectionsByItem = {}, itemName, reason, category = "") {
  const normalizedItem = String(itemName || "").trim() || "Unknown item"
  const normalizedReason = normalizeDiagnosticReason(reason) || "unknown"
  const normalizedCategory = normalizeItemCategory(category)
  if (!rejectionsByItem[normalizedItem]) {
    rejectionsByItem[normalizedItem] = {
      total: 0,
      category: normalizedCategory,
      reasons: {}
    }
  }
  const bucket = rejectionsByItem[normalizedItem]
  if (!bucket.category) {
    bucket.category = normalizedCategory
  }
  bucket.total += 1
  bucket.reasons[normalizedReason] = Number(bucket.reasons[normalizedReason] || 0) + 1
}

function toTopRejectedItems(rejectionsByItem = {}, limit = 8) {
  return Object.entries(rejectionsByItem)
    .map(([itemName, payload]) => {
      const reasonEntries = Object.entries(payload?.reasons || {}).sort(
        (a, b) => Number(b[1] || 0) - Number(a[1] || 0)
      )
      const [mainReason, mainReasonCount] = reasonEntries[0] || ["unknown", 0]
      return {
        itemName,
        category: normalizeItemCategory(payload?.category),
        rejectedCount: Number(payload?.total || 0),
        mainReason,
        mainReasonCount: Number(mainReasonCount || 0),
        reasons: payload?.reasons || {}
      }
    })
    .sort(
      (a, b) =>
        Number(b.rejectedCount || 0) - Number(a.rejectedCount || 0) ||
        Number(b.mainReasonCount || 0) - Number(a.mainReasonCount || 0)
    )
    .slice(0, Math.max(Number(limit || 0), 0))
}

function toRejectionReasonsByItem(rejectionsByItem = {}, limit = 20) {
  return Object.entries(rejectionsByItem)
    .map(([itemName, payload]) => ({
      itemName,
      category: normalizeItemCategory(payload?.category),
      rejectedCount: Number(payload?.total || 0),
      reasons: payload?.reasons || {}
    }))
    .sort((a, b) => Number(b.rejectedCount || 0) - Number(a.rejectedCount || 0))
    .slice(0, Math.max(Number(limit || 0), 0))
}

function normalizeDiscardStats(counter = {}) {
  const normalized = {}
  for (const key of DIAGNOSTIC_REASON_KEYS) {
    normalized[key] = Number(counter?.[key] || 0)
  }
  for (const [reason, count] of Object.entries(counter || {})) {
    if (String(reason || "").startsWith("__")) continue
    if (DIAGNOSTIC_REASON_KEYS.includes(reason)) continue
    normalized[reason] = Number(count || 0)
  }
  return normalized
}

function normalizeDiscardStatsByCategory(counter = {}) {
  const byCategory =
    counter?.__byCategory && typeof counter.__byCategory === "object"
      ? counter.__byCategory
      : {}

  const normalized = {}
  for (const [category, reasonMap] of Object.entries(byCategory)) {
    const normalizedCategory = normalizeItemCategory(category)
    const reasonStats = normalizeDiscardStats(reasonMap || {})
    normalized[normalizedCategory] = {
      totalRejected: Object.values(reasonStats).reduce(
        (sum, value) => sum + Number(value || 0),
        0
      ),
      reasons: reasonStats
    }
  }

  return normalized
}

function buildInputItemFromSkinAndSnapshot({
  skin = null,
  snapshot = null,
  catalogRow = null,
  marketHashName = "",
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  subcategory = null,
  universeSource = "fallback_curated",
  liquidityRank = null
} = {}) {
  const normalizedName = normalizeMarketHashName(
    marketHashName || skin?.market_hash_name || skin?.marketHashName
  )
  if (!normalizedName) return null

  const catalogReferencePrice = toPositiveOrNull(
    toFiniteOrNull(catalogRow?.reference_price ?? catalogRow?.referencePrice)
  )
  const catalogVolume7d = toPositiveOrNull(toFiniteOrNull(catalogRow?.volume_7d ?? catalogRow?.volume7d))
  const referencePrice = toPositiveOrNull(
    toFiniteOrNull(snapshot?.average_7d_price) ??
      toFiniteOrNull(snapshot?.lowest_listing_price) ??
      catalogReferencePrice
  )
  const volume7d = toPositiveOrNull(resolveVolume7d(snapshot || {}) ?? catalogVolume7d)
  const snapshotCapturedAt =
    snapshot?.captured_at || catalogRow?.snapshot_captured_at || catalogRow?.snapshotCapturedAt || null
  const snapshotStale =
    catalogRow?.snapshot_stale == null
      ? snapshot
        ? isSnapshotStale(snapshot)
        : !snapshotCapturedAt
      : Boolean(catalogRow.snapshot_stale)
  const hasUsableSnapshotLiquidity = referencePrice != null || volume7d != null
  const hasSnapshotData = Boolean(snapshotCapturedAt) && hasUsableSnapshotLiquidity
  const liquidityScore =
    snapshot && hasUsableSnapshotLiquidity ? computeLiquidityScoreFromSnapshot(snapshot) : null
  const sevenDayChangePercent =
    snapshot && hasUsableSnapshotLiquidity ? resolveSevenDayChangePercent(snapshot) : null
  const imageUrl = sanitizeImageUrl(skin?.image_url || skin?.imageUrl)
  const imageUrlLarge =
    sanitizeImageUrl(skin?.image_url_large || skin?.imageUrlLarge) || imageUrl || null
  const candidateStatus = normalizeCatalogCandidateStatus(
    catalogRow?.candidate_status ?? catalogRow?.candidateStatus,
    catalogRow?.scan_eligible
      ? CATALOG_CANDIDATE_STATUS.ELIGIBLE
      : CATALOG_CANDIDATE_STATUS.CANDIDATE
  )
  const marketCoverageCount = Math.max(
    Number(
      toFiniteOrNull(catalogRow?.market_coverage_count ?? catalogRow?.marketCoverageCount) || 0
    ),
    0
  )
  const missingSnapshot =
    catalogRow?.missing_snapshot == null
      ? !hasSnapshotData
      : Boolean(catalogRow.missing_snapshot)
  const missingReference =
    catalogRow?.missing_reference == null
      ? referencePrice == null
      : Boolean(catalogRow.missing_reference)
  const missingMarketCoverage =
    catalogRow?.missing_market_coverage == null
      ? marketCoverageCount < MIN_MARKET_COVERAGE
      : Boolean(catalogRow.missing_market_coverage)
  const enrichmentPriority =
    toFiniteOrNull(catalogRow?.enrichment_priority ?? catalogRow?.enrichmentPriority) ?? 0
  const eligibilityReason = String(
    catalogRow?.eligibility_reason || catalogRow?.eligibilityReason || ""
  ).trim()
  const maturityState = normalizeMaturityState(
    catalogRow?.maturity_state ?? catalogRow?.maturityState,
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
      ? MATURITY_STATES.ELIGIBLE
      : candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
        ? MATURITY_STATES.NEAR_ELIGIBLE
        : candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING
          ? MATURITY_STATES.ENRICHING
          : MATURITY_STATES.COLD
  )
  const scanLayer = String(
    catalogRow?.scan_layer || catalogRow?.scanLayer || resolveScanLayerForMaturity({ maturityState })
  )
    .trim()
    .toLowerCase()
  const snapshotState = normalizeLowerText(catalogRow?.snapshot_state ?? catalogRow?.snapshotState) || null
  const referenceState =
    normalizeLowerText(catalogRow?.reference_state ?? catalogRow?.referenceState) || null
  const liquidityState =
    normalizeLowerText(catalogRow?.liquidity_state ?? catalogRow?.liquidityState) || null
  const coverageState = normalizeLowerText(catalogRow?.coverage_state ?? catalogRow?.coverageState) || null
  const progressionStatus =
    normalizeLowerText(catalogRow?.progression_status ?? catalogRow?.progressionStatus) || null
  const progressionBlockers = normalizeTextList(
    catalogRow?.progression_blockers ?? catalogRow?.progressionBlockers
  )
  const invalidReason =
    normalizeTextValue(catalogRow?.invalid_reason ?? catalogRow?.invalidReason) || null

  return {
    skinId: Number(skin?.id || 0) || null,
    marketHashName: normalizedName,
    itemCategory: normalizeItemCategory(category, normalizedName),
    itemSubcategory: String(subcategory || "").trim() || null,
    itemRarity: String(skin?.rarity || "").trim() || null,
    itemRarityColor:
      String(skin?.rarity_color || skin?.rarityColor || "").trim() || null,
    itemImageUrl: imageUrl,
    itemImageUrlLarge: imageUrlLarge,
    quantity: 1,
    marketVolume7d: volume7d,
    liquidityScore,
    sevenDayChangePercent,
    referencePrice,
    hasSnapshotData,
    snapshotCapturedAt,
    snapshotStale,
    universeSource: String(universeSource || "fallback_curated").trim() || "fallback_curated",
    liquidityRank: toFiniteOrNull(liquidityRank),
    candidateStatus,
    scanEligible:
      catalogRow?.scan_eligible == null ? candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE : Boolean(catalogRow.scan_eligible),
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    enrichmentPriority,
    maturityState,
    maturityScore: clampMaturityScore(catalogRow?.maturity_score ?? catalogRow?.maturityScore),
    scanLayer,
    quoteFetchedAt: catalogRow?.quote_fetched_at || catalogRow?.quoteFetchedAt || null,
    eligibilityReason: eligibilityReason || null,
    marketCoverageCount,
    snapshotState,
    referenceState,
    liquidityState,
    coverageState,
    progressionStatus,
    progressionBlockers,
    invalidReason
  }
}

async function ensureSkinsForMarketNames(marketNames = []) {
  const uniqueNames = Array.from(
    new Set(
      (Array.isArray(marketNames) ? marketNames : [])
        .map((name) => normalizeMarketHashName(name))
        .filter(Boolean)
    )
  )
  if (!uniqueNames.length) {
    return []
  }

  let existing = []
  try {
    existing = await skinRepo.getByMarketHashNames(uniqueNames)
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to fetch skins by market hash name", err.message)
    existing = uniqueNames.map((marketHashName) => ({ market_hash_name: marketHashName }))
  }
  const existingByName = toByNameMap(
    (Array.isArray(existing) ? existing : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const missingNames = uniqueNames.filter((name) => !existingByName[name])
  if (!missingNames.length) {
    return Array.isArray(existing) ? existing : []
  }

  try {
    await skinRepo.upsertSkins(
      missingNames.map((marketHashName) => ({
        market_hash_name: marketHashName
      }))
    )
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to auto-seed missing skins", err.message)
  }

  try {
    return await skinRepo.getByMarketHashNames(uniqueNames)
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to refetch skins after auto-seed", err.message)
    return Array.isArray(existing) ? existing : []
  }
}

function computeStrictCoverageThreshold(totalSeeds = 0) {
  const seedCount = Math.max(Math.round(Number(totalSeeds || 0)), 0)
  if (!seedCount) return MIN_STRICT_SEED_COVERAGE
  const ratioThreshold = Math.round(seedCount * STRICT_SEED_COVERAGE_RATIO_TARGET)
  return Math.max(Math.min(ratioThreshold, seedCount), MIN_STRICT_SEED_COVERAGE)
}

function normalizeCatalogCandidateStatus(value, fallback = CATALOG_CANDIDATE_STATUS.CANDIDATE) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
  if (Object.values(CATALOG_CANDIDATE_STATUS).includes(normalized)) {
    return normalized
  }
  const fallbackNormalized = String(fallback || "")
    .trim()
    .toLowerCase()
  return Object.values(CATALOG_CANDIDATE_STATUS).includes(fallbackNormalized)
    ? fallbackNormalized
    : CATALOG_CANDIDATE_STATUS.CANDIDATE
}

function normalizeMaturityState(value, fallback = MATURITY_STATES.COLD) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
  if (Object.values(MATURITY_STATES).includes(normalized)) {
    return normalized
  }
  return String(fallback || MATURITY_STATES.COLD)
    .trim()
    .toLowerCase()
}

function clampMaturityScore(value) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return 0
  return round2(Math.max(Math.min(parsed, 100), 0))
}

function buildMaturityCounter(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(Object.values(MATURITY_STATES).map((state) => [state, initial]))
}

function buildMaturityByCategoryCounter(initialValue = 0) {
  return Object.fromEntries(
    SCANNER_AUDIT_CATEGORIES.map((category) => [category, buildMaturityCounter(initialValue)])
  )
}

function buildLayerCounter(initialValue = 0) {
  const initial = Number(initialValue || 0)
  return Object.fromEntries(Object.values(SCAN_LAYERS).map((layer) => [layer, initial]))
}

function buildLayerByCategoryCounter(initialValue = 0) {
  return Object.fromEntries(
    SCANNER_AUDIT_CATEGORIES.map((category) => [category, buildLayerCounter(initialValue)])
  )
}

function normalizeCatalogRowsByMarketHashName(rows = []) {
  const map = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeMarketHashName(row?.market_hash_name || row?.marketHashName)
    if (!marketHashName) continue
    map[marketHashName] = row
  }
  return map
}

function applyCatalogStateToSeed(seed = {}, catalogRow = null) {
  if (!catalogRow || typeof catalogRow !== "object") {
    const fallbackCandidateStatus = normalizeCatalogCandidateStatus(
      seed?.candidateStatus,
      seed?.scanEligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
    )
    const fallbackMaturityState = normalizeMaturityState(
      seed?.maturityState,
      fallbackCandidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
        ? MATURITY_STATES.ELIGIBLE
        : fallbackCandidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
          ? MATURITY_STATES.NEAR_ELIGIBLE
          : fallbackCandidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING
            ? MATURITY_STATES.ENRICHING
            : MATURITY_STATES.COLD
    )
    return {
      ...seed,
      candidateStatus: fallbackCandidateStatus,
      scanEligible: Boolean(seed?.scanEligible),
      missingSnapshot: Boolean(seed?.missingSnapshot),
      missingReference: Boolean(seed?.missingReference),
      missingMarketCoverage: Boolean(seed?.missingMarketCoverage),
      enrichmentPriority: toFiniteOrNull(seed?.enrichmentPriority) ?? 0,
      maturityState: fallbackMaturityState,
      maturityScore: clampMaturityScore(seed?.maturityScore),
      scanLayer: String(seed?.scanLayer || resolveScanLayerForMaturity({ maturityState: fallbackMaturityState }))
        .trim()
        .toLowerCase(),
      quoteFetchedAt: seed?.quoteFetchedAt || null,
      eligibilityReason: String(seed?.eligibilityReason || "").trim() || null,
      marketCoverageCount: Math.max(Number(seed?.marketCoverageCount || 0), 0),
      snapshotState: normalizeLowerText(seed?.snapshotState ?? seed?.snapshot_state) || null,
      referenceState: normalizeLowerText(seed?.referenceState ?? seed?.reference_state) || null,
      liquidityState: normalizeLowerText(seed?.liquidityState ?? seed?.liquidity_state) || null,
      coverageState: normalizeLowerText(seed?.coverageState ?? seed?.coverage_state) || null,
      progressionStatus:
        normalizeLowerText(seed?.progressionStatus ?? seed?.progression_status) || null,
      progressionBlockers: normalizeTextList(
        seed?.progressionBlockers ?? seed?.progression_blockers
      ),
      invalidReason: normalizeTextValue(seed?.invalidReason ?? seed?.invalid_reason) || null
    }
  }

  const referencePrice = toFiniteOrNull(seed?.referencePrice)
  const marketCoverageCount = Math.max(
    Number(
      toFiniteOrNull(catalogRow?.market_coverage_count ?? catalogRow?.marketCoverageCount) ??
        seed?.marketCoverageCount ??
        0
    ),
    0
  )
  const snapshotStale =
    catalogRow?.snapshot_stale == null ? Boolean(seed?.snapshotStale) : Boolean(catalogRow.snapshot_stale)
  const snapshotCapturedAt = catalogRow?.snapshot_captured_at || seed?.snapshotCapturedAt || null
  const missingSnapshot =
    catalogRow?.missing_snapshot == null
      ? !Boolean(seed?.hasSnapshotData)
      : Boolean(catalogRow.missing_snapshot)
  const missingReference =
    catalogRow?.missing_reference == null ? referencePrice == null : Boolean(catalogRow.missing_reference)
  const missingMarketCoverage =
    catalogRow?.missing_market_coverage == null
      ? marketCoverageCount < MIN_MARKET_COVERAGE
      : Boolean(catalogRow.missing_market_coverage)
  const candidateStatus = normalizeCatalogCandidateStatus(
    catalogRow?.candidate_status ?? catalogRow?.candidateStatus,
    seed?.scanEligible || catalogRow?.scan_eligible
      ? CATALOG_CANDIDATE_STATUS.ELIGIBLE
      : CATALOG_CANDIDATE_STATUS.CANDIDATE
  )
  const maturityState = normalizeMaturityState(
    catalogRow?.maturity_state ?? catalogRow?.maturityState,
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE
      ? MATURITY_STATES.ELIGIBLE
      : candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
        ? MATURITY_STATES.NEAR_ELIGIBLE
        : candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING
          ? MATURITY_STATES.ENRICHING
          : MATURITY_STATES.COLD
  )

  return {
    ...seed,
    candidateStatus,
    scanEligible:
      catalogRow?.scan_eligible == null ? Boolean(seed?.scanEligible) : Boolean(catalogRow.scan_eligible),
    missingSnapshot,
    missingReference,
    missingMarketCoverage,
    enrichmentPriority:
      toFiniteOrNull(catalogRow?.enrichment_priority ?? catalogRow?.enrichmentPriority) ??
      toFiniteOrNull(seed?.enrichmentPriority) ??
      0,
    eligibilityReason:
      String(catalogRow?.eligibility_reason || catalogRow?.eligibilityReason || seed?.eligibilityReason || "")
        .trim() || null,
    marketCoverageCount,
    maturityState,
    maturityScore: clampMaturityScore(catalogRow?.maturity_score ?? catalogRow?.maturityScore),
    scanLayer: String(
      catalogRow?.scan_layer || catalogRow?.scanLayer || resolveScanLayerForMaturity({ maturityState })
    )
      .trim()
      .toLowerCase(),
    quoteFetchedAt: catalogRow?.quote_fetched_at || catalogRow?.quoteFetchedAt || seed?.quoteFetchedAt || null,
    snapshotStale,
    snapshotCapturedAt,
    snapshotState:
      normalizeLowerText(catalogRow?.snapshot_state ?? catalogRow?.snapshotState ?? seed?.snapshotState) ||
      null,
    referenceState:
      normalizeLowerText(catalogRow?.reference_state ?? catalogRow?.referenceState ?? seed?.referenceState) ||
      null,
    liquidityState:
      normalizeLowerText(catalogRow?.liquidity_state ?? catalogRow?.liquidityState ?? seed?.liquidityState) ||
      null,
    coverageState:
      normalizeLowerText(catalogRow?.coverage_state ?? catalogRow?.coverageState ?? seed?.coverageState) ||
      null,
    progressionStatus:
      normalizeLowerText(
        catalogRow?.progression_status ?? catalogRow?.progressionStatus ?? seed?.progressionStatus
      ) || null,
    progressionBlockers: normalizeTextList(
      catalogRow?.progression_blockers ??
        catalogRow?.progressionBlockers ??
        seed?.progressionBlockers ??
        seed?.progression_blockers
    ),
    invalidReason:
      normalizeTextValue(catalogRow?.invalid_reason ?? catalogRow?.invalidReason ?? seed?.invalidReason) ||
      null
  }
}

function resolveMaturityStateForSeed(seed = {}) {
  const itemCategory = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
  const candidateStatus = normalizeCatalogCandidateStatus(
    seed?.candidateStatus,
    seed?.scanEligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
  )
  const hasSnapshotData = Boolean(seed?.hasSnapshotData)
  const referencePrice = toFiniteOrNull(seed?.referencePrice)
  const coverageCount = Math.max(Number(seed?.marketCoverageCount || 0), 0)
  const volume7d = toFiniteOrNull(seed?.marketVolume7d)
  const missingSnapshot = Boolean(seed?.missingSnapshot) || !hasSnapshotData
  const missingReference = Boolean(seed?.missingReference) || referencePrice == null
  const missingCoverage = Boolean(seed?.missingMarketCoverage) || coverageCount < MIN_MARKET_COVERAGE
  const missingLiquidityContext = volume7d == null
  const freshness = resolveCatalogSeedFreshnessContext(seed, itemCategory)
  const categoryVolumeFloor = Math.max(
    Number(UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY[itemCategory] || 20),
    1
  )
  const partialCoverage = coverageCount >= Math.max(1, MIN_MARKET_COVERAGE - 1)
  const hasMinimumCoverageForProgress =
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN ? coverageCount >= 1 : partialCoverage
  const reasonableVolume =
    volume7d != null && volume7d >= Math.max(categoryVolumeFloor * 0.55, 18)
  const sufficientVolume = volume7d != null && volume7d >= categoryVolumeFloor
  const nearEligibleSupportCount = countTrueValues([
    !missingReference,
    freshness.usable,
    hasMinimumCoverageForProgress || reasonableVolume,
    hasSnapshotData || coverageCount >= 1,
    !missingLiquidityContext || referencePrice != null
  ])
  const missingSignals =
    Number(missingSnapshot) +
    Number(missingReference) +
    Number(missingCoverage) +
    Number(missingLiquidityContext)
  const structuralReason = String(seed?.eligibilityReason || "").trim().toLowerCase()
  const structuralPenalty = /\brejected|hard|outofscope|namepattern|unsupported\b/.test(structuralReason)
    ? 12
    : 0
  const categoryBoost =
    itemCategory === ITEM_CATEGORIES.CASE
      ? 4
      : itemCategory === ITEM_CATEGORIES.STICKER_CAPSULE
        ? 5
        : 0
  let maturityState = MATURITY_STATES.COLD
  if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    hasSnapshotData &&
    freshness.usable &&
    !missingReference &&
    !missingCoverage &&
    sufficientVolume
  ) {
    maturityState = MATURITY_STATES.ELIGIBLE
  } else if (
    (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
      candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE ||
      candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING) &&
    missingSignals <= 2 &&
    freshness.usable &&
    !missingReference &&
    (hasMinimumCoverageForProgress || (itemCategory !== ITEM_CATEGORIES.WEAPON_SKIN && reasonableVolume)) &&
    nearEligibleSupportCount >= 3
  ) {
    maturityState = MATURITY_STATES.NEAR_ELIGIBLE
  } else if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE ||
    candidateStatus === CATALOG_CANDIDATE_STATUS.ENRICHING ||
    (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE && nearEligibleSupportCount >= 2)
  ) {
    maturityState = MATURITY_STATES.ENRICHING
  }

  const baseScore =
    maturityState === MATURITY_STATES.ELIGIBLE
      ? 84
      : maturityState === MATURITY_STATES.NEAR_ELIGIBLE
        ? 66
        : maturityState === MATURITY_STATES.ENRICHING
          ? 46
          : 24
  const freshnessBoost =
    freshness.state === FRESHNESS_STATES.FRESH
      ? 8
      : freshness.state === FRESHNESS_STATES.AGING
        ? 3
        : -8
  const referenceBoost = referencePrice == null ? -8 : Math.min(referencePrice, 12)
  const coverageBoost = Math.min(coverageCount * 3, 15)
  const volumeBoost =
    volume7d == null
      ? -6
      : Math.min((volume7d / Math.max(UNIVERSE_MIN_VOLUME_7D_BY_CATEGORY[itemCategory] || 20, 1)) * 12, 14)
  const liquidityBoost = Math.min((toFiniteOrNull(seed?.liquidityRank) ?? 0) * 0.12, 10)
  const maturityScore = clampMaturityScore(
    baseScore +
      categoryBoost +
      freshnessBoost +
      referenceBoost +
      coverageBoost +
      volumeBoost +
      liquidityBoost -
      missingSignals * 6 -
      structuralPenalty
  )

  return {
    maturityState: normalizeMaturityState(maturityState),
    maturityScore,
    missingSignals,
    candidateStatus,
    structuralPenalty,
    freshnessState: freshness.state
  }
}

function isMinimumOpportunityBackfillReadySeed(seed = {}) {
  const candidateStatus = normalizeCatalogCandidateStatus(
    seed?.candidateStatus,
    seed?.scanEligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
  )
  if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
    candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
  ) {
    return true
  }
  if (
    candidateStatus !== CATALOG_CANDIDATE_STATUS.ENRICHING &&
    candidateStatus !== CATALOG_CANDIDATE_STATUS.CANDIDATE
  ) {
    return false
  }

  const itemCategory = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
  const maturityState = normalizeMaturityState(seed?.maturityState)
  if (maturityState === MATURITY_STATES.COLD) {
    return false
  }

  const progressionBlockers = resolveSeedProgressionBlockers(seed)
  if (hasOpportunitySeedRiskProfileBlock(seed, progressionBlockers)) {
    return false
  }

  const referencePrice = toFiniteOrNull(seed?.referencePrice)
  const coverageCount = Math.max(Number(seed?.marketCoverageCount || 0), 0)
  const freshness = resolveCatalogSeedFreshnessContext(seed, itemCategory)
  if (coverageCount <= 0) {
    return false
  }
  const snapshotCapturedAt = normalizeTextValue(seed?.snapshotCapturedAt ?? seed?.snapshot_captured_at)
  const hasSnapshotSignal =
    Boolean(seed?.hasSnapshotData) || Boolean(snapshotCapturedAt) || freshness.hasQuoteFreshness
  if (!hasSnapshotSignal) {
    return false
  }

  const volume7d = toFiniteOrNull(seed?.marketVolume7d ?? seed?.volume_7d)
  const liquidityRank = toFiniteOrNull(seed?.liquidityRank ?? seed?.liquidity_rank) ?? 0
  const hasLiquidityProxy = volume7d != null || liquidityRank >= 28
  const hasReferenceOrSafeProxy =
    referencePrice != null || (coverageCount >= 1 && hasSnapshotSignal && hasLiquidityProxy)
  if (!hasReferenceOrSafeProxy) {
    return false
  }

  if (candidateStatus === CATALOG_CANDIDATE_STATUS.CANDIDATE) {
    const supportCount = countTrueValues([
      referencePrice != null,
      freshness.usable,
      hasSnapshotSignal,
      hasLiquidityProxy,
      coverageCount >= MIN_MARKET_COVERAGE
    ])
    if (supportCount < 2) {
      return false
    }
  }

  return true
}

function resolveScanLayerForMaturity(seed = {}) {
  const maturityState = normalizeMaturityState(seed?.maturityState)
  if (maturityState === MATURITY_STATES.ELIGIBLE) return SCAN_LAYERS.HOT
  if (
    maturityState === MATURITY_STATES.NEAR_ELIGIBLE ||
    maturityState === MATURITY_STATES.ENRICHING
  ) {
    return SCAN_LAYERS.WARM
  }
  return SCAN_LAYERS.COLD
}

function computeLayerPriority(seed = {}) {
  const category = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
  const maturityScore = clampMaturityScore(seed?.maturityScore)
  const liquidityRank = toFiniteOrNull(seed?.liquidityRank) ?? 0
  const enrichmentPriority = toFiniteOrNull(seed?.enrichmentPriority) ?? 0
  const marketCoverageCount = Math.max(Number(seed?.marketCoverageCount || 0), 0)
  const categoryBoost =
    category === ITEM_CATEGORIES.CASE
      ? 6
      : category === ITEM_CATEGORIES.STICKER_CAPSULE
        ? 8
        : 0
  const sourceBoost =
    seed?.universeSource === "catalog_eligible"
      ? 8
      : seed?.universeSource === "catalog_near_eligible"
        ? 6
        : seed?.universeSource === "curated_db"
          ? 5
          : seed?.universeSource === "catalog_enriching"
            ? 3
            : seed?.universeSource === "fallback_curated"
              ? 2
              : 0
  const volumeBoost = Math.min((toFiniteOrNull(seed?.marketVolume7d) ?? 0) / 20, 10)
  const referenceBoost = Math.min((toFiniteOrNull(seed?.referencePrice) ?? 0) / 4, 10)
  const coverageBoost = Math.min(marketCoverageCount * 4, 16)
  const signalHistoryBoost = Math.min(
    toFiniteOrNull(seed?.signalHistoryScore ?? seed?.signalHistory?.score) ?? 0,
    18
  )
  return round2(
    maturityScore +
      liquidityRank * 0.5 +
      enrichmentPriority * 0.25 +
      categoryBoost +
      sourceBoost +
      volumeBoost +
      referenceBoost +
      coverageBoost +
      signalHistoryBoost
  )
}

function buildLayerDiagnostics(seeds = []) {
  const maturityFunnel = buildMaturityCounter(0)
  const maturityByCategory = buildMaturityByCategoryCounter(0)
  const layers = buildLayerCounter(0)
  const layersByCategory = buildLayerByCategoryCounter(0)

  for (const row of Array.isArray(seeds) ? seeds : []) {
    const category = normalizeItemCategory(row?.itemCategory, row?.marketHashName)
    if (!SCANNER_AUDIT_CATEGORIES.includes(category)) continue
    const maturityState = normalizeMaturityState(row?.maturityState)
    const scanLayer = String(row?.scanLayer || "").trim().toLowerCase()
    if (maturityFunnel[maturityState] == null) {
      maturityFunnel[maturityState] = 0
    }
    maturityFunnel[maturityState] += 1
    maturityByCategory[category][maturityState] =
      Number(maturityByCategory[category]?.[maturityState] || 0) + 1

    if (!Object.values(SCAN_LAYERS).includes(scanLayer)) continue
    layers[scanLayer] = Number(layers[scanLayer] || 0) + 1
    layersByCategory[category][scanLayer] = Number(layersByCategory[category]?.[scanLayer] || 0) + 1
  }

  return {
    maturityFunnel,
    maturityByCategory,
    layers,
    layersByCategory
  }
}

function buildOpportunityAdmissionDiagnostics() {
  return {
    universe_total: 0,
    universe_loaded_for_scan: 0,
    universe_deferred_before_scan: 0,
    universe_eligible: 0,
    universe_near_eligible: 0,
    universe_blocked: 0,
    near_eligible_total: 0,
    near_eligible_scanable: 0,
    near_eligible_blocked: 0,
    scan_candidates_loaded: 0,
    scan_candidates_deferred: 0,
    scan_candidates_executed: 0,
    scan_candidates_from_strict_eligible: 0,
    scan_candidates_from_risky_ready: 0,
    risky_scan_ready_loaded: 0,
    risky_scan_ready_executed: 0,
    deferred_due_to_missing_reference: 0,
    deferred_due_to_missing_snapshot: 0,
    deferred_due_to_missing_liquidity: 0,
    deferred_due_to_stale: 0,
    deferred_due_to_maturity: 0,
    deferred_due_to_risk_profile: 0,
    deferred_due_to_feed_visibility: 0,
    deferred_due_to_visibility_or_feed_floor: 0,
    deferred_near_eligible_missing_reference: 0,
    deferred_near_eligible_missing_snapshot: 0,
    deferred_near_eligible_insufficient_coverage: 0,
    deferred_near_eligible_liquidity_only: 0,
    executed_from_strict_eligible: 0,
    executed_from_near_eligible: 0,
    skins_scanned_count: 0,
    cases_scanned_count: 0,
    capsules_scanned_count: 0
  }
}

function resolveSeedCandidateStatus(seed = {}) {
  return normalizeCatalogCandidateStatus(
    seed?.candidateStatus,
    seed?.scanEligible ? CATALOG_CANDIDATE_STATUS.ELIGIBLE : CATALOG_CANDIDATE_STATUS.CANDIDATE
  )
}

function resolveSeedSnapshotDiagnosticState(seed = {}) {
  const explicit = normalizeLowerText(seed?.snapshotState ?? seed?.snapshot_state)
  if (Object.values(SOURCE_CATALOG_SNAPSHOT_STATES).includes(explicit)) {
    return explicit
  }
  const snapshotCapturedAt = seed?.snapshotCapturedAt ?? seed?.snapshot_captured_at
  if (!normalizeTextValue(snapshotCapturedAt)) {
    return SOURCE_CATALOG_SNAPSHOT_STATES.MISSING
  }
  if (Boolean(seed?.snapshotStale ?? seed?.snapshot_stale)) {
    return SOURCE_CATALOG_SNAPSHOT_STATES.STALE
  }
  const hasReference = toFiniteOrNull(seed?.referencePrice ?? seed?.reference_price) != null
  const hasLiquidity = toFiniteOrNull(seed?.marketVolume7d ?? seed?.volume_7d) != null
  return hasReference && hasLiquidity
    ? SOURCE_CATALOG_SNAPSHOT_STATES.READY
    : SOURCE_CATALOG_SNAPSHOT_STATES.PARTIAL
}

function resolveSeedReferenceDiagnosticState(seed = {}) {
  const explicit = normalizeLowerText(seed?.referenceState ?? seed?.reference_state)
  if (Object.values(SOURCE_CATALOG_REFERENCE_STATES).includes(explicit)) {
    return explicit
  }
  return toFiniteOrNull(seed?.referencePrice ?? seed?.reference_price) == null
    ? SOURCE_CATALOG_REFERENCE_STATES.MISSING
    : resolveSeedSnapshotDiagnosticState(seed) === SOURCE_CATALOG_SNAPSHOT_STATES.READY
      ? SOURCE_CATALOG_REFERENCE_STATES.SNAPSHOT
      : SOURCE_CATALOG_REFERENCE_STATES.QUOTE
}

function resolveSeedLiquidityDiagnosticState(seed = {}) {
  const explicit = normalizeLowerText(seed?.liquidityState ?? seed?.liquidity_state)
  if (Object.values(SOURCE_CATALOG_LIQUIDITY_STATES).includes(explicit)) {
    return explicit
  }
  const volume7d = toFiniteOrNull(seed?.marketVolume7d ?? seed?.volume_7d)
  const liquidityRank = toFiniteOrNull(seed?.liquidityRank ?? seed?.liquidity_rank) ?? 0
  if (volume7d != null) return SOURCE_CATALOG_LIQUIDITY_STATES.READY
  return liquidityRank > 0
    ? SOURCE_CATALOG_LIQUIDITY_STATES.PARTIAL
    : SOURCE_CATALOG_LIQUIDITY_STATES.MISSING
}

function resolveSeedCoverageDiagnosticState(seed = {}) {
  const explicit = normalizeLowerText(seed?.coverageState ?? seed?.coverage_state)
  if (Object.values(SOURCE_CATALOG_COVERAGE_STATES).includes(explicit)) {
    return explicit
  }
  const coverageCount = Math.max(Number(seed?.marketCoverageCount || 0), 0)
  if (coverageCount <= 0) return SOURCE_CATALOG_COVERAGE_STATES.MISSING
  if (coverageCount < MIN_MARKET_COVERAGE) {
    return SOURCE_CATALOG_COVERAGE_STATES.INSUFFICIENT
  }
  return SOURCE_CATALOG_COVERAGE_STATES.READY
}

function resolveSeedProgressionStatus(seed = {}, candidateStatus = resolveSeedCandidateStatus(seed)) {
  const explicit = normalizeLowerText(seed?.progressionStatus ?? seed?.progression_status)
  if (Object.values(SOURCE_CATALOG_PROGRESSION_STATUS).includes(explicit)) {
    return explicit
  }
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED) {
    return SOURCE_CATALOG_PROGRESSION_STATUS.REJECTED
  }
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE) {
    return SOURCE_CATALOG_PROGRESSION_STATUS.ELIGIBLE
  }
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) {
    return SOURCE_CATALOG_PROGRESSION_STATUS.BLOCKED_ELIGIBLE
  }
  return SOURCE_CATALOG_PROGRESSION_STATUS.BLOCKED_NEAR_ELIGIBLE
}

function resolveSeedProgressionBlockers(seed = {}) {
  return normalizeTextList(seed?.progressionBlockers ?? seed?.progression_blockers)
}

function hasUsableRiskyReferenceSeed(seed = {}) {
  const referenceState = resolveSeedReferenceDiagnosticState(seed)
  const referencePrice = toPositiveOrNull(seed?.referencePrice ?? seed?.reference_price)
  return referencePrice != null && referenceState !== SOURCE_CATALOG_REFERENCE_STATES.MISSING
}

function hasBorderlineStaleSnapshotForRiskySeed(
  seed = {},
  freshness = {},
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  if (resolveSeedSnapshotDiagnosticState(seed) !== SOURCE_CATALOG_SNAPSHOT_STATES.STALE) {
    return false
  }
  if (normalizeFreshnessState(freshness?.quoteState) === FRESHNESS_STATES.STALE) {
    return false
  }
  const snapshotAgeMinutes = resolveSnapshotAgeMinutes(seed)
  const rules = getCategoryStaleRules(itemCategory)
  const agingMaxMinutes = Math.max(Number(rules?.agingMaxMinutes || 0), 1)
  const borderlineMaxMinutes = Math.max(Math.round(agingMaxMinutes * 1.5), agingMaxMinutes + 20, 60)
  return snapshotAgeMinutes != null && snapshotAgeMinutes <= borderlineMaxMinutes
}

function hasOpportunitySeedRiskProfileBlock(seed = {}, progressionBlockers = []) {
  const candidateStatus = resolveSeedCandidateStatus(seed)
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.REJECTED) {
    return true
  }
  if (
    progressionBlockers.includes("anti_fake_guard") ||
    progressionBlockers.includes("structural_reason")
  ) {
    return true
  }
  const invalidReason = normalizeLowerText(seed?.invalidReason ?? seed?.invalid_reason)
  if (invalidReason) {
    return true
  }
  const eligibilityReason = normalizeLowerText(seed?.eligibilityReason ?? seed?.eligibility_reason)
  return /(rejected|hard|outofscope|namepattern|unsupported)/.test(eligibilityReason)
}

function evaluateNearEligibleRiskyScanReadiness(seed = {}) {
  const itemCategory = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
  const candidateStatus = resolveSeedCandidateStatus(seed)
  const maturityState = normalizeMaturityState(seed?.maturityState)
  const progressionStatus = resolveSeedProgressionStatus(seed, candidateStatus)
  const progressionBlockers = resolveSeedProgressionBlockers(seed)
  const coverageCount = Math.max(Number(seed?.marketCoverageCount || 0), 0)
  const snapshotState = resolveSeedSnapshotDiagnosticState(seed)
  const referenceState = resolveSeedReferenceDiagnosticState(seed)
  const liquidityState = resolveSeedLiquidityDiagnosticState(seed)
  const freshness = resolveCatalogSeedFreshnessContext(seed, itemCategory)
  const riskProfileBlocked = hasOpportunitySeedRiskProfileBlock(seed, progressionBlockers)
  const snapshotCapturedAt = normalizeTextValue(seed?.snapshotCapturedAt ?? seed?.snapshot_captured_at)
  const hasCoverageForRisky = coverageCount >= 1
  const isRiskyLaneCandidate =
    (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
      candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) &&
    (maturityState === MATURITY_STATES.NEAR_ELIGIBLE || maturityState === MATURITY_STATES.ELIGIBLE) &&
    progressionStatus !== SOURCE_CATALOG_PROGRESSION_STATUS.REJECTED
  const hasSnapshotPresence =
    Boolean(snapshotCapturedAt) && snapshotState !== SOURCE_CATALOG_SNAPSHOT_STATES.MISSING
  const borderlineStaleSnapshot = hasBorderlineStaleSnapshotForRiskySeed(seed, freshness, itemCategory)
  const hasUsableSnapshot =
    snapshotState === SOURCE_CATALOG_SNAPSHOT_STATES.READY ||
    snapshotState === SOURCE_CATALOG_SNAPSHOT_STATES.PARTIAL ||
    borderlineStaleSnapshot
  const hasUsableReference = hasUsableRiskyReferenceSeed(seed)
  const hasLiquidityReadyOrPartial =
    liquidityState === SOURCE_CATALOG_LIQUIDITY_STATES.READY ||
    liquidityState === SOURCE_CATALOG_LIQUIDITY_STATES.PARTIAL

  const blockingReasons = []
  if (!isRiskyLaneCandidate) {
    blockingReasons.push("deferred_due_to_maturity")
  }
  if (riskProfileBlocked) {
    blockingReasons.push("deferred_due_to_risk_profile")
  }

  // Missing snapshot/reference/liquidity are non-fatal at admission time.
  // They are carried into risky scoring as penalties or risky classifications.
  const softReasons = []
  if (!hasSnapshotPresence) {
    softReasons.push("deferred_near_eligible_missing_snapshot")
  } else if (!hasUsableSnapshot || !freshness.usable) {
    softReasons.push("deferred_due_to_stale")
  }
  if (!hasCoverageForRisky) {
    softReasons.push("deferred_near_eligible_insufficient_coverage")
  }
  if (!hasUsableReference) {
    softReasons.push("deferred_near_eligible_missing_reference")
    softReasons.push("deferred_due_to_missing_reference")
  }

  const normalizedPreLiquidityReasons = normalizeTextList(softReasons)
  if (!hasLiquidityReadyOrPartial) {
    if (!normalizedPreLiquidityReasons.length) {
      softReasons.push("deferred_near_eligible_liquidity_only")
    }
    softReasons.push("deferred_due_to_missing_liquidity")
  }

  const normalizedBlockingReasons = normalizeTextList(blockingReasons)
  return {
    ready: normalizedBlockingReasons.length === 0,
    reasons: normalizedBlockingReasons,
    softReasons: normalizeTextList(softReasons),
    isRiskyScanReady: normalizedBlockingReasons.length === 0,
    borderlineStaleSnapshot,
    hasUsableSnapshot,
    hasUsableReference,
    hasLiquidityReadyOrPartial,
    snapshotState,
    referenceState,
    liquidityState,
    progressionStatus,
    progressionBlockers
  }
}

function evaluateOpportunitySeedAdmission(seed = {}) {
  const itemCategory = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
  const candidateStatus = resolveSeedCandidateStatus(seed)
  const maturityState = normalizeMaturityState(seed?.maturityState)
  const progressionStatus = resolveSeedProgressionStatus(seed, candidateStatus)
  const progressionBlockers = resolveSeedProgressionBlockers(seed)
  const coverageCount = Math.max(Number(seed?.marketCoverageCount || 0), 0)
  const snapshotState = resolveSeedSnapshotDiagnosticState(seed)
  const referenceState = resolveSeedReferenceDiagnosticState(seed)
  const liquidityState = resolveSeedLiquidityDiagnosticState(seed)
  const coverageState = resolveSeedCoverageDiagnosticState(seed)
  const freshness = resolveCatalogSeedFreshnessContext(seed, itemCategory)
  const riskProfileBlocked = hasOpportunitySeedRiskProfileBlock(seed, progressionBlockers)
  const catalogApproved =
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
    candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
  const maturityApproved =
    maturityState === MATURITY_STATES.ELIGIBLE || maturityState === MATURITY_STATES.NEAR_ELIGIBLE
  const strictReady =
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    maturityState === MATURITY_STATES.ELIGIBLE &&
    freshness.usable &&
    !riskProfileBlocked
  const riskyScanReadiness = evaluateNearEligibleRiskyScanReadiness(seed)

  const reasons = []
  if (!catalogApproved || !maturityApproved) {
    reasons.push("deferred_due_to_maturity")
  }
  if (riskProfileBlocked) {
    reasons.push("deferred_due_to_risk_profile")
  }
  if (!freshness.usable) {
    reasons.push("deferred_due_to_stale")
  }
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE && referenceState === SOURCE_CATALOG_REFERENCE_STATES.MISSING) {
    reasons.push("deferred_due_to_missing_reference")
  }
  if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    snapshotState === SOURCE_CATALOG_SNAPSHOT_STATES.MISSING
  ) {
    reasons.push("deferred_due_to_maturity")
  }
  if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    liquidityState === SOURCE_CATALOG_LIQUIDITY_STATES.MISSING
  ) {
    reasons.push("deferred_due_to_missing_liquidity")
  }
  if (
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
    coverageCount < MIN_MARKET_COVERAGE
  ) {
    reasons.push("deferred_due_to_maturity")
  }
  if (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) {
    reasons.push(...riskyScanReadiness.reasons)
  } else if (
    candidateStatus !== CATALOG_CANDIDATE_STATUS.ELIGIBLE &&
    itemCategory !== ITEM_CATEGORIES.WEAPON_SKIN &&
    coverageState !== SOURCE_CATALOG_COVERAGE_STATES.READY
  ) {
    reasons.push("deferred_due_to_maturity")
  }

  const normalizedReasons = strictReady
    ? []
    : riskyScanReadiness.ready
      ? []
      : normalizeTextList(reasons)

  return {
    ready: strictReady || riskyScanReadiness.ready,
    reasons: normalizedReasons,
    isStrictExecutionReady: strictReady,
    isRiskyScanReady: riskyScanReadiness.ready,
    executionLane: strictReady
      ? "strict_eligible"
      : riskyScanReadiness.ready
        ? "near_eligible_risky"
        : "blocked",
    candidateStatus,
    maturityState,
    snapshotState,
    referenceState,
    liquidityState,
    coverageState,
    progressionStatus,
    progressionBlockers,
    riskyScanReadiness
  }
}

function summarizeOpportunitySeedAdmissions(admissions = [], executedSeeds = []) {
  const summary = buildOpportunityAdmissionDiagnostics()
  summary.universe_total = Array.isArray(admissions) ? admissions.length : 0
  summary.scan_candidates_executed = Array.isArray(executedSeeds) ? executedSeeds.length : 0

  for (const entry of Array.isArray(admissions) ? admissions : []) {
    const seed = entry?.row || {}
    const admission = entry?.admission || evaluateOpportunitySeedAdmission(seed)
    const candidateStatus = resolveSeedCandidateStatus(seed)
    if (candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE) {
      summary.universe_eligible += 1
    } else if (candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) {
      summary.universe_near_eligible += 1
      summary.near_eligible_total += 1
      if (Boolean(admission?.isRiskyScanReady)) {
        summary.near_eligible_scanable += 1
        summary.risky_scan_ready_loaded += 1
      } else {
        summary.near_eligible_blocked += 1
      }
    } else {
      summary.universe_blocked += 1
    }

    if (admission.ready) {
      summary.scan_candidates_loaded += 1
      if (Boolean(admission?.isStrictExecutionReady)) {
        summary.scan_candidates_from_strict_eligible += 1
      } else if (Boolean(admission?.isRiskyScanReady)) {
        summary.scan_candidates_from_risky_ready += 1
      }
      continue
    }

    summary.scan_candidates_deferred += 1
    for (const reason of normalizeTextList(admission?.reasons)) {
      if (summary[reason] == null) {
        summary[reason] = 0
      }
      summary[reason] += 1
      if (
        reason === "deferred_due_to_missing_snapshot" ||
        reason === "deferred_near_eligible_missing_snapshot"
      ) {
        summary.deferred_due_to_missing_snapshot += 1
      }
      if (
        reason === "deferred_due_to_feed_visibility" ||
        reason === "deferred_due_to_visibility_or_feed_floor"
      ) {
        summary.deferred_due_to_feed_visibility += 1
      }
    }
  }

  for (const seed of Array.isArray(executedSeeds) ? executedSeeds : []) {
    const itemCategory = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
    if (itemCategory === ITEM_CATEGORIES.WEAPON_SKIN) {
      summary.skins_scanned_count += 1
    } else if (itemCategory === ITEM_CATEGORIES.CASE) {
      summary.cases_scanned_count += 1
    } else if (itemCategory === ITEM_CATEGORIES.STICKER_CAPSULE) {
      summary.capsules_scanned_count += 1
    }

    const admission = evaluateOpportunitySeedAdmission(seed)
    if (Boolean(admission?.isStrictExecutionReady)) {
      summary.executed_from_strict_eligible += 1
    }
    if (Boolean(admission?.isRiskyScanReady)) {
      summary.executed_from_near_eligible += 1
      summary.risky_scan_ready_executed += 1
    }
  }

  summary.universe_loaded_for_scan = Number(summary.scan_candidates_loaded || 0)
  summary.universe_deferred_before_scan = Number(summary.scan_candidates_deferred || 0)
  if (summary.deferred_due_to_missing_snapshot <= 0) {
    summary.deferred_due_to_missing_snapshot = Number(summary.deferred_near_eligible_missing_snapshot || 0)
  }
  if (summary.deferred_due_to_feed_visibility <= 0) {
    summary.deferred_due_to_feed_visibility = Number(
      summary.deferred_due_to_visibility_or_feed_floor || 0
    )
  }

  return summary
}

function dedupeSeedRows(rows = []) {
  const seen = new Set()
  const deduped = []
  for (const row of Array.isArray(rows) ? rows : []) {
    const key = normalizeMarketHashName(row?.marketHashName)
    if (!key || seen.has(key)) continue
    seen.add(key)
    deduped.push(row)
  }
  return deduped
}

function rankLayerRows(rows = []) {
  return dedupeSeedRows(rows).sort(
    (a, b) =>
      Number(b.layerPriority || 0) - Number(a.layerPriority || 0) ||
      Number(b.maturityScore || 0) - Number(a.maturityScore || 0) ||
      Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
      sourceRank(b.universeSource) - sourceRank(a.universeSource)
  )
}

function selectSeedsForLayeredScanning(seeds = [], options = {}) {
  const ranked = rankLayerRows(seeds)
  const opportunityFilter =
    typeof options?.opportunityFilter === "function" ? options.opportunityFilter : null
  const opportunityCandidates = opportunityFilter ? ranked.filter(opportunityFilter) : ranked
  const deferredOpportunityRows =
    Array.isArray(options?.opportunityDeferredRows) && options.opportunityDeferredRows.length
      ? rankLayerRows(options.opportunityDeferredRows)
      : opportunityFilter
        ? ranked.filter((row) => !opportunityFilter(row))
        : []
  const byLayer = {
    [SCAN_LAYERS.HOT]: opportunityCandidates.filter((row) => row.scanLayer === SCAN_LAYERS.HOT),
    [SCAN_LAYERS.WARM]: opportunityCandidates.filter((row) => row.scanLayer === SCAN_LAYERS.WARM),
    [SCAN_LAYERS.COLD]: opportunityCandidates.filter((row) => row.scanLayer === SCAN_LAYERS.COLD)
  }
  const allByLayer = {
    [SCAN_LAYERS.HOT]: ranked.filter((row) => row.scanLayer === SCAN_LAYERS.HOT),
    [SCAN_LAYERS.WARM]: ranked.filter((row) => row.scanLayer === SCAN_LAYERS.WARM),
    [SCAN_LAYERS.COLD]: ranked.filter((row) => row.scanLayer === SCAN_LAYERS.COLD)
  }
  const opportunityTarget = Math.max(
    Math.min(
      Number(options?.opportunityTarget || HOT_OPPORTUNITY_SCAN_TARGET),
      PRE_COMPARE_UNIVERSE_LIMIT
    ),
    1
  )
  const enrichmentTarget = Math.max(
    Math.min(
      Number(options?.enrichmentTarget || ENRICHMENT_ONLY_TARGET),
      PRE_COMPARE_UNIVERSE_LIMIT
    ),
    0
  )
  const nearEligibleTarget = Math.max(
    Math.min(
      Number(options?.nearEligibleTarget || OPPORTUNITY_NEAR_ELIGIBLE_LIMIT),
      opportunityTarget
    ),
    0
  )
  const hotTarget = Math.max(
    Math.min(
      Number(options?.hotTarget || Math.max(opportunityTarget - nearEligibleTarget, 0)),
      opportunityTarget
    ),
    0
  )
  const coreSeeds = byLayer[SCAN_LAYERS.HOT].slice(0, Math.min(HIGH_YIELD_CORE_TARGET, hotTarget))
  const selectedNames = new Set(coreSeeds.map((row) => normalizeMarketHashName(row?.marketHashName)))
  const opportunitySeeds = [...coreSeeds]
  const nearEligibleWarmSeeds = byLayer[SCAN_LAYERS.WARM].filter(
    (row) => normalizeMaturityState(row?.maturityState) === MATURITY_STATES.NEAR_ELIGIBLE
  )
  const enrichingWarmSeeds = byLayer[SCAN_LAYERS.WARM].filter(
    (row) => normalizeMaturityState(row?.maturityState) !== MATURITY_STATES.NEAR_ELIGIBLE
  )
  const hotUniverse = rankLayerRows([...byLayer[SCAN_LAYERS.HOT], ...nearEligibleWarmSeeds])
  const enrichingOpportunityBackfillLimit = Math.max(Math.round(opportunityTarget * 0.2), 8)

  const addFromLayer = (rows = [], limit = 0) => {
    const maxItems = Math.max(Number(limit || 0), 0)
    if (!maxItems) return 0
    let added = 0
    for (const row of rows) {
      if (added >= maxItems || opportunitySeeds.length >= opportunityTarget) break
      const key = normalizeMarketHashName(row?.marketHashName)
      if (!key || selectedNames.has(key)) continue
      selectedNames.add(key)
      opportunitySeeds.push(row)
      added += 1
    }
    return added
  }

  const existingCore = coreSeeds.length
  addFromLayer(byLayer[SCAN_LAYERS.HOT], Math.max(hotTarget - existingCore, 0))
  addFromLayer(nearEligibleWarmSeeds, nearEligibleTarget)
  addFromLayer(
    enrichingWarmSeeds,
    Math.min(
      Math.max(opportunityTarget - opportunitySeeds.length, 0),
      enrichingOpportunityBackfillLimit
    )
  )

  const enrichmentSeeds = []
  const enrichmentSeen = new Set()
  const allNearEligibleWarmSeeds = allByLayer[SCAN_LAYERS.WARM].filter(
    (row) => normalizeMaturityState(row?.maturityState) === MATURITY_STATES.NEAR_ELIGIBLE
  )
  const allEnrichingWarmSeeds = allByLayer[SCAN_LAYERS.WARM].filter(
    (row) => normalizeMaturityState(row?.maturityState) !== MATURITY_STATES.NEAR_ELIGIBLE
  )
  const enrichmentCandidates = rankLayerRows([
    ...allNearEligibleWarmSeeds,
    ...allEnrichingWarmSeeds,
    ...allByLayer[SCAN_LAYERS.COLD]
  ])
  for (const row of enrichmentCandidates) {
    if (enrichmentSeeds.length >= enrichmentTarget) break
    const key = normalizeMarketHashName(row?.marketHashName)
    if (!key || selectedNames.has(key) || enrichmentSeen.has(key)) continue
    enrichmentSeen.add(key)
    enrichmentSeeds.push(row)
  }

  const layerDiagnostics = buildLayerDiagnostics(ranked)
  const opportunityDiagnostics = buildLayerDiagnostics(opportunitySeeds)
  const enrichmentDiagnostics = buildLayerDiagnostics(enrichmentSeeds)
  const deferredToEnrichmentByCategory = countRowsByCategory(
    deferredOpportunityRows.map((row) => ({
      itemCategory: row?.itemCategory,
      itemName: row?.marketHashName
    }))
  )

  return {
    allRankedSeeds: ranked,
    coreSeeds,
    opportunitySeeds,
    enrichmentSeeds,
    diagnostics: {
      totalRankedSeeds: ranked.length,
      coreUniverseSize: coreSeeds.length,
      opportunityTarget,
      hotTarget,
      nearEligibleTarget,
      enrichingOpportunityBackfillLimit,
      enrichmentTarget,
      matureOnlyOpportunitySelection: Boolean(opportunityFilter),
      opportunityCandidatePoolSize: opportunityCandidates.length,
      selectedForOpportunity: opportunitySeeds.length,
      selectedForEnrichment: enrichmentSeeds.length,
      matureOpportunityShortfall: Math.max(opportunityTarget - opportunitySeeds.length, 0),
      deferredToEnrichmentItems: deferredOpportunityRows.length,
      deferredToEnrichmentByCategory: toScannerAuditCategoryCounts(
        deferredToEnrichmentByCategory
      ),
      selectedEligibleForOpportunity: opportunitySeeds.filter(
        (row) => normalizeMaturityState(row?.maturityState) === MATURITY_STATES.ELIGIBLE
      ).length,
      selectedNearEligibleForOpportunity: opportunitySeeds.filter(
        (row) => normalizeMaturityState(row?.maturityState) === MATURITY_STATES.NEAR_ELIGIBLE
      ).length,
      selectedEnrichingForOpportunity: opportunitySeeds.filter(
        (row) => normalizeMaturityState(row?.maturityState) === MATURITY_STATES.ENRICHING
      ).length,
      hotUniverse: {
        total: hotUniverse.length,
        eligibleCount: byLayer[SCAN_LAYERS.HOT].length,
        nearEligibleCount: nearEligibleWarmSeeds.length,
        itemsByCategory: countRowsByCategory(
          hotUniverse.map((row) => ({
            itemCategory: row?.itemCategory,
            itemName: row?.marketHashName
          }))
        ),
        selectedByCategory: countRowsByCategory(
          opportunitySeeds.map((row) => ({
            itemCategory: row?.itemCategory,
            itemName: row?.marketHashName
          }))
        )
      },
      allSeeds: layerDiagnostics,
      opportunity: opportunityDiagnostics,
      enrichment: enrichmentDiagnostics
    }
  }
}

function dedupeCatalogRowsByMarketHashName(rows = []) {
  const seen = new Set()
  const deduped = []
  for (const row of Array.isArray(rows) ? rows : []) {
    const marketHashName = normalizeMarketHashName(
      row?.market_hash_name || row?.marketHashName || row?.item_name || row?.itemName
    )
    if (!marketHashName || seen.has(marketHashName)) continue
    seen.add(marketHashName)
    deduped.push({
      ...row,
      market_hash_name: marketHashName
    })
  }
  return deduped
}

function compareCatalogSeedRows(a = {}, b = {}) {
  const candidatePriority = (row = {}) => {
    const status = normalizeCatalogCandidateStatus(row?.candidate_status ?? row?.candidateStatus)
    if (status === CATALOG_CANDIDATE_STATUS.ELIGIBLE) return 4
    if (status === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE) return 3
    if (status === CATALOG_CANDIDATE_STATUS.ENRICHING) return 2
    return 1
  }
  return (
    candidatePriority(b) - candidatePriority(a) ||
    Number(toFiniteOrNull(b?.maturity_score ?? b?.maturityScore) ?? 0) -
      Number(toFiniteOrNull(a?.maturity_score ?? a?.maturityScore) ?? 0) ||
    Number(toFiniteOrNull(b?.enrichment_priority ?? b?.enrichmentPriority) ?? 0) -
      Number(toFiniteOrNull(a?.enrichment_priority ?? a?.enrichmentPriority) ?? 0) ||
    Number(toFiniteOrNull(b?.market_coverage_count ?? b?.marketCoverageCount) ?? 0) -
      Number(toFiniteOrNull(a?.market_coverage_count ?? a?.marketCoverageCount) ?? 0) ||
    Number(toFiniteOrNull(b?.liquidity_rank ?? b?.liquidityRank) ?? 0) -
      Number(toFiniteOrNull(a?.liquidity_rank ?? a?.liquidityRank) ?? 0) ||
    Number(toFiniteOrNull(b?.volume_7d ?? b?.volume7d) ?? 0) -
      Number(toFiniteOrNull(a?.volume_7d ?? a?.volume7d) ?? 0) ||
    Number(toFiniteOrNull(b?.reference_price ?? b?.referencePrice) ?? 0) -
      Number(toFiniteOrNull(a?.reference_price ?? a?.referencePrice) ?? 0)
  )
}

async function buildSeedsFromCatalogRows(rows = [], options = {}) {
  const normalizedRows = dedupeCatalogRowsByMarketHashName(rows)
    .sort(compareCatalogSeedRows)
    .slice(0, Math.max(Number(options?.limit || rows.length || 0), 0))
  if (!normalizedRows.length) {
    return []
  }

  const marketNames = normalizedRows.map((row) => row.market_hash_name)
  const skins = await ensureSkinsForMarketNames(marketNames)
  const skinsByName = toByNameMap(
    (Array.isArray(skins) ? skins : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )
  const skinIds = (Array.isArray(skins) ? skins : [])
    .map((row) => Number(row?.id || 0))
    .filter((value) => Number.isInteger(value) && value > 0)
  const snapshotsBySkinId = skinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(skinIds)
    : {}

  return normalizedRows
    .map((row) => {
      const marketHashName = normalizeMarketHashName(row?.market_hash_name || row?.marketHashName)
      const skin = skinsByName[marketHashName] || null
      return buildInputItemFromSkinAndSnapshot({
        skin,
        snapshot:
          Number(skin?.id || 0) > 0 ? snapshotsBySkinId[Number(skin.id)] || null : null,
        marketHashName,
        category: row?.category,
        subcategory: row?.subcategory,
        catalogRow: row,
        universeSource: resolveCatalogSeedSource(row?.candidate_status ?? row?.candidateStatus),
        liquidityRank: toFiniteOrNull(row?.liquidity_rank ?? row?.liquidityRank)
      })
    })
    .filter(Boolean)
}

async function loadMatureCatalogUniverseSeeds(limit = HOT_MATURE_POOL_LIMIT) {
  const safeLimit = Math.max(Math.round(Number(limit || HOT_MATURE_POOL_LIMIT)), 0)
  if (!safeLimit) return []

  const categories = [
    ITEM_CATEGORIES.WEAPON_SKIN,
    ITEM_CATEGORIES.CASE,
    ITEM_CATEGORIES.STICKER_CAPSULE
  ]
  let eligibleRows = []
  let candidateRows = []
  try {
    ;[eligibleRows, candidateRows] = await Promise.all([
      marketSourceCatalogRepo.listScanEligible({
        limit: safeLimit,
        categories
      }),
      marketSourceCatalogRepo.listCandidatePool({
        limit: safeLimit,
        categories,
        candidateStatuses: [
          CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE,
          CATALOG_CANDIDATE_STATUS.ENRICHING
        ]
      })
    ])
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to load mature source catalog seeds", err.message)
    return []
  }

  const seeds = await buildSeedsFromCatalogRows([...(eligibleRows || []), ...(candidateRows || [])], {
    limit: safeLimit
  })
  return seeds.filter(isMinimumOpportunityBackfillReadySeed)
}

function normalizeFeedEventType(value) {
  const text = String(value || "")
    .trim()
    .toLowerCase()
  if (
    text === "new" ||
    text === "updated" ||
    text === "reactivated" ||
    text === "duplicate"
  ) {
    return text
  }
  return ""
}

function computeSignalHistoryScore(signalHistory = {}) {
  const totalEvents = Math.max(Number(signalHistory?.totalEvents || 0), 0)
  const activeEvents = Math.max(Number(signalHistory?.activeEvents || 0), 0)
  const updatedEvents = Math.max(Number(signalHistory?.updatedEvents || 0), 0)
  const reactivatedEvents = Math.max(Number(signalHistory?.reactivatedEvents || 0), 0)
  const lastDetectedAtMs = parseIsoTimestampMs(signalHistory?.lastDetectedAt)
  let recencyBoost = 0
  if (lastDetectedAtMs != null) {
    const ageHours = Math.max((Date.now() - lastDetectedAtMs) / (60 * 60 * 1000), 0)
    if (ageHours <= 6) recencyBoost = 10
    else if (ageHours <= 24) recencyBoost = 6
    else if (ageHours <= 72) recencyBoost = 3
  }

  return round2(
    Math.min(totalEvents * 2.5, 10) +
      Math.min(activeEvents * 4, 10) +
      Math.min(updatedEvents * 2, 6) +
      Math.min(reactivatedEvents * 2.5, 6) +
      recencyBoost
  )
}

function buildSignalHistoryByItem(rows = []) {
  const byItem = {}
  for (const row of Array.isArray(rows) ? rows : []) {
    const itemName = normalizeMarketHashName(row?.item_name || row?.itemName)
    if (!itemName) continue
    if (!byItem[itemName]) {
      byItem[itemName] = {
        totalEvents: 0,
        activeEvents: 0,
        updatedEvents: 0,
        reactivatedEvents: 0,
        duplicateEvents: 0,
        lastDetectedAt: null
      }
    }
    const bucket = byItem[itemName]
    const detectedAt = row?.detected_at || row?.detectedAt || null
    if (detectedAt && (!bucket.lastDetectedAt || detectedAt > bucket.lastDetectedAt)) {
      bucket.lastDetectedAt = detectedAt
    }
    if (Boolean(row?.is_active)) {
      bucket.activeEvents += 1
    }
    if (Boolean(row?.is_duplicate)) {
      bucket.duplicateEvents += 1
      continue
    }
    bucket.totalEvents += 1
    const eventType = normalizeFeedEventType(row?.metadata?.event_type)
    if (eventType === "updated") {
      bucket.updatedEvents += 1
    } else if (eventType === "reactivated") {
      bucket.reactivatedEvents += 1
    }
  }

  return Object.fromEntries(
    Object.entries(byItem).map(([itemName, signalHistory]) => [
      itemName,
      {
        ...signalHistory,
        score: computeSignalHistoryScore(signalHistory)
      }
    ])
  )
}

async function loadFeedSignalHistoryForItems(itemNames = []) {
  const names = Array.from(
    new Set((Array.isArray(itemNames) ? itemNames : []).map((value) => normalizeMarketHashName(value)).filter(Boolean))
  )
  if (!names.length) {
    return {}
  }

  const sinceIso = new Date(
    Date.now() - SIGNAL_HISTORY_LOOKBACK_HOURS * 60 * 60 * 1000
  ).toISOString()
  try {
    const recentRows = await arbitrageFeedRepo.getRecentRowsByItems({
      itemNames: names,
      sinceIso,
      limit: Math.min(Math.max(names.length * 6, 2000), 10000)
    })
    return buildSignalHistoryByItem(recentRows)
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to load recent signal history", err.message)
    return {}
  }
}

async function loadCuratedUniverseSeeds() {
  let curatedRows = []
  try {
    curatedRows = await marketUniverseRepo.listActiveByLiquidityRank({
      limit: UNIVERSE_DB_LIMIT,
      categories: [
        ITEM_CATEGORIES.WEAPON_SKIN,
        ITEM_CATEGORIES.CASE,
        ITEM_CATEGORIES.STICKER_CAPSULE
      ]
    })
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to load curated market universe", err.message)
    return []
  }

  if (!Array.isArray(curatedRows) || !curatedRows.length) {
    return []
  }

  const normalizedRows = curatedRows
    .map((row) => {
      const marketHashName = normalizeMarketHashName(
        row?.market_hash_name || row?.marketHashName || row?.item_name || row?.itemName
      )
      if (!marketHashName) return null
      return {
        marketHashName,
        category: normalizeItemCategory(row?.category, marketHashName),
        subcategory: String(row?.subcategory || "").trim() || null,
        liquidityRank: toFiniteOrNull(row?.liquidity_rank || row?.liquidityRank)
      }
    })
    .filter(Boolean)

  if (!normalizedRows.length) {
    return []
  }

  let catalogRows = []
  try {
    catalogRows = await marketSourceCatalogService.getCatalogRowsByMarketHashNames(
      normalizedRows.map((row) => row.marketHashName),
      {
        categories: [
          ITEM_CATEGORIES.WEAPON_SKIN,
          ITEM_CATEGORIES.CASE,
          ITEM_CATEGORIES.STICKER_CAPSULE
        ]
      }
    )
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to load source catalog metadata for curated seeds", err.message)
    catalogRows = []
  }
  const catalogByName = normalizeCatalogRowsByMarketHashName(catalogRows)

  const skins = await ensureSkinsForMarketNames(normalizedRows.map((row) => row.marketHashName))
  const skinsByName = toByNameMap(
    (Array.isArray(skins) ? skins : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const skinIds = (Array.isArray(skins) ? skins : [])
    .map((row) => Number(row?.id || 0))
    .filter((value) => Number.isInteger(value) && value > 0)
  const snapshotsBySkinId = skinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(skinIds)
    : {}

  return normalizedRows
    .map((row) =>
      buildInputItemFromSkinAndSnapshot({
        skin: skinsByName[row.marketHashName] || null,
        snapshot:
          Number(skinsByName[row.marketHashName]?.id || 0) > 0
            ? snapshotsBySkinId[Number(skinsByName[row.marketHashName].id)] || null
            : null,
        marketHashName: row.marketHashName,
        category: row.category,
        subcategory: row.subcategory,
        catalogRow: catalogByName[row.marketHashName] || null,
        universeSource: "curated_db",
        liquidityRank: row.liquidityRank
      })
    )
    .filter(Boolean)
}

async function loadFallbackUniverseSeeds() {
  const marketNames = FALLBACK_UNIVERSE.map((entry) => entry.marketHashName)
  let catalogRows = []
  try {
    catalogRows = await marketSourceCatalogService.getCatalogRowsByMarketHashNames(marketNames, {
      categories: [
        ITEM_CATEGORIES.WEAPON_SKIN,
        ITEM_CATEGORIES.CASE,
        ITEM_CATEGORIES.STICKER_CAPSULE
      ]
    })
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to load source catalog metadata for fallback seeds", err.message)
    catalogRows = []
  }
  const catalogByName = normalizeCatalogRowsByMarketHashName(catalogRows)
  const skins = await ensureSkinsForMarketNames(marketNames)
  const skinsByName = toByNameMap(
    (Array.isArray(skins) ? skins : []).map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const skinIds = (Array.isArray(skins) ? skins : [])
    .map((row) => Number(row?.id || 0))
    .filter((value) => Number.isInteger(value) && value > 0)
  const snapshotsBySkinId = skinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(skinIds)
    : {}

  return FALLBACK_UNIVERSE.map((entry, index) =>
    buildInputItemFromSkinAndSnapshot({
      skin: skinsByName[entry.marketHashName] || null,
      snapshot:
        Number(skinsByName[entry.marketHashName]?.id || 0) > 0
          ? snapshotsBySkinId[Number(skinsByName[entry.marketHashName].id)] || null
          : null,
      marketHashName: entry.marketHashName,
      category: entry.category,
      subcategory: entry.subcategory,
      catalogRow: catalogByName[entry.marketHashName] || null,
      universeSource: "fallback_curated",
      liquidityRank: index + 1
    })
  ).filter(Boolean)
}

function passesUniverseSeedFilters(
  inputItem = {},
  discardStats = {},
  rejectedByItem = null,
  options = {}
) {
  const allowMissingSnapshotData = Boolean(options?.allowMissingSnapshotData)
  const weaponSkinDiagnostics =
    options?.weaponSkinDiagnostics && typeof options.weaponSkinDiagnostics === "object"
      ? options.weaponSkinDiagnostics
      : null
  const marketHashName = String(inputItem?.marketHashName || "").trim()
  const itemCategory = normalizeItemCategory(inputItem?.itemCategory, marketHashName)
  const candidateStatus = resolveSeedCandidateStatus(inputItem)
  const hasCatalogMatureStatus =
    candidateStatus === CATALOG_CANDIDATE_STATUS.ELIGIBLE ||
    candidateStatus === CATALOG_CANDIDATE_STATUS.NEAR_ELIGIBLE
  const hasCoverageSignal = Math.max(Number(inputItem?.marketCoverageCount || 0), 0) >= 1
  const hasReferenceSignal = toFiniteOrNull(inputItem?.referencePrice) != null
  if (!marketHashName) return false
  const isWeaponSkin = itemCategory === ITEM_CATEGORIES.WEAPON_SKIN
  const weaponSkinFilter = isWeaponSkin ? evaluateWeaponSkinSeedFilters(inputItem) : null

  if (isLowValueJunkName(marketHashName) && !isWeaponSkin) {
    incrementReasonCounter(discardStats, "ignored_low_value_universe", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        marketHashName,
        "ignored_low_value_universe",
        itemCategory
      )
    }
    return false
  }

  if (
    isWeaponSkin &&
    !hasLiquidNameSignal(marketHashName) &&
    !HIGH_SIGNAL_WEAPON_PREFIXES.has(extractWeaponPrefix(marketHashName)) &&
    weaponSkinFilter?.hardRejectLowValue
  ) {
    incrementReasonCounter(discardStats, "hard_reject_low_value", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        marketHashName,
        "hard_reject_low_value",
        itemCategory
      )
    }
    incrementWeaponSkinFilterDiagnostic(
      weaponSkinDiagnostics,
      "hard_reject_low_value",
      marketHashName
    )
    return false
  }

  if (!Boolean(inputItem?.hasSnapshotData)) {
    if (isWeaponSkin) {
      if (weaponSkinFilter?.hardRejectMissingLiquidity) {
        incrementReasonCounter(discardStats, "hard_reject_missing_liquidity", itemCategory)
        if (rejectedByItem) {
          incrementItemReasonCounter(
            rejectedByItem,
            marketHashName,
            "hard_reject_missing_liquidity",
            itemCategory
          )
        }
        incrementWeaponSkinFilterDiagnostic(
          weaponSkinDiagnostics,
          "hard_reject_missing_liquidity",
          marketHashName
        )
        return false
      }
      if (weaponSkinFilter?.penaltyMissingLiquidity) {
        incrementWeaponSkinFilterDiagnostic(
          weaponSkinDiagnostics,
          "penalty_missing_liquidity_allowed_forward",
          marketHashName
        )
      }
    } else {
      const hasHighSignalFallback = isHighSignalMissingSnapshotSeed(marketHashName, itemCategory)
      const allowMatureCatalogFallback =
        hasCatalogMatureStatus &&
        hasCoverageSignal &&
        (hasReferenceSignal || hasHighSignalFallback)
      if (!allowMissingSnapshotData && !allowMatureCatalogFallback) {
        incrementReasonCounter(discardStats, "ignored_missing_liquidity_data", itemCategory)
        if (rejectedByItem) {
          incrementItemReasonCounter(
            rejectedByItem,
            marketHashName,
            "ignored_missing_liquidity_data",
            itemCategory
          )
        }
        return false
      }
      if (allowMissingSnapshotData && !allowMatureCatalogFallback && !hasHighSignalFallback) {
        incrementReasonCounter(discardStats, "ignored_missing_liquidity_data", itemCategory)
        if (rejectedByItem) {
          incrementItemReasonCounter(
            rejectedByItem,
            marketHashName,
            "ignored_missing_liquidity_data",
            itemCategory
          )
        }
        return false
      }
    }
  }

  if (isWeaponSkin) {
    // The contextual low-value hard reject is handled above for low-signal skins.
    // Do not reapply it globally here, or mature skins get filtered before enrichment can help.
    if (weaponSkinFilter?.penaltyLowValue) {
      incrementWeaponSkinFilterDiagnostic(
        weaponSkinDiagnostics,
        "penalty_low_value_allowed_forward",
        marketHashName
      )
    }
    for (const key of weaponSkinFilter?.variantPenaltyKeys || []) {
      incrementWeaponSkinFilterDiagnostic(weaponSkinDiagnostics, key, marketHashName)
    }
  }

  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, itemCategory)
  if (isWeaponSkin) {
    const coverageCount = Math.max(Number(inputItem?.marketCoverageCount || 0), 0)
    if (weaponSkinFilter?.hardRejectStale && coverageCount <= 0) {
      incrementReasonCounter(discardStats, "ignored_stale_data", itemCategory)
      if (rejectedByItem) {
        incrementItemReasonCounter(rejectedByItem, marketHashName, "ignored_stale_data", itemCategory)
      }
      incrementWeaponSkinFilterDiagnostic(weaponSkinDiagnostics, "hard_reject_stale", marketHashName)
      return false
    }
    if (weaponSkinFilter?.hardRejectStale) {
      incrementWeaponSkinFilterDiagnostic(
        weaponSkinDiagnostics,
        "stale_penalty_allowed_forward",
        marketHashName
      )
    }
    if (weaponSkinFilter?.freshnessPenaltyKey) {
      incrementWeaponSkinFilterDiagnostic(
        weaponSkinDiagnostics,
        weaponSkinFilter.freshnessPenaltyKey,
        marketHashName
      )
    }
  }

  const referencePrice = toFiniteOrNull(inputItem?.referencePrice)
  const universePriceFloor = Number(
    UNIVERSE_MIN_PRICE_FLOOR_BY_CATEGORY[itemCategory] ?? UNIVERSE_MIN_PRICE_FLOOR_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
  if (!isWeaponSkin && referencePrice != null && referencePrice < universePriceFloor) {
    incrementReasonCounter(discardStats, "ignored_low_value_universe", itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        marketHashName,
        "ignored_low_value_universe",
        itemCategory
      )
    }
    return false
  }

  // Keep low-liquidity checks in post-computation risk policy so we can compare first.
  return true
}

function resolveLiquidityMetrics(opportunity = {}, inputItem = {}) {
  const liquiditySignal = opportunity?.antiFake?.liquidity || {}
  let volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
  let liquidityScore = toFiniteOrNull(inputItem?.liquidityScore)
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const outlier = opportunity?.antiFake?.outlier && typeof opportunity.antiFake.outlier === "object"
    ? opportunity.antiFake.outlier
    : {}
  const referenceDeviation =
    opportunity?.antiFake?.referenceDeviation && typeof opportunity.antiFake.referenceDeviation === "object"
      ? opportunity.antiFake.referenceDeviation
      : {}

  if (liquiditySignal?.signalType === "volume_7d") {
    volume7d = toFiniteOrNull(liquiditySignal?.signalValue)
  }
  if (liquiditySignal?.signalType === "liquidity_score") {
    liquidityScore = toFiniteOrNull(liquiditySignal?.signalValue)
  }

  return {
    volume7d,
    liquidityScore,
    hasMissingDepth: depthFlags.includes("MISSING_DEPTH"),
    hasOutlierAdjusted: depthFlags.some(
      (flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED"
    ),
    hasSuspiciousDepthGap: depthFlags.some(
      (flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS"
    ),
    hasExtremeDepthGap: depthFlags.some(
      (flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME"
    ),
    buyDepthMissing: Boolean(outlier?.buyDepthMissing),
    sellDepthMissing: Boolean(outlier?.sellDepthMissing),
    referenceDeviationRatio: toFiniteOrNull(referenceDeviation?.maxRatio),
    hasStrongReferenceDeviation:
      Boolean(referenceDeviation?.strong) ||
      (toFiniteOrNull(referenceDeviation?.maxRatio) ?? 0) > PREMIUM_REFERENCE_PENALTY_RATIO,
    hasExtremeReferenceDeviation:
      Boolean(referenceDeviation?.extreme) ||
      (toFiniteOrNull(referenceDeviation?.maxRatio) ?? 0) > PREMIUM_REFERENCE_REJECT_RATIO
  }
}

function passesScannerGuards(opportunity = {}, liquidity = {}) {
  const itemCategory = normalizeItemCategory(
    opportunity?.itemCategory || opportunity?.category,
    opportunity?.itemName || opportunity?.marketHashName
  )
  const rules = getCategoryScanRules(itemCategory, "strict")
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const hasMissingDepth = Boolean(liquidity?.hasMissingDepth)
  const hasBothDepthMissing = Boolean(liquidity?.buyDepthMissing) && Boolean(liquidity?.sellDepthMissing)
  const hasSuspiciousDepthGap = Boolean(liquidity?.hasSuspiciousDepthGap)
  const hasExtremeDepthGap = Boolean(liquidity?.hasExtremeDepthGap)
  const referenceSignals = resolveReferenceSignals(opportunity, liquidity)
  const isPremiumCategory = PREMIUM_ITEM_CATEGORIES.has(itemCategory)

  if (!opportunity?.isOpportunity) return false
  if (profit == null || profit <= 0) return false
  if (buyPrice == null || buyPrice < Number(rules.minPriceUsd || MIN_EXECUTION_PRICE_USD)) return false
  if (
    spread == null ||
    spread < Number(rules.minSpreadPercent || MIN_SPREAD_PERCENT) ||
    spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)
  ) {
    return false
  }
  if (!isPremiumCategory && (volume7d == null || volume7d < Number(rules.minVolume7d || MIN_VOLUME_7D))) {
    return false
  }
  if (isPremiumCategory) {
    if (volume7d != null && volume7d < PREMIUM_MIN_VOLUME_REJECT) return false
    if (volume7d == null && marketCoverage < PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) return false
    if (referenceSignals.hasExtremeReferenceDeviation) return false
    if (hasExtremeDepthGap) return false
  }
  if (marketCoverage < Number(rules.minMarketCoverage || MIN_MARKET_COVERAGE)) return false
  if (isPremiumCategory && (hasBothDepthMissing || hasMissingDepth || hasSuspiciousDepthGap)) return false

  return true
}

function getPrimaryHardRejectionReason(opportunity = {}) {
  const reasons = Array.isArray(opportunity?.antiFake?.reasons) ? opportunity.antiFake.reasons : []
  for (const reason of reasons) {
    if (HARD_REJECTION_REASONS.has(String(reason || "").trim())) {
      return String(reason || "").trim()
    }
  }
  return ""
}

function getCategoryScanRules(itemCategory, profileName = "strict") {
  const category = normalizeItemCategory(itemCategory)
  const categoryRules = CATEGORY_SCAN_RULES[category] || CATEGORY_SCAN_RULES[ITEM_CATEGORIES.WEAPON_SKIN]
  const normalizedProfileName = String(profileName || "")
    .trim()
    .toLowerCase()
  if (normalizedProfileName === "risky" || normalizedProfileName.startsWith("risky_")) {
    return categoryRules.risky
  }
  return categoryRules.strict
}

function getCategoryRiskyProfile(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    CATEGORY_RISKY_MODE_PROFILES[category] ||
    CATEGORY_RISKY_MODE_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function getLegacyCategoryRiskyProfile(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    LEGACY_CATEGORY_RISKY_MODE_PROFILES[category] ||
    LEGACY_CATEGORY_RISKY_MODE_PROFILES[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function getRiskyQualityFloor(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    RISKY_QUALITY_FLOOR_BY_CATEGORY[category] ||
    RISKY_QUALITY_FLOOR_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function getLegacyRiskyQualityFloor(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    LEGACY_RISKY_QUALITY_FLOOR_BY_CATEGORY[category] ||
    LEGACY_RISKY_QUALITY_FLOOR_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN]
  )
}

function getRiskyBorderlinePromotionRules(itemCategory) {
  const category = normalizeItemCategory(itemCategory)
  return (
    BORDERLINE_RISKY_PROMOTION_BY_CATEGORY[category] ||
    BORDERLINE_RISKY_PROMOTION_BY_CATEGORY[ITEM_CATEGORIES.WEAPON_SKIN] ||
    { enabled: false }
  )
}

function resolveDepthSignals(opportunity = {}, liquidity = {}) {
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const outlier = opportunity?.antiFake?.outlier && typeof opportunity.antiFake.outlier === "object"
    ? opportunity.antiFake.outlier
    : {}
  const depthQuality =
    opportunity?.antiFake?.depthQuality && typeof opportunity.antiFake.depthQuality === "object"
      ? opportunity.antiFake.depthQuality
      : {}
  const buyDepthMissing = Boolean(liquidity?.buyDepthMissing ?? outlier?.buyDepthMissing)
  const sellDepthMissing = Boolean(liquidity?.sellDepthMissing ?? outlier?.sellDepthMissing)
  const hasMissingDepth = Boolean(liquidity?.hasMissingDepth) || depthFlags.includes("MISSING_DEPTH")
  const hasOutlierAdjusted =
    Boolean(liquidity?.hasOutlierAdjusted) ||
    depthFlags.some((flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED")
  const hasSuspiciousDepthGap =
    Boolean(liquidity?.hasSuspiciousDepthGap) ||
    Boolean(depthQuality?.buyDepthGapSuspicious) ||
    Boolean(depthQuality?.sellDepthGapSuspicious) ||
    depthFlags.some((flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS")
  const hasExtremeDepthGap =
    Boolean(liquidity?.hasExtremeDepthGap) ||
    Boolean(depthQuality?.buyDepthGapExtreme) ||
    Boolean(depthQuality?.sellDepthGapExtreme) ||
    depthFlags.some((flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME")

  return {
    buyDepthMissing,
    sellDepthMissing,
    hasMissingDepth,
    hasOutlierAdjusted,
    hasSuspiciousDepthGap,
    hasExtremeDepthGap,
    hasBothDepthMissing: buyDepthMissing && sellDepthMissing
  }
}

function resolveReferenceSignals(opportunity = {}, liquidity = {}) {
  const referenceDeviation =
    opportunity?.antiFake?.referenceDeviation && typeof opportunity.antiFake.referenceDeviation === "object"
      ? opportunity.antiFake.referenceDeviation
      : {}
  const maxRatio =
    toFiniteOrNull(liquidity?.referenceDeviationRatio) ??
    toFiniteOrNull(referenceDeviation?.maxRatio)
  const hasStrongReferenceDeviation =
    Boolean(liquidity?.hasStrongReferenceDeviation) ||
    Boolean(referenceDeviation?.strong) ||
    (maxRatio ?? 0) > PREMIUM_REFERENCE_PENALTY_RATIO
  const hasExtremeReferenceDeviation =
    Boolean(liquidity?.hasExtremeReferenceDeviation) ||
    Boolean(referenceDeviation?.extreme) ||
    (maxRatio ?? 0) > PREMIUM_REFERENCE_REJECT_RATIO

  return {
    ratio: maxRatio,
    hasStrongReferenceDeviation,
    hasExtremeReferenceDeviation
  }
}

function evaluateWeaponSkinRiskContext({
  opportunity = {},
  liquidity = {},
  inputItem = {},
  profile = RISKY_SCAN_PROFILE,
  rules = {},
  isRiskyProfile = false,
  snapshotState = FRESHNESS_STATES.STALE,
  quoteFreshnessState = FRESHNESS_STATES.STALE,
  referenceSignals = {},
  stale = {},
  depthSignals = {}
} = {}) {
  const marketHashName = String(opportunity?.itemName || inputItem?.marketHashName || "").trim()
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const referencePrice = toFiniteOrNull(opportunity?.referencePrice ?? inputItem?.referencePrice)
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const supportSignals = buildWeaponSkinSupportSignals({
    marketHashName,
    referencePrice,
    executionPrice: buyPrice,
    profit,
    spread,
    volume7d,
    marketCoverage,
    snapshotFreshnessState: snapshotState,
    quoteFreshnessState,
    hasStrongReferenceDeviation: referenceSignals?.hasStrongReferenceDeviation,
    hasExtremeReferenceDeviation: referenceSignals?.hasExtremeReferenceDeviation,
    isUsefulCandidate:
      marketCoverage >= MIN_MARKET_COVERAGE ||
      (volume7d != null && volume7d >= Number(rules.minVolume7d || profile.minVolume7d || 0))
  })
  const lowPrice = buyPrice != null && buyPrice < WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_PRICE_USD
  const lowProfit = profit != null && profit < WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_PROFIT_USD
  const weakSpread =
    spread != null && spread < WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_SPREAD_PERCENT
  const lowUtility =
    (volume7d != null && volume7d < WEAPON_SKIN_CONTEXTUAL_LOW_VALUE_VOLUME_7D) ||
    supportSignals.coverageState === "blocked"
  const poorMarketRelevance =
    !supportSignals.hasNameSignal &&
    (!supportSignals.hasNonTrivialPrice || supportSignals.coverageState === "blocked")
  const borderlineLowValueForwardCandidate =
    supportSignals.hasNonTrivialPrice &&
    supportSignals.hasMeaningfulProfit &&
    supportSignals.hasSaneSpread &&
    supportSignals.hasAcceptableReference &&
    (supportSignals.hasAnyCoverage || supportSignals.hasNameSignal)
  const lowValuePolicy = evaluateWeaponSkinLowValuePolicy({
    weakSignals: [
      lowPrice,
      lowProfit,
      weakSpread,
      lowUtility,
      poorMarketRelevance,
      supportSignals.lowValuePattern
    ],
    supportSignals: [
      supportSignals.hasNameSignal,
      supportSignals.hasNonTrivialPrice,
      supportSignals.hasMeaningfulProfit,
      supportSignals.hasSaneSpread,
      supportSignals.hasAnyCoverage,
      supportSignals.hasAcceptableReference,
      supportSignals.hasNonStaleQuotes
    ],
    allowForward: borderlineLowValueForwardCandidate,
    hardRejectSupportMax: 2,
    penaltySignals: [
      supportSignals.lowValuePattern,
      lowPrice,
      lowProfit,
      weakSpread,
      lowUtility,
      poorMarketRelevance
    ]
  })
  const borderlineLowValueAllowedForward =
    lowValuePolicy.weakCount >= 2 && borderlineLowValueForwardCandidate
  const hardRejectLowValue = lowValuePolicy.hardReject
  const penaltyKeys = []

  if (!hardRejectLowValue && borderlineLowValueAllowedForward && lowValuePolicy.penalty) {
    penaltyKeys.push("penalty_low_value_allowed_forward")
  }
  if (isStatTrakVariantName(marketHashName)) {
    penaltyKeys.push("stattrak_penalty")
  }
  if (isSouvenirVariantName(marketHashName)) {
    penaltyKeys.push("souvenir_penalty")
  }

  const fallbackEvidenceCount = countTrueValues([
    supportSignals.hasMeaningfulProfit,
    supportSignals.hasSaneSpread,
    supportSignals.hasNonStaleQuotes,
    supportSignals.hasNameSignal,
    supportSignals.isUsefulCandidate,
    supportSignals.hasUsefulVolume
  ])
  const hasMissingLiquidityFallbackPath =
    volume7d == null &&
    isRiskyProfile &&
    supportSignals.hasCoverage &&
    supportSignals.hasAcceptableReference &&
    supportSignals.hasNonTrivialPrice &&
    supportSignals.hasNonStaleQuotes &&
    fallbackEvidenceCount >= 2
  if (volume7d == null && isRiskyProfile && hasMissingLiquidityFallbackPath) {
    penaltyKeys.push("penalty_missing_liquidity_allowed_forward")
  }
  const weakVariantSupport =
    volume7d == null ||
    !supportSignals.hasCoverage ||
    !supportSignals.hasNonStaleQuotes ||
    !supportSignals.hasAcceptableReference
  const variantSpeculativeAllowed =
    (penaltyKeys.includes("stattrak_penalty") || penaltyKeys.includes("souvenir_penalty")) &&
    weakVariantSupport &&
    supportSignals.hasNonTrivialPrice &&
    supportSignals.hasSaneSpread &&
    supportSignals.hasAcceptableReference
  const usableMarketsAfterFreshness = Math.max(
    Number(stale?.usableMarkets || marketCoverage || 0),
    0
  )
  const hasInsufficientUsableMarkets = Boolean(stale?.hasInsufficientUsableMarkets)
  const hasStaleFreshness =
    normalizeFreshnessState(quoteFreshnessState) === FRESHNESS_STATES.STALE ||
    normalizeFreshnessState(snapshotState) === FRESHNESS_STATES.STALE
  const hasAgingFreshness =
    !hasStaleFreshness &&
    (
      normalizeFreshnessState(quoteFreshnessState) === FRESHNESS_STATES.AGING ||
      normalizeFreshnessState(snapshotState) === FRESHNESS_STATES.AGING
    )
  const staleSupportCount = countTrueValues([
    supportSignals.hasCoverage,
    supportSignals.hasNonTrivialPrice,
    supportSignals.hasMeaningfulProfit,
    supportSignals.hasSaneSpread,
    supportSignals.hasAcceptableReference,
    supportSignals.hasNameSignal || supportSignals.isUsefulCandidate,
    supportSignals.hasUsefulVolume,
    normalizeFreshnessState(snapshotState) !== FRESHNESS_STATES.STALE
  ])
  const staleWeakCount = countTrueValues([
    marketCoverage < MIN_MARKET_COVERAGE,
    !supportSignals.hasNonTrivialPrice,
    !supportSignals.hasMeaningfulProfit,
    !supportSignals.hasSaneSpread,
    !supportSignals.hasAcceptableReference,
    !supportSignals.hasNameSignal && !supportSignals.isUsefulCandidate && !supportSignals.hasUsefulVolume,
    Boolean(depthSignals?.hasExtremeDepthGap),
    Boolean(referenceSignals?.hasExtremeReferenceDeviation)
  ])
  const hasStrongStaleFallback =
    supportSignals.hasCoverage &&
    supportSignals.hasAcceptableReference &&
    supportSignals.hasNonTrivialPrice &&
    supportSignals.hasMeaningfulProfit &&
    supportSignals.hasSaneSpread &&
    staleSupportCount >= 5
  const staleRejected =
    hasStaleFreshness &&
    (
      Boolean(referenceSignals?.hasExtremeReferenceDeviation) ||
      Boolean(depthSignals?.hasExtremeDepthGap) ||
      staleWeakCount >= 5 ||
      (
        (hasInsufficientUsableMarkets || usableMarketsAfterFreshness < MIN_MARKET_COVERAGE) &&
        !hasStrongStaleFallback
      ) ||
      (staleWeakCount >= 4 && staleSupportCount <= 3)
    )
  let staleForwardedTier = ""
  if (!staleRejected && hasStaleFreshness && isRiskyProfile) {
    penaltyKeys.push("stale_penalty_allowed_forward")
    staleForwardedTier =
      normalizeFreshnessState(quoteFreshnessState) === FRESHNESS_STATES.STALE
        ? "speculative"
        : "risky"
  } else if (hasAgingFreshness && isRiskyProfile) {
    penaltyKeys.push("aging_penalty_allowed_forward")
    staleForwardedTier = "risky"
  }
  const uniquePenaltyKeys = Array.from(new Set(penaltyKeys))

  return {
    hardRejectLowValue,
    penaltyKeys: uniquePenaltyKeys,
    hasMissingLiquidityFallbackPath,
    speculativeEligible:
      hasMissingLiquidityFallbackPath ||
      borderlineLowValueAllowedForward ||
      variantSpeculativeAllowed ||
      staleForwardedTier === "speculative",
    allowLowConfidencePath:
      hasMissingLiquidityFallbackPath ||
      borderlineLowValueAllowedForward ||
      variantSpeculativeAllowed ||
      staleForwardedTier === "speculative",
    missingLiquidityRejected: false,
    missingLiquidityPenaltyApplied: volume7d == null && isRiskyProfile,
    staleRejected,
    staleForwardedTier
  }
}

function evaluateRiskyBorderlinePromotion({
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN,
  profile = {},
  rules = {},
  opportunity = {},
  liquidity = {},
  stale = {},
  depthSignals = {},
  referenceSignals = {},
  snapshotState = FRESHNESS_STATES.FRESH,
  quoteFreshnessState = FRESHNESS_STATES.FRESH
} = {}) {
  const category = normalizeItemCategory(itemCategory)
  const promotionRules = getRiskyBorderlinePromotionRules(category)
  const isRiskyProfile = String(profile?.name || "")
    .trim()
    .toLowerCase()
    .startsWith("risky")
  if (!isRiskyProfile || !Boolean(profile?.allowBorderlinePromotion) || !promotionRules?.enabled) {
    return {
      allowProfit: false,
      allowSpread: false,
      allowLiquidity: false,
      allowCoverage: false,
      promotionKeys: []
    }
  }

  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const minPriceUsd = Number(rules.minPriceUsd ?? profile?.minPriceUsd ?? 0)
  const minProfitUsd = Number(rules.minProfitUsd ?? profile?.minProfitUsd ?? 0)
  const minSpreadPercent = Number(rules.minSpreadPercent ?? profile?.minSpreadPercent ?? 0)
  const minVolume7d = Number(rules.minVolume7d ?? profile?.minVolume7d ?? 0)
  const minMarketCoverage = Number(
    rules.minMarketCoverage ?? profile?.minMarketCoverage ?? MIN_MARKET_COVERAGE
  )
  const usableMarkets = Math.max(Number(stale?.usableMarkets || marketCoverage || 0), 0)
  const quoteState = normalizeFreshnessState(quoteFreshnessState)
  const snapshotFreshnessState = normalizeFreshnessState(snapshotState)
  const promotionKeys = []
  const allowCoverage =
    category === ITEM_CATEGORIES.WEAPON_SKIN &&
    marketCoverage === 1 &&
    usableMarkets >= 1 &&
    buyPrice != null &&
    buyPrice >= minPriceUsd &&
    profit != null &&
    profit >= minProfitUsd + Number(promotionRules?.minProfitBufferUsd || 0) &&
    spread != null &&
    spread >= minSpreadPercent + Number(promotionRules?.minSpreadBufferPercent || 0) &&
    volume7d != null &&
    volume7d >= minVolume7d &&
    quoteState !== FRESHNESS_STATES.STALE &&
    snapshotFreshnessState !== FRESHNESS_STATES.STALE &&
    !Boolean(referenceSignals?.hasStrongReferenceDeviation) &&
    !Boolean(referenceSignals?.hasExtremeReferenceDeviation) &&
    !Boolean(depthSignals?.hasSuspiciousDepthGap) &&
    !Boolean(depthSignals?.hasExtremeDepthGap) &&
    !Boolean(stale?.hasInsufficientUsableMarkets)

  if (
    buyPrice == null ||
    buyPrice < minPriceUsd ||
    profit == null ||
    profit <= 0 ||
    spread == null ||
    (marketCoverage < minMarketCoverage && !allowCoverage) ||
    (usableMarkets < MIN_MARKET_COVERAGE && !allowCoverage) ||
    quoteState === FRESHNESS_STATES.STALE ||
    snapshotFreshnessState === FRESHNESS_STATES.STALE ||
    Boolean(referenceSignals?.hasStrongReferenceDeviation) ||
    Boolean(referenceSignals?.hasExtremeReferenceDeviation) ||
    Boolean(depthSignals?.hasSuspiciousDepthGap) ||
    Boolean(depthSignals?.hasExtremeDepthGap) ||
    Boolean(stale?.hasInsufficientUsableMarkets)
  ) {
    return {
      allowProfit: false,
      allowSpread: false,
      allowLiquidity: false,
      allowCoverage: false,
      promotionKeys
    }
  }

  const profitShortfall = Math.max(minProfitUsd - Number(profit || 0), 0)
  const spreadShortfall = Math.max(minSpreadPercent - Number(spread || 0), 0)
  const minimumBorderlineVolume = Math.max(
    Math.floor(minVolume7d * Number(promotionRules?.minVolumeRatio || 1)),
    1
  )
  const allowProfit =
    profit < minProfitUsd &&
    profitShortfall <= Number(promotionRules?.maxProfitShortfallUsd || 0) &&
    spread >= minSpreadPercent + Number(promotionRules?.minSpreadBufferPercent || 0) &&
    volume7d != null &&
    volume7d >= minVolume7d
  const allowSpread =
    spread < minSpreadPercent &&
    spreadShortfall <= Number(promotionRules?.maxSpreadShortfallPercent || 0) &&
    profit >= minProfitUsd + Number(promotionRules?.minProfitBufferUsd || 0) &&
    volume7d != null &&
    volume7d >= minVolume7d
  const allowLiquidity =
    volume7d != null &&
    volume7d < minVolume7d &&
    volume7d >= minimumBorderlineVolume &&
    profit >= minProfitUsd + Number(promotionRules?.minProfitBufferUsd || 0) &&
    spread >= minSpreadPercent + Number(promotionRules?.minSpreadBufferPercent || 0)

  if (allowProfit) promotionKeys.push("borderline_profit_promoted")
  if (allowSpread) promotionKeys.push("borderline_spread_promoted")
  if (allowLiquidity) promotionKeys.push("borderline_liquidity_promoted")
  if (allowCoverage) promotionKeys.push("borderline_market_coverage_promoted")

  return {
    allowProfit,
    allowSpread,
    allowLiquidity,
    allowCoverage,
    promotionKeys
  }
}

function computeRiskAdjustments({
  opportunity = {},
  liquidity = {},
  stale = {},
  inputItem = {},
  profile = RISKY_SCAN_PROFILE
} = {}) {
  const itemCategory = normalizeItemCategory(
    inputItem?.itemCategory || opportunity?.itemCategory,
    opportunity?.itemName || inputItem?.marketHashName
  )
  const rules = getCategoryScanRules(itemCategory, profile?.name || "strict")
  const buyPrice = toFiniteOrNull(opportunity?.buyPrice)
  const profit = toFiniteOrNull(opportunity?.profit)
  const spread = toFiniteOrNull(opportunity?.spreadPercent ?? opportunity?.spread_pct)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const marketCoverage = Number(opportunity?.marketCoverage || 0)
  const staleMinutes = toFiniteOrNull(stale?.maxAgeMinutes)
  const quoteFreshnessState = String(
    stale?.selectedState || stale?.state || FRESHNESS_STATES.FRESH
  )
    .trim()
    .toLowerCase()
  const usableMarketsAfterFreshness = Math.max(
    Number(stale?.usableMarkets || marketCoverage || 0),
    0
  )
  const hasInsufficientUsableMarkets = Boolean(stale?.hasInsufficientUsableMarkets)
  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, itemCategory)
  const snapshotState = String(snapshotFreshness?.state || FRESHNESS_STATES.STALE)
    .trim()
    .toLowerCase()
  const hasSnapshotStale = snapshotState === FRESHNESS_STATES.STALE
  const hasSnapshotAging = snapshotState === FRESHNESS_STATES.AGING
  const isPremiumCategory = PREMIUM_ITEM_CATEGORIES.has(itemCategory)
  const isRiskyProfile = String(profile?.name || "").trim().toLowerCase().startsWith("risky")
  const minProfitUsd = Number(rules.minProfitUsd ?? profile?.minProfitUsd ?? 0)
  const allowMissingDepthWithPenalty = Boolean(
    rules.allowMissingDepthWithPenalty ?? profile?.allowMissingDepthWithPenalty
  )
  const depthSignals = resolveDepthSignals(opportunity, liquidity)
  const referenceSignals = resolveReferenceSignals(opportunity, liquidity)
  const weaponSkinRiskContext =
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN
      ? evaluateWeaponSkinRiskContext({
          opportunity,
          liquidity,
          inputItem,
          profile,
          rules,
          isRiskyProfile,
          snapshotState,
          quoteFreshnessState,
          referenceSignals,
          stale,
          depthSignals
        })
      : {
          hardRejectLowValue: false,
          penaltyKeys: [],
          speculativeEligible: false,
          allowLowConfidencePath: false,
          missingLiquidityRejected: false,
          missingLiquidityPenaltyApplied: false,
          staleRejected: false,
          staleForwardedTier: ""
        }
  const borderlinePromotion = evaluateRiskyBorderlinePromotion({
    itemCategory,
    profile,
    rules,
    opportunity,
    liquidity,
    stale,
    depthSignals,
    referenceSignals,
    snapshotState,
    quoteFreshnessState
  })

  if (profit == null || profit <= 0) {
    return { passed: false, primaryReason: "non_positive_profit", penalty: 0 }
  }
  if (isRiskyProfile && profit < minProfitUsd) {
    if (!borderlinePromotion.allowProfit) {
      return { passed: false, primaryReason: "risky_low_profit", penalty: 0 }
    }
  }
  if (buyPrice == null || buyPrice < Number(rules.minPriceUsd || profile.minPriceUsd || 0)) {
    return {
      passed: false,
      primaryReason: isRiskyProfile ? "risky_low_price" : "ignored_execution_floor",
      penalty: 0
    }
  }
  if (
    spread == null ||
    spread < Number(rules.minSpreadPercent || profile.minSpreadPercent || 0) ||
    spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)
  ) {
    const isBorderlineSpread =
      spread != null &&
      spread < Number(rules.minSpreadPercent || profile.minSpreadPercent || 0) &&
      spread <= Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT) &&
      borderlinePromotion.allowSpread
    if (!isBorderlineSpread) {
      return {
        passed: false,
        primaryReason:
          spread != null && spread > Number(rules.maxSpreadPercent || MAX_SPREAD_PERCENT)
            ? "ignored_extreme_spread"
            : "spread_below_min",
        penalty: 0
      }
    }
  }
  if (
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
    isRiskyProfile &&
    weaponSkinRiskContext.staleRejected
  ) {
    return {
      passed: false,
      primaryReason: "ignored_stale_data",
      penalty: 0,
      diagnosticRejectionKey: "hard_reject_stale"
    }
  }
  if (hasInsufficientUsableMarkets || usableMarketsAfterFreshness < MIN_MARKET_COVERAGE) {
    const allowWeaponSkinStaleFallback =
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      isRiskyProfile &&
      (Boolean(weaponSkinRiskContext.staleForwardedTier) || Boolean(borderlinePromotion.allowCoverage))
    if (!allowWeaponSkinStaleFallback) {
      return { passed: false, primaryReason: "ignored_stale_data", penalty: 0 }
    }
  }
  if (marketCoverage <= 0) {
    return { passed: false, primaryReason: "ignored_missing_markets", penalty: 0 }
  }
  if (marketCoverage < Number(rules.minMarketCoverage || MIN_MARKET_COVERAGE)) {
    if (!borderlinePromotion.allowCoverage) {
      return { passed: false, primaryReason: "ignored_missing_markets", penalty: 0 }
    }
  }

  if (isPremiumCategory) {
    if (referenceSignals.hasExtremeReferenceDeviation) {
      return { passed: false, primaryReason: "ignored_reference_deviation", penalty: 0 }
    }
    if (depthSignals.hasExtremeDepthGap) {
      return { passed: false, primaryReason: "ignored_missing_depth", penalty: 0 }
    }
    if (volume7d != null && volume7d < PREMIUM_MIN_VOLUME_REJECT) {
      return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
    }
    if (volume7d == null) {
      if (profile.name === "strict" || !profile.allowMissingLiquidity) {
        return { passed: false, primaryReason: "ignored_missing_liquidity_data", penalty: 0 }
      }
      if (marketCoverage < PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) {
        return { passed: false, primaryReason: "ignored_missing_markets", penalty: 0 }
      }
      if (
        depthSignals.hasMissingDepth ||
        depthSignals.hasOutlierAdjusted ||
        depthSignals.hasSuspiciousDepthGap
      ) {
        return {
          passed: false,
          primaryReason: isRiskyProfile ? "risky_missing_depth" : "ignored_missing_depth",
          penalty: 0
        }
      }
      if (referenceSignals.hasStrongReferenceDeviation) {
        return { passed: false, primaryReason: "ignored_reference_deviation", penalty: 0 }
      }
    } else if (volume7d < Number(rules.minVolume7d || profile.minVolume7d || PREMIUM_MIN_VOLUME_REJECT)) {
      return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
    }
  } else if (volume7d == null) {
    const allowWeaponSkinMissingLiquidityFallback =
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      isRiskyProfile &&
      Boolean(weaponSkinRiskContext.hasMissingLiquidityFallbackPath)
    // Keep the category profile strict by default, but honor the existing
    // evidence-based weapon-skin fallback when it is explicitly satisfied.
    if (!profile.allowMissingLiquidity && !allowWeaponSkinMissingLiquidityFallback) {
      return { passed: false, primaryReason: "ignored_missing_liquidity_data", penalty: 0 }
    }
    if (weaponSkinRiskContext.missingLiquidityRejected) {
      return { passed: false, primaryReason: "hard_reject_missing_liquidity", penalty: 0 }
    }
  } else if (volume7d < Number(rules.minVolume7d || profile.minVolume7d || 0)) {
    if (!borderlinePromotion.allowLiquidity) {
      return { passed: false, primaryReason: "ignored_low_liquidity", penalty: 0 }
    }
  }

  if (
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
    isRiskyProfile &&
    weaponSkinRiskContext.hardRejectLowValue
  ) {
    return { passed: false, primaryReason: "hard_reject_low_value", penalty: 0 }
  }

  if (
    !isPremiumCategory &&
    isRiskyProfile &&
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
    depthSignals.hasMissingDepth &&
    marketCoverage <= MIN_MARKET_COVERAGE
  ) {
    return { passed: false, primaryReason: "risky_missing_depth", penalty: 0 }
  }

  const weakPremiumConfidence =
    hasSnapshotStale ||
    quoteFreshnessState === FRESHNESS_STATES.STALE ||
    (staleMinutes != null && staleMinutes >= Number(getCategoryStaleRules(itemCategory).agingMaxMinutes || 0)) ||
    referenceSignals.hasStrongReferenceDeviation ||
    depthSignals.hasOutlierAdjusted ||
    depthSignals.hasSuspiciousDepthGap
  if (isPremiumCategory && depthSignals.hasBothDepthMissing) {
    if (!isRiskyProfile || !allowMissingDepthWithPenalty || weakPremiumConfidence) {
      return {
        passed: false,
        primaryReason: isRiskyProfile ? "risky_missing_depth" : "ignored_missing_depth",
        penalty: 0
      }
    }
  }
  if (isPremiumCategory && depthSignals.hasMissingDepth && !depthSignals.hasBothDepthMissing) {
    if (!isRiskyProfile || !allowMissingDepthWithPenalty) {
      return { passed: false, primaryReason: "ignored_missing_depth", penalty: 0 }
    }
    if (weakPremiumConfidence && marketCoverage <= MIN_MARKET_COVERAGE) {
      return { passed: false, primaryReason: "risky_missing_depth", penalty: 0 }
    }
  }

  if (isPremiumCategory && isRiskyProfile && quoteFreshnessState === FRESHNESS_STATES.STALE) {
    if (
      usableMarketsAfterFreshness <= MIN_MARKET_COVERAGE ||
      referenceSignals.hasStrongReferenceDeviation
    ) {
      return { passed: false, primaryReason: "ignored_stale_data", penalty: 0 }
    }
  }

  let penalty = 0
  if (isPremiumCategory) {
    if (volume7d == null) {
      penalty += 18
      if (marketCoverage === PREMIUM_UNKNOWN_VOLUME_MIN_MARKET_COVERAGE) {
        penalty += 8
      }
    } else if (volume7d < PREMIUM_MIN_VOLUME_MEDIUM) {
      penalty += 14
    } else if (volume7d < PREMIUM_MIN_VOLUME_HIGH) {
      penalty += 5
    }

    if (spread != null && spread > PREMIUM_SPREAD_HEAVY_PENALTY_PERCENT) {
      penalty += 22
    }
    if (depthSignals.hasBothDepthMissing) {
      penalty += 28
    } else if (depthSignals.hasMissingDepth) {
      penalty += 22
    } else if (depthSignals.hasOutlierAdjusted || depthSignals.hasSuspiciousDepthGap) {
      penalty += 16
    }
    if (referenceSignals.hasStrongReferenceDeviation) {
      penalty += 18
    }
    if (quoteFreshnessState === FRESHNESS_STATES.STALE) {
      penalty += Number(getCategoryStaleRules(itemCategory).stalePenalty || 12) + 8
    } else if (quoteFreshnessState === FRESHNESS_STATES.AGING) {
      penalty += Math.max(Number(getCategoryStaleRules(itemCategory).agingPenalty || 4) - 1, 0)
    }
    if (hasSnapshotStale) {
      penalty += 8
    } else if (hasSnapshotAging) {
      penalty += 2
    }
  } else {
    if (volume7d == null) {
      penalty +=
        itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
        weaponSkinRiskContext.missingLiquidityPenaltyApplied
          ? WEAPON_SKIN_MISSING_LIQUIDITY_PENALTY
          : profile.allowMissingLiquidity
            ? 16
            : 0
    } else if (volume7d < Number(rules.minVolume7d || MIN_VOLUME_7D)) {
      penalty += volume7d < 60 ? 14 : 6
    }

    if (buyPrice < Number(rules.minPriceUsd || MIN_EXECUTION_PRICE_USD)) {
      penalty += 7
    }
    if (spread < Number(rules.minSpreadPercent || MIN_SPREAD_PERCENT)) {
      penalty += 10
    }
    if (quoteFreshnessState === FRESHNESS_STATES.STALE) {
      penalty += Number(getCategoryStaleRules(itemCategory).stalePenalty || 20)
    } else if (quoteFreshnessState === FRESHNESS_STATES.AGING) {
      penalty += Math.max(Number(getCategoryStaleRules(itemCategory).agingPenalty || 8) - 2, 0)
    }
    if (hasSnapshotStale) {
      penalty += 10
    } else if (hasSnapshotAging) {
      penalty += 4
    }
    if (
      isRiskyProfile &&
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      (depthSignals.hasMissingDepth || depthSignals.hasOutlierAdjusted)
    ) {
      penalty += marketCoverage <= MIN_MARKET_COVERAGE ? 14 : 7
    }
    if (isRiskyProfile && itemCategory === ITEM_CATEGORIES.WEAPON_SKIN && profit < 1.2) {
      penalty += 6
    }
    if (borderlinePromotion.allowProfit) {
      penalty += 4
    }
    if (borderlinePromotion.allowSpread) {
      penalty += 4
    }
    if (borderlinePromotion.allowLiquidity) {
      penalty += 5
    }
    if (borderlinePromotion.allowCoverage) {
      penalty += 5
    }
    if (
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      weaponSkinRiskContext.penaltyKeys.includes("penalty_low_value_allowed_forward")
    ) {
      penalty += WEAPON_SKIN_LOW_VALUE_PENALTY
    }
    if (
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      weaponSkinRiskContext.penaltyKeys.includes("stattrak_penalty")
    ) {
      penalty += WEAPON_SKIN_STATTRAK_VARIANT_PENALTY
    }
    if (
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
      weaponSkinRiskContext.penaltyKeys.includes("souvenir_penalty")
    ) {
      penalty += WEAPON_SKIN_SOUVENIR_VARIANT_PENALTY
    }
  }

  return {
    passed: true,
    primaryReason: "",
    penalty: round2(Math.max(penalty, 0)),
    diagnosticPenaltyKeys: weaponSkinRiskContext.penaltyKeys,
    speculativeEligible: weaponSkinRiskContext.speculativeEligible,
    allowLowConfidencePath: weaponSkinRiskContext.allowLowConfidencePath,
    staleForwardedTier: weaponSkinRiskContext.staleForwardedTier,
    borderlinePromotionKeys: borderlinePromotion.promotionKeys
  }
}

function evaluateRiskyCandidateOutcome({
  opportunity = {},
  liquidity = {},
  stale = {},
  inputItem = {},
  perMarket = [],
  profile = RISKY_SCAN_PROFILE,
  qualityFloor = null,
  isHighConfidenceEligible = false
} = {}) {
  const itemCategory = normalizeItemCategory(
    inputItem?.itemCategory || opportunity?.itemCategory,
    opportunity?.itemName || inputItem?.marketHashName
  )
  const evaluation = computeRiskAdjustments({
    opportunity,
    liquidity,
    stale,
    inputItem,
    profile
  })
  if (!evaluation.passed) {
    return {
      passed: false,
      primaryReason: evaluation.primaryReason,
      diagnosticRejectionKey: evaluation?.diagnosticRejectionKey || "",
      evaluation,
      apiRow: null,
      speculativeEligible: false
    }
  }

  const apiRow = buildApiOpportunityRow({
    opportunity,
    inputItem,
    liquidity,
    stale,
    perMarket,
    extraPenalty: evaluation.penalty,
    isRiskyEligible: true,
    isHighConfidenceEligible
  })
  const floor = qualityFloor || getRiskyQualityFloor(itemCategory)
  if (Number(apiRow?.profit || 0) < Number(floor?.minProfitUsd || 0)) {
    return {
      passed: false,
      primaryReason: "risky_low_profit",
      evaluation,
      apiRow,
      speculativeEligible: false
    }
  }
  if (Number(apiRow?.score || 0) < Number(floor?.minScore || RISKY_MIN_SCORE)) {
    return {
      passed: false,
      primaryReason: "risky_low_score",
      evaluation,
      apiRow,
      speculativeEligible: false
    }
  }
  if (
    itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
    normalizeConfidence(apiRow?.executionConfidence) === "Low" &&
    !Boolean(evaluation?.allowLowConfidencePath)
  ) {
    return {
      passed: false,
      primaryReason: "risky_low_confidence",
      evaluation,
      apiRow,
      speculativeEligible: false
    }
  }

  return {
    passed: true,
    primaryReason: "",
    evaluation,
    apiRow,
    speculativeEligible:
      itemCategory === ITEM_CATEGORIES.WEAPON_SKIN && Boolean(evaluation?.speculativeEligible)
  }
}

function normalizeConfidence(value) {
  const safe = String(value || "").trim().toLowerCase()
  if (safe === "high") return "High"
  if (safe === "medium") return "Medium"
  return "Low"
}

function downgradeConfidenceOneLevel(value) {
  const confidence = normalizeConfidence(value)
  if (confidence === "High") return "Medium"
  return "Low"
}

function downgradeConfidenceForStale(
  baseConfidence,
  stale = {},
  snapshotFreshnessState = FRESHNESS_STATES.FRESH
) {
  const confidence = normalizeConfidence(baseConfidence)
  const quoteState = String(stale?.selectedState || stale?.state || FRESHNESS_STATES.FRESH)
    .trim()
    .toLowerCase()
  const snapshotState = String(snapshotFreshnessState || FRESHNESS_STATES.FRESH)
    .trim()
    .toLowerCase()
  if (quoteState === FRESHNESS_STATES.STALE) return "Low"
  if (quoteState === FRESHNESS_STATES.FRESH && snapshotState === FRESHNESS_STATES.FRESH) {
    return confidence
  }
  if (snapshotState === FRESHNESS_STATES.STALE) {
    return downgradeConfidenceOneLevel(confidence)
  }
  if (quoteState === FRESHNESS_STATES.AGING) {
    return confidence === "High" ? "Medium" : confidence
  }
  if (snapshotState === FRESHNESS_STATES.AGING) {
    return confidence
  }
  return downgradeConfidenceOneLevel(confidence)
}

function normalizeBadges(rawBadges = []) {
  const unique = new Set()
  for (const badge of Array.isArray(rawBadges) ? rawBadges : []) {
    const text = String(badge || "").trim()
    if (!text) continue
    unique.add(text)
  }
  return Array.from(unique)
}

function buildApiOpportunityRow({
  opportunity = {},
  inputItem = {},
  liquidity = {},
  stale = {},
  perMarket = [],
  extraPenalty = 0,
  isHighConfidenceEligible = false,
  isRiskyEligible = false
}) {
  const bySource = toBySourceMap(perMarket)
  const buySource = normalizeMarketLabel(opportunity?.buyMarket)
  const sellSource = normalizeMarketLabel(opportunity?.sellMarket)
  const buyQuote = bySource[buySource] || null
  const sellQuote = bySource[sellSource] || null
  const itemCategory = normalizeItemCategory(
    opportunity?.itemCategory || inputItem?.itemCategory,
    opportunity?.itemName || inputItem?.marketHashName
  )
  const baseScore = toFiniteOrNull(opportunity?.opportunityScore) ?? 0
  const stalePenalty = toFiniteOrNull(stale?.penalty) ?? 0
  const softPenalty = toFiniteOrNull(extraPenalty) ?? 0
  const score = clampScore(baseScore - stalePenalty - softPenalty)
  const depthFlags = Array.isArray(opportunity?.depthFlags) ? opportunity.depthFlags : []
  const hasOutlierAdjusted = depthFlags.some(
    (flag) => flag === "BUY_OUTLIER_ADJUSTED" || flag === "SELL_OUTLIER_ADJUSTED"
  )
  const hasMissingDepth = depthFlags.includes("MISSING_DEPTH")
  const hasDepthGapSuspicious = depthFlags.some(
    (flag) => flag === "BUY_DEPTH_GAP_SUSPICIOUS" || flag === "SELL_DEPTH_GAP_SUSPICIOUS"
  )
  const hasDepthGapExtreme = depthFlags.some(
    (flag) => flag === "BUY_DEPTH_GAP_EXTREME" || flag === "SELL_DEPTH_GAP_EXTREME"
  )
  const snapshotFreshness = resolveSnapshotFreshnessState(inputItem, itemCategory)
  const snapshotFreshnessState = normalizeFreshnessState(snapshotFreshness?.state)
  const snapshotStale = snapshotFreshnessState === FRESHNESS_STATES.STALE
  const quoteFreshnessState = normalizeFreshnessState(stale?.selectedState || stale?.state)
  const executionConfidence = downgradeConfidenceForStale(
    opportunity?.executionConfidence,
    stale,
    snapshotFreshnessState
  )
  const buyPrice = roundPrice(opportunity?.buyPrice || 0)
  const volume7d = toFiniteOrNull(liquidity?.volume7d)
  const premiumRiskyBadges =
    isRiskyEligible && PREMIUM_ITEM_CATEGORIES.has(itemCategory)
      ? [
          "Premium category",
          buyPrice >= 100 ? "High-ticket opportunity" : "",
          volume7d == null || volume7d < PREMIUM_MIN_VOLUME_HIGH ? "Low-frequency market" : "",
          hasMissingDepth ? "Missing depth tolerated" : ""
        ].filter(Boolean)
      : []

  const badges = normalizeBadges([
    ...(Array.isArray(opportunity?.reasonBadges) ? opportunity.reasonBadges : []),
    ...premiumRiskyBadges,
    quoteFreshnessState === FRESHNESS_STATES.STALE ? ["Stale market data"] : [],
    quoteFreshnessState === FRESHNESS_STATES.AGING ? ["Aging market data"] : [],
    snapshotFreshnessState === FRESHNESS_STATES.STALE ? ["Stale snapshot"] : [],
    hasDepthGapExtreme ? ["Depth anomaly"] : [],
    !hasDepthGapExtreme && hasDepthGapSuspicious ? ["Depth gap flagged"] : [],
    !hasDepthGapExtreme && !hasDepthGapSuspicious && hasMissingDepth ? ["Missing depth"] : [],
    hasOutlierAdjusted ? ["Outlier adjusted"] : [],
    !hasOutlierAdjusted && !hasMissingDepth && !hasDepthGapSuspicious && !hasDepthGapExtreme
      ? ["Good depth"]
      : []
  ])

  return {
    itemId: Number(opportunity?.itemId || inputItem?.skinId || 0) || null,
    itemName: String(opportunity?.itemName || inputItem?.marketHashName || "Tracked Item"),
    itemCategory,
    itemSubcategory: String(inputItem?.itemSubcategory || "").trim() || null,
    itemRarity:
      String(
        opportunity?.itemRarity || inputItem?.itemRarity || inputItem?.rarity || ""
      ).trim() || null,
    itemRarityColor:
      String(
        opportunity?.itemRarityColor ||
          inputItem?.itemRarityColor ||
          inputItem?.rarityColor ||
          ""
      ).trim() || null,
    itemImageUrl:
      sanitizeImageUrl(inputItem?.itemImageUrlLarge || inputItem?.itemImageUrl) || null,
    buyMarket: buySource || null,
    buyPrice,
    sellMarket: sellSource || null,
    sellNet: roundPrice(opportunity?.sellNet || 0),
    profit: roundPrice(opportunity?.profit || 0),
    spread: round2(opportunity?.spreadPercent || opportunity?.spread_pct || 0),
    score,
    scoreCategory: String(opportunity?.scoreCategory || arbitrageEngine.categorizeOpportunityScore(score)),
    executionConfidence,
    liquidityBand: String(opportunity?.liquidityBand || "Low"),
    liquidity:
      toFiniteOrNull(liquidity?.volume7d) ??
      toFiniteOrNull(liquidity?.liquidityScore) ??
      toFiniteOrNull(opportunity?.liquiditySample) ??
      null,
    liquidityScore: toFiniteOrNull(liquidity?.liquidityScore),
    volume7d,
    marketCoverage: Number(opportunity?.marketCoverage || 0),
    referencePrice: toFiniteOrNull(opportunity?.referencePrice ?? inputItem?.referencePrice),
    stalePenalty,
    softPenalty,
    maxQuoteAgeMinutes: toFiniteOrNull(stale?.maxAgeMinutes),
    quoteFreshnessState,
    snapshotFreshnessState,
    staleMarkets: Number(stale?.staleMarkets || 0),
    agingMarkets: Number(stale?.agingMarkets || 0),
    usableMarketsAfterFreshness: Number(stale?.usableMarkets || 0),
    buyUrl: buyQuote?.url || opportunity?.buyUrl || null,
    sellUrl: sellQuote?.url || opportunity?.sellUrl || null,
    snapshotStale,
    flags: depthFlags,
    badges,
    spreadScore: toFiniteOrNull(opportunity?.scores?.spreadScore),
    liquidityScoreComponent: toFiniteOrNull(opportunity?.scores?.liquidityScore),
    stabilityScore: toFiniteOrNull(opportunity?.scores?.stabilityScore),
    marketReliabilityScore: toFiniteOrNull(opportunity?.scores?.marketScore),
    depthConfidenceScore: toFiniteOrNull(opportunity?.scores?.depthConfidenceScore),
    rawOpportunityScore: baseScore,
    isHighConfidenceEligible: Boolean(isHighConfidenceEligible),
    isRiskyEligible: Boolean(isRiskyEligible)
  }
}

function confidenceRank(value) {
  const safe = normalizeConfidence(value)
  if (safe === "High") return 3
  if (safe === "Medium") return 2
  return 1
}

function toGradeFromScore(score, row = {}) {
  const safeScore = clampScore(score)
  const executionConfidence = normalizeConfidence(row?.executionConfidence)
  const isHighConfidence = Boolean(row?.isHighConfidenceEligible)
  if (isHighConfidence && safeScore >= 90) return "A"
  if (isHighConfidence && safeScore >= 78) return "B"
  if (isHighConfidence) return "C"
  if (safeScore >= DEFAULT_SCORE_CUTOFF && executionConfidence !== "Low") return "C"
  return "RISKY"
}

function toMetadataObject(row = {}) {
  const rawQuoteState = String(row?.quoteFreshnessState || "").trim()
  const rawSnapshotState = String(row?.snapshotFreshnessState || "").trim()
  return {
    item_id: Number(row?.itemId || 0) || null,
    item_subcategory: String(row?.itemSubcategory || "").trim() || null,
    item_rarity: String(row?.itemRarity || "").trim() || null,
    item_rarity_color: String(row?.itemRarityColor || "").trim() || null,
    item_image_url: sanitizeImageUrl(row?.itemImageUrl) || null,
    score_category: String(row?.scoreCategory || "").trim() || null,
    liquidity_value: toFiniteOrNull(row?.liquidity),
    liquidity_score: toFiniteOrNull(row?.liquidityScore),
    volume_7d: toFiniteOrNull(row?.volume7d),
    market_coverage: Number(row?.marketCoverage || 0),
    reference_price: toFiniteOrNull(row?.referencePrice),
    stale_penalty: toFiniteOrNull(row?.stalePenalty),
    quote_freshness_state: rawQuoteState
      ? normalizeFreshnessState(rawQuoteState)
      : FRESHNESS_STATES.FRESH,
    snapshot_freshness_state: rawSnapshotState
      ? normalizeFreshnessState(rawSnapshotState)
      : Boolean(row?.snapshotStale)
        ? FRESHNESS_STATES.STALE
        : FRESHNESS_STATES.FRESH,
    stale_markets: Number(row?.staleMarkets || 0),
    aging_markets: Number(row?.agingMarkets || 0),
    usable_markets_after_freshness: Number(row?.usableMarketsAfterFreshness || 0),
    soft_penalty: toFiniteOrNull(row?.softPenalty),
    buy_url: String(row?.buyUrl || "").trim() || null,
    sell_url: String(row?.sellUrl || "").trim() || null,
    flags: Array.isArray(row?.flags) ? row.flags : [],
    badges: Array.isArray(row?.badges) ? row.badges : [],
    spread_score: toFiniteOrNull(row?.spreadScore),
    liquidity_score_component: toFiniteOrNull(row?.liquidityScoreComponent),
    stability_score: toFiniteOrNull(row?.stabilityScore),
    market_reliability_score: toFiniteOrNull(row?.marketReliabilityScore),
    depth_confidence_score: toFiniteOrNull(row?.depthConfidenceScore),
    raw_opportunity_score: toFiniteOrNull(row?.rawOpportunityScore),
    is_high_confidence_eligible: Boolean(row?.isHighConfidenceEligible),
    is_risky_eligible: Boolean(row?.isRiskyEligible),
    snapshot_stale: Boolean(row?.snapshotStale)
  }
}

function buildFeedInsertRow(row = {}, scanRunId = "", options = {}) {
  const detectedAt = options.detectedAt || new Date().toISOString()
  const isDuplicate = Boolean(options.isDuplicate)
  const score = clampScore(row?.score)
  const executionConfidence = normalizeConfidence(row?.executionConfidence)
  const eventType = normalizeFeedEventType(options.eventType) || (isDuplicate ? "duplicate" : "new")
  const eventAnalysis =
    options?.eventAnalysis && typeof options.eventAnalysis === "object" ? options.eventAnalysis : {}
  const previousRow =
    options?.previousRow && typeof options.previousRow === "object" ? options.previousRow : {}
  return {
    item_name: String(row?.itemName || "Tracked Item").trim(),
    market_hash_name: String(row?.itemName || "Tracked Item").trim(),
    category: normalizeItemCategory(row?.itemCategory, row?.itemName),
    buy_market: normalizeMarketLabel(row?.buyMarket),
    buy_price: roundPrice(row?.buyPrice || 0),
    sell_market: normalizeMarketLabel(row?.sellMarket),
    sell_net: roundPrice(row?.sellNet || 0),
    profit: roundPrice(row?.profit || 0),
    spread_pct: round2(row?.spread || 0),
    opportunity_score: Math.round(score),
    execution_confidence: executionConfidence,
    quality_grade: toGradeFromScore(score, row),
    liquidity_label: String(row?.liquidityBand || "Low").trim() || "Low",
    detected_at: detectedAt,
    scan_run_id: String(scanRunId || "").trim() || null,
    is_active: true,
    is_duplicate: isDuplicate,
    metadata: {
      ...toMetadataObject(row),
      event_type: eventType,
      change_reasons: Array.isArray(eventAnalysis?.changeReasons) ? eventAnalysis.changeReasons : [],
      previous_feed_id: String(previousRow?.id || "").trim() || null,
      previous_detected_at: previousRow?.detected_at || previousRow?.detectedAt || null,
      profit_delta_pct: toFiniteOrNull(eventAnalysis?.profitDeltaPercent),
      spread_delta_pct: toFiniteOrNull(eventAnalysis?.spreadDelta),
      score_delta: toFiniteOrNull(eventAnalysis?.scoreDelta),
      confidence_delta_levels: toFiniteOrNull(eventAnalysis?.confidenceDelta),
      liquidity_delta_pct: toFiniteOrNull(eventAnalysis?.liquidityDeltaPercent)
    }
  }
}

function buildDedupeSignature(itemName = "", buyMarket = "", sellMarket = "") {
  return `${String(itemName || "").trim().toLowerCase()}::${String(buyMarket || "")
    .trim()
    .toLowerCase()}::${String(sellMarket || "")
    .trim()
    .toLowerCase()}`
}

function toProfitDeltaPercent(currentProfit, previousProfit) {
  const current = toFiniteOrNull(currentProfit)
  const previous = toFiniteOrNull(previousProfit)
  if (current == null || previous == null) return Infinity
  const denominator = Math.max(Math.abs(previous), 0.01)
  return Math.abs(current - previous) / denominator * 100
}

function toAbsoluteDelta(currentValue, previousValue) {
  const current = toFiniteOrNull(currentValue)
  const previous = toFiniteOrNull(previousValue)
  if (current == null || previous == null) return null
  return Math.abs(current - previous)
}

function toPercentDelta(currentValue, previousValue, minBase = 1) {
  const current = toFiniteOrNull(currentValue)
  const previous = toFiniteOrNull(previousValue)
  if (current == null || previous == null) return null
  return Math.abs(current - previous) / Math.max(Math.abs(previous), minBase) * 100
}

function resolvePreviousLiquidityValue(previous = {}) {
  return (
    toFiniteOrNull(previous?.metadata?.liquidity_value) ??
    toFiniteOrNull(previous?.metadata?.volume_7d) ??
    toFiniteOrNull(previous?.liquidity_value) ??
    toFiniteOrNull(previous?.volume_7d)
  )
}

function classifyOpportunityFeedEvent(current = {}, previous = {}) {
  const hasPrevious = Boolean(previous && Object.keys(previous).length)
  if (!hasPrevious) {
    return {
      eventType: "new",
      materiallyChanged: true,
      changeReasons: ["new"],
      profitDeltaPercent: Infinity,
      spreadDelta: Infinity,
      scoreDelta: Infinity,
      confidenceDelta: Infinity,
      liquidityDeltaPercent: Infinity
    }
  }

  const profitDeltaPercent = toProfitDeltaPercent(current?.profit, previous?.profit)
  const spreadDelta = toAbsoluteDelta(current?.spread, previous?.spread_pct)
  const scoreDelta = toAbsoluteDelta(current?.score, previous?.opportunity_score)
  const confidenceDelta = Math.abs(
    confidenceRank(current?.executionConfidence) -
      confidenceRank(previous?.execution_confidence || previous?.executionConfidence)
  )
  const previousLiquidityLabel = String(
    previous?.liquidity_label || previous?.liquidityLabel || ""
  )
    .trim()
    .toLowerCase()
  const currentLiquidityLabel = String(current?.liquidityBand || current?.liquidityLabel || "")
    .trim()
    .toLowerCase()
  const liquidityDeltaPercent = toPercentDelta(
    current?.liquidity ?? current?.volume7d,
    resolvePreviousLiquidityValue(previous),
    1
  )
  const changeReasons = []

  if (profitDeltaPercent >= MIN_PROFIT_CHANGE_PCT) {
    changeReasons.push("profit")
  }
  if (spreadDelta != null && spreadDelta >= MIN_SPREAD_CHANGE_PCT) {
    changeReasons.push("spread")
  }
  if (scoreDelta != null && scoreDelta >= MIN_SCORE_CHANGE) {
    changeReasons.push("score")
  }
  if (confidenceDelta >= MIN_CONFIDENCE_CHANGE_LEVELS) {
    changeReasons.push("confidence")
  }
  if (
    currentLiquidityLabel !== previousLiquidityLabel ||
    (liquidityDeltaPercent != null && liquidityDeltaPercent >= MIN_LIQUIDITY_CHANGE_PCT)
  ) {
    changeReasons.push("liquidity")
  }

  const reactivated = previous?.is_active === false
  return {
    eventType: reactivated ? "reactivated" : changeReasons.length ? "updated" : "duplicate",
    materiallyChanged: reactivated || changeReasons.length > 0,
    changeReasons,
    profitDeltaPercent,
    spreadDelta,
    scoreDelta,
    confidenceDelta,
    liquidityDeltaPercent
  }
}

function isMateriallyNewOpportunity(current = {}, previous = {}) {
  return classifyOpportunityFeedEvent(current, previous).materiallyChanged
}

function mapFeedRowToApiRow(row = {}) {
  const metadata = row?.metadata && typeof row.metadata === "object" ? row.metadata : {}
  const score = clampScore(row?.opportunity_score)
  const executionConfidence = normalizeConfidence(row?.execution_confidence)
  const itemName = String(row?.item_name || "Tracked Item").trim() || "Tracked Item"
  const rawQuoteState = String(metadata?.quote_freshness_state || "").trim()
  const rawSnapshotState = String(metadata?.snapshot_freshness_state || "").trim()
  return {
    feedId: String(row?.id || "").trim() || null,
    detectedAt: row?.detected_at || null,
    scanRunId: String(row?.scan_run_id || "").trim() || null,
    isActive: Boolean(row?.is_active),
    isDuplicate: Boolean(row?.is_duplicate),
    eventType: normalizeFeedEventType(metadata?.event_type) || (Boolean(row?.is_duplicate) ? "duplicate" : "new"),
    changeReasons: Array.isArray(metadata?.change_reasons) ? metadata.change_reasons : [],
    previousFeedId: String(metadata?.previous_feed_id || "").trim() || null,
    previousDetectedAt: metadata?.previous_detected_at || null,
    profitDeltaPercent: toFiniteOrNull(metadata?.profit_delta_pct),
    spreadDelta: toFiniteOrNull(metadata?.spread_delta_pct),
    scoreDelta: toFiniteOrNull(metadata?.score_delta),
    confidenceDeltaLevels: toFiniteOrNull(metadata?.confidence_delta_levels),
    liquidityDeltaPercent: toFiniteOrNull(metadata?.liquidity_delta_pct),
    itemId: Number(metadata?.item_id || 0) || null,
    itemName,
    itemCategory: normalizeItemCategory(row?.category, itemName),
    itemSubcategory: String(metadata?.item_subcategory || "").trim() || null,
    itemRarity: String(metadata?.item_rarity || "").trim() || null,
    itemRarityColor: String(metadata?.item_rarity_color || "").trim() || null,
    itemImageUrl: sanitizeImageUrl(metadata?.item_image_url) || null,
    buyMarket: normalizeMarketLabel(row?.buy_market),
    buyPrice: roundPrice(row?.buy_price || 0),
    sellMarket: normalizeMarketLabel(row?.sell_market),
    sellNet: roundPrice(row?.sell_net || 0),
    profit: roundPrice(row?.profit || 0),
    spread: round2(row?.spread_pct || 0),
    score,
    scoreCategory:
      String(metadata?.score_category || "").trim() || arbitrageEngine.categorizeOpportunityScore(score),
    executionConfidence,
    qualityGrade: String(row?.quality_grade || "").trim() || null,
    liquidityBand: String(row?.liquidity_label || "Low").trim() || "Low",
    liquidityLabel: String(row?.liquidity_label || "Low").trim() || "Low",
    liquidity: toFiniteOrNull(metadata?.liquidity_value),
    liquidityScore: toFiniteOrNull(metadata?.liquidity_score),
    volume7d: toFiniteOrNull(metadata?.volume_7d),
    marketCoverage: Number(metadata?.market_coverage || 0),
    referencePrice: toFiniteOrNull(metadata?.reference_price),
    stalePenalty: toFiniteOrNull(metadata?.stale_penalty),
    quoteFreshnessState: rawQuoteState
      ? normalizeFreshnessState(rawQuoteState)
      : FRESHNESS_STATES.FRESH,
    snapshotFreshnessState: rawSnapshotState
      ? normalizeFreshnessState(rawSnapshotState)
      : Boolean(metadata?.snapshot_stale)
        ? FRESHNESS_STATES.STALE
        : FRESHNESS_STATES.FRESH,
    staleMarkets: Number(metadata?.stale_markets || 0),
    agingMarkets: Number(metadata?.aging_markets || 0),
    usableMarketsAfterFreshness: Number(metadata?.usable_markets_after_freshness || 0),
    softPenalty: toFiniteOrNull(metadata?.soft_penalty),
    buyUrl: String(metadata?.buy_url || "").trim() || null,
    sellUrl: String(metadata?.sell_url || "").trim() || null,
    snapshotStale: Boolean(metadata?.snapshot_stale),
    flags: Array.isArray(metadata?.flags) ? metadata.flags : [],
    badges: Array.isArray(metadata?.badges) ? metadata.badges : [],
    spreadScore: toFiniteOrNull(metadata?.spread_score),
    liquidityScoreComponent: toFiniteOrNull(metadata?.liquidity_score_component),
    stabilityScore: toFiniteOrNull(metadata?.stability_score),
    marketReliabilityScore: toFiniteOrNull(metadata?.market_reliability_score),
    depthConfidenceScore: toFiniteOrNull(metadata?.depth_confidence_score),
    rawOpportunityScore: toFiniteOrNull(metadata?.raw_opportunity_score),
    isHighConfidenceEligible: Boolean(metadata?.is_high_confidence_eligible),
    isRiskyEligible: Boolean(metadata?.is_risky_eligible)
  }
}

async function enrichFeedRowsWithSkinMetadata(rows = []) {
  const items = Array.isArray(rows) ? rows : []
  if (!items.length) return []

  const skinIds = Array.from(
    new Set(
      items
        .map((row) => Number(row?.itemId || 0))
        .filter((value) => Number.isInteger(value) && value > 0)
    )
  )
  const marketHashNames = Array.from(
    new Set(
      items
        .map((row) => normalizeMarketHashName(row?.itemName))
        .filter(Boolean)
    )
  )
  if (!marketHashNames.length && !skinIds.length) return items

  let skinsByNameRows = []
  let skinsByIdRows = []
  try {
    const [byName, byId] = await Promise.all([
      marketHashNames.length ? skinRepo.getByMarketHashNames(marketHashNames) : Promise.resolve([]),
      skinIds.length ? skinRepo.getByIds(skinIds) : Promise.resolve([])
    ])
    skinsByNameRows = Array.isArray(byName) ? byName : []
    skinsByIdRows = Array.isArray(byId) ? byId : []
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to enrich feed rows with skin metadata", err.message)
    return items
  }

  const skinsById = {}
  for (const row of skinsByIdRows) {
    const id = Number(row?.id || 0)
    if (!Number.isInteger(id) || id <= 0) continue
    skinsById[id] = row
  }
  const skinsByName = toByNameMap(
    skinsByNameRows.map((row) => ({
      ...row,
      market_hash_name: normalizeMarketHashName(row?.market_hash_name)
    })),
    "market_hash_name"
  )

  const merged = items.map((row) => {
    const key = normalizeMarketHashName(row?.itemName)
    const skinId = Number(row?.itemId || 0)
    const skin =
      (Number.isInteger(skinId) && skinId > 0 ? skinsById[skinId] || null : null) ||
      (key ? skinsByName[key] || null : null)
    if (!skin) return row
    const currentImage = sanitizeImageUrl(row?.itemImageUrl)
    const skinImage = sanitizeImageUrl(skin?.image_url_large || skin?.image_url)
    return {
      ...row,
      itemRarity:
        String(row?.itemRarity || "").trim() || String(skin?.rarity || "").trim() || null,
      itemRarityColor:
        String(row?.itemRarityColor || "").trim() ||
        String(skin?.rarity_color || skin?.rarityColor || "").trim() ||
        null,
      itemImageUrl:
        currentImage || skinImage || null
    }
  })

  const missingMetadataNames = Array.from(
    new Set(
      merged
        .filter((row) => {
          const itemName = normalizeMarketHashName(row?.itemName)
          if (!itemName) return false
          const rarity = String(row?.itemRarity || "")
            .trim()
            .toLowerCase()
          const missingImage = !sanitizeImageUrl(row?.itemImageUrl)
          const missingRarity = !rarity
          const weakRarity = rarity === "consumer grade"
          return missingImage || missingRarity || weakRarity
        })
        .map((row) => normalizeMarketHashName(row?.itemName))
        .filter(Boolean)
    )
  ).slice(0, FEED_METADATA_ENRICH_LIMIT)

  if (!missingMetadataNames.length) {
    return merged
  }

  let steamMetadataByName = {}
  try {
    steamMetadataByName = await marketImageService.fetchSteamSearchMetadataBatch(
      missingMetadataNames,
      {
        timeoutMs: 8000,
        maxRetries: 1,
        concurrency: 2,
        count: 50
      }
    )
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to fetch steam metadata for feed rows", err.message)
    steamMetadataByName = {}
  }

  const steamKeys = Object.keys(steamMetadataByName || {})
  if (!steamKeys.length) {
    return merged
  }

  try {
    const upsertRows = steamKeys
      .map((marketHashName) => {
        const metadata = steamMetadataByName[marketHashName] || {}
        const imageUrl = String(metadata?.imageUrl || "").trim() || null
        const imageUrlLarge = String(metadata?.imageUrlLarge || "").trim() || imageUrl
        const rarity = String(metadata?.rarity || "").trim() || null
        const rarityColor = String(metadata?.rarityColor || "").trim() || null
        if (!imageUrl && !imageUrlLarge && !rarity && !rarityColor) {
          return null
        }
        return {
          market_hash_name: marketHashName,
          rarity,
          rarity_color: rarityColor,
          image_url: imageUrl,
          image_url_large: imageUrlLarge
        }
      })
      .filter(Boolean)
    if (upsertRows.length) {
      await skinRepo.upsertSkins(upsertRows)
    }
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to persist steam metadata enrichment", err.message)
  }

  return merged.map((row) => {
    const key = normalizeMarketHashName(row?.itemName)
    const metadata = key ? steamMetadataByName[key] || null : null
    if (!metadata) return row

    const currentRarity = String(row?.itemRarity || "").trim()
    const currentRarityColor = String(row?.itemRarityColor || "").trim()
    const shouldReplaceRarity = !currentRarity || currentRarity.toLowerCase() === "consumer grade"
    const shouldReplaceRarityColor =
      shouldReplaceRarity ||
      !currentRarityColor ||
      currentRarityColor.toLowerCase() === "#b0c3d9" ||
      currentRarityColor.toLowerCase() === "#7f8ba5"
    const currentImage = sanitizeImageUrl(row?.itemImageUrl)
    const metadataImage = sanitizeImageUrl(metadata?.imageUrlLarge || metadata?.imageUrl)

    return {
      ...row,
      itemRarity:
        shouldReplaceRarity && metadata?.rarity
          ? String(metadata.rarity).trim() || currentRarity || null
          : currentRarity || null,
      itemRarityColor:
        shouldReplaceRarityColor && metadata?.rarityColor
          ? String(metadata.rarityColor).trim() || currentRarityColor || null
          : currentRarityColor || null,
      itemImageUrl:
        currentImage || metadataImage || null
    }
  })
}

function computeRiskyRankScore(row = {}) {
  const baseScore = Number(row?.score || 0)
  const category = normalizeItemCategory(row?.itemCategory, row?.itemName)
  const price = toFiniteOrNull(row?.buyPrice) ?? 0
  const profit = toFiniteOrNull(row?.profit) ?? 0
  const volume7d = toFiniteOrNull(row?.volume7d)
  const marketCoverage = Number(row?.marketCoverage || 0)
  const confidence = normalizeConfidence(row?.executionConfidence)
  const hasMissingDepth = Array.isArray(row?.flags) && row.flags.includes("MISSING_DEPTH")
  const isHighConfidence = Boolean(row?.isHighConfidenceEligible)
  let rankScore = baseScore

  if (isHighConfidence) {
    return rankScore + 20
  }

  if (PREMIUM_ITEM_CATEGORIES.has(category)) {
    rankScore += 6
    rankScore += Math.min(price / 120, 10)
    rankScore += Math.min(profit / 4, 8)
    rankScore += Math.min(marketCoverage * 2, 8)
    if (confidence !== "Low") rankScore += 2
    if (volume7d != null && volume7d < PREMIUM_MIN_VOLUME_HIGH) rankScore -= 2
    if (hasMissingDepth) rankScore -= 3
    return round2(rankScore)
  }

  if (category === ITEM_CATEGORIES.WEAPON_SKIN) {
    if (price < 6) rankScore -= 8
    if (profit < 1) rankScore -= 7
    if ((volume7d ?? 0) < 60) rankScore -= 5
    if (confidence === "Low") rankScore -= 6
  }
  return round2(rankScore)
}

function sortOpportunities(rows = []) {
  return [...rows].sort((a, b) => {
    const riskyRankDelta = computeRiskyRankScore(b) - computeRiskyRankScore(a)
    if (riskyRankDelta) return riskyRankDelta
    return (
      Number(b?.score || 0) - Number(a?.score || 0) ||
      confidenceRank(b?.executionConfidence) - confidenceRank(a?.executionConfidence) ||
      Number(b?.profit || 0) - Number(a?.profit || 0) ||
      Number(b?.spread || 0) - Number(a?.spread || 0)
    )
  })
}

function buildInputItemForComparison(item = {}) {
  return {
    skinId: Number(item?.skinId || 0) || null,
    marketHashName: String(item?.marketHashName || "").trim(),
    itemCategory: normalizeItemCategory(item?.itemCategory, item?.marketHashName),
    itemSubcategory: String(item?.itemSubcategory || "").trim() || null,
    itemRarity: String(item?.itemRarity || item?.rarity || "").trim() || null,
    itemRarityColor:
      String(item?.itemRarityColor || item?.rarityColor || "").trim() || null,
    imageUrl: sanitizeImageUrl(item?.itemImageUrl) || null,
    imageUrlLarge:
      sanitizeImageUrl(item?.itemImageUrlLarge) || sanitizeImageUrl(item?.itemImageUrl) || null,
    quantity: 1,
    steamPrice:
      toFiniteOrNull(item?.referencePrice) ??
      toFiniteOrNull(item?.steamPrice) ??
      0,
    steamCurrency: "USD",
    steamRecordedAt: item?.snapshotCapturedAt || null,
    sevenDayChangePercent: toFiniteOrNull(item?.sevenDayChangePercent),
    liquidityScore: toFiniteOrNull(item?.liquidityScore),
    marketVolume7d: toFiniteOrNull(item?.marketVolume7d),
    referencePrice: toFiniteOrNull(item?.referencePrice),
    snapshotStale: Boolean(item?.snapshotStale),
    universeSource: String(item?.universeSource || "").trim() || "fallback_curated",
    liquidityRank: toFiniteOrNull(item?.liquidityRank)
  }
}

function collectDiscardReasonsFromOpportunity(
  opportunity = {},
  discardStats = {},
  rejectedByItem = null,
  itemName = "",
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN
) {
  const reasons = Array.isArray(opportunity?.antiFake?.reasons) ? opportunity.antiFake.reasons : []
  for (const reason of reasons) {
    incrementReasonCounter(discardStats, reason, itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        itemName || opportunity?.itemName,
        reason,
        itemCategory
      )
    }
  }

  const debugReasons = Array.isArray(opportunity?.antiFake?.debugReasons)
    ? opportunity.antiFake.debugReasons
    : []
  for (const reason of debugReasons) {
    incrementReasonCounter(discardStats, reason, itemCategory)
    if (rejectedByItem) {
      incrementItemReasonCounter(
        rejectedByItem,
        itemName || opportunity?.itemName,
        reason,
        itemCategory
      )
    }
  }
}

function createJobState() {
  return {
    latest: null,
    inFlight: null,
    inFlightRunId: null,
    timer: null,
    lastError: null,
    nextScheduledAt: null,
    lastStartedAt: null,
    lastCompletedAt: null,
    lastPersistSummary: null,
    lastStaleReconcileAt: 0,
    coordination: {
      lockAcquired: 0,
      lockDenied: 0,
      skippedAlreadyRunning: 0,
      staleReconciled: 0,
      timedOutReconciled: 0,
      crossJobBlocked: 0
    }
  }
}

const scannerState = createJobState()
const enrichmentState = createJobState()

function parseTimestampMs(value) {
  const text = String(value || "").trim()
  if (!text) return null
  const parsed = new Date(text).getTime()
  return Number.isFinite(parsed) ? parsed : null
}

function getJobState(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN) {
  const normalized = String(scannerType || "")
    .trim()
    .toLowerCase()
  return normalized === SCANNER_TYPES.ENRICHMENT ? enrichmentState : scannerState
}

function getJobIntervalMs(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN) {
  const normalized = String(scannerType || "")
    .trim()
    .toLowerCase()
  return normalized === SCANNER_TYPES.ENRICHMENT ? ENRICHMENT_INTERVAL_MS : OPPORTUNITY_SCAN_INTERVAL_MS
}

function getJobOverdueGraceMs(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN) {
  const normalized = String(scannerType || "")
    .trim()
    .toLowerCase()
  return normalized === SCANNER_TYPES.ENRICHMENT
    ? ENRICHMENT_OVERDUE_GRACE_MS
    : SCANNER_OVERDUE_GRACE_MS
}

function getJobTimeoutMs(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN) {
  const normalized = String(scannerType || "")
    .trim()
    .toLowerCase()
  return normalized === SCANNER_TYPES.ENRICHMENT ? ENRICHMENT_JOB_TIMEOUT_MS : OPPORTUNITY_JOB_TIMEOUT_MS
}

function updateNextScheduledAt(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN) {
  const state = getJobState(scannerType)
  const intervalMs = getJobIntervalMs(scannerType)
  state.nextScheduledAt = new Date(Date.now() + intervalMs).toISOString()
}

function getOtherScannerType(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN) {
  const normalized = String(scannerType || "")
    .trim()
    .toLowerCase()
  return normalized === SCANNER_TYPES.ENRICHMENT
    ? SCANNER_TYPES.OPPORTUNITY_SCAN
    : SCANNER_TYPES.ENRICHMENT
}

function computeElapsedMs(startedAt) {
  const startedMs = parseTimestampMs(startedAt)
  if (startedMs == null) return null
  return Math.max(Date.now() - startedMs, 0)
}

function toActiveRunDescriptor(run = null, fallback = {}) {
  const runId = String(run?.id || fallback?.id || "").trim() || null
  const startedAt = run?.started_at || run?.startedAt || fallback?.startedAt || null
  return {
    runId,
    startedAt,
    elapsedMs: computeElapsedMs(startedAt)
  }
}

function buildAlreadyRunningResult(scannerType, run = null, fallback = {}) {
  const descriptor = toActiveRunDescriptor(run, fallback)
  return {
    scannerType,
    scanRunId: descriptor.runId,
    alreadyRunning: true,
    status: "already_running",
    existingRunId: descriptor.runId,
    existingRunStartedAt: descriptor.startedAt,
    elapsedMs: descriptor.elapsedMs
  }
}

function buildCrossJobBlockedResult(scannerType, blockingType, blockingRun = null, fallback = {}) {
  const descriptor = toActiveRunDescriptor(blockingRun, fallback)
  return {
    scannerType,
    scanRunId: null,
    alreadyRunning: false,
    status: "blocked_by_other_job",
    blockedByCrossJob: true,
    blockingScannerType: blockingType,
    blockingRunId: descriptor.runId,
    blockingRunStartedAt: descriptor.startedAt,
    blockingElapsedMs: descriptor.elapsedMs
  }
}

async function maybeRecordSkippedAlreadyRunningRun(
  scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN,
  trigger = "manual",
  reason = "already_running",
  activeRun = null
) {
  if (!RECORD_SKIPPED_ALREADY_RUNNING) return null
  const nowIso = new Date().toISOString()
  return scannerRunRepo
    .createRun({
      scannerType,
      status: "skipped_already_running",
      startedAt: nowIso,
      completedAt: nowIso,
      durationMs: 0,
      failureReason: reason,
      diagnosticsSummary: {
        scannerType,
        trigger: String(trigger || "manual"),
        coordination: {
          event: "skipped_already_running",
          reason,
          existingRunId: String(activeRun?.id || "").trim() || null,
          existingRunStartedAt: activeRun?.started_at || null
        }
      }
    })
    .catch((err) => {
      console.error(
        `[arbitrage-scanner] Failed to record skipped_already_running run (${scannerType})`,
        err.message
      )
      return null
    })
}

function isScannerRunOverdue(status = {}, nowMs = Date.now(), options = {}) {
  const scannerType = String(options?.scannerType || SCANNER_TYPES.OPPORTUNITY_SCAN)
    .trim()
    .toLowerCase()
  const state = options?.state || getJobState(scannerType)
  const intervalMs = Math.max(Number(options?.intervalMs || getJobIntervalMs(scannerType)), 1)
  const overdueGraceMs = Math.max(
    Number(options?.overdueGraceMs || getJobOverdueGraceMs(scannerType)),
    1000
  )
  const latestStartedMs = parseTimestampMs(status?.latestRun?.started_at || state.lastStartedAt)
  const latestRunStatus = String(status?.latestRun?.status || "")
    .trim()
    .toLowerCase()
  if (latestRunStatus === "running") {
    if (state.inFlight) return false
    if (!latestStartedMs) return true
    return nowMs - latestStartedMs >= intervalMs + overdueGraceMs
  }

  const latestCompletedMs = parseTimestampMs(
    status?.latestCompletedRun?.completed_at ||
      status?.latestCompletedRun?.started_at ||
      state.lastCompletedAt
  )
  const lastActivityMs = Math.max(latestCompletedMs || 0, latestStartedMs || 0)
  if (!lastActivityMs) return true
  return nowMs - lastActivityMs >= intervalMs + overdueGraceMs
}

async function reconcileStaleRunningRunsForType(
  scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN,
  nowMs = Date.now(),
  options = {}
) {
  const state = getJobState(scannerType)
  if (state.inFlight) {
    return {
      runningRows: 0,
      staleRows: 0,
      markedFailed: 0,
      skipped: true
    }
  }

  const force = Boolean(options.force)
  if (
    !force &&
    Number(state.lastStaleReconcileAt || 0) > 0 &&
    nowMs - Number(state.lastStaleReconcileAt || 0) < STALE_RUN_RECONCILE_COOLDOWN_MS
  ) {
    return {
      runningRows: 0,
      staleRows: 0,
      markedFailed: 0,
      skipped: true
    }
  }

  state.lastStaleReconcileAt = nowMs
  const staleThresholdMs = getJobIntervalMs(scannerType) + getJobOverdueGraceMs(scannerType)
  const timeoutMs = getJobTimeoutMs(scannerType)
  let runningRows = []
  try {
    runningRows = await scannerRunRepo.listRunningRuns(scannerType, { limit: 120 })
  } catch (err) {
    console.error(
      `[arbitrage-scanner] Failed to list running runs for stale reconciliation (${scannerType})`,
      err.message
    )
    return {
      runningRows: 0,
      staleRows: 0,
      markedFailed: 0,
      skipped: true
    }
  }

  let staleRows = 0
  let markedFailed = 0
  for (const run of runningRows) {
    const runId = String(run?.id || "").trim()
    if (!runId || runId === String(state.inFlightRunId || "").trim()) continue
    const startedMs = parseTimestampMs(run?.started_at)
    const elapsedMs = startedMs == null ? staleThresholdMs + 1 : Math.max(nowMs - startedMs, 0)
    if (elapsedMs < staleThresholdMs) continue

    staleRows += 1
    const existingDiagnostics =
      run?.diagnostics_summary && typeof run.diagnostics_summary === "object"
        ? run.diagnostics_summary
        : {}
    try {
      const timedOut = elapsedMs >= timeoutMs
      await scannerRunRepo.markFailed(runId, {
        status: timedOut ? "timed_out" : "failed",
        durationMs: elapsedMs,
        failureReason: timedOut ? "job_timeout_reconciled" : "scanner_run_stale_reconciled",
        diagnosticsSummary: {
          ...existingDiagnostics,
          trigger: String(existingDiagnostics?.trigger || "watchdog"),
          scannerType,
          staleRunReconciled: true,
          staleDurationMinutes: Math.max(Math.round(elapsedMs / 60000), 1),
          timeoutMs
        },
        error: timedOut ? "job_timeout_reconciled" : "scanner_run_stale_reconciled"
      })
      markedFailed += 1
      if (timedOut) {
        state.coordination.timedOutReconciled += 1
      } else {
        state.coordination.staleReconciled += 1
      }
      console.warn(
        `[arbitrage-scanner] Reconciled stale ${scannerType} run ${runId} as ${
          timedOut ? "timed_out" : "failed"
        } after ${elapsedMs}ms`
      )
    } catch (err) {
      console.error(
        `[arbitrage-scanner] Failed to mark stale run ${runId} as failed (${scannerType})`,
        err.message
      )
    }
  }

  return {
    runningRows: runningRows.length,
    staleRows,
    markedFailed,
    skipped: false
  }
}

function mergeDiscardStats(target = {}, source = {}) {
  for (const [reason, count] of Object.entries(source || {})) {
    if (reason === "__byCategory") continue
    target[reason] = Number(target[reason] || 0) + Number(count || 0)
  }

  const sourceByCategory =
    source?.__byCategory && typeof source.__byCategory === "object" ? source.__byCategory : {}
  if (!Object.keys(sourceByCategory).length) return

  if (!target.__byCategory || typeof target.__byCategory !== "object") {
    target.__byCategory = {}
  }

  for (const [category, reasonStats] of Object.entries(sourceByCategory)) {
    const normalizedCategory = normalizeItemCategory(category)
    if (!target.__byCategory[normalizedCategory]) {
      target.__byCategory[normalizedCategory] = {}
    }
    const bucket = target.__byCategory[normalizedCategory]
    for (const [reason, count] of Object.entries(reasonStats || {})) {
      const normalizedReason = normalizeDiagnosticReason(reason)
      if (!normalizedReason) continue
      bucket[normalizedReason] = Number(bucket[normalizedReason] || 0) + Number(count || 0)
    }
  }
}

function mergeRejectedByItem(target = {}, source = {}) {
  for (const [itemName, payload] of Object.entries(source || {})) {
    const normalizedItem = String(itemName || "").trim() || "Unknown item"
    if (!target[normalizedItem]) {
      target[normalizedItem] = {
        total: 0,
        category: normalizeItemCategory(payload?.category, normalizedItem),
        reasons: {}
      }
    }
    const bucket = target[normalizedItem]
    if (!bucket.category) {
      bucket.category = normalizeItemCategory(payload?.category, normalizedItem)
    }
    bucket.total += Number(payload?.total || 0)
    for (const [reason, count] of Object.entries(payload?.reasons || {})) {
      const normalizedReason = normalizeDiagnosticReason(reason) || "unknown"
      bucket.reasons[normalizedReason] = Number(bucket.reasons[normalizedReason] || 0) + Number(count || 0)
    }
  }
}

function filterUniverseSeedsForScan(seeds = [], options = {}) {
  const seedList = Array.isArray(seeds) ? seeds : []
  const discardStats = {}
  const rejectedByItem = {}
  const weaponSkinDiagnostics = buildWeaponSkinFilterDiagnostics()
  const filterOptions = {
    ...options,
    weaponSkinDiagnostics
  }
  const selectedSeeds = seedList.filter((row) =>
    passesUniverseSeedFilters(row, discardStats, rejectedByItem, filterOptions)
  )

  return {
    selectedSeeds,
    discardStats,
    rejectedByItem,
    weaponSkinDiagnostics
  }
}

async function loadScannerInputs(discardStats = {}, rejectedByItem = {}, options = {}) {
  const mode = String(options?.mode || SCANNER_TYPES.OPPORTUNITY_SCAN)
    .trim()
    .toLowerCase()
  const isOpportunityMode = mode === SCANNER_TYPES.OPPORTUNITY_SCAN
  const enrichmentBatchSize = Math.max(
    Math.min(Number(options?.enrichmentBatchSize || ENRICHMENT_BATCH_SIZE), PRE_COMPARE_UNIVERSE_LIMIT),
    1
  )
  const opportunityBatchSize = Math.max(
    Math.min(Number(options?.opportunityBatchSize || OPPORTUNITY_BATCH_SIZE), PRE_COMPARE_UNIVERSE_LIMIT),
    1
  )
  const includeNearEligibleInOpportunity = options?.includeNearEligibleInOpportunity !== false
  const enableSnapshotWarmup =
    options?.enableSnapshotWarmup == null
      ? mode === SCANNER_TYPES.ENRICHMENT
      : Boolean(options.enableSnapshotWarmup)

  const [curatedSeeds, matureCatalogSeeds, fallbackSeeds] = await Promise.all([
    loadCuratedUniverseSeeds().catch((err) => {
      console.error("[arbitrage-scanner] Curated universe load failed", err.message)
      return []
    }),
    loadMatureCatalogUniverseSeeds(Math.max(HOT_MATURE_POOL_LIMIT, opportunityBatchSize * 4)).catch(
      (err) => {
        console.error("[arbitrage-scanner] Mature catalog seed load failed", err.message)
        return []
      }
    ),
    loadFallbackUniverseSeeds().catch((err) => {
      console.error("[arbitrage-scanner] Fallback universe load failed", err.message)
      return []
    })
  ])

  const mergedSeeds = mergeUniverseSeeds([curatedSeeds, matureCatalogSeeds, fallbackSeeds]).map((seed) =>
    applyCatalogStateToSeed(seed, null)
  )
  const preMaturitySeeds = mergedSeeds.map((seed) => {
    const preliminaryMaturity = resolveMaturityStateForSeed(seed)
    return {
      ...seed,
      maturityState: preliminaryMaturity.maturityState,
      maturityScore: preliminaryMaturity.maturityScore,
      missingSignals: preliminaryMaturity.missingSignals,
      scanLayer: resolveScanLayerForMaturity(preliminaryMaturity),
      layerPriority: computeLayerPriority({
        ...seed,
        ...preliminaryMaturity
      })
    }
  })
  const snapshotWarmupBacklog = summarizeSnapshotWarmupBacklog(preMaturitySeeds)
  const { seeds: hydratedSeeds, snapshotWarmup } = enableSnapshotWarmup
    ? await refreshSeedSnapshotsIfNeeded(preMaturitySeeds)
    : {
        seeds: preMaturitySeeds,
        snapshotWarmup: toSnapshotWarmupSummary({
          reason: isOpportunityMode ? "moved_to_enrichment_job" : "disabled",
          movedToEnrichment: isOpportunityMode,
          disabledInOpportunityScan: isOpportunityMode,
          freshSeedsBefore: snapshotWarmupBacklog.freshSeedsBefore,
          warmupCandidates: snapshotWarmupBacklog.warmupCandidates,
          warmupCandidatesByCategory: toScannerAuditCategoryCounts(
            snapshotWarmupBacklog.warmupCandidatesByCategory
          ),
          missingSnapshotBacklog: snapshotWarmupBacklog.missingSnapshotBacklog,
          deferredToEnrichmentItems: snapshotWarmupBacklog.warmupCandidates,
          deferredToEnrichmentByCategory: toScannerAuditCategoryCounts(
            snapshotWarmupBacklog.warmupCandidatesByCategory
          )
        })
      }
  const strictFilterResult = filterUniverseSeedsForScan(hydratedSeeds)
  const strictCoverageThreshold = computeStrictCoverageThreshold(hydratedSeeds.length)
  let selectedFilterResult = strictFilterResult
  let seedFilterMode = "strict"
  if (mode === SCANNER_TYPES.ENRICHMENT) {
    selectedFilterResult = filterUniverseSeedsForScan(hydratedSeeds, {
      allowMissingSnapshotData: true
    })
    seedFilterMode = "allow_missing_snapshot_data"
  } else if (!isOpportunityMode && strictFilterResult.selectedSeeds.length < strictCoverageThreshold) {
    const relaxedFilterResult = filterUniverseSeedsForScan(hydratedSeeds, {
      allowMissingSnapshotData: true
    })
    if (relaxedFilterResult.selectedSeeds.length > strictFilterResult.selectedSeeds.length) {
      selectedFilterResult = relaxedFilterResult
      seedFilterMode =
        strictFilterResult.selectedSeeds.length > 0
          ? "strict_plus_missing_snapshot_data"
          : "allow_missing_snapshot_data"
    }
  } else if (isOpportunityMode) {
    seedFilterMode = "strict_mature_only"
  }

  mergeDiscardStats(discardStats, selectedFilterResult.discardStats)
  mergeRejectedByItem(rejectedByItem, selectedFilterResult.rejectedByItem)

  const roughRanked = selectedFilterResult.selectedSeeds
    .map((row) => ({
      ...row,
      ...computeLiquidityRank({
        volume7d: row.marketVolume7d,
        marketCoverage: Math.max(Number(row.marketCoverageCount || 0), 0),
        sevenDayChangePercent: row.sevenDayChangePercent,
        referencePrice: row.referencePrice,
        itemCategory: row.itemCategory
      }),
      ...resolveMaturityStateForSeed(row)
    }))
    .map((row) => ({
      ...row,
      scanLayer: resolveScanLayerForMaturity(row),
      layerPriority: computeLayerPriority(row)
    }))
    .sort(
      (a, b) =>
        Number(b.layerPriority || 0) - Number(a.layerPriority || 0) ||
        Number(b.maturityScore || 0) - Number(a.maturityScore || 0) ||
        Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
        sourceRank(b.universeSource) - sourceRank(a.universeSource) ||
        Number(b.marketVolume7d || 0) - Number(a.marketVolume7d || 0)
    )
    .slice(0, PRE_COMPARE_UNIVERSE_LIMIT)

  const signalHistoryByItem = await loadFeedSignalHistoryForItems(
    roughRanked
      .slice(0, Math.min(Math.max(HOT_MATURE_POOL_LIMIT, opportunityBatchSize * 6), PRE_COMPARE_UNIVERSE_LIMIT))
      .map((row) => row?.marketHashName)
  )
  const ranked = roughRanked
    .map((row) => {
      const signalHistory =
        signalHistoryByItem[normalizeMarketHashName(row?.marketHashName)] || null
      return {
        ...row,
        signalHistory,
        signalHistoryScore: Number(signalHistory?.score || 0)
      }
    })
    .map((row) => ({
      ...row,
      layerPriority: computeLayerPriority(row)
    }))
    .sort(
      (a, b) =>
        Number(b.layerPriority || 0) - Number(a.layerPriority || 0) ||
        Number(b.maturityScore || 0) - Number(a.maturityScore || 0) ||
        Number(b.signalHistoryScore || 0) - Number(a.signalHistoryScore || 0) ||
        Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
        sourceRank(b.universeSource) - sourceRank(a.universeSource) ||
        Number(b.marketCoverageCount || 0) - Number(a.marketCoverageCount || 0) ||
        Number(b.marketVolume7d || 0) - Number(a.marketVolume7d || 0)
    )
    .slice(0, PRE_COMPARE_UNIVERSE_LIMIT)

  const opportunityAdmissions = isOpportunityMode
    ? ranked.map((row) => ({
        row,
        admission: evaluateOpportunitySeedAdmission(row)
      }))
    : []
  const opportunityAdmissionByName = new Map(
    opportunityAdmissions.map(({ row, admission }) => [normalizeMarketHashName(row?.marketHashName), admission])
  )
  const resolveOpportunityAdmission = (row = {}) =>
    opportunityAdmissionByName.get(normalizeMarketHashName(row?.marketHashName)) ||
    evaluateOpportunitySeedAdmission(row)
  const deferredToEnrichmentRows = isOpportunityMode
    ? opportunityAdmissions
        .filter(({ admission }) => !admission?.ready)
        .map(({ row }) => row)
    : []
  const deferredToEnrichmentByCategory = countRowsByCategory(
    deferredToEnrichmentRows.map((row) => ({
      itemCategory: row?.itemCategory,
      itemName: row?.marketHashName
    }))
  )

  const layeredSelection = selectSeedsForLayeredScanning(ranked, {
    opportunityTarget: opportunityBatchSize,
    hotTarget: Math.max(
      opportunityBatchSize - (includeNearEligibleInOpportunity ? OPPORTUNITY_NEAR_ELIGIBLE_LIMIT : 0),
      0
    ),
    nearEligibleTarget: includeNearEligibleInOpportunity ? OPPORTUNITY_NEAR_ELIGIBLE_LIMIT : 0,
    enrichmentTarget: enrichmentBatchSize,
    opportunityFilter: isOpportunityMode ? (row) => resolveOpportunityAdmission(row).ready : null,
    opportunityDeferredRows: deferredToEnrichmentRows
  })
  const allRankedSeeds = Array.isArray(layeredSelection?.allRankedSeeds)
    ? layeredSelection.allRankedSeeds
    : ranked
  const opportunitySeeds = Array.isArray(layeredSelection?.opportunitySeeds)
    ? layeredSelection.opportunitySeeds.slice(0, opportunityBatchSize)
    : []
  const enrichmentSeeds = Array.isArray(layeredSelection?.enrichmentSeeds)
    ? layeredSelection.enrichmentSeeds.slice(0, enrichmentBatchSize)
    : []
  const scanAdmission = isOpportunityMode
    ? summarizeOpportunitySeedAdmissions(opportunityAdmissions, opportunitySeeds)
    : buildOpportunityAdmissionDiagnostics()

  return {
    seeds: mode === SCANNER_TYPES.ENRICHMENT ? [] : opportunitySeeds,
    enrichmentSeeds,
    allSeeds: allRankedSeeds,
    scanAdmission,
    weaponSkinFilterDiagnostics:
      selectedFilterResult?.weaponSkinDiagnostics || buildWeaponSkinFilterDiagnostics(),
    snapshotWarmup: toSnapshotWarmupSummary({
      ...snapshotWarmup,
      seedFilterMode,
      strictCoverageThreshold,
      strictEligibleSeeds: strictFilterResult.selectedSeeds.length,
      selectedEligibleSeeds: selectedFilterResult.selectedSeeds.length,
      deferredToEnrichmentItems: isOpportunityMode
        ? deferredToEnrichmentRows.length
        : Number(snapshotWarmup?.deferredToEnrichmentItems || 0),
      deferredToEnrichmentByCategory: isOpportunityMode
        ? toScannerAuditCategoryCounts(deferredToEnrichmentByCategory)
        : toScannerAuditCategoryCounts(snapshotWarmup?.deferredToEnrichmentByCategory || {}),
      maturityFunnel: layeredSelection.diagnostics?.allSeeds?.maturityFunnel || buildMaturityCounter(0),
      maturityByCategory:
        layeredSelection.diagnostics?.allSeeds?.maturityByCategory || buildMaturityByCategoryCounter(0),
      layerDistribution: layeredSelection.diagnostics?.allSeeds?.layers || buildLayerCounter(0),
      layerDistributionByCategory:
        layeredSelection.diagnostics?.allSeeds?.layersByCategory || buildLayerByCategoryCounter(0),
      highYieldCoreSize: Number(layeredSelection?.coreSeeds?.length || 0),
      selectedForOpportunity: opportunitySeeds.length,
      selectedForEnrichment: enrichmentSeeds.length
    }),
    layeredScanning:
      layeredSelection.diagnostics && typeof layeredSelection.diagnostics === "object"
        ? {
            ...layeredSelection.diagnostics,
            scanAdmission
          }
        : {
            totalRankedSeeds: ranked.length,
            coreUniverseSize: 0,
            opportunityTarget: opportunityBatchSize,
            enrichmentTarget: enrichmentBatchSize,
            selectedForOpportunity: opportunitySeeds.length,
            selectedForEnrichment: enrichmentSeeds.length,
            allSeeds: buildLayerDiagnostics(ranked),
            opportunity: buildLayerDiagnostics(opportunitySeeds),
            enrichment: buildLayerDiagnostics(enrichmentSeeds),
            scanAdmission
          }
  }
}

function toSnapshotWarmupSummary(overrides = {}) {
  return {
    triggered: false,
    reason: "",
    movedToEnrichment: false,
    disabledInOpportunityScan: false,
    freshSeedsBefore: 0,
    warmupCandidates: 0,
    warmupCandidatesByCategory: buildScannerAuditCategoryCounter(0),
    attemptedItems: 0,
    refreshedItems: 0,
    failedItems: 0,
    deferredToEnrichmentItems: 0,
    deferredToEnrichmentByCategory: buildScannerAuditCategoryCounter(0),
    batchSize: SNAPSHOT_WARMUP_MAX_ITEMS,
    concurrency: SNAPSHOT_WARMUP_CONCURRENCY,
    seedFilterMode: "strict",
    strictCoverageThreshold: MIN_STRICT_SEED_COVERAGE,
    strictEligibleSeeds: 0,
    selectedEligibleSeeds: 0,
    missingSnapshotBacklog: 0,
    attemptedByCategory: buildScannerAuditCategoryCounter(0),
    refreshedByCategory: buildScannerAuditCategoryCounter(0),
    errors: [],
    ...overrides
  }
}

async function mapWithConcurrency(items = [], concurrencyLimit = 1, mapper = async () => null) {
  const source = Array.isArray(items) ? items : []
  if (!source.length) return []

  const limit = Math.max(Number(concurrencyLimit || 0), 1)
  const results = new Array(source.length)
  let index = 0

  async function worker() {
    while (true) {
      const current = index
      index += 1
      if (current >= source.length) return
      results[current] = await mapper(source[current], current)
    }
  }

  const workers = Array.from({ length: Math.min(limit, source.length) }, () => worker())
  await Promise.all(workers)
  return results
}

function hasUsableItemImage(inputItem = {}) {
  return Boolean(
    sanitizeImageUrl(inputItem?.itemImageUrlLarge) || sanitizeImageUrl(inputItem?.itemImageUrl)
  )
}

function applyImageToInputItem(inputItem = {}, image = {}) {
  if (!inputItem || typeof inputItem !== "object") return false
  const imageUrl = sanitizeImageUrl(image?.imageUrl) || null
  const imageUrlLarge = sanitizeImageUrl(image?.imageUrlLarge) || imageUrl || null
  const currentImage = sanitizeImageUrl(inputItem?.itemImageUrl)
  const currentImageLarge = sanitizeImageUrl(inputItem?.itemImageUrlLarge) || currentImage || null

  let changed = false
  if (!currentImage && imageUrl) {
    inputItem.itemImageUrl = imageUrl
    changed = true
  }
  if (!currentImageLarge && imageUrlLarge) {
    inputItem.itemImageUrlLarge = imageUrlLarge
    changed = true
  }
  return changed
}

function toSkinImageUpsertRows(imageByName = {}) {
  const rows = []
  for (const [marketHashName, image] of Object.entries(imageByName || {})) {
    const normalizedName = normalizeMarketHashName(marketHashName)
    if (!normalizedName) continue
    const imageUrl = sanitizeImageUrl(image?.imageUrl) || null
    const imageUrlLarge = sanitizeImageUrl(image?.imageUrlLarge) || imageUrl || null
    if (!imageUrl && !imageUrlLarge) continue
    rows.push({
      market_hash_name: normalizedName,
      image_url: imageUrl || imageUrlLarge,
      image_url_large: imageUrlLarge || imageUrl
    })
  }
  return rows
}

function buildDefaultImageEnrichmentSummary() {
  return {
    enabled: IMAGE_ENRICH_BATCH_SIZE > 0,
    missingBefore: 0,
    missingAfter: 0,
    fromMarketPayload: 0,
    steamSearchAttempted: 0,
    fromSteamSearch: 0,
    persistedRows: 0,
    errors: []
  }
}

async function hydrateUniverseImages(inputByName = {}, comparisonItems = []) {
  const summary = buildDefaultImageEnrichmentSummary()
  const inputItems = Object.values(inputByName || {})
  if (!inputItems.length) return summary

  summary.missingBefore = inputItems.filter((item) => !hasUsableItemImage(item)).length
  if (!summary.missingBefore) return summary

  const imageUpdatesByName = {}
  for (const comparisonItem of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const marketHashName = normalizeMarketHashName(comparisonItem?.marketHashName)
    if (!marketHashName) continue
    const inputItem = inputByName[marketHashName] || null
    if (!inputItem || hasUsableItemImage(inputItem)) continue

    for (const quote of Array.isArray(comparisonItem?.perMarket) ? comparisonItem.perMarket : []) {
      const image = marketImageService.pickImageFromMarketRow(quote)
      if (!image?.imageUrl && !image?.imageUrlLarge) continue
      const changed = applyImageToInputItem(inputItem, image)
      if (!changed) continue
      imageUpdatesByName[marketHashName] = {
        imageUrl: String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() || null,
        imageUrlLarge:
          String(inputItem?.itemImageUrlLarge || image?.imageUrlLarge || "").trim() ||
          String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() ||
          null
      }
      summary.fromMarketPayload += 1
      break
    }
  }

  const missingAfterPayload = inputItems.filter((item) => !hasUsableItemImage(item))
  if (IMAGE_ENRICH_BATCH_SIZE > 0 && missingAfterPayload.length) {
    const targetNames = missingAfterPayload
      .slice()
      .sort(
        (a, b) =>
          Number(b?.marketVolume7d || 0) - Number(a?.marketVolume7d || 0) ||
          Number(b?.referencePrice || 0) - Number(a?.referencePrice || 0)
      )
      .slice(0, IMAGE_ENRICH_BATCH_SIZE)
      .map((row) => normalizeMarketHashName(row?.marketHashName))
      .filter(Boolean)

    summary.steamSearchAttempted = targetNames.length
    if (targetNames.length) {
      try {
        const steamImagesByName = await marketImageService.fetchSteamSearchImagesBatch(targetNames, {
          concurrency: IMAGE_ENRICH_CONCURRENCY,
          timeoutMs: IMAGE_ENRICH_TIMEOUT_MS,
          maxRetries: 2
        })
        for (const [marketHashName, image] of Object.entries(steamImagesByName || {})) {
          const inputItem = inputByName[marketHashName] || null
          if (!inputItem) continue
          const changed = applyImageToInputItem(inputItem, image)
          if (!changed) continue
          imageUpdatesByName[marketHashName] = {
            imageUrl: String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() || null,
            imageUrlLarge:
              String(inputItem?.itemImageUrlLarge || image?.imageUrlLarge || "").trim() ||
              String(inputItem?.itemImageUrl || image?.imageUrl || "").trim() ||
              null
          }
          summary.fromSteamSearch += 1
        }
      } catch (err) {
        summary.errors.push(String(err?.message || "steam_image_search_failed"))
      }
    }
  }

  const upsertRows = toSkinImageUpsertRows(imageUpdatesByName)
  if (upsertRows.length) {
    try {
      const persisted = await skinRepo.upsertSkins(upsertRows)
      summary.persistedRows = Array.isArray(persisted) ? persisted.length : upsertRows.length
    } catch (err) {
      summary.errors.push(String(err?.message || "skin_image_upsert_failed"))
    }
  }

  summary.missingAfter = inputItems.filter((item) => !hasUsableItemImage(item)).length
  summary.errors = summary.errors.slice(0, 5)
  return summary
}

function mergeSeedWithSnapshot(seed = {}, snapshot = null) {
  if (!snapshot || typeof snapshot !== "object") return seed
  const referencePrice = toPositiveOrNull(
    toFiniteOrNull(snapshot?.average_7d_price) ??
      toFiniteOrNull(snapshot?.lowest_listing_price) ??
      toFiniteOrNull(seed?.referencePrice)
  )
  const marketVolume7d = toPositiveOrNull(
    toFiniteOrNull(resolveVolume7d(snapshot)) ?? toFiniteOrNull(seed?.marketVolume7d)
  )
  const hasUsableSnapshotLiquidity = referencePrice != null || marketVolume7d != null

  return {
    ...seed,
    marketVolume7d,
    liquidityScore:
      hasUsableSnapshotLiquidity ? computeLiquidityScoreFromSnapshot(snapshot) : null,
    sevenDayChangePercent:
      hasUsableSnapshotLiquidity ? resolveSevenDayChangePercent(snapshot) : null,
    referencePrice,
    hasSnapshotData: hasUsableSnapshotLiquidity,
    snapshotCapturedAt: snapshot?.captured_at || seed?.snapshotCapturedAt || null,
    snapshotStale: isSnapshotStale(snapshot)
  }
}

function selectSnapshotWarmupCandidates(candidates = [], limit = SNAPSHOT_WARMUP_MAX_ITEMS) {
  const safeCandidates = Array.isArray(candidates) ? candidates : []
  const safeLimit = Math.max(Math.round(Number(limit || 0)), 0)
  if (!safeLimit || !safeCandidates.length) return []

  const categoryTargets = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: Math.max(Math.round(safeLimit * 0.62), 1),
    [ITEM_CATEGORIES.CASE]: Math.max(Math.round(safeLimit * 0.2), 1),
    [ITEM_CATEGORIES.STICKER_CAPSULE]: Math.max(Math.round(safeLimit * 0.18), 1)
  }
  const byCategory = {
    [ITEM_CATEGORIES.WEAPON_SKIN]: [],
    [ITEM_CATEGORIES.CASE]: [],
    [ITEM_CATEGORIES.STICKER_CAPSULE]: []
  }
  for (const row of safeCandidates) {
    const category = normalizeItemCategory(row?.itemCategory, row?.marketHashName)
    if (!byCategory[category]) continue
    byCategory[category].push(row)
  }
  for (const category of Object.keys(byCategory)) {
    byCategory[category].sort(
      (a, b) =>
        Number(b?.maturityScore || 0) - Number(a?.maturityScore || 0) ||
        Number(b?.enrichmentPriority || 0) - Number(a?.enrichmentPriority || 0) ||
        Number(b?.layerPriority || 0) - Number(a?.layerPriority || 0) ||
        Number(a?.liquidityRank || Number.MAX_SAFE_INTEGER) -
          Number(b?.liquidityRank || Number.MAX_SAFE_INTEGER)
    )
  }

  const selected = []
  const used = new Set()
  const take = (row) => {
    const key = normalizeMarketHashName(row?.marketHashName)
    if (!key || used.has(key) || selected.length >= safeLimit) return false
    used.add(key)
    selected.push(row)
    return true
  }

  for (const category of SCANNER_AUDIT_CATEGORIES) {
    const target = Math.min(Number(categoryTargets[category] || 0), byCategory[category].length)
    for (let index = 0; index < target; index += 1) {
      take(byCategory[category][index])
    }
  }

  if (selected.length >= safeLimit) return selected
  for (const row of safeCandidates) {
    if (selected.length >= safeLimit) break
    take(row)
  }
  return selected
}

function summarizeSnapshotWarmupBacklog(seeds = []) {
  const rows = Array.isArray(seeds) ? seeds : []
  const freshSeedsBefore = rows.filter(
    (seed) => Boolean(seed?.hasSnapshotData) && !Boolean(seed?.snapshotStale)
  ).length
  const missingSnapshotBacklog = rows.filter((seed) => !Boolean(seed?.hasSnapshotData)).length
  const warmupCandidateRows = rows
    .filter((seed) => {
      const skinId = Number(seed?.skinId || 0)
      return Number.isInteger(skinId) && skinId > 0 && (!seed?.hasSnapshotData || seed?.snapshotStale)
    })
    .sort(
      (a, b) =>
        Number(b?.maturityScore || 0) - Number(a?.maturityScore || 0) ||
        Number(b?.enrichmentPriority || 0) - Number(a?.enrichmentPriority || 0) ||
        sourceRank(b?.universeSource) - sourceRank(a?.universeSource) ||
        Number(a?.liquidityRank || Number.MAX_SAFE_INTEGER) -
          Number(b?.liquidityRank || Number.MAX_SAFE_INTEGER)
    )

  return {
    freshSeedsBefore,
    missingSnapshotBacklog,
    warmupCandidateRows,
    warmupCandidates: warmupCandidateRows.length,
    warmupCandidatesByCategory: countRowsByCategory(
      warmupCandidateRows.map((row) => ({
        itemCategory: row?.itemCategory,
        itemName: row?.marketHashName
      }))
    )
  }
}

function isOpportunityScanReadySeed(seed = {}) {
  return evaluateOpportunitySeedAdmission(seed).ready
}

async function refreshSeedSnapshotsIfNeeded(seeds = [], options = {}) {
  const rows = Array.isArray(seeds) ? seeds : []
  if (!rows.length) {
    return {
      seeds: rows,
      snapshotWarmup: toSnapshotWarmupSummary()
    }
  }
  const backlog = summarizeSnapshotWarmupBacklog(rows)
  const freshSeedsBefore = backlog.freshSeedsBefore
  const missingSnapshotBacklog = backlog.missingSnapshotBacklog
  const warmupCandidates = backlog.warmupCandidateRows

  if (!warmupCandidates.length) {
    return {
      seeds: rows,
      snapshotWarmup: toSnapshotWarmupSummary({
        freshSeedsBefore,
        warmupCandidates: backlog.warmupCandidates,
        warmupCandidatesByCategory: toScannerAuditCategoryCounts(
          backlog.warmupCandidatesByCategory
        ),
        missingSnapshotBacklog
      })
    }
  }

  const hotLayerCount = rows.filter((seed) => String(seed?.scanLayer || "").trim().toLowerCase() === SCAN_LAYERS.HOT).length
  const backlogBoost =
    missingSnapshotBacklog >= 400 || warmupCandidates.length >= 500
      ? 2
      : missingSnapshotBacklog >= 180
        ? 1.5
        : 1
  const explicitWarmupLimit = Number(options?.warmupLimit || 0)
  const warmupLimit =
    explicitWarmupLimit > 0
      ? Math.max(Math.round(explicitWarmupLimit), 5)
      : freshSeedsBefore < Math.max(SNAPSHOT_WARMUP_TRIGGER_FRESH_MIN, Math.round(hotLayerCount * 0.65))
        ? Math.max(Math.round(SNAPSHOT_WARMUP_MAX_ITEMS * backlogBoost), SNAPSHOT_WARMUP_MAX_ITEMS)
        : Math.max(Math.round((SNAPSHOT_WARMUP_MAX_ITEMS / 2) * backlogBoost), 5)
  const selected = selectSnapshotWarmupCandidates(warmupCandidates, warmupLimit)
  const errors = []
  const refreshedSkinIds = []
  const attemptedByCategory = buildScannerAuditCategoryCounter(0)
  const refreshedByCategory = buildScannerAuditCategoryCounter(0)
  const results = await mapWithConcurrency(
    selected,
    SNAPSHOT_WARMUP_CONCURRENCY,
    async (seed) => {
      const skinId = Number(seed?.skinId || 0)
      const category = normalizeItemCategory(seed?.itemCategory, seed?.marketHashName)
      if (attemptedByCategory[category] == null) {
        attemptedByCategory[category] = 0
      }
      attemptedByCategory[category] += 1
      try {
        await marketService.getLiquidityScore(skinId)
        refreshedSkinIds.push(skinId)
        if (refreshedByCategory[category] == null) {
          refreshedByCategory[category] = 0
        }
        refreshedByCategory[category] += 1
        return true
      } catch (err) {
        errors.push(String(err?.message || `snapshot_refresh_failed:${skinId}`))
        return false
      }
    }
  )

  const refreshedCount = results.filter((ok) => Boolean(ok)).length
  const snapshotsBySkinId = refreshedSkinIds.length
    ? await marketSnapshotRepo.getLatestBySkinIds(refreshedSkinIds)
    : {}

  const refreshedBySkinId = {}
  for (const skinId of refreshedSkinIds) {
    if (snapshotsBySkinId[skinId]) {
      refreshedBySkinId[skinId] = snapshotsBySkinId[skinId]
    }
  }

  const hydratedSeeds = rows.map((seed) => {
    const skinId = Number(seed?.skinId || 0)
    return mergeSeedWithSnapshot(seed, refreshedBySkinId[skinId] || null)
  })

  return {
    seeds: hydratedSeeds,
    snapshotWarmup: toSnapshotWarmupSummary({
      triggered: true,
      reason: "insufficient_fresh_snapshot_coverage",
      freshSeedsBefore,
      warmupCandidates: backlog.warmupCandidates,
      warmupCandidatesByCategory: toScannerAuditCategoryCounts(
        backlog.warmupCandidatesByCategory
      ),
      missingSnapshotBacklog,
      attemptedItems: selected.length,
      refreshedItems: refreshedCount,
      failedItems: Math.max(selected.length - refreshedCount, 0),
      attemptedByCategory,
      refreshedByCategory,
      errors: errors.slice(0, 8)
    })
  }
}

function countAvailableQuotes(items = []) {
  let total = 0
  for (const item of Array.isArray(items) ? items : []) {
    for (const row of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const source = normalizeMarketLabel(row?.source || row?.market)
      if (!source) continue
      const hasGross = Number.isFinite(Number(row?.grossPrice)) && Number(row.grossPrice) > 0
      const hasNet =
        Number.isFinite(Number(row?.netPriceAfterFees)) && Number(row.netPriceAfterFees) > 0
      if (Boolean(row?.available) && (hasGross || hasNet)) {
        total += 1
      }
    }
  }
  return total
}

function isBatchTimeoutError(err = {}) {
  return String(err?.code || "")
    .trim()
    .toLowerCase() === "scan_batch_timeout"
}

async function compareBatchItems(batch = [], options = {}) {
  const allowLiveFetch = options.allowLiveFetch !== false
  const compareTimeoutMs = allowLiveFetch
    ? Math.max(SCAN_TIMEOUT_PER_BATCH + 5000, SCAN_TIMEOUT_PER_BATCH)
    : SCAN_TIMEOUT_PER_BATCH
  const comparePromise = marketComparisonService.compareItems(batch, {
    currency: "USD",
    pricingMode: "lowest_buy",
    allowLiveFetch,
    forceRefresh: Boolean(options.forceRefresh),
    userId: null,
    concurrency: MAX_CONCURRENT_MARKET_REQUESTS,
    timeoutMs: SCAN_TIMEOUT_PER_BATCH
  })

  let timeoutId = null
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      const timeoutErr = new Error(`scan_batch_timeout_${compareTimeoutMs}ms`)
      timeoutErr.code = "scan_batch_timeout"
      reject(timeoutErr)
    }, compareTimeoutMs)
  })

  try {
    return await Promise.race([comparePromise, timeoutPromise])
  } finally {
    clearTimeout(timeoutId)
  }
}

async function refreshQuotesInBatches(inputItems = [], options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const batches = chunkArray(inputItems, SCAN_BATCH_SIZE)
  let itemsCompared = 0
  let availableQuotes = 0
  let completedBatches = 0
  let timedOutBatches = 0
  let totalBatchMs = 0
  let slowestBatchMs = 0
  const errors = []

  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index]
    const startedAt = Date.now()
    try {
      const comparison = await compareBatchItems(batch, {
        allowLiveFetch: true,
        forceRefresh
      })
      const comparedItems = Array.isArray(comparison?.items) ? comparison.items : []
      itemsCompared += comparedItems.length
      availableQuotes += countAvailableQuotes(comparedItems)
      completedBatches += 1
    } catch (err) {
      if (isBatchTimeoutError(err)) {
        timedOutBatches += 1
      }
      errors.push(`[batch ${index + 1}] ${String(err?.message || "quote_batch_failed")}`)
    } finally {
      const batchMs = Math.max(Date.now() - startedAt, 0)
      totalBatchMs += batchMs
      slowestBatchMs = Math.max(slowestBatchMs, batchMs)
    }
  }

  return {
    batchSize: SCAN_BATCH_SIZE,
    timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
    maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
    requestedItems: inputItems.length,
    totalBatches: batches.length,
    completedBatches,
    failedBatches: errors.length,
    timedOutBatches,
    itemsCompared,
    availableQuotes,
    averageBatchMs: round2(totalBatchMs / Math.max(batches.length, 1)),
    slowestBatchMs,
    errors: errors.slice(0, 5)
  }
}

async function compareFromSavedQuotes(inputItems = []) {
  const batches = chunkArray(inputItems, SCAN_BATCH_SIZE)
  const items = []
  let itemsCompared = 0
  let availableQuotes = 0
  let completedBatches = 0
  let timedOutBatches = 0
  let totalBatchMs = 0
  let slowestBatchMs = 0
  const errors = []
  let currency = "USD"

  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index]
    const startedAt = Date.now()
    try {
      const comparison = await compareBatchItems(batch, {
        allowLiveFetch: false,
        forceRefresh: false
      })
      const comparedItems = Array.isArray(comparison?.items) ? comparison.items : []
      items.push(...comparedItems)
      itemsCompared += comparedItems.length
      availableQuotes += countAvailableQuotes(comparedItems)
      completedBatches += 1
      currency = String(comparison?.currency || currency)
        .trim()
        .toUpperCase()
    } catch (err) {
      if (isBatchTimeoutError(err)) {
        timedOutBatches += 1
      }
      errors.push(`[batch ${index + 1}] ${String(err?.message || "compare_saved_quotes_failed")}`)
    } finally {
      const batchMs = Math.max(Date.now() - startedAt, 0)
      totalBatchMs += batchMs
      slowestBatchMs = Math.max(slowestBatchMs, batchMs)
    }
  }

  return {
    currency,
    items,
    diagnostics: {
      batchSize: SCAN_BATCH_SIZE,
      timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
      maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
      requestedItems: inputItems.length,
      totalBatches: batches.length,
      completedBatches,
      failedBatches: errors.length,
      timedOutBatches,
      itemsCompared,
      availableQuotes,
      averageBatchMs: round2(totalBatchMs / Math.max(batches.length, 1)),
      slowestBatchMs,
      errors: errors.slice(0, 5)
    }
  }
}

function buildDefaultEnrichmentPipelineSummary() {
  return {
    attempted: false,
    requestedItems: 0,
    enrichedItems: 0,
    durationMs: 0,
    timingMs: {
      quoteFetchingMs: 0,
      dbWritesMs: 0
    },
    selectedLayers: buildLayerCounter(0),
    selectedLayersByCategory: buildLayerByCategoryCounter(0),
    quoteRefresh: {
      batchSize: SCAN_BATCH_SIZE,
      timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
      maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
      requestedItems: 0,
      totalBatches: 0,
      completedBatches: 0,
      failedBatches: 0,
      timedOutBatches: 0,
      itemsCompared: 0,
      availableQuotes: 0,
      averageBatchMs: 0,
      slowestBatchMs: 0,
      errors: []
    },
    computeFromSavedQuotes: {
      batchSize: SCAN_BATCH_SIZE,
      timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
      maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
      requestedItems: 0,
      totalBatches: 0,
      completedBatches: 0,
      failedBatches: 0,
      timedOutBatches: 0,
      itemsCompared: 0,
      availableQuotes: 0,
      averageBatchMs: 0,
      slowestBatchMs: 0,
      errors: []
    },
    quoteSnapshot: {
      rowsPrepared: 0,
      rowsInserted: 0,
      persisted: true
    }
  }
}

async function runEnrichmentPipeline(seeds = [], options = {}) {
  const selectedSeeds = Array.isArray(seeds) ? seeds : []
  if (!selectedSeeds.length) {
    return buildDefaultEnrichmentPipelineSummary()
  }

  const pipelineStartedAt = Date.now()
  const comparisonInputItems = selectedSeeds.map((row) => buildInputItemForComparison(row))
  const inputByName = toByNameMap(
    selectedSeeds.map((row) => ({
      ...row,
      marketHashName: normalizeMarketHashName(row?.marketHashName)
    })),
    "marketHashName"
  )
  const quoteFetchingStartedAt = Date.now()
  const quoteRefresh = await refreshQuotesInBatches(comparisonInputItems, {
    forceRefresh: Boolean(options.forceRefresh)
  })
  const compareFromSaved = await compareFromSavedQuotes(comparisonInputItems)
  const quoteFetchingMs = Date.now() - quoteFetchingStartedAt
  const dbWritesStartedAt = Date.now()
  const quoteSnapshot = await persistQuoteSnapshot(compareFromSaved.items, inputByName)
  const dbWritesMs = Date.now() - dbWritesStartedAt
  const layerDiagnostics = buildLayerDiagnostics(selectedSeeds)

  return {
    attempted: true,
    requestedItems: selectedSeeds.length,
    enrichedItems: Number(compareFromSaved?.diagnostics?.itemsCompared || 0),
    durationMs: Date.now() - pipelineStartedAt,
    timingMs: {
      quoteFetchingMs,
      dbWritesMs
    },
    selectedLayers: layerDiagnostics.layers,
    selectedLayersByCategory: layerDiagnostics.layersByCategory,
    quoteRefresh,
    computeFromSavedQuotes: compareFromSaved?.diagnostics || {},
    quoteSnapshot
  }
}

function buildQuoteSnapshotRows(comparisonItems = [], inputByName = {}) {
  const rows = []
  const nowIso = new Date().toISOString()

  for (const item of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const marketHashName = normalizeMarketHashName(item?.marketHashName)
    if (!marketHashName) continue
    const inputItem = inputByName[marketHashName] || null
    const volume7d = toFiniteOrNull(inputItem?.marketVolume7d)
    const liquidityScore = toFiniteOrNull(inputItem?.liquidityScore)

    for (const quote of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const market = normalizeMarketLabel(quote?.source || quote?.market)
      if (!market || !SOURCE_ORDER.includes(market)) continue
      const gross = toFiniteOrNull(quote?.grossPrice)
      const sellNet = toFiniteOrNull(quote?.netPriceAfterFees)
      rows.push({
        item_name: marketHashName,
        market,
        best_buy: gross,
        best_sell: gross,
        best_sell_net: sellNet,
        volume_7d: volume7d,
        liquidity_score: liquidityScore,
        fetched_at: quote?.updatedAt || nowIso,
        quality_flags: {
          available: Boolean(quote?.available),
          confidence: String(quote?.confidence || "low").trim() || "low",
          unavailable_reason: String(quote?.unavailableReason || "").trim() || null,
          quote_age_minutes: resolveQuoteAgeMinutes(quote),
          snapshot_stale: Boolean(inputItem?.snapshotStale),
          universe_source: String(inputItem?.universeSource || "").trim() || "fallback_curated"
        }
      })
    }
  }

  return rows
}

async function persistQuoteSnapshot(comparisonItems = [], inputByName = {}) {
  const rows = buildQuoteSnapshotRows(comparisonItems, inputByName)
  if (!rows.length) {
    return {
      rowsPrepared: 0,
      rowsInserted: 0,
      persisted: true
    }
  }

  try {
    const rowsInserted = await marketQuoteRepo.insertRows(rows)
    return {
      rowsPrepared: rows.length,
      rowsInserted,
      persisted: true
    }
  } catch (err) {
    console.error("[arbitrage-scanner] Failed to persist scanner quote snapshot", err.message)
    return {
      rowsPrepared: rows.length,
      rowsInserted: 0,
      persisted: false,
      error: String(err?.message || "quote_snapshot_persist_failed")
    }
  }
}

function selectTopUniverseItems(
  comparisonItems = [],
  inputByName = {},
  discardStats = {},
  rejectedByItem = {}
) {
  const ranked = []
  for (const comparisonItem of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const name = normalizeMarketHashName(comparisonItem?.marketHashName)
    const inputItem = inputByName[name] || null
    if (!inputItem) continue
    const itemCategory = normalizeItemCategory(inputItem?.itemCategory, name)
    const marketCoverage = countAvailableMarkets(comparisonItem?.perMarket)
    if (marketCoverage <= 0) {
      incrementReasonCounter(discardStats, "ignored_missing_markets", itemCategory)
      incrementItemReasonCounter(
        rejectedByItem,
        name || inputItem?.marketHashName,
        "ignored_missing_markets",
        itemCategory
      )
      continue
    }
    const rank = computeLiquidityRank({
      volume7d: inputItem.marketVolume7d,
      marketCoverage,
      sevenDayChangePercent: inputItem.sevenDayChangePercent,
      referencePrice: inputItem.referencePrice,
      itemCategory: inputItem.itemCategory
    })
    ranked.push({
      inputItem,
      comparisonItem,
      marketCoverage,
      ...rank
    })
  }

  return ranked
    .sort(
      (a, b) => {
        const curatedRankA = toFiniteOrNull(a.inputItem?.liquidityRank)
        const curatedRankB = toFiniteOrNull(b.inputItem?.liquidityRank)
        const curatedRankDelta =
          curatedRankA == null || curatedRankB == null ? 0 : curatedRankA - curatedRankB
        return (
        Number(b.liquidityRank || 0) - Number(a.liquidityRank || 0) ||
        sourceRank(b.inputItem?.universeSource) - sourceRank(a.inputItem?.universeSource) ||
        curatedRankDelta ||
        Number(b.inputItem?.marketVolume7d || 0) - Number(a.inputItem?.marketVolume7d || 0)
        )
      }
    )
    .slice(0, UNIVERSE_TARGET_SIZE)
}

function countRowsByCategory(rows = []) {
  const counts = Object.fromEntries(
    Object.values(ITEM_CATEGORIES).map((category) => [category, 0])
  )
  for (const row of Array.isArray(rows) ? rows : []) {
    const category = normalizeItemCategory(row?.itemCategory, row?.itemName)
    counts[category] = Number(counts[category] || 0) + 1
  }
  return counts
}

function buildHotScanSummary({
  layeredScanning = {},
  opportunitySeeds = [],
  selectedUniverseByCategory = {},
  opportunitiesByCategory = {},
  qualifiedItems = 0,
  opportunitiesFound = 0,
  persisted = {},
  snapshotWarmup = {}
} = {}) {
  const hotUniverse =
    layeredScanning?.hotUniverse && typeof layeredScanning.hotUniverse === "object"
      ? layeredScanning.hotUniverse
      : {}
  const hotItemsByCategory = countRowsByCategory(
    (Array.isArray(opportunitySeeds) ? opportunitySeeds : []).map((row) => ({
      itemCategory: row?.itemCategory,
      itemName: row?.marketHashName
    }))
  )
  const hotUniverseByCategory = toScannerAuditCategoryCounts(hotUniverse?.itemsByCategory || {})
  const queuedByCategory = toScannerAuditCategoryCounts(hotItemsByCategory)
  const qualifiedByCategory = toScannerAuditCategoryCounts(selectedUniverseByCategory)
  const foundByCategory = toScannerAuditCategoryCounts(opportunitiesByCategory)

  return {
    target: Number(layeredScanning?.opportunityTarget || opportunitySeeds.length || 0),
    hotUniverseSize: Number(hotUniverse?.total || 0),
    eligibleCount: Number(hotUniverse?.eligibleCount || 0),
    nearEligibleCount: Number(hotUniverse?.nearEligibleCount || 0),
    matureOnlySelection: Boolean(layeredScanning?.matureOnlyOpportunitySelection),
    deferredToEnrichmentItems: Number(
      layeredScanning?.deferredToEnrichmentItems ||
        snapshotWarmup?.deferredToEnrichmentItems ||
        0
    ),
    deferredToEnrichmentByCategory: toScannerAuditCategoryCounts(
      layeredScanning?.deferredToEnrichmentByCategory ||
        snapshotWarmup?.deferredToEnrichmentByCategory ||
        {}
    ),
    itemsQueued: Number(opportunitySeeds?.length || 0),
    itemsQueuedByCategory: queuedByCategory,
    qualifiedItems: Number(qualifiedItems || 0),
    qualifiedItemsByCategory: qualifiedByCategory,
    opportunitiesFound: Number(opportunitiesFound || 0),
    opportunitiesByCategory: foundByCategory,
    newOpportunitiesAdded: Number(persisted?.newCount || 0),
    updatedOpportunities: Number(persisted?.updatedCount || 0),
    reactivatedOpportunities: Number(persisted?.reactivatedCount || 0),
    selectedEligibleForOpportunity: Number(layeredScanning?.selectedEligibleForOpportunity || 0),
    selectedNearEligibleForOpportunity: Number(
      layeredScanning?.selectedNearEligibleForOpportunity || 0
    ),
    selectedEnrichingForOpportunity: Number(
      layeredScanning?.selectedEnrichingForOpportunity || 0
    ),
    categoryContribution: {
      hotUniverse: hotUniverseByCategory,
      hotItems: queuedByCategory,
      qualifiedItems: qualifiedByCategory,
      opportunities: foundByCategory
    }
  }
}

function toRejectedCountByCategory(discardedReasonsByCategory = {}) {
  const counts = Object.fromEntries(
    Object.values(ITEM_CATEGORIES).map((category) => [category, 0])
  )
  for (const [category, payload] of Object.entries(discardedReasonsByCategory || {})) {
    const normalizedCategory = normalizeItemCategory(category)
    counts[normalizedCategory] = Number(payload?.totalRejected || 0)
  }
  return counts
}

function toKnifeGloveRejectionSummary(discardedReasonsByCategory = {}) {
  const reasonMapByCategory = {
    [ITEM_CATEGORIES.KNIFE]: discardedReasonsByCategory?.[ITEM_CATEGORIES.KNIFE]?.reasons || {},
    [ITEM_CATEGORIES.GLOVE]: discardedReasonsByCategory?.[ITEM_CATEGORIES.GLOVE]?.reasons || {}
  }

  const result = {}
  for (const [category, reasons] of Object.entries(reasonMapByCategory)) {
    result[category] = {
      low_liquidity: Number(reasons.ignored_low_liquidity || 0),
      extreme_spread: Number(reasons.ignored_extreme_spread || 0),
      stale_market_data: Number(reasons.ignored_stale_data || 0),
      missing_depth: Number(reasons.ignored_missing_depth || 0) + Number(reasons.risky_missing_depth || 0),
      reference_deviation: Number(reasons.ignored_reference_deviation || 0),
      weak_market_coverage: Number(reasons.ignored_missing_markets || 0)
    }
  }
  return result
}

function resolveEffectiveRiskyThresholdProfile(
  itemCategory = ITEM_CATEGORIES.WEAPON_SKIN,
  options = {}
) {
  const useLegacy = Boolean(options?.legacy)
  const category = normalizeItemCategory(itemCategory)
  const profile = useLegacy ? getLegacyCategoryRiskyProfile(category) : getCategoryRiskyProfile(category)
  const floor = useLegacy ? getLegacyRiskyQualityFloor(category) : getRiskyQualityFloor(category)
  const rules = getCategoryScanRules(category, profile?.name || "strict")

  return {
    rulePath: useLegacy ? "legacy_strict_fallback" : "category_risky_rules",
    minPriceUsd: Number(rules.minPriceUsd ?? profile?.minPriceUsd ?? 0),
    minProfitUsd: Number(rules.minProfitUsd ?? profile?.minProfitUsd ?? 0),
    minSpreadPercent: Number(rules.minSpreadPercent ?? profile?.minSpreadPercent ?? 0),
    maxSpreadPercent: Number(rules.maxSpreadPercent ?? profile?.maxSpreadPercent ?? MAX_SPREAD_PERCENT),
    minVolume7d: Number(rules.minVolume7d ?? profile?.minVolume7d ?? 0),
    minMarketCoverage: Number(rules.minMarketCoverage ?? profile?.minMarketCoverage ?? MIN_MARKET_COVERAGE),
    minScore: Number(floor?.minScore ?? profile?.minScore ?? RISKY_MIN_SCORE),
    qualityFloorProfitUsd: Number(floor?.minProfitUsd ?? profile?.minProfitUsd ?? 0),
    allowMissingLiquidity: Boolean(profile?.allowMissingLiquidity),
    allowMissingDepthWithPenalty: Boolean(profile?.allowMissingDepthWithPenalty),
    allowBorderlinePromotion: Boolean(profile?.allowBorderlinePromotion)
  }
}

function toTopReasonCounts(reasonMap = {}, limit = 3) {
  return Object.entries(reasonMap || {})
    .map(([reason, count]) => ({
      reason: String(reason || "").trim(),
      count: Number(count || 0)
    }))
    .filter((entry) => entry.reason && entry.count > 0)
    .sort((left, right) => right.count - left.count || left.reason.localeCompare(right.reason))
    .slice(0, Math.max(Number(limit || 0), 0))
}

function buildRiskyProfileDiagnostics() {
  const diagnostics = {}
  for (const category of Object.values(ITEM_CATEGORIES)) {
    const profile = resolveEffectiveRiskyThresholdProfile(category)
    diagnostics[category] = {
      previousProfile: resolveEffectiveRiskyThresholdProfile(category, { legacy: true }),
      profile,
      attempted: 0,
      accepted: 0,
      rejected: 0,
      baselineAccepted: 0,
      baselineRejected: 0,
      additionalAcceptedVsBaseline: 0,
      borderlinePromoted: 0,
      borderlinePromotionReasons: {},
      acceptedReasons: {},
      rejectedReasons: {},
      topRejectedReasons: [],
      topBorderlinePromotionReasons: []
    }
  }
  return diagnostics
}

function trackRiskyDecision(
  diagnostics = {},
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  status = "attempted",
  reason = ""
) {
  const normalizedCategory = normalizeItemCategory(category)
  if (!diagnostics[normalizedCategory]) {
    diagnostics[normalizedCategory] = {
      previousProfile: resolveEffectiveRiskyThresholdProfile(normalizedCategory, { legacy: true }),
      profile: resolveEffectiveRiskyThresholdProfile(normalizedCategory),
      attempted: 0,
      accepted: 0,
      rejected: 0,
      baselineAccepted: 0,
      baselineRejected: 0,
      additionalAcceptedVsBaseline: 0,
      borderlinePromoted: 0,
      borderlinePromotionReasons: {},
      acceptedReasons: {},
      rejectedReasons: {},
      topRejectedReasons: [],
      topBorderlinePromotionReasons: []
    }
  }
  const bucket = diagnostics[normalizedCategory]
  if (status === "attempted") {
    bucket.attempted += 1
    return
  }
  if (status === "accepted") {
    bucket.accepted += 1
    if (reason) {
      bucket.acceptedReasons[reason] = Number(bucket.acceptedReasons[reason] || 0) + 1
    }
    return
  }
  bucket.rejected += 1
  if (reason) {
    bucket.rejectedReasons[reason] = Number(bucket.rejectedReasons[reason] || 0) + 1
  }
}

function trackRiskyBaselineOutcome(
  diagnostics = {},
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  accepted = false
) {
  const normalizedCategory = normalizeItemCategory(category)
  const bucket = diagnostics[normalizedCategory]
  if (!bucket) return
  if (accepted) {
    bucket.baselineAccepted = Number(bucket.baselineAccepted || 0) + 1
    return
  }
  bucket.baselineRejected = Number(bucket.baselineRejected || 0) + 1
}

function trackRiskyBorderlinePromotion(
  diagnostics = {},
  category = ITEM_CATEGORIES.WEAPON_SKIN,
  promotionKeys = []
) {
  const normalizedCategory = normalizeItemCategory(category)
  const bucket = diagnostics[normalizedCategory]
  if (!bucket) return
  const keys = Array.from(
    new Set(
      (Array.isArray(promotionKeys) ? promotionKeys : [])
        .map((key) => String(key || "").trim())
        .filter(Boolean)
    )
  )
  if (!keys.length) return
  bucket.borderlinePromoted = Number(bucket.borderlinePromoted || 0) + 1
  for (const key of keys) {
    bucket.borderlinePromotionReasons[key] = Number(bucket.borderlinePromotionReasons[key] || 0) + 1
  }
}

function toRiskyProfileDiagnosticsSummary(diagnostics = {}) {
  const result = {}
  for (const category of Object.values(ITEM_CATEGORIES)) {
    const bucket = diagnostics?.[category] || {}
    result[category] = {
      previousProfile: bucket.previousProfile || resolveEffectiveRiskyThresholdProfile(category, { legacy: true }),
      profile: bucket.profile || resolveEffectiveRiskyThresholdProfile(category),
      attempted: Number(bucket.attempted || 0),
      accepted: Number(bucket.accepted || 0),
      rejected: Number(bucket.rejected || 0),
      baselineAccepted: Number(bucket.baselineAccepted || 0),
      baselineRejected: Number(bucket.baselineRejected || 0),
      additionalAcceptedVsBaseline: Number(bucket.additionalAcceptedVsBaseline || 0),
      borderlinePromoted: Number(bucket.borderlinePromoted || 0),
      borderlinePromotionReasons: bucket.borderlinePromotionReasons || {},
      acceptedReasons: bucket.acceptedReasons || {},
      rejectedReasons: bucket.rejectedReasons || {},
      topRejectedReasons: toTopReasonCounts(bucket.rejectedReasons, 3),
      topBorderlinePromotionReasons: toTopReasonCounts(bucket.borderlinePromotionReasons, 3)
    }
  }
  return result
}

function buildRiskyThresholdDiagnosticsSummary(diagnostics = {}) {
  const byCategory = toRiskyProfileDiagnosticsSummary(diagnostics)
  const aggregateRejectedReasons = {}
  let additionalAcceptedVsBaseline = 0
  let borderlinePromoted = 0
  for (const category of Object.values(ITEM_CATEGORIES)) {
    const bucket = byCategory[category] || {}
    additionalAcceptedVsBaseline += Number(bucket.additionalAcceptedVsBaseline || 0)
    borderlinePromoted += Number(bucket.borderlinePromoted || 0)
    for (const [reason, count] of Object.entries(bucket.rejectedReasons || {})) {
      aggregateRejectedReasons[reason] = Number(aggregateRejectedReasons[reason] || 0) + Number(count || 0)
    }
  }
  return {
    additionalAcceptedVsBaseline,
    borderlinePromoted,
    topRejectedReasons: toTopReasonCounts(aggregateRejectedReasons, 5)
  }
}

function buildScanProgressStats({
  universeTarget = 0,
  candidateItems = 0,
  requestedItems = 0,
  qualifiedItems = 0,
  scannedItems = 0,
  quoteRefresh = {},
  computeFromSavedQuotes = {}
} = {}) {
  const refreshBatches = Number(quoteRefresh?.totalBatches || 0)
  const computeBatches = Number(computeFromSavedQuotes?.totalBatches || 0)
  const totalBatches = refreshBatches + computeBatches
  const completedBatches =
    Number(quoteRefresh?.completedBatches || 0) +
    Number(computeFromSavedQuotes?.completedBatches || 0)
  const failedBatches =
    Number(quoteRefresh?.failedBatches || 0) +
    Number(computeFromSavedQuotes?.failedBatches || 0)
  const timedOutBatches =
    Number(quoteRefresh?.timedOutBatches || 0) +
    Number(computeFromSavedQuotes?.timedOutBatches || 0)

  return {
    universeTarget: Number(universeTarget || 0),
    candidateItems: Number(candidateItems || 0),
    requestedItems: Number(requestedItems || 0),
    qualifiedItems: Number(qualifiedItems || 0),
    scannedItems: Number(scannedItems || 0),
    batchSize: SCAN_BATCH_SIZE,
    maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
    timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
    totalBatches,
    completedBatches,
    failedBatches,
    timedOutBatches,
    completionPercent:
      totalBatches > 0 ? round2((completedBatches / Math.max(totalBatches, 1)) * 100) : 100
  }
}

function buildBatchSizingDiagnostics({
  scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN,
  runDurationMs = 0,
  scannedItems = 0,
  qualifiedItems = 0,
  opportunitiesFound = 0,
  selectedItems = 0,
  itemsUpdated = 0,
  snapshotWarmup = {},
  layeredScanning = {}
} = {}) {
  return {
    scannerType: String(scannerType || SCANNER_TYPES.OPPORTUNITY_SCAN),
    configuredEnrichmentBatchSize: Number(ENRICHMENT_BATCH_SIZE || 0),
    configuredOpportunityBatchCeiling: Number(OPPORTUNITY_BATCH_SIZE || 0),
    configuredHotOpportunityScanTarget: Number(HOT_OPPORTUNITY_SCAN_TARGET || 0),
    configuredOpportunityScanBatchSize: Number(Math.max(OPPORTUNITY_BATCH_SIZE, 1)),
    runDurationMs: toAuditDurationMs(runDurationMs),
    scannedItems: Number(scannedItems || 0),
    qualifiedItems: Number(qualifiedItems || 0),
    opportunitiesFound: Number(opportunitiesFound || 0),
    selectedItems: Number(selectedItems || 0),
    itemsUpdated: Number(itemsUpdated || 0),
    warmupMovedOutOfOpportunityScan:
      String(scannerType || "").trim().toLowerCase() === SCANNER_TYPES.OPPORTUNITY_SCAN &&
      Boolean(snapshotWarmup?.movedToEnrichment),
    deferredToEnrichmentItems: Number(
      layeredScanning?.deferredToEnrichmentItems ||
        snapshotWarmup?.deferredToEnrichmentItems ||
        0
    ),
    matureOnlyOpportunitySelection: Boolean(layeredScanning?.matureOnlyOpportunitySelection)
  }
}

function toAuditDurationMs(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 0
  return Math.max(Math.round(parsed), 0)
}

function buildScannerAuditCategoryCounter(initialValue = 0) {
  const initial = Math.max(Number(initialValue || 0), 0)
  return Object.fromEntries(SCANNER_AUDIT_CATEGORIES.map((category) => [category, initial]))
}

function toScannerAuditCategoryCounts(counter = {}, nestedField = "") {
  const counts = buildScannerAuditCategoryCounter(0)
  const source = counter && typeof counter === "object" ? counter : {}
  for (const category of SCANNER_AUDIT_CATEGORIES) {
    const value = source?.[category]
    if (nestedField && value && typeof value === "object") {
      counts[category] = Math.max(Number(value?.[nestedField] || 0), 0)
      continue
    }
    counts[category] = Math.max(Number(value || 0), 0)
  }
  return counts
}

function resolveSourceCatalogTotalRows(sourceCatalogDiagnostics = {}) {
  const sourceCatalog =
    sourceCatalogDiagnostics?.sourceCatalog && typeof sourceCatalogDiagnostics.sourceCatalog === "object"
      ? sourceCatalogDiagnostics.sourceCatalog
      : {}
  const totalRows = Number(sourceCatalog?.totalRows || 0)
  if (totalRows > 0) return totalRows
  return Math.max(Number(sourceCatalog?.activeCatalogRows || 0), 0)
}

function resolveSourceCatalogEligibleByCategory(sourceCatalogDiagnostics = {}) {
  const sourceCatalog =
    sourceCatalogDiagnostics?.sourceCatalog && typeof sourceCatalogDiagnostics.sourceCatalog === "object"
      ? sourceCatalogDiagnostics.sourceCatalog
      : {}
  const direct = sourceCatalog?.eligibleRowsByCategory
  if (direct && typeof direct === "object") {
    const directCounts = toScannerAuditCategoryCounts(direct)
    const hasDirect = Object.values(directCounts).some((value) => Number(value || 0) > 0)
    if (hasDirect) return directCounts
  }

  return toScannerAuditCategoryCounts(sourceCatalog?.byCategory || {}, "eligible")
}

function deriveSourceCatalogDiagnosticsFromScan({
  sourceCatalogDiagnostics = {},
  candidateItems = 0,
  scannedItems = 0,
  candidateByCategory = {},
  selectedByCategory = {},
  universeTarget = UNIVERSE_TARGET_SIZE
} = {}) {
  const base =
    sourceCatalogDiagnostics && typeof sourceCatalogDiagnostics === "object"
      ? sourceCatalogDiagnostics
      : {}
  const sourceCatalog =
    base?.sourceCatalog && typeof base.sourceCatalog === "object" ? base.sourceCatalog : {}
  const universeBuild =
    base?.universeBuild && typeof base.universeBuild === "object" ? base.universeBuild : {}
  const hasUniverseCoverage = Number(universeBuild?.activeUniverseBuilt || 0) > 0
  if (hasUniverseCoverage) {
    return base
  }

  const hasScannerCoverage = Number(scannedItems || 0) > 0 || Number(candidateItems || 0) > 0
  if (!hasScannerCoverage) {
    return base
  }

  const derivedCandidateByCategory = toScannerAuditCategoryCounts(candidateByCategory)
  const derivedSelectedByCategory = toScannerAuditCategoryCounts(selectedByCategory)
  const normalizedTarget = Math.max(Number(universeTarget || UNIVERSE_TARGET_SIZE), 0)
  const normalizedScanned = Math.max(Number(scannedItems || 0), 0)
  const normalizedCandidates = Math.max(Number(candidateItems || 0), 0)

  return {
    ...base,
    degradedFromScannerInputs: true,
    sourceCatalog: {
      ...sourceCatalog,
      totalRows: Math.max(Number(sourceCatalog?.totalRows || 0), normalizedCandidates),
      activeCatalogRows: Math.max(Number(sourceCatalog?.activeCatalogRows || 0), normalizedCandidates),
      eligibleTradableRows: Math.max(
        Number(sourceCatalog?.eligibleTradableRows || 0),
        normalizedCandidates
      ),
      eligibleRowsByCategory: derivedCandidateByCategory
    },
    universeBuild: {
      ...universeBuild,
      targetUniverseSize: normalizedTarget,
      activeUniverseBuilt: Math.max(Number(universeBuild?.activeUniverseBuilt || 0), normalizedScanned),
      missingToTarget: Math.max(normalizedTarget - normalizedScanned, 0),
      selectedByCategory: derivedSelectedByCategory
    }
  }
}

function buildTopReasonSummary(reasonCounter = {}, limit = 8) {
  return Object.entries(reasonCounter || {})
    .filter(([, count]) => Number(count || 0) > 0)
    .map(([reason, count]) => ({
      reason: String(reason || "").trim() || "unknown",
      count: Number(count || 0)
    }))
    .sort((a, b) => Number(b.count || 0) - Number(a.count || 0))
    .slice(0, Math.max(Number(limit || 0), 0))
}

function buildTopReasonSummaryByCategory(reasonsByCategory = {}, limit = 5) {
  const output = {}
  const source = reasonsByCategory && typeof reasonsByCategory === "object" ? reasonsByCategory : {}
  for (const category of SCANNER_AUDIT_CATEGORIES) {
    output[category] = buildTopReasonSummary(source?.[category]?.reasons || source?.[category] || {}, limit)
  }
  return output
}

function buildOpportunitiesByConfidenceTier(rows = []) {
  const counts = {
    high: 0,
    medium: 0,
    low: 0,
    total: 0,
    highConfidenceEligible: 0,
    riskyEligible: 0
  }
  for (const row of Array.isArray(rows) ? rows : []) {
    const confidence = normalizeConfidence(row?.executionConfidence)
    if (confidence === "High") counts.high += 1
    else if (confidence === "Medium") counts.medium += 1
    else counts.low += 1

    if (Boolean(row?.isHighConfidenceEligible)) counts.highConfidenceEligible += 1
    if (Boolean(row?.isRiskyEligible)) counts.riskyEligible += 1
    counts.total += 1
  }
  return counts
}

function buildQuoteCoverageByMarket(comparisonItems = [], universeSize = 0) {
  const byMarket = Object.fromEntries(
    SOURCE_ORDER.map((source) => [
      source,
      {
        itemsSeen: 0,
        itemsWithAvailableQuote: 0,
        availableQuoteRows: 0,
        unavailableQuoteRows: 0,
        coveragePct: 0
      }
    ])
  )

  for (const item of Array.isArray(comparisonItems) ? comparisonItems : []) {
    const seenInItem = {}
    const availableInItem = {}
    for (const quote of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const source = normalizeMarketLabel(quote?.source || quote?.market)
      if (!source || !SOURCE_ORDER.includes(source)) continue
      seenInItem[source] = true
      if (hasUsableQuotePrice(quote)) {
        availableInItem[source] = true
        byMarket[source].availableQuoteRows += 1
      } else {
        byMarket[source].unavailableQuoteRows += 1
      }
    }

    for (const source of Object.keys(seenInItem)) {
      byMarket[source].itemsSeen += 1
      if (availableInItem[source]) {
        byMarket[source].itemsWithAvailableQuote += 1
      }
    }
  }

  const safeUniverseSize = Math.max(Number(universeSize || 0), 0)
  for (const source of SOURCE_ORDER) {
    const coverageBase = Math.max(safeUniverseSize, 1)
    byMarket[source].coveragePct =
      safeUniverseSize > 0
        ? round2((Number(byMarket[source].itemsWithAvailableQuote || 0) / coverageBase) * 100)
        : 0
  }

  return {
    totalUniverseItems: safeUniverseSize,
    byMarket
  }
}

function buildDefaultPerformanceStageDurations() {
  return {
    sourceCatalogPreparationMs: 0,
    inputHydrationMs: 0,
    quoteFetchingMs: 0,
    normalizationMs: 0,
    opportunityComputationMs: 0,
    dbWritesMs: 0,
    diagnosticsAggregationMs: 0
  }
}

function normalizePerformanceStageDurations(stageDurations = {}) {
  const defaults = buildDefaultPerformanceStageDurations()
  const normalized = {}
  for (const key of PERFORMANCE_STAGE_KEYS) {
    normalized[key] = toAuditDurationMs(stageDurations?.[key] ?? defaults[key])
  }
  return normalized
}

function toPerformanceStageSharePercent(stageDurations = {}, totalDurationMs = 0) {
  const total = Math.max(Number(totalDurationMs || 0), 0)
  const denominator =
    total > 0
      ? total
      : Math.max(
          Object.values(stageDurations || {}).reduce((sum, value) => sum + Number(value || 0), 0),
          1
        )
  const share = {}
  for (const [stage, duration] of Object.entries(stageDurations || {})) {
    share[stage] = round2((Math.max(Number(duration || 0), 0) / denominator) * 100)
  }
  return share
}

function resolveSlowestPerformanceStage(stageDurations = {}) {
  const entries = Object.entries(stageDurations || {}).sort(
    (a, b) => Number(b[1] || 0) - Number(a[1] || 0)
  )
  const [stage, durationMs] = entries[0] || ["", 0]
  return {
    stage: String(stage || "").trim(),
    durationMs: toAuditDurationMs(durationMs)
  }
}

function evaluateScannerPerformanceSafety({
  scanDurationMs = 0,
  stageDurationsMs = {},
  batching = {}
} = {}) {
  const totalDurationMs = toAuditDurationMs(scanDurationMs)
  const stageDurations = normalizePerformanceStageDurations(stageDurationsMs)
  const refresh = batching?.quoteRefresh || {}
  const compute = batching?.computeFromSavedQuotes || {}

  const maxTotalDurationMs = Math.max(Math.round(SCANNER_INTERVAL_MS * 0.85), 120000)
  const maxQuoteFetchingMs = Math.max(Math.round(maxTotalDurationMs * 0.75), 90000)
  const maxNormalizationMs = Math.max(Math.round(maxTotalDurationMs * 0.18), 15000)
  const maxOpportunityComputationMs = Math.max(Math.round(maxTotalDurationMs * 0.24), 25000)
  const maxDbWritesMs = Math.max(Math.round(maxTotalDurationMs * 0.1), 15000)
  const maxDiagnosticsAggregationMs = 12000
  const maxAverageBatchMs = Math.max(Math.round(SCAN_TIMEOUT_PER_BATCH * 0.8), 1500)

  const checks = [
    {
      name: "scan_duration",
      withinBound: totalDurationMs <= maxTotalDurationMs,
      actual: totalDurationMs,
      bound: maxTotalDurationMs,
      unit: "ms"
    },
    {
      name: "quote_fetching_stage",
      withinBound: Number(stageDurations.quoteFetchingMs || 0) <= maxQuoteFetchingMs,
      actual: Number(stageDurations.quoteFetchingMs || 0),
      bound: maxQuoteFetchingMs,
      unit: "ms"
    },
    {
      name: "normalization_stage",
      withinBound: Number(stageDurations.normalizationMs || 0) <= maxNormalizationMs,
      actual: Number(stageDurations.normalizationMs || 0),
      bound: maxNormalizationMs,
      unit: "ms"
    },
    {
      name: "opportunity_computation_stage",
      withinBound:
        Number(stageDurations.opportunityComputationMs || 0) <= maxOpportunityComputationMs,
      actual: Number(stageDurations.opportunityComputationMs || 0),
      bound: maxOpportunityComputationMs,
      unit: "ms"
    },
    {
      name: "db_writes_stage",
      withinBound: Number(stageDurations.dbWritesMs || 0) <= maxDbWritesMs,
      actual: Number(stageDurations.dbWritesMs || 0),
      bound: maxDbWritesMs,
      unit: "ms"
    },
    {
      name: "diagnostics_aggregation_stage",
      withinBound:
        Number(stageDurations.diagnosticsAggregationMs || 0) <= maxDiagnosticsAggregationMs,
      actual: Number(stageDurations.diagnosticsAggregationMs || 0),
      bound: maxDiagnosticsAggregationMs,
      unit: "ms"
    },
    {
      name: "quote_refresh_batch_timeout",
      withinBound: Number(refresh?.timedOutBatches || 0) === 0,
      actual: Number(refresh?.timedOutBatches || 0),
      bound: 0,
      unit: "count"
    },
    {
      name: "saved_quote_compute_batch_timeout",
      withinBound: Number(compute?.timedOutBatches || 0) === 0,
      actual: Number(compute?.timedOutBatches || 0),
      bound: 0,
      unit: "count"
    },
    {
      name: "quote_refresh_average_batch_ms",
      withinBound:
        Number(refresh?.totalBatches || 0) === 0 ||
        Number(refresh?.averageBatchMs || 0) <= maxAverageBatchMs,
      actual: Number(refresh?.averageBatchMs || 0),
      bound: maxAverageBatchMs,
      unit: "ms"
    },
    {
      name: "saved_quote_compute_average_batch_ms",
      withinBound:
        Number(compute?.totalBatches || 0) === 0 ||
        Number(compute?.averageBatchMs || 0) <= maxAverageBatchMs,
      actual: Number(compute?.averageBatchMs || 0),
      bound: maxAverageBatchMs,
      unit: "ms"
    }
  ]

  const breachedChecks = checks.filter((check) => !check.withinBound)
  return {
    withinSafeBounds: !breachedChecks.length,
    breachedChecks,
    checks,
    bounds: {
      maxTotalDurationMs,
      maxQuoteFetchingMs,
      maxNormalizationMs,
      maxOpportunityComputationMs,
      maxDbWritesMs,
      maxDiagnosticsAggregationMs,
      maxAverageBatchMs,
      maxTimedOutBatches: 0
    }
  }
}

function buildScannerPerformanceAudit({
  sourceCatalogDiagnostics = {},
  selectedUniverseByCategory = {},
  staleDiagnostics = {},
  discardedReasons = {},
  discardedReasonsByCategory = {},
  sortedRows = [],
  selectedUniverseRows = [],
  quoteRefreshSummary = {},
  computeFromSavedSummary = {},
  stageDurationsMs = {},
  scanDurationMs = 0
} = {}) {
  const stageDurations = normalizePerformanceStageDurations(stageDurationsMs)
  const safeScanDurationMs = toAuditDurationMs(scanDurationMs)
  const sourceCatalogTotalRows = resolveSourceCatalogTotalRows(sourceCatalogDiagnostics)
  const sourceCatalogEligibleByCategory = resolveSourceCatalogEligibleByCategory(sourceCatalogDiagnostics)
  const activeUniverseByCategory = toScannerAuditCategoryCounts(selectedUniverseByCategory)
  const staleDataByCategory = toScannerAuditCategoryCounts(staleDiagnostics?.staleByCategory || {}, "")
  const opportunitiesByConfidenceTier = buildOpportunitiesByConfidenceTier(sortedRows)
  const quoteCoverageByMarket = buildQuoteCoverageByMarket(
    selectedUniverseRows.map((row) => row?.comparisonItem || null).filter(Boolean),
    selectedUniverseRows.length
  )
  const topRejectedReasons = buildTopReasonSummary(discardedReasons, 10)
  const topRejectedReasonsByCategory = buildTopReasonSummaryByCategory(
    discardedReasonsByCategory,
    5
  )

  const refreshBatches = Number(quoteRefreshSummary?.totalBatches || 0)
  const computeBatches = Number(computeFromSavedSummary?.totalBatches || 0)
  const totalBatches = refreshBatches + computeBatches
  const weightedAverageBatchMs =
    totalBatches > 0
      ? round2(
          ((Number(quoteRefreshSummary?.averageBatchMs || 0) * refreshBatches +
            Number(computeFromSavedSummary?.averageBatchMs || 0) * computeBatches) /
            totalBatches)
        )
      : 0
  const batching = {
    totalBatches,
    completedBatches:
      Number(quoteRefreshSummary?.completedBatches || 0) +
      Number(computeFromSavedSummary?.completedBatches || 0),
    failedBatches:
      Number(quoteRefreshSummary?.failedBatches || 0) +
      Number(computeFromSavedSummary?.failedBatches || 0),
    timedOutBatches:
      Number(quoteRefreshSummary?.timedOutBatches || 0) +
      Number(computeFromSavedSummary?.timedOutBatches || 0),
    slowestBatchMs: Math.max(
      Number(quoteRefreshSummary?.slowestBatchMs || 0),
      Number(computeFromSavedSummary?.slowestBatchMs || 0)
    ),
    averageBatchMs: weightedAverageBatchMs,
    quoteRefresh: quoteRefreshSummary || {},
    computeFromSavedQuotes: computeFromSavedSummary || {}
  }
  const stageSharePercent = toPerformanceStageSharePercent(stageDurations, safeScanDurationMs)
  const slowestStage = resolveSlowestPerformanceStage(stageDurations)
  const safeBounds = evaluateScannerPerformanceSafety({
    scanDurationMs: safeScanDurationMs,
    stageDurationsMs: stageDurations,
    batching
  })

  return {
    categories: SCANNER_AUDIT_CATEGORIES,
    sourceCatalog: {
      totalRows: sourceCatalogTotalRows,
      eligibleRowsByCategory: sourceCatalogEligibleByCategory
    },
    activeUniverse: {
      totalRows: Number(selectedUniverseRows.length || 0),
      byCategory: activeUniverseByCategory
    },
    scanDurationMs: {
      core: safeScanDurationMs,
      endToEnd: safeScanDurationMs
    },
    stageDurationsMs: stageDurations,
    stageSharePercent,
    slowestStage,
    batching,
    staleDataByCategory,
    topRejectedReasons,
    topRejectedReasonsByCategory,
    opportunitiesByConfidenceTier,
    quoteCoverageByMarket,
    safeBounds
  }
}

function extendPerformanceAudit(
  performanceAudit = {},
  options = {}
) {
  if (!performanceAudit || typeof performanceAudit !== "object") {
    return performanceAudit
  }
  const additionalDbWriteDurationMs = toAuditDurationMs(options?.additionalDbWriteDurationMs)
  const additionalDiagnosticsAggregationMs = toAuditDurationMs(
    options?.additionalDiagnosticsAggregationMs
  )
  const endToEndScanDurationMs = options?.endToEndScanDurationMs

  const stageDurations = normalizePerformanceStageDurations(performanceAudit?.stageDurationsMs || {})
  stageDurations.dbWritesMs += additionalDbWriteDurationMs
  stageDurations.diagnosticsAggregationMs += additionalDiagnosticsAggregationMs

  const coreDurationMs = toAuditDurationMs(performanceAudit?.scanDurationMs?.core)
  const endToEndDurationMs = toAuditDurationMs(
    endToEndScanDurationMs == null
      ? performanceAudit?.scanDurationMs?.endToEnd ?? coreDurationMs
      : endToEndScanDurationMs
  )
  const totalDurationMs = Math.max(endToEndDurationMs, coreDurationMs)
  const stageSharePercent = toPerformanceStageSharePercent(stageDurations, totalDurationMs)
  const slowestStage = resolveSlowestPerformanceStage(stageDurations)
  const batching = performanceAudit?.batching || {}
  const safeBounds = evaluateScannerPerformanceSafety({
    scanDurationMs: totalDurationMs,
    stageDurationsMs: stageDurations,
    batching
  })

  return {
    ...performanceAudit,
    scanDurationMs: {
      core: coreDurationMs,
      endToEnd: totalDurationMs
    },
    stageDurationsMs: stageDurations,
    stageSharePercent,
    slowestStage,
    safeBounds
  }
}

async function runScanInternal(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const opportunityUniverseTarget = Math.max(
    Math.min(OPPORTUNITY_BATCH_SIZE, PRE_COMPARE_UNIVERSE_LIMIT),
    1
  )
  const scanStartedAt = Date.now()
  const stageDurationsMs = buildDefaultPerformanceStageDurations()
  let imageEnrichmentSummary = buildDefaultImageEnrichmentSummary()
  const sourceCatalogStartedAt = Date.now()
  const sourceCatalogDiagnostics = await marketSourceCatalogService
    .prepareSourceCatalog({
      targetUniverseSize: UNIVERSE_TARGET_SIZE,
      forceRefresh
    })
    .catch((err) => {
      console.error("[arbitrage-scanner] Source catalog refresh failed", err.message)
      return {
        ...marketSourceCatalogService.getLastDiagnostics(),
        error: String(err?.message || "source_catalog_refresh_failed")
      }
    })
  stageDurationsMs.sourceCatalogPreparationMs = Date.now() - sourceCatalogStartedAt
  const discardStats = {}
  const rejectedByItem = {}
  const staleDiagnosticsAccumulator = buildStaleDiagnosticsAccumulator()
  const inputHydrationStartedAt = Date.now()
  const scannerInputs = await loadScannerInputs(discardStats, rejectedByItem, {
    mode: SCANNER_TYPES.OPPORTUNITY_SCAN,
    opportunityBatchSize: opportunityUniverseTarget,
    enrichmentBatchSize: ENRICHMENT_BATCH_SIZE,
    includeNearEligibleInOpportunity: true,
    enableSnapshotWarmup: false
  })
  stageDurationsMs.inputHydrationMs = Date.now() - inputHydrationStartedAt
  const universeSeeds = Array.isArray(scannerInputs?.seeds) ? scannerInputs.seeds : []
  const enrichmentSeeds = Array.isArray(scannerInputs?.enrichmentSeeds)
    ? scannerInputs.enrichmentSeeds
    : []
  const allLayeredSeeds = Array.isArray(scannerInputs?.allSeeds) ? scannerInputs.allSeeds : universeSeeds
  const weaponSkinFilterDiagnostics =
    scannerInputs?.weaponSkinFilterDiagnostics || buildWeaponSkinFilterDiagnostics()
  const snapshotWarmupSummary = scannerInputs?.snapshotWarmup || toSnapshotWarmupSummary()
  const scanAdmissionSummary =
    scannerInputs?.scanAdmission || buildOpportunityAdmissionDiagnostics()
  const layeredScanningSummary =
    scannerInputs?.layeredScanning && typeof scannerInputs.layeredScanning === "object"
      ? {
          ...scannerInputs.layeredScanning,
          scanAdmission:
            scannerInputs?.layeredScanning?.scanAdmission || scanAdmissionSummary
        }
      : {
          totalRankedSeeds: universeSeeds.length,
          coreUniverseSize: 0,
          opportunityTarget: opportunityUniverseTarget,
          enrichmentTarget: ENRICHMENT_BATCH_SIZE,
          selectedForOpportunity: universeSeeds.length,
          selectedForEnrichment: enrichmentSeeds.length,
          allSeeds: buildLayerDiagnostics(universeSeeds),
          opportunity: buildLayerDiagnostics(universeSeeds),
          enrichment: buildLayerDiagnostics(enrichmentSeeds),
          scanAdmission: scanAdmissionSummary
        }
  let enrichmentPipelineSummary = buildDefaultEnrichmentPipelineSummary()
  if (!universeSeeds.length) {
    const diagnosticsAggregationStartedAt = Date.now()
    const generatedTs = Date.now()
    const emptyByCategory = Object.fromEntries(
      Object.values(ITEM_CATEGORIES).map((category) => [category, 0])
    )
    const discardedReasons = normalizeDiscardStats(discardStats)
    const discardedReasonsByCategory = normalizeDiscardStatsByCategory(discardStats)
    const staleDiagnostics = toStaleDiagnosticsSummary(staleDiagnosticsAccumulator)
    const emptyPayload = {
      generatedAt: new Date(generatedTs).toISOString(),
      expiresAt: generatedTs + CACHE_TTL_MS,
      ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
      currency: "USD",
      summary: {
        scannedItems: 0,
        qualifiedItems: 0,
        opportunities: 0,
        totalDetected: 0,
        universeSize: 0,
        universeTarget: opportunityUniverseTarget,
        candidateItems: allLayeredSeeds.length,
        discardedReasons,
        discardedReasonsByCategory,
        rejectedByCategory: { ...emptyByCategory },
        topRejectedItems: toTopRejectedItems(rejectedByItem),
        rejectionReasonsByItem: toRejectionReasonsByItem(rejectedByItem),
        selectedUniverseByCategory: { ...emptyByCategory },
        requestedUniverseByCategory: { ...emptyByCategory },
        opportunitiesByCategory: { ...emptyByCategory },
        staleDiagnostics,
        knifeGloveRejections: toKnifeGloveRejectionSummary({}),
        weaponSkinFiltering: toWeaponSkinFilterDiagnosticsSummary(weaponSkinFilterDiagnostics),
        riskyProfileDiagnostics: toRiskyProfileDiagnosticsSummary(buildRiskyProfileDiagnostics()),
        riskyThresholdDiagnostics: buildRiskyThresholdDiagnosticsSummary(buildRiskyProfileDiagnostics()),
        snapshotWarmup: snapshotWarmupSummary,
        imageEnrichment: imageEnrichmentSummary,
        layeredScanning: layeredScanningSummary,
        scanAdmission: scanAdmissionSummary,
        enrichmentPipeline: enrichmentPipelineSummary,
        maturity: {
          funnel: layeredScanningSummary?.allSeeds?.maturityFunnel || buildMaturityCounter(0),
          byCategory:
            layeredScanningSummary?.allSeeds?.maturityByCategory || buildMaturityByCategoryCounter(0),
          layers: layeredScanningSummary?.allSeeds?.layers || buildLayerCounter(0),
          layersByCategory:
            layeredScanningSummary?.allSeeds?.layersByCategory || buildLayerByCategoryCounter(0),
          coreUniverseSize: Number(layeredScanningSummary?.coreUniverseSize || 0),
          selectedForOpportunity: Number(layeredScanningSummary?.selectedForOpportunity || 0),
          selectedForEnrichment: Number(layeredScanningSummary?.selectedForEnrichment || 0),
          promotedToNearEligible: Number(
            sourceCatalogDiagnostics?.sourceCatalog?.promotedToNearEligible || 0
          ),
          promotedToEligible: Number(sourceCatalogDiagnostics?.sourceCatalog?.promotedToEligible || 0),
          demotedToEnriching: Number(sourceCatalogDiagnostics?.sourceCatalog?.demotedToEnriching || 0),
          promotedToNearEligibleByCategory:
            sourceCatalogDiagnostics?.sourceCatalog?.promotedToNearEligibleByCategory ||
            buildScannerAuditCategoryCounter(0),
          promotedToEligibleByCategory:
            sourceCatalogDiagnostics?.sourceCatalog?.promotedToEligibleByCategory ||
            buildScannerAuditCategoryCounter(0),
          demotedToEnrichingByCategory:
            sourceCatalogDiagnostics?.sourceCatalog?.demotedToEnrichingByCategory ||
            buildScannerAuditCategoryCounter(0)
        },
        sourceCatalog: sourceCatalogDiagnostics,
        scanProgress: buildScanProgressStats({
          universeTarget: opportunityUniverseTarget,
          candidateItems: allLayeredSeeds.length,
          requestedItems: universeSeeds.length,
          qualifiedItems: 0,
          scannedItems: 0
        }),
        batchSizing: buildBatchSizingDiagnostics({
          scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
          runDurationMs: Date.now() - scanStartedAt,
          scannedItems: 0,
          qualifiedItems: 0,
          opportunitiesFound: 0,
          snapshotWarmup: snapshotWarmupSummary,
          layeredScanning: layeredScanningSummary
        }),
        hotScan: buildHotScanSummary({
          layeredScanning: layeredScanningSummary,
          opportunitySeeds: universeSeeds,
          qualifiedItems: 0,
          snapshotWarmup: snapshotWarmupSummary
        }),
        highConfidence: 0,
        riskyEligible: 0,
        speculativeEligible: 0
      },
      opportunities: [],
      pipeline: {
        sequence: [
          "fetch_quotes",
          "save_market_prices",
          "compute_from_saved_quotes",
          "apply_freshness_scoring",
          "persist_diagnostics"
        ],
        config: {
          defaultUniverseLimit: DEFAULT_UNIVERSE_LIMIT,
          universeTargetSize: opportunityUniverseTarget,
          preCompareUniverseLimit: PRE_COMPARE_UNIVERSE_LIMIT,
          universeDbLimit: UNIVERSE_DB_LIMIT,
          scanBatchSize: SCAN_BATCH_SIZE,
          enrichmentBatchSize: ENRICHMENT_BATCH_SIZE,
          opportunityBatchSize: opportunityUniverseTarget,
          maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
          scanTimeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
          imageEnrichBatchSize: IMAGE_ENRICH_BATCH_SIZE,
          imageEnrichConcurrency: IMAGE_ENRICH_CONCURRENCY,
          imageEnrichTimeoutMs: IMAGE_ENRICH_TIMEOUT_MS,
          highYieldCoreTarget: HIGH_YIELD_CORE_TARGET,
          hotLayerScanTarget: HOT_LAYER_SCAN_TARGET,
          warmLayerScanTarget: WARM_LAYER_SCAN_TARGET,
          coldLayerScanTarget: COLD_LAYER_SCAN_TARGET,
          opportunityScanTarget: opportunityUniverseTarget,
          hotOpportunityScanTarget: HOT_OPPORTUNITY_SCAN_TARGET,
          enrichmentOnlyTarget: ENRICHMENT_BATCH_SIZE
        },
        quoteRefresh: {
          batchSize: SCAN_BATCH_SIZE,
          timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
          maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
          requestedItems: 0,
          totalBatches: 0,
          completedBatches: 0,
          failedBatches: 0,
          timedOutBatches: 0,
          itemsCompared: 0,
          availableQuotes: 0,
          averageBatchMs: 0,
          slowestBatchMs: 0,
          errors: []
        },
        computeFromSavedQuotes: {
          batchSize: SCAN_BATCH_SIZE,
          timeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
          maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
          requestedItems: 0,
          totalBatches: 0,
          completedBatches: 0,
          failedBatches: 0,
          timedOutBatches: 0,
          itemsCompared: 0,
          availableQuotes: 0,
          averageBatchMs: 0,
          slowestBatchMs: 0,
          errors: []
        },
        quoteSnapshot: {
          rowsPrepared: 0,
          rowsInserted: 0,
          persisted: true
        },
        snapshotWarmup: snapshotWarmupSummary,
        imageEnrichment: imageEnrichmentSummary,
        layeredScanning: layeredScanningSummary,
        scanAdmission: scanAdmissionSummary,
        enrichmentPipeline: enrichmentPipelineSummary,
        sourceCatalog: sourceCatalogDiagnostics
      }
    }

    stageDurationsMs.diagnosticsAggregationMs = Date.now() - diagnosticsAggregationStartedAt
    const auditBuildStartedAt = Date.now()
    let performanceAudit = buildScannerPerformanceAudit({
      sourceCatalogDiagnostics,
      selectedUniverseByCategory: emptyByCategory,
      staleDiagnostics,
      discardedReasons,
      discardedReasonsByCategory,
      sortedRows: [],
      selectedUniverseRows: [],
      quoteRefreshSummary: emptyPayload.pipeline.quoteRefresh,
      computeFromSavedSummary: emptyPayload.pipeline.computeFromSavedQuotes,
      stageDurationsMs,
      scanDurationMs: Date.now() - scanStartedAt
    })
    const auditBuildDurationMs = Date.now() - auditBuildStartedAt
    performanceAudit = extendPerformanceAudit(performanceAudit, {
      additionalDiagnosticsAggregationMs: auditBuildDurationMs,
      endToEndScanDurationMs: Date.now() - scanStartedAt
    })
    emptyPayload.summary.performanceAudit = performanceAudit
    emptyPayload.pipeline.performanceAudit = performanceAudit

    return emptyPayload
  }

  const normalizationStartedAt = Date.now()
  const comparisonInputItems = universeSeeds.map((row) => buildInputItemForComparison(row))
  const inputByName = toByNameMap(
    universeSeeds.map((row) => ({
      ...row,
      marketHashName: normalizeMarketHashName(row?.marketHashName)
    })),
    "marketHashName"
  )
  stageDurationsMs.normalizationMs += Date.now() - normalizationStartedAt

  if (enrichmentSeeds.length) {
    const enrichmentLayers = buildLayerDiagnostics(enrichmentSeeds)
    enrichmentPipelineSummary = {
      ...enrichmentPipelineSummary,
      attempted: false,
      requestedItems: enrichmentSeeds.length,
      selectedLayers: enrichmentLayers.layers,
      selectedLayersByCategory: enrichmentLayers.layersByCategory
    }
  }

  const quoteFetchingStartedAt = Date.now()
  const quoteRefreshSummary = await refreshQuotesInBatches(comparisonInputItems, {
    forceRefresh
  })
  const comparisonFromSaved = await compareFromSavedQuotes(comparisonInputItems)
  stageDurationsMs.quoteFetchingMs += Date.now() - quoteFetchingStartedAt

  const normalizationEnrichmentStartedAt = Date.now()
  imageEnrichmentSummary = await hydrateUniverseImages(inputByName, comparisonFromSaved.items)
  stageDurationsMs.normalizationMs += Date.now() - normalizationEnrichmentStartedAt

  const quoteSnapshotWriteStartedAt = Date.now()
  const quoteSnapshotSummary = await persistQuoteSnapshot(comparisonFromSaved.items, inputByName)
  stageDurationsMs.dbWritesMs += Date.now() - quoteSnapshotWriteStartedAt

  const opportunityComputationStartedAt = Date.now()
  const selectedUniverse = selectTopUniverseItems(
    comparisonFromSaved?.items,
    inputByName,
    discardStats,
    rejectedByItem
  )
  const rows = []
  const riskyProfileDiagnostics = buildRiskyProfileDiagnostics()
  for (const selected of selectedUniverse) {
    const item = selected?.comparisonItem || null
    const inputItem = selected?.inputItem || null
    const itemName = String(inputItem?.marketHashName || item?.marketHashName || "").trim()
    const itemCategory = normalizeItemCategory(inputItem?.itemCategory, itemName)
    if (!item || !inputItem) continue
    const opportunity = item?.arbitrage || null
    if (!opportunity) {
      incrementReasonCounter(discardStats, "insufficient_market_data", itemCategory)
      incrementItemReasonCounter(rejectedByItem, itemName, "insufficient_market_data", itemCategory)
      continue
    }

    const enrichedOpportunity = {
      ...opportunity,
      itemCategory,
      marketCoverage: selected.marketCoverage
    }
    const liquidity = resolveLiquidityMetrics(enrichedOpportunity, inputItem)
    const stale = resolveStaleDataPenalty(item?.perMarket, enrichedOpportunity, itemCategory)
    trackStaleDiagnostics(staleDiagnosticsAccumulator, itemCategory, stale)
    trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "attempted")

    const hardRejectionReason = getPrimaryHardRejectionReason(enrichedOpportunity)
    if (hardRejectionReason) {
      collectDiscardReasonsFromOpportunity(
        enrichedOpportunity,
        discardStats,
        rejectedByItem,
        itemName,
        itemCategory
      )
      if (!Array.isArray(enrichedOpportunity?.antiFake?.reasons) || !enrichedOpportunity.antiFake.reasons.length) {
        incrementReasonCounter(discardStats, hardRejectionReason, itemCategory)
        incrementItemReasonCounter(rejectedByItem, itemName, hardRejectionReason, itemCategory)
      }
      trackRiskyBaselineOutcome(riskyProfileDiagnostics, itemCategory, false)
      trackRiskyDecision(riskyProfileDiagnostics, itemCategory, "rejected", hardRejectionReason)
      continue
    }

    const strictEvaluation = computeRiskAdjustments({
      opportunity: enrichedOpportunity,
      liquidity,
      stale,
      inputItem,
      profile: STRICT_SCAN_PROFILE
    })
    const riskyProfile = getCategoryRiskyProfile(itemCategory)
    const riskyOutcome = evaluateRiskyCandidateOutcome({
      opportunity: enrichedOpportunity,
      liquidity,
      stale,
      inputItem,
      perMarket: item?.perMarket,
      profile: riskyProfile,
      qualityFloor: getRiskyQualityFloor(itemCategory),
      isHighConfidenceEligible: strictEvaluation.passed
    })
    const legacyRiskyOutcome = evaluateRiskyCandidateOutcome({
      opportunity: enrichedOpportunity,
      liquidity,
      stale,
      inputItem,
      perMarket: item?.perMarket,
      profile: getLegacyCategoryRiskyProfile(itemCategory),
      qualityFloor: getLegacyRiskyQualityFloor(itemCategory),
      isHighConfidenceEligible: strictEvaluation.passed
    })
    trackRiskyBaselineOutcome(riskyProfileDiagnostics, itemCategory, legacyRiskyOutcome.passed)
    if (!riskyOutcome.passed) {
      if (
        itemCategory === ITEM_CATEGORIES.WEAPON_SKIN &&
        riskyOutcome?.diagnosticRejectionKey
      ) {
        incrementWeaponSkinFilterDiagnostic(
          weaponSkinFilterDiagnostics,
          riskyOutcome.diagnosticRejectionKey,
          itemName
        )
      }
      incrementReasonCounter(discardStats, riskyOutcome.primaryReason, itemCategory)
      incrementItemReasonCounter(
        rejectedByItem,
        itemName,
        riskyOutcome.primaryReason,
        itemCategory
      )
      trackRiskyDecision(
        riskyProfileDiagnostics,
        itemCategory,
        "rejected",
        riskyOutcome.primaryReason
      )
      continue
    }
    for (const key of riskyOutcome?.evaluation?.diagnosticPenaltyKeys || []) {
      incrementWeaponSkinFilterDiagnostic(weaponSkinFilterDiagnostics, key, itemName)
    }
    trackRiskyBorderlinePromotion(
      riskyProfileDiagnostics,
      itemCategory,
      riskyOutcome?.evaluation?.borderlinePromotionKeys
    )
    if (!legacyRiskyOutcome.passed && riskyProfileDiagnostics?.[itemCategory]) {
      riskyProfileDiagnostics[itemCategory].additionalAcceptedVsBaseline =
        Number(riskyProfileDiagnostics[itemCategory].additionalAcceptedVsBaseline || 0) + 1
    }
    const apiRow = riskyOutcome.apiRow
    const speculativeEligible = Boolean(riskyOutcome.speculativeEligible)

    const highConfidenceEligible =
      Boolean(strictEvaluation?.passed) &&
      Number(apiRow?.score || 0) >= HIGH_CONFIDENCE_MIN_SCORE &&
      String(apiRow?.executionConfidence || "")
        .trim()
        .toLowerCase() !== "low"
    rows.push({
      ...apiRow,
      isHighConfidenceEligible: highConfidenceEligible,
      isSpeculativeEligible: speculativeEligible
    })
    if (itemCategory === ITEM_CATEGORIES.WEAPON_SKIN) {
      incrementWeaponSkinFilterDiagnostic(weaponSkinFilterDiagnostics, "survived_into_risky", itemName)
      if (riskyOutcome?.evaluation?.staleForwardedTier === "risky") {
        incrementWeaponSkinFilterDiagnostic(
          weaponSkinFilterDiagnostics,
          "stale_forwarded_to_risky",
          itemName
        )
      }
      if (riskyOutcome?.evaluation?.staleForwardedTier === "speculative") {
        incrementWeaponSkinFilterDiagnostic(
          weaponSkinFilterDiagnostics,
          "stale_forwarded_to_speculative",
          itemName
        )
      }
      if (speculativeEligible) {
        incrementWeaponSkinFilterDiagnostic(
          weaponSkinFilterDiagnostics,
          "survived_into_speculative",
          itemName
        )
      }
    }
    const premiumAcceptedWithMissingDepth =
      PREMIUM_ITEM_CATEGORIES.has(itemCategory) &&
      (
        Boolean(liquidity?.hasMissingDepth) ||
        (Boolean(liquidity?.buyDepthMissing) && Boolean(liquidity?.sellDepthMissing))
      )
    trackRiskyDecision(
      riskyProfileDiagnostics,
      itemCategory,
      "accepted",
      speculativeEligible
        ? "accepted_weapon_skin_speculative"
        : Array.isArray(riskyOutcome?.evaluation?.borderlinePromotionKeys) &&
            riskyOutcome.evaluation.borderlinePromotionKeys.length
          ? "accepted_borderline_promotion"
        : premiumAcceptedWithMissingDepth
        ? "accepted_premium_missing_depth_tolerated"
        : PREMIUM_ITEM_CATEGORIES.has(itemCategory)
          ? "accepted_premium_profile"
          : "accepted_standard_profile"
    )
  }

  const sortedRows = sortOpportunities(rows)
  stageDurationsMs.opportunityComputationMs = Date.now() - opportunityComputationStartedAt
  const diagnosticsAggregationStartedAt = Date.now()
  const highConfidenceCount = sortedRows.filter(
    (row) =>
      Boolean(row?.isHighConfidenceEligible) &&
      Number(row?.score || 0) >= DEFAULT_SCORE_CUTOFF &&
      String(row?.executionConfidence || "")
        .trim()
        .toLowerCase() !== "low"
  ).length
  const riskyEligibleCount = sortedRows.filter((row) => Boolean(row?.isRiskyEligible)).length
  const speculativeEligibleCount = sortedRows.filter((row) => Boolean(row?.isSpeculativeEligible)).length
  const selectedUniverseByCategory = countRowsByCategory(
    selectedUniverse.map((row) => ({
      itemCategory: row?.inputItem?.itemCategory,
      itemName: row?.inputItem?.marketHashName
    }))
  )
  const requestedUniverseByCategory = countRowsByCategory(
    universeSeeds.map((row) => ({
      itemCategory: row?.itemCategory,
      itemName: row?.marketHashName
    }))
  )
  const candidateUniverseByCategory = countRowsByCategory(
    allLayeredSeeds.map((row) => ({
      itemCategory: row?.itemCategory,
      itemName: row?.marketHashName
    }))
  )
  const effectiveSourceCatalogDiagnostics = deriveSourceCatalogDiagnosticsFromScan({
    sourceCatalogDiagnostics,
    candidateItems: allLayeredSeeds.length,
    scannedItems: selectedUniverse.length,
    candidateByCategory: candidateUniverseByCategory,
    selectedByCategory: selectedUniverseByCategory,
    universeTarget: opportunityUniverseTarget
  })
  const opportunitiesByCategory = countRowsByCategory(sortedRows)
  const discardedReasons = normalizeDiscardStats(discardStats)
  const discardedReasonsByCategory = normalizeDiscardStatsByCategory(discardStats)
  const rejectedByCategory = toRejectedCountByCategory(discardedReasonsByCategory)
  const staleDiagnostics = toStaleDiagnosticsSummary(staleDiagnosticsAccumulator)
  const knifeGloveRejections = toKnifeGloveRejectionSummary(discardedReasonsByCategory)
  const riskyProfileDiagnosticsSummary = toRiskyProfileDiagnosticsSummary(riskyProfileDiagnostics)
  const riskyThresholdDiagnostics = buildRiskyThresholdDiagnosticsSummary(riskyProfileDiagnostics)
  const scanProgress = buildScanProgressStats({
    universeTarget: opportunityUniverseTarget,
    candidateItems: allLayeredSeeds.length,
    requestedItems: universeSeeds.length,
    qualifiedItems: selectedUniverse.length,
    scannedItems: universeSeeds.length,
    quoteRefresh: quoteRefreshSummary,
    computeFromSavedQuotes: comparisonFromSaved?.diagnostics || {}
  })
  const hotScan = buildHotScanSummary({
    layeredScanning: layeredScanningSummary,
    opportunitySeeds: universeSeeds,
    selectedUniverseByCategory,
    opportunitiesByCategory,
    qualifiedItems: selectedUniverse.length,
    opportunitiesFound: sortedRows.length,
    snapshotWarmup: snapshotWarmupSummary
  })
  const generatedTs = Date.now()
  const payload = {
    generatedAt: new Date(generatedTs).toISOString(),
    expiresAt: generatedTs + CACHE_TTL_MS,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: String(comparisonFromSaved?.currency || "USD")
      .trim()
      .toUpperCase(),
    summary: {
      scannedItems: universeSeeds.length,
      qualifiedItems: selectedUniverse.length,
      opportunities: highConfidenceCount,
      totalDetected: sortedRows.length,
      universeSize: universeSeeds.length,
      universeTarget: opportunityUniverseTarget,
      candidateItems: allLayeredSeeds.length,
      discardedReasons,
      discardedReasonsByCategory,
      rejectedByCategory,
      topRejectedItems: toTopRejectedItems(rejectedByItem),
      rejectionReasonsByItem: toRejectionReasonsByItem(rejectedByItem),
      selectedUniverseByCategory,
      requestedUniverseByCategory,
      opportunitiesByCategory,
      staleDiagnostics,
      knifeGloveRejections,
      weaponSkinFiltering: toWeaponSkinFilterDiagnosticsSummary(weaponSkinFilterDiagnostics),
      riskyProfileDiagnostics: riskyProfileDiagnosticsSummary,
      riskyThresholdDiagnostics,
      snapshotWarmup: snapshotWarmupSummary,
      imageEnrichment: imageEnrichmentSummary,
      layeredScanning: layeredScanningSummary,
      scanAdmission: scanAdmissionSummary,
      enrichmentPipeline: enrichmentPipelineSummary,
      maturity: {
        funnel: layeredScanningSummary?.allSeeds?.maturityFunnel || buildMaturityCounter(0),
        byCategory:
          layeredScanningSummary?.allSeeds?.maturityByCategory || buildMaturityByCategoryCounter(0),
        layers: layeredScanningSummary?.allSeeds?.layers || buildLayerCounter(0),
        layersByCategory:
          layeredScanningSummary?.allSeeds?.layersByCategory || buildLayerByCategoryCounter(0),
        coreUniverseSize: Number(layeredScanningSummary?.coreUniverseSize || 0),
        selectedForOpportunity: Number(layeredScanningSummary?.selectedForOpportunity || 0),
        selectedForEnrichment: Number(layeredScanningSummary?.selectedForEnrichment || 0),
        promotedToNearEligible: Number(
          effectiveSourceCatalogDiagnostics?.sourceCatalog?.promotedToNearEligible || 0
        ),
        promotedToEligible: Number(
          effectiveSourceCatalogDiagnostics?.sourceCatalog?.promotedToEligible || 0
        ),
        demotedToEnriching: Number(
          effectiveSourceCatalogDiagnostics?.sourceCatalog?.demotedToEnriching || 0
        ),
        promotedToNearEligibleByCategory:
          effectiveSourceCatalogDiagnostics?.sourceCatalog?.promotedToNearEligibleByCategory ||
          buildScannerAuditCategoryCounter(0),
        promotedToEligibleByCategory:
          effectiveSourceCatalogDiagnostics?.sourceCatalog?.promotedToEligibleByCategory ||
          buildScannerAuditCategoryCounter(0),
        demotedToEnrichingByCategory:
          effectiveSourceCatalogDiagnostics?.sourceCatalog?.demotedToEnrichingByCategory ||
          buildScannerAuditCategoryCounter(0)
      },
      sourceCatalog: effectiveSourceCatalogDiagnostics,
      scanProgress,
      scanAdmission: scanAdmissionSummary,
      batchSizing: buildBatchSizingDiagnostics({
        scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
        runDurationMs: Date.now() - scanStartedAt,
        scannedItems: universeSeeds.length,
        qualifiedItems: selectedUniverse.length,
        opportunitiesFound: sortedRows.length,
        snapshotWarmup: snapshotWarmupSummary,
        layeredScanning: layeredScanningSummary
      }),
      hotScan,
      highConfidence: highConfidenceCount,
      riskyEligible: riskyEligibleCount,
      speculativeEligible: speculativeEligibleCount
    },
    opportunities: sortedRows,
    pipeline: {
      sequence: [
        "fetch_quotes",
        "save_market_prices",
        "compute_from_saved_quotes",
        "apply_freshness_scoring",
        "persist_diagnostics"
      ],
      config: {
        defaultUniverseLimit: DEFAULT_UNIVERSE_LIMIT,
        universeTargetSize: opportunityUniverseTarget,
        preCompareUniverseLimit: PRE_COMPARE_UNIVERSE_LIMIT,
        universeDbLimit: UNIVERSE_DB_LIMIT,
        scanBatchSize: SCAN_BATCH_SIZE,
        enrichmentBatchSize: ENRICHMENT_BATCH_SIZE,
        opportunityBatchSize: opportunityUniverseTarget,
        maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
        scanTimeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH,
        imageEnrichBatchSize: IMAGE_ENRICH_BATCH_SIZE,
        imageEnrichConcurrency: IMAGE_ENRICH_CONCURRENCY,
          imageEnrichTimeoutMs: IMAGE_ENRICH_TIMEOUT_MS,
          highYieldCoreTarget: HIGH_YIELD_CORE_TARGET,
          hotLayerScanTarget: HOT_LAYER_SCAN_TARGET,
          warmLayerScanTarget: WARM_LAYER_SCAN_TARGET,
          coldLayerScanTarget: COLD_LAYER_SCAN_TARGET,
          opportunityScanTarget: opportunityUniverseTarget,
          hotOpportunityScanTarget: HOT_OPPORTUNITY_SCAN_TARGET,
          enrichmentOnlyTarget: ENRICHMENT_BATCH_SIZE
        },
      quoteRefresh: quoteRefreshSummary,
      computeFromSavedQuotes: comparisonFromSaved?.diagnostics || null,
      quoteSnapshot: quoteSnapshotSummary,
      snapshotWarmup: snapshotWarmupSummary,
      imageEnrichment: imageEnrichmentSummary,
      layeredScanning: layeredScanningSummary,
      scanAdmission: scanAdmissionSummary,
      enrichmentPipeline: enrichmentPipelineSummary,
      sourceCatalog: effectiveSourceCatalogDiagnostics
    }
  }

  stageDurationsMs.diagnosticsAggregationMs = Date.now() - diagnosticsAggregationStartedAt
  const auditBuildStartedAt = Date.now()
  let performanceAudit = buildScannerPerformanceAudit({
    sourceCatalogDiagnostics: effectiveSourceCatalogDiagnostics,
    selectedUniverseByCategory,
    staleDiagnostics,
    discardedReasons,
    discardedReasonsByCategory,
    sortedRows,
    selectedUniverseRows: selectedUniverse,
    quoteRefreshSummary,
    computeFromSavedSummary: comparisonFromSaved?.diagnostics || {},
    stageDurationsMs,
    scanDurationMs: Date.now() - scanStartedAt
  })
  const auditBuildDurationMs = Date.now() - auditBuildStartedAt
  performanceAudit = extendPerformanceAudit(performanceAudit, {
    additionalDiagnosticsAggregationMs: auditBuildDurationMs,
    endToEndScanDurationMs: Date.now() - scanStartedAt
  })
  payload.summary.performanceAudit = performanceAudit
  payload.pipeline.performanceAudit = performanceAudit

  return payload
}

function sumMissingSignalsFromSourceCatalog(sourceCatalog = {}) {
  const byCategory =
    sourceCatalog?.byCategory && typeof sourceCatalog.byCategory === "object"
      ? sourceCatalog.byCategory
      : {}
  let missingSnapshot = 0
  let missingReference = 0
  let missingMarketCoverage = 0
  for (const payload of Object.values(byCategory)) {
    missingSnapshot += Number(payload?.missingSnapshot || 0)
    missingReference += Number(payload?.missingReference || 0)
    missingMarketCoverage += Number(payload?.missingMarketCoverage || 0)
  }
  return {
    missingSnapshot,
    missingReference,
    missingMarketCoverage
  }
}

async function runEnrichmentInternal(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh)
  const runStartedAt = Date.now()
  const sourceCatalogStartedAt = Date.now()
  const sourceCatalogDiagnostics = await marketSourceCatalogService
    .prepareSourceCatalog({
      targetUniverseSize: UNIVERSE_TARGET_SIZE,
      forceRefresh
    })
    .catch((err) => {
      console.error("[arbitrage-scanner] Source catalog refresh failed (enrichment)", err.message)
      return {
        ...marketSourceCatalogService.getLastDiagnostics(),
        error: String(err?.message || "source_catalog_refresh_failed")
      }
    })
  const sourceCatalogPreparationMs = Date.now() - sourceCatalogStartedAt

  const discardStats = {}
  const rejectedByItem = {}
  const inputHydrationStartedAt = Date.now()
  const scannerInputs = await loadScannerInputs(discardStats, rejectedByItem, {
    mode: SCANNER_TYPES.ENRICHMENT,
    enrichmentBatchSize: ENRICHMENT_BATCH_SIZE,
    opportunityBatchSize: Math.max(Math.min(HOT_OPPORTUNITY_SCAN_TARGET, OPPORTUNITY_BATCH_SIZE), 1),
    includeNearEligibleInOpportunity: false,
    enableSnapshotWarmup: true
  })
  const inputHydrationMs = Date.now() - inputHydrationStartedAt
  const enrichmentSeeds = Array.isArray(scannerInputs?.enrichmentSeeds)
    ? scannerInputs.enrichmentSeeds.slice(0, ENRICHMENT_BATCH_SIZE)
    : []
  const allLayeredSeeds = Array.isArray(scannerInputs?.allSeeds) ? scannerInputs.allSeeds : []
  const snapshotWarmupSummary = scannerInputs?.snapshotWarmup || toSnapshotWarmupSummary()
  const layeredScanningSummary =
    scannerInputs?.layeredScanning && typeof scannerInputs.layeredScanning === "object"
      ? scannerInputs.layeredScanning
      : {
          totalRankedSeeds: allLayeredSeeds.length,
          coreUniverseSize: 0,
          opportunityTarget: Math.max(Math.min(HOT_OPPORTUNITY_SCAN_TARGET, OPPORTUNITY_BATCH_SIZE), 1),
          enrichmentTarget: ENRICHMENT_BATCH_SIZE,
          selectedForOpportunity: 0,
          selectedForEnrichment: enrichmentSeeds.length,
          allSeeds: buildLayerDiagnostics(allLayeredSeeds),
          opportunity: buildLayerDiagnostics([]),
          enrichment: buildLayerDiagnostics(enrichmentSeeds)
        }

  const enrichmentPipelineSummary = await runEnrichmentPipeline(enrichmentSeeds, {
    forceRefresh: true
  })

  const sourceCatalog =
    sourceCatalogDiagnostics?.sourceCatalog && typeof sourceCatalogDiagnostics.sourceCatalog === "object"
      ? sourceCatalogDiagnostics.sourceCatalog
      : {}
  const missingSignals = sumMissingSignalsFromSourceCatalog(sourceCatalog)
  const diagnosticsSummary = {
    trigger: String(options.trigger || "scheduled"),
    generatedAt: new Date().toISOString(),
    selectedItems: enrichmentSeeds.length,
    itemsUpdated: Number(enrichmentPipelineSummary?.enrichedItems || 0),
    promotedToNearEligible: Number(sourceCatalog?.promotedToNearEligible || 0),
    promotedToEligible: Number(sourceCatalog?.promotedToEligible || 0),
    stillMissingData: missingSignals,
    timing: {
      sourceCatalogPreparationMs,
      inputHydrationMs,
      quoteFetchingMs: Number(enrichmentPipelineSummary?.timingMs?.quoteFetchingMs || 0),
      writesMs: Number(enrichmentPipelineSummary?.timingMs?.dbWritesMs || 0),
      totalDurationMs: Date.now() - runStartedAt
    },
    hotWarmCold: layeredScanningSummary?.allSeeds?.layers || buildLayerCounter(0),
    maturity: layeredScanningSummary?.allSeeds?.maturityFunnel || buildMaturityCounter(0),
    candidateStates:
      sourceCatalog?.candidateFunnel && typeof sourceCatalog.candidateFunnel === "object"
        ? sourceCatalog.candidateFunnel
        : {},
    sourceCatalog: sourceCatalogDiagnostics,
    snapshotWarmup: snapshotWarmupSummary,
    enrichmentPipeline: enrichmentPipelineSummary,
    rejectedByItem: toTopRejectedItems(rejectedByItem),
    discardedReasons: normalizeDiscardStats(discardStats),
    batchSizing: buildBatchSizingDiagnostics({
      scannerType: SCANNER_TYPES.ENRICHMENT,
      runDurationMs: Date.now() - runStartedAt,
      selectedItems: enrichmentSeeds.length,
      itemsUpdated: Number(enrichmentPipelineSummary?.enrichedItems || 0)
    })
  }

  return {
    generatedAt: diagnosticsSummary.generatedAt,
    summary: diagnosticsSummary,
    pipeline: {
      sequence: ["prepare_source_catalog", "select_enrichment_batch", "refresh_quotes", "write_quotes"],
      config: {
        enrichmentBatchSize: ENRICHMENT_BATCH_SIZE,
        scanBatchSize: SCAN_BATCH_SIZE,
        maxConcurrentMarketRequests: MAX_CONCURRENT_MARKET_REQUESTS,
        scanTimeoutPerBatchMs: SCAN_TIMEOUT_PER_BATCH
      },
      snapshotWarmup: snapshotWarmupSummary,
      layeredScanning: layeredScanningSummary,
      sourceCatalog: sourceCatalogDiagnostics,
      enrichmentPipeline: enrichmentPipelineSummary
    },
    opportunities: []
  }
}

async function runScan(options = {}) {
  const enqueue = await enqueueScan(options)
  if (enqueue.alreadyRunning && scannerState.inFlight) {
    return scannerState.inFlight
  }
  return scannerState.inFlight || scannerState.latest
}

function normalizeLimit(value, fallback = DEFAULT_API_LIMIT, maxLimit = MAX_API_LIMIT) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.min(Math.max(Math.round(parsed), 1), maxLimit)
}

function toOpportunityDiagnosticsSummary(scanPayload = {}, persistSummary = {}, trigger = "manual") {
  const hotScanSummary =
    scanPayload?.summary?.hotScan && typeof scanPayload.summary.hotScan === "object"
      ? {
          ...scanPayload.summary.hotScan,
          newOpportunitiesAdded: Number(persistSummary?.newCount || 0),
          updatedOpportunities: Number(persistSummary?.updatedCount || 0),
          reactivatedOpportunities: Number(persistSummary?.reactivatedCount || 0)
        }
      : buildHotScanSummary({
          layeredScanning:
            scanPayload?.summary?.layeredScanning || scanPayload?.pipeline?.layeredScanning || {},
          selectedUniverseByCategory: scanPayload?.summary?.selectedUniverseByCategory || {},
          opportunitiesByCategory: scanPayload?.summary?.opportunitiesByCategory || {},
          qualifiedItems: Number(scanPayload?.summary?.qualifiedItems || 0),
          opportunitiesFound: Number(
            scanPayload?.opportunities?.length || scanPayload?.summary?.totalDetected || 0
          ),
          persisted: persistSummary || {}
        })
  return {
    scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    trigger: String(trigger || "manual"),
    generatedAt: scanPayload?.generatedAt || null,
    scannedItems: Number(scanPayload?.summary?.scannedItems || 0),
    qualifiedItems: Number(scanPayload?.summary?.qualifiedItems || 0),
    opportunities: Number(scanPayload?.summary?.opportunities || 0),
    opportunitiesFound: Number(scanPayload?.opportunities?.length || scanPayload?.summary?.totalDetected || 0),
    totalDetected: Number(scanPayload?.summary?.totalDetected || 0),
    universeSize: Number(scanPayload?.summary?.universeSize || 0),
    universeTarget: Number(scanPayload?.summary?.universeTarget || HOT_OPPORTUNITY_SCAN_TARGET),
    candidateItems: Number(scanPayload?.summary?.candidateItems || 0),
    discardedReasons: scanPayload?.summary?.discardedReasons || {},
    discardedReasonsByCategory: scanPayload?.summary?.discardedReasonsByCategory || {},
    rejectedByCategory: scanPayload?.summary?.rejectedByCategory || {},
    topRejectedItems: scanPayload?.summary?.topRejectedItems || [],
    rejectionReasonsByItem: scanPayload?.summary?.rejectionReasonsByItem || [],
    selectedUniverseByCategory: scanPayload?.summary?.selectedUniverseByCategory || {},
    requestedUniverseByCategory: scanPayload?.summary?.requestedUniverseByCategory || {},
    opportunitiesByCategory: scanPayload?.summary?.opportunitiesByCategory || {},
    staleDiagnostics: scanPayload?.summary?.staleDiagnostics || {},
    knifeGloveRejections: scanPayload?.summary?.knifeGloveRejections || {},
    weaponSkinFiltering: scanPayload?.summary?.weaponSkinFiltering || {},
    riskyProfileDiagnostics: scanPayload?.summary?.riskyProfileDiagnostics || {},
    riskyThresholdDiagnostics: scanPayload?.summary?.riskyThresholdDiagnostics || {},
    snapshotWarmup:
      scanPayload?.summary?.snapshotWarmup || scanPayload?.pipeline?.snapshotWarmup || {},
    imageEnrichment:
      scanPayload?.summary?.imageEnrichment || scanPayload?.pipeline?.imageEnrichment || {},
    layeredScanning:
      scanPayload?.summary?.layeredScanning || scanPayload?.pipeline?.layeredScanning || {},
    enrichmentPipeline:
      scanPayload?.summary?.enrichmentPipeline || scanPayload?.pipeline?.enrichmentPipeline || {},
    maturity: scanPayload?.summary?.maturity || {},
    sourceCatalog:
      scanPayload?.summary?.sourceCatalog || scanPayload?.pipeline?.sourceCatalog || {},
    scanProgress: scanPayload?.summary?.scanProgress || {},
    batchSizing: scanPayload?.summary?.batchSizing || {},
    hotScan: hotScanSummary,
    performanceAudit:
      scanPayload?.summary?.performanceAudit || scanPayload?.pipeline?.performanceAudit || {},
    highConfidence: Number(scanPayload?.summary?.highConfidence || 0),
    riskyEligible: Number(scanPayload?.summary?.riskyEligible || 0),
    speculativeEligible: Number(scanPayload?.summary?.speculativeEligible || 0),
    pipeline: scanPayload?.pipeline || {},
    persisted: persistSummary || {}
  }
}

function toEnrichmentDiagnosticsSummary(enrichmentPayload = {}, trigger = "manual") {
  const summary =
    enrichmentPayload?.summary && typeof enrichmentPayload.summary === "object"
      ? enrichmentPayload.summary
      : {}
  return {
    scannerType: SCANNER_TYPES.ENRICHMENT,
    trigger: String(trigger || "manual"),
    generatedAt: enrichmentPayload?.generatedAt || summary?.generatedAt || new Date().toISOString(),
    selectedItems: Number(summary?.selectedItems || 0),
    itemsUpdated: Number(summary?.itemsUpdated || 0),
    promotedToNearEligible: Number(summary?.promotedToNearEligible || 0),
    promotedToEligible: Number(summary?.promotedToEligible || 0),
    stillMissingData: summary?.stillMissingData || {},
    timing: summary?.timing || {},
    hotWarmCold: summary?.hotWarmCold || buildLayerCounter(0),
    maturity: summary?.maturity || buildMaturityCounter(0),
    candidateStates: summary?.candidateStates || {},
    sourceCatalog: summary?.sourceCatalog || enrichmentPayload?.pipeline?.sourceCatalog || {},
    snapshotWarmup: summary?.snapshotWarmup || enrichmentPayload?.pipeline?.snapshotWarmup || {},
    enrichmentPipeline:
      summary?.enrichmentPipeline || enrichmentPayload?.pipeline?.enrichmentPipeline || {},
    batchSizing: summary?.batchSizing || {},
    discardedReasons: summary?.discardedReasons || {},
    rejectedByItem: summary?.rejectedByItem || [],
    pipeline: enrichmentPayload?.pipeline || {}
  }
}

function createJobTimeoutError(scannerType, timeoutMs) {
  const err = new Error(`${scannerType}_job_timeout_${timeoutMs}ms`)
  err.code = "job_timeout"
  err.scannerType = scannerType
  err.timeoutMs = timeoutMs
  return err
}

async function runWithTimeout(jobRunner = async () => null, scannerType = "", timeoutMs = 60000) {
  let timeoutId = null
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(createJobTimeoutError(scannerType, timeoutMs))
    }, timeoutMs)
  })
  try {
    return await Promise.race([jobRunner(), timeoutPromise])
  } finally {
    clearTimeout(timeoutId)
  }
}

async function applyFeedRetention() {
  const cutoffIso = new Date(Date.now() - FEED_RETENTION_HOURS * 60 * 60 * 1000).toISOString()
  const byAge = await arbitrageFeedRepo.markInactiveOlderThan(cutoffIso)
  const byLimit = await arbitrageFeedRepo.markInactiveBeyondLimit(FEED_ACTIVE_LIMIT)
  return {
    retentionCutoffIso: cutoffIso,
    deactivatedByAge: byAge,
    deactivatedByLimit: byLimit
  }
}

async function appendOpportunitiesToFeed(rows = [], scanRunId = "") {
  const candidates = (Array.isArray(rows) ? rows : [])
    .filter((row) => {
      const marketHashName = String(row?.itemName || "").trim()
      if (!marketHashName) return false
      if (!Boolean(row?.isRiskyEligible) && !Boolean(row?.isHighConfidenceEligible)) return false
      const itemCategory = normalizeItemCategory(row?.itemCategory, marketHashName)
      const floor = getRiskyQualityFloor(itemCategory)
      const score = Number(row?.score || 0)
      const minScore = Boolean(row?.isHighConfidenceEligible)
        ? DEFAULT_SCORE_CUTOFF
        : Number(floor?.minScore || FEED_RISKY_MIN_SCORE)
      const minProfit = Boolean(row?.isHighConfidenceEligible)
        ? 0
        : Number(floor?.minProfitUsd || 0)
      return score >= minScore && Number(row?.profit || 0) >= minProfit
    })
    .map((row) => ({
      ...row,
      itemName: String(row?.itemName || "").trim(),
      buyMarket: normalizeMarketLabel(row?.buyMarket),
      sellMarket: normalizeMarketLabel(row?.sellMarket)
    }))

  if (!candidates.length) {
    const retention = await applyFeedRetention()
    return {
      candidates: 0,
      insertedCount: 0,
      newCount: 0,
      updatedCount: 0,
      reactivatedCount: 0,
      duplicateSkipped: 0,
      duplicateInserted: 0,
      replacedActiveCount: 0,
      ...retention
    }
  }

  const dedupeLookbackHours = Math.max(DUPLICATE_WINDOW_HOURS, SIGNAL_HISTORY_LOOKBACK_HOURS)
  const dedupeCutoffIso = new Date(Date.now() - dedupeLookbackHours * 60 * 60 * 1000).toISOString()
  const recentRows = await arbitrageFeedRepo.getRecentRowsByItems({
    itemNames: candidates.map((row) => row.itemName),
    sinceIso: dedupeCutoffIso,
    limit: 10000
  })

  const latestBySignature = {}
  const activeIdsBySignature = {}
  for (const row of recentRows) {
    const signature = buildDedupeSignature(row?.item_name, row?.buy_market, row?.sell_market)
    if (!signature) continue
    if (!latestBySignature[signature]) {
      latestBySignature[signature] = row
    }
    if (Boolean(row?.is_active)) {
      if (!activeIdsBySignature[signature]) {
        activeIdsBySignature[signature] = new Set()
      }
      if (String(row?.id || "").trim()) {
        activeIdsBySignature[signature].add(String(row.id).trim())
      }
    }
  }

  const detectedAt = new Date().toISOString()
  const toInsert = []
  const idsToDeactivate = new Set()
  let duplicateSkipped = 0
  let duplicateInserted = 0
  let newCount = 0
  let updatedCount = 0
  let reactivatedCount = 0
  for (const row of candidates) {
    const signature = buildDedupeSignature(row?.itemName, row?.buyMarket, row?.sellMarket)
    const previous = latestBySignature[signature] || null
    const eventAnalysis = classifyOpportunityFeedEvent(row, previous || {})
    if (!eventAnalysis.materiallyChanged && !INSERT_DUPLICATES) {
      duplicateSkipped += 1
      continue
    }

    const isDuplicate = eventAnalysis.eventType === "duplicate"
    if (isDuplicate) {
      duplicateInserted += 1
    } else if (eventAnalysis.eventType === "new") {
      newCount += 1
    } else if (eventAnalysis.eventType === "updated") {
      updatedCount += 1
    } else if (eventAnalysis.eventType === "reactivated") {
      reactivatedCount += 1
    }

    if (!isDuplicate) {
      for (const id of activeIdsBySignature[signature] || []) {
        idsToDeactivate.add(id)
      }
    }

    toInsert.push(
      buildFeedInsertRow(row, scanRunId, {
        detectedAt,
        isDuplicate,
        eventType: eventAnalysis.eventType,
        eventAnalysis,
        previousRow: previous
      })
    )

    latestBySignature[signature] = {
      id: null,
      profit: row?.profit,
      spread_pct: row?.spread,
      opportunity_score: row?.score,
      execution_confidence: row?.executionConfidence,
      liquidity_label: row?.liquidityBand,
      detected_at: detectedAt,
      is_active: true,
      is_duplicate: isDuplicate,
      metadata: {
        liquidity_value: row?.liquidity,
        volume_7d: row?.volume7d,
        event_type: eventAnalysis.eventType
      }
    }
  }

  const insertedRows = toInsert.length ? await arbitrageFeedRepo.insertRows(toInsert) : []
  const replacedActiveCount = idsToDeactivate.size
    ? await arbitrageFeedRepo.markRowsInactiveByIds(Array.from(idsToDeactivate))
    : 0
  const retention = await applyFeedRetention()

  return {
    candidates: candidates.length,
    insertedCount: insertedRows.length,
    newCount,
    updatedCount,
    reactivatedCount,
    duplicateSkipped,
    duplicateInserted,
    replacedActiveCount,
    ...retention
  }
}

async function runOpportunityWithRunRecord(runRecord = {}, options = {}, state = scannerState) {
  const trigger = String(options.trigger || "manual")
  const forceRefresh = Boolean(options.forceRefresh)
  const runStartedAt = Date.now()
  try {
    const scanPayload = await runWithTimeout(
      () => runScanInternal({ forceRefresh }),
      SCANNER_TYPES.OPPORTUNITY_SCAN,
      getJobTimeoutMs(SCANNER_TYPES.OPPORTUNITY_SCAN)
    )
    const feedPersistStartedAt = Date.now()
    const persistSummary = await appendOpportunitiesToFeed(scanPayload?.opportunities || [], runRecord?.id)
    const feedPersistDurationMs = Date.now() - feedPersistStartedAt
    const performanceAuditBeforePersist =
      scanPayload?.summary?.performanceAudit || scanPayload?.pipeline?.performanceAudit || null
    if (performanceAuditBeforePersist && typeof performanceAuditBeforePersist === "object") {
      const extendedPerformanceAudit = extendPerformanceAudit(performanceAuditBeforePersist, {
        additionalDbWriteDurationMs: feedPersistDurationMs,
        endToEndScanDurationMs: Date.now() - runStartedAt
      })
      scanPayload.summary.performanceAudit = extendedPerformanceAudit
      if (scanPayload.pipeline && typeof scanPayload.pipeline === "object") {
        scanPayload.pipeline.performanceAudit = extendedPerformanceAudit
      }
    }

    const diagnosticsAggregationStartedAt = Date.now()
    const diagnosticsSummary = toOpportunityDiagnosticsSummary(scanPayload, persistSummary, trigger)
    const diagnosticsAggregationDurationMs = Date.now() - diagnosticsAggregationStartedAt
    if (scanPayload?.summary?.performanceAudit && typeof scanPayload.summary.performanceAudit === "object") {
      const extendedPerformanceAudit = extendPerformanceAudit(scanPayload.summary.performanceAudit, {
        additionalDiagnosticsAggregationMs: diagnosticsAggregationDurationMs,
        endToEndScanDurationMs: Date.now() - runStartedAt
      })
      scanPayload.summary.performanceAudit = extendedPerformanceAudit
      if (scanPayload.pipeline && typeof scanPayload.pipeline === "object") {
        scanPayload.pipeline.performanceAudit = extendedPerformanceAudit
      }
      diagnosticsSummary.performanceAudit = extendedPerformanceAudit
    }

    const durationMs = Date.now() - runStartedAt
    const completedRun = await scannerRunRepo.markCompleted(runRecord?.id, {
      itemsScanned: Number(scanPayload?.summary?.scannedItems || 0),
      opportunitiesFound: Number(scanPayload?.opportunities?.length || 0),
      newOpportunitiesAdded: Number(persistSummary?.newCount || 0),
      durationMs,
      diagnosticsSummary
    })
    console.info(
      `[arbitrage-scanner] opportunity_scan run ${
        String(runRecord?.id || "").trim() || "unknown"
      } completed in ${durationMs}ms`
    )

    state.latest = scanPayload
    state.lastError = null
    state.lastPersistSummary = persistSummary
    state.lastCompletedAt = completedRun?.completed_at || scanPayload?.generatedAt || null
    return {
      run: completedRun || runRecord,
      payload: scanPayload,
      persistSummary
    }
  } catch (err) {
    const timedOut = String(err?.code || "").trim().toLowerCase() === "job_timeout"
    state.lastError = err
    console.error(
      `[arbitrage-scanner] opportunity_scan run ${
        String(runRecord?.id || "").trim() || "unknown"
      } failed (${timedOut ? "timed_out" : "failed"})`,
      String(err?.message || err)
    )
    await scannerRunRepo
      .markFailed(runRecord?.id, {
        status: timedOut ? "timed_out" : "failed",
        durationMs: Date.now() - runStartedAt,
        failureReason: timedOut ? "job_timeout" : "opportunity_scan_failed",
        diagnosticsSummary: {
          scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
          trigger,
          error: String(err?.message || "scan_failed")
        },
        error: err?.message
      })
      .catch((persistErr) => {
        console.error("[arbitrage-scanner] Failed to mark run as failed", persistErr.message)
      })
    throw err
  } finally {
    state.inFlight = null
    state.inFlightRunId = null
  }
}

async function runEnrichmentWithRunRecord(runRecord = {}, options = {}, state = enrichmentState) {
  const trigger = String(options.trigger || "manual")
  const forceRefresh = Boolean(options.forceRefresh)
  const runStartedAt = Date.now()
  try {
    const enrichmentPayload = await runWithTimeout(
      () => runEnrichmentInternal({ forceRefresh, trigger }),
      SCANNER_TYPES.ENRICHMENT,
      getJobTimeoutMs(SCANNER_TYPES.ENRICHMENT)
    )
    const diagnosticsSummary = toEnrichmentDiagnosticsSummary(enrichmentPayload, trigger)
    const durationMs = Date.now() - runStartedAt
    const completedRun = await scannerRunRepo.markCompleted(runRecord?.id, {
      itemsScanned: Number(diagnosticsSummary?.selectedItems || 0),
      opportunitiesFound: 0,
      newOpportunitiesAdded: 0,
      durationMs,
      diagnosticsSummary
    })
    console.info(
      `[arbitrage-scanner] enrichment run ${
        String(runRecord?.id || "").trim() || "unknown"
      } completed in ${durationMs}ms`
    )
    state.latest = enrichmentPayload
    state.lastError = null
    state.lastPersistSummary = null
    state.lastCompletedAt = completedRun?.completed_at || enrichmentPayload?.generatedAt || null
    return {
      run: completedRun || runRecord,
      payload: enrichmentPayload,
      persistSummary: null
    }
  } catch (err) {
    const timedOut = String(err?.code || "").trim().toLowerCase() === "job_timeout"
    state.lastError = err
    console.error(
      `[arbitrage-scanner] enrichment run ${
        String(runRecord?.id || "").trim() || "unknown"
      } failed (${timedOut ? "timed_out" : "failed"})`,
      String(err?.message || err)
    )
    await scannerRunRepo
      .markFailed(runRecord?.id, {
        status: timedOut ? "timed_out" : "failed",
        durationMs: Date.now() - runStartedAt,
        failureReason: timedOut ? "job_timeout" : "enrichment_failed",
        diagnosticsSummary: {
          scannerType: SCANNER_TYPES.ENRICHMENT,
          trigger,
          error: String(err?.message || "enrichment_failed")
        },
        error: err?.message
      })
      .catch((persistErr) => {
        console.error("[arbitrage-scanner] Failed to mark enrichment run as failed", persistErr.message)
      })
    throw err
  } finally {
    state.inFlight = null
    state.inFlightRunId = null
  }
}

async function enqueueJob(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN, options = {}) {
  const normalizedType = String(scannerType || SCANNER_TYPES.OPPORTUNITY_SCAN)
    .trim()
    .toLowerCase()
  const state = getJobState(normalizedType)
  const trigger = String(options.trigger || "manual")
  await reconcileStaleRunningRunsForType(normalizedType, Date.now(), { force: true }).catch((err) => {
    console.error(
      `[arbitrage-scanner] Stale run reconciliation before enqueue failed (${normalizedType})`,
      err.message
    )
  })

  if (state.inFlight) {
    const activeRun = {
      id: state.inFlightRunId,
      started_at: state.lastStartedAt
    }
    state.coordination.lockDenied += 1
    state.coordination.skippedAlreadyRunning += 1
    console.info(
      `[arbitrage-scanner] Lock denied for ${normalizedType}: already running (in-memory run ${
        String(state.inFlightRunId || "").trim() || "unknown"
      })`
    )
    await maybeRecordSkippedAlreadyRunningRun(normalizedType, trigger, "already_running", activeRun)
    return buildAlreadyRunningResult(normalizedType, activeRun)
  }

  if (!ALLOW_CROSS_JOB_PARALLELISM) {
    const blockingType = getOtherScannerType(normalizedType)
    const blockingState = getJobState(blockingType)
    let blockingRun = null
    if (blockingState.inFlight) {
      blockingRun = {
        id: blockingState.inFlightRunId,
        started_at: blockingState.lastStartedAt
      }
    } else {
      blockingRun = await scannerRunRepo.getLatestRunningRun(blockingType).catch((err) => {
        console.error(
          `[arbitrage-scanner] Failed to inspect cross-job running rows before enqueue (${normalizedType})`,
          err.message
        )
        return null
      })
    }
    if (blockingRun?.id) {
      state.coordination.crossJobBlocked += 1
      console.info(
        `[arbitrage-scanner] ${normalizedType} enqueue blocked by ${blockingType} run ${blockingRun.id} (ALLOW_CROSS_JOB_PARALLELISM=false)`
      )
      return buildCrossJobBlockedResult(normalizedType, blockingType, blockingRun)
    }
  }

  const createAttempt = await scannerRunRepo.tryCreateRunningRun({
    scannerType: normalizedType,
    diagnosticsSummary: {
      scannerType: normalizedType,
      trigger,
      coordination: {
        event: "lock_acquire_attempt",
        allowCrossJobParallelism: ALLOW_CROSS_JOB_PARALLELISM
      }
    }
  })
  if (createAttempt?.alreadyRunning) {
    const activeRun =
      createAttempt?.existingRun || (await scannerRunRepo.getLatestRunningRun(normalizedType).catch(() => null))
    state.coordination.lockDenied += 1
    state.coordination.skippedAlreadyRunning += 1
    console.info(
      `[arbitrage-scanner] Lock denied for ${normalizedType}: already running (run ${
        String(activeRun?.id || "").trim() || "unknown"
      })`
    )
    await maybeRecordSkippedAlreadyRunningRun(normalizedType, trigger, "already_running", activeRun)
    return buildAlreadyRunningResult(normalizedType, activeRun, {
      id: state.inFlightRunId,
      startedAt: state.lastStartedAt
    })
  }

  const runRecord = createAttempt?.run || null
  if (!runRecord?.id) {
    throw new Error(`[arbitrage-scanner] Failed to acquire run lock for ${normalizedType}`)
  }
  state.coordination.lockAcquired += 1
  console.info(`[arbitrage-scanner] Lock acquired for ${normalizedType} run ${runRecord.id}`)

  state.inFlightRunId = runRecord?.id || null
  state.lastStartedAt = runRecord?.started_at || new Date().toISOString()
  const runner =
    normalizedType === SCANNER_TYPES.ENRICHMENT
      ? runEnrichmentWithRunRecord(runRecord, options, state)
      : runOpportunityWithRunRecord(runRecord, options, state)
  state.inFlight = runner.catch((err) => {
    console.error(
      `[arbitrage-scanner] ${normalizedType} run failed`,
      String(err?.message || err)
    )
    if (state.latest) {
      return {
        run: runRecord,
        payload: state.latest,
        persistSummary: state.lastPersistSummary || null,
        error: err
      }
    }
    return {
      run: runRecord,
      payload: null,
      persistSummary: state.lastPersistSummary || null,
      error: err
    }
  })

  return {
    scannerType: normalizedType,
    scanRunId: runRecord?.id || null,
    alreadyRunning: false,
    status: "started",
    startedAt: runRecord?.started_at || null,
    elapsedMs: 0
  }
}

async function enqueueScan(options = {}) {
  return enqueueJob(SCANNER_TYPES.OPPORTUNITY_SCAN, options)
}

async function enqueueEnrichment(options = {}) {
  return enqueueJob(SCANNER_TYPES.ENRICHMENT, options)
}

async function getLatestRunWithFallback(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN, completed = false) {
  const normalizedType = String(scannerType || SCANNER_TYPES.OPPORTUNITY_SCAN)
    .trim()
    .toLowerCase()
  const primary = completed
    ? await scannerRunRepo.getLatestCompletedRun(normalizedType)
    : await scannerRunRepo.getLatestRun(normalizedType)
  if (primary || normalizedType !== SCANNER_TYPES.OPPORTUNITY_SCAN) {
    return primary || null
  }
  return completed
    ? scannerRunRepo.getLatestCompletedRun(LEGACY_SCANNER_TYPE)
    : scannerRunRepo.getLatestRun(LEGACY_SCANNER_TYPE)
}

async function getJobStatusInternal(scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN, options = {}) {
  const normalizedType = String(scannerType || SCANNER_TYPES.OPPORTUNITY_SCAN)
    .trim()
    .toLowerCase()
  await reconcileStaleRunningRunsForType(normalizedType).catch((err) => {
    console.error(
      `[arbitrage-scanner] Stale run reconciliation during status failed (${normalizedType})`,
      err.message
    )
  })
  const state = getJobState(normalizedType)
  const [latestRun, latestCompletedRun] = await Promise.all([
    getLatestRunWithFallback(normalizedType, false),
    getLatestRunWithFallback(normalizedType, true)
  ])
  const latestRunStatus = String(latestRun?.status || "")
    .trim()
    .toLowerCase()
  const currentStatus = state.inFlight || latestRunStatus === "running" ? "running" : "idle"
  const currentRunId = state.inFlightRunId || (currentStatus === "running" ? latestRun?.id || null : null)
  const currentRunStartedAt =
    currentStatus === "running" ? latestRun?.started_at || state.lastStartedAt || null : null
  const currentRunElapsedMs = currentRunStartedAt ? computeElapsedMs(currentRunStartedAt) : null
  const intervalMinutes =
    normalizedType === SCANNER_TYPES.ENRICHMENT
      ? ENRICHMENT_INTERVAL_MINUTES
      : OPPORTUNITY_SCAN_INTERVAL_MINUTES
  const status = {
    scannerType: normalizedType,
    intervalMinutes,
    schedulerRunning: Boolean(state.timer),
    currentStatus,
    currentRunId,
    currentRunStartedAt,
    currentRunElapsedMs,
    nextScheduledAt: state.nextScheduledAt,
    latestRun,
    latestCompletedRun,
    coordination: {
      lockAcquired: Number(state?.coordination?.lockAcquired || 0),
      lockDenied: Number(state?.coordination?.lockDenied || 0),
      skippedAlreadyRunning: Number(state?.coordination?.skippedAlreadyRunning || 0),
      staleReconciled: Number(state?.coordination?.staleReconciled || 0),
      timedOutReconciled: Number(state?.coordination?.timedOutReconciled || 0),
      crossJobBlocked: Number(state?.coordination?.crossJobBlocked || 0),
      allowCrossJobParallelism: ALLOW_CROSS_JOB_PARALLELISM,
      recordSkippedAlreadyRunning: RECORD_SKIPPED_ALREADY_RUNNING
    }
  }
  if (options.includeActiveCount) {
    status.activeOpportunities = Number(options.activeCount || 0)
  }
  return status
}

async function getScannerStatusInternal() {
  const [activeCount, opportunityStatus, enrichmentStatus] = await Promise.all([
    arbitrageFeedRepo.countFeed({ includeInactive: false }),
    getJobStatusInternal(SCANNER_TYPES.OPPORTUNITY_SCAN),
    getJobStatusInternal(SCANNER_TYPES.ENRICHMENT)
  ])
  return {
    scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
    schedulerRunning: Boolean(scannerState.timer) || Boolean(enrichmentState.timer),
    currentStatus: opportunityStatus?.currentStatus || "idle",
    currentRunId: opportunityStatus?.currentRunId || null,
    currentRunStartedAt: opportunityStatus?.currentRunStartedAt || null,
    currentRunElapsedMs: opportunityStatus?.currentRunElapsedMs ?? null,
    nextScheduledAt: opportunityStatus?.nextScheduledAt || null,
    activeOpportunities: Number(activeCount || 0),
    latestRun: opportunityStatus?.latestRun || null,
    latestCompletedRun: opportunityStatus?.latestCompletedRun || null,
    coordination: opportunityStatus?.coordination || {},
    jobs: {
      [SCANNER_TYPES.OPPORTUNITY_SCAN]: {
        ...opportunityStatus,
        activeOpportunities: Number(activeCount || 0)
      },
      [SCANNER_TYPES.ENRICHMENT]: enrichmentStatus
    }
  }
}

async function ensureScheduledJobHeartbeat(
  scannerType = SCANNER_TYPES.OPPORTUNITY_SCAN,
  statusHint = null,
  trigger = "watchdog"
) {
  const normalizedType = String(scannerType || SCANNER_TYPES.OPPORTUNITY_SCAN)
    .trim()
    .toLowerCase()
  const state = getJobState(normalizedType)
  if (state.inFlight) return false

  const status = statusHint || (await getJobStatusInternal(normalizedType))
  if (!isScannerRunOverdue(status, Date.now(), { scannerType: normalizedType, state })) {
    return false
  }

  const enqueue = await enqueueJob(normalizedType, {
    forceRefresh: false,
    trigger
  })
  if (!enqueue?.alreadyRunning) {
    updateNextScheduledAt(normalizedType)
  }
  return Boolean(enqueue?.scanRunId)
}

function parseIsoTimestampMs(value) {
  const text = String(value || "").trim()
  if (!text) return null
  const ts = new Date(text).getTime()
  return Number.isFinite(ts) ? ts : null
}

async function resolvePlanContext(options = {}) {
  if (options?.entitlements && typeof options.entitlements === "object") {
    return {
      userId: String(options.userId || "").trim(),
      planTier: planService.normalizePlanTier(
        options.planTier || options.entitlements.planTier
      ),
      entitlements: options.entitlements
    }
  }

  const userId = String(options.userId || "").trim()
  if (!userId) {
    const planTier = planService.normalizePlanTier(options.planTier || "full_access")
    return {
      userId: "",
      planTier,
      entitlements: planService.getEntitlements(planTier)
    }
  }

  const { planTier, entitlements } = await planService.getUserPlanProfile(userId)
  return { userId, planTier, entitlements }
}

function applyFeedPlanRestrictions(rows = [], entitlements = {}) {
  const safeRows = Array.isArray(rows) ? rows : []
  const planConfig = planService.getPlanConfig(entitlements?.planTier || entitlements)
  const visibleFeedLimit = Math.max(Number(planConfig?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
  const limitedRows = safeRows.filter((row, index) =>
    Boolean(
      planService.canViewOpportunity(
        planConfig,
        { position: index },
        row?.itemCategory || "weapon_skin"
      )?.visibleAllowed
    )
  )

  const delayedSignals = planService.hasFeatureAccess(planConfig, "delayed_signals")
  const signalDelayMinutes = delayedSignals
    ? Math.max(Number(planConfig?.signalDelayMinutes || 0), 0)
    : 0
  const cutoffTs = signalDelayMinutes > 0 ? Date.now() - signalDelayMinutes * 60 * 1000 : null
  const delayFilteredRows =
    cutoffTs == null
      ? limitedRows
      : limitedRows.filter((row) => {
          const detectedAtTs = parseIsoTimestampMs(row?.detectedAt || row?.detected_at)
          if (detectedAtTs == null) return true
          return detectedAtTs <= cutoffTs
        })
  const premiumPreviewResult = premiumCategoryAccessService.applyPremiumPreviewLock(
    delayFilteredRows,
    entitlements
  )
  const premiumCategoryAccess = premiumCategoryAccessService.hasPremiumCategoryAccess(entitlements)

  return {
    rows: premiumPreviewResult.rows,
    planLimits: {
      visibleFeedLimit,
      delayedSignals,
      signalDelayMinutes,
      advancedFilters: planService.canUseAdvancedFilters(planConfig),
      fullGlobalScanner: planService.hasFeatureAccess(planConfig, "full_global_scanner"),
      fullOpportunitiesFeed: planService.hasFeatureAccess(
        planConfig,
        "full_opportunities_feed"
      ),
      premiumCategoryAccess,
      knivesGlovesAccess: premiumCategoryAccess,
      lockedPremiumPreviewRows: Number(premiumPreviewResult.lockedCount || 0),
      feedTruncatedByLimit: Math.max(safeRows.length - limitedRows.length, 0),
      feedTruncatedByDelay: Math.max(limitedRows.length - delayFilteredRows.length, 0),
      feedTruncatedByPremiumLock: 0
    }
  }
}

function pruneManualRefreshTracker(nowMs) {
  if (manualRefreshTracker.size <= MANUAL_REFRESH_TRACKER_MAX) {
    return
  }

  const staleCutoffMs = nowMs - 7 * 24 * 60 * 60 * 1000
  for (const [userId, entry] of manualRefreshTracker.entries()) {
    if (Number(entry?.lastTriggeredAtMs || 0) < staleCutoffMs) {
      manualRefreshTracker.delete(userId)
    }
    if (manualRefreshTracker.size <= MANUAL_REFRESH_TRACKER_MAX) {
      break
    }
  }
}

function formatRetryWindow(remainingMs) {
  const remainingMinutes = Math.max(Math.ceil(Number(remainingMs || 0) / 60000), 1)
  if (remainingMinutes >= 60) {
    const hours = Math.max(Math.ceil(remainingMinutes / 60), 1)
    return `${hours} hour(s)`
  }
  return `${remainingMinutes} minute(s)`
}

function enforceManualRefreshCooldown(userId, entitlements = {}, nowMs = Date.now()) {
  const safeUserId = String(userId || "").trim()
  if (!safeUserId) return

  const previous = manualRefreshTracker.get(safeUserId)
  const lastTriggeredAtMs = Number(previous?.lastTriggeredAtMs || 0)
  const refreshPolicy = planService.canRefreshScanner(entitlements, lastTriggeredAtMs, {
    nowMs
  })
  const intervalMinutes = Math.max(
    Number(
      refreshPolicy?.intervalMinutes == null
        ? SCANNER_INTERVAL_MINUTES
        : refreshPolicy.intervalMinutes
    ),
    0
  )

  if (!refreshPolicy.allowed) {
    const retryAfterMs = Math.max(Number(refreshPolicy?.retryAfterMs || 0), 0)
    const err = new AppError(
      `Manual scanner refresh is available every ${intervalMinutes} minute(s) on your plan. Try again in ${formatRetryWindow(
        retryAfterMs
      )}.`,
      429,
      "SCANNER_REFRESH_COOLDOWN"
    )
    err.retryAfterMs = retryAfterMs
    throw err
  }

  manualRefreshTracker.set(safeUserId, {
    lastTriggeredAtMs: nowMs,
    intervalMinutes
  })
  pruneManualRefreshTracker(nowMs)
}

function resolveNoOpportunitiesReason(summary = {}, status = {}, opportunities = []) {
  if (Array.isArray(opportunities) && opportunities.length) {
    return null
  }

  const currentStatus = String(status?.currentStatus || "")
    .trim()
    .toLowerCase()
  if (currentStatus === "running") {
    return {
      code: "scan_in_progress",
      message: "Scanner run is in progress. Feed will update after completion."
    }
  }

  const scannedItems = Number(summary?.scannedItems || 0)
  if (!scannedItems) {
    const sourceCatalog = summary?.sourceCatalog || {}
    const missingToTarget = Number(sourceCatalog?.universeBuild?.missingToTarget || 0)
    if (missingToTarget > 0) {
      return {
        code: "insufficient_catalog_coverage",
        count: missingToTarget,
        message: `Universe build is short by ${missingToTarget} item(s) due to insufficient eligible source catalog coverage.`
      }
    }
    return {
      code: "no_items_scanned",
      message: "No universe items were eligible for scan in the latest run."
    }
  }

  const discardedReasons = summary?.discardedReasons && typeof summary.discardedReasons === "object"
    ? summary.discardedReasons
    : {}
  const topReasonEntry = Object.entries(discardedReasons)
    .filter(([, count]) => Number(count || 0) > 0)
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))[0]
  if (!topReasonEntry) {
    return {
      code: "no_matching_feed_rows",
      message: "Scanner completed but no opportunities passed current filters."
    }
  }

  const [code, count] = topReasonEntry
  const reasonMessageMap = {
    ignored_execution_floor: "Most candidates failed execution floor checks.",
    ignored_low_price: "Most candidates failed execution floor checks.",
    ignored_low_value_universe: "Most candidates were filtered as low-value universe items.",
    hard_reject_low_value: "Most weapon skins were rejected as low-value across multiple weak signals.",
    ignored_low_liquidity: "Most candidates were rejected for low liquidity.",
    risky_low_price: "Most risky candidates were below category price floors.",
    risky_low_profit: "Most risky candidates had insufficient profit for their category.",
    risky_low_score: "Most risky candidates scored below category quality floors.",
    risky_low_confidence: "Most risky candidates had low execution confidence.",
    risky_missing_depth: "Most risky candidates failed depth checks under risky mode.",
    spread_below_min: "Most candidates were below spread baseline.",
    non_positive_profit: "Most candidates had non-positive projected profit.",
    ignored_extreme_spread: "Most candidates were rejected for extreme spread values.",
    ignored_reference_deviation: "Most candidates failed reference deviation checks.",
    ignored_missing_markets: "Most candidates were missing enough market coverage.",
    ignored_missing_liquidity_data: "Most candidates were missing liquidity data.",
    hard_reject_missing_liquidity:
      "Most weapon skins were missing liquidity support and failed fallback evidence checks.",
    ignored_missing_depth: "Most candidates were rejected due to missing orderbook depth.",
    ignored_low_score: "Most candidates scored below profile thresholds.",
    ignored_stale_data: "Most candidates were rejected due to stale data."
  }
  return {
    code,
    count: Number(count || 0),
    message:
      reasonMessageMap[code] || "Most candidates were rejected by scanner quality filters."
  }
}

exports.getFeed = async (options = {}) => {
  const planContext = await resolvePlanContext(options)
  const entitlements =
    planContext?.entitlements || planService.getEntitlements(planContext?.planTier)
  const planConfig = planService.getPlanConfig(planContext?.planTier || entitlements?.planTier || "free")
  const advancedFiltersEnabled = planService.canUseAdvancedFilters(entitlements)
  const requestedLimit = normalizeLimit(options.limit, DEFAULT_API_LIMIT, MAX_FEED_LIMIT)
  const planVisibleLimit = Math.max(Number(planConfig?.visibleFeedLimit || MAX_FEED_LIMIT), 1)
  const limit = Math.min(requestedLimit, planVisibleLimit)
  const requestedShowRisky = normalizeBoolean(options.showRisky)
  const requestedIncludeOlder = normalizeBoolean(options.includeOlder || options.showOlder)
  const requestedCategory = normalizeCategoryFilter(options.category)
  const showRisky = advancedFiltersEnabled ? requestedShowRisky : false
  const includeOlder = advancedFiltersEnabled ? requestedIncludeOlder : false
  const categoryFilter = advancedFiltersEnabled ? requestedCategory : "all"

  const minScore = showRisky ? FEED_RISKY_MIN_SCORE : DEFAULT_SCORE_CUTOFF
  const excludeLowConfidence = !showRisky
  const highConfidenceOnly = !showRisky

  const [feedRows, totalCount, activeCount, status] = await Promise.all([
    arbitrageFeedRepo.listFeed({
      limit,
      includeInactive: includeOlder,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence,
      highConfidenceOnly
    }),
    arbitrageFeedRepo.countFeed({
      includeInactive: includeOlder,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence,
      highConfidenceOnly
    }),
    arbitrageFeedRepo.countFeed({
      includeInactive: false,
      category: categoryFilter === "all" ? "" : categoryFilter,
      minScore,
      excludeLowConfidence,
      highConfidenceOnly
    }),
    getScannerStatusInternal()
  ])

  let mappedRows = (Array.isArray(feedRows) ? feedRows : []).map((row) => mapFeedRowToApiRow(row))
  mappedRows = await enrichFeedRowsWithSkinMetadata(mappedRows)
  const feedRestrictionResult = applyFeedPlanRestrictions(mappedRows, entitlements)
  mappedRows = feedRestrictionResult.rows
  const feedPlanLimits = feedRestrictionResult.planLimits
  const latestCompleted = status?.latestCompletedRun || null
  const diagnosticsSummary =
    latestCompleted?.diagnostics_summary && typeof latestCompleted.diagnostics_summary === "object"
      ? latestCompleted.diagnostics_summary
      : {}
  const summary = {
    scannedItems:
      Number(diagnosticsSummary?.scannedItems || 0) || Number(latestCompleted?.items_scanned || 0),
    qualifiedItems: Number(diagnosticsSummary?.qualifiedItems || 0),
    opportunities: mappedRows.length,
    totalDetected: Number(totalCount || mappedRows.length),
    activeOpportunities: Number(activeCount || 0),
    universeSize: Number(diagnosticsSummary?.universeSize || 0),
    universeTarget: Number(diagnosticsSummary?.universeTarget || HOT_OPPORTUNITY_SCAN_TARGET),
    candidateItems: Number(diagnosticsSummary?.candidateItems || 0),
    discardedReasons: diagnosticsSummary?.discardedReasons || {},
    discardedReasonsByCategory: diagnosticsSummary?.discardedReasonsByCategory || {},
    rejectedByCategory: diagnosticsSummary?.rejectedByCategory || {},
    topRejectedItems: diagnosticsSummary?.topRejectedItems || [],
    rejectionReasonsByItem: diagnosticsSummary?.rejectionReasonsByItem || [],
    selectedUniverseByCategory: diagnosticsSummary?.selectedUniverseByCategory || {},
    requestedUniverseByCategory: diagnosticsSummary?.requestedUniverseByCategory || {},
    opportunitiesByCategory: diagnosticsSummary?.opportunitiesByCategory || {},
    staleDiagnostics: diagnosticsSummary?.staleDiagnostics || {},
    knifeGloveRejections: diagnosticsSummary?.knifeGloveRejections || {},
    weaponSkinFiltering: diagnosticsSummary?.weaponSkinFiltering || {},
    riskyProfileDiagnostics: diagnosticsSummary?.riskyProfileDiagnostics || {},
    riskyThresholdDiagnostics: diagnosticsSummary?.riskyThresholdDiagnostics || {},
    snapshotWarmup:
      diagnosticsSummary?.snapshotWarmup || diagnosticsSummary?.pipeline?.snapshotWarmup || {},
    imageEnrichment:
      diagnosticsSummary?.imageEnrichment || diagnosticsSummary?.pipeline?.imageEnrichment || {},
    layeredScanning:
      diagnosticsSummary?.layeredScanning || diagnosticsSummary?.pipeline?.layeredScanning || {},
    enrichmentPipeline:
      diagnosticsSummary?.enrichmentPipeline || diagnosticsSummary?.pipeline?.enrichmentPipeline || {},
    maturity: diagnosticsSummary?.maturity || {},
    sourceCatalog:
      diagnosticsSummary?.sourceCatalog || diagnosticsSummary?.pipeline?.sourceCatalog || {},
    scanProgress: diagnosticsSummary?.scanProgress || {},
    batchSizing: diagnosticsSummary?.batchSizing || {},
    hotScan: diagnosticsSummary?.hotScan || {},
    performanceAudit:
      diagnosticsSummary?.performanceAudit || diagnosticsSummary?.pipeline?.performanceAudit || {},
    highConfidence: Number(diagnosticsSummary?.highConfidence || 0),
    riskyEligible: Number(diagnosticsSummary?.riskyEligible || 0),
    speculativeEligible: Number(diagnosticsSummary?.speculativeEligible || 0),
    newOpportunitiesAdded:
      Number(latestCompleted?.new_opportunities_added || diagnosticsSummary?.persisted?.newCount || 0),
    updatedOpportunities: Number(diagnosticsSummary?.persisted?.updatedCount || 0),
    reactivatedOpportunities: Number(diagnosticsSummary?.persisted?.reactivatedCount || 0),
    signalEventsAdded: Number(diagnosticsSummary?.persisted?.insertedCount || 0),
    feedRetentionHours: FEED_RETENTION_HOURS,
    feedActiveLimit: FEED_ACTIVE_LIMIT,
    plan: {
      planTier: planContext?.planTier || "free",
      requestedLimit,
      appliedLimit: limit,
      requestedShowRisky,
      appliedShowRisky: showRisky,
      requestedIncludeOlder,
      appliedIncludeOlder: includeOlder,
      requestedCategory,
      appliedCategory: categoryFilter,
      ...feedPlanLimits
    }
  }
  if (!mappedRows.length && Number(feedPlanLimits?.feedTruncatedByDelay || 0) > 0) {
    summary.noOpportunitiesReason = {
      code: "signals_delayed_by_plan",
      message: `Signals are delayed by ${Number(feedPlanLimits?.signalDelayMinutes || 0)} minute(s) on your current plan.`
    }
  } else {
    summary.noOpportunitiesReason = resolveNoOpportunitiesReason(summary, status, mappedRows)
  }

  return {
    generatedAt: latestCompleted?.completed_at || latestCompleted?.started_at || null,
    ttlSeconds: Math.round(CACHE_TTL_MS / 1000),
    currency: "USD",
    summary,
    opportunities: mappedRows,
    plan: summary.plan,
    status: {
      scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
      intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
      schedulerRunning: Boolean(status?.schedulerRunning),
      currentStatus: status?.currentStatus || "idle",
      currentRunId: status?.currentRunId || null,
      nextScheduledAt: status?.nextScheduledAt || null,
      activeOpportunities: Number(status?.activeOpportunities || 0),
      latestRun: status?.latestRun || null,
      latestCompletedRun: latestCompleted,
      jobs: status?.jobs || {}
    }
  }
}

exports.getTopOpportunities = async (options = {}) => exports.getFeed(options)

exports.triggerRefresh = async (options = {}) => {
  const planContext = await resolvePlanContext(options)
  enforceManualRefreshCooldown(planContext?.userId, planContext?.entitlements, Date.now())
  const forceRefresh = options.forceRefresh == null ? true : normalizeBoolean(options.forceRefresh)
  const requestedJobType = String(options.jobType || "").trim().toLowerCase()
  const trigger = String(options.trigger || "manual")
  const runOpportunity = !requestedJobType || requestedJobType === SCANNER_TYPES.OPPORTUNITY_SCAN
  const runEnrichment = !requestedJobType || requestedJobType === SCANNER_TYPES.ENRICHMENT
  const [opportunityEnqueue, enrichmentEnqueue] = await Promise.all([
    runOpportunity ? enqueueScan({ forceRefresh, trigger }) : Promise.resolve(null),
    runEnrichment ? enqueueEnrichment({ forceRefresh, trigger }) : Promise.resolve(null)
  ])
  const toJobResult = (result) =>
    !result
      ? null
      : {
          scanRunId: result?.scanRunId || null,
          status: String(
            result?.status || (result?.alreadyRunning ? "already_running" : "started")
          ),
          alreadyRunning: Boolean(result?.alreadyRunning),
          startedAt: result?.startedAt || null,
          elapsedMs: result?.elapsedMs ?? null,
          existingRunId: result?.existingRunId || null,
          existingRunStartedAt: result?.existingRunStartedAt || null,
          blockedByCrossJob: Boolean(result?.blockedByCrossJob),
          blockingScannerType: result?.blockingScannerType || null,
          blockingRunId: result?.blockingRunId || null,
          blockingRunStartedAt: result?.blockingRunStartedAt || null,
          blockingElapsedMs: result?.blockingElapsedMs ?? null
        }
  const scanRunId =
    opportunityEnqueue?.scanRunId || enrichmentEnqueue?.scanRunId || null
  const alreadyRunning = Boolean(
    (runOpportunity ? opportunityEnqueue?.alreadyRunning : true) &&
      (runEnrichment ? enrichmentEnqueue?.alreadyRunning : true)
  )
  return {
    scanRunId,
    alreadyRunning,
    startedAt: new Date().toISOString(),
    jobs: {
      [SCANNER_TYPES.OPPORTUNITY_SCAN]: runOpportunity
        ? toJobResult(opportunityEnqueue)
        : null,
      [SCANNER_TYPES.ENRICHMENT]: runEnrichment
        ? toJobResult(enrichmentEnqueue)
        : null
    },
    plan: {
      planTier: planContext?.planTier || "free",
      scannerRefreshIntervalMinutes: (() => {
        const configuredInterval = Number(
          planService.getPlanConfig(planContext?.entitlements || planContext?.planTier)
            .scannerRefreshIntervalMinutes
        )
        if (Number.isFinite(configuredInterval)) {
          return Math.max(configuredInterval, 0)
        }
        return SCANNER_INTERVAL_MINUTES
      })(),
      allowCrossJobParallelism: ALLOW_CROSS_JOB_PARALLELISM
    }
  }
}

exports.getStatus = async () => {
  const status = await getScannerStatusInternal()
  return {
    scannerType: SCANNER_TYPES.OPPORTUNITY_SCAN,
    intervalMinutes: OPPORTUNITY_SCAN_INTERVAL_MINUTES,
    schedulerRunning: Boolean(status?.schedulerRunning),
    currentStatus: status?.currentStatus || "idle",
    currentRunId: status?.currentRunId || null,
    currentRunStartedAt: status?.currentRunStartedAt || null,
    currentRunElapsedMs: status?.currentRunElapsedMs ?? null,
    nextScheduledAt: status?.nextScheduledAt || null,
    activeOpportunities: Number(status?.activeOpportunities || 0),
    latestRun: status?.latestRun || null,
    latestCompletedRun: status?.latestCompletedRun || null,
    coordination: status?.coordination || {},
    jobs: status?.jobs || {}
  }
}

exports.startScheduler = () => {
  if (!enrichmentState.timer) {
    enqueueEnrichment({ forceRefresh: false, trigger: "startup_enrichment" }).catch((err) => {
      console.error("[arbitrage-scanner] Initial enrichment enqueue failed", err.message)
    })
    updateNextScheduledAt(SCANNER_TYPES.ENRICHMENT)
    enrichmentState.timer = setInterval(() => {
      enqueueEnrichment({ forceRefresh: false, trigger: "scheduled_enrichment" }).catch((err) => {
        console.error("[arbitrage-scanner] Scheduled enrichment enqueue failed", err.message)
      })
      updateNextScheduledAt(SCANNER_TYPES.ENRICHMENT)
    }, ENRICHMENT_INTERVAL_MS)
    enrichmentState.timer.unref?.()
  }

  if (!scannerState.timer) {
    enqueueScan({ forceRefresh: false, trigger: "startup_opportunity_scan" }).catch((err) => {
      console.error("[arbitrage-scanner] Initial opportunity scan enqueue failed", err.message)
    })
    updateNextScheduledAt(SCANNER_TYPES.OPPORTUNITY_SCAN)
    scannerState.timer = setInterval(() => {
      enqueueScan({ forceRefresh: false, trigger: "scheduled_opportunity_scan" }).catch((err) => {
        console.error("[arbitrage-scanner] Scheduled opportunity scan enqueue failed", err.message)
      })
      updateNextScheduledAt(SCANNER_TYPES.OPPORTUNITY_SCAN)
    }, OPPORTUNITY_SCAN_INTERVAL_MS)
    scannerState.timer.unref?.()
  }

  console.log(
    `[arbitrage-scanner] Scheduler started (enrichment=${ENRICHMENT_INTERVAL_MINUTES}m, opportunity_scan=${OPPORTUNITY_SCAN_INTERVAL_MINUTES}m)`
  )
}

exports.stopScheduler = () => {
  if (scannerState.timer) {
    clearInterval(scannerState.timer)
    scannerState.timer = null
    scannerState.nextScheduledAt = null
  }
  if (enrichmentState.timer) {
    clearInterval(enrichmentState.timer)
    enrichmentState.timer = null
    enrichmentState.nextScheduledAt = null
  }
}

exports.forceRefresh = async () => runScan({ forceRefresh: true, trigger: "manual" })

exports.__testables = {
  normalizeUniverseEntries,
  normalizeItemCategory,
  normalizeCategoryFilter,
  computeLiquidityScoreFromSnapshot,
  resolveVolume7d,
  resolveStaleDataPenalty,
  resolveLiquidityMetrics,
  passesUniverseSeedFilters,
  passesScannerGuards,
  computeRiskAdjustments,
  buildApiOpportunityRow,
  buildFeedInsertRow,
  mapFeedRowToApiRow,
  classifyOpportunityFeedEvent,
  isMateriallyNewOpportunity,
  isScannerRunOverdue,
  clampScore,
  computeLiquidityRank,
  countAvailableMarkets,
  isLowValueJunkName,
  computeStrictCoverageThreshold,
  buildRiskyProfileDiagnostics,
  trackRiskyDecision,
  trackRiskyBaselineOutcome,
  trackRiskyBorderlinePromotion,
  toRiskyProfileDiagnosticsSummary,
  resolveMaturityStateForSeed,
  resolveScanLayerForMaturity,
  resolveCatalogSeedFreshnessContext,
  evaluateOpportunitySeedAdmission,
  summarizeOpportunitySeedAdmissions,
  isOpportunityScanReadySeed,
  isMinimumOpportunityBackfillReadySeed,
  summarizeSnapshotWarmupBacklog,
  mergeSeedWithSnapshot,
  computeLayerPriority,
  selectSeedsForLayeredScanning,
  DEFAULT_UNIVERSE_LIMIT,
  HOT_OPPORTUNITY_SCAN_TARGET,
  SCAN_BATCH_SIZE,
  MAX_CONCURRENT_MARKET_REQUESTS,
  SCAN_TIMEOUT_PER_BATCH
}

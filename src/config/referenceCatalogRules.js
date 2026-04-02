const PRIMARY_GENERATION_CATEGORIES = Object.freeze([
  "weapon_skin",
  "case",
  "sticker_capsule"
])

const PRIMARY_CATEGORY_SHARE_RULES = Object.freeze({
  weapon_skin: Object.freeze({
    min: 0.76,
    target: 0.76,
    max: 0.76
  }),
  case: Object.freeze({
    min: 0.14,
    target: 0.14,
    max: 0.14
  }),
  sticker_capsule: Object.freeze({
    min: 0.1,
    target: 0.1,
    max: 0.1
  })
})

const ACTIVE_GENERATION_TARGET = Object.freeze({
  min: 500,
  target: 500,
  max: 500
})

const REFERENCE_SEED_TARGET = Object.freeze({
  min: 500,
  target: 500,
  max: 500
})

const HEALTHY_OUTPUT_TARGET = Object.freeze({
  scannable: Object.freeze({
    min: 250,
    target: 350,
    max: 500
  }),
  hot_universe: Object.freeze({
    min: 120,
    target: 180,
    max: 320
  })
})

const ACTIVE_ADMISSION_RULES = Object.freeze({
  requireReferencePrice: true,
  requireCoverageCountAboveZero: true,
  requireUsableFreshness: true,
  prioritySupportRequiresFreshEvidence: true,
  recentHistoryMaxAgeHours: 168
})

const REPEATED_FAILURE_RULES = Object.freeze({
  consecutiveMissesRequired: 2,
  types: Object.freeze([
    "no_coverage",
    "no_reference",
    "no_freshness"
  ])
})

module.exports = Object.freeze({
  PRIMARY_GENERATION_CATEGORIES,
  PRIMARY_CATEGORY_SHARE_RULES,
  ACTIVE_GENERATION_TARGET,
  REFERENCE_SEED_TARGET,
  HEALTHY_OUTPUT_TARGET,
  ACTIVE_ADMISSION_RULES,
  REPEATED_FAILURE_RULES
})

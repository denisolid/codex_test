const CANONICAL_RARITY = Object.freeze({
  CONSUMER_GRADE: "consumer_grade",
  INDUSTRIAL_GRADE: "industrial_grade",
  MIL_SPEC_GRADE: "mil_spec_grade",
  RESTRICTED: "restricted",
  CLASSIFIED: "classified",
  COVERT: "covert",
  CONTRABAND: "contraband",
  KNIFE_GLOVES: "knife_gloves",
  UNKNOWN: "unknown"
})

const CANONICAL_RARITY_DISPLAY = Object.freeze({
  [CANONICAL_RARITY.CONSUMER_GRADE]: "Consumer Grade",
  [CANONICAL_RARITY.INDUSTRIAL_GRADE]: "Industrial Grade",
  [CANONICAL_RARITY.MIL_SPEC_GRADE]: "Mil-Spec Grade",
  [CANONICAL_RARITY.RESTRICTED]: "Restricted",
  [CANONICAL_RARITY.CLASSIFIED]: "Classified",
  [CANONICAL_RARITY.COVERT]: "Covert",
  [CANONICAL_RARITY.CONTRABAND]: "Contraband",
  [CANONICAL_RARITY.KNIFE_GLOVES]: "Knife/Gloves",
  [CANONICAL_RARITY.UNKNOWN]: "Unknown"
})

const CANONICAL_RARITY_COLORS = Object.freeze({
  [CANONICAL_RARITY.CONSUMER_GRADE]: "#b0c3d9",
  [CANONICAL_RARITY.INDUSTRIAL_GRADE]: "#5e98d9",
  [CANONICAL_RARITY.MIL_SPEC_GRADE]: "#4b69ff",
  [CANONICAL_RARITY.RESTRICTED]: "#8847ff",
  [CANONICAL_RARITY.CLASSIFIED]: "#d32ce6",
  [CANONICAL_RARITY.COVERT]: "#eb4b4b",
  [CANONICAL_RARITY.CONTRABAND]: "#e4ae39",
  [CANONICAL_RARITY.KNIFE_GLOVES]: "#f7ca63",
  [CANONICAL_RARITY.UNKNOWN]: "#8a93a3"
})

const CANONICAL_RARITY_SET = new Set(Object.values(CANONICAL_RARITY))

const CATEGORY = Object.freeze({
  WEAPON_SKIN: "weapon_skin",
  CASE: "case",
  STICKER_CAPSULE: "sticker_capsule",
  KNIFE: "knife",
  GLOVE: "glove"
})

const CATEGORY_SET = new Set(Object.values(CATEGORY))
const KNIFE_GLOVE_TOKEN_PATTERN =
  /(?:^|\s)(?:knife|knives|glove|gloves|wraps|bayonet|karambit|butterfly|talon|ursus|navaja|stiletto|falchion|daggers|hand wraps|shadow daggers|huntsman|bowie)(?:\s|$)/i
const CASE_TOKEN_PATTERN = /\b(case|container|souvenir package|gift package)\b/i
const CAPSULE_TOKEN_PATTERN = /\b(capsule|autograph capsule|sticker capsule)\b/i

const UNKNOWN_TEXT_SET = new Set([
  "unknown",
  "default",
  "none",
  "n/a",
  "na",
  "null",
  "-",
  "?"
])

const TEXT_TO_CANONICAL = Object.freeze({
  "consumer grade": CANONICAL_RARITY.CONSUMER_GRADE,
  "base grade": CANONICAL_RARITY.CONSUMER_GRADE,
  "industrial grade": CANONICAL_RARITY.INDUSTRIAL_GRADE,
  "high grade": CANONICAL_RARITY.INDUSTRIAL_GRADE,
  "mil-spec grade": CANONICAL_RARITY.MIL_SPEC_GRADE,
  "mil spec grade": CANONICAL_RARITY.MIL_SPEC_GRADE,
  "mil-spec": CANONICAL_RARITY.MIL_SPEC_GRADE,
  restricted: CANONICAL_RARITY.RESTRICTED,
  remarkable: CANONICAL_RARITY.RESTRICTED,
  classified: CANONICAL_RARITY.CLASSIFIED,
  exotic: CANONICAL_RARITY.CLASSIFIED,
  covert: CANONICAL_RARITY.COVERT,
  immortal: CANONICAL_RARITY.COVERT,
  contraband: CANONICAL_RARITY.CONTRABAND,
  extraordinary: CANONICAL_RARITY.KNIFE_GLOVES,
  "knife/gloves": CANONICAL_RARITY.KNIFE_GLOVES,
  knife: CANONICAL_RARITY.KNIFE_GLOVES,
  gloves: CANONICAL_RARITY.KNIFE_GLOVES
})

function normalizeText(value) {
  return String(value || "").trim()
}

function sanitizeHexColor(value) {
  const raw = normalizeText(value).replace(/^#/, "")
  if (!/^[\da-f]{6}$/i.test(raw)) return null
  return `#${raw.toLowerCase()}`
}

function normalizeCategory(value, marketHashName = "", weapon = "") {
  const raw = normalizeText(value).toLowerCase()
  if (CATEGORY_SET.has(raw)) return raw
  const source = `${normalizeText(marketHashName)} ${normalizeText(weapon)}`.toLowerCase()
  if (/\u2605/.test(source) || KNIFE_GLOVE_TOKEN_PATTERN.test(source)) {
    if (/\bglove|gloves|wraps|hand wraps\b/.test(source)) {
      return CATEGORY.GLOVE
    }
    return CATEGORY.KNIFE
  }
  if (CAPSULE_TOKEN_PATTERN.test(source)) return CATEGORY.STICKER_CAPSULE
  if (CASE_TOKEN_PATTERN.test(source)) return CATEGORY.CASE
  return CATEGORY.WEAPON_SKIN
}

function normalizeCanonicalRarity(value, options = {}) {
  const raw = normalizeText(value)
  if (!raw) return null

  const safeLower = raw.toLowerCase().replace(/\s+/g, " ")
  if (CANONICAL_RARITY_SET.has(safeLower)) return safeLower
  if (UNKNOWN_TEXT_SET.has(safeLower)) return null

  const alias = TEXT_TO_CANONICAL[safeLower]
  if (alias) return alias

  const source = `${safeLower} ${normalizeText(options.marketHashName)} ${normalizeText(options.weapon)}`.toLowerCase()
  if (/\u2605/.test(raw) || KNIFE_GLOVE_TOKEN_PATTERN.test(source)) {
    return CANONICAL_RARITY.KNIFE_GLOVES
  }
  if (/\bcontraband\b/.test(source)) return CANONICAL_RARITY.CONTRABAND
  if (/\b(?:covert|immortal)\b/.test(source)) return CANONICAL_RARITY.COVERT
  if (/\b(?:classified|exotic)\b/.test(source)) return CANONICAL_RARITY.CLASSIFIED
  if (/\b(?:restricted|remarkable)\b/.test(source)) return CANONICAL_RARITY.RESTRICTED
  if (/\bmil[- ]?spec\b/.test(source)) return CANONICAL_RARITY.MIL_SPEC_GRADE
  if (/\bindustrial grade\b|\bhigh grade\b/.test(source)) {
    return CANONICAL_RARITY.INDUSTRIAL_GRADE
  }
  if (/\bconsumer grade\b|\bbase grade\b/.test(source)) {
    return CANONICAL_RARITY.CONSUMER_GRADE
  }

  return null
}

function resolveDeterministicFallback(options = {}) {
  const safeCategory = normalizeCategory(
    options.category,
    options.marketHashName,
    options.weapon
  )
  const source =
    `${normalizeText(options.marketHashName)} ${normalizeText(options.weapon)} ${safeCategory}`
      .toLowerCase()

  if (/\bcontraband\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.CONTRABAND,
      fallbackReason: "fallback_contraband_token"
    }
  }

  if (safeCategory === CATEGORY.KNIFE || safeCategory === CATEGORY.GLOVE) {
    return {
      canonicalRarity: CANONICAL_RARITY.KNIFE_GLOVES,
      fallbackReason: "fallback_category_knife_glove"
    }
  }

  if (/\u2605/.test(source) || KNIFE_GLOVE_TOKEN_PATTERN.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.KNIFE_GLOVES,
      fallbackReason: "fallback_item_token_knife_glove"
    }
  }

  if (/\b(?:covert|immortal)\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.COVERT,
      fallbackReason: "fallback_item_token_covert"
    }
  }

  if (/\b(?:classified|exotic)\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.CLASSIFIED,
      fallbackReason: "fallback_item_token_classified"
    }
  }

  if (/\b(?:restricted|remarkable)\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.RESTRICTED,
      fallbackReason: "fallback_item_token_restricted"
    }
  }

  if (/\bmil[- ]?spec\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.MIL_SPEC_GRADE,
      fallbackReason: "fallback_item_token_milspec"
    }
  }

  if (/\bindustrial grade\b|\bhigh grade\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.INDUSTRIAL_GRADE,
      fallbackReason: "fallback_item_token_industrial"
    }
  }

  if (/\bconsumer grade\b|\bbase grade\b/.test(source)) {
    return {
      canonicalRarity: CANONICAL_RARITY.CONSUMER_GRADE,
      fallbackReason: "fallback_item_token_consumer"
    }
  }

  if (safeCategory === CATEGORY.CASE || safeCategory === CATEGORY.STICKER_CAPSULE) {
    return {
      canonicalRarity: CANONICAL_RARITY.CONSUMER_GRADE,
      fallbackReason: "fallback_category_case_capsule"
    }
  }

  return {
    canonicalRarity: CANONICAL_RARITY.UNKNOWN,
    fallbackReason: "fallback_unresolved"
  }
}

function canonicalRarityToDisplay(value) {
  const normalized = normalizeCanonicalRarity(value) || CANONICAL_RARITY.UNKNOWN
  return CANONICAL_RARITY_DISPLAY[normalized] || CANONICAL_RARITY_DISPLAY[CANONICAL_RARITY.UNKNOWN]
}

function getCanonicalRarityColor(value) {
  const normalized = normalizeCanonicalRarity(value) || CANONICAL_RARITY.UNKNOWN
  return CANONICAL_RARITY_COLORS[normalized] || CANONICAL_RARITY_COLORS[CANONICAL_RARITY.UNKNOWN]
}

function isKnownCanonicalRarity(value) {
  const normalized = normalizeCanonicalRarity(value)
  return Boolean(normalized && normalized !== CANONICAL_RARITY.UNKNOWN)
}

function resolveCanonicalRarity(options = {}) {
  const safeMarketHashName = normalizeText(options.marketHashName)
  const safeWeapon = normalizeText(options.weapon)
  const safeCategory = normalizeCategory(options.category, safeMarketHashName, safeWeapon)
  const catalogRarityRaw = normalizeText(options.catalogRarity)
  const sourceRarityRaw = normalizeText(options.sourceRarity)

  const normalizedCatalog = normalizeCanonicalRarity(catalogRarityRaw, {
    marketHashName: safeMarketHashName,
    weapon: safeWeapon
  })
  if (normalizedCatalog) {
    return {
      canonicalRarity: normalizedCatalog,
      rarity: canonicalRarityToDisplay(normalizedCatalog),
      rarityColor: getCanonicalRarityColor(normalizedCatalog),
      source: "catalog_rarity",
      diagnostics: {
        category: safeCategory,
        fallbackReason: null,
        unknownReason: null,
        catalogRarityRaw: catalogRarityRaw || null,
        sourceRarityRaw: sourceRarityRaw || null
      }
    }
  }

  const normalizedSource = normalizeCanonicalRarity(sourceRarityRaw, {
    marketHashName: safeMarketHashName,
    weapon: safeWeapon
  })
  if (normalizedSource) {
    return {
      canonicalRarity: normalizedSource,
      rarity: canonicalRarityToDisplay(normalizedSource),
      rarityColor: getCanonicalRarityColor(normalizedSource),
      source: "source_rarity",
      diagnostics: {
        category: safeCategory,
        fallbackReason: null,
        unknownReason: null,
        catalogRarityRaw: catalogRarityRaw || null,
        sourceRarityRaw: sourceRarityRaw || null
      }
    }
  }

  const fallback = resolveDeterministicFallback({
    category: safeCategory,
    marketHashName: safeMarketHashName,
    weapon: safeWeapon
  })
  const fallbackRarity =
    fallback?.canonicalRarity && CANONICAL_RARITY_SET.has(fallback.canonicalRarity)
      ? fallback.canonicalRarity
      : CANONICAL_RARITY.UNKNOWN
  const unknownReason =
    fallbackRarity === CANONICAL_RARITY.UNKNOWN
      ? fallback?.fallbackReason || "catalog_source_and_fallback_unresolved"
      : null

  return {
    canonicalRarity: fallbackRarity,
    rarity: canonicalRarityToDisplay(fallbackRarity),
    rarityColor: getCanonicalRarityColor(fallbackRarity),
    source:
      fallbackRarity === CANONICAL_RARITY.UNKNOWN
        ? "unknown"
        : "deterministic_fallback",
    diagnostics: {
      category: safeCategory,
      fallbackReason: fallback?.fallbackReason || null,
      unknownReason,
      catalogRarityRaw: catalogRarityRaw || null,
      sourceRarityRaw: sourceRarityRaw || null
    }
  }
}

function buildUnknownRarityDiagnostics(result = {}, extra = {}) {
  if (normalizeCanonicalRarity(result?.canonicalRarity) !== CANONICAL_RARITY.UNKNOWN) {
    return null
  }
  return {
    canonical_rarity: CANONICAL_RARITY.UNKNOWN,
    reason:
      normalizeText(result?.diagnostics?.unknownReason) ||
      normalizeText(result?.diagnostics?.fallbackReason) ||
      "unknown_rarity",
    resolver_source: normalizeText(result?.source || "unknown") || "unknown",
    category: normalizeText(result?.diagnostics?.category || extra?.category || null) || null,
    market_hash_name:
      normalizeText(extra?.marketHashName || result?.marketHashName || null) || null,
    weapon: normalizeText(extra?.weapon || result?.weapon || null) || null,
    catalog_rarity_raw:
      normalizeText(result?.diagnostics?.catalogRarityRaw || extra?.catalogRarity || null) || null,
    source_rarity_raw:
      normalizeText(result?.diagnostics?.sourceRarityRaw || extra?.sourceRarity || null) || null
  }
}

function normalizeRarityColor(value, fallbackCanonicalRarity = CANONICAL_RARITY.UNKNOWN) {
  return (
    sanitizeHexColor(value) ||
    getCanonicalRarityColor(fallbackCanonicalRarity || CANONICAL_RARITY.UNKNOWN)
  )
}

module.exports = {
  CANONICAL_RARITY,
  CANONICAL_RARITY_COLORS,
  CANONICAL_RARITY_DISPLAY,
  CATEGORY,
  canonicalRarityToDisplay,
  getCanonicalRarityColor,
  isKnownCanonicalRarity,
  normalizeCanonicalRarity,
  normalizeCategory,
  normalizeRarityColor,
  resolveCanonicalRarity,
  buildUnknownRarityDiagnostics,
  __testables: {
    sanitizeHexColor,
    resolveDeterministicFallback
  }
}

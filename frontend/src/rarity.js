export const CANONICAL_RARITY = Object.freeze({
  CONSUMER_GRADE: "consumer_grade",
  INDUSTRIAL_GRADE: "industrial_grade",
  MIL_SPEC_GRADE: "mil_spec_grade",
  RESTRICTED: "restricted",
  CLASSIFIED: "classified",
  COVERT: "covert",
  CONTRABAND: "contraband",
  KNIFE_GLOVES: "knife_gloves",
  UNKNOWN: "unknown",
});

export const canonicalRarityLabels = Object.freeze({
  [CANONICAL_RARITY.CONSUMER_GRADE]: "Consumer Grade",
  [CANONICAL_RARITY.INDUSTRIAL_GRADE]: "Industrial Grade",
  [CANONICAL_RARITY.MIL_SPEC_GRADE]: "Mil-Spec Grade",
  [CANONICAL_RARITY.RESTRICTED]: "Restricted",
  [CANONICAL_RARITY.CLASSIFIED]: "Classified",
  [CANONICAL_RARITY.COVERT]: "Covert",
  [CANONICAL_RARITY.CONTRABAND]: "Contraband",
  [CANONICAL_RARITY.KNIFE_GLOVES]: "Knife/Gloves",
  [CANONICAL_RARITY.UNKNOWN]: "Unknown",
});

export const canonicalRarityColors = Object.freeze({
  [CANONICAL_RARITY.CONSUMER_GRADE]: "#b0c3d9",
  [CANONICAL_RARITY.INDUSTRIAL_GRADE]: "#5e98d9",
  [CANONICAL_RARITY.MIL_SPEC_GRADE]: "#4b69ff",
  [CANONICAL_RARITY.RESTRICTED]: "#8847ff",
  [CANONICAL_RARITY.CLASSIFIED]: "#d32ce6",
  [CANONICAL_RARITY.COVERT]: "#eb4b4b",
  [CANONICAL_RARITY.CONTRABAND]: "#e4ae39",
  [CANONICAL_RARITY.KNIFE_GLOVES]: "#f7ca63",
  [CANONICAL_RARITY.UNKNOWN]: "#8a93a3",
});

export const rarityColors = Object.freeze({
  "Consumer Grade": canonicalRarityColors[CANONICAL_RARITY.CONSUMER_GRADE],
  "Industrial Grade": canonicalRarityColors[CANONICAL_RARITY.INDUSTRIAL_GRADE],
  "Mil-Spec Grade": canonicalRarityColors[CANONICAL_RARITY.MIL_SPEC_GRADE],
  Restricted: canonicalRarityColors[CANONICAL_RARITY.RESTRICTED],
  Classified: canonicalRarityColors[CANONICAL_RARITY.CLASSIFIED],
  Covert: canonicalRarityColors[CANONICAL_RARITY.COVERT],
  Contraband: canonicalRarityColors[CANONICAL_RARITY.CONTRABAND],
  "Knife/Gloves": canonicalRarityColors[CANONICAL_RARITY.KNIFE_GLOVES],
  Unknown: canonicalRarityColors[CANONICAL_RARITY.UNKNOWN],
  Default: canonicalRarityColors[CANONICAL_RARITY.UNKNOWN],
});

export const defaultSkinImage = "/skin-placeholder.svg";
export const defaultCaseImage = "/case-placeholder.svg";

const canonicalRaritySet = new Set(Object.values(CANONICAL_RARITY));
const knownBrokenImageHosts = new Set(["example.com", "www.example.com"]);
const unknownTextSet = new Set([
  "unknown",
  "default",
  "none",
  "n/a",
  "na",
  "null",
  "-",
  "?",
]);
const KNIFE_GLOVE_PATTERN =
  /(?:^|\s)(?:knife|knives|glove|gloves|wraps|bayonet|karambit|butterfly|talon|ursus|navaja|stiletto|falchion|daggers|hand wraps|shadow daggers|huntsman|bowie)(?:\s|$)/i;

const rarityAliases = Object.freeze({
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
  gloves: CANONICAL_RARITY.KNIFE_GLOVES,
});

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizeCanonicalRarity(value, marketHashName = "", weapon = "") {
  const raw = normalizeText(value);
  if (!raw) return null;
  const lowerRaw = raw.toLowerCase().replace(/\s+/g, " ");
  if (canonicalRaritySet.has(lowerRaw)) return lowerRaw;
  if (unknownTextSet.has(lowerRaw)) return null;

  const alias = rarityAliases[lowerRaw];
  if (alias) return alias;

  const source = `${lowerRaw} ${normalizeText(marketHashName)} ${normalizeText(weapon)}`.toLowerCase();
  if (raw.includes("\u2605") || KNIFE_GLOVE_PATTERN.test(source)) return CANONICAL_RARITY.KNIFE_GLOVES;
  if (/\bcontraband\b/.test(source)) return CANONICAL_RARITY.CONTRABAND;
  if (/\b(?:covert|immortal)\b/.test(source)) return CANONICAL_RARITY.COVERT;
  if (/\b(?:classified|exotic)\b/.test(source)) return CANONICAL_RARITY.CLASSIFIED;
  if (/\b(?:restricted|remarkable)\b/.test(source)) return CANONICAL_RARITY.RESTRICTED;
  if (/\bmil[- ]?spec\b/.test(source)) return CANONICAL_RARITY.MIL_SPEC_GRADE;
  if (/\bindustrial grade\b|\bhigh grade\b/.test(source)) return CANONICAL_RARITY.INDUSTRIAL_GRADE;
  if (/\bconsumer grade\b|\bbase grade\b/.test(source)) return CANONICAL_RARITY.CONSUMER_GRADE;
  return null;
}

function resolveDeterministicFallback(item = {}) {
  const category = normalizeText(item.category || item.itemCategory).toLowerCase();
  const source = `${normalizeText(item.marketHashName)} ${normalizeText(item.weapon)} ${category}`.toLowerCase();

  if (/\bcontraband\b/.test(source)) return CANONICAL_RARITY.CONTRABAND;
  if (
    category === "knife" ||
    category === "glove" ||
    source.includes("\u2605") ||
    KNIFE_GLOVE_PATTERN.test(source)
  ) {
    return CANONICAL_RARITY.KNIFE_GLOVES;
  }
  if (/\b(?:covert|immortal)\b/.test(source)) return CANONICAL_RARITY.COVERT;
  if (/\b(?:classified|exotic)\b/.test(source)) return CANONICAL_RARITY.CLASSIFIED;
  if (/\b(?:restricted|remarkable)\b/.test(source)) return CANONICAL_RARITY.RESTRICTED;
  if (/\bmil[- ]?spec\b/.test(source)) return CANONICAL_RARITY.MIL_SPEC_GRADE;
  if (/\bindustrial grade\b|\bhigh grade\b/.test(source)) return CANONICAL_RARITY.INDUSTRIAL_GRADE;
  if (/\bconsumer grade\b|\bbase grade\b/.test(source)) return CANONICAL_RARITY.CONSUMER_GRADE;
  if (category === "case" || category === "sticker_capsule" || isCaseLikeItem(item)) {
    return CANONICAL_RARITY.CONSUMER_GRADE;
  }
  return CANONICAL_RARITY.UNKNOWN;
}

function resolveCanonicalRarity(item = {}) {
  const marketHashName = normalizeText(item.marketHashName);
  const weapon = normalizeText(item.weapon);

  const normalizedCatalog = normalizeCanonicalRarity(
    item.canonicalRarity || item.canonical_rarity,
    marketHashName,
    weapon
  );
  if (normalizedCatalog) return { canonicalRarity: normalizedCatalog, source: "catalog_rarity" };

  const normalizedSource = normalizeCanonicalRarity(
    item.rarity || item.itemRarity,
    marketHashName,
    weapon
  );
  if (normalizedSource) return { canonicalRarity: normalizedSource, source: "source_rarity" };

  const fallback = resolveDeterministicFallback(item);
  if (fallback !== CANONICAL_RARITY.UNKNOWN) {
    return { canonicalRarity: fallback, source: "deterministic_fallback" };
  }
  return { canonicalRarity: CANONICAL_RARITY.UNKNOWN, source: "unknown" };
}

export function normalizeRarity(rarity, marketHashName = "", options = {}) {
  const resolution = resolveCanonicalRarity({
    rarity,
    marketHashName,
    category: options?.category,
    itemCategory: options?.itemCategory,
    weapon: options?.weapon,
    canonicalRarity: options?.canonicalRarity,
    canonical_rarity: options?.canonical_rarity,
  });
  return canonicalRarityLabels[resolution.canonicalRarity] || "Unknown";
}

export function getRarityColor(rarity, marketHashName = "", _rarityColor = "", options = {}) {
  const resolution = resolveCanonicalRarity({
    rarity,
    marketHashName,
    category: options?.category,
    itemCategory: options?.itemCategory,
    weapon: options?.weapon,
    canonicalRarity: options?.canonicalRarity,
    canonical_rarity: options?.canonical_rarity,
  });
  return canonicalRarityColors[resolution.canonicalRarity] || canonicalRarityColors.unknown;
}

export function getRarityTheme(item = {}) {
  const resolution = resolveCanonicalRarity(item);
  const canonicalRarity = resolution.canonicalRarity || CANONICAL_RARITY.UNKNOWN;
  const rarity = canonicalRarityLabels[canonicalRarity] || "Unknown";
  const color = canonicalRarityColors[canonicalRarity] || canonicalRarityColors.unknown;
  const unknownReason =
    canonicalRarity === CANONICAL_RARITY.UNKNOWN
      ? "catalog_source_and_fallback_unresolved"
      : null;
  return { rarity, color, canonicalRarity, source: resolution.source, unknownReason };
}

export function isKnownBrokenImageUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  try {
    const parsed = new URL(raw);
    return knownBrokenImageHosts.has(parsed.hostname.toLowerCase());
  } catch (_err) {
    return false;
  }
}

export function isCaseLikeItem(item = {}) {
  const name = String(item.marketHashName || item.skinName || "").toLowerCase();
  const weapon = String(item.weapon || "").toLowerCase();
  return (
    /\b(case|container|capsule|souvenir package|gift package)\b/.test(name) ||
    /\bcontainer\b/.test(weapon)
  );
}

export function resolveItemImageUrl(item = {}) {
  const candidates = [item.imageUrlLarge, item.imageUrl];
  for (const candidate of candidates) {
    const raw = String(candidate || "").trim();
    if (!/^https?:\/\//i.test(raw)) continue;
    if (isKnownBrokenImageUrl(raw)) continue;
    return raw;
  }
  return isCaseLikeItem(item) ? defaultCaseImage : defaultSkinImage;
}

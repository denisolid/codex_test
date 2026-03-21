export const rarityColors = Object.freeze({
  "Consumer Grade": "#b0c3d9",
  "Industrial Grade": "#5e98d9",
  "Mil-Spec Grade": "#4b69ff",
  Restricted: "#8847ff",
  Classified: "#d32ce6",
  Covert: "#eb4b4b",
  Contraband: "#e4ae39",
  "Knife/Gloves": "#f7ca63",
  Default: "#7f8ba5"
});

export const defaultSkinImage =
  "/skin-placeholder.svg";
export const defaultCaseImage = "/case-placeholder.svg";

const knownBrokenImageHosts = new Set(["example.com", "www.example.com"]);

const rarityAliases = Object.freeze({
  "base grade": "Consumer Grade",
  "high grade": "Industrial Grade",
  remarkable: "Restricted",
  exotic: "Classified",
  immortal: "Covert",
  extraordinary: "Knife/Gloves",
  "consumer grade": "Consumer Grade",
  "industrial grade": "Industrial Grade",
  "mil-spec grade": "Mil-Spec Grade",
  "mil spec grade": "Mil-Spec Grade",
  "mil-spec": "Mil-Spec Grade",
  restricted: "Restricted",
  classified: "Classified",
  covert: "Covert",
  contraband: "Contraband",
  knife: "Knife/Gloves",
  gloves: "Knife/Gloves"
});

function sanitizeHexColor(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const normalized = raw.startsWith("#") ? raw : `#${raw}`;
  return /^#[\da-f]{6}$/i.test(normalized) ? normalized.toLowerCase() : null;
}

export function normalizeRarity(rarity, marketHashName = "") {
  const raw = String(rarity || "").trim();
  const lowerRaw = raw.toLowerCase();
  const name = String(marketHashName || "").toLowerCase();

  const looksLikeKnifeOrGlove =
    /(?:^|\s)(?:knife|glove|gloves|wraps|bayonet|karambit|butterfly|talon|ursus|navaja|stiletto|falchion|daggers|hand wraps)(?:\s|$)/i.test(
      name
    ) || /\u2605/.test(raw);

  if (looksLikeKnifeOrGlove) {
    return "Knife/Gloves";
  }

  const alias = rarityAliases[lowerRaw];
  if (alias) {
    return alias;
  }

  const normalized = raw.replace(/\s+/g, " ").trim();
  if (rarityColors[normalized]) {
    return normalized;
  }

  const source = `${lowerRaw} ${name}`.trim();
  if (/\bextraordinary\b/.test(source)) return "Knife/Gloves";
  if (/\bimmortal\b/.test(source)) return "Covert";
  if (/\bexotic\b/.test(source)) return "Classified";
  if (/\bremarkable\b/.test(source)) return "Restricted";
  if (/\bcovert\b/.test(source)) return "Covert";
  if (/\bclassified\b/.test(source)) return "Classified";
  if (/\brestricted\b/.test(source)) return "Restricted";
  if (/\bmil[- ]?spec\b/.test(source)) return "Mil-Spec Grade";
  if (/\bindustrial grade\b/.test(source)) return "Industrial Grade";
  if (/\bconsumer grade\b/.test(source)) return "Consumer Grade";
  if (/\bcontraband\b/.test(source)) return "Contraband";
  return normalized || "Default";
}

export function getRarityColor(rarity, marketHashName = "", rarityColor = "") {
  const explicit = sanitizeHexColor(rarityColor);
  if (explicit) return explicit;

  const normalized = normalizeRarity(rarity, marketHashName);
  return sanitizeHexColor(rarityColors[normalized]) || rarityColors.Default;
}

export function getRarityTheme(item = {}) {
  const normalizedRarity = normalizeRarity(item.rarity, item.marketHashName);
  const rarity = normalizedRarity === "Default" ? "Unknown" : normalizedRarity;
  const color = getRarityColor(item.rarity, item.marketHashName, item.rarityColor);
  return { rarity, color };
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

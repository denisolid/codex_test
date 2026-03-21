const AppError = require("../utils/AppError");
const {
  skinEnrichmentMaxAgeDays,
  defaultSkinImageUrl
} = require("../config/env");
const {
  resolveCanonicalRarity,
  buildUnknownRarityDiagnostics,
  canonicalRarityToDisplay,
  getCanonicalRarityColor,
  normalizeRarityColor,
  normalizeCanonicalRarity
} = require("../utils/rarityResolver");

const STEAM_IMAGE_BASE = "https://community.akamai.steamstatic.com/economy/image/";
const DEFAULT_PLACEHOLDER = String(defaultSkinImageUrl || "").trim();
const ENRICHMENT_MAX_AGE_DAYS = Math.max(Number(skinEnrichmentMaxAgeDays || 14), 1);
const ENRICHMENT_MAX_AGE_MS = ENRICHMENT_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
const KNOWN_BAD_IMAGE_HOSTS = new Set(["example.com", "www.example.com"]);

function sanitizeHexColor(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const normalized = raw.startsWith("#") ? raw : `#${raw}`;
  return /^#[\da-f]{6}$/i.test(normalized) ? normalized.toLowerCase() : null;
}

function normalizeRarityName(value, marketHashName = "", weapon = "") {
  const resolution = resolveCanonicalRarity({
    sourceRarity: value,
    marketHashName,
    weapon
  });
  return canonicalRarityToDisplay(resolution.canonicalRarity);
}

function getRarityColor(rarity) {
  const canonical = normalizeCanonicalRarity(rarity) || null;
  return getCanonicalRarityColor(canonical);
}

function sanitizeHttpUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return null;
    }
    return parsed.toString();
  } catch (_err) {
    return null;
  }
}

function isKnownBadImageUrl(value) {
  const safe = sanitizeHttpUrl(value);
  if (!safe) return false;
  try {
    const host = new URL(safe).hostname.toLowerCase();
    return KNOWN_BAD_IMAGE_HOSTS.has(host);
  } catch (_err) {
    return false;
  }
}

function sanitizeImageUrl(value) {
  const safe = sanitizeHttpUrl(value);
  if (!safe) return null;
  if (isKnownBadImageUrl(safe)) return null;
  return safe;
}

function buildSteamImageUrlFromIcon(iconRef) {
  const icon = String(iconRef || "").trim();
  if (!icon) return null;
  return `${STEAM_IMAGE_BASE}${icon}`;
}

function buildLargeImageUrl(imageUrl) {
  const safeImageUrl = sanitizeImageUrl(imageUrl);
  if (!safeImageUrl) return null;

  if (safeImageUrl.includes("/economy/image/")) {
    return `${safeImageUrl}/512fx512f`;
  }

  return safeImageUrl;
}

function shouldRefreshMetadata(existingSkin) {
  if (!existingSkin) return true;
  const hasValidImage = Boolean(
    sanitizeImageUrl(existingSkin.image_url) || sanitizeImageUrl(existingSkin.image_url_large)
  );
  if (
    !hasValidImage ||
    !existingSkin.rarity ||
    !existingSkin.rarity_color ||
    !existingSkin.canonical_rarity
  ) {
    return true;
  }
  const updatedAt = new Date(existingSkin.updated_at || existingSkin.created_at || 0).getTime();
  if (!Number.isFinite(updatedAt) || updatedAt <= 0) return true;
  return Date.now() - updatedAt >= ENRICHMENT_MAX_AGE_MS;
}

function buildSkinMetadata(item, existingSkin = null) {
  const refresh = shouldRefreshMetadata(existingSkin);
  const fallbackImage =
    sanitizeImageUrl(DEFAULT_PLACEHOLDER) ||
    "https://community.akamai.steamstatic.com/public/images/apps/730/header.jpg";

  const sourceImage =
    sanitizeImageUrl(item.imageUrl) ||
    sanitizeImageUrl(buildSteamImageUrlFromIcon(item.iconUrl)) ||
    sanitizeImageUrl(existingSkin?.image_url) ||
    sanitizeImageUrl(existingSkin?.image_url_large) ||
    fallbackImage;

  const sourceLargeImage =
    sanitizeImageUrl(item.imageUrlLarge) ||
    buildLargeImageUrl(sourceImage) ||
    sanitizeImageUrl(existingSkin?.image_url_large) ||
    sourceImage;

  const rarityResolution = resolveCanonicalRarity({
    catalogRarity: existingSkin?.canonical_rarity || existingSkin?.rarity,
    sourceRarity: item.rarity,
    category: item.itemCategory || item.category || null,
    marketHashName: item.marketHashName,
    weapon: item.weapon
  });
  const resolvedCanonicalRarity = rarityResolution.canonicalRarity;
  const resolvedRarity = canonicalRarityToDisplay(resolvedCanonicalRarity);
  const resolvedColor = getCanonicalRarityColor(resolvedCanonicalRarity);
  const unknownDiagnostics = buildUnknownRarityDiagnostics(rarityResolution, {
    category: item.itemCategory || item.category || null,
    marketHashName: item.marketHashName,
    weapon: item.weapon,
    catalogRarity: existingSkin?.canonical_rarity || existingSkin?.rarity || null,
    sourceRarity: item.rarity || null
  });

  if (!refresh) {
    const existingColor = normalizeRarityColor(
      existingSkin?.rarity_color,
      resolvedCanonicalRarity
    );
    return {
      imageUrl:
        sanitizeImageUrl(existingSkin.image_url) ||
        sanitizeImageUrl(existingSkin.image_url_large) ||
        sourceImage,
      imageUrlLarge:
        sanitizeImageUrl(existingSkin.image_url_large) ||
        buildLargeImageUrl(sanitizeImageUrl(existingSkin.image_url)) ||
        sourceLargeImage,
      rarity: resolvedRarity,
      canonicalRarity: resolvedCanonicalRarity,
      rarityColor: existingColor,
      rarityDiagnostics: unknownDiagnostics
    };
  }

  return {
    imageUrl: sourceImage,
    imageUrlLarge: sourceLargeImage,
    rarity: resolvedRarity,
    canonicalRarity: resolvedCanonicalRarity,
    rarityColor: resolvedColor,
    rarityDiagnostics: unknownDiagnostics
  };
}

function sanitizeInventoryItem(item) {
  if (!item || typeof item !== "object") {
    throw new AppError("Invalid inventory item payload", 400, "INVALID_INVENTORY_ITEM");
  }

  const marketHashName = String(item.marketHashName || "").trim();
  if (!marketHashName) {
    throw new AppError(
      "Inventory item is missing marketHashName",
      400,
      "INVALID_INVENTORY_ITEM"
    );
  }

  const quantity = Number(item.quantity || 0);
  const safeQuantity = Number.isFinite(quantity) ? Math.max(quantity, 0) : 0;

  return {
    ...item,
    marketHashName,
    quantity: safeQuantity
  };
}

function enrichInventoryItems(items, existingSkinByName = {}) {
  const safeItems = Array.isArray(items) ? items : [];

  const enrichedItems = safeItems.map((rawItem) => {
    const item = sanitizeInventoryItem(rawItem);
    const existingSkin = existingSkinByName[item.marketHashName] || null;
    const metadata = buildSkinMetadata(item, existingSkin);

    return {
      ...item,
      rarity: metadata.rarity,
      canonicalRarity: metadata.canonicalRarity,
      rarityColor: metadata.rarityColor,
      rarityDiagnostics: metadata.rarityDiagnostics,
      imageUrl: metadata.imageUrl,
      imageUrlLarge: metadata.imageUrlLarge
    };
  });

  const skinRows = enrichedItems.map((item) => ({
    market_hash_name: item.marketHashName,
    weapon: item.weapon || null,
    skin_name: item.skinName || null,
    exterior: item.exterior || null,
    rarity: item.rarity || null,
    canonical_rarity: item.canonicalRarity || null,
    rarity_color:
      sanitizeHexColor(item.rarityColor) ||
      getCanonicalRarityColor(item.canonicalRarity || item.rarity),
    image_url: item.imageUrl || DEFAULT_PLACEHOLDER,
    image_url_large: item.imageUrlLarge || item.imageUrl || DEFAULT_PLACEHOLDER
  }));

  return { enrichedItems, skinRows };
}

module.exports = {
  enrichInventoryItems,
  getRarityColor,
  normalizeRarityName,
  __testables: {
    sanitizeHexColor,
    sanitizeHttpUrl,
    sanitizeImageUrl,
    isKnownBadImageUrl,
    buildLargeImageUrl,
    buildSteamImageUrlFromIcon,
    shouldRefreshMetadata
  }
};

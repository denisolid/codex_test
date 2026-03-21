const { fetchJsonWithRetry, mapWithConcurrency } = require("../markets/marketHttp");
const {
  resolveCanonicalRarity,
  canonicalRarityToDisplay,
  getCanonicalRarityColor
} = require("../utils/rarityResolver");

const STEAM_SEARCH_BASE_URL = "https://steamcommunity.com/market/search/render/";
const STEAM_IMAGE_BASE_URL = "https://community.akamai.steamstatic.com/economy/image/";
const DEFAULT_STEAM_SEARCH_COUNT = 50;
const MAX_STEAM_SEARCH_COUNT = 100;

const MARKET_NAME_STOP_WORDS = new Set([
  "field",
  "tested",
  "factory",
  "new",
  "minimal",
  "wear",
  "battle",
  "scarred",
  "well",
  "worn",
  "stattrak",
  "souvenir",
  "the",
  "and"
]);

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizeNameKey(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/[\u2122\u00ae]/g, "")
    .replace(/\s+/g, " ");
}

function normalizeComparableName(value) {
  return normalizeNameKey(value)
    .replace(/\b(?:stattrak|souvenir)\b/g, " ")
    .replace(/\((?:factory new|minimal wear|field-tested|well-worn|battle-scarred)\)/g, " ")
    .replace(/[|]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHexColor(value) {
  const raw = normalizeText(value).replace(/^#/, "");
  if (!/^[\da-f]{6}$/i.test(raw)) return null;
  return `#${raw.toLowerCase()}`;
}

function normalizeRarity(value, marketHashName = "") {
  const resolution = resolveCanonicalRarity({
    sourceRarity: value,
    marketHashName
  });
  if (resolution.source === "unknown") return null;
  return canonicalRarityToDisplay(resolution.canonicalRarity);
}

function extractDistinctiveTokens(value) {
  return Array.from(
    new Set(
      normalizeComparableName(value)
        .split(/[^a-z0-9]+/)
        .map((token) => token.trim())
        .filter(
          (token) =>
            token &&
            token.length >= 3 &&
            !MARKET_NAME_STOP_WORDS.has(token)
        )
    )
  );
}

function matchByDistinctiveTokens(candidateName, marketHashName) {
  const targetTokens = extractDistinctiveTokens(marketHashName);
  if (!targetTokens.length) return true;
  const candidateTokens = new Set(extractDistinctiveTokens(candidateName));
  return targetTokens.some((token) => candidateTokens.has(token));
}

function stripWearSuffix(value) {
  return normalizeText(value).replace(
    /\s*\((?:factory new|minimal wear|field-tested|well-worn|battle-scarred)\)\s*$/i,
    ""
  );
}

function stripSpecialPrefixes(value) {
  return normalizeText(value).replace(/^(?:stattrak\u2122?|souvenir)\s+/i, "");
}

function buildSearchQueries(marketHashName) {
  const raw = normalizeText(marketHashName);
  const noPrefix = stripSpecialPrefixes(raw);
  const noWear = stripWearSuffix(raw);
  const noPrefixNoWear = stripWearSuffix(noPrefix);
  return Array.from(
    new Set([raw, noWear, noPrefix, noPrefixNoWear].map(normalizeText).filter(Boolean))
  );
}

function toSafeHttpUrl(value) {
  const raw = normalizeText(value);
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) return null;
    return parsed.toString();
  } catch (_err) {
    return null;
  }
}

function buildSteamImageUrlFromIcon(iconRef, variant = "") {
  const icon = normalizeText(iconRef);
  if (!icon) return null;
  const suffix = normalizeText(variant);
  return `${STEAM_IMAGE_BASE_URL}${icon}${suffix ? `/${suffix}` : ""}`;
}

function resolveImageFromCandidate(candidate = {}) {
  const source = candidate && typeof candidate === "object" ? candidate : {};
  const directImage = [
    source.image_url,
    source.imageUrl,
    source.image,
    source.icon,
    source?.item?.image_url,
    source?.item?.imageUrl,
    source?.item?.image,
    source?.asset_description?.icon_url_large
      ? buildSteamImageUrlFromIcon(source.asset_description.icon_url_large, "360fx360f")
      : null
  ]
    .map(toSafeHttpUrl)
    .find(Boolean);
  const directLarge = [
    source.image_url_large,
    source.imageUrlLarge,
    source?.item?.image_url_large,
    source?.item?.imageUrlLarge
  ]
    .map(toSafeHttpUrl)
    .find(Boolean);

  const icon = [
    source.icon_url,
    source.iconUrl,
    source?.asset_description?.icon_url,
    source?.item?.icon_url,
    source?.item?.iconUrl,
    source?.item?.asset_description?.icon_url
  ]
    .map(normalizeText)
    .find(Boolean);
  const iconLarge = [
    source.icon_url_large,
    source.iconUrlLarge,
    source?.asset_description?.icon_url_large,
    source?.item?.icon_url_large,
    source?.item?.iconUrlLarge,
    source?.item?.asset_description?.icon_url_large
  ]
    .map(normalizeText)
    .find(Boolean);

  const iconImage = buildSteamImageUrlFromIcon(icon, "360fx360f");
  const iconImageLarge = buildSteamImageUrlFromIcon(iconLarge || icon, "512fx512f");
  const imageUrl = directImage || iconImage || null;
  const imageUrlLarge = directLarge || iconImageLarge || imageUrl || null;
  if (!imageUrl && !imageUrlLarge) return null;

  return {
    imageUrl,
    imageUrlLarge
  };
}

function pickImageFromMarketRow(row = {}) {
  const candidates = [
    row,
    row?.raw,
    row?.raw?.item,
    row?.raw?.asset_description,
    row?.raw?.asset
  ];
  for (const candidate of candidates) {
    const image = resolveImageFromCandidate(candidate);
    if (image?.imageUrl || image?.imageUrlLarge) {
      return image;
    }
  }
  return null;
}

function buildSteamSearchUrl(marketHashName, options = {}) {
  const count = Math.min(
    Math.max(Number(options.count || DEFAULT_STEAM_SEARCH_COUNT), 1),
    MAX_STEAM_SEARCH_COUNT
  );
  const params = new URLSearchParams();
  params.set("appid", "730");
  params.set("norender", "1");
  params.set("count", String(count));
  params.set("start", "0");
  params.set("query", normalizeText(marketHashName));
  return `${STEAM_SEARCH_BASE_URL}?${params.toString()}`;
}

function pickSteamSearchResult(payload = {}, marketHashName = "") {
  const rows = Array.isArray(payload?.results) ? payload.results : [];
  if (!rows.length) return null;
  const targetKey = normalizeNameKey(marketHashName);
  if (!targetKey) return null;

  const exact = rows.find((row) => {
    const candidateName = normalizeText(
      row?.hash_name || row?.market_hash_name || row?.name
    );
    return normalizeNameKey(candidateName) === targetKey;
  });
  if (exact) return exact;

  const comparableTarget = normalizeComparableName(marketHashName);
  const comparableExact = rows.find((row) => {
    const candidateName = normalizeText(
      row?.hash_name || row?.market_hash_name || row?.name
    );
    return (
      normalizeComparableName(candidateName) === comparableTarget &&
      matchByDistinctiveTokens(candidateName, marketHashName)
    );
  });
  if (comparableExact) return comparableExact;

  const relaxed = rows.find((row) => {
    const candidateName = normalizeText(
      row?.hash_name || row?.market_hash_name || row?.name
    );
    const candidateKey = normalizeComparableName(candidateName);
    return (
      candidateKey &&
      (candidateKey.includes(comparableTarget) || comparableTarget.includes(candidateKey)) &&
      matchByDistinctiveTokens(candidateName, marketHashName)
    );
  });
  return relaxed || null;
}

function resolveRarityFromSearchResult(result = {}, marketHashName = "") {
  const asset = result?.asset_description && typeof result.asset_description === "object"
    ? result.asset_description
    : {};
  const tags = Array.isArray(asset?.tags) ? asset.tags : [];
  const rarityTag = tags.find(
    (tag) => String(tag?.category || "").trim().toLowerCase() === "rarity"
  );
  const rarityResolution = resolveCanonicalRarity({
    sourceRarity:
      rarityTag?.localized_tag_name ||
      rarityTag?.name ||
      String(asset?.type || result?.type || "").trim(),
    marketHashName
  });
  const canonicalRarity =
    rarityResolution.source === "unknown" ? null : rarityResolution.canonicalRarity;
  const rarity = canonicalRarity ? canonicalRarityToDisplay(canonicalRarity) : null;
  const rarityColor = canonicalRarity ? getCanonicalRarityColor(canonicalRarity) : null;
  return { rarity, canonicalRarity, rarityColor };
}

async function fetchSteamSearchPayload(query, options = {}) {
  const url = buildSteamSearchUrl(query, { count: options.count });
  return fetchJsonWithRetry(url, {
    timeoutMs: options.timeoutMs,
    maxRetries: options.maxRetries,
    headers: {
      Accept: "application/json",
      "User-Agent": "cs2-portfolio-analyzer/1.0"
    }
  });
}

async function fetchSteamSearchMetadataByMarketHashName(marketHashName, options = {}) {
  const normalizedName = normalizeText(marketHashName);
  if (!normalizedName) return null;

  const queries = buildSearchQueries(normalizedName);
  for (const query of queries) {
    let payload = null;
    try {
      payload = await fetchSteamSearchPayload(query, options);
    } catch (_err) {
      payload = null;
    }
    if (!payload) continue;

    const picked = pickSteamSearchResult(payload, normalizedName);
    if (!picked) continue;

    const image = resolveImageFromCandidate({
      ...picked,
      icon_url: picked?.asset_description?.icon_url || picked?.icon_url || null,
      icon_url_large:
        picked?.asset_description?.icon_url_large || picked?.icon_url_large || null
    });
    const rarityMeta = resolveRarityFromSearchResult(picked, normalizedName);
    if (!image?.imageUrl && !image?.imageUrlLarge && !rarityMeta.rarity) continue;

    return {
      marketHashName: normalizedName,
      imageUrl: image?.imageUrl || null,
      imageUrlLarge: image?.imageUrlLarge || image?.imageUrl || null,
      rarity: rarityMeta.rarity || null,
      canonicalRarity: rarityMeta.canonicalRarity || null,
      rarityColor: rarityMeta.rarityColor || null
    };
  }

  return null;
}

async function fetchSteamSearchImageByMarketHashName(marketHashName, options = {}) {
  const metadata = await fetchSteamSearchMetadataByMarketHashName(marketHashName, options);
  if (!metadata) return null;
  if (!metadata.imageUrl && !metadata.imageUrlLarge) return null;
  return {
    marketHashName: metadata.marketHashName,
    imageUrl: metadata.imageUrl || null,
    imageUrlLarge: metadata.imageUrlLarge || metadata.imageUrl || null
  };
}

async function fetchSteamSearchImagesBatch(marketHashNames = [], options = {}) {
  const names = Array.from(
    new Set((Array.isArray(marketHashNames) ? marketHashNames : []).map(normalizeText).filter(Boolean))
  );
  if (!names.length) {
    return {};
  }

  const timeoutMs = Math.max(Number(options.timeoutMs || 9000), 1000);
  const maxRetries = Math.max(Number(options.maxRetries || 2), 1);
  const concurrency = Math.max(Number(options.concurrency || 2), 1);
  const rows = await mapWithConcurrency(
    names,
    async (marketHashName) => {
      try {
        return await fetchSteamSearchImageByMarketHashName(marketHashName, {
          timeoutMs,
          maxRetries
        });
      } catch (_err) {
        return null;
      }
    },
    concurrency
  );

  const byName = {};
  for (const row of rows) {
    const key = normalizeText(row?.marketHashName);
    if (!key) continue;
    if (!row?.imageUrl && !row?.imageUrlLarge) continue;
    byName[key] = {
      imageUrl: row.imageUrl || null,
      imageUrlLarge: row.imageUrlLarge || row.imageUrl || null
    };
  }
  return byName;
}

async function fetchSteamSearchMetadataBatch(marketHashNames = [], options = {}) {
  const names = Array.from(
    new Set((Array.isArray(marketHashNames) ? marketHashNames : []).map(normalizeText).filter(Boolean))
  );
  if (!names.length) {
    return {};
  }

  const timeoutMs = Math.max(Number(options.timeoutMs || 9000), 1000);
  const maxRetries = Math.max(Number(options.maxRetries || 2), 1);
  const concurrency = Math.max(Number(options.concurrency || 2), 1);
  const count = Math.min(
    Math.max(Number(options.count || DEFAULT_STEAM_SEARCH_COUNT), 1),
    MAX_STEAM_SEARCH_COUNT
  );
  const rows = await mapWithConcurrency(
    names,
    async (marketHashName) => {
      try {
        return await fetchSteamSearchMetadataByMarketHashName(marketHashName, {
          timeoutMs,
          maxRetries,
          count
        });
      } catch (_err) {
        return null;
      }
    },
    concurrency
  );

  const byName = {};
  for (const row of rows) {
    const key = normalizeText(row?.marketHashName);
    if (!key) continue;
    if (!row?.imageUrl && !row?.imageUrlLarge && !row?.rarity && !row?.canonicalRarity) continue;
    byName[key] = {
      imageUrl: row.imageUrl || null,
      imageUrlLarge: row.imageUrlLarge || row.imageUrl || null,
      rarity: row.rarity || null,
      canonicalRarity: row.canonicalRarity || null,
      rarityColor: normalizeHexColor(row.rarityColor) || null
    };
  }
  return byName;
}

module.exports = {
  pickImageFromMarketRow,
  fetchSteamSearchImageByMarketHashName,
  fetchSteamSearchImagesBatch,
  fetchSteamSearchMetadataByMarketHashName,
  fetchSteamSearchMetadataBatch,
  __testables: {
    normalizeNameKey,
    normalizeComparableName,
    toSafeHttpUrl,
    normalizeHexColor,
    normalizeRarity,
    extractDistinctiveTokens,
    matchByDistinctiveTokens,
    stripWearSuffix,
    stripSpecialPrefixes,
    buildSearchQueries,
    buildSteamImageUrlFromIcon,
    resolveImageFromCandidate,
    pickSteamSearchResult,
    resolveRarityFromSearchResult
  }
};

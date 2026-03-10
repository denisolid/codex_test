const { fetchJsonWithRetry, mapWithConcurrency } = require("../markets/marketHttp");

const STEAM_SEARCH_BASE_URL = "https://steamcommunity.com/market/search/render/";
const STEAM_IMAGE_BASE_URL = "https://community.akamai.steamstatic.com/economy/image/";

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizeNameKey(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/[\u2122\u00ae]/g, "")
    .replace(/\s+/g, " ");
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

function buildSteamSearchUrl(marketHashName) {
  const params = new URLSearchParams();
  params.set("appid", "730");
  params.set("norender", "1");
  params.set("count", "10");
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

  const relaxed = rows.find((row) => {
    const candidateName = normalizeText(
      row?.hash_name || row?.market_hash_name || row?.name
    );
    const candidateKey = normalizeNameKey(candidateName);
    return candidateKey && (candidateKey.includes(targetKey) || targetKey.includes(candidateKey));
  });
  return relaxed || null;
}

async function fetchSteamSearchImageByMarketHashName(marketHashName, options = {}) {
  const normalizedName = normalizeText(marketHashName);
  if (!normalizedName) return null;
  const url = buildSteamSearchUrl(normalizedName);
  const payload = await fetchJsonWithRetry(url, {
    timeoutMs: options.timeoutMs,
    maxRetries: options.maxRetries,
    headers: {
      Accept: "application/json",
      "User-Agent": "cs2-portfolio-analyzer/1.0"
    }
  });
  const picked = pickSteamSearchResult(payload, normalizedName);
  if (!picked) return null;

  const image = resolveImageFromCandidate({
    ...picked,
    icon_url: picked?.asset_description?.icon_url || picked?.icon_url || null,
    icon_url_large:
      picked?.asset_description?.icon_url_large || picked?.icon_url_large || null
  });
  if (!image?.imageUrl && !image?.imageUrlLarge) return null;

  return {
    marketHashName: normalizedName,
    imageUrl: image.imageUrl || null,
    imageUrlLarge: image.imageUrlLarge || image.imageUrl || null
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

module.exports = {
  pickImageFromMarketRow,
  fetchSteamSearchImageByMarketHashName,
  fetchSteamSearchImagesBatch,
  __testables: {
    normalizeNameKey,
    toSafeHttpUrl,
    buildSteamImageUrlFromIcon,
    resolveImageFromCandidate,
    pickSteamSearchResult
  }
};


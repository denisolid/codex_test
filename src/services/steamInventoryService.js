const AppError = require("../utils/AppError");
const mockPriceProvider = require("./mockPriceProviderService");

const STEAM_IMAGE_BASE = "https://community.akamai.steamstatic.com/economy/image/";

function normalizeExterior(name) {
  const m = /\(([^)]+)\)\s*$/.exec(name || "");
  return m ? m[1] : null;
}

function parseMarketHashName(marketHashName) {
  if (!marketHashName) {
    return {
      weapon: null,
      skinName: null,
      exterior: null
    };
  }

  const [weaponPart, skinPartRaw] = marketHashName.split(" | ");
  const skinPart = skinPartRaw || "";
  const exterior = normalizeExterior(skinPart);
  const skinName = exterior
    ? skinPart.replace(new RegExp(`\\s*\\(${exterior.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\)$`), "")
    : skinPart || null;

  return {
    weapon: weaponPart || null,
    skinName,
    exterior
  };
}

function isSkinLikeItem(marketHashName) {
  return typeof marketHashName === "string" && marketHashName.includes(" | ");
}

function getRarity(desc) {
  const tags = Array.isArray(desc.tags) ? desc.tags : [];
  const rarityTag = tags.find((t) => t.category === "Rarity");
  return rarityTag?.localized_tag_name || rarityTag?.name || null;
}

function buildInventoryUrl(steamId64, startAssetId, count, language) {
  const params = new URLSearchParams();
  params.set("l", language);
  params.set("count", String(count));
  if (startAssetId) {
    params.set("start_assetid", String(startAssetId));
  }

  return `https://steamcommunity.com/inventory/${steamId64}/730/2?${params.toString()}`;
}

async function fetchPage(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        "User-Agent": "cs2-portfolio-analyzer/1.0"
      }
    });

    if (res.status === 403) {
      throw new AppError("Steam inventory is private or inaccessible", 400);
    }
    if (res.status === 429) {
      throw new AppError("Steam inventory rate limited", 429);
    }
    if (!res.ok) {
      throw new AppError(`Steam inventory fetch failed with status ${res.status}`, 502);
    }

    const json = await res.json();
    if (!json || json.success !== 1) {
      throw new AppError("Steam inventory response is invalid", 502);
    }

    return json;
  } catch (err) {
    if (err.name === "AbortError") {
      throw new AppError("Steam inventory request timed out", 504);
    }
    throw err;
  } finally {
    clearTimeout(t);
  }
}

exports.fetchInventory = async (steamId64, options = {}) => {
  const timeoutMs = Number(options.timeoutMs || 12000);
  const requestedPageSize = Number(options.pageSize || 2000);
  const pageSize = Math.min(Math.max(requestedPageSize, 1), 2000);
  const maxPages = Number(options.maxPages || 20);
  const language = options.language || "english";

  const descriptionsByKey = new Map();
  const descriptionByMarketHashName = new Map();
  const quantityByMarketHashName = new Map();

  let startAssetId = null;
  let pages = 0;

  while (pages < maxPages) {
    const url = buildInventoryUrl(steamId64, startAssetId, pageSize, language);
    const payload = await fetchPage(url, timeoutMs);
    pages += 1;

    for (const desc of payload.descriptions || []) {
      const key = `${desc.classid}_${desc.instanceid}`;
      descriptionsByKey.set(key, desc);
      if (desc.market_hash_name && !descriptionByMarketHashName.has(desc.market_hash_name)) {
        descriptionByMarketHashName.set(desc.market_hash_name, desc);
      }
    }

    for (const asset of payload.assets || []) {
      const key = `${asset.classid}_${asset.instanceid}`;
      const desc = descriptionsByKey.get(key);
    const marketHashName = desc?.market_hash_name;
      if (!marketHashName || !isSkinLikeItem(marketHashName)) continue;

      const qty = Number(asset.amount || 1);
      const current = quantityByMarketHashName.get(marketHashName) || 0;
      quantityByMarketHashName.set(marketHashName, current + qty);
    }

    if (!payload.more_items || !payload.last_assetid) {
      break;
    }
    startAssetId = payload.last_assetid;
  }

  const items = [];
  for (const [marketHashName, quantity] of quantityByMarketHashName.entries()) {
    const desc = descriptionByMarketHashName.get(marketHashName);
    const parsed = parseMarketHashName(marketHashName);
    const imageUrl = desc?.icon_url ? `${STEAM_IMAGE_BASE}${desc.icon_url}` : null;
    const price = await mockPriceProvider.getLatestPrice(marketHashName);

    items.push({
      marketHashName,
      weapon: parsed.weapon,
      skinName: parsed.skinName,
      exterior: parsed.exterior,
      rarity: getRarity(desc),
      imageUrl,
      quantity,
      price
    });
  }

  return items;
};

const AppError = require("../utils/AppError");

const STEAM_IMAGE_BASE = "https://community.akamai.steamstatic.com/economy/image/";

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function normalizeExterior(name) {
  const m = /\(([^)]+)\)\s*$/.exec(name || "");
  return m ? m[1] : null;
}

function parseMarketHashName(marketHashName, typeName) {
  if (!marketHashName) {
    return {
      weapon: null,
      skinName: null,
      exterior: null
    };
  }

  const [leftPart, ...rightParts] = marketHashName.split(" | ");
  const rightPart = rightParts.join(" | ");

  if (!rightPart) {
    return {
      weapon: typeName || null,
      skinName: marketHashName,
      exterior: null
    };
  }

  const skinPart = rightPart || "";
  const exterior = normalizeExterior(skinPart);
  const skinName = exterior
    ? skinPart.replace(
        new RegExp(
          `\\s*\\(${exterior.replace(/[.*+?^${}()|[\\]\\]/g, "\\$&")}\\)$`
        ),
        ""
      )
    : skinPart || null;

  return {
    weapon: leftPart || typeName || null,
    skinName,
    exterior
  };
}

function getRarity(desc) {
  const tags = Array.isArray(desc?.tags) ? desc.tags : [];
  const rarityTag = tags.find((t) => t.category === "Rarity");
  return rarityTag?.localized_tag_name || rarityTag?.name || null;
}

function getTypeName(desc) {
  const tags = Array.isArray(desc?.tags) ? desc.tags : [];
  const typeTag = tags.find((t) => t.category === "Type");
  return typeTag?.localized_tag_name || typeTag?.name || null;
}

function classifyDescription(desc) {
  if (!desc) return { include: false, reason: "missing-description" };
  if (Number(desc.marketable || 0) !== 1) {
    return { include: false, reason: "not-marketable" };
  }

  if (!desc.market_hash_name) {
    return { include: false, reason: "missing-market-hash-name" };
  }

  return { include: true, reason: null };
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

function shouldRetrySteamInventory(err) {
  const status =
    Number(err?.statusCode || err?.status || err?.httpStatus || err?.response?.status || 0) ||
    0;
  return status === 429 || status === 502 || status === 503 || status === 504;
}

function computeRetryDelayMs(baseDelayMs, attempt) {
  const expDelay = baseDelayMs * Math.pow(2, attempt);
  const jitter = Math.floor(Math.random() * Math.max(Math.floor(baseDelayMs / 3), 1));
  return expDelay + jitter;
}

async function fetchPageWithRetries(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 12000);
  const maxRetries = Math.max(Number(options.maxRetries || 0), 0);
  const retryBaseMs = Math.max(Number(options.retryBaseMs || 1200), 100);

  let lastError = null;
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      return await fetchPage(url, timeoutMs);
    } catch (err) {
      lastError = err;
      if (!shouldRetrySteamInventory(err) || attempt === maxRetries) {
        throw err;
      }
      await sleep(computeRetryDelayMs(retryBaseMs, attempt));
    }
  }

  throw lastError;
}

exports.fetchInventory = async (steamId64, options = {}) => {
  const timeoutMs = Number(options.timeoutMs || 12000);
  const requestedPageSize = Number(options.pageSize || 2000);
  const pageSize = Math.min(Math.max(requestedPageSize, 1), 2000);
  const maxPages = Number(options.maxPages || 20);
  const language = options.language || "english";
  const maxRetries = Number(options.maxRetries || 0);
  const retryBaseMs = Number(options.retryBaseMs || 1200);

  const descriptionsByKey = new Map();
  const descriptionByMarketHashName = new Map();
  const quantityByMarketHashName = new Map();
  const assetIdsByMarketHashName = new Map();
  const excludedByMarketHashName = new Map();

  let startAssetId = null;
  let pages = 0;

  while (pages < maxPages) {
    const url = buildInventoryUrl(steamId64, startAssetId, pageSize, language);
    const payload = await fetchPageWithRetries(url, {
      timeoutMs,
      maxRetries,
      retryBaseMs
    });
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
      const cls = classifyDescription(desc);
      if (!marketHashName || !cls.include) {
        if (marketHashName && !excludedByMarketHashName.has(marketHashName)) {
          excludedByMarketHashName.set(marketHashName, cls.reason || "excluded");
        }
        continue;
      }

      const qty = Number(asset.amount || 1);
      const current = quantityByMarketHashName.get(marketHashName) || 0;
      quantityByMarketHashName.set(marketHashName, current + qty);
      const currentAssetIds = assetIdsByMarketHashName.get(marketHashName) || [];
      if (asset.assetid) {
        currentAssetIds.push(String(asset.assetid));
      }
      assetIdsByMarketHashName.set(marketHashName, currentAssetIds);
    }

    if (!payload.more_items || !payload.last_assetid) {
      break;
    }
    startAssetId = payload.last_assetid;
  }

  const items = [];
  for (const [marketHashName, quantity] of quantityByMarketHashName.entries()) {
    const desc = descriptionByMarketHashName.get(marketHashName);
    const parsed = parseMarketHashName(marketHashName, getTypeName(desc));
    const imageUrl = desc?.icon_url ? `${STEAM_IMAGE_BASE}${desc.icon_url}` : null;

    items.push({
      marketHashName,
      weapon: parsed.weapon,
      skinName: parsed.skinName,
      exterior: parsed.exterior,
      rarity: getRarity(desc),
      imageUrl,
      quantity,
      steamItemIds: assetIdsByMarketHashName.get(marketHashName) || [],
      price: null
    });
  }

  const excludedItems = Array.from(excludedByMarketHashName.entries()).map(
    ([marketHashName, reason]) => ({
      marketHashName,
      reason
    })
  );

  return { items, excludedItems };
};

exports.__testables = {
  parseMarketHashName,
  classifyDescription
};

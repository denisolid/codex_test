const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const skinRepo = require("../repositories/skinRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const {
  steamInventorySource,
  steamInventoryTimeoutMs,
  marketPriceFallbackToMock,
  marketPriceRateLimitPerSecond,
  marketPriceCacheTtlMinutes
} = require("../config/env");
const priceProviderService = require("./priceProviderService");
const mockPriceProviderService = require("./mockPriceProviderService");
const mockSteamService = require("./mockSteamService");
const steamInventoryService = require("./steamInventoryService");

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function withRetries(fn, retries = 3, baseDelayMs = 250) {
  let lastErr;
  for (let i = 0; i < retries; i += 1) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < retries - 1) {
        await sleep(baseDelayMs * (i + 1));
      }
    }
  }
  throw lastErr;
}

async function fetchInventoryByConfiguredSource(steamId64) {
  if (steamInventorySource === "mock") {
    return {
      items: await mockSteamService.fetchInventory(steamId64),
      excludedItems: [],
      source: "mock"
    };
  }

  if (steamInventorySource === "real") {
    const data = await steamInventoryService.fetchInventory(steamId64, {
      timeoutMs: steamInventoryTimeoutMs
    });
    return { ...data, source: "steam" };
  }

  try {
    const data = await steamInventoryService.fetchInventory(steamId64, {
      timeoutMs: steamInventoryTimeoutMs
    });
    return { ...data, source: "steam" };
  } catch (_err) {
    const items = await mockSteamService.fetchInventory(steamId64);
    return { items, excludedItems: [], source: "mock-fallback" };
  }
}

function isFresh(recordedAt, ttlMinutes) {
  if (!recordedAt || ttlMinutes <= 0) return false;
  const ts = new Date(recordedAt).getTime();
  if (Number.isNaN(ts)) return false;
  return Date.now() - ts <= ttlMinutes * 60 * 1000;
}

function canUseCachedSource(source) {
  if (!source) return false;
  if (marketPriceFallbackToMock) return true;
  return !String(source).includes("mock");
}

exports.syncUserInventory = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User profile not found", 404);
  }
  if (!user.steam_id64) {
    throw new AppError("Connect Steam ID first", 400);
  }

  const {
    items,
    excludedItems: inventoryExcludedItems = [],
    source
  } = await fetchInventoryByConfiguredSource(user.steam_id64);
  if (!items.length) {
    throw new AppError("No CS2 inventory items found", 404);
  }

  const upsertedSkins = await skinRepo.upsertSkins(
    items.map((i) => ({
      market_hash_name: i.marketHashName,
      weapon: i.weapon,
      skin_name: i.skinName,
      exterior: i.exterior,
      rarity: i.rarity,
      image_url: i.imageUrl
    }))
  );

  const skinMap = Object.fromEntries(
    upsertedSkins.map((skin) => [skin.market_hash_name, skin.id])
  );

  const latestPriceBySkinId = await priceRepo.getLatestPriceRowsBySkinIds(
    upsertedSkins.map((s) => s.id)
  );

  const pricedItems = [];
  const pauseMs = Math.max(
    Math.floor(1000 / Math.max(marketPriceRateLimitPerSecond, 1)),
    1
  );
  let cacheHitCount = 0;

  for (const item of items) {
    const skinId = skinMap[item.marketHashName];
    const cached = latestPriceBySkinId[skinId];
    if (
      cached &&
      isFresh(cached.recorded_at, marketPriceCacheTtlMinutes) &&
      canUseCachedSource(cached.source)
    ) {
      pricedItems.push({
        ...item,
        skinId,
        price: Number(cached.price),
        priceSource: `cache:${cached.source}`
      });
      cacheHitCount += 1;
      continue;
    }

    try {
      const priced = await withRetries(
        () => priceProviderService.getPrice(item.marketHashName),
        3,
        300
      );
      pricedItems.push({
        ...item,
        skinId,
        price: priced.price,
        priceSource: priced.source
      });
    } catch (err) {
      if (!marketPriceFallbackToMock) {
        pricedItems.push({
          ...item,
          skinId,
          price: null,
          priceSource: "unpriced",
          priceError: err.message
        });
        await sleep(pauseMs);
        continue;
      }
      const fallbackPrice = await mockPriceProviderService.getLatestPrice(
        item.marketHashName
      );
      pricedItems.push({
        ...item,
        skinId,
        price: fallbackPrice,
        priceSource: "mock-price-fallback"
      });
    }

    await sleep(pauseMs);
  }

  await inventoryRepo.syncInventorySnapshot(
    userId,
    pricedItems.map((i) => ({
      skin_id: i.skinId,
      quantity: i.quantity,
      steam_item_ids: i.steamItemIds || []
    }))
  );

  await priceRepo.insertPriceRows(
    pricedItems
      .filter((i) => i.price != null)
      .map((i) => ({
        skin_id: i.skinId,
        price: i.price,
        currency: "USD",
        source: i.priceSource
      }))
  );

  const unpricedItems = pricedItems
    .filter((i) => i.price == null)
    .map((i) => ({
      marketHashName: i.marketHashName,
      reason: i.priceError || "No price available"
    }));

  return {
    synced: true,
    itemsSynced: pricedItems.length,
    pricedItems: pricedItems.length - unpricedItems.length,
    unpricedItemsCount: unpricedItems.length,
    unpricedItems,
    excludedItemsCount: inventoryExcludedItems.length,
    excludedItems: inventoryExcludedItems,
    priceCacheHitCount: cacheHitCount,
    priceFetchedCount: pricedItems.length - cacheHitCount,
    inventorySource: source,
    priceSource: marketPriceFallbackToMock ? "mixed" : "strict-real",
    syncedAt: new Date().toISOString()
  };
};

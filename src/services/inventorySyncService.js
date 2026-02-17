const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const skinRepo = require("../repositories/skinRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const { steamInventorySource, steamInventoryTimeoutMs } = require("../config/env");
const priceProviderService = require("./priceProviderService");
const mockPriceProviderService = require("./mockPriceProviderService");
const mockSteamService = require("./mockSteamService");
const steamInventoryService = require("./steamInventoryService");

async function fetchInventoryByConfiguredSource(steamId64) {
  if (steamInventorySource === "mock") {
    return { items: await mockSteamService.fetchInventory(steamId64), source: "mock" };
  }

  if (steamInventorySource === "real") {
    const items = await steamInventoryService.fetchInventory(steamId64, {
      timeoutMs: steamInventoryTimeoutMs
    });
    return { items, source: "steam" };
  }

  try {
    const items = await steamInventoryService.fetchInventory(steamId64, {
      timeoutMs: steamInventoryTimeoutMs
    });
    return { items, source: "steam" };
  } catch (_err) {
    const items = await mockSteamService.fetchInventory(steamId64);
    return { items, source: "mock-fallback" };
  }
}

exports.syncUserInventory = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User profile not found", 404);
  }
  if (!user.steam_id64) {
    throw new AppError("Connect Steam ID first", 400);
  }

  const { items, source } = await fetchInventoryByConfiguredSource(user.steam_id64);
  if (!items.length) {
    throw new AppError("No CS2 inventory items found", 404);
  }

  const pricedItems = await Promise.all(
    items.map(async (item) => {
      try {
        const priced = await priceProviderService.getPrice(item.marketHashName);
        return {
          ...item,
          price: priced.price,
          priceSource: priced.source
        };
      } catch (_err) {
        const fallbackPrice = await mockPriceProviderService.getLatestPrice(
          item.marketHashName
        );
        return {
          ...item,
          price: fallbackPrice,
          priceSource: "mock-price-fallback"
        };
      }
    })
  );

  const upsertedSkins = await skinRepo.upsertSkins(
    pricedItems.map((i) => ({
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

  await inventoryRepo.syncInventorySnapshot(
    userId,
    pricedItems.map((i) => ({
      skin_id: skinMap[i.marketHashName],
      quantity: i.quantity
    }))
  );

  await priceRepo.insertPriceRows(
    pricedItems.map((i) => ({
      skin_id: skinMap[i.marketHashName],
      price: i.price,
      currency: "USD",
      source: i.priceSource
    }))
  );

  return {
    synced: true,
    itemsSynced: pricedItems.length,
    inventorySource: source,
    priceSource: "mixed",
    syncedAt: new Date().toISOString()
  };
};

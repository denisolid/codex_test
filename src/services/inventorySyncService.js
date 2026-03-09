const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const skinRepo = require("../repositories/skinRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const ownershipAlertRepo = require("../repositories/ownershipAlertRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const {
  steamInventorySource,
  steamInventoryTimeoutMs,
  steamInventoryMaxRetries,
  steamInventoryRetryBaseMs,
  marketPriceSource,
  marketPriceFallbackToMock,
  marketPriceCacheTtlMinutes,
  inventorySyncPriceConcurrency
} = require("../config/env");
const priceProviderService = require("./priceProviderService");
const mockPriceProviderService = require("./mockPriceProviderService");
const mockSteamService = require("./mockSteamService");
const steamInventoryService = require("./steamInventoryService");
const { enrichInventoryItems } = require("./itemEnrichmentService");

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const safeItems = Array.isArray(items) ? items : [];
  if (!safeItems.length) return [];

  const limit = Math.max(Number(concurrency || 1), 1);
  const results = new Array(safeItems.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= safeItems.length) {
        return;
      }

      results[index] = await mapper(safeItems[index], index);
    }
  }

  const workers = Array.from(
    { length: Math.min(limit, safeItems.length) },
    () => worker()
  );
  await Promise.all(workers);
  return results;
}

async function fetchInventoryByConfiguredSource(steamId64) {
  const fetchOptions = {
    timeoutMs: steamInventoryTimeoutMs,
    maxRetries: steamInventoryMaxRetries,
    retryBaseMs: steamInventoryRetryBaseMs
  };

  if (steamInventorySource === "mock") {
    return {
      items: await mockSteamService.fetchInventory(steamId64),
      excludedItems: [],
      source: "mock"
    };
  }

  if (steamInventorySource === "real") {
    const data = await steamInventoryService.fetchInventory(steamId64, fetchOptions);
    return { ...data, source: "steam" };
  }

  try {
    const data = await steamInventoryService.fetchInventory(steamId64, fetchOptions);
    return { ...data, source: "steam" };
  } catch (_err) {
    const items = await mockSteamService.fetchInventory(steamId64);
    return { items, excludedItems: [], source: "mock-fallback" };
  }
}

function getStatusCode(err) {
  return (
    Number(
      err?.statusCode || err?.status || err?.httpStatus || err?.response?.status || 0
    ) || 0
  );
}

function buildItemsFromStoredHoldings(previousHoldings = []) {
  const safeRows = Array.isArray(previousHoldings) ? previousHoldings : [];
  const items = [];

  for (const row of safeRows) {
    const marketHashName = String(row?.skins?.market_hash_name || "").trim();
    const quantity = Number(row?.quantity || 0);

    if (!marketHashName || !Number.isFinite(quantity) || quantity <= 0) {
      continue;
    }

    const steamItemIds = Array.isArray(row?.steam_item_ids)
      ? row.steam_item_ids
          .map((id) => String(id || "").trim())
          .filter(Boolean)
      : [];

    items.push({
      marketHashName,
      quantity: Math.max(Math.floor(quantity), 1),
      steamItemIds,
      price: null
    });
  }

  return items;
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

  const previousHoldings = await inventoryRepo.getUserHoldings(userId);

  let inventoryPayload = null;
  let inventoryWarning = "";
  try {
    inventoryPayload = await fetchInventoryByConfiguredSource(user.steam_id64);
  } catch (err) {
    const status = getStatusCode(err);
    const fallbackItems = buildItemsFromStoredHoldings(previousHoldings);
    if (status === 429 && fallbackItems.length) {
      inventoryPayload = {
        items: fallbackItems,
        excludedItems: [],
        source: "stored-holdings-fallback"
      };
      inventoryWarning =
        "Steam inventory is rate limited. Using last stored holdings for this sync.";
    } else {
      throw err;
    }
  }

  const {
    items,
    excludedItems: inventoryExcludedItems = [],
    source
  } = inventoryPayload;
  if (!items.length) {
    throw new AppError("No CS2 inventory items found", 404);
  }

  const existingSkins = await skinRepo.getByMarketHashNames(
    items.map((item) => item.marketHashName)
  );
  const existingSkinByName = Object.fromEntries(
    (Array.isArray(existingSkins) ? existingSkins : []).map((skin) => [
      skin.market_hash_name,
      skin
    ])
  );
  const { enrichedItems, skinRows } = enrichInventoryItems(items, existingSkinByName);

  const upsertedSkins = await skinRepo.upsertSkins(skinRows);
  if (!upsertedSkins.length) {
    throw new AppError("Failed to upsert inventory skin metadata", 500);
  }

  const skinMap = Object.fromEntries(
    upsertedSkins.map((skin) => [skin.market_hash_name, skin.id])
  );

  const priceLookupSkinIds = Array.from(
    new Set([
      ...upsertedSkins.map((s) => Number(s.id)),
      ...previousHoldings.map((row) => Number(row.skin_id))
    ])
  );
  const latestPriceBySkinId = await priceRepo.getLatestPriceRowsBySkinIds(
    priceLookupSkinIds
  );

  const pricedItems = await mapWithConcurrency(
    enrichedItems,
    inventorySyncPriceConcurrency,
    async (item) => {
      const skinId = Number(skinMap[item.marketHashName] || 0);
      if (!skinId) {
        throw new AppError(
          `Missing skin id for ${item.marketHashName}`,
          500,
          "SKIN_UPSERT_INCONSISTENT"
        );
      }

      const cached = latestPriceBySkinId[skinId];
      if (
        cached &&
        isFresh(cached.recorded_at, marketPriceCacheTtlMinutes) &&
        canUseCachedSource(cached.source)
      ) {
        return {
          ...item,
          skinId,
          price: Number(cached.price),
          priceSource: `cache:${cached.source}`,
          __cacheHit: true
        };
      }

      try {
        // priceProviderService already applies provider-level retry logic.
        const priced = await priceProviderService.getPrice(item.marketHashName);
        return {
          ...item,
          skinId,
          price: priced.price,
          priceSource: priced.source,
          __cacheHit: false
        };
      } catch (err) {
        if (!marketPriceFallbackToMock) {
          return {
            ...item,
            skinId,
            price: null,
            priceSource: "unpriced",
            priceError: err.message,
            __cacheHit: false
          };
        }

        const fallbackPrice = await mockPriceProviderService.getLatestPrice(
          item.marketHashName
        );
        return {
          ...item,
          skinId,
          price: fallbackPrice,
          priceSource: "mock-price-fallback",
          __cacheHit: false
        };
      }
    }
  );

  const cacheHitCount = pricedItems.reduce(
    (acc, item) => (item.__cacheHit ? acc + 1 : acc),
    0
  );

  const normalizedPricedItems = pricedItems.map((item) => {
    if (!Object.prototype.hasOwnProperty.call(item, "__cacheHit")) {
      return item;
    }
    const copy = { ...item };
    delete copy.__cacheHit;
    return copy;
  });

  await inventoryRepo.syncInventorySnapshot(
    userId,
    normalizedPricedItems.map((i) => ({
      skin_id: i.skinId,
      quantity: i.quantity,
      steam_item_ids: i.steamItemIds || []
    }))
  );

  await priceRepo.insertPriceRows(
    normalizedPricedItems
      .filter((i) => i.price != null)
      .map((i) => ({
        skin_id: i.skinId,
        price: i.price,
        currency: "USD",
        source: i.priceSource
      }))
  );

  const syncedAt = new Date().toISOString();
  const previousBySkinId = previousHoldings.reduce((acc, row) => {
    acc[Number(row.skin_id)] = {
      quantity: Number(row.quantity || 0),
      marketHashName: row?.skins?.market_hash_name || `Skin #${row.skin_id}`
    };
    return acc;
  }, {});
  const currentBySkinId = normalizedPricedItems.reduce((acc, row) => {
    const skinId = Number(row.skinId);
    if (!acc[skinId]) {
      acc[skinId] = {
        quantity: 0,
        marketHashName: row.marketHashName || `Skin #${skinId}`,
        price: row.price == null ? null : Number(row.price)
      };
    }

    acc[skinId].quantity += Number(row.quantity || 0);
    if (acc[skinId].price == null && row.price != null) {
      acc[skinId].price = Number(row.price);
    }
    return acc;
  }, {});

  const skinIds = Array.from(
    new Set([
      ...Object.keys(previousBySkinId).map(Number),
      ...Object.keys(currentBySkinId).map(Number)
    ])
  );

  const ownershipChanges = skinIds
    .map((skinId) => {
      const previous = previousBySkinId[skinId] || {
        quantity: 0,
        marketHashName: `Skin #${skinId}`
      };
      const current = currentBySkinId[skinId] || {
        quantity: 0,
        marketHashName: previous.marketHashName,
        price: null
      };

      const previousQuantity = Number(previous.quantity || 0);
      const newQuantity = Number(current.quantity || 0);
      if (previousQuantity === newQuantity) {
        return null;
      }

      const quantityDelta = newQuantity - previousQuantity;
      const changeType =
        previousQuantity === 0 && newQuantity > 0
          ? "acquired"
          : previousQuantity > 0 && newQuantity === 0
            ? "disposed"
            : quantityDelta > 0
              ? "increased"
              : "decreased";

      const rowPrice =
        current.price != null
          ? Number(current.price)
          : latestPriceBySkinId[skinId]?.price != null
            ? Number(latestPriceBySkinId[skinId].price)
            : null;
      const estimatedValueDelta =
        rowPrice == null ? null : Number((quantityDelta * rowPrice).toFixed(2));

      return {
        skinId,
        marketHashName: current.marketHashName || previous.marketHashName,
        changeType,
        previousQuantity,
        newQuantity,
        quantityDelta,
        estimatedValueDelta,
        currency: "USD",
        syncedAt
      };
    })
    .filter(Boolean)
    .sort((a, b) => Math.abs(Number(b.quantityDelta || 0)) - Math.abs(Number(a.quantityDelta || 0)));

  if (user.ownership_alerts_enabled !== false && ownershipChanges.length) {
    await ownershipAlertRepo.insertEvents(userId, ownershipChanges);
  }

  const unpricedItems = normalizedPricedItems
    .filter((i) => i.price == null)
    .map((i) => ({
      marketHashName: i.marketHashName,
      reason: i.priceError || "No price available"
    }));

  return {
    synced: true,
    inventoryFallbackUsed: Boolean(inventoryWarning),
    inventoryWarning: inventoryWarning || null,
    itemsSynced: normalizedPricedItems.length,
    pricedItems: normalizedPricedItems.length - unpricedItems.length,
    unpricedItemsCount: unpricedItems.length,
    unpricedItems,
    excludedItemsCount: inventoryExcludedItems.length,
    excludedItems: inventoryExcludedItems,
    priceCacheHitCount: cacheHitCount,
    priceFetchedCount: normalizedPricedItems.length - cacheHitCount,
    inventorySource: source,
    configuredInventorySource: steamInventorySource,
    configuredMarketPriceSource: marketPriceSource,
    configuredMarketPriceFallbackToMock: Boolean(marketPriceFallbackToMock),
    steamId64: String(user.steam_id64 || ""),
    priceSource: marketPriceFallbackToMock ? "mixed" : "strict-real",
    ownershipChangesCount: ownershipChanges.length,
    ownershipChanges: ownershipChanges.slice(0, 20),
    syncedAt
  };
};

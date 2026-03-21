const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");
const INVENTORY_PAGE_SIZE = 1000;
const SKIN_LOOKUP_CHUNK_SIZE = 200;
const PRIMARY_JOIN_TIMEOUT_MS = 2500;

function normalizeUserId(userId) {
  return String(userId || "").trim();
}

function chunkValues(values = [], chunkSize = SKIN_LOOKUP_CHUNK_SIZE) {
  const rows = Array.isArray(values) ? values : [];
  const size = Math.max(Number(chunkSize || 0), 1);
  const chunks = [];
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }
  return chunks;
}

function withTimeout(promise, timeoutMs = PRIMARY_JOIN_TIMEOUT_MS) {
  const safeTimeoutMs = Math.max(Number(timeoutMs || 0), 1000);
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("inventory_join_query_timeout")), safeTimeoutMs)
    )
  ]);
}

function isMissingColumnError(error, columnName = "") {
  const message = String(error?.message || "").toLowerCase();
  const code = String(error?.code || "").toUpperCase();
  return (
    code === "42703" ||
    (message.includes("does not exist") &&
      (!columnName || message.includes(String(columnName || "").toLowerCase())))
  );
}

exports.syncInventorySnapshot = async (userId, items) => {
  const skinIds = items.map((i) => i.skin_id);

  if (!skinIds.length) {
    const { error: deleteAllError } = await supabaseAdmin
      .from("inventories")
      .delete()
      .eq("user_id", userId);
    if (deleteAllError) {
      throw new AppError(deleteAllError.message, 500);
    }
    return;
  }

  const { error: deleteMissingError } = await supabaseAdmin
    .from("inventories")
    .delete()
    .eq("user_id", userId)
    .not("skin_id", "in", `(${skinIds.join(",")})`);

  if (deleteMissingError) {
    throw new AppError(deleteMissingError.message, 500);
  }

  const rows = items.map((i) => ({
    user_id: userId,
    skin_id: i.skin_id,
    quantity: i.quantity,
    steam_item_ids: i.steam_item_ids || [],
    last_synced_at: new Date().toISOString()
  }));

  const { error } = await supabaseAdmin
    .from("inventories")
    .upsert(rows, { onConflict: "user_id,skin_id" });

  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.getUserHoldings = async (userId) => {
  const safeUserId = normalizeUserId(userId);
  if (!safeUserId) return [];

  // Primary path (single query with join).
  let primaryRes = null;
  try {
    primaryRes = await withTimeout(
      supabaseAdmin
        .from("inventories")
        .select(
          "skin_id, quantity, steam_item_ids, purchase_price, skins!inner(market_hash_name, rarity, canonical_rarity, rarity_color, image_url, image_url_large)"
        )
        .eq("user_id", safeUserId)
    );
  } catch (_err) {
    primaryRes = { data: null, error: { message: "inventory_join_query_timeout" } };
  }
  if (primaryRes?.error && isMissingColumnError(primaryRes.error, "canonical_rarity")) {
    primaryRes = await withTimeout(
      supabaseAdmin
        .from("inventories")
        .select(
          "skin_id, quantity, steam_item_ids, purchase_price, skins!inner(market_hash_name, rarity, rarity_color, image_url, image_url_large)"
        )
        .eq("user_id", safeUserId)
    );
  }

  if (!primaryRes.error) {
    return primaryRes.data || [];
  }

  // Fallback path for statement-timeout-prone joins:
  // query inventories first, then resolve skins in chunked lookups.
  const inventories = [];
  let offset = 0;
  while (true) {
    let pageRes = null;
    try {
      pageRes = await withTimeout(
        supabaseAdmin
          .from("inventories")
          .select("skin_id, quantity, steam_item_ids, purchase_price")
          .eq("user_id", safeUserId)
          .range(offset, offset + INVENTORY_PAGE_SIZE - 1)
      );
    } catch (_err) {
      pageRes = { data: null, error: { message: "inventory_fallback_query_timeout" } };
    }

    if (pageRes.error) {
      throw new AppError(pageRes.error.message || primaryRes.error.message, 500);
    }

    const chunk = Array.isArray(pageRes.data) ? pageRes.data : [];
    inventories.push(...chunk);
    if (chunk.length < INVENTORY_PAGE_SIZE) break;
    offset += chunk.length;
  }

  if (!inventories.length) return [];

  const skinIds = Array.from(
    new Set(
      inventories
        .map((row) => Number(row.skin_id))
        .filter((skinId) => Number.isInteger(skinId) && skinId > 0)
    )
  );

  const skinsById = {};
  for (const chunk of chunkValues(skinIds)) {
    let skinsRes = null;
    try {
      skinsRes = await withTimeout(
        supabaseAdmin
          .from("skins")
          .select("id, market_hash_name, rarity, canonical_rarity, rarity_color, image_url, image_url_large")
          .in("id", chunk)
      );
    } catch (_err) {
      skinsRes = { data: null, error: { message: "inventory_skin_lookup_timeout" } };
    }
    if (skinsRes?.error && isMissingColumnError(skinsRes.error, "canonical_rarity")) {
      skinsRes = await withTimeout(
        supabaseAdmin
          .from("skins")
          .select("id, market_hash_name, rarity, rarity_color, image_url, image_url_large")
          .in("id", chunk)
      );
    }

    if (skinsRes.error) {
      throw new AppError(skinsRes.error.message || primaryRes.error.message, 500);
    }

    for (const row of skinsRes.data || []) {
      const skinId = Number(row?.id);
      if (!Number.isInteger(skinId) || skinId <= 0) continue;
      skinsById[skinId] = {
        market_hash_name: row.market_hash_name || null,
        rarity: row.rarity || null,
        canonical_rarity: row.canonical_rarity || null,
        rarity_color: row.rarity_color || null,
        image_url: row.image_url || null,
        image_url_large: row.image_url_large || null
      };
    }
  }

  return inventories.map((row) => ({
    ...row,
    skins: skinsById[Number(row.skin_id)] || {
      market_hash_name: null,
      rarity: null,
      canonical_rarity: null,
      rarity_color: null,
      image_url: null,
      image_url_large: null
    }
  }));
};

exports.getUserInventoryBySteamItemId = async (userId, steamItemId) => {
  const { data, error } = await supabaseAdmin
    .from("inventories")
    .select("skin_id, steam_item_ids")
    .eq("user_id", userId)
    .contains("steam_item_ids", [steamItemId])
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.getHoldingsByUserIds = async (userIds) => {
  const safeIds = Array.from(
    new Set(
      (Array.isArray(userIds) ? userIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!safeIds.length) {
    return [];
  }

  const { data, error } = await supabaseAdmin
    .from("inventories")
    .select("user_id, skin_id, quantity")
    .in("user_id", safeIds);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

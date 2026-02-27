const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

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
  const { data, error } = await supabaseAdmin
    .from("inventories")
    .select(
      "skin_id, quantity, steam_item_ids, purchase_price, skins!inner(market_hash_name)"
    )
    .eq("user_id", userId);

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || [];
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
    .select(
      "user_id, skin_id, quantity, steam_item_ids, skins!inner(market_hash_name)"
    )
    .in("user_id", safeIds);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

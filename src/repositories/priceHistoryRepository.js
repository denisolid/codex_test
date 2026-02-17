const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.insertPriceRows = async (rows) => {
  const { error } = await supabaseAdmin.from("price_history").insert(rows);
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.getLatestPricesBySkinIds = async (skinIds) => {
  if (!skinIds.length) {
    return {};
  }

  const { data, error } = await supabaseAdmin
    .from("price_history")
    .select("skin_id, price, recorded_at")
    .in("skin_id", skinIds)
    .order("recorded_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  const map = {};
  for (const row of data) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = Number(row.price);
    }
  }

  return map;
};

exports.getLatestPricesBeforeDate = async (skinIds, date) => {
  if (!skinIds.length) {
    return {};
  }

  const { data, error } = await supabaseAdmin
    .from("price_history")
    .select("skin_id, price, recorded_at")
    .in("skin_id", skinIds)
    .lte("recorded_at", date.toISOString())
    .order("recorded_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  const map = {};
  for (const row of data) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = Number(row.price);
    }
  }

  return map;
};

exports.getLatestPriceBySkinId = async (skinId) => {
  const { data, error } = await supabaseAdmin
    .from("price_history")
    .select("price, currency, recorded_at")
    .eq("skin_id", skinId)
    .order("recorded_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || null;
};

exports.getHistoryBySkinId = async (skinId, limit = 30) => {
  const { data, error } = await supabaseAdmin
    .from("price_history")
    .select("price, currency, recorded_at")
    .eq("skin_id", skinId)
    .order("recorded_at", { ascending: false })
    .limit(limit);

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || [];
};

const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.insertSnapshot = async (row) => {
  const { data, error } = await supabaseAdmin
    .from("market_item_snapshots")
    .insert(row)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.getLatestBySkinId = async (skinId) => {
  const { data, error } = await supabaseAdmin
    .from("market_item_snapshots")
    .select("*")
    .eq("skin_id", skinId)
    .order("captured_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.getLatestBySkinIds = async (skinIds) => {
  if (!skinIds.length) {
    return {};
  }

  const { data, error } = await supabaseAdmin
    .from("market_item_snapshots")
    .select("*")
    .in("skin_id", skinIds)
    .order("captured_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  const map = {};
  for (const row of data || []) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = row;
    }
  }

  return map;
};

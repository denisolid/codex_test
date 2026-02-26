const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.getById = async (id) => {
  const { data, error } = await supabaseAdmin
    .from("users")
    .select("*")
    .eq("id", id)
    .single();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }
  return data || null;
};

exports.getBySteamId64 = async (steamId64) => {
  const { data, error } = await supabaseAdmin
    .from("users")
    .select("*")
    .eq("steam_id64", steamId64)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.ensureExists = async (id, email) => {
  const { error } = await supabaseAdmin
    .from("users")
    .upsert([{ id, email }], { onConflict: "id" });
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.updateSteamProfileById = async (id, updates = {}) => {
  const patch = {};

  if (updates.steamId64 != null) {
    const steamId64 = String(updates.steamId64).trim();
    if (!/^\d{17}$/.test(steamId64)) {
      throw new AppError("steamId64 must be 17 digits", 400);
    }
    patch.steam_id64 = steamId64;
  }

  if (updates.displayName != null) {
    patch.display_name = String(updates.displayName || "").trim() || null;
  }

  if (updates.avatarUrl != null) {
    patch.avatar_url = String(updates.avatarUrl || "").trim() || null;
  }

  if (!Object.keys(patch).length) {
    return exports.getById(id);
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .update(patch)
    .eq("id", id)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.updateSteamId = async (id, steamId64) => {
  return exports.updateSteamProfileById(id, { steamId64 });
};

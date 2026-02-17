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

exports.ensureExists = async (id, email) => {
  const { error } = await supabaseAdmin
    .from("users")
    .upsert([{ id, email }], { onConflict: "id" });
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.updateSteamId = async (id, steamId64) => {
  if (!/^\d{17}$/.test(steamId64)) {
    throw new AppError("steamId64 must be 17 digits", 400);
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .update({ steam_id64: steamId64 })
    .eq("id", id)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data;
};

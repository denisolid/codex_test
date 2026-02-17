const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.upsertSkins = async (rows) => {
  const { data, error } = await supabaseAdmin
    .from("skins")
    .upsert(rows, { onConflict: "market_hash_name" })
    .select("*");

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data;
};

exports.getById = async (id) => {
  const { data, error } = await supabaseAdmin
    .from("skins")
    .select("*")
    .eq("id", id)
    .single();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }
  return data || null;
};

exports.listAll = async () => {
  const { data, error } = await supabaseAdmin
    .from("skins")
    .select("id, market_hash_name")
    .order("id", { ascending: true });

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || [];
};

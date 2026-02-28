const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

function normalizeMarketHashNames(names = []) {
  return Array.from(
    new Set(
      (Array.isArray(names) ? names : [])
        .map((name) => String(name || "").trim())
        .filter(Boolean)
    )
  );
}

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

exports.getByMarketHashNames = async (names = []) => {
  const safeNames = normalizeMarketHashNames(names);
  if (!safeNames.length) {
    return [];
  }

  const { data, error } = await supabaseAdmin
    .from("skins")
    .select("*")
    .in("market_hash_name", safeNames);

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || [];
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

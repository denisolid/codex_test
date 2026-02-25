const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

const KEY_SELECT =
  "id, user_id, key_prefix, label, last_used_at, expires_at, revoked_at, created_at";

exports.create = async (userId, payload) => {
  const row = {
    user_id: userId,
    key_hash: payload.keyHash,
    key_prefix: payload.keyPrefix,
    label: payload.label || "default",
    expires_at: payload.expiresAt || null
  };

  const { data, error } = await supabaseAdmin
    .from("extension_api_keys")
    .insert(row)
    .select(KEY_SELECT)
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.listByUser = async (userId) => {
  const { data, error } = await supabaseAdmin
    .from("extension_api_keys")
    .select(KEY_SELECT)
    .eq("user_id", userId)
    .order("created_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.revoke = async (userId, id) => {
  const { data, error } = await supabaseAdmin
    .from("extension_api_keys")
    .update({ revoked_at: new Date().toISOString() })
    .eq("user_id", userId)
    .eq("id", id)
    .is("revoked_at", null)
    .select("id")
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return Boolean(data);
};

exports.getActiveByHash = async (keyHash) => {
  const { data, error } = await supabaseAdmin
    .from("extension_api_keys")
    .select("id, user_id, key_prefix, label, expires_at, revoked_at")
    .eq("key_hash", keyHash)
    .is("revoked_at", null)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  if (!data) {
    return null;
  }

  if (data.expires_at) {
    const expiresTs = new Date(data.expires_at).getTime();
    if (!Number.isNaN(expiresTs) && expiresTs <= Date.now()) {
      return null;
    }
  }

  return data;
};

exports.touchLastUsed = async (id) => {
  const { error } = await supabaseAdmin
    .from("extension_api_keys")
    .update({ last_used_at: new Date().toISOString() })
    .eq("id", id);

  if (error) {
    throw new AppError(error.message, 500);
  }
};

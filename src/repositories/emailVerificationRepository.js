const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.invalidatePendingByUser = async (userId) => {
  const safeUserId = String(userId || "").trim();
  if (!safeUserId) return 0;

  const { data, error } = await supabaseAdmin
    .from("user_email_verifications")
    .update({ used_at: new Date().toISOString() })
    .eq("user_id", safeUserId)
    .is("used_at", null)
    .select("id");

  if (error) {
    throw new AppError(error.message, 500);
  }

  return Array.isArray(data) ? data.length : 0;
};

exports.create = async (payload = {}) => {
  const row = {
    user_id: String(payload.userId || "").trim(),
    email: String(payload.email || "").trim().toLowerCase(),
    token_hash: String(payload.tokenHash || "").trim(),
    expires_at: payload.expiresAt
  };

  const { data, error } = await supabaseAdmin
    .from("user_email_verifications")
    .insert(row)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.getByTokenHash = async (tokenHash) => {
  const safeTokenHash = String(tokenHash || "").trim();
  if (!safeTokenHash) return null;

  const { data, error } = await supabaseAdmin
    .from("user_email_verifications")
    .select("*")
    .eq("token_hash", safeTokenHash)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.consumeById = async (id) => {
  const safeId = String(id || "").trim();
  if (!safeId) return null;

  const { data, error } = await supabaseAdmin
    .from("user_email_verifications")
    .update({ used_at: new Date().toISOString() })
    .eq("id", safeId)
    .is("used_at", null)
    .select("*")
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.getLatestPendingByUser = async (userId) => {
  const safeUserId = String(userId || "").trim();
  if (!safeUserId) return null;

  const { data, error } = await supabaseAdmin
    .from("user_email_verifications")
    .select("*")
    .eq("user_id", safeUserId)
    .is("used_at", null)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

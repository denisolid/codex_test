const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

function normalizeUserId(value) {
  return String(value || "").trim();
}

exports.getByUserId = async (userId) => {
  const safeUserId = normalizeUserId(userId);
  if (!safeUserId) {
    return null;
  }

  const { data, error } = await supabaseAdmin
    .from("user_price_preferences")
    .select("*")
    .eq("user_id", safeUserId)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.upsertByUserId = async (userId, updates = {}) => {
  const safeUserId = normalizeUserId(userId);
  if (!safeUserId) {
    throw new AppError("Invalid user id", 400, "VALIDATION_ERROR");
  }

  const row = {
    user_id: safeUserId
  };

  if (updates.pricingMode != null) {
    row.pricing_mode = String(updates.pricingMode || "")
      .trim()
      .toLowerCase();
  }

  if (updates.preferredCurrency != null) {
    row.preferred_currency = String(updates.preferredCurrency || "")
      .trim()
      .toUpperCase();
  }

  const { data, error } = await supabaseAdmin
    .from("user_price_preferences")
    .upsert(row, { onConflict: "user_id" })
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

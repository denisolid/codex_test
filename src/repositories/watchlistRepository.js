const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.add = async (userId, targetUserId) => {
  const row = {
    user_id: userId,
    target_user_id: targetUserId
  };

  const { data, error } = await supabaseAdmin
    .from("watchlists")
    .upsert(row, { onConflict: "user_id,target_user_id" })
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.remove = async (userId, targetUserId) => {
  const { data, error } = await supabaseAdmin
    .from("watchlists")
    .delete()
    .eq("user_id", userId)
    .eq("target_user_id", targetUserId)
    .select("user_id, target_user_id")
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return Boolean(data);
};

exports.listByUser = async (userId) => {
  const { data, error } = await supabaseAdmin
    .from("watchlists")
    .select("user_id, target_user_id, created_at")
    .eq("user_id", userId)
    .order("created_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.listByTargetIds = async (targetUserIds) => {
  if (!Array.isArray(targetUserIds) || !targetUserIds.length) {
    return [];
  }

  const { data, error } = await supabaseAdmin
    .from("watchlists")
    .select("target_user_id")
    .in("target_user_id", targetUserIds);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

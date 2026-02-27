const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

exports.insertEvents = async (userId, events = []) => {
  if (!Array.isArray(events) || !events.length) {
    return [];
  }

  const rows = events.map((event) => ({
    user_id: userId,
    skin_id: event.skinId ?? null,
    market_hash_name: event.marketHashName,
    change_type: event.changeType,
    previous_quantity: Number(event.previousQuantity || 0),
    new_quantity: Number(event.newQuantity || 0),
    quantity_delta: Number(event.quantityDelta || 0),
    estimated_value_delta:
      event.estimatedValueDelta == null ? null : Number(event.estimatedValueDelta),
    currency: event.currency || "USD",
    synced_at: event.syncedAt || new Date().toISOString()
  }));

  const { data, error } = await supabaseAdmin
    .from("ownership_alert_events")
    .insert(rows)
    .select("*");

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.listByUser = async (userId, limit = 100) => {
  const normalizedLimit = Math.min(Math.max(Number(limit) || 100, 1), 500);
  const { data, error } = await supabaseAdmin
    .from("ownership_alert_events")
    .select(
      "id, user_id, skin_id, market_hash_name, change_type, previous_quantity, new_quantity, quantity_delta, estimated_value_delta, currency, synced_at, created_at"
    )
    .eq("user_id", userId)
    .order("created_at", { ascending: false })
    .limit(normalizedLimit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

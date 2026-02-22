const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

const ALERT_SELECT =
  "id, user_id, skin_id, target_price, percent_change_threshold, direction, enabled, cooldown_minutes, last_triggered_at, created_at, updated_at";

exports.create = async (userId, payload) => {
  const row = {
    user_id: userId,
    skin_id: payload.skinId,
    target_price: payload.targetPrice ?? null,
    percent_change_threshold: payload.percentChangeThreshold ?? null,
    direction: payload.direction || "both",
    enabled: payload.enabled ?? true,
    cooldown_minutes: payload.cooldownMinutes ?? 60
  };

  const { data, error } = await supabaseAdmin
    .from("price_alerts")
    .insert(row)
    .select(ALERT_SELECT)
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.listByUser = async (userId) => {
  const { data, error } = await supabaseAdmin
    .from("price_alerts")
    .select(`${ALERT_SELECT}, skins!inner(market_hash_name)`)
    .eq("user_id", userId)
    .order("created_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.getById = async (userId, id) => {
  const { data, error } = await supabaseAdmin
    .from("price_alerts")
    .select(ALERT_SELECT)
    .eq("user_id", userId)
    .eq("id", id)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.update = async (userId, id, payload) => {
  const updates = {};
  if (payload.skinId != null) updates.skin_id = payload.skinId;
  if (payload.targetPrice !== undefined) updates.target_price = payload.targetPrice;
  if (payload.percentChangeThreshold !== undefined) {
    updates.percent_change_threshold = payload.percentChangeThreshold;
  }
  if (payload.direction != null) updates.direction = payload.direction;
  if (payload.enabled != null) updates.enabled = payload.enabled;
  if (payload.cooldownMinutes != null) updates.cooldown_minutes = payload.cooldownMinutes;

  const { data, error } = await supabaseAdmin
    .from("price_alerts")
    .update(updates)
    .eq("user_id", userId)
    .eq("id", id)
    .select(ALERT_SELECT)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.remove = async (userId, id) => {
  const { data, error } = await supabaseAdmin
    .from("price_alerts")
    .delete()
    .eq("user_id", userId)
    .eq("id", id)
    .select("id")
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return Boolean(data);
};

exports.listEnabled = async (limit = 250) => {
  const { data, error } = await supabaseAdmin
    .from("price_alerts")
    .select(ALERT_SELECT)
    .eq("enabled", true)
    .order("id", { ascending: true })
    .limit(limit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.markTriggered = async (id, triggeredAt) => {
  const { error } = await supabaseAdmin
    .from("price_alerts")
    .update({ last_triggered_at: triggeredAt })
    .eq("id", id);

  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.createEvent = async (eventPayload) => {
  const row = {
    alert_id: eventPayload.alertId,
    user_id: eventPayload.userId,
    skin_id: eventPayload.skinId,
    trigger_type: eventPayload.triggerType,
    trigger_value: eventPayload.triggerValue,
    market_price: eventPayload.marketPrice,
    previous_price: eventPayload.previousPrice ?? null,
    change_percent: eventPayload.changePercent ?? null,
    triggered_at: eventPayload.triggeredAt || new Date().toISOString()
  };

  const { data, error } = await supabaseAdmin
    .from("alert_events")
    .insert(row)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.listEventsByUser = async (userId, limit = 100) => {
  const { data, error } = await supabaseAdmin
    .from("alert_events")
    .select("id, alert_id, skin_id, trigger_type, trigger_value, market_price, previous_price, change_percent, triggered_at, skins!inner(market_hash_name)")
    .eq("user_id", userId)
    .order("triggered_at", { ascending: false })
    .limit(limit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

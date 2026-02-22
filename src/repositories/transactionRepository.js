const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

const TX_SELECT_FIELDS =
  "id, skin_id, type, quantity, unit_price, commission_percent, gross_total, net_total, currency, executed_at, created_at";

exports.create = async (userId, payload) => {
  const row = {
    user_id: userId,
    skin_id: payload.skinId,
    type: payload.type,
    quantity: payload.quantity,
    unit_price: payload.unitPrice,
    commission_percent: payload.commissionPercent,
    gross_total: payload.grossTotal,
    net_total: payload.netTotal,
    currency: payload.currency || "USD",
    executed_at: payload.executedAt || new Date().toISOString()
  };

  const { data, error } = await supabaseAdmin
    .from("transactions")
    .insert(row)
    .select(TX_SELECT_FIELDS)
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data;
};

exports.listByUser = async (userId) => {
  const { data, error } = await supabaseAdmin
    .from("transactions")
    .select(TX_SELECT_FIELDS)
    .eq("user_id", userId)
    .order("executed_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || [];
};

exports.getById = async (userId, id) => {
  const { data, error } = await supabaseAdmin
    .from("transactions")
    .select(TX_SELECT_FIELDS)
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
  if (payload.type != null) updates.type = payload.type;
  if (payload.quantity != null) updates.quantity = payload.quantity;
  if (payload.unitPrice != null) updates.unit_price = payload.unitPrice;
  if (payload.commissionPercent != null) {
    updates.commission_percent = payload.commissionPercent;
  }
  if (payload.grossTotal != null) updates.gross_total = payload.grossTotal;
  if (payload.netTotal != null) updates.net_total = payload.netTotal;
  if (payload.currency != null) updates.currency = payload.currency;
  if (payload.executedAt != null) updates.executed_at = payload.executedAt;

  const { data, error } = await supabaseAdmin
    .from("transactions")
    .update(updates)
    .eq("user_id", userId)
    .eq("id", id)
    .select(TX_SELECT_FIELDS)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || null;
};

exports.remove = async (userId, id) => {
  const { data, error } = await supabaseAdmin
    .from("transactions")
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

exports.getPositionCostBasisBySkin = async (userId) => {
  const { data, error } = await supabaseAdmin
    .from("transactions")
    .select("skin_id, type, quantity, unit_price, net_total, commission_percent, executed_at, created_at")
    .eq("user_id", userId)
    .order("executed_at", { ascending: true })
    .order("created_at", { ascending: true });

  if (error) {
    throw new AppError(error.message, 500);
  }

  const state = {};
  for (const tx of data || []) {
    const key = tx.skin_id;
    if (!state[key]) {
      state[key] = { quantity: 0, cost: 0 };
    }

    const pos = state[key];
    const qty = Number(tx.quantity);
    const value =
      tx.net_total != null ? Number(tx.net_total) : qty * Number(tx.unit_price);

    if (tx.type === "buy") {
      pos.quantity += qty;
      pos.cost += value;
      continue;
    }

    if (tx.type === "sell") {
      if (pos.quantity <= 0) {
        continue;
      }
      const avgCost = pos.cost / pos.quantity;
      const effectiveQty = Math.min(qty, pos.quantity);
      pos.quantity -= effectiveQty;
      pos.cost -= avgCost * effectiveQty;

      if (pos.quantity <= 0) {
        pos.quantity = 0;
        pos.cost = 0;
      }
    }
  }

  return state;
};

exports.getPnlStateBySkin = async (userId) => {
  const { data, error } = await supabaseAdmin
    .from("transactions")
    .select("skin_id, type, quantity, unit_price, net_total, commission_percent, executed_at, created_at")
    .eq("user_id", userId)
    .order("executed_at", { ascending: true })
    .order("created_at", { ascending: true });

  if (error) {
    throw new AppError(error.message, 500);
  }

  const state = {};

  for (const tx of data || []) {
    const key = tx.skin_id;
    if (!state[key]) {
      state[key] = { quantity: 0, cost: 0, realized: 0 };
    }

    const pos = state[key];
    const qty = Number(tx.quantity || 0);
    if (qty <= 0) {
      continue;
    }

    const grossValue = qty * Number(tx.unit_price || 0);
    const txNetTotal =
      tx.net_total != null
        ? Number(tx.net_total)
        : tx.type === "sell"
          ? grossValue * (1 - Number(tx.commission_percent || 0) / 100)
          : grossValue;

    if (tx.type === "buy") {
      pos.quantity += qty;
      pos.cost += txNetTotal;
      continue;
    }

    if (tx.type !== "sell" || pos.quantity <= 0) {
      continue;
    }

    const effectiveQty = Math.min(qty, pos.quantity);
    const avgCost = pos.cost / pos.quantity;
    const proceeds = txNetTotal * (effectiveQty / qty);
    const costRemoved = avgCost * effectiveQty;

    pos.quantity -= effectiveQty;
    pos.cost -= costRemoved;
    pos.realized += proceeds - costRemoved;

    if (pos.quantity <= 0) {
      pos.quantity = 0;
      pos.cost = 0;
    }
  }

  return state;
};

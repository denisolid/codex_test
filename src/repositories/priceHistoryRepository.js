const { supabaseAdmin } = require("../config/supabase");
const { marketPriceFallbackToMock } = require("../config/env");
const AppError = require("../utils/AppError");

function applyPriceSourceFilter(query) {
  if (marketPriceFallbackToMock) {
    return query;
  }

  return query.not("source", "ilike", "%mock%");
}

exports.insertPriceRows = async (rows) => {
  const { error } = await supabaseAdmin.from("price_history").insert(rows);
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.getLatestPricesBySkinIds = async (skinIds) => {
  if (!skinIds.length) {
    return {};
  }

  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
    .from("price_history")
    .select("skin_id, price, recorded_at")
    .in("skin_id", skinIds)
    .order("recorded_at", { ascending: false })
  );

  if (error) {
    throw new AppError(error.message, 500);
  }

  const map = {};
  for (const row of data) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = Number(row.price);
    }
  }

  return map;
};

exports.getLatestPriceRowsBySkinIds = async (skinIds) => {
  if (!skinIds.length) {
    return {};
  }

  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
    .from("price_history")
    .select("skin_id, price, currency, source, recorded_at")
    .in("skin_id", skinIds)
    .order("recorded_at", { ascending: false })
  );

  if (error) {
    throw new AppError(error.message, 500);
  }

  const map = {};
  for (const row of data || []) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = row;
    }
  }
  return map;
};

exports.getLatestPricesBeforeDate = async (skinIds, date) => {
  if (!skinIds.length) {
    return {};
  }

  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
    .from("price_history")
    .select("skin_id, price, recorded_at")
    .in("skin_id", skinIds)
    .lte("recorded_at", date.toISOString())
    .order("recorded_at", { ascending: false })
  );

  if (error) {
    throw new AppError(error.message, 500);
  }

  const map = {};
  for (const row of data) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = Number(row.price);
    }
  }

  return map;
};

exports.getLatestPriceBySkinId = async (skinId) => {
  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
    .from("price_history")
    .select("price, currency, source, recorded_at")
    .eq("skin_id", skinId)
    .order("recorded_at", { ascending: false })
    .limit(1)
    .maybeSingle()
  );

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || null;
};

exports.getHistoryBySkinId = async (skinId, limit = 30) => {
  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
    .from("price_history")
    .select("price, currency, recorded_at")
    .eq("skin_id", skinId)
    .order("recorded_at", { ascending: false })
    .limit(limit)
  );

  if (error) {
    throw new AppError(error.message, 500);
  }
  return data || [];
};

exports.getHistoryBySkinIdSince = async (skinId, sinceDate, limit = 2000) => {
  const sinceIso =
    sinceDate instanceof Date ? sinceDate.toISOString() : String(sinceDate);

  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
      .from("price_history")
      .select("price, currency, recorded_at")
      .eq("skin_id", skinId)
      .gte("recorded_at", sinceIso)
      .order("recorded_at", { ascending: false })
      .limit(limit)
  );

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.getHistoryBySkinIdsSince = async (skinIds, sinceDate, limit = 12000) => {
  if (!Array.isArray(skinIds) || !skinIds.length) {
    return [];
  }

  const sinceIso =
    sinceDate instanceof Date ? sinceDate.toISOString() : String(sinceDate);

  const { data, error } = await applyPriceSourceFilter(
    supabaseAdmin
      .from("price_history")
      .select("skin_id, price, currency, recorded_at")
      .in("skin_id", skinIds)
      .gte("recorded_at", sinceIso)
      .order("recorded_at", { ascending: false })
      .limit(limit)
  );

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.deleteMockPriceRows = async () => {
  const countQuery = await supabaseAdmin
    .from("price_history")
    .select("id", { count: "exact", head: true })
    .ilike("source", "%mock%");

  if (countQuery.error) {
    throw new AppError(countQuery.error.message, 500);
  }

  const toDelete = Number(countQuery.count || 0);
  if (!toDelete) {
    return 0;
  }

  const { error } = await supabaseAdmin
    .from("price_history")
    .delete()
    .ilike("source", "%mock%");

  if (error) {
    throw new AppError(error.message, 500);
  }

  return toDelete;
};

exports.__testables = {
  applyPriceSourceFilter
};

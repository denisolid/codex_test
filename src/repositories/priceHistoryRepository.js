const { supabaseAdmin } = require("../config/supabase");
const { marketPriceFallbackToMock } = require("../config/env");
const AppError = require("../utils/AppError");
const LATEST_PRICE_LOOKUP_CHUNK_SIZE = 200;

function applyPriceSourceFilter(query) {
  if (marketPriceFallbackToMock) {
    return query;
  }

  return query.not("source", "ilike", "%mock%");
}

function applyHistorySourceFilter(query, options = {}) {
  if (options.excludeMock === true) {
    return query.not("source", "ilike", "%mock%");
  }
  return applyPriceSourceFilter(query);
}

function normalizeSkinIds(skinIds = []) {
  return Array.from(
    new Set(
      (Array.isArray(skinIds) ? skinIds : [])
        .map((id) => Number(id))
        .filter((id) => Number.isInteger(id) && id > 0)
    )
  );
}

function chunkValues(values = [], chunkSize = LATEST_PRICE_LOOKUP_CHUNK_SIZE) {
  const rows = Array.isArray(values) ? values : [];
  const size = Math.max(Number(chunkSize || 0), 1);
  const chunks = [];
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size));
  }
  return chunks;
}

function buildLatestBySkinMap(rows = [], toValue) {
  const map = {};
  for (const row of rows || []) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = toValue(row);
    }
  }
  return map;
}

function isMissingRpcError(error) {
  if (!error) return false;
  const code = String(error.code || "").toUpperCase();
  const message = String(error.message || "").toLowerCase();
  return (
    code === "PGRST202" ||
    message.includes("could not find the function public.get_latest_price_rows_by_skin_ids")
  );
}

async function getLatestRowsWithFallback(skinIds, options = {}) {
  const safeIds = normalizeSkinIds(skinIds);
  if (!safeIds.length) {
    return [];
  }

  const beforeIso =
    options.beforeDate instanceof Date
      ? options.beforeDate.toISOString()
      : options.beforeDate
        ? String(options.beforeDate)
        : null;

  const rows = [];
  for (const chunk of chunkValues(safeIds)) {
    const rpcPayload = {
      p_skin_ids: chunk,
      p_before: beforeIso,
      p_exclude_mock: !marketPriceFallbackToMock
    };

    const rpcRes = await supabaseAdmin.rpc("get_latest_price_rows_by_skin_ids", rpcPayload);
    if (!rpcRes.error) {
      rows.push(...(rpcRes.data || []));
      continue;
    }

    if (!isMissingRpcError(rpcRes.error)) {
      throw new AppError(rpcRes.error.message, 500);
    }

    let query = supabaseAdmin
      .from("price_history")
      .select("skin_id, price, currency, source, recorded_at")
      .in("skin_id", chunk);

    if (beforeIso) {
      query = query.lte("recorded_at", beforeIso);
    }

    const fallbackRes = await applyPriceSourceFilter(
      query.order("recorded_at", { ascending: false })
    );

    if (fallbackRes.error) {
      throw new AppError(fallbackRes.error.message, 500);
    }

    rows.push(...(fallbackRes.data || []));
  }

  return rows;
}

exports.insertPriceRows = async (rows) => {
  const { error } = await supabaseAdmin.from("price_history").insert(rows);
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.getLatestPricesBySkinIds = async (skinIds) => {
  const rows = await getLatestRowsWithFallback(skinIds);
  return buildLatestBySkinMap(rows, (row) => Number(row.price));
};

exports.getLatestPriceRowsBySkinIds = async (skinIds) => {
  const rows = await getLatestRowsWithFallback(skinIds);
  return buildLatestBySkinMap(rows, (row) => row);
};

exports.getLatestPricesBeforeDate = async (skinIds, date) => {
  const rows = await getLatestRowsWithFallback(skinIds, { beforeDate: date });
  return buildLatestBySkinMap(rows, (row) => Number(row.price));
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

exports.getHistoryBySkinIdSince = async (
  skinId,
  sinceDate,
  limit = 2000,
  options = {}
) => {
  const sinceIso =
    sinceDate instanceof Date ? sinceDate.toISOString() : String(sinceDate);

  const { data, error } = await applyHistorySourceFilter(
    supabaseAdmin
      .from("price_history")
      .select("price, currency, source, recorded_at")
      .eq("skin_id", skinId)
      .gte("recorded_at", sinceIso)
      .order("recorded_at", { ascending: false })
      .limit(limit),
    options
  );

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.getHistoryBySkinIdsSince = async (
  skinIds,
  sinceDate,
  limit = 12000,
  options = {}
) => {
  if (!Array.isArray(skinIds) || !skinIds.length) {
    return [];
  }

  const sinceIso =
    sinceDate instanceof Date ? sinceDate.toISOString() : String(sinceDate);

  const { data, error } = await applyHistorySourceFilter(
    supabaseAdmin
      .from("price_history")
      .select("skin_id, price, currency, source, recorded_at")
      .in("skin_id", skinIds)
      .gte("recorded_at", sinceIso)
      .order("recorded_at", { ascending: false })
      .limit(limit),
    options
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
  applyPriceSourceFilter,
  applyHistorySourceFilter,
  normalizeSkinIds,
  isMissingRpcError
};

const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");
const SNAPSHOT_QUERY_BATCH_SIZE = 200

function normalizeSkinIds(skinIds = []) {
  return Array.from(
    new Set(
      (Array.isArray(skinIds) ? skinIds : [])
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value > 0)
    )
  )
}

function chunkArray(values = [], chunkSize = 100) {
  const rows = Array.isArray(values) ? values : []
  const size = Math.max(Number(chunkSize || 0), 1)
  const chunks = []
  for (let index = 0; index < rows.length; index += size) {
    chunks.push(rows.slice(index, index + size))
  }
  return chunks
}

function isMissingLatestSnapshotsRpcError(error) {
  if (!error) return false
  const code = String(error.code || "").toUpperCase()
  const message = String(error.message || "").toLowerCase()
  return (
    code === "PGRST202" ||
    message.includes("could not find the function public.get_latest_market_snapshots_by_skin_ids")
  )
}

async function fetchLatestSnapshotsFallback(skinIds = []) {
  const rows = []
  for (const chunk of chunkArray(skinIds, SNAPSHOT_QUERY_BATCH_SIZE)) {
    const { data, error } = await supabaseAdmin
      .from("market_item_snapshots")
      .select(
        "skin_id, lowest_listing_price, average_7d_price, volume_24h, spread_percent, volatility_7d_percent, currency, source, captured_at"
      )
      .in("skin_id", chunk)
      .order("captured_at", { ascending: false });

    if (error) {
      throw new AppError(error.message, 500);
    }
    if (Array.isArray(data) && data.length) {
      rows.push(...data)
    }
  }
  return rows
}

exports.insertSnapshot = async (row) => {
  const { data, error } = await supabaseAdmin
    .from("market_item_snapshots")
    .insert(row)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.getLatestBySkinId = async (skinId) => {
  const { data, error } = await supabaseAdmin
    .from("market_item_snapshots")
    .select("*")
    .eq("skin_id", skinId)
    .order("captured_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.getLatestBySkinIds = async (skinIds) => {
  const safeSkinIds = normalizeSkinIds(skinIds)
  if (!safeSkinIds.length) {
    return {};
  }

  let rpcAvailable = true
  const rows = []
  for (const chunk of chunkArray(safeSkinIds, SNAPSHOT_QUERY_BATCH_SIZE)) {
    let chunkRows = []

    if (rpcAvailable) {
      const rpcResult = await supabaseAdmin.rpc("get_latest_market_snapshots_by_skin_ids", {
        p_skin_ids: chunk
      })
      if (!rpcResult.error) {
        chunkRows = Array.isArray(rpcResult.data) ? rpcResult.data : []
      } else if (isMissingLatestSnapshotsRpcError(rpcResult.error)) {
        rpcAvailable = false
      } else {
        throw new AppError(rpcResult.error.message, 500)
      }
    }

    if (!rpcAvailable) {
      chunkRows = await fetchLatestSnapshotsFallback(chunk)
    }

    if (chunkRows.length) {
      rows.push(...chunkRows)
    }
  }

  const map = {};
  for (const row of rows) {
    if (map[row.skin_id] == null) {
      map[row.skin_id] = row;
    }
  }

  return map;
};

exports.getRecentSnapshots = async (options = {}) => {
  const limit = Math.min(
    Math.max(Number(options.limit || 20000), 1),
    50000
  );
  const { data, error } = await supabaseAdmin
    .from("market_item_snapshots")
    .select(
      "skin_id, lowest_listing_price, average_7d_price, volume_24h, spread_percent, volatility_7d_percent, currency, captured_at"
    )
    .order("captured_at", { ascending: false })
    .limit(limit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

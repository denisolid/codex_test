const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

const SOURCES = new Set(["steam", "skinport", "csfloat", "dmarket"]);

function normalizeName(value) {
  return String(value || "").trim();
}

function normalizeNames(names = []) {
  return Array.from(
    new Set(
      (Array.isArray(names) ? names : [])
        .map((name) => normalizeName(name))
        .filter(Boolean)
    )
  );
}

function normalizeSource(value) {
  const source = String(value || "")
    .trim()
    .toLowerCase();
  return SOURCES.has(source) ? source : null;
}

function normalizeSources(values = []) {
  const safeValues = Array.isArray(values) && values.length ? values : Array.from(SOURCES);
  return Array.from(
    new Set(
      safeValues
        .map((source) => normalizeSource(source))
        .filter(Boolean)
    )
  );
}

function toLatestBySourceAndName(rows = []) {
  const result = {};
  for (const row of rows || []) {
    const source = normalizeSource(row.market);
    const marketHashName = normalizeName(row.market_hash_name);
    if (!source || !marketHashName) continue;

    if (!result[source]) {
      result[source] = {};
    }

    if (!result[source][marketHashName]) {
      result[source][marketHashName] = row;
    }
  }
  return result;
}

exports.getLatestByMarketHashNames = async (names = [], options = {}) => {
  const marketHashNames = normalizeNames(names);
  if (!marketHashNames.length) {
    return {};
  }

  const sources = normalizeSources(options.sources);

  const { data, error } = await supabaseAdmin
    .from("market_prices")
    .select("*")
    .in("market_hash_name", marketHashNames)
    .in("market", sources)
    .order("fetched_at", { ascending: false });

  if (error) {
    throw new AppError(error.message, 500);
  }

  return toLatestBySourceAndName(data || []);
};

exports.upsertRows = async (rows = []) => {
  const payload = (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const source = normalizeSource(row.market || row.source);
      const marketHashName = normalizeName(row.market_hash_name || row.marketHashName);
      const grossPrice = Number(row.gross_price ?? row.grossPrice);
      const netPrice = Number(row.net_price ?? row.netPriceAfterFees);
      if (!source || !marketHashName || !Number.isFinite(grossPrice) || !Number.isFinite(netPrice)) {
        return null;
      }

      return {
        market: source,
        market_hash_name: marketHashName,
        currency: String(row.currency || "USD")
          .trim()
          .toUpperCase(),
        gross_price: Number(grossPrice.toFixed(2)),
        net_price: Number(netPrice.toFixed(2)),
        url: row.url ? String(row.url) : null,
        fetched_at: row.fetched_at || row.updatedAt || new Date().toISOString(),
        raw: row.raw || null
      };
    })
    .filter(Boolean);

  if (!payload.length) {
    return 0;
  }

  const { error } = await supabaseAdmin
    .from("market_prices")
    .upsert(payload, { onConflict: "market,market_hash_name" });

  if (error) {
    throw new AppError(error.message, 500);
  }

  return payload.length;
};

exports.deleteOlderThan = async (cutoffIso) => {
  const cutoff = String(cutoffIso || "").trim();
  if (!cutoff) return 0;

  const { count, error: countError } = await supabaseAdmin
    .from("market_prices")
    .select("id", { count: "exact", head: true })
    .lt("fetched_at", cutoff);

  if (countError) {
    throw new AppError(countError.message, 500);
  }

  const estimated = Number(count || 0);
  if (!estimated) {
    return 0;
  }

  const { error } = await supabaseAdmin
    .from("market_prices")
    .delete()
    .lt("fetched_at", cutoff);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return estimated;
};

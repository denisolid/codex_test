const asyncHandler = require("../utils/asyncHandler");
const priceRepo = require("../repositories/priceHistoryRepository");
const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

function parseWindowDays(value, fallback = 30) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), 365);
}

async function listActiveUserIds(sinceIso) {
  const sources = [
    { table: "users", userColumn: "id", timeColumn: "created_at" },
    { table: "inventories", userColumn: "user_id", timeColumn: "updated_at" },
    { table: "transactions", userColumn: "user_id", timeColumn: "executed_at" },
    { table: "price_alerts", userColumn: "user_id", timeColumn: "updated_at" },
    { table: "alert_events", userColumn: "user_id", timeColumn: "triggered_at" }
  ];

  const ids = new Set();

  for (const source of sources) {
    const { data, error } = await supabaseAdmin
      .from(source.table)
      .select(source.userColumn)
      .gte(source.timeColumn, sinceIso)
      .limit(10000);

    if (error) {
      throw new AppError(error.message, 500);
    }

    for (const row of data || []) {
      const id = String(row[source.userColumn] || "").trim();
      if (id) ids.add(id);
    }
  }

  return Array.from(ids);
}

exports.cleanupMockPrices = asyncHandler(async (_req, res) => {
  const deletedCount = await priceRepo.deleteMockPriceRows();
  res.json({
    ok: true,
    deletedCount
  });
});

exports.getSteamLinkRate = asyncHandler(async (req, res) => {
  const windowDays = parseWindowDays(req.query.windowDays, 30);
  const since = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();
  const activeUserIds = await listActiveUserIds(since);

  if (!activeUserIds.length) {
    res.json({
      ok: true,
      windowDays,
      since,
      activeUsers: 0,
      linkedActiveUsers: 0,
      linkedPercent: 0
    });
    return;
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .select("id, steam_id64")
    .in("id", activeUserIds);

  if (error) {
    throw new AppError(error.message, 500);
  }

  const rows = data || [];
  const linkedActiveUsers = rows.filter((row) => String(row.steam_id64 || "").trim()).length;
  const linkedPercent = Number(((linkedActiveUsers / rows.length) * 100).toFixed(2));

  res.json({
    ok: true,
    windowDays,
    since,
    activeUsers: rows.length,
    linkedActiveUsers,
    linkedPercent
  });
});

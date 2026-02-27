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

async function listUserActivityRows(sinceIso) {
  const sources = [
    { table: "inventories", userColumn: "user_id", timeColumn: "updated_at" },
    { table: "transactions", userColumn: "user_id", timeColumn: "executed_at" },
    { table: "price_alerts", userColumn: "user_id", timeColumn: "updated_at" },
    { table: "alert_events", userColumn: "user_id", timeColumn: "triggered_at" },
    {
      table: "ownership_alert_events",
      userColumn: "user_id",
      timeColumn: "created_at"
    },
    { table: "watchlists", userColumn: "user_id", timeColumn: "created_at" },
    {
      table: "public_portfolio_views",
      userColumn: "owner_user_id",
      timeColumn: "viewed_at"
    }
  ];

  const rows = [];

  for (const source of sources) {
    const { data, error } = await supabaseAdmin
      .from(source.table)
      .select(`${source.userColumn}, ${source.timeColumn}`)
      .gte(source.timeColumn, sinceIso)
      .limit(10000);

    if (error) {
      throw new AppError(error.message, 500);
    }

    for (const row of data || []) {
      const userId = String(row[source.userColumn] || "").trim();
      const ts = String(row[source.timeColumn] || "").trim();
      if (!userId || !ts) continue;
      rows.push({ userId, at: ts });
    }
  }

  return rows;
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

exports.getGrowthMetrics = asyncHandler(async (req, res) => {
  const windowDays = parseWindowDays(req.query.windowDays, 30);
  const now = new Date();
  const since = new Date(now.getTime() - windowDays * 24 * 60 * 60 * 1000);
  const sinceIso = since.toISOString();
  const retentionScanSince = new Date(
    now.getTime() - (windowDays + 30) * 24 * 60 * 60 * 1000
  ).toISOString();

  const [cohortRes, activityRows, viewsRes] = await Promise.all([
    supabaseAdmin
      .from("users")
      .select("id, created_at")
      .gte("created_at", sinceIso)
      .order("created_at", { ascending: false })
      .limit(10000),
    listUserActivityRows(retentionScanSince),
    supabaseAdmin
      .from("public_portfolio_views")
      .select("owner_user_id, referrer, viewed_at")
      .gte("viewed_at", sinceIso)
      .limit(10000)
  ]);

  if (cohortRes.error) {
    throw new AppError(cohortRes.error.message, 500);
  }
  if (viewsRes.error) {
    throw new AppError(viewsRes.error.message, 500);
  }

  const activityByUser = activityRows.reduce((acc, row) => {
    if (!acc[row.userId]) {
      acc[row.userId] = [];
    }
    acc[row.userId].push(new Date(row.at).getTime());
    return acc;
  }, {});

  const users = cohortRes.data || [];
  let d7Eligible = 0;
  let d7Retained = 0;
  let d30Eligible = 0;
  let d30Retained = 0;

  for (const user of users) {
    const createdAtMs = new Date(user.created_at).getTime();
    if (Number.isNaN(createdAtMs)) continue;

    const events = activityByUser[String(user.id)] || [];
    const hasActivityAfter = (offsetDays) =>
      events.some((ts) => Number(ts) >= createdAtMs + offsetDays * 24 * 60 * 60 * 1000);

    if (now.getTime() - createdAtMs >= 7 * 24 * 60 * 60 * 1000) {
      d7Eligible += 1;
      if (hasActivityAfter(7)) {
        d7Retained += 1;
      }
    }

    if (now.getTime() - createdAtMs >= 30 * 24 * 60 * 60 * 1000) {
      d30Eligible += 1;
      if (hasActivityAfter(30)) {
        d30Retained += 1;
      }
    }
  }

  const viewsRows = viewsRes.data || [];
  const shares = viewsRows.length;
  const referrals = viewsRows.filter((row) => String(row.referrer || "").trim()).length;

  res.json({
    ok: true,
    windowDays,
    since: sinceIso,
    newUsers: users.length,
    retention: {
      d7: {
        eligibleUsers: d7Eligible,
        retainedUsers: d7Retained,
        percent: d7Eligible ? Number(((d7Retained / d7Eligible) * 100).toFixed(2)) : 0
      },
      d30: {
        eligibleUsers: d30Eligible,
        retainedUsers: d30Retained,
        percent: d30Eligible ? Number(((d30Retained / d30Eligible) * 100).toFixed(2)) : 0
      }
    },
    growth: {
      shares,
      referrals
    }
  });
});

const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const watchlistRepo = require("../repositories/watchlistRepository");
const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const publicViewRepo = require("../repositories/publicPortfolioViewRepository");
const { resolveCurrency, convertUsdAmount } = require("./currencyService");

function validateSteamId64(steamId64) {
  const safeSteamId64 = String(steamId64 || "").trim();
  if (!/^\d{17}$/.test(safeSteamId64)) {
    throw new AppError("Invalid Steam ID", 400, "INVALID_STEAM_ID");
  }
  return safeSteamId64;
}

function normalizeLimit(limit, fallback = 20, max = 200) {
  const n = Number(limit);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

async function computePortfolioSummaries(userIds, currency) {
  const safeIds = Array.from(
    new Set(
      (Array.isArray(userIds) ? userIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!safeIds.length) {
    return {};
  }

  const holdings = await inventoryRepo.getHoldingsByUserIds(safeIds);
  const skinIds = Array.from(new Set(holdings.map((row) => Number(row.skin_id))));
  const latestPrices = await priceRepo.getLatestPricesBySkinIds(skinIds);

  const summary = {};
  for (const userId of safeIds) {
    summary[userId] = {
      totalValueUsd: 0,
      holdingsCount: 0,
      uniqueItems: 0
    };
  }

  for (const row of holdings) {
    const userId = String(row.user_id || "");
    if (!summary[userId]) continue;
    const skinId = Number(row.skin_id);
    const quantity = Number(row.quantity || 0);
    const price = Number(latestPrices[skinId] || 0);

    summary[userId].totalValueUsd += quantity * price;
    summary[userId].holdingsCount += quantity;
    summary[userId].uniqueItems += 1;
  }

  Object.values(summary).forEach((item) => {
    item.totalValueUsd = round2(item.totalValueUsd);
    item.totalValue = convertUsdAmount(item.totalValueUsd, currency);
    item.currency = currency;
  });

  return summary;
}

function mapFollowerCounts(rows = []) {
  return (Array.isArray(rows) ? rows : []).reduce((acc, row) => {
    const targetId = String(row.target_user_id || "").trim();
    if (!targetId) return acc;
    acc[targetId] = Number(acc[targetId] || 0) + 1;
    return acc;
  }, {});
}

async function resolveTargetUserBySteamId(steamId64, currentUserId) {
  const safeSteamId64 = validateSteamId64(steamId64);
  const targetUser = await userRepo.getBySteamId64(safeSteamId64);
  if (!targetUser || !targetUser.steam_id64) {
    throw new AppError("Steam user not found", 404, "STEAM_USER_NOT_FOUND");
  }
  if (String(targetUser.id) === String(currentUserId)) {
    throw new AppError("Cannot watch your own profile", 400, "WATCHLIST_SELF_FORBIDDEN");
  }
  if (targetUser.public_portfolio_enabled === false) {
    throw new AppError("Target public profile is hidden", 409, "PUBLIC_PORTFOLIO_DISABLED");
  }
  return targetUser;
}

exports.listWatchlist = async (userId, options = {}) => {
  const displayCurrency = resolveCurrency(options.currency);
  const rows = await watchlistRepo.listByUser(userId);
  const targetIds = rows.map((row) => String(row.target_user_id || "").trim()).filter(Boolean);

  if (!targetIds.length) {
    return {
      currency: displayCurrency,
      items: []
    };
  }

  const [users, portfolioSummaries, followerRows, viewStats] = await Promise.all([
    userRepo.getByIds(targetIds),
    computePortfolioSummaries(targetIds, displayCurrency),
    watchlistRepo.listByTargetIds(targetIds),
    publicViewRepo.countByOwnersSince(
      targetIds,
      new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()
    )
  ]);

  const userById = Object.fromEntries(users.map((user) => [String(user.id), user]));
  const followerCounts = mapFollowerCounts(followerRows);
  const watchedSinceByTarget = Object.fromEntries(
    rows.map((row) => [String(row.target_user_id || ""), row.created_at || null])
  );

  const items = targetIds
    .map((targetId) => {
      const user = userById[targetId];
      if (!user || !user.steam_id64 || user.public_portfolio_enabled === false) {
        return null;
      }

      const summary = portfolioSummaries[targetId] || {
        totalValueUsd: 0,
        totalValue: 0,
        holdingsCount: 0,
        uniqueItems: 0,
        currency: displayCurrency
      };
      const growth = viewStats[targetId] || { views: 0, referrals: 0 };

      return {
        steamId64: user.steam_id64,
        displayName: user.display_name || null,
        avatarUrl: user.avatar_url || null,
        watchedSince: watchedSinceByTarget[targetId] || null,
        followers: Number(followerCounts[targetId] || 0),
        totalValue: summary.totalValue,
        totalValueUsd: summary.totalValueUsd,
        holdingsCount: Number(summary.holdingsCount || 0),
        uniqueItems: Number(summary.uniqueItems || 0),
        views30d: Number(growth.views || 0),
        referrals30d: Number(growth.referrals || 0),
        currency: summary.currency || displayCurrency
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(b.totalValueUsd || 0) - Number(a.totalValueUsd || 0));

  return {
    currency: displayCurrency,
    items
  };
};

exports.addToWatchlist = async (userId, steamId64) => {
  const targetUser = await resolveTargetUserBySteamId(steamId64, userId);
  await watchlistRepo.add(userId, targetUser.id);

  return {
    steamId64: targetUser.steam_id64,
    displayName: targetUser.display_name || null,
    avatarUrl: targetUser.avatar_url || null
  };
};

exports.removeFromWatchlist = async (userId, steamId64) => {
  const safeSteamId64 = validateSteamId64(steamId64);
  const targetUser = await userRepo.getBySteamId64(safeSteamId64);
  if (!targetUser?.id) {
    return false;
  }

  return watchlistRepo.remove(userId, targetUser.id);
};

exports.getLeaderboard = async (userId, options = {}) => {
  const displayCurrency = resolveCurrency(options.currency);
  const scope = String(options.scope || "global").toLowerCase();
  const limit = normalizeLimit(options.limit, 20, 100);
  const poolLimit = Math.max(limit * 5, 200);

  const [watchRows, poolUsers] = await Promise.all([
    watchlistRepo.listByUser(userId),
    userRepo.listPublicSteamUsers(poolLimit)
  ]);

  const watchedIds = new Set(
    watchRows.map((row) => String(row.target_user_id || "").trim()).filter(Boolean)
  );
  const publicUsers = (poolUsers || []).filter((row) => row.public_portfolio_enabled !== false);

  let selectedUsers = publicUsers;
  if (scope === "watchlist") {
    selectedUsers = publicUsers.filter((row) => watchedIds.has(String(row.id)));
  }

  const selectedUserIds = selectedUsers.map((row) => String(row.id));
  const [summaries, followerRows, viewStats] = await Promise.all([
    computePortfolioSummaries(selectedUserIds, displayCurrency),
    watchlistRepo.listByTargetIds(selectedUserIds),
    publicViewRepo.countByOwnersSince(
      selectedUserIds,
      new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()
    )
  ]);

  const followerCounts = mapFollowerCounts(followerRows);

  const rows = selectedUsers
    .map((user) => {
      const summary = summaries[String(user.id)] || {
        totalValueUsd: 0,
        totalValue: 0,
        holdingsCount: 0,
        uniqueItems: 0,
        currency: displayCurrency
      };
      const growth = viewStats[String(user.id)] || { views: 0, referrals: 0 };

      return {
        steamId64: user.steam_id64,
        displayName: user.display_name || null,
        avatarUrl: user.avatar_url || null,
        totalValue: summary.totalValue,
        totalValueUsd: summary.totalValueUsd,
        holdingsCount: Number(summary.holdingsCount || 0),
        uniqueItems: Number(summary.uniqueItems || 0),
        followers: Number(followerCounts[String(user.id)] || 0),
        views30d: Number(growth.views || 0),
        referrals30d: Number(growth.referrals || 0),
        currency: summary.currency || displayCurrency,
        inWatchlist: watchedIds.has(String(user.id))
      };
    })
    .sort((a, b) => Number(b.totalValueUsd || 0) - Number(a.totalValueUsd || 0))
    .slice(0, limit)
    .map((row, idx) => ({
      rank: idx + 1,
      ...row
    }));

  return {
    scope: scope === "watchlist" ? "watchlist" : "global",
    currency: displayCurrency,
    items: rows
  };
};

exports.updatePublicSettings = async (userId, payload = {}) => {
  if (typeof payload?.publicPortfolioEnabled !== "boolean") {
    throw new AppError("publicPortfolioEnabled must be boolean", 400);
  }

  const updated = await userRepo.updatePreferencesById(userId, {
    publicPortfolioEnabled: payload.publicPortfolioEnabled
  });

  return {
    publicPortfolioEnabled: Boolean(updated?.public_portfolio_enabled)
  };
};

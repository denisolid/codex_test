const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const publicViewRepo = require("../repositories/publicPortfolioViewRepository");
const portfolioService = require("./portfolioService");

function validateSteamId64(steamId64) {
  const safeSteamId64 = String(steamId64 || "").trim();
  if (!/^\d{17}$/.test(safeSteamId64)) {
    throw new AppError("Invalid Steam ID", 400, "INVALID_STEAM_ID");
  }
  return safeSteamId64;
}

function sanitizePublicItems(items = []) {
  return (Array.isArray(items) ? items : []).map((item) => ({
    skinId: item.skinId,
    primarySteamItemId: item.primarySteamItemId || null,
    marketHashName: item.marketHashName,
    quantity: item.quantity,
    currentPrice: item.currentPrice,
    currency: item.currency,
    oneDayChangePercent: item.oneDayChangePercent,
    sevenDayChangePercent: item.sevenDayChangePercent,
    lineValue: item.lineValue,
    priceStatus: item.priceStatus,
    priceConfidenceLabel: item.priceConfidenceLabel,
    priceConfidenceScore: item.priceConfidenceScore
  }));
}

function sanitizePublicPortfolio(portfolio = {}) {
  return {
    totalValue: portfolio.totalValue,
    oneDayChangePercent: portfolio.oneDayChangePercent,
    sevenDayChangePercent: portfolio.sevenDayChangePercent,
    unpricedItemsCount: portfolio.unpricedItemsCount,
    staleItemsCount: portfolio.staleItemsCount,
    analytics: portfolio.analytics,
    managementSummary: portfolio.managementSummary,
    currency: portfolio.currency,
    items: sanitizePublicItems(portfolio.items)
  };
}

exports.getBySteamId64 = async (steamId64, options = {}) => {
  const safeSteamId64 = validateSteamId64(steamId64);
  const user = await userRepo.getBySteamId64(safeSteamId64);

  if (!user || !user.steam_id64) {
    throw new AppError("Public portfolio not found", 404, "PUBLIC_PORTFOLIO_NOT_FOUND");
  }

  if (user.public_portfolio_enabled === false) {
    throw new AppError("Public portfolio is hidden", 403, "PUBLIC_PORTFOLIO_DISABLED");
  }

  const [portfolio, history] = await Promise.all([
    portfolioService.getPortfolio(user.id, {
      currency: options.currency
    }),
    portfolioService.getPortfolioHistory(user.id, options.historyDays || 30, {
      currency: options.currency
    })
  ]);

  try {
    await publicViewRepo.recordView(user.id, options.referrer || null);
  } catch (_err) {
    // View tracking should not block page rendering.
  }

  return {
    profile: {
      steamId64: user.steam_id64,
      displayName: user.display_name || null,
      avatarUrl: user.avatar_url || null
    },
    portfolio: sanitizePublicPortfolio(portfolio),
    history
  };
};

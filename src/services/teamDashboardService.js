const portfolioService = require("./portfolioService");
const planService = require("./planService");
const watchlistRepo = require("../repositories/watchlistRepository");
const publicViewRepo = require("../repositories/publicPortfolioViewRepository");
const ownershipAlertRepo = require("../repositories/ownershipAlertRepository");

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

exports.getDashboard = async (userId, options = {}) => {
  const { user, planTier } = await planService.requireFeature(userId, "teamDashboard", {
    message: "Team dashboard is available on Team plan."
  });

  const [portfolio, history, followersRows, viewStatsByOwner, ownershipEvents] =
    await Promise.all([
      portfolioService.getPortfolio(userId, {
        currency: options.currency,
        planTier
      }),
      portfolioService.getPortfolioHistory(userId, 90, {
        currency: options.currency,
        maxHistoryDays: 365
      }),
      watchlistRepo.listByTargetIds([user.id]),
      publicViewRepo.countByOwnersSince(
        [user.id],
        new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString()
      ),
      ownershipAlertRepo.listByUser(userId, 50)
    ]);

  const items = Array.isArray(portfolio.items) ? portfolio.items : [];
  const sorted = [...items].sort((a, b) => Number(b.lineValue || 0) - Number(a.lineValue || 0));
  const top5Value = sorted.slice(0, 5).reduce((sum, item) => sum + Number(item.lineValue || 0), 0);
  const totalValue = Number(portfolio.totalValue || 0);
  const followerCount = Number((followersRows || []).length || 0);
  const growth = viewStatsByOwner[String(user.id)] || { views: 0, referrals: 0 };
  const unpricedRatio =
    items.length > 0 ? round2((Number(portfolio.unpricedItemsCount || 0) / items.length) * 100) : 0;
  const staleRatio =
    items.length > 0 ? round2((Number(portfolio.staleItemsCount || 0) / items.length) * 100) : 0;

  const ownershipBreakdown = (ownershipEvents || []).reduce(
    (acc, event) => {
      const key = String(event.change_type || "").toLowerCase();
      if (key in acc) {
        acc[key] += 1;
      }
      return acc;
    },
    { acquired: 0, disposed: 0, increased: 0, decreased: 0 }
  );

  return {
    planTier,
    currency: portfolio.currency || options.currency || "USD",
    summary: {
      totalValue: portfolio.totalValue,
      holdingsCount: items.length,
      top5WeightPercent: totalValue > 0 ? round2((top5Value / totalValue) * 100) : 0,
      unpricedRatioPercent: unpricedRatio,
      staleRatioPercent: staleRatio
    },
    creatorMetrics: {
      followers: followerCount,
      views30d: Number(growth.views || 0),
      referrals30d: Number(growth.referrals || 0),
      publicProfileEnabled: user.public_portfolio_enabled !== false
    },
    operations: {
      ownershipChangeEvents30d: Number((ownershipEvents || []).length),
      ownershipBreakdown
    },
    analytics: portfolio.analytics || null,
    advancedAnalytics: portfolio.advancedAnalytics || null,
    history90d: history.points || []
  };
};

const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");

const PLAN_ORDER = {
  free: 0,
  pro: 1,
  team: 2
};

const PLAN_ENTITLEMENTS = {
  free: {
    planTier: "free",
    maxAlerts: 5,
    maxHistoryDays: 30,
    maxBacktestDays: 0,
    maxCsvRows: 0,
    advancedAnalytics: false,
    csvExport: false,
    backtesting: false,
    teamDashboard: false
  },
  pro: {
    planTier: "pro",
    maxAlerts: 50,
    maxHistoryDays: 365,
    maxBacktestDays: 365,
    maxCsvRows: 10000,
    advancedAnalytics: true,
    csvExport: true,
    backtesting: true,
    teamDashboard: false
  },
  team: {
    planTier: "team",
    maxAlerts: 250,
    maxHistoryDays: 730,
    maxBacktestDays: 1095,
    maxCsvRows: 100000,
    advancedAnalytics: true,
    csvExport: true,
    backtesting: true,
    teamDashboard: true
  }
};

const TRADER_MODE_ENTITLEMENT_OVERRIDES = {
  maxAlerts: 25,
  maxHistoryDays: 365,
  maxBacktestDays: 365,
  maxCsvRows: 10000,
  advancedAnalytics: true,
  csvExport: true,
  backtesting: true
};

function normalizePlanTier(planTier) {
  const safe = String(planTier || "").trim().toLowerCase();
  if (safe in PLAN_ORDER) {
    return safe;
  }
  return "free";
}

function isAtLeast(planTier, requiredPlanTier) {
  return (
    Number(PLAN_ORDER[normalizePlanTier(planTier)] || 0) >=
    Number(PLAN_ORDER[normalizePlanTier(requiredPlanTier)] || 0)
  );
}

function featureEnabled(entitlements, featureKey) {
  return Boolean(entitlements?.[featureKey]);
}

exports.normalizePlanTier = normalizePlanTier;
exports.isAtLeast = isAtLeast;
exports.isPaidPlan = (planTier) => normalizePlanTier(planTier) !== "free";

exports.getEntitlements = (planTier, options = {}) => {
  const safe = normalizePlanTier(planTier);
  const traderModeUnlocked = Boolean(options.traderModeUnlocked);
  const base = { ...(PLAN_ENTITLEMENTS[safe] || PLAN_ENTITLEMENTS.free) };

  if (traderModeUnlocked) {
    base.maxAlerts = Math.max(
      Number(base.maxAlerts || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.maxAlerts)
    );
    base.maxHistoryDays = Math.max(
      Number(base.maxHistoryDays || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.maxHistoryDays)
    );
    base.maxBacktestDays = Math.max(
      Number(base.maxBacktestDays || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.maxBacktestDays)
    );
    base.maxCsvRows = Math.max(
      Number(base.maxCsvRows || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.maxCsvRows)
    );
    base.advancedAnalytics =
      Boolean(base.advancedAnalytics) ||
      Boolean(TRADER_MODE_ENTITLEMENT_OVERRIDES.advancedAnalytics);
    base.csvExport =
      Boolean(base.csvExport) || Boolean(TRADER_MODE_ENTITLEMENT_OVERRIDES.csvExport);
    base.backtesting =
      Boolean(base.backtesting) || Boolean(TRADER_MODE_ENTITLEMENT_OVERRIDES.backtesting);
  }

  base.traderModeUnlocked = traderModeUnlocked;
  return base;
};

exports.getUserPlanProfile = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const planTier = normalizePlanTier(user.plan_tier);
  const traderModeUnlocked = Boolean(user.trader_mode_unlocked);
  return {
    user,
    planTier,
    traderModeUnlocked,
    traderModeUnlockedAt: user.trader_mode_unlocked_at || null,
    traderModeUnlockSource: user.trader_mode_unlock_source || null,
    entitlements: exports.getEntitlements(planTier, { traderModeUnlocked })
  };
};

exports.requireFeature = async (userId, featureKey, options = {}) => {
  const { user, planTier, entitlements } = await exports.getUserPlanProfile(userId);
  if (!featureEnabled(entitlements, featureKey)) {
    throw new AppError(
      options.message || "This feature requires a higher plan tier.",
      402,
      "PLAN_UPGRADE_REQUIRED"
    );
  }

  return { user, planTier, entitlements };
};

exports.requireMinTier = async (userId, requiredPlanTier, options = {}) => {
  const { user, planTier, entitlements } = await exports.getUserPlanProfile(userId);
  if (!isAtLeast(planTier, requiredPlanTier)) {
    throw new AppError(
      options.message || `This feature requires ${normalizePlanTier(requiredPlanTier)} plan.`,
      402,
      "PLAN_UPGRADE_REQUIRED"
    );
  }

  return { user, planTier, entitlements };
};

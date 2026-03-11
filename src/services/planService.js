const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const { testSubscriptionSwitcherEnabled } = require("../config/env");

const PLAN_ALIASES = Object.freeze({
  free: "free",
  pro: "full_access",
  team: "full_access",
  full_access: "full_access",
  api_advanced: "full_access"
});

const PLAN_ORDER = Object.freeze({
  free: 0,
  full_access: 1
});

const PLAN_ENTITLEMENTS = Object.freeze({
  free: Object.freeze({
    planTier: "free",
    opportunitiesDailyLimit: 3,
    alertsLimit: 3,
    maxAlerts: 3,
    scannerRefreshIntervalMinutes: 720,
    historyDaysLimit: 7,
    maxHistoryDays: 7,
    visibleFeedLimit: 10,
    advancedFilters: false,
    delayedSignals: true,
    signalDelayMinutes: 15,
    compareView: "limited",
    compareViewMaxItems: 3,
    portfolioInsights: "basic",
    advancedAnalytics: false,
    csvExport: false,
    maxCsvRows: 0,
    backtesting: false,
    maxBacktestDays: 0,
    teamDashboard: false,
    fullGlobalScanner: false,
    fullOpportunitiesFeed: false,
    premiumCategoryAccess: false,
    knivesGlovesAccess: false,
    scannerCategories: Object.freeze(["weapon_skin", "case", "sticker_capsule"]),
    scannerCategoryAccessNote: "Knives and gloves are preview-only on Free.",
    exportApiReady: false,
    webhooksReady: false,
    automationReady: false
  }),
  full_access: Object.freeze({
    planTier: "full_access",
    opportunitiesDailyLimit: 500,
    alertsLimit: 25,
    maxAlerts: 25,
    scannerRefreshIntervalMinutes: 30,
    historyDaysLimit: 90,
    maxHistoryDays: 90,
    visibleFeedLimit: 500,
    advancedFilters: true,
    delayedSignals: false,
    signalDelayMinutes: 0,
    compareView: "full",
    compareViewMaxItems: 200,
    portfolioInsights: "full",
    advancedAnalytics: true,
    csvExport: true,
    maxCsvRows: 100000,
    backtesting: true,
    maxBacktestDays: 90,
    teamDashboard: true,
    fullGlobalScanner: true,
    fullOpportunitiesFeed: true,
    premiumCategoryAccess: true,
    knivesGlovesAccess: true,
    scannerCategories: Object.freeze([
      "weapon_skin",
      "case",
      "sticker_capsule",
      "knife",
      "glove"
    ]),
    scannerCategoryAccessNote: "All scanner categories unlocked, including knives and gloves.",
    exportApiReady: false,
    webhooksReady: false,
    automationReady: false
  })
});

const TRADER_MODE_ENTITLEMENT_OVERRIDES = Object.freeze({
  alertsLimit: 25,
  maxAlerts: 25,
  historyDaysLimit: 90,
  maxHistoryDays: 90,
  maxBacktestDays: 90,
  maxCsvRows: 10000,
  advancedAnalytics: true,
  csvExport: true,
  backtesting: true,
  portfolioInsights: "full"
});

function normalizePlanTier(planTier) {
  const safe = String(planTier || "")
    .trim()
    .toLowerCase();
  if (safe in PLAN_ALIASES) {
    return PLAN_ALIASES[safe];
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
  const value = entitlements?.[featureKey];
  if (typeof value === "boolean") {
    return value;
  }
  return Number(value || 0) > 0;
}

function clonePlanEntitlements(planTier) {
  const safeTier = normalizePlanTier(planTier);
  return {
    ...(PLAN_ENTITLEMENTS[safeTier] || PLAN_ENTITLEMENTS.free)
  };
}

exports.PLAN_TIERS = Object.keys(PLAN_ORDER);
exports.normalizePlanTier = normalizePlanTier;
exports.isAtLeast = isAtLeast;
exports.isPaidPlan = (planTier) => normalizePlanTier(planTier) !== "free";
exports.isTestSubscriptionSwitcherEnabled = () => Boolean(testSubscriptionSwitcherEnabled);

exports.assertTestSubscriptionSwitcherEnabled = () => {
  if (exports.isTestSubscriptionSwitcherEnabled()) {
    return;
  }

  throw new AppError(
    "Temporary subscription switching is disabled for this environment.",
    403,
    "SUBSCRIPTION_SWITCHER_DISABLED"
  );
};

exports.getEntitlements = (planTier, options = {}) => {
  const safe = normalizePlanTier(planTier);
  const traderModeUnlocked = Boolean(options.traderModeUnlocked);
  const base = clonePlanEntitlements(safe);

  if (traderModeUnlocked) {
    base.alertsLimit = Math.max(
      Number(base.alertsLimit || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.alertsLimit)
    );
    base.maxAlerts = Math.max(
      Number(base.maxAlerts || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.maxAlerts)
    );
    base.historyDaysLimit = Math.max(
      Number(base.historyDaysLimit || 0),
      Number(TRADER_MODE_ENTITLEMENT_OVERRIDES.historyDaysLimit)
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
    base.portfolioInsights =
      base.portfolioInsights === "full"
        ? "full"
        : TRADER_MODE_ENTITLEMENT_OVERRIDES.portfolioInsights;
  }

  base.traderModeUnlocked = traderModeUnlocked;
  return base;
};

exports.getUserPlanProfile = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const planTier = normalizePlanTier(user.plan_tier || user.plan);
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
      options.message || "This feature requires a higher access level.",
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

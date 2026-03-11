const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const { testSubscriptionSwitcherEnabled } = require("../config/env");
const {
  PLAN_CONFIG,
  PLAN_ALIASES,
  PLAN_ORDER,
  ACTIVE_PLAN_TIERS,
  ALL_PLAN_TIERS,
  TRADER_MODE_ENTITLEMENT_OVERRIDES,
} = require("../config/planConfig");

const CATEGORY_ALIASES = Object.freeze({
  skin: "weapon_skin",
  skins: "weapon_skin",
  weapon_skin: "weapon_skin",
  case: "case",
  cases: "case",
  capsule: "sticker_capsule",
  capsules: "sticker_capsule",
  sticker_capsule: "sticker_capsule",
  sticker_capsules: "sticker_capsule",
  knife: "knife",
  knives: "knife",
  glove: "glove",
  gloves: "glove",
});

const FEATURE_KEY_ALIASES = Object.freeze({
  opportunities_daily_limit: ["opportunities_daily_limit", "opportunitiesDailyLimit"],
  alerts_limit: ["alerts_limit", "alertsLimit", "maxAlerts"],
  minimum_alert_cooldown_minutes: [
    "minimum_alert_cooldown_minutes",
    "minimumAlertCooldownMinutes",
  ],
  scanner_refresh_interval_minutes: [
    "scanner_refresh_interval_minutes",
    "scannerRefreshIntervalMinutes",
  ],
  history_days_limit: ["history_days_limit", "historyDaysLimit", "maxHistoryDays"],
  visible_feed_limit: ["visible_feed_limit", "visibleFeedLimit"],
  delayed_signals: ["delayed_signals", "delayedSignals"],
  signal_delay_minutes: ["signal_delay_minutes", "signalDelayMinutes"],
  advanced_filters: ["advanced_filters", "advancedFilters"],
  compare_view: ["compare_view", "compareView"],
  compare_view_max_items: ["compare_view_max_items", "compareViewMaxItems"],
  portfolio_insights: ["portfolio_insights", "portfolioInsights"],
  scanner_categories: ["scanner_categories", "scannerCategories"],
  knives_gloves_access: [
    "knives_gloves_access",
    "knivesGlovesAccess",
    "premium_category_access",
    "premiumCategoryAccess",
  ],
  premium_category_access: ["premium_category_access", "premiumCategoryAccess"],
  premium_rare_item_intelligence: [
    "premium_rare_item_intelligence",
    "premiumRareItemIntelligence",
  ],
  automation: ["automation", "automationReady"],
  export_api_ready: ["export_api_ready", "exportApiReady"],
  webhooks_ready: ["webhooks_ready", "webhooksReady"],
  advanced_analytics: ["advanced_analytics", "advancedAnalytics"],
  csv_export: ["csv_export", "csvExport"],
  max_csv_rows: ["max_csv_rows", "maxCsvRows"],
  backtesting: ["backtesting"],
  max_backtest_days: ["max_backtest_days", "maxBacktestDays"],
  team_dashboard: ["team_dashboard", "teamDashboard"],
  full_global_scanner: ["full_global_scanner", "fullGlobalScanner"],
  full_opportunities_feed: ["full_opportunities_feed", "fullOpportunitiesFeed"],
});

function normalizePlanTier(planTier) {
  const safe = String(planTier || "")
    .trim()
    .toLowerCase();
  if (safe in PLAN_ALIASES) {
    return PLAN_ALIASES[safe];
  }
  if (safe in PLAN_CONFIG) {
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

function cloneBasePlanConfig(planTier) {
  const safeTier = normalizePlanTier(planTier);
  const raw = PLAN_CONFIG[safeTier] || PLAN_CONFIG.free;
  return {
    ...raw,
    scanner_categories: Array.isArray(raw.scanner_categories)
      ? [...raw.scanner_categories]
      : [],
  };
}

function toSafeNumber(value, fallback = 0, min = -Infinity) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(parsed, min);
}

function withLegacyAliases(config = {}) {
  const scannerCategories = Array.isArray(config.scanner_categories)
    ? [...config.scanner_categories]
    : [];
  const alertsLimit = toSafeNumber(config.alerts_limit, 0, 0);
  const historyDaysLimit = toSafeNumber(config.history_days_limit, 0, 0);
  const maxBacktestDays = toSafeNumber(config.max_backtest_days, 0, 0);
  const maxCsvRows = toSafeNumber(config.max_csv_rows, 0, 0);
  const compareViewMaxItems = toSafeNumber(config.compare_view_max_items, 0, 0);
  const minimumAlertCooldownMinutes = toSafeNumber(
    config.minimum_alert_cooldown_minutes,
    0,
    0
  );

  return {
    ...config,
    planTier: String(config.plan_tier || "free"),
    opportunitiesDailyLimit: toSafeNumber(config.opportunities_daily_limit, 0, 0),
    alertsLimit,
    maxAlerts: alertsLimit,
    minimumAlertCooldownMinutes,
    scannerRefreshIntervalMinutes: toSafeNumber(
      config.scanner_refresh_interval_minutes,
      1,
      1
    ),
    historyDaysLimit,
    maxHistoryDays: historyDaysLimit,
    visibleFeedLimit: toSafeNumber(config.visible_feed_limit, 1, 1),
    delayedSignals: Boolean(config.delayed_signals),
    signalDelayMinutes: toSafeNumber(config.signal_delay_minutes, 0, 0),
    advancedFilters: Boolean(config.advanced_filters),
    compareView: String(config.compare_view || "limited"),
    compareViewMaxItems,
    portfolioInsights: String(config.portfolio_insights || "basic"),
    scannerCategories,
    knivesGlovesAccess: Boolean(
      config.knives_gloves_access || config.premium_category_access
    ),
    premiumCategoryAccess: Boolean(
      config.premium_category_access || config.knives_gloves_access
    ),
    premiumRareItemIntelligence: Boolean(config.premium_rare_item_intelligence),
    automationReady: Boolean(config.automation),
    exportApiReady: Boolean(config.export_api_ready),
    webhooksReady: Boolean(config.webhooks_ready),
    advancedAnalytics: Boolean(config.advanced_analytics),
    csvExport: Boolean(config.csv_export),
    maxCsvRows,
    backtesting: Boolean(config.backtesting),
    maxBacktestDays,
    teamDashboard: Boolean(config.team_dashboard),
    fullGlobalScanner: Boolean(config.full_global_scanner),
    fullOpportunitiesFeed: Boolean(config.full_opportunities_feed),
    scannerCategoryAccessNote: String(config.scanner_category_access_note || "").trim(),
  };
}

function applyTraderModeOverrides(config = {}) {
  const next = { ...config };
  next.alerts_limit = Math.max(
    toSafeNumber(next.alerts_limit, 0, 0),
    toSafeNumber(TRADER_MODE_ENTITLEMENT_OVERRIDES.alerts_limit, 0, 0)
  );
  next.history_days_limit = Math.max(
    toSafeNumber(next.history_days_limit, 0, 0),
    toSafeNumber(TRADER_MODE_ENTITLEMENT_OVERRIDES.history_days_limit, 0, 0)
  );
  next.max_backtest_days = Math.max(
    toSafeNumber(next.max_backtest_days, 0, 0),
    toSafeNumber(TRADER_MODE_ENTITLEMENT_OVERRIDES.max_backtest_days, 0, 0)
  );
  next.max_csv_rows = Math.max(
    toSafeNumber(next.max_csv_rows, 0, 0),
    toSafeNumber(TRADER_MODE_ENTITLEMENT_OVERRIDES.max_csv_rows, 0, 0)
  );
  next.advanced_analytics =
    Boolean(next.advanced_analytics) ||
    Boolean(TRADER_MODE_ENTITLEMENT_OVERRIDES.advanced_analytics);
  next.csv_export =
    Boolean(next.csv_export) || Boolean(TRADER_MODE_ENTITLEMENT_OVERRIDES.csv_export);
  next.backtesting =
    Boolean(next.backtesting) || Boolean(TRADER_MODE_ENTITLEMENT_OVERRIDES.backtesting);
  next.portfolio_insights =
    String(next.portfolio_insights || "").trim().toLowerCase() === "full"
      ? "full"
      : TRADER_MODE_ENTITLEMENT_OVERRIDES.portfolio_insights;
  return next;
}

function extractPlanTierFromInput(input) {
  if (input == null) return "";
  if (typeof input === "string") return input;
  if (typeof input === "object") {
    return String(input.planTier || input.plan_tier || "").trim();
  }
  return "";
}

function applyFeatureAliasOverrides(config = {}, input = {}) {
  const next = { ...config };
  for (const aliases of Object.values(FEATURE_KEY_ALIASES)) {
    const overrideKey = aliases.find((key) =>
      Object.prototype.hasOwnProperty.call(input, key)
    );
    if (!overrideKey) continue;
    const overrideValue = input[overrideKey];
    for (const key of aliases) {
      next[key] = overrideValue;
    }
  }
  return next;
}

function syncScannerCategoryAliases(config = {}, input = {}) {
  const next = { ...config };
  if (Array.isArray(input.scanner_categories)) {
    next.scanner_categories = [...input.scanner_categories];
  }
  if (Array.isArray(input.scannerCategories)) {
    next.scannerCategories = [...input.scannerCategories];
  }
  if (!Array.isArray(next.scanner_categories) && Array.isArray(next.scannerCategories)) {
    next.scanner_categories = [...next.scannerCategories];
  }
  if (!Array.isArray(next.scannerCategories) && Array.isArray(next.scanner_categories)) {
    next.scannerCategories = [...next.scanner_categories];
  }
  return next;
}

function getPlanConfig(planTier, options = {}) {
  if (planTier && typeof planTier === "object" && !Array.isArray(planTier)) {
    const inferredTier = normalizePlanTier(
      extractPlanTierFromInput(planTier) || options.planTier
    );
    const base = getPlanConfig(inferredTier, {
      traderModeUnlocked:
        options.traderModeUnlocked == null
          ? Boolean(planTier.traderModeUnlocked)
          : options.traderModeUnlocked,
    });
    const merged = applyFeatureAliasOverrides(
      {
        ...base,
        ...planTier,
      },
      planTier
    );
    return syncScannerCategoryAliases(merged, planTier);
  }

  const safeTier = normalizePlanTier(planTier);
  const traderModeUnlocked = Boolean(options.traderModeUnlocked);
  const base = cloneBasePlanConfig(safeTier);
  const withOverrides = traderModeUnlocked ? applyTraderModeOverrides(base) : base;
  const normalized = withLegacyAliases(withOverrides);
  normalized.traderModeUnlocked = traderModeUnlocked;
  return normalized;
}

function resolvePlanConfig(input, options = {}) {
  if (input && typeof input === "object" && !Array.isArray(input)) {
    return getPlanConfig(input, options);
  }

  const safeTier = normalizePlanTier(input || options.planTier || "free");
  return getPlanConfig(safeTier, options);
}

function normalizeFeatureKey(featureKey) {
  const raw = String(featureKey || "").trim();
  if (!raw) return "";
  if (FEATURE_KEY_ALIASES[raw]) return raw;
  return raw.replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`);
}

function readFeatureValue(config = {}, featureKey = "") {
  const normalized = normalizeFeatureKey(featureKey);
  if (!normalized) return undefined;
  const aliases = FEATURE_KEY_ALIASES[normalized] || [normalized];
  for (const key of aliases) {
    if (Object.prototype.hasOwnProperty.call(config, key)) {
      return config[key];
    }
  }
  return undefined;
}

function hasFeatureAccess(plan, featureKey, options = {}) {
  const config = resolvePlanConfig(plan, options);
  const value = readFeatureValue(config, featureKey);
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) && value > 0;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (!normalized) return false;
    return !["no", "none", "false", "off", "disabled", "0", "locked"].includes(normalized);
  }
  return Boolean(value);
}

function normalizeCategory(category) {
  const raw = String(category || "")
    .trim()
    .toLowerCase();
  if (!raw) return "weapon_skin";
  return CATEGORY_ALIASES[raw] || raw;
}

function canAccessCategory(plan, category, options = {}) {
  const config = resolvePlanConfig(plan, options);
  const normalizedCategory = normalizeCategory(category);
  const categoriesRaw = readFeatureValue(config, "scanner_categories");
  const categories = Array.isArray(categoriesRaw) ? categoriesRaw : [];
  if (categories.includes(normalizedCategory)) return true;
  if (
    normalizedCategory === "knife" ||
    normalizedCategory === "glove"
  ) {
    return hasFeatureAccess(config, "knives_gloves_access");
  }
  return false;
}

function canUseAdvancedFilters(plan, options = {}) {
  return hasFeatureAccess(plan, "advanced_filters", options);
}

function canAccessKnivesAndGloves(plan, options = {}) {
  const direct = hasFeatureAccess(plan, "knives_gloves_access", options);
  if (direct) return true;
  if (hasFeatureAccess(plan, "premium_category_access", options)) return true;
  return (
    canAccessCategory(plan, "knife", options) && canAccessCategory(plan, "glove", options)
  );
}

function canCreateAlert(plan, currentCount = 0, options = {}) {
  const config = resolvePlanConfig(plan, options);
  const alertsLimit = Math.max(
    toSafeNumber(readFeatureValue(config, "alerts_limit"), 0, 0),
    0
  );
  const minimumCooldownMinutes = Math.max(
    toSafeNumber(readFeatureValue(config, "minimum_alert_cooldown_minutes"), 0, 0),
    0
  );
  const count = Math.max(toSafeNumber(currentCount, 0, 0), 0);
  const requestedCooldown =
    options.cooldownMinutes == null ? null : toSafeNumber(options.cooldownMinutes, -1);
  const limitBlocked = count >= alertsLimit;
  const cooldownBlocked =
    requestedCooldown != null &&
    Number.isFinite(requestedCooldown) &&
    requestedCooldown >= 0 &&
    requestedCooldown < minimumCooldownMinutes;
  return {
    allowed: !limitBlocked && !cooldownBlocked,
    limitBlocked,
    cooldownBlocked,
    alertsLimit,
    currentCount: count,
    remaining: Math.max(alertsLimit - count, 0),
    minimumCooldownMinutes,
  };
}

function parseTimestampMs(value) {
  if (value == null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function canRefreshScanner(plan, lastRefreshAt, options = {}) {
  const config = resolvePlanConfig(plan, options);
  const nowMs = toSafeNumber(options.nowMs, Date.now());
  const intervalMinutes = Math.max(
    toSafeNumber(readFeatureValue(config, "scanner_refresh_interval_minutes"), 1, 1),
    1
  );
  const intervalMs = intervalMinutes * 60 * 1000;
  const lastRefreshMs = parseTimestampMs(lastRefreshAt);
  if (lastRefreshMs == null) {
    return {
      allowed: true,
      intervalMinutes,
      retryAfterMs: 0,
      retryAfterMinutes: 0,
    };
  }
  const elapsedMs = Math.max(nowMs - lastRefreshMs, 0);
  const retryAfterMs = Math.max(intervalMs - elapsedMs, 0);
  return {
    allowed: retryAfterMs <= 0,
    intervalMinutes,
    retryAfterMs,
    retryAfterMinutes: Math.max(Math.ceil(retryAfterMs / 60000), 0),
  };
}

function canViewHistory(plan, itemAgeDays = 0, options = {}) {
  const config = resolvePlanConfig(plan, options);
  const maxHistoryDays = Math.max(
    toSafeNumber(readFeatureValue(config, "history_days_limit"), 0, 0),
    0
  );
  const ageDays = Math.max(toSafeNumber(itemAgeDays, 0, 0), 0);
  return {
    allowed: ageDays <= maxHistoryDays,
    maxHistoryDays,
    requestedDays: ageDays,
  };
}

function canViewOpportunity(plan, usageState = {}, opportunityCategory = "", options = {}) {
  const config = resolvePlanConfig(plan, options);
  const visibleFeedLimit = Math.max(
    toSafeNumber(readFeatureValue(config, "visible_feed_limit"), 1, 1),
    1
  );
  const dailyLimit = Math.max(
    toSafeNumber(readFeatureValue(config, "opportunities_daily_limit"), visibleFeedLimit, 1),
    1
  );
  const index = toSafeNumber(
    usageState.position ?? usageState.index ?? usageState.rowIndex,
    0
  );
  const dailyUsed = toSafeNumber(
    usageState.dailyUsed ?? usageState.opportunitiesUsedToday ?? usageState.usedToday,
    0
  );
  const categoryAllowed = canAccessCategory(config, opportunityCategory || "weapon_skin");
  const visibleAllowed = index < visibleFeedLimit;
  const dailyAllowed = dailyUsed < dailyLimit;
  return {
    allowed: categoryAllowed && visibleAllowed && dailyAllowed,
    categoryAllowed,
    visibleAllowed,
    dailyAllowed,
    visibleFeedLimit,
    opportunitiesDailyLimit: dailyLimit,
  };
}

exports.PLAN_TIERS = [...ACTIVE_PLAN_TIERS];
exports.ALL_PLAN_TIERS = [...ALL_PLAN_TIERS];
exports.normalizePlanTier = normalizePlanTier;
exports.isAtLeast = isAtLeast;
exports.isPaidPlan = (planTier) => normalizePlanTier(planTier) !== "free";
exports.isTestSubscriptionSwitcherEnabled = () => Boolean(testSubscriptionSwitcherEnabled);
exports.getPlanConfig = getPlanConfig;
exports.hasFeatureAccess = hasFeatureAccess;
exports.canAccessCategory = canAccessCategory;
exports.canUseAdvancedFilters = canUseAdvancedFilters;
exports.canAccessKnivesAndGloves = canAccessKnivesAndGloves;
exports.canCreateAlert = canCreateAlert;
exports.canRefreshScanner = canRefreshScanner;
exports.canViewHistory = canViewHistory;
exports.canViewOpportunity = canViewOpportunity;

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

exports.getEntitlements = (planTier, options = {}) =>
  getPlanConfig(planTier, options);

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
    entitlements: exports.getEntitlements(planTier, { traderModeUnlocked }),
  };
};

exports.requireFeature = async (userId, featureKey, options = {}) => {
  const { user, planTier, entitlements } = await exports.getUserPlanProfile(userId);
  if (!hasFeatureAccess(entitlements, featureKey)) {
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

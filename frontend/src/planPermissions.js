const PLAN_ALIASES = Object.freeze({
  free: "free",
  pro: "full_access",
  team: "full_access",
  full_access: "full_access",
  api_advanced: "full_access",
  alpha_access: "alpha_access",
});

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

const PLAN_CONFIG = Object.freeze({
  free: Object.freeze({
    planTier: "free",
    opportunitiesDailyLimit: 3,
    alertsLimit: 3,
    minimumAlertCooldownMinutes: 30,
    scannerRefreshIntervalMinutes: 720,
    historyDaysLimit: 7,
    visibleFeedLimit: 10,
    delayedSignals: true,
    signalDelayMinutes: 15,
    advancedFilters: false,
    compareView: "limited",
    compareViewMaxItems: 3,
    portfolioInsights: "basic",
    scannerCategories: Object.freeze(["weapon_skin", "case", "sticker_capsule"]),
    knivesGlovesAccess: false,
    premiumRareItemIntelligence: false,
    automationReady: false,
    exportApiReady: false,
    webhooksReady: false,
    fullGlobalScanner: false,
    fullOpportunitiesFeed: false,
    premiumCategoryAccess: false,
    teamDashboard: false,
  }),
  full_access: Object.freeze({
    planTier: "full_access",
    opportunitiesDailyLimit: 500,
    alertsLimit: 25,
    minimumAlertCooldownMinutes: 0,
    scannerRefreshIntervalMinutes: 0,
    historyDaysLimit: 90,
    visibleFeedLimit: 500,
    delayedSignals: false,
    signalDelayMinutes: 0,
    advancedFilters: true,
    compareView: "full",
    compareViewMaxItems: 200,
    portfolioInsights: "full",
    scannerCategories: Object.freeze([
      "weapon_skin",
      "case",
      "sticker_capsule",
      "knife",
      "glove",
    ]),
    knivesGlovesAccess: true,
    premiumRareItemIntelligence: false,
    automationReady: false,
    exportApiReady: false,
    webhooksReady: false,
    fullGlobalScanner: true,
    fullOpportunitiesFeed: true,
    premiumCategoryAccess: true,
    teamDashboard: true,
  }),
  alpha_access: Object.freeze({
    planTier: "alpha_access",
    opportunitiesDailyLimit: 5000,
    alertsLimit: 100,
    minimumAlertCooldownMinutes: 0,
    scannerRefreshIntervalMinutes: 5,
    historyDaysLimit: 365,
    visibleFeedLimit: 5000,
    delayedSignals: false,
    signalDelayMinutes: 0,
    advancedFilters: true,
    compareView: "advanced",
    compareViewMaxItems: 500,
    portfolioInsights: "premium",
    scannerCategories: Object.freeze([
      "weapon_skin",
      "case",
      "sticker_capsule",
      "knife",
      "glove",
    ]),
    knivesGlovesAccess: true,
    premiumRareItemIntelligence: true,
    automationReady: true,
    exportApiReady: true,
    webhooksReady: true,
    fullGlobalScanner: true,
    fullOpportunitiesFeed: true,
    premiumCategoryAccess: true,
    teamDashboard: true,
  }),
});

const ACCOUNT_PLAN_LIMITS = Object.freeze({
  free: Object.freeze({
    opportunitiesDailyLimit: "3 opportunities/day",
    alertsLimit: "3 active alerts",
    scannerRefresh: "Refresh every 12 hours",
    historyDaysLimit: "7 days history",
    visibleFeedLimit: "Top 10 feed items",
    advancedFilters: "Advanced filters locked",
    delayedSignals: "Signals delayed by 15 minutes",
    compareView: "Compare view limited",
    portfolioInsights: "Portfolio insights: basic",
    scannerCategories: "Skins, Cases, Capsules",
    knivesGloves: "Locked preview rows only",
  }),
  full_access: Object.freeze({
    opportunitiesDailyLimit: "High/unlimited opportunities",
    alertsLimit: "25 active alerts",
    scannerRefresh: "No manual refresh cooldown",
    historyDaysLimit: "90 days history",
    visibleFeedLimit: "Full opportunities feed",
    advancedFilters: "Advanced filters enabled",
    delayedSignals: "Real-time signals (no delay)",
    compareView: "Compare view full",
    portfolioInsights: "Portfolio insights: full",
    scannerCategories: "All categories unlocked",
    knivesGloves: "Unlocked (full feed + compare + inspect)",
  }),
  alpha_access: Object.freeze({
    opportunitiesDailyLimit: "Premium",
    alertsLimit: "Premium",
    scannerRefresh: "Priority",
    historyDaysLimit: "Premium",
    visibleFeedLimit: "Premium",
    advancedFilters: "Advanced",
    delayedSignals: "Advanced real-time signals",
    compareView: "Advanced",
    portfolioInsights: "Premium",
    scannerCategories: "All + high-value logic",
    knivesGloves: "Advanced intelligence",
  }),
});

function normalizeNumber(value, fallback = 0, min = -Infinity) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(parsed, min);
}

function normalizeCategory(category) {
  const raw = String(category || "")
    .trim()
    .toLowerCase();
  if (!raw) return "weapon_skin";
  return CATEGORY_ALIASES[raw] || raw;
}

function normalizeFeatureKey(featureKey) {
  const raw = String(featureKey || "").trim();
  if (!raw) return "";
  return raw.replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`);
}

function readFeatureValue(config = {}, featureKey = "") {
  const normalized = normalizeFeatureKey(featureKey);
  if (!normalized) return undefined;
  if (Object.prototype.hasOwnProperty.call(config, normalized)) {
    return config[normalized];
  }
  if (Object.prototype.hasOwnProperty.call(config, featureKey)) {
    return config[featureKey];
  }
  const camel = normalized.replace(/_([a-z])/g, (_m, c) => c.toUpperCase());
  if (Object.prototype.hasOwnProperty.call(config, camel)) {
    return config[camel];
  }
  return undefined;
}

export function normalizePlanTier(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return "free";
  if (Object.prototype.hasOwnProperty.call(PLAN_ALIASES, raw)) {
    return PLAN_ALIASES[raw];
  }
  if (Object.prototype.hasOwnProperty.call(PLAN_CONFIG, raw)) {
    return raw;
  }
  return "free";
}

export function getPlanConfig(plan) {
  const safePlan = normalizePlanTier(plan);
  const base = PLAN_CONFIG[safePlan] || PLAN_CONFIG.free;
  return {
    ...base,
    scannerCategories: Array.isArray(base.scannerCategories)
      ? [...base.scannerCategories]
      : [],
  };
}

export function hasFeatureAccess(plan, featureKey) {
  const config = typeof plan === "object" ? plan : getPlanConfig(plan);
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

export function canAccessCategory(plan, category) {
  const config = typeof plan === "object" ? plan : getPlanConfig(plan);
  const normalized = normalizeCategory(category);
  const categories = Array.isArray(config?.scannerCategories) ? config.scannerCategories : [];
  return categories.includes(normalized);
}

export function canUseAdvancedFilters(plan) {
  return hasFeatureAccess(plan, "advancedFilters");
}

export function canAccessKnivesAndGloves(plan) {
  return (
    hasFeatureAccess(plan, "knivesGlovesAccess") ||
    (canAccessCategory(plan, "knife") && canAccessCategory(plan, "glove"))
  );
}

export function canCreateAlert(plan, currentCount = 0, options = {}) {
  const config = typeof plan === "object" ? plan : getPlanConfig(plan);
  const alertsLimit = Math.max(normalizeNumber(config.alertsLimit, 0, 0), 0);
  const minimumCooldownMinutes = Math.max(
    normalizeNumber(config.minimumAlertCooldownMinutes, 0, 0),
    0,
  );
  const requestedCooldown =
    options.cooldownMinutes == null ? null : normalizeNumber(options.cooldownMinutes, -1);
  const count = Math.max(normalizeNumber(currentCount, 0, 0), 0);
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
    minimumCooldownMinutes,
  };
}

export function canRefreshScanner(plan, lastRefreshAt, nowMs = Date.now()) {
  const config = typeof plan === "object" ? plan : getPlanConfig(plan);
  const intervalMinutes = Math.max(normalizeNumber(config.scannerRefreshIntervalMinutes, 1, 0), 0);
  if (intervalMinutes <= 0) {
    return { allowed: true, intervalMinutes: 0, retryAfterMs: 0 };
  }
  const intervalMs = intervalMinutes * 60 * 1000;
  const lastTs =
    typeof lastRefreshAt === "number"
      ? lastRefreshAt
      : Number.isFinite(new Date(lastRefreshAt).getTime())
        ? new Date(lastRefreshAt).getTime()
        : null;
  if (lastTs == null) {
    return { allowed: true, intervalMinutes, retryAfterMs: 0 };
  }
  const elapsedMs = Math.max(nowMs - lastTs, 0);
  const retryAfterMs = Math.max(intervalMs - elapsedMs, 0);
  return {
    allowed: retryAfterMs <= 0,
    intervalMinutes,
    retryAfterMs,
  };
}

export function canViewHistory(plan, itemAgeDays = 0) {
  const config = typeof plan === "object" ? plan : getPlanConfig(plan);
  const maxHistoryDays = Math.max(normalizeNumber(config.historyDaysLimit, 0, 0), 0);
  return {
    allowed: normalizeNumber(itemAgeDays, 0, 0) <= maxHistoryDays,
    maxHistoryDays,
  };
}

export function canViewOpportunity(plan, usageState = {}, opportunityCategory = "") {
  const config = typeof plan === "object" ? plan : getPlanConfig(plan);
  const index = normalizeNumber(
    usageState.position ?? usageState.index ?? usageState.rowIndex,
    0,
  );
  const dailyUsed = normalizeNumber(
    usageState.dailyUsed ?? usageState.opportunitiesUsedToday ?? usageState.usedToday,
    0,
  );
  const visibleFeedLimit = Math.max(normalizeNumber(config.visibleFeedLimit, 1, 1), 1);
  const opportunitiesDailyLimit = Math.max(
    normalizeNumber(config.opportunitiesDailyLimit, visibleFeedLimit, 1),
    1,
  );
  const categoryAllowed = canAccessCategory(config, opportunityCategory || "weapon_skin");
  return {
    allowed:
      categoryAllowed && index < visibleFeedLimit && dailyUsed < opportunitiesDailyLimit,
    categoryAllowed,
    visibleFeedLimit,
    opportunitiesDailyLimit,
  };
}

export function shouldShowLockedPreview(plan, category) {
  return !canAccessCategory(plan, category);
}

export function shouldShowUpgradePrompt(plan, feature) {
  return !hasFeatureAccess(plan, feature);
}

export function getPlanBadgeLabel(plan) {
  const tier = normalizePlanTier(plan);
  if (tier === "alpha_access") return "Coming Soon";
  if (tier === "full_access") return "Most Popular";
  return "Starter";
}

export function getPlanUpgradeTarget(plan) {
  const tier = normalizePlanTier(plan);
  if (tier === "free") return "full_access";
  if (tier === "full_access") return "alpha_access";
  return "alpha_access";
}

export function planTierToLabel(plan) {
  const tier = normalizePlanTier(plan);
  if (tier === "alpha_access") return "Alpha Access";
  if (tier === "full_access") return "Full Access";
  return "Free";
}

export function getAccountPlanLimits(plan) {
  const tier = normalizePlanTier(plan);
  return ACCOUNT_PLAN_LIMITS[tier] || ACCOUNT_PLAN_LIMITS.free;
}

export function getProfileEntitlements(profile = {}) {
  const tier = normalizePlanTier(profile?.planTier || profile?.plan || "free");
  const fallback = getPlanConfig(tier);
  const fromProfile =
    profile?.entitlements && typeof profile.entitlements === "object"
      ? profile.entitlements
      : {};
  const merged = {
    ...fallback,
    ...fromProfile,
  };
  if (!Array.isArray(merged.scannerCategories)) {
    merged.scannerCategories = [...fallback.scannerCategories];
  }
  return merged;
}

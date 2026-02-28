const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const planService = require("./planService");
const { traderModePriceUsd, traderModeMockCheckoutEnabled } = require("../config/env");

const PRICING = {
  free: {
    monthlyUsd: 0,
    headline: "Core tracking and sync"
  },
  pro: {
    monthlyUsd: 12,
    headline: "Power analytics and exports"
  },
  team: {
    monthlyUsd: 49,
    headline: "Creator/team operations"
  }
};

const TRADER_MODE_PRODUCT = {
  sku: "trader_mode_unlock",
  oneTimeUsd: Number(traderModePriceUsd || 29),
  headline: "One-time unlock for power analytics",
  description:
    "Unlock advanced analytics, CSV exports, and historical backtesting without a subscription."
};

const PLAN_TIERS = new Set(Object.keys(PRICING));

function normalizeBillingStatusForPlan(planTier) {
  if (planTier === "free") {
    return "inactive";
  }
  return "active";
}

exports.getPricing = () => {
  return {
    plans: Object.keys(PRICING).map((planTier) => ({
      planTier,
      ...PRICING[planTier],
      entitlements: planService.getEntitlements(planTier)
    })),
    oneTimeProducts: [
      {
        ...TRADER_MODE_PRODUCT,
        mockCheckoutEnabled: Boolean(traderModeMockCheckoutEnabled)
      }
    ]
  };
};

exports.getMyPlan = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const planTier = planService.normalizePlanTier(user.plan_tier);
  const traderModeUnlocked = Boolean(user.trader_mode_unlocked);
  const [events, traderModeEvents] = await Promise.all([
    userRepo.listPlanChangeEventsByUser(userId, 30),
    userRepo.listTraderModeUnlockEventsByUser(userId, 30)
  ]);

  return {
    planTier,
    billingStatus: user.billing_status || "inactive",
    planSeats: Number(user.plan_seats || 1),
    planStartedAt: user.plan_started_at || null,
    traderMode: {
      ...TRADER_MODE_PRODUCT,
      mockCheckoutEnabled: Boolean(traderModeMockCheckoutEnabled),
      unlocked: traderModeUnlocked,
      unlockedAt: user.trader_mode_unlocked_at || null,
      unlockSource: user.trader_mode_unlock_source || null
    },
    entitlements: planService.getEntitlements(planTier, { traderModeUnlocked }),
    changeHistory: events.map((event) => ({
      id: event.id,
      oldPlanTier: event.old_plan_tier,
      newPlanTier: event.new_plan_tier,
      changedBy: event.changed_by,
      createdAt: event.created_at
    })),
    traderModeHistory: traderModeEvents.map((event) => ({
      id: event.id,
      action: event.action,
      source: event.source,
      changedBy: event.changed_by,
      note: event.note || null,
      createdAt: event.created_at
    }))
  };
};

exports.updateMyPlan = async (userId, payload = {}) => {
  const requestedTierRaw = String(payload.planTier || "").trim().toLowerCase();
  if (!requestedTierRaw || !PLAN_TIERS.has(requestedTierRaw)) {
    throw new AppError(
      'planTier must be one of: "free", "pro", "team"',
      400,
      "VALIDATION_ERROR"
    );
  }
  const requestedTier = planService.normalizePlanTier(requestedTierRaw);
  const current = await userRepo.getById(userId);
  if (!current) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const currentTier = planService.normalizePlanTier(current.plan_tier);
  if (requestedTier === currentTier) {
    return exports.getMyPlan(userId);
  }

  const billingStatus = normalizeBillingStatusForPlan(requestedTier);
  const planSeats =
    requestedTier === "team"
      ? Math.max(Number(payload.planSeats) || Number(current.plan_seats) || 5, 1)
      : 1;

  await userRepo.updatePlanById(userId, {
    planTier: requestedTier,
    billingStatus,
    planSeats,
    planStartedAt: new Date().toISOString()
  });
  await userRepo.insertPlanChangeEvent({
    userId,
    oldPlanTier: currentTier,
    newPlanTier: requestedTier,
    changedBy: "self_service"
  });

  return exports.getMyPlan(userId);
};

exports.getTraderMode = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const planTier = planService.normalizePlanTier(user.plan_tier);
  const traderModeUnlocked = Boolean(user.trader_mode_unlocked);
  const events = await userRepo.listTraderModeUnlockEventsByUser(userId, 20);

  return {
    ...TRADER_MODE_PRODUCT,
    mockCheckoutEnabled: Boolean(traderModeMockCheckoutEnabled),
    unlocked: traderModeUnlocked,
    unlockedAt: user.trader_mode_unlocked_at || null,
    unlockSource: user.trader_mode_unlock_source || null,
    entitlements: planService.getEntitlements(planTier, { traderModeUnlocked }),
    history: events.map((event) => ({
      id: event.id,
      action: event.action,
      source: event.source,
      changedBy: event.changed_by,
      note: event.note || null,
      createdAt: event.created_at
    }))
  };
};

exports.unlockTraderMode = async (userId, payload = {}) => {
  const mode = String(payload.mode || "mock").trim().toLowerCase();
  if (mode !== "mock") {
    throw new AppError('mode must be "mock"', 400, "VALIDATION_ERROR");
  }

  if (!traderModeMockCheckoutEnabled) {
    throw new AppError(
      "Mock trader mode checkout is disabled on this environment.",
      503,
      "TRADER_MODE_CHECKOUT_DISABLED"
    );
  }

  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  if (!user.trader_mode_unlocked) {
    await userRepo.updateTraderModeById(userId, {
      traderModeUnlocked: true,
      traderModeUnlockedAt: new Date().toISOString(),
      traderModeUnlockSource: "mock_checkout"
    });
    await userRepo.insertTraderModeUnlockEvent({
      userId,
      action: "unlocked",
      source: "mock_checkout",
      changedBy: "self_service",
      note: "Mock checkout approved"
    });
  }

  return exports.getTraderMode(userId);
};

exports.setTraderModeForUser = async (userId, payload = {}) => {
  const current = await userRepo.getById(userId);
  if (!current) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const unlocked = Boolean(payload.unlocked);
  const currentUnlocked = Boolean(current.trader_mode_unlocked);
  const source = String(payload.source || "admin_toggle").trim() || "admin_toggle";
  const changedBy = String(payload.changedBy || "admin").trim() || "admin";
  const note = payload.note == null ? null : String(payload.note).trim();

  if (unlocked !== currentUnlocked) {
    await userRepo.updateTraderModeById(userId, {
      traderModeUnlocked: unlocked,
      traderModeUnlockedAt: unlocked
        ? payload.unlockedAt || current.trader_mode_unlocked_at || new Date().toISOString()
        : null,
      traderModeUnlockSource: unlocked ? source : null
    });
    await userRepo.insertTraderModeUnlockEvent({
      userId,
      action: unlocked ? "unlocked" : "locked",
      source,
      changedBy,
      note
    });
  }

  return exports.getTraderMode(userId);
};

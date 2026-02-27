const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const planService = require("./planService");

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
    }))
  };
};

exports.getMyPlan = async (userId) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const planTier = planService.normalizePlanTier(user.plan_tier);
  const events = await userRepo.listPlanChangeEventsByUser(userId, 30);

  return {
    planTier,
    billingStatus: user.billing_status || "inactive",
    planSeats: Number(user.plan_seats || 1),
    planStartedAt: user.plan_started_at || null,
    entitlements: planService.getEntitlements(planTier),
    changeHistory: events.map((event) => ({
      id: event.id,
      oldPlanTier: event.old_plan_tier,
      newPlanTier: event.new_plan_tier,
      changedBy: event.changed_by,
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

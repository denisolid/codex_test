const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");

const PLAN_SCHEMA_MIGRATION_FILE = "2026-03-10-temporary-subscription-plan-switcher.sql";
const LEGACY_PLAN_TIER_BY_NEW_TIER = Object.freeze({
  free: "free",
  full_access: "pro"
});
const USER_PLAN_CONSTRAINT_NAMES = Object.freeze([
  "users_plan_tier_check",
  "plan_tier_check",
  "users_plan_check",
  "plan_check"
]);
const PLAN_CHANGE_EVENT_CONSTRAINT_NAMES = Object.freeze([
  "plan_change_events_old_plan_tier_check",
  "plan_change_events_new_plan_tier_check",
  "old_plan_tier_check",
  "new_plan_tier_check"
]);

function hasOwn(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj || {}, key);
}

function toIsoOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function isUniqueViolation(errorLike) {
  const code = String(errorLike?.code || "").trim().toLowerCase();
  const message = String(errorLike?.message || "").trim().toLowerCase();
  return code === "23505" || /duplicate|unique/.test(message);
}

function isForeignKeyViolation(errorLike) {
  const code = String(errorLike?.code || "").trim().toLowerCase();
  const message = String(errorLike?.message || "").trim().toLowerCase();
  return (
    code === "23503" ||
    /foreign key/.test(message) ||
    /users_id_fkey/.test(message)
  );
}

function isCheckConstraintViolation(errorLike, constraintNames = []) {
  const code = String(errorLike?.code || "").trim().toLowerCase();
  const message = String(errorLike?.message || "").trim().toLowerCase();
  const details = String(errorLike?.details || "").trim().toLowerCase();
  const hint = String(errorLike?.hint || "").trim().toLowerCase();
  const combined = `${message} ${details} ${hint}`;
  const looksLikeCheckViolation =
    code === "23514" || combined.includes("violates check constraint");

  if (!looksLikeCheckViolation) {
    return false;
  }

  if (!Array.isArray(constraintNames) || !constraintNames.length) {
    return true;
  }

  return constraintNames.some((name) =>
    combined.includes(String(name || "").trim().toLowerCase())
  );
}

function toLegacyPlanTier(planTier) {
  const safeTier = String(planTier || "").trim().toLowerCase();
  return LEGACY_PLAN_TIER_BY_NEW_TIER[safeTier] || null;
}

function buildPlanSchemaOutdatedError(planTier) {
  const safeTier = String(planTier || "").trim().toLowerCase() || "unknown";
  const message =
    safeTier === "api_advanced"
      ? `Database plan constraints are outdated and do not support "${safeTier}". Apply migration ${PLAN_SCHEMA_MIGRATION_FILE}.`
      : `Database plan constraints are outdated. Apply migration ${PLAN_SCHEMA_MIGRATION_FILE}.`;
  return new AppError(message, 409, "PLAN_SCHEMA_OUTDATED");
}

async function hasAuthUser(id) {
  const safeId = String(id || "").trim();
  if (!safeId) {
    return false;
  }

  const { data, error } = await supabaseAdmin.auth.admin.getUserById(safeId);
  if (!error) {
    return Boolean(data?.user);
  }

  if (
    Number(error?.status || 0) === 404 ||
    /user\s+not\s+found/i.test(String(error?.message || ""))
  ) {
    return false;
  }

  throw new AppError(error.message, 500);
}

exports.getById = async (id) => {
  const { data, error } = await supabaseAdmin
    .from("users")
    .select("*")
    .eq("id", id)
    .single();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }
  return data || null;
};

exports.getBySteamId64 = async (steamId64) => {
  const { data, error } = await supabaseAdmin
    .from("users")
    .select("*")
    .eq("steam_id64", steamId64)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.getByEmail = async (email) => {
  const safeEmail = String(email || "").trim().toLowerCase();
  if (!safeEmail) return null;

  const { data, error } = await supabaseAdmin
    .from("users")
    .select("*")
    .eq("email", safeEmail)
    .maybeSingle();

  if (error && error.code !== "PGRST116") {
    throw new AppError(error.message, 500);
  }

  return data || null;
};

exports.getByIds = async (ids = []) => {
  const safeIds = Array.from(
    new Set(
      (Array.isArray(ids) ? ids : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!safeIds.length) {
    return [];
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .select(
      "id, email, pending_email, email_verified, onboarding_completed, plan, plan_status, display_name, avatar_url, steam_id64, public_portfolio_enabled, ownership_alerts_enabled, plan_tier, billing_status, plan_seats, plan_started_at, trader_mode_unlocked, trader_mode_unlocked_at, trader_mode_unlock_source, created_at, updated_at"
    )
    .in("id", safeIds);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.listPublicSteamUsers = async (limit = 200) => {
  const normalizedLimit = Math.min(Math.max(Number(limit) || 200, 1), 2000);
  const { data, error } = await supabaseAdmin
    .from("users")
    .select(
      "id, display_name, avatar_url, steam_id64, public_portfolio_enabled, ownership_alerts_enabled, plan_tier, plan_seats"
    )
    .not("steam_id64", "is", null)
    .eq("public_portfolio_enabled", true)
    .order("updated_at", { ascending: false })
    .limit(normalizedLimit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.ensureExists = async (id, email) => {
  const safeId = String(id || "").trim();
  const safeEmail = String(email || "").trim().toLowerCase();

  const { error } = await supabaseAdmin
    .from("users")
    .upsert([{ id: safeId, email: safeEmail }], { onConflict: "id" });

  if (!error) {
    return exports.getById(safeId);
  }

  if (isUniqueViolation(error) && safeEmail) {
    const existingByEmail = await exports.getByEmail(safeEmail);
    if (existingByEmail && String(existingByEmail.id || "") !== safeId) {
      const conflict = new AppError("This email is already in use.", 409, "EMAIL_IN_USE");
      conflict.existingUserId = existingByEmail.id;
      conflict.requestedUserId = safeId;
      conflict.email = safeEmail;
      throw conflict;
    }
  }

  if (isForeignKeyViolation(error)) {
    const authUserExists = await hasAuthUser(safeId);
    if (!authUserExists) {
      throw new AppError(
        "Auth user is not available for profile creation.",
        409,
        "PROFILE_AUTH_USER_MISSING"
      );
    }
  }

  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.updateSteamProfileById = async (id, updates = {}) => {
  const patch = {};

  if (hasOwn(updates, "steamId64")) {
    if (updates.steamId64 == null || String(updates.steamId64).trim() === "") {
      patch.steam_id64 = null;
    } else {
      const steamId64 = String(updates.steamId64).trim();
      if (!/^\d{17}$/.test(steamId64)) {
        throw new AppError("steamId64 must be 17 digits", 400);
      }
      patch.steam_id64 = steamId64;
    }
  }

  if (hasOwn(updates, "displayName")) {
    patch.display_name = String(updates.displayName || "").trim() || null;
  }

  if (hasOwn(updates, "avatarUrl")) {
    patch.avatar_url = String(updates.avatarUrl || "").trim() || null;
  }

  if (!Object.keys(patch).length) {
    return exports.getById(id);
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .update(patch)
    .eq("id", id)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.updateOnboardingById = async (id, updates = {}) => {
  const patch = {};

  if (hasOwn(updates, "email")) {
    const safeEmail = String(updates.email || "").trim().toLowerCase();
    if (!safeEmail) {
      throw new AppError("email is required", 400, "VALIDATION_ERROR");
    }
    patch.email = safeEmail;
  }

  if (hasOwn(updates, "pendingEmail")) {
    patch.pending_email = String(updates.pendingEmail || "").trim().toLowerCase() || null;
  }

  if (hasOwn(updates, "emailVerified")) {
    patch.email_verified = Boolean(updates.emailVerified);
  }

  if (hasOwn(updates, "onboardingCompleted")) {
    patch.onboarding_completed = Boolean(updates.onboardingCompleted);
  }

  if (hasOwn(updates, "plan")) {
    patch.plan = String(updates.plan || "").trim().toLowerCase() || "free";
  }

  if (hasOwn(updates, "planStatus")) {
    patch.plan_status = String(updates.planStatus || "").trim().toLowerCase() || "active";
  }

  if (!Object.keys(patch).length) {
    return exports.getById(id);
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .update(patch)
    .eq("id", id)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.updatePlanById = async (id, updates = {}) => {
  const patch = {};
  if (hasOwn(updates, "planTier")) {
    const normalizedPlanTier = String(updates.planTier || "").trim().toLowerCase() || "free";
    patch.plan_tier = normalizedPlanTier;
    patch.plan = normalizedPlanTier;
  }
  if (hasOwn(updates, "plan")) {
    patch.plan = String(updates.plan || "").trim().toLowerCase() || "free";
  }
  if (hasOwn(updates, "planStatus")) {
    patch.plan_status = String(updates.planStatus || "").trim().toLowerCase() || "active";
  }
  if (hasOwn(updates, "billingStatus")) {
    patch.billing_status = String(updates.billingStatus || "").trim().toLowerCase() || "inactive";
  }
  if (hasOwn(updates, "planSeats")) {
    patch.plan_seats = Math.max(Number(updates.planSeats) || 1, 1);
  }
  if (hasOwn(updates, "planStartedAt")) {
    patch.plan_started_at = updates.planStartedAt || null;
  }

  if (!Object.keys(patch).length) {
    return exports.getById(id);
  }

  const runUpdate = async (patchToApply) =>
    supabaseAdmin
      .from("users")
      .update(patchToApply)
      .eq("id", id)
      .select("*")
      .single();

  let { data, error } = await runUpdate(patch);

  if (
    error &&
    (hasOwn(updates, "planTier") || hasOwn(updates, "plan")) &&
    isCheckConstraintViolation(error, USER_PLAN_CONSTRAINT_NAMES)
  ) {
    const requestedPlanTier = String(patch.plan_tier || patch.plan || "")
      .trim()
      .toLowerCase();
    const legacyPlanTier = toLegacyPlanTier(requestedPlanTier);

    if (!legacyPlanTier) {
      throw buildPlanSchemaOutdatedError(requestedPlanTier);
    }

    const legacyPatch = {
      ...patch,
      plan_tier: legacyPlanTier,
      plan: legacyPlanTier
    };
    ({ data, error } = await runUpdate(legacyPatch));

    if (error && isCheckConstraintViolation(error, USER_PLAN_CONSTRAINT_NAMES)) {
      throw buildPlanSchemaOutdatedError(requestedPlanTier);
    }
  }

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.updateTraderModeById = async (id, updates = {}) => {
  const patch = {};
  if (hasOwn(updates, "traderModeUnlocked")) {
    patch.trader_mode_unlocked = Boolean(updates.traderModeUnlocked);
  }
  if (hasOwn(updates, "traderModeUnlockedAt")) {
    patch.trader_mode_unlocked_at = updates.traderModeUnlockedAt || null;
  }
  if (hasOwn(updates, "traderModeUnlockSource")) {
    patch.trader_mode_unlock_source =
      String(updates.traderModeUnlockSource || "").trim() || null;
  }

  if (!Object.keys(patch).length) {
    return exports.getById(id);
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .update(patch)
    .eq("id", id)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.insertPlanChangeEvent = async (payload = {}) => {
  const row = {
    user_id: payload.userId,
    old_plan_tier: payload.oldPlanTier,
    new_plan_tier: payload.newPlanTier,
    changed_by: payload.changedBy || "self_service"
  };

  const insertRow = async (rowToInsert) =>
    supabaseAdmin.from("plan_change_events").insert(rowToInsert);

  let { error } = await insertRow(row);
  if (error && isCheckConstraintViolation(error, PLAN_CHANGE_EVENT_CONSTRAINT_NAMES)) {
    const legacyOldPlanTier = toLegacyPlanTier(row.old_plan_tier);
    const legacyNewPlanTier = toLegacyPlanTier(row.new_plan_tier);

    if (!legacyOldPlanTier || !legacyNewPlanTier) {
      throw buildPlanSchemaOutdatedError(row.new_plan_tier || row.old_plan_tier);
    }

    ({ error } = await insertRow({
      ...row,
      old_plan_tier: legacyOldPlanTier,
      new_plan_tier: legacyNewPlanTier
    }));

    if (error && isCheckConstraintViolation(error, PLAN_CHANGE_EVENT_CONSTRAINT_NAMES)) {
      throw buildPlanSchemaOutdatedError(row.new_plan_tier || row.old_plan_tier);
    }
  }

  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.listPlanChangeEventsByUser = async (userId, limit = 50) => {
  const normalizedLimit = Math.min(Math.max(Number(limit) || 50, 1), 500);
  const { data, error } = await supabaseAdmin
    .from("plan_change_events")
    .select("id, old_plan_tier, new_plan_tier, changed_by, created_at")
    .eq("user_id", userId)
    .order("created_at", { ascending: false })
    .limit(normalizedLimit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.insertTraderModeUnlockEvent = async (payload = {}) => {
  const row = {
    user_id: payload.userId,
    action: payload.action,
    source: payload.source || "admin_toggle",
    changed_by: payload.changedBy || "system",
    note: payload.note || null
  };

  const { error } = await supabaseAdmin.from("trader_mode_unlock_events").insert(row);
  if (error) {
    throw new AppError(error.message, 500);
  }
};

exports.listTraderModeUnlockEventsByUser = async (userId, limit = 50) => {
  const normalizedLimit = Math.min(Math.max(Number(limit) || 50, 1), 500);
  const { data, error } = await supabaseAdmin
    .from("trader_mode_unlock_events")
    .select("id, action, source, changed_by, note, created_at")
    .eq("user_id", userId)
    .order("created_at", { ascending: false })
    .limit(normalizedLimit);

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data || [];
};

exports.updateSteamId = async (id, steamId64) => {
  return exports.updateSteamProfileById(id, { steamId64 });
};

exports.updatePreferencesById = async (id, updates = {}) => {
  const patch = {};

  if (hasOwn(updates, "publicPortfolioEnabled")) {
    patch.public_portfolio_enabled = Boolean(updates.publicPortfolioEnabled);
  }

  if (hasOwn(updates, "ownershipAlertsEnabled")) {
    patch.ownership_alerts_enabled = Boolean(updates.ownershipAlertsEnabled);
  }

  if (!Object.keys(patch).length) {
    return exports.getById(id);
  }

  const { data, error } = await supabaseAdmin
    .from("users")
    .update(patch)
    .eq("id", id)
    .select("*")
    .single();

  if (error) {
    throw new AppError(error.message, 500);
  }

  return data;
};

exports.mergeUserData = async (fromUserId, toUserId) => {
  const fromId = String(fromUserId || "").trim();
  const toId = String(toUserId || "").trim();

  if (!fromId || !toId || fromId === toId) {
    return;
  }

  const [fromInventoriesRes, toInventoriesRes] = await Promise.all([
    supabaseAdmin
      .from("inventories")
      .select(
        "skin_id, quantity, steam_item_ids, purchase_price, purchase_currency, last_synced_at"
      )
      .eq("user_id", fromId),
    supabaseAdmin
      .from("inventories")
      .select(
        "skin_id, quantity, steam_item_ids, purchase_price, purchase_currency, last_synced_at"
      )
      .eq("user_id", toId)
  ]);

  if (fromInventoriesRes.error) {
    throw new AppError(fromInventoriesRes.error.message, 500);
  }
  if (toInventoriesRes.error) {
    throw new AppError(toInventoriesRes.error.message, 500);
  }

  const fromRows = fromInventoriesRes.data || [];
  const toRows = toInventoriesRes.data || [];
  const toBySkinId = new Map(toRows.map((row) => [Number(row.skin_id), row]));
  const mergedRows = [];

  for (const row of fromRows) {
    const skinId = Number(row.skin_id);
    const existing = toBySkinId.get(skinId);

    if (existing) {
      const existingItems = Array.isArray(existing.steam_item_ids) ? existing.steam_item_ids : [];
      const sourceItems = Array.isArray(row.steam_item_ids) ? row.steam_item_ids : [];

      mergedRows.push({
        user_id: toId,
        skin_id: skinId,
        quantity: Number(existing.quantity || 0) + Number(row.quantity || 0),
        steam_item_ids: Array.from(new Set([...existingItems, ...sourceItems])),
        purchase_price: existing.purchase_price ?? row.purchase_price ?? null,
        purchase_currency: existing.purchase_currency || row.purchase_currency || "USD",
        last_synced_at:
          toIsoOrNull(existing.last_synced_at) ||
          toIsoOrNull(row.last_synced_at) ||
          new Date().toISOString()
      });
      continue;
    }

    mergedRows.push({
      user_id: toId,
      skin_id: skinId,
      quantity: Number(row.quantity || 0),
      steam_item_ids: Array.isArray(row.steam_item_ids) ? row.steam_item_ids : [],
      purchase_price: row.purchase_price ?? null,
      purchase_currency: row.purchase_currency || "USD",
      last_synced_at: toIsoOrNull(row.last_synced_at) || new Date().toISOString()
    });
  }

  if (mergedRows.length) {
    const { error } = await supabaseAdmin
      .from("inventories")
      .upsert(mergedRows, { onConflict: "user_id,skin_id" });

    if (error) {
      throw new AppError(error.message, 500);
    }
  }

  const moveTables = [
    "transactions",
    "price_alerts",
    "alert_events",
    "extension_api_keys"
  ];

  for (const table of moveTables) {
    const { error } = await supabaseAdmin
      .from(table)
      .update({ user_id: toId })
      .eq("user_id", fromId);

    if (error) {
      throw new AppError(error.message, 500);
    }
  }

  const { error: deleteFromInventoryError } = await supabaseAdmin
    .from("inventories")
    .delete()
    .eq("user_id", fromId);

  if (deleteFromInventoryError) {
    throw new AppError(deleteFromInventoryError.message, 500);
  }
};

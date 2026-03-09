const crypto = require("node:crypto");
const { supabaseAdmin } = require("../config/supabase");
const { emailVerificationTtlMinutes } = require("../config/env");
const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const emailVerificationRepo = require("../repositories/emailVerificationRepository");
const emailService = require("./emailService");

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const STEAM_MANAGED_EMAIL_REGEX = /^steam_\d{17}@steam\.local$/i;

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isSteamManagedEmail(email) {
  return STEAM_MANAGED_EMAIL_REGEX.test(normalizeEmail(email));
}

function validateEmail(email) {
  const safeEmail = normalizeEmail(email);
  if (!EMAIL_REGEX.test(safeEmail)) {
    throw new AppError("A valid email is required", 400, "INVALID_EMAIL");
  }
  if (isSteamManagedEmail(safeEmail)) {
    throw new AppError("Please use a real inbox email", 400, "INVALID_EMAIL");
  }
  return safeEmail;
}

function parseTimestampMs(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  const ts = new Date(text).getTime();
  return Number.isFinite(ts) ? ts : null;
}

function isDuplicateUserError(message) {
  return /already\s+registered|duplicate|unique/i.test(String(message || ""));
}

function toIsoTimestamp(value) {
  return new Date(value).toISOString();
}

function resolveProfileProvider(profile = {}, authUser = {}) {
  const metadata = authUser?.user_metadata || {};
  const raw = String(
    profile?.provider || metadata?.provider || (profile?.steam_id64 ? "steam" : "email")
  )
    .trim()
    .toLowerCase();
  return raw || "email";
}

function resolveOnboardingState({ userProfile = {}, authUser = null } = {}) {
  const provider = resolveProfileProvider(userProfile, authUser);
  const email = normalizeEmail(userProfile?.email || authUser?.email || "");
  const pendingEmail = normalizeEmail(userProfile?.pending_email || "");
  const steamManagedPrimaryEmail = isSteamManagedEmail(email);
  const isSteamAccount = Boolean(userProfile?.steam_id64) || provider === "steam";
  const explicitEmailVerified =
    typeof userProfile?.email_verified === "boolean" ? userProfile.email_verified : null;
  const explicitOnboardingCompleted =
    typeof userProfile?.onboarding_completed === "boolean"
      ? userProfile.onboarding_completed
      : null;
  const authEmailConfirmed = Boolean(authUser?.email_confirmed_at || authUser?.confirmed_at);

  const inferredEmailVerified =
    steamManagedPrimaryEmail
      ? false
      : explicitEmailVerified != null
        ? explicitEmailVerified
        : authEmailConfirmed || (!isSteamAccount && Boolean(email));

  const inferredOnboardingCompleted =
    steamManagedPrimaryEmail
      ? false
      : explicitOnboardingCompleted != null
        ? explicitOnboardingCompleted
        : inferredEmailVerified || !isSteamAccount;

  const onboardingRequired =
    isSteamAccount && (!inferredEmailVerified || !inferredOnboardingCompleted);

  const plan = String(userProfile?.plan || userProfile?.plan_tier || "free")
    .trim()
    .toLowerCase();
  const planStatus = String(
    userProfile?.plan_status || (onboardingRequired ? "pending_verification" : "active")
  )
    .trim()
    .toLowerCase();

  return {
    email,
    pendingEmail: pendingEmail || null,
    provider,
    isSteamAccount,
    emailVerified: Boolean(inferredEmailVerified),
    onboardingCompleted: Boolean(inferredOnboardingCompleted),
    onboardingRequired,
    plan: plan || "free",
    planStatus: planStatus || "active"
  };
}

function ensureSteamOnboardingAccount(state = {}) {
  if (!state.isSteamAccount) {
    throw new AppError(
      "Email onboarding is only required for Steam sign-in accounts.",
      400,
      "ONBOARDING_NOT_REQUIRED"
    );
  }
}

function buildTokenHash(token) {
  return crypto.createHash("sha256").update(String(token || "")).digest("hex");
}

function buildVerifyUrl({
  apiOrigin,
  token,
  next,
  verifyPath = "/api/auth/onboarding/verify",
  email = ""
}) {
  const base = String(apiOrigin || "").replace(/\/+$/, "");
  if (!base) {
    throw new AppError("Missing API public URL for email verification link", 500, "MISSING_API_ORIGIN");
  }
  const path = String(verifyPath || "/api/auth/onboarding/verify").trim();
  const safePath = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(safePath, `${base}/`);
  url.searchParams.set("token", token);
  if (next) {
    url.searchParams.set("next", String(next || ""));
  }
  if (email) {
    url.searchParams.set("email", String(email || ""));
  }
  return url.toString();
}

async function ensureEmailAvailable(email, userId) {
  const existing = await userRepo.getByEmail(email);
  if (existing && String(existing.id) !== String(userId)) {
    throw new AppError("This email is already in use.", 409, "EMAIL_IN_USE");
  }
}

async function writeVerificationRequest({
  userId,
  email,
  apiOrigin,
  next,
  displayName,
  verifyPath = "/api/auth/onboarding/verify",
  verificationType = "steam_onboarding"
}) {
  const token = crypto.randomBytes(32).toString("hex");
  const tokenHash = buildTokenHash(token);
  const ttlMinutes = Math.max(Number(emailVerificationTtlMinutes || 30), 5);
  const expiresAt = toIsoTimestamp(Date.now() + ttlMinutes * 60 * 1000);

  await emailVerificationRepo.invalidatePendingByUser(userId);
  await emailVerificationRepo.create({
    userId,
    email,
    tokenHash,
    expiresAt
  });

  await userRepo.updateOnboardingById(userId, {
    pendingEmail: email,
    emailVerified: false,
    onboardingCompleted: false,
    plan: "free",
    planStatus: "pending_verification"
  });

  const verifyUrl = buildVerifyUrl({
    apiOrigin,
    token,
    next,
    verifyPath,
    email
  });

  if (verificationType === "account") {
    await emailService.sendAccountVerificationEmail({
      to: email,
      verifyUrl,
      displayName
    });
  } else {
    await emailService.sendSteamOnboardingVerificationEmail({
      to: email,
      verifyUrl,
      displayName
    });
  }

  return {
    email,
    expiresAt
  };
}

exports.requestEmailVerification = async ({
  userId,
  email,
  apiOrigin,
  next = ""
} = {}) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const onboarding = resolveOnboardingState({ userProfile: user });
  ensureSteamOnboardingAccount(onboarding);

  if (onboarding.emailVerified && onboarding.onboardingCompleted && !isSteamManagedEmail(onboarding.email)) {
    return {
      alreadyVerified: true,
      message: "Email is already verified.",
      pendingEmail: null
    };
  }

  const safeEmail = validateEmail(email);
  await ensureEmailAvailable(safeEmail, user.id);
  const verification = await writeVerificationRequest({
    userId: user.id,
    email: safeEmail,
    apiOrigin,
    next,
    displayName: user.display_name
  });

  return {
    alreadyVerified: false,
    message: "Verification email sent. Check your inbox.",
    pendingEmail: verification.email,
    expiresAt: verification.expiresAt
  };
};

exports.requestAccountEmailVerification = async ({
  userId,
  email = "",
  apiOrigin,
  next = ""
} = {}) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const onboarding = resolveOnboardingState({ userProfile: user });
  if (onboarding.emailVerified && onboarding.onboardingCompleted) {
    return {
      alreadyVerified: true,
      message: "Email is already verified.",
      pendingEmail: null
    };
  }

  const safeEmail = validateEmail(email || user.email);
  await ensureEmailAvailable(safeEmail, user.id);
  const verification = await writeVerificationRequest({
    userId: user.id,
    email: safeEmail,
    apiOrigin,
    next,
    verifyPath: "/api/auth/verify-email",
    verificationType: "account",
    displayName: user.display_name
  });

  return {
    alreadyVerified: false,
    message: "Verification email sent. Check your inbox.",
    pendingEmail: verification.email,
    expiresAt: verification.expiresAt
  };
};

exports.resendEmailVerification = async ({
  userId,
  apiOrigin,
  next = "",
  email = ""
} = {}) => {
  const user = await userRepo.getById(userId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const onboarding = resolveOnboardingState({ userProfile: user });
  ensureSteamOnboardingAccount(onboarding);

  if (onboarding.emailVerified && onboarding.onboardingCompleted && !isSteamManagedEmail(onboarding.email)) {
    return {
      alreadyVerified: true,
      message: "Email is already verified.",
      pendingEmail: null
    };
  }

  let targetEmail = normalizeEmail(email || onboarding.pendingEmail);
  if (!targetEmail) {
    const latest = await emailVerificationRepo.getLatestPendingByUser(user.id);
    targetEmail = normalizeEmail(latest?.email || "");
  }
  if (!targetEmail) {
    throw new AppError("No pending email to resend. Submit email first.", 400, "PENDING_EMAIL_REQUIRED");
  }

  const safeEmail = validateEmail(targetEmail);
  await ensureEmailAvailable(safeEmail, user.id);
  const verification = await writeVerificationRequest({
    userId: user.id,
    email: safeEmail,
    apiOrigin,
    next,
    displayName: user.display_name
  });

  return {
    alreadyVerified: false,
    message: "Verification email resent. Check your inbox.",
    pendingEmail: verification.email,
    expiresAt: verification.expiresAt
  };
};

exports.verifyEmailToken = async (token) => {
  const safeToken = String(token || "").trim();
  if (!safeToken) {
    throw new AppError("Verification token is required", 400, "EMAIL_VERIFICATION_TOKEN_MISSING");
  }

  const verification = await emailVerificationRepo.getByTokenHash(buildTokenHash(safeToken));
  if (!verification) {
    throw new AppError("Verification link is invalid.", 400, "EMAIL_VERIFICATION_INVALID");
  }

  if (verification.used_at) {
    throw new AppError("Verification link has already been used.", 400, "EMAIL_VERIFICATION_USED");
  }

  const expiresMs = parseTimestampMs(verification.expires_at);
  if (!expiresMs || expiresMs < Date.now()) {
    throw new AppError("Verification link has expired.", 400, "EMAIL_VERIFICATION_EXPIRED");
  }

  const consumed = await emailVerificationRepo.consumeById(verification.id);
  if (!consumed) {
    throw new AppError("Verification link has already been used.", 400, "EMAIL_VERIFICATION_USED");
  }

  const user = await userRepo.getById(consumed.user_id);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const safeEmail = validateEmail(consumed.email);
  await ensureEmailAvailable(safeEmail, user.id);

  await userRepo.updateOnboardingById(user.id, {
    email: safeEmail,
    pendingEmail: null,
    emailVerified: true,
    onboardingCompleted: true,
    plan: "free",
    planStatus: "active"
  });

  await supabaseAdmin.auth.admin
    .updateUserById(user.id, {
      email: safeEmail,
      email_confirm: true
    })
    .then(({ error }) => {
      if (!error) return;
      if (isDuplicateUserError(error.message)) {
        throw new AppError("This email is already in use.", 409, "EMAIL_IN_USE");
      }
      throw new AppError("Failed to verify email.", 500, "EMAIL_VERIFICATION_FAILED");
    });

  return {
    userId: user.id,
    email: safeEmail,
    verifiedAt: new Date().toISOString()
  };
};

exports.syncProfileVerificationState = async ({ userProfile = {}, authUser = null } = {}) => {
  if (!userProfile?.id) {
    return userProfile || null;
  }

  const onboarding = resolveOnboardingState({ userProfile, authUser });
  const shouldHydrateFromAuth =
    !isSteamManagedEmail(onboarding.email) &&
    Boolean(authUser?.email_confirmed_at || authUser?.confirmed_at) &&
    (!onboarding.emailVerified || !onboarding.onboardingCompleted);

  if (!shouldHydrateFromAuth) {
    return userProfile;
  }

  return userRepo.updateOnboardingById(userProfile.id, {
    emailVerified: true,
    onboardingCompleted: true,
    pendingEmail: null,
    plan: onboarding.plan || "free",
    planStatus: "active"
  });
};

exports.resolveOnboardingState = resolveOnboardingState;
exports.normalizeEmail = normalizeEmail;
exports.validateEmail = validateEmail;
exports.isSteamManagedEmail = isSteamManagedEmail;

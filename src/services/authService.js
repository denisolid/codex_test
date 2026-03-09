const crypto = require("node:crypto");
const { supabaseAdmin, supabaseAuthClient } = require("../config/supabase");
const { authEmailRedirectTo, appAuthSecret } = require("../config/env");
const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const emailOnboardingService = require("./emailOnboardingService");

function isSteamManagedEmail(email) {
  return emailOnboardingService.isSteamManagedEmail(email);
}

function validateEmail(email) {
  return emailOnboardingService.validateEmail(email);
}

function validatePassword(password) {
  if (String(password || "").length < 6) {
    throw new AppError(
      "Password must be at least 6 characters",
      400,
      "INVALID_PASSWORD"
    );
  }
  return String(password);
}

function validateSteamId64(steamId64) {
  const safeSteamId64 = String(steamId64 || "").trim();
  if (!/^\d{17}$/.test(safeSteamId64)) {
    throw new AppError("Invalid Steam ID", 400, "INVALID_STEAM_ID");
  }
  return safeSteamId64;
}

function buildSteamEmail(steamId64) {
  return `steam_${steamId64}@steam.local`;
}

function buildSteamPassword(steamId64) {
  const digest = crypto
    .createHmac("sha256", appAuthSecret)
    .update(`steam-login:${steamId64}`)
    .digest("hex");

  return `Steam!${digest}`;
}

function normalizeSteamProfile(profile = {}) {
  return {
    displayName: String(profile.displayName || "").trim() || null,
    avatarUrl: String(profile.avatarUrl || "").trim() || null
  };
}

function toAuthUserFromProfileRow(profileRow, steamId64) {
  const safeSteamId64 = validateSteamId64(steamId64);
  return {
    id: profileRow.id,
    email: profileRow.email || buildSteamEmail(safeSteamId64),
    user_metadata: {
      provider: "steam",
      steam_id64: safeSteamId64,
      display_name: profileRow.display_name || null,
      avatar_url: profileRow.avatar_url || null
    }
  };
}

function toAuthUserFromProfileRowWithoutSteam(profileRow) {
  return {
    id: profileRow.id,
    email: profileRow.email || null,
    user_metadata: {
      provider: "email",
      steam_id64: null,
      display_name: profileRow.display_name || null,
      avatar_url: profileRow.avatar_url || null
    }
  };
}

function isDuplicateUserError(errorLike) {
  const message = String(
    errorLike?.message || errorLike?.error_description || errorLike || ""
  );
  const code = String(
    errorLike?.code || errorLike?.error_code || ""
  )
    .trim()
    .toLowerCase();
  const status = Number(errorLike?.status || errorLike?.statusCode || 0);

  if (code === "email_exists" || code === "user_already_exists" || code === "23505") {
    return true;
  }

  if (status === 409) {
    return true;
  }

  return /already\s+(been\s+)?registered|already\s+exists|user\s+already|already\s+in\s+use|duplicate|unique/i.test(
    message
  );
}

function isEmailNotConfirmedMessage(message) {
  return /email\s+not\s+confirmed/i.test(String(message || ""));
}

function isRateLimitMessage(message) {
  return /rate\s*limit|too\s*many\s*requests/i.test(String(message || ""));
}

exports.register = async (email, password, { apiOrigin, next } = {}) => {
  const safeEmail = validateEmail(email);
  const safePassword = validatePassword(password);
  const verifyNext = String(next || authEmailRedirectTo || "").trim();
  const safeApiOrigin = String(apiOrigin || "").trim();

  const { data, error } = await supabaseAdmin.auth.admin.createUser({
    email: safeEmail,
    password: safePassword,
    email_confirm: false,
    user_metadata: {
      provider: "email"
    }
  });

  if (error) {
    if (isDuplicateUserError(error)) {
      throw new AppError(
        "This email is already registered. Log in or request another verification email.",
        409,
        "EMAIL_IN_USE"
      );
    }

    if (isRateLimitMessage(error.message)) {
      throw new AppError(
        "Too many sign-up attempts. Try again shortly.",
        429,
        "RATE_LIMITED"
      );
    }

    throw new AppError(
      "Unable to complete registration. Try again.",
      400,
      "REGISTER_FAILED"
    );
  }

  const user = data?.user || null;
  if (!user?.id || !user?.email) {
    throw new AppError("Unable to complete registration. Try again.", 400, "REGISTER_FAILED");
  }

  try {
    await userRepo.ensureExists(user.id, safeEmail);
  } catch (err) {
    if (String(err?.code || "").trim().toUpperCase() !== "PROFILE_AUTH_USER_MISSING") {
      throw err;
    }
  }

  let verificationEmailSent = true;
  try {
    await emailOnboardingService.requestAccountEmailVerification({
      userId: user.id,
      email: safeEmail,
      apiOrigin: safeApiOrigin,
      next: verifyNext
    });
  } catch (err) {
    const code = String(err?.code || "").trim().toUpperCase();
    if (
      code === "EMAIL_PROVIDER_NOT_CONFIGURED" ||
      code === "EMAIL_SEND_FAILED" ||
      code === "MISSING_API_ORIGIN"
    ) {
      verificationEmailSent = false;
    } else {
      throw err;
    }
  }

  return {
    user,
    session: null,
    requiresEmailConfirmation: true,
    verificationEmailSent,
    message: verificationEmailSent
      ? "Registration successful. Check your inbox for a confirmation link before login."
      : "Registration successful. We could not send your verification email right now. Use resend confirmation on login."
  };
};

exports.login = async (email, password) => {
  const safeEmail = validateEmail(email);
  const safePassword = validatePassword(password);

  const { data, error } = await supabaseAuthClient.auth.signInWithPassword({
    email: safeEmail,
    password: safePassword
  });

  if (error) {
    if (isEmailNotConfirmedMessage(error.message)) {
      throw new AppError(
        "Email not confirmed. Check your inbox and confirm your email before login.",
        401,
        "EMAIL_NOT_CONFIRMED"
      );
    }

    if (isRateLimitMessage(error.message)) {
      throw new AppError(
        "Too many login attempts. Try again shortly.",
        429,
        "RATE_LIMITED"
      );
    }

    throw new AppError("Invalid email or password", 401, "INVALID_CREDENTIALS");
  }

  if (!data?.session?.access_token || !data?.user) {
    throw new AppError("Login failed. Try again.", 401, "LOGIN_FAILED");
  }

  return {
    accessToken: data.session.access_token,
    user: data.user
  };
};

exports.getUserByAccessToken = async (token) => {
  if (!token) {
    throw new AppError("Missing access token", 401, "MISSING_ACCESS_TOKEN");
  }

  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !data || !data.user) {
    throw new AppError("Invalid access token", 401, "INVALID_ACCESS_TOKEN");
  }

  return data.user;
};

exports.resendConfirmation = async (email, { apiOrigin, next } = {}) => {
  const safeEmail = validateEmail(email);
  const safeApiOrigin = String(apiOrigin || "").trim();
  const verifyNext = String(next || authEmailRedirectTo || "").trim();
  const genericMessage =
    "If this account exists and is not confirmed, a confirmation link has been sent. Check inbox and spam.";

  const user = await userRepo.getByEmail(safeEmail);
  if (!user?.id) {
    return {
      message: genericMessage
    };
  }

  const onboarding = emailOnboardingService.resolveOnboardingState({
    userProfile: user
  });
  if (onboarding.emailVerified && onboarding.onboardingCompleted) {
    return {
      message: genericMessage
    };
  }

  try {
    await emailOnboardingService.requestAccountEmailVerification({
      userId: user.id,
      email: safeEmail,
      apiOrigin: safeApiOrigin,
      next: verifyNext
    });
  } catch (err) {
    if (isRateLimitMessage(err?.message) || String(err?.code || "").trim().toUpperCase() === "RATE_LIMITED") {
      throw new AppError(
        "Too many resend attempts. Try again shortly.",
        429,
        "RATE_LIMITED"
      );
    }
    throw err;
  }

  return {
    message: genericMessage
  };
};

exports.loginWithSteam = async (steamId64, profile = {}) => {
  const safeSteamId64 = validateSteamId64(steamId64);
  const safeProfile = normalizeSteamProfile(profile);
  const steamEmail = buildSteamEmail(safeSteamId64);
  const steamPassword = buildSteamPassword(safeSteamId64);
  let createdSteamUser = false;
  let createUserError = null;

  let profileRow = await userRepo.getBySteamId64(safeSteamId64);

  if (!profileRow) {
    const createPayload = {
      email: steamEmail,
      password: steamPassword,
      email_confirm: true,
      user_metadata: {
        provider: "steam",
        steam_id64: safeSteamId64
      }
    };

    const { data, error } = await supabaseAdmin.auth.admin.createUser(createPayload);
    if (error && !isDuplicateUserError(error)) {
      createUserError = error;
    }

    const authUser = data?.user;
    if (authUser?.id) {
      createdSteamUser = true;
      await userRepo.ensureExists(authUser.id, steamEmail);
      profileRow = await userRepo.updateSteamProfileById(authUser.id, {
        steamId64: safeSteamId64,
        displayName: safeProfile.displayName,
        avatarUrl: safeProfile.avatarUrl
      });
    }
  }

  if (!profileRow) {
    const { data, error } = await supabaseAuthClient.auth.signInWithPassword({
      email: steamEmail,
      password: steamPassword
    });

    if (error || !data?.user?.id) {
      if (createUserError) {
        throw new AppError(
          "Unable to create Steam account",
          500,
          "STEAM_ACCOUNT_CREATE_FAILED"
        );
      }
      throw new AppError("Steam login failed", 401, "STEAM_LOGIN_FAILED");
    }

    await userRepo.ensureExists(data.user.id, steamEmail);
    profileRow = await userRepo.updateSteamProfileById(data.user.id, {
      steamId64: safeSteamId64,
      displayName: safeProfile.displayName,
      avatarUrl: safeProfile.avatarUrl
    });
  }

  if (!profileRow?.id) {
    throw new AppError("Steam login failed", 401, "STEAM_LOGIN_FAILED");
  }

  if (isSteamManagedEmail(profileRow.email)) {
    profileRow = await userRepo.updateOnboardingById(profileRow.id, {
      emailVerified: false,
      onboardingCompleted: false,
      plan: "free",
      planStatus: "pending_verification"
    });
  }

  const linkResult = await exports.linkSteamToUser(
    profileRow.id,
    safeSteamId64,
    safeProfile
  );
  return {
    user: linkResult.user,
    isNewSteamUser: createdSteamUser
  };
};

exports.linkSteamToUser = async (userId, steamId64, profile = {}) => {
  const safeUserId = String(userId || "").trim();
  const safeSteamId64 = validateSteamId64(steamId64);
  const safeProfile = normalizeSteamProfile(profile);

  const currentUser = await userRepo.getById(safeUserId);
  if (!currentUser) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  let mergedFromUserId = null;
  const steamOwner = await userRepo.getBySteamId64(safeSteamId64);

  if (steamOwner && steamOwner.id !== safeUserId) {
    const canMerge =
      isSteamManagedEmail(steamOwner.email) && !isSteamManagedEmail(currentUser.email);

    if (!canMerge) {
      throw new AppError(
        "This Steam account is already linked to another user.",
        409,
        "STEAM_ALREADY_LINKED"
      );
    }

    await userRepo.mergeUserData(steamOwner.id, safeUserId);
    await userRepo.updateSteamProfileById(steamOwner.id, { steamId64: null });
    mergedFromUserId = steamOwner.id;
  }

  const updated = await userRepo.updateSteamProfileById(safeUserId, {
    steamId64: safeSteamId64,
    displayName: safeProfile.displayName,
    avatarUrl: safeProfile.avatarUrl
  });

  return {
    mergedFromUserId,
    user: toAuthUserFromProfileRow(updated, safeSteamId64)
  };
};

exports.unlinkSteamFromUser = async (userId) => {
  const safeUserId = String(userId || "").trim();
  const currentUser = await userRepo.getById(safeUserId);
  if (!currentUser) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  if (!String(currentUser.steam_id64 || "").trim()) {
    return {
      user: toAuthUserFromProfileRowWithoutSteam(currentUser),
      disconnected: false
    };
  }

  if (isSteamManagedEmail(currentUser.email)) {
    throw new AppError(
      "Cannot disconnect Steam from a Steam-only account. Sign in with email/password first.",
      409,
      "STEAM_DISCONNECT_FORBIDDEN"
    );
  }

  const updated = await userRepo.updateSteamProfileById(safeUserId, {
    steamId64: null
  });

  return {
    user: toAuthUserFromProfileRowWithoutSteam(updated),
    disconnected: true
  };
};

exports.logoutAllSessions = async ({ authProvider, authToken } = {}) => {
  const provider = String(authProvider || "").trim().toLowerCase();
  const token = String(authToken || "").trim();

  if (provider !== "supabase" || !token) {
    return { revoked: false, unsupported: true };
  }

  const { error } = await supabaseAdmin.auth.admin.signOut(token, "global");
  if (error) {
    throw new AppError("Failed to sign out from all sessions", 500, "LOGOUT_ALL_FAILED");
  }

  return { revoked: true, unsupported: false };
};

exports.deleteUserAccount = async (userId) => {
  const safeUserId = String(userId || "").trim();
  const user = await userRepo.getById(safeUserId);
  if (!user) {
    throw new AppError("User not found", 404, "USER_NOT_FOUND");
  }

  const { error } = await supabaseAdmin.auth.admin.deleteUser(safeUserId);
  if (error) {
    throw new AppError("Failed to delete account", 500, "DELETE_ACCOUNT_FAILED");
  }
};

exports.isSteamManagedEmail = isSteamManagedEmail;

const crypto = require("node:crypto");
const { supabaseAdmin, supabaseAuthClient } = require("../config/supabase");
const { authEmailRedirectTo, appAuthSecret } = require("../config/env");
const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const STEAM_MANAGED_EMAIL_REGEX = /^steam_\d{17}@steam\.local$/i;

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function isSteamManagedEmail(email) {
  return STEAM_MANAGED_EMAIL_REGEX.test(normalizeEmail(email));
}

function validateEmail(email) {
  const normalized = normalizeEmail(email);
  if (!EMAIL_REGEX.test(normalized)) {
    throw new AppError("A valid email is required", 400, "INVALID_EMAIL");
  }
  return normalized;
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

function isDuplicateUserError(message) {
  return /already\s+registered|duplicate|unique/i.test(String(message || ""));
}

function isEmailNotConfirmedMessage(message) {
  return /email\s+not\s+confirmed/i.test(String(message || ""));
}

function isRateLimitMessage(message) {
  return /rate\s*limit|too\s*many\s*requests/i.test(String(message || ""));
}

function isUserNotFoundMessage(message) {
  return /user\s+not\s+found/i.test(String(message || ""));
}

exports.register = async (email, password) => {
  const safeEmail = validateEmail(email);
  const safePassword = validatePassword(password);

  const { data, error } = await supabaseAuthClient.auth.signUp({
    email: safeEmail,
    password: safePassword,
    options: {
      emailRedirectTo: authEmailRedirectTo
    }
  });

  if (error) {
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

  return {
    user: data.user,
    session: data.session,
    requiresEmailConfirmation: !data.session,
    message: data.session
      ? "Registration successful."
      : "Registration successful. Check your inbox for a confirmation link before login."
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

exports.resendConfirmation = async (email) => {
  const safeEmail = validateEmail(email);

  const { error } = await supabaseAuthClient.auth.resend({
    type: "signup",
    email: safeEmail,
    options: {
      emailRedirectTo: authEmailRedirectTo
    }
  });

  if (error) {
    if (isRateLimitMessage(error.message)) {
      throw new AppError(
        "Too many resend attempts. Try again shortly.",
        429,
        "RATE_LIMITED"
      );
    }

    if (!isUserNotFoundMessage(error.message)) {
      throw new AppError(
        "Could not resend confirmation email right now. Try again later.",
        500,
        "RESEND_FAILED"
      );
    }
  }

  return {
    message:
      "If this account exists and is not confirmed, a confirmation link has been sent. Check inbox and spam."
  };
};

exports.loginWithSteam = async (steamId64, profile = {}) => {
  const safeSteamId64 = validateSteamId64(steamId64);
  const safeProfile = normalizeSteamProfile(profile);
  const steamEmail = buildSteamEmail(safeSteamId64);
  const steamPassword = buildSteamPassword(safeSteamId64);

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
    if (error && !isDuplicateUserError(error.message)) {
      throw new AppError("Unable to create Steam account", 500, "STEAM_ACCOUNT_CREATE_FAILED");
    }

    const authUser = data?.user;
    if (authUser?.id) {
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

  const linkResult = await exports.linkSteamToUser(
    profileRow.id,
    safeSteamId64,
    safeProfile
  );
  return {
    user: linkResult.user
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

exports.isSteamManagedEmail = isSteamManagedEmail;

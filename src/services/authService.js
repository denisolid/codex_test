const { supabaseAdmin, supabaseAuthClient } = require("../config/supabase");
const { authEmailRedirectTo } = require("../config/env");
const AppError = require("../utils/AppError");

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
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

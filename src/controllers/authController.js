const asyncHandler = require("../utils/asyncHandler");
const authService = require("../services/authService");
const authMiddleware = require("../middleware/authMiddleware");
const userRepo = require("../repositories/userRepository");
const {
  setAuthCookie,
  clearAuthCookie,
  AUTH_COOKIE_NAME
} = require("../utils/authCookie");
const {
  createAppSessionToken,
  isAppSessionToken,
  verifyAppSessionToken
} = require("../utils/appSessionToken");
const steamAuthService = require("../services/steamAuthService");
const planService = require("../services/planService");
const { getCookieValue } = require("../utils/cookies");
const AppError = require("../utils/AppError");
const { frontendOrigin, frontendOrigins, apiPublicUrl } = require("../config/env");

function parseHostname(hostRaw) {
  try {
    return new URL(`http://${String(hostRaw || "").trim()}`).hostname;
  } catch (_err) {
    return "";
  }
}

function isLocalHostname(hostname) {
  const safe = String(hostname || "").trim().toLowerCase();
  return safe === "localhost" || safe === "127.0.0.1" || safe === "::1" || safe === "0.0.0.0";
}

function resolveApiOrigin(req) {
  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  const protocol = forwardedProto || req.protocol || "http";
  const host = req.get("host");
  const requestOrigin = `${protocol}://${host}`;

  if (apiPublicUrl && !isLocalHostname(parseHostname(host))) {
    return String(apiPublicUrl).replace(/\/+$/, "");
  }

  return requestOrigin;
}

function resolveFrontendTarget(nextRaw) {
  const fallback = `${frontendOrigin.replace(/\/+$/, "")}/auth-callback.html`;
  const safeNext = String(nextRaw || "").trim();

  if (!safeNext) {
    return fallback;
  }

  try {
    const parsed = new URL(safeNext);
    if (!frontendOrigins.includes(parsed.origin)) {
      return fallback;
    }
    return parsed.toString();
  } catch (_err) {
    if (!safeNext.startsWith("/")) {
      return fallback;
    }

    return `${frontendOrigin.replace(/\/+$/, "")}${safeNext}`;
  }
}

function buildSteamErrorRedirect(targetUrl, messageCode) {
  const url = new URL(targetUrl);
  url.searchParams.set("steam", "1");
  url.searchParams.set("error", String(messageCode || "steam_auth_failed"));
  return url.toString();
}

function encodeLinkStateNext(nextUrl) {
  return Buffer.from(String(nextUrl || ""), "utf8").toString("base64url");
}

function decodeLinkStateNext(encoded) {
  try {
    return Buffer.from(String(encoded || ""), "base64url").toString("utf8");
  } catch (_err) {
    return "";
  }
}

async function resolveSteamLinkActor(req) {
  const authHeader = String(req.headers.authorization || "");
  const bearerToken = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7).trim()
    : "";
  const cookieToken = getCookieValue(req.headers.cookie, AUTH_COOKIE_NAME) || "";
  const queryToken = String(req.query.accessToken || "").trim();
  const token = bearerToken || cookieToken || queryToken;

  if (!token) {
    throw new AppError("Unauthorized", 401, "UNAUTHORIZED");
  }

  if (isAppSessionToken(token)) {
    const payload = verifyAppSessionToken(token);
    const row = await userRepo.getById(payload.sub);
    if (!row) {
      throw new AppError("Unauthorized", 401, "USER_NOT_FOUND");
    }

    return {
      userId: row.id,
      email: row.email
    };
  }

  const authUser = await authService.getUserByAccessToken(token);
  await userRepo.ensureExists(authUser.id, authUser.email);

  return {
    userId: authUser.id,
    email: authUser.email
  };
}

exports.register = asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  const data = await authService.register(email, password);

  if (data?.user?.id && data?.user?.email) {
    await userRepo.ensureExists(data.user.id, data.user.email);
  }

  res.status(201).json(data);
});

exports.login = asyncHandler(async (req, res) => {
  const { email, password } = req.body;
  const data = await authService.login(email, password);
  setAuthCookie(res, data.accessToken);
  res.json({ user: data.user, accessToken: data.accessToken });
});

exports.createSession = asyncHandler(async (req, res) => {
  const { accessToken } = req.body;
  const user = await authService.getUserByAccessToken(accessToken);
  await userRepo.ensureExists(user.id, user.email);
  setAuthCookie(res, accessToken);
  res.json({ user, accessToken });
});

exports.steamStart = asyncHandler(async (req, res) => {
  const next = resolveFrontendTarget(req.query.next);
  const apiOrigin = resolveApiOrigin(req);
  const callbackUrl = new URL("/api/auth/steam/callback", `${apiOrigin}/`);
  callbackUrl.searchParams.set("next", next);

  const steamUrl = steamAuthService.buildSteamStartUrl({
    callbackUrl: callbackUrl.toString(),
    realm: apiOrigin
  });

  res.redirect(302, steamUrl);
});

exports.steamLinkStart = asyncHandler(async (req, res) => {
  const actor = await resolveSteamLinkActor(req);
  const next = resolveFrontendTarget(req.query.next);
  const apiOrigin = resolveApiOrigin(req);
  const stateToken = createAppSessionToken(
    {
      sub: actor.userId,
      email: encodeLinkStateNext(next),
      provider: "steam_link"
    },
    { expiresInSeconds: 10 * 60 }
  );

  const callbackUrl = new URL("/api/auth/steam/link/callback", `${apiOrigin}/`);
  callbackUrl.searchParams.set("state", stateToken);

  const steamUrl = steamAuthService.buildSteamStartUrl({
    callbackUrl: callbackUrl.toString(),
    realm: apiOrigin
  });

  res.redirect(302, steamUrl);
});

exports.steamCallback = asyncHandler(async (req, res) => {
  const target = resolveFrontendTarget(req.query.next);

  try {
    const { steamId64 } = await steamAuthService.verifySteamAssertion(req.query, {
      timeoutMs: 10000
    });
    const profile = await steamAuthService.fetchSteamProfile(steamId64, {
      timeoutMs: 10000
    });

    const data = await authService.loginWithSteam(steamId64, profile);
    const accessToken = createAppSessionToken({
      sub: data.user.id,
      email: data.user.email,
      provider: "steam"
    });

    setAuthCookie(res, accessToken);

    const redirectUrl = new URL(target);
    const hashParams = new URLSearchParams({
      accessToken,
      provider: "steam"
    });
    if (data.isNewSteamUser) {
      hashParams.set("steamOnboarding", "1");
    }
    redirectUrl.hash = hashParams.toString();
    res.redirect(302, redirectUrl.toString());
  } catch (err) {
    const code = String(err?.code || "steam_auth_failed").toLowerCase();
    res.redirect(302, buildSteamErrorRedirect(target, code));
  }
});

exports.steamLinkCallback = asyncHandler(async (req, res) => {
  const rawState = String(req.query.state || "").trim();
  let target = resolveFrontendTarget("");

  try {
    if (!rawState) {
      throw new AppError("Missing Steam link state", 400, "STEAM_LINK_STATE_MISSING");
    }

    const statePayload = verifyAppSessionToken(rawState);
    if (statePayload.provider !== "steam_link") {
      throw new AppError("Invalid Steam link state", 401, "STEAM_LINK_STATE_INVALID");
    }

    target = resolveFrontendTarget(decodeLinkStateNext(statePayload.email));

    const { steamId64 } = await steamAuthService.verifySteamAssertion(req.query, {
      timeoutMs: 10000
    });
    const profile = await steamAuthService.fetchSteamProfile(steamId64, {
      timeoutMs: 10000
    });

    const result = await authService.linkSteamToUser(
      statePayload.sub,
      steamId64,
      profile
    );

    const redirectUrl = new URL(target);
    redirectUrl.searchParams.set("steam", "1");
    redirectUrl.searchParams.set("linkedSteam", "1");
    if (result.mergedFromUserId) {
      redirectUrl.searchParams.set("merged", "1");
    }

    res.redirect(302, redirectUrl.toString());
  } catch (err) {
    const code = String(err?.code || "steam_link_failed").toLowerCase();
    res.redirect(302, buildSteamErrorRedirect(target, code));
  }
});

exports.resendConfirmation = asyncHandler(async (req, res) => {
  const { email } = req.body;
  const result = await authService.resendConfirmation(email);
  res.json(result);
});

exports.logout = asyncHandler(async (_req, res) => {
  clearAuthCookie(res);
  res.status(204).send();
});

exports.me = [
  authMiddleware,
  asyncHandler(async (req, res) => {
    const profileRow = await userRepo.getById(req.userId);
    const metadata = req.authUser?.user_metadata || {};
    const steamId64 = profileRow?.steam_id64 || metadata.steam_id64 || null;
    const planTier = planService.normalizePlanTier(profileRow?.plan_tier);
    const traderModeUnlocked = Boolean(profileRow?.trader_mode_unlocked);
    const entitlements = planService.getEntitlements(planTier, {
      traderModeUnlocked
    });

    const emailConfirmed =
      req.authProvider === "app"
        ? true
        : Boolean(req.authUser?.email_confirmed_at || req.authUser?.confirmed_at);

    res.json({
      user: req.authUser,
      emailConfirmed,
      profile: {
        steamId64,
        displayName: profileRow?.display_name || metadata.display_name || null,
        avatarUrl: profileRow?.avatar_url || metadata.avatar_url || null,
        linkedSteam: Boolean(steamId64),
        publicPortfolioEnabled: profileRow?.public_portfolio_enabled !== false,
        ownershipAlertsEnabled: profileRow?.ownership_alerts_enabled !== false,
        planTier,
        billingStatus: profileRow?.billing_status || "inactive",
        planSeats: Number(profileRow?.plan_seats || 1),
        planStartedAt: profileRow?.plan_started_at || null,
        traderModeUnlocked,
        traderModeUnlockedAt: profileRow?.trader_mode_unlocked_at || null,
        traderModeUnlockSource: profileRow?.trader_mode_unlock_source || null,
        entitlements,
        provider:
          metadata.provider ||
          (Boolean(steamId64) ? "steam" : "email")
      }
    });
  })
];

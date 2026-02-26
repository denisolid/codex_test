const asyncHandler = require("../utils/asyncHandler");
const authService = require("../services/authService");
const authMiddleware = require("../middleware/authMiddleware");
const userRepo = require("../repositories/userRepository");
const { setAuthCookie, clearAuthCookie } = require("../utils/authCookie");
const { createAppSessionToken } = require("../utils/appSessionToken");
const steamAuthService = require("../services/steamAuthService");
const { frontendOrigin, frontendOrigins, apiPublicUrl } = require("../config/env");

function resolveApiOrigin(req) {
  if (apiPublicUrl) {
    return String(apiPublicUrl).replace(/\/+$/, "");
  }

  const forwardedProto = String(req.headers["x-forwarded-proto"] || "").split(",")[0].trim();
  const protocol = forwardedProto || req.protocol || "http";
  const host = req.get("host");
  return `${protocol}://${host}`;
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
    redirectUrl.hash = `accessToken=${encodeURIComponent(accessToken)}&provider=steam`;
    res.redirect(302, redirectUrl.toString());
  } catch (err) {
    const code = String(err?.code || "steam_auth_failed").toLowerCase();
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
    const emailConfirmed =
      req.authProvider === "app"
        ? true
        : Boolean(req.authUser?.email_confirmed_at || req.authUser?.confirmed_at);

    res.json({ user: req.authUser, emailConfirmed });
  })
];

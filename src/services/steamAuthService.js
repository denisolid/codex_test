const AppError = require("../utils/AppError");
const { steamWebApiKey } = require("../config/env");

const STEAM_OPENID_ENDPOINT = "https://steamcommunity.com/openid/login";
const OPENID_NS = "http://specs.openid.net/auth/2.0";
const OPENID_IDENTIFIER_SELECT = `${OPENID_NS}/identifier_select`;
const STEAM_CLAIMED_ID_REGEX = /^https?:\/\/steamcommunity\.com\/openid\/id\/(\d{17})$/i;

function stripTrailingSlash(value) {
  return String(value || "").replace(/\/+$/, "");
}

function normalizeTimeoutMs(value, fallback = 10000) {
  const timeoutMs = Number(value);
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return fallback;
  }
  return Math.floor(timeoutMs);
}

function extractClaimedSteamId(claimedId) {
  const match = String(claimedId || "").trim().match(STEAM_CLAIMED_ID_REGEX);
  if (!match) {
    throw new AppError("Invalid Steam identity response", 401, "STEAM_INVALID_ID");
  }

  return match[1];
}

async function postOpenIdVerification(params, options = {}) {
  const timeoutMs = normalizeTimeoutMs(options.timeoutMs);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(STEAM_OPENID_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params.toString(),
      signal: controller.signal
    });

    if (!res.ok) {
      throw new AppError("Steam login verification failed", 502, "STEAM_VERIFY_FAILED");
    }

    return res.text();
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new AppError("Steam login verification timed out", 504, "STEAM_VERIFY_TIMEOUT");
    }
    if (err instanceof AppError) {
      throw err;
    }
    throw new AppError("Steam login verification failed", 502, "STEAM_VERIFY_FAILED");
  } finally {
    clearTimeout(timeout);
  }
}

exports.buildSteamStartUrl = ({ callbackUrl, realm }) => {
  const safeCallbackUrl = String(callbackUrl || "").trim();
  const safeRealm = stripTrailingSlash(realm);

  if (!safeCallbackUrl) {
    throw new AppError("Steam callback URL is required", 500, "STEAM_CONFIG_ERROR");
  }

  if (!safeRealm) {
    throw new AppError("Steam realm is required", 500, "STEAM_CONFIG_ERROR");
  }

  const params = new URLSearchParams({
    "openid.ns": OPENID_NS,
    "openid.mode": "checkid_setup",
    "openid.return_to": safeCallbackUrl,
    "openid.realm": safeRealm,
    "openid.identity": OPENID_IDENTIFIER_SELECT,
    "openid.claimed_id": OPENID_IDENTIFIER_SELECT
  });

  return `${STEAM_OPENID_ENDPOINT}?${params.toString()}`;
};

exports.verifySteamAssertion = async (query = {}, options = {}) => {
  const mode = String(query["openid.mode"] || "").trim();
  if (mode !== "id_res") {
    throw new AppError("Steam login was cancelled", 401, "STEAM_LOGIN_CANCELLED");
  }

  const params = new URLSearchParams();
  Object.entries(query || {}).forEach(([key, value]) => {
    if (!String(key).startsWith("openid.")) return;
    params.set(String(key), String(value));
  });
  params.set("openid.mode", "check_authentication");

  const body = await postOpenIdVerification(params, options);
  if (!/is_valid\s*:\s*true/i.test(body)) {
    throw new AppError("Steam login verification failed", 401, "STEAM_VERIFY_FAILED");
  }

  const claimedId = query["openid.claimed_id"] || query["openid.identity"];
  const steamId64 = extractClaimedSteamId(claimedId);

  return { steamId64 };
};

exports.fetchSteamProfile = async (steamId64, options = {}) => {
  if (!steamWebApiKey) {
    return {
      displayName: null,
      avatarUrl: null
    };
  }

  const timeoutMs = normalizeTimeoutMs(options.timeoutMs);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const params = new URLSearchParams({
      key: steamWebApiKey,
      steamids: String(steamId64)
    });

    const res = await fetch(
      `https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?${params.toString()}`,
      {
        method: "GET",
        signal: controller.signal
      }
    );

    if (!res.ok) {
      return { displayName: null, avatarUrl: null };
    }

    const payload = await res.json().catch(() => ({}));
    const player = payload?.response?.players?.[0] || null;

    return {
      displayName: String(player?.personaname || "").trim() || null,
      avatarUrl:
        String(player?.avatarfull || player?.avatarmedium || player?.avatar || "").trim() ||
        null
    };
  } catch (_err) {
    return { displayName: null, avatarUrl: null };
  } finally {
    clearTimeout(timeout);
  }
};

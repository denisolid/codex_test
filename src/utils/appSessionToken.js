const crypto = require("node:crypto");
const env = require("../config/env");
const AppError = require("./AppError");

const APP_TOKEN_PREFIX = "app_";
const DEFAULT_EXPIRES_SECONDS = 60 * 60;

function encodeBase64Url(value) {
  return Buffer.from(String(value), "utf8").toString("base64url");
}

function decodeBase64Url(value) {
  return Buffer.from(String(value), "base64url").toString("utf8");
}

function sign(input) {
  return crypto
    .createHmac("sha256", env.appAuthSecret)
    .update(input)
    .digest("base64url");
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch (_err) {
    return null;
  }
}

function normalizeExpirySeconds(expiresInSeconds) {
  const seconds = Number(expiresInSeconds);
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return DEFAULT_EXPIRES_SECONDS;
  }
  return Math.floor(seconds);
}

exports.createAppSessionToken = (payload = {}, options = {}) => {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const expiresInSeconds = normalizeExpirySeconds(options.expiresInSeconds);

  const body = {
    sub: String(payload.sub || "").trim(),
    email: String(payload.email || "").trim(),
    provider: String(payload.provider || "app").trim(),
    iat: nowSeconds,
    exp: nowSeconds + expiresInSeconds
  };

  if (!body.sub) {
    throw new AppError("Invalid app session payload", 500, "APP_TOKEN_INVALID_PAYLOAD");
  }

  const encoded = encodeBase64Url(JSON.stringify(body));
  const signature = sign(encoded);
  return `${APP_TOKEN_PREFIX}${encoded}.${signature}`;
};

exports.isAppSessionToken = (token) => {
  return String(token || "").startsWith(APP_TOKEN_PREFIX);
};

exports.verifyAppSessionToken = (token) => {
  const raw = String(token || "");
  if (!raw.startsWith(APP_TOKEN_PREFIX)) {
    throw new AppError("Invalid token", 401, "INVALID_TOKEN");
  }

  const withoutPrefix = raw.slice(APP_TOKEN_PREFIX.length);
  const parts = withoutPrefix.split(".");
  if (parts.length !== 2) {
    throw new AppError("Invalid token", 401, "INVALID_TOKEN");
  }

  const [encodedPayload, signature] = parts;
  const expected = sign(encodedPayload);
  const expectedBytes = Buffer.from(expected);
  const signatureBytes = Buffer.from(signature);

  if (
    !signature ||
    expectedBytes.length !== signatureBytes.length ||
    !crypto.timingSafeEqual(expectedBytes, signatureBytes)
  ) {
    throw new AppError("Invalid token", 401, "INVALID_TOKEN");
  }

  const payload = safeJsonParse(decodeBase64Url(encodedPayload));
  if (!payload || !payload.sub || !payload.exp) {
    throw new AppError("Invalid token", 401, "INVALID_TOKEN");
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  if (Number(payload.exp) <= nowSeconds) {
    throw new AppError("Session expired", 401, "SESSION_EXPIRED");
  }

  return payload;
};

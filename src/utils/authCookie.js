const env = require("../config/env");

const AUTH_COOKIE_NAME = "accessToken";

const baseCookieOptions = {
  httpOnly: true,
  secure: env.nodeEnv === "production",
  sameSite: env.nodeEnv === "production" ? "none" : "lax",
  path: "/"
};

exports.AUTH_COOKIE_NAME = AUTH_COOKIE_NAME;

exports.setAuthCookie = (res, token) => {
  res.cookie(AUTH_COOKIE_NAME, token, {
    ...baseCookieOptions,
    maxAge: 60 * 60 * 1000
  });
};

exports.clearAuthCookie = (res) => {
  res.clearCookie(AUTH_COOKIE_NAME, baseCookieOptions);
};

const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");
const { AUTH_COOKIE_NAME } = require("../utils/authCookie");
const { getCookieValue } = require("../utils/cookies");

module.exports = async (req, _res, next) => {
  try {
    const authHeader = req.headers.authorization || "";
    const bearerToken = authHeader.startsWith("Bearer ")
      ? authHeader.slice(7)
      : null;
    const cookieToken = getCookieValue(req.headers.cookie, AUTH_COOKIE_NAME);
    const token = bearerToken || cookieToken;

    if (!token) {
      throw new AppError("Unauthorized", 401);
    }

    const { data, error } = await supabaseAdmin.auth.getUser(token);
    if (error || !data || !data.user) {
      throw new AppError("Invalid token", 401);
    }

    req.userId = data.user.id;
    req.authUser = data.user;

    const userProfile = await userRepo.getById(data.user.id);
    if (!userProfile) {
      throw new AppError("Unauthorized", 401, "USER_NOT_FOUND");
    }

    next();
  } catch (err) {
    next(err);
  }
};

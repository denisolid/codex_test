const { supabaseAdmin } = require("../config/supabase");
const AppError = require("../utils/AppError");
const userRepo = require("../repositories/userRepository");

module.exports = async (req, _res, next) => {
  try {
    const authHeader = req.headers.authorization || "";
    const token = authHeader.startsWith("Bearer ")
      ? authHeader.slice(7)
      : null;

    if (!token) {
      throw new AppError("Missing Bearer token", 401);
    }

    const { data, error } = await supabaseAdmin.auth.getUser(token);
    if (error || !data || !data.user) {
      throw new AppError("Invalid token", 401);
    }

    req.userId = data.user.id;
    req.authUser = data.user;

    await userRepo.ensureExists(data.user.id, data.user.email);
    next();
  } catch (err) {
    next(err);
  }
};

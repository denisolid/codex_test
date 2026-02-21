const { adminApiToken } = require("../config/env");
const AppError = require("../utils/AppError");

module.exports = (req, _res, next) => {
  if (!adminApiToken) {
    return next(new AppError("Admin API token is not configured", 503));
  }

  const headerToken = req.headers["x-admin-token"];
  if (!headerToken || headerToken !== adminApiToken) {
    return next(new AppError("Forbidden", 403));
  }

  return next();
};

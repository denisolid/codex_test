const AppError = require("../utils/AppError");

function ensureValidatedBag(req) {
  if (!req.validated || typeof req.validated !== "object") {
    req.validated = {};
  }
  return req.validated;
}

exports.requirePositiveIntParam =
  (paramName, options = {}) =>
  (req, _res, next) => {
    const storeAs = options.storeAs || paramName;
    const message = options.message || `Invalid ${paramName}`;
    const value = Number(req.params?.[paramName]);

    if (!Number.isInteger(value) || value <= 0) {
      return next(new AppError(message, 400, "VALIDATION_ERROR"));
    }

    ensureValidatedBag(req)[storeAs] = value;
    return next();
  };

exports.requireDigitsParam =
  (paramName, options = {}) =>
  (req, _res, next) => {
    const storeAs = options.storeAs || paramName;
    const message = options.message || `Invalid ${paramName}`;
    const value = String(req.params?.[paramName] || "").trim();

    if (!/^\d+$/.test(value)) {
      return next(new AppError(message, 400, "VALIDATION_ERROR"));
    }

    ensureValidatedBag(req)[storeAs] = value;
    return next();
  };

exports.parseOptionalNumericQuery =
  (queryName, options = {}) =>
  (req, _res, next) => {
    const storeAs = options.storeAs || queryName;
    const message = options.message || `${queryName} must be numeric`;
    const raw = req.query?.[queryName];

    if (raw == null || raw === "") {
      ensureValidatedBag(req)[storeAs] = undefined;
      return next();
    }

    const value = Number(raw);
    if (!Number.isFinite(value)) {
      return next(new AppError(message, 400, "VALIDATION_ERROR"));
    }

    ensureValidatedBag(req)[storeAs] = value;
    return next();
  };

const AppError = require("../utils/AppError");

function createRateLimiter({ windowMs, max, message }) {
  const hits = new Map();

  return (req, _res, next) => {
    const now = Date.now();
    const key = `${req.ip}:${req.path}`;
    const entry = hits.get(key);

    if (!entry || now > entry.resetAt) {
      hits.set(key, { count: 1, resetAt: now + windowMs });
      return next();
    }

    entry.count += 1;
    if (entry.count > max) {
      return next(new AppError(message || "Too many requests", 429));
    }

    return next();
  };
}

module.exports = {
  createRateLimiter
};

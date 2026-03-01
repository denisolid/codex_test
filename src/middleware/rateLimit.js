const AppError = require("../utils/AppError");

function createRateLimiter({ windowMs, max, message }) {
  const hits = new Map();
  const safeWindowMs = Math.max(Number(windowMs) || 60000, 1000);
  const safeMax = Math.max(Number(max) || 1, 1);
  const cleanupIntervalMs = Math.max(Math.floor(safeWindowMs / 2), 1000);
  const maxEntries = 50000;
  let lastCleanupAt = 0;

  function normalizePath(pathname) {
    return String(pathname || "/")
      .replace(
        /[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi,
        ":id"
      )
      .replace(/\/\d+(?=\/|$)/g, "/:id");
  }

  function cleanup(now) {
    if (now - lastCleanupAt < cleanupIntervalMs) {
      return;
    }

    lastCleanupAt = now;
    for (const [key, entry] of hits.entries()) {
      if (!entry || now > entry.resetAt) {
        hits.delete(key);
      }
    }

    // Safety valve in case clients spray unique route ids to grow memory usage.
    if (hits.size <= maxEntries) {
      return;
    }

    const overflow = hits.size - maxEntries;
    const sortedKeys = Array.from(hits.entries())
      .sort((a, b) => Number(a[1]?.resetAt || 0) - Number(b[1]?.resetAt || 0))
      .slice(0, overflow)
      .map(([key]) => key);

    for (const key of sortedKeys) {
      hits.delete(key);
    }
  }

  return (req, res, next) => {
    const now = Date.now();
    cleanup(now);

    const routeKey = normalizePath(`${req.baseUrl || ""}${req.path || ""}`);
    const key = `${req.ip}:${String(req.method || "GET").toUpperCase()}:${routeKey}`;
    const entry = hits.get(key);

    if (!entry || now > entry.resetAt) {
      hits.set(key, { count: 1, resetAt: now + safeWindowMs });
      return next();
    }

    entry.count += 1;
    if (entry.count > safeMax) {
      const retryAfterSeconds = Math.max(
        Math.ceil((entry.resetAt - now) / 1000),
        1
      );
      res.set("Retry-After", String(retryAfterSeconds));
      return next(new AppError(message || "Too many requests", 429, "RATE_LIMITED"));
    }

    return next();
  };
}

module.exports = {
  createRateLimiter
};

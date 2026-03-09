const { nodeEnv } = require("../config/env");

module.exports = (err, _req, res, _next) => {
  const status = err.statusCode || 500;
  const retryAfterSeconds = Math.max(
    Number(err?.retryAfterSeconds || 0) ||
      Math.ceil(Number(err?.retryAfterMs || 0) / 1000) ||
      0,
    0
  );
  const safeProductionCodes = new Set([
    "EMAIL_PROVIDER_NOT_CONFIGURED",
    "EMAIL_SEND_FAILED"
  ]);
  const exposeMessage =
    status < 500 ||
    nodeEnv !== "production" ||
    safeProductionCodes.has(String(err.code || "").trim().toUpperCase());
  const payload = {
    error: exposeMessage ? err.message || "Request failed" : "Internal Server Error"
  };

  if (err.code) {
    payload.code = err.code;
  }

  if (status === 429 && retryAfterSeconds > 0) {
    res.set("Retry-After", String(retryAfterSeconds));
  }

  res.status(status).json(payload);
};

const { nodeEnv } = require("../config/env");

module.exports = (err, _req, res, _next) => {
  const status = err.statusCode || 500;
  const exposeMessage = status < 500 || nodeEnv !== "production";
  const payload = {
    error: exposeMessage ? err.message || "Request failed" : "Internal Server Error"
  };

  if (err.code) {
    payload.code = err.code;
  }

  res.status(status).json(payload);
};

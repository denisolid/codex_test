module.exports = (err, _req, res, _next) => {
  const status = err.statusCode || 500;
  const payload = {
    error: err.message || "Internal Server Error"
  };

  if (err.code) {
    payload.code = err.code;
  }

  res.status(status).json(payload);
};

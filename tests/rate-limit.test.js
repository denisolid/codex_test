const test = require("node:test");
const assert = require("node:assert/strict");

const { createRateLimiter } = require("../src/middleware/rateLimit");

function createResRecorder() {
  return {
    headers: {},
    set(name, value) {
      this.headers[String(name)] = String(value);
    }
  };
}

function runLimiter(limiter, req, res) {
  return new Promise((resolve) => {
    limiter(req, res, (err) => resolve(err || null));
  });
}

test("rate limiter normalizes numeric path params and applies a shared key", async () => {
  const limiter = createRateLimiter({
    windowMs: 60_000,
    max: 1,
    message: "Too many requests"
  });
  const res = createResRecorder();

  const firstErr = await runLimiter(
    limiter,
    {
      ip: "203.0.113.10",
      method: "GET",
      baseUrl: "/api/transactions",
      path: "/101"
    },
    res
  );
  assert.equal(firstErr, null);

  const secondErr = await runLimiter(
    limiter,
    {
      ip: "203.0.113.10",
      method: "GET",
      baseUrl: "/api/transactions",
      path: "/202"
    },
    res
  );

  assert.ok(secondErr);
  assert.equal(secondErr.statusCode, 429);
  assert.equal(secondErr.code, "RATE_LIMITED");
  assert.ok(Number(res.headers["Retry-After"]) >= 1);
});

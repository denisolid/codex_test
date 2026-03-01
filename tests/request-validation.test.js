const test = require("node:test");
const assert = require("node:assert/strict");

const {
  requirePositiveIntParam,
  requireDigitsParam,
  parseOptionalNumericQuery
} = require("../src/middleware/requestValidation");

function invoke(middleware, req = {}) {
  return new Promise((resolve) => {
    middleware(req, {}, (err) => resolve({ err: err || null, req }));
  });
}

test("requirePositiveIntParam stores validated integer into req.validated", async () => {
  const middleware = requirePositiveIntParam("skinId", {
    message: "Invalid item id"
  });
  const result = await invoke(middleware, { params: { skinId: "42" } });

  assert.equal(result.err, null);
  assert.equal(result.req.validated.skinId, 42);
});

test("requireDigitsParam rejects non-digit values", async () => {
  const middleware = requireDigitsParam("steamItemId", {
    message: "Invalid steam item id"
  });
  const result = await invoke(middleware, { params: { steamItemId: "12A34" } });

  assert.ok(result.err);
  assert.equal(result.err.statusCode, 400);
  assert.equal(result.err.code, "VALIDATION_ERROR");
});

test("parseOptionalNumericQuery stores undefined for empty query and number for valid query", async () => {
  const middleware = parseOptionalNumericQuery("commissionPercent");

  const empty = await invoke(middleware, { query: { commissionPercent: "" } });
  assert.equal(empty.err, null);
  assert.equal(empty.req.validated.commissionPercent, undefined);

  const numeric = await invoke(middleware, { query: { commissionPercent: "13.5" } });
  assert.equal(numeric.err, null);
  assert.equal(numeric.req.validated.commissionPercent, 13.5);
});

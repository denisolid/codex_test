const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const planService = require("../src/services/planService");

test("full access has no manual scanner refresh cooldown", () => {
  const nowMs = Date.now();
  const policy = planService.canRefreshScanner("full_access", nowMs, { nowMs: nowMs + 15 * 1000 });

  assert.equal(policy.allowed, true);
  assert.equal(policy.intervalMinutes, 0);
  assert.equal(policy.retryAfterMs, 0);
  assert.equal(policy.retryAfterMinutes, 0);
});

test("free plan still enforces manual scanner refresh cooldown", () => {
  const nowMs = Date.now();
  const policy = planService.canRefreshScanner("free", nowMs, { nowMs: nowMs + 60 * 1000 });

  assert.equal(policy.allowed, false);
  assert.equal(policy.intervalMinutes, 720);
  assert.equal(Number(policy.retryAfterMs || 0) > 0, true);
  assert.equal(Number(policy.retryAfterMinutes || 0) > 0, true);
});

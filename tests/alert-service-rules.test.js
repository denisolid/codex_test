const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  __testables: {
    evaluateTargetTrigger,
    evaluatePercentChangeTrigger,
    isOnCooldown
  }
} = require("../src/services/alertService");

test("target trigger supports crossing logic for both direction", () => {
  assert.equal(evaluateTargetTrigger("both", 100, 102, 95), true);
  assert.equal(evaluateTargetTrigger("both", 100, 98, 105), true);
  assert.equal(evaluateTargetTrigger("both", 100, 101, 102), false);
});

test("percent trigger supports up/down/both", () => {
  assert.equal(evaluatePercentChangeTrigger("up", 5, 6), true);
  assert.equal(evaluatePercentChangeTrigger("down", 5, -6), true);
  assert.equal(evaluatePercentChangeTrigger("both", 5, -6), true);
  assert.equal(evaluatePercentChangeTrigger("both", 5, 2), false);
});

test("cooldown check returns true only inside cooldown window", () => {
  const nowMs = Date.now();
  const recent = new Date(nowMs - 2 * 60 * 1000).toISOString();
  const old = new Date(nowMs - 20 * 60 * 1000).toISOString();

  assert.equal(isOnCooldown(recent, 5, nowMs), true);
  assert.equal(isOnCooldown(old, 5, nowMs), false);
});

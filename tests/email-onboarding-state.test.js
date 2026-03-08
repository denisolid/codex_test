const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const servicePath = path.resolve(__dirname, "../src/services/emailOnboardingService.js");
const { resolveOnboardingState } = require(servicePath);

test("steam placeholder email always requires onboarding", () => {
  const state = resolveOnboardingState({
    userProfile: {
      email: "steam_76561198170121307@steam.local",
      steam_id64: "76561198170121307",
      email_verified: true,
      onboarding_completed: true,
      plan: "free",
      plan_status: "active"
    },
    authUser: {
      email_confirmed_at: new Date().toISOString(),
      user_metadata: { provider: "steam" }
    }
  });

  assert.equal(state.emailVerified, false);
  assert.equal(state.onboardingCompleted, false);
  assert.equal(state.onboardingRequired, true);
});

test("steam account with real verified email passes onboarding", () => {
  const state = resolveOnboardingState({
    userProfile: {
      email: "trader@skinalpha.app",
      steam_id64: "76561198170121307",
      email_verified: true,
      onboarding_completed: true,
      plan: "free",
      plan_status: "active"
    },
    authUser: {
      email_confirmed_at: new Date().toISOString(),
      user_metadata: { provider: "steam" }
    }
  });

  assert.equal(state.emailVerified, true);
  assert.equal(state.onboardingCompleted, true);
  assert.equal(state.onboardingRequired, false);
});

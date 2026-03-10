const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const controllerPath = path.resolve(__dirname, "../src/controllers/authController.js");
const authServicePath = path.resolve(__dirname, "../src/services/authService.js");
const authMiddlewarePath = path.resolve(__dirname, "../src/middleware/authMiddleware.js");
const userRepoPath = path.resolve(__dirname, "../src/repositories/userRepository.js");
const authCookiePath = path.resolve(__dirname, "../src/utils/authCookie.js");
const appTokenPath = path.resolve(__dirname, "../src/utils/appSessionToken.js");
const steamAuthServicePath = path.resolve(__dirname, "../src/services/steamAuthService.js");
const planServicePath = path.resolve(__dirname, "../src/services/planService.js");
const emailOnboardingServicePath = path.resolve(__dirname, "../src/services/emailOnboardingService.js");
const cookiesPath = path.resolve(__dirname, "../src/utils/cookies.js");
const envPath = path.resolve(__dirname, "../src/config/env.js");

function primeModule(modulePath, exportsValue) {
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports: exportsValue
  };
}

function clearModule(modulePath) {
  delete require.cache[modulePath];
}

function clearAllModules() {
  [
    controllerPath,
    authServicePath,
    authMiddlewarePath,
    userRepoPath,
    authCookiePath,
    appTokenPath,
    steamAuthServicePath,
    planServicePath,
    emailOnboardingServicePath,
    cookiesPath,
    envPath
  ].forEach(clearModule);
}

async function runHandler(handler, req) {
  return await new Promise((resolve) => {
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    const res = {
      statusCode: 200,
      status(code) {
        this.statusCode = code;
        return this;
      },
      json(payload) {
        this.payload = payload;
        finish({ res: this, err: null });
        return this;
      }
    };

    handler(req, res, (err) => finish({ res, err: err || null }));
    setTimeout(() => {
      finish({
        res,
        err: new Error("Handler did not finish")
      });
    }, 200);
  });
}

test("bootstrap returns compact auth payload for routing/onboarding decisions", async () => {
  clearAllModules();

  primeModule(envPath, {
    frontendOrigin: "http://localhost:5173",
    frontendOrigins: ["http://localhost:5173"],
    apiPublicUrl: ""
  });
  primeModule(authServicePath, {});
  primeModule(authMiddlewarePath, (_req, _res, next) => next());
  primeModule(userRepoPath, {
    getById: async () => null
  });
  primeModule(authCookiePath, {
    AUTH_COOKIE_NAME: "accessToken",
    setAuthCookie: () => {},
    clearAuthCookie: () => {}
  });
  primeModule(appTokenPath, {
    createAppSessionToken: () => "unused",
    isAppSessionToken: () => false,
    verifyAppSessionToken: () => ({})
  });
  primeModule(steamAuthServicePath, {});
  primeModule(planServicePath, {
    normalizePlanTier: (tier) => {
      const safe = String(tier || "free").trim().toLowerCase();
      if (safe === "pro" || safe === "team") return "full_access";
      if (safe === "api_advanced") return "api_advanced";
      return "free";
    },
    getEntitlements: (tier) => ({
      planTier: String(tier || "free"),
      advancedAnalytics: String(tier || "free") !== "free"
    }),
    isTestSubscriptionSwitcherEnabled: () => true
  });
  primeModule(emailOnboardingServicePath, {
    syncProfileVerificationState: async ({ userProfile }) => userProfile,
    resolveOnboardingState: ({ userProfile }) => ({
      email: userProfile.email,
      pendingEmail: userProfile.pending_email,
      emailVerified: true,
      onboardingCompleted: true,
      onboardingRequired: false,
      plan: "full_access",
      planStatus: "active"
    })
  });
  primeModule(cookiesPath, { getCookieValue: () => null });

  const ctrl = require(controllerPath);
  const bootstrapHandler = Array.isArray(ctrl.bootstrap) ? ctrl.bootstrap[1] : ctrl.bootstrap;
  const { res, err } = await runHandler(bootstrapHandler, {
    userId: "user-1",
    authUser: {
      id: "user-1",
      email: "trader@example.com",
      created_at: "2026-03-09T11:00:00.000Z",
      user_metadata: {
        provider: "steam",
        display_name: "Trader One",
        avatar_url: "https://cdn.example/avatar.png"
      }
    },
    userProfile: {
      id: "user-1",
      email: "trader@example.com",
      pending_email: null,
      steam_id64: "76561198000000000",
      display_name: "Trader One",
      avatar_url: "https://cdn.example/avatar.png",
      public_portfolio_enabled: true,
      ownership_alerts_enabled: true,
      plan_tier: "pro"
    }
  });

  assert.equal(err, null);
  assert.equal(res.payload?.user?.id, "user-1");
  assert.equal(res.payload?.profile?.planTier, "full_access");
  assert.equal(res.payload?.profile?.linkedSteam, true);
  assert.equal(res.payload?.profile?.onboardingRequired, false);
  assert.equal(res.payload?.profile?.entitlements?.advancedAnalytics, true);
  assert.equal(res.payload?.profile?.subscriptionSwitcherEnabled, true);
  assert.equal("billingStatus" in (res.payload?.profile || {}), true);
});

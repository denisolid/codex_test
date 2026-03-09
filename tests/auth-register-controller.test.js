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
const emailOnboardingServicePath = path.resolve(
  __dirname,
  "../src/services/emailOnboardingService.js"
);
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

function primeControllerDeps({ registerResult, ensureExists }) {
  primeModule(envPath, {
    frontendOrigin: "http://localhost:5173",
    frontendOrigins: ["http://localhost:5173"],
    apiPublicUrl: ""
  });

  primeModule(authServicePath, {
    register: async () => registerResult
  });

  primeModule(userRepoPath, {
    ensureExists
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

  primeModule(authMiddlewarePath, (_req, _res, next) => next());
  primeModule(steamAuthServicePath, {});
  primeModule(planServicePath, {});
  primeModule(emailOnboardingServicePath, {});
  primeModule(cookiesPath, { getCookieValue: () => null });
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

test("register ignores PROFILE_AUTH_USER_MISSING during profile sync", async () => {
  clearAllModules();

  const registerResult = {
    user: {
      id: "fake-or-obfuscated-user-id",
      email: "owner@example.com"
    },
    session: null,
    requiresEmailConfirmation: true
  };

  let ensureExistsCalls = 0;
  primeControllerDeps({
    registerResult,
    ensureExists: async () => {
      ensureExistsCalls += 1;
      const err = new Error("auth user missing");
      err.code = "PROFILE_AUTH_USER_MISSING";
      throw err;
    }
  });

  const ctrl = require(controllerPath);
  const { res, err } = await runHandler(ctrl.register, {
    body: { email: "owner@example.com", password: "hunter2" }
  });

  assert.equal(err, null);
  assert.equal(ensureExistsCalls, 1);
  assert.equal(res.statusCode, 201);
  assert.equal(res.payload?.requiresEmailConfirmation, true);
});

test("register still fails when ensureExists throws unexpected error", async () => {
  clearAllModules();

  primeControllerDeps({
    registerResult: {
      user: {
        id: "real-user-id",
        email: "owner@example.com"
      },
      session: null,
      requiresEmailConfirmation: true
    },
    ensureExists: async () => {
      throw new Error("db unavailable");
    }
  });

  const ctrl = require(controllerPath);
  const { err } = await runHandler(ctrl.register, {
    body: { email: "owner@example.com", password: "hunter2" }
  });

  assert.ok(err);
  assert.match(String(err.message || ""), /db unavailable/);
});

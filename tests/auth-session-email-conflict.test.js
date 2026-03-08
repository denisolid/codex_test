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
      cookies: [],
      statusCode: 200,
      cookie(name, value, options) {
        this.cookies.push({ name, value, options });
        return this;
      },
      clearCookie() {
        return this;
      },
      status(code) {
        this.statusCode = code;
        return this;
      },
      json(payload) {
        this.payload = payload;
        finish({ res: this, err: null });
        return this;
      },
      redirect(code, url) {
        this.redirected = { code, url };
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

function primeControllerDeps({
  authUser,
  ensureExists,
  getById,
  getByEmail,
  createAppToken
}) {
  primeModule(envPath, {
    frontendOrigin: "http://localhost:5173",
    frontendOrigins: ["http://localhost:5173"],
    apiPublicUrl: ""
  });

  primeModule(authServicePath, {
    getUserByAccessToken: async () => authUser
  });

  primeModule(userRepoPath, {
    ensureExists,
    getById,
    getByEmail
  });

  primeModule(authCookiePath, {
    AUTH_COOKIE_NAME: "accessToken",
    setAuthCookie: (res, token) => {
      res.cookie("accessToken", token, { httpOnly: true });
    },
    clearAuthCookie: () => {}
  });

  primeModule(appTokenPath, {
    createAppSessionToken: createAppToken,
    isAppSessionToken: () => false,
    verifyAppSessionToken: () => ({})
  });

  primeModule(authMiddlewarePath, (_req, _res, next) => next());
  primeModule(steamAuthServicePath, {});
  primeModule(planServicePath, {});
  primeModule(emailOnboardingServicePath, {});
  primeModule(cookiesPath, { getCookieValue: () => null });
}

test("createSession stores Supabase token when user profile upsert succeeds", async () => {
  clearAllModules();

  const authUser = {
    id: "google-user-1",
    email: "owner@example.com",
    app_metadata: { provider: "google" },
    user_metadata: {}
  };

  let ensureCalled = 0;
  primeControllerDeps({
    authUser,
    ensureExists: async () => {
      ensureCalled += 1;
    },
    getById: async () => null,
    getByEmail: async () => null,
    createAppToken: () => "unused-app-token"
  });

  const ctrl = require(controllerPath);
  const { res, err } = await runHandler(ctrl.createSession, {
    body: { accessToken: "google-access-token" }
  });

  assert.equal(err, null);
  assert.equal(ensureCalled, 1);
  assert.equal(res.payload?.accessToken, "google-access-token");
  assert.equal(res.payload?.user?.id, "google-user-1");
  assert.equal(res.cookies[0]?.value, "google-access-token");
});

test("createSession bridges to existing profile when OAuth email already exists", async () => {
  clearAllModules();

  const authUser = {
    id: "google-user-2",
    email: "owner@example.com",
    app_metadata: { provider: "google" },
    user_metadata: {}
  };
  const existingProfile = {
    id: "steam-user-1",
    email: "owner@example.com"
  };

  const conflictError = new Error("duplicate email");
  conflictError.code = "EMAIL_IN_USE";
  conflictError.existingUserId = existingProfile.id;

  let appTokenPayload = null;
  primeControllerDeps({
    authUser,
    ensureExists: async () => {
      throw conflictError;
    },
    getById: async (id) => (id === existingProfile.id ? existingProfile : null),
    getByEmail: async () => existingProfile,
    createAppToken: (payload) => {
      appTokenPayload = payload;
      return "app-bridge-token";
    }
  });

  const ctrl = require(controllerPath);
  const { res, err } = await runHandler(ctrl.createSession, {
    body: { accessToken: "google-access-token" }
  });

  assert.equal(err, null);
  assert.equal(res.payload?.accessToken, "app-bridge-token");
  assert.equal(res.payload?.user?.id, existingProfile.id);
  assert.equal(res.payload?.user?.email, existingProfile.email);
  assert.equal(res.cookies[0]?.value, "app-bridge-token");
  assert.equal(appTokenPayload?.sub, existingProfile.id);
  assert.equal(appTokenPayload?.email, existingProfile.email);
  assert.equal(appTokenPayload?.provider, "google");
});

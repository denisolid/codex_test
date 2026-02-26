const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const middlewarePath = path.resolve(__dirname, "../src/middleware/authMiddleware.js");
const supabasePath = path.resolve(__dirname, "../src/config/supabase.js");
const userRepoPath = path.resolve(__dirname, "../src/repositories/userRepository.js");
const appTokenPath = path.resolve(__dirname, "../src/utils/appSessionToken.js");

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

test("auth middleware accepts access token from HttpOnly cookie", async () => {
  clearModule(middlewarePath);
  clearModule(supabasePath);
  clearModule(userRepoPath);

  let seenToken = null;
  primeModule(supabasePath, {
    supabaseAdmin: {
      auth: {
        getUser: async (token) => {
          seenToken = token;
          return {
            data: { user: { id: "u-1", email: "u1@example.com" } },
            error: null
          };
        }
      }
    }
  });

  primeModule(userRepoPath, {
    getById: async (id) => ({
      id,
      email: "u1@example.com"
    })
  });

  const middleware = require(middlewarePath);
  const req = { headers: { cookie: "accessToken=cookie-token" } };
  let nextErr = null;

  await new Promise((resolve) => {
    middleware(req, {}, (err) => {
      nextErr = err || null;
      resolve();
    });
  });

  assert.equal(nextErr, null);
  assert.equal(seenToken, "cookie-token");
  assert.equal(req.userId, "u-1");
});

test("auth middleware rejects request when no bearer/cookie token is present", async () => {
  clearModule(middlewarePath);
  clearModule(supabasePath);
  clearModule(userRepoPath);

  primeModule(supabasePath, {
    supabaseAdmin: {
      auth: {
        getUser: async () => ({
          data: null,
          error: null
        })
      }
    }
  });
  primeModule(userRepoPath, {
    getById: async () => null
  });

  const middleware = require(middlewarePath);
  const req = { headers: {} };
  let nextErr = null;

  await new Promise((resolve) => {
    middleware(req, {}, (err) => {
      nextErr = err || null;
      resolve();
    });
  });

  assert.ok(nextErr);
  assert.equal(nextErr.statusCode, 401);
  assert.equal(nextErr.message, "Unauthorized");
});

test("auth middleware rejects request when user profile was deleted", async () => {
  clearModule(middlewarePath);
  clearModule(supabasePath);
  clearModule(userRepoPath);

  primeModule(supabasePath, {
    supabaseAdmin: {
      auth: {
        getUser: async () => ({
          data: { user: { id: "u-2", email: "u2@example.com" } },
          error: null
        })
      }
    }
  });

  primeModule(userRepoPath, {
    getById: async () => null
  });

  const middleware = require(middlewarePath);
  const req = { headers: { cookie: "accessToken=valid-token" } };
  let nextErr = null;

  await new Promise((resolve) => {
    middleware(req, {}, (err) => {
      nextErr = err || null;
      resolve();
    });
  });

  assert.ok(nextErr);
  assert.equal(nextErr.statusCode, 401);
  assert.equal(nextErr.message, "Unauthorized");
});

test("auth middleware accepts app session token for Steam auth flow", async () => {
  clearModule(middlewarePath);
  clearModule(supabasePath);
  clearModule(userRepoPath);
  clearModule(appTokenPath);

  primeModule(supabasePath, {
    supabaseAdmin: {
      auth: {
        getUser: async () => {
          throw new Error("supabase should not be called for app token");
        }
      }
    }
  });

  primeModule(userRepoPath, {
    getById: async () => ({
      id: "steam-user-1",
      email: "steam_76561198000000000@steam.local",
      steam_id64: "76561198000000000",
      display_name: "Steam Player",
      avatar_url: "https://cdn.example/avatar.jpg"
    })
  });

  const { createAppSessionToken } = require(appTokenPath);
  const token = createAppSessionToken({
    sub: "steam-user-1",
    email: "steam_76561198000000000@steam.local",
    provider: "steam"
  });

  const middleware = require(middlewarePath);
  const req = { headers: { cookie: `accessToken=${encodeURIComponent(token)}` } };
  let nextErr = null;

  await new Promise((resolve) => {
    middleware(req, {}, (err) => {
      nextErr = err || null;
      resolve();
    });
  });

  assert.equal(nextErr, null);
  assert.equal(req.userId, "steam-user-1");
  assert.equal(req.authProvider, "app");
  assert.equal(req.authUser?.user_metadata?.provider, "steam");
});

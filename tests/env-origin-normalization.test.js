const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

const envModulePath = path.resolve(__dirname, "../src/config/env.js");

const baseEnv = {
  SUPABASE_URL: "https://example.supabase.co",
  SUPABASE_ANON_KEY: "anon",
  SUPABASE_SERVICE_ROLE_KEY: "service-role",
  APP_AUTH_SECRET: "app-secret"
};

const managedKeys = [
  ...Object.keys(baseEnv),
  "FRONTEND_URL",
  "FRONTEND_ORIGINS",
  "FRONTEND_ORIGIN",
  "AUTH_EMAIL_REDIRECT_TO"
];

function loadEnv(overrides = {}) {
  const snapshot = new Map(managedKeys.map((key) => [key, process.env[key]]));
  const nextEnv = {
    ...baseEnv,
    FRONTEND_URL: "",
    FRONTEND_ORIGINS: "",
    FRONTEND_ORIGIN: "",
    AUTH_EMAIL_REDIRECT_TO: "",
    ...overrides
  };

  try {
    managedKeys.forEach((key) => {
      if (nextEnv[key] == null || nextEnv[key] === "") {
        delete process.env[key];
        return;
      }

      process.env[key] = String(nextEnv[key]);
    });

    delete require.cache[envModulePath];
    return require(envModulePath);
  } finally {
    delete require.cache[envModulePath];
    managedKeys.forEach((key) => {
      const previous = snapshot.get(key);
      if (previous == null) {
        delete process.env[key];
      } else {
        process.env[key] = previous;
      }
    });
  }
}

test("frontend origins are normalized to canonical origins", () => {
  const env = loadEnv({
    FRONTEND_URL: " https://skinalpha.app/ , https://www.skinalpha.app/login.html ",
    FRONTEND_ORIGINS: "https://staging.skinalpha.app/",
    FRONTEND_ORIGIN: "http://localhost:5173/"
  });

  assert.deepEqual(env.frontendOrigins, [
    "https://skinalpha.app",
    "https://www.skinalpha.app",
    "https://staging.skinalpha.app",
    "http://localhost:5173"
  ]);
  assert.equal(env.frontendOrigin, "https://skinalpha.app");
});

test("frontend origins fall back to localhost when allowlist is empty", () => {
  const env = loadEnv({
    FRONTEND_URL: "",
    FRONTEND_ORIGINS: "",
    FRONTEND_ORIGIN: ""
  });

  assert.deepEqual(env.frontendOrigins, ["http://localhost:5173"]);
  assert.equal(env.frontendOrigin, "http://localhost:5173");
});

const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";
process.env.APP_AUTH_SECRET = process.env.APP_AUTH_SECRET || "app-secret-for-tests";

const authServicePath = path.resolve(__dirname, "../src/services/authService.js");
const userRepoPath = path.resolve(__dirname, "../src/repositories/userRepository.js");
const supabasePath = path.resolve(__dirname, "../src/config/supabase.js");

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

function buildSupabaseStub() {
  return {
    supabaseAdmin: {
      auth: {
        admin: {
          createUser: async () => ({ data: null, error: null })
        },
        getUser: async () => ({ data: null, error: null })
      }
    },
    supabaseAuthClient: {
      auth: {
        signInWithPassword: async () => ({ data: null, error: null })
      }
    }
  };
}

test("linkSteamToUser merges steam-only duplicate into existing email account", async () => {
  clearModule(authServicePath);
  clearModule(userRepoPath);
  clearModule(supabasePath);

  const users = {
    "email-user": {
      id: "email-user",
      email: "owner@example.com",
      steam_id64: null,
      display_name: null,
      avatar_url: null
    },
    "steam-user": {
      id: "steam-user",
      email: "steam_76561198000000000@steam.local",
      steam_id64: "76561198000000000",
      display_name: "Old Steam Name",
      avatar_url: "https://old.example/avatar.jpg"
    }
  };
  let mergeCalled = false;

  primeModule(supabasePath, buildSupabaseStub());
  primeModule(userRepoPath, {
    getById: async (id) => users[id] || null,
    getBySteamId64: async (steamId64) =>
      Object.values(users).find((row) => row.steam_id64 === steamId64) || null,
    mergeUserData: async () => {
      mergeCalled = true;
    },
    updateSteamProfileById: async (id, updates = {}) => {
      const row = users[id];
      if (!row) return null;

      if (Object.prototype.hasOwnProperty.call(updates, "steamId64")) {
        row.steam_id64 = updates.steamId64 == null ? null : String(updates.steamId64);
      }
      if (Object.prototype.hasOwnProperty.call(updates, "displayName")) {
        row.display_name = updates.displayName || null;
      }
      if (Object.prototype.hasOwnProperty.call(updates, "avatarUrl")) {
        row.avatar_url = updates.avatarUrl || null;
      }

      return { ...row };
    }
  });

  const authService = require(authServicePath);
  const result = await authService.linkSteamToUser(
    "email-user",
    "76561198000000000",
    {
      displayName: "New Steam Name",
      avatarUrl: "https://new.example/avatar.jpg"
    }
  );

  assert.equal(result.mergedFromUserId, "steam-user");
  assert.equal(mergeCalled, true);
  assert.equal(users["steam-user"].steam_id64, null);
  assert.equal(users["email-user"].steam_id64, "76561198000000000");
  assert.equal(users["email-user"].display_name, "New Steam Name");
  assert.equal(users["email-user"].avatar_url, "https://new.example/avatar.jpg");
});

test("linkSteamToUser rejects linking when steam id belongs to another non-steam account", async () => {
  clearModule(authServicePath);
  clearModule(userRepoPath);
  clearModule(supabasePath);

  const users = {
    "current-user": {
      id: "current-user",
      email: "me@example.com",
      steam_id64: null,
      display_name: null,
      avatar_url: null
    },
    "owner-user": {
      id: "owner-user",
      email: "owner@example.com",
      steam_id64: "76561198000000000",
      display_name: "Owner",
      avatar_url: null
    }
  };

  primeModule(supabasePath, buildSupabaseStub());
  primeModule(userRepoPath, {
    getById: async (id) => users[id] || null,
    getBySteamId64: async (steamId64) =>
      Object.values(users).find((row) => row.steam_id64 === steamId64) || null,
    mergeUserData: async () => {},
    updateSteamProfileById: async (id) => users[id] || null
  });

  const authService = require(authServicePath);

  await assert.rejects(
    () => authService.linkSteamToUser("current-user", "76561198000000000"),
    (err) => {
      assert.equal(err.code, "STEAM_ALREADY_LINKED");
      assert.equal(err.statusCode, 409);
      return true;
    }
  );
});

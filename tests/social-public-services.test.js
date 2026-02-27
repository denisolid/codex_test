const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

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

const socialServicePath = path.resolve(__dirname, "../src/services/socialService.js");
const publicServicePath = path.resolve(
  __dirname,
  "../src/services/publicPortfolioService.js"
);
const userRepoPath = path.resolve(__dirname, "../src/repositories/userRepository.js");
const watchlistRepoPath = path.resolve(__dirname, "../src/repositories/watchlistRepository.js");
const inventoryRepoPath = path.resolve(__dirname, "../src/repositories/inventoryRepository.js");
const priceRepoPath = path.resolve(__dirname, "../src/repositories/priceHistoryRepository.js");
const viewsRepoPath = path.resolve(
  __dirname,
  "../src/repositories/publicPortfolioViewRepository.js"
);
const currencyServicePath = path.resolve(__dirname, "../src/services/currencyService.js");
const portfolioServicePath = path.resolve(__dirname, "../src/services/portfolioService.js");

test("social leaderboard ranks by portfolio value and marks watchlist membership", async () => {
  [
    socialServicePath,
    userRepoPath,
    watchlistRepoPath,
    inventoryRepoPath,
    priceRepoPath,
    viewsRepoPath,
    currencyServicePath
  ].forEach(clearModule);

  primeModule(userRepoPath, {
    listPublicSteamUsers: async () => [
      {
        id: "u2",
        steam_id64: "76561198000000002",
        display_name: "Second",
        avatar_url: null,
        public_portfolio_enabled: true
      },
      {
        id: "u3",
        steam_id64: "76561198000000003",
        display_name: "Third",
        avatar_url: null,
        public_portfolio_enabled: true
      }
    ],
    getBySteamId64: async () => null
  });
  primeModule(watchlistRepoPath, {
    listByUser: async () => [{ target_user_id: "u2", created_at: "2026-02-01T00:00:00.000Z" }],
    listByTargetIds: async () => [{ target_user_id: "u2" }, { target_user_id: "u2" }],
    add: async () => ({}),
    remove: async () => true
  });
  primeModule(inventoryRepoPath, {
    getHoldingsByUserIds: async () => [
      { user_id: "u2", skin_id: 1, quantity: 2 },
      { user_id: "u3", skin_id: 2, quantity: 1 }
    ]
  });
  primeModule(priceRepoPath, {
    getLatestPricesBySkinIds: async () => ({ 1: 100, 2: 50 })
  });
  primeModule(viewsRepoPath, {
    countByOwnersSince: async () => ({
      u2: { views: 10, referrals: 3 },
      u3: { views: 4, referrals: 1 }
    })
  });
  primeModule(currencyServicePath, {
    resolveCurrency: () => "USD",
    convertUsdAmount: (value) => Number(value || 0)
  });

  const socialService = require(socialServicePath);
  const result = await socialService.getLeaderboard("u1", {
    scope: "global",
    limit: 10,
    currency: "USD"
  });

  assert.equal(result.items.length, 2);
  assert.equal(result.items[0].steamId64, "76561198000000002");
  assert.equal(result.items[0].rank, 1);
  assert.equal(result.items[0].inWatchlist, true);
  assert.equal(result.items[0].totalValueUsd, 200);
  assert.equal(result.items[1].rank, 2);
});

test("public portfolio service rejects hidden public profile", async () => {
  [publicServicePath, userRepoPath, portfolioServicePath, viewsRepoPath].forEach(clearModule);

  primeModule(userRepoPath, {
    getBySteamId64: async () => ({
      id: "hidden-user",
      steam_id64: "76561198000000009",
      display_name: "Hidden",
      avatar_url: null,
      public_portfolio_enabled: false
    })
  });
  primeModule(portfolioServicePath, {
    getPortfolio: async () => ({}),
    getPortfolioHistory: async () => ({ points: [] })
  });
  primeModule(viewsRepoPath, {
    recordView: async () => {}
  });

  const publicService = require(publicServicePath);

  await assert.rejects(
    () => publicService.getBySteamId64("76561198000000009"),
    (err) => {
      assert.equal(err.code, "PUBLIC_PORTFOLIO_DISABLED");
      assert.equal(err.statusCode, 403);
      return true;
    }
  );
});

const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const servicePath = require.resolve("../src/services/priceProviderService");
const envPath = require.resolve("../src/config/env");
const steamPath = require.resolve("../src/services/steamMarketPriceService");
const mockPath = require.resolve("../src/services/mockPriceProviderService");

function unload(pathname) {
  if (require.cache[pathname]) {
    delete require.cache[pathname];
  }
}

function stubModule(pathname, exportsValue) {
  require.cache[pathname] = {
    id: pathname,
    filename: pathname,
    loaded: true,
    exports: exportsValue
  };
}

function loadPriceProvider({ marketPriceSource, steamLatestPrice, mockLatestPrice }) {
  process.env.MARKET_PRICE_SOURCE = marketPriceSource;

  unload(servicePath);
  unload(envPath);
  unload(steamPath);
  unload(mockPath);

  stubModule(steamPath, {
    getLatestPrice: async (...args) => {
      if (typeof steamLatestPrice === "function") {
        return steamLatestPrice(...args);
      }
      return steamLatestPrice;
    }
  });

  stubModule(mockPath, {
    getLatestPrice: async (...args) => {
      if (typeof mockLatestPrice === "function") {
        return mockLatestPrice(...args);
      }
      return mockLatestPrice;
    }
  });

  return require(servicePath);
}

test.afterEach(() => {
  unload(servicePath);
  unload(envPath);
  unload(steamPath);
  unload(mockPath);
  delete process.env.MARKET_PRICE_SOURCE;
});

test("price provider rejects mock mode when mock source is disabled per request", async () => {
  const service = loadPriceProvider({
    marketPriceSource: "mock",
    steamLatestPrice: 9.99,
    mockLatestPrice: 1.23
  });

  await assert.rejects(
    () =>
      service.getPrice("AK-47 | Redline (Field-Tested)", {
        allowMockSource: false
      }),
    /Mock price source is disabled/
  );
});

test("price provider in auto mode can skip mock fallback when disabled", async () => {
  let mockCalls = 0;
  const service = loadPriceProvider({
    marketPriceSource: "auto",
    steamLatestPrice: async () => {
      throw new Error("Steam temporarily unavailable");
    },
    mockLatestPrice: async () => {
      mockCalls += 1;
      return 2.34;
    }
  });

  await assert.rejects(
    () =>
      service.getPrice("M4A1-S | Basilisk (Field-Tested)", {
        allowMockFallback: false
      }),
    /Steam temporarily unavailable/
  );
  assert.equal(mockCalls, 0);
});

test("price provider in auto mode uses mock fallback by default", async () => {
  const service = loadPriceProvider({
    marketPriceSource: "auto",
    steamLatestPrice: async () => {
      throw new Error("Steam timeout");
    },
    mockLatestPrice: 3.21
  });

  const priced = await service.getPrice("USP-S | Ticket to Hell (Field-Tested)");
  assert.equal(priced.source, "mock-price");
  assert.equal(priced.price, 3.21);
});

test("price provider returns steam source when steam price succeeds", async () => {
  const service = loadPriceProvider({
    marketPriceSource: "auto",
    steamLatestPrice: 17.45,
    mockLatestPrice: 4.56
  });

  const priced = await service.getPrice("Desert Eagle | Urban Rubble (Minimal Wear)");
  assert.equal(priced.source, "steam-market");
  assert.equal(priced.price, 17.45);
});

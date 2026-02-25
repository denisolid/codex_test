const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const envPath = path.resolve(__dirname, "../src/config/env.js");
const repoPath = path.resolve(__dirname, "../src/repositories/priceHistoryRepository.js");
const supabasePath = path.resolve(__dirname, "../src/config/supabase.js");

function primeModule(modulePath, exportsValue) {
  require.cache[modulePath] = {
    id: modulePath,
    filename: modulePath,
    loaded: true,
    exports: exportsValue
  };
}

function clearForReload() {
  delete require.cache[repoPath];
  delete require.cache[envPath];
  delete require.cache[supabasePath];
}

function loadRepoWithFallback(enabled) {
  process.env.MARKET_PRICE_SOURCE = "steam";
  process.env.MARKET_PRICE_FALLBACK_TO_MOCK = enabled ? "true" : "false";
  clearForReload();
  primeModule(supabasePath, { supabaseAdmin: {} });
  return require(repoPath);
}

test("strict real mode adds source filter that excludes mock rows", () => {
  const repo = loadRepoWithFallback(false);
  const { applyPriceSourceFilter } = repo.__testables;
  const query = {
    called: false,
    not(field, operator, value) {
      this.called = true;
      this.args = [field, operator, value];
      return this;
    }
  };

  const output = applyPriceSourceFilter(query);
  assert.equal(output, query);
  assert.equal(query.called, true);
  assert.deepEqual(query.args, ["source", "ilike", "%mock%"]);
});

test("mixed mode keeps query unchanged without excluding mock rows", () => {
  const repo = loadRepoWithFallback(true);
  const { applyPriceSourceFilter } = repo.__testables;
  const query = {
    called: false,
    not() {
      this.called = true;
      return this;
    }
  };

  const output = applyPriceSourceFilter(query);
  assert.equal(output, query);
  assert.equal(query.called, false);
});

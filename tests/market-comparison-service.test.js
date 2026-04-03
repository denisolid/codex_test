const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const marketComparisonService = require("../src/services/marketComparisonService");
const marketPriceRepo = require("../src/repositories/marketPriceRepository");
const marketQuoteRepo = require("../src/repositories/marketQuoteRepository");

const {
  __testables: {
    normalizePricingMode,
    normalizeItems,
    applyQuoteCoverageFallback,
    buildRouteFreshnessContractFromCompareResult,
    pickBestBuy,
    pickBestSellNet,
    selectByPricingMode,
    getModeUnitPrice,
    parseDmarketUsdMinorValue,
    maybeRepairLegacyDmarketGross,
    normalizeSourceState,
    resolveUnavailableContextForSource,
    getCachedRowUpdatedAt,
    fromCachedRow
  }
} = require("../src/services/marketComparisonService");

test("normalizePricingMode keeps known values and falls back safely", () => {
  assert.equal(normalizePricingMode("steam"), "steam");
  assert.equal(normalizePricingMode("best_sell_net"), "best_sell_net");
  assert.equal(normalizePricingMode("LOWEST_BUY"), "lowest_buy");
  assert.equal(normalizePricingMode("unknown"), "lowest_buy");
});

test("best buy and best sell net pick expected sources", () => {
  const perMarket = [
    {
      source: "steam",
      available: true,
      grossPrice: 13,
      netPriceAfterFees: 11.31
    },
    {
      source: "skinport",
      available: true,
      grossPrice: 12.4,
      netPriceAfterFees: 10.91
    },
    {
      source: "csfloat",
      available: true,
      grossPrice: 12.8,
      netPriceAfterFees: 12.54
    }
  ];

  assert.equal(pickBestBuy(perMarket).source, "skinport");
  assert.equal(pickBestSellNet(perMarket).source, "csfloat");
});

test("mode selection chooses expected market row and unit value", () => {
  const steam = { source: "steam", grossPrice: 20, netPriceAfterFees: 17.4 };
  const bestBuy = { source: "skinport", grossPrice: 18, netPriceAfterFees: 15.84 };
  const bestSellNet = { source: "csfloat", grossPrice: 19, netPriceAfterFees: 18.62 };

  const stream = { steam, bestBuy, bestSellNet };

  const steamSelected = selectByPricingMode("steam", stream);
  const lowSelected = selectByPricingMode("lowest_buy", stream);
  const netSelected = selectByPricingMode("best_sell_net", stream);

  assert.equal(steamSelected.source, "steam");
  assert.equal(lowSelected.source, "skinport");
  assert.equal(netSelected.source, "csfloat");

  assert.equal(getModeUnitPrice("steam", steamSelected), 20);
  assert.equal(getModeUnitPrice("lowest_buy", lowSelected), 18);
  assert.equal(getModeUnitPrice("best_sell_net", netSelected), 18.62);
});

test("dmarket USD minor parser converts cents and keeps decimal values", () => {
  assert.equal(parseDmarketUsdMinorValue("174"), 1.74);
  assert.equal(parseDmarketUsdMinorValue("0.93"), 0.93);
  assert.equal(parseDmarketUsdMinorValue(null), null);
});

test("legacy dmarket cached rows are repaired from raw USD cents", () => {
  const repaired = maybeRepairLegacyDmarketGross(
    {
      raw: {
        price: {
          USD: "650"
        }
      }
    },
    "dmarket",
    "USD",
    1
  );

  assert.equal(repaired, 6.5);
  assert.equal(
    maybeRepairLegacyDmarketGross({ raw: { price: { USD: "100" } } }, "dmarket", "USD", 1),
    1
  );
});

test("cached rows prefer source_updated_at over fetched_at for freshness display", () => {
  const sourceUpdatedAt = new Date(Date.now() - 15 * 60 * 1000).toISOString();
  const fetchedAt = new Date().toISOString();
  const row = fromCachedRow(
    {
      market: "steam",
      market_hash_name: "Fracture Case",
      currency: "USD",
      gross_price: 1.25,
      net_price: 1.09,
      fetched_at: fetchedAt,
      raw: {
        source_updated_at: sourceUpdatedAt
      }
    },
    "USD"
  );

  assert.equal(getCachedRowUpdatedAt({ raw: { source_updated_at: sourceUpdatedAt } }), sourceUpdatedAt);
  assert.equal(row.updatedAt, sourceUpdatedAt);
});

test("compare item normalization keeps explicit liquidity/volume fields", () => {
  const [item] = normalizeItems([
    {
      marketHashName: "AWP | Neo-Noir (Field-Tested)",
      liquiditySales: 273,
      volume7d: 273,
      liquidityScore: 88
    }
  ]);

  assert.equal(item.marketHashName, "AWP | Neo-Noir (Field-Tested)");
  assert.equal(item.liquiditySales, 273);
  assert.equal(item.volume7d, 273);
  assert.equal(item.marketVolume7d, null);
  assert.equal(item.liquidityScore, 88);
});

test("compare item liquidity falls back to saved quote coverage volume", () => {
  const [normalized] = normalizeItems([
    {
      marketHashName: "AWP | Neo-Noir (Field-Tested)"
    }
  ]);

  const [enriched] = applyQuoteCoverageFallback([normalized], {
    "AWP | Neo-Noir (Field-Tested)": {
      marketCoverageCount: 4,
      volume7dMax: 273
    }
  });

  assert.equal(enriched.volume7d, 273);
  assert.equal(enriched.marketVolume7d, 273);
  assert.equal(enriched.liquiditySales, 273);
  assert.equal(enriched.marketCoverageCount, 4);
});

test("7d coverage overrides ambiguous liquiditySales when explicit 7d is missing", () => {
  const [normalized] = normalizeItems([
    {
      marketHashName: "AWP | Neo-Noir (Field-Tested)",
      liquiditySales: 30
    }
  ]);

  const [enriched] = applyQuoteCoverageFallback([normalized], {
    "AWP | Neo-Noir (Field-Tested)": {
      marketCoverageCount: 4,
      volume7dMax: 273
    }
  });

  assert.equal(enriched.volume7d, 273);
  assert.equal(enriched.marketVolume7d, 273);
  assert.equal(enriched.liquiditySales, 30);
});

test("compare route freshness contract preserves fresh route timestamps and listing provenance", () => {
  const nowIso = new Date().toISOString();
  const contract = buildRouteFreshnessContractFromCompareResult(
    {
      perMarket: [
        {
          source: "steam",
          available: true,
          grossPrice: 10.5,
          netPriceAfterFees: 9.14,
          updatedAt: nowIso
        },
        {
          source: "skinport",
          available: true,
          grossPrice: 11.2,
          netPriceAfterFees: 12.74,
          updatedAt: nowIso,
          raw: {
            listing_id: "sp-123"
          }
        }
      ],
      bestBuy: { source: "steam" },
      bestSellNet: { source: "skinport" },
      arbitrage: { buyMarket: "steam", sellMarket: "skinport" }
    },
    {
      buyMarket: "steam",
      sellMarket: "skinport"
    }
  );

  assert.equal(contract.buyRouteAvailable, true);
  assert.equal(contract.sellRouteAvailable, true);
  assert.equal(contract.buyRouteUpdatedAt, nowIso);
  assert.equal(contract.sellRouteUpdatedAt, nowIso);
  assert.equal(contract.sellListingAvailable, true);
  assert.equal(contract.requiredRouteState, "ready");
  assert.equal(contract.listingAvailabilityState, "available");
  assert.equal(contract.contractSource, "compare_result");
});

test("compare source-state helper keeps CSFloat auth failures structured", () => {
  const context = resolveUnavailableContextForSource(
    "csfloat",
    "AK-47 | Redline (Field-Tested)",
    {
      csfloat: {
        failuresByName: {
          "AK-47 | Redline (Field-Tested)":
            "CSFloat authentication failed. Check CSFLOAT_API_KEY."
        },
        stateByName: {
          "AK-47 | Redline (Field-Tested)": "auth_failed"
        },
        diagnosticsByName: {
          "AK-47 | Redline (Field-Tested)": {
            credentials_present: true,
            auth_ok: false,
            request_sent: true,
            response_status: 403,
            source_failure_reason: "auth_failed"
          }
        },
        sourceUnavailableReason: "CSFloat authentication failed. Check CSFLOAT_API_KEY.",
        sourceFailureReason: "auth_failed"
      }
    }
  );

  assert.equal(context.unavailableReason, "CSFloat authentication failed. Check CSFLOAT_API_KEY.");
  assert.equal(context.sourceState, "auth_failed");
  assert.deepEqual(context.sourceDiagnostics, {
    market_enabled: true,
    credentials_present: true,
    auth_ok: false,
    request_sent: true,
    response_status: 403,
    response_parsed: null,
    listings_found: null,
    buy_price_present: null,
    sell_price_present: null,
    freshness_present: null,
    listing_url_present: null,
    source_failure_reason: "auth_failed",
    last_success_at: null,
    last_failure_at: null
  });
  assert.equal(normalizeSourceState("AUTH_FAILED"), "auth_failed");
});

test("compareItems does not synthesize Steam fallback when no real Steam data exists", async () => {
  const originals = {
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestByMarketHashNames: marketPriceRepo.getLatestByMarketHashNames
  };

  marketQuoteRepo.getLatestCoverageByItemNames = async () => ({});
  marketPriceRepo.getLatestByMarketHashNames = async () => ({});

  try {
    const result = await marketComparisonService.compareItems(
      [
        {
          marketHashName: "Revolution Case",
          quantity: 1,
          steamPrice: 1.45,
          steamCurrency: "USD",
          steamRecordedAt: "2026-04-03T12:00:00.000Z"
        }
      ],
      {
        allowLiveFetch: false,
        planTier: "alpha_access"
      }
    );

    const steam = result.items[0].perMarket.find((row) => row.source === "steam");
    assert.equal(steam.available, false);
    assert.equal(steam.sourceState, "no_data");
    assert.equal(result.items[0].selectedPricingSource, null);
  } finally {
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames;
    marketPriceRepo.getLatestByMarketHashNames = originals.getLatestByMarketHashNames;
  }
});

test("compareItems disables DMarket in scanner baseline policy even when cached rows exist", async () => {
  const nowIso = new Date().toISOString();
  const originals = {
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestByMarketHashNames: marketPriceRepo.getLatestByMarketHashNames
  };

  marketQuoteRepo.getLatestCoverageByItemNames = async () => ({});
  marketPriceRepo.getLatestByMarketHashNames = async () => ({
    dmarket: {
      "Revolution Case": {
        market: "dmarket",
        market_hash_name: "Revolution Case",
        currency: "USD",
        gross_price: 0.9,
        net_price: 0.82,
        url: "https://dmarket.example/revolution-case",
        fetched_at: nowIso,
        raw: {
          source_updated_at: nowIso
        }
      }
    },
    skinport: {
      "Revolution Case": {
        market: "skinport",
        market_hash_name: "Revolution Case",
        currency: "USD",
        gross_price: 1.05,
        net_price: 0.91,
        url: "https://skinport.example/revolution-case",
        fetched_at: nowIso,
        raw: {
          source_updated_at: nowIso
        }
      }
    }
  });
  try {
    const result = await marketComparisonService.compareItems(
      [
        {
          marketHashName: "Revolution Case",
          quantity: 1
        }
      ],
      {
        allowLiveFetch: false,
        planTier: "alpha_access",
        marketReliabilityPolicy: "scanner_baseline"
      }
    );

    const item = result.items[0];
    const dmarket = item.perMarket.find((row) => row.source === "dmarket");
    assert.equal(dmarket.available, false);
    assert.equal(dmarket.sourceState, "disabled");
    assert.equal(item.bestBuy.source, "skinport");
    assert.deepEqual(result.diagnostics.scannerPolicy, {
      markets_enabled_for_scanner: ["skinport", "csfloat"],
      markets_degraded_for_scanner: ["steam"],
      markets_disabled_for_scanner: ["dmarket"]
    });
  } finally {
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames;
    marketPriceRepo.getLatestByMarketHashNames = originals.getLatestByMarketHashNames;
  }
});

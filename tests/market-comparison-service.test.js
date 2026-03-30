const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

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
    resolveUnavailableContextForSource
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
            api_key_present: true,
            auth_header_sent: true,
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
    api_key_present: true,
    auth_header_sent: true,
    response_status: 403,
    source_failure_reason: "auth_failed"
  });
  assert.equal(normalizeSourceState("AUTH_FAILED"), "auth_failed");
});

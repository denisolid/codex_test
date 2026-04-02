const path = require("path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const ROOT = path.resolve(__dirname, "..");
const { buildMarketPriceRecord } = require("../src/markets/marketUtils");
const {
  SOURCE_STATES,
  buildMarketHealthDiagnostics,
  attachMarketHealth
} = require("../src/markets/marketSourceDiagnostics");

function isoMinutesAgo(minutes) {
  return new Date(Date.now() - Number(minutes || 0) * 60 * 1000).toISOString();
}

const FIXTURE_ITEMS = [
  {
    marketHashName: "AK-47 | Redline (Field-Tested)",
    quantity: 1,
    steamPrice: 13.55,
    steamCurrency: "USD",
    steamRecordedAt: isoMinutesAgo(12),
    itemCategory: "weapon_skin",
    volume7d: 180,
    marketCoverageCount: 3
  },
  {
    marketHashName: "Gamma Case",
    quantity: 1,
    steamPrice: 2.95,
    steamCurrency: "USD",
    steamRecordedAt: isoMinutesAgo(18),
    itemCategory: "case",
    volume7d: 520,
    marketCoverageCount: 2
  }
];

function makeOkRecord(source, marketHashName, grossPrice, updatedAt, raw = {}, url = null) {
  const diagnostics = buildMarketHealthDiagnostics({
    marketEnabled: true,
    requestSent: true,
    responseStatus: 200,
    responseParsed: true,
    listingsFound: true,
    buyPricePresent: true,
    sellPricePresent: true,
    freshnessPresent: true,
    listingUrlPresent: Boolean(url),
    lastSuccessAt: updatedAt
  });
  return buildMarketPriceRecord({
    source,
    marketHashName,
    grossPrice,
    currency: "USD",
    url,
    updatedAt,
    confidence: "high",
    raw: attachMarketHealth(raw, diagnostics)
  });
}

function makeFailure(state, reason, extra = {}) {
  return {
    kind: "failure",
    state,
    reason,
    diagnostics: buildMarketHealthDiagnostics({
      marketEnabled: extra.marketEnabled == null ? true : extra.marketEnabled,
      credentialsPresent: extra.credentialsPresent,
      authOk: extra.authOk,
      requestSent: extra.requestSent == null ? true : extra.requestSent,
      responseStatus: extra.responseStatus,
      responseParsed: extra.responseParsed,
      listingsFound: extra.listingsFound,
      buyPricePresent: false,
      sellPricePresent: false,
      freshnessPresent: extra.freshnessPresent,
      listingUrlPresent: false,
      sourceFailureReason: state,
      lastFailureAt: extra.lastFailureAt || new Date().toISOString()
    })
  };
}

const MARKET_FIXTURES = {
  steam: {
    "AK-47 | Redline (Field-Tested)": {
      kind: "ok",
      record: makeOkRecord(
        "steam",
        "AK-47 | Redline (Field-Tested)",
        13.55,
        isoMinutesAgo(12),
        { volume: 180 },
        "https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Redline%20(Field-Tested)"
      )
    },
    "Gamma Case": {
      kind: "ok",
      record: makeOkRecord(
        "steam",
        "Gamma Case",
        2.95,
        isoMinutesAgo(18),
        { volume: 520 },
        "https://steamcommunity.com/market/listings/730/Gamma%20Case"
      )
    }
  },
  skinport: {
    "AK-47 | Redline (Field-Tested)": {
      kind: "ok",
      record: makeOkRecord(
        "skinport",
        "AK-47 | Redline (Field-Tested)",
        12.8,
        isoMinutesAgo(9),
        {
          skinport_quote_price: 12.8,
          skinport_quote_currency: "USD",
          skinport_quote_observed_at: isoMinutesAgo(9),
          skinport_quote_type: "live_executable",
          skinport_item_slug: "ak-47-redline-field-tested",
          skinport_listing_id: "sp-123",
          skinport_price_integrity_status: "confirmed"
        },
        "https://skinport.com/item/ak-47-redline-field-tested"
      )
    },
    "Gamma Case": makeFailure(SOURCE_STATES.STALE, "Skinport quote is stale.", {
      responseStatus: 200,
      responseParsed: true,
      listingsFound: true,
      freshnessPresent: true
    })
  },
  csfloat: {
    "AK-47 | Redline (Field-Tested)": makeFailure(
      SOURCE_STATES.AUTH_FAILED,
      "CSFloat authentication failed. Check CSFLOAT_API_KEY.",
      { credentialsPresent: true, authOk: false, responseStatus: 403 }
    ),
    "Gamma Case": {
      kind: "ok",
      record: makeOkRecord(
        "csfloat",
        "Gamma Case",
        2.91,
        isoMinutesAgo(7),
        { csfloat_listing_id: "cf-321" },
        "https://csfloat.com/item/cf-321"
      )
    }
  },
  dmarket: {
    "AK-47 | Redline (Field-Tested)": makeFailure(
      SOURCE_STATES.NO_LISTING,
      "No DMarket listing found.",
      { responseStatus: 200, responseParsed: true, listingsFound: false }
    ),
    "Gamma Case": makeFailure(SOURCE_STATES.TIMEOUT, "DMarket request timed out.", {
      responseStatus: 504
    })
  }
};

function abs(relPath) {
  return path.join(ROOT, relPath);
}

function createAdapter(source) {
  return {
    source,
    async batchGetPrices(items = []) {
      const byName = {};
      const failuresByName = {};
      const stateByName = {};
      const diagnosticsByName = {};
      for (const item of items) {
        const marketHashName = String(item?.marketHashName || "").trim();
        const scenario = MARKET_FIXTURES[source]?.[marketHashName];
        if (!scenario) continue;
        if (scenario.kind === "ok") {
          byName[marketHashName] = scenario.record;
          stateByName[marketHashName] = SOURCE_STATES.OK;
          diagnosticsByName[marketHashName] = scenario.record.raw.market_health;
        } else {
          failuresByName[marketHashName] = scenario.reason;
          stateByName[marketHashName] = scenario.state;
          diagnosticsByName[marketHashName] = scenario.diagnostics;
        }
      }
      Object.defineProperty(byName, "__meta", {
        value: {
          failuresByName,
          stateByName,
          diagnosticsByName,
          market_enabled: true,
          request_sent: true,
          response_status: 200,
          response_parsed: true,
          last_success_at: new Date().toISOString(),
          last_failure_at: new Date().toISOString(),
          pipeline: { requestedItems: items.length, mappedItems: Object.keys(byName).length }
        },
        enumerable: false
      });
      return byName;
    }
  };
}

function stubModule(filePath, exportsValue) {
  const previous = require.cache[filePath];
  require.cache[filePath] = {
    id: filePath,
    filename: filePath,
    loaded: true,
    exports: exportsValue
  };
  return () => {
    if (previous) require.cache[filePath] = previous;
    else delete require.cache[filePath];
  };
}

async function main() {
  const capturedMarketPriceRows = [];
  const cleanup = [
    stubModule(abs("src/repositories/marketPriceRepository.js"), {
      getLatestByMarketHashNames: async () => ({}),
      upsertRows: async (rows = []) => {
        capturedMarketPriceRows.push(...rows);
        return rows.length;
      }
    }),
    stubModule(abs("src/repositories/marketQuoteRepository.js"), {
      getLatestCoverageByItemNames: async () =>
        Object.fromEntries(
          FIXTURE_ITEMS.map((item) => [
            item.marketHashName,
            {
              marketCoverageCount: item.marketCoverageCount,
              volume7dMax: item.volume7d
            }
          ])
        )
    }),
    stubModule(abs("src/repositories/userPricePreferenceRepository.js"), {
      getByUserId: async () => null
    }),
    stubModule(abs("src/services/planService.js"), {
      normalizePlanTier: (value) => String(value || "alpha_access"),
      getEntitlements: (planTier) => ({ planTier, compareView: "full" }),
      getPlanConfig: () => ({ compareView: "full", compareViewMaxItems: 25 }),
      getUserPlanProfile: async () => ({
        planTier: "alpha_access",
        entitlements: { planTier: "alpha_access", compareView: "full" }
      })
    }),
    stubModule(abs("src/services/premiumCategoryAccessService.js"), {
      normalizeItemCategory: (value) => value || "weapon_skin",
      hasPremiumCategoryAccess: () => true,
      isPremiumCategory: () => false
    }),
    stubModule(abs("src/services/arbitrageEngineService.js"), {
      evaluateItemOpportunity: ({ marketHashName, perMarket = [] }) => {
        const available = perMarket.filter((row) => row?.available);
        const buy = available.slice().sort((a, b) => a.grossPrice - b.grossPrice)[0] || null;
        const sell =
          available.slice().sort((a, b) => b.netPriceAfterFees - a.netPriceAfterFees)[0] || null;
        const profit = buy && sell ? Number((sell.netPriceAfterFees - buy.grossPrice).toFixed(2)) : 0;
        return {
          marketHashName,
          isOpportunity: profit > 0,
          buyMarket: buy?.source || null,
          sellMarket: sell?.source || null,
          profit,
          spreadPercent: buy ? Number(((profit / buy.grossPrice) * 100).toFixed(2)) : 0,
          opportunityScore: profit > 0 ? 75 : 20,
          scoreCategory: profit > 0 ? "Strong" : "None",
          executionConfidence: profit > 0 ? "High" : "Low"
        };
      },
      rankOpportunities: (rows = []) => rows.filter((row) => row?.isOpportunity)
    }),
    stubModule(abs("src/services/currencyService.js"), {
      resolveCurrency: (value) => String(value || "USD").trim().toUpperCase() || "USD",
      convertAmount: (value) => Number(value),
      ensureFreshFxRates: async () => {}
    }),
    stubModule(abs("src/markets/steam.market.js"), createAdapter("steam")),
    stubModule(abs("src/markets/skinport.market.js"), createAdapter("skinport")),
    stubModule(abs("src/markets/csfloat.market.js"), createAdapter("csfloat")),
    stubModule(abs("src/markets/dmarket.market.js"), createAdapter("dmarket"))
  ];

  try {
    delete require.cache[abs("src/services/marketComparisonService.js")];
    delete require.cache[abs("src/services/upstreamMarketFreshnessRecoveryService.js")];
    const marketComparisonService = require("../src/services/marketComparisonService");
    const {
      __testables: { buildQuoteInsertRow }
    } = require("../src/services/upstreamMarketFreshnessRecoveryService");

    const compare = await marketComparisonService.compareItems(FIXTURE_ITEMS, {
      planTier: "alpha_access",
      entitlements: { planTier: "alpha_access", compareView: "full" },
      allowLiveFetch: true,
      forceRefresh: true
    });

    const scannerQuoteRows = [];
    for (const item of FIXTURE_ITEMS) {
      for (const source of Object.keys(MARKET_FIXTURES)) {
        const scenario = MARKET_FIXTURES[source][item.marketHashName];
        if (!scenario || scenario.kind !== "ok") continue;
        const quoteRow = buildQuoteInsertRow(scenario.record, item, new Date().toISOString());
        if (quoteRow) scannerQuoteRows.push(quoteRow);
      }
    }

    const compareStatesBySource = {};
    const contractMismatches = [];
    for (const item of compare.items || []) {
      for (const row of item.perMarket || []) {
        const source = row.source;
        if (!compareStatesBySource[source]) compareStatesBySource[source] = {};
        const state = String(row.sourceState || (row.available ? "ok" : "no_data")).trim().toLowerCase();
        compareStatesBySource[source][state] =
          Number(compareStatesBySource[source][state] || 0) + 1;
        const scannerRow = scannerQuoteRows.find(
          (quote) => quote.item_name === item.marketHashName && quote.market === source
        );
        if (Boolean(row.available) !== Boolean(scannerRow)) {
          contractMismatches.push({
            marketHashName: item.marketHashName,
            source,
            compareAvailable: Boolean(row.available),
            scannerRowPresent: Boolean(scannerRow)
          });
        }
      }
    }

    console.log(
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          cohortSize: FIXTURE_ITEMS.length,
          compareStatesBySource,
          marketPriceRowsUpserted: capturedMarketPriceRows.length,
          scannerQuoteRows: scannerQuoteRows.length,
          contractMismatchCount: contractMismatches.length,
          contractMismatches,
          diagnosticsBySource: compare?.diagnostics?.liveFetch?.bySource || {}
        },
        null,
        2
      )
    );
  } finally {
    cleanup.reverse().forEach((restore) => restore());
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

const path = require("path");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const ROOT = path.resolve(__dirname, "..");
const MARKET_SOURCES = ["steam", "skinport", "csfloat", "dmarket"];
const COHORT = Object.freeze([
  ["AK-47 | Slate (Field-Tested)", "weapon_skin"],
  ["AK-47 | Legion of Anubis (Field-Tested)", "weapon_skin"],
  ["M4A1-S | Decimator (Field-Tested)", "weapon_skin"],
  ["M4A4 | The Battlestar (Factory New)", "weapon_skin"],
  ["FAMAS | Roll Cage (Field-Tested)", "weapon_skin"],
  ["AWP | Asiimov (Field-Tested)", "weapon_skin"],
  ["AWP | Neo-Noir (Field-Tested)", "weapon_skin"],
  ["StatTrak AWP | Fever Dream (Field-Tested)", "weapon_skin"],
  ["USP-S | Cortex (Field-Tested)", "weapon_skin"],
  ["Glock-18 | Vogue (Field-Tested)", "weapon_skin"],
  ["P250 | See Ya Later (Battle-Scarred)", "weapon_skin"],
  ["MP9 | Mount Fuji (Field-Tested)", "weapon_skin"],
  ["Souvenir USP-S | Jawbreaker (Field-Tested)", "weapon_skin"],
  ["Souvenir M4A1-S | Control Panel (Field-Tested)", "weapon_skin"],
  ["Revolution Case", "case"],
  ["Fracture Case", "case"],
  ["Prisma 2 Case", "case"],
  ["Sticker Capsule 2", "sticker_capsule"],
  ["Paris 2023 Legends Autograph Capsule", "sticker_capsule"],
  ["DreamHack 2014 Legends Sticker Capsule", "sticker_capsule"]
].map(([marketHashName, itemCategory]) => ({
  marketHashName,
  itemCategory,
  quantity: 1,
  steamPrice: 0,
  steamCurrency: "USD",
  steamRecordedAt: null,
  volume7d: null,
  marketCoverageCount: null
})));

const {
  sourceFeePercent
} = require("../src/markets/marketUtils");
const {
  SOURCE_STATES,
  buildMarketHealthDiagnostics,
  hasMarketHealthDiagnostics,
  normalizeSourceState,
  readMarketHealth
} = require("../src/markets/marketSourceDiagnostics");
const { disabledMarketSources } = require("../src/config/env");

function abs(relPath) {
  return path.join(ROOT, relPath);
}

function normalizeText(value) {
  return String(value || "").trim();
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
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

function summarizeAuthStatus(rows = []) {
  const states = new Set(rows.map((row) => row.state).filter(Boolean));
  const diagnostics = rows.map((row) => row.diagnostics).filter(hasMarketHealthDiagnostics);
  const credentialsPresent = diagnostics.some((diag) => diag.credentials_present === true);
  const authFailed = states.has(SOURCE_STATES.AUTH_FAILED);
  const authOk = diagnostics.some((diag) => diag.auth_ok === true);
  if (states.has(SOURCE_STATES.DISABLED)) return "disabled";
  if (authFailed) return credentialsPresent ? "failed_with_credentials" : "failed_no_credentials";
  if (authOk) return "ok";
  if (credentialsPresent) return "credentials_present_public_read";
  return "public_or_not_required";
}

function buildFallbackDiagnostics(input = {}) {
  return buildMarketHealthDiagnostics({
    marketEnabled: input.marketEnabled == null ? true : input.marketEnabled,
    credentialsPresent: input.credentialsPresent,
    authOk: input.authOk,
    requestSent: input.requestSent,
    responseStatus: input.responseStatus,
    responseParsed: input.responseParsed,
    listingsFound: input.listingsFound,
    buyPricePresent: input.buyPricePresent,
    sellPricePresent: input.sellPricePresent,
    freshnessPresent: input.freshnessPresent,
    listingUrlPresent: input.listingUrlPresent,
    sourceFailureReason: input.sourceFailureReason,
    lastSuccessAt: input.lastSuccessAt,
    lastFailureAt: input.lastFailureAt
  });
}

function buildCompareRows(result = null) {
  const byMarket = Object.fromEntries(MARKET_SOURCES.map((source) => [source, []]));
  for (const item of Array.isArray(result?.items) ? result.items : []) {
    for (const row of Array.isArray(item?.perMarket) ? item.perMarket : []) {
      const source = normalizeText(row?.source).toLowerCase();
      if (!byMarket[source]) continue;
      const state = normalizeSourceState(
        row?.sourceState || row?.sourceFailureReason || (row?.available ? SOURCE_STATES.OK : SOURCE_STATES.NO_DATA)
      ) || SOURCE_STATES.NO_DATA;
      const diagnostics =
        hasMarketHealthDiagnostics(row?.sourceDiagnostics)
          ? buildFallbackDiagnostics({
              marketEnabled: row.sourceDiagnostics.market_enabled,
              credentialsPresent: row.sourceDiagnostics.credentials_present,
              authOk: row.sourceDiagnostics.auth_ok,
              requestSent: row.sourceDiagnostics.request_sent,
              responseStatus: row.sourceDiagnostics.response_status,
              responseParsed: row.sourceDiagnostics.response_parsed,
              listingsFound: row.sourceDiagnostics.listings_found,
              buyPricePresent: row.sourceDiagnostics.buy_price_present,
              sellPricePresent: row.sourceDiagnostics.sell_price_present,
              freshnessPresent: row.sourceDiagnostics.freshness_present,
              listingUrlPresent: row.sourceDiagnostics.listing_url_present,
              sourceFailureReason: row.sourceDiagnostics.source_failure_reason,
              lastSuccessAt: row.sourceDiagnostics.last_success_at,
              lastFailureAt: row.sourceDiagnostics.last_failure_at
            })
          : row?.available
            ? readMarketHealth(row?.raw) ||
              buildFallbackDiagnostics({
                marketEnabled: true,
                requestSent: true,
                buyPricePresent: toFiniteOrNull(row?.grossPrice) != null,
                sellPricePresent: toFiniteOrNull(row?.netPriceAfterFees) != null,
                freshnessPresent: Boolean(row?.updatedAt),
                listingUrlPresent: Boolean(row?.url),
                lastSuccessAt: row?.updatedAt
              })
            : buildFallbackDiagnostics({
                marketEnabled: !disabledMarketSources.includes(source),
                requestSent: false,
                sourceFailureReason: state,
                lastFailureAt: new Date().toISOString()
              });

      byMarket[source].push({
        itemName: item.marketHashName,
        source,
        path: "compare",
        state,
        available: Boolean(row?.available),
        diagnostics,
        buyPrice: toFiniteOrNull(row?.grossPrice),
        sellPrice: toFiniteOrNull(row?.netPriceAfterFees),
        url: normalizeText(row?.url) || null,
        feePercent: toFiniteOrNull(row?.feePercent),
        reason: normalizeText(row?.unavailableReason) || null
      });
    }
  }
  return byMarket;
}

function groupLatestMarketPriceRows(rows = []) {
  const grouped = {};
  for (const row of Array.isArray(rows) ? rows : []) {
    const source = normalizeText(row?.market || row?.source).toLowerCase();
    const marketHashName = normalizeText(row?.market_hash_name || row?.marketHashName);
    if (!source || !marketHashName) continue;
    if (!grouped[source]) grouped[source] = {};
    const existing = grouped[source][marketHashName];
    const nextFetchedAt = Date.parse(String(row?.fetched_at || ""));
    const prevFetchedAt = Date.parse(String(existing?.fetched_at || ""));
    if (!existing || !Number.isFinite(prevFetchedAt) || nextFetchedAt >= prevFetchedAt) {
      grouped[source][marketHashName] = row;
    }
  }
  return grouped;
}

function buildDiagnosticsFromQuoteQualityFlags(flags = {}, source = "") {
  return buildFallbackDiagnostics({
    marketEnabled:
      flags?.market_enabled == null ? !disabledMarketSources.includes(source) : flags.market_enabled,
    credentialsPresent: flags?.credentials_present,
    authOk: flags?.auth_ok,
    requestSent: flags?.request_sent,
    responseStatus: flags?.response_status,
    responseParsed: flags?.response_parsed,
    listingsFound: flags?.listings_found,
    buyPricePresent: flags?.buy_price_present,
    sellPricePresent: flags?.sell_price_present,
    freshnessPresent: flags?.freshness_present,
    listingUrlPresent: flags?.listing_url_present,
    sourceFailureReason: flags?.source_failure_reason || flags?.source_state,
    lastSuccessAt: flags?.last_success_at || flags?.source_updated_at,
    lastFailureAt: flags?.last_failure_at
  });
}

async function runScannerPath(items = [], compareCacheRows = []) {
  const marketQuoteRepo = require("../src/repositories/marketQuoteRepository");
  const marketPriceRepo = require("../src/repositories/marketPriceRepository");
  const skinRepo = require("../src/repositories/skinRepository");
  const marketService = require("../src/services/marketService");
  const upstreamMarketFreshnessRecoveryService = require("../src/services/upstreamMarketFreshnessRecoveryService");
  const compareCacheBySource = groupLatestMarketPriceRows(compareCacheRows);
  const originals = {
    insertRows: marketQuoteRepo.insertRows,
    upsertRows: marketPriceRepo.upsertRows,
    getLatestByMarketHashNames: marketPriceRepo.getLatestByMarketHashNames,
    getByMarketHashNames: skinRepo.getByMarketHashNames,
    refreshSnapshotsForSkins: marketService.refreshSnapshotsForSkins
  };

  const insertedQuoteRows = [];
  const scannerMarketPriceRows = [];

  marketQuoteRepo.insertRows = async (rows = []) => {
    insertedQuoteRows.push(...rows);
    return rows.length;
  };
  marketPriceRepo.upsertRows = async (rows = []) => {
    scannerMarketPriceRows.push(...rows);
    return rows.length;
  };
  marketPriceRepo.getLatestByMarketHashNames = async (names = [], options = {}) => {
    const requestedNames = new Set((Array.isArray(names) ? names : []).map((name) => normalizeText(name)));
    const sources = Array.isArray(options?.sources) && options.sources.length
      ? options.sources.map((source) => normalizeText(source).toLowerCase())
      : Object.keys(compareCacheBySource);
    return Object.fromEntries(
      sources.map((source) => [
        source,
        Object.fromEntries(
          Object.entries(compareCacheBySource?.[source] || {}).filter(([marketHashName]) =>
            requestedNames.has(normalizeText(marketHashName))
          )
        )
      ])
    );
  };
  skinRepo.getByMarketHashNames = async () => [];
  marketService.refreshSnapshotsForSkins = async () => [];

  try {
    const repairResult = await upstreamMarketFreshnessRecoveryService.repairCatalogRows(items, {
      quoteBatchSize: items.length,
      snapshotBatchSize: 1,
      collectQuoteRowOutcomes: true,
      timeoutMs: 10000,
      maxRetries: 2,
      concurrency: 4
    });

    const byMarket = Object.fromEntries(MARKET_SOURCES.map((source) => [source, []]));
    const summaries = Object.fromEntries(
      MARKET_SOURCES.map((source) => [
        source,
        toJsonObject(repairResult?.quoteRefresh?.quoteSourceDiagnostics?.[source])
      ])
    );
    const quoteRowsByKey = new Map(
      insertedQuoteRows.map((row) => [`${normalizeText(row?.market).toLowerCase()}::${normalizeText(row?.item_name)}`, row])
    );
    const outcomeByKey = toJsonObject(repairResult?.quoteRefresh?.quoteRowOutcomesByKey);

    for (const source of MARKET_SOURCES) {
      for (const item of items) {
        const key = `${source}::${item.marketHashName}`;
        const quoteRow = quoteRowsByKey.get(key) || null;
        const outcome = toJsonObject(outcomeByKey[key]);
        const qualityFlags = toJsonObject(quoteRow?.quality_flags);
        const state =
          normalizeSourceState(qualityFlags?.source_state || outcome?.sourceState) ||
          (quoteRow ? SOURCE_STATES.OK : SOURCE_STATES.NO_DATA);
        const diagnostics = quoteRow
          ? buildDiagnosticsFromQuoteQualityFlags(qualityFlags, source)
          : outcome && Object.keys(outcome).length
            ? buildFallbackDiagnostics({
                marketEnabled: !disabledMarketSources.includes(source),
                requestSent: outcome.requestSent,
                sourceFailureReason: outcome.sourceState,
                lastFailureAt: new Date().toISOString()
              })
            : buildFallbackDiagnostics({
                marketEnabled: !disabledMarketSources.includes(source),
                requestSent: false,
                sourceFailureReason: state,
                lastFailureAt: new Date().toISOString()
              });

        byMarket[source].push({
          itemName: item.marketHashName,
          source,
          path: "scanner",
          state,
          available: Boolean(quoteRow),
          diagnostics,
          buyPrice: toFiniteOrNull(quoteRow?.best_buy),
          sellPrice: toFiniteOrNull(quoteRow?.best_sell_net),
          url: normalizeText(qualityFlags?.url) || null,
          feePercent: sourceFeePercent(source),
          reason: normalizeText(outcome?.reason) || null,
          scannerStatus:
            normalizeText(qualityFlags?.steam_scanner_status || outcome?.scannerStatus) || null
        });
      }
    }

    return {
      byMarket,
      summaries,
      repairDiagnostics: repairResult?.quoteRefresh || {},
      scannerMarketPriceRows
    };
  } finally {
    marketQuoteRepo.insertRows = originals.insertRows;
    marketPriceRepo.upsertRows = originals.upsertRows;
    marketPriceRepo.getLatestByMarketHashNames = originals.getLatestByMarketHashNames;
    skinRepo.getByMarketHashNames = originals.getByMarketHashNames;
    marketService.refreshSnapshotsForSkins = originals.refreshSnapshotsForSkins;
  }
}

function computePathSummary(rows = []) {
  const total = rows.length || 1;
  const statusDistribution = {};
  const sourceSpecificStatusDistribution = {};
  const reasonBuckets = {};
  for (const row of rows) {
    const state = row.state || SOURCE_STATES.NO_DATA;
    statusDistribution[state] = Number(statusDistribution[state] || 0) + 1;
    if (row.scannerStatus) {
      sourceSpecificStatusDistribution[row.scannerStatus] =
        Number(sourceSpecificStatusDistribution[row.scannerStatus] || 0) + 1;
    }
    if (!row.available) {
      const key = row.reason || row.state || "unknown";
      reasonBuckets[key] = Number(reasonBuckets[key] || 0) + 1;
    }
  }

  const count = (predicate) => rows.filter(predicate).length;
  const ratio = (value) => Number((value / total).toFixed(3));
  return {
    itemCount: rows.length,
    authStatus: summarizeAuthStatus(rows),
    requestSuccessRate: ratio(
      count(
        (row) =>
          row.diagnostics?.request_sent === true &&
          ![SOURCE_STATES.TIMEOUT, SOURCE_STATES.UNAVAILABLE, SOURCE_STATES.AUTH_FAILED].includes(
            row.state
          )
      )
    ),
    parseSuccessRate: ratio(count((row) => row.diagnostics?.response_parsed === true)),
    freshnessCoverage: ratio(
      count(
        (row) =>
          row.diagnostics?.freshness_present === true ||
          (row.available && Boolean(row.buyPrice != null))
      )
    ),
    buyPriceCoverage: ratio(count((row) => row.buyPrice != null && row.buyPrice > 0)),
    sellPriceCoverage: ratio(count((row) => row.sellPrice != null && row.sellPrice > 0)),
    listingUrlCoverage: ratio(
      count((row) => row.diagnostics?.listing_url_present === true || Boolean(row.url))
    ),
    feeCoverage: ratio(count((row) => Number.isFinite(Number(row.feePercent)))),
    statusDistribution,
    sourceSpecificStatusDistribution,
    reasonBuckets
  };
}

function buildMismatchReport(compareRows = [], scannerRows = []) {
  const byKey = new Map();
  for (const row of compareRows) {
    byKey.set(`${row.source}::${row.itemName}`, { compare: row, scanner: null });
  }
  for (const row of scannerRows) {
    const key = `${row.source}::${row.itemName}`;
    const existing = byKey.get(key) || { compare: null, scanner: null };
    existing.scanner = row;
    byKey.set(key, existing);
  }
  return Array.from(byKey.values())
    .filter((entry) => Boolean(entry.compare?.available) !== Boolean(entry.scanner?.available))
    .map((entry) => ({
      source: entry.compare?.source || entry.scanner?.source || null,
      itemName: entry.compare?.itemName || entry.scanner?.itemName || null,
      compareAvailable: Boolean(entry.compare?.available),
      compareState: entry.compare?.state || null,
      compareReason: entry.compare?.reason || null,
      scannerAvailable: Boolean(entry.scanner?.available),
      scannerState: entry.scanner?.state || null,
      scannerReason: entry.scanner?.reason || null,
      scannerStatus: entry.scanner?.scannerStatus || null
    }));
}

function classifyScannerSafety(scannerSummary = {}, mismatchCount = 0) {
  if (mismatchCount > 0) return "degrade";
  if (scannerSummary.authStatus.startsWith("failed")) return "disable";
  if (scannerSummary.buyPriceCoverage >= 0.7 && scannerSummary.freshnessCoverage >= 0.7) {
    return "safe";
  }
  if (scannerSummary.buyPriceCoverage > 0) return "degrade";
  return "disable";
}

async function main() {
  const capturedMarketPriceRows = [];
  const compareCleanup = [
    stubModule(abs("src/repositories/marketPriceRepository.js"), {
      getLatestByMarketHashNames: async () => ({}),
      upsertRows: async (rows = []) => {
        capturedMarketPriceRows.push(...rows);
        return rows.length;
      }
    }),
    stubModule(abs("src/repositories/marketQuoteRepository.js"), {
      getLatestCoverageByItemNames: async () => ({})
    }),
    stubModule(abs("src/repositories/userPricePreferenceRepository.js"), {
      getByUserId: async () => null
    }),
    stubModule(abs("src/services/currencyService.js"), {
      resolveCurrency: (value) => normalizeText(value).toUpperCase() || "USD",
      convertAmount: (value) => Number(value),
      ensureFreshFxRates: async () => {}
    })
  ];

  try {
    delete require.cache[abs("src/services/marketComparisonService.js")];
    const marketComparisonService = require("../src/services/marketComparisonService");
    const compareResult = await marketComparisonService.compareItems(COHORT, {
      planTier: "full_access",
      entitlements: { planTier: "full_access", compareView: "full" },
      allowLiveFetch: true,
      forceRefresh: true,
      failWhenAllBlocked: false,
      currency: "USD",
      timeoutMs: 10000,
      maxRetries: 2,
      ttlMinutes: 60
    });

    compareCleanup.reverse().forEach((restore) => restore());

    const compareByMarket = buildCompareRows(compareResult);
    const scannerResult = await runScannerPath(COHORT, capturedMarketPriceRows);

    const perMarket = {};
    const mismatches = [];
    const failingReasonBuckets = {};
    const scannerSafe = [];
    const scannerDegraded = [];

    for (const source of MARKET_SOURCES) {
      const compareSummary = computePathSummary(compareByMarket[source] || []);
      const scannerSummary = computePathSummary(scannerResult.byMarket[source] || []);
      const sourceMismatches = buildMismatchReport(
        compareByMarket[source] || [],
        scannerResult.byMarket[source] || []
      );
      mismatches.push(...sourceMismatches);
      for (const [reason, count] of Object.entries(scannerSummary.reasonBuckets || {})) {
        const key = `${source}:${reason}`;
        failingReasonBuckets[key] = Number(failingReasonBuckets[key] || 0) + Number(count || 0);
      }
      for (const [reason, count] of Object.entries(compareSummary.reasonBuckets || {})) {
        const key = `${source}:${reason}`;
        failingReasonBuckets[key] = Number(failingReasonBuckets[key] || 0) + Number(count || 0);
      }

      const scannerClassification = classifyScannerSafety(
        scannerSummary,
        sourceMismatches.length
      );
      if (scannerClassification === "safe") scannerSafe.push(source);
      else scannerDegraded.push({
        source,
        mode: scannerClassification,
        dominantReasons: Object.entries(scannerSummary.reasonBuckets || {})
          .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
          .slice(0, 3)
      });

      perMarket[source] = {
        disabledByEnv: disabledMarketSources.includes(source),
        compare: compareSummary,
        scanner: scannerSummary,
        mismatchCount: sourceMismatches.length,
        compareLiveDiagnostics: toJsonObject(compareResult?.diagnostics?.liveFetch?.bySource?.[source]),
        scannerAdapterDiagnostics: toJsonObject(scannerResult?.summaries?.[source])
      };
    }

    console.log(
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          cohort: COHORT.map((item) => ({
            marketHashName: item.marketHashName,
            itemCategory: item.itemCategory
          })),
          marketPriceRowsCapturedFromCompare: capturedMarketPriceRows.length,
          perMarket,
          mismatches,
          failingReasonBuckets,
          safeToUseInScanner: scannerSafe,
          mustBeDegradedOrDisabled: scannerDegraded
        },
        null,
        2
      )
    );
  } finally {
    for (const restore of compareCleanup.reverse()) {
      try {
        restore();
      } catch (_err) {
        // ignore cleanup failures
      }
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

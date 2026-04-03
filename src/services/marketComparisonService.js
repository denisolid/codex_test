const AppError = require("../utils/AppError");
const marketPriceRepo = require("../repositories/marketPriceRepository");
const marketQuoteRepo = require("../repositories/marketQuoteRepository");
const userPricePreferenceRepo = require("../repositories/userPricePreferenceRepository");
const steamMarket = require("../markets/steam.market");
const skinportMarket = require("../markets/skinport.market");
const csfloatMarket = require("../markets/csfloat.market");
const dmarketMarket = require("../markets/dmarket.market");
const {
  round2,
  roundPrice,
  sourceFeePercent,
  buildMarketPriceRecord,
  normalizePriceNumber,
  normalizePriceFromMinorUnits
} = require("../markets/marketUtils");
const {
  resolveCurrency,
  convertAmount,
  ensureFreshFxRates
} = require("./currencyService");
const planService = require("./planService");
const arbitrageEngine = require("./arbitrageEngineService");
const premiumCategoryAccessService = require("./premiumCategoryAccessService");
const {
  buildRouteFreshnessContractFromCompareResult
} = require("./scanner/publishValidation");
const {
  marketCompareCacheTtlMinutes,
  marketCompareConcurrency,
  marketCompareTimeoutMs,
  marketCompareMaxRetries,
  disabledMarketSources
} = require("../config/env");
const {
  SOURCE_STATES,
  normalizeSourceState,
  buildMarketHealthDiagnostics,
  hasMarketHealthDiagnostics,
  readMarketHealth,
  toIsoOrNull: toMarketIsoOrNull
} = require("../markets/marketSourceDiagnostics");
const {
  buildScannerMarketPolicyDiagnostics,
  getScannerCoverageMarkets,
  isScannerMarketDisabled
} = require("./scanner/marketReliabilityPolicy");

const SOURCE_ORDER = Object.freeze(["steam", "skinport", "csfloat", "dmarket"]);
const ADAPTERS = Object.freeze({
  steam: steamMarket,
  skinport: skinportMarket,
  csfloat: csfloatMarket,
  dmarket: dmarketMarket
});

const PRICING_MODES = Object.freeze(["steam", "best_sell_net", "lowest_buy"]);
const DEFAULT_PRICING_MODE = "lowest_buy";
const CACHE_TTL_MINUTES = Math.max(Number(marketCompareCacheTtlMinutes || 60), 1);

function normalizeText(value) {
  return String(value || "").trim();
}

function toFiniteOrNull(value) {
  if (value == null) return null;
  if (typeof value === "string" && !value.trim()) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toIsoStringOrNull(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  const ts = new Date(raw).getTime();
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toISOString();
}

function normalizePricingMode(value) {
  const mode = String(value || "")
    .trim()
    .toLowerCase();
  return PRICING_MODES.includes(mode) ? mode : DEFAULT_PRICING_MODE;
}

function isFreshTimestamp(isoValue, ttlMinutes) {
  if (!isoValue) return false;
  const ts = new Date(isoValue).getTime();
  if (Number.isNaN(ts)) return false;
  return Date.now() - ts <= Math.max(Number(ttlMinutes || CACHE_TTL_MINUTES), 1) * 60 * 1000;
}

function resolveUnavailableContextForSource(
  source,
  marketHashName = "",
  liveDiagnosticsBySource = null
) {
  const sourceDiagnostics =
    liveDiagnosticsBySource && typeof liveDiagnosticsBySource === "object"
      ? liveDiagnosticsBySource[source]
      : null;
  if (!sourceDiagnostics || typeof sourceDiagnostics !== "object") {
    return {
      unavailableReason: null,
      sourceState: null,
      sourceDiagnostics: null
    };
  }

  const itemDiagnostics =
    sourceDiagnostics?.diagnosticsByName &&
    typeof sourceDiagnostics.diagnosticsByName === "object"
      ? sourceDiagnostics.diagnosticsByName[marketHashName] || null
      : null;
  const marketReason = String(
    sourceDiagnostics?.failuresByName?.[marketHashName] || ""
  ).trim();
  const sourceReason = String(sourceDiagnostics?.sourceUnavailableReason || "").trim();
  const sourceState =
    normalizeSourceState(sourceDiagnostics?.stateByName?.[marketHashName]) ||
    normalizeSourceState(sourceDiagnostics?.sourceFailureReason) ||
    normalizeSourceState(sourceDiagnostics?.source_failure_reason);
  const diagnostics = hasMarketHealthDiagnostics(itemDiagnostics)
    ? buildMarketHealthDiagnostics({
        marketEnabled: itemDiagnostics.market_enabled,
        credentialsPresent: itemDiagnostics.credentials_present,
        authOk: itemDiagnostics.auth_ok,
        requestSent: itemDiagnostics.request_sent,
        responseStatus: itemDiagnostics.response_status,
        responseParsed: itemDiagnostics.response_parsed,
        listingsFound: itemDiagnostics.listings_found,
        buyPricePresent: itemDiagnostics.buy_price_present,
        sellPricePresent: itemDiagnostics.sell_price_present,
        freshnessPresent: itemDiagnostics.freshness_present,
        listingUrlPresent: itemDiagnostics.listing_url_present,
        sourceFailureReason: itemDiagnostics.source_failure_reason,
        lastSuccessAt: itemDiagnostics.last_success_at,
        lastFailureAt: itemDiagnostics.last_failure_at
      })
    : hasMarketHealthDiagnostics(sourceDiagnostics)
      ? buildMarketHealthDiagnostics({
          marketEnabled: sourceDiagnostics.market_enabled,
          credentialsPresent: sourceDiagnostics.credentials_present,
          authOk: sourceDiagnostics.auth_ok,
          requestSent: sourceDiagnostics.request_sent,
          responseStatus: sourceDiagnostics.response_status,
          responseParsed: sourceDiagnostics.response_parsed,
          listingsFound: sourceDiagnostics.listings_found,
          buyPricePresent: sourceDiagnostics.buy_price_present,
          sellPricePresent: sourceDiagnostics.sell_price_present,
          freshnessPresent: sourceDiagnostics.freshness_present,
          listingUrlPresent: sourceDiagnostics.listing_url_present,
          sourceFailureReason:
            sourceDiagnostics.source_failure_reason || sourceDiagnostics.sourceFailureReason,
          lastSuccessAt: sourceDiagnostics.last_success_at,
          lastFailureAt: sourceDiagnostics.last_failure_at
        })
      : null;

  return {
    unavailableReason: marketReason || sourceReason || null,
    sourceState,
    sourceDiagnostics: diagnostics
  };
}

function toUnavailable(source, currency, details = {}) {
  const unavailableReason =
    details && typeof details === "object" ? details.unavailableReason : details;
  const sourceState =
    details && typeof details === "object"
      ? normalizeSourceState(details.sourceState)
      : null;
  const sourceDiagnostics =
    details && typeof details === "object" ? details.sourceDiagnostics || null : null;
  return {
    source,
    grossPrice: null,
    netPriceAfterFees: null,
    feePercent: sourceFeePercent(source),
    currency,
    url: details?.url || null,
    updatedAt: details?.updatedAt || null,
    confidence: "low",
    available: false,
    sourceState,
    sourceFailureReason:
      normalizeSourceState(sourceDiagnostics?.source_failure_reason) || sourceState || null,
    sourceDiagnostics,
    unavailableReason: unavailableReason
      ? String(unavailableReason || "").trim()
      : null
  };
}

function isSourceDisabled(source) {
  return Array.isArray(disabledMarketSources)
    ? disabledMarketSources.includes(String(source || "").trim().toLowerCase())
    : false;
}

function isScannerBaselinePolicyActive(options = {}) {
  return (
    String(options?.marketReliabilityPolicy || "")
      .trim()
      .toLowerCase() === "scanner_baseline"
  );
}

function getRecordDiagnostics(record = null) {
  const direct = readMarketHealth(record?.raw);
  if (direct) return direct;
  return null;
}

function getCachedRowUpdatedAt(row = null) {
  if (!row || typeof row !== "object") return null;
  return (
    toMarketIsoOrNull(row?.raw?.source_updated_at) ||
    toMarketIsoOrNull(row?.raw?.updated_at) ||
    toMarketIsoOrNull(row?.raw?.updatedAt) ||
    readMarketHealth(row?.raw)?.last_success_at ||
    toIsoStringOrNull(row?.fetched_at || row?.updatedAt)
  );
}

function normalizeItems(items = []) {
  const normalized = [];
  const seen = new Set();

  for (const item of Array.isArray(items) ? items : []) {
    const marketHashName = normalizeText(item?.marketHashName || item?.market_hash_name);
    if (!marketHashName) continue;
    if (seen.has(marketHashName)) continue;
    seen.add(marketHashName);

    const quantity = Math.max(Number(item?.quantity || 0), 0);
    const sevenDayChangePercent = toFiniteOrNull(
      item?.sevenDayChangePercent ??
        item?.seven_day_change_percent ??
        item?.change7dPercent ??
        item?.priceChange7dPercent
    );
    const liquiditySales = toFiniteOrNull(
      item?.liquiditySales ??
        item?.volume7d ??
        item?.volume_7d ??
        item?.salesCount ??
        item?.sales ??
        item?.volume24h ??
        item?.marketVolume24h ??
        item?.marketVolume7d
    );
    const liquidityScore = toFiniteOrNull(
      item?.liquidityScore ??
        item?.managementClue?.metrics?.liquidityScore ??
        item?.marketComparison?.liquidityScore
    );
    normalized.push({
      skinId: Number(item?.skinId || item?.skin_id || 0) || null,
      marketHashName,
      itemCategory: premiumCategoryAccessService.normalizeItemCategory(
        item?.itemCategory || item?.category,
        marketHashName
      ),
      itemSubcategory: String(item?.itemSubcategory || item?.subcategory || "").trim() || null,
      quantity,
      steamPrice: Number(item?.steamPrice || item?.currentPrice || 0),
      steamCurrency: normalizeText(item?.steamCurrency || item?.currency || "USD") || "USD",
      steamRecordedAt: item?.steamRecordedAt || item?.currentPriceRecordedAt || null,
      sevenDayChangePercent,
      liquiditySales,
      liquidityScore,
      volume7d: toFiniteOrNull(item?.volume7d ?? item?.volume_7d),
      marketVolume7d: toFiniteOrNull(item?.marketVolume7d ?? item?.market_volume_7d),
      marketCoverageCount: toFiniteOrNull(item?.marketCoverageCount ?? item?.market_coverage_count)
    });
  }

  return normalized;
}

function applyQuoteCoverageFallback(items = [], coverageByItemName = {}) {
  return (Array.isArray(items) ? items : []).map((item) => {
    const marketHashName = normalizeText(item?.marketHashName);
    const coverage =
      marketHashName && coverageByItemName && typeof coverageByItemName === "object"
        ? coverageByItemName[marketHashName] || {}
        : {};
    const coverageVolume7d = toFiniteOrNull(coverage?.volume7dMax);
    const directVolume7d = [
      toFiniteOrNull(item?.volume7d),
      toFiniteOrNull(item?.marketVolume7d)
    ].find((value) => value != null && value >= 0);
    const resolvedVolume7d = directVolume7d != null ? directVolume7d : coverageVolume7d;
    const directMarketCoverage = toFiniteOrNull(item?.marketCoverageCount);
    const coverageMarketCount = toFiniteOrNull(coverage?.marketCoverageCount);

    return {
      ...item,
      volume7d: resolvedVolume7d,
      marketVolume7d: resolvedVolume7d,
      liquiditySales:
        toFiniteOrNull(item?.liquiditySales) != null
          ? toFiniteOrNull(item?.liquiditySales)
          : resolvedVolume7d,
      marketCoverageCount:
        directMarketCoverage != null ? directMarketCoverage : coverageMarketCount
    };
  });
}

function normalizeLiveRecord(record, displayCurrency) {
  if (!record || record.grossPrice == null) return null;

  const fromCurrency = String(record.currency || "USD")
    .trim()
    .toUpperCase();
  const gross =
    fromCurrency === displayCurrency
      ? Number(record.grossPrice)
      : convertAmount(record.grossPrice, fromCurrency, displayCurrency);
  if (!Number.isFinite(gross)) return null;

  return buildMarketPriceRecord({
    source: record.source,
    marketHashName: record.marketHashName,
    grossPrice: gross,
    currency: displayCurrency,
    url: record.url || null,
    updatedAt: record.updatedAt || new Date().toISOString(),
    confidence: record.confidence || "medium",
    raw: record.raw || null,
    feePercent: record.feePercent
  });
}

function parseDmarketUsdMinorValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  if (/^-?\d+$/.test(raw)) {
    return normalizePriceFromMinorUnits(Number(raw));
  }
  return normalizePriceNumber(raw);
}

function maybeRepairLegacyDmarketGross(row, source, rowCurrency, rowGross) {
  if (source !== "dmarket" || rowCurrency !== "USD" || !Number.isFinite(rowGross) || rowGross <= 0) {
    return rowGross;
  }

  const rawUsd = row?.raw?.price?.USD ?? row?.raw?.price?.usd;
  const parsedUsd = parseDmarketUsdMinorValue(rawUsd);
  if (!Number.isFinite(parsedUsd) || parsedUsd <= 0) {
    return rowGross;
  }

  // Repair legacy cached rows where dmarket "amount" was parsed as gross price.
  if (parsedUsd > rowGross && Math.abs(parsedUsd - rowGross) >= 0.2) {
    return parsedUsd;
  }

  return rowGross;
}

function fromCachedRow(row, displayCurrency) {
  if (!row) return null;
  const source = String(row.market || row.source || "").trim().toLowerCase();
  const marketHashName = normalizeText(row.market_hash_name || row.marketHashName);
  const rowCurrency = String(row.currency || "USD")
    .trim()
    .toUpperCase();
  const rowGrossRaw = Number(row.gross_price ?? row.grossPrice);
  const rowGross = maybeRepairLegacyDmarketGross(row, source, rowCurrency, rowGrossRaw);
  if (!source || !marketHashName || !Number.isFinite(rowGross)) {
    return null;
  }

  const gross =
    rowCurrency === displayCurrency
      ? rowGross
      : convertAmount(rowGross, rowCurrency, displayCurrency);
  if (!Number.isFinite(gross)) {
    return null;
  }

  return buildMarketPriceRecord({
    source,
    marketHashName,
    grossPrice: gross,
    currency: displayCurrency,
    url: row.url || null,
    updatedAt: getCachedRowUpdatedAt(row),
    confidence: row?.raw?.confidence || "medium",
    raw: row.raw || null
  });
}

function toUpsertRow(record, fetchedAtIso = null) {
  if (!record || !record.source || !record.marketHashName) return null;
  const fetchedAt = toIsoStringOrNull(fetchedAtIso) || new Date().toISOString();
  const sourceUpdatedAt = toIsoStringOrNull(
    record.updatedAt || record.fetched_at || record.fetchedAt
  );
  return {
    market: record.source,
    market_hash_name: record.marketHashName,
    currency: record.currency || "USD",
    gross_price: Number(record.grossPrice || 0),
    net_price: Number(record.netPriceAfterFees || 0),
    url: record.url || null,
    fetched_at: fetchedAt,
    raw: {
      ...(record.raw || {}),
      confidence: record.confidence || "medium",
      source_updated_at: sourceUpdatedAt
    }
  };
}

function pickBestBuy(perMarket = []) {
  return perMarket
    .filter((row) => row.available && Number.isFinite(Number(row.grossPrice)))
    .sort((a, b) => Number(a.grossPrice) - Number(b.grossPrice))[0] || null;
}

function pickBestSellNet(perMarket = []) {
  return perMarket
    .filter((row) => row.available && Number.isFinite(Number(row.netPriceAfterFees)))
    .sort((a, b) => Number(b.netPriceAfterFees) - Number(a.netPriceAfterFees))[0] || null;
}

function selectByPricingMode(pricingMode, stream) {
  const steam = stream?.steam || null;
  const bestBuy = stream?.bestBuy || null;
  const bestSellNet = stream?.bestSellNet || null;

  if (pricingMode === "steam") {
    return steam || bestBuy || bestSellNet || null;
  }
  if (pricingMode === "best_sell_net") {
    return bestSellNet || steam || bestBuy || null;
  }
  return bestBuy || steam || bestSellNet || null;
}

function getModeUnitPrice(pricingMode, selected) {
  if (!selected) return 0;
  if (pricingMode === "best_sell_net") {
    return Number(selected.netPriceAfterFees || 0);
  }
  return Number(selected.grossPrice || 0);
}

function buildFeeSummary() {
  return {
    steam: sourceFeePercent("steam"),
    skinport: sourceFeePercent("skinport"),
    csfloat: sourceFeePercent("csfloat"),
    dmarket: sourceFeePercent("dmarket")
  };
}

async function resolvePlanContext(options = {}) {
  if (options?.entitlements && typeof options.entitlements === "object") {
    return {
      planTier: planService.normalizePlanTier(
        options.planTier || options.entitlements.planTier
      ),
      entitlements: options.entitlements
    };
  }

  const userId = normalizeText(options?.userId);
  if (userId) {
    const { planTier, entitlements } = await planService.getUserPlanProfile(userId);
    return { planTier, entitlements };
  }

  const planTier = planService.normalizePlanTier(options.planTier || "full_access");
  return {
    planTier,
    entitlements: planService.getEntitlements(planTier)
  };
}

async function fetchLiveMarketData(itemsBySource = {}, displayCurrency, options = {}) {
  const result = {};
  const diagnosticsBySource = {};
  const rowsToStore = [];
  const scannerBaselinePolicy = isScannerBaselinePolicyActive(options);
  const fetchCurrency = String(options.fetchCurrency || "USD")
    .trim()
    .toUpperCase();
  const concurrency = Math.max(Number(options.concurrency || marketCompareConcurrency || 4), 1);
  const timeoutMs = Number(options.timeoutMs || marketCompareTimeoutMs || 9000);
  const maxRetries = Number(options.maxRetries || marketCompareMaxRetries || 3);

  const sourceResults = await Promise.all(
    SOURCE_ORDER.map(async (source) => {
      const adapter = ADAPTERS[source];
      const sourceItems = Array.isArray(itemsBySource[source]) ? itemsBySource[source] : [];
      if (!sourceItems.length) {
        return { source, recordsByName: {}, diagnostics: {} };
      }
      if (scannerBaselinePolicy && isScannerMarketDisabled(source)) {
        const failuresByName = Object.fromEntries(
          sourceItems.map((item) => [item.marketHashName, "Market disabled for scanner policy."])
        );
        const stateByName = Object.fromEntries(
          sourceItems.map((item) => [item.marketHashName, SOURCE_STATES.DISABLED])
        );
        const diagnosticsByName = Object.fromEntries(
          sourceItems.map((item) => [
            item.marketHashName,
            buildMarketHealthDiagnostics({
              marketEnabled: true,
              requestSent: false,
              sourceFailureReason: SOURCE_STATES.DISABLED
            })
          ])
        );
        return {
          source,
          recordsByName: {},
          diagnostics: {
            failuresByName,
            stateByName,
            diagnosticsByName,
            market_enabled: true,
            credentials_present: null,
            auth_ok: null,
            request_sent: false,
            response_status: null,
            response_parsed: null,
            listings_found: null,
            buy_price_present: false,
            sell_price_present: false,
            freshness_present: null,
            listing_url_present: false,
            sourceUnavailableReason: "Market disabled for scanner policy.",
            sourceFailureReason: SOURCE_STATES.DISABLED,
            source_failure_reason: SOURCE_STATES.DISABLED,
            last_success_at: null,
            last_failure_at: new Date().toISOString(),
            pipeline: null
          }
        };
      }
      if (isSourceDisabled(source)) {
        const failuresByName = Object.fromEntries(
          sourceItems.map((item) => [item.marketHashName, `${source} source disabled`])
        );
        const stateByName = Object.fromEntries(
          sourceItems.map((item) => [item.marketHashName, SOURCE_STATES.DISABLED])
        );
        const diagnosticsByName = Object.fromEntries(
          sourceItems.map((item) => [
            item.marketHashName,
            buildMarketHealthDiagnostics({
              marketEnabled: false,
              requestSent: false,
              sourceFailureReason: SOURCE_STATES.DISABLED
            })
          ])
        );
        return {
          source,
          recordsByName: {},
          diagnostics: {
            failuresByName,
            stateByName,
            diagnosticsByName,
            market_enabled: false,
            credentials_present: null,
            auth_ok: null,
            request_sent: false,
            response_status: null,
            response_parsed: null,
            listings_found: null,
            buy_price_present: false,
            sell_price_present: false,
            freshness_present: null,
            listing_url_present: false,
            sourceUnavailableReason: `${source} source disabled`,
            sourceFailureReason: SOURCE_STATES.DISABLED,
            source_failure_reason: SOURCE_STATES.DISABLED,
            last_success_at: null,
            last_failure_at: new Date().toISOString(),
            pipeline: null
          }
        };
      }
      if (!adapter?.batchGetPrices) {
        return {
          source,
          recordsByName: {},
          diagnostics: {
            failuresByName: {},
            stateByName: {},
            diagnosticsByName: {},
            market_enabled: false,
            request_sent: false,
            sourceUnavailableReason: `${source} adapter unavailable`,
            sourceFailureReason: SOURCE_STATES.UNAVAILABLE,
            source_failure_reason: SOURCE_STATES.UNAVAILABLE
          }
        };
      }

      try {
        const byName = await adapter.batchGetPrices(sourceItems, {
          currency: fetchCurrency,
          concurrency,
          timeoutMs,
          maxRetries
        });
        const sourceFetchedAt = new Date().toISOString();
        const adapterMeta = byName && typeof byName === "object" ? byName.__meta : null;
        const recordsByName = {};
        const upsertRows = [];
        for (const [marketHashName, rawRecord] of Object.entries(byName || {})) {
          const normalized = normalizeLiveRecord(rawRecord, displayCurrency);
          if (!normalized) continue;
          recordsByName[marketHashName] = normalized;
          const row = toUpsertRow(rawRecord, sourceFetchedAt);
          if (row) upsertRows.push(row);
        }

        return {
          source,
          recordsByName,
          diagnostics: {
            failuresByName:
              adapterMeta?.failuresByName && typeof adapterMeta.failuresByName === "object"
                ? adapterMeta.failuresByName
                : {},
            stateByName:
              adapterMeta?.stateByName && typeof adapterMeta.stateByName === "object"
                ? adapterMeta.stateByName
                : {},
            diagnosticsByName:
              adapterMeta?.diagnosticsByName && typeof adapterMeta.diagnosticsByName === "object"
                ? adapterMeta.diagnosticsByName
                : {},
            market_enabled:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "market_enabled")
                ? Boolean(adapterMeta.market_enabled)
                : true,
            credentials_present:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "credentials_present")
                ? adapterMeta.credentials_present
                : null,
            auth_ok:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "auth_ok")
                ? adapterMeta.auth_ok
                : null,
            request_sent:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "request_sent")
                ? adapterMeta.request_sent
                : sourceItems.length > 0,
            response_status: Number.isFinite(Number(adapterMeta?.response_status))
              ? Number(adapterMeta.response_status)
              : null,
            response_parsed:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "response_parsed")
                ? adapterMeta.response_parsed
                : null,
            listings_found:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "listings_found")
                ? adapterMeta.listings_found
                : null,
            buy_price_present:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "buy_price_present")
                ? adapterMeta.buy_price_present
                : null,
            sell_price_present:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "sell_price_present")
                ? adapterMeta.sell_price_present
                : null,
            freshness_present:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "freshness_present")
                ? adapterMeta.freshness_present
                : null,
            listing_url_present:
              adapterMeta && Object.prototype.hasOwnProperty.call(adapterMeta, "listing_url_present")
                ? adapterMeta.listing_url_present
                : null,
            sourceUnavailableReason: normalizeText(adapterMeta?.sourceUnavailableReason) || null,
            sourceFailureReason:
              normalizeSourceState(
                adapterMeta?.sourceFailureReason || adapterMeta?.source_failure_reason
              ) || null,
            source_failure_reason:
              normalizeSourceState(adapterMeta?.source_failure_reason) ||
              normalizeSourceState(adapterMeta?.sourceFailureReason) ||
              null,
            last_success_at: toMarketIsoOrNull(adapterMeta?.last_success_at),
            last_failure_at: toMarketIsoOrNull(adapterMeta?.last_failure_at),
            pipeline:
              adapterMeta?.pipeline && typeof adapterMeta.pipeline === "object"
                ? adapterMeta.pipeline
                : null
          },
          upsertRows
        };
      } catch (err) {
        const message = normalizeText(err?.message) || `${source} live fetch failed`;
        const diagnostics = buildMarketHealthDiagnostics({
          marketEnabled: true,
          requestSent: true,
          responseStatus: err?.status || err?.statusCode || err?.upstreamStatus || null,
          sourceFailureReason: SOURCE_STATES.UNAVAILABLE,
          lastFailureAt: new Date().toISOString()
        });
        return {
          source,
          recordsByName: {},
          diagnostics: {
            failuresByName: Object.fromEntries(sourceItems.map((item) => [item.marketHashName, message])),
            stateByName: Object.fromEntries(
              sourceItems.map((item) => [item.marketHashName, SOURCE_STATES.UNAVAILABLE])
            ),
            diagnosticsByName: Object.fromEntries(
              sourceItems.map((item) => [item.marketHashName, diagnostics])
            ),
            market_enabled: true,
            request_sent: true,
            response_status: diagnostics.response_status,
            response_parsed: false,
            listings_found: null,
            buy_price_present: false,
            sell_price_present: false,
            freshness_present: null,
            listing_url_present: false,
            sourceUnavailableReason: message,
            sourceFailureReason: SOURCE_STATES.UNAVAILABLE,
            source_failure_reason: SOURCE_STATES.UNAVAILABLE,
            last_success_at: null,
            last_failure_at: diagnostics.last_failure_at,
            pipeline: null
          }
        };
      }
    })
  );

  for (const sourceResult of sourceResults) {
    const source = String(sourceResult?.source || "").trim().toLowerCase();
    if (!source) continue;
    result[source] =
      sourceResult?.recordsByName && typeof sourceResult.recordsByName === "object"
        ? sourceResult.recordsByName
        : {};
    diagnosticsBySource[source] =
      sourceResult?.diagnostics && typeof sourceResult.diagnostics === "object"
        ? sourceResult.diagnostics
        : {};
    rowsToStore.push(...(Array.isArray(sourceResult?.upsertRows) ? sourceResult.upsertRows : []));
  }

  if (rowsToStore.length) {
    await marketPriceRepo.upsertRows(rowsToStore);
  }

  return {
    recordsBySource: result,
    diagnosticsBySource
  };
}

function buildSteamFallback(item, displayCurrency) {
  const steamPrice = Number(item.steamPrice || 0);
  if (!Number.isFinite(steamPrice) || steamPrice <= 0) {
    return null;
  }

  const steamCurrency = String(item.steamCurrency || "USD")
    .trim()
    .toUpperCase();
  const gross =
    steamCurrency === displayCurrency
      ? steamPrice
      : convertAmount(steamPrice, steamCurrency, displayCurrency);
  if (!Number.isFinite(gross) || gross <= 0) {
    return null;
  }

  return buildMarketPriceRecord({
    source: "steam",
    marketHashName: item.marketHashName,
    grossPrice: gross,
    currency: displayCurrency,
    url: `https://steamcommunity.com/market/listings/730/${encodeURIComponent(
      item.marketHashName
    )}`,
    updatedAt: item.steamRecordedAt || new Date().toISOString(),
    confidence: "medium",
    raw: {
      source: "portfolio-price-history-fallback"
    }
  });
}

async function resolvePricingMode(userId, requestedMode, displayCurrency) {
  const safeUserId = normalizeText(userId);
  const requested = normalizePricingMode(requestedMode);
  if (!safeUserId) {
    return {
      pricingMode: requested,
      preferredCurrency: displayCurrency
    };
  }

  const stored = await userPricePreferenceRepo.getByUserId(safeUserId);
  if (!stored) {
    return {
      pricingMode: requested,
      preferredCurrency: displayCurrency
    };
  }

  return {
    pricingMode: requestedMode ? requested : normalizePricingMode(stored.pricing_mode),
    preferredCurrency: resolveCurrency(stored.preferred_currency || displayCurrency)
  };
}

exports.getUserPricePreference = async (userId, options = {}) => {
  const fallbackCurrency = resolveCurrency(options.currency || "USD");
  const safeUserId = normalizeText(userId);
  if (!safeUserId) {
    return {
      pricingMode: DEFAULT_PRICING_MODE,
      preferredCurrency: fallbackCurrency
    };
  }

  const row = await userPricePreferenceRepo.getByUserId(safeUserId);
  if (!row) {
    return {
      pricingMode: DEFAULT_PRICING_MODE,
      preferredCurrency: fallbackCurrency
    };
  }

  return {
    pricingMode: normalizePricingMode(row.pricing_mode),
    preferredCurrency: resolveCurrency(row.preferred_currency || fallbackCurrency)
  };
};

exports.updateUserPricePreference = async (userId, updates = {}) => {
  const safeUserId = normalizeText(userId);
  if (!safeUserId) {
    throw new AppError("Unauthorized", 401, "UNAUTHORIZED");
  }

  const patch = {};
  if (updates.pricingMode != null) {
    const mode = String(updates.pricingMode || "")
      .trim()
      .toLowerCase();
    if (!PRICING_MODES.includes(mode)) {
      throw new AppError(
        `pricingMode must be one of: ${PRICING_MODES.join(", ")}`,
        400,
        "VALIDATION_ERROR"
      );
    }
    patch.pricingMode = mode;
  }

  if (updates.preferredCurrency != null) {
    patch.preferredCurrency = resolveCurrency(updates.preferredCurrency);
  }

  if (!Object.keys(patch).length) {
    return exports.getUserPricePreference(safeUserId);
  }

  const row = await userPricePreferenceRepo.upsertByUserId(safeUserId, patch);
  return {
    pricingMode: normalizePricingMode(row.pricing_mode),
    preferredCurrency: resolveCurrency(row.preferred_currency)
  };
};

exports.compareItems = async (items = [], options = {}) => {
  await ensureFreshFxRates();
  const planContext = await resolvePlanContext(options);
  const entitlements =
    planContext?.entitlements || planService.getEntitlements(planContext?.planTier);
  const planConfig = planService.getPlanConfig(planContext?.planTier || entitlements?.planTier || "free");
  const compareView = String(planConfig?.compareView || entitlements?.compareView || "limited")
    .trim()
    .toLowerCase();
  const compareViewLimited = compareView === "limited";
  const premiumCategoryAccess = premiumCategoryAccessService.hasPremiumCategoryAccess(
    entitlements
  );
  const compareViewMaxItems = Math.max(Number(planConfig?.compareViewMaxItems || 200), 1);
  const normalizedItemsAll = normalizeItems(items);
  const comparedItemsSubmitted = normalizedItemsAll.length;
  const blockedPremiumItemNames = [];
  const normalizedEligibleItems = normalizedItemsAll.filter((item) => {
    const category = premiumCategoryAccessService.normalizeItemCategory(
      item?.itemCategory,
      item?.marketHashName
    );
    if (!premiumCategoryAccessService.isPremiumCategory(category)) return true;
    if (premiumCategoryAccess) return true;
    blockedPremiumItemNames.push(String(item?.marketHashName || "").trim() || "Premium item");
    return false;
  });
  const normalizedItems = normalizedEligibleItems.slice(0, compareViewMaxItems);
  const comparedItemsRequested = normalizedEligibleItems.length;
  const comparedItemsProcessed = normalizedItems.length;
  const truncatedByPlan = Math.max(comparedItemsRequested - comparedItemsProcessed, 0);
  const blockedPremiumItems = blockedPremiumItemNames.length;
  const displayCurrency = resolveCurrency(options.currency || "USD");
  const scannerBaselinePolicy = isScannerBaselinePolicyActive(options);
  const modeResolution = await resolvePricingMode(
    options.userId,
    options.pricingMode,
    displayCurrency
  );
  const pricingMode = modeResolution.pricingMode;

  if (
    !normalizedItems.length &&
    blockedPremiumItems > 0 &&
    options.failWhenAllBlocked !== false
  ) {
    throw new AppError(
      "Unlock knife and glove opportunities with Full Access to compare premium high-value market categories.",
      402,
      "PLAN_UPGRADE_REQUIRED"
    );
  }

  if (!normalizedItems.length) {
    return {
      currency: displayCurrency,
      pricingMode,
      fees: buildFeeSummary(),
      generatedAt: new Date().toISOString(),
      ttlMinutes: CACHE_TTL_MINUTES,
      opportunities: [],
      items: [],
      plan: {
        planTier: planContext?.planTier || "free",
        compareView,
        compareViewMaxItems,
        premiumCategoryAccess,
        comparedItemsSubmitted,
        comparedItemsRequested,
        comparedItemsProcessed,
        truncatedByPlan,
        blockedPremiumItems
      },
      summary: {
        totalValueSelected: 0,
        totalValueSteam: 0,
        totalValueBestSellNet: 0,
        totalValueLowestBuy: 0,
        pricedItemsCount: 0,
        unavailableItemsCount: 0,
        blockedPremiumItems,
        arbitrageCandidatesCount: 0,
        arbitrageOpportunitiesCount: 0
      },
      diagnostics: {
        scannerPolicy: scannerBaselinePolicy ? buildScannerMarketPolicyDiagnostics() : null,
        liveFetch: {
          enabled: false,
          forceRefresh: false,
          requestedBySource: {},
          bySource: {}
        }
      }
    };
  }

  const marketHashNames = normalizedItems.map((item) => item.marketHashName);
  let quoteCoverageByItemName = {};
  try {
    quoteCoverageByItemName = await marketQuoteRepo.getLatestCoverageByItemNames(marketHashNames, {
      markets: scannerBaselinePolicy ? getScannerCoverageMarkets() : undefined
    });
  } catch (_err) {
    quoteCoverageByItemName = {};
  }
  const normalizedItemsWithCoverage = applyQuoteCoverageFallback(
    normalizedItems,
    quoteCoverageByItemName
  );

  const cachedBySource = await marketPriceRepo.getLatestByMarketHashNames(marketHashNames, {
    sources: SOURCE_ORDER
  });

  const allowLiveFetch = options.allowLiveFetch !== false;
  const forceRefresh = Boolean(options.forceRefresh);
  const staleItemsBySource = {};

  for (const source of SOURCE_ORDER) {
    if ((scannerBaselinePolicy && isScannerMarketDisabled(source)) || isSourceDisabled(source)) {
      staleItemsBySource[source] = [];
      continue;
    }
    const sourceRows = cachedBySource[source] || {};
    staleItemsBySource[source] = normalizedItemsWithCoverage.filter((item) => {
      const cached = sourceRows[item.marketHashName];
      if (!cached) return true;
      if (forceRefresh) return true;
      return !isFreshTimestamp(cached.fetched_at, options.ttlMinutes || CACHE_TTL_MINUTES);
    });
  }
  const liveFetchRequestedBySource = Object.fromEntries(
    SOURCE_ORDER.map((source) => [source, Number(staleItemsBySource[source]?.length || 0)])
  );

  let liveBySource = {};
  let liveDiagnosticsBySource = {};
  if (allowLiveFetch) {
    const hasPending = SOURCE_ORDER.some(
      (source) => Array.isArray(staleItemsBySource[source]) && staleItemsBySource[source].length
    );
    if (hasPending) {
      const liveData = await fetchLiveMarketData(staleItemsBySource, displayCurrency, {
        ...options,
        fetchCurrency: "USD",
        concurrency: options.concurrency,
        timeoutMs: options.timeoutMs,
        maxRetries: options.maxRetries
      });
      liveBySource = liveData?.recordsBySource || {};
      liveDiagnosticsBySource = liveData?.diagnosticsBySource || {};
    }
  }

  const summary = {
    totalValueSelected: 0,
    totalValueSteam: 0,
    totalValueBestSellNet: 0,
    totalValueLowestBuy: 0,
    pricedItemsCount: 0,
    unavailableItemsCount: 0
  };

  const enrichedItems = normalizedItemsWithCoverage.map((item) => {
    const perMarket = SOURCE_ORDER.map((source) => {
      const sourceCachedRow = cachedBySource?.[source]?.[item.marketHashName] || null;
      const cached = fromCachedRow(sourceCachedRow, displayCurrency);
      const cachedFresh =
        Boolean(sourceCachedRow) &&
        !forceRefresh &&
        isFreshTimestamp(sourceCachedRow?.fetched_at, options.ttlMinutes || CACHE_TTL_MINUTES);
      const live = liveBySource?.[source]?.[item.marketHashName] || null;
      if (scannerBaselinePolicy && isScannerMarketDisabled(source)) {
        return toUnavailable(source, displayCurrency, {
          unavailableReason: "Market disabled for scanner policy.",
          sourceState: SOURCE_STATES.DISABLED,
          sourceDiagnostics: buildMarketHealthDiagnostics({
            marketEnabled: true,
            requestSent: false,
            sourceFailureReason: SOURCE_STATES.DISABLED,
            lastFailureAt: new Date().toISOString()
          })
        });
      }
      if (live) {
        return {
          ...live,
          available: true,
          sourceState: SOURCE_STATES.OK,
          sourceFailureReason: null,
          sourceDiagnostics: getRecordDiagnostics(live)
        };
      }

      if (cached && cachedFresh) {
        return {
          ...cached,
          available: true,
          sourceState: SOURCE_STATES.OK,
          sourceFailureReason: null,
          sourceDiagnostics: getRecordDiagnostics(cached)
        };
      }

      if (isSourceDisabled(source)) {
        return toUnavailable(source, displayCurrency, {
          unavailableReason: `${source} source disabled`,
          sourceState: SOURCE_STATES.DISABLED,
          sourceDiagnostics: buildMarketHealthDiagnostics({
            marketEnabled: false,
            requestSent: false,
            sourceFailureReason: SOURCE_STATES.DISABLED,
            lastFailureAt: new Date().toISOString()
          })
        });
      }

      if (source === "steam" && options.allowSyntheticSteamFallback === true) {
        const fallback = buildSteamFallback(item, displayCurrency);
        if (fallback) {
          return {
            ...fallback,
            available: true,
            sourceState: SOURCE_STATES.OK,
            sourceFailureReason: null,
            sourceDiagnostics: buildMarketHealthDiagnostics({
              marketEnabled: true,
              requestSent: false,
              responseParsed: true,
              listingsFound: true,
              buyPricePresent: true,
              sellPricePresent: true,
              freshnessPresent: Boolean(fallback.updatedAt),
              listingUrlPresent: Boolean(fallback.url),
              lastSuccessAt: fallback.updatedAt
            })
          };
        }
      }

      const liveFailureContext = resolveUnavailableContextForSource(
        source,
        item.marketHashName,
        liveDiagnosticsBySource
      );
      if (liveFailureContext.sourceState || liveFailureContext.unavailableReason) {
        return toUnavailable(source, displayCurrency, {
          ...liveFailureContext,
          updatedAt: cached?.updatedAt || null,
          url: cached?.url || null
        });
      }

      if (cached) {
        const cachedDiagnostics =
          getRecordDiagnostics(cached) ||
          buildMarketHealthDiagnostics({
            marketEnabled: true,
            requestSent: false,
            responseParsed: true,
            listingsFound: true,
            buyPricePresent: true,
            sellPricePresent: true,
            freshnessPresent: Boolean(cached.updatedAt),
            listingUrlPresent: Boolean(cached.url),
            sourceFailureReason: SOURCE_STATES.STALE,
            lastSuccessAt: cached.updatedAt,
            lastFailureAt: new Date().toISOString()
          });
        return toUnavailable(source, displayCurrency, {
          unavailableReason: "Market data is stale.",
          sourceState: SOURCE_STATES.STALE,
          sourceDiagnostics: cachedDiagnostics,
          updatedAt: cached.updatedAt,
          url: cached.url
        });
      }

      return toUnavailable(source, displayCurrency, {
        unavailableReason: "No market data available.",
        sourceState: SOURCE_STATES.NO_DATA,
        sourceDiagnostics: buildMarketHealthDiagnostics({
          marketEnabled: true,
          requestSent: false,
          responseParsed: null,
          listingsFound: false,
          buyPricePresent: false,
          sellPricePresent: false,
          freshnessPresent: false,
          listingUrlPresent: false,
          sourceFailureReason: SOURCE_STATES.NO_DATA,
          lastFailureAt: new Date().toISOString()
        })
      });
    });

    const bestBuy = pickBestBuy(perMarket);
    const bestSellNet = pickBestSellNet(perMarket);
    const steam = perMarket.find((row) => row.source === "steam" && row.available) || null;
    const selected = selectByPricingMode(pricingMode, {
      steam,
      bestBuy,
      bestSellNet
    });
    const quantity = Number(item.quantity || 0);
    const selectedUnitPrice = roundPrice(getModeUnitPrice(pricingMode, selected));
    const selectedLineValue = roundPrice(selectedUnitPrice * quantity);

    const steamUnit = Number(steam?.grossPrice || 0);
    const bestBuyUnit = Number(bestBuy?.grossPrice || 0);
    const bestSellNetUnit = Number(bestSellNet?.netPriceAfterFees || 0);
    const arbitrage = arbitrageEngine.evaluateItemOpportunity({
      skinId: item.skinId,
      marketHashName: item.marketHashName,
      itemCategory: item.itemCategory,
      itemSubcategory: item.itemSubcategory,
      perMarket,
      sevenDayChangePercent: item.sevenDayChangePercent,
      liquiditySales: item.liquiditySales,
      liquidityScore: item.liquidityScore
    });
    const limitedPerMarket = compareViewLimited ? perMarket.slice(0, 2) : perMarket;
    const responseBestBuy = pickBestBuy(limitedPerMarket) || bestBuy;
    const responseBestSellNet = pickBestSellNet(limitedPerMarket) || bestSellNet;
    const responseArbitrage = compareViewLimited
      ? {
          isOpportunity: false,
          scoreCategory: "Locked",
          executionConfidence: "Locked",
          lockReason: "compare_view_limited",
          lockMessage: "Unlock Full Access for complete compare drawer depth and arbitrage diagnostics."
        }
      : arbitrage;
    const routeFreshnessContract = buildRouteFreshnessContractFromCompareResult(
      {
        perMarket: limitedPerMarket,
        bestBuy: responseBestBuy,
        bestSellNet: responseBestSellNet,
        arbitrage: responseArbitrage
      },
      {
        buyMarket: responseArbitrage?.buyMarket || responseBestBuy?.source,
        sellMarket: responseArbitrage?.sellMarket || responseBestSellNet?.source
      }
    );

    summary.totalValueSelected += selectedLineValue;
    summary.totalValueSteam += steamUnit * quantity;
    summary.totalValueBestSellNet += bestSellNetUnit * quantity;
    summary.totalValueLowestBuy += bestBuyUnit * quantity;

    if (selectedUnitPrice > 0) {
      summary.pricedItemsCount += 1;
    } else {
      summary.unavailableItemsCount += 1;
    }

    return {
      skinId: item.skinId,
      marketHashName: item.marketHashName,
      itemCategory: item.itemCategory,
      itemSubcategory: item.itemSubcategory,
      quantity: Number(item.quantity || 0),
      perMarket: limitedPerMarket,
      bestBuy: responseBestBuy
        ? {
            source: responseBestBuy.source,
            grossPrice: responseBestBuy.grossPrice,
            currency: responseBestBuy.currency,
            url: responseBestBuy.url
          }
        : null,
      bestSellNet: responseBestSellNet
        ? {
            source: responseBestSellNet.source,
            netPriceAfterFees: responseBestSellNet.netPriceAfterFees,
            currency: responseBestSellNet.currency,
            url: responseBestSellNet.url
          }
        : null,
      selectedPricingSource: selected?.source || null,
      selectedUnitPrice,
      selectedLineValue,
      routeFreshnessContract,
      buyRouteAvailable: routeFreshnessContract.buyRouteAvailable,
      sellRouteAvailable: routeFreshnessContract.sellRouteAvailable,
      buyRouteUpdatedAt: routeFreshnessContract.buyRouteUpdatedAt,
      sellRouteUpdatedAt: routeFreshnessContract.sellRouteUpdatedAt,
      buyListingAvailable: routeFreshnessContract.buyListingAvailable,
      sellListingAvailable: routeFreshnessContract.sellListingAvailable,
      requiredRouteState: routeFreshnessContract.requiredRouteState,
      listingAvailabilityState: routeFreshnessContract.listingAvailabilityState,
      arbitrage: responseArbitrage,
      totalsByMode: {
        steam: roundPrice(steamUnit * quantity),
        best_sell_net: roundPrice(bestSellNetUnit * quantity),
        lowest_buy: roundPrice(bestBuyUnit * quantity)
      }
    };
  });
  const rankedOpportunities = arbitrageEngine.rankOpportunities(
    enrichedItems.map((item) => item.arbitrage),
    {
      limit: 5,
      includeRisky: false,
      sortBy: "score"
    }
  );
  summary.arbitrageCandidatesCount = enrichedItems.length;
  summary.arbitrageOpportunitiesCount = arbitrageEngine.rankOpportunities(
    enrichedItems.map((item) => item.arbitrage),
    {
      includeRisky: false
    }
  ).length;

  return {
    currency: displayCurrency,
    pricingMode,
    fees: buildFeeSummary(),
    generatedAt: new Date().toISOString(),
    ttlMinutes: options.ttlMinutes || CACHE_TTL_MINUTES,
    opportunities: rankedOpportunities,
    items: enrichedItems,
    plan: {
      planTier: planContext?.planTier || "free",
      compareView,
      compareViewMaxItems,
      premiumCategoryAccess,
      comparedItemsSubmitted,
      comparedItemsRequested,
      comparedItemsProcessed,
      truncatedByPlan,
      blockedPremiumItems
    },
    summary: {
      ...summary,
      blockedPremiumItems,
      totalValueSelected: round2(summary.totalValueSelected),
      totalValueSteam: round2(summary.totalValueSteam),
      totalValueBestSellNet: round2(summary.totalValueBestSellNet),
      totalValueLowestBuy: round2(summary.totalValueLowestBuy)
    },
    diagnostics: {
      scannerPolicy: scannerBaselinePolicy ? buildScannerMarketPolicyDiagnostics() : null,
      liveFetch: {
        enabled: allowLiveFetch,
        forceRefresh,
        requestedBySource: liveFetchRequestedBySource,
        bySource: liveDiagnosticsBySource
      }
    }
  };
};

exports.PRICING_MODES = PRICING_MODES;
exports.DEFAULT_PRICING_MODE = DEFAULT_PRICING_MODE;
exports.__testables = {
  normalizePricingMode,
  normalizeItems,
  applyQuoteCoverageFallback,
  pickBestBuy,
  pickBestSellNet,
  buildRouteFreshnessContractFromCompareResult,
  selectByPricingMode,
  getModeUnitPrice,
  isFreshTimestamp,
  parseDmarketUsdMinorValue,
  maybeRepairLegacyDmarketGross,
  normalizeSourceState,
  resolveUnavailableContextForSource,
  getCachedRowUpdatedAt,
  fromCachedRow
};

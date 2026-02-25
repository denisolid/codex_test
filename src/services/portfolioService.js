const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const txRepo = require("../repositories/transactionRepository");
const { daysAgoStart, daysAgoEnd } = require("../utils/date");
const { marketPriceStaleHours } = require("../config/env");
const { derivePriceStatus } = require("../utils/priceStatus");
const { buildPortfolioAnalytics } = require("../utils/portfolioAnalytics");
const {
  buildDailyClosePriceMap,
  computeDailyVolatilityPercent,
  buildManagementClue
} = require("../utils/portfolioGuidance");
const { resolveCurrency, convertUsdAmount } = require("./currencyService");

function round2(n) {
  return Number((n || 0).toFixed(2));
}

exports.getPortfolio = async (userId, options = {}) => {
  const displayCurrency = resolveCurrency(options.currency);
  const txPnlState = await txRepo.getPnlStateBySkin(userId);
  const realizedProfit = Object.values(txPnlState).reduce(
    (sum, state) => sum + Number(state.realized || 0),
    0
  );
  const holdings = await inventoryRepo.getUserHoldings(userId);
  if (!holdings.length) {
    return {
      totalValue: 0,
      costBasis: 0,
      realizedProfit: convertUsdAmount(round2(realizedProfit), displayCurrency),
      unrealizedProfit: 0,
      roiPercent: null,
      oneDayChangePercent: null,
      sevenDayChangePercent: null,
      unpricedItemsCount: 0,
      staleItemsCount: 0,
      alerts: [],
      analytics: buildPortfolioAnalytics([], 0),
      managementSummary: {
        hold: 0,
        watch: 0,
        sell: 0
      },
      currency: displayCurrency,
      items: []
    };
  }

  const skinIds = holdings.map((h) => h.skin_id);
  const latestRows = await priceRepo.getLatestPriceRowsBySkinIds(skinIds);
  const prices7d = await priceRepo.getLatestPricesBeforeDate(skinIds, daysAgoStart(7));
  const prices1d = await priceRepo.getLatestPricesBeforeDate(skinIds, daysAgoStart(1));
  const recentHistoryRows = await priceRepo.getHistoryBySkinIdsSince(
    skinIds,
    daysAgoStart(45),
    14000
  );
  const dailyPricesBySkin = buildDailyClosePriceMap(recentHistoryRows);

  let totalValue = 0;
  let costBasis = 0;
  let value7d = 0;
  let value1d = 0;
  let unpricedCount = 0;
  let staleCount = 0;

  const items = holdings.map((h) => {
    const latestRow = latestRows[h.skin_id] || null;
    const currentPrice = Number(latestRow?.price || 0);
    const oldPrice = prices7d[h.skin_id] || currentPrice || 0;
    const oneDayPrice = prices1d[h.skin_id] || currentPrice || 0;
    const priceMeta = derivePriceStatus(latestRow);
    const volatilityDailyPercent = computeDailyVolatilityPercent(
      dailyPricesBySkin[h.skin_id] || []
    );

    const lineValue = h.quantity * currentPrice;
    const lineValue7d = h.quantity * oldPrice;
    const lineValue1d = h.quantity * oneDayPrice;

    totalValue += lineValue;
    value7d += lineValue7d;
    value1d += lineValue1d;

    if (priceMeta.status === "unpriced") {
      unpricedCount += 1;
    }
    if (priceMeta.status === "stale") {
      staleCount += 1;
    }

    if (h.purchase_price != null) {
      const state = txPnlState[h.skin_id];
      if (state && state.quantity > 0) {
        const avgCost = state.cost / state.quantity;
        costBasis += h.quantity * avgCost;
      } else {
        costBasis += h.quantity * Number(h.purchase_price);
      }
    } else {
      const state = txPnlState[h.skin_id];
      if (state && state.quantity > 0) {
        const avgCost = state.cost / state.quantity;
        costBasis += h.quantity * avgCost;
      }
    }

    return {
      skinId: h.skin_id,
      primarySteamItemId:
        Array.isArray(h.steam_item_ids) && h.steam_item_ids.length
          ? h.steam_item_ids[0]
          : null,
      steamItemIds: Array.isArray(h.steam_item_ids) ? h.steam_item_ids : [],
      marketHashName: h.skins.market_hash_name,
      quantity: h.quantity,
      purchasePrice: h.purchase_price,
      currentPrice,
      currentPriceSource: latestRow?.source || null,
      currentPriceRecordedAt: latestRow?.recorded_at || null,
      oneDayReferencePrice: round2(oneDayPrice),
      sevenDayReferencePrice: round2(oldPrice),
      oneDayChangePercent:
        oneDayPrice > 0 ? round2(((currentPrice - oneDayPrice) / oneDayPrice) * 100) : null,
      sevenDayChangePercent:
        oldPrice > 0 ? round2(((currentPrice - oldPrice) / oldPrice) * 100) : null,
      oneDayLinePnl: round2(lineValue - lineValue1d),
      sevenDayLinePnl: round2(lineValue - lineValue7d),
      priceStatus: priceMeta.status,
      priceConfidenceLabel: priceMeta.confidenceLabel,
      priceConfidenceScore: priceMeta.confidenceScore,
      volatilityDailyPercent,
      lineValue: round2(lineValue)
    };
  });

  const itemsWithGuidance = items.map((item) => {
    const concentrationWeightPercent =
      totalValue > 0 ? round2((item.lineValue / totalValue) * 100) : 0;
    const managementClue = buildManagementClue({
      currentPrice: item.currentPrice,
      oneDayChangePercent: item.oneDayChangePercent,
      sevenDayChangePercent: item.sevenDayChangePercent,
      volatilityDailyPercent: item.volatilityDailyPercent,
      concentrationWeightPercent,
      priceConfidenceScore: item.priceConfidenceScore,
      priceStatus: item.priceStatus
    });

    return {
      ...item,
      concentrationWeightPercent,
      managementClue
    };
  });

  const roiPercent =
    costBasis > 0 ? round2(((totalValue - costBasis) / costBasis) * 100) : null;
  const sevenDayChangePercent =
    value7d > 0 ? round2(((totalValue - value7d) / value7d) * 100) : null;
  const oneDayChangePercent =
    value1d > 0 ? round2(((totalValue - value1d) / value1d) * 100) : null;
  const analytics = buildPortfolioAnalytics(itemsWithGuidance, totalValue);
  const unrealizedProfit = totalValue - costBasis;
  const displayItems = itemsWithGuidance.map((item) => {
    const clue = item.managementClue || null;
    const prediction = clue?.prediction || null;

    return {
      ...item,
      purchasePrice:
        item.purchasePrice == null
          ? null
          : convertUsdAmount(Number(item.purchasePrice), displayCurrency),
      currentPrice: convertUsdAmount(item.currentPrice, displayCurrency),
      oneDayReferencePrice: convertUsdAmount(item.oneDayReferencePrice, displayCurrency),
      sevenDayReferencePrice: convertUsdAmount(
        item.sevenDayReferencePrice,
        displayCurrency
      ),
      oneDayLinePnl: convertUsdAmount(item.oneDayLinePnl, displayCurrency),
      sevenDayLinePnl: convertUsdAmount(item.sevenDayLinePnl, displayCurrency),
      lineValue: convertUsdAmount(item.lineValue, displayCurrency),
      managementClue: clue
        ? {
            ...clue,
            prediction: prediction
              ? {
                  ...prediction,
                  expectedPrice: convertUsdAmount(
                    Number(prediction.expectedPrice || 0),
                    displayCurrency
                  ),
                  rangeLow: convertUsdAmount(Number(prediction.rangeLow || 0), displayCurrency),
                  rangeHigh: convertUsdAmount(
                    Number(prediction.rangeHigh || 0),
                    displayCurrency
                  ),
                  currency: displayCurrency
                }
              : null
          }
        : null,
      currency: displayCurrency
    };
  });
  const managementSummary = displayItems.reduce(
    (acc, item) => {
      const action = String(item.managementClue?.action || "watch").toLowerCase();
      if (action === "hold" || action === "sell" || action === "watch") {
        acc[action] += 1;
      } else {
        acc.watch += 1;
      }
      return acc;
    },
    { hold: 0, watch: 0, sell: 0 }
  );

  const alerts = [];
  if (unpricedCount > 0) {
    alerts.push({
      severity: "warning",
      code: "UNPRICED_ITEMS",
      message: `${unpricedCount} item(s) are unpriced right now.`
    });
  }
  if (staleCount > 0) {
    alerts.push({
      severity: "warning",
      code: "STALE_PRICES",
      message: `${staleCount} item(s) use stale prices older than ${marketPriceStaleHours}h.`
    });
  }
  if (oneDayChangePercent != null && Math.abs(oneDayChangePercent) >= 10) {
    alerts.push({
      severity: "info",
      code: "ONE_DAY_MOVE",
      message: `Large 24h move detected: ${oneDayChangePercent > 0 ? "+" : ""}${oneDayChangePercent}%.`
    });
  }
  if (sevenDayChangePercent != null && Math.abs(sevenDayChangePercent) >= 20) {
    alerts.push({
      severity: "info",
      code: "SEVEN_DAY_MOVE",
      message: `Large 7d move detected: ${sevenDayChangePercent > 0 ? "+" : ""}${sevenDayChangePercent}%.`
    });
  }
  if (analytics.concentrationRisk === "high") {
    alerts.push({
      severity: "warning",
      code: "HIGH_CONCENTRATION",
      message: `High concentration risk: top position is ${analytics.concentrationTop1Percent}% of portfolio.`
    });
  }
  if (
    analytics.breadth.advancerRatioPercent != null &&
    analytics.breadth.advancerRatioPercent < 35
  ) {
    alerts.push({
      severity: "info",
      code: "WEAK_BREADTH",
      message: `Weak breadth: only ${analytics.breadth.advancerRatioPercent}% of movers are advancing.`
    });
  }

  return {
    totalValue: convertUsdAmount(round2(totalValue), displayCurrency),
    costBasis: convertUsdAmount(round2(costBasis), displayCurrency),
    realizedProfit: convertUsdAmount(round2(realizedProfit), displayCurrency),
    unrealizedProfit: convertUsdAmount(round2(unrealizedProfit), displayCurrency),
    roiPercent,
    oneDayChangePercent,
    sevenDayChangePercent,
    unpricedItemsCount: unpricedCount,
    staleItemsCount: staleCount,
    alerts,
    analytics,
    managementSummary,
    currency: displayCurrency,
    items: displayItems
  };
};

exports.getPortfolioHistory = async (userId, days = 7, options = {}) => {
  const displayCurrency = resolveCurrency(options.currency);
  const normalizedDays = Math.min(Math.max(Number(days) || 7, 1), 180);
  const holdings = await inventoryRepo.getUserHoldings(userId);
  if (!holdings.length) {
    return { currency: displayCurrency, points: [] };
  }

  const skinIds = holdings.map((h) => h.skin_id);
  const latestRows = await priceRepo.getLatestPriceRowsBySkinIds(skinIds);
  const latestPriceBySkin = {};
  for (const [skinId, row] of Object.entries(latestRows)) {
    latestPriceBySkin[Number(skinId)] = Number(row?.price || 0);
  }

  const points = [];

  for (let i = normalizedDays - 1; i >= 0; i -= 1) {
    const referenceDate = i === 0 ? new Date() : daysAgoEnd(i);
    const labelDate = daysAgoStart(i);
    const px = await priceRepo.getLatestPricesBeforeDate(skinIds, referenceDate);
    const total = holdings.reduce(
      (acc, h) => {
        const price = px[h.skin_id] ?? latestPriceBySkin[h.skin_id] ?? 0;
        return acc + h.quantity * price;
      },
      0
    );

    points.push({
      date: labelDate.toISOString().slice(0, 10),
      totalValue: convertUsdAmount(round2(total), displayCurrency)
    });
  }

  return { currency: displayCurrency, points };
};

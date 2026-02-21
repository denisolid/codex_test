const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const txRepo = require("../repositories/transactionRepository");
const { daysAgoStart } = require("../utils/date");
const { marketPriceStaleHours } = require("../config/env");
const { derivePriceStatus } = require("../utils/priceStatus");
const { buildPortfolioAnalytics } = require("../utils/portfolioAnalytics");

function round2(n) {
  return Number((n || 0).toFixed(2));
}

exports.getPortfolio = async (userId) => {
  const holdings = await inventoryRepo.getUserHoldings(userId);
  if (!holdings.length) {
    return {
      totalValue: 0,
      costBasis: 0,
      roiPercent: null,
      oneDayChangePercent: null,
      sevenDayChangePercent: null,
      unpricedItemsCount: 0,
      staleItemsCount: 0,
      alerts: [],
      analytics: buildPortfolioAnalytics([], 0),
      items: []
    };
  }

  const skinIds = holdings.map((h) => h.skin_id);
  const latestRows = await priceRepo.getLatestPriceRowsBySkinIds(skinIds);
  const prices7d = await priceRepo.getLatestPricesBeforeDate(skinIds, daysAgoStart(7));
  const prices1d = await priceRepo.getLatestPricesBeforeDate(skinIds, daysAgoStart(1));
  const txCostState = await txRepo.getPositionCostBasisBySkin(userId);

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
      const state = txCostState[h.skin_id];
      if (state && state.quantity > 0) {
        const avgCost = state.cost / state.quantity;
        costBasis += h.quantity * avgCost;
      } else {
        costBasis += h.quantity * Number(h.purchase_price);
      }
    } else {
      const state = txCostState[h.skin_id];
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
      lineValue: round2(lineValue)
    };
  });

  const roiPercent =
    costBasis > 0 ? round2(((totalValue - costBasis) / costBasis) * 100) : null;
  const sevenDayChangePercent =
    value7d > 0 ? round2(((totalValue - value7d) / value7d) * 100) : null;
  const oneDayChangePercent =
    value1d > 0 ? round2(((totalValue - value1d) / value1d) * 100) : null;
  const analytics = buildPortfolioAnalytics(items, totalValue);

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
    totalValue: round2(totalValue),
    costBasis: round2(costBasis),
    roiPercent,
    oneDayChangePercent,
    sevenDayChangePercent,
    unpricedItemsCount: unpricedCount,
    staleItemsCount: staleCount,
    alerts,
    analytics,
    items
  };
};

exports.getPortfolioHistory = async (userId, days = 7) => {
  const holdings = await inventoryRepo.getUserHoldings(userId);
  if (!holdings.length) {
    return { points: [] };
  }

  const skinIds = holdings.map((h) => h.skin_id);
  const points = [];

  for (let i = days - 1; i >= 0; i -= 1) {
    const dt = daysAgoStart(i);
    const px = await priceRepo.getLatestPricesBeforeDate(skinIds, dt);
    const total = holdings.reduce(
      (acc, h) => acc + h.quantity * (px[h.skin_id] || 0),
      0
    );
    points.push({
      date: dt.toISOString().slice(0, 10),
      totalValue: round2(total)
    });
  }

  return { points };
};

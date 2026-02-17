const inventoryRepo = require("../repositories/inventoryRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const txRepo = require("../repositories/transactionRepository");
const { daysAgoStart } = require("../utils/date");

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
      sevenDayChangePercent: null,
      items: []
    };
  }

  const skinIds = holdings.map((h) => h.skin_id);
  const latestPrices = await priceRepo.getLatestPricesBySkinIds(skinIds);
  const prices7d = await priceRepo.getLatestPricesBeforeDate(skinIds, daysAgoStart(7));
  const txCostState = await txRepo.getPositionCostBasisBySkin(userId);

  let totalValue = 0;
  let costBasis = 0;
  let value7d = 0;

  const items = holdings.map((h) => {
    const currentPrice = latestPrices[h.skin_id] || 0;
    const oldPrice = prices7d[h.skin_id] || currentPrice || 0;

    const lineValue = h.quantity * currentPrice;
    const lineValue7d = h.quantity * oldPrice;

    totalValue += lineValue;
    value7d += lineValue7d;

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
      lineValue: round2(lineValue)
    };
  });

  const roiPercent =
    costBasis > 0 ? round2(((totalValue - costBasis) / costBasis) * 100) : null;
  const sevenDayChangePercent =
    value7d > 0 ? round2(((totalValue - value7d) / value7d) * 100) : null;

  return {
    totalValue: round2(totalValue),
    costBasis: round2(costBasis),
    roiPercent,
    sevenDayChangePercent,
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

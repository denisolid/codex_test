const AppError = require("../utils/AppError");
const { resolveCurrency } = require("./currencyService");

function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

function validateInput(payload) {
  const buyPrice = Number(payload.buyPrice);
  const sellPrice = Number(payload.sellPrice);
  const quantity = payload.quantity == null ? 1 : Number(payload.quantity);
  const commissionPercent =
    payload.commissionPercent == null ? 13 : Number(payload.commissionPercent);
  const currency = resolveCurrency(payload.currency);

  if (!Number.isFinite(buyPrice) || buyPrice < 0) {
    throw new AppError("buyPrice must be a number >= 0", 400);
  }
  if (!Number.isFinite(sellPrice) || sellPrice < 0) {
    throw new AppError("sellPrice must be a number >= 0", 400);
  }
  if (!Number.isFinite(quantity) || quantity <= 0) {
    throw new AppError("quantity must be a number > 0", 400);
  }
  if (
    !Number.isFinite(commissionPercent) ||
    commissionPercent < 0 ||
    commissionPercent >= 100
  ) {
    throw new AppError("commissionPercent must be in range [0, 100)", 400);
  }

  return { buyPrice, sellPrice, quantity, commissionPercent, currency };
}

exports.calculateTrade = (payload) => {
  const { buyPrice, sellPrice, quantity, commissionPercent, currency } =
    validateInput(payload);

  const grossBuy = buyPrice * quantity;
  const grossSell = sellPrice * quantity;
  const commissionAmount = grossSell * (commissionPercent / 100);
  const netSell = grossSell - commissionAmount;
  const netProfit = netSell - grossBuy;
  const roiPercent = grossBuy > 0 ? (netProfit / grossBuy) * 100 : null;
  const breakEvenSellPrice = buyPrice / (1 - commissionPercent / 100);

  return {
    buyPrice: round2(buyPrice),
    sellPrice: round2(sellPrice),
    quantity: round2(quantity),
    commissionPercent: round2(commissionPercent),
    grossBuy: round2(grossBuy),
    grossSell: round2(grossSell),
    commissionAmount: round2(commissionAmount),
    netSell: round2(netSell),
    netProfit: round2(netProfit),
    roiPercent: roiPercent == null ? null : round2(roiPercent),
    breakEvenSellPrice: round2(breakEvenSellPrice),
    currency
  };
};

exports.__testables = {
  validateInput
};

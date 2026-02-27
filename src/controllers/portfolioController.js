const asyncHandler = require("../utils/asyncHandler");
const portfolioService = require("../services/portfolioService");
const planService = require("../services/planService");
const { toCsv } = require("../utils/csv");

exports.getPortfolio = asyncHandler(async (req, res) => {
  const { planTier, entitlements } = await planService.getUserPlanProfile(req.userId);
  const data = await portfolioService.getPortfolio(req.userId, {
    currency: req.query.currency,
    planTier,
    entitlements
  });
  res.json(data);
});

exports.getPortfolioHistory = asyncHandler(async (req, res) => {
  const { planTier, entitlements } = await planService.getUserPlanProfile(req.userId);
  const days = Number(req.query.days || 7);
  const data = await portfolioService.getPortfolioHistory(req.userId, days, {
    currency: req.query.currency,
    planTier,
    maxHistoryDays: entitlements.maxHistoryDays
  });
  res.json(data);
});

exports.getPortfolioBacktest = asyncHandler(async (req, res) => {
  const { planTier, entitlements } = await planService.getUserPlanProfile(req.userId);
  const days = Number(req.query.days || 90);
  const data = await portfolioService.getPortfolioBacktest(req.userId, {
    days,
    currency: req.query.currency,
    planTier,
    entitlements
  });
  res.json(data);
});

exports.exportPortfolioCsv = asyncHandler(async (req, res) => {
  const { planTier, entitlements } = await planService.requireFeature(
    req.userId,
    "csvExport",
    {
      message: "CSV export is available on Pro plan and above."
    }
  );

  const portfolio = await portfolioService.getPortfolio(req.userId, {
    currency: req.query.currency,
    planTier,
    entitlements
  });

  const headers = [
    "skinId",
    "marketHashName",
    "quantity",
    "currentPrice",
    "lineValue",
    "oneDayChangePercent",
    "sevenDayChangePercent",
    "priceStatus",
    "priceConfidenceLabel",
    "currency"
  ];

  const rows = (portfolio.items || []).slice(0, entitlements.maxCsvRows).map((item) => ({
    skinId: item.skinId,
    marketHashName: item.marketHashName,
    quantity: item.quantity,
    currentPrice: item.currentPrice,
    lineValue: item.lineValue,
    oneDayChangePercent: item.oneDayChangePercent,
    sevenDayChangePercent: item.sevenDayChangePercent,
    priceStatus: item.priceStatus,
    priceConfidenceLabel: item.priceConfidenceLabel,
    currency: portfolio.currency
  }));

  const csv = toCsv(headers, rows);
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", "attachment; filename=\"portfolio-export.csv\"");
  res.status(200).send(csv);
});

const asyncHandler = require("../utils/asyncHandler");
const portfolioService = require("../services/portfolioService");

exports.getPortfolio = asyncHandler(async (req, res) => {
  const data = await portfolioService.getPortfolio(req.userId, {
    currency: req.query.currency
  });
  res.json(data);
});

exports.getPortfolioHistory = asyncHandler(async (req, res) => {
  const days = Number(req.query.days || 7);
  const data = await portfolioService.getPortfolioHistory(req.userId, days, {
    currency: req.query.currency
  });
  res.json(data);
});

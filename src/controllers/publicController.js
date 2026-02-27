const asyncHandler = require("../utils/asyncHandler");
const publicPortfolioService = require("../services/publicPortfolioService");

exports.getPublicPortfolio = asyncHandler(async (req, res) => {
  const data = await publicPortfolioService.getBySteamId64(req.params.steamId64, {
    currency: req.query.currency,
    historyDays: req.query.historyDays,
    referrer: req.query.ref
  });
  res.json(data);
});

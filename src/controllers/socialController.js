const asyncHandler = require("../utils/asyncHandler");
const socialService = require("../services/socialService");

exports.listWatchlist = asyncHandler(async (req, res) => {
  const data = await socialService.listWatchlist(req.userId, {
    currency: req.query.currency
  });
  res.json(data);
});

exports.addWatchlist = asyncHandler(async (req, res) => {
  const result = await socialService.addToWatchlist(
    req.userId,
    req.body?.steamId64
  );
  res.status(201).json(result);
});

exports.removeWatchlist = asyncHandler(async (req, res) => {
  const removed = await socialService.removeFromWatchlist(
    req.userId,
    req.params.steamId64
  );

  if (!removed) {
    res.status(404).json({
      error: "Watchlist entry not found"
    });
    return;
  }

  res.status(204).send();
});

exports.getLeaderboard = asyncHandler(async (req, res) => {
  const data = await socialService.getLeaderboard(req.userId, {
    scope: req.query.scope,
    limit: req.query.limit,
    currency: req.query.currency
  });
  res.json(data);
});

exports.updatePublicSettings = asyncHandler(async (req, res) => {
  const data = await socialService.updatePublicSettings(req.userId, req.body || {});
  res.json(data);
});

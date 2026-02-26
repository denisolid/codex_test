const asyncHandler = require("../utils/asyncHandler");
const authService = require("../services/authService");

exports.connectSteam = asyncHandler(async (req, res) => {
  const { steamId64 } = req.body;
  const result = await authService.linkSteamToUser(req.userId, steamId64);
  res.json({
    message: result.mergedFromUserId
      ? "Steam account linked and duplicate Steam-only profile merged."
      : "Steam account linked.",
    mergedFromUserId: result.mergedFromUserId,
    user: result.user
  });
});

const asyncHandler = require("../utils/asyncHandler");
const authService = require("../services/authService");
const userRepo = require("../repositories/userRepository");
const AppError = require("../utils/AppError");

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

exports.disconnectSteam = asyncHandler(async (req, res) => {
  const result = await authService.unlinkSteamFromUser(req.userId);
  res.json({
    message: result.disconnected
      ? "Steam account disconnected."
      : "Steam account is already disconnected.",
    user: result.user
  });
});

exports.updateProfile = asyncHandler(async (req, res) => {
  const displayNameRaw = req.body?.displayName;
  const displayName = String(displayNameRaw == null ? "" : displayNameRaw).trim();

  if (displayName.length > 80) {
    throw new AppError("displayName must be <= 80 characters", 400, "VALIDATION_ERROR");
  }

  const updated = await userRepo.updateSteamProfileById(req.userId, {
    displayName: displayName || null
  });

  res.json({
    profile: {
      displayName: updated?.display_name || null,
      avatarUrl: updated?.avatar_url || null,
      steamId64: updated?.steam_id64 || null
    }
  });
});

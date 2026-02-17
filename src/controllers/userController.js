const asyncHandler = require("../utils/asyncHandler");
const userRepo = require("../repositories/userRepository");

exports.connectSteam = asyncHandler(async (req, res) => {
  const { steamId64 } = req.body;
  const user = await userRepo.updateSteamId(req.userId, steamId64);
  res.json({ message: "Steam ID connected", user });
});

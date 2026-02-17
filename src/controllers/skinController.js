const asyncHandler = require("../utils/asyncHandler");
const skinService = require("../services/skinService");

exports.getSkinById = asyncHandler(async (req, res) => {
  const skin = await skinService.getSkinDetails(Number(req.params.id));
  res.json(skin);
});

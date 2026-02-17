const asyncHandler = require("../utils/asyncHandler");
const inventorySyncService = require("../services/inventorySyncService");

exports.syncInventory = asyncHandler(async (req, res) => {
  const result = await inventorySyncService.syncUserInventory(req.userId);
  res.status(200).json(result);
});

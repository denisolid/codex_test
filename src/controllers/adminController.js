const asyncHandler = require("../utils/asyncHandler");
const priceRepo = require("../repositories/priceHistoryRepository");

exports.cleanupMockPrices = asyncHandler(async (_req, res) => {
  const deletedCount = await priceRepo.deleteMockPriceRows();
  res.json({
    ok: true,
    deletedCount
  });
});

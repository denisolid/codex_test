const asyncHandler = require("../utils/asyncHandler");
const teamDashboardService = require("../services/teamDashboardService");

exports.getDashboard = asyncHandler(async (req, res) => {
  const data = await teamDashboardService.getDashboard(req.userId, {
    currency: req.query.currency
  });
  res.json(data);
});

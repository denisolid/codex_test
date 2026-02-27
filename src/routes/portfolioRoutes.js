const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/portfolioController");

router.get("/", auth, ctrl.getPortfolio);
router.get("/history", auth, ctrl.getPortfolioHistory);
router.get("/backtest", auth, ctrl.getPortfolioBacktest);
router.get("/export.csv", auth, ctrl.exportPortfolioCsv);

module.exports = router;

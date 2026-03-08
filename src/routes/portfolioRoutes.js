const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const ctrl = require("../controllers/portfolioController");

router.get("/", auth, onboardingGate, ctrl.getPortfolio);
router.get("/history", auth, onboardingGate, ctrl.getPortfolioHistory);
router.get("/backtest", auth, onboardingGate, ctrl.getPortfolioBacktest);
router.get("/export.csv", auth, onboardingGate, ctrl.exportPortfolioCsv);

module.exports = router;

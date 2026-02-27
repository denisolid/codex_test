const router = require("express").Router();
const adminAuth = require("../middleware/adminAuth");
const ctrl = require("../controllers/adminController");

router.post("/prices/cleanup-mock", adminAuth, ctrl.cleanupMockPrices);
router.get("/metrics/steam-link-rate", adminAuth, ctrl.getSteamLinkRate);
router.get("/metrics/growth", adminAuth, ctrl.getGrowthMetrics);

module.exports = router;

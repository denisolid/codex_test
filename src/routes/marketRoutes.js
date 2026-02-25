const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/marketController");

router.use(auth);
router.get("/inventory/value", ctrl.getInventoryValue);
router.get("/items/:skinId/sell-suggestion", ctrl.getQuickSellSuggestion);
router.get("/items/:skinId/liquidity", ctrl.getLiquidityScore);

module.exports = router;

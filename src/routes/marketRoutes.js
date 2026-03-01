const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/marketController");
const {
  requirePositiveIntParam,
  parseOptionalNumericQuery
} = require("../middleware/requestValidation");

router.use(auth);
router.get(
  "/inventory/value",
  parseOptionalNumericQuery("commissionPercent", {
    storeAs: "commissionPercent",
    message: "commissionPercent must be numeric"
  }),
  ctrl.getInventoryValue
);
router.get(
  "/items/:skinId/sell-suggestion",
  requirePositiveIntParam("skinId", { message: "Invalid item id" }),
  parseOptionalNumericQuery("commissionPercent", {
    storeAs: "commissionPercent",
    message: "commissionPercent must be numeric"
  }),
  ctrl.getQuickSellSuggestion
);
router.get(
  "/items/:skinId/liquidity",
  requirePositiveIntParam("skinId", { message: "Invalid item id" }),
  ctrl.getLiquidityScore
);

module.exports = router;

const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const adminAuth = require("../middleware/adminAuth");
const ctrl = require("../controllers/marketController");
const {
  requirePositiveIntParam,
  parseOptionalNumericQuery
} = require("../middleware/requestValidation");

router.use(auth, onboardingGate);
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
router.get(
  "/opportunities",
  parseOptionalNumericQuery("minProfit", {
    storeAs: "minProfit",
    message: "minProfit must be numeric"
  }),
  parseOptionalNumericQuery("minSpread", {
    storeAs: "minSpread",
    message: "minSpread must be numeric"
  }),
  parseOptionalNumericQuery("minScore", {
    storeAs: "minScore",
    message: "minScore must be numeric"
  }),
  parseOptionalNumericQuery("liquidityMin", {
    storeAs: "liquidityMin",
    message: "liquidityMin must be numeric"
  }),
  parseOptionalNumericQuery("limit", {
    storeAs: "limit",
    message: "limit must be numeric"
  }),
  ctrl.getArbitrageOpportunities
);
router.get("/preferences", ctrl.getPricePreferences);
router.patch("/preferences", ctrl.updatePricePreferences);
router.post("/compare", ctrl.compareItems);
router.post("/refresh", adminAuth, ctrl.refreshComparisonCache);

module.exports = router;

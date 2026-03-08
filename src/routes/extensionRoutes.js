const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const extensionApiKeyAuth = require("../middleware/extensionApiKeyAuth");
const ctrl = require("../controllers/extensionController");
const {
  requirePositiveIntParam
} = require("../middleware/requestValidation");

router.get("/keys", auth, onboardingGate, ctrl.listApiKeys);
router.post("/keys", auth, onboardingGate, ctrl.createApiKey);
router.delete(
  "/keys/:id",
  auth,
  onboardingGate,
  requirePositiveIntParam("id", { storeAs: "keyId", message: "Invalid API key id" }),
  ctrl.revokeApiKey
);

router.get("/inventory/value", extensionApiKeyAuth, ctrl.getInventoryValue);
router.get(
  "/items/:skinId/sell-suggestion",
  extensionApiKeyAuth,
  requirePositiveIntParam("skinId", { message: "Invalid item id" }),
  ctrl.getQuickSellSuggestion
);
router.post("/trade/calculate", extensionApiKeyAuth, ctrl.calculateTrade);

module.exports = router;

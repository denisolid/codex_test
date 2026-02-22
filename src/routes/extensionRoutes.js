const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const extensionApiKeyAuth = require("../middleware/extensionApiKeyAuth");
const ctrl = require("../controllers/extensionController");

router.get("/keys", auth, ctrl.listApiKeys);
router.post("/keys", auth, ctrl.createApiKey);
router.delete("/keys/:id", auth, ctrl.revokeApiKey);

router.get("/inventory/value", extensionApiKeyAuth, ctrl.getInventoryValue);
router.get(
  "/items/:skinId/sell-suggestion",
  extensionApiKeyAuth,
  ctrl.getQuickSellSuggestion
);
router.post("/trade/calculate", extensionApiKeyAuth, ctrl.calculateTrade);

module.exports = router;

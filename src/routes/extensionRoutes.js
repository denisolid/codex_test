const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const extensionApiKeyAuth = require("../middleware/extensionApiKeyAuth");
const ctrl = require("../controllers/extensionController");
const {
  requirePositiveIntParam
} = require("../middleware/requestValidation");

router.get("/keys", auth, ctrl.listApiKeys);
router.post("/keys", auth, ctrl.createApiKey);
router.delete(
  "/keys/:id",
  auth,
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

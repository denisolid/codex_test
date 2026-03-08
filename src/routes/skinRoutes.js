const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const ctrl = require("../controllers/skinController");
const {
  requirePositiveIntParam,
  requireDigitsParam
} = require("../middleware/requestValidation");

router.get(
  "/by-steam-item/:steamItemId",
  auth,
  onboardingGate,
  requireDigitsParam("steamItemId", { message: "Invalid steam item id" }),
  ctrl.getSkinBySteamItemId
);
router.get(
  "/:id",
  auth,
  onboardingGate,
  requirePositiveIntParam("id", { storeAs: "skinId", message: "Invalid item id" }),
  ctrl.getSkinById
);

module.exports = router;

const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/skinController");
const {
  requirePositiveIntParam,
  requireDigitsParam
} = require("../middleware/requestValidation");

router.get(
  "/by-steam-item/:steamItemId",
  auth,
  requireDigitsParam("steamItemId", { message: "Invalid steam item id" }),
  ctrl.getSkinBySteamItemId
);
router.get(
  "/:id",
  auth,
  requirePositiveIntParam("id", { storeAs: "skinId", message: "Invalid item id" }),
  ctrl.getSkinById
);

module.exports = router;

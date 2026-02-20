const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/skinController");

router.get("/by-steam-item/:steamItemId", auth, ctrl.getSkinBySteamItemId);
router.get("/:id", auth, ctrl.getSkinById);

module.exports = router;

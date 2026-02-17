const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/inventoryController");

router.post("/sync", auth, ctrl.syncInventory);

module.exports = router;

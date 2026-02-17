const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/skinController");

router.get("/:id", auth, ctrl.getSkinById);

module.exports = router;

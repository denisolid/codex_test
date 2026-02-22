const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/tradeController");

router.post("/calculate", auth, ctrl.calculate);

module.exports = router;

const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/userController");

router.patch("/me/steam", auth, ctrl.connectSteam);

module.exports = router;

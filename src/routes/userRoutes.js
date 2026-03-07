const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/userController");

router.patch("/me/profile", auth, ctrl.updateProfile);
router.patch("/me/steam", auth, ctrl.connectSteam);
router.delete("/me/steam", auth, ctrl.disconnectSteam);

module.exports = router;

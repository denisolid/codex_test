const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const ctrl = require("../controllers/userController");

router.patch("/me/profile", auth, onboardingGate, ctrl.updateProfile);
router.patch("/me/steam", auth, onboardingGate, ctrl.connectSteam);
router.delete("/me/steam", auth, onboardingGate, ctrl.disconnectSteam);

module.exports = router;

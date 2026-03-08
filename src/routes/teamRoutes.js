const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const ctrl = require("../controllers/teamController");

router.use(auth, onboardingGate);
router.get("/dashboard", ctrl.getDashboard);

module.exports = router;

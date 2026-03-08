const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const ctrl = require("../controllers/tradeController");

router.post("/calculate", auth, onboardingGate, ctrl.calculate);

module.exports = router;

const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/monetizationController");

router.get("/pricing", ctrl.getPricing);

router.use(auth);
router.get("/me", ctrl.getMyPlan);
router.patch("/plan", ctrl.updateMyPlan);

module.exports = router;

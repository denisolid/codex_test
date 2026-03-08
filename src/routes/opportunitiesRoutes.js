const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const adminAuth = require("../middleware/adminAuth");
const ctrl = require("../controllers/opportunitiesController");
const { parseOptionalNumericQuery } = require("../middleware/requestValidation");

router.post("/refresh/admin", adminAuth, ctrl.refreshFeedAdmin);

router.use(auth, onboardingGate);

router.get(
  "/top",
  parseOptionalNumericQuery("limit", {
    storeAs: "limit",
    message: "limit must be numeric"
  }),
  ctrl.getTopOpportunities
);

router.get(
  "/feed",
  parseOptionalNumericQuery("limit", {
    storeAs: "limit",
    message: "limit must be numeric"
  }),
  ctrl.getFeed
);

router.post("/refresh", ctrl.refreshFeed);

router.get("/status", ctrl.getScannerStatus);

module.exports = router;

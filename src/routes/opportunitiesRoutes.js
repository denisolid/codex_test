const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/opportunitiesController");
const { parseOptionalNumericQuery } = require("../middleware/requestValidation");

router.use(auth);

router.get(
  "/top",
  parseOptionalNumericQuery("limit", {
    storeAs: "limit",
    message: "limit must be numeric"
  }),
  ctrl.getTopOpportunities
);

module.exports = router;

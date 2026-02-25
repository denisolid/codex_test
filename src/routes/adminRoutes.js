const router = require("express").Router();
const adminAuth = require("../middleware/adminAuth");
const ctrl = require("../controllers/adminController");

router.post("/prices/cleanup-mock", adminAuth, ctrl.cleanupMockPrices);

module.exports = router;

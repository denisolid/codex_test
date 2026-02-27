const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/teamController");

router.use(auth);
router.get("/dashboard", ctrl.getDashboard);

module.exports = router;

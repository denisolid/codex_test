const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const adminAuth = require("../middleware/adminAuth");
const ctrl = require("../controllers/alertController");

router.post("/check", adminAuth, ctrl.checkNow);

router.use(auth);
router.get("/", ctrl.list);
router.post("/", ctrl.create);
router.get("/events", ctrl.listEvents);
router.patch("/:id", ctrl.update);
router.delete("/:id", ctrl.remove);

module.exports = router;

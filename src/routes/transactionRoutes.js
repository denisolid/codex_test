const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const onboardingGate = require("../middleware/onboardingGate");
const ctrl = require("../controllers/transactionController");

router.use(auth, onboardingGate);

router.get("/", ctrl.list);
router.get("/export.csv", ctrl.exportCsv);
router.post("/", ctrl.create);
router.get("/:id", ctrl.getById);
router.patch("/:id", ctrl.update);
router.delete("/:id", ctrl.remove);

module.exports = router;

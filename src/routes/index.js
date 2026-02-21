const router = require("express").Router();

router.use("/auth", require("./authRoutes"));
router.use("/admin", require("./adminRoutes"));
router.use("/users", require("./userRoutes"));
router.use("/portfolio", require("./portfolioRoutes"));
router.use("/inventory", require("./inventoryRoutes"));
router.use("/skins", require("./skinRoutes"));
router.use("/transactions", require("./transactionRoutes"));

module.exports = router;

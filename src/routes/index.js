const router = require("express").Router();

router.use("/auth", require("./authRoutes"));
router.use("/admin", require("./adminRoutes"));
router.use("/users", require("./userRoutes"));
router.use("/portfolio", require("./portfolioRoutes"));
router.use("/inventory", require("./inventoryRoutes"));
router.use("/skins", require("./skinRoutes"));
router.use("/transactions", require("./transactionRoutes"));
router.use("/market", require("./marketRoutes"));
router.use("/trade", require("./tradeRoutes"));
router.use("/alerts", require("./alertRoutes"));
router.use("/extension", require("./extensionRoutes"));
router.use("/social", require("./socialRoutes"));
router.use("/public", require("./publicRoutes"));
router.use("/monetization", require("./monetizationRoutes"));
router.use("/team", require("./teamRoutes"));

module.exports = router;

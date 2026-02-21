const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/inventoryController");
const { syncRateLimitWindowMs, syncRateLimitMax } = require("../config/env");
const { createRateLimiter } = require("../middleware/rateLimit");

const syncRateLimiter = createRateLimiter({
  windowMs: syncRateLimitWindowMs,
  max: syncRateLimitMax,
  message: "Too many sync attempts, wait before trying again."
});

router.post("/sync", syncRateLimiter, auth, ctrl.syncInventory);

module.exports = router;

const router = require("express").Router();
const ctrl = require("../controllers/authController");
const { authRateLimitWindowMs, authRateLimitMax } = require("../config/env");
const { createRateLimiter } = require("../middleware/rateLimit");

const authRateLimiter = createRateLimiter({
  windowMs: authRateLimitWindowMs,
  max: authRateLimitMax,
  message: "Too many auth requests, try again shortly."
});

router.post("/register", authRateLimiter, ctrl.register);
router.post("/login", authRateLimiter, ctrl.login);
router.post("/session", authRateLimiter, ctrl.createSession);
router.get("/steam/start", authRateLimiter, ctrl.steamStart);
router.get("/steam/callback", authRateLimiter, ctrl.steamCallback);
router.post("/resend-confirmation", authRateLimiter, ctrl.resendConfirmation);
router.post("/logout", authRateLimiter, ctrl.logout);
router.get("/me", authRateLimiter, ctrl.me);

module.exports = router;

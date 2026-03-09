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
router.get("/steam/link/start", authRateLimiter, ctrl.steamLinkStart);
router.get("/steam/link/callback", authRateLimiter, ctrl.steamLinkCallback);
router.post("/resend-confirmation", authRateLimiter, ctrl.resendConfirmation);
router.post("/onboarding/email/start", authRateLimiter, ctrl.startSteamEmailOnboarding);
router.post("/onboarding/email/resend", authRateLimiter, ctrl.resendSteamEmailOnboarding);
router.get("/onboarding/verify", authRateLimiter, ctrl.verifySteamEmailOnboarding);
router.post("/logout", authRateLimiter, ctrl.logout);
router.post("/logout-all", authRateLimiter, ctrl.logoutAll);
router.delete("/me", authRateLimiter, ctrl.deleteAccount);
router.get("/bootstrap", authRateLimiter, ctrl.bootstrap);
router.get("/me", authRateLimiter, ctrl.me);

module.exports = router;

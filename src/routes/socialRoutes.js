const router = require("express").Router();
const auth = require("../middleware/authMiddleware");
const ctrl = require("../controllers/socialController");

router.use(auth);

router.get("/watchlist", ctrl.listWatchlist);
router.post("/watchlist", ctrl.addWatchlist);
router.delete("/watchlist/:steamId64", ctrl.removeWatchlist);
router.get("/leaderboard", ctrl.getLeaderboard);
router.patch("/settings", ctrl.updatePublicSettings);

module.exports = router;

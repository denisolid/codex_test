const router = require("express").Router();
const ctrl = require("../controllers/publicController");

router.get("/u/:steamId64", ctrl.getPublicPortfolio);

module.exports = router;

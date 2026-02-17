const {
  priceUpdaterIntervalMinutes,
  priceUpdaterRateLimitPerSecond
} = require("../config/env");
const priceUpdaterService = require("../services/priceUpdaterService");

let isRunning = false;

async function runCycle() {
  if (isRunning) {
    console.log("[price-updater] Skip cycle: previous cycle still running");
    return;
  }

  isRunning = true;
  const startedAt = new Date();
  console.log(`[price-updater] Cycle started at ${startedAt.toISOString()}`);

  try {
    const result = await priceUpdaterService.updateAllSkinPrices({
      rateLimitPerSecond: priceUpdaterRateLimitPerSecond
    });
    console.log("[price-updater] Cycle finished", result);
  } catch (err) {
    console.error("[price-updater] Cycle failed", err.message);
  } finally {
    isRunning = false;
  }
}

const intervalMs = Math.max(priceUpdaterIntervalMinutes, 1) * 60 * 1000;

runCycle();
setInterval(runCycle, intervalMs);

console.log(
  `[price-updater] Worker running every ${Math.max(
    priceUpdaterIntervalMinutes,
    1
  )} minute(s)`
);

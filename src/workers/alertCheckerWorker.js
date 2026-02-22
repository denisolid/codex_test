const {
  alertCheckIntervalMinutes,
  alertCheckBatchSize
} = require("../config/env");
const alertService = require("../services/alertService");

let isRunning = false;

async function runCycle() {
  if (isRunning) {
    console.log("[alert-checker] Skip cycle: previous cycle still running");
    return;
  }

  isRunning = true;
  const startedAt = new Date();
  console.log(`[alert-checker] Cycle started at ${startedAt.toISOString()}`);

  try {
    const result = await alertService.checkAlertsNow({
      limit: alertCheckBatchSize
    });
    console.log("[alert-checker] Cycle finished", result);
  } catch (err) {
    console.error("[alert-checker] Cycle failed", err.message);
  } finally {
    isRunning = false;
  }
}

const intervalMs = Math.max(alertCheckIntervalMinutes, 1) * 60 * 1000;

runCycle();
setInterval(runCycle, intervalMs);

console.log(
  `[alert-checker] Worker running every ${Math.max(
    alertCheckIntervalMinutes,
    1
  )} minute(s)`
);

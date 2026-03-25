const app = require("./app");
const env = require("./config/env");
const arbitrageScannerService = require("./services/arbitrageScannerService");
const feedRevalidationService = require("./services/feed/feedRevalidationService");

arbitrageScannerService.startScheduler();
if (env.globalFeedV2Enabled) {
  feedRevalidationService.startScheduler();
}

const server = app.listen(env.port, () => {
  console.log(`API running on port ${env.port}`);
});

let shuttingDown = false;

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`Received ${signal}. Closing HTTP server...`);

  feedRevalidationService.stopScheduler();
  arbitrageScannerService.stopScheduler();

  server.close(() => {
    console.log("HTTP server closed.");
    process.exit(0);
  });

  setTimeout(() => {
    console.error("Force shutdown after timeout.");
    process.exit(1);
  }, 10000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

import { inject } from "@vercel/analytics";
import { injectSpeedInsights } from "@vercel/speed-insights";

let observabilityInitialized = false;

export function initObservability() {
  if (observabilityInitialized) return;
  observabilityInitialized = true;

  inject({
    mode: import.meta.env.DEV ? "development" : "production",
  });
  injectSpeedInsights();
}

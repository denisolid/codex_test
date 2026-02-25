const { marketPriceStaleHours } = require("../config/env");

function derivePriceStatus(row) {
  if (!row || row.price == null) {
    return {
      status: "unpriced",
      confidenceLabel: "low",
      confidenceScore: 0
    };
  }

  const source = String(row.source || "").toLowerCase();
  const recordedTs = new Date(row.recorded_at || 0).getTime();
  const staleCutoffMs = Math.max(marketPriceStaleHours, 1) * 60 * 60 * 1000;
  const stale =
    !Number.isFinite(recordedTs) || Date.now() - recordedTs > staleCutoffMs;

  if (stale) {
    return {
      status: "stale",
      confidenceLabel: "medium",
      confidenceScore: 0.45
    };
  }

  if (source.includes("mock")) {
    return {
      status: "mock",
      confidenceLabel: "low",
      confidenceScore: 0.2
    };
  }

  if (source.startsWith("cache:")) {
    return {
      status: "cached",
      confidenceLabel: "high",
      confidenceScore: 0.75
    };
  }

  if (source.includes("steam")) {
    return {
      status: "real",
      confidenceLabel: "high",
      confidenceScore: 0.95
    };
  }

  return {
    status: "unknown",
    confidenceLabel: "medium",
    confidenceScore: 0.5
  };
}

module.exports = {
  derivePriceStatus
};

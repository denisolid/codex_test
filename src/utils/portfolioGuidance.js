function round2(n) {
  return Number((Number(n || 0)).toFixed(2));
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stdDev(values) {
  if (values.length <= 1) return 0;
  const mean = average(values);
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function buildDailyClosePriceMap(historyRows) {
  const dailyBySkin = {};

  for (const row of historyRows || []) {
    const skinId = Number(row.skin_id);
    const price = toNumber(row.price, 0);
    const day = String(row.recorded_at || "").slice(0, 10);

    if (!skinId || !day || price < 0) {
      continue;
    }

    if (!dailyBySkin[skinId]) {
      dailyBySkin[skinId] = new Map();
    }

    // Query is sorted descending by recorded_at. The first row we see for each
    // day is the latest close snapshot for that day.
    if (!dailyBySkin[skinId].has(day)) {
      dailyBySkin[skinId].set(day, price);
    }
  }

  const out = {};
  for (const [skinId, dayMap] of Object.entries(dailyBySkin)) {
    out[Number(skinId)] = [...dayMap.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map((entry) => entry[1]);
  }
  return out;
}

function computeDailyVolatilityPercent(dailyPrices) {
  const prices = (dailyPrices || [])
    .map((value) => toNumber(value, NaN))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (prices.length < 3) {
    return 0;
  }

  const returns = [];
  for (let i = 1; i < prices.length; i += 1) {
    const prev = prices[i - 1];
    const cur = prices[i];
    if (prev > 0 && Number.isFinite(cur)) {
      returns.push(((cur - prev) / prev) * 100);
    }
  }

  if (!returns.length) {
    return 0;
  }

  return round2(Math.max(stdDev(returns), 0));
}

function pickAction(holdScore, sellScore, status) {
  const weakStatus = ["unpriced", "stale", "mock"].includes(status);
  if (weakStatus) return "watch";

  const delta = holdScore - sellScore;
  if (delta >= 10) return "hold";
  if (delta <= -10) return "sell";
  return "watch";
}

function buildManagementClue(input = {}) {
  const currentPrice = Math.max(toNumber(input.currentPrice, 0), 0);
  const oneDayChangePercent = toNumber(input.oneDayChangePercent, 0);
  const sevenDayChangePercent = toNumber(input.sevenDayChangePercent, 0);
  const volatilityDailyPercent = Math.max(
    toNumber(input.volatilityDailyPercent, 0),
    0
  );
  const concentrationWeightPercent = Math.max(
    toNumber(input.concentrationWeightPercent, 0),
    0
  );
  const priceConfidenceScore = clamp(
    toNumber(input.priceConfidenceScore, 0.5),
    0,
    1
  );
  const priceStatus = String(input.priceStatus || "unknown").toLowerCase();

  const momentumPercent = round2(sevenDayChangePercent * 0.65 + oneDayChangePercent * 0.35);
  const volatilityDampener = clamp(1 - volatilityDailyPercent / 18, 0.45, 1);
  const expectedMovePercent = round2(
    clamp(momentumPercent * volatilityDampener, -35, 35)
  );
  const uncertaintyPercent = round2(clamp(3 + volatilityDailyPercent * 1.35, 2.5, 30));
  const expectedPrice = round2(currentPrice * (1 + expectedMovePercent / 100));
  const rangeLow = round2(Math.max(expectedPrice * (1 - uncertaintyPercent / 100), 0));
  const rangeHigh = round2(Math.max(expectedPrice * (1 + uncertaintyPercent / 100), 0));

  let holdScore = 50;
  let sellScore = 50;

  const holdReasons = [];
  const sellReasons = [];
  const watchReasons = [];

  if (momentumPercent >= 6) {
    holdScore += 14;
    holdReasons.push(`Positive momentum (${momentumPercent}%) across 1D/7D.`);
  } else if (momentumPercent <= -6) {
    sellScore += 14;
    sellReasons.push(`Negative momentum (${momentumPercent}%) across 1D/7D.`);
  }

  if (expectedMovePercent >= 4) {
    holdScore += 18;
    holdReasons.push(`Projection shows ~${expectedMovePercent}% upside over 7 days.`);
  } else if (expectedMovePercent <= -4) {
    sellScore += 18;
    sellReasons.push(`Projection shows ~${expectedMovePercent}% downside over 7 days.`);
  }

  if (volatilityDailyPercent >= 8) {
    sellScore += 12;
    holdScore -= 6;
    sellReasons.push(
      `High daily volatility (${volatilityDailyPercent}%) increases drawdown risk.`
    );
  } else if (volatilityDailyPercent <= 3.5) {
    holdScore += 8;
    holdReasons.push(`Low daily volatility (${volatilityDailyPercent}%) supports holding.`);
  }

  if (concentrationWeightPercent >= 35 && volatilityDailyPercent >= 7) {
    sellScore += 10;
    sellReasons.push(
      `Large concentration (${round2(
        concentrationWeightPercent
      )}% of portfolio) with elevated volatility.`
    );
  } else if (concentrationWeightPercent >= 35) {
    watchReasons.push(
      `Large concentration (${round2(concentrationWeightPercent)}% of portfolio).`
    );
  }

  if (priceConfidenceScore < 0.55) {
    holdScore -= 8;
    sellScore -= 4;
    watchReasons.push(`Price confidence is low (${Math.round(priceConfidenceScore * 100)}%).`);
  }

  if (["unpriced", "stale", "mock"].includes(priceStatus)) {
    holdScore -= 10;
    sellScore -= 8;
    watchReasons.push(`Price status is ${priceStatus}; guidance is less reliable.`);
  }

  const action = pickAction(holdScore, sellScore, priceStatus);
  const delta = holdScore - sellScore;
  const confidenceBase =
    50 +
    Math.min(Math.abs(delta) * 1.8, 28) +
    priceConfidenceScore * 14 -
    Math.min(volatilityDailyPercent * 1.2, 16);
  let confidence = clamp(Math.round(confidenceBase), 30, 93);
  if (action === "watch") {
    confidence = Math.max(35, Math.min(confidence, 72));
  }
  if (["unpriced", "stale", "mock"].includes(priceStatus)) {
    confidence = Math.min(confidence, 55);
  }

  const reasonsPool =
    action === "hold"
      ? [...holdReasons, ...watchReasons]
      : action === "sell"
        ? [...sellReasons, ...watchReasons]
        : [...watchReasons, ...sellReasons, ...holdReasons];
  const reasons = reasonsPool.filter(Boolean).slice(0, 3);
  if (!reasons.length) {
    reasons.push("Signals are mixed; monitor trend before changing position.");
  }

  return {
    action,
    confidence,
    scores: {
      hold: round2(holdScore),
      sell: round2(sellScore)
    },
    metrics: {
      momentumPercent,
      volatilityDailyPercent: round2(volatilityDailyPercent),
      concentrationWeightPercent: round2(concentrationWeightPercent)
    },
    prediction: {
      horizonDays: 7,
      expectedMovePercent,
      expectedPrice,
      rangeLow,
      rangeHigh,
      uncertaintyPercent
    },
    reasons
  };
}

module.exports = {
  buildDailyClosePriceMap,
  computeDailyVolatilityPercent,
  buildManagementClue
};

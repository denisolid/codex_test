const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildDailyClosePriceMap,
  computeDailyVolatilityPercent,
  buildManagementClue
} = require("../src/utils/portfolioGuidance");

test("daily close map keeps one close per day and sorts ascending", () => {
  const rows = [
    { skin_id: 101, price: 11.5, recorded_at: "2026-02-22T20:00:00.000Z" },
    { skin_id: 101, price: 11.2, recorded_at: "2026-02-22T10:00:00.000Z" },
    { skin_id: 101, price: 10.1, recorded_at: "2026-02-21T23:00:00.000Z" },
    { skin_id: 101, price: 9.9, recorded_at: "2026-02-20T23:00:00.000Z" },
    { skin_id: 202, price: 5.1, recorded_at: "2026-02-22T23:00:00.000Z" }
  ];

  const bySkin = buildDailyClosePriceMap(rows);
  assert.deepEqual(bySkin[101], [9.9, 10.1, 11.5]);
  assert.deepEqual(bySkin[202], [5.1]);
});

test("volatility computes standard deviation of daily returns", () => {
  const lowVol = computeDailyVolatilityPercent([10, 10.1, 10.05, 10.2, 10.12]);
  const highVol = computeDailyVolatilityPercent([10, 12, 9, 13, 8]);

  assert.ok(lowVol < highVol);
  assert.ok(highVol > 10);
});

test("management clue recommends hold for stable uptrend", () => {
  const clue = buildManagementClue({
    currentPrice: 120,
    oneDayChangePercent: 4.5,
    sevenDayChangePercent: 12,
    volatilityDailyPercent: 2.8,
    concentrationWeightPercent: 14,
    priceConfidenceScore: 0.88,
    priceStatus: "real"
  });

  assert.equal(clue.action, "hold");
  assert.ok(clue.confidence >= 55);
  assert.ok(clue.prediction.expectedMovePercent > 0);
});

test("management clue recommends sell for high-volatility downtrend", () => {
  const clue = buildManagementClue({
    currentPrice: 120,
    oneDayChangePercent: -6,
    sevenDayChangePercent: -18,
    volatilityDailyPercent: 11.4,
    concentrationWeightPercent: 44,
    priceConfidenceScore: 0.82,
    priceStatus: "real"
  });

  assert.equal(clue.action, "sell");
  assert.ok(clue.prediction.expectedMovePercent < 0);
  assert.ok(clue.reasons.some((reason) => reason.toLowerCase().includes("volatility")));
});

test("management clue falls back to watch when pricing status is stale", () => {
  const clue = buildManagementClue({
    currentPrice: 10,
    oneDayChangePercent: 3,
    sevenDayChangePercent: 9,
    volatilityDailyPercent: 1.5,
    concentrationWeightPercent: 8,
    priceConfidenceScore: 0.9,
    priceStatus: "stale"
  });

  assert.equal(clue.action, "watch");
  assert.ok(clue.confidence <= 55);
});

const test = require("node:test");
const assert = require("node:assert/strict");

const { buildDailyCarryForwardSeries } = require("../src/utils/historySeries");

test("buildDailyCarryForwardSeries creates a full daily range using carry-forward", () => {
  const rows = [
    { price: 15, recorded_at: "2026-01-05T18:00:00.000Z", source: "steam" },
    { price: 13, recorded_at: "2026-01-03T18:00:00.000Z", source: "steam" },
    { price: 12, recorded_at: "2026-01-02T18:00:00.000Z", source: "steam" }
  ];

  const series = buildDailyCarryForwardSeries(rows, {
    startDate: "2026-01-01T00:00:00.000Z",
    endDate: "2026-01-05T23:59:59.000Z"
  });

  assert.deepEqual(
    series.map((row) => String(row.recorded_at).slice(0, 10)),
    ["2026-01-05", "2026-01-04", "2026-01-03", "2026-01-02", "2026-01-01"]
  );
  assert.deepEqual(series.map((row) => row.price), [15, 13, 13, 12, 12]);
});

test("buildDailyCarryForwardSeries uses seed row when there is no baseline point in range", () => {
  const rows = [{ price: 13, recorded_at: "2026-01-03T18:00:00.000Z", source: "steam" }];

  const series = buildDailyCarryForwardSeries(rows, {
    startDate: "2026-01-01T00:00:00.000Z",
    endDate: "2026-01-03T23:59:59.000Z",
    seedRow: {
      price: 10,
      source: "baseline",
      recorded_at: "2025-12-31T23:00:00.000Z"
    }
  });

  assert.deepEqual(series.map((row) => row.price), [13, 10, 10]);
});

test("buildDailyCarryForwardSeries returns empty when there is no row and no seed", () => {
  const series = buildDailyCarryForwardSeries([], {
    startDate: "2026-01-01T00:00:00.000Z",
    endDate: "2026-01-05T23:59:59.000Z",
    backfillFromFirstObserved: false
  });

  assert.deepEqual(series, []);
});

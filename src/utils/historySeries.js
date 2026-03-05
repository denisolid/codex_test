function toDayKey(value) {
  return String(value || "").slice(0, 10);
}

function toUtcDayStart(value) {
  const date =
    value instanceof Date
      ? new Date(value.getTime())
      : new Date(String(value || ""));
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())
  );
}

function addUtcDays(day, delta) {
  const next = new Date(day.getTime());
  next.setUTCDate(next.getUTCDate() + delta);
  return next;
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      ...row,
      price: Number(row?.price)
    }))
    .filter((row) => Number.isFinite(row.price) && row.price >= 0 && toDayKey(row.recorded_at))
    .sort(
      (a, b) => new Date(b.recorded_at).getTime() - new Date(a.recorded_at).getTime()
    );
}

function buildDailyCarryForwardSeries(rows = [], options = {}) {
  const startDay = toUtcDayStart(options.startDate);
  const endDay = toUtcDayStart(options.endDate || new Date());
  if (!startDay || !endDay || startDay.getTime() > endDay.getTime()) {
    return [];
  }

  const normalizedRows = normalizeRows(rows);
  const latestRowByDay = new Map();
  for (const row of normalizedRows) {
    const dayKey = toDayKey(row.recorded_at);
    if (!latestRowByDay.has(dayKey)) {
      latestRowByDay.set(dayKey, row);
    }
  }

  let carryRow = null;
  const seedRow = options.seedRow || null;
  if (seedRow && Number.isFinite(Number(seedRow.price))) {
    carryRow = {
      ...seedRow,
      price: Number(seedRow.price)
    };
  } else if (latestRowByDay.size && options.backfillFromFirstObserved !== false) {
    const oldestDay = [...latestRowByDay.keys()].sort((a, b) => a.localeCompare(b))[0];
    carryRow = latestRowByDay.get(oldestDay);
  }

  const out = [];
  for (
    let cursor = new Date(startDay.getTime());
    cursor.getTime() <= endDay.getTime();
    cursor = addUtcDays(cursor, 1)
  ) {
    const dayKey = cursor.toISOString().slice(0, 10);
    const dayRow = latestRowByDay.get(dayKey);
    if (dayRow) {
      carryRow = dayRow;
    }

    if (!carryRow) {
      continue;
    }

    out.push({
      ...carryRow,
      recorded_at: dayRow ? dayRow.recorded_at : `${dayKey}T23:59:59.000Z`,
      price: Number(carryRow.price)
    });
  }

  if (options.descending !== false) {
    out.reverse();
  }

  return out;
}

module.exports = {
  buildDailyCarryForwardSeries
};

function round2(n) {
  return Number((n || 0).toFixed(2));
}

function safePercent(part, whole) {
  if (!Number.isFinite(part) || !Number.isFinite(whole) || whole <= 0) {
    return null;
  }
  return round2((part / whole) * 100);
}

function classifyConcentrationRisk(top1Percent, hhi) {
  if ((top1Percent || 0) >= 45 || (hhi || 0) >= 0.28) return "high";
  if ((top1Percent || 0) >= 30 || (hhi || 0) >= 0.18) return "medium";
  return "low";
}

function pickLeader(items, comparator) {
  const valid = items.filter((x) => Number.isFinite(x.sevenDayChangePercent));
  if (!valid.length) return null;

  const picked = valid.reduce((best, cur) => {
    if (!best) return cur;
    return comparator(cur.sevenDayChangePercent, best.sevenDayChangePercent)
      ? cur
      : best;
  }, null);

  if (!picked) return null;

  return {
    marketHashName: picked.marketHashName,
    sevenDayChangePercent: round2(picked.sevenDayChangePercent),
    lineValue: round2(picked.lineValue)
  };
}

function buildPortfolioAnalytics(items, totalValue) {
  const normalizedTotal = Number(totalValue || 0);
  if (!Array.isArray(items) || !items.length || normalizedTotal <= 0) {
    return {
      holdingsCount: 0,
      concentrationTop1Percent: null,
      concentrationTop3Percent: null,
      concentrationHhi: null,
      effectiveHoldings: null,
      concentrationRisk: "low",
      weightedAverageMove7dPercent: null,
      breadth: {
        advancers: 0,
        decliners: 0,
        unchanged: 0,
        advancerRatioPercent: null
      },
      leaders: {
        topGainer: null,
        topLoser: null
      }
    };
  }

  const sortedByValue = [...items].sort(
    (a, b) => Number(b.lineValue || 0) - Number(a.lineValue || 0)
  );
  const top1Value = Number(sortedByValue[0]?.lineValue || 0);
  const top3Value = sortedByValue
    .slice(0, 3)
    .reduce((acc, item) => acc + Number(item.lineValue || 0), 0);

  let hhi = 0;
  let weightedAbsMove7d = 0;
  let advancers = 0;
  let decliners = 0;
  let unchanged = 0;

  for (const item of items) {
    const lineValue = Number(item.lineValue || 0);
    const weight = lineValue / normalizedTotal;
    hhi += weight * weight;

    const change = Number(item.sevenDayChangePercent);
    if (Number.isFinite(change)) {
      weightedAbsMove7d += Math.abs(change) * weight;

      if (change > 0.5) {
        advancers += 1;
      } else if (change < -0.5) {
        decliners += 1;
      } else {
        unchanged += 1;
      }
    } else {
      unchanged += 1;
    }
  }

  const movers = advancers + decliners;
  const top1Percent = safePercent(top1Value, normalizedTotal);
  const top3Percent = safePercent(top3Value, normalizedTotal);

  return {
    holdingsCount: items.length,
    concentrationTop1Percent: top1Percent,
    concentrationTop3Percent: top3Percent,
    concentrationHhi: round2(hhi),
    effectiveHoldings: hhi > 0 ? round2(1 / hhi) : null,
    concentrationRisk: classifyConcentrationRisk(top1Percent, hhi),
    weightedAverageMove7dPercent: round2(weightedAbsMove7d),
    breadth: {
      advancers,
      decliners,
      unchanged,
      advancerRatioPercent: movers > 0 ? round2((advancers / movers) * 100) : null
    },
    leaders: {
      topGainer: pickLeader(items, (cur, best) => cur > best),
      topLoser: pickLeader(items, (cur, best) => cur < best)
    }
  };
}

module.exports = {
  buildPortfolioAnalytics
};

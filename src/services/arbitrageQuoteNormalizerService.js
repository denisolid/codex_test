const { roundPrice, sourceFeePercent } = require("../markets/marketUtils");

const KNOWN_MARKETS = new Set(["steam", "skinport", "csfloat", "dmarket"]);

function normalizeMarket(source) {
  const raw = String(source || "")
    .trim()
    .toLowerCase();
  if (!raw) return "";
  if (raw === "dm") return "dmarket";
  if (raw === "cs float") return "csfloat";
  return raw;
}

function toFiniteOrNull(value) {
  if (value == null) return null;
  if (typeof value === "string" && !value.trim()) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value);
  return parsed != null && parsed > 0 ? parsed : null;
}

function toNonNegativeOrNull(value) {
  const parsed = toFiniteOrNull(value);
  return parsed != null && parsed >= 0 ? parsed : null;
}

function readPath(obj, path) {
  let current = obj;
  for (const token of path) {
    if (current == null || typeof current !== "object") {
      return null;
    }
    current = current[token];
  }
  return current;
}

function pickFirstPositive(candidates = []) {
  for (const candidate of candidates) {
    const parsed = toPositiveOrNull(candidate);
    if (parsed != null) return roundPrice(parsed);
  }
  return null;
}

function pickFirstNonNegative(candidates = []) {
  for (const candidate of candidates) {
    const parsed = toNonNegativeOrNull(candidate);
    if (parsed != null) return parsed;
  }
  return null;
}

function scaleToSevenDayVolume(value, days) {
  const volume = toNonNegativeOrNull(value);
  const periodDays = toPositiveOrNull(days);
  if (volume == null || periodDays == null) return null;
  return Math.max((volume / periodDays) * 7, 0);
}

function resolveRawVolume7d(raw = {}) {
  if (!raw || typeof raw !== "object") return null;

  const direct = pickFirstNonNegative([
    raw?.volume7d,
    raw?.volume_7d,
    raw?.sales7d,
    raw?.sales_7d,
    raw?.liquiditySales,
    readPath(raw, ["last_7_days", "volume"]),
    readPath(raw, ["last_7_days", "sales"]),
    readPath(raw, ["last7days", "volume"]),
    readPath(raw, ["last7days", "sales"])
  ]);
  if (direct != null) return direct;

  const from24h = scaleToSevenDayVolume(
    pickFirstNonNegative([
      readPath(raw, ["last_24_hours", "volume"]),
      readPath(raw, ["last_24_hours", "sales"]),
      readPath(raw, ["last24h", "volume"]),
      readPath(raw, ["last24h", "sales"])
    ]),
    1
  );
  if (from24h != null) return from24h;

  const from30d = scaleToSevenDayVolume(
    pickFirstNonNegative([
      readPath(raw, ["last_30_days", "volume"]),
      readPath(raw, ["last_30_days", "sales"]),
      readPath(raw, ["last30days", "volume"]),
      readPath(raw, ["last30days", "sales"])
    ]),
    30
  );
  if (from30d != null) return from30d;

  const from90d = scaleToSevenDayVolume(
    pickFirstNonNegative([
      readPath(raw, ["last_90_days", "volume"]),
      readPath(raw, ["last_90_days", "sales"]),
      readPath(raw, ["last90days", "volume"]),
      readPath(raw, ["last90days", "sales"])
    ]),
    90
  );
  if (from90d != null) return from90d;

  return null;
}

function resolveVolume7d(item = {}) {
  return pickFirstNonNegative([
    item?.volume7d,
    item?.volume_7d,
    item?.sales7d,
    item?.liquiditySales,
    item?.salesCount,
    item?.sales,
    item?.volume24h,
    item?.marketVolume24h,
    item?.marketVolume7d,
    item?.marketInsight?.sellSuggestion?.volume24h
  ]);
}

function resolveLiquidityScore(item = {}) {
  return pickFirstNonNegative([
    item?.liquidityScore,
    item?.marketComparison?.liquidityScore,
    item?.managementClue?.metrics?.liquidityScore
  ]);
}

function resolveSevenDayChangePercent(item = {}) {
  for (const candidate of [
    item?.sevenDayChangePercent,
    item?.seven_day_change_percent,
    item?.change7dPercent,
    item?.priceChange7dPercent,
    item?.marketInsight?.sellSuggestion?.change7dPercent
  ]) {
    const parsed = toFiniteOrNull(candidate);
    if (parsed != null) return parsed;
  }
  return null;
}

function extractOrderbookTopPrice(entry) {
  if (entry == null) return null;
  if (typeof entry === "number" || typeof entry === "string") {
    return toPositiveOrNull(entry);
  }
  if (typeof entry !== "object") return null;
  return toPositiveOrNull(
    entry.price ??
      entry.unitPrice ??
      entry.amount ??
      entry.value ??
      entry[0]
  );
}

function normalizeOrderbook(orderbookInput) {
  const orderbook = orderbookInput && typeof orderbookInput === "object" ? orderbookInput : null;
  if (!orderbook) return null;

  const bids = Array.isArray(orderbook.bids) ? orderbook.bids : [];
  const asks = Array.isArray(orderbook.asks) ? orderbook.asks : [];
  const buyTop1 = toPositiveOrNull(
    orderbook.buy_top1 ?? orderbook.buyTop1 ?? extractOrderbookTopPrice(bids[0])
  );
  const buyTop2 = toPositiveOrNull(
    orderbook.buy_top2 ?? orderbook.buyTop2 ?? extractOrderbookTopPrice(bids[1])
  );
  const sellTop1 = toPositiveOrNull(
    orderbook.sell_top1 ?? orderbook.sellTop1 ?? extractOrderbookTopPrice(asks[0])
  );
  const sellTop2 = toPositiveOrNull(
    orderbook.sell_top2 ?? orderbook.sellTop2 ?? extractOrderbookTopPrice(asks[1])
  );

  if (buyTop1 == null && buyTop2 == null && sellTop1 == null && sellTop2 == null) {
    return null;
  }

  return {
    buy_top1: buyTop1 != null ? roundPrice(buyTop1) : null,
    buy_top2: buyTop2 != null ? roundPrice(buyTop2) : null,
    sell_top1: sellTop1 != null ? roundPrice(sellTop1) : null,
    sell_top2: sellTop2 != null ? roundPrice(sellTop2) : null
  };
}

function pickOrderbookFromRow(row = {}) {
  const raw = row?.raw && typeof row.raw === "object" ? row.raw : null;
  const candidates = [
    row?.orderbook,
    row?.depth,
    row?.book,
    raw?.orderbook,
    raw?.depth,
    raw?.book,
    raw?.marketDepth
  ];
  for (const candidate of candidates) {
    const normalized = normalizeOrderbook(candidate);
    if (normalized) return normalized;
  }
  return null;
}

function normalizeQuoteFromMarketRow(row = {}, item = {}) {
  const market = normalizeMarket(row?.source || row?.market);
  if (!KNOWN_MARKETS.has(market)) return null;

  const feePercent = pickFirstNonNegative([row?.feePercent, sourceFeePercent(market)]);
  const bestBuy = pickFirstPositive([
    row?.grossPrice,
    row?.bestBuy,
    row?.buyPrice,
    row?.lowestBuyPrice,
    readPath(row, ["raw", "best_buy"]),
    readPath(row, ["raw", "bestBuy"])
  ]);
  const bestSell = pickFirstPositive([
    row?.bestSell,
    row?.bestSellPrice,
    row?.sellPrice,
    row?.grossSellPrice,
    row?.grossPrice,
    readPath(row, ["raw", "best_sell"]),
    readPath(row, ["raw", "bestSell"])
  ]);
  const bestSellNetInline = pickFirstPositive([
    row?.netPriceAfterFees,
    row?.bestSellNet,
    row?.sellNet,
    row?.netPrice,
    readPath(row, ["raw", "best_sell_net"]),
    readPath(row, ["raw", "bestSellNet"])
  ]);
  const bestSellNet =
    bestSellNetInline != null
      ? roundPrice(bestSellNetInline)
      : bestSell != null && feePercent != null
        ? roundPrice(bestSell * (1 - Number(feePercent) / 100))
        : null;

  const volume7d = pickFirstNonNegative([
    row?.volume7d,
    row?.volume_7d,
    row?.sales7d,
    row?.sales,
    row?.liquiditySales,
    readPath(row, ["raw", "volume7d"]),
    readPath(row, ["raw", "volume_7d"]),
    readPath(row, ["raw", "sales7d"]),
    readPath(row, ["raw", "liquiditySales"]),
    resolveRawVolume7d(row?.raw),
    resolveVolume7d(item)
  ]);
  const liquidityScore = pickFirstNonNegative([
    row?.liquidityScore,
    row?.liquidity_score,
    readPath(row, ["raw", "liquidityScore"]),
    readPath(row, ["raw", "liquidity_score"]),
    resolveLiquidityScore(item)
  ]);
  const orderbook = pickOrderbookFromRow(row);

  return {
    market,
    best_buy: bestBuy,
    best_sell: bestSell,
    best_sell_net: bestSellNet,
    fee_percent: feePercent != null ? Number(feePercent) : null,
    volume_7d: volume7d,
    liquidity_score: liquidityScore,
    orderbook,
    url: row?.url ? String(row.url) : null
  };
}

function normalizeMarketQuotes(item = {}) {
  const perMarket = Array.isArray(item?.perMarket) ? item.perMarket : [];
  const byMarket = {};

  for (const row of perMarket) {
    const quote = normalizeQuoteFromMarketRow(row, item);
    if (!quote) continue;
    byMarket[quote.market] = quote;
  }

  return {
    quotes: Object.values(byMarket),
    byMarket
  };
}

module.exports = {
  normalizeMarket,
  normalizeMarketQuotes,
  resolveVolume7d,
  resolveLiquidityScore,
  resolveSevenDayChangePercent,
  __testables: {
    toFiniteOrNull,
    toPositiveOrNull,
    normalizeOrderbook,
    normalizeQuoteFromMarketRow
  }
};

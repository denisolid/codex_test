const test = require("node:test");
const assert = require("node:assert/strict");

const {
  normalizeMarketQuotes,
  resolveVolume7d,
  resolveLiquidityScore
} = require("../src/services/arbitrageQuoteNormalizerService");

test("normalizeMarketQuotes maps per-market rows into normalized quote model", () => {
  const item = {
    liquiditySales: 180,
    perMarket: [
      {
        source: "steam",
        grossPrice: 10.25,
        netPriceAfterFees: 8.92,
        feePercent: 13,
        url: "https://steam"
      },
      {
        source: "skinport",
        grossPrice: 9.8,
        netPriceAfterFees: 8.62,
        feePercent: 12,
        url: "https://skinport"
      }
    ]
  };

  const normalized = normalizeMarketQuotes(item);
  assert.equal(Array.isArray(normalized.quotes), true);
  assert.equal(normalized.quotes.length, 2);

  const steam = normalized.byMarket.steam;
  assert.equal(steam.market, "steam");
  assert.equal(steam.best_buy, 10.25);
  assert.equal(steam.best_sell_net, 8.92);
  assert.equal(steam.fee_percent, 13);
  assert.equal(steam.volume_7d, 180);
  assert.equal(steam.orderbook, null);
});

test("normalizeMarketQuotes extracts orderbook depth from raw payload when present", () => {
  const item = {
    perMarket: [
      {
        source: "dmarket",
        grossPrice: 1.2,
        netPriceAfterFees: 1.11,
        raw: {
          orderbook: {
            bids: [{ price: 1.2 }, { price: 1.14 }],
            asks: [{ price: 1.29 }, { price: 1.25 }]
          }
        }
      }
    ]
  };

  const normalized = normalizeMarketQuotes(item);
  const dmarket = normalized.byMarket.dmarket;
  assert.equal(dmarket.orderbook.buy_top1, 1.2);
  assert.equal(dmarket.orderbook.buy_top2, 1.14);
  assert.equal(dmarket.orderbook.sell_top1, 1.29);
  assert.equal(dmarket.orderbook.sell_top2, 1.25);
});

test("normalizeMarketQuotes derives 7d volume from raw market history payloads", () => {
  const item = {
    perMarket: [
      {
        source: "skinport",
        grossPrice: 3.1,
        netPriceAfterFees: 2.73,
        raw: {
          last_7_days: {
            volume: 142
          }
        }
      }
    ]
  };

  const normalized = normalizeMarketQuotes(item);
  const skinport = normalized.byMarket.skinport;
  assert.equal(skinport.volume_7d, 142);
});

test("volume and liquidity resolvers degrade gracefully", () => {
  assert.equal(resolveVolume7d({ liquiditySales: 75 }), 75);
  assert.equal(resolveVolume7d({}), null);
  assert.equal(resolveLiquidityScore({ liquidityScore: 42 }), 42);
  assert.equal(resolveLiquidityScore({}), null);
});

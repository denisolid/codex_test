const {
  marketPriceSource,
  steamMarketCurrency,
  steamMarketTimeoutMs
} = require("../config/env");
const mockPriceProvider = require("./mockPriceProviderService");
const steamMarketPriceService = require("./steamMarketPriceService");

async function fromSteam(marketHashName) {
  const price = await steamMarketPriceService.getLatestPrice(marketHashName, {
    currency: steamMarketCurrency,
    timeoutMs: steamMarketTimeoutMs
  });
  return { price, source: "steam-market" };
}

async function fromMock(marketHashName) {
  const price = await mockPriceProvider.getLatestPrice(marketHashName);
  return { price, source: "mock-price" };
}

exports.getPrice = async (marketHashName) => {
  if (marketPriceSource === "mock") {
    return fromMock(marketHashName);
  }

  if (marketPriceSource === "steam") {
    return fromSteam(marketHashName);
  }

  try {
    return await fromSteam(marketHashName);
  } catch (_err) {
    return fromMock(marketHashName);
  }
};

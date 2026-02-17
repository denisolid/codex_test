const skinRepo = require("../repositories/skinRepository");
const priceRepo = require("../repositories/priceHistoryRepository");
const priceProviderService = require("./priceProviderService");
const mockPriceProviderService = require("./mockPriceProviderService");

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function withRetries(fn, retries = 3, baseDelayMs = 300) {
  let lastErr;
  for (let i = 0; i < retries; i += 1) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < retries - 1) {
        await sleep(baseDelayMs * (i + 1));
      }
    }
  }
  throw lastErr;
}

exports.updateAllSkinPrices = async (options = {}) => {
  const rateLimitPerSecond = Number(options.rateLimitPerSecond || 5);
  const pauseMs = Math.max(Math.floor(1000 / Math.max(rateLimitPerSecond, 1)), 1);
  const now = new Date().toISOString();

  const skins = await skinRepo.listAll();
  if (!skins.length) {
    return {
      ok: true,
      updatedCount: 0,
      skippedCount: 0,
      failedCount: 0
    };
  }

  let updatedCount = 0;
  let failedCount = 0;

  for (const skin of skins) {
    try {
      let priced;
      try {
        priced = await withRetries(
          () => priceProviderService.getPrice(skin.market_hash_name),
          3,
          250
        );
      } catch (_err) {
        const fallbackPrice = await withRetries(
          () => mockPriceProviderService.getLatestPrice(skin.market_hash_name),
          3,
          250
        );
        priced = { price: fallbackPrice, source: "mock-price-fallback" };
      }

      await withRetries(
        () =>
          priceRepo.insertPriceRows([
            {
              skin_id: skin.id,
              price: priced.price,
              currency: "USD",
              source: `scheduled-${priced.source}`,
              recorded_at: now
            }
          ]),
        3,
        300
      );

      updatedCount += 1;
    } catch (_err) {
      failedCount += 1;
    }

    await sleep(pauseMs);
  }

  return {
    ok: true,
    updatedCount,
    skippedCount: 0,
    failedCount
  };
};

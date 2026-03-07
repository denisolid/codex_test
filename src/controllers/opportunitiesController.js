const asyncHandler = require("../utils/asyncHandler");
const scannerService = require("../services/arbitrageScannerService");

exports.getTopOpportunities = asyncHandler(async (req, res) => {
  const data = await scannerService.getTopOpportunities({
    limit: req.validated?.limit,
    showRisky: req.query?.showRisky
  });

  res.json({
    opportunities: (Array.isArray(data?.opportunities) ? data.opportunities : []).map((row) => ({
      itemName: row?.itemName || "Tracked Item",
      buyMarket: row?.buyMarket || null,
      buyPrice: row?.buyPrice ?? null,
      sellMarket: row?.sellMarket || null,
      sellNet: row?.sellNet ?? null,
      profit: row?.profit ?? null,
      spread: row?.spread ?? null,
      score: row?.score ?? null,
      scoreCategory: row?.scoreCategory || null,
      executionConfidence: row?.executionConfidence || null,
      liquidity: row?.liquidity ?? null,
      liquidityBand: row?.liquidityBand || null,
      volume7d: row?.volume7d ?? null,
      marketCoverage: row?.marketCoverage ?? null,
      referencePrice: row?.referencePrice ?? null,
      flags: Array.isArray(row?.flags) ? row.flags : [],
      badges: Array.isArray(row?.badges) ? row.badges : [],
      itemId: row?.itemId || null,
      buyUrl: row?.buyUrl || null,
      sellUrl: row?.sellUrl || null
    })),
    generatedAt: data?.generatedAt || null,
    ttlSeconds: data?.ttlSeconds || null,
    summary: data?.summary || null,
    currency: data?.currency || "USD"
  });
});

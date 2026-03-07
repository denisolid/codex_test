const asyncHandler = require("../utils/asyncHandler")
const scannerService = require("../services/arbitrageScannerService")

function toOpportunityRow(row = {}) {
  return {
    feedId: row?.feedId || null,
    detectedAt: row?.detectedAt || null,
    scanRunId: row?.scanRunId || null,
    isActive: row?.isActive == null ? true : Boolean(row.isActive),
    isDuplicate: Boolean(row?.isDuplicate),
    itemName: row?.itemName || "Tracked Item",
    itemCategory: row?.itemCategory || "weapon_skin",
    itemSubcategory: row?.itemSubcategory || null,
    itemImageUrl: row?.itemImageUrl || null,
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
  }
}

function toFeedResponse(data = {}) {
  return {
    opportunities: (Array.isArray(data?.opportunities) ? data.opportunities : []).map((row) =>
      toOpportunityRow(row)
    ),
    generatedAt: data?.generatedAt || null,
    ttlSeconds: data?.ttlSeconds || null,
    summary: data?.summary || null,
    status: data?.status || null,
    currency: data?.currency || "USD"
  }
}

exports.getTopOpportunities = asyncHandler(async (req, res) => {
  const data = await scannerService.getTopOpportunities({
    limit: req.validated?.limit,
    showRisky: req.query?.showRisky,
    forceRefresh: req.query?.force,
    category: req.query?.category,
    includeOlder: req.query?.includeOlder || req.query?.showOlder
  })

  res.json(toFeedResponse(data))
})

exports.getFeed = asyncHandler(async (req, res) => {
  const data = await scannerService.getFeed({
    limit: req.validated?.limit,
    showRisky: req.query?.showRisky,
    category: req.query?.category,
    includeOlder: req.query?.includeOlder || req.query?.showOlder
  })

  res.json(toFeedResponse(data))
})

exports.refreshFeed = asyncHandler(async (req, res) => {
  const result = await scannerService.triggerRefresh({
    trigger: req.body?.trigger || "manual"
  })

  res.status(202).json(result)
})

exports.getScannerStatus = asyncHandler(async (_req, res) => {
  const status = await scannerService.getStatus()
  res.json(status || {})
})

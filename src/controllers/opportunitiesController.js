const asyncHandler = require("../utils/asyncHandler")
const scannerService = require("../services/arbitrageScannerService")

function toOpportunityRow(row = {}) {
  const qualityScoreDisplay = row?.qualityScoreDisplay ?? row?.quality_score_display ?? null
  return {
    feedId: row?.feedId || null,
    detectedAt: row?.detectedAt || null,
    scanRunId: row?.scanRunId || null,
    isActive: row?.isActive == null ? true : Boolean(row.isActive),
    isDuplicate: Boolean(row?.isDuplicate),
    itemName: row?.itemName || "Tracked Item",
    itemCategory: row?.itemCategory || "weapon_skin",
    itemSubcategory: row?.itemSubcategory || null,
    itemRarity: row?.itemRarity || null,
    itemRarityColor: row?.itemRarityColor || null,
    itemImageUrl: row?.itemImageUrl || null,
    buyMarket: row?.buyMarket || null,
    buyPrice: row?.buyPrice ?? null,
    sellMarket: row?.sellMarket || null,
    sellNet: row?.sellNet ?? null,
    profit: row?.profit ?? null,
    spread: row?.spread ?? null,
    score: row?.score ?? null,
    qualityScoreDisplay,
    quality_score_display: qualityScoreDisplay,
    scoreCategory: row?.scoreCategory || null,
    executionConfidence: row?.executionConfidence || null,
    qualityGrade: row?.qualityGrade || null,
    liquidity: row?.liquidity ?? null,
    liquidityBand: row?.liquidityBand || null,
    liquidityLabel: row?.liquidityLabel || row?.liquidityBand || null,
    volume7d: row?.volume7d ?? null,
    marketCoverage: row?.marketCoverage ?? null,
    referencePrice: row?.referencePrice ?? null,
    flags: Array.isArray(row?.flags) ? row.flags : [],
    badges: Array.isArray(row?.badges) ? row.badges : [],
    isHighConfidenceEligible: Boolean(row?.isHighConfidenceEligible),
    isRiskyEligible: Boolean(row?.isRiskyEligible),
    itemId: row?.itemId || null,
    buyUrl: row?.buyUrl || null,
    sellUrl: row?.sellUrl || null,
    isLockedPreview: Boolean(row?.isLockedPreview),
    premiumCategory: row?.premiumCategory || null,
    premiumCategoryLabel: row?.premiumCategoryLabel || null,
    lockReason: row?.lockReason || null,
    lockMessage: row?.lockMessage || null,
    lockHint: row?.lockHint || null,
    previewSummary: row?.previewSummary || null,
    previewBuyPrice: row?.previewBuyPrice ?? null,
    previewSellNet: row?.previewSellNet ?? null,
    previewProfit: row?.previewProfit ?? null,
    previewSpread: row?.previewSpread ?? null,
    previewScoreBand: row?.previewScoreBand || null
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
    userId: req.userId,
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
    userId: req.userId,
    limit: req.validated?.limit,
    showRisky: req.query?.showRisky,
    category: req.query?.category,
    includeOlder: req.query?.includeOlder || req.query?.showOlder
  })

  res.json(toFeedResponse(data))
})

exports.refreshFeed = asyncHandler(async (req, res) => {
  const forceScanFromCatalog = req.body?.forceScanFromCatalog ?? req.body?.force_scan_from_catalog
  const queryForceScanFromCatalog = req.query?.forceScanFromCatalog ?? req.query?.force_scan_from_catalog

  const result = await scannerService.triggerRefresh({
    userId: req.userId,
    trigger: req.body?.trigger || "manual",
    forceRefresh: req.body?.forceRefresh,
    forceScanFromCatalog:
      forceScanFromCatalog == null ? queryForceScanFromCatalog : forceScanFromCatalog
  })

  res.status(202).json(result)
})

exports.refreshFeedAdmin = asyncHandler(async (req, res) => {
  const forceScanFromCatalog = req.body?.forceScanFromCatalog ?? req.body?.force_scan_from_catalog
  const queryForceScanFromCatalog = req.query?.forceScanFromCatalog ?? req.query?.force_scan_from_catalog

  const result = await scannerService.triggerRefresh({
    trigger: req.body?.trigger || "admin_cron",
    forceRefresh: req.body?.forceRefresh == null ? false : req.body.forceRefresh,
    forceScanFromCatalog:
      forceScanFromCatalog == null ? queryForceScanFromCatalog : forceScanFromCatalog
  })

  res.status(202).json(result)
})

exports.getScannerStatus = asyncHandler(async (_req, res) => {
  const status = await scannerService.getStatus()
  res.json(status || {})
})

const asyncHandler = require("../utils/asyncHandler")
const scannerService = require("../services/arbitrageScannerService")
const opportunityInsightService = require("../services/opportunityInsightService")

function toOpportunityRow(row = {}) {
  const qualityScoreDisplay = row?.qualityScoreDisplay ?? row?.quality_score_display ?? null
  const payload = {
    feedId: row?.feedId || row?.id || null,
    detectedAt: row?.detectedAt || null,
    scanRunId: row?.scanRunId || null,
    isActive: row?.isActive == null ? true : Boolean(row.isActive),
    isDuplicate: Boolean(row?.isDuplicate),
    itemId: row?.itemId || null,
    itemName: row?.itemName || "Tracked Item",
    itemCategory: row?.itemCategory || "weapon_skin",
    itemSubcategory: row?.itemSubcategory || null,
    itemCanonicalRarity: row?.itemCanonicalRarity || row?.item_canonical_rarity || null,
    itemRarity: row?.itemRarity || null,
    itemRarityColor: row?.itemRarityColor || null,
    itemRarityUnknownReason:
      row?.itemRarityUnknownReason || row?.item_rarity_unknown_reason || null,
    itemRarityDiagnostics:
      row?.itemRarityDiagnostics || row?.item_rarity_diagnostics || null,
    itemImageUrl: row?.itemImageUrl || null,
    buyMarket: row?.buyMarket || null,
    buyPrice: row?.buyPrice ?? null,
    sellMarket: row?.sellMarket || null,
    sellNet: row?.sellNet ?? null,
    profit: row?.profit ?? null,
    spread: row?.spread ?? null,
    score: row?.score ?? null,
    qualityScoreDisplay,
    scoreCategory: row?.scoreCategory || null,
    executionConfidence: row?.executionConfidence || null,
    qualityGrade: row?.qualityGrade || null,
    liquidity: row?.liquidity ?? null,
    liquidityBand: row?.liquidityBand || null,
    liquidityLabel: row?.liquidityLabel || row?.liquidityBand || null,
    volume7d: row?.volume7d ?? null,
    marketCoverage: row?.marketCoverage ?? null,
    referencePrice: row?.referencePrice ?? null,
    latestSignalAgeHours:
      row?.latestSignalAgeHours ?? row?.latest_signal_age_hours ?? null,
    refreshStatus: row?.refreshStatus || row?.refresh_status || "pending",
    liveStatus: row?.liveStatus || row?.live_status || "degraded",
    verdict: row?.verdict || null,
    buyUrl: row?.buyUrl || null,
    sellUrl: row?.sellUrl || null,
    flags: Array.isArray(row?.flags) ? row.flags : [],
    badges: Array.isArray(row?.badges) ? row.badges : [],
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

  return Object.fromEntries(
    Object.entries(payload).filter(([, value]) => {
      if (value == null) return false
      if (Array.isArray(value) && !value.length) return false
      return true
    })
  )
}

function toFeedResponse(data = {}) {
  return {
    opportunities: (Array.isArray(data?.opportunities) ? data.opportunities : []).map((row) =>
      toOpportunityRow(row)
    ),
    generatedAt: data?.generatedAt || null,
    ttlSeconds: data?.ttlSeconds || null,
    summary: data?.summary || null,
    pagination: data?.pagination || null,
    status: data?.status || null,
    currency: data?.currency || "USD"
  }
}

exports.getTopOpportunities = asyncHandler(async (req, res) => {
  const data = await scannerService.getTopOpportunities({
    userId: req.userId,
    limit: req.validated?.limit,
    page: req.validated?.page,
    cursor: req.query?.cursor,
    includeCount: req.query?.includeCount,
    historyHours: req.validated?.historyHours,
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
    page: req.validated?.page,
    cursor: req.query?.cursor,
    includeCount: req.query?.includeCount,
    historyHours: req.validated?.historyHours,
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

exports.getOpportunityInsight = asyncHandler(async (req, res) => {
  const opportunityId = String(req.params?.opportunityId || "").trim()
  const insight = await opportunityInsightService.getOpportunityInsight(opportunityId, {
    forceRefresh: req.query?.force
  })
  res.json(insight)
})

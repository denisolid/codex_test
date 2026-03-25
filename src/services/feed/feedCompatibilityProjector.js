const arbitrageFeedRepo = require("../../repositories/arbitrageFeedRepository")

function normalizeText(value) {
  return String(value || "").trim()
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIsoOrNull(value) {
  const text = normalizeText(value)
  if (!text) return null
  const ts = new Date(text).getTime()
  if (!Number.isFinite(ts)) return null
  return new Date(ts).toISOString()
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function toSafeInteger(value, fallback = 1, min = 1) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), min)
}

function buildFeedKey(row = {}) {
  const itemName = normalizeText(row.item_name || row.itemName || row.market_hash_name)
  const buyMarket = normalizeText(row.buy_market || row.buyMarket).toLowerCase()
  const sellMarket = normalizeText(row.sell_market || row.sellMarket).toLowerCase()
  if (!itemName || !buyMarket || !sellMarket) return ""
  return `${itemName}::${buyMarket}::${sellMarket}`.toLowerCase()
}

function resolveFingerprint(row = {}) {
  return normalizeText(
    row?.opportunity_fingerprint ||
      row?.opportunityFingerprint ||
      row?.metadata?.opportunity_fingerprint
  ).toLowerCase()
}

function resolveMaterialChangeHash(row = {}) {
  return normalizeText(
    row?.material_change_hash ||
      row?.materialChangeHash ||
      row?.metadata?.material_change_hash
  )
}

function buildRowDedupIdentity(row = {}) {
  const fingerprint = resolveFingerprint(row)
  if (fingerprint) return `fp:${fingerprint}`
  const signature = normalizeText(buildFeedKey(row)).toLowerCase()
  if (!signature) return ""
  return `sig:${signature}::material:${resolveMaterialChangeHash(row) || "na"}`
}

function resolveRowRecencyParts(row = {}) {
  return [
    row?.last_published_at,
    row?.lastPublishedAt,
    row?.last_seen_at,
    row?.lastSeenAt,
    row?.detected_at,
    row?.detectedAt,
    row?.created_at,
    row?.createdAt
  ].map((value) => {
    const iso = toIsoOrNull(value)
    if (!iso) return 0
    const ts = new Date(iso).getTime()
    return Number.isFinite(ts) ? ts : 0
  })
}

function compareRowRecency(left = {}, right = {}) {
  const leftParts = resolveRowRecencyParts(left)
  const rightParts = resolveRowRecencyParts(right)
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const delta = Number(rightParts[index] || 0) - Number(leftParts[index] || 0)
    if (delta !== 0) return delta
  }
  return normalizeText(right?.id).localeCompare(normalizeText(left?.id))
}

function collectOlderActiveDuplicateIds(rows = []) {
  const uniqueRows = []
  const seenIds = new Set()
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!Boolean(row?.is_active)) continue
    const id = normalizeText(row?.id)
    if (!id || seenIds.has(id)) continue
    seenIds.add(id)
    uniqueRows.push(row)
  }

  uniqueRows.sort((left, right) => {
    return compareRowRecency(left, right)
  })

  const seen = new Set()
  const duplicateIds = []
  for (const row of uniqueRows) {
    const dedupIdentity = buildRowDedupIdentity(row)
    if (!dedupIdentity) continue
    if (seen.has(dedupIdentity)) {
      duplicateIds.push(normalizeText(row?.id))
      continue
    }
    seen.add(dedupIdentity)
  }
  return Array.from(new Set(duplicateIds.filter(Boolean)))
}

function buildLegacyMetadata(activeRow = {}, previousLegacy = {}, projectionExtras = {}) {
  const previousMetadata = toJsonObject(previousLegacy?.metadata)
  const activeMetadata = toJsonObject(activeRow?.metadata)
  const extrasMetadata = toJsonObject(projectionExtras?.metadata)
  const hasPreviousLegacy = Boolean(previousLegacy && normalizeText(previousLegacy.id))
  const previousTimesSeen = hasPreviousLegacy
    ? toSafeInteger(previousLegacy?.times_seen ?? previousLegacy?.timesSeen, 1, 1)
    : 0
  const timesSeen = projectionExtras.incrementTimesSeen
    ? Math.max(previousTimesSeen + 1, 1)
    : Math.max(previousTimesSeen || 1, 1)

  return {
    ...previousMetadata,
    ...activeMetadata,
    ...extrasMetadata,
    opportunity_fingerprint: resolveFingerprint(activeRow) || resolveFingerprint(previousLegacy) || null,
    material_change_hash:
      normalizeText(activeRow?.material_change_hash) ||
      normalizeText(previousLegacy?.material_change_hash) ||
      null,
    first_seen_at:
      toIsoOrNull(activeRow?.first_seen_at) ||
      toIsoOrNull(previousLegacy?.first_seen_at) ||
      toIsoOrNull(previousLegacy?.discovered_at) ||
      null,
    last_seen_at:
      toIsoOrNull(activeRow?.last_seen_at) ||
      toIsoOrNull(previousLegacy?.last_seen_at) ||
      null,
    last_published_at:
      toIsoOrNull(activeRow?.last_published_at) ||
      toIsoOrNull(previousLegacy?.last_published_at) ||
      null,
    times_seen: timesSeen
  }
}

function buildLegacyProjection(activeRow = {}, previousLegacy = {}, options = {}) {
  const safeNowIso = toIsoOrNull(options.nowIso) || new Date().toISOString()
  const stage = normalizeText(options.stage).toLowerCase() || "publish"
  const projectionExtras = options.projectionExtras || {}
  const firstSeenAt =
    toIsoOrNull(activeRow?.first_seen_at) ||
    toIsoOrNull(previousLegacy?.first_seen_at) ||
    toIsoOrNull(previousLegacy?.discovered_at) ||
    safeNowIso
  const lastSeenAt =
    toIsoOrNull(activeRow?.last_seen_at) ||
    toIsoOrNull(previousLegacy?.last_seen_at) ||
    safeNowIso
  const lastPublishedAt =
    toIsoOrNull(activeRow?.last_published_at) ||
    toIsoOrNull(previousLegacy?.last_published_at) ||
    safeNowIso
  const refreshAttemptAt =
    stage === "revalidate"
      ? safeNowIso
      : toIsoOrNull(previousLegacy?.last_refresh_attempt_at) || null
  const insightRefreshedAt =
    stage === "revalidate"
      ? safeNowIso
      : toIsoOrNull(previousLegacy?.insight_refreshed_at) || null
  const isActive = normalizeText(activeRow?.live_status).toLowerCase() === "live"
  const verdict =
    normalizeText(projectionExtras?.verdict) ||
    normalizeText(activeRow?.metadata?.publish_refresh?.verdict) ||
    normalizeText(previousLegacy?.verdict) ||
    null
  const confidenceScore =
    toFiniteOrNull(projectionExtras?.confidence_score ?? projectionExtras?.confidenceScore) ??
    toFiniteOrNull(previousLegacy?.confidence_score ?? previousLegacy?.confidenceScore) ??
    null
  const freshnessScore =
    toFiniteOrNull(projectionExtras?.freshness_score ?? projectionExtras?.freshnessScore) ??
    toFiniteOrNull(previousLegacy?.freshness_score ?? previousLegacy?.freshnessScore) ??
    null
  const netProfitAfterFees =
    toFiniteOrNull(
      projectionExtras?.net_profit_after_fees ??
        projectionExtras?.netProfitAfterFees ??
        activeRow?.profit
    ) ??
    toFiniteOrNull(previousLegacy?.net_profit_after_fees ?? previousLegacy?.netProfitAfterFees) ??
    null
  const metadata = buildLegacyMetadata(activeRow, previousLegacy, {
    ...projectionExtras,
    incrementTimesSeen: stage === "publish"
  })
  const timesSeen = toSafeInteger(metadata.times_seen, 1, 1)

  return {
    item_name: activeRow.item_name,
    market_hash_name: activeRow.market_hash_name,
    category: activeRow.category,
    buy_market: activeRow.buy_market,
    buy_price: activeRow.buy_price,
    sell_market: activeRow.sell_market,
    sell_net: activeRow.sell_net,
    profit: activeRow.profit,
    spread_pct: activeRow.spread_pct,
    opportunity_score: activeRow.opportunity_score,
    execution_confidence: activeRow.execution_confidence,
    quality_grade: activeRow.quality_grade,
    liquidity_label: activeRow.liquidity_label,
    detected_at:
      stage === "publish"
        ? lastPublishedAt
        : toIsoOrNull(previousLegacy?.detected_at) || lastPublishedAt,
    discovered_at: firstSeenAt,
    first_seen_at: firstSeenAt,
    last_seen_at: lastSeenAt,
    last_published_at: lastPublishedAt,
    times_seen: timesSeen,
    opportunity_fingerprint: resolveFingerprint(activeRow) || null,
    material_change_hash: normalizeText(activeRow.material_change_hash) || null,
    market_signal_observed_at: toIsoOrNull(activeRow.market_signal_observed_at) || null,
    feed_published_at: lastPublishedAt,
    insight_refreshed_at: insightRefreshedAt,
    last_refresh_attempt_at: refreshAttemptAt,
    latest_signal_age_hours: toFiniteOrNull(activeRow.latest_signal_age_hours),
    net_profit_after_fees: netProfitAfterFees,
    confidence_score:
      confidenceScore == null ? null : Math.min(Math.max(Math.round(confidenceScore), 0), 100),
    freshness_score:
      freshnessScore == null ? null : Math.min(Math.max(Math.round(freshnessScore), 0), 100),
    verdict,
    refresh_status: normalizeText(activeRow.refresh_status).toLowerCase() || "pending",
    live_status: normalizeText(activeRow.live_status).toLowerCase() || "degraded",
    scan_run_id:
      stage === "publish"
        ? normalizeText(activeRow.scan_run_id) || null
        : normalizeText(previousLegacy?.scan_run_id || activeRow.scan_run_id) || null,
    is_active: isActive,
    is_duplicate: normalizeText(activeRow?.metadata?.feed_event).toLowerCase() === "duplicate",
    metadata
  }
}

async function syncRows({
  activeRows = [],
  stage = "publish",
  nowIso,
  projectionContextByFingerprint = {}
} = {}) {
  const safeNowIso = toIsoOrNull(nowIso) || new Date().toISOString()
  const rows = (Array.isArray(activeRows) ? activeRows : []).filter(
    (row) => normalizeText(row?.opportunity_fingerprint).toLowerCase()
  )
  if (!rows.length) {
    return {
      rowsWritten: 0,
      insertedCount: 0,
      updatedCount: 0,
      duplicateActivesMarkedInactive: 0
    }
  }

  const itemNames = Array.from(
    new Set(rows.map((row) => normalizeText(row.item_name || row.market_hash_name)).filter(Boolean))
  )
  const fingerprints = Array.from(
    new Set(rows.map((row) => normalizeText(row.opportunity_fingerprint).toLowerCase()).filter(Boolean))
  )

  const [legacyRecentRowsAll, legacyActiveFingerprintRows] = await Promise.all([
    arbitrageFeedRepo.getRecentRowsByItems({
      itemNames,
      includeInactive: true,
      limit: Math.max(250, itemNames.length * 20)
    }),
    arbitrageFeedRepo.getActiveRowsByFingerprints({
      fingerprints,
      limit: Math.max(250, fingerprints.length * 3)
    })
  ])

  const duplicateActiveIds = collectOlderActiveDuplicateIds([
    ...(legacyRecentRowsAll || []),
    ...(legacyActiveFingerprintRows || [])
  ])
  const duplicateActivesMarkedInactive = duplicateActiveIds.length
    ? await arbitrageFeedRepo.markRowsInactiveByIds(duplicateActiveIds)
    : 0

  const deactivatedIds = new Set(duplicateActiveIds.map((value) => normalizeText(value)))
  const legacyRecentRows = (legacyRecentRowsAll || []).filter(
    (row) => !deactivatedIds.has(normalizeText(row?.id))
  )
  const legacyActiveRows = (legacyActiveFingerprintRows || []).filter(
    (row) => !deactivatedIds.has(normalizeText(row?.id))
  )

  const activeByFingerprint = {}
  for (const row of legacyActiveRows) {
    const fingerprint = resolveFingerprint(row)
    if (fingerprint && !activeByFingerprint[fingerprint]) {
      activeByFingerprint[fingerprint] = row
    }
  }

  const latestByFingerprint = {}
  for (const row of legacyRecentRows) {
    const fingerprint = resolveFingerprint(row)
    if (fingerprint && !latestByFingerprint[fingerprint]) {
      latestByFingerprint[fingerprint] = row
    }
  }

  const legacyUpdates = []
  const legacyInserts = []
  for (const activeRow of rows) {
    const fingerprint = resolveFingerprint(activeRow)
    if (!fingerprint) continue
    const previousLegacy = activeByFingerprint[fingerprint] || latestByFingerprint[fingerprint] || null
    const projection = buildLegacyProjection(activeRow, previousLegacy, {
      stage,
      nowIso: safeNowIso,
      projectionExtras:
        projectionContextByFingerprint?.[fingerprint] &&
        typeof projectionContextByFingerprint[fingerprint] === "object"
          ? projectionContextByFingerprint[fingerprint]
          : {}
    })

    if (previousLegacy && normalizeText(previousLegacy.id)) {
      legacyUpdates.push({
        id: previousLegacy.id,
        patch: projection
      })
      continue
    }
    legacyInserts.push(projection)
  }

  const updatedCount = legacyUpdates.length
    ? await arbitrageFeedRepo.updateRowsById(legacyUpdates)
    : 0
  const insertedRows = legacyInserts.length ? await arbitrageFeedRepo.insertRows(legacyInserts) : []
  const insertedCount = Array.isArray(insertedRows) ? insertedRows.length : 0

  return {
    rowsWritten: duplicateActivesMarkedInactive + updatedCount + insertedCount,
    insertedCount,
    updatedCount,
    duplicateActivesMarkedInactive
  }
}

module.exports = {
  syncRows
}

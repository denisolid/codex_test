const { supabaseAdmin } = require("../config/supabase")
const AppError = require("../utils/AppError")

const TABLE = "market_quotes"
const SOURCES = new Set(["steam", "skinport", "csfloat", "dmarket"])
const INSERT_BATCH_SIZE = 400
const QUERY_BATCH_SIZE = 60

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeSource(value) {
  const source = normalizeText(value).toLowerCase()
  return SOURCES.has(source) ? source : ""
}

function toFiniteOrNull(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toPositiveOrNull(value) {
  const parsed = toFiniteOrNull(value)
  return parsed != null && parsed > 0 ? parsed : null
}

function toIntegerOrNull(value, options = {}) {
  const parsed = toFiniteOrNull(value)
  if (parsed == null) return null
  const min = Number.isFinite(Number(options.min)) ? Number(options.min) : -Infinity
  const max = Number.isFinite(Number(options.max)) ? Number(options.max) : Infinity
  return Math.min(Math.max(Math.round(parsed), min), max)
}

function roundPrice(value, decimals = 4) {
  const parsed = toPositiveOrNull(value)
  if (parsed == null) return null
  return Number(parsed.toFixed(decimals))
}

function resolveReferenceCandidate(row = {}) {
  return (
    roundPrice(row?.best_sell_net) ??
    roundPrice(row?.best_sell) ??
    roundPrice(row?.best_buy)
  )
}

function resolveConservativeMedian(values = []) {
  const sorted = (Array.isArray(values) ? values : [])
    .map((value) => toPositiveOrNull(value))
    .filter((value) => value != null)
    .sort((a, b) => a - b)
  if (!sorted.length) return null
  return roundPrice(sorted[Math.floor((sorted.length - 1) / 2)])
}

function toJsonObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {}
  }
  return value
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const itemName = normalizeText(row?.item_name || row?.itemName)
      const market = normalizeSource(row?.market || row?.source)
      if (!itemName || !market) return null

      const bestBuy = toFiniteOrNull(row?.best_buy ?? row?.bestBuy)
      const bestSell = toFiniteOrNull(row?.best_sell ?? row?.bestSell)
      const bestSellNet = toFiniteOrNull(row?.best_sell_net ?? row?.bestSellNet)
      const volume7d = toIntegerOrNull(row?.volume_7d ?? row?.volume7d, { min: 0 })
      const liquidityScore = toIntegerOrNull(row?.liquidity_score ?? row?.liquidityScore, {
        min: 0,
        max: 100
      })

      return {
        item_name: itemName,
        market,
        best_buy: bestBuy == null ? null : Number(bestBuy.toFixed(4)),
        best_sell: bestSell == null ? null : Number(bestSell.toFixed(4)),
        best_sell_net: bestSellNet == null ? null : Number(bestSellNet.toFixed(4)),
        volume_7d: volume7d,
        liquidity_score: liquidityScore,
        fetched_at: row?.fetched_at || row?.fetchedAt || new Date().toISOString(),
        quality_flags: toJsonObject(row?.quality_flags || row?.qualityFlags)
      }
    })
    .filter(Boolean)
}

function normalizeItemNames(names = []) {
  return Array.from(
    new Set(
      (Array.isArray(names) ? names : [])
        .map((name) => normalizeText(name))
        .filter(Boolean)
    )
  )
}

function formatSupabaseError(error, fallbackMessage = "database_error") {
  const message = normalizeText(error?.message) || fallbackMessage
  const details = normalizeText(error?.details)
  const hint = normalizeText(error?.hint)
  const code = normalizeText(error?.code)

  const chunks = [message]
  if (details) chunks.push(`details: ${details}`)
  if (hint) chunks.push(`hint: ${hint}`)
  if (code) chunks.push(`code: ${code}`)
  return chunks.join(" | ")
}

function isMissingCoverageRpcError(error) {
  if (!error) return false
  const code = normalizeText(error?.code).toUpperCase()
  const message = normalizeText(error?.message).toLowerCase()
  return (
    code === "PGRST202" ||
    message.includes("could not find the function public.get_latest_market_quote_rows_by_item_names")
  )
}

function normalizeMarkets(values = []) {
  const safeValues = Array.isArray(values) ? values : []
  if (!safeValues.length) return []
  return Array.from(
    new Set(
      safeValues
        .map((value) => normalizeSource(value))
        .filter(Boolean)
    )
  )
}

async function getCoverageRowsFallback(itemNames = [], lookbackIso = null, maxRows = 5000) {
  const { data, error } = await supabaseAdmin
    .from(TABLE)
    .select("item_name, market, best_buy, best_sell, best_sell_net, volume_7d, fetched_at")
    .in("item_name", itemNames)
    .gte("fetched_at", lookbackIso)
    .order("fetched_at", { ascending: false })
    .limit(maxRows)

  if (error) {
    throw new AppError(formatSupabaseError(error, "market_quote_coverage_lookup_failed"), 500)
  }

  return Array.isArray(data) ? data : []
}

exports.insertRows = async (rows = []) => {
  const payload = normalizeRows(rows)
  if (!payload.length) return 0

  let insertedCount = 0
  for (let index = 0; index < payload.length; index += INSERT_BATCH_SIZE) {
    const chunk = payload.slice(index, index + INSERT_BATCH_SIZE)
    const { error } = await supabaseAdmin.from(TABLE).insert(chunk)
    if (error) {
      throw new AppError(formatSupabaseError(error, "market_quotes_insert_failed"), 500)
    }
    insertedCount += chunk.length
  }

  return insertedCount
}

exports.getLatestCoverageByItemNames = async (itemNames = [], options = {}) => {
  const safeNames = normalizeItemNames(itemNames)
  if (!safeNames.length) return {}

  const lookbackHours = Math.max(Math.round(Number(options.lookbackHours || 72)), 1)
  const maxRowsPerChunk = Math.max(Math.round(Number(options.maxRowsPerChunk || 5000)), 200)
  const lookbackIso = new Date(Date.now() - lookbackHours * 60 * 60 * 1000).toISOString()
  const latestByItemMarket = {}
  let rpcAvailable = true

  for (let index = 0; index < safeNames.length; index += QUERY_BATCH_SIZE) {
    const chunk = safeNames.slice(index, index + QUERY_BATCH_SIZE)
    let rows = []

    if (rpcAvailable) {
      const rpcResult = await supabaseAdmin.rpc("get_latest_market_quote_rows_by_item_names", {
        p_item_names: chunk,
        p_lookback: lookbackIso
      })

      if (!rpcResult.error) {
        rows = Array.isArray(rpcResult.data) ? rpcResult.data : []
      } else if (isMissingCoverageRpcError(rpcResult.error)) {
        rpcAvailable = false
      } else {
        throw new AppError(
          formatSupabaseError(rpcResult.error, "market_quote_coverage_lookup_failed"),
          500
        )
      }
    }

    if (!rpcAvailable) {
      rows = await getCoverageRowsFallback(chunk, lookbackIso, maxRowsPerChunk)
    }

    for (const row of rows) {
      const itemName = normalizeText(row?.item_name)
      const market = normalizeSource(row?.market)
      if (!itemName || !market) continue
      const signature = `${itemName}::${market}`
      if (!latestByItemMarket[signature]) {
        latestByItemMarket[signature] = row
      }
    }
  }

  const coverageByItem = {}
  for (const row of Object.values(latestByItemMarket)) {
    const itemName = normalizeText(row?.item_name)
    if (!coverageByItem[itemName]) {
      coverageByItem[itemName] = {
        marketCoverageCount: 0,
        markets: {},
        volume7dMax: null,
        latestFetchedAt: null,
        referencePriceCandidates: []
      }
    }
    const bucket = coverageByItem[itemName]
    const market = normalizeSource(row?.market)
    if (!bucket.markets[market]) {
      bucket.markets[market] = true
      bucket.marketCoverageCount += 1
    }

    const volume7d = toIntegerOrNull(row?.volume_7d, { min: 0 })
    if (volume7d != null) {
      bucket.volume7dMax =
        bucket.volume7dMax == null ? volume7d : Math.max(Number(bucket.volume7dMax), volume7d)
    }
    const referenceCandidate = resolveReferenceCandidate(row)
    if (referenceCandidate != null) {
      bucket.referencePriceCandidates.push(referenceCandidate)
    }
    const fetchedAt = normalizeText(row?.fetched_at)
    if (fetchedAt) {
      const nextTs = new Date(fetchedAt).getTime()
      const prevTs = bucket.latestFetchedAt
        ? new Date(bucket.latestFetchedAt).getTime()
        : NaN
      if (!Number.isFinite(prevTs) || (Number.isFinite(nextTs) && nextTs > prevTs)) {
        bucket.latestFetchedAt = fetchedAt
      }
    }
  }

  return Object.fromEntries(
    Object.entries(coverageByItem).map(([itemName, bucket]) => [
      itemName,
      {
        marketCoverageCount: Number(bucket?.marketCoverageCount || 0),
        markets: bucket?.markets || {},
        volume7dMax: bucket?.volume7dMax == null ? null : Number(bucket.volume7dMax),
        latestFetchedAt: bucket?.latestFetchedAt || null,
        referencePriceMedian: resolveConservativeMedian(bucket?.referencePriceCandidates),
        referencePriceCandidateCount: Array.isArray(bucket?.referencePriceCandidates)
          ? bucket.referencePriceCandidates.length
          : 0
      }
    ])
  )
}

exports.getLatestRowsByItemNames = async (itemNames = [], options = {}) => {
  const safeNames = normalizeItemNames(itemNames)
  if (!safeNames.length) return {}

  const lookbackHours = Math.max(Math.round(Number(options.lookbackHours || 72)), 1)
  const lookbackIso = new Date(Date.now() - lookbackHours * 60 * 60 * 1000).toISOString()
  const allowedMarkets = normalizeMarkets(options.markets)
  const rowsByItem = {}
  let rpcAvailable = true

  for (let index = 0; index < safeNames.length; index += QUERY_BATCH_SIZE) {
    const chunk = safeNames.slice(index, index + QUERY_BATCH_SIZE)
    let rows = []

    if (rpcAvailable) {
      const rpcResult = await supabaseAdmin.rpc("get_latest_market_quote_rows_by_item_names", {
        p_item_names: chunk,
        p_lookback: lookbackIso
      })
      if (!rpcResult.error) {
        rows = Array.isArray(rpcResult.data) ? rpcResult.data : []
      } else if (isMissingCoverageRpcError(rpcResult.error)) {
        rpcAvailable = false
      } else {
        throw new AppError(
          formatSupabaseError(rpcResult.error, "market_quote_rows_lookup_failed"),
          500
        )
      }
    }

    if (!rpcAvailable) {
      const { data, error } = await supabaseAdmin
        .from(TABLE)
        .select("item_name, market, best_buy, best_sell, best_sell_net, volume_7d, fetched_at")
        .in("item_name", chunk)
        .gte("fetched_at", lookbackIso)
        .order("fetched_at", { ascending: false })
        .limit(5000)
      if (error) {
        throw new AppError(
          formatSupabaseError(error, "market_quote_rows_lookup_failed"),
          500
        )
      }
      rows = Array.isArray(data) ? data : []
    }

    for (const row of rows) {
      const itemName = normalizeText(row?.item_name)
      const market = normalizeSource(row?.market)
      if (!itemName || !market) continue
      if (allowedMarkets.length && !allowedMarkets.includes(market)) continue

      if (!rowsByItem[itemName]) rowsByItem[itemName] = {}
      if (!rowsByItem[itemName][market]) {
        rowsByItem[itemName][market] = {
          item_name: itemName,
          market,
          best_buy: toPositiveOrNull(row?.best_buy),
          best_sell: toPositiveOrNull(row?.best_sell),
          best_sell_net: toPositiveOrNull(row?.best_sell_net),
          volume_7d: toIntegerOrNull(row?.volume_7d, { min: 0 }),
          fetched_at: normalizeText(row?.fetched_at) || null
        }
      }
    }
  }

  return rowsByItem
}

exports.__testables = {
  normalizeItemNames,
  formatSupabaseError,
  resolveReferenceCandidate,
  resolveConservativeMedian,
  normalizeMarkets
}

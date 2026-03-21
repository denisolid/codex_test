#!/usr/bin/env node

require("dotenv").config()

const { supabaseAdmin } = require("../src/config/supabase")
const {
  CANONICAL_RARITY,
  resolveCanonicalRarity,
  canonicalRarityToDisplay,
  getCanonicalRarityColor,
  buildUnknownRarityDiagnostics,
  normalizeCanonicalRarity
} = require("../src/utils/rarityResolver")

const DEFAULT_PAGE_SIZE = 1000
const DEFAULT_WRITE_BATCH_SIZE = 250
const UNKNOWN_TEXT_SET = new Set([
  "unknown",
  "default",
  "none",
  "n/a",
  "na",
  "null",
  "-",
  "?"
])

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeBoolean(value, fallback = false) {
  if (value == null) return fallback
  if (typeof value === "boolean") return value
  const raw = String(value).trim().toLowerCase()
  if (!raw) return fallback
  return raw === "true" || raw === "1" || raw === "yes"
}

function normalizeInteger(value, fallback = 0, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  const rounded = Math.round(parsed)
  return Math.min(Math.max(rounded, min), max)
}

function parseArgs(argv = []) {
  const map = {}
  for (const rawValue of Array.isArray(argv) ? argv : []) {
    const raw = String(rawValue || "").trim()
    if (!raw.startsWith("--")) continue
    const [left, right] = raw.split("=", 2)
    const key = left.replace(/^--/, "").trim().toLowerCase()
    map[key] = right == null ? true : right
  }
  return map
}

function isNullOrUnknownText(value) {
  const safe = normalizeText(value).toLowerCase()
  if (!safe) return true
  return UNKNOWN_TEXT_SET.has(safe)
}

function chunkArray(values = [], size = DEFAULT_WRITE_BATCH_SIZE) {
  const safeValues = Array.isArray(values) ? values : []
  const safeSize = Math.max(Number(size || 0), 1)
  const chunks = []
  for (let index = 0; index < safeValues.length; index += safeSize) {
    chunks.push(safeValues.slice(index, index + safeSize))
  }
  return chunks
}

function isMissingColumnError(error, columnName = "") {
  const message = String(error?.message || "").toLowerCase()
  const code = String(error?.code || "").toUpperCase()
  return (
    code === "42703" ||
    (message.includes("does not exist") &&
      (!columnName || message.includes(String(columnName || "").toLowerCase())))
  )
}

async function listAllSkins({ pageSize = DEFAULT_PAGE_SIZE, maxRows = 0 } = {}) {
  const rows = []
  let offset = 0
  let canonicalColumnAvailable = true

  while (true) {
    const to = offset + Math.max(Number(pageSize || 0), 1) - 1
    let data = null
    let error = null
    if (canonicalColumnAvailable) {
      const result = await supabaseAdmin
        .from("skins")
        .select("id, market_hash_name, weapon, rarity, canonical_rarity, rarity_color")
        .order("id", { ascending: true })
        .range(offset, to)
      data = result.data
      error = result.error
      if (error && isMissingColumnError(error, "canonical_rarity")) {
        canonicalColumnAvailable = false
      }
    }
    if (!canonicalColumnAvailable) {
      const fallback = await supabaseAdmin
        .from("skins")
        .select("id, market_hash_name, weapon, rarity, rarity_color")
        .order("id", { ascending: true })
        .range(offset, to)
      data = Array.isArray(fallback.data)
        ? fallback.data.map((row) => ({ ...row, canonical_rarity: null }))
        : fallback.data
      error = fallback.error
    }

    if (error) {
      throw new Error(error.message || "skins_select_failed")
    }

    const chunk = Array.isArray(data) ? data : []
    if (!chunk.length) break

    rows.push(...chunk)
    if (maxRows > 0 && rows.length >= maxRows) {
      return rows.slice(0, maxRows)
    }
    if (chunk.length < pageSize) break
    offset += chunk.length
  }

  return { rows, canonicalColumnAvailable }
}

function buildAuditRows(rows = [], options = {}) {
  const canonicalColumnAvailable = options.canonicalColumnAvailable !== false
  const unresolvedRows = []
  const backfillableRows = []
  const unchangedRows = []
  const unknownReasonCounts = {}

  let missingOrUnknownDisplayRarityCount = 0
  let missingOrUnknownCanonicalRarityCount = 0
  let validCatalogRarityCount = 0
  let validSourceRarityCount = 0
  let deterministicFallbackCount = 0
  let unknownResolvedCount = 0

  for (const row of Array.isArray(rows) ? rows : []) {
    const catalogRarityRaw = normalizeText(row?.canonical_rarity || "")
    const sourceRarityRaw = normalizeText(row?.rarity || "")
    const marketHashName = normalizeText(row?.market_hash_name || "")
    const weapon = normalizeText(row?.weapon || "")
    const canonicalFromCatalog = normalizeCanonicalRarity(catalogRarityRaw)
    const canonicalFromSource = normalizeCanonicalRarity(sourceRarityRaw)

    if (isNullOrUnknownText(sourceRarityRaw)) {
      missingOrUnknownDisplayRarityCount += 1
    }
    if (!canonicalFromCatalog || canonicalFromCatalog === CANONICAL_RARITY.UNKNOWN) {
      missingOrUnknownCanonicalRarityCount += 1
    }

    const resolution = resolveCanonicalRarity({
      catalogRarity: catalogRarityRaw || null,
      sourceRarity: sourceRarityRaw || null,
      marketHashName,
      weapon
    })
    const resolvedCanonical = resolution.canonicalRarity
    const resolvedRarity = canonicalRarityToDisplay(resolvedCanonical)
    const resolvedColor = getCanonicalRarityColor(resolvedCanonical)
    const unknownDiagnostics = buildUnknownRarityDiagnostics(resolution, {
      marketHashName,
      weapon,
      catalogRarity: catalogRarityRaw || null,
      sourceRarity: sourceRarityRaw || null
    })

    if (resolution.source === "catalog_rarity") validCatalogRarityCount += 1
    else if (resolution.source === "source_rarity") validSourceRarityCount += 1
    else if (resolution.source === "deterministic_fallback") deterministicFallbackCount += 1
    else unknownResolvedCount += 1

    const needsDisplayRarityBackfill = isNullOrUnknownText(sourceRarityRaw)
    const needsCanonicalRarityBackfill = canonicalColumnAvailable
      ? !canonicalFromCatalog ||
        canonicalFromCatalog === CANONICAL_RARITY.UNKNOWN ||
        canonicalFromCatalog !== resolvedCanonical
      : false
    const needsColorBackfill = normalizeText(row?.rarity_color).toLowerCase() !== resolvedColor
    const shouldBackfill = resolvedCanonical !== CANONICAL_RARITY.UNKNOWN
    const needsBackfill =
      shouldBackfill &&
      (needsDisplayRarityBackfill || needsCanonicalRarityBackfill || needsColorBackfill)

    if (resolvedCanonical === CANONICAL_RARITY.UNKNOWN) {
      const unknownReason = normalizeText(unknownDiagnostics?.reason || "unknown_rarity")
      unknownReasonCounts[unknownReason] = Number(unknownReasonCounts[unknownReason] || 0) + 1
      unresolvedRows.push({
        id: row?.id || null,
        market_hash_name: marketHashName || null,
        rarity: sourceRarityRaw || null,
        canonical_rarity: catalogRarityRaw || null,
        unknown_reason: unknownReason,
        resolver_source: resolution.source || "unknown"
      })
      continue
    }

    if (needsBackfill) {
      backfillableRows.push({
        id: row?.id,
        market_hash_name: marketHashName,
        update: {
          id: row?.id,
          market_hash_name: marketHashName,
          rarity: resolvedRarity,
          canonical_rarity: resolvedCanonical,
          rarity_color: resolvedColor
        },
        reason: {
          needsDisplayRarityBackfill,
          needsCanonicalRarityBackfill,
          needsColorBackfill,
          resolverSource: resolution.source
        }
      })
    } else {
      unchangedRows.push({
        id: row?.id,
        market_hash_name: marketHashName
      })
    }
  }

  return {
    unresolvedRows,
    backfillableRows,
    unchangedRows,
    unknownReasonCounts,
    counters: {
      totalRows: rows.length,
      missingOrUnknownDisplayRarityCount,
      missingOrUnknownCanonicalRarityCount,
      validCatalogRarityCount,
      validSourceRarityCount,
      deterministicFallbackCount,
      unknownResolvedCount,
      backfillableCount: backfillableRows.length,
      unresolvedCount: unresolvedRows.length,
      unchangedCount: unchangedRows.length
    }
  }
}

async function applyBackfillRows(
  rows = [],
  batchSize = DEFAULT_WRITE_BATCH_SIZE,
  options = {}
) {
  const canonicalColumnAvailable = options.canonicalColumnAvailable !== false
  const payload = (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const update = row?.update ? { ...row.update } : null
      if (!update) return null
      if (!canonicalColumnAvailable) {
        delete update.canonical_rarity
      }
      return update
    })
    .filter(Boolean)
  if (!payload.length) {
    return { updated: 0, attempted: 0 }
  }

  let updated = 0
  for (const chunk of chunkArray(payload, batchSize)) {
    const { error } = await supabaseAdmin
      .from("skins")
      .upsert(chunk, { onConflict: "id" })
    if (error) {
      throw new Error(error.message || "skins_backfill_upsert_failed")
    }
    updated += chunk.length
  }

  return { updated, attempted: payload.length, canonicalColumnAvailable }
}

async function main() {
  const cli = parseArgs(process.argv.slice(2))
  const apply = normalizeBoolean(cli.apply, false)
  const maxRows = normalizeInteger(cli.limit, 0, 0)
  const pageSize = normalizeInteger(cli["page-size"], DEFAULT_PAGE_SIZE, 50, 5000)
  const batchSize = normalizeInteger(
    cli["batch-size"],
    DEFAULT_WRITE_BATCH_SIZE,
    1,
    1000
  )
  const sampleSize = normalizeInteger(cli.sample, 30, 1, 500)

  const listed = await listAllSkins({ pageSize, maxRows })
  const rows = Array.isArray(listed?.rows) ? listed.rows : []
  const canonicalColumnAvailable = listed?.canonicalColumnAvailable !== false
  const audit = buildAuditRows(rows, { canonicalColumnAvailable })
  const applyResult = apply
    ? await applyBackfillRows(audit.backfillableRows, batchSize, { canonicalColumnAvailable })
    : { updated: 0, attempted: 0, canonicalColumnAvailable }

  const report = {
    generatedAt: new Date().toISOString(),
    applyMode: apply,
    canonicalColumnAvailable,
    counters: audit.counters,
    unknownReasonCounts: audit.unknownReasonCounts,
    backfill: applyResult,
    samples: {
      backfillable: audit.backfillableRows.slice(0, sampleSize),
      unresolved: audit.unresolvedRows.slice(0, sampleSize)
    }
  }

  console.log(JSON.stringify(report, null, 2))
}

main().catch((err) => {
  console.error(
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        error: String(err?.message || err || "rarity_audit_failed")
      },
      null,
      2
    )
  )
  process.exit(1)
})

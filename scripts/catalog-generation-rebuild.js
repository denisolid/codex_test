#!/usr/bin/env node

require("dotenv").config()

const catalogGenerationService = require("../src/services/catalogGenerationService")

function normalizeText(value) {
  return String(value || "").trim()
}

function normalizeInteger(value, fallback, min = 0) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(Math.round(parsed), min)
}

function parseList(value) {
  const text = normalizeText(value)
  if (!text) return []
  return text
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
}

function normalizeArgKey(raw = "") {
  return normalizeText(raw).replace(/^--/, "").toLowerCase()
}

function parseArgs(argv = []) {
  const map = {}
  for (const raw of Array.isArray(argv) ? argv : []) {
    const text = normalizeText(raw)
    if (!text.startsWith("--")) continue
    const [left, right] = text.split("=", 2)
    map[normalizeArgKey(left)] = right == null ? true : right
  }
  return map
}

function normalizeEnableScanMode(value) {
  const text = normalizeText(value).toLowerCase()
  if (text === "force") return "force"
  if (text === "never" || text === "off" || text === "false") return "never"
  return "auto"
}

function summarizeResult(result = {}) {
  return {
    startedAt: result?.startedAt || null,
    completedAt: result?.completedAt || null,
    targetUniverseSize: Number(result?.targetUniverseSize || 0),
    categories: Array.isArray(result?.categories) ? result.categories : [],
    previousGeneration: result?.previousGeneration || null,
    activeGeneration: result?.activeGeneration || null,
    readiness: result?.diagnostics?.readiness || {},
    comparison: result?.diagnostics?.comparison || {},
    previousSummary: result?.diagnostics?.previousSummary || null,
    nextSummary: result?.diagnostics?.nextSummary || null
  }
}

async function main() {
  const cli = parseArgs(process.argv.slice(2))
  const targetUniverseSize = Math.max(normalizeInteger(cli.target, 600, 1), 1)
  const categories = parseList(cli.categories)
  const enableScanMode = normalizeEnableScanMode(cli["enable-scan"] || "auto")

  console.log(
    JSON.stringify(
      {
        step: "catalog_generation_reset_start",
        targetUniverseSize,
        categories,
        enableScanMode
      },
      null,
      2
    )
  )

  const result = await catalogGenerationService.runCatalogGenerationReset({
    targetUniverseSize,
    categories,
    generationKey: normalizeText(cli["generation-key"]) || null,
    autoEnableOpportunityScan: enableScanMode === "auto",
    forceEnableOpportunityScan: enableScanMode === "force"
  })

  console.log(
    JSON.stringify(
      {
        step: "catalog_generation_reset_done",
        result: summarizeResult(result)
      },
      null,
      2
    )
  )
}

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        step: "catalog_generation_reset_failed",
        error: normalizeText(error?.message) || "unknown_error"
      },
      null,
      2
    )
  )
  process.exit(1)
})

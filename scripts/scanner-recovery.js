#!/usr/bin/env node

require("dotenv").config()

const scannerService = require("../src/services/arbitrageScannerService")
const upstreamMarketFreshnessRecoveryService = require("../src/services/upstreamMarketFreshnessRecoveryService")
const scannerRunRepo = require("../src/repositories/scannerRunRepository")
const { SCANNER_TYPES } = require("../src/services/scanner/config")

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(Number(ms || 0), 0)))
}

function toInt(value, fallback) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? Math.round(parsed) : fallback
}

function normalizeArgKey(raw = "") {
  return String(raw || "")
    .trim()
    .replace(/^--/, "")
    .toLowerCase()
}

function parseArgs(argv = []) {
  const args = Array.isArray(argv) ? argv : []
  const map = {}
  for (const raw of args) {
    const text = String(raw || "").trim()
    if (!text.startsWith("--")) continue
    const [left, right] = text.split("=", 2)
    map[normalizeArgKey(left)] = right == null ? true : right
  }
  return map
}

function summarizeRecoveryDiagnostics(diag = {}) {
  return {
    generatedAt: diag?.generatedAt || null,
    targetLimit: Number(diag?.targetLimit || 0),
    targets: diag?.targets || {},
    healthGate: diag?.healthGate || {},
    quoteRefresh: diag?.quoteRefresh || {},
    snapshotRefresh: diag?.snapshotRefresh || {},
    postRefresh: {
      totalRows: Number(diag?.postRefresh?.totalRows || 0),
      rowsStillStale: Number(diag?.postRefresh?.rowsStillStale || 0),
      upstreamNewerThanCatalog: Number(diag?.postRefresh?.upstreamNewerThanCatalog || 0),
      byCategory: diag?.postRefresh?.byCategory || {}
    },
    catalogRecompute: diag?.catalogRecompute || {}
  }
}

function summarizeRun(run = {}) {
  const diag = run?.diagnostics_summary || {}
  return {
    id: run?.id || null,
    scannerType: run?.scanner_type || null,
    status: run?.status || null,
    startedAt: run?.started_at || null,
    completedAt: run?.completed_at || null,
    itemsScanned: Number(run?.items_scanned || 0),
    opportunitiesFound: Number(run?.opportunities_found || 0),
    newOpportunitiesAdded: Number(run?.new_opportunities_added || 0),
    failureReason: run?.failure_reason || null,
    scannedItems: Number(diag?.scannedItems || 0),
    strongFound: Number(diag?.strongFound || 0),
    riskyFound: Number(diag?.riskyFound || 0),
    speculativeFound: Number(diag?.speculativeFound || 0),
    rejectedFound: Number(diag?.rejectedFound || 0),
    batchScan: diag?.batchScan || {},
    rejectedByReason: diag?.rejectedByReason || {}
  }
}

async function waitForRunCompletion(runId, options = {}) {
  const timeoutMs = Math.max(toInt(options.timeoutMs, 600000), 10000)
  const pollMs = Math.max(toInt(options.pollMs, 5000), 1000)
  const startedAtMs = Date.now()

  while (Date.now() - startedAtMs < timeoutMs) {
    const run = await scannerRunRepo.getById(runId)
    if (!run) {
      throw new Error(`Run not found: ${runId}`)
    }
    if (String(run.status || "").toLowerCase() !== "running") {
      return run
    }
    await sleep(pollMs)
  }

  throw new Error(`Timed out waiting for run ${runId}`)
}

async function main() {
  const cli = parseArgs(process.argv.slice(2))
  const targetUniverseSize = Math.max(
    toInt(
      cli.target ||
        process.env.ARBITRAGE_SCANNER_UNIVERSE_TARGET_SIZE ||
        process.env.ARBITRAGE_DEFAULT_UNIVERSE_LIMIT ||
        3000,
      3000
    ),
    1
  )
  const refreshLimit = Math.max(toInt(cli.limit, 900), 1)
  const quoteBatchSize = Math.max(toInt(cli["quote-batch"], 80), 1)
  const snapshotBatchSize = Math.max(toInt(cli["snapshot-batch"], 60), 1)
  const skipScan = Boolean(cli["skip-scan"] || false)
  const skipRecompute = Boolean(cli["skip-recompute"] || false)
  const wait = Boolean(cli.wait || false)
  const waitMs = Math.max(toInt(cli["wait-ms"], 600000), 10000)
  const pollMs = Math.max(toInt(cli["poll-ms"], 5000), 1000)

  console.log(
    JSON.stringify(
      {
        step: "refresh_upstream_market_freshness",
        targetUniverseSize,
        refreshLimit,
        quoteBatchSize,
        snapshotBatchSize,
        recompute: !skipRecompute
      },
      null,
      2
    )
  )
  const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
    limit: refreshLimit,
    quoteBatchSize,
    snapshotBatchSize,
    targetUniverseSize,
    recompute: !skipRecompute
  })
  console.log(
    JSON.stringify(
      {
        step: "refresh_upstream_market_freshness_done",
        diagnostics: summarizeRecoveryDiagnostics(diagnostics)
      },
      null,
      2
    )
  )

  if (skipScan || !diagnostics?.healthGate?.healthyEnough || !diagnostics?.catalogRecompute?.executed) {
    return
  }

  const refresh = await scannerService.triggerRefresh({
    trigger: "manual_recovery_script",
    forceRefresh: true,
    jobType: SCANNER_TYPES.OPPORTUNITY_SCAN
  })
  const opportunityJob = refresh?.jobs?.[SCANNER_TYPES.OPPORTUNITY_SCAN] || null
  const runId = opportunityJob?.scanRunId || refresh?.scanRunId || null

  console.log(
    JSON.stringify(
      {
        step: "trigger_opportunity_scan",
        refresh: {
          scanRunId: refresh?.scanRunId || null,
          alreadyRunning: Boolean(refresh?.alreadyRunning),
          job: opportunityJob
        }
      },
      null,
      2
    )
  )

  if (!wait || !runId) {
    return
  }

  const run = await waitForRunCompletion(runId, { timeoutMs: waitMs, pollMs })
  console.log(
    JSON.stringify(
      {
        step: "opportunity_scan_completed",
        run: summarizeRun(run)
      },
      null,
      2
    )
  )
}

main().catch((err) => {
  console.error(
    JSON.stringify(
      {
        step: "failed",
        error: String(err?.message || err || "unknown_error")
      },
      null,
      2
    )
  )
  process.exit(1)
})

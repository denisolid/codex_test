#!/usr/bin/env node

require("dotenv").config()

const scannerService = require("../src/services/scannerV2Service")
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

function toBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value
  if (value == null) return fallback
  const text = String(value).trim().toLowerCase()
  if (!text) return fallback
  if (["1", "true", "yes", "on"].includes(text)) return true
  if (["0", "false", "no", "off"].includes(text)) return false
  return fallback
}

function parseList(value) {
  const text = String(value || "").trim()
  if (!text) return []
  return text
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
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
    completed: Boolean(diag?.completed),
    paused: Boolean(diag?.paused),
    pauseReason: diag?.pauseReason || null,
    timedOut: Boolean(diag?.timedOut),
    failedStage: diag?.failedStage || null,
    error: diag?.error || null,
    targetLimit: Number(diag?.targetLimit || 0),
    selectionBatchSize: Number(diag?.selectionBatchSize || 0),
    completedBatches: Number(diag?.completedBatches || 0),
    processedRows: Number(diag?.processedRows || 0),
    processedRowsByCategory: diag?.processedRowsByCategory || {},
    checkpoint: diag?.checkpoint || null,
    targets: diag?.targets || {},
    healthGate: diag?.healthGate || {},
    categoryHealthGate: diag?.categoryHealthGate || {},
    quoteRefresh: diag?.quoteRefresh || {},
    snapshotRefresh: diag?.snapshotRefresh || {},
    weaponSkinVerification: diag?.weaponSkinVerification || {},
    snapshotPacing: diag?.snapshotPacing || {},
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
    eligibleFound: Number(diag?.eligibleFound || 0),
    nearEligibleFound: Number(diag?.nearEligibleFound || 0),
    candidateFound: Number(diag?.candidateFound || 0),
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
  const targetUniverseSize = Math.max(toInt(cli.target, 600), 1)
  const refreshLimit = Math.max(toInt(cli.limit, 180), 1)
  const selectionBatchSize = Math.max(toInt(cli["selection-batch"], 30), 1)
  const quoteBatchSize = Math.max(toInt(cli["quote-batch"], 20), 1)
  const snapshotBatchSize = Math.max(toInt(cli["snapshot-batch"], 10), 1)
  const maxBatches =
    Number.isFinite(Number(cli["max-batches"])) && Number(cli["max-batches"]) > 0
      ? Math.max(toInt(cli["max-batches"], 0), 1)
      : null
  const categories = parseList(cli.categories)
  const resumeState = cli["resume-state"] ? String(cli["resume-state"]).trim() : null
  const startCategory = cli["start-category"] ? String(cli["start-category"]).trim() : null
  const startOffset = Math.max(toInt(cli["start-offset"], 0), 0)
  const skipScan = toBoolean(cli["skip-scan"], false)
  const skipRecompute = toBoolean(cli["skip-recompute"], false)
  const wait = toBoolean(cli.wait, false)
  const waitMs = Math.max(toInt(cli["wait-ms"], 600000), 10000)
  const pollMs = Math.max(toInt(cli["poll-ms"], 5000), 1000)

  console.log(
    JSON.stringify(
      {
        step: "refresh_upstream_market_freshness",
        categories: categories.length ? categories : undefined,
        resumeStatePresent: Boolean(resumeState),
        startCategory,
        startOffset,
        targetUniverseSize,
        refreshLimit,
        selectionBatchSize,
        quoteBatchSize,
        snapshotBatchSize,
        maxBatches,
        recompute: !skipRecompute
      },
      null,
      2
    )
  )
  const diagnostics = await upstreamMarketFreshnessRecoveryService.runFreshnessRecovery({
    categories: categories.length ? categories : undefined,
    limit: refreshLimit,
    selectionBatchSize,
    quoteBatchSize,
    snapshotBatchSize,
    maxBatches,
    resumeState,
    startCategory,
    startOffset,
    targetUniverseSize,
    recompute: !skipRecompute,
    logProgress(event = {}) {
      console.log(
        JSON.stringify({
          step: "recovery_progress",
          ...event
        })
      )
    }
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

  if (diagnostics?.timedOut || diagnostics?.failedStage) {
    process.exitCode = 1
    return
  }

  if (
    skipScan ||
    !diagnostics?.catalogRecompute?.executed ||
    !diagnostics?.catalogRecompute?.opportunityScanSafeToResume
  ) {
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

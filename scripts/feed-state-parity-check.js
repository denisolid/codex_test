const globalActiveOpportunityRepo = require("../src/repositories/globalActiveOpportunityRepository")
const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")

function normalizeText(value) {
  return String(value || "").trim().toLowerCase()
}

function normalizeNumber(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return null
  return Number(parsed.toFixed(4))
}

function comparableRow(row = {}) {
  return {
    live_status: normalizeText(row.live_status),
    refresh_status: normalizeText(row.refresh_status),
    buy_price: normalizeNumber(row.buy_price),
    sell_net: normalizeNumber(row.sell_net),
    profit: normalizeNumber(row.profit),
    spread_pct: normalizeNumber(row.spread_pct),
    material_change_hash: normalizeText(row.material_change_hash)
  }
}

async function main() {
  const [activeRows, legacyRows] = await Promise.all([
    globalActiveOpportunityRepo.listRowsForRevalidation({ limit: 5000 }),
    arbitrageFeedRepo.listFeed({ includeInactive: false, limit: 5000 })
  ])

  const activeByFingerprint = {}
  for (const row of activeRows) {
    const fingerprint = normalizeText(row.opportunity_fingerprint)
    if (fingerprint && !activeByFingerprint[fingerprint]) {
      activeByFingerprint[fingerprint] = comparableRow(row)
    }
  }
  const legacyByFingerprint = {}
  for (const row of legacyRows) {
    const fingerprint = normalizeText(
      row.opportunity_fingerprint || row.opportunityFingerprint || row?.metadata?.opportunity_fingerprint
    )
    if (fingerprint && !legacyByFingerprint[fingerprint]) {
      legacyByFingerprint[fingerprint] = comparableRow(row)
    }
  }

  const activeFingerprints = new Set(Object.keys(activeByFingerprint))
  const legacyFingerprints = new Set(Object.keys(legacyByFingerprint))

  const missingInLegacy = Array.from(activeFingerprints).filter(
    (value) => !legacyFingerprints.has(value)
  )
  const missingInActive = Array.from(legacyFingerprints).filter(
    (value) => !activeFingerprints.has(value)
  )
  const mismatches = []
  for (const fingerprint of Array.from(activeFingerprints)) {
    if (!legacyFingerprints.has(fingerprint)) continue
    const activeRow = activeByFingerprint[fingerprint]
    const legacyRow = legacyByFingerprint[fingerprint]
    const differentFields = Object.keys(activeRow).filter((key) => activeRow[key] !== legacyRow[key])
    if (!differentFields.length) continue
    mismatches.push({
      fingerprint,
      differentFields,
      active: activeRow,
      legacy: legacyRow
    })
  }

  console.log(
    JSON.stringify(
      {
        activeVisibleCount: activeFingerprints.size,
        legacyVisibleCount: legacyFingerprints.size,
        missingInLegacyCount: missingInLegacy.length,
        missingInActiveCount: missingInActive.length,
        mismatchCount: mismatches.length,
        missingInLegacy: missingInLegacy.slice(0, 20),
        missingInActive: missingInActive.slice(0, 20),
        mismatches: mismatches.slice(0, 20)
      },
      null,
      2
    )
  )
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

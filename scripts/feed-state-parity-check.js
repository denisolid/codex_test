const globalActiveOpportunityRepo = require("../src/repositories/globalActiveOpportunityRepository")
const arbitrageFeedRepo = require("../src/repositories/arbitrageFeedRepository")

function normalizeText(value) {
  return String(value || "").trim().toLowerCase()
}

async function main() {
  const [activeRows, legacyRows] = await Promise.all([
    globalActiveOpportunityRepo.listRowsForRevalidation({ limit: 5000 }),
    arbitrageFeedRepo.listFeed({ includeInactive: false, limit: 5000 })
  ])

  const activeFingerprints = new Set(
    activeRows
      .map((row) => normalizeText(row.opportunity_fingerprint))
      .filter(Boolean)
  )
  const legacyFingerprints = new Set(
    legacyRows
      .map((row) =>
        normalizeText(
          row.opportunity_fingerprint || row.opportunityFingerprint || row?.metadata?.opportunity_fingerprint
        )
      )
      .filter(Boolean)
  )

  const missingInLegacy = Array.from(activeFingerprints).filter(
    (value) => !legacyFingerprints.has(value)
  )
  const missingInActive = Array.from(legacyFingerprints).filter(
    (value) => !activeFingerprints.has(value)
  )

  console.log(
    JSON.stringify(
      {
        activeVisibleCount: activeFingerprints.size,
        legacyVisibleCount: legacyFingerprints.size,
        missingInLegacyCount: missingInLegacy.length,
        missingInActiveCount: missingInActive.length,
        missingInLegacy: missingInLegacy.slice(0, 20),
        missingInActive: missingInActive.slice(0, 20)
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

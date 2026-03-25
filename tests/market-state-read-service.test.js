const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const marketQuoteRepo = require("../src/repositories/marketQuoteRepository")
const marketSnapshotRepo = require("../src/repositories/marketSnapshotRepository")
const marketStateReadService = require("../src/services/marketStateReadService")

test("market state read service proxies quote coverage and snapshot reads", async () => {
  const originals = {
    getLatestRowsByItemNames: marketQuoteRepo.getLatestRowsByItemNames,
    getLatestCoverageByItemNames: marketQuoteRepo.getLatestCoverageByItemNames,
    getLatestBySkinIds: marketSnapshotRepo.getLatestBySkinIds
  }

  const calls = []
  marketQuoteRepo.getLatestRowsByItemNames = async (itemNames, options) => {
    calls.push({ type: "quotes", itemNames, options })
    return { "AK-47 | Redline (Field-Tested)": { steam: { best_buy: 10 } } }
  }
  marketQuoteRepo.getLatestCoverageByItemNames = async (itemNames, options) => {
    calls.push({ type: "coverage", itemNames, options })
    return { "AK-47 | Redline (Field-Tested)": { marketCoverageCount: 2 } }
  }
  marketSnapshotRepo.getLatestBySkinIds = async (skinIds) => {
    calls.push({ type: "snapshots", skinIds })
    return { 11: { skin_id: 11, captured_at: "2026-03-25T12:00:00.000Z" } }
  }

  try {
    const quotes = await marketStateReadService.getLatestQuotesByItemNames({
      itemNames: ["AK-47 | Redline (Field-Tested)"],
      lookbackHours: 48,
      includeQualityFlags: true
    })
    const coverage = await marketStateReadService.getLatestQuoteCoverageByItemNames({
      itemNames: ["AK-47 | Redline (Field-Tested)"],
      lookbackHours: 24
    })
    const snapshots = await marketStateReadService.getLatestSnapshotsBySkinIds({
      skinIds: [11]
    })

    assert.equal(quotes["AK-47 | Redline (Field-Tested)"].steam.best_buy, 10)
    assert.equal(coverage["AK-47 | Redline (Field-Tested)"].marketCoverageCount, 2)
    assert.equal(snapshots[11].skin_id, 11)
    assert.deepEqual(calls, [
      {
        type: "quotes",
        itemNames: ["AK-47 | Redline (Field-Tested)"],
        options: { lookbackHours: 48, includeQualityFlags: true }
      },
      {
        type: "coverage",
        itemNames: ["AK-47 | Redline (Field-Tested)"],
        options: { lookbackHours: 24 }
      },
      {
        type: "snapshots",
        skinIds: [11]
      }
    ])
  } finally {
    marketQuoteRepo.getLatestRowsByItemNames = originals.getLatestRowsByItemNames
    marketQuoteRepo.getLatestCoverageByItemNames = originals.getLatestCoverageByItemNames
    marketSnapshotRepo.getLatestBySkinIds = originals.getLatestBySkinIds
  }
})

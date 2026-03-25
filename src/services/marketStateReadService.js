const marketQuoteRepo = require("../repositories/marketQuoteRepository")
const marketSnapshotRepo = require("../repositories/marketSnapshotRepository")

async function getLatestQuotesByItemNames({
  itemNames,
  lookbackHours = 72,
  includeQualityFlags = true
} = {}) {
  return marketQuoteRepo.getLatestRowsByItemNames(itemNames, {
    lookbackHours,
    includeQualityFlags
  })
}

async function getLatestQuoteCoverageByItemNames({
  itemNames,
  lookbackHours = 72
} = {}) {
  return marketQuoteRepo.getLatestCoverageByItemNames(itemNames, {
    lookbackHours
  })
}

async function getLatestSnapshotsBySkinIds({ skinIds } = {}) {
  return marketSnapshotRepo.getLatestBySkinIds(skinIds)
}

module.exports = {
  getLatestQuotesByItemNames,
  getLatestQuoteCoverageByItemNames,
  getLatestSnapshotsBySkinIds
}

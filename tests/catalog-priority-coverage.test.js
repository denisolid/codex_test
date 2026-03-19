const test = require("node:test")
const assert = require("node:assert/strict")

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co"
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon"
process.env.SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role"

const priorityConfig = require("../src/config/catalogPriorityCoverageSet")
const {
  buildPriorityEntries,
  canonicalizeItemName,
  buildPriorityKey
} = require("../src/services/catalogPriorityCoverageService")

test("priority set build keeps metadata and tier coverage stable", () => {
  const built = buildPriorityEntries(priorityConfig)
  assert.equal(built.setName, "skinalpha_priority_coverage_set")
  assert.equal(built.version, 1)
  assert.equal(Array.isArray(built.entries), true)
  assert.equal(built.entries.length >= 100, true)

  const tierNames = new Set(built.entries.map((entry) => entry.tier))
  assert.equal(tierNames.has("tier_a"), true)
  assert.equal(tierNames.has("tier_b"), true)
})

test("priority key canonicalization collapses diacritics and wear variants", () => {
  const withDiacritics = buildPriorityKey("skin", "Desert Eagle | Emerald Jörmungandr")
  const asciiOnly = buildPriorityKey("weapon_skin", "Desert Eagle | Emerald Jormungandr")
  assert.equal(withDiacritics, asciiOnly)

  assert.equal(
    canonicalizeItemName("StatTrak™ AK-47 | Redline (Field-Tested)", "weapon_skin"),
    "AK-47 | Redline"
  )
  assert.equal(canonicalizeItemName("★ Karambit | Fade (Factory New)", "knife"), "Karambit | Fade")
})

test("tier a receives stronger priority boost than tier b at equal rank", () => {
  const built = buildPriorityEntries(priorityConfig)
  const tierA = built.entries.find((entry) => entry.tier === "tier_a" && entry.rank === 1)
  const tierB = built.entries.find((entry) => entry.tier === "tier_b" && entry.rank === 1)
  assert.equal(Boolean(tierA), true)
  assert.equal(Boolean(tierB), true)
  assert.equal(Number(tierA.priorityBoost) > Number(tierB.priorityBoost), true)
})

const test = require("node:test")
const assert = require("node:assert/strict")

const {
  CANONICAL_RARITY,
  canonicalRarityToDisplay,
  getCanonicalRarityColor,
  resolveCanonicalRarity,
  buildUnknownRarityDiagnostics
} = require("../src/utils/rarityResolver")

test("resolver prioritizes catalog rarity over source rarity", () => {
  const resolved = resolveCanonicalRarity({
    catalogRarity: "restricted",
    sourceRarity: "Covert",
    marketHashName: "AK-47 | Redline (Field-Tested)"
  })

  assert.equal(resolved.canonicalRarity, CANONICAL_RARITY.RESTRICTED)
  assert.equal(resolved.source, "catalog_rarity")
  assert.equal(resolved.rarity, "Restricted")
})

test("resolver uses deterministic fallback for knife/glove items", () => {
  const resolved = resolveCanonicalRarity({
    catalogRarity: null,
    sourceRarity: null,
    marketHashName: "Karambit | Doppler (Factory New)"
  })

  assert.equal(resolved.canonicalRarity, CANONICAL_RARITY.KNIFE_GLOVES)
  assert.equal(resolved.source, "deterministic_fallback")
  assert.equal(resolved.rarityColor, "#f7ca63")
})

test("resolver emits diagnostics when rarity remains unknown", () => {
  const resolved = resolveCanonicalRarity({
    catalogRarity: null,
    sourceRarity: null,
    category: "weapon_skin",
    marketHashName: "P250 | Sand Dune (Field-Tested)"
  })

  assert.equal(resolved.canonicalRarity, CANONICAL_RARITY.UNKNOWN)
  assert.equal(resolved.source, "unknown")
  const diagnostics = buildUnknownRarityDiagnostics(resolved, {
    marketHashName: "P250 | Sand Dune (Field-Tested)"
  })
  assert.equal(Boolean(diagnostics?.reason), true)
})

test("canonical display and color maps stay stable", () => {
  assert.equal(canonicalRarityToDisplay(CANONICAL_RARITY.CLASSIFIED), "Classified")
  assert.equal(getCanonicalRarityColor(CANONICAL_RARITY.CLASSIFIED), "#d32ce6")
  assert.equal(canonicalRarityToDisplay("unknown"), "Unknown")
  assert.equal(getCanonicalRarityColor("unknown"), "#8a93a3")
})

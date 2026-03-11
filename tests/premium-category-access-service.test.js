const test = require("node:test")
const assert = require("node:assert/strict")

const premiumAccess = require("../src/services/premiumCategoryAccessService")

test("applyPremiumPreviewLock redacts knife rows for free access", () => {
  const result = premiumAccess.applyPremiumPreviewLock(
    [
      {
        itemName: "★ Karambit | Doppler (Factory New)",
        itemCategory: "knife",
        buyMarket: "steam",
        buyPrice: 1200,
        sellMarket: "skinport",
        sellNet: 1320,
        profit: 120,
        spread: 10,
        score: 82
      }
    ],
    { premiumCategoryAccess: false }
  )

  assert.equal(result.lockedCount, 1)
  assert.equal(result.rows[0].isLockedPreview, true)
  assert.equal(result.rows[0].buyPrice, null)
  assert.equal(result.rows[0].sellNet, null)
  assert.equal(result.rows[0].profit, null)
  assert.equal(result.rows[0].score, null)
  assert.equal(result.rows[0].lockReason, "premium_category")
  assert.equal(result.rows[0].premiumCategory, "knife")
  assert.equal(result.rows[0].badges.includes("LOCKED"), true)
})

test("applyPremiumPreviewLock keeps premium rows unlocked for full access", () => {
  const row = {
    itemName: "Sport Gloves | Pandora's Box (Field-Tested)",
    itemCategory: "glove",
    buyPrice: 1500
  }

  const result = premiumAccess.applyPremiumPreviewLock([row], { premiumCategoryAccess: true })
  assert.equal(result.lockedCount, 0)
  assert.equal(result.rows[0].isLockedPreview, false)
  assert.equal(result.rows[0].buyPrice, 1500)
})

test("assertPremiumCategoryAccess enforces knives/gloves for free", () => {
  assert.throws(
    () =>
      premiumAccess.assertPremiumCategoryAccess({
        entitlements: { premiumCategoryAccess: false },
        itemCategory: "knife"
      }),
    /Unlock knife and glove opportunities with Full Access/
  )

  assert.equal(
    premiumAccess.assertPremiumCategoryAccess({
      entitlements: { premiumCategoryAccess: true },
      itemCategory: "knife"
    }),
    "knife"
  )
})

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  __testables: { classifyDescription, parseMarketHashName }
} = require("../src/services/steamInventoryService");

test("classifyDescription includes marketable non-weapon items", () => {
  const sticker = {
    marketable: 1,
    market_hash_name: "Sticker | Dragon Lore (Holo)",
    tags: [{ category: "Type", localized_tag_name: "Sticker" }]
  };
  const caseItem = {
    marketable: 1,
    market_hash_name: "Revolution Case",
    tags: []
  };

  assert.deepEqual(classifyDescription(sticker), { include: true, reason: null });
  assert.deepEqual(classifyDescription(caseItem), { include: true, reason: null });
});

test("classifyDescription excludes non-marketable or malformed items", () => {
  const notMarketable = {
    marketable: 0,
    market_hash_name: "AK-47 | Redline (Field-Tested)"
  };
  const missingHash = {
    marketable: 1
  };

  assert.deepEqual(classifyDescription(notMarketable), {
    include: false,
    reason: "not-marketable"
  });
  assert.deepEqual(classifyDescription(missingHash), {
    include: false,
    reason: "missing-market-hash-name"
  });
});

test("parseMarketHashName handles weapon and non-weapon formats", () => {
  const weaponItem = parseMarketHashName(
    "AK-47 | Redline (Field-Tested)",
    "Rifle"
  );
  const caseItem = parseMarketHashName("Revolution Case", "Container");

  assert.equal(weaponItem.weapon, "AK-47");
  assert.equal(weaponItem.skinName, "Redline");
  assert.equal(weaponItem.exterior, "Field-Tested");

  assert.equal(caseItem.weapon, "Container");
  assert.equal(caseItem.skinName, "Revolution Case");
  assert.equal(caseItem.exterior, null);
});

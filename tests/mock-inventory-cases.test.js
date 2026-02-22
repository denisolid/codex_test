const test = require("node:test");
const assert = require("node:assert/strict");

const mockSteamService = require("../src/services/mockSteamService");

test("mock inventory includes multiple case items", async () => {
  const items = await mockSteamService.fetchInventory("76561198000000000");
  const caseItems = items.filter((x) => /case/i.test(String(x.marketHashName || "")));

  assert.ok(caseItems.length >= 3);
  assert.ok(caseItems.some((x) => x.marketHashName === "Revolution Case"));
  assert.ok(caseItems.some((x) => x.marketHashName === "Fracture Case"));
  assert.ok(caseItems.some((x) => x.marketHashName === "Prisma 2 Case"));
});

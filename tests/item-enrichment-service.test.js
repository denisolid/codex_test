const test = require("node:test");
const assert = require("node:assert/strict");

process.env.SUPABASE_URL = process.env.SUPABASE_URL || "https://example.supabase.co";
process.env.SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "anon";
process.env.SUPABASE_SERVICE_ROLE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || "service-role";

const {
  enrichInventoryItems,
  getRarityColor,
  normalizeRarityName,
  __testables: { buildSteamImageUrlFromIcon }
} = require("../src/services/itemEnrichmentService");

test("normalizeRarityName handles aliases and knife special-case", () => {
  assert.equal(normalizeRarityName("Remarkable"), "Restricted");
  assert.equal(
    normalizeRarityName("Covert", "Karambit | Doppler (Factory New)", "Karambit"),
    "Knife/Gloves"
  );
  assert.equal(getRarityColor("Covert"), "#eb4b4b");
});

test("enrichInventoryItems reuses fresh metadata from cache", () => {
  const nowIso = new Date().toISOString();
  const existingSkinByName = {
    "AK-47 | Redline (Field-Tested)": {
      market_hash_name: "AK-47 | Redline (Field-Tested)",
      image_url: "https://community.akamai.steamstatic.com/economy/image/icon123",
      image_url_large:
        "https://community.akamai.steamstatic.com/economy/image/icon123/512fx512f",
      rarity: "Classified",
      rarity_color: "#d32ce6",
      updated_at: nowIso
    }
  };

  const { enrichedItems, skinRows } = enrichInventoryItems(
    [
      {
        marketHashName: "AK-47 | Redline (Field-Tested)",
        weapon: "AK-47",
        skinName: "Redline",
        exterior: "Field-Tested",
        rarity: "Consumer Grade",
        quantity: 1,
        iconUrl: "different"
      }
    ],
    existingSkinByName
  );

  assert.equal(enrichedItems[0].rarity, "Classified");
  assert.equal(enrichedItems[0].rarityColor, "#d32ce6");
  assert.equal(
    enrichedItems[0].imageUrl,
    "https://community.akamai.steamstatic.com/economy/image/icon123"
  );
  assert.equal(skinRows[0].rarity_color, "#d32ce6");
});

test("enrichInventoryItems refreshes stale entries and derives steam image from icon", () => {
  const staleIso = new Date(Date.now() - 20 * 24 * 60 * 60 * 1000).toISOString();
  const iconUrl = "3f6de9f5f2d80f4f94dbacae31f9f2cf4f4cc27a";

  const { enrichedItems, skinRows } = enrichInventoryItems(
    [
      {
        marketHashName: "AWP | Asiimov (Battle-Scarred)",
        weapon: "AWP",
        skinName: "Asiimov",
        exterior: "Battle-Scarred",
        rarity: "Exotic",
        iconUrl,
        quantity: 1
      }
    ],
    {
      "AWP | Asiimov (Battle-Scarred)": {
        market_hash_name: "AWP | Asiimov (Battle-Scarred)",
        image_url: "https://old.example/image.png",
        rarity: "Consumer Grade",
        rarity_color: "#b0c3d9",
        updated_at: staleIso
      }
    }
  );

  assert.equal(
    enrichedItems[0].imageUrl,
    buildSteamImageUrlFromIcon(iconUrl)
  );
  assert.equal(enrichedItems[0].imageUrlLarge.endsWith("/512fx512f"), true);
  assert.equal(enrichedItems[0].rarity, "Classified");
  assert.equal(skinRows[0].rarity_color, "#d32ce6");
});

test("enrichInventoryItems uses placeholder when no image is available", () => {
  const { enrichedItems } = enrichInventoryItems([
    {
      marketHashName: "Fracture Case",
      weapon: "Container",
      skinName: "Fracture Case",
      rarity: "Base Grade",
      quantity: 1
    }
  ]);

  assert.match(enrichedItems[0].imageUrl, /^https:\/\//);
  assert.equal(enrichedItems[0].rarity, "Consumer Grade");
});

test("enrichInventoryItems refreshes fresh rows with known bad image hosts", () => {
  const nowIso = new Date().toISOString();
  const iconUrl = "3f6de9f5f2d80f4f94dbacae31f9f2cf4f4cc27a";

  const { enrichedItems } = enrichInventoryItems(
    [
      {
        marketHashName: "Revolution Case",
        weapon: "Container",
        skinName: "Revolution Case",
        rarity: "Base Grade",
        iconUrl,
        quantity: 1
      }
    ],
    {
      "Revolution Case": {
        market_hash_name: "Revolution Case",
        image_url: "https://example.com/revolution-case.png",
        image_url_large: "https://example.com/revolution-case-large.png",
        rarity: "Consumer Grade",
        rarity_color: "#b0c3d9",
        updated_at: nowIso
      }
    }
  );

  assert.equal(
    enrichedItems[0].imageUrl,
    buildSteamImageUrlFromIcon(iconUrl)
  );
});

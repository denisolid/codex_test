const test = require("node:test");
const assert = require("node:assert/strict");

const {
  pickImageFromMarketRow,
  __testables: {
    normalizeNameKey,
    buildSearchQueries,
    buildSteamImageUrlFromIcon,
    resolveImageFromCandidate,
    pickSteamSearchResult,
    resolveRarityFromSearchResult
  }
} = require("../src/services/marketImageService");

test("market image helpers normalize names and build steam image urls", () => {
  assert.equal(
    normalizeNameKey("AWP | Neo-Noir (Field-Tested)\u2122"),
    "awp | neo-noir (field-tested)"
  );
  assert.equal(
    buildSteamImageUrlFromIcon("abc123", "512fx512f"),
    "https://community.akamai.steamstatic.com/economy/image/abc123/512fx512f"
  );
});

test("image resolver extracts direct and icon-based image candidates", () => {
  const fromIcon = resolveImageFromCandidate({
    asset_description: {
      icon_url: "icon_hash_1"
    }
  });
  assert.equal(
    fromIcon.imageUrl,
    "https://community.akamai.steamstatic.com/economy/image/icon_hash_1/360fx360f"
  );
  assert.equal(
    fromIcon.imageUrlLarge,
    "https://community.akamai.steamstatic.com/economy/image/icon_hash_1/512fx512f"
  );

  const fromDirect = resolveImageFromCandidate({
    image_url: "https://example-cdn.com/image.png"
  });
  assert.equal(fromDirect.imageUrl, "https://example-cdn.com/image.png");
  assert.equal(fromDirect.imageUrlLarge, "https://example-cdn.com/image.png");
});

test("steam search result picker prefers exact hash name matches", () => {
  const payload = {
    results: [
      { hash_name: "AWP | Neo-Noir (Minimal Wear)" },
      { hash_name: "AWP | Neo-Noir (Field-Tested)" }
    ]
  };
  const result = pickSteamSearchResult(payload, "AWP | Neo-Noir (Field-Tested)");
  assert.equal(result.hash_name, "AWP | Neo-Noir (Field-Tested)");
});

test("steam search helper builds fallback queries without wear suffix", () => {
  const queries = buildSearchQueries("StatTrak™ Desert Eagle | Printstream (Field-Tested)");
  assert.equal(
    queries.includes("StatTrak™ Desert Eagle | Printstream (Field-Tested)"),
    true
  );
  assert.equal(queries.includes("StatTrak™ Desert Eagle | Printstream"), true);
  assert.equal(queries.includes("Desert Eagle | Printstream"), true);
});

test("rarity resolver extracts rarity and color from steam search result", () => {
  const rarity = resolveRarityFromSearchResult(
    {
      asset_description: {
        type: "Covert Pistol",
        name_color: "eb4b4b"
      }
    },
    "Desert Eagle | Printstream (Field-Tested)"
  );
  assert.equal(rarity.rarity, "Covert");
  assert.equal(rarity.canonicalRarity, "covert");
  assert.equal(rarity.rarityColor, "#eb4b4b");
});

test("market row image picker reads nested raw payload image metadata", () => {
  const image = pickImageFromMarketRow({
    raw: {
      asset_description: {
        icon_url: "nested_icon_hash"
      }
    }
  });

  assert.equal(
    image.imageUrl,
    "https://community.akamai.steamstatic.com/economy/image/nested_icon_hash/360fx360f"
  );
});

function seededOffset(steamId64, mod) {
  let acc = 0;
  for (let i = 0; i < steamId64.length; i += 1) {
    acc += steamId64.charCodeAt(i);
  }
  return acc % mod;
}

exports.fetchInventory = async (steamId64) => {
  const k = seededOffset(steamId64, 7);

  return [
    {
      marketHashName: "AK-47 | Redline (Field-Tested)",
      weapon: "AK-47",
      skinName: "Redline",
      exterior: "Field-Tested",
      rarity: "Classified",
      imageUrl: "https://example.com/ak-redline.png",
      quantity: 1 + (k % 2),
      steamItemIds: [`mock-ak-${k}`],
      price: 22.5 + k
    },
    {
      marketHashName: "AWP | Asiimov (Battle-Scarred)",
      weapon: "AWP",
      skinName: "Asiimov",
      exterior: "Battle-Scarred",
      rarity: "Covert",
      imageUrl: "https://example.com/awp-asiimov.png",
      quantity: 1,
      steamItemIds: [`mock-awp-${k}`],
      price: 96 + k
    },
    {
      marketHashName: "USP-S | Printstream (Minimal Wear)",
      weapon: "USP-S",
      skinName: "Printstream",
      exterior: "Minimal Wear",
      rarity: "Covert",
      imageUrl: "https://example.com/usp-printstream.png",
      quantity: 2,
      steamItemIds: [`mock-usp-${k}-1`, `mock-usp-${k}-2`],
      price: 41 + k
    },
    {
      marketHashName: "Sticker | Dragon Lore (Holo)",
      weapon: "Sticker",
      skinName: "Dragon Lore",
      exterior: "Holo",
      rarity: "Remarkable",
      imageUrl: "https://example.com/sticker-dragon-lore-holo.png",
      quantity: 1,
      steamItemIds: [`mock-sticker-${k}`],
      price: 58 + k
    },
    {
      marketHashName: "Music Kit | Scarlxrd, CHAIN$AW.LXADXUT.",
      weapon: "Music Kit",
      skinName: "Scarlxrd, CHAIN$AW.LXADXUT.",
      exterior: null,
      rarity: "High Grade",
      imageUrl: "https://example.com/music-kit-scarlxrd.png",
      quantity: 1,
      steamItemIds: [`mock-music-${k}`],
      price: 5 + k
    },
    {
      marketHashName: "Revolution Case",
      weapon: "Container",
      skinName: "Revolution Case",
      exterior: null,
      rarity: "Base Grade",
      imageUrl: "https://example.com/revolution-case.png",
      quantity: 2,
      steamItemIds: [`mock-case-${k}-1`, `mock-case-${k}-2`],
      price: 0.65 + k * 0.03
    },
    {
      marketHashName: "Fracture Case",
      weapon: "Container",
      skinName: "Fracture Case",
      exterior: null,
      rarity: "Base Grade",
      imageUrl: "https://example.com/fracture-case.png",
      quantity: 3,
      steamItemIds: [
        `mock-fracture-case-${k}-1`,
        `mock-fracture-case-${k}-2`,
        `mock-fracture-case-${k}-3`
      ],
      price: 0.31 + k * 0.02
    },
    {
      marketHashName: "Prisma 2 Case",
      weapon: "Container",
      skinName: "Prisma 2 Case",
      exterior: null,
      rarity: "Base Grade",
      imageUrl: "https://example.com/prisma-2-case.png",
      quantity: 4,
      steamItemIds: [
        `mock-prisma2-case-${k}-1`,
        `mock-prisma2-case-${k}-2`,
        `mock-prisma2-case-${k}-3`,
        `mock-prisma2-case-${k}-4`
      ],
      price: 0.39 + k * 0.02
    },
    {
      marketHashName: "Operation Breakout Case Key",
      weapon: "Key",
      skinName: "Operation Breakout Case Key",
      exterior: null,
      rarity: "Base Grade",
      imageUrl: "https://example.com/case-key.png",
      quantity: 1,
      steamItemIds: [`mock-key-${k}`],
      price: 8.1 + k * 0.2
    }
  ];
};

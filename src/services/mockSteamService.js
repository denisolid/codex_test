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
      price: 41 + k
    }
  ];
};

const marketUniverseTop100 = require("./marketUniverseTop100.json")

const WEAR_STATES = Object.freeze([
  "Factory New",
  "Minimal Wear",
  "Field-Tested",
  "Well-Worn",
  "Battle-Scarred"
])

const CORE_WEAR_STATES = Object.freeze([
  "Minimal Wear",
  "Field-Tested",
  "Well-Worn",
  "Battle-Scarred"
])

const EXTRA_WEAPON_SKIN_BASES = Object.freeze([
  "AK-47 | Aquamarine Revenge",
  "AK-47 | Frontside Misty",
  "AK-47 | Wasteland Rebel",
  "AK-47 | Orbit Mk01",
  "AK-47 | Cartel",
  "AK-47 | Blue Laminate",
  "AK-47 | Neon Revolution",
  "AK-47 | Point Disarray",
  "AK-47 | Jaguar",
  "AK-47 | Head Shot",
  "AK-47 | Legion of Anubis",
  "AWP | Atheris",
  "AWP | Exoskeleton",
  "AWP | Elite Build",
  "AWP | Corticera",
  "AWP | Worm God",
  "AWP | Phobos",
  "AWP | Graphite",
  "AWP | Electric Hive",
  "AWP | BOOM",
  "AWP | Sun in Leo",
  "AWP | Man-o'-war",
  "M4A1-S | Nightmare",
  "M4A1-S | Golden Coil",
  "M4A1-S | Basilisk",
  "M4A1-S | Cyrex",
  "M4A1-S | Atomic Alloy",
  "M4A1-S | Nitro",
  "M4A1-S | Control Panel",
  "M4A1-S | Emphorosaur-S",
  "M4A1-S | Black Lotus",
  "M4A4 | Magnesium",
  "M4A4 | Tooth Fairy",
  "M4A4 | Griffin",
  "M4A4 | Spider Lily",
  "M4A4 | Dragon King",
  "M4A4 | X-Ray",
  "M4A4 | Bullet Rain",
  "M4A4 | Poseidon",
  "M4A4 | Daybreak",
  "USP-S | Stainless",
  "USP-S | Overgrowth",
  "USP-S | Orion",
  "USP-S | Cyrex",
  "USP-S | Whiteout",
  "USP-S | Caiman",
  "USP-S | Road Rash",
  "USP-S | Dark Water",
  "USP-S | Flashback",
  "USP-S | Guardian",
  "Glock-18 | Wasteland Rebel",
  "Glock-18 | Nuclear Garden",
  "Glock-18 | Royal Legion",
  "Glock-18 | Candy Apple",
  "Glock-18 | Weasel",
  "Glock-18 | Water Elemental",
  "Glock-18 | Dragon Tattoo",
  "Glock-18 | Off World",
  "Glock-18 | Steel Disruption",
  "Desert Eagle | Light Rail",
  "Desert Eagle | Naga",
  "Desert Eagle | Bronze Deco",
  "Desert Eagle | Hypnotic",
  "Desert Eagle | Midnight Storm",
  "Desert Eagle | Emerald Jormungandr",
  "P250 | Mehndi",
  "P250 | Cartel",
  "P250 | Valence",
  "P250 | Wingshot",
  "P250 | Franklin",
  "P250 | Mehndi",
  "P250 | Digital Architect",
  "Five-SeveN | Triumvirate",
  "Five-SeveN | Boost Protocol",
  "Five-SeveN | Copper Galaxy",
  "Five-SeveN | Retrobution",
  "Five-SeveN | Case Hardened",
  "Tec-9 | Decimator",
  "Tec-9 | Fuel Injector",
  "Tec-9 | Isaac",
  "Tec-9 | Bamboo Forest",
  "Tec-9 | Remote Control",
  "MP9 | Food Chain",
  "MP9 | Starlight Protector",
  "MP9 | Mount Fuji",
  "MP9 | Hydra",
  "MP9 | Ruby Poison Dart",
  "MP9 | Hot Rod",
  "MAC-10 | Neon Rider",
  "MAC-10 | Disco Tech",
  "MAC-10 | Sakkaku",
  "MAC-10 | Heat",
  "MAC-10 | Curse",
  "UMP-45 | Primal Saber",
  "UMP-45 | Momentum",
  "UMP-45 | Plastique",
  "UMP-45 | Blaze",
  "UMP-45 | Crime Scene",
  "FAMAS | Roll Cage",
  "FAMAS | Commemoration",
  "FAMAS | Mecha Industries",
  "FAMAS | Eye of Athena",
  "FAMAS | Pulse",
  "Galil AR | Chatterbox",
  "Galil AR | Eco",
  "Galil AR | Sugar Rush",
  "Galil AR | Firefight",
  "Galil AR | Phoenix Blacklight",
  "SG 553 | Integrale",
  "SG 553 | Cyrex",
  "SG 553 | Pulse",
  "SG 553 | Colony IV",
  "SG 553 | Tiger Moth",
  "AUG | Akihabara Accept",
  "AUG | Momentum",
  "AUG | Bengal Tiger",
  "AUG | Chameleon",
  "AUG | Syd Mead",
  "P90 | Death by Kitty",
  "P90 | Trigon",
  "P90 | Emerald Dragon",
  "P90 | Nostalgia",
  "P90 | Shallow Grave",
  "SSG 08 | Blood in the Water",
  "SSG 08 | Dragonfire",
  "SSG 08 | Turbo Peek",
  "SSG 08 | Ghost Crusader",
  "SSG 08 | Big Iron",
  "CZ75-Auto | Victoria",
  "CZ75-Auto | Xiangliu",
  "CZ75-Auto | Yellow Jacket",
  "CZ75-Auto | Tuxedo",
  "CZ75-Auto | Pole Position",
  "R8 Revolver | Fade",
  "R8 Revolver | Reboot",
  "R8 Revolver | Amber Fade",
  "R8 Revolver | Crimson Web",
  "R8 Revolver | Bone Forged"
])

const EXTRA_CASES = Object.freeze([
  "CS:GO Weapon Case",
  "CS:GO Weapon Case 2",
  "CS:GO Weapon Case 3",
  "Operation Bravo Case",
  "eSports 2013 Case",
  "eSports 2013 Winter Case",
  "eSports 2014 Summer Case",
  "Operation Phoenix Weapon Case",
  "Huntsman Weapon Case",
  "Operation Breakout Weapon Case",
  "Falchion Case",
  "Shadow Case",
  "Revolver Case",
  "Operation Vanguard Weapon Case",
  "Operation Wildfire Case",
  "Chroma Case",
  "Chroma 2 Case",
  "Chroma 3 Case",
  "Gamma Case",
  "Gamma 2 Case",
  "Spectrum Case",
  "Spectrum 2 Case",
  "Glove Case",
  "Clutch Case",
  "Prisma Case",
  "Prisma 2 Case",
  "CS20 Case",
  "Danger Zone Case",
  "Horizon Case",
  "Fracture Case",
  "Snakebite Case",
  "Operation Broken Fang Case",
  "Operation Riptide Case",
  "Recoil Case",
  "Dreams & Nightmares Case",
  "Revolution Case",
  "Kilowatt Case",
  "Gallery Case",
  "Chromatic Case"
])

const EXTRA_CAPSULES = Object.freeze([
  "Katowice 2019 Legends Sticker Capsule",
  "Katowice 2019 Challengers Sticker Capsule",
  "Katowice 2019 Minor Challengers Sticker Capsule",
  "Berlin 2019 Legends Sticker Capsule",
  "Berlin 2019 Challengers Sticker Capsule",
  "Berlin 2019 Minor Challengers Sticker Capsule",
  "Stockholm 2021 Legends Sticker Capsule",
  "Stockholm 2021 Challengers Sticker Capsule",
  "Stockholm 2021 Contenders Sticker Capsule",
  "Antwerp 2022 Legends Sticker Capsule",
  "Antwerp 2022 Challengers Sticker Capsule",
  "Antwerp 2022 Contenders Sticker Capsule",
  "Rio 2022 Legends Sticker Capsule",
  "Rio 2022 Challengers Sticker Capsule",
  "Rio 2022 Contenders Sticker Capsule",
  "Paris 2023 Legends Sticker Capsule",
  "Paris 2023 Challengers Sticker Capsule",
  "Paris 2023 Contenders Sticker Capsule",
  "Copenhagen 2024 Legends Sticker Capsule",
  "Copenhagen 2024 Challengers Sticker Capsule",
  "Copenhagen 2024 Contenders Sticker Capsule",
  "Shanghai 2024 Legends Sticker Capsule",
  "Shanghai 2024 Challengers Sticker Capsule",
  "Shanghai 2024 Contenders Sticker Capsule",
  "Legends Autograph Capsule",
  "Challengers Autograph Capsule",
  "Contenders Autograph Capsule",
  "Legends Sticker Capsule",
  "Challengers Sticker Capsule",
  "Contenders Sticker Capsule"
])

function normalizeText(value) {
  return String(value || "").trim()
}

function isWearName(value) {
  return /\((Factory New|Minimal Wear|Field-Tested|Well-Worn|Battle-Scarred)\)$/i.test(
    normalizeText(value)
  )
}

function stripWear(value) {
  return normalizeText(value).replace(
    /\s+\((Factory New|Minimal Wear|Field-Tested|Well-Worn|Battle-Scarred)\)$/i,
    ""
  )
}

function inferCategory(value) {
  const name = normalizeText(value).toLowerCase()
  if (!name) return "weapon_skin"
  if (name.endsWith(" case")) return "case"
  if (name.includes("sticker capsule")) return "sticker_capsule"
  return "weapon_skin"
}

function inferSubcategory(value, category) {
  if (category === "case") return "weapon_case"
  if (category === "sticker_capsule") return "major_sticker_capsule"

  const name = normalizeText(value)
  if (name.startsWith("AK-47") || name.startsWith("M4A1-S") || name.startsWith("M4A4")) {
    return "rifle"
  }
  if (name.startsWith("AWP") || name.startsWith("SSG 08") || name.startsWith("SCAR-20")) {
    return "sniper"
  }
  if (
    name.startsWith("USP-S") ||
    name.startsWith("Glock-18") ||
    name.startsWith("Desert Eagle") ||
    name.startsWith("P250") ||
    name.startsWith("Tec-9") ||
    name.startsWith("Five-SeveN") ||
    name.startsWith("CZ75") ||
    name.startsWith("R8")
  ) {
    return "pistol"
  }
  if (
    name.startsWith("MP9") ||
    name.startsWith("MAC-10") ||
    name.startsWith("UMP-45") ||
    name.startsWith("P90")
  ) {
    return "smg"
  }
  return "weapon_skin"
}

function toCatalogRow(marketHashName, categoryHint = "") {
  const name = normalizeText(marketHashName)
  if (!name) return null
  const category = categoryHint || inferCategory(name)
  const subcategory = inferSubcategory(name, category)

  return {
    marketHashName: name,
    itemName: name,
    category,
    subcategory,
    tradable: true,
    scanEligible: true,
    isActive: true,
    sourceTag: "curated_seed"
  }
}

function collectWeaponSkinBases() {
  const fromTop100 = (Array.isArray(marketUniverseTop100) ? marketUniverseTop100 : [])
    .map((row) => normalizeText(row?.marketHashName))
    .filter((name) => inferCategory(name) === "weapon_skin")
    .map((name) => (isWearName(name) ? stripWear(name) : name))

  return Array.from(new Set([...fromTop100, ...EXTRA_WEAPON_SKIN_BASES].map(normalizeText).filter(Boolean)))
}

function buildWeaponSkinRows() {
  const rows = []
  for (const base of collectWeaponSkinBases()) {
    const wears = base.includes("| ") ? CORE_WEAR_STATES : WEAR_STATES
    for (const wear of wears) {
      rows.push(toCatalogRow(`${base} (${wear})`, "weapon_skin"))
    }
  }
  return rows.filter(Boolean)
}

function buildCaseRows() {
  const fromTop100 = (Array.isArray(marketUniverseTop100) ? marketUniverseTop100 : [])
    .map((row) => normalizeText(row?.marketHashName))
    .filter((name) => inferCategory(name) === "case")

  const names = Array.from(new Set([...fromTop100, ...EXTRA_CASES].map(normalizeText).filter(Boolean)))
  return names.map((name) => toCatalogRow(name, "case")).filter(Boolean)
}

function buildCapsuleRows() {
  const fromTop100 = (Array.isArray(marketUniverseTop100) ? marketUniverseTop100 : [])
    .map((row) => normalizeText(row?.marketHashName))
    .filter((name) => inferCategory(name) === "sticker_capsule")

  const names = Array.from(new Set([...fromTop100, ...EXTRA_CAPSULES].map(normalizeText).filter(Boolean)))
  return names.map((name) => toCatalogRow(name, "sticker_capsule")).filter(Boolean)
}

function buildSourceCatalogSeed(limit = 1000) {
  const targetLimit = Math.max(Math.round(Number(limit || 0)), 1)
  const allRows = [...buildWeaponSkinRows(), ...buildCaseRows(), ...buildCapsuleRows()].filter(Boolean)

  const deduped = []
  const seen = new Set()
  for (const row of allRows) {
    const key = normalizeText(row?.marketHashName)
    if (!key || seen.has(key)) continue
    seen.add(key)
    deduped.push(row)
    if (deduped.length >= targetLimit) break
  }

  return deduped
}

const defaultSeed = buildSourceCatalogSeed(1000)

module.exports = Object.freeze(defaultSeed)
module.exports.buildSourceCatalogSeed = buildSourceCatalogSeed
module.exports.__testables = {
  collectWeaponSkinBases,
  inferCategory,
  inferSubcategory,
  stripWear,
  isWearName
}
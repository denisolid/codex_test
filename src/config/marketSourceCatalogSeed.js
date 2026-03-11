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

const CURATED_CASES = Object.freeze([
  "Revolution Case",
  "Recoil Case",
  "Fracture Case",
  "Snakebite Case",
  "Dreams & Nightmares Case",
  "Clutch Case",
  "Prisma 2 Case",
  "Prisma Case",
  "Danger Zone Case",
  "Horizon Case",
  "CS20 Case",
  "Gamma 2 Case",
  "Gamma Case",
  "Chroma 3 Case",
  "Chroma 2 Case",
  "Chroma Case",
  "Spectrum 2 Case",
  "Spectrum Case",
  "Operation Breakout Weapon Case",
  "Operation Phoenix Weapon Case",
  "Operation Wildfire Case",
  "Shadow Case",
  "Falchion Case",
  "Glove Case",
  "Weapon Case 2",
  "Weapon Case 3",
  "CS:GO Weapon Case",
  "CS:GO Weapon Case 2",
  "eSports 2013 Case",
  "eSports 2013 Winter Case",
  "eSports 2014 Summer Case",
  "Kilowatt Case",
  "Gallery Case"
])

const CURATED_STICKER_CAPSULES = Object.freeze([
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
  "Copenhagen 2024 Contenders Sticker Capsule"
])

const CURATED_WEAPON_SKIN_BASES = Object.freeze([
  "AK-47 | Redline",
  "AK-47 | Vulcan",
  "AK-47 | Bloodsport",
  "AK-47 | Frontside Misty",
  "AK-47 | Neon Rider",
  "AK-47 | The Empress",
  "AK-47 | Aquamarine Revenge",
  "AK-47 | Asiimov",
  "AK-47 | Slate",
  "AK-47 | Ice Coaled",
  "AK-47 | Legion of Anubis",
  "AK-47 | Head Shot",
  "AK-47 | Phantom Disruptor",
  "AK-47 | Elite Build",
  "AK-47 | Case Hardened",

  "AWP | Asiimov",
  "AWP | Neo-Noir",
  "AWP | Hyper Beast",
  "AWP | Redline",
  "AWP | Wildfire",
  "AWP | Man-o'-war",
  "AWP | Mortis",
  "AWP | Fever Dream",
  "AWP | Chromatic Aberration",
  "AWP | Electric Hive",
  "AWP | Graphite",
  "AWP | BOOM",
  "AWP | Sun in Leo",
  "AWP | Duality",
  "AWP | Exoskeleton",

  "M4A1-S | Printstream",
  "M4A1-S | Hyper Beast",
  "M4A1-S | Player Two",
  "M4A1-S | Cyrex",
  "M4A1-S | Chantico's Fire",
  "M4A1-S | Nightmare",
  "M4A1-S | Decimator",
  "M4A1-S | Atomic Alloy",
  "M4A1-S | Black Lotus",
  "M4A1-S | Leaded Glass",
  "M4A1-S | Mecha Industries",

  "M4A4 | Neo-Noir",
  "M4A4 | The Emperor",
  "M4A4 | Desolate Space",
  "M4A4 | Buzz Kill",
  "M4A4 | In Living Color",
  "M4A4 | Temukau",
  "M4A4 | Cyber Security",
  "M4A4 | The Battlestar",
  "M4A4 | Evil Daimyo",
  "M4A4 | Tooth Fairy",
  "M4A4 | \u9f8d\u738b (Dragon King)",

  "USP-S | Kill Confirmed",
  "USP-S | Neo-Noir",
  "USP-S | Cortex",
  "USP-S | Printstream",
  "USP-S | Monster Mashup",
  "USP-S | The Traitor",
  "USP-S | Orion",
  "USP-S | Overgrowth",
  "USP-S | Cyrex",
  "USP-S | Jawbreaker",
  "USP-S | Black Lotus",
  "USP-S | Ticket to Hell",

  "Glock-18 | Vogue",
  "Glock-18 | Neo-Noir",
  "Glock-18 | Bullet Queen",
  "Glock-18 | Water Elemental",
  "Glock-18 | Wasteland Rebel",
  "Glock-18 | Gold Toof",
  "Glock-18 | Gamma Doppler",
  "Glock-18 | Franklin",
  "Glock-18 | Dragon Tattoo",
  "Glock-18 | Nuclear Garden",

  "Desert Eagle | Blaze",
  "Desert Eagle | Printstream",
  "Desert Eagle | Ocean Drive",
  "Desert Eagle | Code Red",
  "Desert Eagle | Kumicho Dragon",
  "Desert Eagle | Conspiracy",
  "Desert Eagle | Mecha Industries",
  "Desert Eagle | Crimson Web",
  "Desert Eagle | Light Rail",
  "Desert Eagle | Trigger Discipline",

  "P250 | Asiimov",
  "P250 | See Ya Later",
  "P250 | Visions",
  "P250 | Muertos",
  "P250 | Cyber Shell",
  "P250 | Cartel",
  "P250 | X-Ray",
  "P250 | Mehndi",

  "Five-SeveN | Fairy Tale",
  "Five-SeveN | Angry Mob",
  "Five-SeveN | Hyper Beast",
  "Five-SeveN | Monkey Business",
  "Five-SeveN | Boost Protocol",
  "Five-SeveN | Case Hardened",

  "MP9 | Starlight Protector",
  "MP9 | Hydra",
  "MP9 | Food Chain",
  "MP9 | Mount Fuji",
  "MP9 | Rose Iron",
  "MP9 | Goo",

  "MAC-10 | Neon Rider",
  "MAC-10 | Disco Tech",
  "MAC-10 | Stalker",
  "MAC-10 | Propaganda",
  "MAC-10 | Heat",
  "MAC-10 | Malachite",

  "UMP-45 | Primal Saber",
  "UMP-45 | Moonrise",
  "UMP-45 | Wild Child",
  "UMP-45 | Scaffold",
  "UMP-45 | Momentum",

  "FAMAS | Commemoration",
  "FAMAS | Eye of Athena",
  "FAMAS | Roll Cage",
  "FAMAS | Pulse",
  "FAMAS | Djinn",

  "Galil AR | Chatterbox",
  "Galil AR | Sugar Rush",
  "Galil AR | Eco",
  "Galil AR | Chromatic Aberration",
  "Galil AR | Signal",

  "SG 553 | Cyrex",
  "SG 553 | Integrale",
  "SG 553 | Pulse",
  "SG 553 | Darkwing",

  "AUG | Chameleon",
  "AUG | Syd Mead",
  "AUG | Momentum",
  "AUG | Arctic Wolf",
  "AUG | Akihabara Accept",

  "P90 | Asiimov",
  "P90 | Death by Kitty",
  "P90 | Trigon",
  "P90 | Shapewood",
  "P90 | Nostalgia",

  "CZ75-Auto | Xiangliu",
  "CZ75-Auto | Yellow Jacket",
  "CZ75-Auto | Victoria",
  "CZ75-Auto | Tacticat",

  "Tec-9 | Fuel Injector",
  "Tec-9 | Decimator",
  "Tec-9 | Remote Control",
  "Tec-9 | Isaac",
  "Tec-9 | Rebel",

  "MP7 | Bloodsport",
  "MP7 | Nemesis",
  "MP7 | Abyssal Apparition",
  "MP7 | Fade",
  "MP7 | Whiteout",

  "XM1014 | Entombed",
  "XM1014 | Incinegator",
  "XM1014 | Seasons",
  "XM1014 | XOXO",

  "Nova | Hyper Beast",
  "Nova | Antique",
  "Nova | Bloomstick",
  "Nova | Toy Soldier",

  "MAG-7 | Justice",
  "MAG-7 | Monster Call",
  "MAG-7 | BI83 Spectrum",
  "MAG-7 | Cinquedea"
])

const CURATED_KNIFE_SEEDS = Object.freeze([
  "Karambit | Doppler",
  "Karambit | Gamma Doppler",
  "Karambit | Fade",
  "Karambit | Tiger Tooth",
  "Karambit | Slaughter",
  "Karambit | Marble Fade",
  "Karambit | Lore",
  "Karambit | Autotronic",
  "Karambit | Black Laminate",
  "Karambit | Damascus Steel",

  "M9 Bayonet | Doppler",
  "M9 Bayonet | Gamma Doppler",
  "M9 Bayonet | Fade",
  "M9 Bayonet | Tiger Tooth",
  "M9 Bayonet | Slaughter",
  "M9 Bayonet | Marble Fade",
  "M9 Bayonet | Lore",
  "M9 Bayonet | Autotronic",
  "M9 Bayonet | Freehand",
  "M9 Bayonet | Damascus Steel",

  "Bayonet | Doppler",
  "Bayonet | Gamma Doppler",
  "Bayonet | Fade",
  "Bayonet | Tiger Tooth",
  "Bayonet | Marble Fade",
  "Bayonet | Lore",
  "Bayonet | Autotronic",
  "Bayonet | Black Laminate",

  "Butterfly Knife | Doppler",
  "Butterfly Knife | Gamma Doppler",
  "Butterfly Knife | Fade",
  "Butterfly Knife | Tiger Tooth",
  "Butterfly Knife | Marble Fade",
  "Butterfly Knife | Slaughter",
  "Butterfly Knife | Crimson Web",
  "Butterfly Knife | Freehand",
  "Butterfly Knife | Case Hardened",

  "Talon Knife | Doppler",
  "Talon Knife | Fade",
  "Talon Knife | Tiger Tooth",
  "Talon Knife | Marble Fade",
  "Talon Knife | Slaughter",
  "Talon Knife | Damascus Steel",

  "Flip Knife | Doppler",
  "Flip Knife | Fade",
  "Flip Knife | Tiger Tooth",
  "Flip Knife | Marble Fade",
  "Flip Knife | Lore",
  "Flip Knife | Autotronic",

  "Huntsman Knife | Doppler",
  "Huntsman Knife | Fade",
  "Huntsman Knife | Tiger Tooth",
  "Huntsman Knife | Marble Fade",
  "Huntsman Knife | Crimson Web",

  "Skeleton Knife | Fade",
  "Skeleton Knife | Crimson Web",
  "Skeleton Knife | Slaughter",
  "Skeleton Knife | Case Hardened",

  "Nomad Knife | Fade",
  "Nomad Knife | Slaughter",
  "Nomad Knife | Case Hardened",

  "Stiletto Knife | Doppler",
  "Stiletto Knife | Fade",
  "Stiletto Knife | Marble Fade",
  "Stiletto Knife | Tiger Tooth",

  "Bowie Knife | Doppler",
  "Bowie Knife | Fade",
  "Bowie Knife | Tiger Tooth",
  "Bowie Knife | Marble Fade",

  "Ursus Knife | Doppler",
  "Falchion Knife | Doppler",
  "Paracord Knife | Fade",
  "Survival Knife | Fade",
  "Gut Knife | Doppler",
  "Navaja Knife | Doppler",
  "Shadow Daggers | Doppler"
])

const CURATED_GLOVE_SEEDS = Object.freeze([
  "Sport Gloves | Pandora's Box",
  "Sport Gloves | Vice",
  "Sport Gloves | Hedge Maze",
  "Sport Gloves | Omega",
  "Sport Gloves | Amphibious",
  "Sport Gloves | Nocts",
  "Sport Gloves | Arid",
  "Sport Gloves | Scarlet Shamagh",
  "Sport Gloves | Slingshot",
  "Sport Gloves | Big Game",

  "Specialist Gloves | Crimson Kimono",
  "Specialist Gloves | Emerald Web",
  "Specialist Gloves | Foundation",
  "Specialist Gloves | Mogul",
  "Specialist Gloves | Marble Fade",
  "Specialist Gloves | Fade",
  "Specialist Gloves | Forest DDPAT",

  "Moto Gloves | Spearmint",
  "Moto Gloves | Cool Mint",
  "Moto Gloves | Boom!",
  "Moto Gloves | Polygon",
  "Moto Gloves | Eclipse",
  "Moto Gloves | Smoke Out",
  "Moto Gloves | Blood Pressure",
  "Moto Gloves | Transport",

  "Driver Gloves | King Snake",
  "Driver Gloves | Imperial Plaid",
  "Driver Gloves | Snow Leopard",
  "Driver Gloves | Rezan the Red",
  "Driver Gloves | Lunar Weave",
  "Driver Gloves | Overtake",
  "Driver Gloves | Diamondback",
  "Driver Gloves | Queen Jaguar",

  "Hand Wraps | Cobalt Skulls",
  "Hand Wraps | Slaughter",
  "Hand Wraps | Overprint",
  "Hand Wraps | Arboreal",
  "Hand Wraps | Desert Shamagh",
  "Hand Wraps | Duct Tape",

  "Hydra Gloves | Case Hardened",
  "Hydra Gloves | Emerald",
  "Hydra Gloves | Mangrove",
  "Hydra Gloves | Rattler",

  "Bloodhound Gloves | Charred",
  "Bloodhound Gloves | Guerrilla",
  "Bloodhound Gloves | Bronzed",
  "Bloodhound Gloves | Snakebite",

  "Broken Fang Gloves | Jade",
  "Broken Fang Gloves | Yellow-banded",
  "Broken Fang Gloves | Unhinged",
  "Broken Fang Gloves | Needle Point"
])

const CATEGORY_ORDER = Object.freeze([
  "weapon_skin",
  "case",
  "sticker_capsule"
])

const CATEGORY_SEED_TARGETS = Object.freeze({
  weapon_skin: 4400,
  case: 350,
  sticker_capsule: 250
})

const CATEGORY_SEED_BASE_TOTAL = Object.values(CATEGORY_SEED_TARGETS).reduce(
  (sum, value) => sum + Number(value || 0),
  0
)

function normalizeText(value) {
  return String(value || "").trim()
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
  if (category === "knife" || category === "glove") {
    const family = normalizeText(value)
      .replace(/^★\s*/, "")
      .split("|")[0]
    return family
      .trim()
      .toLowerCase()
      .replace(/[^\w\s-]/g, "")
      .replace(/\s+/g, "_")
  }

  const name = normalizeText(value)
  if (name.startsWith("AK-47") || name.startsWith("M4A1-S") || name.startsWith("M4A4") || name.startsWith("FAMAS") || name.startsWith("Galil AR") || name.startsWith("SG 553") || name.startsWith("AUG")) {
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
    name.startsWith("P90") ||
    name.startsWith("MP7")
  ) {
    return "smg"
  }
  if (name.startsWith("XM1014") || name.startsWith("Nova") || name.startsWith("MAG-7")) {
    return "shotgun"
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

function buildWeaponSkinRows() {
  const rows = []
  for (const base of CURATED_WEAPON_SKIN_BASES) {
    const name = normalizeText(base)
    if (!name) continue
    const wears = name.includes("| ") ? WEAR_STATES : CORE_WEAR_STATES
    for (const wear of wears) {
      rows.push(toCatalogRow(`${name} (${wear})`, "weapon_skin"))
      rows.push(toCatalogRow(`StatTrak\u2122 ${name} (${wear})`, "weapon_skin"))
    }
    for (const wear of ["Minimal Wear", "Field-Tested", "Well-Worn"]) {
      rows.push(toCatalogRow(`Souvenir ${name} (${wear})`, "weapon_skin"))
    }
  }
  return rows.filter(Boolean)
}

function buildCaseRows() {
  return CURATED_CASES.map((name) => toCatalogRow(name, "case")).filter(Boolean)
}

function buildCapsuleRows() {
  return CURATED_STICKER_CAPSULES.map((name) => toCatalogRow(name, "sticker_capsule")).filter(Boolean)
}

function buildKnifeRows() {
  const fnFinishes = new Set(["Doppler", "Gamma Doppler", "Tiger Tooth", "Fade", "Marble Fade"])
  const mwFinishes = new Set([
    "Slaughter",
    "Lore",
    "Autotronic",
    "Freehand",
    "Black Laminate",
    "Damascus Steel"
  ])

  const rows = CURATED_KNIFE_SEEDS.map((seed) => {
    const normalized = normalizeText(seed)
    if (!normalized) return null
    const [family, finish] = normalized.split("|").map((part) => normalizeText(part))
    if (!family || !finish) return null
    const wear = fnFinishes.has(finish)
      ? "Factory New"
      : mwFinishes.has(finish)
        ? "Minimal Wear"
        : "Field-Tested"
    return toCatalogRow(`★ ${family} | ${finish} (${wear})`, "knife")
  })

  return rows.filter(Boolean)
}

function buildGloveRows() {
  return CURATED_GLOVE_SEEDS.map((seed) =>
    toCatalogRow(`★ ${normalizeText(seed)} (Field-Tested)`, "glove")
  ).filter(Boolean)
}

function buildCategorySeedQuotas(limit = 5000) {
  const targetLimit = Math.max(Math.round(Number(limit || 0)), 1)
  const quotas = {
    weapon_skin: 0,
    case: 0,
    sticker_capsule: 0
  }

  if (!CATEGORY_SEED_BASE_TOTAL) {
    quotas.weapon_skin = targetLimit
    return quotas
  }

  const buckets = CATEGORY_ORDER.map((category) => {
    const baseTarget = Number(CATEGORY_SEED_TARGETS[category] || 0)
    const exact = (baseTarget * targetLimit) / CATEGORY_SEED_BASE_TOTAL
    const floor = Math.max(Math.floor(exact), 0)
    quotas[category] = floor
    return {
      category,
      baseTarget,
      remainder: exact - floor
    }
  })

  let remainderSlots = targetLimit - Object.values(quotas).reduce((sum, value) => sum + Number(value || 0), 0)
  if (remainderSlots > 0) {
    const byRemainder = buckets
      .slice()
      .sort((a, b) => Number(b.remainder || 0) - Number(a.remainder || 0) || Number(b.baseTarget || 0) - Number(a.baseTarget || 0))
    let index = 0
    while (remainderSlots > 0) {
      const bucket = byRemainder[index % byRemainder.length]
      quotas[bucket.category] += 1
      remainderSlots -= 1
      index += 1
    }
  }

  return quotas
}

function buildSourceCatalogSeed(limit = 5000) {
  const targetLimit = Math.max(Math.round(Number(limit || 0)), 1)
  const byCategory = {
    weapon_skin: buildWeaponSkinRows(),
    case: buildCaseRows(),
    sticker_capsule: buildCapsuleRows()
  }
  const quotas = buildCategorySeedQuotas(targetLimit)
  const selected = []
  const seen = new Set()

  for (const category of CATEGORY_ORDER) {
    const categoryRows = Array.isArray(byCategory[category]) ? byCategory[category] : []
    const categoryQuota = Math.max(Number(quotas[category] || 0), 0)
    let added = 0
    for (const row of categoryRows) {
      if (added >= categoryQuota || selected.length >= targetLimit) break
      const key = normalizeText(row?.marketHashName).toLowerCase()
      if (!key || seen.has(key)) continue
      seen.add(key)
      selected.push(row)
      added += 1
    }
  }

  if (selected.length >= targetLimit) {
    return selected.slice(0, targetLimit)
  }

  const leftovers = CATEGORY_ORDER.flatMap((category) => Array.isArray(byCategory[category]) ? byCategory[category] : [])
  for (const row of leftovers) {
    if (selected.length >= targetLimit) break
    const key = normalizeText(row?.marketHashName).toLowerCase()
    if (!key || seen.has(key)) continue
    seen.add(key)
    selected.push(row)
  }

  return selected
}

const defaultSeed = buildSourceCatalogSeed(5000)

module.exports = defaultSeed
module.exports.buildSourceCatalogSeed = buildSourceCatalogSeed
module.exports.__testables = {
  inferCategory,
  inferSubcategory
}

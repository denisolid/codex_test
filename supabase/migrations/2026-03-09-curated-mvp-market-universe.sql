create table if not exists public.market_universe (
  id uuid primary key default gen_random_uuid(),
  market_hash_name text not null,
  item_name text not null,
  liquidity_rank integer not null check (liquidity_rank > 0),
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_market_universe_market_hash_name
  on public.market_universe(market_hash_name);

update public.market_universe
set
  is_active = false,
  updated_at = now()
where is_active = true;

with curated(market_hash_name, item_name, liquidity_rank) as (
  values
    ('AK-47 | Redline (Field-Tested)', 'AK-47 | Redline (Field-Tested)', 1),
    ('AK-47 | Slate (Field-Tested)', 'AK-47 | Slate (Field-Tested)', 2),
    ('AK-47 | Bloodsport (Field-Tested)', 'AK-47 | Bloodsport (Field-Tested)', 3),
    ('AK-47 | Vulcan (Field-Tested)', 'AK-47 | Vulcan (Field-Tested)', 4),
    ('AK-47 | Asiimov (Field-Tested)', 'AK-47 | Asiimov (Field-Tested)', 5),
    ('AK-47 | Neon Rider (Field-Tested)', 'AK-47 | Neon Rider (Field-Tested)', 6),
    ('AWP | Asiimov (Battle-Scarred)', 'AWP | Asiimov (Battle-Scarred)', 7),
    ('AWP | Hyper Beast (Field-Tested)', 'AWP | Hyper Beast (Field-Tested)', 8),
    ('AWP | Neo-Noir (Field-Tested)', 'AWP | Neo-Noir (Field-Tested)', 9),
    ('AWP | Redline (Field-Tested)', 'AWP | Redline (Field-Tested)', 10),
    ('AWP | Wildfire (Field-Tested)', 'AWP | Wildfire (Field-Tested)', 11),
    ('M4A4 | Neo-Noir (Field-Tested)', 'M4A4 | Neo-Noir (Field-Tested)', 12),
    ('M4A4 | The Emperor (Field-Tested)', 'M4A4 | The Emperor (Field-Tested)', 13),
    ('M4A4 | Temukau (Field-Tested)', 'M4A4 | Temukau (Field-Tested)', 14),
    ('M4A1-S | Printstream (Field-Tested)', 'M4A1-S | Printstream (Field-Tested)', 15),
    ('M4A1-S | Hyper Beast (Field-Tested)', 'M4A1-S | Hyper Beast (Field-Tested)', 16),
    ('M4A1-S | Mecha Industries (Field-Tested)', 'M4A1-S | Mecha Industries (Field-Tested)', 17),
    ('M4A1-S | Decimator (Field-Tested)', 'M4A1-S | Decimator (Field-Tested)', 18),
    ('Desert Eagle | Printstream (Field-Tested)', 'Desert Eagle | Printstream (Field-Tested)', 19),
    ('Desert Eagle | Code Red (Field-Tested)', 'Desert Eagle | Code Red (Field-Tested)', 20),
    ('Glock-18 | Neo-Noir (Field-Tested)', 'Glock-18 | Neo-Noir (Field-Tested)', 21),
    ('Glock-18 | Vogue (Field-Tested)', 'Glock-18 | Vogue (Field-Tested)', 22),
    ('USP-S | Kill Confirmed (Field-Tested)', 'USP-S | Kill Confirmed (Field-Tested)', 23),
    ('USP-S | Printstream (Field-Tested)', 'USP-S | Printstream (Field-Tested)', 24),
    ('USP-S | Neo-Noir (Field-Tested)', 'USP-S | Neo-Noir (Field-Tested)', 25),
    ('P90 | Asiimov (Field-Tested)', 'P90 | Asiimov (Field-Tested)', 26),
    ('Revolution Case', 'Revolution Case', 27),
    ('Fracture Case', 'Fracture Case', 28),
    ('Recoil Case', 'Recoil Case', 29),
    ('Dreams & Nightmares Case', 'Dreams & Nightmares Case', 30),
    ('Prisma 2 Case', 'Prisma 2 Case', 31),
    ('Chroma 3 Case', 'Chroma 3 Case', 32),
    ('Clutch Case', 'Clutch Case', 33),
    ('Danger Zone Case', 'Danger Zone Case', 34),
    ('Gamma 2 Case', 'Gamma 2 Case', 35),
    ('Operation Broken Fang Case', 'Operation Broken Fang Case', 36),
    ('Operation Riptide Case', 'Operation Riptide Case', 37),
    ('CS20 Case', 'CS20 Case', 38),
    ('Paris 2023 Legends Sticker Capsule', 'Paris 2023 Legends Sticker Capsule', 39),
    ('Paris 2023 Challengers Sticker Capsule', 'Paris 2023 Challengers Sticker Capsule', 40),
    ('Paris 2023 Contenders Sticker Capsule', 'Paris 2023 Contenders Sticker Capsule', 41),
    ('Antwerp 2022 Legends Sticker Capsule', 'Antwerp 2022 Legends Sticker Capsule', 42),
    ('Antwerp 2022 Challengers Sticker Capsule', 'Antwerp 2022 Challengers Sticker Capsule', 43),
    ('Antwerp 2022 Contenders Sticker Capsule', 'Antwerp 2022 Contenders Sticker Capsule', 44),
    ('Stockholm 2021 Legends Sticker Capsule', 'Stockholm 2021 Legends Sticker Capsule', 45),
    ('Stockholm 2021 Challengers Sticker Capsule', 'Stockholm 2021 Challengers Sticker Capsule', 46),
    ('Stockholm 2021 Contenders Sticker Capsule', 'Stockholm 2021 Contenders Sticker Capsule', 47),
    ('Copenhagen 2024 Legends Sticker Capsule', 'Copenhagen 2024 Legends Sticker Capsule', 48),
    ('Copenhagen 2024 Challengers Sticker Capsule', 'Copenhagen 2024 Challengers Sticker Capsule', 49),
    ('Copenhagen 2024 Contenders Sticker Capsule', 'Copenhagen 2024 Contenders Sticker Capsule', 50)
)
insert into public.market_universe (
  market_hash_name,
  item_name,
  liquidity_rank,
  is_active
)
select
  market_hash_name,
  item_name,
  liquidity_rank,
  true
from curated
on conflict (market_hash_name) do update
set
  item_name = excluded.item_name,
  liquidity_rank = excluded.liquidity_rank,
  is_active = true,
  updated_at = now();

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

create unique index if not exists idx_market_universe_item_name
  on public.market_universe(item_name);

create index if not exists idx_market_universe_active_rank
  on public.market_universe(is_active, liquidity_rank);

drop trigger if exists trg_market_universe_updated_at on public.market_universe;
create trigger trg_market_universe_updated_at
before update on public.market_universe
for each row execute function public.set_updated_at();

alter table public.market_universe enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'market_universe'
      and policyname = 'market_universe_read_authenticated'
  ) then
    create policy "market_universe_read_authenticated"
    on public.market_universe
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

revoke all on table public.market_universe from anon, authenticated;
grant select on table public.market_universe to authenticated;

with seed(market_hash_name, item_name, liquidity_rank) as (
  values
    ('AK-47 | Redline (Field-Tested)', 'AK-47 | Redline (Field-Tested)', 1),
    ('AWP | Asiimov (Battle-Scarred)', 'AWP | Asiimov (Battle-Scarred)', 2),
    ('M4A4 | Neo-Noir (Field-Tested)', 'M4A4 | Neo-Noir (Field-Tested)', 3),
    ('AWP | Lightning Strike', 'AWP | Lightning Strike', 4),
    ('Desert Eagle | Blaze', 'Desert Eagle | Blaze', 5),
    ('AK-47 | Vulcan (Field-Tested)', 'AK-47 | Vulcan (Field-Tested)', 6),
    ('M4A1-S | Printstream (Field-Tested)', 'M4A1-S | Printstream (Field-Tested)', 7),
    ('AWP | Graphite', 'AWP | Graphite', 8),
    ('AK-47 | Case Hardened (Field-Tested)', 'AK-47 | Case Hardened (Field-Tested)', 9),
    ('AK-47 | Fuel Injector (Field-Tested)', 'AK-47 | Fuel Injector (Field-Tested)', 10),
    ('AK-47 | Bloodsport (Field-Tested)', 'AK-47 | Bloodsport (Field-Tested)', 11),
    ('AK-47 | Neon Rider (Field-Tested)', 'AK-47 | Neon Rider (Field-Tested)', 12),
    ('AK-47 | Asiimov (Field-Tested)', 'AK-47 | Asiimov (Field-Tested)', 13),
    ('AK-47 | The Empress (Field-Tested)', 'AK-47 | The Empress (Field-Tested)', 14),
    ('AK-47 | Nightwish (Field-Tested)', 'AK-47 | Nightwish (Field-Tested)', 15),
    ('AK-47 | Ice Coaled (Factory New)', 'AK-47 | Ice Coaled (Factory New)', 16),
    ('AK-47 | Slate (Field-Tested)', 'AK-47 | Slate (Field-Tested)', 17),
    ('AWP | Hyper Beast (Field-Tested)', 'AWP | Hyper Beast (Field-Tested)', 18),
    ('AWP | Wildfire (Field-Tested)', 'AWP | Wildfire (Field-Tested)', 19),
    ('AWP | Neo-Noir (Field-Tested)', 'AWP | Neo-Noir (Field-Tested)', 20),
    ('AWP | Containment Breach (Field-Tested)', 'AWP | Containment Breach (Field-Tested)', 21),
    ('AWP | Redline (Field-Tested)', 'AWP | Redline (Field-Tested)', 22),
    ('AWP | Chromatic Aberration (Field-Tested)', 'AWP | Chromatic Aberration (Field-Tested)', 23),
    ('AWP | Mortis (Field-Tested)', 'AWP | Mortis (Field-Tested)', 24),
    ('AWP | Fever Dream (Field-Tested)', 'AWP | Fever Dream (Field-Tested)', 25),
    ('AWP | Electric Hive (Factory New)', 'AWP | Electric Hive (Factory New)', 26),
    ('AWP | BOOM (Field-Tested)', 'AWP | BOOM (Field-Tested)', 27),
    ('AWP | Man-o''-war (Field-Tested)', 'AWP | Man-o''-war (Field-Tested)', 28),
    ('AWP | Sun in Leo (Factory New)', 'AWP | Sun in Leo (Factory New)', 29),
    ('M4A4 | The Emperor (Field-Tested)', 'M4A4 | The Emperor (Field-Tested)', 30),
    ('M4A4 | Temukau (Field-Tested)', 'M4A4 | Temukau (Field-Tested)', 31),
    ('M4A4 | Desolate Space (Field-Tested)', 'M4A4 | Desolate Space (Field-Tested)', 32),
    ('M4A4 | In Living Color (Field-Tested)', 'M4A4 | In Living Color (Field-Tested)', 33),
    ('M4A4 | Hellfire (Field-Tested)', 'M4A4 | Hellfire (Field-Tested)', 34),
    ('M4A4 | Buzz Kill (Field-Tested)', 'M4A4 | Buzz Kill (Field-Tested)', 35),
    ('M4A4 | Cyber Security (Field-Tested)', 'M4A4 | Cyber Security (Field-Tested)', 36),
    ('M4A4 | Royal Paladin (Field-Tested)', 'M4A4 | Royal Paladin (Field-Tested)', 37),
    ('M4A4 | Evil Daimyo (Field-Tested)', 'M4A4 | Evil Daimyo (Field-Tested)', 38),
    ('M4A1-S | Cyrex (Factory New)', 'M4A1-S | Cyrex (Factory New)', 39),
    ('M4A1-S | Decimator (Field-Tested)', 'M4A1-S | Decimator (Field-Tested)', 40),
    ('M4A1-S | Hyper Beast (Field-Tested)', 'M4A1-S | Hyper Beast (Field-Tested)', 41),
    ('M4A1-S | Player Two (Field-Tested)', 'M4A1-S | Player Two (Field-Tested)', 42),
    ('M4A1-S | Chantico''s Fire (Field-Tested)', 'M4A1-S | Chantico''s Fire (Field-Tested)', 43),
    ('M4A1-S | Mecha Industries (Field-Tested)', 'M4A1-S | Mecha Industries (Field-Tested)', 44),
    ('M4A1-S | Golden Coil (Field-Tested)', 'M4A1-S | Golden Coil (Field-Tested)', 45),
    ('M4A1-S | Nightmare (Field-Tested)', 'M4A1-S | Nightmare (Field-Tested)', 46),
    ('M4A1-S | Leaded Glass (Field-Tested)', 'M4A1-S | Leaded Glass (Field-Tested)', 47),
    ('M4A1-S | Night Terror (Field-Tested)', 'M4A1-S | Night Terror (Field-Tested)', 48),
    ('M4A1-S | Basilisk (Field-Tested)', 'M4A1-S | Basilisk (Field-Tested)', 49),
    ('Desert Eagle | Printstream (Field-Tested)', 'Desert Eagle | Printstream (Field-Tested)', 50),
    ('Desert Eagle | Code Red (Field-Tested)', 'Desert Eagle | Code Red (Field-Tested)', 51),
    ('Desert Eagle | Ocean Drive (Field-Tested)', 'Desert Eagle | Ocean Drive (Field-Tested)', 52),
    ('Desert Eagle | Kumicho Dragon (Field-Tested)', 'Desert Eagle | Kumicho Dragon (Field-Tested)', 53),
    ('Desert Eagle | Conspiracy (Factory New)', 'Desert Eagle | Conspiracy (Factory New)', 54),
    ('Desert Eagle | Mecha Industries (Field-Tested)', 'Desert Eagle | Mecha Industries (Field-Tested)', 55),
    ('Desert Eagle | Trigger Discipline (Field-Tested)', 'Desert Eagle | Trigger Discipline (Field-Tested)', 56),
    ('Desert Eagle | Corinthian (Factory New)', 'Desert Eagle | Corinthian (Factory New)', 57),
    ('Desert Eagle | Crimson Web (Field-Tested)', 'Desert Eagle | Crimson Web (Field-Tested)', 58),
    ('Glock-18 | Fade (Factory New)', 'Glock-18 | Fade (Factory New)', 59),
    ('Glock-18 | Vogue (Field-Tested)', 'Glock-18 | Vogue (Field-Tested)', 60),
    ('Glock-18 | Neo-Noir (Field-Tested)', 'Glock-18 | Neo-Noir (Field-Tested)', 61),
    ('Glock-18 | Bullet Queen (Field-Tested)', 'Glock-18 | Bullet Queen (Field-Tested)', 62),
    ('Glock-18 | Water Elemental (Field-Tested)', 'Glock-18 | Water Elemental (Field-Tested)', 63),
    ('Glock-18 | Gamma Doppler (Factory New)', 'Glock-18 | Gamma Doppler (Factory New)', 64),
    ('Glock-18 | Snack Attack (Field-Tested)', 'Glock-18 | Snack Attack (Field-Tested)', 65),
    ('Glock-18 | Wasteland Rebel (Field-Tested)', 'Glock-18 | Wasteland Rebel (Field-Tested)', 66),
    ('Glock-18 | Weasel (Field-Tested)', 'Glock-18 | Weasel (Field-Tested)', 67),
    ('USP-S | Kill Confirmed (Field-Tested)', 'USP-S | Kill Confirmed (Field-Tested)', 68),
    ('USP-S | The Traitor (Field-Tested)', 'USP-S | The Traitor (Field-Tested)', 69),
    ('USP-S | Printstream (Field-Tested)', 'USP-S | Printstream (Field-Tested)', 70),
    ('USP-S | Neo-Noir (Field-Tested)', 'USP-S | Neo-Noir (Field-Tested)', 71),
    ('USP-S | Monster Mashup (Field-Tested)', 'USP-S | Monster Mashup (Field-Tested)', 72),
    ('USP-S | Cortex (Field-Tested)', 'USP-S | Cortex (Field-Tested)', 73),
    ('USP-S | Jawbreaker (Field-Tested)', 'USP-S | Jawbreaker (Field-Tested)', 74),
    ('USP-S | Cyrex (Factory New)', 'USP-S | Cyrex (Factory New)', 75),
    ('USP-S | Orion (Factory New)', 'USP-S | Orion (Factory New)', 76),
    ('USP-S | Ticket to Hell (Field-Tested)', 'USP-S | Ticket to Hell (Field-Tested)', 77),
    ('P250 | See Ya Later (Field-Tested)', 'P250 | See Ya Later (Field-Tested)', 78),
    ('P250 | Asiimov (Field-Tested)', 'P250 | Asiimov (Field-Tested)', 79),
    ('P250 | Muertos (Field-Tested)', 'P250 | Muertos (Field-Tested)', 80),
    ('P250 | Visions (Field-Tested)', 'P250 | Visions (Field-Tested)', 81),
    ('Five-SeveN | Hyper Beast (Field-Tested)', 'Five-SeveN | Hyper Beast (Field-Tested)', 82),
    ('Five-SeveN | Angry Mob (Field-Tested)', 'Five-SeveN | Angry Mob (Field-Tested)', 83),
    ('Five-SeveN | Fairy Tale (Factory New)', 'Five-SeveN | Fairy Tale (Factory New)', 84),
    ('Five-SeveN | Monkey Business (Field-Tested)', 'Five-SeveN | Monkey Business (Field-Tested)', 85),
    ('Tec-9 | Decimator (Field-Tested)', 'Tec-9 | Decimator (Field-Tested)', 86),
    ('Tec-9 | Fuel Injector (Field-Tested)', 'Tec-9 | Fuel Injector (Field-Tested)', 87),
    ('MP9 | Starlight Protector (Field-Tested)', 'MP9 | Starlight Protector (Field-Tested)', 88),
    ('MP9 | Food Chain (Field-Tested)', 'MP9 | Food Chain (Field-Tested)', 89),
    ('MP7 | Bloodsport (Field-Tested)', 'MP7 | Bloodsport (Field-Tested)', 90),
    ('P90 | Asiimov (Field-Tested)', 'P90 | Asiimov (Field-Tested)', 91),
    ('P90 | Death by Kitty (Factory New)', 'P90 | Death by Kitty (Factory New)', 92),
    ('FAMAS | Commemoration (Field-Tested)', 'FAMAS | Commemoration (Field-Tested)', 93),
    ('FAMAS | Roll Cage (Field-Tested)', 'FAMAS | Roll Cage (Field-Tested)', 94),
    ('Galil AR | Chatterbox (Field-Tested)', 'Galil AR | Chatterbox (Field-Tested)', 95),
    ('Galil AR | Eco (Field-Tested)', 'Galil AR | Eco (Field-Tested)', 96),
    ('SG 553 | Cyrex (Factory New)', 'SG 553 | Cyrex (Factory New)', 97),
    ('AUG | Akihabara Accept (Field-Tested)', 'AUG | Akihabara Accept (Field-Tested)', 98),
    ('UMP-45 | Primal Saber (Field-Tested)', 'UMP-45 | Primal Saber (Field-Tested)', 99),
    ('MAC-10 | Neon Rider (Field-Tested)', 'MAC-10 | Neon Rider (Field-Tested)', 100)
)
insert into public.market_universe (
  market_hash_name,
  item_name,
  liquidity_rank,
  is_active
)
select
  seed.market_hash_name,
  seed.item_name,
  seed.liquidity_rank,
  true
from seed
on conflict (market_hash_name) do update
set
  item_name = excluded.item_name,
  liquidity_rank = excluded.liquidity_rank,
  is_active = true,
  updated_at = now();

create table if not exists public.market_quotes (
  id uuid primary key default gen_random_uuid(),
  item_name text not null,
  market text not null check (market in ('steam', 'skinport', 'csfloat', 'dmarket')),
  best_buy numeric(12,2) check (best_buy is null or best_buy >= 0),
  best_sell numeric(12,2) check (best_sell is null or best_sell >= 0),
  best_sell_net numeric(12,2) check (best_sell_net is null or best_sell_net >= 0),
  volume_7d integer check (volume_7d is null or volume_7d >= 0),
  liquidity_score integer check (liquidity_score is null or (liquidity_score >= 0 and liquidity_score <= 100)),
  fetched_at timestamptz not null default now(),
  quality_flags jsonb not null default '{}'::jsonb
);

create index if not exists idx_market_quotes_item_name
  on public.market_quotes(item_name);

create index if not exists idx_market_quotes_market
  on public.market_quotes(market);

create index if not exists idx_market_quotes_fetched_at
  on public.market_quotes(fetched_at desc);

create index if not exists idx_market_quotes_item_market_fetched
  on public.market_quotes(item_name, market, fetched_at desc);

alter table public.market_quotes enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'market_quotes'
      and policyname = 'market_quotes_read_authenticated'
  ) then
    create policy "market_quotes_read_authenticated"
    on public.market_quotes
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

revoke all on table public.market_quotes from anon, authenticated;
grant select on table public.market_quotes to authenticated;

create table if not exists public.arbitrage_opportunities (
  id uuid primary key default gen_random_uuid(),
  item_name text not null,
  buy_market text not null check (buy_market in ('steam', 'skinport', 'csfloat', 'dmarket')),
  buy_price numeric(12,2) not null check (buy_price >= 0),
  sell_market text not null check (sell_market in ('steam', 'skinport', 'csfloat', 'dmarket')),
  sell_net numeric(12,2) not null check (sell_net >= 0),
  profit numeric(12,2) not null,
  spread_pct numeric(8,2) not null,
  opportunity_score integer not null check (opportunity_score >= 0 and opportunity_score <= 100),
  quality_grade text not null check (quality_grade in ('A', 'B', 'C', 'RISKY')),
  detected_at timestamptz not null default now()
);

create index if not exists idx_arbitrage_opportunities_item_name
  on public.arbitrage_opportunities(item_name);

create index if not exists idx_arbitrage_opportunities_score
  on public.arbitrage_opportunities(opportunity_score desc);

create index if not exists idx_arbitrage_opportunities_detected_at
  on public.arbitrage_opportunities(detected_at desc);

alter table public.arbitrage_opportunities enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'arbitrage_opportunities'
      and policyname = 'arbitrage_opportunities_read_authenticated'
  ) then
    create policy "arbitrage_opportunities_read_authenticated"
    on public.arbitrage_opportunities
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

revoke all on table public.arbitrage_opportunities from anon, authenticated;
grant select on table public.arbitrage_opportunities to authenticated;

create or replace function public.refresh_arbitrage_opportunities_cache()
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  inserted_count integer := 0;
begin
  delete from public.arbitrage_opportunities;

  with active_items as (
    select item_name
    from public.market_universe
    where is_active
  ),
  latest_quotes as (
    select distinct on (q.item_name, q.market)
      q.item_name,
      q.market,
      q.best_buy,
      q.best_sell_net,
      q.volume_7d,
      q.liquidity_score,
      q.fetched_at
    from public.market_quotes q
    join active_items a on a.item_name = q.item_name
    order by q.item_name, q.market, q.fetched_at desc
  ),
  buy_ranked as (
    select
      item_name,
      market as buy_market,
      best_buy as buy_price,
      row_number() over (partition by item_name order by best_buy asc) as rn
    from latest_quotes
    where best_buy is not null and best_buy > 0
  ),
  sell_ranked as (
    select
      item_name,
      market as sell_market,
      best_sell_net as sell_net,
      row_number() over (partition by item_name order by best_sell_net desc) as rn
    from latest_quotes
    where best_sell_net is not null and best_sell_net > 0
  ),
  liquidity as (
    select
      item_name,
      max(coalesce(volume_7d, 0)) as volume_7d,
      max(coalesce(liquidity_score, 0)) as liquidity_score
    from latest_quotes
    group by item_name
  ),
  candidates as (
    select
      b.item_name,
      b.buy_market,
      b.buy_price,
      s.sell_market,
      s.sell_net,
      round((s.sell_net - b.buy_price)::numeric, 2) as profit,
      round((((s.sell_net - b.buy_price) / nullif(b.buy_price, 0)) * 100)::numeric, 2) as spread_pct,
      coalesce(l.volume_7d, 0) as volume_7d,
      coalesce(l.liquidity_score, 0) as liquidity_score
    from buy_ranked b
    join sell_ranked s on s.item_name = b.item_name
    left join liquidity l on l.item_name = b.item_name
    where b.rn = 1 and s.rn = 1
  ),
  scored as (
    select
      c.*,
      least(
        greatest(
          round(
            (
              least(greatest(c.spread_pct, 0), 100) * 0.35 +
              least(greatest(c.liquidity_score, 0), 100) * 0.35 +
              70 * 0.20 +
              (
                (
                  case c.buy_market
                    when 'steam' then 100
                    when 'skinport' then 90
                    when 'csfloat' then 80
                    when 'dmarket' then 75
                    else 70
                  end +
                  case c.sell_market
                    when 'steam' then 100
                    when 'skinport' then 90
                    when 'csfloat' then 80
                    when 'dmarket' then 75
                    else 70
                  end
                ) / 2.0
              ) * 0.10
            )
          )::integer,
          0
        ),
        100
      ) as opportunity_score
    from candidates c
    where c.profit > 0
      and c.spread_pct >= 5
      and c.spread_pct <= 300
      and c.volume_7d >= 50
      and c.liquidity_score >= 30
  )
  insert into public.arbitrage_opportunities (
    item_name,
    buy_market,
    buy_price,
    sell_market,
    sell_net,
    profit,
    spread_pct,
    opportunity_score,
    quality_grade,
    detected_at
  )
  select
    s.item_name,
    s.buy_market,
    s.buy_price,
    s.sell_market,
    s.sell_net,
    s.profit,
    s.spread_pct,
    s.opportunity_score,
    case
      when s.opportunity_score >= 85 then 'A'
      when s.opportunity_score >= 70 then 'B'
      when s.opportunity_score >= 55 then 'C'
      else 'RISKY'
    end as quality_grade,
    now()
  from scored s
  order by s.opportunity_score desc, s.profit desc;

  get diagnostics inserted_count = row_count;
  return inserted_count;
end;
$$;

create or replace function public.arbitrage_scanner_job(batch_size integer default 50)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  safe_batch_size integer := greatest(least(coalesce(batch_size, 50), 500), 1);
  active_count integer := 0;
  queued_batches integer := 0;
  offset_value integer := 0;
  refreshed_count integer := 0;
begin
  select count(*)
  into active_count
  from public.market_universe
  where is_active;

  while offset_value < active_count loop
    perform pg_notify(
      'arbitrage_scanner_batch',
      jsonb_build_object(
        'offset', offset_value,
        'limit', safe_batch_size,
        'requested_at', now()
      )::text
    );
    queued_batches := queued_batches + 1;
    offset_value := offset_value + safe_batch_size;
  end loop;

  refreshed_count := public.refresh_arbitrage_opportunities_cache();

  return jsonb_build_object(
    'ok', true,
    'active_items', active_count,
    'queued_batches', queued_batches,
    'batch_size', safe_batch_size,
    'opportunities_refreshed', refreshed_count,
    'ran_at', now()
  );
end;
$$;

do $$
declare
  cron_available boolean := false;
  existing_job record;
begin
  select exists(
    select 1
    from pg_available_extensions
    where name = 'pg_cron'
  )
  into cron_available;

  if not cron_available then
    raise notice 'pg_cron extension is unavailable. Create job manually in your deployment.';
    return;
  end if;

  create extension if not exists pg_cron;

  for existing_job in
    select jobid
    from cron.job
    where jobname = 'arbitrage_scanner_job'
  loop
    perform cron.unschedule(existing_job.jobid);
  end loop;

  perform cron.schedule(
    'arbitrage_scanner_job',
    '*/5 * * * *',
    $schedule$select public.arbitrage_scanner_job(50);$schedule$
  );
end;
$$;

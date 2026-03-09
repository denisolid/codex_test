create table if not exists public.scanner_runs (
  id uuid primary key default gen_random_uuid(),
  scanner_type text not null default 'global_arbitrage',
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  status text not null default 'running',
  items_scanned integer not null default 0,
  opportunities_found integer not null default 0,
  new_opportunities_added integer not null default 0,
  diagnostics_summary jsonb not null default '{}'::jsonb
);

alter table public.scanner_runs
  add column if not exists scanner_type text not null default 'global_arbitrage',
  add column if not exists started_at timestamptz not null default now(),
  add column if not exists completed_at timestamptz,
  add column if not exists status text not null default 'running',
  add column if not exists items_scanned integer not null default 0,
  add column if not exists opportunities_found integer not null default 0,
  add column if not exists new_opportunities_added integer not null default 0,
  add column if not exists diagnostics_summary jsonb not null default '{}'::jsonb;

create index if not exists idx_scanner_runs_type_started_desc
  on public.scanner_runs(scanner_type, started_at desc);

create index if not exists idx_scanner_runs_type_completed_desc
  on public.scanner_runs(scanner_type, completed_at desc);

create table if not exists public.arbitrage_feed (
  id uuid primary key default gen_random_uuid(),
  item_name text not null,
  market_hash_name text,
  category text not null default 'weapon_skin',
  buy_market text not null,
  buy_price numeric not null,
  sell_market text not null,
  sell_net numeric not null,
  profit numeric not null,
  spread_pct numeric not null,
  opportunity_score integer not null,
  execution_confidence text,
  quality_grade text,
  liquidity_label text,
  detected_at timestamptz not null default now(),
  scan_run_id uuid references public.scanner_runs(id) on delete set null,
  is_active boolean not null default true,
  is_duplicate boolean not null default false,
  metadata jsonb not null default '{}'::jsonb
);

alter table public.arbitrage_feed
  add column if not exists item_name text,
  add column if not exists market_hash_name text,
  add column if not exists category text not null default 'weapon_skin',
  add column if not exists buy_market text,
  add column if not exists buy_price numeric,
  add column if not exists sell_market text,
  add column if not exists sell_net numeric,
  add column if not exists profit numeric,
  add column if not exists spread_pct numeric,
  add column if not exists opportunity_score integer,
  add column if not exists execution_confidence text,
  add column if not exists quality_grade text,
  add column if not exists liquidity_label text,
  add column if not exists detected_at timestamptz not null default now(),
  add column if not exists scan_run_id uuid references public.scanner_runs(id) on delete set null,
  add column if not exists is_active boolean not null default true,
  add column if not exists is_duplicate boolean not null default false,
  add column if not exists metadata jsonb not null default '{}'::jsonb;

create index if not exists idx_arbitrage_feed_detected_active
  on public.arbitrage_feed(is_active, detected_at desc);

create index if not exists idx_arbitrage_feed_scan_run_id
  on public.arbitrage_feed(scan_run_id);

create index if not exists idx_arbitrage_feed_quality_score
  on public.arbitrage_feed(quality_grade, opportunity_score desc);

create index if not exists idx_arbitrage_feed_high_confidence_flag
  on public.arbitrage_feed(((metadata ->> 'is_high_confidence_eligible')));

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

create index if not exists idx_market_quotes_item_market_fetched_desc
  on public.market_quotes(item_name, market, fetched_at desc);

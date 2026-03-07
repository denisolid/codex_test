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

create index if not exists idx_scanner_runs_started_at_desc
  on public.scanner_runs(started_at desc);

create index if not exists idx_scanner_runs_scanner_type
  on public.scanner_runs(scanner_type);

create index if not exists idx_scanner_runs_status
  on public.scanner_runs(status);

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

create index if not exists idx_arbitrage_feed_detected_at_desc
  on public.arbitrage_feed(detected_at desc);

create index if not exists idx_arbitrage_feed_item_name
  on public.arbitrage_feed(item_name);

create index if not exists idx_arbitrage_feed_opportunity_score
  on public.arbitrage_feed(opportunity_score desc);

create index if not exists idx_arbitrage_feed_is_active
  on public.arbitrage_feed(is_active);

create index if not exists idx_arbitrage_feed_signature_recent
  on public.arbitrage_feed(item_name, buy_market, sell_market, detected_at desc);

alter table public.scanner_runs enable row level security;
alter table public.arbitrage_feed enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'scanner_runs'
      and policyname = 'scanner_runs_read_authenticated'
  ) then
    create policy "scanner_runs_read_authenticated"
    on public.scanner_runs
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'arbitrage_feed'
      and policyname = 'arbitrage_feed_read_authenticated'
  ) then
    create policy "arbitrage_feed_read_authenticated"
    on public.arbitrage_feed
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

revoke all on table public.scanner_runs from anon, authenticated;
revoke all on table public.arbitrage_feed from anon, authenticated;
grant select on table public.scanner_runs to authenticated;
grant select on table public.arbitrage_feed to authenticated;

create table if not exists public.global_active_opportunities (
  id uuid primary key default gen_random_uuid(),
  opportunity_fingerprint text not null,
  material_change_hash text,
  scan_run_id uuid references public.scanner_runs(id) on delete set null,
  market_hash_name text,
  item_name text not null,
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
  market_signal_observed_at timestamptz,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_published_at timestamptz not null default now(),
  refresh_status text not null default 'pending',
  live_status text not null default 'live',
  latest_signal_age_hours numeric,
  metadata jsonb not null default '{}'::jsonb
);

create unique index if not exists idx_global_active_opportunities_fingerprint
  on public.global_active_opportunities(opportunity_fingerprint);

create index if not exists idx_global_active_opportunities_live_status_published
  on public.global_active_opportunities(live_status, last_published_at desc);

create index if not exists idx_global_active_opportunities_signal_observed
  on public.global_active_opportunities(market_signal_observed_at desc);

create index if not exists idx_global_active_opportunities_category_published
  on public.global_active_opportunities(category, last_published_at desc);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'global_active_opportunities_live_status_chk'
  ) then
    alter table public.global_active_opportunities
      add constraint global_active_opportunities_live_status_chk
      check (live_status in ('live', 'stale', 'degraded'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'global_active_opportunities_refresh_status_chk'
  ) then
    alter table public.global_active_opportunities
      add constraint global_active_opportunities_refresh_status_chk
      check (refresh_status in ('pending', 'ok', 'stale', 'degraded', 'failed'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'global_active_opportunities_state_pair_chk'
  ) then
    alter table public.global_active_opportunities
      add constraint global_active_opportunities_state_pair_chk
      check (
        (live_status = 'live' and refresh_status in ('pending', 'ok')) or
        (live_status = 'stale' and refresh_status = 'stale') or
        (live_status = 'degraded' and refresh_status in ('degraded', 'failed'))
      );
  end if;
end;
$$;

create table if not exists public.global_opportunity_history (
  id uuid primary key default gen_random_uuid(),
  source_event_key text not null,
  active_opportunity_id uuid references public.global_active_opportunities(id) on delete set null,
  opportunity_fingerprint text not null,
  scan_run_id uuid references public.scanner_runs(id) on delete set null,
  event_type text not null,
  event_at timestamptz not null default now(),
  refresh_status text,
  live_status text,
  reason text,
  snapshot jsonb not null default '{}'::jsonb
);

create unique index if not exists idx_global_opportunity_history_source_event_key
  on public.global_opportunity_history(source_event_key);

create index if not exists idx_global_opportunity_history_fingerprint_event_at
  on public.global_opportunity_history(opportunity_fingerprint, event_at desc);

create index if not exists idx_global_opportunity_history_run_event_at
  on public.global_opportunity_history(scan_run_id, event_at desc);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'global_opportunity_history_event_type_chk'
  ) then
    alter table public.global_opportunity_history
      add constraint global_opportunity_history_event_type_chk
      check (event_type in ('new', 'updated', 'reactivated', 'expired'));
  end if;
end;
$$;

drop index if exists public.idx_scanner_runs_single_active_per_type;

create unique index if not exists idx_scanner_runs_single_active_per_type
  on public.scanner_runs(scanner_type)
  where status = 'running'
    and scanner_type in ('enrichment', 'opportunity_scan', 'feed_revalidation');

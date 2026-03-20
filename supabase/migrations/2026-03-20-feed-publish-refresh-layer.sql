alter table public.arbitrage_feed
  add column if not exists discovered_at timestamptz,
  add column if not exists market_signal_observed_at timestamptz,
  add column if not exists feed_published_at timestamptz,
  add column if not exists insight_refreshed_at timestamptz,
  add column if not exists last_refresh_attempt_at timestamptz,
  add column if not exists latest_signal_age_hours numeric,
  add column if not exists net_profit_after_fees numeric,
  add column if not exists confidence_score integer,
  add column if not exists freshness_score integer,
  add column if not exists verdict text,
  add column if not exists refresh_status text,
  add column if not exists live_status text;

update public.arbitrage_feed
set
  discovered_at = coalesce(discovered_at, detected_at),
  net_profit_after_fees = coalesce(net_profit_after_fees, profit),
  refresh_status = coalesce(nullif(refresh_status, ''), 'pending'),
  live_status = coalesce(nullif(live_status, ''), 'degraded')
where
  discovered_at is null
  or net_profit_after_fees is null
  or refresh_status is null
  or live_status is null
  or refresh_status = ''
  or live_status = '';

alter table public.arbitrage_feed
  alter column discovered_at set default now();

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'arbitrage_feed'
      and column_name = 'discovered_at'
      and is_nullable = 'YES'
  ) then
    alter table public.arbitrage_feed
      alter column discovered_at set not null;
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'arbitrage_feed_live_status_chk'
  ) then
    alter table public.arbitrage_feed
      add constraint arbitrage_feed_live_status_chk
      check (live_status in ('live', 'stale', 'degraded'));
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'arbitrage_feed_refresh_status_chk'
  ) then
    alter table public.arbitrage_feed
      add constraint arbitrage_feed_refresh_status_chk
      check (refresh_status in ('pending', 'ok', 'stale', 'degraded', 'failed'));
  end if;
end;
$$;

create index if not exists idx_arbitrage_feed_live_status_detected
  on public.arbitrage_feed(live_status, detected_at desc, id desc);

create index if not exists idx_arbitrage_feed_refresh_status_detected
  on public.arbitrage_feed(refresh_status, detected_at desc, id desc);

create index if not exists idx_arbitrage_feed_signal_observed_desc
  on public.arbitrage_feed(market_signal_observed_at desc, id desc);

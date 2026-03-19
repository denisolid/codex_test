create or replace function public.get_latest_market_snapshots_by_skin_ids(
  p_skin_ids bigint[]
)
returns table (
  skin_id bigint,
  lowest_listing_price numeric,
  average_7d_price numeric,
  volume_24h integer,
  spread_percent numeric,
  volatility_7d_percent numeric,
  currency text,
  source text,
  captured_at timestamptz
)
language sql
stable
as $$
  select distinct on (snap.skin_id)
    snap.skin_id,
    snap.lowest_listing_price,
    snap.average_7d_price,
    snap.volume_24h,
    snap.spread_percent,
    snap.volatility_7d_percent,
    snap.currency,
    snap.source,
    snap.captured_at
  from public.market_item_snapshots snap
  where snap.skin_id = any(p_skin_ids)
  order by snap.skin_id, snap.captured_at desc
$$;

create or replace function public.get_latest_market_quote_rows_by_item_names(
  p_item_names text[],
  p_lookback timestamptz default null
)
returns table (
  item_name text,
  market text,
  best_buy numeric,
  best_sell numeric,
  best_sell_net numeric,
  volume_7d integer,
  fetched_at timestamptz
)
language sql
stable
as $$
  select distinct on (mq.item_name, mq.market)
    mq.item_name,
    mq.market,
    mq.best_buy,
    mq.best_sell,
    mq.best_sell_net,
    mq.volume_7d,
    mq.fetched_at
  from public.market_quotes mq
  where mq.item_name = any(p_item_names)
    and (p_lookback is null or mq.fetched_at >= p_lookback)
  order by mq.item_name, mq.market, mq.fetched_at desc
$$;

create index if not exists idx_market_source_catalog_rebuild_pool
  on public.market_source_catalog(
    is_active,
    tradable,
    category,
    candidate_status,
    enrichment_priority desc nulls last,
    liquidity_rank desc nulls last
  );

create index if not exists idx_market_source_catalog_scan_eligible_rank
  on public.market_source_catalog(
    is_active,
    tradable,
    scan_eligible,
    category,
    liquidity_rank desc nulls last
  );

do $$
declare
  cron_available boolean := false;
  existing_job record;
begin
  select exists(
    select 1
    from pg_extension
    where extname = 'pg_cron'
  )
  into cron_available;

  if not cron_available then
    return;
  end if;

  for existing_job in
    select jobid
    from cron.job
    where jobname = 'arbitrage_scanner_job'
  loop
    perform cron.unschedule(existing_job.jobid);
  end loop;
end;
$$;

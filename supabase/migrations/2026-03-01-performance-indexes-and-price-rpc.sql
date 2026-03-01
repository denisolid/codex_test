create or replace function public.get_latest_price_rows_by_skin_ids(
  p_skin_ids bigint[],
  p_before timestamptz default null,
  p_exclude_mock boolean default false
)
returns table (
  skin_id bigint,
  price numeric,
  currency text,
  source text,
  recorded_at timestamptz
)
language sql
stable
as $$
  select distinct on (ph.skin_id)
    ph.skin_id,
    ph.price,
    ph.currency,
    ph.source,
    ph.recorded_at
  from public.price_history ph
  where ph.skin_id = any(p_skin_ids)
    and (p_before is null or ph.recorded_at <= p_before)
    and (not p_exclude_mock or ph.source not ilike '%mock%')
  order by ph.skin_id, ph.recorded_at desc
$$;

create index if not exists idx_transactions_user_executed_created
  on public.transactions(user_id, executed_at desc, created_at desc);

create index if not exists idx_watchlists_user_created_at
  on public.watchlists(user_id, created_at desc);

create index if not exists idx_inventories_steam_item_ids_gin
  on public.inventories using gin (steam_item_ids);

create index if not exists idx_users_public_updated_at
  on public.users(updated_at desc)
  where public_portfolio_enabled = true and steam_id64 is not null;

create index if not exists idx_price_alerts_enabled_id
  on public.price_alerts(enabled, id);

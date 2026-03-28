do $$
declare
  active_generation_id uuid;
begin
  if to_regclass('public.market_universe') is null
    or to_regclass('public.catalog_generations') is null then
    return;
  end if;

  alter table public.market_universe
    add column if not exists catalog_generation_id uuid;

  select id
  into active_generation_id
  from public.catalog_generations
  where is_active = true
  order by activated_at desc nulls last, created_at desc
  limit 1;

  if active_generation_id is null then
    raise exception 'market_universe_generation_boundary_requires_active_catalog_generation';
  end if;

  update public.market_universe
  set catalog_generation_id = active_generation_id
  where catalog_generation_id is null;

  alter table public.market_universe
    alter column catalog_generation_id set not null;

  alter table public.market_universe
    drop constraint if exists market_universe_catalog_generation_fk;

  alter table public.market_universe
    add constraint market_universe_catalog_generation_fk
    foreign key (catalog_generation_id)
    references public.catalog_generations(id)
    on delete restrict;

  drop index if exists public.idx_market_universe_market_hash_name;
  drop index if exists public.idx_market_universe_item_name;
  drop index if exists public.idx_market_universe_active_rank;
  drop index if exists public.idx_market_universe_active_category_rank;

  create unique index if not exists idx_market_universe_generation_market_hash_name
    on public.market_universe(catalog_generation_id, market_hash_name);

  create index if not exists idx_market_universe_generation_item_name
    on public.market_universe(catalog_generation_id, item_name);

  create index if not exists idx_market_universe_generation_active_rank
    on public.market_universe(catalog_generation_id, is_active, liquidity_rank);

  create index if not exists idx_market_universe_generation_active_category_rank
    on public.market_universe(catalog_generation_id, is_active, category, liquidity_rank);
end;
$$;

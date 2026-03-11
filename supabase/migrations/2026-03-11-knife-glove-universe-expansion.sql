do $$
begin
  if to_regclass('public.market_source_catalog') is not null then
    alter table public.market_source_catalog
      drop constraint if exists market_source_catalog_category_check;

    alter table public.market_source_catalog
      add constraint market_source_catalog_category_check
      check (category in ('weapon_skin', 'case', 'sticker_capsule', 'knife', 'glove'));
  end if;
end;
$$;

do $$
begin
  if to_regclass('public.market_universe') is not null then
    alter table public.market_universe
      add column if not exists category text not null default 'weapon_skin',
      add column if not exists subcategory text;

    update public.market_universe
    set category = case
      when lower(coalesce(market_hash_name, '')) like '%sticker capsule' then 'sticker_capsule'
      when lower(coalesce(market_hash_name, '')) like '% case' then 'case'
      when lower(coalesce(market_hash_name, '')) ~ '(gloves|glove|hand wraps)' then 'glove'
      when lower(coalesce(market_hash_name, '')) ~ '(knife|bayonet|karambit|daggers)' then 'knife'
      else 'weapon_skin'
    end
    where category is null
      or category not in ('weapon_skin', 'case', 'sticker_capsule', 'knife', 'glove');

    alter table public.market_universe
      drop constraint if exists market_universe_category_check;

    alter table public.market_universe
      add constraint market_universe_category_check
      check (category in ('weapon_skin', 'case', 'sticker_capsule', 'knife', 'glove'));

    create index if not exists idx_market_universe_active_category_rank
      on public.market_universe(is_active, category, liquidity_rank);
  end if;
end;
$$;

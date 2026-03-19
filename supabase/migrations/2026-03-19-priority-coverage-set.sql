do $$
begin
  if to_regclass('public.catalog_priority_sets') is null then
    create table public.catalog_priority_sets (
      id uuid primary key default gen_random_uuid(),
      set_name text not null unique,
      version integer not null check (version >= 1),
      description text,
      policy_hints jsonb not null default '{}'::jsonb,
      raw_payload jsonb not null default '{}'::jsonb,
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );

    create index if not exists idx_catalog_priority_sets_active
      on public.catalog_priority_sets(is_active, set_name);

    drop trigger if exists trg_catalog_priority_sets_updated_at on public.catalog_priority_sets;
    create trigger trg_catalog_priority_sets_updated_at
    before update on public.catalog_priority_sets
    for each row execute function public.set_updated_at();
  end if;
end;
$$;

do $$
begin
  if to_regclass('public.catalog_priority_set_items') is null then
    create table public.catalog_priority_set_items (
      id uuid primary key default gen_random_uuid(),
      set_name text not null references public.catalog_priority_sets(set_name) on delete cascade,
      canonical_category text not null check (canonical_category in ('weapon_skin', 'case', 'knife', 'glove')),
      item_name text not null,
      canonical_item_name text not null,
      priority_tier text not null check (priority_tier in ('tier_a', 'tier_b')),
      priority_rank integer not null check (priority_rank >= 1),
      priority_boost numeric(10, 2) not null default 0,
      policy_hints jsonb not null default '{}'::jsonb,
      is_active boolean not null default true,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      unique(set_name, canonical_category, canonical_item_name)
    );

    create index if not exists idx_catalog_priority_set_items_active
      on public.catalog_priority_set_items(set_name, is_active, priority_boost desc, priority_rank asc);

    drop trigger if exists trg_catalog_priority_set_items_updated_at on public.catalog_priority_set_items;
    create trigger trg_catalog_priority_set_items_updated_at
    before update on public.catalog_priority_set_items
    for each row execute function public.set_updated_at();
  end if;
end;
$$;

do $$
declare
  has_catalog_status boolean;
  has_catalog_quality_score boolean;
  has_last_market_signal_at boolean;
  has_category boolean;
begin
  if to_regclass('public.market_source_catalog') is null then
    return;
  end if;

  alter table public.market_source_catalog
    add column if not exists priority_set_name text,
    add column if not exists priority_tier text,
    add column if not exists priority_rank integer,
    add column if not exists priority_boost numeric(10, 2) not null default 0,
    add column if not exists is_priority_item boolean not null default false;

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_priority_tier_check;

  alter table public.market_source_catalog
    add constraint market_source_catalog_priority_tier_check
    check (priority_tier is null or priority_tier in ('tier_a', 'tier_b'));

  select exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'market_source_catalog'
      and column_name = 'catalog_status'
  ) into has_catalog_status;

  select exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'market_source_catalog'
      and column_name = 'catalog_quality_score'
  ) into has_catalog_quality_score;

  select exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'market_source_catalog'
      and column_name = 'last_market_signal_at'
  ) into has_last_market_signal_at;

  if has_catalog_status and has_catalog_quality_score and has_last_market_signal_at then
    execute '
      create index if not exists idx_market_source_catalog_priority_order
      on public.market_source_catalog(
        is_active,
        tradable,
        catalog_status,
        priority_tier asc nulls last,
        catalog_quality_score desc nulls last,
        last_market_signal_at desc nulls last,
        priority_boost desc nulls last
      )';
  else
    execute '
      create index if not exists idx_market_source_catalog_priority_order
      on public.market_source_catalog(
        is_active,
        tradable,
        priority_tier asc nulls last,
        priority_boost desc nulls last
      )';
  end if;

  select exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'market_source_catalog'
      and column_name = 'category'
  ) into has_category;

  if has_category then
    execute '
      create index if not exists idx_market_source_catalog_priority_flags
      on public.market_source_catalog(is_priority_item, priority_tier, category, priority_rank)';
  else
    execute '
      create index if not exists idx_market_source_catalog_priority_flags
      on public.market_source_catalog(is_priority_item, priority_tier, priority_rank)';
  end if;
end;
$$;

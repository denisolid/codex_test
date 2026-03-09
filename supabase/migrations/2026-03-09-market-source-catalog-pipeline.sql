create table if not exists public.market_source_catalog (
  id uuid primary key default gen_random_uuid(),
  market_hash_name text not null,
  item_name text not null,
  category text not null check (category in ('weapon_skin', 'case', 'sticker_capsule')),
  subcategory text,
  tradable boolean not null default true,
  scan_eligible boolean not null default false,
  reference_price numeric(12, 4),
  market_coverage_count integer not null default 0 check (market_coverage_count >= 0),
  liquidity_rank numeric(10, 2),
  volume_7d integer check (volume_7d is null or volume_7d >= 0),
  snapshot_stale boolean not null default false,
  snapshot_captured_at timestamptz,
  invalid_reason text,
  source_tag text not null default 'curated_seed',
  is_active boolean not null default true,
  last_enriched_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_market_source_catalog_market_hash_name
  on public.market_source_catalog(market_hash_name);

create index if not exists idx_market_source_catalog_active_tradable
  on public.market_source_catalog(is_active, tradable, scan_eligible, category);

create index if not exists idx_market_source_catalog_rank
  on public.market_source_catalog(liquidity_rank desc nulls last);

create index if not exists idx_market_source_catalog_updated_at
  on public.market_source_catalog(updated_at desc);

drop trigger if exists trg_market_source_catalog_updated_at on public.market_source_catalog;
create trigger trg_market_source_catalog_updated_at
before update on public.market_source_catalog
for each row execute function public.set_updated_at();

alter table public.market_source_catalog enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'market_source_catalog'
      and policyname = 'market_source_catalog_read_authenticated'
  ) then
    create policy "market_source_catalog_read_authenticated"
    on public.market_source_catalog
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

revoke all on table public.market_source_catalog from anon, authenticated;
grant select on table public.market_source_catalog to authenticated;
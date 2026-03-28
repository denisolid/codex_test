create table if not exists public.catalog_generations (
  id uuid primary key default gen_random_uuid(),
  generation_key text not null unique,
  status text not null default 'active' check (status in ('active', 'archived')),
  is_active boolean not null default false,
  opportunity_scan_enabled boolean not null default false,
  source_generation_id uuid references public.catalog_generations(id) on delete set null,
  activated_at timestamptz,
  archived_at timestamptz,
  opportunity_scan_enabled_at timestamptz,
  diagnostics_summary jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_catalog_generations_updated_at on public.catalog_generations;
create trigger trg_catalog_generations_updated_at
before update on public.catalog_generations
for each row execute function public.set_updated_at();

create unique index if not exists idx_catalog_generations_single_active
  on public.catalog_generations(is_active)
  where is_active = true;

create index if not exists idx_catalog_generations_status_created_at
  on public.catalog_generations(status, created_at desc);

alter table public.catalog_generations enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'catalog_generations'
      and policyname = 'catalog_generations_read_authenticated'
  ) then
    create policy "catalog_generations_read_authenticated"
    on public.catalog_generations
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

revoke all on table public.catalog_generations from anon, authenticated;
grant select on table public.catalog_generations to authenticated;

do $$
declare
  active_generation_id uuid;
begin
  if to_regclass('public.market_source_catalog') is null then
    return;
  end if;

  alter table public.market_source_catalog
    add column if not exists catalog_generation_id uuid;

  select id
  into active_generation_id
  from public.catalog_generations
  where is_active = true
  order by activated_at desc nulls last, created_at desc
  limit 1;

  if active_generation_id is null then
    insert into public.catalog_generations (
      generation_key,
      status,
      is_active,
      opportunity_scan_enabled,
      activated_at,
      opportunity_scan_enabled_at,
      diagnostics_summary
    )
    values (
      concat('legacy-import-', to_char(now(), 'YYYYMMDDHH24MISSMS')),
      'active',
      true,
      true,
      now(),
      now(),
      jsonb_build_object('migratedFromLegacyCatalog', true)
    )
    returning id into active_generation_id;
  end if;

  update public.market_source_catalog
  set catalog_generation_id = active_generation_id
  where catalog_generation_id is null;

  alter table public.market_source_catalog
    alter column catalog_generation_id set not null;

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_catalog_generation_fk;

  alter table public.market_source_catalog
    add constraint market_source_catalog_catalog_generation_fk
    foreign key (catalog_generation_id)
    references public.catalog_generations(id)
    on delete restrict;

  drop index if exists public.idx_market_source_catalog_market_hash_name;

  create unique index if not exists idx_market_source_catalog_generation_market_hash_name
    on public.market_source_catalog(catalog_generation_id, market_hash_name);

  create index if not exists idx_market_source_catalog_generation_active_tradable
    on public.market_source_catalog(
      catalog_generation_id,
      is_active,
      tradable,
      category,
      candidate_status,
      catalog_status
    );

  create index if not exists idx_market_source_catalog_generation_last_enriched
    on public.market_source_catalog(
      catalog_generation_id,
      last_enriched_at asc nulls first,
      liquidity_rank desc nulls last
    );
end;
$$;

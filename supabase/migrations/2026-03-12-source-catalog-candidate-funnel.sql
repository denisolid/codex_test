do $$
begin
  if to_regclass('public.market_source_catalog') is null then
    return;
  end if;

  alter table public.market_source_catalog
    add column if not exists candidate_status text not null default 'candidate',
    add column if not exists missing_snapshot boolean not null default false,
    add column if not exists missing_reference boolean not null default false,
    add column if not exists missing_market_coverage boolean not null default false,
    add column if not exists enrichment_priority numeric(10, 2) not null default 0,
    add column if not exists eligibility_reason text;

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_candidate_status_check;

  alter table public.market_source_catalog
    add constraint market_source_catalog_candidate_status_check
    check (candidate_status in ('candidate', 'enriching', 'eligible', 'rejected'));

  update public.market_source_catalog
  set candidate_status = case
    when scan_eligible = true then 'eligible'
    when tradable = true then 'candidate'
    else 'rejected'
  end
  where candidate_status is null
    or candidate_status not in ('candidate', 'enriching', 'eligible', 'rejected');

  create index if not exists idx_market_source_catalog_candidate_status
    on public.market_source_catalog(is_active, tradable, candidate_status, category);

  create index if not exists idx_market_source_catalog_enrichment_priority
    on public.market_source_catalog(category, candidate_status, enrichment_priority desc nulls last);
end;
$$;

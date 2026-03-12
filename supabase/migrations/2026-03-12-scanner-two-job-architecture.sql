alter table public.scanner_runs
  add column if not exists duration_ms integer not null default 0,
  add column if not exists failure_reason text;

create index if not exists idx_scanner_runs_type_status_started_desc
  on public.scanner_runs(scanner_type, status, started_at desc);

create index if not exists idx_scanner_runs_type_status_completed_desc
  on public.scanner_runs(scanner_type, status, completed_at desc);

do $$
begin
  if to_regclass('public.market_source_catalog') is null then
    return;
  end if;

  alter table public.market_source_catalog
    add column if not exists maturity_state text not null default 'cold',
    add column if not exists maturity_score numeric(10, 2) not null default 0,
    add column if not exists scan_layer text not null default 'cold',
    add column if not exists quote_fetched_at timestamptz;

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_candidate_status_check;

  alter table public.market_source_catalog
    add constraint market_source_catalog_candidate_status_check
    check (
      candidate_status in ('candidate', 'enriching', 'near_eligible', 'eligible', 'rejected')
    );

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_maturity_state_check;

  alter table public.market_source_catalog
    add constraint market_source_catalog_maturity_state_check
    check (maturity_state in ('cold', 'enriching', 'near_eligible', 'eligible'));

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_scan_layer_check;

  alter table public.market_source_catalog
    add constraint market_source_catalog_scan_layer_check
    check (scan_layer in ('hot', 'warm', 'cold'));

  update public.market_source_catalog
  set candidate_status = case
    when candidate_status = 'eligible' then 'eligible'
    when candidate_status = 'rejected' then 'rejected'
    when coalesce(missing_snapshot, false) = false
      and coalesce(missing_reference, false) = false
      and coalesce(missing_market_coverage, false) = false then 'near_eligible'
    when candidate_status = 'enriching' then 'enriching'
    else 'candidate'
  end
  where candidate_status is null
    or candidate_status not in ('candidate', 'enriching', 'near_eligible', 'eligible', 'rejected');

  update public.market_source_catalog
  set maturity_state = case
    when coalesce(scan_eligible, false) = true then 'eligible'
    when candidate_status = 'near_eligible' then 'near_eligible'
    when candidate_status = 'enriching' then 'enriching'
    else 'cold'
  end,
  scan_layer = case
    when coalesce(scan_eligible, false) = true then 'hot'
    when candidate_status in ('near_eligible', 'enriching') then 'warm'
    else 'cold'
  end
  where maturity_state is null
    or maturity_state not in ('cold', 'enriching', 'near_eligible', 'eligible')
    or scan_layer is null
    or scan_layer not in ('hot', 'warm', 'cold');

  create index if not exists idx_market_source_catalog_maturity_state
    on public.market_source_catalog(maturity_state, candidate_status, scan_layer, category);
end;
$$;

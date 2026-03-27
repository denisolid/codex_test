create table if not exists public.global_opportunity_lifecycle_log (
  id uuid primary key default gen_random_uuid(),
  lifecycle_event_key text not null,
  active_opportunity_id uuid references public.global_active_opportunities(id) on delete set null,
  opportunity_fingerprint text not null,
  scan_run_id uuid references public.scanner_runs(id) on delete set null,
  lifecycle_status text not null,
  event_at timestamptz not null default now(),
  category text,
  market_hash_name text,
  item_name text,
  reason text,
  snapshot jsonb not null default '{}'::jsonb
);

create unique index if not exists idx_global_opportunity_lifecycle_log_event_key
  on public.global_opportunity_lifecycle_log(lifecycle_event_key);

create index if not exists idx_global_opportunity_lifecycle_log_fingerprint_event_at
  on public.global_opportunity_lifecycle_log(opportunity_fingerprint, event_at desc);

create index if not exists idx_global_opportunity_lifecycle_log_status_event_at
  on public.global_opportunity_lifecycle_log(lifecycle_status, event_at desc);

create index if not exists idx_global_opportunity_lifecycle_log_active_event_at
  on public.global_opportunity_lifecycle_log(active_opportunity_id, event_at desc);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'global_opportunity_lifecycle_log_status_chk'
  ) then
    alter table public.global_opportunity_lifecycle_log
      add constraint global_opportunity_lifecycle_log_status_chk
      check (
        lifecycle_status in (
          'detected',
          'published',
          'expired',
          'invalidated',
          'blocked_on_emit'
        )
      );
  end if;
end;
$$;

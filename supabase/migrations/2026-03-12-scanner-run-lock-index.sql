alter table public.scanner_runs
  drop constraint if exists scanner_runs_status_check;

update public.scanner_runs
set
  status = 'failed',
  completed_at = coalesce(completed_at, now()),
  diagnostics_summary = coalesce(diagnostics_summary, '{}'::jsonb) || jsonb_build_object(
    'coordination',
    jsonb_build_object(
      'normalizedLegacyStatus',
      true,
      'reason',
      'invalid_status_before_status_check'
    )
  )
where status is null
  or status not in (
    'queued',
    'running',
    'completed',
    'failed',
    'timed_out',
    'skipped_already_running'
  );

alter table public.scanner_runs
  add constraint scanner_runs_status_check
  check (
    status in (
      'queued',
      'running',
      'completed',
      'failed',
      'timed_out',
      'skipped_already_running'
    )
  );

with ranked_running as (
  select
    id,
    scanner_type,
    row_number() over (
      partition by scanner_type
      order by started_at desc nulls last, id desc
    ) as row_rank
  from public.scanner_runs
  where status = 'running'
    and scanner_type in ('enrichment', 'opportunity_scan')
)
update public.scanner_runs as runs
set
  status = 'failed',
  completed_at = coalesce(runs.completed_at, now()),
  diagnostics_summary = coalesce(runs.diagnostics_summary, '{}'::jsonb) || jsonb_build_object(
    'coordination',
    jsonb_build_object(
      'dedupedByLockMigration',
      true,
      'reason',
      'running_conflict_before_unique_index'
    )
  )
from ranked_running as ranked
where runs.id = ranked.id
  and ranked.row_rank > 1;

create unique index if not exists idx_scanner_runs_single_active_per_type
  on public.scanner_runs(scanner_type)
  where status = 'running'
    and scanner_type in ('enrichment', 'opportunity_scan');

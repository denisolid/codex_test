alter table public.global_active_opportunities
  add column if not exists last_revalidation_attempt_at timestamptz;

create index if not exists idx_global_active_opportunities_live_revalidation_queue
  on public.global_active_opportunities(
    live_status,
    coalesce(last_revalidation_attempt_at, last_published_at),
    id
  )
  where live_status = 'live';

create index if not exists idx_global_opportunity_history_active_event_at
  on public.global_opportunity_history(active_opportunity_id, event_at desc);

alter table public.global_opportunity_history
  drop constraint if exists global_opportunity_history_event_type_chk;

alter table public.global_opportunity_history
  add constraint global_opportunity_history_event_type_chk
  check (event_type in ('new', 'updated', 'reactivated', 'expired', 'degraded'));

update public.global_opportunity_history as h
set active_opportunity_id = a.id
from public.global_active_opportunities as a
where h.active_opportunity_id is null
  and coalesce(nullif(h.opportunity_fingerprint, ''), '') <> ''
  and lower(h.opportunity_fingerprint) = lower(a.opportunity_fingerprint);

alter table public.scanner_runs
  add column if not exists heartbeat_at timestamptz;

update public.scanner_runs
set heartbeat_at = coalesce(heartbeat_at, started_at, now())
where heartbeat_at is null;

create index if not exists idx_scanner_runs_type_status_heartbeat_desc
  on public.scanner_runs(scanner_type, status, heartbeat_at desc);

with ranked as (
  select
    id,
    row_number() over (
      partition by lower(opportunity_fingerprint)
      order by
        coalesce(last_published_at, detected_at) desc nulls last,
        coalesce(last_seen_at, detected_at) desc nulls last,
        detected_at desc nulls last,
        id desc
    ) as rank_order
  from public.arbitrage_feed
  where is_active = true
    and coalesce(nullif(opportunity_fingerprint, ''), '') <> ''
)
update public.arbitrage_feed as feed
set is_active = false
from ranked
where feed.id = ranked.id
  and ranked.rank_order > 1;

create unique index if not exists idx_arbitrage_feed_single_active_fingerprint
  on public.arbitrage_feed(opportunity_fingerprint)
  where is_active = true
    and opportunity_fingerprint is not null;

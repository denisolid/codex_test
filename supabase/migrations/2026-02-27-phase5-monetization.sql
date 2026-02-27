alter table public.users
  add column if not exists plan_tier text not null default 'free'
  check (plan_tier in ('free', 'pro', 'team'));

alter table public.users
  add column if not exists billing_status text not null default 'inactive'
  check (billing_status in ('inactive', 'trialing', 'active', 'past_due', 'canceled'));

alter table public.users
  add column if not exists plan_seats integer not null default 1
  check (plan_seats > 0);

alter table public.users
  add column if not exists plan_started_at timestamptz;

create table if not exists public.plan_change_events (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  old_plan_tier text not null check (old_plan_tier in ('free', 'pro', 'team')),
  new_plan_tier text not null check (new_plan_tier in ('free', 'pro', 'team')),
  changed_by text not null default 'self_service',
  created_at timestamptz not null default now()
);

create index if not exists idx_plan_change_events_user_time
  on public.plan_change_events(user_id, created_at desc);

alter table public.plan_change_events enable row level security;

create policy "plan_change_events_read_own"
on public.plan_change_events for select
using (auth.uid() = user_id);

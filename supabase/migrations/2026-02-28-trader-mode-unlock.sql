alter table public.users
  add column if not exists trader_mode_unlocked boolean not null default false;

alter table public.users
  add column if not exists trader_mode_unlocked_at timestamptz;

alter table public.users
  add column if not exists trader_mode_unlock_source text;

create table if not exists public.trader_mode_unlock_events (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  action text not null check (action in ('unlocked', 'locked')),
  source text not null default 'admin_toggle',
  changed_by text not null default 'system',
  note text,
  created_at timestamptz not null default now()
);

create index if not exists idx_trader_mode_unlock_events_user_time
  on public.trader_mode_unlock_events(user_id, created_at desc);

alter table public.trader_mode_unlock_events enable row level security;

create policy "trader_mode_unlock_events_read_own"
on public.trader_mode_unlock_events for select
using (auth.uid() = user_id);

alter table public.users
  add column if not exists public_portfolio_enabled boolean not null default true;

alter table public.users
  add column if not exists ownership_alerts_enabled boolean not null default true;

create table if not exists public.watchlists (
  user_id uuid not null references public.users(id) on delete cascade,
  target_user_id uuid not null references public.users(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (user_id, target_user_id),
  constraint watchlists_no_self_follow_chk check (user_id <> target_user_id)
);

create index if not exists idx_watchlists_target_user_id
  on public.watchlists(target_user_id);

create table if not exists public.ownership_alert_events (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  skin_id bigint references public.skins(id) on delete set null,
  market_hash_name text not null,
  change_type text not null check (
    change_type in ('acquired', 'disposed', 'increased', 'decreased')
  ),
  previous_quantity integer not null default 0 check (previous_quantity >= 0),
  new_quantity integer not null default 0 check (new_quantity >= 0),
  quantity_delta integer not null,
  estimated_value_delta numeric(14,2),
  currency text not null default 'USD',
  synced_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists idx_ownership_alert_events_user_time
  on public.ownership_alert_events(user_id, created_at desc);

create table if not exists public.public_portfolio_views (
  id bigserial primary key,
  owner_user_id uuid not null references public.users(id) on delete cascade,
  referrer text,
  viewed_at timestamptz not null default now()
);

create index if not exists idx_public_portfolio_views_owner_time
  on public.public_portfolio_views(owner_user_id, viewed_at desc);

alter table public.watchlists enable row level security;
alter table public.ownership_alert_events enable row level security;
alter table public.public_portfolio_views enable row level security;

create policy "watchlists_manage_own"
on public.watchlists for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "ownership_alert_events_read_own"
on public.ownership_alert_events for select
using (auth.uid() = user_id);

create policy "public_portfolio_views_read_own"
on public.public_portfolio_views for select
using (auth.uid() = owner_user_id);

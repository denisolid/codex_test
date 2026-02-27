create extension if not exists pgcrypto;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.users (
  id uuid primary key references auth.users(id) on delete cascade,
  email text not null unique,
  display_name text,
  avatar_url text,
  steam_id64 text unique,
  public_portfolio_enabled boolean not null default true,
  ownership_alerts_enabled boolean not null default true,
  plan_tier text not null default 'free' check (plan_tier in ('free', 'pro', 'team')),
  billing_status text not null default 'inactive' check (
    billing_status in ('inactive', 'trialing', 'active', 'past_due', 'canceled')
  ),
  plan_seats integer not null default 1 check (plan_seats > 0),
  plan_started_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint steam_id64_format_chk check (
    steam_id64 is null or steam_id64 ~ '^[0-9]{17}$'
  )
);

create trigger trg_users_updated_at
before update on public.users
for each row execute function public.set_updated_at();

create table if not exists public.skins (
  id bigserial primary key,
  market_hash_name text not null unique,
  weapon text,
  skin_name text,
  exterior text,
  rarity text,
  image_url text,
  created_at timestamptz not null default now()
);

create table if not exists public.inventories (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  skin_id bigint not null references public.skins(id) on delete restrict,
  quantity integer not null default 1 check (quantity > 0),
  steam_item_ids text[] not null default '{}',
  purchase_price numeric(12,2),
  purchase_currency text not null default 'USD',
  last_synced_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, skin_id)
);

create trigger trg_inventories_updated_at
before update on public.inventories
for each row execute function public.set_updated_at();

create index if not exists idx_inventories_user_id on public.inventories(user_id);
create index if not exists idx_inventories_skin_id on public.inventories(skin_id);

create table if not exists public.price_history (
  id bigserial primary key,
  skin_id bigint not null references public.skins(id) on delete cascade,
  price numeric(12,2) not null check (price >= 0),
  currency text not null default 'USD',
  source text not null default 'mock',
  recorded_at timestamptz not null default now()
);

create index if not exists idx_price_history_skin_time
  on public.price_history(skin_id, recorded_at desc);

create table if not exists public.transactions (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  skin_id bigint not null references public.skins(id) on delete restrict,
  type text not null check (type in ('buy', 'sell')),
  quantity integer not null check (quantity > 0),
  unit_price numeric(12,2) not null check (unit_price >= 0),
  commission_percent numeric(5,2) not null default 13.00 check (
    commission_percent >= 0 and commission_percent <= 100
  ),
  gross_total numeric(14,2),
  net_total numeric(14,2),
  currency text not null default 'USD',
  executed_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists idx_transactions_user_id on public.transactions(user_id);
create index if not exists idx_transactions_skin_id on public.transactions(skin_id);

create table if not exists public.market_item_snapshots (
  id bigserial primary key,
  skin_id bigint not null references public.skins(id) on delete cascade,
  lowest_listing_price numeric(12,2) not null check (lowest_listing_price >= 0),
  average_7d_price numeric(12,2) not null check (average_7d_price >= 0),
  volume_24h integer not null default 0 check (volume_24h >= 0),
  spread_percent numeric(8,2) not null default 0 check (spread_percent >= 0),
  volatility_7d_percent numeric(8,2) not null default 0 check (volatility_7d_percent >= 0),
  currency text not null default 'USD',
  source text not null default 'derived-price-history',
  captured_at timestamptz not null default now()
);

create index if not exists idx_market_snapshots_skin_time
  on public.market_item_snapshots(skin_id, captured_at desc);

create table if not exists public.price_alerts (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  skin_id bigint not null references public.skins(id) on delete cascade,
  target_price numeric(12,2) check (target_price >= 0),
  percent_change_threshold numeric(8,2) check (percent_change_threshold >= 0),
  direction text not null default 'both' check (direction in ('up', 'down', 'both')),
  enabled boolean not null default true,
  cooldown_minutes integer not null default 60 check (cooldown_minutes >= 0),
  last_triggered_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint price_alerts_condition_chk check (
    target_price is not null or percent_change_threshold is not null
  )
);

create trigger trg_price_alerts_updated_at
before update on public.price_alerts
for each row execute function public.set_updated_at();

create index if not exists idx_price_alerts_user_enabled
  on public.price_alerts(user_id, enabled);
create index if not exists idx_price_alerts_skin_enabled
  on public.price_alerts(skin_id, enabled);

create table if not exists public.alert_events (
  id bigserial primary key,
  alert_id bigint not null references public.price_alerts(id) on delete cascade,
  user_id uuid not null references public.users(id) on delete cascade,
  skin_id bigint not null references public.skins(id) on delete cascade,
  trigger_type text not null check (trigger_type in ('target_price', 'percent_change')),
  trigger_value numeric(12,2) not null,
  market_price numeric(12,2) not null check (market_price >= 0),
  previous_price numeric(12,2),
  change_percent numeric(8,2),
  triggered_at timestamptz not null default now()
);

create index if not exists idx_alert_events_user_time
  on public.alert_events(user_id, triggered_at desc);

create table if not exists public.extension_api_keys (
  id bigserial primary key,
  user_id uuid not null references public.users(id) on delete cascade,
  key_hash text not null unique,
  key_prefix text not null,
  label text not null default 'default',
  last_used_at timestamptz,
  expires_at timestamptz,
  revoked_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_extension_api_keys_user
  on public.extension_api_keys(user_id, revoked_at);

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

create or replace function public.handle_new_auth_user()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.users (id, email)
  values (new.id, coalesce(new.email, 'unknown@example.com'))
  on conflict (id) do nothing;
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
after insert on auth.users
for each row execute procedure public.handle_new_auth_user();

alter table public.users enable row level security;
alter table public.inventories enable row level security;
alter table public.transactions enable row level security;
alter table public.skins enable row level security;
alter table public.price_history enable row level security;
alter table public.market_item_snapshots enable row level security;
alter table public.price_alerts enable row level security;
alter table public.alert_events enable row level security;
alter table public.extension_api_keys enable row level security;
alter table public.watchlists enable row level security;
alter table public.ownership_alert_events enable row level security;
alter table public.public_portfolio_views enable row level security;
alter table public.plan_change_events enable row level security;

create policy "users_select_own"
on public.users for select
using (auth.uid() = id);

create policy "users_update_own"
on public.users for update
using (auth.uid() = id);

create policy "inventories_manage_own"
on public.inventories for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "transactions_manage_own"
on public.transactions for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "skins_read_authenticated"
on public.skins for select
using (auth.role() = 'authenticated');

create policy "price_history_read_authenticated"
on public.price_history for select
using (auth.role() = 'authenticated');

create policy "market_item_snapshots_read_authenticated"
on public.market_item_snapshots for select
using (auth.role() = 'authenticated');

create policy "price_alerts_manage_own"
on public.price_alerts for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

create policy "alert_events_read_own"
on public.alert_events for select
using (auth.uid() = user_id);

create policy "extension_api_keys_manage_own"
on public.extension_api_keys for all
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

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

create policy "plan_change_events_read_own"
on public.plan_change_events for select
using (auth.uid() = user_id);

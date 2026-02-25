alter table public.transactions
add column if not exists commission_percent numeric(5,2) not null default 13.00 check (commission_percent >= 0 and commission_percent <= 100),
add column if not exists gross_total numeric(14,2),
add column if not exists net_total numeric(14,2);

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
  on public.market_item_snapshots (skin_id, captured_at desc);

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
  on public.price_alerts (user_id, enabled);
create index if not exists idx_price_alerts_skin_enabled
  on public.price_alerts (skin_id, enabled);

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
  on public.alert_events (user_id, triggered_at desc);

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
  on public.extension_api_keys (user_id, revoked_at);

alter table public.market_item_snapshots enable row level security;
alter table public.price_alerts enable row level security;
alter table public.alert_events enable row level security;
alter table public.extension_api_keys enable row level security;

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

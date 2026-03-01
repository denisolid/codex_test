create table if not exists public.market_prices (
  id uuid primary key default gen_random_uuid(),
  market text not null check (market in ('steam', 'skinport', 'csfloat', 'dmarket')),
  market_hash_name text not null,
  currency text not null default 'USD',
  gross_price numeric(12,2) not null check (gross_price >= 0),
  net_price numeric(12,2) not null check (net_price >= 0),
  url text,
  fetched_at timestamptz not null default now(),
  raw jsonb
);

create unique index if not exists idx_market_prices_market_name_unique
  on public.market_prices(market, market_hash_name);

create index if not exists idx_market_prices_name
  on public.market_prices(market_hash_name);

create index if not exists idx_market_prices_fetched_at
  on public.market_prices(fetched_at desc);

create table if not exists public.user_price_preferences (
  user_id uuid primary key references public.users(id) on delete cascade,
  pricing_mode text not null default 'lowest_buy' check (
    pricing_mode in ('steam', 'best_sell_net', 'lowest_buy')
  ),
  preferred_currency text not null default 'USD' check (char_length(preferred_currency) = 3),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

drop trigger if exists trg_user_price_preferences_updated_at on public.user_price_preferences;
create trigger trg_user_price_preferences_updated_at
before update on public.user_price_preferences
for each row execute function public.set_updated_at();

create index if not exists idx_user_price_preferences_mode
  on public.user_price_preferences(pricing_mode);

alter table public.market_prices enable row level security;
alter table public.user_price_preferences enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'market_prices'
      and policyname = 'market_prices_read_authenticated'
  ) then
    create policy "market_prices_read_authenticated"
    on public.market_prices
    for select
    using (auth.role() = 'authenticated');
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'public'
      and tablename = 'user_price_preferences'
      and policyname = 'user_price_preferences_manage_own'
  ) then
    create policy "user_price_preferences_manage_own"
    on public.user_price_preferences
    for all
    using (auth.uid() = user_id)
    with check (auth.uid() = user_id);
  end if;
end;
$$;

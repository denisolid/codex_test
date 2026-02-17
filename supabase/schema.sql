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
  steam_id64 text unique,
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
  currency text not null default 'USD',
  executed_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists idx_transactions_user_id on public.transactions(user_id);
create index if not exists idx_transactions_skin_id on public.transactions(skin_id);

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

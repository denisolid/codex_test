alter table public.inventories
add column if not exists steam_item_ids text[] not null default '{}';

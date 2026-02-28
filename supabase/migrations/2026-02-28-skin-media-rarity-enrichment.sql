alter table if exists public.skins
  add column if not exists image_url_large text;

alter table if exists public.skins
  add column if not exists rarity_color text;

alter table if exists public.skins
  add column if not exists updated_at timestamptz not null default now();

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_skins_updated_at on public.skins;
create trigger trg_skins_updated_at
before update on public.skins
for each row execute function public.set_updated_at();

create index if not exists idx_skins_rarity on public.skins(rarity);
create index if not exists idx_skins_updated_at on public.skins(updated_at desc);

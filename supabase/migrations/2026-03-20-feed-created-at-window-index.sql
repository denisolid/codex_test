alter table public.arbitrage_feed
  add column if not exists created_at timestamptz;

update public.arbitrage_feed
set created_at = coalesce(created_at, detected_at, now())
where created_at is null;

alter table public.arbitrage_feed
  alter column created_at set default now();

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'arbitrage_feed'
      and column_name = 'created_at'
      and is_nullable = 'YES'
  ) then
    alter table public.arbitrage_feed
      alter column created_at set not null;
  end if;
end;
$$;

create index if not exists idx_arbitrage_feed_created_at_desc
  on public.arbitrage_feed(created_at desc, id desc);

create index if not exists idx_arbitrage_feed_active_created_at_desc
  on public.arbitrage_feed(is_active, created_at desc, id desc);

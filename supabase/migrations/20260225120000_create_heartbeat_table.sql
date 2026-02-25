create table if not exists public.heartbeat (
  id integer primary key,
  updated_at timestamptz not null default now()
);

insert into public.heartbeat (id)
values (1)
on conflict (id) do nothing;

alter table public.heartbeat enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'heartbeat'
      and policyname = 'heartbeat_select_public'
  ) then
    create policy "heartbeat_select_public"
      on public.heartbeat
      for select
      using (true);
  end if;
end
$$;

revoke all on table public.heartbeat from anon, authenticated;
grant select on table public.heartbeat to anon, authenticated;

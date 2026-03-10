update public.users
set plan_tier = case
  when plan_tier in ('pro', 'team') then 'full_access'
  when plan_tier in ('free', 'full_access', 'api_advanced') then plan_tier
  else 'free'
end;

update public.users
set plan = case
  when coalesce(nullif(plan, ''), '') in ('pro', 'team') then 'full_access'
  when coalesce(nullif(plan, ''), '') in ('free', 'full_access', 'api_advanced') then plan
  else plan_tier
end;

alter table public.users
  drop constraint if exists users_plan_tier_check;

alter table public.users
  drop constraint if exists plan_tier_check;

alter table public.users
  add constraint users_plan_tier_check
  check (plan_tier in ('free', 'full_access', 'api_advanced'));

alter table public.users
  drop constraint if exists users_plan_check;

alter table public.users
  add constraint users_plan_check
  check (plan in ('free', 'full_access', 'api_advanced'));

update public.plan_change_events
set old_plan_tier = case
  when old_plan_tier in ('pro', 'team') then 'full_access'
  when old_plan_tier in ('free', 'full_access', 'api_advanced') then old_plan_tier
  else 'free'
end;

update public.plan_change_events
set new_plan_tier = case
  when new_plan_tier in ('pro', 'team') then 'full_access'
  when new_plan_tier in ('free', 'full_access', 'api_advanced') then new_plan_tier
  else 'free'
end;

alter table public.plan_change_events
  drop constraint if exists plan_change_events_old_plan_tier_check;

alter table public.plan_change_events
  drop constraint if exists old_plan_tier_check;

alter table public.plan_change_events
  add constraint plan_change_events_old_plan_tier_check
  check (old_plan_tier in ('free', 'full_access', 'api_advanced'));

alter table public.plan_change_events
  drop constraint if exists plan_change_events_new_plan_tier_check;

alter table public.plan_change_events
  drop constraint if exists new_plan_tier_check;

alter table public.plan_change_events
  add constraint plan_change_events_new_plan_tier_check
  check (new_plan_tier in ('free', 'full_access', 'api_advanced'));

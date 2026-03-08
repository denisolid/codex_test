alter table public.users
  add column if not exists email_verified boolean not null default false;

alter table public.users
  add column if not exists onboarding_completed boolean not null default false;

alter table public.users
  add column if not exists pending_email text;

alter table public.users
  add column if not exists plan text not null default 'free';

alter table public.users
  add column if not exists plan_status text not null default 'active';

update public.users
set plan = coalesce(nullif(plan, ''), nullif(plan_tier, ''), 'free')
where plan is null
   or btrim(plan) = '';

update public.users
set email_verified = true,
    onboarding_completed = true,
    plan_status = coalesce(nullif(plan_status, ''), 'active')
where coalesce(email, '') !~* '^steam_[0-9]{17}@steam\\.local$';

update public.users
set email_verified = false,
    onboarding_completed = false,
    plan_status = 'pending_verification'
where coalesce(email, '') ~* '^steam_[0-9]{17}@steam\\.local$';

create table if not exists public.user_email_verifications (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.users(id) on delete cascade,
  email text not null,
  token_hash text not null unique,
  expires_at timestamptz not null,
  used_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_user_email_verifications_user_created
  on public.user_email_verifications(user_id, created_at desc);

create index if not exists idx_user_email_verifications_expires
  on public.user_email_verifications(expires_at);

alter table public.user_email_verifications enable row level security;

revoke all on table public.user_email_verifications from anon, authenticated;

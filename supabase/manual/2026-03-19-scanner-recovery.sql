-- Scanner recovery one-shot script
-- Run in Supabase SQL editor against the target project.

begin;

-- 1) Align catalog schema with scanner code expectations.
alter table public.market_source_catalog
  add column if not exists snapshot_state text,
  add column if not exists reference_state text,
  add column if not exists liquidity_state text,
  add column if not exists coverage_state text,
  add column if not exists progression_status text,
  add column if not exists progression_blockers text[] not null default '{}'::text[];

-- 2) Recompute lightweight progression diagnostics (no hard gating).
update public.market_source_catalog
set snapshot_state = case
      when coalesce(missing_snapshot, false) = true or snapshot_captured_at is null
        then 'missing_snapshot'
      when coalesce(snapshot_stale, false) = true
        then 'stale_snapshot'
      when reference_price is null or volume_7d is null
        then 'partial_snapshot'
      else 'snapshot_ready'
    end,
    reference_state = case
      when reference_price is null
        then 'missing_reference'
      when snapshot_captured_at is not null
        then 'snapshot_reference'
      else 'quote_reference'
    end,
    liquidity_state = case
      when volume_7d is null and coalesce(liquidity_rank, 0) > 0
        then 'partial_liquidity'
      when volume_7d is null
        then 'missing_liquidity'
      else 'liquidity_ready'
    end,
    coverage_state = case
      when coalesce(market_coverage_count, 0) <= 0
        then 'missing_coverage'
      when coalesce(market_coverage_count, 0) < 2
        then 'insufficient_coverage'
      else 'coverage_ready'
    end,
    progression_status = case
      when coalesce(scan_eligible, false) = true
        then 'eligible'
      when candidate_status = 'near_eligible'
        then 'blocked_from_eligible'
      when candidate_status = 'rejected'
        then 'rejected'
      else 'blocked_from_near_eligible'
    end,
    progression_blockers = case
      when coalesce(scan_eligible, false) = true then '{}'::text[]
      else array_remove(array[
        case when candidate_status = 'rejected' then 'anti_fake_guard' end,
        case
          when coalesce(missing_snapshot, false) = true or snapshot_captured_at is null
            then 'missing_snapshot'
          when coalesce(snapshot_stale, false) = true
            then 'stale_snapshot'
        end,
        case
          when snapshot_captured_at is not null and (reference_price is null or volume_7d is null)
            then 'partial_snapshot'
        end,
        case when reference_price is null then 'missing_reference' end,
        case
          when volume_7d is null and coalesce(liquidity_rank, 0) > 0
            then 'partial_liquidity'
          when volume_7d is null
            then 'missing_liquidity'
        end,
        case when coalesce(market_coverage_count, 0) < 2 then 'insufficient_coverage' end
      ], null)
    end;

-- 3) Remove legacy invalid_reason labels that should not behave as hard rejects now.
update public.market_source_catalog
set invalid_reason = null
where candidate_status in ('candidate', 'enriching', 'near_eligible', 'eligible')
  and invalid_reason in (
    'excludedMissingReferenceItems',
    'excludedLowLiquidityItems',
    'excludedLowValueItems',
    'excludedWeakMarketCoverageItems'
  );

commit;

-- 4) Quick verification (run after commit).
select
  count(*) as active_rows,
  count(*) filter (where candidate_status = 'candidate') as candidate_rows,
  count(*) filter (where candidate_status = 'enriching') as enriching_rows,
  count(*) filter (where candidate_status = 'near_eligible') as near_eligible_rows,
  count(*) filter (where candidate_status = 'eligible') as eligible_rows,
  count(*) filter (where candidate_status = 'rejected') as rejected_rows
from public.market_source_catalog
where is_active = true;

select
  count(*) filter (where snapshot_state is null) as missing_snapshot_state,
  count(*) filter (where reference_state is null) as missing_reference_state,
  count(*) filter (where liquidity_state is null) as missing_liquidity_state,
  count(*) filter (where coverage_state is null) as missing_coverage_state,
  count(*) filter (where progression_status is null) as missing_progression_status
from public.market_source_catalog
where is_active = true;

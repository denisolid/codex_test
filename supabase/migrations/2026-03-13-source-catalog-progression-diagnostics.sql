do $$
begin
  if to_regclass('public.market_source_catalog') is null then
    return;
  end if;

  alter table public.market_source_catalog
    add column if not exists snapshot_state text,
    add column if not exists reference_state text,
    add column if not exists liquidity_state text,
    add column if not exists coverage_state text,
    add column if not exists progression_status text,
    add column if not exists progression_blockers text[] not null default '{}'::text[];

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
      end
  where snapshot_state is null
    or reference_state is null
    or liquidity_state is null
    or coverage_state is null
    or progression_status is null
    or progression_blockers is null;
end;
$$;

do $$
begin
  if to_regclass('public.market_source_catalog') is null then
    return;
  end if;

  alter table public.market_source_catalog
    add column if not exists catalog_status text,
    add column if not exists catalog_block_reason text,
    add column if not exists catalog_quality_score numeric(10, 2),
    add column if not exists last_market_signal_at timestamptz;

  alter table public.market_source_catalog
    alter column catalog_status set default 'shadow';

  alter table public.market_source_catalog
    alter column catalog_quality_score set default 0;

  update public.market_source_catalog
  set
    catalog_status = coalesce(catalog_status, 'shadow'),
    catalog_quality_score = coalesce(catalog_quality_score, 0)
  where catalog_status is null or catalog_quality_score is null;

  alter table public.market_source_catalog
    drop constraint if exists market_source_catalog_catalog_status_check;

  alter table public.market_source_catalog
    add constraint market_source_catalog_catalog_status_check
    check (catalog_status in ('scannable', 'shadow', 'blocked'));

  with classified as (
    select
      m.id,
      coalesce(
        greatest(m.snapshot_captured_at, m.quote_fetched_at),
        m.snapshot_captured_at,
        m.quote_fetched_at
      ) as next_last_market_signal_at,
      case
        when coalesce(m.invalid_reason, '') ~* '(invalid|rejected|anti[_\\s-]?fake|not[_\\s-]?tradable|broken|outofscope|namepattern|unsupported)'
          then 'blocked'
        when m.reference_price is not null and m.reference_price > 0 and m.reference_price < 2
          then 'blocked'
        when coalesce(m.market_coverage_count, 0) <= 0
          and m.reference_price is null
          and m.snapshot_captured_at is null
          and m.quote_fetched_at is null
          then 'blocked'
        when m.reference_price is null
          then 'shadow'
        when coalesce(m.market_coverage_count, 0) < 2
          then 'shadow'
        when coalesce(
          greatest(m.snapshot_captured_at, m.quote_fetched_at),
          m.snapshot_captured_at,
          m.quote_fetched_at
        ) is null
          then 'shadow'
        when coalesce(
          greatest(m.snapshot_captured_at, m.quote_fetched_at),
          m.snapshot_captured_at,
          m.quote_fetched_at
        ) < (now() - interval '4 hours')
          then 'shadow'
        else 'scannable'
      end as next_catalog_status,
      case
        when coalesce(m.invalid_reason, '') ~* '(invalid|rejected|anti[_\\s-]?fake|not[_\\s-]?tradable|broken|outofscope|namepattern|unsupported)'
          then 'invalid_catalog_reason'
        when m.reference_price is not null and m.reference_price > 0 and m.reference_price < 2
          then 'below_min_cost_floor'
        when coalesce(m.market_coverage_count, 0) <= 0
          and m.reference_price is null
          and m.snapshot_captured_at is null
          and m.quote_fetched_at is null
          then 'unusable_market_coverage'
        when coalesce(
          greatest(m.snapshot_captured_at, m.quote_fetched_at),
          m.snapshot_captured_at,
          m.quote_fetched_at
        ) is not null
          and coalesce(
            greatest(m.snapshot_captured_at, m.quote_fetched_at),
            m.snapshot_captured_at,
            m.quote_fetched_at
          ) < (now() - interval '4 hours')
          then 'stale_only_signals'
        when coalesce(m.market_coverage_count, 0) < 2
          then 'weak_market_coverage'
        when m.reference_price is null
          then 'incomplete_reference_pricing'
        else null
      end as next_catalog_block_reason,
      (
        coalesce(m.maturity_score, 0) * 0.58 +
        least(greatest(coalesce(m.liquidity_rank, 0), 0), 100) * 0.32 +
        least(greatest(coalesce(m.market_coverage_count, 0), 0) * 4, 12) +
        case when m.reference_price is null then 0 else least(greatest(m.reference_price, 0) * 1.5, 10) end +
        case
          when coalesce(
            greatest(m.snapshot_captured_at, m.quote_fetched_at),
            m.snapshot_captured_at,
            m.quote_fetched_at
          ) >= (now() - interval '2 hours') then 8
          when coalesce(
            greatest(m.snapshot_captured_at, m.quote_fetched_at),
            m.snapshot_captured_at,
            m.quote_fetched_at
          ) >= (now() - interval '6 hours') then 4
          when coalesce(
            greatest(m.snapshot_captured_at, m.quote_fetched_at),
            m.snapshot_captured_at,
            m.quote_fetched_at
          ) is not null then -6
          else -10
        end
      ) as quality_base
    from public.market_source_catalog m
  )
  update public.market_source_catalog as m
  set
    catalog_status = classified.next_catalog_status,
    catalog_block_reason = classified.next_catalog_block_reason,
    last_market_signal_at = classified.next_last_market_signal_at,
    catalog_quality_score = case
      when classified.next_catalog_status = 'blocked'
        then 0
      when classified.next_catalog_status = 'shadow'
        then round(greatest(5, least(100, classified.quality_base - 22))::numeric, 2)
      else
        round(greatest(0, least(100, classified.quality_base))::numeric, 2)
    end
  from classified
  where m.id = classified.id;

  update public.market_source_catalog
  set
    catalog_status = coalesce(catalog_status, 'shadow'),
    catalog_quality_score = coalesce(catalog_quality_score, 0)
  where catalog_status is null or catalog_quality_score is null;

  alter table public.market_source_catalog
    alter column catalog_status set not null;

  alter table public.market_source_catalog
    alter column catalog_quality_score set not null;

  create index if not exists idx_market_source_catalog_scanner_source_v2
    on public.market_source_catalog(
      category,
      catalog_quality_score desc nulls last,
      last_market_signal_at desc nulls last,
      liquidity_rank desc nulls last
    )
    where is_active = true and tradable = true and catalog_status = 'scannable';

  create index if not exists idx_market_source_catalog_status_reason
    on public.market_source_catalog(catalog_status, catalog_block_reason, category);
end;
$$;

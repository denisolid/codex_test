update public.market_universe
set
  is_active = false,
  updated_at = now()
where is_active = true;

with latest_snapshot as (
  select distinct on (mis.skin_id)
    mis.skin_id,
    mis.lowest_listing_price,
    mis.average_7d_price,
    mis.volume_24h,
    mis.spread_percent,
    mis.volatility_7d_percent,
    mis.captured_at
  from public.market_item_snapshots mis
  order by mis.skin_id, mis.captured_at desc
),
candidate_pool as (
  select
    s.market_hash_name,
    s.market_hash_name as item_name,
    case
      when lower(s.market_hash_name) like '%sticker capsule' then 'sticker_capsule'
      when lower(s.market_hash_name) like '% case' then 'case'
      else 'weapon_skin'
    end as inferred_category,
    coalesce(ls.average_7d_price, ls.lowest_listing_price, 0) as reference_price,
    coalesce(ls.volume_24h, 0) as volume_24h,
    coalesce(ls.spread_percent, 0) as spread_percent,
    coalesce(ls.volatility_7d_percent, 0) as volatility_7d_percent,
    ls.captured_at
  from latest_snapshot ls
  join public.skins s on s.id = ls.skin_id
  where coalesce(trim(s.market_hash_name), '') <> ''
    and ls.captured_at >= now() - interval '45 days'
),
quality_filtered as (
  select *
  from candidate_pool c
  where
    (
      c.inferred_category = 'weapon_skin'
      and c.market_hash_name like '%|%'
      and c.reference_price >= 2
      and c.volume_24h >= 8
    )
    or (
      c.inferred_category = 'case'
      and c.reference_price >= 1
      and c.volume_24h >= 10
    )
    or (
      c.inferred_category = 'sticker_capsule'
      and c.reference_price >= 1
      and c.volume_24h >= 6
    )
),
scored as (
  select
    q.*,
    (
      least(q.volume_24h, 2500) * 12
      + least(q.reference_price, 300) * 9
      + greatest(0, 60 - least(q.spread_percent, 60)) * 7
      + greatest(0, 40 - least(q.volatility_7d_percent, 40)) * 2
    ) as liquidity_signal,
    row_number() over (
      partition by q.inferred_category
      order by
        (
          least(q.volume_24h, 2500) * 12
          + least(q.reference_price, 300) * 9
          + greatest(0, 60 - least(q.spread_percent, 60)) * 7
          + greatest(0, 40 - least(q.volatility_7d_percent, 40)) * 2
        ) desc,
        q.volume_24h desc,
        q.reference_price desc,
        q.market_hash_name asc
    ) as category_rank
  from quality_filtered q
),
balanced as (
  select
    s.*,
    case
      when s.inferred_category = 'weapon_skin' and s.category_rank <= 360 then 1
      when s.inferred_category = 'case' and s.category_rank <= 90 then 1
      when s.inferred_category = 'sticker_capsule' and s.category_rank <= 50 then 1
      else 0
    end as category_quota_pass
  from scored s
),
top_500 as (
  select
    b.market_hash_name,
    b.item_name,
    row_number() over (
      order by
        b.category_quota_pass desc,
        b.liquidity_signal desc,
        b.volume_24h desc,
        b.reference_price desc,
        b.market_hash_name asc
    ) as liquidity_rank
  from balanced b
  order by liquidity_rank
  limit 500
)
insert into public.market_universe (
  market_hash_name,
  item_name,
  liquidity_rank,
  is_active
)
select
  t.market_hash_name,
  t.item_name,
  t.liquidity_rank,
  true
from top_500 t
on conflict (market_hash_name) do update
set
  item_name = excluded.item_name,
  liquidity_rank = excluded.liquidity_rank,
  is_active = true,
  updated_at = now();
